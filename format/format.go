package format

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"

	"github.com/var-raphael/vexaro-engine/clean"
	"github.com/var-raphael/vexaro-engine/db"
	"github.com/var-raphael/vexaro-engine/storage"
)

func Format(schema *Schema) error {
	database := db.Get()

	rows, err := database.Query(`
		SELECT q.queue_id, du.folder_path, COALESCE(d.include_links, 0)
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE q.status = 'proceed-format'
		AND du.folder_path IS NOT NULL
		ORDER BY q.queue_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query proceed-format: %w", err)
	}
	defer rows.Close()

	type formatJob struct {
		QueueID      int64
		FolderPath   string
		IncludeLinks bool
	}

	var jobs []formatJob
	for rows.Next() {
		var j formatJob
		if err := rows.Scan(&j.QueueID, &j.FolderPath, &j.IncludeLinks); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan proceed-format rows: %w", err)
	}

	if len(jobs) == 0 {
		log.Printf("[format] no proceed-format urls in queue")
		return nil
	}

	log.Printf("[format] processing %d proceed-format jobs", len(jobs))

	succeeded := 0
	failed := 0

	for _, job := range jobs {
		cleanedPath := filepath.Join(job.FolderPath, "cleaned.json")

		cleaned, err := loadCleanedJSON(cleanedPath)
		if err != nil {
			log.Printf("[format] failed to load queue_id=%d path=%s: %v", job.QueueID, cleanedPath, err)
			markFailed(database, job.QueueID)
			failed++
			continue
		}

		payload := FilterBySchema(cleaned, schema)

		if raw, ok := payload["content"].(string); ok && raw != "" {
			payload["content"] = compressContent(raw, job.IncludeLinks)
		}

		outPath := filepath.Join(job.FolderPath, "format.json")
		if err := writeJSON(outPath, payload); err != nil {
			log.Printf("[format] write failed queue_id=%d: %v", job.QueueID, err)
			markFailed(database, job.QueueID)
			failed++
			continue
		}

		markProceedExtract(database, job.QueueID)
		log.Printf("[format] done queue_id=%d → %s", job.QueueID, outPath)
		succeeded++
	}

	log.Printf("[format] done — %d formatted, %d failed", succeeded, failed)
	return nil
}

// ---------------------------------------------------------------- db ------

func markProceedExtract(database *sql.DB, queueID int64) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'proceed-extract', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[format] markProceedExtract queue_id=%d: %v", queueID, err)
	}
}

func markProceedVersion(database *sql.DB, queueID int64) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'proceed-version', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[format] markProceedVersion queue_id=%d: %v", queueID, err)
	}
}

func markFailed(database *sql.DB, queueID int64) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'failed', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[format] markFailed queue_id=%d: %v", queueID, err)
	}
}

// ---------------------------------------------------------------- helpers --

func loadCleanedJSON(path string) (*clean.CleanedData, error) {
	b, err := storage.Read(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	var data clean.CleanedData
	if err := json.Unmarshal(b, &data); err != nil {
		return nil, fmt.Errorf("parse json: %w", err)
	}
	return &data, nil
}

func writeJSON(path string, v interface{}) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return storage.Write(path, b)
}

func FormatDataset(datasetID int64) error {
	database := db.Get()

	var datasetType string
	err := database.QueryRow(`
		SELECT COALESCE(dataset_type, 'web') FROM datasets WHERE dataset_id = ?
	`, datasetID).Scan(&datasetType)
	if err != nil {
		return fmt.Errorf("load dataset type: %w", err)
	}

	if datasetType == "amazon" {
		return formatAmazon(datasetID, database)
	}

	var includeLinks bool
	err = database.QueryRow(`
		SELECT COALESCE(include_links, 0) FROM datasets WHERE dataset_id = ?
	`, datasetID).Scan(&includeLinks)
	if err != nil {
		return fmt.Errorf("load include_links: %w", err)
	}

	var fieldsJSON string
	err = database.QueryRow(`
		SELECT fields FROM dataset_schema WHERE dataset_id = ?
	`, datasetID).Scan(&fieldsJSON)
	if err != nil {
		return fmt.Errorf("load schema: %w", err)
	}

	var rawFields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(fieldsJSON), &rawFields); err != nil {
		return fmt.Errorf("parse schema: %w", err)
	}

	schema, err := ParseSchema(rawFields)
	if err != nil {
		return fmt.Errorf("parse schema fields: %w", err)
	}

	rows, err := database.Query(`
		SELECT q.queue_id, du.folder_path, du.url
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-format'
		AND du.folder_path IS NOT NULL
		AND du.dataset_id = ?
		ORDER BY q.queue_id ASC
	`, datasetID)
	if err != nil {
		return fmt.Errorf("query proceed-format: %w", err)
	}
	defer rows.Close()

	type formatJob struct {
		QueueID    int64
		FolderPath string
		URL        string
	}

	var jobs []formatJob
	for rows.Next() {
		var j formatJob
		if err := rows.Scan(&j.QueueID, &j.FolderPath, &j.URL); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan proceed-format rows: %w", err)
	}

	if len(jobs) == 0 {
		return nil
	}

	log.Printf("[format] dataset_id=%d — processing %d proceed-format jobs", datasetID, len(jobs))

	for _, job := range jobs {
		cleanedPath := filepath.Join(job.FolderPath, "cleaned.json")

		cleaned, err := loadCleanedJSON(cleanedPath)
		if err != nil {
			log.Printf("[format] failed queue_id=%d path=%s: %v", job.QueueID, cleanedPath, err)
			markFailed(database, job.QueueID)
			continue
		}

		payload := FilterBySchema(cleaned, schema)

		if raw, ok := payload["content"].(string); ok && raw != "" {
			payload["content"] = compressContent(raw, includeLinks)
		}

		outPath := filepath.Join(job.FolderPath, "format.json")
		if err := writeJSON(outPath, payload); err != nil {
			log.Printf("[format] write failed queue_id=%d: %v", job.QueueID, err)
			markFailed(database, job.QueueID)
			continue
		}

		markProceedExtract(database, job.QueueID)
		log.Printf("[format] done queue_id=%d → %s", job.QueueID, outPath)
	}

	return nil
}

// ── Amazon format ─────────────────────────────────────────────────────────────

func formatAmazon(datasetID int64, database *sql.DB) error {
	var fieldsJSON string
	err := database.QueryRow(`
		SELECT fields FROM dataset_schema WHERE dataset_id = ?
	`, datasetID).Scan(&fieldsJSON)
	if err != nil {
		return fmt.Errorf("load amazon schema: %w", err)
	}

	var schemaFields map[string]map[string]string
	if err := json.Unmarshal([]byte(fieldsJSON), &schemaFields); err != nil {
		return fmt.Errorf("parse amazon schema: %w", err)
	}

	selectedFields := make(map[string]bool, len(schemaFields))
	for key := range schemaFields {
		selectedFields[key] = true
	}

	rows, err := database.Query(`
		SELECT q.queue_id, du.folder_path, du.url
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-format'
		AND du.folder_path IS NOT NULL
		AND du.dataset_id = ?
		ORDER BY q.queue_id ASC
	`, datasetID)
	if err != nil {
		return fmt.Errorf("query amazon proceed-format: %w", err)
	}
	defer rows.Close()

	type amazonJob struct {
		QueueID    int64
		FolderPath string
		URL        string
	}

	var jobs []amazonJob
	for rows.Next() {
		var j amazonJob
		if err := rows.Scan(&j.QueueID, &j.FolderPath, &j.URL); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan amazon proceed-format rows: %w", err)
	}

	if len(jobs) == 0 {
		return nil
	}

	log.Printf("[format/amazon] dataset_id=%d — processing %d jobs", datasetID, len(jobs))

	for _, job := range jobs {
		rawPath := filepath.Join(job.FolderPath, "raw.json")
		rawBytes, err := storage.Read(rawPath)
		if err != nil {
			log.Printf("[format/amazon] read raw.json failed queue_id=%d: %v", job.QueueID, err)
			markFailed(database, job.QueueID)
			continue
		}

		var scraped struct {
			Raw string `json:"raw"`
			URL string `json:"url"`
		}
		if err := json.Unmarshal(rawBytes, &scraped); err != nil {
			log.Printf("[format/amazon] parse raw.json failed queue_id=%d: %v", job.QueueID, err)
			markFailed(database, job.QueueID)
			continue
		}

		var apiResponse map[string]interface{}
		if err := json.Unmarshal([]byte(scraped.Raw), &apiResponse); err != nil {
			log.Printf("[format/amazon] parse api response failed queue_id=%d: %v", job.QueueID, err)
			markFailed(database, job.QueueID)
			continue
		}

		entity := make(map[string]interface{}, len(selectedFields)+1)
		for field := range selectedFields {
			if val, ok := apiResponse[field]; ok {
				entity[field] = val
			}
		}
		entity["_source"] = job.URL

		extractPayload := map[string]interface{}{
			"entities":   []map[string]interface{}{entity},
			"source_url": job.URL,
		}

		extractPath := filepath.Join(job.FolderPath, "extract.json")
		if err := writeJSON(extractPath, extractPayload); err != nil {
			log.Printf("[format/amazon] write extract.json failed queue_id=%d: %v", job.QueueID, err)
			markFailed(database, job.QueueID)
			continue
		}

		markProceedVersion(database, job.QueueID)
		log.Printf("[format/amazon] done queue_id=%d → %s", job.QueueID, extractPath)
	}

	return nil
}