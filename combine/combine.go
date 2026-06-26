package combine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/var-raphael/vexaro-engine/db"
	"github.com/var-raphael/vexaro-engine/versioning"
	"github.com/var-raphael/vexaro-engine/storage"
)

func Combine() error {
	database := db.Get()

	rows, err := database.Query(`
		SELECT q.queue_id, du.folder_path, du.url, d.data_name, d.dataset_id
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE q.status = 'proceed-version'
		AND du.folder_path IS NOT NULL
		ORDER BY d.data_name ASC, q.queue_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query proceed-version: %w", err)
	}

	type combineJob struct {
		QueueID    int64
		FolderPath string
		SourceURL  string
		DataName   string
		DatasetID  int64
	}

	datasetJobs := map[string][]combineJob{}
	var datasetOrder []string
	seenDataset := map[string]bool{}

	for rows.Next() {
		var j combineJob
		var folderPath sql.NullString
		if err := rows.Scan(&j.QueueID, &folderPath, &j.SourceURL, &j.DataName, &j.DatasetID); err != nil {
			log.Printf("[combine] scan error: %v", err)
			continue
		}
		if !folderPath.Valid || folderPath.String == "" {
			log.Printf("[combine] skip queue_id=%d — empty folder_path", j.QueueID)
			continue
		}
		j.FolderPath = folderPath.String

		key := fmt.Sprintf("%s-%d", j.DataName, j.DatasetID)
		if !seenDataset[key] {
			datasetOrder = append(datasetOrder, key)
			seenDataset[key] = true
		}
		datasetJobs[key] = append(datasetJobs[key], j)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan proceed-version rows: %w", err)
	}
	rows.Close()

	if len(datasetOrder) == 0 {
		log.Printf("[combine] no proceed-version urls in queue")
		return nil
	}

	log.Printf("[combine] processing %d dataset(s)", len(datasetOrder))

	for _, key := range datasetOrder {
		jobs := datasetJobs[key]
		var datasetID int64
		if len(jobs) > 0 {
			datasetID = jobs[0].DatasetID
		}

		var inFlightCount int
		err := database.QueryRow(`
			SELECT COUNT(*) FROM queue q
			JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
			WHERE du.dataset_id = ?
			AND q.status NOT IN ('done', 'failed', 'proceed-version')
		`, datasetID).Scan(&inFlightCount)
		if err != nil {
			log.Printf("[combine] in-flight check error dataset=%s: %v", key, err)
			continue
		}
		if inFlightCount > 0 {
			log.Printf("[combine] dataset=%s — %d urls still in-flight, deferring version save", key, inFlightCount)
			continue
		}

		log.Printf("[combine] dataset=%s — %d urls to combine", key, len(jobs))

		var entities []map[string]interface{}
		var sources []map[string]interface{}
		var doneQueueIDs []int64
		var failedQueueIDs []int64

		for _, job := range jobs {
			extractPath := filepath.Join(job.FolderPath, "extract.json")

			data, err := loadJSON(extractPath)
			if err != nil {
				log.Printf("[combine] missing or unreadable extract.json queue_id=%d path=%s: %v — marking failed", job.QueueID, extractPath, err)
				failedQueueIDs = append(failedQueueIDs, job.QueueID)
				continue
			}

			sourceURL, _ := data["source_url"].(string)
			if sourceURL == "" {
				sourceURL = job.SourceURL
			}

			extractEntities, ok := data["entities"].([]interface{})
			if !ok || len(extractEntities) == 0 {
				log.Printf("[combine] queue_id=%d has no entities in extract.json — marking failed", job.QueueID)
				failedQueueIDs = append(failedQueueIDs, job.QueueID)
				continue
			}

			entityCount := 0
			for _, e := range extractEntities {
				if entity, ok := e.(map[string]interface{}); ok {
					entity["_source"] = sourceURL
					entities = append(entities, entity)
					entityCount++
				}
			}
			log.Printf("[combine] queue_id=%d — loaded %d entities from %s", job.QueueID, entityCount, extractPath)

			if sourceURL != "" {
				sources = append(sources, map[string]interface{}{"url": sourceURL})
			}

			doneQueueIDs = append(doneQueueIDs, job.QueueID)
		}

		if len(entities) == 0 {
			log.Printf("[combine] dataset=%s — no entities collected, skipping version save", key)
			for _, id := range failedQueueIDs {
				markFailed(database, id)
			}
			continue
		}

		log.Printf("[combine] dataset=%s — total %d entities from %d sources, saving version...", key, len(entities), len(sources))

		if err := versioning.SaveVersion(datasetID, key, entities, sources); err != nil {
			log.Printf("[combine] versioning failed for %s: %v", key, err)
			for _, id := range doneQueueIDs {
				markFailed(database, id)
			}
			for _, id := range failedQueueIDs {
				markFailed(database, id)
			}
			continue
		}

		log.Printf("[combine] dataset=%s — version saved successfully", key)

		for _, id := range doneQueueIDs {
			markDone(database, id)
		}
		for _, id := range failedQueueIDs {
			markFailed(database, id)
		}
	}

	log.Printf("[combine] done")
	return nil
}

func ResolveDatasetID(datasetFolder string) (int64, error) {
	parts := strings.Split(datasetFolder, "-")
	if len(parts) < 2 {
		return 0, fmt.Errorf("cannot parse dataset_id from folder: %s", datasetFolder)
	}
	idStr := parts[len(parts)-1]
	var id int64
	_, err := fmt.Sscanf(idStr, "%d", &id)
	if err != nil {
		return 0, fmt.Errorf("parse dataset_id from %q: %w", idStr, err)
	}
	return id, nil
}

func ResolveDatasetFolder(apiName string) (string, error) {
	var datasetID int64
	err := db.Get().QueryRow(`
		SELECT dataset_id FROM datasets
		WHERE data_name = ?
		ORDER BY created_at DESC
		LIMIT 1
	`, apiName).Scan(&datasetID)
	if err != nil {
		return "", fmt.Errorf("dataset not found for api_name=%s: %w", apiName, err)
	}
	return fmt.Sprintf("%s-%d", apiName, datasetID), nil
}

// ---------------------------------------------------------------- db ------

func markDone(database *sql.DB, queueID int64) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'done', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[combine] markDone queue_id=%d: %v", queueID, err)
	}
}

func markFailed(database *sql.DB, queueID int64) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'failed', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[combine] markFailed queue_id=%d: %v", queueID, err)
	}
}

// ---------------------------------------------------------------- helpers --

func loadJSON(path string) (map[string]interface{}, error) {
	b, err := storage.Read(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(b, &data); err != nil {
		return nil, fmt.Errorf("parse json: %w", err)
	}
	return data, nil
}

func writeJSON(path string, v interface{}) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return storage.Write(path, b)
}

func CombineDataset(datasetID int64) error {
	database := db.Get()

	var inFlightCount int
	err := database.QueryRow(`
		SELECT COUNT(*) FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE du.dataset_id = ?
		AND q.status NOT IN ('done', 'failed', 'proceed-version')
	`, datasetID).Scan(&inFlightCount)
	if err != nil {
		return fmt.Errorf("check in-flight: %w", err)
	}
	if inFlightCount > 0 {
		log.Printf("[combine] dataset_id=%d — %d urls still in-flight, deferring version save", datasetID, inFlightCount)
		return nil
	}

	rows, err := database.Query(`
		SELECT q.queue_id, du.folder_path, du.url, d.data_name, d.dataset_id
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE q.status = 'proceed-version'
		AND du.folder_path IS NOT NULL
		AND du.dataset_id = ?
		ORDER BY q.queue_id ASC
	`, datasetID)
	if err != nil {
		return fmt.Errorf("query proceed-version: %w", err)
	}
	defer rows.Close()

	type combineJob struct {
		QueueID    int64
		FolderPath string
		SourceURL  string
		DataName   string
		DatasetID  int64
	}

	var jobs []combineJob
	for rows.Next() {
		var j combineJob
		var folderPath sql.NullString
		if err := rows.Scan(&j.QueueID, &folderPath, &j.SourceURL, &j.DataName, &j.DatasetID); err != nil {
			log.Printf("[combine] scan error: %v", err)
			continue
		}
		if !folderPath.Valid || folderPath.String == "" {
			continue
		}
		j.FolderPath = folderPath.String
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan proceed-version rows: %w", err)
	}
	rows.Close()

	if len(jobs) == 0 {
		return nil
	}

	log.Printf("[combine] dataset_id=%d — %d urls to combine", datasetID, len(jobs))

	var entities []map[string]interface{}
	var sources []map[string]interface{}
	var doneQueueIDs []int64
	var failedQueueIDs []int64

	dataName := jobs[0].DataName
	key := fmt.Sprintf("%s-%d", dataName, datasetID)

	for _, job := range jobs {
		extractPath := filepath.Join(job.FolderPath, "extract.json")

		data, err := loadJSON(extractPath)
		if err != nil {
			log.Printf("[combine] missing or unreadable extract.json queue_id=%d path=%s: %v — marking failed", job.QueueID, extractPath, err)
			failedQueueIDs = append(failedQueueIDs, job.QueueID)
			continue
		}

		sourceURL, _ := data["source_url"].(string)
		if sourceURL == "" {
			sourceURL = job.SourceURL
		}

		extractEntities, ok := data["entities"].([]interface{})
		if !ok || len(extractEntities) == 0 {
			log.Printf("[combine] queue_id=%d has no entities — marking failed", job.QueueID)
			failedQueueIDs = append(failedQueueIDs, job.QueueID)
			continue
		}

		for _, e := range extractEntities {
			if entity, ok := e.(map[string]interface{}); ok {
				entity["_source"] = sourceURL
				entities = append(entities, entity)
			}
		}

		if sourceURL != "" {
			sources = append(sources, map[string]interface{}{"url": sourceURL})
		}

		doneQueueIDs = append(doneQueueIDs, job.QueueID)
	}

	if len(entities) == 0 {
		log.Printf("[combine] dataset_id=%d — no entities collected, skipping version save", datasetID)
		for _, id := range failedQueueIDs {
			markFailed(database, id)
		}
		return nil
	}

	if err := versioning.SaveVersion(datasetID, key, entities, sources); err != nil {
		log.Printf("[combine] versioning failed dataset_id=%d: %v", datasetID, err)
		for _, id := range doneQueueIDs {
			markFailed(database, id)
		}
		for _, id := range failedQueueIDs {
			markFailed(database, id)
		}
		return fmt.Errorf("save version: %w", err)
	}

	for _, id := range doneQueueIDs {
		markDone(database, id)
	}
	for _, id := range failedQueueIDs {
		markFailed(database, id)
	}

	log.Printf("[combine] dataset_id=%d — version saved, %d entities", datasetID, len(entities))
	return nil
}