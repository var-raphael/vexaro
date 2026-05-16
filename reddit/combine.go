package reddit

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/var-raphael/vexaro-engine/db"
)

type combineRow struct {
	RedditQueueID int64
	DatasetURLID  int64
	FolderPath    string
}

func RunCombine(datasetID int64) error {
	rows, err := fetchPendingCombine(datasetID)
	if err != nil {
		return fmt.Errorf("fetch combine rows: %w", err)
	}
	if len(rows) == 0 {
		log.Printf("[reddit/combine] no rows to process for dataset_id=%d", datasetID)
		return nil
	}

	schema := DefaultSchema()
	failed := 0

	for _, row := range rows {
		if err := processRow(row, schema); err != nil {
			log.Printf("[reddit/combine] failed row %d: %v", row.RedditQueueID, err)
			markFailed(row.RedditQueueID)
			failed++
			continue
		}
		markProceedVersion(row.RedditQueueID)
	}

	log.Printf("[reddit/combine] done — %d processed, %d failed", len(rows)-failed, failed)
	return nil
}

func fetchPendingCombine(datasetID int64) ([]combineRow, error) {
	database := db.Get()
	q := `
		SELECT rq.reddit_queue_id, rq.dataset_url_id, du.folder_path
		FROM reddit_queue rq
		JOIN datasets_url du ON du.dataset_url_id = rq.dataset_url_id
		WHERE rq.status = 'proceed-combine'
		  AND du.dataset_id = ?
		  AND du.folder_path IS NOT NULL
	`
	sqlRows, err := database.Query(q, datasetID)
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	var out []combineRow
	for sqlRows.Next() {
		var r combineRow
		if err := sqlRows.Scan(&r.RedditQueueID, &r.DatasetURLID, &r.FolderPath); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, sqlRows.Err()
}

func processRow(row combineRow, schema RedditSchema) error {
	rawPath := row.FolderPath + "/raw.json"
	data, err := os.ReadFile(rawPath)
	if err != nil {
		return fmt.Errorf("read raw.json: %w", err)
	}

	filtered, err := ParseRaw(data, schema)
	if err != nil {
		return fmt.Errorf("parse raw: %w", err)
	}

	b, err := json.MarshalIndent(filtered, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal filtered: %w", err)
	}

	filteredPath := row.FolderPath + "/filtered.json"
	if err := os.WriteFile(filteredPath, b, 0644); err != nil {
		return fmt.Errorf("write filtered.json: %w", err)
	}

	log.Printf("[reddit/combine] wrote → %s", filteredPath)
	return nil
}

func markProceedVersion(id int64) {
	_, err := db.Get().Exec(`
		UPDATE reddit_queue SET status = 'proceed-version', locked_at = NULL
		WHERE reddit_queue_id = ?
	`, id)
	if err != nil {
		log.Printf("[reddit/combine] mark proceed-version failed for %d: %v", id, err)
	}
}

func markFailed(id int64) {
	_, err := db.Get().Exec(`
		UPDATE reddit_queue SET status = 'failed', locked_at = NULL
		WHERE reddit_queue_id = ?
	`, id)
	if err != nil {
		log.Printf("[reddit/combine] mark failed for %d: %v", id, err)
	}
}