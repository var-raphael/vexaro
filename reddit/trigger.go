package reddit

import (
	"fmt"
	"log"
	"sync"

	"github.com/var-raphael/vexaro-engine/db"
)

var (
	triggerMu      sync.Mutex
	activeDatasets = make(map[int64]bool)
)

func RunCombineAndVersion(datasetID int64) error {
	triggerMu.Lock()
	if activeDatasets[datasetID] {
		triggerMu.Unlock()
		log.Printf("[reddit/trigger] already running for dataset_id=%d — skipping", datasetID)
		return nil
	}
	activeDatasets[datasetID] = true
	triggerMu.Unlock()

	defer func() {
		triggerMu.Lock()
		delete(activeDatasets, datasetID)
		triggerMu.Unlock()
	}()

	log.Printf("[reddit/trigger] running combine — dataset_id=%d", datasetID)
	if err := RunCombine(datasetID); err != nil {
		return fmt.Errorf("combine: %w", err)
	}

	log.Printf("[reddit/trigger] running version — dataset_id=%d", datasetID)
	if err := RunVersion(datasetID); err != nil {
		return fmt.Errorf("version: %w", err)
	}

	log.Printf("[reddit/trigger] done — dataset_id=%d", datasetID)
	return nil
}

func loadExistingURLs(datasetID int64) (map[string]bool, error) {
	rows, err := db.Get().Query(`
		SELECT url FROM datasets_url
		WHERE dataset_id = ? AND source_type = 'reddit'
	`, datasetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	existing := make(map[string]bool)
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			continue
		}
		existing[u] = true
	}
	return existing, rows.Err()
}