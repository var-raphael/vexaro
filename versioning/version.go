package versioning

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/var-raphael/vexaro-engine/db"
	"github.com/var-raphael/vexaro-engine/notification"
	"github.com/var-raphael/vexaro-engine/storage"
)

type EntityMap = map[string]interface{}

func SaveVersion(datasetID int64, datasetFolder string, entities []EntityMap, sources []map[string]interface{}) error {
	if len(entities) == 0 {
		log.Printf("[versioning] no entities — skipping version save for %s", datasetFolder)
		return nil
	}

	dir := filepath.Join("data", datasetFolder)

	lastVersion, lastEntities, err := loadLastVersion(datasetID, dir)
	if err != nil {
		return fmt.Errorf("load last version: %w", err)
	}

	if lastVersion == 0 {
		return writeVersion(datasetID, datasetFolder, dir, 1, entities, sources)
	}

	if !hasChanged(lastEntities, entities) {
		log.Printf("[versioning] no change detected for %s — skipping", datasetFolder)
		return nil
	}

	nextVersion := lastVersion + 1
	log.Printf("[versioning] change detected for %s — saving v%d", datasetFolder, nextVersion)
	return writeVersion(datasetID, datasetFolder, dir, nextVersion, entities, sources)
}

func Rollback(datasetID int64, targetVersion int) error {
	database := db.Get()

	var count int
	err := database.QueryRow(`
		SELECT COUNT(*) FROM dataset_versions
		WHERE dataset_id = ? AND version_number = ?
	`, datasetID, targetVersion).Scan(&count)
	if err != nil {
		return fmt.Errorf("verify version: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("version %d not found for dataset_id=%d", targetVersion, datasetID)
	}

	_, err = database.Exec(`
		UPDATE datasets SET active_version = ? WHERE dataset_id = ?
	`, targetVersion, datasetID)
	if err != nil {
		return fmt.Errorf("update active_version: %w", err)
	}

	log.Printf("[versioning] dataset_id=%d rolled back to v%d", datasetID, targetVersion)
	return nil
}

func ActiveVersionPath(datasetID int64, datasetFolder string) (string, error) {
	database := db.Get()

	var filePath string
	err := database.QueryRow(`
		SELECT dv.file_path
		FROM dataset_versions dv
		JOIN datasets d ON d.dataset_id = dv.dataset_id
		WHERE dv.dataset_id = ?
		AND dv.version_number = d.active_version
	`, datasetID).Scan(&filePath)
	if err != nil {
		return "", fmt.Errorf("query active version path: %w", err)
	}

	return filePath, nil
}

// ---------------------------------------------------------------- internal --

func writeVersion(datasetID int64, datasetFolder, dir string, version int, entities []EntityMap, sources []map[string]interface{}) error {
	fileName := fmt.Sprintf("result-v%d.json", version)
	filePath := filepath.Join(dir, fileName)

	result := map[string]interface{}{
		"version":    version,
		"created_at": time.Now().UTC().Format(time.RFC3339),
		"entities":   entities,
	}

	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if err := storage.Write(filePath, b); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	log.Printf("[versioning] wrote %s (%d entities)", filePath, len(entities))

	if err := persistToDB(datasetID, version, filePath, len(entities)); err != nil {
		return fmt.Errorf("persist to db: %w", err)
	}

	go func() {
		var userID, dataName, alias string
		db.Get().QueryRow(`
			SELECT user_id, data_name, COALESCE(alias, data_name) FROM datasets WHERE dataset_id = ?
		`, datasetID).Scan(&userID, &dataName, &alias)

		if userID != "" {
			notification.Notify(
				userID,
				&datasetID,
				"new_version",
				fmt.Sprintf("Dataset \"%s\" has a new version ready — v%d.", alias, version),
			)
		}

		fireWebhook(datasetID, alias, version, len(entities))
	}()

	return nil
}

func fireWebhook(datasetID int64, name string, version, entityCount int) {
	var webhookURL, secret string
	err := db.Get().QueryRow(`
		SELECT url, COALESCE(secret, '') FROM dataset_webhooks
		WHERE dataset_id = ? AND is_active = 1
	`, datasetID).Scan(&webhookURL, &secret)
	if err != nil {
		return
	}

	payload := map[string]interface{}{
		"dataset_id":   datasetID,
		"name":         name,
		"version":      version,
		"entity_count": entityCount,
		"fired_at":     time.Now().UTC().Format(time.RFC3339),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[webhook] marshal error dataset_id=%d: %v", datasetID, err)
		return
	}

	var lastStatus int
	var attempt int

	for attempt = 1; attempt <= 3; attempt++ {
		req, err := http.NewRequest(http.MethodPost, webhookURL, bytes.NewReader(b))
		if err != nil {
			log.Printf("[webhook] build request error dataset_id=%d: %v", datasetID, err)
			break
		}
		req.Header.Set("Content-Type", "application/json")
		if secret != "" {
			req.Header.Set("X-Vexaro-Secret", secret)
		}

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[webhook] attempt %d failed dataset_id=%d: %v", attempt, datasetID, err)
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
			continue
		}
		resp.Body.Close()
		lastStatus = resp.StatusCode

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Printf("[webhook] fired dataset_id=%d version=v%d status=%d", datasetID, version, resp.StatusCode)
			break
		}

		log.Printf("[webhook] attempt %d bad status dataset_id=%d status=%d", attempt, datasetID, resp.StatusCode)
		time.Sleep(time.Duration(attempt) * 2 * time.Second)
	}

	db.Get().Exec(`
		UPDATE dataset_webhooks
		SET last_fired_at = NOW(), last_status = ?
		WHERE dataset_id = ?
	`, lastStatus, datasetID)
}

func persistToDB(datasetID int64, version int, filePath string, entityCount int) error {
	database := db.Get()

	_, err := database.Exec(`
		UPDATE dataset_versions SET is_active = FALSE
		WHERE dataset_id = ?
	`, datasetID)
	if err != nil {
		return fmt.Errorf("deactivate old versions: %w", err)
	}

	var existingCount int
	err = database.QueryRow(`
		SELECT COUNT(*) FROM dataset_versions
		WHERE dataset_id = ? AND version_number = ?
	`, datasetID, version).Scan(&existingCount)
	if err != nil {
		return fmt.Errorf("check existing version: %w", err)
	}

	if existingCount > 0 {
		_, err = database.Exec(`
			UPDATE dataset_versions
			SET file_path = ?, is_active = TRUE, entity_count = ?
			WHERE dataset_id = ? AND version_number = ?
		`, filePath, entityCount, datasetID, version)
		if err != nil {
			return fmt.Errorf("update version row: %w", err)
		}
	} else {
		_, err = database.Exec(`
			INSERT INTO dataset_versions (dataset_id, version_number, file_path, is_active, entity_count)
			VALUES (?, ?, ?, TRUE, ?)
		`, datasetID, version, filePath, entityCount)
		if err != nil {
			return fmt.Errorf("insert version row: %w", err)
		}
	}

	_, err = database.Exec(`
		UPDATE datasets SET active_version = ? WHERE dataset_id = ?
	`, version, datasetID)
	if err != nil {
		return fmt.Errorf("update active_version: %w", err)
	}

	log.Printf("[versioning] db updated — dataset_id=%d active_version=v%d entity_count=%d", datasetID, version, entityCount)
	return nil
}

func loadLastVersion(datasetID int64, dir string) (int, []EntityMap, error) {
	database := db.Get()

	rows, err := database.Query(`
		SELECT version_number, file_path FROM dataset_versions
		WHERE dataset_id = ?
		ORDER BY version_number DESC
	`, datasetID)
	if err != nil {
		return 0, nil, fmt.Errorf("query versions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var version int
		var filePath string
		if err := rows.Scan(&version, &filePath); err != nil {
			continue
		}

		b, err := storage.Read(filePath)
		if err != nil {
			log.Printf("[versioning] could not read %s — skipping: %v", filePath, err)
			continue
		}

		var result map[string]interface{}
		if err := json.Unmarshal(b, &result); err != nil {
			log.Printf("[versioning] could not parse %s — deleting: %v", filePath, err)
			deleteVersion(datasetID, version, filePath)
			continue
		}

		entities, err := extractEntities(result)
		if err != nil || len(entities) == 0 {
			log.Printf("[versioning] empty or invalid entities in %s — deleting", filePath)
			deleteVersion(datasetID, version, filePath)
			continue
		}

		return version, entities, nil
	}

	return 0, nil, nil
}

func deleteVersion(datasetID int64, version int, filePath string) {
	if err := storage.Delete(filePath); err != nil {
		log.Printf("[versioning] failed to delete %s: %v", filePath, err)
	} else {
		log.Printf("[versioning] deleted bad version file %s", filePath)
	}

	database := db.Get()

	_, err := database.Exec(`
		DELETE FROM dataset_versions WHERE dataset_id = ? AND version_number = ?
	`, datasetID, version)
	if err != nil {
		log.Printf("[versioning] failed to delete db row for v%d: %v", version, err)
	}

	var lastGood int
	err = database.QueryRow(`
		SELECT COALESCE(MAX(version_number), 0) FROM dataset_versions WHERE dataset_id = ?
	`, datasetID).Scan(&lastGood)
	if err != nil {
		log.Printf("[versioning] failed to resolve last good version: %v", err)
		return
	}

	_, err = database.Exec(`
		UPDATE datasets SET active_version = ? WHERE dataset_id = ?
	`, lastGood, datasetID)
	if err != nil {
		log.Printf("[versioning] failed to update active_version after cleanup: %v", err)
	} else {
		log.Printf("[versioning] active_version reset to v%d for dataset_id=%d", lastGood, datasetID)
	}
}

func parseVersionNumber(name string) int {
	if !strings.HasPrefix(name, "result-v") || !strings.HasSuffix(name, ".json") {
		return 0
	}
	mid := strings.TrimPrefix(name, "result-v")
	mid = strings.TrimSuffix(mid, ".json")
	n, err := strconv.Atoi(mid)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

func extractEntities(result map[string]interface{}) ([]EntityMap, error) {
	raw, ok := result["entities"]
	if !ok {
		return []EntityMap{}, nil
	}
	if raw == nil {
		return []EntityMap{}, nil
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return []EntityMap{}, nil
	}
	entities := make([]EntityMap, 0, len(arr))
	for _, item := range arr {
		e, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		entities = append(entities, e)
	}
	return entities, nil
}

func hasChanged(old, new []EntityMap) bool {
	if len(old) != len(new) {
		log.Printf("[versioning] entity count changed: %d → %d", len(old), len(new))
		return true
	}

	for i, newEntity := range new {
		oldEntity := old[i]
		for field, newVal := range newEntity {
			oldVal, exists := oldEntity[field]
			if !exists || !valuesEqual(oldVal, newVal) {
				log.Printf("[versioning] field changed: entity[%d].%s", i, field)
				return true
			}
		}
		for field := range oldEntity {
			if _, exists := newEntity[field]; !exists {
				log.Printf("[versioning] field removed: entity[%d].%s", i, field)
				return true
			}
		}
	}

	return false
}

func valuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	aB, _ := json.Marshal(a)
	bB, _ := json.Marshal(b)
	return string(aB) == string(bB)
}