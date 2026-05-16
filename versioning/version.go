package versioning

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/var-raphael/vexaro-engine/db"
)

type EntityMap = map[string]interface{}

// SaveVersion checks if entities have changed since the last version.
// If changed — writes result-vN.json, inserts a dataset_versions row,
// updates datasets.active_version.
// If nothing changed — no file written, no DB row inserted.
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

// Rollback updates active_version pointer in DB to targetVersion.
// Does not touch files, does not interrupt the pipeline.
func Rollback(datasetID int64, targetVersion int) error {
	database := db.Get()

	// Verify the version exists in DB
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

// ActiveVersionPath returns the file path of the currently active version for a dataset.
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
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	fileName := fmt.Sprintf("result-v%d.json", version)
	filePath := filepath.Join(dir, fileName)

	result := map[string]interface{}{
		"version":    version,
		"created_at": time.Now().UTC().Format(time.RFC3339),
		"entities":   entities,
		"sources":    sources,
	}

	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := os.WriteFile(filePath, b, 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	log.Printf("[versioning] wrote %s (%d entities, %d sources)", filePath, len(entities), len(sources))

	if err := persistToDB(datasetID, version, filePath); err != nil {
		return fmt.Errorf("persist to db: %w", err)
	}

	return nil
}

func persistToDB(datasetID int64, version int, filePath string) error {
	database := db.Get()

	// Mark all previous versions inactive
	_, err := database.Exec(`
		UPDATE dataset_versions SET is_active = FALSE
		WHERE dataset_id = ?
	`, datasetID)
	if err != nil {
		return fmt.Errorf("deactivate old versions: %w", err)
	}

	// Insert new version as active
	_, err = database.Exec(`
		INSERT INTO dataset_versions (dataset_id, version_number, file_path, is_active)
		VALUES (?, ?, ?, TRUE)
	`, datasetID, version, filePath)
	if err != nil {
		return fmt.Errorf("insert version row: %w", err)
	}

	// Update active_version pointer on datasets table
	_, err = database.Exec(`
		UPDATE datasets SET active_version = ? WHERE dataset_id = ?
	`, version, datasetID)
	if err != nil {
		return fmt.Errorf("update active_version: %w", err)
	}

	log.Printf("[versioning] db updated — dataset_id=%d active_version=v%d", datasetID, version)
	return nil
}

// loadLastVersion scans the dataset dir for result-vN.json files,
// cleans up any empty versions, and returns the highest valid version
// with its entities. Returns version=0 if none found.
func loadLastVersion(datasetID int64, dir string) (int, []EntityMap, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil, nil
		}
		return 0, nil, fmt.Errorf("read dir: %w", err)
	}

	var versions []int
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := parseVersionNumber(e.Name())
		if n > 0 {
			versions = append(versions, n)
		}
	}

	if len(versions) == 0 {
		return 0, nil, nil
	}

	sort.Sort(sort.Reverse(sort.IntSlice(versions)))

	for _, v := range versions {
		filePath := filepath.Join(dir, fmt.Sprintf("result-v%d.json", v))
		b, err := os.ReadFile(filePath)
		if err != nil {
			log.Printf("[versioning] could not read %s — skipping: %v", filePath, err)
			continue
		}

		var result map[string]interface{}
		if err := json.Unmarshal(b, &result); err != nil {
			log.Printf("[versioning] could not parse %s — deleting: %v", filePath, err)
			deleteVersion(datasetID, v, filePath)
			continue
		}

		entities, err := extractEntities(result)
		if err != nil || len(entities) == 0 {
			log.Printf("[versioning] empty or invalid entities in %s — deleting", filePath)
			deleteVersion(datasetID, v, filePath)
			continue
		}

		return v, entities, nil
	}

	return 0, nil, nil
}

// deleteVersion removes the file and cleans up the DB row, then repoints
// active_version to the highest remaining valid version.
func deleteVersion(datasetID int64, version int, filePath string) {
	if err := os.Remove(filePath); err != nil {
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

	// Repoint active_version to highest remaining valid version
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

// parseVersionNumber extracts N from "result-vN.json", returns 0 if no match.
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

// hasChanged returns true if entity count differs or any field value differs.
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