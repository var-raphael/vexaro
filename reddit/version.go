package reddit

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/var-raphael/vexaro-engine/db"
)

type DatasetOutput struct {
	DatasetID   int64          `json:"dataset_id"`
	DataName    string         `json:"data_name"`
	Total       int            `json:"total"`
	CreatedAt   time.Time      `json:"created_at"`
	GeneratedAt time.Time      `json:"generated_at"`
	Posts       []FilteredPost `json:"posts"`
}

func RunVersion(datasetID int64) error {
	meta, err := fetchDatasetMeta(datasetID)
	if err != nil {
		return fmt.Errorf("fetch dataset meta: %w", err)
	}

	allPaths, err := loadAllFolderPaths(datasetID)
	if err != nil {
		return fmt.Errorf("load all folder paths: %w", err)
	}
	if len(allPaths) == 0 {
		log.Printf("[reddit/version] no folder paths for dataset_id=%d", datasetID)
		return nil
	}

	var posts []FilteredPost
	failed := 0

	for _, folderPath := range allPaths {
		filteredPath := folderPath + "/filtered.json"
		data, err := os.ReadFile(filteredPath)
		if err != nil {
			rawPath := folderPath + "/raw.json"
			data, err = os.ReadFile(rawPath)
			if err != nil {
				log.Printf("[reddit/version] cannot read %s or raw.json: skipping", filteredPath)
				failed++
				continue
			}
			post, err := ParseRaw(data, DefaultSchema())
			if err != nil {
				log.Printf("[reddit/version] parse raw failed for %s: %v", folderPath, err)
				failed++
				continue
			}
			posts = append(posts, *post)
			continue
		}

		var post FilteredPost
		if err := json.Unmarshal(data, &post); err != nil {
			log.Printf("[reddit/version] unmarshal filtered.json failed for %s: %v", folderPath, err)
			failed++
			continue
		}
		posts = append(posts, post)
	}

	if len(posts) == 0 {
		log.Printf("[reddit/version] no posts to version for dataset_id=%d", datasetID)
		return nil
	}

	// ── Dedup check: skip if content hasn't changed ───────────────────────────
	var lastFilePath string
	db.Get().QueryRow(`
		SELECT file_path FROM dataset_versions
		WHERE dataset_id = ? ORDER BY version_number DESC LIMIT 1
	`, datasetID).Scan(&lastFilePath)

	if lastFilePath != "" {
		if existing, err := os.ReadFile(lastFilePath); err == nil {
			var lastOutput DatasetOutput
			if json.Unmarshal(existing, &lastOutput) == nil {
				if lastOutput.Total == len(posts) && hashPosts(lastOutput.Posts) == hashPosts(posts) {
					log.Printf("[reddit/version] no change — skipping dataset_id=%d (%d posts)", datasetID, len(posts))
					if err := markAllProceedVersionDone(datasetID); err != nil {
						log.Printf("[reddit/version] mark done failed: %v", err)
					}
					return nil
				}
			}
		}
	}

	nextVersion, err := getNextVersionNumber(datasetID)
	if err != nil {
		return fmt.Errorf("get next version: %w", err)
	}

	output := DatasetOutput{
		DatasetID:   datasetID,
		DataName:    meta.dataName,
		Total:       len(posts),
		CreatedAt:   meta.createdAt,
		GeneratedAt: time.Now(),
		Posts:       posts,
	}

	filePath, err := writeVersionedOutput(output, meta.folderBase, nextVersion)
	if err != nil {
		return fmt.Errorf("write versioned output: %w", err)
	}

	if err := insertVersion(datasetID, nextVersion, filePath, len(posts)); err != nil {
		return fmt.Errorf("insert version: %w", err)
	}

	if err := markAllProceedVersionDone(datasetID); err != nil {
		log.Printf("[reddit/version] mark proceed-version done failed: %v", err)
	}

	log.Printf("[reddit/version] done — dataset_id=%d v%d posts=%d failed=%d",
		datasetID, nextVersion, len(posts), failed)
	return nil
}

// hashPosts returns a sha256 hash of the post URLs in order, fast and stable.
func hashPosts(posts []FilteredPost) string {
	h := sha256.New()
	for _, p := range posts {
		h.Write([]byte(p.URL))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// ── DB helpers ────────────────────────────────────────────────────────────────

type datasetMeta struct {
	dataName   string
	createdAt  time.Time
	folderBase string
}

func fetchDatasetMeta(datasetID int64) (*datasetMeta, error) {
	var m datasetMeta
	var intent sql.NullString
	err := db.Get().QueryRow(`
		SELECT data_name, COALESCE(intent, ''), created_at
		FROM datasets WHERE dataset_id = ?
	`, datasetID).Scan(&m.dataName, &intent, &m.createdAt)
	if err != nil {
		return nil, fmt.Errorf("query dataset meta: %w", err)
	}

	var samplePath string
	err = db.Get().QueryRow(`
		SELECT folder_path FROM datasets_url
		WHERE dataset_id = ?
		  AND source_type = 'reddit'
		  AND folder_path IS NOT NULL
		LIMIT 1
	`, datasetID).Scan(&samplePath)
	if err != nil {
		return nil, fmt.Errorf("query sample folder_path: %w", err)
	}

	m.folderBase = extractBase(samplePath)
	return &m, nil
}

func loadAllFolderPaths(datasetID int64) ([]string, error) {
	rows, err := db.Get().Query(`
		SELECT DISTINCT du.folder_path
		FROM datasets_url du
		WHERE du.dataset_id = ?
		  AND du.source_type = 'reddit'
		  AND du.folder_path IS NOT NULL
	`, datasetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			continue
		}
		paths = append(paths, p)
	}
	return paths, rows.Err()
}

func getNextVersionNumber(datasetID int64) (int, error) {
	var maxVersion sql.NullInt64
	err := db.Get().QueryRow(`
		SELECT MAX(version_number) FROM dataset_versions WHERE dataset_id = ?
	`, datasetID).Scan(&maxVersion)
	if err != nil {
		return 0, err
	}
	if !maxVersion.Valid {
		return 1, nil
	}
	return int(maxVersion.Int64) + 1, nil
}

func insertVersion(datasetID int64, versionNumber int, filePath string, entityCount int) error {
	tx, err := db.Get().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO dataset_versions (dataset_id, version_number, file_path, is_active, entity_count)
		VALUES (?, ?, ?, 1, ?)
	`, datasetID, versionNumber, filePath, entityCount)
	if err != nil {
		return fmt.Errorf("insert dataset_versions: %w", err)
	}

	_, err = tx.Exec(`
		UPDATE dataset_versions
		SET is_active = 0
		WHERE dataset_id = ? AND version_number != ?
	`, datasetID, versionNumber)
	if err != nil {
		return fmt.Errorf("deactivate old versions: %w", err)
	}

	_, err = tx.Exec(`
		UPDATE datasets SET active_version = ? WHERE dataset_id = ?
	`, versionNumber, datasetID)
	if err != nil {
		return fmt.Errorf("update active_version: %w", err)
	}

	return tx.Commit()
}

func markAllProceedVersionDone(datasetID int64) error {
	_, err := db.Get().Exec(`
		UPDATE reddit_queue rq
		JOIN datasets_url du ON du.dataset_url_id = rq.dataset_url_id
		SET rq.status = 'done', rq.locked_at = NULL
		WHERE du.dataset_id = ?
		  AND rq.status = 'proceed-combine'
	`, datasetID)
	return err
}

func writeVersionedOutput(output DatasetOutput, folderBase string, version int) (string, error) {
	if err := os.MkdirAll(folderBase, 0755); err != nil {
		return "", fmt.Errorf("mkdir: %w", err)
	}

	b, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	filePath := fmt.Sprintf("%s/dataset-v%d.json", folderBase, version)
	if err := os.WriteFile(filePath, b, 0644); err != nil {
		return "", fmt.Errorf("write: %w", err)
	}

	log.Printf("[reddit/version] wrote → %s (%d posts)", filePath, output.Total)
	return filePath, nil
}

func extractBase(path string) string {
	count := 0
	for i, c := range path {
		if c == '/' {
			count++
			if count == 2 {
				return path[:i]
			}
		}
	}
	return path
}