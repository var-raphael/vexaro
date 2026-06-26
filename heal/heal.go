package heal

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/var-raphael/vexaro-engine/db"
	"github.com/var-raphael/vexaro-engine/storage"
)

const (
	healInterval        = 15 * time.Minute
	stuckJobThreshold   = 30 * time.Minute
	pendingJobThreshold = 2 * time.Hour
)

var BillingSweepFn func()

func Start() {
	log.Println("[heal] healer started — interval: 15m")
	time.Sleep(10 * time.Second)
	for {
		run()
		time.Sleep(healInterval)
	}
}

func Run() {
	run()
}

func run() {
	log.Println("[heal] starting heal run")
	start := time.Now()

	fixed := 0
	fixed += healStuckLockedJobs()
	fixed += healStuckPipelineJobs()
	fixed += healRawOnlyFolders()
	fixed += healStuckCrawlingJobs()
	fixed += healOrphanedQueueRows()
	fixed += healOrphanedURLRows()
	fixed += healBrokenActiveVersions()
	fixed += healMissingVersionFiles()
	fixed += healZeroEntityCounts()
	fixed += healAmazonRawReady()

  if BillingSweepFn != nil {
    BillingSweepFn()
}

	log.Printf("[heal] run complete — %d fixes applied in %s", fixed, time.Since(start).Round(time.Millisecond))
}

// ── 1. Unlock jobs stuck with locked_at set but never finished ────────────────

func healStuckLockedJobs() int {
	threshold := time.Now().Add(-stuckJobThreshold)
	res, err := db.Get().Exec(`
		UPDATE queue
		SET locked_at = NULL
		WHERE locked_at IS NOT NULL
		  AND locked_at < ?
		  AND status NOT IN ('done', 'failed')
	`, threshold)
	if err != nil {
		log.Printf("[heal] healStuckLockedJobs error: %v", err)
		return 0
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		log.Printf("[heal] unlocked %d stuck locked jobs", n)
	}
	return int(n)
}

// ── 2. Reset jobs stuck mid-pipeline for too long ─────────────────────────────

func healStuckPipelineJobs() int {
	threshold := time.Now().Add(-pendingJobThreshold)
	res, err := db.Get().Exec(`
		UPDATE queue
		SET status = 'pending',
		    locked_at = NULL
		WHERE status IN ('proceed-format', 'proceed-extract', 'proceed-version')
		  AND created_at < ?
	`, threshold)
	if err != nil {
		log.Printf("[heal] healStuckPipelineJobs error: %v", err)
		return 0
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		log.Printf("[heal] reset %d stuck pipeline jobs to pending", n)
	}
	return int(n)
}

// ── 3. Folders with raw.json but no format/extract → mark failed ──────────────

func healRawOnlyFolders() int {
	rows, err := db.Get().Query(`
		SELECT q.queue_id, du.folder_path
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'done'
		  AND du.folder_path IS NOT NULL
		  AND du.folder_path != ''
	`)
	if err != nil {
		log.Printf("[heal] healRawOnlyFolders query error: %v", err)
		return 0
	}
	defer rows.Close()

	type job struct {
		QueueID    int64
		FolderPath string
	}
	var jobs []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.QueueID, &j.FolderPath); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}

	fixed := 0
	for _, j := range jobs {
		rawPath := filepath.Join(j.FolderPath, "raw.json")
		extractPath := filepath.Join(j.FolderPath, "extract.json")
		formatPath := filepath.Join(j.FolderPath, "format.json")

		rawExists := fileExists(rawPath)
		extractExists := fileExists(extractPath)
		formatExists := fileExists(formatPath)

		if rawExists && !formatExists && !extractExists {
			_, err := db.Get().Exec(`
				UPDATE queue SET status = 'failed', locked_at = NULL WHERE queue_id = ?
			`, j.QueueID)
			if err != nil {
				log.Printf("[heal] healRawOnlyFolders mark failed queue_id=%d: %v", j.QueueID, err)
				continue
			}
			log.Printf("[heal] raw-only folder — marked failed queue_id=%d path=%s", j.QueueID, j.FolderPath)
			fixed++
		}
	}
	return fixed
}

// ── 4. Orphaned queue rows ────────────────────────────────────────────────────

func healOrphanedQueueRows() int {
	res, err := db.Get().Exec(`
		DELETE q FROM queue q
		LEFT JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE du.dataset_url_id IS NULL
	`)
	if err != nil {
		log.Printf("[heal] healOrphanedQueueRows error: %v", err)
		return 0
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		log.Printf("[heal] deleted %d orphaned queue rows", n)
	}
	return int(n)
}

// ── 5. Orphaned datasets_url rows ────────────────────────────────────────────

func healOrphanedURLRows() int {
	res, err := db.Get().Exec(`
		DELETE du FROM datasets_url du
		LEFT JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE d.dataset_id IS NULL
	`)
	if err != nil {
		log.Printf("[heal] healOrphanedURLRows error: %v", err)
		return 0
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		log.Printf("[heal] deleted %d orphaned datasets_url rows", n)
	}
	return int(n)
}

// ── 6. active_version pointing to non-existent version ───────────────────────

func healBrokenActiveVersions() int {
	rows, err := db.Get().Query(`
		SELECT d.dataset_id, d.active_version
		FROM datasets d
		WHERE d.active_version IS NOT NULL
		  AND NOT EXISTS (
			SELECT 1 FROM dataset_versions dv
			WHERE dv.dataset_id = d.dataset_id
			  AND dv.version_number = d.active_version
		  )
	`)
	if err != nil {
		log.Printf("[heal] healBrokenActiveVersions query error: %v", err)
		return 0
	}
	defer rows.Close()

	fixed := 0
	for rows.Next() {
		var datasetID int64
		var activeVersion int
		if err := rows.Scan(&datasetID, &activeVersion); err != nil {
			continue
		}

		var latestVersion sql.NullInt64
		db.Get().QueryRow(`
			SELECT MAX(version_number) FROM dataset_versions WHERE dataset_id = ?
		`, datasetID).Scan(&latestVersion)

		if latestVersion.Valid {
			db.Get().Exec(`
				UPDATE datasets SET active_version = ? WHERE dataset_id = ?
			`, latestVersion.Int64, datasetID)
			log.Printf("[heal] fixed active_version dataset_id=%d was=%d now=%d", datasetID, activeVersion, latestVersion.Int64)
		} else {
			db.Get().Exec(`
				UPDATE datasets SET active_version = NULL WHERE dataset_id = ?
			`, datasetID)
			log.Printf("[heal] nulled active_version dataset_id=%d — no versions exist", datasetID)
		}
		fixed++
	}
	return fixed
}

// ── 7. dataset_versions with file_path that doesn't exist in B2 ──────────────

func healMissingVersionFiles() int {
	rows, err := db.Get().Query(`
		SELECT version_id, dataset_id, version_number, file_path
		FROM dataset_versions
	`)
	if err != nil {
		log.Printf("[heal] healMissingVersionFiles query error: %v", err)
		return 0
	}
	defer rows.Close()

	fixed := 0
	for rows.Next() {
		var versionID, datasetID int64
		var versionNumber int
		var filePath string
		if err := rows.Scan(&versionID, &datasetID, &versionNumber, &filePath); err != nil {
			continue
		}
		if !fileExists(filePath) {
			log.Printf("[heal] WARNING version file missing — dataset_id=%d version=%d path=%s", datasetID, versionNumber, filePath)
			fixed++
		}
	}
	return fixed
}

func healStuckCrawlingJobs() int {
    threshold := time.Now().Add(-stuckJobThreshold)
    res, err := db.Get().Exec(`
        UPDATE queue
        SET status = 'pending',
            locked_at = NULL,
            crawl_attempts = crawl_attempts - 1
        WHERE status = 'crawling'
          AND (
            locked_at IS NOT NULL AND locked_at < ?
            OR
            locked_at IS NULL AND created_at < ?
          )
    `, threshold, threshold)
    if err != nil {
        log.Printf("[heal] healStuckCrawlingJobs error: %v", err)
        return 0
    }
    n, _ := res.RowsAffected()
    if n > 0 {
        log.Printf("[heal] reset %d stuck crawling jobs to pending", n)
    }
    return int(n)
}


// ── 8. entity_count = 0 but result file has entities ─────────────────────────

func healZeroEntityCounts() int {
	rows, err := db.Get().Query(`
		SELECT version_id, dataset_id, version_number, file_path
		FROM dataset_versions
		WHERE entity_count = 0
		  AND file_path IS NOT NULL
		  AND file_path != ''
	`)
	if err != nil {
		log.Printf("[heal] healZeroEntityCounts query error: %v", err)
		return 0
	}
	defer rows.Close()

	type vrow struct {
		VersionID     int64
		DatasetID     int64
		VersionNumber int
		FilePath      string
	}
	var vrows []vrow
	for rows.Next() {
		var v vrow
		if err := rows.Scan(&v.VersionID, &v.DatasetID, &v.VersionNumber, &v.FilePath); err != nil {
			continue
		}
		vrows = append(vrows, v)
	}

	fixed := 0
	for _, v := range vrows {
		count, err := countEntitiesInFile(v.FilePath)
		if err != nil || count == 0 {
			continue
		}
		_, err = db.Get().Exec(`
			UPDATE dataset_versions SET entity_count = ? WHERE version_id = ?
		`, count, v.VersionID)
		if err != nil {
			log.Printf("[heal] healZeroEntityCounts update version_id=%d: %v", v.VersionID, err)
			continue
		}
		log.Printf("[heal] fixed entity_count dataset_id=%d version=%d count=%d", v.DatasetID, v.VersionNumber, count)
		fixed++
	}
	return fixed
}

// ── 9. Amazon: raw.json exists + status=done but no extract.json ─────────────

func healAmazonRawReady() int {
	rows, err := db.Get().Query(`
		SELECT q.queue_id, du.folder_path
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE d.dataset_type = 'amazon'
		  AND q.status = 'done'
		  AND du.folder_path IS NOT NULL
		  AND du.folder_path != ''
	`)
	if err != nil {
		log.Printf("[heal] healAmazonRawReady query error: %v", err)
		return 0
	}
	defer rows.Close()

	type job struct {
		QueueID    int64
		FolderPath string
	}
	var jobs []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.QueueID, &j.FolderPath); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}

	fixed := 0
	for _, j := range jobs {
		rawPath := filepath.Join(j.FolderPath, "raw.json")
		extractPath := filepath.Join(j.FolderPath, "extract.json")

		if fileExists(rawPath) && !fileExists(extractPath) {
			_, err := db.Get().Exec(`
				UPDATE queue SET status = 'failed', locked_at = NULL WHERE queue_id = ?
			`, j.QueueID)
			if err != nil {
				log.Printf("[heal] healAmazonRawReady mark failed queue_id=%d: %v", j.QueueID, err)
				continue
			}
			log.Printf("[heal] amazon raw-only — marked failed queue_id=%d path=%s", j.QueueID, j.FolderPath)
			fixed++
		}
	}
	return fixed
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func fileExists(path string) bool {
    _, err := storage.Size(path)
    if err == nil {
        return true
    }
    errStr := err.Error()
    if strings.Contains(errStr, "NoSuchKey") || strings.Contains(errStr, "404") {
        return false
    }
    // Network/auth/other error — assume file exists to avoid false warnings
    log.Printf("[heal] fileExists could not verify %s: %v", path, err)
    return true
}

func countEntitiesInFile(filePath string) (int, error) {
	b, err := storage.Read(filePath)
	if err != nil {
		return 0, err
	}
	var rf struct {
		Entities []json.RawMessage `json:"entities"`
		Posts    []json.RawMessage `json:"posts"`
		Total    int               `json:"total"`
	}
	if err := json.Unmarshal(b, &rf); err != nil {
		return 0, err
	}
	if rf.Total > 0 {
		return rf.Total, nil
	}
	if len(rf.Posts) > 0 {
		return len(rf.Posts), nil
	}
	return len(rf.Entities), nil
}

func trimPath(s string) string {
	return strings.TrimRight(strings.TrimSpace(s), "/")
}

// suppress unused warning
var _ = fmt.Sprintf