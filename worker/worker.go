package worker

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/var-raphael/vexaro-engine/ai"
	"github.com/var-raphael/vexaro-engine/clean"
	"github.com/var-raphael/vexaro-engine/combine"
	"github.com/var-raphael/vexaro-engine/crawl"
	"github.com/var-raphael/vexaro-engine/db"
  "github.com/var-raphael/vexaro-engine/diff"
	"github.com/var-raphael/vexaro-engine/format"
	"github.com/var-raphael/vexaro-engine/reddit"
	"github.com/var-raphael/vexaro-engine/storage"
)

const (
	workerTickInterval  = 30 * time.Second
	batchSize           = 20
	maxConcurrentCrawls = 5
	maxPrimaryAttempts  = 3
)

// ── Entry point ───────────────────────────────────────────────────────────────

var tickMu sync.Mutex

func Start() {
	log.Println("[worker] starting")
	for {
		if err := tick(); err != nil {
			log.Printf("[worker] tick error: %v", err)
		}
		time.Sleep(workerTickInterval)
	}
}

func tick() error {
	if !tickMu.TryLock() {
		log.Println("[worker] previous tick still running, skipping")
		return nil
	}
	defer tickMu.Unlock()

	if err := buildClusterMap(); err != nil {
		log.Printf("[worker] build cluster map error: %v", err)
	}
	
	if err := processDiff(); err != nil {
    log.Printf("[worker] diff error: %v", err)
  }

	if err := processCrawlBatch(); err != nil {
		log.Printf("[worker] crawl batch error: %v", err)
	}
	if err := processClean(); err != nil {
		log.Printf("[worker] clean error: %v", err)
	}
	if err := processFormat(); err != nil {
		log.Printf("[worker] format error: %v", err)
	}
	if err := buildExtractClusterMap(); err != nil {
		log.Printf("[worker] build extract cluster map error: %v", err)
	}
	if err := processExtract(); err != nil {
		log.Printf("[worker] extract error: %v", err)
	}
	if err := processCombine(); err != nil {
		log.Printf("[worker] combine error: %v", err)
	}

	if err := buildRedditClusterMap(); err != nil {
		log.Printf("[worker] build reddit cluster map error: %v", err)
	}
	if err := processRedditFetch(); err != nil {
		log.Printf("[worker] reddit fetch error: %v", err)
	}
	if err := processRedditImport(); err != nil {
		log.Printf("[worker] reddit import error: %v", err)
	}
	if err := processRedditQueue(); err != nil {
		log.Printf("[worker] reddit queue error: %v", err)
	}
	return nil
}

// ── Cluster map ───────────────────────────────────────────────────────────────

func buildClusterMap() error {
	rows, err := db.Get().Query(`
		SELECT q.queue_id, q.dataset_url_id, du.url
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'pending'
		AND q.cluster_role = 'solo'
		ORDER BY q.queue_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query pending: %w", err)
	}
	defer rows.Close()

	type pendingRow struct {
		QueueID      int64
		DatasetURLID int64
		URL          string
	}

	urlGroups := make(map[string][]pendingRow)
	var order []string

	for rows.Next() {
		var p pendingRow
		if err := rows.Scan(&p.QueueID, &p.DatasetURLID, &p.URL); err != nil {
			continue
		}
		normalized := normalizeURL(p.URL)
		if _, exists := urlGroups[normalized]; !exists {
			order = append(order, normalized)
		}
		urlGroups[normalized] = append(urlGroups[normalized], p)
	}
	rows.Close()

	for _, normalized := range order {
		group := urlGroups[normalized]
		if len(group) == 1 {
			continue
		}

		primary := group[0]
		waiters := group[1:]

		_, err := db.Get().Exec(`
			UPDATE queue SET cluster_role = 'primary'
			WHERE queue_id = ?
		`, primary.QueueID)
		if err != nil {
			log.Printf("[worker] mark primary error queue_id=%d: %v", primary.QueueID, err)
			continue
		}

		for _, w := range waiters {
			_, err := db.Get().Exec(`
				UPDATE queue
				SET cluster_role = 'waiting',
				    primary_dataset_url_id = ?,
				    status = 'pending'
				WHERE queue_id = ?
			`, primary.DatasetURLID, w.QueueID)
			if err != nil {
				log.Printf("[worker] mark waiter error queue_id=%d: %v", w.QueueID, err)
			}
		}

		log.Printf("[worker] cluster built — url=%s primary=%d waiters=%d",
			normalized, primary.DatasetURLID, len(waiters))
	}

	return nil
}

// ── Crawl batch ───────────────────────────────────────────────────────────────

func processCrawlBatch() error {
	rows, err := db.Get().Query(`
		SELECT q.queue_id, q.dataset_url_id, du.url, du.dataset_id,
		       q.crawl_attempts, q.crawl_type
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE q.status = 'pending'
		AND q.cluster_role IN ('primary', 'solo')
		ORDER BY
			q.crawl_attempts ASC,
			q.created_at ASC,
			(SELECT COUNT(*) FROM datasets_url du2 WHERE du2.dataset_id = du.dataset_id) ASC
		LIMIT ?
	`, batchSize)
	if err != nil {
		return fmt.Errorf("query crawl batch: %w", err)
	}
	defer rows.Close()

	type crawlJob struct {
		QueueID      int64
		DatasetURLID int64
		URL          string
		DatasetID    int64
		Attempts     int
		CrawlType    string
	}

	var jobs []crawlJob
	for rows.Next() {
		var j crawlJob
		if err := rows.Scan(&j.QueueID, &j.DatasetURLID, &j.URL, &j.DatasetID, &j.Attempts, &j.CrawlType); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	rows.Close()

	if len(jobs) == 0 {
		return nil
	}

	log.Printf("[worker] crawling %d jobs", len(jobs))

	sem := make(chan struct{}, maxConcurrentCrawls)
	var wg sync.WaitGroup
	cfg := crawl.Config{BrowserlessKey: os.Getenv("BROWSERLESS_KEYS")}

	for _, job := range jobs {
		job := job
		wg.Add(1)
		sem <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if rec := recover(); rec != nil {
					log.Printf("[worker] panic recovered queue_id=%d url=%s: %v", job.QueueID, job.URL, rec)
					handleCrawlFailure(job.QueueID, job.DatasetURLID, job.Attempts+1)
				}
			}()

			_, _ = db.Get().Exec(`
				UPDATE queue SET status = 'crawling', crawl_attempts = crawl_attempts + 1
				WHERE queue_id = ?
			`, job.QueueID)

			err := crawl.CrawlSingle(cfg, job.QueueID, job.DatasetURLID, job.URL, job.CrawlType)
			if err != nil {
				log.Printf("[worker] crawl failed queue_id=%d url=%s: %v", job.QueueID, job.URL, err)
				handleCrawlFailure(job.QueueID, job.DatasetURLID, job.Attempts+1)
				return
			}

			_, _ = db.Get().Exec(`
				UPDATE queue SET fail_count = 0 WHERE queue_id = ?
			`, job.QueueID)

			fanOutToWaiters(job.DatasetURLID)

			var datasetType string
			db.Get().QueryRow(`
				SELECT COALESCE(dataset_type, 'web') FROM datasets WHERE dataset_id = ?
			`, job.DatasetID).Scan(&datasetType)

			if datasetType != "amazon" {
				_, _ = db.Get().Exec(`
					UPDATE queue SET status = 'proceed-clean'
					WHERE queue_id = ?
				`, job.QueueID)
			}

			log.Printf("[worker] crawl success queue_id=%d url=%s", job.QueueID, job.URL)
		}()
	}

	wg.Wait()
	return nil
}

// ── Fan out ───────────────────────────────────────────────────────────────────

func fanOutToWaiters(primaryDatasetURLID int64) {
	var folderPath sql.NullString
	var primaryURL string
	err := db.Get().QueryRow(`
		SELECT folder_path, url FROM datasets_url WHERE dataset_url_id = ?
	`, primaryDatasetURLID).Scan(&folderPath, &primaryURL)
	if err != nil || !folderPath.Valid {
		log.Printf("[worker] fan out — no folder path for primary dataset_url_id=%d", primaryDatasetURLID)
		return
	}

	rawPath := filepath.Join(folderPath.String, "raw.json")
	rawBytes, err := storage.Read(rawPath)
	if err != nil {
		log.Printf("[worker] fan out — cannot read raw.json from %s: %v", rawPath, err)
		return
	}

	rows, err := db.Get().Query(`
		SELECT q.queue_id, q.dataset_url_id, du.url, d.user_id, d.data_name, d.dataset_id
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE q.primary_dataset_url_id = ?
		AND q.cluster_role = 'waiting'
		AND q.status = 'pending'
	`, primaryDatasetURLID)
	if err != nil {
		log.Printf("[worker] fan out query error: %v", err)
		return
	}
	defer rows.Close()

	type waiter struct {
		QueueID      int64
		DatasetURLID int64
		URL          string
		UserID       string
		DataName     string
		DatasetID    int64
	}
	var waiters []waiter
	for rows.Next() {
		var w waiter
		if err := rows.Scan(&w.QueueID, &w.DatasetURLID, &w.URL, &w.UserID, &w.DataName, &w.DatasetID); err != nil {
			continue
		}
		waiters = append(waiters, w)
	}
	rows.Close()

	if len(waiters) == 0 {
		return
	}

	log.Printf("[worker] fanning out primary=%d to %d waiters", primaryDatasetURLID, len(waiters))

	for _, w := range waiters {
		waiterFolder := crawl.BuildFolderPath(w.DataName, w.URL, w.UserID, w.DatasetID)
		waiterRawPath := filepath.Join(waiterFolder, "raw.json")

		if err := storage.Write(waiterRawPath, rawBytes); err != nil {
			log.Printf("[worker] fan out write error waiter=%d: %v", w.DatasetURLID, err)
			_, _ = db.Get().Exec(`
				UPDATE queue SET status = 'failed', fail_count = fail_count + 1
				WHERE queue_id = ?
			`, w.QueueID)
			continue
		}

		_, _ = db.Get().Exec(`
			UPDATE datasets_url SET folder_path = ?
			WHERE dataset_url_id = ?
		`, waiterFolder, w.DatasetURLID)

		_, _ = db.Get().Exec(`
			UPDATE queue SET status = 'proceed-clean', cluster_role = 'solo'
			WHERE queue_id = ?
		`, w.QueueID)

		log.Printf("[worker] fan out done waiter=%d folder=%s", w.DatasetURLID, waiterFolder)
	}
}

// ── Crawl failure ─────────────────────────────────────────────────────────────

func handleCrawlFailure(queueID int64, datasetURLID int64, attempts int) {
	if attempts >= maxPrimaryAttempts {
		_, _ = db.Get().Exec(`
			UPDATE queue SET status = 'failed'
			WHERE queue_id = ?
		`, queueID)

		_, _ = db.Get().Exec(`
			UPDATE queue SET status = 'failed'
			WHERE primary_dataset_url_id = ?
			AND cluster_role = 'waiting'
		`, datasetURLID)

		log.Printf("[worker] primary exhausted attempts — marking all failed dataset_url_id=%d", datasetURLID)
		return
	}

	var newPrimaryQueueID int64
	var newPrimaryDatasetURLID int64
	err := db.Get().QueryRow(`
		SELECT queue_id, dataset_url_id FROM queue
		WHERE primary_dataset_url_id = ?
		AND cluster_role = 'waiting'
		AND status = 'pending'
		LIMIT 1
	`, datasetURLID).Scan(&newPrimaryQueueID, &newPrimaryDatasetURLID)

	if err == sql.ErrNoRows {
		_, _ = db.Get().Exec(`
			UPDATE queue SET status = 'failed' WHERE queue_id = ?
		`, queueID)
		return
	}
	if err != nil {
		log.Printf("[worker] promote primary query error: %v", err)
		return
	}

	_, _ = db.Get().Exec(`
		UPDATE queue
		SET cluster_role = 'primary',
		    primary_dataset_url_id = NULL,
		    crawl_attempts = ?,
		    status = 'pending'
		WHERE queue_id = ?
	`, attempts, newPrimaryQueueID)

	_, _ = db.Get().Exec(`
		UPDATE queue
		SET primary_dataset_url_id = ?
		WHERE primary_dataset_url_id = ?
		AND cluster_role = 'waiting'
		AND queue_id != ?
	`, newPrimaryDatasetURLID, datasetURLID, newPrimaryQueueID)

	_, _ = db.Get().Exec(`
		UPDATE queue SET status = 'failed' WHERE queue_id = ?
	`, queueID)

	log.Printf("[worker] promoted new primary queue_id=%d after %d attempts", newPrimaryQueueID, attempts)
}

// ── Clean ─────────────────────────────────────────────────────────────────────

func processClean() error {
	rows, err := db.Get().Query(`
		SELECT DISTINCT du.dataset_id
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-clean'
		LIMIT ?
	`, batchSize)
	if err != nil {
		return fmt.Errorf("query clean batch: %w", err)
	}
	defer rows.Close()

	var datasetIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			continue
		}
		datasetIDs = append(datasetIDs, id)
	}
	rows.Close()

	for _, datasetID := range datasetIDs {
		if err := clean.CleanDataset(datasetID); err != nil {
			log.Printf("[worker] clean error dataset_id=%d: %v", datasetID, err)
		}
	}
	return nil
}

// ── Format ────────────────────────────────────────────────────────────────────

func processFormat() error {
	rows, err := db.Get().Query(`
		SELECT DISTINCT du.dataset_id
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-format'
		LIMIT ?
	`, batchSize)
	if err != nil {
		return fmt.Errorf("query format batch: %w", err)
	}
	defer rows.Close()

	var datasetIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			continue
		}
		datasetIDs = append(datasetIDs, id)
	}
	rows.Close()

	for _, datasetID := range datasetIDs {
		if err := format.FormatDataset(datasetID); err != nil {
			log.Printf("[worker] format error dataset_id=%d: %v", datasetID, err)
		}
	}
	return nil
}

// ── Extract ───────────────────────────────────────────────────────────────────

func processExtract() error {
	rows, err := db.Get().Query(`
		SELECT DISTINCT du.dataset_id
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-extract'
		AND q.extract_cluster_role IN ('primary', 'solo')
		LIMIT ?
	`, batchSize)
	if err != nil {
		return fmt.Errorf("query extract batch: %w", err)
	}
	defer rows.Close()

	var datasetIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			continue
		}
		datasetIDs = append(datasetIDs, id)
	}
	rows.Close()

	for _, datasetID := range datasetIDs {
		schema, err := loadSchemaForDataset(datasetID)
		if err != nil {
			log.Printf("[worker] load schema error dataset_id=%d: %v", datasetID, err)
			continue
		}

		skipped, err := skipExtractIfAlreadyDone(datasetID)
		if err != nil {
			log.Printf("[worker] skip extract check error dataset_id=%d: %v", datasetID, err)
		} else if skipped > 0 {
			log.Printf("[worker] skipped %d extractions via copy dataset_id=%d", skipped, datasetID)
		}

		if err := ai.ExtractDataset(datasetID, schema); err != nil {
			log.Printf("[worker] extract error dataset_id=%d: %v", datasetID, err)
		}
		if err := fanOutExtractToWaiters(datasetID); err != nil {
			log.Printf("[worker] extract fan out error dataset_id=%d: %v", datasetID, err)
		}
		if err := fanOutExtractToLateArrivals(datasetID); err != nil {
			log.Printf("[worker] late fan out error dataset_id=%d: %v", datasetID, err)
		}
	}
	return nil
}

// ── Combine ───────────────────────────────────────────────────────────────────

func processCombine() error {
	rows, err := db.Get().Query(`
		SELECT DISTINCT du.dataset_id
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-version'
		LIMIT ?
	`, batchSize)
	if err != nil {
		return fmt.Errorf("query combine batch: %w", err)
	}
	defer rows.Close()

	var datasetIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			continue
		}
		datasetIDs = append(datasetIDs, id)
	}
	rows.Close()

	for _, datasetID := range datasetIDs {
		if err := combine.CombineDataset(datasetID); err != nil {
			log.Printf("[worker] combine error dataset_id=%d: %v", datasetID, err)
		}
	}
	return nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func normalizeURL(u string) string {
	return strings.TrimRight(strings.ToLower(strings.TrimSpace(u)), "/")
}

func loadSchemaForDataset(datasetID int64) (map[string]*ai.SchemaField, error) {
	var fieldsJSON string
	err := db.Get().QueryRow(`
		SELECT fields FROM dataset_schema WHERE dataset_id = ?
	`, datasetID).Scan(&fieldsJSON)
	if err != nil {
		return nil, fmt.Errorf("load schema: %w", err)
	}

	var rawFields map[string]struct {
		Type        string `json:"type"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal([]byte(fieldsJSON), &rawFields); err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}

	schema := map[string]*ai.SchemaField{}
	for key, f := range rawFields {
		schema[key] = &ai.SchemaField{Type: f.Type, Description: f.Description}
	}

	return schema, nil
}

func buildExtractClusterMap() error {
	rows, err := db.Get().Query(`
		SELECT q.queue_id, q.dataset_url_id, du.url, du.dataset_id
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-extract'
		AND q.extract_cluster_role = 'solo'
		AND du.folder_path IS NOT NULL
		ORDER BY q.queue_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query proceed-extract: %w", err)
	}
	defer rows.Close()

	type extractRow struct {
		QueueID      int64
		DatasetURLID int64
		URL          string
		DatasetID    int64
	}

	var pending []extractRow
	for rows.Next() {
		var p extractRow
		if err := rows.Scan(&p.QueueID, &p.DatasetURLID, &p.URL, &p.DatasetID); err != nil {
			continue
		}
		pending = append(pending, p)
	}
	rows.Close()

	if len(pending) == 0 {
		return nil
	}

	type clusterRow struct {
		extractRow
		ClusterKey string
	}

	var clusterRows []clusterRow
	for _, p := range pending {
		schemaHash, err := getSchemaHash(p.DatasetID)
		if err != nil {
			log.Printf("[worker] schema hash error dataset_id=%d: %v", p.DatasetID, err)
			continue
		}
		key := schemaHash + "|" + normalizeURL(p.URL)
		clusterRows = append(clusterRows, clusterRow{p, key})
	}

	groups := make(map[string][]clusterRow)
	var order []string
	for _, r := range clusterRows {
		if _, exists := groups[r.ClusterKey]; !exists {
			order = append(order, r.ClusterKey)
		}
		groups[r.ClusterKey] = append(groups[r.ClusterKey], r)
	}

	for _, key := range order {
		group := groups[key]
		if len(group) == 1 {
			continue
		}

		primary := group[0]
		waiters := group[1:]

		_, err := db.Get().Exec(`
			UPDATE queue SET extract_cluster_role = 'primary'
			WHERE queue_id = ?
		`, primary.QueueID)
		if err != nil {
			log.Printf("[worker] mark extract primary error queue_id=%d: %v", primary.QueueID, err)
			continue
		}

		for _, w := range waiters {
			_, err := db.Get().Exec(`
				UPDATE queue
				SET extract_cluster_role = 'waiting',
				    extract_primary_queue_id = ?
				WHERE queue_id = ?
			`, primary.QueueID, w.QueueID)
			if err != nil {
				log.Printf("[worker] mark extract waiter error queue_id=%d: %v", w.QueueID, err)
			}
		}

		log.Printf("[worker] extract cluster built — key=%s primary=%d waiters=%d",
			key[:16], primary.QueueID, len(waiters))
	}

	return nil
}

func fanOutExtractToWaiters(datasetID int64) error {
	rows, err := db.Get().Query(`
		SELECT q.queue_id, du.folder_path
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-version'
		AND q.extract_cluster_role = 'primary'
		AND du.dataset_id = ?
	`, datasetID)
	if err != nil {
		return fmt.Errorf("query primaries: %w", err)
	}
	defer rows.Close()

	type primaryRow struct {
		QueueID    int64
		FolderPath string
	}
	var primaries []primaryRow
	for rows.Next() {
		var p primaryRow
		var folderPath sql.NullString
		if err := rows.Scan(&p.QueueID, &folderPath); err != nil {
			continue
		}
		if !folderPath.Valid {
			continue
		}
		p.FolderPath = folderPath.String
		primaries = append(primaries, p)
	}
	rows.Close()

	for _, p := range primaries {
		extractBytes, err := storage.Read(filepath.Join(p.FolderPath, "extract.json"))
		if err != nil {
			log.Printf("[worker] extract fan out — cannot read extract.json from %s: %v", p.FolderPath, err)
			continue
		}

		wRows, err := db.Get().Query(`
			SELECT q.queue_id, du.folder_path
			FROM queue q
			JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
			WHERE q.extract_primary_queue_id = ?
			AND q.extract_cluster_role = 'waiting'
			AND q.status = 'proceed-extract'
		`, p.QueueID)
		if err != nil {
			log.Printf("[worker] extract fan out query error: %v", err)
			continue
		}

		type waiterRow struct {
			QueueID    int64
			FolderPath string
		}
		var waiters []waiterRow
		for wRows.Next() {
			var w waiterRow
			var folderPath sql.NullString
			if err := wRows.Scan(&w.QueueID, &folderPath); err != nil {
				continue
			}
			if !folderPath.Valid {
				continue
			}
			w.FolderPath = folderPath.String
			waiters = append(waiters, w)
		}
		wRows.Close()

		if len(waiters) == 0 {
			continue
		}

		log.Printf("[worker] extract fan out primary=%d to %d waiters", p.QueueID, len(waiters))

		for _, w := range waiters {
			waiterExtractPath := filepath.Join(w.FolderPath, "extract.json")
			if err := storage.Write(waiterExtractPath, extractBytes); err != nil {
				log.Printf("[worker] extract fan out write error waiter=%d: %v", w.QueueID, err)
				_, _ = db.Get().Exec(`
					UPDATE queue SET status = 'failed', fail_count = fail_count + 1
					WHERE queue_id = ?
				`, w.QueueID)
				continue
			}

			_, _ = db.Get().Exec(`
				UPDATE queue
				SET status = 'proceed-version',
				    extract_cluster_role = 'solo',
				    extract_primary_queue_id = NULL
				WHERE queue_id = ?
			`, w.QueueID)

			log.Printf("[worker] extract fan out done waiter=%d folder=%s", w.QueueID, w.FolderPath)
		}
	}

	return nil
}

func fanOutExtractToLateArrivals(primaryDatasetID int64) error {
	rows, err := db.Get().Query(`
		SELECT q.queue_id, du.folder_path, du.url, du.dataset_id
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-extract'
		AND q.extract_cluster_role = 'solo'
		AND du.dataset_id != ?
		AND du.folder_path IS NOT NULL
	`, primaryDatasetID)
	if err != nil {
		return fmt.Errorf("query late arrivals: %w", err)
	}
	defer rows.Close()

	type lateRow struct {
		QueueID    int64
		FolderPath string
		URL        string
		DatasetID  int64
	}
	var lateArrivals []lateRow
	for rows.Next() {
		var l lateRow
		var fp sql.NullString
		if err := rows.Scan(&l.QueueID, &fp, &l.URL, &l.DatasetID); err != nil {
			continue
		}
		if !fp.Valid {
			continue
		}
		l.FolderPath = fp.String
		lateArrivals = append(lateArrivals, l)
	}
	rows.Close()

	if len(lateArrivals) == 0 {
		return nil
	}

	primaryHash, err := getSchemaHash(primaryDatasetID)
	if err != nil {
		return fmt.Errorf("primary schema hash: %w", err)
	}

	type extractEntry struct {
		URL        string
		FolderPath string
	}
	primaryRows, err := db.Get().Query(`
		SELECT du.url, du.folder_path
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-version'
		AND du.dataset_id = ?
		AND du.folder_path IS NOT NULL
	`, primaryDatasetID)
	if err != nil {
		return fmt.Errorf("query primary extracts: %w", err)
	}
	defer primaryRows.Close()

	primaryExtracts := map[string]string{}
	for primaryRows.Next() {
		var url string
		var fp sql.NullString
		if err := primaryRows.Scan(&url, &fp); err != nil {
			continue
		}
		if fp.Valid {
			primaryExtracts[normalizeURL(url)] = fp.String
		}
	}
	primaryRows.Close()

	for _, late := range lateArrivals {
		lateHash, err := getSchemaHash(late.DatasetID)
		if err != nil {
			continue
		}
		if lateHash != primaryHash {
			continue
		}

		primaryFolder, ok := primaryExtracts[normalizeURL(late.URL)]
		if !ok {
			continue
		}

		extractBytes, err := storage.Read(filepath.Join(primaryFolder, "extract.json"))
		if err != nil {
			continue
		}

		if err := storage.Write(filepath.Join(late.FolderPath, "extract.json"), extractBytes); err != nil {
			log.Printf("[worker] late fan out write error queue_id=%d: %v", late.QueueID, err)
			continue
		}

		_, _ = db.Get().Exec(`
			UPDATE queue SET status = 'proceed-version'
			WHERE queue_id = ?
		`, late.QueueID)

		log.Printf("[worker] late extract fan out done — queue_id=%d dataset_id=%d url=%s",
			late.QueueID, late.DatasetID, late.URL)
	}

	return nil
}

func getSchemaHash(datasetID int64) (string, error) {
	var fieldsJSON string
	err := db.Get().QueryRow(`
		SELECT fields FROM dataset_schema WHERE dataset_id = ?
	`, datasetID).Scan(&fieldsJSON)
	if err != nil {
		return "", fmt.Errorf("load schema: %w", err)
	}

	var rawFields map[string]interface{}
	if err := json.Unmarshal([]byte(fieldsJSON), &rawFields); err != nil {
		return "", fmt.Errorf("parse schema: %w", err)
	}

	keys := make([]string, 0, len(rawFields))
	for k := range rawFields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		v, _ := json.Marshal(rawFields[k])
		sb.WriteString(k)
		sb.WriteString(":")
		sb.WriteString(string(v))
		sb.WriteString("|")
	}

	h := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(h[:]), nil
}

func skipExtractIfAlreadyDone(datasetID int64) (int, error) {
	rows, err := db.Get().Query(`
		SELECT q.queue_id, du.folder_path, du.url
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-extract'
		AND du.dataset_id = ?
		AND du.folder_path IS NOT NULL
	`, datasetID)
	if err != nil {
		return 0, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	type job struct {
		QueueID    int64
		FolderPath string
		URL        string
	}
	var jobs []job
	for rows.Next() {
		var j job
		var fp sql.NullString
		if err := rows.Scan(&j.QueueID, &fp, &j.URL); err != nil {
			continue
		}
		if !fp.Valid {
			continue
		}
		j.FolderPath = fp.String
		jobs = append(jobs, j)
	}
	rows.Close()

	if len(jobs) == 0 {
		return 0, nil
	}

	myHash, err := getSchemaHash(datasetID)
	if err != nil {
		return 0, fmt.Errorf("schema hash: %w", err)
	}

	skipped := 0
	for _, j := range jobs {
		siblingRows, err := db.Get().Query(`
			SELECT du.folder_path, du.dataset_id
			FROM datasets_url du
			JOIN queue q ON q.dataset_url_id = du.dataset_url_id
			WHERE du.url = ?
			AND du.dataset_id != ?
			AND du.folder_path IS NOT NULL
			AND q.status IN ('proceed-version', 'done')
		`, j.URL, datasetID)
		if err != nil {
			continue
		}

		type sibling struct {
			FolderPath string
			DatasetID  int64
		}
		var siblings []sibling
		for siblingRows.Next() {
			var s sibling
			var fp sql.NullString
			if err := siblingRows.Scan(&fp, &s.DatasetID); err != nil {
				continue
			}
			if fp.Valid {
				s.FolderPath = fp.String
				siblings = append(siblings, s)
			}
		}
		siblingRows.Close()

		for _, s := range siblings {
			sibHash, err := getSchemaHash(s.DatasetID)
			if err != nil || sibHash != myHash {
				continue
			}

			extractBytes, err := storage.Read(filepath.Join(s.FolderPath, "extract.json"))
			if err != nil {
				continue
			}

			if err := storage.Write(filepath.Join(j.FolderPath, "extract.json"), extractBytes); err != nil {
				log.Printf("[worker] skip extract write error queue_id=%d: %v", j.QueueID, err)
				continue
			}

			_, _ = db.Get().Exec(`
				UPDATE queue SET status = 'proceed-version'
				WHERE queue_id = ?
			`, j.QueueID)

			log.Printf("[worker] skip extract — copied from dataset_id=%d queue_id=%d url=%s",
				s.DatasetID, j.QueueID, j.URL)
			skipped++
			break
		}
	}

	return skipped, nil
}

// ── Reddit queue ──────────────────────────────────────────────────────────────

func processRedditQueue() error {
	rows, err := db.Get().Query(`
		SELECT DISTINCT du.dataset_id
		FROM reddit_queue rq
		JOIN datasets_url du ON du.dataset_url_id = rq.dataset_url_id
		WHERE rq.status = 'proceed-combine'
		AND NOT EXISTS (
			SELECT 1 FROM reddit_queue rq2
			JOIN datasets_url du2 ON du2.dataset_url_id = rq2.dataset_url_id
			WHERE du2.dataset_id = du.dataset_id
			AND rq2.status = 'pending'
		)
		LIMIT ?
	`, batchSize)
	if err != nil {
		return fmt.Errorf("query reddit queue: %w", err)
	}
	defer rows.Close()

	var datasetIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			continue
		}
		datasetIDs = append(datasetIDs, id)
	}
	rows.Close()

	if len(datasetIDs) == 0 {
		return nil
	}

	for _, datasetID := range datasetIDs {
		datasetID := datasetID
		log.Printf("[worker] reddit combine+version dataset_id=%d", datasetID)
		if err := reddit.RunCombineAndVersion(datasetID); err != nil {
			log.Printf("[worker] reddit error dataset_id=%d: %v", datasetID, err)
		}
	}

	return nil
}

func buildRedditClusterMap() error {
	rows, err := db.Get().Query(`
		SELECT rq.reddit_queue_id, rq.dataset_url_id, du.url
		FROM reddit_queue rq
		JOIN datasets_url du ON du.dataset_url_id = rq.dataset_url_id
		WHERE rq.status = 'pending'
		AND rq.cluster_role = 'solo'
		AND du.url_type = 'discovery'
		ORDER BY rq.reddit_queue_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query pending reddit: %w", err)
	}
	defer rows.Close()

	type pendingRow struct {
		QueueID      int64
		DatasetURLID int64
		URL          string
	}

	urlGroups := make(map[string][]pendingRow)
	var order []string

	for rows.Next() {
		var p pendingRow
		if err := rows.Scan(&p.QueueID, &p.DatasetURLID, &p.URL); err != nil {
			continue
		}
		normalized := normalizeURL(p.URL)
		if _, exists := urlGroups[normalized]; !exists {
			order = append(order, normalized)
		}
		urlGroups[normalized] = append(urlGroups[normalized], p)
	}
	rows.Close()

	for _, normalized := range order {
		group := urlGroups[normalized]
		if len(group) == 1 {
			continue
		}

		primary := group[0]
		waiters := group[1:]

		_, err := db.Get().Exec(`
			UPDATE reddit_queue SET cluster_role = 'primary'
			WHERE reddit_queue_id = ?
		`, primary.QueueID)
		if err != nil {
			log.Printf("[worker] reddit mark primary error queue_id=%d: %v", primary.QueueID, err)
			continue
		}

		for _, w := range waiters {
			_, err := db.Get().Exec(`
				UPDATE reddit_queue
				SET cluster_role = 'waiting',
				    primary_dataset_url_id = ?
				WHERE reddit_queue_id = ?
			`, primary.DatasetURLID, w.QueueID)
			if err != nil {
				log.Printf("[worker] reddit mark waiter error queue_id=%d: %v", w.QueueID, err)
			}
		}

		log.Printf("[worker] reddit cluster built — url=%s primary=%d waiters=%d",
			normalized, primary.DatasetURLID, len(waiters))
	}

	return nil
}

func processRedditFetch() error {
	rows, err := db.Get().Query(`
		SELECT rq.reddit_queue_id, rq.dataset_url_id, du.url,
		       du.dataset_id, d.data_name, d.user_id
		FROM reddit_queue rq
		JOIN datasets_url du ON du.dataset_url_id = rq.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE rq.status = 'pending'
		AND rq.cluster_role IN ('primary', 'solo')
		AND du.url_type = 'discovery'
		ORDER BY rq.reddit_queue_id ASC
		LIMIT ?
	`, batchSize)
	if err != nil {
		return fmt.Errorf("query reddit fetch batch: %w", err)
	}
	defer rows.Close()

	type fetchJob struct {
		QueueID      int64
		DatasetURLID int64
		URL          string
		DatasetID    int64
		DataName     string
		UserID       string
	}

	var jobs []fetchJob
	for rows.Next() {
		var j fetchJob
		if err := rows.Scan(&j.QueueID, &j.DatasetURLID, &j.URL,
			&j.DatasetID, &j.DataName, &j.UserID); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	rows.Close()

	if len(jobs) == 0 {
		return nil
	}

	for _, job := range jobs {
		job := job
		log.Printf("[worker] reddit fetch queue_id=%d url=%s", job.QueueID, job.URL)

		schema := reddit.DefaultSchema()

		rawBytes, err := reddit.FetchRaw(job.URL)
		if err != nil {
			log.Printf("[worker] reddit fetch error queue_id=%d: %v", job.QueueID, err)
			_, _ = db.Get().Exec(`
				UPDATE reddit_queue SET status = 'failed', fail_count = fail_count + 1
				WHERE reddit_queue_id = ?
			`, job.QueueID)
			_, _ = db.Get().Exec(`
				UPDATE reddit_queue SET status = 'failed', fail_count = fail_count + 1
				WHERE primary_dataset_url_id = ? AND cluster_role = 'waiting'
			`, job.DatasetURLID)
			continue
		}

		if err := reddit.SaveRawForExistingDataset(
			rawBytes, job.URL,
			job.DatasetID, job.UserID, job.DataName,
			schema,
		); err != nil {
			log.Printf("[worker] reddit save error queue_id=%d: %v", job.QueueID, err)
			_, _ = db.Get().Exec(`
				UPDATE reddit_queue SET status = 'failed', fail_count = fail_count + 1
				WHERE reddit_queue_id = ?
			`, job.QueueID)
			continue
		}

		waiterRows, _ := db.Get().Query(`
			SELECT rq.reddit_queue_id, du.dataset_id, d.data_name, d.user_id
			FROM reddit_queue rq
			JOIN datasets_url du ON du.dataset_url_id = rq.dataset_url_id
			JOIN datasets d ON d.dataset_id = du.dataset_id
			WHERE rq.primary_dataset_url_id = ?
			AND rq.cluster_role = 'waiting'
			AND rq.status = 'pending'
		`, job.DatasetURLID)

		if waiterRows != nil {
			type waiter struct {
				QueueID   int64
				DatasetID int64
				DataName  string
				UserID    string
			}
			var waiters []waiter
			for waiterRows.Next() {
				var w waiter
				if waiterRows.Scan(&w.QueueID, &w.DatasetID, &w.DataName, &w.UserID) == nil {
					waiters = append(waiters, w)
				}
			}
			waiterRows.Close()

			for _, w := range waiters {
				if err := reddit.SaveRawForExistingDataset(
					rawBytes, job.URL,
					w.DatasetID, w.UserID, w.DataName,
					schema,
				); err != nil {
					log.Printf("[worker] reddit fan out save error waiter=%d: %v", w.QueueID, err)
					_, _ = db.Get().Exec(`
						UPDATE reddit_queue SET status = 'failed', fail_count = fail_count + 1
						WHERE reddit_queue_id = ?
					`, w.QueueID)
					continue
				}
				_, _ = db.Get().Exec(`
					UPDATE reddit_queue SET status = 'proceed-combine', cluster_role = 'solo'
					WHERE reddit_queue_id = ?
				`, w.QueueID)
				log.Printf("[worker] reddit fan out done waiter=%d dataset_id=%d", w.QueueID, w.DatasetID)
			}
		}

		_, _ = db.Get().Exec(`
			UPDATE reddit_queue SET status = 'proceed-combine'
			WHERE reddit_queue_id = ?
		`, job.QueueID)

		log.Printf("[worker] reddit fetch done queue_id=%d", job.QueueID)
	}

	return nil
}

func processRedditImport() error {
	rows, err := db.Get().Query(`
		SELECT rq.reddit_queue_id, rq.dataset_url_id, du.url,
		       du.dataset_id, d.data_name, d.user_id
		FROM reddit_queue rq
		JOIN datasets_url du ON du.dataset_url_id = rq.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE rq.status = 'pending'
		AND rq.cluster_role = 'solo'
		AND du.url_type = 'import'
		ORDER BY rq.reddit_queue_id ASC
		LIMIT ?
	`, batchSize)
	if err != nil {
		return fmt.Errorf("query reddit import batch: %w", err)
	}
	defer rows.Close()

	type importJob struct {
		QueueID      int64
		DatasetURLID int64
		URL          string
		DatasetID    int64
		DataName     string
		UserID       string
	}

	var jobs []importJob
	for rows.Next() {
		var j importJob
		if err := rows.Scan(&j.QueueID, &j.DatasetURLID, &j.URL,
			&j.DatasetID, &j.DataName, &j.UserID); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	rows.Close()

	if len(jobs) == 0 {
		return nil
	}

	for _, job := range jobs {
		job := job
		log.Printf("[worker] reddit import queue_id=%d url=%s", job.QueueID, job.URL)

		schema := reddit.DefaultSchema()

		rawBytes, err := reddit.FetchRaw(job.URL)
		if err != nil {
			log.Printf("[worker] reddit import fetch error queue_id=%d: %v", job.QueueID, err)
			_, _ = db.Get().Exec(`
				UPDATE reddit_queue SET status = 'failed', fail_count = fail_count + 1
				WHERE reddit_queue_id = ?
			`, job.QueueID)
			continue
		}

		if err := reddit.SaveRawForExistingDataset(
			rawBytes, job.URL,
			job.DatasetID, job.UserID, job.DataName,
			schema,
		); err != nil {
			log.Printf("[worker] reddit import save error queue_id=%d: %v", job.QueueID, err)
			_, _ = db.Get().Exec(`
				UPDATE reddit_queue SET status = 'failed', fail_count = fail_count + 1
				WHERE reddit_queue_id = ?
			`, job.QueueID)
			continue
		}

		_, _ = db.Get().Exec(`
			UPDATE reddit_queue SET status = 'proceed-combine'
			WHERE reddit_queue_id = ?
		`, job.QueueID)

		log.Printf("[worker] reddit import done queue_id=%d", job.QueueID)
	}

	return nil
}

func processDiff() error {
	rows, err := db.Get().Query(`
		SELECT DISTINCT du.dataset_id, d.data_name, COALESCE(ds.source, 'web')
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		LEFT JOIN dataset_schema ds ON ds.dataset_id = du.dataset_id
		WHERE q.status = 'pending-diff'
		LIMIT ?
	`, batchSize)
	if err != nil {
		return fmt.Errorf("query pending-diff: %w", err)
	}
	defer rows.Close()

	type diffJob struct {
		DatasetID int64
		DataName  string
		Source    string
	}

	var jobs []diffJob
	for rows.Next() {
		var j diffJob
		if err := rows.Scan(&j.DatasetID, &j.DataName, &j.Source); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	rows.Close()

	if len(jobs) == 0 {
		return nil
	}

	cfg := crawl.Config{BrowserlessKey: os.Getenv("BROWSERLESS_KEYS")}

	for _, job := range jobs {
		job := job
		log.Printf("[worker] diff dataset_id=%d", job.DatasetID)
		if err := diff.Run(job.DatasetID, cfg); err != nil {
			log.Printf("[worker] diff error dataset_id=%d: %v", job.DatasetID, err)
		}
	}

	return nil
}