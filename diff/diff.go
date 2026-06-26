package diff

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/var-raphael/vexaro-engine/ai"
	"github.com/var-raphael/vexaro-engine/crawl"
	"github.com/var-raphael/vexaro-engine/clean"
	"github.com/var-raphael/vexaro-engine/db"
	"github.com/var-raphael/vexaro-engine/storage"
)

const (
	badFetchThreshold    = 0.27
	autoDropThreshold    = 0.02
	autoProceedThreshold = 0.15
	maxAILines           = 75
	maxLineLen           = 200
	maxTotalLen          = 8000
)

var reOnlySymbols = regexp.MustCompile(`^[\s\W]+$`)

type diffJob struct {
	QueueID         int64
	DatasetURLID    int64
	URL             string
	FolderPath      string
	DiffClusterRole string
}

type cleanedDoc struct {
	Content string `json:"content"`
}

// ---------------------------------------------------------------- Run ------

func Run(datasetID int64, cfg crawl.Config) error {
	jobs, err := loadJobs(datasetID)
	if err != nil {
		return fmt.Errorf("load jobs: %w", err)
	}
	if len(jobs) == 0 {
		log.Printf("[diff] no urls found for dataset_id=%d", datasetID)
		return nil
	}

	var datasetType string
	err = db.Get().QueryRow(`
		SELECT COALESCE(dataset_type, 'web') FROM datasets WHERE dataset_id = ?
	`, datasetID).Scan(&datasetType)
	if err != nil {
		return fmt.Errorf("load dataset type: %w", err)
	}

	if datasetType == "amazon" {
		return runAmazonDiff(datasetID, jobs)
	}

	return runWebDiff(datasetID, jobs, cfg)
}

// ---------------------------------------------------------------- Amazon diff --

func runAmazonDiff(datasetID int64, jobs []diffJob) error {
	apiKey := os.Getenv("AMAZON_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("AMAZON_API_KEY not set")
	}

	var marketplace string
	err := db.Get().QueryRow(`
		SELECT COALESCE(marketplace, 'com') FROM datasets WHERE dataset_id = ?
	`, datasetID).Scan(&marketplace)
	if err != nil {
		return fmt.Errorf("load marketplace: %w", err)
	}

	changed := 0

	for _, job := range jobs {
		if job.DiffClusterRole == "waiting" {
			continue
		}

		log.Printf("[diff/amazon] processing %s", job.URL)

		if job.FolderPath == "" {
			log.Printf("[diff/amazon] no folder_path for %s — skipping", job.URL)
			markDone(job.QueueID)
			FanOutDiffToWaiters(job.DatasetURLID, false, "amazon")
			continue
		}

		asin := extractASIN(job.URL)
		if asin == "" {
			log.Printf("[diff/amazon] cannot extract ASIN from %s — skipping", job.URL)
			markDone(job.QueueID)
			FanOutDiffToWaiters(job.DatasetURLID, false, "amazon")
			continue
		}

		oldEntity, err := loadAmazonExtract(job.FolderPath)
		if err != nil {
			log.Printf("[diff/amazon] load extract.json failed for %s: %v — forcing refresh", job.URL, err)
			if err := fetchAndSaveAmazon(job, asin, marketplace, apiKey); err != nil {
				log.Printf("[diff/amazon] fetch failed for %s: %v", job.URL, err)
				FanOutDiffToWaiters(job.DatasetURLID, false, "amazon")
				continue
			}
			markProceedFormat(job.QueueID)
			FanOutDiffToWaiters(job.DatasetURLID, true, "amazon")
			changed++
			continue
		}

		freshResponse, err := fetchAmazonAPI(asin, marketplace, apiKey)
		if err != nil {
			log.Printf("[diff/amazon] api fetch failed for %s: %v — skipping", job.URL, err)
			FanOutDiffToWaiters(job.DatasetURLID, false, "amazon")
			continue
		}

		if !amazonEntityChanged(oldEntity, freshResponse) {
			log.Printf("[diff/amazon] no change for %s — skipping", job.URL)
			markDone(job.QueueID)
			FanOutDiffToWaiters(job.DatasetURLID, false, "amazon")
			continue
		}

		log.Printf("[diff/amazon] change detected for %s — proceeding", job.URL)

		if err := saveAmazonRaw(job, freshResponse); err != nil {
			log.Printf("[diff/amazon] save raw failed for %s: %v", job.URL, err)
			FanOutDiffToWaiters(job.DatasetURLID, false, "amazon")
			continue
		}

		markProceedFormat(job.QueueID)
		FanOutDiffToWaiters(job.DatasetURLID, true, "amazon")
		changed++
	}

	if changed == 0 {
		log.Printf("[diff/amazon] no changes for dataset_id=%d", datasetID)
	} else {
		log.Printf("[diff/amazon] %d urls changed for dataset_id=%d", changed, datasetID)
	}

	return nil
}

// ── Amazon helpers ────────────────────────────────────────────────────────────

func extractASIN(rawURL string) string {
	parts := strings.Split(rawURL, "/dp/")
	if len(parts) < 2 {
		return ""
	}
	asin := strings.Split(parts[1], "/")[0]
	asin = strings.TrimSpace(asin)
	if len(asin) != 10 {
		return ""
	}
	return asin
}

func fetchAmazonAPI(asin, marketplace, apiKey string) (map[string]interface{}, error) {
	apiURL := fmt.Sprintf(
		"https://api.amazonscraperapi.com/api/v1/amazon/product?query=%s&domain=%s&api_key=%s",
		asin, marketplace, apiKey,
	)

	resp, err := http.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("ASIN not found: %s", asin)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("api returned status %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	return result, nil
}

func loadAmazonExtract(folderPath string) (map[string]interface{}, error) {
	extractPath := filepath.Join(folderPath, "extract.json")
	b, err := storage.Read(extractPath)
	if err != nil {
		return nil, fmt.Errorf("read extract.json: %w", err)
	}

	var payload struct {
		Entities []map[string]interface{} `json:"entities"`
	}
	if err := json.Unmarshal(b, &payload); err != nil {
		return nil, fmt.Errorf("parse extract.json: %w", err)
	}

	if len(payload.Entities) == 0 {
		return nil, fmt.Errorf("no entities in extract.json")
	}

	return payload.Entities[0], nil
}

func amazonEntityChanged(oldEntity, freshResponse map[string]interface{}) bool {
	for field, oldVal := range oldEntity {
		if field == "_source" {
			continue
		}
		newVal, ok := freshResponse[field]
		if !ok {
			continue
		}
		oldJSON, _ := json.Marshal(oldVal)
		newJSON, _ := json.Marshal(newVal)
		if string(oldJSON) != string(newJSON) {
			log.Printf("[diff/amazon] field %q changed: %s → %s", field, oldJSON, newJSON)
			return true
		}
	}
	return false
}

func saveAmazonRaw(job diffJob, apiResponse map[string]interface{}) error {
	asin := extractASIN(job.URL)

	type scrapedData struct {
		URL       string            `json:"url"`
		Title     string            `json:"title"`
		LayerUsed string            `json:"layer_used"`
		Metadata  map[string]string `json:"metadata"`
		Raw       string            `json:"raw,omitempty"`
	}

	rawBytes, err := json.Marshal(apiResponse)
	if err != nil {
		return fmt.Errorf("marshal api response: %w", err)
	}

	title := ""
	if t, ok := apiResponse["title"].(string); ok {
		title = t
	}

	data := scrapedData{
		URL:       job.URL,
		Title:     title,
		LayerUsed: "amazon-api",
		Metadata:  map[string]string{"asin": asin},
		Raw:       string(rawBytes),
	}

	outBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal scraped data: %w", err)
	}

	rawPath := filepath.Join(job.FolderPath, "raw.json")
	return storage.Write(rawPath, outBytes)
}

func fetchAndSaveAmazon(job diffJob, asin, marketplace, apiKey string) error {
	freshResponse, err := fetchAmazonAPI(asin, marketplace, apiKey)
	if err != nil {
		return err
	}
	return saveAmazonRaw(job, freshResponse)
}

func markProceedFormat(queueID int64) {
	_, err := db.Get().Exec(`
		UPDATE queue SET status = 'proceed-format', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[diff/amazon] markProceedFormat queue_id=%d: %v", queueID, err)
	}
}

// ---------------------------------------------------------------- Web diff --

func runWebDiff(datasetID int64, jobs []diffJob, cfg crawl.Config) error {
	database := db.Get()

	schemaChanged, err := checkSchemaChanged(datasetID)
	if err != nil {
		return fmt.Errorf("check schema_changed: %w", err)
	}

	var includeLinks bool
	err = db.Get().QueryRow(`
		SELECT COALESCE(include_links, 0) FROM datasets WHERE dataset_id = ?
	`, datasetID).Scan(&includeLinks)
	if err != nil {
		return fmt.Errorf("load include_links: %w", err)
	}

	changed := 0

	if schemaChanged {
		log.Printf("[diff] schema changed for dataset_id=%d — forcing full pipeline", datasetID)
		for _, job := range jobs {
			if job.DiffClusterRole == "waiting" {
				continue
			}
			if job.FolderPath == "" {
				markDone(job.QueueID)
				FanOutDiffToWaiters(job.DatasetURLID, false, "web")
				continue
			}
			if err := crawl.FetchAndSave(cfg, job.FolderPath, job.URL, includeLinks); err != nil {
				log.Printf("[diff] FetchAndSave failed for %s: %v — skipping", job.URL, err)
				FanOutDiffToWaiters(job.DatasetURLID, false, "web")
				continue
			}
			_, err = database.Exec(`
				UPDATE queue SET status = 'proceed-clean', locked_at = NULL WHERE queue_id = ?
			`, job.QueueID)
			if err != nil {
				log.Printf("[diff] markProceedClean queue_id=%d: %v", job.QueueID, err)
			}
			FanOutDiffToWaiters(job.DatasetURLID, true, "web")
			changed++
		}
	} else {
		for _, job := range jobs {
			if job.DiffClusterRole == "waiting" {
				continue
			}

			log.Printf("[diff] processing %s", job.URL)

			if job.FolderPath == "" {
				log.Printf("[diff] no folder_path for %s — skipping", job.URL)
				markDone(job.QueueID)
				FanOutDiffToWaiters(job.DatasetURLID, false, "web")
				continue
			}

			storedContent := loadStoredContent(job.FolderPath)

newHTML, err := crawl.FetchRaw(cfg, job.URL)
if err != nil {
    if errors.Is(err, crawl.ErrNotFound) {
        log.Printf("[diff] 404 for %s — marking dead", job.URL)
        bumpFailCount(job.QueueID)
        FanOutDiffToWaiters(job.DatasetURLID, false, "web")
        continue
    }
    log.Printf("[diff] fetch failed for %s: %v — skipping", job.URL, err)
    FanOutDiffToWaiters(job.DatasetURLID, false, "web")
    continue
}

newContent := clean.CleanContentFromHTML(newHTML, includeLinks)
if newContent == "" {
    log.Printf("[diff] incoming content empty after clean for %s — skipping", job.URL)
    markDone(job.QueueID)
    FanOutDiffToWaiters(job.DatasetURLID, false, "web")
    continue
}

			// URL-set diff for link-extraction datasets — bypass L1/L2/L3/L4
			if includeLinks {
				oldURLs := extractURLSet(storedContent)
				newURLs := extractURLSet(newContent)
				if hasNewURLs(oldURLs, newURLs) {
					log.Printf("[diff] include_links — new urls detected for %s — proceeding", job.URL)
					if err := crawl.FetchAndSave(cfg, job.FolderPath, job.URL, includeLinks); err != nil {
						log.Printf("[diff] FetchAndSave failed for %s: %v", job.URL, err)
						FanOutDiffToWaiters(job.DatasetURLID, false, "web")
						continue
					}
					_, err = database.Exec(`
						UPDATE queue SET status = 'proceed-clean', locked_at = NULL WHERE queue_id = ?
					`, job.QueueID)
					if err != nil {
						log.Printf("[diff] markProceedClean queue_id=%d: %v", job.QueueID, err)
					}
					FanOutDiffToWaiters(job.DatasetURLID, true, "web")
					changed++
				} else {
					log.Printf("[diff] include_links — no new urls for %s — skipping", job.URL)
					markDone(job.QueueID)
					FanOutDiffToWaiters(job.DatasetURLID, false, "web")
				}
				continue
			}

			// L1: content hash check
			if storedContent != "" && hashString(newContent) == hashString(storedContent) {
				log.Printf("[diff] L1 — no change for %s", job.URL)
				markDone(job.QueueID)
				FanOutDiffToWaiters(job.DatasetURLID, false, "web")
				continue
			}
			log.Printf("[diff] L1 — content hash mismatch for %s — proceeding to L2", job.URL)

			// L2: bad fetch check
			storedWords := countWords(splitLines(storedContent))
			if storedWords > 0 {
				incomingWords := countWords(splitLines(newContent))
				incomingRatio := float64(incomingWords) / float64(storedWords)
				if incomingRatio < badFetchThreshold {
					log.Printf("[diff] L2 — bad fetch suspected (%.1f%%) for %s — preserving old", incomingRatio*100, job.URL)
					markDone(job.QueueID)
					FanOutDiffToWaiters(job.DatasetURLID, false, "web")
					continue
				}
			}

			// L3: delta size check
			addedLines := computeAdded(storedContent, newContent)
			removedLines := computeRemoved(storedContent, newContent)
			deltaWords := countWords(addedLines) + countWords(removedLines)

			urlChanged := false

			if storedWords > 0 {
				deltaRatio := float64(deltaWords) / float64(storedWords)

				if deltaRatio < autoDropThreshold {
					log.Printf("[diff] L3 — delta too small (%.1f%%) for %s — dropping", deltaRatio*100, job.URL)
					markDone(job.QueueID)
					FanOutDiffToWaiters(job.DatasetURLID, false, "web")
					continue
				}

				if deltaRatio >= autoProceedThreshold {
					log.Printf("[diff] L3 — large delta (%.1f%%) for %s — proceeding", deltaRatio*100, job.URL)
					urlChanged = true
				}
			}

			if !urlChanged {
				// L4: AI gray zone check
				log.Printf("[diff] L4 — gray zone (%d delta words) for %s — asking AI", deltaWords, job.URL)
				extractSchema, err := loadSchema(datasetID)
				if err != nil {
					log.Printf("[diff] L4 load schema failed for %s: %v — proceeding anyway", job.URL, err)
					urlChanged = true
				} else {
					combined := append(
						sampleLines(addedLines, maxAILines/2),
						sampleLines(removedLines, maxAILines/2)...,
					)
					meaningful, err := aiCheckDiff(combined, schemaFieldNames(extractSchema))
					if err != nil {
						log.Printf("[diff] L4 AI check failed for %s: %v — proceeding anyway", job.URL, err)
						urlChanged = true
					} else if !meaningful {
						log.Printf("[diff] L4 — AI: not meaningful for %s — dropping", job.URL)
						markDone(job.QueueID)
						FanOutDiffToWaiters(job.DatasetURLID, false, "web")
						continue
					} else {
						log.Printf("[diff] L4 — AI: meaningful change for %s", job.URL)
						urlChanged = true
					}
				}
			}

			if urlChanged {
				if err := crawl.FetchAndSave(cfg, job.FolderPath, job.URL, includeLinks); err != nil {
					log.Printf("[diff] FetchAndSave failed for %s: %v — skipping", job.URL, err)
					FanOutDiffToWaiters(job.DatasetURLID, false, "web")
					continue
				}
				_, err = database.Exec(`
					UPDATE queue SET status = 'proceed-clean', locked_at = NULL WHERE queue_id = ?
				`, job.QueueID)
				if err != nil {
					log.Printf("[diff] markProceedClean queue_id=%d: %v", job.QueueID, err)
				}
				FanOutDiffToWaiters(job.DatasetURLID, true, "web")
				changed++
			}
		}
	}

	if changed == 0 {
		log.Printf("[diff] no meaningful changes for dataset_id=%d — all done", datasetID)
		return nil
	}

	if schemaChanged {
		if _, err := database.Exec(`
			UPDATE dataset_schema SET schema_changed = 0 WHERE dataset_id = ?
		`, datasetID); err != nil {
			log.Printf("[diff] failed to reset schema_changed for dataset_id=%d: %v", datasetID, err)
		} else {
			log.Printf("[diff] schema_changed reset for dataset_id=%d", datasetID)
		}
	}

	log.Printf("[diff] %d urls changed for dataset_id=%d — queued for worker", changed, datasetID)
	return nil
}

// ---------------------------------------------------------------- db ------

func loadJobs(datasetID int64) ([]diffJob, error) {
	rows, err := db.Get().Query(`
		SELECT q.queue_id, q.dataset_url_id, du.url,
		       COALESCE(du.folder_path, ''),
		       COALESCE(q.diff_cluster_role, 'solo')
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE du.dataset_id = ?
		AND q.status = 'pending-diff'
		AND q.crawl_type = 'nightly-recrawl'
		ORDER BY q.queue_id ASC
	`, datasetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []diffJob
	for rows.Next() {
		var j diffJob
		if err := rows.Scan(&j.QueueID, &j.DatasetURLID, &j.URL, &j.FolderPath, &j.DiffClusterRole); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func checkSchemaChanged(datasetID int64) (bool, error) {
	var schemaChanged bool
	err := db.Get().QueryRow(`
		SELECT schema_changed FROM dataset_schema WHERE dataset_id = ?
	`, datasetID).Scan(&schemaChanged)
	if err != nil {
		return false, fmt.Errorf("query schema_changed: %w", err)
	}
	return schemaChanged, nil
}

func loadSchema(datasetID int64) (map[string]*ai.SchemaField, error) {
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

func markDone(queueID int64) {
	_, err := db.Get().Exec(`
		UPDATE queue SET status = 'done', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[diff] markDone queue_id=%d: %v", queueID, err)
	}
}

func bumpFailCount(queueID int64) {
	_, err := db.Get().Exec(`
		UPDATE queue SET fail_count = 7, status = 'done', locked_at = NULL
		WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[diff] bumpFailCount queue_id=%d: %v", queueID, err)
	}
}

// ---------------------------------------------------------------- storage --

func loadStoredContent(folderPath string) string {
	b, err := storage.Read(filepath.Join(folderPath, "cleaned.json"))
	if err != nil {
		return ""
	}
	var doc cleanedDoc
	if err := json.Unmarshal(b, &doc); err != nil {
		return ""
	}
	return doc.Content
}

// ---------------------------------------------------------------- diff -----

func splitLines(s string) []string {
	var lines []string
	for _, l := range strings.Split(s, "\n") {
		l = strings.TrimSpace(l)
		if l == "" || reOnlySymbols.MatchString(l) {
			continue
		}
		lines = append(lines, l)
	}
	return lines
}

func computeAdded(old, new string) []string {
	oldSet := map[string]bool{}
	for _, l := range splitLines(old) {
		oldSet[l] = true
	}
	var added []string
	for _, l := range splitLines(new) {
		if !oldSet[l] {
			added = append(added, l)
		}
	}
	return added
}

func computeRemoved(old, new string) []string {
	newSet := map[string]bool{}
	for _, l := range splitLines(new) {
		newSet[l] = true
	}
	var removed []string
	for _, l := range splitLines(old) {
		if !newSet[l] {
			removed = append(removed, l)
		}
	}
	return removed
}

func countWords(lines []string) int {
	total := 0
	for _, l := range lines {
		total += len(strings.Fields(l))
	}
	return total
}

func hashString(s string) string {
	h := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", h)
}

func sampleLines(lines []string, max int) []string {
	if len(lines) <= max {
		return lines
	}
	step := len(lines) / max
	sampled := make([]string, 0, max)
	for i := 0; i < len(lines) && len(sampled) < max; i += step {
		sampled = append(sampled, lines[i])
	}
	return sampled
}

// ---------------------------------------------------------------- AI -------

func aiCheckDiff(lines []string, schemaFields []string) (bool, error) {
	if len(lines) > maxAILines {
		lines = lines[:maxAILines]
	}

	truncated := make([]string, 0, len(lines))
	totalLen := 0
	for _, l := range lines {
		if len(l) > maxLineLen {
			l = l[:maxLineLen] + "…"
		}
		totalLen += len(l)
		if totalLen > maxTotalLen {
			break
		}
		truncated = append(truncated, l)
	}

	prompt := fmt.Sprintf(
		"These lines changed on a webpage:\n%s\n\nSchema fields we care about: [%s]\n\nDo any of these changes affect the schema fields? Reply YES or NO only.",
		strings.Join(truncated, "\n"),
		strings.Join(schemaFields, ", "),
	)
	result, err := ai.Ask(prompt)
	if err != nil {
		return true, err
	}
	return strings.TrimSpace(strings.ToUpper(result)) == "YES", nil
}

func schemaFieldNames(schema map[string]*ai.SchemaField) []string {
	var fields []string
	for k := range schema {
		fields = append(fields, k)
	}
	return fields
}

// ---------------------------------------------------------------- cluster --

func BuildDiffClusterMap() error {
	rows, err := db.Get().Query(`
		SELECT q.queue_id, q.dataset_url_id, du.url
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'pending-diff'
		AND q.diff_cluster_role = 'solo'
		ORDER BY q.queue_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query pending-diff: %w", err)
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
			UPDATE queue SET diff_cluster_role = 'primary'
			WHERE queue_id = ?
		`, primary.QueueID)
		if err != nil {
			log.Printf("[diff] mark primary error queue_id=%d: %v", primary.QueueID, err)
			continue
		}

		for _, w := range waiters {
			_, err := db.Get().Exec(`
				UPDATE queue
				SET diff_cluster_role = 'waiting',
				    diff_primary_dataset_url_id = ?
				WHERE queue_id = ?
			`, primary.DatasetURLID, w.QueueID)
			if err != nil {
				log.Printf("[diff] mark waiter error queue_id=%d: %v", w.QueueID, err)
			}
		}

		log.Printf("[diff] cluster built — url=%s primary=%d waiters=%d",
			normalized, primary.DatasetURLID, len(waiters))
	}

	return nil
}

func FanOutDiffToWaiters(primaryDatasetURLID int64, changed bool, datasetType string) {
	rows, err := db.Get().Query(`
		SELECT q.queue_id
		FROM queue q
		WHERE q.diff_primary_dataset_url_id = ?
		AND q.diff_cluster_role = 'waiting'
		AND q.status = 'pending-diff'
	`, primaryDatasetURLID)
	if err != nil {
		log.Printf("[diff] fan out query error: %v", err)
		return
	}
	defer rows.Close()

	var queueIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			continue
		}
		queueIDs = append(queueIDs, id)
	}
	rows.Close()

	if len(queueIDs) == 0 {
		return
	}

	for _, qid := range queueIDs {
		if changed {
			nextStatus := "proceed-clean"
			if datasetType == "amazon" {
				nextStatus = "proceed-format"
			}
			_, _ = db.Get().Exec(`
				UPDATE queue
				SET status = ?,
				    diff_cluster_role = 'solo',
				    diff_primary_dataset_url_id = NULL,
				    locked_at = NULL
				WHERE queue_id = ?
			`, nextStatus, qid)
		} else {
			_, _ = db.Get().Exec(`
				UPDATE queue
				SET status = 'done',
				    diff_cluster_role = 'solo',
				    diff_primary_dataset_url_id = NULL,
				    locked_at = NULL
				WHERE queue_id = ?
			`, qid)
		}
		log.Printf("[diff] fan out waiter queue_id=%d changed=%v type=%s", qid, changed, datasetType)
	}
}

func normalizeURL(u string) string {
	return strings.TrimRight(strings.ToLower(strings.TrimSpace(u)), "/")
}

// ---------------------------------------------------------------- url set diff --

func extractURLSet(content string) map[string]bool {
	urls := map[string]bool{}
	for _, line := range splitLines(content) {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "http://") || strings.HasPrefix(line, "https://") {
			urls[line] = true
		}
	}
	return urls
}

func hasNewURLs(oldURLs, newURLs map[string]bool) bool {
	for u := range newURLs {
		if !oldURLs[u] {
			return true
		}
	}
	return false
}