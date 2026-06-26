package clean

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/var-raphael/vexaro-engine/db"
	"github.com/var-raphael/vexaro-engine/storage"
)

type RawData struct {
	URL       string            `json:"url"`
	Title     string            `json:"title"`
	Content   string            `json:"content"`
	Metadata  map[string]string `json:"metadata"`
	LayerUsed string            `json:"layer_used"`
	CrawledAt time.Time         `json:"crawled_at"`
}

type CleanedData struct {
	URL         string            `json:"source_url"`
	Title       string            `json:"title"`
	Content     string            `json:"content"`
	WordCount   int               `json:"word_count"`
	Score       float64           `json:"score"`
	Description string            `json:"description"`
	Metadata    map[string]string `json:"metadata"`
	LayerUsed   string            `json:"layer_used"`
	CrawledAt   time.Time         `json:"crawled_at"`
	CleanedAt   time.Time         `json:"cleaned_at"`
}

type flattenedJSON struct {
	Text string
}

var jsonLDTypeLabels = map[string]struct{}{
	"NewsArticle": {}, "Article": {}, "WebPage": {}, "WebSite": {},
	"Organization": {}, "ImageObject": {}, "Person": {}, "PostalAddress": {},
	"ItemList": {}, "ListItem": {}, "BreadcrumbList": {}, "SearchAction": {},
	"Product": {}, "Event": {}, "Review": {}, "Rating": {}, "AggregateRating": {},
	"VideoObject": {}, "AudioObject": {}, "MusicRecording": {}, "Movie": {},
	"TVSeries": {}, "Book": {}, "Recipe": {}, "HowTo": {}, "FAQPage": {},
	"Question": {}, "Answer": {}, "SiteNavigationElement": {}, "WPHeader": {},
	"WPFooter": {}, "WPSideBar": {}, "CreativeWork": {}, "Thing": {},
}

var noiseVocabURLs = []string{
	"https://schema.org",
	"http://schema.org",
	"https://ogp.me",
}

// ------------------------------------------------------------------ public --

func Clean() error {
	database := db.Get()

	rows, err := database.Query(`
		SELECT q.queue_id, du.folder_path, COALESCE(d.include_links, 0)
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE q.status = 'proceed-clean'
		AND du.folder_path IS NOT NULL
		ORDER BY q.queue_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query proceed-clean: %w", err)
	}
	defer rows.Close()

	type cleanJob struct {
		QueueID      int64
		FolderPath   string
		IncludeLinks bool
	}

	var jobs []cleanJob
	for rows.Next() {
		var j cleanJob
		if err := rows.Scan(&j.QueueID, &j.FolderPath, &j.IncludeLinks); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan proceed-clean rows: %w", err)
	}

	if len(jobs) == 0 {
		log.Printf("[clean] no proceed-clean urls in queue")
		return nil
	}

	log.Printf("[clean] processing %d proceed-clean jobs", len(jobs))

	c := New()
	succeeded := 0
	skipped := 0
	failed := 0

	for _, job := range jobs {
		rawPath := filepath.Join(job.FolderPath, "raw.json")

		result, err := cleanFile(c, rawPath, job.IncludeLinks)
		if err != nil {
			log.Printf("[clean] failed queue_id=%d path=%s: %v", job.QueueID, rawPath, err)
			markFailed(database, job.QueueID)
			failed++
			continue
		}
		if result.Skipped {
			log.Printf("[clean] skipped queue_id=%d: %s", job.QueueID, result.SkipReason)
			markFailed(database, job.QueueID)
			skipped++
			continue
		}

		outPath := filepath.Join(job.FolderPath, "cleaned.json")
		if err := writeCleanedJSON(outPath, result.Data); err != nil {
			log.Printf("[clean] write failed queue_id=%d: %v", job.QueueID, err)
			markFailed(database, job.QueueID)
			failed++
			continue
		}

		markProceedFormat(database, job.QueueID)
		log.Printf("[clean] done queue_id=%d → %s (words: %d, score: %.2f)",
			job.QueueID, outPath, result.Data.WordCount, result.Data.Score)
		succeeded++
	}

	log.Printf("[clean] done — %d cleaned, %d skipped, %d failed", succeeded, skipped, failed)
	return nil
}

// ---------------------------------------------------------------- db ------

func markProceedFormat(database *sql.DB, queueID int64) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'proceed-format', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[clean] markProceedFormat queue_id=%d: %v", queueID, err)
	}
}

func markFailed(database *sql.DB, queueID int64) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'failed', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[clean] markFailed queue_id=%d: %v", queueID, err)
	}
}

// ---------------------------------------------------------------- internal --

type fileResult struct {
	Data       *CleanedData
	Skipped    bool
	SkipReason string
}

func cleanFile(c *Cleaner, rawPath string, includeLinks bool) (fileResult, error) {
	b, err := storage.Read(rawPath)
	if err != nil {
		return fileResult{}, fmt.Errorf("load raw: %w", err)
	}

	b = []byte(fixUnicode(string(b)))

	var full struct {
		URL       string            `json:"url"`
		Title     string            `json:"title"`
		Content   string            `json:"content"`
		Raw       string            `json:"raw"`
		Metadata  map[string]string `json:"metadata"`
		LayerUsed string            `json:"layer_used"`
		CrawledAt time.Time         `json:"crawled_at"`
	}
	if err := json.Unmarshal(b, &full); err != nil {
		return fileResult{}, fmt.Errorf("parse json: %w", err)
	}

	text := full.Content

	if full.LayerUsed == "layer2" && text == "" && full.Raw != "" {
		flat := flattenJSONData(full.Raw)
		text = flat.Text
	}

	result := c.CleanMixed(text, includeLinks)
	if result.Skipped {
		return fileResult{Skipped: true, SkipReason: result.SkipReason}, nil
	}

	title := full.Title
	if title == "" && full.LayerUsed == "layer2" {
		title = extractJSONTitle(full.Raw)
	}

	description := ""
	if full.Metadata != nil {
		if v, ok := full.Metadata["og:description"]; ok {
			description = v
		} else if v, ok := full.Metadata["description"]; ok {
			description = v
		}
	}

	data := &CleanedData{
		URL:         full.URL,
		Title:       title,
		Content:     result.Text,
		WordCount:   result.WordCount,
		Score:       result.Score,
		Description: description,
		Metadata:    full.Metadata,
		LayerUsed:   full.LayerUsed,
		CrawledAt:   full.CrawledAt,
		CleanedAt:   time.Now(),
	}

	return fileResult{Data: data}, nil
}

func writeCleanedJSON(path string, data *CleanedData) error {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return storage.Write(path, b)
}

// ---------------------------------------------------------------- json flatten --

func flattenJSONData(raw string) flattenedJSON {
	raw = fixUnicode(raw)

	var obj interface{}
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return flattenedJSON{Text: raw}
	}

	var textParts []string
	collectValues(obj, &textParts)
	return flattenedJSON{Text: strings.Join(textParts, " ")}
}

func collectValues(v interface{}, textParts *[]string) {
	switch val := v.(type) {
	case string:
		val = strings.TrimSpace(val)
		if val == "" {
			return
		}
		if _, isLabel := jsonLDTypeLabels[val]; isLabel {
			return
		}
		if strings.HasPrefix(val, "http://") || strings.HasPrefix(val, "https://") {
			for _, vocab := range noiseVocabURLs {
				if strings.HasPrefix(val, vocab) {
					return
				}
			}
			return // skip all URLs
		}
		if len(val) > 10 {
			*textParts = append(*textParts, val)
		}
	case map[string]interface{}:
		for _, child := range val {
			collectValues(child, textParts)
		}
	case []interface{}:
		for _, child := range val {
			collectValues(child, textParts)
		}
	}
}

func extractJSONTitle(raw string) string {
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return ""
	}
	if graph, ok := obj["@graph"].([]interface{}); ok {
		for _, item := range graph {
			if m, ok := item.(map[string]interface{}); ok {
				if h, ok := m["headline"].(string); ok && h != "" {
					return h
				}
				if n, ok := m["name"].(string); ok && n != "" {
					return n
				}
			}
		}
	}
	if h, ok := obj["headline"].(string); ok && h != "" {
		return h
	}
	if n, ok := obj["name"].(string); ok && n != "" {
		return n
	}
	return ""
}

// ---------------------------------------------------------------- unicode --

func fixUnicode(s string) string {
	quoted := `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
	if !utf8.ValidString(s) || !strings.Contains(s, `\u`) {
		return s
	}
	var decoded string
	if err := json.Unmarshal([]byte(quoted), &decoded); err != nil {
		return s
	}
	return decoded
}

// ---------------------------------------------------------------- helpers --

func CleanDataset(datasetID int64) error {
	database := db.Get()

	rows, err := database.Query(`
		SELECT q.queue_id, du.folder_path, COALESCE(d.include_links, 0)
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE q.status = 'proceed-clean'
		AND du.folder_path IS NOT NULL
		AND du.dataset_id = ?
		ORDER BY q.queue_id ASC
	`, datasetID)
	if err != nil {
		return fmt.Errorf("query proceed-clean: %w", err)
	}
	defer rows.Close()

	type cleanJob struct {
		QueueID      int64
		FolderPath   string
		IncludeLinks bool
	}

	var jobs []cleanJob
	for rows.Next() {
		var j cleanJob
		if err := rows.Scan(&j.QueueID, &j.FolderPath, &j.IncludeLinks); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan proceed-clean rows: %w", err)
	}

	if len(jobs) == 0 {
		return nil
	}

	log.Printf("[clean] dataset_id=%d — processing %d proceed-clean jobs", datasetID, len(jobs))

	c := New()
	for _, job := range jobs {
		rawPath := filepath.Join(job.FolderPath, "raw.json")

		result, err := cleanFile(c, rawPath, job.IncludeLinks)
		if err != nil {
			log.Printf("[clean] failed queue_id=%d path=%s: %v", job.QueueID, rawPath, err)
			markFailed(database, job.QueueID)
			continue
		}
		if result.Skipped {
			log.Printf("[clean] skipped queue_id=%d: %s", job.QueueID, result.SkipReason)
			markFailed(database, job.QueueID)
			continue
		}

		outPath := filepath.Join(job.FolderPath, "cleaned.json")
		if err := writeCleanedJSON(outPath, result.Data); err != nil {
			log.Printf("[clean] write failed queue_id=%d: %v", job.QueueID, err)
			markFailed(database, job.QueueID)
			continue
		}

		markProceedFormat(database, job.QueueID)
		log.Printf("[clean] done queue_id=%d → %s", job.QueueID, outPath)
	}

	return nil
}

// CleanContentFromHTML runs raw HTML through the clean pipeline in memory.
// Returns the cleaned content string. No disk writes. Used by diff for
// in-memory comparison before deciding whether to persist anything.
func CleanContentFromHTML(html string, includeLinks bool) string {
	c := New()
	result := c.CleanMixed(html, includeLinks)
	if result.Skipped {
		return ""
	}
	return result.Text
}