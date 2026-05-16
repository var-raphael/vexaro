package clean

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/var-raphael/vexaro-engine/db"
)

// RawData mirrors the fields we care about from raw.json
type RawData struct {
	URL       string            `json:"url"`
	Title     string            `json:"title"`
	Content   string            `json:"content"`
	Metadata  map[string]string `json:"metadata"`
	LayerUsed string            `json:"layer_used"`
	CrawledAt time.Time         `json:"crawled_at"`
}

// CleanedData is what gets written to cleaned.json
type CleanedData struct {
	URL         string            `json:"source_url"`
	Title       string            `json:"title"`
	Content     string            `json:"content"`
	WordCount   int               `json:"word_count"`
	Score       float64           `json:"score"`
	Description string            `json:"description"`
	Links       []string          `json:"links,omitempty"`
	Images      []string          `json:"images,omitempty"`
	Downloads   []string          `json:"downloads,omitempty"`
	Metadata    map[string]string `json:"metadata"`
	LayerUsed   string            `json:"layer_used"`
	CrawledAt   time.Time         `json:"crawled_at"`
	CleanedAt   time.Time         `json:"cleaned_at"`
}

// flattenedJSON holds the result of walking a JSON-LD tree.
type flattenedJSON struct {
	Text      string
	Links     []string
	Images    []string
	Downloads []string
}

// mediaExtensions — file types worth preserving as downloads
var mediaExtensions = []string{
	".mp3", ".mp4", ".m4a", ".wav", ".ogg", ".flac",
	".mov", ".avi", ".mkv", ".webm",
	".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv",
	".zip", ".tar", ".gz",
}

// imageExtensions — image file types
var imageExtensions = []string{
	".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".avif",
}

// jsonLDTypeLabels — structural JSON-LD type strings that leak into content
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

// noiseVocabURLs — JSON-LD vocabulary base URLs that are structural, not real links
var noiseVocabURLs = []string{
	"https://schema.org",
	"http://schema.org",
	"https://ogp.me",
}

// ------------------------------------------------------------------ public --

// Clean is the DB-driven entry point — picks up proceed-clean rows from the
// queue, cleans each file, writes cleaned.json, and advances status to proceed-extract.
func Clean() error {
	database := db.Get()

	rows, err := database.Query(`
		SELECT q.queue_id, du.folder_path
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-clean'
		AND du.folder_path IS NOT NULL
		ORDER BY q.queue_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query proceed-clean: %w", err)
	}
	defer rows.Close()

	type cleanJob struct {
		QueueID    int64
		FolderPath string
	}

	var jobs []cleanJob
	for rows.Next() {
		var j cleanJob
		if err := rows.Scan(&j.QueueID, &j.FolderPath); err != nil {
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

		result, err := cleanFile(c, rawPath)
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

// CleanFromPaths reads paths.txt, skips any already in clean.txt,
// cleans the rest, and appends their paths to clean.txt.
// Kept for dev/debug use.
func CleanFromPaths(pathsFile string) error {
	paths, err := readPaths(pathsFile)
	if err != nil {
		return fmt.Errorf("read paths file: %w", err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("no paths found in %s", pathsFile)
	}

	cleaned, err := loadCleanedPaths("clean.txt")
	if err != nil {
		return fmt.Errorf("read clean.txt: %w", err)
	}

	log.Printf("[clean] loaded %d paths, %d already cleaned", len(paths), len(cleaned))

	c := New()
	succeeded := 0
	skipped := 0
	alreadyDone := 0
	failed := 0

	for _, rawPath := range paths {
		if cleaned[rawPath] {
			alreadyDone++
			continue
		}

		result, err := cleanFile(c, rawPath)
		if err != nil {
			log.Printf("[clean] failed %s: %v", rawPath, err)
			failed++
			continue
		}
		if result.Skipped {
			log.Printf("[clean] skipped %s: %s", rawPath, result.SkipReason)
			skipped++
			continue
		}

		outPath := filepath.Join(filepath.Dir(rawPath), "cleaned.json")
		if err := writeCleanedJSON(outPath, result.Data); err != nil {
			log.Printf("[clean] write failed %s: %v", outPath, err)
			failed++
			continue
		}

		if err := appendCleanedPath("clean.txt", rawPath); err != nil {
			log.Printf("[clean] warning: could not write to clean.txt: %v", err)
		}

		log.Printf("[clean] saved → %s (words: %d, score: %.2f)", outPath, result.Data.WordCount, result.Data.Score)
		succeeded++
	}

	log.Printf("[clean] done — %d saved, %d skipped, %d already done, %d failed", succeeded, skipped, alreadyDone, failed)
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

func cleanFile(c *Cleaner, rawPath string) (fileResult, error) {
	b, err := os.ReadFile(rawPath)
	if err != nil {
		return fileResult{}, fmt.Errorf("load raw: %w", err)
	}

	b = []byte(fixUnicode(string(b)))

	var full struct {
		URL       string            `json:"url"`
		Title     string            `json:"title"`
		Content   string            `json:"content"`
		Raw       string            `json:"raw"`
		Links     []string          `json:"links"`
		Images    []string          `json:"images"`
		Documents []string          `json:"documents"`
		Metadata  map[string]string `json:"metadata"`
		LayerUsed string            `json:"layer_used"`
		CrawledAt time.Time         `json:"crawled_at"`
	}
	if err := json.Unmarshal(b, &full); err != nil {
		return fileResult{}, fmt.Errorf("parse json: %w", err)
	}

	text := full.Content
	var extraLinks, extraImages, extraDownloads []string

	if full.LayerUsed == "layer2" && text == "" && full.Raw != "" {
		flat := flattenJSONData(full.Raw)
		text = flat.Text
		extraLinks = flat.Links
		extraImages = flat.Images
		extraDownloads = flat.Downloads
	}

	result := c.CleanMixed(text)
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

	links := mergeUnique(full.Links, extraLinks)
	images := mergeUnique(full.Images, extraImages)
	downloads := mergeUnique(full.Documents, extraDownloads)

	data := &CleanedData{
		URL:         full.URL,
		Title:       title,
		Content:     result.Text,
		WordCount:   result.WordCount,
		Score:       result.Score,
		Description: description,
		Links:       links,
		Images:      images,
		Downloads:   downloads,
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
	return os.WriteFile(path, b, 0644)
}

// ---------------------------------------------------------------- json flatten --

func flattenJSONData(raw string) flattenedJSON {
	raw = fixUnicode(raw)

	var obj interface{}
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return flattenedJSON{Text: raw}
	}

	var textParts []string
	var result flattenedJSON
	collectValues(obj, &textParts, &result)
	result.Text = strings.Join(textParts, " ")
	return result
}

func collectValues(v interface{}, textParts *[]string, result *flattenedJSON) {
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
			classifyURL(val, result)
			return
		}
		if len(val) > 10 {
			*textParts = append(*textParts, val)
		}
	case map[string]interface{}:
		for _, child := range val {
			collectValues(child, textParts, result)
		}
	case []interface{}:
		for _, child := range val {
			collectValues(child, textParts, result)
		}
	}
}

func classifyURL(u string, result *flattenedJSON) {
	for _, vocab := range noiseVocabURLs {
		if strings.HasPrefix(u, vocab) {
			return
		}
	}
	if idx := strings.Index(u, "#"); idx != -1 {
		path := u[:idx]
		parsed := strings.TrimRight(path, "/")
		parts := strings.Split(parsed, "/")
		if len(parts) <= 3 {
			return
		}
	}

	lower := strings.ToLower(u)
	for _, ext := range mediaExtensions {
		if strings.Contains(lower, ext) {
			result.Downloads = append(result.Downloads, u)
			return
		}
	}
	for _, ext := range imageExtensions {
		if strings.Contains(lower, ext) {
			result.Images = append(result.Images, u)
			return
		}
	}
	result.Links = append(result.Links, u)
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

func mergeUnique(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	var out []string
	for _, s := range append(a, b...) {
		if _, ok := seen[s]; !ok && s != "" {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func loadCleanedPaths(cleanFile string) (map[string]bool, error) {
	cleaned := map[string]bool{}
	f, err := os.Open(cleanFile)
	if os.IsNotExist(err) {
		return cleaned, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			cleaned[line] = true
		}
	}
	return cleaned, scanner.Err()
}

func appendCleanedPath(cleanFile, rawPath string) error {
	f, err := os.OpenFile(cleanFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(rawPath + "\n")
	return err
}

func readPaths(pathsFile string) ([]string, error) {
	f, err := os.Open(pathsFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var paths []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			paths = append(paths, line)
		}
	}
	return paths, scanner.Err()
}

// CleanRaw cleans a raw HTML string directly and saves cleaned.json.
// Returns the path to cleaned.json. Used by diff module.
func CleanRaw(apiName, rawURL, html string) (string, error) {
	c := New()

	result := c.CleanMixed(html)
	if result.Skipped {
		return "", fmt.Errorf("skipped: %s", result.SkipReason)
	}

	folder := buildFolder(apiName, rawURL)
	outPath := filepath.Join(folder, "cleaned.json")

	data := &CleanedData{
		URL:       rawURL,
		Content:   result.Text,
		WordCount: result.WordCount,
		Score:     result.Score,
		CleanedAt: time.Now(),
	}

	if err := os.MkdirAll(folder, 0755); err != nil {
		return "", fmt.Errorf("mkdir: %w", err)
	}
	if err := writeCleanedJSON(outPath, data); err != nil {
		return "", err
	}

	return outPath, nil
}

func buildFolder(apiName, rawURL string) string {
	domain := extractDomain(rawURL)
	return filepath.Join("data", apiName, domain, "rescrape")
}

func extractDomain(u string) string {
	u = strings.TrimPrefix(u, "https://")
	u = strings.TrimPrefix(u, "http://")
	u = strings.TrimPrefix(u, "www.")
	return strings.Split(u, "/")[0]
}
