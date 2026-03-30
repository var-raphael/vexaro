package crawl

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// CrawlURL represents a single URL to crawl with its tracking state.
type CrawlURL struct {
	ID       int64
	URL      string
	Position int
	Status   string
	Retries  int
}

// Config holds runtime config for the crawl module.
type Config struct {
	BrowserlessKey string
}

// ------------------------------------------------------------------ public --

// CrawlFromFile reads URLs from a serp results txt file and crawls each one.
// Used during development before DB wiring is complete.
func CrawlFromFile(cfg Config, apiName, path string) error {
	urls, err := readURLsFromFile(path)
	if err != nil {
		return fmt.Errorf("read urls from file: %w", err)
	}
	if len(urls) == 0 {
		return fmt.Errorf("no urls found in %s", path)
	}

	log.Printf("[crawl] loaded %d urls from %s", len(urls), path)

	for i, u := range urls {
		cu := CrawlURL{ID: int64(i + 1), URL: u, Position: i + 1, Status: "pending"}
		if err := processURL(cfg, apiName, cu); err != nil {
			log.Printf("[crawl] failed %d/%s: %v", cu.Position, cu.URL, err)
			continue
		}
		log.Printf("[crawl] done %d/%s", cu.Position, cu.URL)
	}

	return nil
}

// Crawl is the production entry point called by the build handler.
func Crawl(db *sql.DB, cfg Config, apiName string) error {
	userID := "dev-user" // replaced by real userID when auth is wired

	if err := seedCrawlURLs(db, userID, apiName); err != nil {
		return fmt.Errorf("seed crawl urls: %w", err)
	}

	urls, err := loadPending(db, userID, apiName)
	if err != nil {
		return fmt.Errorf("load pending urls: %w", err)
	}

	if len(urls) == 0 {
		log.Printf("[crawl] no pending urls for %s/%s", userID, apiName)
		return nil
	}

	log.Printf("[crawl] starting %d urls for %s/%s", len(urls), userID, apiName)

	for _, u := range urls {
		if err := processURL(cfg, apiName, u); err != nil {
			log.Printf("[crawl] failed %s: %v", u.URL, err)
			markFailed(db, u.ID, err.Error())
			continue
		}
		markCrawled(db, u.ID)
	}

	return nil
}

// Retry picks up failed URLs under the retry limit.
func Retry(db *sql.DB, cfg Config, apiName string) error {
	userID := "dev-user"

	urls, err := loadFailed(db, userID, apiName)
	if err != nil {
		return fmt.Errorf("load failed urls: %w", err)
	}
	if len(urls) == 0 {
		return nil
	}

	log.Printf("[crawl] retrying %d failed urls for %s/%s", len(urls), userID, apiName)

	for _, u := range urls {
		if err := processURL(cfg, apiName, u); err != nil {
			log.Printf("[crawl] retry failed %s: %v", u.URL, err)
			markFailed(db, u.ID, err.Error())
			continue
		}
		markCrawled(db, u.ID)
	}

	return nil
}

// ---------------------------------------------------------------- pipeline --

// processURL runs the full layer cascade for a single URL.
//
// Flow:
//  1. layer1 — TLS HTTP fetch
//     a. success + content rich (spa score < 3) → extract → save raw.json
//     b. success + SPA shell (score ≥ 3) → layer2 on HTML → layer3 if needed
//     c. fail entirely → layer2 endpoint probe → layer3 if needed
//  2. layer2 — embedded data + API endpoint scan
//     → success → extract → save raw.json
//     → fail → layer3
//  3. layer3 — Browserless full JS render
//     → success → extract → save raw.json
//     → fail → return error (caller marks as failed)
func processURL(cfg Config, apiName string, u CrawlURL) error {
	log.Printf("[crawl] processing %d/%s", u.Position, u.URL)

	// ── Layer 1 ──────────────────────────────────────────────────────────────
	html, err := layer1(u.URL, true)
	if err != nil {
		log.Printf("[crawl] layer1 failed for %s: %v — trying layer2", u.URL, err)
		return tryLayer2ThenLayer3(cfg, apiName, u.URL, "")
	}

	// layer1 succeeded — check SPA score
	score, reasons := spaScore(html)
	log.Printf("[crawl] spa score %d for %s — %v", score, u.URL, reasons)

	if score < scoreThreshold {
		// static page with real content — extract and save
		return extractAndSave(apiName, u.URL, html, "layer1")
	}

	// SPA shell — try layer2 on the HTML first
	log.Printf("[crawl] SPA detected for %s — trying layer2", u.URL)
	result, l2err := layer2(u.URL, html)
	if l2err == nil && result != "" {
		return extractAndSave(apiName, u.URL, result, "layer2")
	}

	// layer2 found nothing — escalate to layer3
	log.Printf("[crawl] layer2 found nothing for %s — escalating to layer3", u.URL)
	return tryLayer3(cfg, apiName, u.URL)
}

// tryLayer2ThenLayer3 is called when layer1 completely fails.
// Probes for API endpoints with no prior HTML, then falls back to layer3.
func tryLayer2ThenLayer3(cfg Config, apiName, rawURL, html string) error {
	result, err := layer2(rawURL, html)
	if err == nil && result != "" {
		return extractAndSave(apiName, rawURL, result, "layer2")
	}
	log.Printf("[crawl] layer2 failed for %s — trying layer3", rawURL)
	return tryLayer3(cfg, apiName, rawURL)
}

// tryLayer3 sends the URL to Browserless for full JS rendering.
func tryLayer3(cfg Config, apiName, rawURL string) error {
	if cfg.BrowserlessKey == "" {
		return fmt.Errorf("layer3 required but no browserless key configured")
	}
	html, err := layer3(rawURL, cfg.BrowserlessKey)
	if err != nil {
		return fmt.Errorf("layer3 failed: %w", err)
	}
	return extractAndSave(apiName, rawURL, html, "layer3")
}

// extractAndSave extracts structured data from HTML/JSON and saves raw.json.
func extractAndSave(apiName, rawURL, html, layer string) error {
	data := extract(rawURL, html, layer)
	return saveRaw(apiName, data)
}

// ------------------------------------------------------------------ db ------

func seedCrawlURLs(db *sql.DB, userID, apiName string) error {
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM crawl_urls
		WHERE user_id = ? AND api_name = ?
	`, userID, apiName).Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	row := db.QueryRow(`
		SELECT id FROM serp_cache
		WHERE user_id = ? AND api_name = ?
		ORDER BY created_at DESC
		LIMIT 1
	`, userID, apiName)

	var cacheID int64
	if err := row.Scan(&cacheID); err != nil {
		return fmt.Errorf("no serp cache found for %s/%s: %w", userID, apiName, err)
	}

	rows, err := db.Query(`
		SELECT url, position FROM serp_urls
		WHERE cache_id = ?
		ORDER BY position ASC
	`, cacheID)
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO crawl_urls (user_id, api_name, url, position)
		VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var u string
		var pos int
		if err := rows.Scan(&u, &pos); err != nil {
			continue
		}
		if _, err := stmt.Exec(userID, apiName, u, pos); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func loadPending(db *sql.DB, userID, apiName string) ([]CrawlURL, error) {
	return loadByStatus(db, userID, apiName, "pending")
}

func loadFailed(db *sql.DB, userID, apiName string) ([]CrawlURL, error) {
	rows, err := db.Query(`
		SELECT id, url, position, status, retries
		FROM crawl_urls
		WHERE user_id = ? AND api_name = ? AND status = 'failed' AND retries < 3
		ORDER BY position ASC
	`, userID, apiName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanURLs(rows)
}

func loadByStatus(db *sql.DB, userID, apiName, status string) ([]CrawlURL, error) {
	rows, err := db.Query(`
		SELECT id, url, position, status, retries
		FROM crawl_urls
		WHERE user_id = ? AND api_name = ? AND status = ?
		ORDER BY position ASC
	`, userID, apiName, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanURLs(rows)
}

func scanURLs(rows *sql.Rows) ([]CrawlURL, error) {
	var out []CrawlURL
	for rows.Next() {
		var u CrawlURL
		if err := rows.Scan(&u.ID, &u.URL, &u.Position, &u.Status, &u.Retries); err != nil {
			continue
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func markCrawled(db *sql.DB, id int64) {
	db.Exec(`
		UPDATE crawl_urls
		SET status = 'crawled', error = NULL, crawled_at = ?
		WHERE id = ?
	`, time.Now().Format("2006-01-02 15:04:05"), id)
}

func markFailed(db *sql.DB, id int64, errMsg string) {
	db.Exec(`
		UPDATE crawl_urls
		SET status = 'failed', error = ?, retries = retries + 1
		WHERE id = ?
	`, errMsg, id)
}

// ---------------------------------------------------------------- file ------

func readURLsFromFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var urls []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "http") {
			urls = append(urls, line)
		}
	}
	return urls, scanner.Err()
}
