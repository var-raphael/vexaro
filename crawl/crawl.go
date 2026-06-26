package crawl

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/joho/godotenv"
	"github.com/var-raphael/vexaro-engine/db"
	"os"
)

const (
	workers   = 3
	jitterMax = 500
)

var ErrNeedsLayer5 = errors.New("needs layer5")

var knownStaticDomains = map[string]bool{
	"en.wikipedia.org":     true,
	"wikipedia.org":        true,
	"simple.wikipedia.org": true,
}

var blockedDomains = map[string]bool{
	"twitter.com":     true,
	"x.com":           true,
	"facebook.com":    true,
	"instagram.com":   true,
	"tiktok.com":      true,
	"pinterest.com":   true,
	"snapchat.com":    true,
	"reddit.com":      true,
	"old.reddit.com":  true,
	"threads.net":     true,
	"whatsapp.com":    true,
	"telegram.org":    true,
	"discord.com":     true,
	"twitch.tv":       true,
	"tumblr.com":      true,
	"substack.com":    true,
}

var crawlGroqKey = ""

func init() {
	if err := godotenv.Load(); err != nil {
		log.Printf("[crawl] no .env file found, relying on shell environment")
	}
	crawlGroqKey = os.Getenv("SERP_GROQ_KEYS")
}

// ------------------------------------------------------------------ types --

type CrawlURL struct {
	QueueID      int64
	DatasetURLID int64
	DatasetID    int64
	URL          string
	URLType      string
	Position     int
	Status       string
	UserID       string
	ApiName      string
	CrawlType    string
	IncludeLinks bool
}

type Config struct {
	BrowserlessKey string
}

// ------------------------------------------------------------------ domain gate --

type domainGate struct {
	mu    sync.Mutex
	gates map[string]chan struct{}
}

func newDomainGate() *domainGate {
	return &domainGate{gates: map[string]chan struct{}{}}
}

func (dg *domainGate) acquire(domain string) {
	dg.mu.Lock()
	ch, ok := dg.gates[domain]
	if !ok {
		ch = make(chan struct{}, 1)
		dg.gates[domain] = ch
	}
	dg.mu.Unlock()
	ch <- struct{}{}
}

func (dg *domainGate) release(domain string) {
	dg.mu.Lock()
	ch := dg.gates[domain]
	dg.mu.Unlock()
	<-ch
}

// ------------------------------------------------------------------ public --

func Crawl(cfg Config, apiName string) error {
	database := db.Get()
	round := 1

	for {
		urls, err := loadPending(database)
		if err != nil {
			return fmt.Errorf("load pending urls (round %d): %w", round, err)
		}
		if len(urls) == 0 {
			log.Printf("[crawl] no pending urls — done after %d round(s)", round-1)
			return nil
		}

		log.Printf("[crawl] round %d — starting %d urls (%d workers)", round, len(urls), workers)

		sem := make(chan struct{}, workers)
		gate := newDomainGate()
		var wg sync.WaitGroup

		var layer5Mu sync.Mutex
		var layer5Queue []CrawlURL

		for _, u := range urls {
			wg.Add(1)

			go func(u CrawlURL) {
				defer wg.Done()

				// Reject bad file types before doing anything
				if IsRejectedURL(u.URL) {
					log.Printf("[crawl] rejected url type — skipping url=%s", u.URL)
					markFailed(database, u.QueueID, "rejected file type")
					return
				}

				domain := extractDomain(u.URL)
				time.Sleep(time.Duration(rand.Intn(jitterMax)) * time.Millisecond)
				gate.acquire(domain)
				sem <- struct{}{}
				defer func() {
					<-sem
					gate.release(domain)
				}()

				markCrawling(database, u.QueueID)

				err := processURL(cfg, apiName, u)
				if errors.Is(err, ErrNeedsLayer5) {
					layer5Mu.Lock()
					layer5Queue = append(layer5Queue, u)
					layer5Mu.Unlock()
					return
				}
				if err != nil {
					log.Printf("[crawl] failed queue_id=%d url=%s: %v", u.QueueID, u.URL, err)
					markFailed(database, u.QueueID, err.Error())
					return
				}

				markProceedClean(database, u.QueueID)
				log.Printf("[crawl] done — queue_id=%d url=%s type=%s", u.QueueID, u.URL, u.URLType)
			}(u)
		}

		wg.Wait()

		if len(layer5Queue) > 0 {
			log.Printf("[crawl] round %d — sending %d urls to layer5 (Apify)", round, len(layer5Queue))
			tryLayer5Bulk(apiName, layer5Queue)
		}

		log.Printf("[crawl] round %d complete", round)
		round++
	}
}

func Retry(cfg Config, apiName string) error {
	database := db.Get()

	urls, err := loadFailed(database)
	if err != nil {
		return fmt.Errorf("load failed urls: %w", err)
	}
	if len(urls) == 0 {
		return nil
	}

	log.Printf("[crawl] retrying %d failed urls (%d workers)", len(urls), workers)

	sem := make(chan struct{}, workers)
	gate := newDomainGate()
	var wg sync.WaitGroup

	var layer5Mu sync.Mutex
	var layer5Queue []CrawlURL

	for _, u := range urls {
		wg.Add(1)

		go func(u CrawlURL) {
			defer wg.Done()

			if IsRejectedURL(u.URL) {
				log.Printf("[crawl] rejected url type on retry — skipping url=%s", u.URL)
				markFailed(database, u.QueueID, "rejected file type")
				return
			}

			domain := extractDomain(u.URL)
			time.Sleep(time.Duration(rand.Intn(jitterMax)) * time.Millisecond)
			gate.acquire(domain)
			sem <- struct{}{}
			defer func() {
				<-sem
				gate.release(domain)
			}()

			markCrawling(database, u.QueueID)

			err := processURL(cfg, apiName, u)
			if errors.Is(err, ErrNeedsLayer5) {
				layer5Mu.Lock()
				layer5Queue = append(layer5Queue, u)
				layer5Mu.Unlock()
				return
			}
			if err != nil {
				log.Printf("[crawl] retry failed queue_id=%d url=%s: %v", u.QueueID, u.URL, err)
				markFailed(database, u.QueueID, err.Error())
				return
			}

			markProceedClean(database, u.QueueID)
			log.Printf("[crawl] retry done — queue_id=%d url=%s", u.QueueID, u.URL)
		}(u)
	}

	wg.Wait()

	if len(layer5Queue) > 0 {
		log.Printf("[crawl] retry — sending %d urls to layer5 (Apify)", len(layer5Queue))
		tryLayer5Bulk(apiName, layer5Queue)
	}

	log.Printf("[crawl] all retries processed")
	return nil
}

// ---------------------------------------------------------------- pipeline --

func processURL(cfg Config, apiName string, u CrawlURL) error {
	domain := extractDomain(u.URL)

	if strings.Contains(domain, "amazon.") {
		log.Printf("[crawl] amazon url detected — routing to API url=%s", u.URL)
		return processAmazonURL(u)
	}

	if blockedDomains[domain] {
		return fmt.Errorf("domain %s is blocked — skipping", domain)
	}

	log.Printf("[crawl] processing %d/%s (type=%s crawl_type=%s)", u.Position, u.URL, u.URLType, u.CrawlType)

	html, err := layer1(u.URL, true)
	if err != nil {
		log.Printf("[crawl] layer1 failed for %s: %v — trying layer2", u.URL, err)
		return tryLayer2ThenLayer3(cfg, apiName, u, "")
	}

	if knownStaticDomains[domain] {
		log.Printf("[crawl] static domain — skipping SPA detection for %s", u.URL)
		return extractAndSave(apiName, u, html, "layer1")
	}

	doc, docErr := goquery.NewDocumentFromReader(strings.NewReader(html))
	if docErr == nil {
		if confirmed, reason := isConfirmedSPA(html, doc); confirmed {
			log.Printf("[crawl] confirmed SPA for %s — %s — trying layer2", u.URL, reason)
			result, l2err := layer2(u.URL, html)
			if l2err == nil && result != "" {
				return extractAndSave(apiName, u, result, "layer2")
			}
			log.Printf("[crawl] layer2 found nothing for %s — escalating to layer3", u.URL)
			return tryLayer3(cfg, apiName, u)
		}
	}

	score, reasons := spaScore(html)
	log.Printf("[crawl] spa score %d for %s — %v", score, u.URL, reasons)

	if score >= scoreThreshold && hasAnchorSignal(reasons) {
		log.Printf("[crawl] SPA detected for %s — trying layer2", u.URL)
		result, l2err := layer2(u.URL, html)
		if l2err == nil && result != "" {
			return extractAndSave(apiName, u, result, "layer2")
		}
		log.Printf("[crawl] layer2 found nothing for %s — escalating to layer3", u.URL)
		return tryLayer3(cfg, apiName, u)
	}

	return extractAndSave(apiName, u, html, "layer1")
}

func tryLayer2ThenLayer3(cfg Config, apiName string, u CrawlURL, html string) error {
	result, err := layer2(u.URL, html)
	if err == nil && result != "" {
		return extractAndSave(apiName, u, result, "layer2")
	}
	log.Printf("[crawl] layer2 failed for %s — trying layer3 (Steel)", u.URL)
	return tryLayer3(cfg, apiName, u)
}

func tryLayer3(cfg Config, apiName string, u CrawlURL) error {
	html, err := layer3(u.URL)
	if err != nil {
		log.Printf("[crawl] layer3 failed for %s — trying layer4 (Browserless): %v", u.URL, err)
		return tryLayer4(cfg, apiName, u)
	}
	return extractAndSave(apiName, u, html, "layer3")
}

func tryLayer4(cfg Config, apiName string, u CrawlURL) error {
	if cfg.BrowserlessKey != "" {
		html, err := layer4(u.URL, cfg.BrowserlessKey)
		if err == nil {
			return extractAndSave(apiName, u, html, "layer4")
		}
		log.Printf("[crawl] layer4 failed for %s — queuing for layer5 (Apify): %v", u.URL, err)
	} else {
		log.Printf("[crawl] no browserless key — queuing %s for layer5 (Apify)", u.URL)
	}
	return ErrNeedsLayer5
}

func extractAndSave(apiName string, u CrawlURL, html, layer string) error {
	if !isUsableHTML(html) {
		return fmt.Errorf("unusable html: binary or too short (len=%d)", len(html))
	}
	data := extract(u.URL, html, layer, u.IncludeLinks)
	return saveRaw(data, u.DatasetURLID, u.UserID, u.ApiName, u.DatasetID)
}

// ---------------------------------------------------------------- helpers --

func extractDomain(rawURL string) string {
	rawURL = strings.TrimPrefix(rawURL, "https://")
	rawURL = strings.TrimPrefix(rawURL, "http://")
	rawURL = strings.TrimPrefix(rawURL, "www.")
	return strings.Split(rawURL, "/")[0]
}

// ------------------------------------------------------------------ db ------

func loadPending(database *sql.DB) ([]CrawlURL, error) {
	return loadByStatus(database, "pending")
}

func loadFailed(database *sql.DB) ([]CrawlURL, error) {
	return loadByStatus(database, "failed")
}

func loadByStatus(database *sql.DB, status string) ([]CrawlURL, error) {
	rows, err := database.Query(`
		SELECT
			q.queue_id,
			q.dataset_url_id,
			q.crawl_type,
			du.url,
			du.url_type,
			d.user_id,
			d.dataset_id,
			d.data_name,
			d.include_links
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE q.status = ?
		ORDER BY q.queue_id ASC
	`, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CrawlURL
	pos := 1

	for rows.Next() {
		var u CrawlURL
		var crawlType, urlType sql.NullString
		var includeLinks bool
		if err := rows.Scan(
			&u.QueueID,
			&u.DatasetURLID,
			&crawlType,
			&u.URL,
			&urlType,
			&u.UserID,
			&u.DatasetID,
			&u.ApiName,
			&includeLinks,
		); err != nil {
			continue
		}
		u.Position = pos
		u.Status = status
		u.CrawlType = crawlType.String
		if u.CrawlType == "" {
			u.CrawlType = "fresh"
		}
		u.URLType = urlType.String
		if u.URLType == "" {
			u.URLType = "extraction"
		}
		u.IncludeLinks = includeLinks
		pos++
		out = append(out, u)
	}

	return out, rows.Err()
}

func markCrawling(database *sql.DB, queueID int64) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'crawling', locked_at = ? WHERE queue_id = ?
	`, time.Now().Format("2006-01-02 15:04:05"), queueID)
	if err != nil {
		log.Printf("[crawl] markCrawling queue_id=%d: %v", queueID, err)
	}
}

func markProceedClean(database *sql.DB, queueID int64) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'proceed-clean', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[crawl] markProceedClean queue_id=%d: %v", queueID, err)
	}
}

func markFailed(database *sql.DB, queueID int64, errMsg string) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'failed', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[crawl] markFailed queue_id=%d: %v — original error: %s", queueID, err, errMsg)
	}
}

// ---------------------------------------------------------------- fetch raw --

func FetchRaw(cfg Config, u string) (string, error) {
	domain := extractDomain(u)

	if blockedDomains[domain] {
		return "", fmt.Errorf("domain %s is blocked", domain)
	}

	html, err := layer1(u, true)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return "", ErrNotFound
		}
		result, err := layer2(u, "")
		if err == nil && result != "" {
			return result, nil
		}
		html, err = layer3(u)
		if err == nil && html != "" {
			return html, nil
		}
		if cfg.BrowserlessKey != "" {
			html, err = layer4(u, cfg.BrowserlessKey)
			if err == nil && html != "" {
				return html, nil
			}
		}
		return layer5Single(u)
	}

	if knownStaticDomains[domain] {
		return html, nil
	}

	doc, docErr := goquery.NewDocumentFromReader(strings.NewReader(html))
	if docErr == nil {
		if confirmed, _ := isConfirmedSPA(html, doc); confirmed {
			result, err := layer2(u, html)
			if err == nil && result != "" {
				return result, nil
			}
			html, err = layer3(u)
			if err == nil && html != "" {
				return html, nil
			}
			if cfg.BrowserlessKey != "" {
				html, err = layer4(u, cfg.BrowserlessKey)
				if err == nil && html != "" {
					return html, nil
				}
			}
			return layer5Single(u)
		}
	}

	score, reasons := spaScore(html)
	if score >= scoreThreshold && hasAnchorSignal(reasons) {
		result, err := layer2(u, html)
		if err == nil && result != "" {
			return result, nil
		}
		html, err = layer3(u)
		if err == nil && html != "" {
			return html, nil
		}
		if cfg.BrowserlessKey != "" {
			html, err = layer4(u, cfg.BrowserlessKey)
			if err == nil && html != "" {
				return html, nil
			}
		}
		return layer5Single(u)
	}

	return html, nil
}

func CrawlSingle(cfg Config, queueID int64, datasetURLID int64, url string, crawlType string) error {
	var userID, dataName string
	var datasetID int64
	var includeLinks bool

	err := db.Get().QueryRow(`
		SELECT d.user_id, d.data_name, d.dataset_id, d.include_links
		FROM datasets_url du
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE du.dataset_url_id = ?
	`, datasetURLID).Scan(&userID, &dataName, &datasetID, &includeLinks)
	if err != nil {
		return fmt.Errorf("load dataset context: %w", err)
	}

	if IsRejectedURL(url) {
		return fmt.Errorf("rejected file type: %s", url)
	}

	u := CrawlURL{
		QueueID:      queueID,
		DatasetURLID: datasetURLID,
		DatasetID:    datasetID,
		URL:          url,
		URLType:      "extraction",
		CrawlType:    crawlType,
		UserID:       userID,
		ApiName:      dataName,
		IncludeLinks: includeLinks,
	}
	if u.CrawlType == "" {
		u.CrawlType = "fresh"
	}

	err = processURL(cfg, dataName, u)
	if errors.Is(err, ErrNeedsLayer5) {
		log.Printf("[crawl] CrawlSingle — queuing single url for layer5: %s", url)
		tryLayer5Bulk(dataName, []CrawlURL{u})
		return nil
	}
	if err != nil {
		return fmt.Errorf("process url: %w", err)
	}
	return nil
}

func processAmazonURL(u CrawlURL) error {
	apiKey := os.Getenv("AMAZON_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("AMAZON_API_KEY not set")
	}

	var marketplace string
	err := db.Get().QueryRow(`
		SELECT COALESCE(marketplace, 'com') FROM datasets WHERE dataset_id = ?
	`, u.DatasetID).Scan(&marketplace)
	if err != nil {
		return fmt.Errorf("load marketplace: %w", err)
	}

	parts := strings.Split(u.URL, "/dp/")
	if len(parts) < 2 {
		return fmt.Errorf("cannot extract ASIN from url: %s", u.URL)
	}
	asin := strings.Split(parts[1], "/")[0]
	asin = strings.TrimSpace(asin)
	if asin == "" {
		return fmt.Errorf("empty ASIN extracted from url: %s", u.URL)
	}

	apiURL := fmt.Sprintf(
		"https://api.amazonscraperapi.com/api/v1/amazon/product?query=%s&domain=%s&api_key=%s",
		asin, marketplace, apiKey,
	)

	log.Printf("[crawl/amazon] calling API asin=%s marketplace=%s", asin, marketplace)

	resp, err := http.Get(apiURL)
	if err != nil {
		return fmt.Errorf("amazon api request failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode == 404 {
		return fmt.Errorf("amazon ASIN not found: %s", asin)
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("amazon api returned status %d for asin=%s", resp.StatusCode, asin)
	}

	var apiResponse map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &apiResponse); err != nil {
		return fmt.Errorf("decode amazon api response: %w", err)
	}

	data := &ScrapedData{
		URL:       u.URL,
		Title:     fmt.Sprintf("%v", apiResponse["title"]),
		LayerUsed: "amazon-api",
		Metadata:  map[string]string{"asin": asin, "marketplace": marketplace},
		CrawledAt: time.Now(),
	}

	rawBytes, err := json.Marshal(apiResponse)
	if err != nil {
		return fmt.Errorf("marshal amazon response: %w", err)
	}
	data.Raw = string(rawBytes)

	if err := saveRaw(data, u.DatasetURLID, u.UserID, u.ApiName, u.DatasetID); err != nil {
		return fmt.Errorf("save amazon raw: %w", err)
	}

	_, err = db.Get().Exec(`
		UPDATE queue SET status = 'proceed-format' WHERE queue_id = ?
	`, u.QueueID)
	if err != nil {
		return fmt.Errorf("update amazon queue status: %w", err)
	}

	log.Printf("[crawl/amazon] done — asin=%s queue_id=%d", asin, u.QueueID)
	return nil
}