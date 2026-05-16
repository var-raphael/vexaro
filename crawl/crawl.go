package crawl

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
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

var knownStaticDomains = map[string]bool{
	"en.wikipedia.org":     true,
	"wikipedia.org":        true,
	"simple.wikipedia.org": true,
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

		for _, u := range urls {
			wg.Add(1)

			go func(u CrawlURL) {
				defer wg.Done()

				domain := extractDomain(u.URL)

				time.Sleep(time.Duration(rand.Intn(jitterMax)) * time.Millisecond)

				gate.acquire(domain)

				sem <- struct{}{}
				defer func() {
					<-sem
					gate.release(domain)
				}()

				markCrawling(database, u.QueueID)

				if err := processURL(cfg, apiName, u); err != nil {
					log.Printf("[crawl] failed queue_id=%d url=%s: %v", u.QueueID, u.URL, err)
					markFailed(database, u.QueueID, err.Error())
					return
				}

				markProceedClean(database, u.QueueID)
				log.Printf("[crawl] done — queue_id=%d url=%s type=%s", u.QueueID, u.URL, u.URLType)
			}(u)
		}

		wg.Wait()
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

	for _, u := range urls {
		wg.Add(1)

		go func(u CrawlURL) {
			defer wg.Done()

			domain := extractDomain(u.URL)

			time.Sleep(time.Duration(rand.Intn(jitterMax)) * time.Millisecond)

			gate.acquire(domain)

			sem <- struct{}{}
			defer func() {
				<-sem
				gate.release(domain)
			}()

			markCrawling(database, u.QueueID)

			if err := processURL(cfg, apiName, u); err != nil {
				log.Printf("[crawl] retry failed queue_id=%d url=%s: %v", u.QueueID, u.URL, err)
				markFailed(database, u.QueueID, err.Error())
				return
			}

			markProceedClean(database, u.QueueID)
			log.Printf("[crawl] retry done — queue_id=%d url=%s", u.QueueID, u.URL)
		}(u)
	}

	wg.Wait()
	log.Printf("[crawl] all retries processed")
	return nil
}

// ---------------------------------------------------------------- pipeline --

func processURL(cfg Config, apiName string, u CrawlURL) error {
	log.Printf("[crawl] processing %d/%s (type=%s crawl_type=%s)", u.Position, u.URL, u.URLType, u.CrawlType)

	domain := extractDomain(u.URL)

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
		log.Printf("[crawl] layer3 (Steel) failed for %s — trying layer4 (Browserless): %v", u.URL, err)
		return tryLayer4(cfg, apiName, u)
	}
	return extractAndSave(apiName, u, html, "layer3")
}

func tryLayer4(cfg Config, apiName string, u CrawlURL) error {
	if cfg.BrowserlessKey == "" {
		return fmt.Errorf("layer4 required but no browserless key configured")
	}
	html, err := layer4(u.URL, cfg.BrowserlessKey)
	if err != nil {
		return fmt.Errorf("layer4 failed: %w", err)
	}
	return extractAndSave(apiName, u, html, "layer4")
}

func extractAndSave(apiName string, u CrawlURL, html, layer string) error {
	if !isUsableHTML(html) {
		return fmt.Errorf("unusable html: binary or too short (len=%d)", len(html))
	}
	data := extract(u.URL, html, layer)
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
			d.data_name
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
		if err := rows.Scan(
			&u.QueueID,
			&u.DatasetURLID,
			&crawlType,
			&u.URL,
			&urlType,
			&u.UserID,
			&u.DatasetID,
			&u.ApiName,
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
	html, err := layer1(u, true)
	if err != nil {
		result, err := layer2(u, "")
		if err == nil && result != "" {
			return result, nil
		}
		html, err = layer3(u)
		if err == nil && html != "" {
			return html, nil
		}
		if cfg.BrowserlessKey == "" {
			return "", fmt.Errorf("layer1/2/3 failed and no browserless key")
		}
		return layer4(u, cfg.BrowserlessKey)
	}

	domain := extractDomain(u)
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
			if cfg.BrowserlessKey == "" {
				return "", fmt.Errorf("confirmed SPA but no browserless key")
			}
			return layer4(u, cfg.BrowserlessKey)
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
		if cfg.BrowserlessKey == "" {
			return "", fmt.Errorf("SPA detected but no browserless key")
		}
		return layer4(u, cfg.BrowserlessKey)
	}

	return html, nil
}