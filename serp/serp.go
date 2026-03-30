package serp

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/andybalholm/brotli"

	// Chrome TLS fingerprinting — drop-in replacement for net/http
	fhttp "github.com/Danny-Dasilva/fhttp"
	"github.com/Danny-Dasilva/fhttp/http2"
	"github.com/PuerkitoBio/goquery"
	utls "github.com/refraction-networking/utls"
)

// ------------------------------------------------------------------ config --

// Proxies — populate with your Webshare proxies when needed
// Format: "http://user:pass@host:port"
var proxies = []string{
	// "http://user:pass@proxy1.webshare.io:port",
}

var proxyCounter uint64

// Chrome 120 TLS cipher suite — matches JA3 fingerprint
var chrome120Ciphers = []uint16{
	utls.TLS_AES_128_GCM_SHA256,
	utls.TLS_AES_256_GCM_SHA384,
	utls.TLS_CHACHA20_POLY1305_SHA256,
	utls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	utls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	utls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	utls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	utls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	utls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
	utls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
	utls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	utls.TLS_RSA_WITH_AES_128_GCM_SHA256,
	utls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	utls.TLS_RSA_WITH_AES_128_CBC_SHA,
	utls.TLS_RSA_WITH_AES_256_CBC_SHA,
}

// HTTP/2 header order — Chrome sends pseudo-headers + real headers in this exact order
// Deviating from this order is a fingerprint signal
var chrome120HeaderOrder = []string{
	"cache-control",
	"sec-ch-ua",
	"sec-ch-ua-mobile",
	"sec-ch-ua-platform",
	"upgrade-insecure-requests",
	"user-agent",
	"accept",
	"sec-fetch-site",
	"sec-fetch-mode",
	"sec-fetch-user",
	"sec-fetch-dest",
	"accept-encoding",
	"accept-language",
}

// uaProfile keeps UA + matching sec-ch-ua headers in sync
// mismatched UA vs sec-ch-ua is a common bot signal
type uaProfile struct {
	userAgent   string
	secChUa     string
	secChUaMob  string
	secChUaPlat string
}

var uaProfiles = []uaProfile{
	{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		`"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"`,
		"?0",
		`"Windows"`,
	},
	{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		`"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"`,
		"?0",
		`"macOS"`,
	},
	{
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
		`"Not_A Brand";v="8", "Chromium";v="119", "Google Chrome";v="119"`,
		"?0",
		`"Linux"`,
	},
	{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
		`"Not_A Brand";v="8", "Chromium";v="118", "Google Chrome";v="118"`,
		"?0",
		`"Windows"`,
	},
}

var acceptLanguages = []string{
	"en-US,en;q=0.9",
	"en-GB,en;q=0.8,en-US;q=0.7",
	"en-US,en;q=0.8",
	"en,en-US;q=0.9",
}

// Groq API key — set via config
var groqKey = ""

// minYieldThreshold — if an engine returns fewer filtered URLs than this,
// escalate to the next engine and merge results instead of returning partial
const minYieldThreshold = 5

// cacheTTL — how long a SERP result is considered fresh
// within this window, Fetch returns cached URLs without hitting any engine
const cacheTTL = 24 * time.Hour

// ----------------------------------------------------------------- blocklists --

// blockedDomains — social, video, and community sites that won't have
// structured data worth scraping for API use cases
var blockedDomains = map[string]struct{}{
	"youtube.com":   {},
	"youtu.be":      {},
	"reddit.com":    {},
	"twitter.com":   {},
	"x.com":         {},
	"facebook.com":  {},
	"instagram.com": {},
	"linkedin.com":  {},
	"tiktok.com":    {},
	"quora.com":     {},
	"pinterest.com": {},
	"wikipedia.org": {},
	"amazon.com":    {},
	"ebay.com":      {},
	"t.me":          {},
	"telegram.org":  {},
	"whatsapp.com":  {},
	"discord.com":   {},
	"twitch.tv":     {},
	"vimeo.com":     {},
}

// blockedPaths — auth, checkout, and account pages that require login
// or have no scrapeable content
var blockedPaths = []string{
	"/login",
	"/signin",
	"/sign-in",
	"/signup",
	"/sign-up",
	"/register",
	"/checkout",
	"/cart",
	"/account",
	"/auth",
	"/subscribe",
	"/membership",
	"/password",
	"/reset",
	"/verify",
	"/oauth",
	"/callback",
	"/logout",
	"/session",
}

// ------------------------------------------------------------------ types ---

// SERPRequest is the single input to Fetch.
type SERPRequest struct {
	UserID  string   `json:"user_id"`
	APIName string   `json:"api_name"`
	Intent  string   `json:"intent"`  // raw user intent
	Exclude []string `json:"exclude"` // URLs already tried — filtered out
	Limit   int      `json:"limit"`   // how many URLs caller needs
}

// SERPResponse is what Fetch returns to the crawl module.
type SERPResponse struct {
	URLs   []string `json:"urls"`
	Source string   `json:"source"` // e.g. "ddg" | "ddg+startpage" | "cache:ddg"
}

type searchFn func(query string, limit int) ([]string, error)

// ------------------------------------------------------------------ public --

// Fetch is the single exported function.
//
// Flow:
//  1. Check DB cache — if fresh results exist for this intent, return them
//  2. AI converts intent → structured search query (falls back to raw intent if no key)
//  3. Try DDG → Startpage in order
//  4. On 429/403 or zero results → macroJitter + next engine
//  5. On low yield (< minYieldThreshold) → continue and merge with next engine
//  6. On acceptable yield → save to DB and return
//  7. Excluded URLs and blocked domains/paths are filtered out at every step
func Fetch(db *sql.DB, req SERPRequest) (SERPResponse, error) {
	if req.Limit <= 0 {
		req.Limit = 10
	}

	// check cache first — skip engines entirely if fresh results exist
	if db != nil {
		if cached, err := loadCache(db, req.Intent, req.Exclude, req.Limit); err == nil && len(cached.URLs) > 0 {
			log.Printf("[serp] cache hit for intent: %q", req.Intent)
			return cached, nil
		}
	}

	query, err := parseIntent(req.Intent)
	if err != nil {
		query = req.Intent
	}

	excludeSet := make(map[string]struct{}, len(req.Exclude))
	for _, u := range req.Exclude {
		excludeSet[u] = struct{}{}
	}

	engines := []struct {
		name string
		fn   searchFn
	}{
		{"ddg", searchDDG},
		{"startpage", searchStartpage},
	}

	// fetch extra to absorb exclusions and blocked URLs
	fetchLimit := req.Limit + len(req.Exclude) + 10

	var accumulated []string
	var usedSources []string

	for _, engine := range engines {
		urls, err := engine.fn(query, fetchLimit)

		if err != nil {
			log.Printf("[serp] engine %s error: %v", engine.name, err)
			macroJitter()
			continue
		}

		if len(urls) == 0 {
			log.Printf("[serp] engine %s returned 0 urls", engine.name)
			macroJitter()
			continue
		}

		need := req.Limit - len(accumulated)
		filtered := filter(urls, excludeSet, need)

		if len(filtered) == 0 {
			macroJitter()
			continue
		}

		accumulated = append(accumulated, filtered...)
		usedSources = append(usedSources, engine.name)

		if len(accumulated) >= req.Limit {
			break
		}

		if len(accumulated) < minYieldThreshold {
			macroJitter()
			continue
		}

		break
	}

	if len(accumulated) == 0 {
		return SERPResponse{}, fmt.Errorf("all engines failed for intent: %q", req.Intent)
	}

	source := strings.Join(usedSources, "+")

	// persist to DB
	if db != nil {
		if err := saveCache(db, req, query, source, accumulated); err != nil {
			log.Printf("[serp] failed to save cache: %v", err)
		}
	}

	return SERPResponse{
		URLs:   accumulated,
		Source: source,
	}, nil
}

// ------------------------------------------------------------------ db ------

// loadCache looks up fresh SERP results for this intent from the DB.
// Returns cached URLs minus any in the exclude list.
func loadCache(db *sql.DB, intent string, exclude []string, limit int) (SERPResponse, error) {
	cutoff := time.Now().Add(-cacheTTL)

	row := db.QueryRow(`
		SELECT id, source FROM serp_cache
		WHERE intent = ? AND created_at > ?
		ORDER BY created_at DESC
		LIMIT 1
	`, intent, cutoff.Format("2006-01-02 15:04:05"))

	var cacheID int64
	var source string
	if err := row.Scan(&cacheID, &source); err != nil {
		return SERPResponse{}, err
	}

	rows, err := db.Query(`
		SELECT url FROM serp_urls
		WHERE cache_id = ?
		ORDER BY position ASC
	`, cacheID)
	if err != nil {
		return SERPResponse{}, err
	}
	defer rows.Close()

	excludeSet := make(map[string]struct{}, len(exclude))
	for _, u := range exclude {
		excludeSet[u] = struct{}{}
	}

	var urls []string
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			continue
		}
		if _, skip := excludeSet[u]; skip {
			continue
		}
		urls = append(urls, u)
		if len(urls) >= limit {
			break
		}
	}

	if len(urls) == 0 {
		return SERPResponse{}, fmt.Errorf("no usable cached urls")
	}

	return SERPResponse{URLs: urls, Source: "cache:" + source}, nil
}

// saveCache inserts a new serp_cache row and its associated serp_urls rows.
func saveCache(db *sql.DB, req SERPRequest, query, source string, urls []string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.Exec(`
		INSERT INTO serp_cache (user_id, api_name, intent, query, source)
		VALUES (?, ?, ?, ?, ?)
	`, req.UserID, req.APIName, req.Intent, query, source)
	if err != nil {
		return err
	}

	cacheID, err := res.LastInsertId()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO serp_urls (cache_id, url, position) VALUES (?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i, u := range urls {
		if _, err := stmt.Exec(cacheID, u, i+1); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// ----------------------------------------------------------------- engines --

func searchDDG(query string, limit int) ([]string, error) {
	doc, err := fetchDoc("https://html.duckduckgo.com/html/?q=" + url.QueryEscape(query))
	if err != nil {
		return nil, err
	}

	var urls []string
	doc.Find(".result__url").Each(func(i int, s *goquery.Selection) {
		if i >= limit {
			return
		}
		raw := strings.TrimSpace(s.Text())
		if raw == "" {
			return
		}
		if !strings.HasPrefix(raw, "http") {
			raw = "https://" + raw
		}
		urls = append(urls, raw)
	})

	return urls, nil
}

func searchStartpage(query string, limit int) ([]string, error) {
	doc, err := fetchDoc("https://www.startpage.com/search?q=" + url.QueryEscape(query))
	if err != nil {
		return nil, err
	}

	var urls []string
	doc.Find("a.result-title").Each(func(i int, s *goquery.Selection) {
		if i >= limit {
			return
		}
		href, exists := s.Attr("href")
		if !exists || href == "" {
			return
		}
		urls = append(urls, href)
	})

	return urls, nil
}

// ------------------------------------------------------------------ http ----

// fetchDoc fetches a URL and returns a parsed goquery document.
// Optional forceLang overrides the Accept-Language header.
func fetchDoc(target string, forceLang ...string) (*goquery.Document, error) {
	lang := ""
	if len(forceLang) > 0 {
		lang = forceLang[0]
	}

	microJitter()

	client, err := newClient()
	if err != nil {
		return nil, err
	}

	profile := uaProfiles[rand.Intn(len(uaProfiles))]

	req, err := fhttp.NewRequestWithContext(context.Background(), fhttp.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}

	req.Header = fhttp.Header{
		"cache-control":             {"max-age=0"},
		"sec-ch-ua":                 {profile.secChUa},
		"sec-ch-ua-mobile":          {profile.secChUaMob},
		"sec-ch-ua-platform":        {profile.secChUaPlat},
		"upgrade-insecure-requests": {"1"},
		"user-agent":                {profile.userAgent},
		"accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"},
		"sec-fetch-site":            {"none"},
		"sec-fetch-mode":            {"navigate"},
		"sec-fetch-user":            {"?1"},
		"sec-fetch-dest":            {"document"},
		"accept-encoding":           {"gzip, deflate, br"},
		"accept-language": {func() string {
			if lang != "" {
				return lang
			}
			return acceptLanguages[rand.Intn(len(acceptLanguages))]
		}()},
		fhttp.HeaderOrderKey: chrome120HeaderOrder,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 || resp.StatusCode == 403 {
		return nil, fmt.Errorf("blocked: status %d", resp.StatusCode)
	}
	if resp.StatusCode != fhttp.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var reader io.Reader = resp.Body
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("gzip reader error: %w", err)
		}
		defer gz.Close()
		reader = gz
	case "br":
		reader = brotli.NewReader(resp.Body)
	}

	return goquery.NewDocumentFromReader(reader)
}

func newClient() (*fhttp.Client, error) {
	tlsCfg := &utls.Config{
		InsecureSkipVerify: false,
		CipherSuites:       chrome120Ciphers,
		CurvePreferences: []utls.CurveID{
			utls.X25519,
			utls.CurveP256,
			utls.CurveP384,
		},
		MinVersion: utls.VersionTLS12,
		MaxVersion: utls.VersionTLS13,
	}

	transport := &fhttp.Transport{
		TLSClientConfig:    tlsCfg,
		DisableCompression: false,
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		ForceAttemptHTTP2:  true,
	}

	http2.ConfigureTransport(transport)

	if len(proxies) > 0 {
		idx := atomic.AddUint64(&proxyCounter, 1)
		proxyURL, err := url.Parse(proxies[idx%uint64(len(proxies))])
		if err != nil {
			return nil, fmt.Errorf("invalid proxy: %w", err)
		}
		transport.Proxy = fhttp.ProxyURL(proxyURL)
	}

	return &fhttp.Client{
		Transport: transport,
		Timeout:   12 * time.Second,
	}, nil
}

// ------------------------------------------------------------------ intent --

func parseIntent(intent string) (string, error) {
	if groqKey == "" {
		return intent, nil
	}

	body := map[string]any{
		"model":      "llama-3.1-8b-instant",
		"max_tokens": 60,
		"messages": []map[string]string{
			{
				"role":    "system",
				"content": "Convert the user intent into a concise search engine query. Return only the query string, nothing else.",
			},
			{
				"role":    "user",
				"content": intent,
			},
		},
	}

	b, _ := json.Marshal(body)
	req, err := fhttp.NewRequest("POST", "https://api.groq.com/openai/v1/chat/completions", strings.NewReader(string(b)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+groqKey)

	client := &fhttp.Client{Timeout: 8 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &result); err != nil || len(result.Choices) == 0 {
		return "", fmt.Errorf("bad Groq response")
	}

	return strings.TrimSpace(result.Choices[0].Message.Content), nil
}

// ------------------------------------------------------------------ jitter --

func microJitter() {
	time.Sleep(time.Duration(rand.Intn(900)+100) * time.Microsecond)
}

func macroJitter() {
	time.Sleep(time.Duration(rand.Intn(3)+2) * time.Second)
}

// ----------------------------------------------------------------- helpers --

func isBlockedURL(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil {
		return true
	}

	host := strings.TrimPrefix(strings.ToLower(parsed.Hostname()), "www.")
	if _, blocked := blockedDomains[host]; blocked {
		return true
	}

	path := strings.ToLower(parsed.Path)
	for _, blocked := range blockedPaths {
		if path == blocked || strings.HasPrefix(path, blocked+"/") || strings.HasPrefix(path, blocked+"?") {
			return true
		}
	}

	return false
}

func filter(urls []string, exclude map[string]struct{}, limit int) []string {
	if limit <= 0 {
		return nil
	}
	seen := make(map[string]struct{})
	var out []string
	for _, u := range urls {
		if len(out) >= limit {
			break
		}
		if _, skip := exclude[u]; skip {
			continue
		}
		if _, dup := seen[u]; dup {
			continue
		}
		if isBlockedURL(u) {
			continue
		}
		seen[u] = struct{}{}
		out = append(out, u)
	}
	return out
}
