package serp

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/joho/godotenv"

	fhttp "github.com/Danny-Dasilva/fhttp"
	"github.com/PuerkitoBio/goquery"
	utls "github.com/refraction-networking/utls"
	gohttp2 "golang.org/x/net/http2"
)

// ------------------------------------------------------------------ config --

var uaProfiles = []struct {
	userAgent string
}{
	{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"},
	{"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"},
	{"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"},
	{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"},
}

var acceptLanguages = []string{
	"en-US,en;q=0.9",
	"en-GB,en;q=0.8,en-US;q=0.7",
	"en-US,en;q=0.8",
	"en,en-US;q=0.9",
}

var groqKey = ""

const minYieldThreshold = 5
const engineRetries = 2
const maxPerVariation = 10
const groqRetries = 3

// ---------------------------------------------------------------- per-engine rate limiter --

type engineRateLimiter struct {
	mu           sync.Mutex
	blockedUntil map[string]time.Time
}

var rateLimiter = &engineRateLimiter{
	blockedUntil: map[string]time.Time{},
}

func (r *engineRateLimiter) isBlocked(engine string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	until, ok := r.blockedUntil[engine]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(r.blockedUntil, engine)
		return false
	}
	return true
}

func (r *engineRateLimiter) block(engine string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blockedUntil[engine] = time.Now().Add(d)
	log.Printf("[serp] engine %s rate limited — cooling down for %.0fs", engine, d.Seconds())
}

// ----------------------------------------------------------------- blocklists --

var hardBlockedDomains = map[string]struct{}{
	"youtube.com":   {},
	"youtu.be":      {},
	"twitter.com":   {},
	"x.com":         {},
	"facebook.com":  {},
	"instagram.com": {},
	"tiktok.com":    {},
	"t.me":          {},
	"telegram.org":  {},
	"whatsapp.com":  {},
	"discord.com":   {},
	"twitch.tv":     {},
	"vimeo.com":     {},
}

var blockedPaths = []string{
	"/login", "/signin", "/sign-in", "/signup", "/sign-up",
	"/register", "/checkout", "/cart", "/account", "/auth",
	"/subscribe", "/membership", "/password", "/reset",
	"/verify", "/oauth", "/callback", "/logout", "/session",
}

// ---------------------------------------------------------------- client pool --

var (
	clientPool   = map[string]*http.Client{}
	clientPoolMu sync.Mutex
)

func getClient(engineName string) (*http.Client, error) {
	clientPoolMu.Lock()
	defer clientPoolMu.Unlock()
	if c, ok := clientPool[engineName]; ok {
		return c, nil
	}
	c := newHTTP2Client()
	clientPool[engineName] = c
	return c, nil
}

func newHTTP2Client() *http.Client {
	h2transport := &gohttp2.Transport{
		TLSClientConfig: &tls.Config{},
		DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
			conn, err := net.DialTimeout(network, addr, 10*time.Second)
			if err != nil {
				return nil, err
			}
			host, _, _ := net.SplitHostPort(addr)
			uconn := utls.UClient(conn, &utls.Config{
				ServerName: host,
				NextProtos: []string{"h2", "http/1.1"},
			}, utls.HelloChrome_Auto)
			if err := uconn.Handshake(); err != nil {
				conn.Close()
				return nil, err
			}
			return uconn, nil
		},
	}
	return &http.Client{
		Timeout:   12 * time.Second,
		Transport: h2transport,
	}
}

// ----------------------------------------------------------------- serper keys --

var serperKeys []string
var serperKeyCounter uint64

func nextSerperKey() string {
	if len(serperKeys) == 0 {
		return ""
	}
	idx := atomic.AddUint64(&serperKeyCounter, 1)
	return serperKeys[idx%uint64(len(serperKeys))]
}

// ------------------------------------------------------------------ init -----

func init() {
	if err := godotenv.Load(); err != nil {
		log.Printf("[serp] no .env file found, relying on shell environment")
	}

	for _, k := range []string{os.Getenv("SERPER1_KEY")} {
		if k != "" {
			serperKeys = append(serperKeys, k)
		}
	}

	if len(serperKeys) == 0 {
		log.Printf("[serp] WARNING: no Serper keys loaded — SERPER1_KEY not set")
	} else {
		log.Printf("[serp] loaded %d Serper key(s)", len(serperKeys))
	}

	groqKey = os.Getenv("SERP_GROQ_KEYS")
}

// ------------------------------------------------------------------ types ---

type URLEntry struct {
	URL        string
	RenderedBy string
	URLType    string // "discovery" or "extraction"
}

type SERPRequest struct {
	UserID   string   `json:"user_id"`
	DataName string   `json:"data_name"`
	Intent   string   `json:"intent"`
	Exclude  []string `json:"exclude"`
	Limit    int      `json:"limit"`
}

type SERPResponse struct {
	URLs    []string   `json:"urls"`
	Entries []URLEntry `json:"entries"`
	Source  string     `json:"source"`
}

type searchFn func(query string, limit int) ([]string, error)

// ------------------------------------------------------------------ Fetch --

func Fetch(req SERPRequest) (SERPResponse, error) {
	if req.Limit <= 0 {
		req.Limit = 60
	}

	const engineLimit = 50
	const wikipediaLimit = 10

	variations, err := generateVariations(req.Intent, computeVariations(engineLimit))
	if err != nil || len(variations) == 0 {
		log.Printf("[serp] variation generation failed, falling back to raw intent: %v", err)
		variations = []string{req.Intent}
	}

	numVariations := len(variations)
	perVariation := engineLimit / numVariations
	if perVariation > maxPerVariation {
		perVariation = maxPerVariation
	}
	if perVariation < 1 {
		perVariation = 1
	}

	log.Printf("[serp] engineBudget=%d variations=%d perVariation=%d", engineLimit, numVariations, perVariation)

	engines := []struct {
		name string
		fn   searchFn
	}{
		{"ddg", searchDDG},
		{"startpage", searchStartpage},
		{"serper", searchSerper},
	}

	globalSeen := make(map[string]struct{})
	for _, u := range req.Exclude {
		globalSeen[u] = struct{}{}
	}

	var serpEntries []URLEntry
	var usedSources []string
	sourceSet := map[string]bool{}

	for i, query := range variations {
		if len(serpEntries) >= engineLimit {
			break
		}

		remaining := engineLimit - len(serpEntries)
		quota := perVariation
		if quota > remaining {
			quota = remaining
		}

		log.Printf("[serp] variation %d/%d: %q — quota=%d", i+1, numVariations, query, quota)

		var variationURLs []string

		for _, engine := range engines {
			if len(variationURLs) >= quota {
				break
			}
			if engine.name == "serper" && len(serperKeys) == 0 {
				continue
			}
			if rateLimiter.isBlocked(engine.name) {
				log.Printf("[serp] engine %s skipped — rate limited", engine.name)
				continue
			}

			need := quota - len(variationURLs)
			fetchLimit := need + 10

			urls, err := retryEngine(engine.name, engine.fn, query, fetchLimit)
			if err != nil {
				log.Printf("[serp] engine %s failed for variation %d: %v", engine.name, i+1, err)
				macroJitter()
				continue
			}

			filtered := filterGlobal(urls, globalSeen, need)
			if len(filtered) == 0 {
				macroJitter()
				continue
			}

			for _, u := range filtered {
				variationURLs = append(variationURLs, u)
				serpEntries = append(serpEntries, URLEntry{
					URL:        u,
					RenderedBy: engine.name,
					URLType:    "discovery",
				})
				globalSeen[u] = struct{}{}
			}

			if !sourceSet[engine.name] {
				usedSources = append(usedSources, engine.name)
				sourceSet[engine.name] = true
			}

			if len(variationURLs) >= quota {
				break
			}
			if len(variationURLs) < minYieldThreshold {
				macroJitter()
			}
		}

		log.Printf("[serp] variation %d collected %d landing urls (total: %d)", i+1, len(variationURLs), len(serpEntries))

		if i < numVariations-1 {
			macroJitter()
		}
	}

	if len(serpEntries) == 0 {
		return SERPResponse{}, fmt.Errorf("all engines failed for intent: %q", req.Intent)
	}

	// wikipedia — direct extraction URLs, no discovery needed
	wikiURLs, wikiErr := fetchWikipedia(req.Intent, wikipediaLimit, globalSeen)
	if wikiErr != nil {
		log.Printf("[serp] wikipedia fetch failed (non-fatal): %v", wikiErr)
	} else {
		for _, u := range wikiURLs {
			serpEntries = append(serpEntries, URLEntry{
				URL:        u,
				RenderedBy: "wikipedia",
				URLType:    "discovery",
			})
		}
		if len(wikiURLs) > 0 && !sourceSet["wikipedia"] {
			usedSources = append(usedSources, "wikipedia")
		}
		log.Printf("[serp] wikipedia added %d urls (total: %d)", len(wikiURLs), len(serpEntries))
	}

	var allURLs []string
	for _, e := range serpEntries {
		allURLs = append(allURLs, e.URL)
	}

	source := strings.Join(usedSources, "+")

	var debugLines []string
	for _, e := range serpEntries {
		debugLines = append(debugLines, fmt.Sprintf("[%s] %s", e.URLType, e.URL))
	}
	content := fmt.Sprintf(
		"Intent: %s\nSource: %s\nTotal URLs: %d\n\nVariations:\n%s\n\nURLs:\n%s",
		req.Intent, source, len(allURLs),
		strings.Join(variations, "\n"),
		strings.Join(debugLines, "\n"),
	)
	if err := os.WriteFile("serp_results.txt", []byte(content), 0644); err != nil {
		log.Printf("[serp] failed to write serp_results.txt: %v", err)
	} else {
		log.Printf("[serp] results written to serp_results.txt")
	}

	return SERPResponse{
		URLs:    allURLs,
		Entries: serpEntries,
		Source:  source,
	}, nil
}

// ------------------------------------------------------------------ variations --

func computeVariations(budget int) int {
	v := budget / 10
	if v < 2 {
		v = 2
	}
	if v > 5 {
		v = 5
	}
	return v
}

func parseGroqWaitTime(errMsg string) float64 {
	idx := strings.Index(errMsg, "try again in ")
	if idx == -1 {
		return 0
	}
	rest := errMsg[idx+len("try again in "):]
	var numStr strings.Builder
	for _, ch := range rest {
		if ch >= '0' && ch <= '9' || ch == '.' {
			numStr.WriteRune(ch)
		} else {
			break
		}
	}
	if numStr.Len() == 0 {
		return 0
	}
	val, err := strconv.ParseFloat(numStr.String(), 64)
	if err != nil {
		return 0
	}
	return val + 1
}

func generateVariations(intent string, count int) ([]string, error) {
	if groqKey == "" {
		return []string{intent}, nil
	}

	var lastErr error
	for attempt := 1; attempt <= groqRetries; attempt++ {
		variations, err := tryGenerateVariations(intent, count)
		if err == nil && len(variations) > 0 {
			return variations, nil
		}
		lastErr = err
		waitSec := parseGroqWaitTime(err.Error())
		if waitSec > 0 {
			log.Printf("[serp] groq rate limit — waiting %.1fs (attempt %d/%d)", waitSec, attempt, groqRetries)
			time.Sleep(time.Duration(waitSec * float64(time.Second)))
		} else {
			log.Printf("[serp] groq retry %d/%d: %v", attempt, groqRetries, err)
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}
	return nil, lastErr
}

func tryGenerateVariations(intent string, count int) ([]string, error) {
	prompt := fmt.Sprintf(
		"Generate exactly %d distinct search engine query variations for the following intent.\n"+
			"Each variation should approach the topic from a different angle to maximize source diversity.\n"+
			"Return ONLY a JSON array of strings. No explanation, no markdown.\n"+
			"Intent: %s",
		count, intent,
	)

	body := map[string]any{
		"model":      "llama-3.1-8b-instant",
		"max_tokens": 200,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	}

	b, _ := json.Marshal(body)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, err := fhttp.NewRequestWithContext(ctx, "POST", "https://api.groq.com/openai/v1/chat/completions", strings.NewReader(string(b)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+groqKey)
	req.Header.Set("Accept-Encoding", "identity")

	client := &fhttp.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("bad Groq response: %w", err)
	}
	if result.Error.Message != "" {
		return nil, fmt.Errorf("groq error: %s", result.Error.Message)
	}
	if len(result.Choices) == 0 {
		return nil, fmt.Errorf("groq: no choices in response")
	}

	raw := strings.TrimSpace(result.Choices[0].Message.Content)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	var variations []string
	if err := json.Unmarshal([]byte(raw), &variations); err != nil {
		return nil, fmt.Errorf("parse variations: %w", err)
	}

	seen := map[string]bool{intent: true}
	final := []string{intent}
	for _, v := range variations {
		v = strings.TrimSpace(v)
		if v != "" && !seen[v] {
			seen[v] = true
			final = append(final, v)
		}
	}

	if len(final) > count {
		final = final[:count]
	}

	log.Printf("[serp] generated %d variations for intent: %q", len(final), intent)
	return final, nil
}

// ---------------------------------------------------------------- retryEngine --

func retryEngine(name string, fn searchFn, query string, limit int) ([]string, error) {
	var lastErr error
	for i := 0; i < engineRetries; i++ {
		urls, err := fn(query, limit)
		if err == nil && len(urls) > 0 {
			return urls, nil
		}
		if err != nil {
			if strings.Contains(err.Error(), "429") || strings.Contains(err.Error(), "blocked") {
				rateLimiter.block(name, time.Duration(30+rand.Intn(30))*time.Second)
				return nil, err
			}
			lastErr = err
		}
		if i < engineRetries-1 {
			macroJitter()
		}
	}
	return nil, fmt.Errorf("engine failed after %d retries: %w", engineRetries, lastErr)
}

// ----------------------------------------------------------------- engines --

var ddgSelectors = []string{
	"a.result__a",
	"h2.result__title a",
	"a[data-testid='result-title-a']",
	".results .result a[href]",
}

func searchDDG(query string, limit int) ([]string, error) {
	doc, err := fetchDoc("ddg", "https://html.duckduckgo.com/html/?q="+url.QueryEscape(query))
	if err != nil {
		return nil, err
	}

	for _, sel := range ddgSelectors {
		var urls []string
		doc.Find(sel).Each(func(i int, s *goquery.Selection) {
			if len(urls) >= limit {
				return
			}
			href, exists := s.Attr("href")
			if !exists || href == "" {
				return
			}
			if strings.HasPrefix(href, "//duckduckgo.com/l/") {
				if parsed, err := url.Parse("https:" + href); err == nil {
					if dest := parsed.Query().Get("uddg"); dest != "" {
						href = dest
					}
				}
			}
			urls = append(urls, href)
		})
		if len(urls) > 0 {
			log.Printf("[serp] ddg selector %q returned %d urls", sel, len(urls))
			return urls, nil
		}
	}

	return nil, fmt.Errorf("ddg: all selectors returned 0 results")
}

var startpageSelectors = []string{
	"a.result-title.result-link",
	"a.w-gl__result-title",
	"h3.search-result__title a",
	".search-result a[href]:first-child",
	"a[data-testid='result-title']",
}

func searchStartpage(query string, limit int) ([]string, error) {
	doc, err := fetchDoc("startpage", "https://www.startpage.com/search?q="+url.QueryEscape(query))
	if err != nil {
		return nil, err
	}

	for _, sel := range startpageSelectors {
		var urls []string
		doc.Find(sel).Each(func(i int, s *goquery.Selection) {
			if len(urls) >= limit {
				return
			}
			href, exists := s.Attr("href")
			if !exists || href == "" {
				return
			}
			urls = append(urls, href)
		})
		if len(urls) > 0 {
			log.Printf("[serp] startpage selector %q returned %d urls", sel, len(urls))
			return urls, nil
		}
	}

	return nil, fmt.Errorf("startpage: all selectors returned 0 results")
}

func searchSerper(query string, limit int) ([]string, error) {
	key := nextSerperKey()
	if key == "" {
		return nil, fmt.Errorf("no serper key available")
	}

	num := limit
	if num > 20 {
		num = 20
	}

	payload, err := json.Marshal(map[string]any{"q": query, "num": num})
	if err != nil {
		return nil, err
	}

	req, err := fhttp.NewRequestWithContext(
		context.Background(), fhttp.MethodPost,
		"https://google.serper.dev/search",
		bytes.NewReader(payload),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-KEY", key)
	req.Header.Set("Content-Type", "application/json")

	client := &fhttp.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		return nil, fmt.Errorf("serper: rate limited (429)")
	}
	if resp.StatusCode != fhttp.StatusOK {
		return nil, fmt.Errorf("serper: unexpected status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Organic []struct {
			Link string `json:"link"`
		} `json:"organic"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("serper: failed to parse response: %w", err)
	}

	var urls []string
	for _, item := range result.Organic {
		if item.Link != "" {
			urls = append(urls, item.Link)
		}
		if len(urls) >= limit {
			break
		}
	}
	return urls, nil
}

// ---------------------------------------------------------------- wikipedia --

func generateWikipediaQueries(intent string, count int) ([]string, error) {
	if groqKey == "" {
		return []string{intent}, nil
	}

	prompt := fmt.Sprintf(
		"Convert the following data intent into exactly %d short Wikipedia search queries.\n"+
			"Each query should be a concise topic name or phrase that would match a Wikipedia article.\n"+
			"Return ONLY a JSON array of strings. No explanation, no markdown.\n"+
			"Intent: %s",
		count, intent,
	)

	body := map[string]any{
		"model":      "llama-3.1-8b-instant",
		"max_tokens": 100,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	}

	b, _ := json.Marshal(body)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, err := fhttp.NewRequestWithContext(ctx, "POST", "https://api.groq.com/openai/v1/chat/completions", strings.NewReader(string(b)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+groqKey)
	req.Header.Set("Accept-Encoding", "identity")

	client := &fhttp.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("bad groq response: %w", err)
	}
	if result.Error.Message != "" {
		return nil, fmt.Errorf("groq error: %s", result.Error.Message)
	}
	if len(result.Choices) == 0 {
		return nil, fmt.Errorf("groq: no choices")
	}

	raw := strings.TrimSpace(result.Choices[0].Message.Content)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	var queries []string
	if err := json.Unmarshal([]byte(raw), &queries); err != nil {
		return nil, fmt.Errorf("parse queries: %w", err)
	}

	log.Printf("[serp/wikipedia] generated %d queries: %v", len(queries), queries)
	return queries, nil
}

func searchWikipedia(query string, limit int) ([]string, error) {
	apiURL := fmt.Sprintf(
		"https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch=%s&srlimit=%d&format=json",
		url.QueryEscape(query), limit,
	)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "vexaro-engine/1.0 (data pipeline; contact@vexaro.com)")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("wikipedia: unexpected status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Query struct {
			Search []struct {
				Title string `json:"title"`
			} `json:"search"`
		} `json:"query"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("wikipedia: parse error: %w", err)
	}

	var urls []string
	for _, item := range result.Query.Search {
		articleURL := "https://en.wikipedia.org/wiki/" + url.PathEscape(strings.ReplaceAll(item.Title, " ", "_"))
		urls = append(urls, articleURL)
		if len(urls) >= limit {
			break
		}
	}

	log.Printf("[serp/wikipedia] query %q returned %d urls", query, len(urls))
	return urls, nil
}

func fetchWikipedia(intent string, limit int, globalSeen map[string]struct{}) ([]string, error) {
	queries, err := generateWikipediaQueries(intent, 3)
	if err != nil {
		log.Printf("[serp/wikipedia] query generation failed, using intent directly: %v", err)
		queries = []string{intent}
	}

	perQuery := limit / len(queries)
	if perQuery < 1 {
		perQuery = 1
	}

	var allURLs []string
	for _, q := range queries {
		urls, err := searchWikipedia(q, perQuery+3)
		if err != nil {
			log.Printf("[serp/wikipedia] query %q failed: %v", q, err)
			continue
		}
		for _, u := range urls {
			if _, dup := globalSeen[u]; dup {
				continue
			}
			globalSeen[u] = struct{}{}
			allURLs = append(allURLs, u)
			if len(allURLs) >= limit {
				break
			}
		}
		if len(allURLs) >= limit {
			break
		}
	}

	return allURLs, nil
}

// ------------------------------------------------------------------ http ----

func fetchDoc(engineName, target string) (*goquery.Document, error) {
	microJitter()

	client, err := getClient(engineName)
	if err != nil {
		return nil, err
	}

	ua := uaProfiles[rand.Intn(len(uaProfiles))].userAgent

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", acceptLanguages[rand.Intn(len(acceptLanguages))])
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("Cache-Control", "max-age=0")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-User", "?1")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		return nil, fmt.Errorf("blocked: status 429")
	}
	if resp.StatusCode == 403 {
		return nil, fmt.Errorf("blocked: status 403")
	}
	if resp.StatusCode != http.StatusOK {
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

// ------------------------------------------------------------------ jitter --

func microJitter() {
	time.Sleep(time.Duration(rand.Intn(800)+200) * time.Millisecond)
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
	if _, blocked := hardBlockedDomains[host]; blocked {
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

func filterGlobal(urls []string, seen map[string]struct{}, limit int) []string {
	if limit <= 0 {
		return nil
	}
	var out []string
	for _, u := range urls {
		if len(out) >= limit {
			break
		}
		if _, dup := seen[u]; dup {
			continue
		}
		if isBlockedURL(u) {
			continue
		}
		out = append(out, u)
	}
	return out
}