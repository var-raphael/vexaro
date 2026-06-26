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

const engineRetries = 2
const maxPerVariation = 10
const mistralRetries = 3

var mistralKeys []string
var mistralKeyCounter uint64

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

func nextMistralKey() string {
	if len(mistralKeys) == 0 {
		return ""
	}
	idx := atomic.AddUint64(&mistralKeyCounter, 1)
	return mistralKeys[idx%uint64(len(mistralKeys))]
}

// ------------------------------------------------------------------ init -----

func init() {
	if err := godotenv.Load(); err != nil {
		log.Printf("[serp] no .env file found, relying on shell environment")
	}

	for _, k := range strings.Split(os.Getenv("SERPER_KEYS"), ",") {
		k = strings.TrimSpace(k)
		if k != "" {
			serperKeys = append(serperKeys, k)
		}
	}
	if len(serperKeys) == 0 {
		if k := strings.TrimSpace(os.Getenv("SERPER1_KEY")); k != "" {
			serperKeys = append(serperKeys, k)
		}
	}
	if len(serperKeys) == 0 {
		log.Printf("[serp] WARNING: no Serper keys loaded")
	} else {
		log.Printf("[serp] loaded %d Serper key(s)", len(serperKeys))
	}

	for _, k := range strings.Split(os.Getenv("SERP_MISTRAL_KEYS"), ",") {
		k = strings.TrimSpace(k)
		if k != "" {
			mistralKeys = append(mistralKeys, k)
		}
	}
	if len(mistralKeys) == 0 {
		log.Printf("[serp] WARNING: no Mistral keys loaded")
	} else {
		log.Printf("[serp] loaded %d Mistral key(s)", len(mistralKeys))
	}
}

// ------------------------------------------------------------------ types ---

type URLEntry struct {
	URL        string
	RenderedBy string
	URLType    string
}

type serpResult struct {
	URL     string
	Title   string
	Snippet string
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

type ProgressFn func(step string, detail string)

type searchFn func(query string, limit int) ([]serpResult, error)

// ------------------------------------------------------------------ Fetch --

func Fetch(req SERPRequest, progress ProgressFn) (SERPResponse, error) {
	if req.Limit <= 0 {
		req.Limit = 60
	}
	if progress == nil {
		progress = func(step, detail string) {}
	}

	const engineLimit = 50
	const wikipediaLimit = 10

	progress("generating", "Generating search variations...")
	variations, err := generateVariations(req.Intent, computeVariations(engineLimit))
	if err != nil || len(variations) == 0 {
		log.Printf("[serp] variation generation failed, falling back to raw intent: %v", err)
		variations = []string{req.Intent}
	}
	log.Printf("[serp] generated %d variations", len(variations))
	progress("variations", fmt.Sprintf("Generated %d search variations", len(variations)))

	numVariations := len(variations)
	perVariation := engineLimit / numVariations
	if perVariation > maxPerVariation {
		perVariation = maxPerVariation
	}
	if perVariation < 1 {
		perVariation = 1
	}

	engines := []struct {
    name string
    fn   searchFn
}{
    {"ddg", searchDDG},
    {"serper", searchSerper},
}

	globalSeen := make(map[string]struct{})
	for _, u := range req.Exclude {
		globalSeen[u] = struct{}{}
	}

	var allResults []serpResult
	var usedSources []string
	sourceSet := map[string]bool{}

	progress("searching", "Searching across engines...")

	for i, query := range variations {
		if len(allResults) >= engineLimit {
			break
		}

		remaining := engineLimit - len(allResults)
		quota := perVariation
		if quota > remaining {
			quota = remaining
		}

		log.Printf("[serp] variation %d/%d: %q — quota=%d", i+1, numVariations, query, quota)

		var variationResults []serpResult

		for _, engine := range engines {
			if len(variationResults) >= quota {
				break
			}
			if engine.name == "serper" && len(serperKeys) == 0 {
				continue
			}
			if rateLimiter.isBlocked(engine.name) {
				log.Printf("[serp] engine %s skipped — rate limited", engine.name)
				continue
			}

			need := quota - len(variationResults)
			fetchLimit := need + 10

			results, err := retryEngine(engine.name, engine.fn, query, fetchLimit)
			if err != nil {
				log.Printf("[serp] engine %s failed for variation %d: %v", engine.name, i+1, err)
				macroJitter()
				continue
			}

			filtered := filterResults(results, globalSeen, need)
			if len(filtered) == 0 {
				macroJitter()
				continue
			}

			for _, r := range filtered {
				variationResults = append(variationResults, r)
				allResults = append(allResults, r)
				globalSeen[r.URL] = struct{}{}
			}

			if !sourceSet[engine.name] {
				usedSources = append(usedSources, engine.name)
				sourceSet[engine.name] = true
			}

			if len(variationResults) >= quota {
				break
			}
			macroJitter()
		}

		log.Printf("[serp] variation %d collected %d urls (total: %d)", i+1, len(variationResults), len(allResults))

		if i < numVariations-1 {
			macroJitter()
		}
	}

	if len(allResults) == 0 {
		return SERPResponse{}, fmt.Errorf("all engines failed for intent: %q", req.Intent)
	}

	progress("wikipedia", "Fetching Wikipedia sources...")
	wikiResults, wikiErr := fetchWikipedia(req.Intent, wikipediaLimit, globalSeen)
	if wikiErr != nil {
		log.Printf("[serp] wikipedia fetch failed (non-fatal): %v", wikiErr)
	} else {
		allResults = append(allResults, wikiResults...)
		if len(wikiResults) > 0 && !sourceSet["wikipedia"] {
			usedSources = append(usedSources, "wikipedia")
		}
		log.Printf("[serp] wikipedia added %d urls (total: %d)", len(wikiResults), len(allResults))
	}

	// -------------------------------------------------------- write serp_results.txt --
	var rawLines []string
	rawLines = append(rawLines, fmt.Sprintf("Intent:  %s", req.Intent))
	rawLines = append(rawLines, fmt.Sprintf("Source:  %s", strings.Join(usedSources, "+")))
	rawLines = append(rawLines, fmt.Sprintf("Total:   %d URLs", len(allResults)))
	rawLines = append(rawLines, "")
	rawLines = append(rawLines, "Variations:")
	for _, v := range variations {
		rawLines = append(rawLines, "  "+v)
	}
	rawLines = append(rawLines, "")
	rawLines = append(rawLines, "Fetched URLs:")
	for i, r := range allResults {
		rawLines = append(rawLines, fmt.Sprintf("  [%d] %s", i, r.URL))
		rawLines = append(rawLines, fmt.Sprintf("      Title:   %s", r.Title))
		rawLines = append(rawLines, fmt.Sprintf("      Snippet: %s", r.Snippet))
		rawLines = append(rawLines, "")
	}
	if err := os.WriteFile("serp_results.txt", []byte(strings.Join(rawLines, "\n")), 0644); err != nil {
		log.Printf("[serp] failed to write serp_results.txt: %v", err)
	}

	progress("filtering", fmt.Sprintf("Filtering %d URLs for relevance...", len(allResults)))
	filtered := filterRelevant(req.Intent, allResults)
	log.Printf("[serp] relevance filter: %d → %d urls", len(allResults), len(filtered))
	progress("filtered", fmt.Sprintf("%d relevant URLs confirmed", len(filtered)))

	// -------------------------------------------------------- write clean-urls.txt --
	var cleanLines []string
	cleanLines = append(cleanLines, fmt.Sprintf("Intent:  %s", req.Intent))
	cleanLines = append(cleanLines, fmt.Sprintf("Total:   %d URLs (filtered from %d)", len(filtered), len(allResults)))
	cleanLines = append(cleanLines, "")
	cleanLines = append(cleanLines, "Filtered URLs:")
	for i, r := range filtered {
		cleanLines = append(cleanLines, fmt.Sprintf("  [%d] %s", i, r.URL))
		cleanLines = append(cleanLines, fmt.Sprintf("      Title:   %s", r.Title))
		cleanLines = append(cleanLines, fmt.Sprintf("      Snippet: %s", r.Snippet))
		cleanLines = append(cleanLines, "")
	}
	if err := os.WriteFile("clean-urls.txt", []byte(strings.Join(cleanLines, "\n")), 0644); err != nil {
		log.Printf("[serp] failed to write clean-urls.txt: %v", err)
	}

	var serpEntries []URLEntry
	var allURLs []string
	for _, r := range filtered {
		serpEntries = append(serpEntries, URLEntry{
			URL:        r.URL,
			RenderedBy: "serp",
			URLType:    "discovery",
		})
		allURLs = append(allURLs, r.URL)
	}

	return SERPResponse{
		URLs:    allURLs,
		Entries: serpEntries,
		Source:  strings.Join(usedSources, "+"),
	}, nil
}

// ------------------------------------------------------------------ variations --

func computeVariations(budget int) int {
	v := budget / 10
	if v < 2 {
		v = 2
	}
	if v > 3 {
		v = 3
	}
	return v
}

func generateVariations(intent string, count int) ([]string, error) {
	if len(mistralKeys) == 0 {
		return []string{intent}, nil
	}

	var lastErr error
	for attempt := 1; attempt <= mistralRetries; attempt++ {
		variations, err := tryGenerateVariations(intent, count)
		if err == nil && len(variations) > 0 {
			return variations, nil
		}
		lastErr = err
		log.Printf("[serp] mistral variation retry %d/%d: %v", attempt, mistralRetries, err)
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	return nil, lastErr
}

func tryGenerateVariations(intent string, count int) ([]string, error) {
	prompt := fmt.Sprintf(
		"Generate exactly %d distinct search engine queries for the following intent.\n"+
			"CRITICAL RULES:\n"+
			"- Every query MUST stay strictly on the same topic as the intent\n"+
			"- Every query MUST include the core subject and timeframe if present (e.g. if intent mentions '2026', all queries must mention '2026')\n"+
			"- Do NOT drift to related topics, subtopics, or broader categories\n"+
			"- Vary only the angle: rankings, reviews, lists, comparisons, official sources\n"+
			"- No near-duplicate phrasings\n"+
			"Return ONLY a JSON array of strings. No preamble, no markdown, no explanation.\n\n"+
			"Example — intent: 'top movies 2026'\n"+
			"Good: [\"best movies 2026\", \"highest grossing films 2026\", \"top rated movies 2026 list\"]\n"+
			"Bad: [\"best actors 2025\", \"Hollywood news\", \"streaming services 2026\"]\n\n"+
			"Intent: %s",
		count, intent,
	)

	return callMistral(prompt, 300)
}

// ---------------------------------------------------------------- relevance filter --

func filterRelevant(intent string, results []serpResult) []serpResult {
	if len(results) == 0 {
		return results
	}
	if len(mistralKeys) == 0 {
		var out []serpResult
		for _, r := range results {
			out = append(out, r)
		}
		return out
	}

	indexes, err := filterRelevantIndexes(intent, results)
	if err != nil {
		log.Printf("[serp] relevance filter failed — returning all results: %v", err)
		return results
	}

	var out []serpResult
	for _, idx := range indexes {
		if idx >= 0 && idx < len(results) {
			out = append(out, results[idx])
		}
	}

	if len(out) == 0 {
		log.Printf("[serp] relevance filter returned 0 — falling back to all results")
		return results
	}

	return out
}

func filterRelevantIndexes(intent string, results []serpResult) ([]int, error) {
	var sb strings.Builder
	for i, r := range results {
		sb.WriteString(fmt.Sprintf("%d. Title: %s\n   URL: %s\n   Snippet: %s\n\n",
			i, r.Title, r.URL, r.Snippet))
	}

	prompt := fmt.Sprintf(
		"You are a relevance filter. The user wants data about: \"%s\"\n\n"+
			"Below is a numbered list of URLs (0-indexed). Return ONLY the indexes of URLs that are clearly relevant to the intent.\n"+
			"A URL is relevant if its title and snippet directly match the topic, subject, and timeframe of the intent.\n"+
			"Be strict — when in doubt, exclude.\n"+
			"Return ONLY a JSON array of integers. No explanation, no markdown.\n"+
			"Example: [0, 2, 5, 7]\n\n"+
			"URLs:\n%s",
		intent, sb.String(),
	)

	body := map[string]interface{}{
		"model":       "mistral-small-latest",
		"max_tokens":  300,
		"temperature": 0.1,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	}

	b, _ := json.Marshal(body)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, "POST", "https://api.mistral.ai/v1/chat/completions", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+nextMistralKey())

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(httpReq)
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
		return nil, fmt.Errorf("parse mistral response: %w", err)
	}
	if result.Error.Message != "" {
		return nil, fmt.Errorf("mistral error: %s", result.Error.Message)
	}
	if len(result.Choices) == 0 {
		return nil, fmt.Errorf("mistral: no choices")
	}

	raw := strings.TrimSpace(result.Choices[0].Message.Content)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	var indexes []int
	if err := json.Unmarshal([]byte(raw), &indexes); err != nil {
		return nil, fmt.Errorf("parse indexes: %w", err)
	}

	return indexes, nil
}

// ---------------------------------------------------------------- mistral helper --

func callMistral(prompt string, maxTokens int) ([]string, error) {
	body := map[string]interface{}{
		"model":       "mistral-small-latest",
		"max_tokens":  maxTokens,
		"temperature": 0.1,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	}

	b, _ := json.Marshal(body)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.mistral.ai/v1/chat/completions", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+nextMistralKey())

	client := &http.Client{Timeout: 20 * time.Second}
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
		return nil, fmt.Errorf("bad mistral response: %w", err)
	}
	if result.Error.Message != "" {
		return nil, fmt.Errorf("mistral error: %s", result.Error.Message)
	}
	if len(result.Choices) == 0 {
		return nil, fmt.Errorf("mistral: no choices")
	}

	raw := strings.TrimSpace(result.Choices[0].Message.Content)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	return out, nil
}

// ---------------------------------------------------------------- retryEngine --

func retryEngine(name string, fn searchFn, query string, limit int) ([]serpResult, error) {
	var lastErr error
	for i := 0; i < engineRetries; i++ {
		results, err := fn(query, limit)
		if err == nil && len(results) > 0 {
			return results, nil
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

func searchDDG(query string, limit int) ([]serpResult, error) {
	doc, err := fetchDoc("ddg", "https://html.duckduckgo.com/html/?q="+url.QueryEscape(query))
	if err != nil {
		return nil, err
	}

	for _, sel := range ddgSelectors {
		var results []serpResult
		doc.Find(sel).Each(func(i int, s *goquery.Selection) {
			if len(results) >= limit {
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
			title := strings.TrimSpace(s.Text())
			snippet := ""
			if parent := s.Closest(".result"); parent.Length() > 0 {
				snippet = strings.TrimSpace(parent.Find(".result__snippet").Text())
			}
			results = append(results, serpResult{URL: href, Title: title, Snippet: snippet})
		})
		if len(results) > 0 {
			log.Printf("[serp] ddg selector %q returned %d results", sel, len(results))
			return results, nil
		}
	}

	return nil, fmt.Errorf("ddg: all selectors returned 0 results")
}



func searchSerper(query string, limit int) ([]serpResult, error) {
	key := nextSerperKey()
	if key == "" {
		return nil, fmt.Errorf("no serper key available")
	}

	num := limit
	if num > 20 {
		num = 20
	}

	payload, err := json.Marshal(map[string]interface{}{"q": query, "num": num})
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
			Link    string `json:"link"`
			Title   string `json:"title"`
			Snippet string `json:"snippet"`
		} `json:"organic"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("serper: failed to parse response: %w", err)
	}

	var results []serpResult
	for _, item := range result.Organic {
		if item.Link != "" {
			results = append(results, serpResult{
				URL:     item.Link,
				Title:   item.Title,
				Snippet: item.Snippet,
			})
		}
		if len(results) >= limit {
			break
		}
	}
	return results, nil
}

// ---------------------------------------------------------------- wikipedia --

func generateWikipediaQueries(intent string, count int) ([]string, error) {
	if len(mistralKeys) == 0 {
		return []string{intent}, nil
	}

	prompt := fmt.Sprintf(
		"Convert the following intent into exactly %d Wikipedia article titles.\n"+
			"Use the exact format Wikipedia uses for article titles: capitalized, specific, encyclopedic.\n"+
			"Choose titles that would realistically exist as Wikipedia articles for this specific topic.\n"+
			"Return ONLY a JSON array of strings. No preamble, no markdown, no explanation.\n\n"+
			"Example output: [\"Topic name\", \"Related concept\", \"Broader category\"]\n\n"+
			"Intent: %s",
		count, intent,
	)

	return callMistral(prompt, 100)
}

func searchWikipedia(query string, limit int) ([]serpResult, error) {
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
				Title   string `json:"title"`
				Snippet string `json:"snippet"`
			} `json:"search"`
		} `json:"query"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("wikipedia: parse error: %w", err)
	}

	var results []serpResult
	for _, item := range result.Query.Search {
		articleURL := "https://en.wikipedia.org/wiki/" + url.PathEscape(strings.ReplaceAll(item.Title, " ", "_"))
		results = append(results, serpResult{
			URL:     articleURL,
			Title:   item.Title,
			Snippet: item.Snippet,
		})
		if len(results) >= limit {
			break
		}
	}

	log.Printf("[serp/wikipedia] query %q returned %d urls", query, len(results))
	return results, nil
}

func fetchWikipedia(intent string, limit int, globalSeen map[string]struct{}) ([]serpResult, error) {
	queries, err := generateWikipediaQueries(intent, 3)
	if err != nil {
		log.Printf("[serp/wikipedia] query generation failed, using intent directly: %v", err)
		queries = []string{intent}
	}

	perQuery := limit / len(queries)
	if perQuery < 1 {
		perQuery = 1
	}

	var allResults []serpResult
	for _, q := range queries {
		results, err := searchWikipedia(q, perQuery+3)
		if err != nil {
			log.Printf("[serp/wikipedia] query %q failed: %v", q, err)
			continue
		}
		for _, r := range results {
			if _, dup := globalSeen[r.URL]; dup {
				continue
			}
			globalSeen[r.URL] = struct{}{}
			allResults = append(allResults, r)
			if len(allResults) >= limit {
				break
			}
		}
		if len(allResults) >= limit {
			break
		}
	}

	return allResults, nil
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

func filterResults(results []serpResult, seen map[string]struct{}, limit int) []serpResult {
	if limit <= 0 {
		return nil
	}
	var out []serpResult
	for _, r := range results {
		if len(out) >= limit {
			break
		}
		if _, dup := seen[r.URL]; dup {
			continue
		}
		if isBlockedURL(r.URL) {
			continue
		}
		out = append(out, r)
	}
	return out
}