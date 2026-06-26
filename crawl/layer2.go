package crawl

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"

	"github.com/Danny-Dasilva/CycleTLS/cycletls"
	"github.com/PuerkitoBio/goquery"
)

const maxProbeBodyBytes = 1 << 20 // 1MB

var embeddedDataPatterns = []string{
	"window.__INITIAL_STATE__",
	"window.__NEXT_DATA__",
	"window.__NUXT__",
	"window.__NUXT_DATA__",
	"window.DATA",
	"window.__STATE__",
	"window.__REDUX_STATE__",
	"window.__PRELOADED_STATE__",
	"window.__APP_STATE__",
	"window.__DATA__",
}

var apiEndpointPatterns = []*regexp.Regexp{
	regexp.MustCompile(`fetch\(['"]([^'"]+)['"]\)`),
	regexp.MustCompile(`axios\.(?:get|post|put)\(['"]([^'"]+)['"]\)`),
	regexp.MustCompile(`\.open\(['"](?:GET|POST)['"],\s*['"]([^'"]+)['"]\)`),
	regexp.MustCompile(`['"](/api/[^'"]+)['"]`),
	regexp.MustCompile(`['"](/v1/[^'"]+)['"]`),
	regexp.MustCompile(`['"](/v2/[^'"]+)['"]`),
	regexp.MustCompile(`['"](/graphql[^'"]*?)['"]`),
	regexp.MustCompile(`['"]([^'"]+\.json)['"]`),
}

var graphqlSignals = []string{
	"__typename",
	"graphql",
	"apolloclient",
	"apollo-client",
	"urql",
}

// isUsefulLayer2Result checks that a layer2 result is actually content
// and not a JS bundle manifest, chunk list, or other garbage.
func isUsefulLayer2Result(s string) bool {
	if len(s) < 200 {
		return false
	}

	// Reject Next.js chunk manifests — array of JS filenames
	if strings.HasPrefix(strings.TrimSpace(s), "[") {
		lower := strings.ToLower(s)
		if strings.Contains(lower, "/_next/static/chunks/") ||
			strings.Contains(lower, ".js?dpl=") ||
			strings.Contains(lower, "webpack") {
			return false
		}
	}

	// Must have at least one meaningful content signal
	lower := strings.ToLower(s)
	signals := []string{
		`"articlebody"`,
		`"text":`,
		`"description":`,
		`"headline":`,
		`"content":`,
		`"body":`,
		`"name":`,
		`"title":`,
		`"job"`,
		`"company"`,
		`"salary"`,
		`"location"`,
		`"price"`,
		`"product"`,
		`"author"`,
		`"date"`,
	}
	for _, sig := range signals {
		if strings.Contains(lower, sig) {
			return true
		}
	}
	return false
}

func layer2(targetURL, html string) (string, error) {
	// Step 1: embedded state objects — try both { and [
	for _, key := range embeddedDataPatterns {
		jsonStr := extractEmbeddedState(html, key)
		if jsonStr != "" && isValidJSON(jsonStr) && isUsefulLayer2Result(jsonStr) {
			return jsonStr, nil
		}
	}

	// Step 2: Next.js 13+ app router — self.__next_f chunks
	if nextData := extractNextFChunks(html); nextData != "" && isUsefulLayer2Result(nextData) {
		return nextData, nil
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))

	if err == nil {
		// Step 3: JSON-LD structured data
		var jsonLD string
		doc.Find(`script[type="application/ld+json"]`).Each(func(i int, s *goquery.Selection) {
			if jsonLD != "" {
				return
			}
			text := strings.TrimSpace(s.Text())
			if isValidJSON(text) && hasUsefulContent(text) {
				jsonLD = text
			}
		})
		if jsonLD != "" {
			return jsonLD, nil
		}

		// Step 4: WordPress REST API — slug-targeted
		if isWordPress(doc) {
			slug := extractSlug(targetURL)
			if slug != "" {
				endpoints := []string{
					baseURL(targetURL) + "/wp-json/wp/v2/posts?slug=" + slug,
					baseURL(targetURL) + "/wp-json/wp/v2/pages?slug=" + slug,
				}
				for _, ep := range endpoints {
					jitterBetween(0.3, 1.0)
					if result := probeEndpoint(ep); result != "" && isUsefulLayer2Result(result) {
						return result, nil
					}
				}
			}
		}

		// Step 5: og:url or canonical as alternate fetch target
		alternateURL := extractAlternateURL(doc, targetURL)
		if alternateURL != "" && alternateURL != targetURL {
			jitterBetween(0.3, 1.0)
			if result := probeEndpoint(alternateURL); result != "" && isUsefulLayer2Result(result) {
				return result, nil
			}
		}

		// Step 6: robots.txt probe for API path hints
		if apiBase := probeRobotsTxt(targetURL); apiBase != "" {
			slug := extractSlug(targetURL)
			if slug != "" {
				ep := apiBase + "/" + slug
				jitterBetween(0.3, 1.0)
				if result := probeEndpoint(ep); result != "" && isUsefulLayer2Result(result) {
					return result, nil
				}
			}
		}
	}

	// Step 7: discovered API endpoints from HTML — parallel
	endpoints := extractEndpoints(html, targetURL)

	// Step 8: common API path guesses
	guesses := guessAPIEndpoints(targetURL)
	endpoints = append(endpoints, guesses...)

	// Step 9: GraphQL probe if signals detected
	if hasGraphQLSignals(html) {
		gqlEndpoint := baseURL(targetURL) + "/graphql"
		endpoints = append(endpoints, gqlEndpoint)
	}

	if len(endpoints) > 0 {
		if result := probeEndpointsParallel(endpoints); result != "" && isUsefulLayer2Result(result) {
			return result, nil
		}
	}

	return "", fmt.Errorf("layer2: no useful content found")
}

// ---------------------------------------------------------------- Next.js 13+ --

func extractNextFChunks(html string) string {
	re := regexp.MustCompile(`self\.__next_f\.push\(\[(\d+),(.+?)\]\)`)
	matches := re.FindAllStringSubmatch(html, -1)
	if len(matches) == 0 {
		return ""
	}

	var parts []string
	for _, m := range matches {
		if len(m) < 3 {
			continue
		}
		payload := strings.TrimSpace(m[2])
		var unwrapped string
		if err := json.Unmarshal([]byte(payload), &unwrapped); err == nil {
			parts = append(parts, unwrapped)
		} else {
			parts = append(parts, payload)
		}
	}

	if len(parts) == 0 {
		return ""
	}

	combined := strings.Join(parts, "")

	for i, ch := range combined {
		if ch == '{' || ch == '[' {
			candidate := combined[i:]
			if isValidJSON(candidate) {
				return candidate
			}
			if extracted := extractBalanced(candidate, ch); extracted != "" && isValidJSON(extracted) {
				return extracted
			}
		}
	}

	return ""
}

// ---------------------------------------------------------------- embedded state --

func extractEmbeddedState(html, key string) string {
	idx := strings.Index(html, key)
	if idx == -1 {
		return ""
	}
	rest := html[idx+len(key):]

	var opener rune
	var openerIdx int
	found := false
	for i, ch := range rest {
		if ch == '{' || ch == '[' {
			opener = ch
			openerIdx = i
			found = true
			break
		}
	}
	if !found {
		return ""
	}

	return extractBalanced(rest[openerIdx:], opener)
}

func extractBalanced(s string, opener rune) string {
	depth := 0
	inString := false
	escape := false

	for i, ch := range s {
		if escape {
			escape = false
			continue
		}
		switch ch {
		case '\\':
			if inString {
				escape = true
			}
		case '"':
			inString = !inString
		case '{', '[':
			if !inString {
				depth++
			}
		case '}', ']':
			if !inString {
				depth--
				if depth == 0 {
					return s[:i+1]
				}
			}
		}
	}
	return ""
}

// ---------------------------------------------------------------- parallel probing --

func probeEndpointsParallel(endpoints []string) string {
	const maxWorkers = 3

	type result struct {
		body string
		idx  int
	}

	sem := make(chan struct{}, maxWorkers)
	results := make(chan result, len(endpoints))
	var wg sync.WaitGroup

	for i, ep := range endpoints {
		wg.Add(1)
		go func(idx int, endpoint string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			body := probeEndpoint(endpoint)
			if body != "" {
				results <- result{body, idx}
			}
		}(i, ep)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	best := result{idx: len(endpoints)}
	for r := range results {
		if r.idx < best.idx {
			best = r
		}
	}

	return best.body
}

// ---------------------------------------------------------------- GraphQL --

func hasGraphQLSignals(html string) bool {
	lower := strings.ToLower(html)
	for _, signal := range graphqlSignals {
		if strings.Contains(lower, signal) {
			return true
		}
	}
	return false
}

func probeGraphQL(endpoint string) string {
	client := newCycleTLSClient()
	defer client.Close()

	ua := randomUserAgent()
	opts := jsonOptions(ua)
	opts.Timeout = 10
	opts.Body = `{"query":"{__schema{queryType{name}}}"}`

	resp, err := client.Do(endpoint, opts, "POST")
	if err != nil {
		return ""
	}

	body := resp.Body
	if len(body) < 100 || len(body) > maxProbeBodyBytes {
		return ""
	}
	if strings.Contains(body, `"__schema"`) && isValidJSON(body) {
		return body
	}
	return ""
}

func probeRobotsTxt(targetURL string) string {
	base := baseURL(targetURL)
	robotsURL := base + "/robots.txt"

	client := newCycleTLSClient()
	defer client.Close()

	ua := randomUserAgent()
	opts := defaultOptions(ua)
	opts.Timeout = 5

	resp, err := client.Do(robotsURL, opts, "GET")
	if err != nil || resp.Status != 200 {
		return ""
	}

	lines := strings.Split(resp.Body, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		lower := strings.ToLower(line)
		if !strings.HasPrefix(lower, "disallow:") && !strings.HasPrefix(lower, "allow:") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) < 2 {
			continue
		}
		path := strings.TrimSpace(parts[1])
		if strings.HasPrefix(path, "/api") ||
			strings.HasPrefix(path, "/v1") ||
			strings.HasPrefix(path, "/v2") ||
			strings.HasPrefix(path, "/graphql") {
			return base + path
		}
	}
	return ""
}

// ---------------------------------------------------------------- helpers --

func extractSlug(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	for i := len(parts) - 1; i >= 0; i-- {
		if parts[i] != "" {
			return parts[i]
		}
	}
	return ""
}

func extractAlternateURL(doc *goquery.Document, original string) string {
	if content, exists := doc.Find(`meta[property="og:url"]`).Attr("content"); exists && content != "" {
		return content
	}
	if href, exists := doc.Find(`link[rel="canonical"]`).Attr("href"); exists && href != "" {
		return href
	}
	return ""
}

func guessAPIEndpoints(rawURL string) []string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil
	}

	base := u.Scheme + "://" + u.Host
	path := strings.Trim(u.Path, "/")
	slug := extractSlug(rawURL)

	var guesses []string

	if path != "" {
		guesses = append(guesses, base+"/"+path+".json")
	}

	if slug != "" {
		guesses = append(guesses,
			base+"/api/posts/"+slug,
			base+"/api/articles/"+slug,
			base+"/api/content/"+slug,
			base+"/api/v1/posts/"+slug,
			base+"/api/v2/posts/"+slug,
			base+"/ghost/api/content/posts/slug/"+slug+"/",
		)
	}

	return guesses
}

func isWordPress(doc *goquery.Document) bool {
	found := false
	doc.Find("script[src], link[href]").Each(func(i int, s *goquery.Selection) {
		src, _ := s.Attr("src")
		href, _ := s.Attr("href")
		if strings.Contains(src, "/wp-content/") || strings.Contains(src, "/wp-includes/") ||
			strings.Contains(href, "/wp-content/") || strings.Contains(href, "/wp-includes/") {
			found = true
		}
	})
	return found
}

func hasUsefulContent(jsonStr string) bool {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return false
	}
	if len(jsonStr) < 200 {
		return false
	}
	lower := strings.ToLower(jsonStr)
	signals := []string{
		`"articlebody"`,
		`"text":`,
		`"description":`,
		`"headline":`,
		`"content":`,
		`"body":`,
		`"name":`,
		`"title":`,
	}
	for _, sig := range signals {
		idx := strings.Index(lower, sig)
		if idx != -1 && len(jsonStr)-idx > 100 {
			return true
		}
	}
	return false
}

func extractEndpoints(html, rawBase string) []string {
	base, err := url.Parse(rawBase)
	if err != nil {
		return nil
	}

	var endpoints []string
	seen := map[string]bool{}

	for _, pattern := range apiEndpointPatterns {
		matches := pattern.FindAllStringSubmatch(html, -1)
		for _, match := range matches {
			if len(match) < 2 {
				continue
			}
			resolved := resolveURL(rawBase, match[len(match)-1])
			if resolved == "" || seen[resolved] {
				continue
			}
			u, err := url.Parse(resolved)
			if err != nil || u.Host != base.Host {
				continue
			}
			seen[resolved] = true
			endpoints = append(endpoints, resolved)
		}
	}
	return endpoints
}

func probeEndpoint(targetURL string) string {
	const maxRetries = 2

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			jitterBetween(0.5, 1.5)
		}

		client := newCycleTLSClient()
		body, ok := doProbe(client, targetURL)
		client.Close()

		if ok {
			return body
		}
	}
	return ""
}

func doProbe(client cycletls.CycleTLS, targetURL string) (string, bool) {
	ua := randomUserAgent()
	opts := jsonOptions(ua)
	opts.Timeout = 10

	resp, err := client.Do(targetURL, opts, "GET")
	if err != nil {
		return "", false
	}

	if resp.Status < 200 || resp.Status >= 300 {
		return "", false
	}

	ct := resp.Headers["Content-Type"]
	if !strings.Contains(ct, "application/json") &&
		!strings.Contains(ct, "text/plain") {
		return "", false
	}

	body := resp.Body
	if len(body) < 100 {
		return "", false
	}
	if len(body) > maxProbeBodyBytes {
		body = body[:maxProbeBodyBytes]
	}

	if isValidJSON(body) {
		return body, true
	}
	return "", false
}

func isValidJSON(s string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(s), &js) == nil
}

func resolveURL(rawBase, href string) string {
	if href == "" {
		return ""
	}
	b, err := url.Parse(rawBase)
	if err != nil {
		return ""
	}
	r, err := url.Parse(href)
	if err != nil {
		return ""
	}
	resolved := b.ResolveReference(r).String()
	u, err := url.Parse(resolved)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return ""
	}
	return resolved
}

func baseURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return u.Scheme + "://" + u.Host
}