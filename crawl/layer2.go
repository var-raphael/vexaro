package crawl

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

var embeddedDataPatterns = []*regexp.Regexp{
	regexp.MustCompile(`window\.__INITIAL_STATE__\s*=\s*({.+?});`),
	regexp.MustCompile(`window\.__NEXT_DATA__\s*=\s*({.+?});`),
	regexp.MustCompile(`window\.__NUXT__\s*=\s*({.+?});`),
	regexp.MustCompile(`window\.DATA\s*=\s*({.+?});`),
	regexp.MustCompile(`window\.__STATE__\s*=\s*({.+?});`),
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

// layer2 scans the already-fetched HTML for embedded data and API endpoints.
// It does not re-fetch the page — it works on what layer1 already returned.
func layer2(targetURL, html string) (string, error) {
	// Step 1: look for window.__NEXT_DATA__ and similar embedded state
	// no network calls here — no jitter needed
	for _, pattern := range embeddedDataPatterns {
		match := pattern.FindString(html)
		if match == "" {
			continue
		}
		start := strings.Index(match, "{")
		if start == -1 {
			continue
		}
		jsonStr := match[start:]
		if isValidJSON(jsonStr) {
			return jsonStr, nil
		}
	}

	// Step 2: JSON-LD structured data
	// no network calls here — no jitter needed
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err == nil {
		var jsonLD string
		doc.Find(`script[type="application/ld+json"]`).Each(func(i int, s *goquery.Selection) {
			if jsonLD == "" {
				text := strings.TrimSpace(s.Text())
				if isValidJSON(text) {
					jsonLD = text
				}
			}
		})
		if jsonLD != "" {
			return jsonLD, nil
		}

		// Step 3: WordPress REST API detection — makes a network call
		isWP := strings.Contains(targetURL, "wp-") ||
			doc.Find(`meta[name="generator"]`).AttrOr("content", "") != ""
		if isWP {
			wpEndpoint := baseURL(targetURL) + "/wp-json/wp/v2/posts"
			jitterBetween(0.3, 1.0)
			if result := probeEndpoint(wpEndpoint); result != "" {
				return result, nil
			}
		}
	}

	// Step 4: scan scripts for API endpoint patterns, probe each one
	// jitter applied per probe since each is a real network request
	endpoints := extractEndpoints(html, targetURL)
	for _, endpoint := range endpoints {
		jitterBetween(0.3, 1.0)
		if result := probeEndpoint(endpoint); result != "" {
			return result, nil
		}
	}

	return "", fmt.Errorf("layer2: no embedded data or API found")
}

func extractEndpoints(html, rawBase string) []string {
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
			seen[resolved] = true
			endpoints = append(endpoints, resolved)
		}
	}
	return endpoints
}

// probeEndpoint hits a discovered API endpoint and returns JSON if found.
// Uses TLS client for consistency with layer1.
func probeEndpoint(targetURL string) string {
	client := newTLSClient()

	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		return ""
	}

	ua := randomUserAgent()
	setJSONHeaders(req, ua)

	// shorter timeout for endpoint probing
	client.Timeout = 10 * time.Second

	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		return ""
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil || len(body) < 100 {
		return ""
	}

	if isValidJSON(string(body)) {
		return string(body)
	}
	return ""
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