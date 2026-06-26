package reddit

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const concurrencyLimit = 5
const concurrencyThreshold = 10

type FetchResult struct {
	URL   string
	Bytes []byte
	Err   error
}

// FetchRaw fetches raw .json bytes from a single Reddit URL.
func FetchRaw(rawURL string) ([]byte, error) {
	jsonURL := toJSONURL(rawURL)
	log.Printf("[reddit/fetch] fetching %s", jsonURL)
	body, err := fetchRedditJSON(jsonURL)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	time.Sleep(burstDelay)
	return body, nil
}

// FetchRawBatch fetches multiple URLs. Uses concurrency if len(urls) > 10.
func FetchRawBatch(urls []string) []FetchResult {
	if len(urls) <= concurrencyThreshold {
		var results []FetchResult
		for _, u := range urls {
			b, err := FetchRaw(u)
			results = append(results, FetchResult{URL: u, Bytes: b, Err: err})
		}
		return results
	}

	results := make([]FetchResult, len(urls))
	sem := make(chan struct{}, concurrencyLimit)
	var wg sync.WaitGroup

	for i, u := range urls {
		wg.Add(1)
		go func(idx int, rawURL string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			jsonURL := toJSONURL(rawURL)
			log.Printf("[reddit/fetch] fetching %s", jsonURL)
			body, err := fetchRedditJSON(jsonURL)
			if err != nil {
				results[idx] = FetchResult{URL: rawURL, Err: fmt.Errorf("fetch: %w", err)}
				return
			}
			time.Sleep(time.Duration(rateLimit) * time.Second)
			results[idx] = FetchResult{URL: rawURL, Bytes: body}
		}(i, u)
	}

	wg.Wait()
	return results
}

// FetchSubredditPage fetches a single page and returns posts + the after token for pagination.
func FetchSubredditPage(rawURL string) ([]RedditPost, string, int, error) {
	jsonURL := toJSONURL(rawURL)
	log.Printf("[reddit/fetch] fetching subreddit page %s", jsonURL)
	body, err := fetchRedditJSON(jsonURL)
	if err != nil {
		return nil, "", 0, fmt.Errorf("fetch: %w", err)
	}

	// Comment pages return a JSON array [listing, comments]
	// We only need the first element (the post listing)
	if len(body) > 0 && body[0] == '[' {
		var arr []RedditListing
		if err := json.Unmarshal(body, &arr); err != nil {
			return nil, "", 0, fmt.Errorf("parse comment array: %w", err)
		}
		if len(arr) == 0 {
			return nil, "", 0, nil
		}
		var posts []RedditPost
		for _, child := range arr[0].Data.Children {
			if child.Kind == "t3" {
				posts = append(posts, child.Data)
			}
		}
		time.Sleep(burstDelay)
		return posts, "", 0, nil
	}

	// Normal listing (subreddit feed, search results)
	var listing RedditListing
	if err := json.Unmarshal(body, &listing); err != nil {
		return nil, "", 0, fmt.Errorf("parse listing: %w", err)
	}
	var posts []RedditPost
	for _, child := range listing.Data.Children {
		if child.Kind == "t3" {
			posts = append(posts, child.Data)
		}
	}
	time.Sleep(burstDelay)
	return posts, listing.Data.After, listing.Data.Dist, nil
}

// FetchSubreddit keeps the old signature for backward compatibility.
func FetchSubreddit(rawURL string) ([]RedditPost, error) {
	posts, _, _, err := FetchSubredditPage(rawURL)
	return posts, err
}

func toJSONURL(rawURL string) string {
    u := strings.TrimRight(rawURL, "/")

    parsed, err := url.Parse(u)
    if err == nil && !strings.HasSuffix(parsed.Path, ".json") {
        parsed.Path += ".json"
        u = parsed.String()
    }

    if strings.Contains(u, "/comments/") {
        // preserve existing query params
        parsed, err := url.Parse(u)
        if err == nil {
            q := parsed.Query()
            q.Set("depth", fmt.Sprintf("%d", commentDepth))
            parsed.RawQuery = q.Encode()
            u = parsed.String()
        }
    }
    return u
}

func IsSubredditURL(u string) bool {
	return !strings.Contains(u, "/comments/")
}

func fetchRedditJSON(endpoint string) ([]byte, error) {
	globalLimiter.acquire()

	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", redditUserAgent)
	req.Header.Set("Accept", "application/json")

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// First attempt always goes direct. On any 429/403 via direct,
		// subsequent attempts fall through to the proxy pool.
		useDirect := attempt == 1 && len(globalProxyPool.proxies) == 0 || attempt == 1
		var currentProxyURL string
		var client *http.Client

		if useDirect || len(globalProxyPool.proxies) == 0 {
			client = &http.Client{Timeout: time.Duration(requestTimeout) * time.Second}
		} else {
			px := globalProxyPool.Pick()
			if px != nil {
				currentProxyURL = px.URL
				proxyURL, err := url.Parse(currentProxyURL)
				if err == nil {
					client = &http.Client{
						Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)},
						Timeout:   time.Duration(requestTimeout) * time.Second,
					}
				} else {
					client = &http.Client{Timeout: time.Duration(requestTimeout) * time.Second}
				}
			} else {
				client = &http.Client{Timeout: time.Duration(requestTimeout) * time.Second}
			}
		}

		source := "direct"
		if currentProxyURL != "" {
			source = currentProxyURL
		}

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[reddit/fetch] request error via %s (attempt %d/%d): %v", source, attempt, maxRetries, err)
			if currentProxyURL != "" {
				globalProxyPool.MarkFailure(currentProxyURL)
			}
			continue
		}

		// --- 429 / 403 handling ---
		// Reddit returns 429 when rate limited and 403 when it has soft-banned
		// the requesting IP after repeated rate limit hits. Both are treated the
		// same way: mark the current path as failed and retry via a proxy.
		if resp.StatusCode == 429 || resp.StatusCode == 403 {
			resp.Body.Close()

			if currentProxyURL != "" {
				globalProxyPool.MarkFailure(currentProxyURL)
			}

			if resp.StatusCode == 429 {
				// Respect Retry-After if Reddit sends one, otherwise use retryDelay.
				// retryDelay should be at least 30s — Reddit's soft-ban window is
				// longer than the 5s default many people assume.
				wait := retryDelay
				if ra := resp.Header.Get("Retry-After"); ra != "" {
					if secs, err := time.ParseDuration(ra + "s"); err == nil {
						wait = secs
					}
				}
				log.Printf("[reddit/fetch] rate limited (429) via %s — waiting %s (attempt %d/%d)", source, wait, attempt, maxRetries)
				time.Sleep(wait)

				// Drain the token bucket so the next acquire() doesn't immediately
				// fire another request into a rate-limited window.
				globalLimiter.mu.Lock()
				globalLimiter.tokens = 0
				globalLimiter.mu.Unlock()
			} else {
				// 403: IP is soft-banned. No point waiting long — just skip to proxy.
				log.Printf("[reddit/fetch] forbidden (403) via %s — retrying via proxy (attempt %d/%d)", source, attempt, maxRetries)
			}

			globalLimiter.acquire()
			continue
		}

		// Any other 4xx/5xx is a hard failure for this endpoint.
		if resp.StatusCode >= 400 {
			resp.Body.Close()
			if currentProxyURL != "" {
				globalProxyPool.MarkFailure(currentProxyURL)
			}
			return nil, fmt.Errorf("reddit HTTP %d for %s", resp.StatusCode, endpoint)
		}

		if currentProxyURL != "" {
			globalProxyPool.MarkSuccess(currentProxyURL)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		log.Printf("[reddit/fetch] success via %s", source)
		return body, nil
	}

	return nil, fmt.Errorf("reddit failed after %d retries: %s", maxRetries, endpoint)
}