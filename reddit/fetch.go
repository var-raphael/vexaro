package reddit

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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

	// Concurrent path — semaphore of 5
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
	if !strings.HasSuffix(u, ".json") {
		u += ".json"
	}
	if strings.Contains(u, "/comments/") {
		u += fmt.Sprintf("?depth=%d", commentDepth)
	}
	return u
}

func IsSubredditURL(u string) bool {
	return !strings.Contains(u, "/comments/")
}

func fetchRedditJSON(endpoint string) ([]byte, error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", redditUserAgent)
	req.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: time.Duration(requestTimeout) * time.Second}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode == 429 {
			retryAfter := resp.Header.Get("Retry-After")
			wait := retryDelay
			if retryAfter != "" {
				if secs, err := time.ParseDuration(retryAfter + "s"); err == nil {
					wait = secs
				}
			}
			log.Printf("[reddit/fetch] rate limited — waiting %s (attempt %d/%d)", wait, attempt, maxRetries)
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("reddit HTTP %d for %s", resp.StatusCode, endpoint)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return body, nil
	}

	return nil, fmt.Errorf("reddit rate limited after %d retries: %s", maxRetries, endpoint)
}