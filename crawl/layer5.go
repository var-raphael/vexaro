package crawl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/var-raphael/vexaro-engine/db"
)

const (
	apifyBaseURL  = "https://api.apify.com/v2"
	apifyActorID  = "apify~playwright-scraper"
	apifyPollWait = 5 * time.Second
	apifyTimeout  = 3 * time.Minute
	apifyBatchMax = 30 // max URLs per single Apify run
)

type apifyRunInput struct {
	StartURLs           []map[string]string `json:"startUrls"`
	PageFunction        string              `json:"pageFunction"`
	MaxRequestsPerCrawl int                 `json:"maxRequestsPerCrawl"`
	MemoryMbytes        int                 `json:"memoryMbytes"`
}

// ---------------------------------------------------------- public entry points --

// tryLayer5Bulk sends all queued URLs to Apify in batches and saves results.
func tryLayer5Bulk(apiName string, failedURLs []CrawlURL) {
	database := db.Get()

	// Split into batches of apifyBatchMax
	for i := 0; i < len(failedURLs); i += apifyBatchMax {
		end := i + apifyBatchMax
		if end > len(failedURLs) {
			end = len(failedURLs)
		}
		batch := failedURLs[i:end]

		log.Printf("[layer5] running batch %d-%d (%d urls)", i+1, end, len(batch))

		urls := make([]string, len(batch))
		for j, u := range batch {
			urls[j] = u.URL
		}

		results, err := layer5(urls)
		if err != nil {
			log.Printf("[layer5] batch failed: %v", err)
			for _, u := range batch {
				markFailed(database, u.QueueID, fmt.Sprintf("layer5: %v", err))
			}
			continue
		}

		for _, u := range batch {
			html, ok := results[u.URL]
			if !ok || html == "" {
				log.Printf("[layer5] no result for url=%s", u.URL)
				markFailed(database, u.QueueID, "layer5: no result returned for url")
				continue
			}
			if err := extractAndSave(apiName, u, html, "layer5"); err != nil {
				log.Printf("[layer5] extractAndSave failed url=%s: %v", u.URL, err)
				markFailed(database, u.QueueID, fmt.Sprintf("layer5 extract: %v", err))
				continue
			}
			markProceedClean(database, u.QueueID)
			log.Printf("[layer5] done — queue_id=%d url=%s", u.QueueID, u.URL)
		}
	}
}

// layer5Single is used by FetchRaw for ad-hoc single URL fetches.
func layer5Single(url string) (string, error) {
	results, err := layer5([]string{url})
	if err != nil {
		return "", fmt.Errorf("layer5 single: %w", err)
	}
	html, ok := results[url]
	if !ok || html == "" {
		return "", fmt.Errorf("layer5 single: no result for url %s", url)
	}
	return html, nil
}

// ---------------------------------------------------------- core Apify call --

func layer5(urls []string) (map[string]string, error) {
	apiToken := os.Getenv("APIFY_API_TOKEN")
	if apiToken == "" {
		return nil, fmt.Errorf("APIFY_API_TOKEN not set")
	}

	startURLs := make([]map[string]string, len(urls))
	for i, u := range urls {
		startURLs[i] = map[string]string{"url": u}
	}

	input := apifyRunInput{
		StartURLs:           startURLs,
		MaxRequestsPerCrawl: len(urls),
		MemoryMbytes:        2048,
		PageFunction: `async ({ page, request, log }) => {
			await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => {});
			const html = await page.content();
			log.info('scraped url=' + request.url + ' len=' + html.length);
			return { url: request.url, html };
		}`,
	}

	body, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal input: %w", err)
	}

	runURL := fmt.Sprintf("%s/acts/%s/runs?token=%s&waitForFinish=60", apifyBaseURL, apifyActorID, apiToken)
	resp, err := http.Post(runURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("apify run request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("apify run returned %d: %s", resp.StatusCode, string(b))
	}

	var runResp struct {
		Data struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&runResp); err != nil {
		return nil, fmt.Errorf("decode run response: %w", err)
	}

	runID := runResp.Data.ID
	log.Printf("[layer5] run started id=%s urls=%d", runID, len(urls))

	// Poll until done
	deadline := time.Now().Add(apifyTimeout)
	for time.Now().Before(deadline) {
		status, results, err := apifyPollResults(apiToken, runID)
		if err != nil {
			return nil, err
		}
		switch status {
		case "SUCCEEDED":
			log.Printf("[layer5] run succeeded id=%s got=%d/%d results", runID, len(results), len(urls))
			return results, nil
		case "FAILED", "ABORTED", "TIMED-OUT":
			return nil, fmt.Errorf("apify run ended with status: %s", status)
		}
		time.Sleep(apifyPollWait)
	}

	return nil, fmt.Errorf("apify layer5 timed out after %s for run %s", apifyTimeout, runID)
}

// ---------------------------------------------------------- polling helpers --

func apifyPollResults(token, runID string) (status string, results map[string]string, err error) {
	statusURL := fmt.Sprintf("%s/actor-runs/%s?token=%s", apifyBaseURL, runID, token)
	r, err := http.Get(statusURL)
	if err != nil {
		return "", nil, fmt.Errorf("poll status: %w", err)
	}
	defer r.Body.Close()

	var statusResp struct {
		Data struct {
			Status           string `json:"status"`
			DefaultDatasetID string `json:"defaultDatasetId"`
		} `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&statusResp); err != nil {
		return "", nil, fmt.Errorf("decode status: %w", err)
	}

	if statusResp.Data.Status != "SUCCEEDED" {
		return statusResp.Data.Status, nil, nil
	}

	dataURL := fmt.Sprintf("%s/datasets/%s/items?token=%s", apifyBaseURL, statusResp.Data.DefaultDatasetID, token)
	dr, err := http.Get(dataURL)
	if err != nil {
		return "SUCCEEDED", nil, fmt.Errorf("fetch dataset: %w", err)
	}
	defer dr.Body.Close()

	var items []struct {
		URL  string `json:"url"`
		HTML string `json:"html"`
	}
	if err := json.NewDecoder(dr.Body).Decode(&items); err != nil {
		return "SUCCEEDED", nil, fmt.Errorf("decode items: %w", err)
	}

	out := make(map[string]string, len(items))
	for _, item := range items {
		out[item.URL] = item.HTML
	}
	return "SUCCEEDED", out, nil
}