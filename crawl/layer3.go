package crawl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const maxLayer3BodyBytes = 10 << 20 // 10MB

type steelScrapeRequest struct {
	URL     string `json:"url"`
}

type steelScrapeResponse struct {
	Content struct {
		HTML string `json:"html"`
	} `json:"content"`
	Metadata struct {
		StatusCode int    `json:"status_code"`
		Title      string `json:"title"`
	} `json:"metadata"`
}

func steelKey() string {
	return os.Getenv("STEEL_API_KEY")
}

func layer3(targetURL string) (string, error) {
	key := steelKey()
	if key == "" {
		return "", fmt.Errorf("layer3: no steel api key configured")
	}

	html, err := steelRender(targetURL, key)
	if err != nil {
		return "", err
	}

	return html, nil
}

func steelRender(targetURL, key string) (string, error) {
	log.Printf("[Steel] Rendering URL: %s", targetURL)

	payload := steelScrapeRequest{
		URL:      targetURL,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("layer3: marshal failed: %w", err)
	}

	req, err := http.NewRequest("POST", "https://api.steel.dev/v1/scrape", bytes.NewBuffer(data))
	if err != nil {
		return "", fmt.Errorf("layer3: request build failed: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("steel-api-key", key)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("layer3: steel request failed: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("[Steel] Response status: %d for %s", resp.StatusCode, targetURL)

	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return "", fmt.Errorf("layer3: invalid steel api key (HTTP %d)", resp.StatusCode)
	}
	if resp.StatusCode == 429 {
		return "", fmt.Errorf("layer3: steel rate limited")
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("layer3: steel error HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxLayer3BodyBytes))
	if err != nil {
		return "", fmt.Errorf("layer3: read failed: %w", err)
	}

	var result steelScrapeResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("layer3: parse response failed: %w", err)
	}

	html := strings.TrimSpace(result.Content.HTML)
	if len(html) < 1500 {
		return "", fmt.Errorf("layer3: rendered page too small (%d bytes), likely a block page", len(html))
	}

	lower := strings.ToLower(html)
	for _, signal := range blockPageSignals {
		if strings.Contains(lower, signal) {
			return "", fmt.Errorf("layer3: block page detected for %s", targetURL)
		}
	}

	log.Printf("[Steel] Successfully rendered %s (%d bytes)", targetURL, len(html))
	return html, nil
}