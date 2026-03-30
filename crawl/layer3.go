package crawl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// browserlessRequest matches the Browserless v2 /content REST API schema.
// Launch args go in the URL query string, not the body.
type browserlessRequest struct {
	URL                 string      `json:"url"`
	WaitForTimeout      int         `json:"waitForTimeout"`
	GoToOptions         gotoOptions `json:"gotoOptions"`
	RejectResourceTypes []string    `json:"rejectResourceTypes"`
	BestAttempt         bool        `json:"bestAttempt"`
}

type gotoOptions struct {
	WaitUntil string `json:"waitUntil"`
	Timeout   int    `json:"timeout"`
}

// browserlessBase returns the configured Browserless base URL.
// Falls back to the default v2 regional endpoint if BROWSERLESS_URL is not set.
func browserlessBase() string {
	if base := os.Getenv("BROWSERLESS_URL"); base != "" {
		return strings.TrimRight(base, "/")
	}
	return "https://production-sfo.browserless.io"
}

// layer3 sends the URL to Browserless v2 for full JS rendering.
// Chrome launch args are passed as query parameters per v2 docs.
// Resource blocking uses rejectResourceTypes (renamed from rejectResources in v1).
func layer3(targetURL, browserlessKey string) (string, error) {
	if browserlessKey == "" {
		return "", fmt.Errorf("layer3: no browserless key configured")
	}

	log.Printf("[Browserless] Rendering URL: %s", targetURL)

	payload := browserlessRequest{
		URL:            targetURL,
		WaitForTimeout: 1500,
		GoToOptions: gotoOptions{
			WaitUntil: "networkidle2",
			Timeout:   25000,
		},
		RejectResourceTypes: []string{"image", "font", "stylesheet"},
		BestAttempt:         true,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("layer3: marshal failed: %w", err)
	}

	// Launch args are query parameters in v2, not body fields.
	params := url.Values{}
	params.Set("token", browserlessKey)
	params.Set("--disable-blink-features", "AutomationControlled")
	params.Set("--no-sandbox", "true")
	params.Set("--disable-dev-shm-usage", "true")
	params.Set("--disable-gpu", "true")

	endpoint := browserlessBase() + "/content?" + params.Encode()

	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(data))
	if err != nil {
		return "", fmt.Errorf("layer3: request build failed: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("User-Agent", randomUserAgent())

	client := &http.Client{Timeout: 35 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("layer3: browserless request failed: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("[Browserless] Response status: %d for %s", resp.StatusCode, targetURL)

	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return "", fmt.Errorf("layer3: invalid browserless key (HTTP %d)", resp.StatusCode)
	}
	if resp.StatusCode == 429 {
		return "", fmt.Errorf("layer3: browserless rate limited")
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("layer3: browserless error HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("layer3: read failed: %w", err)
	}

	html := strings.TrimSpace(string(body))
	if len(html) < 1500 {
    return "", fmt.Errorf("layer3: rendered page too small (%d bytes), likely a block page", len(html))
}

// catch block pages that are large enough to pass the size check
lower := strings.ToLower(html)
if strings.Contains(lower, "403 forbidden") ||
    strings.Contains(lower, "access denied") ||
    strings.Contains(lower, "captcha") ||
    strings.Contains(lower, "just a moment") || // Cloudflare
    strings.Contains(lower, "enable javascript and cookies") { // Cloudflare
    return "", fmt.Errorf("layer3: block page detected for %s", targetURL)
}


	log.Printf("[Browserless] Successfully rendered %s (%d bytes)", targetURL, len(html))
	return html, nil
}