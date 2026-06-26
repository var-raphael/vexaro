package crawl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
	"os"
)

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

func browserlessBase() string {
	if base := os.Getenv("BROWSERLESS_URL"); base != "" {
		return strings.TrimRight(base, "/")
	}
	return "https://production-sfo.browserless.io"
}

func layer4(targetURL, browserlessKey string) (string, error) {
	if browserlessKey == "" {
		return "", fmt.Errorf("layer4: no browserless key configured")
	}

	html, err := browserlessRender(targetURL, browserlessKey, "networkidle2")
	if err != nil {
		log.Printf("[Browserless] networkidle2 failed for %s — retrying with domcontentloaded: %v", targetURL, err)
		html, err = browserlessRender(targetURL, browserlessKey, "domcontentloaded")
		if err != nil {
			return "", err
		}
	}

	return html, nil
}

func browserlessRender(targetURL, browserlessKey, waitUntil string) (string, error) {
	log.Printf("[Browserless] Rendering URL: %s (waitUntil: %s)", targetURL, waitUntil)

	payload := browserlessRequest{
		URL:            targetURL,
		WaitForTimeout: 2500,
		GoToOptions: gotoOptions{
			WaitUntil: waitUntil,
			Timeout:   25000,
		},
		RejectResourceTypes: []string{"image", "font", "stylesheet", "media", "other"},
		BestAttempt:         true,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("layer4: marshal failed: %w", err)
	}

	params := url.Values{}
	params.Set("token", browserlessKey)
	params.Set("--disable-blink-features", "AutomationControlled")
	params.Set("--no-sandbox", "true")
	params.Set("--disable-dev-shm-usage", "true")
	params.Set("--disable-gpu", "true")

	endpoint := browserlessBase() + "/content?" + params.Encode()

	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(data))
	if err != nil {
		return "", fmt.Errorf("layer4: request build failed: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cache-Control", "no-cache")

	client := &http.Client{Timeout: 35 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("layer4: browserless request failed: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("[Browserless] Response status: %d for %s", resp.StatusCode, targetURL)

	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return "", fmt.Errorf("layer4: invalid browserless key (HTTP %d)", resp.StatusCode)
	}
	if resp.StatusCode == 429 {
		return "", fmt.Errorf("layer4: browserless rate limited")
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("layer4: browserless error HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxLayer3BodyBytes))
	if err != nil {
		return "", fmt.Errorf("layer4: read failed: %w", err)
	}

	html := strings.TrimSpace(string(body))
	if len(html) < 1500 {
		return "", fmt.Errorf("layer4: rendered page too small (%d bytes), likely a block page", len(html))
	}

	lower := strings.ToLower(html)
	for _, signal := range blockPageSignals {
		if strings.Contains(lower, signal) {
			return "", fmt.Errorf("layer4: block page detected for %s", targetURL)
		}
	}

	log.Printf("[Browserless] Successfully rendered %s (%d bytes)", targetURL, len(html))
	return html, nil
}