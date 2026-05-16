package crawl

import (
	"fmt"
	"strings"
)

const maxBodyBytes = 5 << 20 // 5MB

var blockPageSignals = []string{
	"just a moment",
	"enable javascript and cookies",
	"checking your browser",
	"please verify you are a human",
	"ray id",
	"cf-browser-verification",
	"403 forbidden",
	"access denied",
	"captcha",
	"unusual traffic",
	"robot or human",
	"please wait",
	"ddos protection",
	"attention required",
	// AWS WAF
	"awswafintegration",
	"awswaf.com",
	"window.gokuprops",
	"challenge.js",
	"challenge-container",
}

func isSoftBlock(html string) bool {
	// Real pages are almost always >15KB — short pages with signals are blocks
	if len(html) > 15000 {
		return false
	}
	lower := strings.ToLower(html)
	for _, signal := range blockPageSignals {
		if strings.Contains(lower, signal) {
			return true
		}
	}
	return false
}

func layer1(targetURL string, rotate bool) (string, error) {
	jitterBetween(0.5, 2.0)

	client := newCycleTLSClient()
	defer client.Close()

	ua := randomUserAgent()
	opts := defaultOptions(ua)

	resp, err := client.Do(targetURL, opts, "GET")
    if err != nil {
        return "", fmt.Errorf("cycletls fetch failed: %w", err)
    }

    if resp.Status == 403 || resp.Status == 401 {
        return "", fmt.Errorf("blocked: HTTP %d", resp.Status)
    }
    if resp.Status == 429 {
        return "", fmt.Errorf("rate limited: HTTP 429")
    }
    if resp.Status == 408 {
        return "", fmt.Errorf("request timeout")
    }
    if resp.Status >= 400 {
        return "", fmt.Errorf("HTTP error: %d", resp.Status)
    }

    body := resp.Body

    // CycleTLS sometimes returns error strings in body instead of err
    if len(body) < 500 && (strings.HasPrefix(strings.TrimSpace(body), "->") ||
        strings.Contains(body, "use of closed network connection") ||
        strings.Contains(body, "connection refused") ||
        strings.Contains(body, "no such host")) {
        return "", fmt.Errorf("cycletls fetch failed: %s", strings.TrimSpace(body))
    }

    // CycleTLS handles decompression automatically — just cap the size
    if len(body) > maxBodyBytes {
        body = body[:maxBodyBytes]
    }

    // Catch Cloudflare/WAF challenge pages returned as HTTP 200
    if isSoftBlock(body) {
        return "", fmt.Errorf("soft block detected (challenge page returned as 200)")
    }

    return body, nil
}