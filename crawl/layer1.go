package crawl

import (
	"compress/flate"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/andybalholm/brotli"
)

// layer1 performs a TLS-fingerprinted HTTP fetch.
// Handles gzip, brotli, and deflate decompression.
// Returns raw HTML string on success.
func layer1(targetURL string, rotate bool) (string, error) {
	// human-like delay before each request
	jitterBetween(0.5, 2.0)

	client := newTLSClient()

	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		return "", fmt.Errorf("request build failed: %w", err)
	}

	ua := randomUserAgent()
	setHumanHeaders(req, ua)

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 403 || resp.StatusCode == 401 {
		return "", fmt.Errorf("blocked: HTTP %d", resp.StatusCode)
	}
	if resp.StatusCode == 429 {
		return "", fmt.Errorf("rate limited: HTTP 429")
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	body, err := decompress(resp)
	if err != nil {
		return "", fmt.Errorf("decompress failed: %w", err)
	}

	return body, nil
}

func decompress(resp *http.Response) (string, error) {
	encoding := strings.ToLower(resp.Header.Get("Content-Encoding"))

	var reader io.Reader
	var err error

	switch encoding {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return "", err
		}
	case "br":
		reader = brotli.NewReader(resp.Body)
	case "deflate":
		reader = flate.NewReader(resp.Body)
	default:
		reader = resp.Body
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	return string(body), nil
}