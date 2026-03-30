package crawl

import (
	"crypto/tls"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	utls "github.com/refraction-networking/utls"
	"golang.org/x/net/http2"
)

// ─────────────────────────────────────────
// TLS CLIENT — Chrome 120 fingerprint
// ─────────────────────────────────────────

func newTLSClient() *http.Client {
	h2transport := &http2.Transport{
		TLSClientConfig: &tls.Config{},
		DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
			return dialUTLS(network, addr)
		},
	}

	h1transport := &http.Transport{
		DialTLS:             func(network, addr string) (net.Conn, error) { return dialUTLS(network, addr) },
		DisableCompression:  false,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: &smartTransport{h2: h2transport, h1: h1transport},
	}
}

// smartTransport tries HTTP/2 first, falls back to HTTP/1.1
type smartTransport struct {
	h2 *http2.Transport
	h1 *http.Transport
}

func (t *smartTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.h2.RoundTrip(req)
	if err == nil {
		return resp, nil
	}
	return t.h1.RoundTrip(req)
}

func dialUTLS(network, addr string) (net.Conn, error) {
	conn, err := net.DialTimeout(network, addr, 10*time.Second)
	if err != nil {
		return nil, err
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}

	uconn := utls.UClient(conn, &utls.Config{
		ServerName: host,
		NextProtos: []string{"h2", "http/1.1"},
	}, utls.HelloChrome_120)

	if err := uconn.Handshake(); err != nil {
		conn.Close()
		return nil, err
	}

	return uconn, nil
}

// ─────────────────────────────────────────
// USER AGENTS
// ─────────────────────────────────────────

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
}

func randomUserAgent() string {
	return userAgents[rand.Intn(len(userAgents))]
}

// ─────────────────────────────────────────
// HEADERS
// ─────────────────────────────────────────

func setHumanHeaders(req *http.Request, ua string) {
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-User", "?1")
	req.Header.Set("Cache-Control", "max-age=0")

	// sec-ch-ua must match the selected UA exactly.
	// Firefox and Safari do not send these headers -- omitting them
	// for those UAs is more accurate than sending mismatched Chrome values.
	switch {
	case strings.Contains(ua, "Chrome/120"):
		req.Header.Set("Sec-Ch-Ua", `"Chromium";v="120", "Google Chrome";v="120", "Not-A.Brand";v="99"`)
		req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
		req.Header.Set("Sec-Ch-Ua-Platform", platformForUA(ua))
	case strings.Contains(ua, "Chrome/119"):
		req.Header.Set("Sec-Ch-Ua", `"Chromium";v="119", "Google Chrome";v="119", "Not-A.Brand";v="99"`)
		req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
		req.Header.Set("Sec-Ch-Ua-Platform", platformForUA(ua))
	// Firefox and Safari — omit sec-ch-ua entirely
	}
}

// platformForUA returns the correct Sec-Ch-Ua-Platform value based on the UA string.
func platformForUA(ua string) string {
	switch {
	case strings.Contains(ua, "Macintosh"):
		return `"macOS"`
	case strings.Contains(ua, "X11"):
		return `"Linux"`
	default:
		return `"Windows"`
	}
}

func setJSONHeaders(req *http.Request, ua string) {
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("Connection", "keep-alive")
}

// ─────────────────────────────────────────
// JITTER — human-like timing
// ─────────────────────────────────────────

// jitter sleeps for base seconds ± up to 1 second randomness
func jitter(baseSeconds float64) {
	offset := (rand.Float64() * 2) - 1 // -1.0 to +1.0
	d := baseSeconds + offset
	if d < 0.3 {
		d = 0.3
	}
	time.Sleep(time.Duration(d * float64(time.Second)))
}

// jitterBetween sleeps between min and max seconds
func jitterBetween(minSeconds, maxSeconds float64) {
	d := minSeconds + rand.Float64()*(maxSeconds-minSeconds)
	time.Sleep(time.Duration(d * float64(time.Second)))
}