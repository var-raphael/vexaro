package crawl

import (
	"math/rand"
	"strings"
	"time"

	"github.com/Danny-Dasilva/CycleTLS/cycletls"
)

// ─────────────────────────────────────────
// CYCLETLS CLIENT
// ─────────────────────────────────────────

// Chrome 133 JA3 fingerprint
const chromeJA3 = "771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,0-23-65281-10-11-35-16-5-13-18-51-45-43-27-17513,29-23-24,0"

// Chrome 133 JA4R fingerprint (raw format — configures the actual TLS ClientHello)
const chromeJA4R = "t13d1516h2_002f,0035,009c,009d,1301,1302,1303,c013,c014,c02b,c02c,c02f,c030,cca8,cca9_0000,0005,000a,000b,000d,0012,0017,001b,0023,002b,002d,0033,44cd,fe0d,ff01_0403,0804,0401,0503,0805,0501,0806,0601"

// Chrome HTTP/2 SETTINGS fingerprint — window size + frame order matching Chrome 133
const chromeHTTP2Fingerprint = "1:65536;2:0;4:6291456;6:262144|15663105|0|m,a,s,p"

// Firefox fallback fingerprints — used for UA rotation variety
const firefoxJA3 = "771,4865-4867-4866-49195-49199-52393-52392-49196-49200-49162-49161-49171-49172-51-57-47-53-10,0-23-65281-10-11-35-16-5-51-43-13-45-28-21,29-23-24-25-256-257,0"
const firefoxHTTP2Fingerprint = "1:65536;2:0;4:131072;5:16384|12517377|0|m,p,a,s"

// newCycleTLSClient returns a fresh CycleTLS client.
// CycleTLS manages its own connection pool internally — one client
// can be reused across requests for the same crawl run.
func newCycleTLSClient() cycletls.CycleTLS {
	return cycletls.Init()
}

// defaultOptions returns CycleTLS options that match a Chrome 133 fingerprint.
// Pass a UA string to keep the fingerprint consistent across TLS + HTTP headers.
func defaultOptions(ua string) cycletls.Options {
	ja3 := chromeJA3
	ja4r := chromeJA4R
	h2fp := chromeHTTP2Fingerprint

	// If we're using a Firefox UA, swap to matching Firefox fingerprints
	// so TLS + UA + H2 are all consistent — mismatches are a bot signal
	if strings.Contains(ua, "Firefox") {
		ja3 = firefoxJA3
		h2fp = firefoxHTTP2Fingerprint
		ja4r = "" // Firefox JA4R not set — avoids Chrome/Firefox mismatch
	}

	opts := cycletls.Options{
		Ja3:              ja3,
		Ja4r:             ja4r,
		HTTP2Fingerprint: h2fp,
		UserAgent:        ua,
		Headers:          humanHeaders(ua),
		HeaderOrder:      chromeHeaderOrder(ua),
		Timeout:          30,
		EnableConnectionReuse: true,
	}

	return opts
}

// ─────────────────────────────────────────
// USER AGENTS
// ─────────────────────────────────────────

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0",
}

func randomUserAgent() string {
	return userAgents[rand.Intn(len(userAgents))]
}

// ─────────────────────────────────────────
// HEADERS
// ─────────────────────────────────────────

// humanHeaders returns a header map consistent with the given UA.
// CycleTLS respects HeaderOrder when sending — order matters for bot detection.
func humanHeaders(ua string) map[string]string {
	headers := map[string]string{
		"Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
		"Accept-Language":           "en-US,en;q=0.9",
		"Accept-Encoding":           "identity",
		"Connection":                "keep-alive",
		"Upgrade-Insecure-Requests": "1",
		"Sec-Fetch-Dest":            "document",
		"Sec-Fetch-Mode":            "navigate",
		"Sec-Fetch-Site":            "none",
		"Sec-Fetch-User":            "?1",
		"Cache-Control":             "max-age=0",
	}

	switch {
	case strings.Contains(ua, "Chrome/133"):
		headers["sec-ch-ua"] = `"Chromium";v="133", "Google Chrome";v="133", "Not-A.Brand";v="24"`
		headers["sec-ch-ua-mobile"] = "?0"
		headers["sec-ch-ua-platform"] = platformForUA(ua)
	case strings.Contains(ua, "Chrome/132"):
		headers["sec-ch-ua"] = `"Chromium";v="132", "Google Chrome";v="132", "Not-A.Brand";v="24"`
		headers["sec-ch-ua-mobile"] = "?0"
		headers["sec-ch-ua-platform"] = platformForUA(ua)
	// Firefox omits sec-ch-ua entirely
	}

	return headers
}

// chromeHeaderOrder matches Chrome 133's actual header ordering.
// Bot detection checks this — Go's default order is different.
func chromeHeaderOrder(ua string) []string {
	if strings.Contains(ua, "Firefox") {
		return []string{
			"host",
			"user-agent",
			"accept",
			"accept-language",
			"accept-encoding",
			"connection",
			"upgrade-insecure-requests",
			"sec-fetch-dest",
			"sec-fetch-mode",
			"sec-fetch-site",
			"sec-fetch-user",
			"cache-control",
		}
	}

	// Chrome order
	return []string{
		"host",
		"sec-ch-ua",
		"sec-ch-ua-mobile",
		"sec-ch-ua-platform",
		"upgrade-insecure-requests",
		"user-agent",
		"accept",
		"sec-fetch-site",
		"sec-fetch-mode",
		"sec-fetch-user",
		"sec-fetch-dest",
		"accept-encoding",
		"accept-language",
		"cache-control",
		"connection",
	}
}

// subsequentOptions adjusts headers for requests after the first on a domain.
func subsequentOptions(ua, referer string) cycletls.Options {
	opts := defaultOptions(ua)
	opts.Headers["Sec-Fetch-Site"] = "same-origin"
	delete(opts.Headers, "Sec-Fetch-User")
	delete(opts.Headers, "Upgrade-Insecure-Requests")
	if referer != "" {
		opts.Headers["Referer"] = referer
	}
	return opts
}

// jsonOptions returns options for probing JSON API endpoints.
func jsonOptions(ua string) cycletls.Options {
	opts := defaultOptions(ua)
	opts.Headers = map[string]string{
		"Accept":          "application/json",
		"Accept-Language": "en-US,en;q=0.9",
		"Accept-Encoding": "identity",
		"Connection":      "keep-alive",
	}
	return opts
}

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

// ─────────────────────────────────────────
// JITTER
// ─────────────────────────────────────────

func jitterBetween(minSeconds, maxSeconds float64) {
	d := minSeconds + rand.Float64()*(maxSeconds-minSeconds)
	time.Sleep(time.Duration(d * float64(time.Second)))
}
