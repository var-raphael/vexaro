package crawl

import (
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

const scoreThreshold = 5

var (
	frameworkRe = regexp.MustCompile(`(?i)(ReactDOM|__vue__|angular\.min|next\.js|nuxt\.js|svelte|ember\.min|backbone\.min)`)
	webpackRe   = regexp.MustCompile(`(?i)(webpack|chunk\.[a-f0-9]+\.js|bundle\.[a-f0-9]+\.js)`)
	noscriptRe  = regexp.MustCompile(`(?i)(enable javascript|javascript is required|javascript must be enabled)`)
	commonWords = []string{"the", "and", "for", "are", "but", "not", "you", "all", "can", "her", "was", "one", "our", "out", "day", "get"}

	// Inevitable SPA signals — framework artifacts no developer controls
	nextDataRe      = regexp.MustCompile(`window\.__NEXT_DATA__`)
	nuxtDataRe      = regexp.MustCompile(`window\.__NUXT__`)
	initialStateRe  = regexp.MustCompile(`window\.__INITIAL_STATE__`)
	reduxStateRe    = regexp.MustCompile(`window\.__REDUX_STATE__`)
	hydrationDataRe = regexp.MustCompile(`window\.__APOLLO_STATE__|window\.__RELAY_STORE__|window\.__PRELOADED_STATE__`)
	frameworkPathRe = regexp.MustCompile(`(/_next/|/_nuxt/|/static/js/main\.|/static/js/[0-9]+\.)`)
	reactRootRe     = regexp.MustCompile(`data-reactroot`)
	vueAttrRe       = regexp.MustCompile(`data-v-[a-f0-9]+`)
	ngAttrRe        = regexp.MustCompile(`(ng-version|data-ng-|_nghost-|_ngcontent-)`)
)

// isConfirmedSPA checks for unambiguous framework artifacts that no static site produces.
// If any match, it's a confirmed SPA regardless of score — skip scoring entirely.
func isConfirmedSPA(html string, doc *goquery.Document) (bool, string) {
	// Global state injection — framework-controlled, not developer-controlled
	if nextDataRe.MatchString(html) {
		return true, "window.__NEXT_DATA__ present (Next.js)"
	}
	if nuxtDataRe.MatchString(html) {
		return true, "window.__NUXT__ present (Nuxt.js)"
	}
	if initialStateRe.MatchString(html) {
		return true, "window.__INITIAL_STATE__ present"
	}
	if reduxStateRe.MatchString(html) {
		return true, "window.__REDUX_STATE__ present"
	}
	if hydrationDataRe.MatchString(html) {
		return true, "hydration state object present (Apollo/Relay/Redux)"
	}

	// Framework-specific DOM attributes injected during rendering
	if reactRootRe.MatchString(html) {
		return true, "data-reactroot attribute present"
	}
	if vueAttrRe.MatchString(html) {
		return true, "Vue scoped attribute (data-v-*) present"
	}
	if ngAttrRe.MatchString(html) {
		return true, "Angular host/content attribute present"
	}

	// Framework-specific asset paths in script src
	confirmed := false
	doc.Find("script[src]").Each(func(i int, s *goquery.Selection) {
		src, _ := s.Attr("src")
		if frameworkPathRe.MatchString(src) {
			confirmed = true
		}
	})
	if confirmed {
		return true, "framework asset path in script src (/_next/, /_nuxt/, etc.)"
	}

	// Body has >10KB HTML but <50 visible words — pure shell
	htmlSizeKB := float64(len(html)) / 1024
	bodyText := strings.TrimSpace(doc.Find("body").Text())
	wordCount := len(strings.Fields(bodyText))
	if htmlSizeKB > 10 && wordCount < 50 {
		return true, "large HTML shell with near-empty body (<50 words)"
	}

	// <script type="application/json"> with large payload — hydration blob
	doc.Find(`script[type="application/json"]`).Each(func(i int, s *goquery.Selection) {
		if len(s.Text()) > 500 {
			confirmed = true
		}
	})
	if confirmed {
		return true, "large JSON hydration blob in script tag"
	}

	return false, ""
}

// spaScore returns a confidence score that a page needs JS to render its content.
// Only called if isConfirmedSPA returns false.
// Escalation requires score >= threshold AND at least one anchor signal present.
func spaScore(html string) (int, []string) {
	score := 0
	var reasons []string

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return 0, nil
	}

	bodyText := strings.TrimSpace(doc.Find("body").Text())
	scriptCount := doc.Find("script").Length()

	// Signal 1: Framework fingerprints in script src only — deduplicated (+2)
	// Restricted to src attributes to avoid false positives on inline content
	// e.g. article copy saying "react to this news" would previously match
	frameworkHit := false
	doc.Find("script[src]").Each(func(i int, s *goquery.Selection) {
		src, _ := s.Attr("src")
		if frameworkRe.MatchString(src) {
			frameworkHit = true
		}
	})
	if frameworkHit {
		score += 2
		reasons = append(reasons, "framework fingerprint")
	}

	// Signal 2: noscript tag says "enable javascript" (+2 — anchor signal)
	doc.Find("noscript").Each(func(i int, s *goquery.Selection) {
		if noscriptRe.MatchString(s.Text()) {
			score += 2
			reasons = append(reasons, "noscript enable-js message")
		}
	})

	// Signal 3: Empty known mount point (+2)
	doc.Find(`div[id="root"], div[id="app"], div[id="__next"], div[id="__nuxt"]`).Each(func(i int, s *goquery.Selection) {
		if strings.TrimSpace(s.Text()) == "" {
			score += 2
			reasons = append(reasons, "empty mount point")
		}
	})

	// Signal 4: More than 10 script tags (+1)
	if scriptCount > 10 {
		score += 1
		reasons = append(reasons, "excessive script tags")
	}

	// Signal 5: Webpack/bundle hash patterns (+1 — anchor signal)
	if webpackRe.MatchString(html) {
		score += 1
		reasons = append(reasons, "webpack bundle hashes")
	}

	// Signal 6: Very low visible content (<300 chars) (+1)
	if len(bodyText) < 300 {
		score += 1
		reasons = append(reasons, "low content density")
	}

	// Signal 7: CSS class names leaked into body text (+1)
	cssLeakRe := regexp.MustCompile(`\b(className|classList|css-[a-z0-9]+)\b`)
	if cssLeakRe.MatchString(bodyText) {
		score += 1
		reasons = append(reasons, "CSS leaked into body")
	}

	// Signal 8: Missing structural tags (+1)
	hasStructure := doc.Find("p, h1, h2, h3, h4, h5, h6, article, main").Length() > 0
	if !hasStructure {
		score += 1
		reasons = append(reasons, "no structural HTML tags")
	}

	// Signal 9: Low common word frequency (+1 — anchor signal)
	if len(bodyText) > 50 {
		wordCount := len(strings.Fields(bodyText))
		commonCount := 0
		lower := strings.ToLower(bodyText)
		for _, w := range commonWords {
			commonCount += strings.Count(lower, " "+w+" ")
		}
		if wordCount > 0 {
			freq := float64(commonCount) / float64(wordCount)
			if freq < 0.05 {
				score += 1
				reasons = append(reasons, "low common word frequency")
			}
		}
	}

	return score, reasons
}

// hasAnchorSignal checks if at least one strong signal is present in the reasons.
// Prevents weak signals (script tags, framework names) from triggering escalation alone.
func hasAnchorSignal(reasons []string) bool {
	anchors := map[string]bool{
		"noscript enable-js message": true,
		"webpack bundle hashes":      true,
		"low common word frequency":  true,
		"empty mount point":          true,
		"title+meta with empty body": true,
	}
	for _, r := range reasons {
		if anchors[r] {
			return true
		}
	}
	return false
}