package crawl

import (
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

const scoreThreshold = 3

var (
	frameworkRe  = regexp.MustCompile(`(?i)(react|vue|angular|next\.js|nuxt|svelte|ember|backbone)`)
	webpackRe    = regexp.MustCompile(`(?i)(webpack|chunk\.[a-f0-9]+\.js|bundle\.[a-f0-9]+\.js)`)
	noscriptRe   = regexp.MustCompile(`(?i)(enable javascript|javascript is required|javascript must be enabled)`)
	commonWords  = []string{"the", "and", "for", "are", "but", "not", "you", "all", "can", "her", "was", "one", "our", "out", "day", "get"}
)

// spaScore returns a confidence score that a page needs JS to render its content.
// Higher = more confident it's a SPA shell.
// Caller decides what to do based on threshold.
func spaScore(html string) (int, []string) {
	score := 0
	var reasons []string

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return 0, nil
	}

	bodyText := strings.TrimSpace(doc.Find("body").Text())
	scriptCount := doc.Find("script").Length()

	// Signal 1: Framework fingerprints in script tags (+2 — strong signal)
	doc.Find("script").Each(func(i int, s *goquery.Selection) {
		src, _ := s.Attr("src")
		content := s.Text()
		if frameworkRe.MatchString(src) || frameworkRe.MatchString(content) {
			score += 2
			reasons = append(reasons, "framework fingerprint")
			return
		}
	})

	// Signal 2: noscript tag says "enable javascript" (+2)
	doc.Find("noscript").Each(func(i int, s *goquery.Selection) {
		if noscriptRe.MatchString(s.Text()) {
			score += 2
			reasons = append(reasons, "noscript enable-js message")
		}
	})

	// Signal 3: Empty SPA mount point (+2)
	emptyMount := false
	doc.Find(`div[id="root"], div[id="app"], div[id="__next"], div[id="__nuxt"]`).Each(func(i int, s *goquery.Selection) {
		if strings.TrimSpace(s.Text()) == "" {
			emptyMount = true
		}
	})
	if emptyMount {
		score += 2
		reasons = append(reasons, "empty mount point")
	}

	// Signal 4: More than 10 script tags (+1)
	if scriptCount > 10 {
		score += 1
		reasons = append(reasons, "excessive script tags")
	}

	// Signal 5: Webpack/bundle hash patterns in HTML (+1)
	if webpackRe.MatchString(html) {
		score += 1
		reasons = append(reasons, "webpack bundle hashes")
	}

	// Signal 6: Very low visible content (<300 chars in body) (+1)
	if len(bodyText) < 300 {
		score += 1
		reasons = append(reasons, "low content density")
	}

	// Signal 7: Title/meta present but body is nearly empty (+1)
	hasTitle := doc.Find("title").Length() > 0
	hasMeta := doc.Find(`meta[name="description"]`).Length() > 0
	if (hasTitle || hasMeta) && len(bodyText) < 100 {
		score += 1
		reasons = append(reasons, "title+meta with empty body")
	}

	// Signal 8: CSS class names leaked into body text (+1)
	cssLeakRe := regexp.MustCompile(`\b(className|classList|css-[a-z0-9]+)\b`)
	if cssLeakRe.MatchString(bodyText) {
		score += 1
		reasons = append(reasons, "CSS leaked into body")
	}

	// Signal 9: Missing structural tags (no <p>, <h1-h6>, <article>, <main>) (+1)
	hasStructure := doc.Find("p, h1, h2, h3, h4, h5, h6, article, main").Length() > 0
	if !hasStructure {
		score += 1
		reasons = append(reasons, "no structural HTML tags")
	}

	// Signal 10: Low common word frequency in body text (+1)
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

	// Signal 11: Word density <1.5 words/KB on pages >5KB (+2 — strong signal)
	htmlSizeKB := float64(len(html)) / 1024
	if htmlSizeKB > 5 {
		wordCount := float64(len(strings.Fields(bodyText)))
		density := wordCount / htmlSizeKB
		if density < 1.5 {
			score += 2
			reasons = append(reasons, "very low word density")
		}
	}

	return score, reasons
}

// htmlSnippet returns first 2000 chars for AI analysis — enough context, minimal tokens
func htmlSnippet(html string) string {
	if len(html) <= 2000 {
		return html
	}
	return html[:2000]
}


func getScoreThreshold() int {
	return 3
}