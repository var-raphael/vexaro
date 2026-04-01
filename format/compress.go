package format

import (
	"regexp"
	"strings"
)

var boilerplatePatterns = []*regexp.Regexp{
	// attribution and credits — tight match only
	regexp.MustCompile(`(?i)credit:\s*[\w\s]+/[\w\s]+`),
	regexp.MustCompile(`(?i)\w+\s*/\s*flickr\s*/\s*cc\s*b[y]\s*[\d.]*`),

	// pagination and navigation
	regexp.MustCompile(`(?i)\d{1,2}\s+of\s+\d{1,2}`),
	regexp.MustCompile(`(?i)continue to( \d+)? of \d+ below`),
	regexp.MustCompile(`(?i)continue to below`),
	regexp.MustCompile(`(?i)see all`),

	// collapsed scrape artifacts only — no-space versions
	regexp.MustCompile(`(?i)\d+inches`),
	regexp.MustCompile(`(?i)\d+pounds`),
	regexp.MustCompile(`(?i)\d+\s*inchesweight:`),
	regexp.MustCompile(`(?i)\d+\s*poundscoat`),
	regexp.MustCompile(`(?i)[a-z]+life\s*expectancy`),
	regexp.MustCompile(`(?i)[a-z]+colorslife`),
	regexp.MustCompile(`(?i)[a-z]+yearslife`),
	regexp.MustCompile(`(?i)minkcolors`),
	regexp.MustCompile(`(?i)smokelife`),

	// academic citations
	regexp.MustCompile(`(?i)\w+,\s*[a-zA-Z]+,?\s+et al[^.]*`),
	regexp.MustCompile(`(?i)journal of [^.]{0,60}`),
	regexp.MustCompile(`(?i)doi:\S+`),

	// medical/institutional noise
	regexp.MustCompile(`(?i)squamous cell cancer[^.]*`),
	regexp.MustCompile(`(?i)cornell university[^.]*`),
	regexp.MustCompile(`(?i)veterinary centers of america`),
	regexp.MustCompile(`(?i)college of veterinary medicine`),
	regexp.MustCompile(`(?i)veterinary review board`),

	// url and number artifacts
	regexp.MustCompile(`\.\d+\b`),
	regexp.MustCompile(`(?i)\bvice\b`),

	// isolated punctuation
	regexp.MustCompile(`(\s+\.\s*){2,}`),
	regexp.MustCompile(`^\s*\.\s*`),
	regexp.MustCompile(`\s+\.\s+`),

	// footer/site boilerplate
	regexp.MustCompile(`(?i)more from .{0,40}`),
	regexp.MustCompile(`(?i)what to buy`),
	regexp.MustCompile(`(?i)terms of service`),
	regexp.MustCompile(`(?i)privacy policy`),
	regexp.MustCompile(`(?i)editorial guidelines`),
	regexp.MustCompile(`(?i)editorial policy`),
	regexp.MustCompile(`(?i)product testing`),
	regexp.MustCompile(`(?i)sweepstakes`),
}

func compressContent(content string) string {
	content = strings.TrimSpace(content)
	if content == "" {
		return ""
	}

	content = stripBoilerplate(content)
	content = stripLeadingNav(content)
	content = deduplicateWindows(content)
	content = stripFragments(content)

	return strings.TrimSpace(content)
}

func stripBoilerplate(content string) string {
	for _, re := range boilerplatePatterns {
		content = re.ReplaceAllString(content, " ")
	}
	content = regexp.MustCompile(`\s{2,}`).ReplaceAllString(content, " ")
	return content
}

func stripLeadingNav(content string) string {
	words := strings.Fields(content)
	if len(words) == 0 {
		return content
	}

	for i := 0; i < len(words); i++ {
		end := i + 15
		if end > len(words) {
			end = len(words)
		}
		chunk := strings.Join(words[i:end], " ")
		if isProseChunk(chunk) {
			return strings.Join(words[i:], " ")
		}
	}

	return content
}

func isProseChunk(chunk string) bool {
	words := strings.Fields(chunk)
	if len(words) < 6 {
		return false
	}

	lowercaseWords := 0
	for _, w := range words {
		if len(w) > 4 && w[0] >= 'a' && w[0] <= 'z' {
			lowercaseWords++
		}
	}

	return lowercaseWords >= 3
}

func deduplicateWindows(content string) string {
	sentences := strings.Split(content, ". ")
	seen := make(map[string]struct{}, len(sentences))
	var kept []string

	for _, s := range sentences {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		key := strings.ToLower(s)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		kept = append(kept, s)
	}

	return strings.Join(kept, ". ")
}

func stripFragments(content string) string {
	parts := strings.Split(content, ". ")
	var kept []string

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		words := strings.Fields(part)
		if len(words) < 8 {
			continue
		}

		hasRealWord := false
		for _, w := range words {
			if len(w) > 4 && w[0] >= 'a' && w[0] <= 'z' {
				hasRealWord = true
				break
			}
		}
		if !hasRealWord {
			continue
		}

		kept = append(kept, part)
	}

	return strings.Join(kept, ". ")
}
