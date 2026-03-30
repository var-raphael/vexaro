package clean

import (
	"regexp"
	"strings"
)

const defaultBoilerplateThreshold = 3
const defaultMinWordCount = 10
const defaultMinQualityScore = 0.3

type Cleaner struct {
	patterns    []*regexp.Regexp
	whitespace  *regexp.Regexp
	backtick    *regexp.Regexp
	pre         *regexp.Regexp
	code        *regexp.Regexp
	boilerplate *boilerplateTracker
	threshold   int
	minWords    int
	minScore    float64
}

type CleanResult struct {
	Text       string
	WordCount  int
	Score      float64
	Skipped    bool
	SkipReason string
}

func New() *Cleaner {
	return &Cleaner{
		patterns:    defaultPatterns(),
		whitespace:  regexp.MustCompile(`\s+`),
		backtick:    regexp.MustCompile("(?s)```.*?```"),
		pre:         regexp.MustCompile("(?si)<pre[^>]*>.*?</pre>"),
		code:        regexp.MustCompile("(?si)<code[^>]*>.*?</code>"),
		boilerplate: newBoilerplateTracker(),
		threshold:   defaultBoilerplateThreshold,
		minWords:    defaultMinWordCount,
		minScore:    defaultMinQualityScore,
	}
}

// Clean runs the full pipeline: regex strip → boilerplate → score
func (c *Cleaner) Clean(text string) CleanResult {
	text = strip(text, c.patterns, c.whitespace, c.backtick, c.pre, c.code)
	text = c.boilerplate.strip(text, c.threshold)

	wc := countWords(text)
	sc := score(text)

	if wc < c.minWords {
		return CleanResult{Skipped: true, SkipReason: "word count too low"}
	}
	if sc < c.minScore {
		return CleanResult{Skipped: true, SkipReason: "quality score too low"}
	}

	return CleanResult{Text: text, WordCount: wc, Score: sc}
}

// CleanMixed preserves code blocks during cleaning
func (c *Cleaner) CleanMixed(text string) CleanResult {
	text = stripMixed(text, c.patterns, c.whitespace, c.backtick, c.pre, c.code)
	text = c.boilerplate.strip(text, c.threshold)

	wc := countWords(text)
	sc := score(text)

	if wc < c.minWords {
		return CleanResult{Skipped: true, SkipReason: "word count too low"}
	}

	return CleanResult{Text: text, WordCount: wc, Score: sc}
}

func defaultPatterns() []*regexp.Regexp {
	raw := []string{
		`<[^>]+>`,
		`&[a-zA-Z]+;`,
		`https?://\S+`,
		`\[.*?\]\(.*?\)`,
		`#{1,6}\s`,
		`\*{1,2}[^*]+\*{1,2}`,
		`_{1,2}[^_]+_{1,2}`,
	}
	var compiled []*regexp.Regexp
	for _, p := range raw {
		if re, err := regexp.Compile("(?i)" + p); err == nil {
			compiled = append(compiled, re)
		}
	}
	return compiled
}

// preClean reduces token usage before AI — drops short/duplicate lines
func preClean(text string) string {
	lines := strings.Split(text, "\n")
	seen := map[string]bool{}
	var result []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if len(trimmed) < 20 {
			continue
		}
		lower := strings.ToLower(trimmed)
		if seen[lower] {
			continue
		}
		seen[lower] = true
		result = append(result, trimmed)
	}

	return strings.Join(result, "\n")
}
