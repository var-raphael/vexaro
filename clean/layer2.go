package clean

import (
	"regexp"
	"strings"
	"sync"
)

type boilerplateTracker struct {
	freq map[string]int
	mu   sync.Mutex
}

func newBoilerplateTracker() *boilerplateTracker {
	return &boilerplateTracker{freq: make(map[string]int)}
}

func (bt *boilerplateTracker) strip(text string, threshold int) string {
	sentences := splitSentences(text)
	if len(sentences) == 0 {
		return text
	}

	bt.mu.Lock()
	defer bt.mu.Unlock()

	for _, s := range sentences {
		norm := normalizeSentence(s)
		if len(norm) > 20 {
			bt.freq[norm]++
		}
	}

	var kept []string
	for _, s := range sentences {
		norm := normalizeSentence(s)
		if bt.freq[norm] >= threshold {
			continue
		}
		kept = append(kept, s)
	}

	return strings.Join(kept, " ")
}

func splitSentences(text string) []string {
	re := regexp.MustCompile(`[.!?]+\s+`)
	parts := re.Split(text, -1)
	var sentences []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if len(p) > 10 {
			sentences = append(sentences, p)
		}
	}
	return sentences
}

func normalizeSentence(s string) string {
	s = strings.ToLower(s)
	s = regexp.MustCompile(`\s+`).ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}
