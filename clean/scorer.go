package clean

import (
	"math"
	"strings"
	"unicode"
)

func score(text string) float64 {
	if len(text) == 0 {
		return 0
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return 0
	}

	// Signal 1: avg word length (2-10 is normal prose)
	totalLen := 0
	for _, w := range words {
		totalLen += len(w)
	}
	avgWordLen := float64(totalLen) / float64(len(words))
	wordLenScore := 1.0 - math.Abs(avgWordLen-6)/10
	if wordLenScore < 0 {
		wordLenScore = 0
	}

	// Signal 2: ratio of alpha chars to total
	alphaCount := 0
	for _, r := range text {
		if unicode.IsLetter(r) {
			alphaCount++
		}
	}
	alphaRatio := float64(alphaCount) / float64(len(text))

	// Signal 3: sentence structure (presence of punctuation)
	punctCount := 0
	for _, r := range text {
		if r == '.' || r == '!' || r == '?' || r == ',' {
			punctCount++
		}
	}
	punctRatio := math.Min(float64(punctCount)/float64(len(words)), 1.0)

	combined := (wordLenScore*0.3 + alphaRatio*0.5 + punctRatio*0.2)
	return math.Round(combined*100) / 100
}

func countWords(text string) int {
	return len(strings.Fields(text))
}
