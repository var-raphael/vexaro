package clean

import "regexp"

var noisePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\d+\s*(days?|hours?|minutes?|seconds?)\s*ago`),
	regexp.MustCompile(`(?i)(last\s+updated|posted|updated)\s*[:\-]?\s*[\w,\s]+\d{4}`),
	regexp.MustCompile(`(?i)[\d,]+\s*(views?|likes?|applicants?|followers?|shares?)`),
	regexp.MustCompile(`(?i)\b\d{1,2}:\d{2}\s*(am|pm)\b`),
	regexp.MustCompile(`(?i)\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s*\d{4}\b`),
	regexp.MustCompile(`(?i)\b\d{1,2}/\d{1,2}/\d{2,4}\b`),
	regexp.MustCompile(`(?i)\b\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}Z?)?\b`),
}

// StripNoise removes volatile data (timestamps, counters, dates) before hashing
// so meaningless churn doesn't trigger a new version
func StripNoise(text string) string {
	for _, re := range noisePatterns {
		text = re.ReplaceAllString(text, "")
	}
	return text
}
