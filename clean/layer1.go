package clean

import (
	"html"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

var unicodeEscapeRe = regexp.MustCompile(`\\u([0-9a-fA-F]{4})`)

func strip(text string, patterns []*regexp.Regexp, whitespaceRe, backtickRe, preRe, codeRe *regexp.Regexp) string {
	text = decodeUnicodeEscapes(text)
	text = html.UnescapeString(text)
	text = applyPatterns(text, patterns)
	text = stripEmojis(text)
	text = stripNonText(text)
	text = whitespaceRe.ReplaceAllString(text, " ")
	return strings.TrimSpace(text)
}

func stripMixed(text string, patterns []*regexp.Regexp, whitespaceRe, backtickRe, preRe, codeRe *regexp.Regexp) string {
	text = decodeUnicodeEscapes(text)
	text = html.UnescapeString(text)

	placeholders := make(map[string]string)
	counter := 0

	text = backtickRe.ReplaceAllStringFunc(text, func(block string) string {
		p := placeholder(counter)
		placeholders[p] = block
		counter++
		return p
	})
	text = preRe.ReplaceAllStringFunc(text, func(block string) string {
		p := placeholder(counter)
		placeholders[p] = block
		counter++
		return p
	})
	text = codeRe.ReplaceAllStringFunc(text, func(block string) string {
		p := placeholder(counter)
		placeholders[p] = block
		counter++
		return p
	})

	text = applyPatterns(text, patterns)
	text = stripEmojis(text)
	text = stripNonText(text)

	for p, block := range placeholders {
		text = strings.Replace(text, p, block, 1)
	}

	text = whitespaceRe.ReplaceAllString(text, " ")
	return strings.TrimSpace(text)
}

func applyPatterns(text string, patterns []*regexp.Regexp) string {
	for _, re := range patterns {
		text = re.ReplaceAllString(text, " ")
	}
	return text
}

func decodeUnicodeEscapes(text string) string {
	replacer := strings.NewReplacer(
		`\u0026`, "&",
		`\u003c`, "<",
		`\u003e`, ">",
		`\u0027`, "'",
		`\u00a0`, " ",
		`\u2019`, "'",
		`\u2018`, "'",
		`\u201c`, "\"",
		`\u201d`, "\"",
		`\u2013`, "-",
		`\u2014`, "-",
		`\u00e9`, "e",
		`\u00e0`, "a",
		`\u00e8`, "e",
		`\u00ea`, "e",
		`\u00f3`, "o",
		`\u00fa`, "u",
	)
	text = replacer.Replace(text)

	return unicodeEscapeRe.ReplaceAllStringFunc(text, func(match string) string {
		n, err := strconv.ParseInt(match[2:], 16, 32)
		if err != nil {
			return match
		}
		r := rune(n)
		if r == 0 || (r < 32 && r != '\n' && r != '\t') {
			return ""
		}
		return string(r)
	})
}

func stripEmojis(text string) string {
	var b strings.Builder
	for _, r := range text {
		if isEmoji(r) {
			b.WriteRune(' ')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func isEmoji(r rune) bool {
	return (r >= 0x1F300 && r <= 0x1FAFF) ||
		(r >= 0x2600 && r <= 0x27BF) ||
		(r >= 0xFE00 && r <= 0xFE0F) ||
		(r >= 0x1F900 && r <= 0x1F9FF)
}

func stripNonText(text string) string {
	var b strings.Builder
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) ||
			unicode.IsSpace(r) || unicode.IsPunct(r) ||
			r == '\n' || r == '\t' {
			b.WriteRune(r)
		} else {
			b.WriteRune(' ')
		}
	}
	return b.String()
}

func placeholder(i int) string {
	return "__CODE_BLOCK_" + strconv.Itoa(i) + "__"
}
