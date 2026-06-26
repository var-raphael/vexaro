package crawl

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// ------------------------------------------------------------------ types --

type ScrapedData struct {
	URL       string            `json:"url"`
	Title     string            `json:"title"`
	Content   string            `json:"content"`
	Metadata  map[string]string `json:"metadata"`
	LayerUsed string            `json:"layer_used"`
	CrawledAt time.Time         `json:"crawled_at"`
	Raw       string            `json:"raw,omitempty"`
}

const noiseContainers = "nav, footer, header, aside, form, " +
	"[role='navigation'], [role='banner'], [role='contentinfo'], [role='complementary']"

var docExtensions = []string{
	".pdf", ".epub", ".mobi", ".zip", ".rar",
	".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
	".csv", ".azw", ".azw3", ".djvu", ".fb2",
}

var rejectedExtensions = []string{
	".mp3", ".mp4", ".mov", ".avi", ".mkv", ".webm",
	".exe", ".dmg", ".iso", ".bin",
	".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico",
	".woff", ".woff2", ".ttf", ".eot",
}

// ---------------------------------------------------------------- extract --

func extract(rawURL, html, layer string, includeLinks bool) *ScrapedData {
	data := &ScrapedData{
		URL:       rawURL,
		LayerUsed: layer,
		Metadata:  map[string]string{},
		CrawledAt: time.Now(),
		Raw:       html,
	}

	ct := detectContentType(html)

	switch ct {
	case "xml":
		data.Content = flattenXML(html)
		return data
	case "json":
		data.Content = flattenJSON(html)
		return data
	case "text":
		data.Content = strings.TrimSpace(html)
		return data
	}

	// HTML path
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		data.Content = html
		return data
	}

	base, _ := url.Parse(rawURL)

	data.Title = strings.TrimSpace(doc.Find("title").First().Text())
	data.Content = extractText(doc, base, includeLinks)
	data.Metadata = extractMetadata(doc)

	return data
}

// ---------------------------------------------------------------- content type detection --

func detectContentType(s string) string {
	trimmed := strings.TrimSpace(s)
	log.Printf("[detectContentType] len=%d prefix=%q", len(trimmed), trimmed[:min(100, len(trimmed))])
	
	if strings.HasPrefix(trimmed, "<?xml") ||
		strings.HasPrefix(trimmed, "<SEC-DOCUMENT") ||
		strings.HasPrefix(trimmed, "<edgarSubmission") {
		return "xml"
	}
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		return "json"
	}
	if !strings.Contains(trimmed, "<") {
		return "text"
	}
	return "html"
}
// ---------------------------------------------------------------- url validation --

func IsRejectedURL(rawURL string) bool {
	lower := strings.ToLower(rawURL)
	for _, ext := range rejectedExtensions {
		if strings.Contains(lower, ext) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- xml flattening --

func flattenXML(raw string) string {
	decoder := xml.NewDecoder(strings.NewReader(raw))
	var parts []string
	var keyStack []string

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}

		switch t := token.(type) {
		case xml.StartElement:
			keyStack = append(keyStack, t.Name.Local)
		case xml.EndElement:
			if len(keyStack) > 0 {
				keyStack = keyStack[:len(keyStack)-1]
			}
		case xml.CharData:
			val := strings.TrimSpace(string(t))
			if val == "" || val == "XXXXXXXX" {
				continue
			}
			if len(keyStack) > 0 {
				key := keyStack[len(keyStack)-1]
				parts = append(parts, fmt.Sprintf("%s: %s", key, val))
			}
		}
	}

	return strings.Join(parts, "\n")
}

// ---------------------------------------------------------------- json flattening --

func flattenJSON(raw string) string {
	log.Printf("[flattenJSON] raw input len=%d prefix=%q", len(raw), raw[:min(50, len(raw))])
	
	var parsed interface{}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		log.Printf("[flattenJSON] unmarshal error: %v", err)
		return strings.TrimSpace(raw)
	}

	var parts []string
	flattenJSONValue("", parsed, &parts, map[string]int{})
	return strings.Join(parts, "\n")
}

func flattenJSONValue(prefix string, val interface{}, parts *[]string, counters map[string]int) {
	switch v := val.(type) {
	case map[string]interface{}:
		for k, child := range v {
			newKey := k
			if prefix != "" {
				newKey = prefix + "_" + k
			}
			flattenJSONValue(newKey, child, parts, counters)
		}
	case []interface{}:
		for _, item := range v {
			counters[prefix]++
			indexedKey := fmt.Sprintf("%s_%d", prefix, counters[prefix])
			flattenJSONValue(indexedKey, item, parts, counters)
		}
	case nil:
		// skip
	default:
		str := strings.TrimSpace(fmt.Sprintf("%v", v))
		if str == "" {
			return
		}
		*parts = append(*parts, fmt.Sprintf("%s: %s", prefix, str))
	}
}

// ---------------------------------------------------------------- html extraction --

func extractText(doc *goquery.Document, base *url.URL, includeLinks bool) string {
	doc.Find("script, style, noscript, iframe").Remove()

	var parts []string
	seen := map[string]bool{}

	doc.Find("p, h1, h2, h3, h4, h5, h6, li, td, th, blockquote, pre").Each(func(i int, s *goquery.Selection) {
		if s.Closest(noiseContainers).Length() > 0 {
			return
		}

		text := strings.TrimSpace(s.Text())
		if len(text) < 20 {
			return
		}
		lower := strings.ToLower(text)
		if seen[lower] {
			return
		}
		seen[lower] = true
		parts = append(parts, text)

		// Doc file links always inlined regardless of includeLinks
		s.Find("a[href]").Each(func(j int, a *goquery.Selection) {
			href, _ := a.Attr("href")
			resolved := resolveURLStr(base, href)
			if resolved == "" {
				return
			}
			lower := strings.ToLower(resolved)
			for _, ext := range docExtensions {
				if strings.Contains(lower, ext) {
					if !seen[resolved] {
						seen[resolved] = true
						parts = append(parts, resolved)
					}
					return
				}
			}

			// Regular links — only if includeLinks is on
			if includeLinks {
				domain := extractDomain(resolved)
				if blockedDomains[domain] {
					return
				}
				if !seen[resolved] {
					seen[resolved] = true
					parts = append(parts, resolved)
				}
			}
		})
	})

	// mailto/tel safety net
	doc.Find("a[href^='mailto:']").Each(func(i int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		email := strings.TrimSpace(strings.TrimPrefix(href, "mailto:"))
		if email == "" {
			return
		}
		line := "Email: " + email
		lower := strings.ToLower(line)
		if seen[lower] {
			return
		}
		seen[lower] = true
		parts = append(parts, line)
	})

	doc.Find("a[href^='tel:']").Each(func(i int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		phone := strings.TrimSpace(strings.TrimPrefix(href, "tel:"))
		if phone == "" {
			return
		}
		line := "Phone: " + phone
		lower := strings.ToLower(line)
		if seen[lower] {
			return
		}
		seen[lower] = true
		parts = append(parts, line)
	})

	return strings.Join(parts, "\n")
}

// ---------------------------------------------------------------- metadata --

func extractMetadata(doc *goquery.Document) map[string]string {
	meta := map[string]string{}
	doc.Find("meta").Each(func(i int, s *goquery.Selection) {
		name, _ := s.Attr("name")
		property, _ := s.Attr("property")
		content, _ := s.Attr("content")
		if name != "" && content != "" {
			meta[name] = content
		}
		if property != "" && content != "" {
			meta[property] = content
		}
	})
	return meta
}

// ---------------------------------------------------------------- helpers --

func resolveURLStr(base *url.URL, href string) string {
	href = strings.TrimSpace(href)
	if href == "" || href == "#" || strings.HasPrefix(href, "javascript:") {
		return ""
	}
	if base == nil {
		return href
	}
	ref, err := url.Parse(href)
	if err != nil {
		return href
	}
	return base.ResolveReference(ref).String()
}


// Public wrappers for testing
func ExtractPublic(rawURL, html, layer string, includeLinks bool) *ScrapedData {
	return extract(rawURL, html, layer, includeLinks)
}

func DetectContentTypePublic(html string) string {
	return detectContentType(html)
}