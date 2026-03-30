package crawl

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// ------------------------------------------------------------------ types --

type ScrapedData struct {
	URL       string            `json:"url"`
	Title     string            `json:"title"`
	Content   string            `json:"content"`
	Links     []string          `json:"links"`
	Images    []string          `json:"images"`
	Videos    []string          `json:"videos"`
	Documents []string          `json:"documents"`
	Emails    []string          `json:"emails"`
	Phones    []string          `json:"phones"`
	Metadata  map[string]string `json:"metadata"`
	LayerUsed string            `json:"layer_used"`
	CrawledAt time.Time         `json:"crawled_at"`
	Raw       string            `json:"raw,omitempty"`
}

// ---------------------------------------------------------------- extract --

// extract pulls structured data from HTML or raw JSON.
// If HTML can't be parsed (e.g. layer2 returned raw JSON), content is stored as-is.
func extract(rawURL, html, layer string) *ScrapedData {
	data := &ScrapedData{
		URL:       rawURL,
		LayerUsed: layer,
		Metadata:  map[string]string{},
		CrawledAt: time.Now(),
		Raw:       html,
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		// raw JSON from layer2 — store as content directly
		data.Content = html
		return data
	}

	base, _ := url.Parse(rawURL)

	data.Title = strings.TrimSpace(doc.Find("title").First().Text())
	data.Content = extractText(doc)
	data.Links = extractLinks(doc, base)
	data.Images = extractImages(doc, base)
	data.Videos = extractVideos(doc, base)
	data.Documents = extractDocuments(doc, base)
	data.Emails = extractEmails(doc)
	data.Phones = extractPhones(doc)
	data.Metadata = extractMetadata(doc)

	return data
}

func extractText(doc *goquery.Document) string {
	doc.Find("script, style, noscript").Remove()
	var parts []string
	doc.Find("p, h1, h2, h3, h4, h5, h6, li, td, th, blockquote, pre").Each(func(i int, s *goquery.Selection) {
		text := strings.TrimSpace(s.Text())
		if len(text) > 10 {
			parts = append(parts, text)
		}
	})
	return strings.Join(parts, "\n")
}

func extractLinks(doc *goquery.Document, base *url.URL) []string {
	var links []string
	seen := map[string]bool{}
	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		resolved := resolveURLStr(base, href)
		if resolved == "" {
			return
		}
		if strings.HasPrefix(resolved, "mailto:") || strings.HasPrefix(resolved, "tel:") {
			return
		}
		if !seen[resolved] {
			seen[resolved] = true
			links = append(links, resolved)
		}
	})
	return links
}

func extractImages(doc *goquery.Document, base *url.URL) []string {
	var images []string
	seen := map[string]bool{}
	doc.Find("img[src]").Each(func(i int, s *goquery.Selection) {
		src, _ := s.Attr("src")
		if strings.HasPrefix(strings.TrimSpace(src), "data:") {
			return
		}
		resolved := resolveURLStr(base, src)
		if resolved == "" || seen[resolved] {
			return
		}
		seen[resolved] = true
		images = append(images, resolved)
	})
	return images
}

func extractVideos(doc *goquery.Document, base *url.URL) []string {
	var videos []string
	seen := map[string]bool{}
	doc.Find("video source, iframe[src]").Each(func(i int, s *goquery.Selection) {
		src, _ := s.Attr("src")
		resolved := resolveURLStr(base, src)
		if resolved == "" || seen[resolved] {
			return
		}
		seen[resolved] = true
		videos = append(videos, resolved)
	})
	return videos
}

func extractDocuments(doc *goquery.Document, base *url.URL) []string {
	var documents []string
	seen := map[string]bool{}
	extensions := []string{".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".csv"}
	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		resolved := resolveURLStr(base, href)
		if resolved == "" {
			return
		}
		for _, ext := range extensions {
			if strings.HasSuffix(strings.ToLower(resolved), ext) && !seen[resolved] {
				seen[resolved] = true
				documents = append(documents, resolved)
			}
		}
	})
	return documents
}

func extractEmails(doc *goquery.Document) []string {
	var emails []string
	seen := map[string]bool{}
	doc.Find("a[href^='mailto:']").Each(func(i int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		email := strings.TrimSpace(strings.TrimPrefix(href, "mailto:"))
		if email != "" && !seen[email] {
			seen[email] = true
			emails = append(emails, email)
		}
	})
	return emails
}

func extractPhones(doc *goquery.Document) []string {
	var phones []string
	seen := map[string]bool{}
	doc.Find("a[href^='tel:']").Each(func(i int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		phone := strings.TrimSpace(strings.TrimPrefix(href, "tel:"))
		if phone != "" && !seen[phone] {
			seen[phone] = true
			phones = append(phones, phone)
		}
	})
	return phones
}

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

// ------------------------------------------------------------------ writer --

// saveRaw writes raw.json to ./data/{apiName}/{domain}/{slug}/raw.json
// and appends the path to paths.txt for the clean module.
func saveRaw(apiName string, data *ScrapedData) error {
	folder := buildFolder(apiName, data.URL, data.Title)
	if err := os.MkdirAll(folder, 0755); err != nil {
		return fmt.Errorf("mkdir failed: %w", err)
	}

	rawPath := filepath.Join(folder, "raw.json")
	if err := writeJSON(rawPath, data); err != nil {
		return err
	}

	// append path to paths.txt for the clean module
	f, err := os.OpenFile("paths.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("paths.txt open failed: %w", err)
	}
	defer f.Close()
	_, err = fmt.Fprintln(f, rawPath)
	return err
}

func buildFolder(apiName, rawURL, title string) string {
	domain := extractDomain(rawURL)
	slug := slugify(title)
	if slug == "" {
		slug = slugify(rawURL)
	}
	return filepath.Join("data", apiName, domain, slug)
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}
	return os.WriteFile(path, b, 0644)
}

func extractDomain(rawURL string) string {
	rawURL = strings.TrimPrefix(rawURL, "https://")
	rawURL = strings.TrimPrefix(rawURL, "http://")
	rawURL = strings.TrimPrefix(rawURL, "www.")
	return strings.Split(rawURL, "/")[0]
}

func slugify(s string) string {
	s = strings.ToLower(s)
	re := regexp.MustCompile(`[^a-z0-9]+`)
	s = re.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if len(s) > 80 {
		s = s[:80]
	}
	return s
}
