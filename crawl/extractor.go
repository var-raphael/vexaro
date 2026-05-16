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
	"github.com/var-raphael/vexaro-engine/db"
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

const noiseContainers = "nav, footer, header, aside, form, " +
	"[role='navigation'], [role='banner'], [role='contentinfo'], [role='complementary']"

// ---------------------------------------------------------------- extract --

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

func saveRaw(data *ScrapedData, datasetURLID int64, userID string, apiName string, datasetID int64) error {
	t := time.Now()
	folder := buildFolder(data.URL, userID, apiName, datasetID, t)
	if err := os.MkdirAll(folder, 0755); err != nil {
		return fmt.Errorf("mkdir failed: %w", err)
	}

	rawPath := filepath.Join(folder, "raw.json")
	if err := writeJSON(rawPath, data); err != nil {
		return err
	}

	if datasetURLID != 0 {
		_, err := db.Get().Exec(`
			UPDATE datasets_url SET folder_path = ? WHERE dataset_url_id = ?
		`, folder, datasetURLID)
		if err != nil {
			return fmt.Errorf("update folder_path: %w", err)
		}
	}

	f, err := os.OpenFile("paths.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("paths.txt open failed: %w", err)
	}
	defer f.Close()
	_, err = fmt.Fprintln(f, rawPath)
	return err
}

// buildFolder — fixed: userID is a string, use %s not %d
func buildFolder(rawURL string, userID string, apiName string, datasetID int64, t time.Time) string {
	domain := extractDomain(rawURL)
	datasetFolder := fmt.Sprintf("%s-%d", apiName, datasetID)
	urlFolder := fmt.Sprintf("%s-%d", userID, t.Unix()) // was %d-%d — bug fixed
	return filepath.Join("data", datasetFolder, domain, urlFolder)
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}
	return os.WriteFile(path, b, 0644)
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

// FetchAndSave fetches a URL and overwrites raw.json in the given folder.
// Used by the diff/rescrape flow.
func FetchAndSave(cfg Config, folderPath, rawURL string) error {
	var html, layer string

	raw, err := layer1(rawURL, true)
	if err == nil {
		doc, docErr := goquery.NewDocumentFromReader(strings.NewReader(raw))
		if docErr == nil {
			if confirmed, _ := isConfirmedSPA(raw, doc); confirmed {
				result, l2err := layer2(rawURL, raw)
				if l2err == nil && result != "" {
					html, layer = result, "layer2"
				}
			}
		}
		if html == "" {
			score, reasons := spaScore(raw)
			if score >= scoreThreshold && hasAnchorSignal(reasons) {
				result, l2err := layer2(rawURL, raw)
				if l2err == nil && result != "" {
					html, layer = result, "layer2"
				}
			}
		}
		if html == "" {
			html, layer = raw, "layer1"
		}
	} else {
		result, l2err := layer2(rawURL, "")
		if l2err == nil && result != "" {
			html, layer = result, "layer2"
		} else {
			result, l3err := layer3(rawURL)
			if l3err == nil && result != "" {
				html, layer = result, "layer3"
			} else if cfg.BrowserlessKey != "" {
				result, l4err := layer4(rawURL, cfg.BrowserlessKey)
				if l4err != nil {
					return fmt.Errorf("all layers failed: %w", l4err)
				}
				html, layer = result, "layer4"
			} else {
				return fmt.Errorf("layer1/2/3 failed and no browserless key")
			}
		}
	}

	data := extract(rawURL, html, layer)
	rawPath := filepath.Join(folderPath, "raw.json")
	return writeJSON(rawPath, data)
}

func isUsableHTML(html string) bool {
    if len(html) < 100 {
        return false
    }
    sample := html
    if len(sample) > 2000 {
        sample = sample[:2000]
    }
    printable := 0
    for _, c := range sample {
        if c >= 32 && c < 127 || c == '\n' || c == '\r' || c == '\t' {
            printable++
        }
    }
    return float64(printable)/float64(len(sample)) > 0.85
}