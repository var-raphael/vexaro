package crawl

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/var-raphael/vexaro-engine/db"
	"github.com/var-raphael/vexaro-engine/storage"
)

// ------------------------------------------------------------------ writer --

func saveRaw(data *ScrapedData, datasetURLID int64, userID string, apiName string, datasetID int64) error {
	var folder string

	if datasetURLID != 0 {
		var existingFolder sql.NullString
		db.Get().QueryRow(`
			SELECT folder_path FROM datasets_url WHERE dataset_url_id = ?
		`, datasetURLID).Scan(&existingFolder)

		if existingFolder.Valid && existingFolder.String != "" {
			folder = existingFolder.String
		}
	}

	if folder == "" {
		folder = buildFolder(data.URL, userID, apiName, datasetID, time.Now())
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
return nil
}

func buildFolder(rawURL string, userID string, apiName string, datasetID int64, t time.Time) string {
	domain := extractDomain(rawURL)
	datasetFolder := fmt.Sprintf("%s-%d", apiName, datasetID)
	urlFolder := fmt.Sprintf("%s-%d", userID, t.Unix())
	return filepath.Join("data", datasetFolder, domain, urlFolder)
}

func BuildFolderPath(apiName string, rawURL string, userID string, datasetID int64) string {
	var existingFolder sql.NullString
	db.Get().QueryRow(`
		SELECT folder_path FROM datasets_url
		WHERE dataset_id = ? AND url = ?
		AND folder_path IS NOT NULL
		LIMIT 1
	`, datasetID, rawURL).Scan(&existingFolder)

	if existingFolder.Valid && existingFolder.String != "" {
		return existingFolder.String
	}

	return buildFolder(rawURL, userID, apiName, datasetID, time.Now())
}


func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}
	return storage.Write(path, b)
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

func FetchAndSave(cfg Config, folderPath, rawURL string, includeLinks bool) error {
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

	data := extract(rawURL, html, layer, includeLinks)
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