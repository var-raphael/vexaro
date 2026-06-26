package reddit

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/var-raphael/vexaro-engine/db"
)

// SaveRaw is used by the legacy /reddit and /run handlers.
// It inserts a new datasets_url row and saves raw.json to disk.
func SaveRaw(rawBytes []byte, rawURL string, datasetID int64, userID string, apiName string) error {
	database := db.Get()

	res, err := database.Exec(`
		INSERT INTO datasets_url (dataset_id, url, rendered_by, source_type)
		VALUES (?, ?, 'reddit-api', 'reddit')
	`, datasetID, rawURL)
	if err != nil {
		return fmt.Errorf("datasets_url insert: %w", err)
	}
	datasetURLID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("last insert id: %w", err)
	}

	slug := sanitizeSlug(rawURL)
	ts := time.Now().Unix()
	folderPath := fmt.Sprintf("data/%s-%d/reddit.com/%s-%d-%d", apiName, datasetID, slug, datasetURLID, ts)

	if err := os.MkdirAll(folderPath, 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	if err := os.WriteFile(folderPath+"/raw.json", rawBytes, 0644); err != nil {
		return fmt.Errorf("write raw.json: %w", err)
	}

	if _, err := database.Exec(`
		UPDATE datasets_url SET folder_path = ? WHERE dataset_url_id = ?
	`, folderPath, datasetURLID); err != nil {
		return fmt.Errorf("update folder_path: %w", err)
	}

	if _, err := database.Exec(`
		INSERT INTO reddit_queue (dataset_url_id, status) VALUES (?, 'proceed-combine')
	`, datasetURLID); err != nil {
		return fmt.Errorf("reddit_queue insert: %w", err)
	}

	log.Printf("[reddit/save] saved → %s", rawURL)
	return nil
}

// SaveRawForExistingDataset is used by RunFromQueue.
// Does NOT create a new dataset — dataset_id is passed in.
// Writes raw.json + filtered.json and inserts proceed-combine into reddit_queue.
func SaveRawForExistingDataset(rawBytes []byte, rawURL string, datasetID int64, userID string, apiName string, schema RedditSchema) error {
	database := db.Get()

	res, err := database.Exec(`
		INSERT INTO datasets_url (dataset_id, url, rendered_by, source_type)
		VALUES (?, ?, 'reddit-api', 'reddit')
	`, datasetID, rawURL)
	if err != nil {
		return fmt.Errorf("datasets_url insert: %w", err)
	}
	datasetURLID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("last insert id: %w", err)
	}

	slug := sanitizeSlug(rawURL)
	ts := time.Now().Unix()
	folderPath := fmt.Sprintf("data/%s-%d/reddit.com/%s-%d-%d", apiName, datasetID, slug, datasetURLID, ts)

	if err := os.MkdirAll(folderPath, 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	if err := os.WriteFile(folderPath+"/raw.json", rawBytes, 0644); err != nil {
		return fmt.Errorf("write raw.json: %w", err)
	}

	// Parse and write filtered.json inline — saves RunCombine a read/write cycle
	filtered, err := ParseRaw(rawBytes, schema)
	if err != nil {
		log.Printf("[reddit/save] parse failed for %s: %v — raw only", rawURL, err)
	} else {
		b, err := json.MarshalIndent(filtered, "", "  ")
		if err == nil {
			if err := os.WriteFile(folderPath+"/filtered.json", b, 0644); err != nil {
				log.Printf("[reddit/save] write filtered.json failed: %v", err)
			}
		}
	}

	if _, err := database.Exec(`
		UPDATE datasets_url SET folder_path = ? WHERE dataset_url_id = ?
	`, folderPath, datasetURLID); err != nil {
		return fmt.Errorf("update folder_path: %w", err)
	}

	if _, err := database.Exec(`
		INSERT INTO reddit_queue (dataset_url_id, status) VALUES (?, 'proceed-combine')
	`, datasetURLID); err != nil {
		return fmt.Errorf("reddit_queue insert: %w", err)
	}

	log.Printf("[reddit/save] saved → %s", rawURL)
	return nil
}

// SavePost saves a pre-parsed FilteredPost (used by legacy handlers).
func SavePost(post FilteredPost, rawURL string, datasetID int64, userID string, apiName string) error {
	database := db.Get()

	res, err := database.Exec(`
		INSERT INTO datasets_url (dataset_id, url, rendered_by, source_type)
		VALUES (?, ?, 'reddit-api', 'reddit')
	`, datasetID, rawURL)
	if err != nil {
		return fmt.Errorf("datasets_url insert: %w", err)
	}
	datasetURLID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("last insert id: %w", err)
	}

	slug := sanitizeSlug(rawURL)
	ts := time.Now().Unix()
	folderPath := fmt.Sprintf("data/%s-%d/reddit.com/%s-%d-%d", apiName, datasetID, slug, datasetURLID, ts)

	if err := os.MkdirAll(folderPath, 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	b, err := json.MarshalIndent(post, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := os.WriteFile(folderPath+"/raw.json", b, 0644); err != nil {
		return fmt.Errorf("write raw.json: %w", err)
	}

	if _, err := database.Exec(`
		UPDATE datasets_url SET folder_path = ? WHERE dataset_url_id = ?
	`, folderPath, datasetURLID); err != nil {
		return fmt.Errorf("update folder_path: %w", err)
	}

	if _, err := database.Exec(`
		INSERT INTO reddit_queue (dataset_url_id, status) VALUES (?, 'proceed-combine')
	`, datasetURLID); err != nil {
		return fmt.Errorf("reddit_queue insert: %w", err)
	}

	log.Printf("[reddit/save] saved → %s", post.Title)
	return nil
}

func sanitizeSlug(rawURL string) string {
	u := strings.TrimPrefix(rawURL, "https://")
	u = strings.TrimPrefix(u, "http://")
	u = strings.NewReplacer(
		"/", "-", "?", "-", "&", "-", "=", "-", ".", "-",
	).Replace(u)

	// Strip non-ASCII characters
	var b strings.Builder
	for _, r := range u {
		if r < 128 {
			b.WriteRune(r)
		}
	}
	u = b.String()

	if len(u) > 60 {
		u = u[:60]
	}
	return u
}