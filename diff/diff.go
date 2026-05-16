package diff

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/var-raphael/vexaro-engine/ai"
	"github.com/var-raphael/vexaro-engine/clean"
	"github.com/var-raphael/vexaro-engine/combine"
	"github.com/var-raphael/vexaro-engine/crawl"
	"github.com/var-raphael/vexaro-engine/db"
	"github.com/var-raphael/vexaro-engine/format"
)

const (
	l25WordThreshold   = 500
	addedLineThreshold = 0.02
	removedLineMax     = 0.15
)

type diffJob struct {
	QueueID    int64
	URL        string
	FolderPath string
}

func Run(datasetID int64, cfg crawl.Config, extractSchema map[string]*ai.SchemaField, formatSchema map[string]json.RawMessage) error {
	jobs, err := loadJobs(datasetID)
	if err != nil {
		return fmt.Errorf("load jobs: %w", err)
	}
	if len(jobs) == 0 {
		log.Printf("[diff] no urls found for dataset_id=%d", datasetID)
		return nil
	}

	database := db.Get()

	schemaChanged, err := checkSchemaChanged(datasetID)
	if err != nil {
		return fmt.Errorf("check schema_changed: %w", err)
	}

	changed := 0

	if schemaChanged {
		log.Printf("[diff] schema changed for dataset_id=%d — forcing full pipeline", datasetID)
		for _, job := range jobs {
			if job.FolderPath == "" {
				markDone(job.QueueID)
				continue
			}
			if err := crawl.FetchAndSave(cfg, job.FolderPath, job.URL); err != nil {
				log.Printf("[diff] FetchAndSave failed for %s: %v — skipping", job.URL, err)
				continue
			}
			_, err = database.Exec(`
				UPDATE queue SET status = 'proceed-clean', locked_at = NULL WHERE queue_id = ?
			`, job.QueueID)
			if err != nil {
				log.Printf("[diff] markProceedClean queue_id=%d: %v", job.QueueID, err)
			}
			changed++
		}
	} else {
		for _, job := range jobs {
			log.Printf("[diff] processing %s", job.URL)

			if job.FolderPath == "" {
				log.Printf("[diff] no folder_path for %s — skipping", job.URL)
				markDone(job.QueueID)
				continue
			}

			storedRaw := loadStoredRaw(job.FolderPath)

			newHTML, err := crawl.FetchRaw(cfg, job.URL)
			if err != nil {
				log.Printf("[diff] fetch failed for %s: %v — skipping", job.URL, err)
				continue
			}

			if storedRaw != "" && hashString(newHTML) == hashString(storedRaw) {
				log.Printf("[diff] L1 — no change for %s", job.URL)
				markDone(job.QueueID)
				continue
			}
			log.Printf("[diff] L1 — hash mismatch for %s — proceeding to L2", job.URL)

			addedLines := computeAdded(storedRaw, newHTML)
			removedLines := computeRemoved(storedRaw, newHTML)

			oldCount := max(lineCount(storedRaw), 1)
			addedRatio := float64(len(addedLines)) / float64(oldCount)
			removedRatio := float64(len(removedLines)) / float64(oldCount)

			if removedRatio > removedLineMax {
				log.Printf("[diff] L2 — large removal %.2f%% for %s — preserving old data", removedRatio*100, job.URL)
				markDone(job.QueueID)
				continue
			}

			if addedRatio >= addedLineThreshold {
				log.Printf("[diff] L2 — added %.2f%% for %s — proceeding to L2.5", addedRatio*100, job.URL)

				addedWordCount := countWords(addedLines)
				if addedWordCount < l25WordThreshold {
					meaningful, err := aiCheckDiff(addedLines, schemaFieldNames(extractSchema))
					if err != nil {
						log.Printf("[diff] L2.5 AI check failed for %s: %v — proceeding anyway", job.URL, err)
					} else if !meaningful {
						log.Printf("[diff] L2.5 — no meaningful addition for %s", job.URL)
						markDone(job.QueueID)
						continue
					}
				}
				log.Printf("[diff] meaningful addition confirmed for %s", job.URL)

			} else if removedRatio >= addedLineThreshold {
				log.Printf("[diff] L2 — low additions, checking removed lines for %s", job.URL)
				meaningful, err := aiCheckDiff(removedLines, schemaFieldNames(extractSchema))
				if err != nil {
					log.Printf("[diff] L2.5 removal AI check failed for %s: %v — skipping", job.URL, err)
					markDone(job.QueueID)
					continue
				}
				if !meaningful {
					log.Printf("[diff] L2.5 — removed lines not meaningful for %s", job.URL)
					markDone(job.QueueID)
					continue
				}
				log.Printf("[diff] meaningful removal confirmed for %s — proceeding", job.URL)

			} else {
				log.Printf("[diff] L2 — no significant change for %s", job.URL)
				markDone(job.QueueID)
				continue
			}

			if err := crawl.FetchAndSave(cfg, job.FolderPath, job.URL); err != nil {
				log.Printf("[diff] FetchAndSave failed for %s: %v — skipping", job.URL, err)
				continue
			}

			_, err = database.Exec(`
				UPDATE queue SET status = 'proceed-clean', locked_at = NULL WHERE queue_id = ?
			`, job.QueueID)
			if err != nil {
				log.Printf("[diff] markProceedClean queue_id=%d: %v", job.QueueID, err)
			}
			changed++
		}
	}

	if changed == 0 {
		log.Printf("[diff] no meaningful changes for dataset_id=%d — all done", datasetID)
		return nil
	}

	log.Printf("[diff] %d urls changed — running pipeline", changed)

	if err := clean.Clean(); err != nil {
		return fmt.Errorf("clean: %w", err)
	}

	fs, err := format.ParseSchema(formatSchema)
	if err != nil {
		return fmt.Errorf("parse format schema: %w", err)
	}
	if err := format.Format(fs); err != nil {
		return fmt.Errorf("format: %w", err)
	}

	if err := ai.Extract(extractSchema); err != nil {
		return fmt.Errorf("extract: %w", err)
	}

	if err := combine.Combine(); err != nil {
		return fmt.Errorf("combine: %w", err)
	}

	if schemaChanged {
		if _, err := database.Exec(`
			UPDATE dataset_schema SET schema_changed = 0 WHERE dataset_id = ?
		`, datasetID); err != nil {
			log.Printf("[diff] failed to reset schema_changed for dataset_id=%d: %v", datasetID, err)
		} else {
			log.Printf("[diff] schema_changed reset for dataset_id=%d", datasetID)
		}
	}

	return nil
}

// ---------------------------------------------------------------- db ------

func loadJobs(datasetID int64) ([]diffJob, error) {
	rows, err := db.Get().Query(`
		SELECT q.queue_id, du.url, du.folder_path
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE du.dataset_id = ?
		AND q.status != 'failed'
		ORDER BY q.queue_id ASC
	`, datasetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []diffJob
	for rows.Next() {
		var j diffJob
		var folderPath *string
		if err := rows.Scan(&j.QueueID, &j.URL, &folderPath); err != nil {
			continue
		}
		if folderPath != nil {
			j.FolderPath = *folderPath
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func checkSchemaChanged(datasetID int64) (bool, error) {
	var schemaChanged bool
	err := db.Get().QueryRow(`
		SELECT schema_changed FROM dataset_schema WHERE dataset_id = ?
	`, datasetID).Scan(&schemaChanged)
	if err != nil {
		return false, fmt.Errorf("query schema_changed: %w", err)
	}
	return schemaChanged, nil
}

func markDone(queueID int64) {
	_, err := db.Get().Exec(`
		UPDATE queue SET status = 'done', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[diff] markDone queue_id=%d: %v", queueID, err)
	}
}

// ---------------------------------------------------------------- raw ------

func loadStoredRaw(folderPath string) string {
	b, err := os.ReadFile(filepath.Join(folderPath, "raw.json"))
	if err != nil {
		return ""
	}
	var data map[string]interface{}
	if err := json.Unmarshal(b, &data); err != nil {
		return ""
	}
	raw, _ := data["raw"].(string)
	return raw
}

// ---------------------------------------------------------------- diff ------

func computeAdded(old, new string) []string {
	oldSet := map[string]bool{}
	for _, l := range strings.Split(old, "\n") {
		oldSet[l] = true
	}
	var added []string
	for _, l := range strings.Split(new, "\n") {
		if l != "" && !oldSet[l] {
			added = append(added, l)
		}
	}
	return added
}

func computeRemoved(old, new string) []string {
	newSet := map[string]bool{}
	for _, l := range strings.Split(new, "\n") {
		newSet[l] = true
	}
	var removed []string
	for _, l := range strings.Split(old, "\n") {
		if l != "" && !newSet[l] {
			removed = append(removed, l)
		}
	}
	return removed
}

func countWords(lines []string) int {
	total := 0
	for _, l := range lines {
		total += len(strings.Fields(l))
	}
	return total
}

func lineCount(s string) int {
	return len(strings.Split(s, "\n"))
}

func hashString(s string) string {
	return fmt.Sprintf("%x", s)
}

func aiCheckDiff(lines []string, schemaFields []string) (bool, error) {
	prompt := fmt.Sprintf(
		"These lines changed on a webpage:\n%s\n\nSchema fields we care about: [%s]\n\nDo any of these changes affect the schema fields? Reply YES or NO only.",
		strings.Join(lines, "\n"),
		strings.Join(schemaFields, ", "),
	)
	result, err := ai.Ask(prompt)
	if err != nil {
		return true, err
	}
	return strings.TrimSpace(strings.ToUpper(result)) == "YES", nil
}

func schemaFieldNames(schema map[string]*ai.SchemaField) []string {
	var fields []string
	for k := range schema {
		fields = append(fields, k)
	}
	return fields
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}