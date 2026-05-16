package format

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/var-raphael/vexaro-engine/clean"
	"github.com/var-raphael/vexaro-engine/db"
)

const maxChunkWords = 6000

// Format is the DB-driven entry point — picks up proceed-format rows from the
// queue, formats each file, writes format.json, and advances status to proceed-extract.
func Format(schema *Schema) error {
	database := db.Get()

	rows, err := database.Query(`
		SELECT q.queue_id, du.folder_path
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-format'
		AND du.folder_path IS NOT NULL
		ORDER BY q.queue_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query proceed-format: %w", err)
	}
	defer rows.Close()

	type formatJob struct {
		QueueID    int64
		FolderPath string
	}

	var jobs []formatJob
	for rows.Next() {
		var j formatJob
		if err := rows.Scan(&j.QueueID, &j.FolderPath); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan proceed-format rows: %w", err)
	}

	if len(jobs) == 0 {
		log.Printf("[format] no proceed-format urls in queue")
		return nil
	}

	log.Printf("[format] processing %d proceed-format jobs", len(jobs))

	succeeded := 0
	failed := 0

	for _, job := range jobs {
		cleanedPath := filepath.Join(job.FolderPath, "cleaned.json")

		cleaned, err := loadCleanedJSON(cleanedPath)
		if err != nil {
			log.Printf("[format] failed to load queue_id=%d path=%s: %v", job.QueueID, cleanedPath, err)
			markFailed(database, job.QueueID)
			failed++
			continue
		}

		payload := FilterBySchema(cleaned, schema)

		// compress and chunk content if present
		if raw, ok := payload["content"].(string); ok && raw != "" {
			compressed := compressContent(raw)
			chunks := chunkContent(compressed, maxChunkWords)

			if len(chunks) == 0 {
				payload["content"] = compressed
			} else {
				payload["content"] = chunks[0]
				payload["chunked"] = len(chunks) > 1

				if len(chunks) > 1 {
					if err := writeChunks(job.FolderPath, chunks); err != nil {
						log.Printf("[format] failed to write strip.txt for queue_id=%d: %v", job.QueueID, err)
					} else {
						log.Printf("[format] %d remaining chunks saved to strip.txt", len(chunks)-1)
					}
				}
			}
		}

		outPath := filepath.Join(job.FolderPath, "format.json")
		if err := writeJSON(outPath, payload); err != nil {
			log.Printf("[format] write failed queue_id=%d: %v", job.QueueID, err)
			markFailed(database, job.QueueID)
			failed++
			continue
		}

		markProceedExtract(database, job.QueueID)
		log.Printf("[format] done queue_id=%d → %s", job.QueueID, outPath)
		succeeded++
	}

	log.Printf("[format] done — %d formatted, %d failed", succeeded, failed)
	return nil
}

// FormatFromPaths reads paths.txt, formats each cleaned.json, and writes format.json.
// Kept for dev/debug use.
func FormatFromPaths(pathsFile string, schema *Schema) error {
	paths, err := readLines(pathsFile)
	if err != nil {
		return fmt.Errorf("read %s: %w", pathsFile, err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("no paths found in %s", pathsFile)
	}

	alreadyFormatted, err := readLinesSet("format.txt")
	if err != nil {
		return fmt.Errorf("read format.txt: %w", err)
	}

	log.Printf("[format] loaded %d paths, %d already formatted", len(paths), len(alreadyFormatted))

	succeeded := 0
	skipped := 0
	failed := 0

	for _, cleanedPath := range paths {
		if alreadyFormatted[cleanedPath] {
			skipped++
			continue
		}

		cleanedJSONPath := toCleanedPath(cleanedPath)

		cleaned, err := loadCleanedJSON(cleanedJSONPath)
		if err != nil {
			log.Printf("[format] failed to load %s: %v", cleanedJSONPath, err)
			failed++
			continue
		}

		dir := filepath.Dir(cleanedJSONPath)
		payload := FilterBySchema(cleaned, schema)

		if raw, ok := payload["content"].(string); ok && raw != "" {
			compressed := compressContent(raw)
			chunks := chunkContent(compressed, maxChunkWords)

			if len(chunks) == 0 {
				payload["content"] = compressed
			} else {
				payload["content"] = chunks[0]
				payload["chunked"] = len(chunks) > 1

				if len(chunks) > 1 {
					if err := writeChunks(dir, chunks); err != nil {
						log.Printf("[format] failed to write strip.txt for %s: %v", dir, err)
					} else {
						log.Printf("[format] %d remaining chunks saved to strip.txt", len(chunks)-1)
					}
				}
			}
		}

		outPath := filepath.Join(dir, "format.json")
		if err := writeJSON(outPath, payload); err != nil {
			log.Printf("[format] write failed %s: %v", outPath, err)
			failed++
			continue
		}

		if err := appendLine("format.txt", cleanedPath); err != nil {
			log.Printf("[format] warning: could not write to format.txt: %v", err)
		}

		log.Printf("[format] saved → %s", outPath)
		succeeded++
	}

	log.Printf("[format] done — %d saved, %d already done, %d failed", succeeded, skipped, failed)
	return nil
}

// ---------------------------------------------------------------- db ------

func markProceedExtract(database *sql.DB, queueID int64) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'proceed-extract', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[format] markProceedExtract queue_id=%d: %v", queueID, err)
	}
}

func markFailed(database *sql.DB, queueID int64) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'failed', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[format] markFailed queue_id=%d: %v", queueID, err)
	}
}

// ---------------------------------------------------------------- helpers --

func toCleanedPath(rawPath string) string {
	dir := filepath.Dir(rawPath)
	return filepath.Join(dir, "cleaned.json")
}

func loadCleanedJSON(path string) (*clean.CleanedData, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	var data clean.CleanedData
	if err := json.Unmarshal(b, &data); err != nil {
		return nil, fmt.Errorf("parse json: %w", err)
	}
	return &data, nil
}

func writeJSON(path string, v interface{}) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return os.WriteFile(path, b, 0644)
}

func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines, scanner.Err()
}

func readLinesSet(path string) (map[string]bool, error) {
	set := map[string]bool{}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return set, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			set[line] = true
		}
	}
	return set, scanner.Err()
}

func appendLine(path, line string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(line + "\n")
	return err
}
