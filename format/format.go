package format

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/var-raphael/vexaro-engine/clean"
)

const maxChunkWords = 6000

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

		// compress and chunk content if present
		if raw, ok := payload["content"].(string); ok && raw != "" {
			compressed := compressContent(raw)
			chunks := chunkContent(compressed, maxChunkWords)

			if len(chunks) == 0 {
				payload["content"] = compressed
			} else {
				// first chunk goes into format.json
				payload["content"] = chunks[0]
				payload["chunked"] = len(chunks) > 1

				// remaining chunks saved to strip.txt
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