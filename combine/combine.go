package combine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func CombineFromPaths(pathsFile string, apiName string) error {
	paths, err := readLines(pathsFile)
	if err != nil {
		return fmt.Errorf("read %s: %w", pathsFile, err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("no paths found in %s", pathsFile)
	}

	var allEntities []map[string]interface{}
	var sources []map[string]interface{}

	for _, rawPath := range paths {
		extractDir := filepath.Dir(rawPath)
		extractPath := filepath.Join(extractDir, "extract.json")

		data, err := loadJSON(extractPath)
		if err != nil {
			log.Printf("[combine] skip %s: %v", extractPath, err)
			continue
		}

		// collect entities
		if entities, ok := data["entities"].([]interface{}); ok {
			for _, e := range entities {
				if entity, ok := e.(map[string]interface{}); ok {
					allEntities = append(allEntities, entity)
				}
			}
		}

		// always build source entry if source_url exists
		sourceURL, hasURL := data["source_url"].(string)
		if !hasURL || sourceURL == "" {
			continue
		}

		source := map[string]interface{}{
			"url": sourceURL,
		}

		// carry through links/images/files — preserve empty arrays, skip if key absent entirely
		if v, ok := data["links"]; ok {
			source["links"] = coerceArray(v)
		}
		if v, ok := data["images"]; ok {
			source["images"] = coerceArray(v)
		}
		if v, ok := data["files"]; ok {
			source["files"] = coerceArray(v)
		}

		sources = append(sources, source)
	}

	result := map[string]interface{}{
		"entities": allEntities,
		"sources":  sources,
	}

	outPath := filepath.Join("data", apiName, "result.json")
	if err := writeJSON(outPath, result); err != nil {
		return fmt.Errorf("write result.json: %w", err)
	}

	log.Printf("[combine] done — %d entities, %d sources → %s", len(allEntities), len(sources), outPath)
	return nil
}

// coerceArray ensures a value is always a []interface{} — never null
func coerceArray(v interface{}) []interface{} {
	if v == nil {
		return []interface{}{}
	}
	if arr, ok := v.([]interface{}); ok {
		return arr
	}
	return []interface{}{}
}

func loadJSON(path string) (map[string]interface{}, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(b, &data); err != nil {
		return nil, fmt.Errorf("parse json: %w", err)
	}
	return data, nil
}

func writeJSON(path string, v interface{}) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
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