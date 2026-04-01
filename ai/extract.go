package ai

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	groqEndpoint  = "https://api.groq.com/openai/v1/chat/completions"
	groqModel     = "llama-3.3-70b-versatile"
	maxChunkWords = 6000
)

type groqMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type groqRequest struct {
	Model       string        `json:"model"`
	Messages    []groqMessage `json:"messages"`
	Temperature float64       `json:"temperature"`
	MaxTokens   int           `json:"max_tokens"`
}

type groqChoice struct {
	Message groqMessage `json:"message"`
}

type groqResponse struct {
	Choices []groqChoice `json:"choices"`
}

type SchemaField struct {
	Type        string   `json:"type"`
	Description string   `json:"description,omitempty"`
	Formats     []string `json:"format,omitempty"`
}

func ExtractFromPaths(pathsFile string, schema map[string]*SchemaField) error {
	paths, err := readLines(pathsFile)
	if err != nil {
		return fmt.Errorf("read %s: %w", pathsFile, err)
	}
	if len(paths) == 0 {
		return fmt.Errorf("no paths found in %s", pathsFile)
	}

	keys, err := loadKeys()
	if err != nil {
		return err
	}

	alreadyDone, err := readLinesSet("extract.txt")
	if err != nil {
		return fmt.Errorf("read extract.txt: %w", err)
	}

	log.Printf("[ai] loaded %d paths, %d already extracted", len(paths), len(alreadyDone))

	// determine which array types the schema requests
	var wantsLinks, wantsImages, wantsFiles bool
	for _, def := range schema {
		switch def.Type {
		case "link":
			wantsLinks = true
		case "image":
			wantsImages = true
		case "file":
			wantsFiles = true
		}
	}

	succeeded := 0
	skipped := 0
	failed := 0

	for _, rawPath := range paths {
		if alreadyDone[rawPath] {
			skipped++
			continue
		}

		formatDir := filepath.Dir(rawPath)
		formatPath := filepath.Join(formatDir, "format.json")
		extractPath := filepath.Join(formatDir, "extract.json")
		stripPath := filepath.Join(formatDir, "strip.txt")

		payload, err := loadJSON(formatPath)
		if err != nil {
			log.Printf("[ai] failed to load %s: %v", formatPath, err)
			failed++
			continue
		}

		// grab page-level arrays and source_url before stripping
		pageURL, _ := payload["source_url"].(string)
		pageLinks, _ := payload["links"].([]interface{})
		pageImages, _ := payload["images"].([]interface{})
		pageFiles, _ := payload["files"].([]interface{})

		// ai only gets content
		aiPayload := map[string]interface{}{}
		if v, ok := payload["content"]; ok {
			aiPayload["content"] = v
		}

		var allEntities []map[string]interface{}
		seen := map[string]bool{}

		entities, err := extractChunk(randomKey(keys), aiPayload, schema, nil)
		if err != nil {
			log.Printf("[ai] extraction failed %s: %v", formatPath, err)
			failed++
			continue
		}
		mergeEntities(&allEntities, entities, schema, seen)

		for {
			nextContent, err := nextChunkFromStrip(stripPath, maxChunkWords)
			if err != nil || nextContent == "" {
				break
			}

			nextPayload := map[string]interface{}{"content": nextContent}

			var lastEntity map[string]interface{}
			if len(allEntities) > 0 {
				lastEntity = allEntities[len(allEntities)-1]
			}

			nextEntities, err := extractChunk(randomKey(keys), nextPayload, schema, lastEntity)
			if err != nil {
				log.Printf("[ai] chunk extraction failed: %v", err)
				break
			}
			mergeEntities(&allEntities, nextEntities, schema, seen)
		}

		result := map[string]interface{}{
			"source_url": pageURL,
			"entities":   allEntities,
		}

		// always write arrays if schema requested them — empty array if nothing found
		if wantsLinks {
			result["links"] = emptyIfNil(pageLinks)
		}
		if wantsImages {
			result["images"] = emptyIfNil(pageImages)
		}
		if wantsFiles {
			result["files"] = emptyIfNil(pageFiles)
		}

		if err := writeJSON(extractPath, result); err != nil {
			log.Printf("[ai] write failed %s: %v", extractPath, err)
			failed++
			continue
		}

		if err := appendLine("extract.txt", rawPath); err != nil {
			log.Printf("[ai] warning: could not write to extract.txt: %v", err)
		}

		log.Printf("[ai] extracted %d entities → %s", len(allEntities), extractPath)
		succeeded++
	}

	log.Printf("[ai] done — %d extracted, %d already done, %d failed", succeeded, skipped, failed)
	return nil
}

func extractChunk(apiKey string, payload map[string]interface{}, schema map[string]*SchemaField, prevEntity map[string]interface{}) ([]map[string]interface{}, error) {
	var schemaDesc strings.Builder
	schemaDesc.WriteString("Fields to extract (return null if not found):\n")
	for fieldName, def := range schema {
		if def.Type == "link" || def.Type == "image" || def.Type == "file" {
			continue
		}
		schemaDesc.WriteString(fmt.Sprintf("- %q (type: %s", fieldName, def.Type))
		if def.Description != "" {
			schemaDesc.WriteString(fmt.Sprintf(", description: %s", def.Description))
		}
		schemaDesc.WriteString(")\n")
	}

	contentStr, _ := payload["content"].(string)

	var systemPrompt strings.Builder
	systemPrompt.WriteString("You are a precise data extraction engine.\n")
	systemPrompt.WriteString("Extract ALL entities found in the content that match the schema fields.\n")
	systemPrompt.WriteString("Each entity must be a separate object in a JSON array.\n")
	systemPrompt.WriteString("Every object must contain ALL schema fields as keys.\n")
	systemPrompt.WriteString("If a field value cannot be found for an entity, set it to null.\n")
	systemPrompt.WriteString("Return ONLY a valid JSON array. No explanation, no markdown, no code blocks.\n")

	var userPrompt strings.Builder
	userPrompt.WriteString(schemaDesc.String())
	userPrompt.WriteString("\nContent:\n")
	userPrompt.WriteString(contentStr)

	if prevEntity != nil {
		prevJSON, _ := json.MarshalIndent(prevEntity, "", "  ")
		userPrompt.WriteString("\n\nLast extracted entity from previous chunk (context only — do not re-extract):\n")
		userPrompt.WriteString(string(prevJSON))
	}

	req := groqRequest{
		Model: groqModel,
		Messages: []groqMessage{
			{Role: "system", Content: systemPrompt.String()},
			{Role: "user", Content: userPrompt.String()},
		},
		Temperature: 0.1,
		MaxTokens:   4096,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequest("POST", groqEndpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("groq request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("groq error %d: %s", resp.StatusCode, string(respBody))
	}

	var groqResp groqResponse
	if err := json.Unmarshal(respBody, &groqResp); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	if len(groqResp.Choices) == 0 {
		return nil, fmt.Errorf("no choices in response")
	}

	raw := strings.TrimSpace(groqResp.Choices[0].Message.Content)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	var results []map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &results); err != nil {
		return nil, fmt.Errorf("parse extracted json: %w", err)
	}

	return results, nil
}

func mergeEntities(all *[]map[string]interface{}, incoming []map[string]interface{}, schema map[string]*SchemaField, seen map[string]bool) {
	dedupField := ""
	for field, def := range schema {
		if def.Type == "text" {
			dedupField = field
			break
		}
	}

	for _, entity := range incoming {
		if dedupField != "" {
			if val, ok := entity[dedupField].(string); ok && val != "" {
				key := strings.ToLower(strings.TrimSpace(val))
				if seen[key] {
					continue
				}
				seen[key] = true
			}
		}
		*all = append(*all, entity)
	}
}

func emptyIfNil(s []interface{}) []interface{} {
	if s == nil {
		return []interface{}{}
	}
	return s
}

func loadKeys() ([]string, error) {
	raw := os.Getenv("EXTRACT_GROQ_KEYS")
	if raw == "" {
		return nil, fmt.Errorf("EXTRACT_GROQ_KEYS not set")
	}
	var keys []string
	for _, k := range strings.Split(raw, ",") {
		k = strings.TrimSpace(k)
		if k != "" {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("EXTRACT_GROQ_KEYS is empty")
	}
	return keys, nil
}

func randomKey(keys []string) string {
	return keys[rand.Intn(len(keys))]
}

func nextChunkFromStrip(stripPath string, maxWords int) (string, error) {
	b, err := os.ReadFile(stripPath)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	content := strings.TrimSpace(string(b))
	if content == "" {
		return "", nil
	}

	words := strings.Fields(content)
	if len(words) <= maxWords {
		os.WriteFile(stripPath, []byte(""), 0644)
		return content, nil
	}

	chunk := strings.Join(words[:maxWords], " ")
	boundary := findSentenceBoundary(chunk)
	if boundary > 0 {
		chunk = chunk[:boundary]
	}

	remainder := strings.TrimSpace(content[len(chunk):])
	if err := os.WriteFile(stripPath, []byte(remainder), 0644); err != nil {
		return "", fmt.Errorf("update strip.txt: %w", err)
	}

	return strings.TrimSpace(chunk), nil
}

func findSentenceBoundary(s string) int {
	for i := len(s) - 1; i >= 1; i-- {
		c := s[i-1]
		next := s[i]
		if (c == '.' || c == '!' || c == '?') && next == ' ' {
			return i
		}
	}
	return -1
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