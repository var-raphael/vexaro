package ai

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/var-raphael/vexaro-engine/db"
)

const (
	groqEndpoint  = "https://api.groq.com/openai/v1/chat/completions"
	groqModel     = "llama-3.3-70b-versatile"
	maxChunkWords = 3000
	maxRetries    = 5
	throttleDelay = 2 * time.Second
	maxWorkers    = 2
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
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

// splitContentChunks splits content into word-based chunks of maxWords each,
// respecting sentence boundaries and adding overlapWords of overlap between
// chunks to avoid cutting mid-entity.
func splitContentChunks(content string, maxWords int, overlapWords int) []string {
	words := strings.Fields(content)
	if len(words) == 0 {
		return nil
	}
	var chunks []string
	start := 0
	for start < len(words) {
		end := start + maxWords
		if end > len(words) {
			end = len(words)
		}
		chunk := strings.Join(words[start:end], " ")
		// snap to sentence boundary only if not at end of content
		if end < len(words) {
			if boundary := findSentenceBoundary(chunk); boundary > 0 {
				chunk = chunk[:boundary]
			}
		}
		chunks = append(chunks, strings.TrimSpace(chunk))
		consumed := len(strings.Fields(chunk))
		next := start + consumed - overlapWords
		if next <= start {
			// guard against infinite loop if overlap >= consumed
			next = start + consumed
		}
		start = next
	}
	return chunks
}

func Extract(schema map[string]*SchemaField) error {
	database := db.Get()

	rows, err := database.Query(`
		SELECT q.queue_id, du.folder_path
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE q.status = 'proceed-extract'
		AND du.folder_path IS NOT NULL
		ORDER BY q.queue_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query proceed-extract: %w", err)
	}
	defer rows.Close()

	type extractJob struct {
		QueueID    int64
		FolderPath string
	}

	var jobs []extractJob
	for rows.Next() {
		var j extractJob
		if err := rows.Scan(&j.QueueID, &j.FolderPath); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan proceed-extract rows: %w", err)
	}

	if len(jobs) == 0 {
		log.Printf("[ai] no proceed-extract urls in queue")
		return nil
	}

	log.Printf("[ai] processing %d proceed-extract jobs", len(jobs))

	keys, err := loadKeys()
	if err != nil {
		return err
	}

	multiKey := len(keys) > 1

	var (
		mu        sync.Mutex
		idx       int
		succeeded int
		failed    int
		workersMu sync.Mutex
		workers   int = 1
		wg        sync.WaitGroup
		sem       = make(chan struct{}, maxWorkers)
	)

	extractWithRateLimit := func(payload map[string]interface{}, prevEntity map[string]interface{}) ([]map[string]interface{}, bool, error) {
	for attempt := 1; attempt <= maxRetries; attempt++ {
		entities, waitSec, err := extractChunkWithRetry(randomKey(keys), payload, schema, prevEntity, keys, multiKey)
		if err == nil {
			return entities, false, nil
		}
		parsedWait := parseGroqWaitTime(err.Error())
		if parsedWait > 0 || waitSec > 0 {
			secs := parsedWait
			if secs == 0 {
				secs = waitSec
			}
			if secs == 0 {
				secs = 5 * float64(attempt) // fallback: 5s, 10s
			}
			log.Printf("[ai] rate limit — waiting %.1fs (attempt %d/%d)", secs, attempt, maxRetries)
			time.Sleep(time.Duration(secs * float64(time.Second)))
			if attempt >= 2 {
				log.Printf("[ai] rate limit persisting after %d attempts — moving on", attempt)
				return nil, true, fmt.Errorf("rate limit retries exhausted after %d attempts", attempt)
			}
			continue
		}
		return nil, false, err
	}
	return nil, true, fmt.Errorf("rate limit retries exhausted after %d attempts", maxRetries)
}

	processOne := func(job extractJob) bool {
		formatPath := filepath.Join(job.FolderPath, "format.json")
		extractPath := filepath.Join(job.FolderPath, "extract.json")

		payload, err := loadJSON(formatPath)
		if err != nil {
			log.Printf("[ai] failed to load queue_id=%d path=%s: %v", job.QueueID, formatPath, err)
			markFailed(database, job.QueueID)
			mu.Lock()
			failed++
			mu.Unlock()
			return false
		}

		pageURL, _ := payload["source_url"].(string)
		pageLinks, _ := payload["links"].([]interface{})
		pageImages, _ := payload["images"].([]interface{})
		pageFiles, _ := payload["files"].([]interface{})

		contentStr, _ := payload["content"].(string)

		// split content into chunks upfront
		chunks := splitContentChunks(contentStr, maxChunkWords, 100)
		if len(chunks) == 0 {
			chunks = []string{contentStr}
		}

		log.Printf("[ai] queue_id=%d — %d chunk(s) to extract", job.QueueID, len(chunks))

		var allEntities []map[string]interface{}
		seen := map[string]bool{}
		rateLimitExhausted := false

		for i, chunk := range chunks {
			chunkPayload := map[string]interface{}{"content": chunk}

			var prevEntity map[string]interface{}
			if len(allEntities) > 0 {
				prevEntity = allEntities[len(allEntities)-1]
			}

			entities, isRateLimit, err := extractWithRateLimit(chunkPayload, prevEntity)
			if err != nil {
				if isRateLimit {
					log.Printf("[ai] rate limit exhausted queue_id=%d chunk=%d — leaving as proceed-extract", job.QueueID, i+1)
					rateLimitExhausted = true
					break
				}
				log.Printf("[ai] chunk=%d extraction failed queue_id=%d: %v — skipping chunk", i+1, job.QueueID, err)
				continue
			}
			mergeEntities(&allEntities, entities, schema, seen)
		}

		if rateLimitExhausted {
			return true
		}

		result := map[string]interface{}{
			"source_url": pageURL,
			"entities":   allEntities,
		}
		if len(pageLinks) > 0 {
			result["links"] = pageLinks
		}
		if len(pageImages) > 0 {
			result["images"] = pageImages
		}
		if len(pageFiles) > 0 {
			result["files"] = pageFiles
		}

		if err := writeJSON(extractPath, result); err != nil {
			log.Printf("[ai] write failed queue_id=%d: %v", job.QueueID, err)
			markFailed(database, job.QueueID)
			mu.Lock()
			failed++
			mu.Unlock()
			return false
		}

		markProceedVersion(database, job.QueueID)
		log.Printf("[ai] extracted %d entities queue_id=%d → %s", len(allEntities), job.QueueID, extractPath)
		mu.Lock()
		succeeded++
		mu.Unlock()

		time.Sleep(throttleDelay)
		return false
	}

	var runWorker func()
	runWorker = func() {
		defer wg.Done()
		defer func() { <-sem }()

		for {
			mu.Lock()
			if idx >= len(jobs) {
				mu.Unlock()
				return
			}
			job := jobs[idx]
			idx++
			mu.Unlock()

			rateLimited := processOne(job)

			if rateLimited && multiKey {
				workersMu.Lock()
				if workers < maxWorkers {
					workers++
					workersMu.Unlock()
					log.Printf("[ai] rate limit hit — spawning second worker (multi-key mode)")
					wg.Add(1)
					go func() {
						sem <- struct{}{}
						runWorker()
					}()
				} else {
					workersMu.Unlock()
				}
			}
		}
	}

	wg.Add(1)
	sem <- struct{}{}
	go runWorker()
	wg.Wait()

	log.Printf("[ai] done — %d extracted, %d failed", succeeded, failed)
	return nil
}

// ---------------------------------------------------------------- db ------

func markProceedVersion(database *sql.DB, queueID int64) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'proceed-version', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[ai] markProceedVersion queue_id=%d: %v", queueID, err)
	}
}

func markFailed(database *sql.DB, queueID int64) {
	_, err := database.Exec(`
		UPDATE queue SET status = 'failed', locked_at = NULL WHERE queue_id = ?
	`, queueID)
	if err != nil {
		log.Printf("[ai] markFailed queue_id=%d: %v", queueID, err)
	}
}

// ExtractFromPaths reads paths from a file and extracts — kept for dev/debug.
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

	var queue []string
	for _, p := range paths {
		if !alreadyDone[p] {
			queue = append(queue, p)
		}
	}

	if len(queue) == 0 {
		log.Printf("[ai] all paths already extracted")
		return nil
	}

	multiKey := len(keys) > 1

	var (
		mu        sync.Mutex
		idx       int
		succeeded int
		failed    int
		workersMu sync.Mutex
		workers   int = 1
		wg        sync.WaitGroup
		sem       = make(chan struct{}, maxWorkers)
	)

	processOne := func(path string) bool {
		formatDir := filepath.Dir(path)
		formatPath := filepath.Join(formatDir, "format.json")
		extractPath := filepath.Join(formatDir, "extract.json")

		payload, err := loadJSON(formatPath)
		if err != nil {
			log.Printf("[ai] failed to load %s: %v", formatPath, err)
			mu.Lock()
			failed++
			mu.Unlock()
			return false
		}

		pageURL, _ := payload["source_url"].(string)
		pageLinks, _ := payload["links"].([]interface{})
		pageImages, _ := payload["images"].([]interface{})
		pageFiles, _ := payload["files"].([]interface{})

		contentStr, _ := payload["content"].(string)

		chunks := splitContentChunks(contentStr, maxChunkWords, 100)
		if len(chunks) == 0 {
			chunks = []string{contentStr}
		}

		var allEntities []map[string]interface{}
		seen := map[string]bool{}
		rateLimited := false

		for i, chunk := range chunks {
			chunkPayload := map[string]interface{}{"content": chunk}

			var prevEntity map[string]interface{}
			if len(allEntities) > 0 {
				prevEntity = allEntities[len(allEntities)-1]
			}

			entities, waitSec, err := extractChunkWithRetry(randomKey(keys), chunkPayload, schema, prevEntity, keys, multiKey)
			if err != nil {
				if waitSec > 0 {
					rateLimited = true
					if !multiKey {
						log.Printf("[ai] single key — waiting %.1fs", waitSec)
						time.Sleep(time.Duration(waitSec * float64(time.Second)))
						entities, _, err = extractChunkWithRetry(randomKey(keys), chunkPayload, schema, prevEntity, keys, false)
					}
				}
				if err != nil {
					log.Printf("[ai] chunk=%d extraction failed %s: %v — skipping chunk", i+1, formatPath, err)
					continue
				}
			}
			mergeEntities(&allEntities, entities, schema, seen)
		}

		result := map[string]interface{}{
			"source_url": pageURL,
			"entities":   allEntities,
		}
		if len(pageLinks) > 0 {
			result["links"] = pageLinks
		}
		if len(pageImages) > 0 {
			result["images"] = pageImages
		}
		if len(pageFiles) > 0 {
			result["files"] = pageFiles
		}

		if err := writeJSON(extractPath, result); err != nil {
			log.Printf("[ai] write failed %s: %v", extractPath, err)
			mu.Lock()
			failed++
			mu.Unlock()
			return false
		}

		if err := appendLine("extract.txt", path); err != nil {
			log.Printf("[ai] warning: could not write to extract.txt: %v", err)
		}

		log.Printf("[ai] extracted %d entities → %s", len(allEntities), extractPath)
		mu.Lock()
		succeeded++
		mu.Unlock()

		time.Sleep(throttleDelay)
		return rateLimited
	}

	var runWorker func()
	runWorker = func() {
		defer wg.Done()
		defer func() { <-sem }()

		for {
			mu.Lock()
			if idx >= len(queue) {
				mu.Unlock()
				return
			}
			path := queue[idx]
			idx++
			mu.Unlock()

			rateLimited := processOne(path)

			if rateLimited && multiKey {
				workersMu.Lock()
				if workers < maxWorkers {
					workers++
					workersMu.Unlock()
					log.Printf("[ai] rate limit hit — spawning second worker (multi-key mode)")
					wg.Add(1)
					go func() {
						sem <- struct{}{}
						runWorker()
					}()
				} else {
					workersMu.Unlock()
				}
			}
		}
	}

	wg.Add(1)
	sem <- struct{}{}
	go runWorker()
	wg.Wait()

	log.Printf("[ai] done — %d extracted, %d already done, %d failed",
		succeeded, len(alreadyDone), failed)
	return nil
}

// ---------------------------------------------------------------- core ------

func extractChunkWithRetry(apiKey string, payload map[string]interface{}, schema map[string]*SchemaField, prevEntity map[string]interface{}, keys []string, rotateOnRetry bool) ([]map[string]interface{}, float64, error) {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		key := apiKey
		if rotateOnRetry && attempt > 1 {
			key = randomKey(keys)
		}
		results, err := extractChunk(key, payload, schema, prevEntity)
		if err == nil {
			return results, 0, nil
		}
		waitSec := parseGroqWaitTime(err.Error())
		if waitSec > 0 {
			if attempt == 1 {
				return nil, waitSec, err
			}
			log.Printf("[ai] rate limited — waiting %.1fs before retry %d/%d", waitSec, attempt, maxRetries)
			time.Sleep(time.Duration(waitSec * float64(time.Second)))
			lastErr = err
			continue
		}
		return nil, 0, err
	}
	return nil, 0, fmt.Errorf("max retries exceeded: %w", lastErr)
}

func parseGroqWaitTime(errMsg string) float64 {
	idx := strings.Index(errMsg, "try again in ")
	if idx == -1 {
		return 0
	}
	rest := errMsg[idx+len("try again in "):]
	var numStr strings.Builder
	for _, ch := range rest {
		if ch >= '0' && ch <= '9' || ch == '.' {
			numStr.WriteRune(ch)
		} else {
			break
		}
	}
	if numStr.Len() == 0 {
		return 0
	}
	val, err := strconv.ParseFloat(numStr.String(), 64)
	if err != nil {
		return 0
	}
	return val + 1
}

func extractChunk(apiKey string, payload map[string]interface{}, schema map[string]*SchemaField, prevEntity map[string]interface{}) ([]map[string]interface{}, error) {
	var schemaDesc strings.Builder
	schemaDesc.WriteString("Fields to extract (return null if not found):\n")
	for fieldName, def := range schema {
		schemaDesc.WriteString(fmt.Sprintf("- %q (type: %s", fieldName, def.Type))
		if def.Description != "" {
			schemaDesc.WriteString(fmt.Sprintf(", description: %s", def.Description))
		}
		schemaDesc.WriteString(")\n")
	}

	contentStr, _ := payload["content"].(string)

	var systemPrompt strings.Builder
	systemPrompt.WriteString("You are a precise data extraction engine.\n")
	systemPrompt.WriteString("The content may describe one or more distinct items (entities).\n")
	systemPrompt.WriteString("Identify each distinct item and extract it as a separate object in a JSON array.\n")
	systemPrompt.WriteString("Every object must contain ALL schema fields as keys.\n")
	systemPrompt.WriteString("Only extract values explicitly stated in the content — do not infer or guess.\n")
	systemPrompt.WriteString("If a field value is not explicitly present for an entity, set it to null.\n")
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
	if len(keys) == 1 {
		log.Printf("[ai] single key detected — concurrency disabled, will wait on rate limits")
	} else {
		log.Printf("[ai] %d keys detected — concurrency will activate on rate limit", len(keys))
	}
	return keys, nil
}

func randomKey(keys []string) string {
	return keys[rand.Intn(len(keys))]
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

// Ask sends a plain prompt to Groq and returns the response string.
// Used by diff module for L2.5 meaningful change check.
func Ask(prompt string) (string, error) {
	keys, err := loadKeys()
	if err != nil {
		return "", err
	}
	req := groqRequest{
		Model: groqModel,
		Messages: []groqMessage{
			{Role: "user", Content: prompt},
		},
		Temperature: 0.1,
		MaxTokens:   10,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	httpReq, err := http.NewRequest("POST", groqEndpoint, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+randomKey(keys))

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("groq request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("groq error %d: %s", resp.StatusCode, string(respBody))
	}
	var groqResp groqResponse
	if err := json.Unmarshal(respBody, &groqResp); err != nil {
		return "", fmt.Errorf("parse response: %w", err)
	}
	if len(groqResp.Choices) == 0 {
		return "", fmt.Errorf("no choices in response")
	}
	return strings.TrimSpace(groqResp.Choices[0].Message.Content), nil
}