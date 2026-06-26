package ai

import (
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
	"github.com/var-raphael/vexaro-engine/storage"
)

const (
	maxChunkWords = 5000
	maxRetries    = 5
	throttleDelay = 2 * time.Second
	maxWorkers    = 2
)

const (
	mistralEndpoint = "https://api.mistral.ai/v1/chat/completions"
	mistralModel    = "mistral-small-latest"
)

type apiKey struct {
	Key      string
	Endpoint string
	Model    string
	Provider string
}

type mistralMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type mistralRequest struct {
	Model       string           `json:"model"`
	Messages    []mistralMessage `json:"messages"`
	Temperature float64          `json:"temperature"`
	MaxTokens   int              `json:"max_tokens"`
}

type mistralChoice struct {
	Message mistralMessage `json:"message"`
}

type mistralResponse struct {
	Choices []mistralChoice `json:"choices"`
}

type SchemaField struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
	DataType    string `json:"data_type,omitempty"`
}

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
		fullWindow := strings.Join(words[start:end], " ")

		chunk := fullWindow
		if end < len(words) {
			if boundary := findSentenceBoundary(chunk); boundary > 0 {
				chunk = chunk[:boundary]
			}
		}
		chunks = append(chunks, strings.TrimSpace(chunk))

		if end >= len(words) {
			break
		}

		// Advance based on the full untruncated window, not the
		// (possibly shorter) sentence-trimmed chunk — otherwise a
		// boundary cut near the end erodes the guaranteed overlap.
		windowConsumed := end - start
		next := start + windowConsumed - overlapWords
		if next <= start {
			next = start + windowConsumed
		}
		start = next
	}
	return chunks
}


// ---------------------------------------------------------------- Key Loading ------

func loadKeys() ([]apiKey, error) {
	var keys []apiKey

	if raw := os.Getenv("EXTRACT_MISTRAL_KEYS"); raw != "" {
		for _, k := range strings.Split(raw, ",") {
			k = strings.TrimSpace(k)
			if k != "" {
				keys = append(keys, apiKey{
					Key:      k,
					Endpoint: mistralEndpoint,
					Model:    mistralModel,
					Provider: "mistral",
				})
			}
		}
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("no API keys set — need EXTRACT_MISTRAL_KEYS")
	}

	log.Printf("[ai] loaded %d mistral key(s)", len(keys))

	if len(keys) == 1 {
		log.Printf("[ai] single key detected — will wait on rate limits")
	} else {
		log.Printf("[ai] %d keys detected — will rotate on rate limit", len(keys))
	}

	return keys, nil
}

func randomKey(keys []apiKey) apiKey {
	return keys[rand.Intn(len(keys))]
}

// ---------------------------------------------------------------- Extract (global) ------

func Extract(schema map[string]*SchemaField) error {
	database := db.Get()

	rows, err := database.Query(`
		SELECT q.queue_id, du.folder_path, COALESCE(d.extract_intent, '')
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE q.status = 'proceed-extract'
		AND du.folder_path IS NOT NULL
		ORDER BY q.queue_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query proceed-extract: %w", err)
	}
	defer rows.Close()

	type extractJob struct {
		QueueID       int64
		FolderPath    string
		ExtractIntent string
	}

	var jobs []extractJob
	for rows.Next() {
		var j extractJob
		if err := rows.Scan(&j.QueueID, &j.FolderPath, &j.ExtractIntent); err != nil {
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

	extractWithRateLimit := func(payload map[string]interface{}, prevEntity map[string]interface{}, extractIntent string) ([]map[string]interface{}, bool, error) {
		availableKeys := keys
		for attempt := 1; attempt <= maxRetries; attempt++ {
			key := randomKey(availableKeys)
			entities, waitSec, err := extractChunkWithRetry(key, payload, schema, prevEntity, availableKeys, multiKey, extractIntent)
			if err == nil {
				return entities, false, nil
			}
			parsedWait := parseWaitTime(err.Error())
			if parsedWait > 0 || waitSec > 0 {
				secs := parsedWait
				if secs == 0 {
					secs = waitSec
				}
				if secs == 0 {
					secs = 5 * float64(attempt)
				}
				log.Printf("[ai] rate limit on mistral — waiting %.1fs (attempt %d/%d)", secs, attempt, maxRetries)
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
		contentStr, _ := payload["content"].(string)

		chunks := splitContentChunks(contentStr, maxChunkWords, 50)
		if len(chunks) == 0 {
			chunks = []string{contentStr}
		}

		log.Printf("[ai] queue_id=%d — %d chunk(s) to extract", job.QueueID, len(chunks))

		var allEntities []map[string]interface{}
		rateLimitExhausted := false

		for i, chunk := range chunks {
			chunkPayload := map[string]interface{}{"content": chunk}

			var prevEntity map[string]interface{}
			if len(allEntities) > 0 {
				prevEntity = allEntities[len(allEntities)-1]
			}

			entities, isRateLimit, err := extractWithRateLimit(chunkPayload, prevEntity, job.ExtractIntent)
			if err != nil {
				if isRateLimit {
					log.Printf("[ai] rate limit exhausted queue_id=%d chunk=%d — leaving as proceed-extract", job.QueueID, i+1)
					rateLimitExhausted = true
					break
				}
				log.Printf("[ai] chunk=%d extraction failed queue_id=%d: %v — skipping chunk", i+1, job.QueueID, err)
				continue
			}
			allEntities = append(allEntities, entities...)
		}

		if rateLimitExhausted {
			return true
		}

		result := map[string]interface{}{
			"source_url": pageURL,
			"entities":   allEntities,
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

// ---------------------------------------------------------------- ExtractDataset ------

func ExtractDataset(datasetID int64, schema map[string]*SchemaField) error {
	database := db.Get()

	rows, err := database.Query(`
		SELECT q.queue_id, du.folder_path, COALESCE(d.extract_intent, '')
		FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		WHERE q.status = 'proceed-extract'
		AND du.folder_path IS NOT NULL
		AND du.dataset_id = ?
		AND q.extract_cluster_role IN ('primary', 'solo')
		ORDER BY q.queue_id ASC
	`, datasetID)
	if err != nil {
		return fmt.Errorf("query proceed-extract: %w", err)
	}
	defer rows.Close()

	type extractJob struct {
		QueueID       int64
		FolderPath    string
		ExtractIntent string
	}

	var jobs []extractJob
	for rows.Next() {
		var j extractJob
		if err := rows.Scan(&j.QueueID, &j.FolderPath, &j.ExtractIntent); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan proceed-extract rows: %w", err)
	}

	if len(jobs) == 0 {
		return nil
	}

	log.Printf("[ai] dataset_id=%d — processing %d proceed-extract jobs", datasetID, len(jobs))

	keys, err := loadKeys()
	if err != nil {
		return err
	}

	multiKey := len(keys) > 1

	var (
		mu         sync.Mutex
		idx        int
		succeeded  int
		failed     int
		workersMu  sync.Mutex
		numWorkers int = 1
		wg         sync.WaitGroup
		sem        = make(chan struct{}, maxWorkers)
	)

	extractWithRateLimit := func(payload map[string]interface{}, prevEntity map[string]interface{}, extractIntent string) ([]map[string]interface{}, bool, error) {
		availableKeys := keys
		for attempt := 1; attempt <= maxRetries; attempt++ {
			key := randomKey(availableKeys)
			entities, waitSec, err := extractChunkWithRetry(key, payload, schema, prevEntity, availableKeys, multiKey, extractIntent)
			if err == nil {
				return entities, false, nil
			}
			parsedWait := parseWaitTime(err.Error())
			if parsedWait > 0 || waitSec > 0 {
				secs := parsedWait
				if secs == 0 {
					secs = waitSec
				}
				if secs == 0 {
					secs = 5 * float64(attempt)
				}
				log.Printf("[ai] rate limit on mistral — waiting %.1fs (attempt %d/%d)", secs, attempt, maxRetries)
				time.Sleep(time.Duration(secs * float64(time.Second)))
				if attempt >= 2 {
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
		contentStr, _ := payload["content"].(string)

		chunks := splitContentChunks(contentStr, maxChunkWords, 50)
		if len(chunks) == 0 {
			chunks = []string{contentStr}
		}

		log.Printf("[ai] queue_id=%d — %d chunk(s) to extract", job.QueueID, len(chunks))

		var allEntities []map[string]interface{}
		rateLimitExhausted := false

		for i, chunk := range chunks {
			chunkPayload := map[string]interface{}{"content": chunk}

			var prevEntity map[string]interface{}
			if len(allEntities) > 0 {
				prevEntity = allEntities[len(allEntities)-1]
			}

			entities, isRateLimit, err := extractWithRateLimit(chunkPayload, prevEntity, job.ExtractIntent)
			if err != nil {
				if isRateLimit {
					log.Printf("[ai] rate limit exhausted queue_id=%d chunk=%d — leaving as proceed-extract", job.QueueID, i+1)
					rateLimitExhausted = true
					break
				}
				log.Printf("[ai] chunk=%d extraction failed queue_id=%d: %v — skipping chunk", i+1, job.QueueID, err)
				continue
			}
			allEntities = append(allEntities, entities...)
		}

		if rateLimitExhausted {
			return true
		}

		result := map[string]interface{}{
			"source_url": pageURL,
			"entities":   allEntities,
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
				if numWorkers < maxWorkers {
					numWorkers++
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

	log.Printf("[ai] dataset_id=%d done — %d extracted, %d failed", datasetID, succeeded, failed)
	return nil
}

// ---------------------------------------------------------------- core ------

func extractChunkWithRetry(key apiKey, payload map[string]interface{}, schema map[string]*SchemaField, prevEntity map[string]interface{}, keys []apiKey, rotateOnRetry bool, extractIntent string) ([]map[string]interface{}, float64, error) {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		currentKey := key
		if rotateOnRetry && attempt > 1 {
			currentKey = randomKey(keys)
		}
		results, err := extractChunk(currentKey, payload, schema, prevEntity, extractIntent)
		if err == nil {
			log.Printf("[ai] chunk extracted via mistral")
			return results, 0, nil
		}
		waitSec := parseWaitTime(err.Error())
		if waitSec > 0 {
			if attempt == 1 {
				return nil, waitSec, err
			}
			log.Printf("[ai] rate limited on mistral — waiting %.1fs before retry %d/%d", waitSec, attempt, maxRetries)
			time.Sleep(time.Duration(waitSec * float64(time.Second)))
			lastErr = err
			continue
		}
		return nil, 0, err
	}
	return nil, 0, fmt.Errorf("max retries exceeded: %w", lastErr)
}

func parseWaitTime(errMsg string) float64 {
	for _, prefix := range []string{"try again in ", "retry after ", "please wait "} {
		idx := strings.Index(errMsg, prefix)
		if idx == -1 {
			continue
		}
		rest := errMsg[idx+len(prefix):]
		var numStr strings.Builder
		for _, ch := range rest {
			if ch >= '0' && ch <= '9' || ch == '.' {
				numStr.WriteRune(ch)
			} else {
				break
			}
		}
		if numStr.Len() == 0 {
			continue
		}
		val, err := strconv.ParseFloat(numStr.String(), 64)
		if err != nil {
			continue
		}
		return val + 1
	}
	if strings.Contains(errMsg, "429") || strings.Contains(errMsg, "rate limit") || strings.Contains(errMsg, "rate_limit") {
		return 5
	}
	return 0
}

func hasURLField(schema map[string]*SchemaField) bool {
	for _, def := range schema {
		if def.DataType == "url" {
			return true
		}
	}
	return false
}

func extractChunk(key apiKey, payload map[string]interface{}, schema map[string]*SchemaField, prevEntity map[string]interface{}, extractIntent string) ([]map[string]interface{}, error) {
	var schemaDesc strings.Builder
	schemaDesc.WriteString("Fields to extract (return null if not found):\n")
	for fieldName, def := range schema {
		schemaDesc.WriteString(fmt.Sprintf("- %q (field: %s", fieldName, def.Type))
		if def.Description != "" {
			schemaDesc.WriteString(fmt.Sprintf(", description: %s", def.Description))
		}
		if def.DataType == "array" {
			schemaDesc.WriteString(" — always return as a flat array of primitives, never nested objects)")
		} else if def.DataType != "" {
			schemaDesc.WriteString(" — always return as a plain " + def.DataType + ", never an object or array)")
		}
		schemaDesc.WriteString("\n")
	}

	contentStr, _ := payload["content"].(string)

	var systemPrompt strings.Builder
	systemPrompt.WriteString("You are a precise data extraction engine.\n")
	systemPrompt.WriteString("The content may describe one or more distinct items (entities).\n")
	systemPrompt.WriteString("Identify each distinct item and extract it as a separate object in a JSON array.\n")
	systemPrompt.WriteString("Every object must contain ALL schema fields as keys — no more, no less.\n")
	systemPrompt.WriteString("Never add fields that are not defined in the schema.\n")
	systemPrompt.WriteString("Only extract values explicitly stated in the content — do not infer or guess.\n")
	systemPrompt.WriteString("If you are not 100% certain a value is explicitly stated, return null. Never infer.\n")
	systemPrompt.WriteString("If a field value is not explicitly present for an entity, set it to null.\n")
	systemPrompt.WriteString("Never return nested objects or arrays of objects anywhere in the output.\n")
	systemPrompt.WriteString("If no entities are found in the content, return an empty JSON array [].\n")
	systemPrompt.WriteString("Return ONLY a valid JSON array. No explanation, no markdown, no code blocks.\n")
	systemPrompt.WriteString("IMPORTANT: Always close the JSON array with ] — never leave it incomplete.\n")

	if hasURLField(schema) {
		systemPrompt.WriteString("\nThe content contains plain-text URLs (e.g. https://example.com/page) inline, ")
		systemPrompt.WriteString("interspersed with the surrounding text rather than as HTML links.\n")
		systemPrompt.WriteString("When a schema field has a url data type, find the URL that most plausibly belongs to ")
		systemPrompt.WriteString("that specific entity based on its position in the content — usually the nearest URL ")
		systemPrompt.WriteString("appearing immediately before or after that entity's other details.\n")
		systemPrompt.WriteString("Do not reuse the same URL for multiple entities unless it is the only URL present.\n")
	}

	if extractIntent != "" {
		systemPrompt.WriteString("\nExtraction intent — use this to guide what matters and what to focus on:\n")
		systemPrompt.WriteString(extractIntent + "\n")
	}

	var userPrompt strings.Builder
	userPrompt.WriteString(schemaDesc.String())
	userPrompt.WriteString("\nContent:\n")
	userPrompt.WriteString(contentStr)

	if prevEntity != nil {
		prevJSON, _ := json.MarshalIndent(prevEntity, "", "  ")
		userPrompt.WriteString("\n\nLast extracted entity from previous chunk (context only — do not re-extract):\n")
		userPrompt.WriteString(string(prevJSON))
	}

	req := mistralRequest{
		Model: key.Model,
		Messages: []mistralMessage{
			{Role: "system", Content: systemPrompt.String()},
			{Role: "user", Content: userPrompt.String()},
		},
		Temperature: 0.1,
		MaxTokens:   8192,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequest("POST", key.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+key.Key)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("mistral request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mistral error %d: %s", resp.StatusCode, string(respBody))
	}

	var mistralResp mistralResponse
	if err := json.Unmarshal(respBody, &mistralResp); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	if len(mistralResp.Choices) == 0 {
		return nil, fmt.Errorf("no choices in response")
	}

	raw := strings.TrimSpace(mistralResp.Choices[0].Message.Content)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	if !strings.HasSuffix(raw, "]") {
		log.Printf("[ai] mistral response appears truncated — attempting salvage")
		if idx := strings.LastIndex(raw, "},"); idx != -1 {
			raw = raw[:idx+1] + "]"
		} else if idx := strings.LastIndex(raw, "}"); idx != -1 {
			raw = raw[:idx+1] + "]"
		} else {
			log.Printf("[ai] could not salvage truncated response — returning empty")
			return []map[string]interface{}{}, nil
		}
		log.Printf("[ai] salvaged truncated JSON array")
	}

	var results []map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &results); err != nil {
		return nil, fmt.Errorf("parse extracted json: %w", err)
	}

	return results, nil
}

// ---------------------------------------------------------------- helpers ------

func findSentenceBoundary(s string) int {
	for i := len(s) - 1; i >= 1; i-- {
		c := s[i-1]
		next := s[i]
		if (c == '.' || c == '!' || c == '?') && next == ' ' {
			if isInsideURL(s, i-1) {
				continue
			}
			return i
		}
	}
	return -1
}

// isInsideURL checks whether the period at index idx in s is part of a
// URL token rather than a real sentence-ending period, by scanning back
// to the start of the current whitespace-delimited token.
func isInsideURL(s string, idx int) bool {
	tokenStart := idx
	for tokenStart > 0 && s[tokenStart-1] != ' ' && s[tokenStart-1] != '\n' && s[tokenStart-1] != '\t' {
		tokenStart--
	}
	token := s[tokenStart : idx+1]
	return strings.HasPrefix(token, "http://") || strings.HasPrefix(token, "https://")
}


func loadJSON(path string) (map[string]interface{}, error) {
	b, err := storage.Read(path)
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
	return storage.Write(path, b)
}

func Ask(prompt string) (string, error) {
	keys, err := loadKeys()
	if err != nil {
		return "", err
	}
	key := randomKey(keys)
	req := mistralRequest{
		Model: key.Model,
		Messages: []mistralMessage{
			{Role: "user", Content: prompt},
		},
		Temperature: 0.1,
		MaxTokens:   10,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	httpReq, err := http.NewRequest("POST", key.Endpoint, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+key.Key)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("mistral request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("mistral error %d: %s", resp.StatusCode, string(respBody))
	}
	var mistralResp mistralResponse
	if err := json.Unmarshal(respBody, &mistralResp); err != nil {
		return "", fmt.Errorf("parse response: %w", err)
	}
	if len(mistralResp.Choices) == 0 {
		return "", fmt.Errorf("no choices in response")
	}
	return strings.TrimSpace(mistralResp.Choices[0].Message.Content), nil
}