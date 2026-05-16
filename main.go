package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/var-raphael/vexaro-engine/ai"
	"github.com/var-raphael/vexaro-engine/clean"
	"github.com/var-raphael/vexaro-engine/combine"
	"github.com/var-raphael/vexaro-engine/crawl"
	"github.com/var-raphael/vexaro-engine/db"
	"github.com/var-raphael/vexaro-engine/diff"
	"github.com/var-raphael/vexaro-engine/format"
	"github.com/var-raphael/vexaro-engine/ping"
	"github.com/var-raphael/vexaro-engine/reddit"
	"github.com/var-raphael/vexaro-engine/serp"
	"github.com/var-raphael/vexaro-engine/versioning"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("[env] no .env file found — relying on system env")
	}

	defer db.Close()

	http.HandleFunc("/queue", corsMiddleware(queueHandler))
	http.HandleFunc("/run", corsMiddleware(runHandler))
	http.HandleFunc("/ping", corsMiddleware(ping.PingHandler))
	http.HandleFunc("/test-serp", corsMiddleware(testSerpHandler))
	http.HandleFunc("/test-reddit", corsMiddleware(testRedditHandler))
	http.HandleFunc("/test-crawl", corsMiddleware(testCrawlHandler))
	http.HandleFunc("/test-clean", corsMiddleware(testCleanHandler))
	http.HandleFunc("/test-format", corsMiddleware(testFormatHandler))
	http.HandleFunc("/test-extract", corsMiddleware(testExtractHandler))
	http.HandleFunc("/test-combine", corsMiddleware(testCombineHandler))
	http.HandleFunc("/test-rescrape", corsMiddleware(testRescrapeHandler))
	http.HandleFunc("/datasets", corsMiddleware(datasetsHandler))
	http.HandleFunc("/test-rollback", corsMiddleware(testRollbackHandler))
	http.HandleFunc("/dataset/freeze", corsMiddleware(freezeHandler))
	http.HandleFunc("/dataset/rollback", corsMiddleware(rollbackHandler))
	http.HandleFunc("/dataset/view", corsMiddleware(datasetViewHandler))
	http.HandleFunc("/dataset/diff", corsMiddleware(datasetDiffHandler))
	http.HandleFunc("/dataset/result", corsMiddleware(datasetResultHandler))
	http.HandleFunc("/dataset/edit", corsMiddleware(editDatasetHandler))
	http.HandleFunc("/queue/reddit", corsMiddleware(queueRedditHandler))
	http.HandleFunc("/dataset/refresh", corsMiddleware(refreshHandler))
	http.HandleFunc("/nightly", corsMiddleware(nightlyHandler))
	http.HandleFunc("/test-retry", corsMiddleware(testRetryHandler))
	http.HandleFunc("/dataset/reddit/view", corsMiddleware(redditDatasetViewHandler))
	http.HandleFunc("/dataset/reddit/diff", corsMiddleware(redditDatasetDiffHandler))
	http.HandleFunc("/dataset/reddit/result", corsMiddleware(redditDatasetResultHandler))
	http.HandleFunc("/dataset/reddit/edit", corsMiddleware(redditEditDatasetHandler))
	http.HandleFunc("/dataset/delete", corsMiddleware(deleteDatasetHandler))
	

	port := ":8080"
	log.Println("server running on", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal(err)
	}
}

// ---------------------------------------------------------------- cors --

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:3000")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

// ---------------------------------------------------------------- types --

type QueueRequest struct {
	UserID      string        `json:"user_id"`
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Tag         string        `json:"tag"`
	Visibility  string        `json:"visibility"`
	Nightly     string        `json:"nightly"`
	Intent      string        `json:"intent"`
	Schema      []SchemaInput `json:"schema"`
	URLs        string        `json:"urls"`
}

type SchemaInput struct {
	ID          int    `json:"id"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

type ClassifiedURLs struct {
	SERP   []string
	Import []string
	Reddit []string
}

// ---------------------------------------------------------------- schema --

func loadSchema(datasetID int64) (map[string]*ai.SchemaField, map[string]json.RawMessage, error) {
	var fieldsJSON string
	err := db.Get().QueryRow(`
		SELECT fields FROM dataset_schema WHERE dataset_id = ?
	`, datasetID).Scan(&fieldsJSON)
	if err != nil {
		return nil, nil, fmt.Errorf("load schema: %w", err)
	}

	var rawFields map[string]struct {
		Type        string `json:"type"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal([]byte(fieldsJSON), &rawFields); err != nil {
		return nil, nil, fmt.Errorf("parse schema: %w", err)
	}

	extractSchema := map[string]*ai.SchemaField{}
	formatSchema := map[string]json.RawMessage{}
	for key, f := range rawFields {
		extractSchema[key] = &ai.SchemaField{Type: f.Type, Description: f.Description}
		formatSchema[key] = json.RawMessage(`"` + f.Type + `"`)
	}

	return extractSchema, formatSchema, nil
}

// ---------------------------------------------------------------- /queue --

func queueHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req QueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	// ── Validation (still sync — fast) ──────────────────────────────────────
	var validationErrors []string
	if strings.TrimSpace(req.UserID) == "" {
		validationErrors = append(validationErrors, "user_id is required")
	}
	if strings.TrimSpace(req.Name) == "" {
		validationErrors = append(validationErrors, "name is required")
	}
	if strings.TrimSpace(req.Description) == "" {
		validationErrors = append(validationErrors, "description is required")
	}
	if len(strings.TrimSpace(req.Intent)) < 20 {
		validationErrors = append(validationErrors, fmt.Sprintf("intent must be at least 20 characters (got %d)", len(strings.TrimSpace(req.Intent))))
	}
	if req.Visibility != "public" && req.Visibility != "private" {
		validationErrors = append(validationErrors, "visibility must be 'public' or 'private'")
	}
	if req.Nightly != "yes" && req.Nightly != "no" {
		validationErrors = append(validationErrors, "nightly must be 'yes' or 'no'")
	}
	if len(req.Schema) == 0 {
		validationErrors = append(validationErrors, "at least one schema field is required")
	}
	for i, f := range req.Schema {
		if strings.TrimSpace(f.Type) == "" || strings.TrimSpace(f.Description) == "" {
			validationErrors = append(validationErrors, fmt.Sprintf("schema field %d must have a type and description", i+1))
		}
	}
	if len(validationErrors) > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":  "validation failed",
			"fields": validationErrors,
		})
		return
	}

	// ── Build schema JSON (sync — fast) ─────────────────────────────────────
	schemaFields := make(map[string]map[string]string)
	for _, f := range req.Schema {
		schemaFields[f.Type] = map[string]string{
			"type":        f.Type,
			"description": f.Description,
		}
	}
	schemaJSON, err := json.Marshal(schemaFields)
	if err != nil {
		http.Error(w, "marshal schema: "+err.Error(), http.StatusInternalServerError)
		return
	}

	nightly := 0
	if req.Nightly == "yes" {
		nightly = 1
	}

	// ── Insert dataset meta immediately ──────────────────────────────────────
	res, err := db.Get().Exec(`
		INSERT INTO datasets (user_id, data_name, alias, description, intent, tag, visibility, nightly, is_cloned)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
	`, req.UserID, req.Name, req.Name, req.Description, req.Intent, req.Tag, req.Visibility, nightly)
	if err != nil {
		http.Error(w, "insert dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	datasetID, err := res.LastInsertId()
	if err != nil {
		http.Error(w, "dataset id: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err := db.Get().Exec(`
		INSERT INTO dataset_schema (dataset_id, fields)
		VALUES (?, ?)
	`, datasetID, string(schemaJSON)); err != nil {
		http.Error(w, "insert schema: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[queue] dataset inserted — id: %d, kicking off background pipeline", datasetID)

	// ── Respond immediately ──────────────────────────────────────────────────
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":         true,
		"dataset_id": datasetID,
	})

	// ── Background: SERP + classify + queue rows ─────────────────────────────
	go func() {
		log.Printf("[queue/bg] running SERP for dataset_id=%d name=%q", datasetID, req.Name)
		serpResult, err := serp.Fetch(serp.SERPRequest{
			UserID:   req.UserID,
			DataName: req.Name,
			Intent:   req.Intent,
			Limit:    60,
		})
		if err != nil {
			log.Printf("[queue/bg] serp error dataset_id=%d: %v", datasetID, err)
			return
		}
		log.Printf("[queue/bg] SERP returned %d urls for dataset_id=%d", len(serpResult.URLs), datasetID)

		var importURLs []string
		for _, line := range strings.Split(req.URLs, "\n") {
			line = strings.TrimSpace(line)
			if line != "" && strings.HasPrefix(line, "http") {
				importURLs = append(importURLs, line)
			}
		}

		seen := make(map[string]bool)
		allURLs := append(serpResult.URLs, importURLs...)
		var dedupedURLs []string
		for _, u := range allURLs {
			normalized := strings.TrimRight(strings.ToLower(strings.TrimSpace(u)), "/")
			if normalized == "" || seen[normalized] {
				continue
			}
			seen[normalized] = true
			dedupedURLs = append(dedupedURLs, u)
		}

		serpSet := make(map[string]bool)
		for _, u := range serpResult.URLs {
			serpSet[strings.TrimRight(strings.ToLower(strings.TrimSpace(u)), "/")] = true
		}

		classified := ClassifiedURLs{}
		for _, u := range dedupedURLs {
			normalized := strings.TrimRight(strings.ToLower(strings.TrimSpace(u)), "/")
			switch {
			case isRedditURL(u):
				classified.Reddit = append(classified.Reddit, u)
			case serpSet[normalized]:
				classified.SERP = append(classified.SERP, u)
			default:
				classified.Import = append(classified.Import, u)
			}
		}
		log.Printf("[queue/bg] classified — serp: %d, import: %d, reddit: %d",
			len(classified.SERP), len(classified.Import), len(classified.Reddit))

		type urlEntry struct {
			url        string
			sourceType string
		}
		var entries []urlEntry
		for _, u := range classified.SERP {
			entries = append(entries, urlEntry{u, "serp"})
		}
		for _, u := range classified.Import {
			entries = append(entries, urlEntry{u, "import"})
		}
		for _, u := range classified.Reddit {
			entries = append(entries, urlEntry{u, "reddit"})
		}

		tx, err := db.Get().Begin()
		if err != nil {
			log.Printf("[queue/bg] begin tx error dataset_id=%d: %v", datasetID, err)
			return
		}
		defer tx.Rollback()

		for _, entry := range entries {
			urlType := "extraction"
			for _, serpEntry := range serpResult.Entries {
				if serpEntry.URL == entry.url {
					urlType = serpEntry.URLType
					break
				}
			}
			if entry.sourceType == "import" {
				urlType = "extraction"
			}

			res, err := tx.Exec(`
				INSERT INTO datasets_url (dataset_id, url, rendered_by, source_type, url_type)
				VALUES (?, ?, 'browserless', ?, ?)
			`, datasetID, entry.url, entry.sourceType, urlType)
			if err != nil {
				log.Printf("[queue/bg] insert url error dataset_id=%d url=%s: %v", datasetID, entry.url, err)
				return
			}
			datasetURLID, err := res.LastInsertId()
			if err != nil {
				log.Printf("[queue/bg] url id error: %v", err)
				return
			}

			if entry.sourceType == "reddit" {
				_, err = tx.Exec(`
					INSERT INTO reddit_queue (dataset_url_id, status)
					VALUES (?, 'pending')
				`, datasetURLID)
			} else {
				_, err = tx.Exec(`
					INSERT INTO queue (dataset_url_id, status, crawl_type)
					VALUES (?, 'pending', 'fresh')
				`, datasetURLID)
			}
			if err != nil {
				log.Printf("[queue/bg] insert queue error dataset_id=%d: %v", datasetID, err)
				return
			}
		}

		if err := tx.Commit(); err != nil {
			log.Printf("[queue/bg] commit error dataset_id=%d: %v", datasetID, err)
			return
		}

		log.Printf("[queue/bg] done — dataset_id=%d total urls queued: %d", datasetID, len(entries))
	}()
}
// ---------------------------------------------------------------- /run --

func runHandler(w http.ResponseWriter, r *http.Request) {
	const apiName = "Upcoming Movies 2026"

	cfg := crawl.Config{BrowserlessKey: os.Getenv("BROWSERLESS_KEYS")}

	var datasetID int64
	var userID string
	if err := db.Get().QueryRow(`
		SELECT dataset_id, user_id FROM datasets WHERE data_name = ? ORDER BY created_at DESC LIMIT 1
	`, apiName).Scan(&datasetID, &userID); err != nil {
		http.Error(w, "load dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("[run] stage 1 — crawl")
	if err := crawl.Crawl(cfg, apiName); err != nil {
		http.Error(w, "crawl: "+err.Error(), http.StatusInternalServerError)
		return
	}
	saveStage("crawl", map[string]interface{}{"api_name": apiName})

	rows, err := db.Get().Query(`
		SELECT du.url FROM reddit_queue rq
		JOIN datasets_url du ON du.dataset_url_id = rq.dataset_url_id
		WHERE du.dataset_id = ? AND rq.status = 'pending'
	`, datasetID)
	if err != nil {
		http.Error(w, "load reddit urls: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			continue
		}
		unwrapped := unwrapGoogleURL(u)
		log.Printf("[run] reddit fetch — %s", unwrapped)
		bytes, err := reddit.FetchRaw(unwrapped)
		if err != nil {
			log.Printf("[run] reddit failed %s: %v", unwrapped, err)
		} else if err := reddit.SaveRaw(bytes, unwrapped, datasetID, userID, apiName); err != nil {
			log.Printf("[run] reddit save failed %s: %v", unwrapped, err)
		}
	}

	log.Println("[run] stage 2 — clean")
	if err := clean.Clean(); err != nil {
		http.Error(w, "clean: "+err.Error(), http.StatusInternalServerError)
		return
	}
	saveStage("clean", map[string]interface{}{"status": "done"})

	log.Println("[run] stage 3 — format")
	extractSchema, formatSchema, err := loadSchema(datasetID)
	if err != nil {
		http.Error(w, "load schema: "+err.Error(), http.StatusInternalServerError)
		return
	}
	fs, err := format.ParseSchema(formatSchema)
	if err != nil {
		http.Error(w, "format schema: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := format.Format(fs); err != nil {
		http.Error(w, "format: "+err.Error(), http.StatusInternalServerError)
		return
	}
	saveStage("format", map[string]interface{}{"status": "done"})

	log.Println("[run] stage 4 — extract")
	if err := ai.Extract(extractSchema); err != nil {
		http.Error(w, "extract: "+err.Error(), http.StatusInternalServerError)
		return
	}
	saveStage("extract", map[string]interface{}{"status": "done"})

	log.Println("[run] stage 5 — combine")
	if err := combine.Combine(); err != nil {
		http.Error(w, "combine: "+err.Error(), http.StatusInternalServerError)
		return
	}
	saveStage("combine", map[string]interface{}{"status": "done"})

	log.Println("[run] pipeline complete")
	w.Write([]byte("done\n"))
}

// ---------------------------------------------------------------- helpers --

func isRedditURL(u string) bool {
	unwrapped := unwrapGoogleURL(u)
	return strings.Contains(unwrapped, "reddit.com") ||
		strings.Contains(unwrapped, "old.reddit.com") ||
		strings.Contains(unwrapped, "redd.it")
}

func unwrapGoogleURL(u string) string {
	if !strings.Contains(u, "google.com/url") {
		return u
	}
	parsed, err := url.Parse(u)
	if err != nil {
		return u
	}
	inner := parsed.Query().Get("url")
	if inner == "" {
		return u
	}
	return inner
}

func saveStage(stage string, data map[string]interface{}) {
	dir := "data/stages/" + stage
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("[run] saveStage mkdir %s: %v", stage, err)
		return
	}
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Printf("[run] saveStage marshal %s: %v", stage, err)
		return
	}
	path := dir + "/" + stage + ".json"
	if err := os.WriteFile(path, b, 0644); err != nil {
		log.Printf("[run] saveStage write %s: %v", stage, err)
		return
	}
	log.Printf("[run] saved stage snapshot → %s", path)
}

// ---------------------------------------------------------------- test handlers --

func testSerpHandler(w http.ResponseWriter, r *http.Request) {
	var datasetID int64
	var userID, intent string
	err := db.Get().QueryRow(`
		SELECT dataset_id, user_id, intent FROM datasets WHERE data_name = ? ORDER BY created_at DESC LIMIT 1
	`, "Upcoming movie").Scan(&datasetID, &userID, &intent)
	if err != nil {
		http.Error(w, "load dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}

	res, err := serp.Fetch(serp.SERPRequest{
		UserID:   userID,
		DataName: "Upcoming movie",
		Intent:   intent,
		Limit:    60,
	})
	if err != nil {
		log.Println("serp error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	content := "Source: " + res.Source + "\n\n" + strings.Join(res.URLs, "\n")
	if err := os.WriteFile("serp_results.txt", []byte(content), 0644); err != nil {
		log.Println("write error:", err)
		http.Error(w, "failed to write file", http.StatusInternalServerError)
		return
	}
	log.Println("serp results written to serp_results.txt")
	w.Write([]byte("done — check serp_results.txt\n"))
}

func testRedditHandler(w http.ResponseWriter, r *http.Request) {
	b, err := os.ReadFile("serp_results.txt")
	if err != nil {
		http.Error(w, "serp_results.txt not found — run /test-serp first", http.StatusInternalServerError)
		return
	}

	var datasetID int64
	var userID string
	err = db.Get().QueryRow(`
		SELECT dataset_id, user_id FROM datasets WHERE data_name = ? ORDER BY created_at DESC LIMIT 1
	`, "test-api").Scan(&datasetID, &userID)
	if err != nil {
		http.Error(w, "load dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var redditURLs []string
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "http") {
			continue
		}
		if isRedditURL(line) {
			redditURLs = append(redditURLs, unwrapGoogleURL(line))
		}
	}
	if len(redditURLs) == 0 {
		http.Error(w, "no reddit urls found in serp_results.txt", http.StatusBadRequest)
		return
	}
	log.Printf("[test-reddit] found %d reddit urls", len(redditURLs))
	for _, u := range redditURLs {
		log.Printf("[test-reddit] fetching %s", u)
		bytes, err := reddit.FetchRaw(u)
		if err != nil {
			log.Printf("[test-reddit] failed %s: %v", u, err)
		} else if err := reddit.SaveRaw(bytes, u, datasetID, userID, "test-api"); err != nil {
			log.Printf("[test-reddit] save failed %s: %v", u, err)
		}
	}
	w.Write([]byte("done — check data/reddit.com/\n"))
}

func testCrawlHandler(w http.ResponseWriter, r *http.Request) {
	cfg := crawl.Config{BrowserlessKey: os.Getenv("BROWSERLESS_KEYS")}
	if err := crawl.Crawl(cfg, "test-api"); err != nil {
		log.Println("crawl error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("crawl complete")
	w.Write([]byte("done — check queue table\n"))
}

func testCleanHandler(w http.ResponseWriter, r *http.Request) {
	if err := clean.Clean(); err != nil {
		log.Println("clean error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("clean complete")
	w.Write([]byte("done — check queue table and cleaned.json files\n"))
}

func testFormatHandler(w http.ResponseWriter, r *http.Request) {
	var datasetID int64
	if err := db.Get().QueryRow(`
		SELECT dataset_id FROM datasets WHERE data_name = ? ORDER BY created_at DESC LIMIT 1
	`, "test-api").Scan(&datasetID); err != nil {
		http.Error(w, "load dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	_, formatSchema, err := loadSchema(datasetID)
	if err != nil {
		log.Println("load schema error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fs, err := format.ParseSchema(formatSchema)
	if err != nil {
		log.Println("format schema error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := format.Format(fs); err != nil {
		log.Println("format error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("format complete")
	w.Write([]byte("done — check queue table and format.json files\n"))
}

func testExtractHandler(w http.ResponseWriter, r *http.Request) {
	var datasetID int64
	if err := db.Get().QueryRow(`
		SELECT dataset_id FROM datasets WHERE data_name = ? ORDER BY created_at DESC LIMIT 1
	`, "test-api").Scan(&datasetID); err != nil {
		http.Error(w, "load dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	extractSchema, _, err := loadSchema(datasetID)
	if err != nil {
		log.Println("load schema error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := ai.Extract(extractSchema); err != nil {
		log.Println("extract error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("extract complete")
	w.Write([]byte("done — check queue table and extract.json files\n"))
}

func testCombineHandler(w http.ResponseWriter, r *http.Request) {
	_, err := db.Get().Exec(`
		UPDATE queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		JOIN datasets d ON d.dataset_id = du.dataset_id
		SET q.status = 'proceed-version'
		WHERE d.data_name = 'test-api'
		AND q.status = 'done'
		AND du.folder_path IS NOT NULL
	`)
	if err != nil {
		http.Error(w, "reset queue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := combine.Combine(); err != nil {
		log.Println("combine error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("combine complete")
	w.Write([]byte("done — check dataset folder for result-vN.json\n"))
}

func testRescrapeHandler(w http.ResponseWriter, r *http.Request) {
	cfg := crawl.Config{BrowserlessKey: os.Getenv("BROWSERLESS_KEYS")}

	var datasetID int64
	if err := db.Get().QueryRow(`
		SELECT dataset_id FROM datasets WHERE data_name = ? ORDER BY created_at DESC LIMIT 1
	`, "test-api").Scan(&datasetID); err != nil {
		http.Error(w, "load dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}

	extractSchema, formatSchema, err := loadSchema(datasetID)
	if err != nil {
		log.Println("load schema error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := diff.Run(datasetID, cfg, extractSchema, formatSchema); err != nil {
		log.Println("rescrape error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte("done — rescrape complete\n"))
}

func testRollbackHandler(w http.ResponseWriter, r *http.Request) {
	versionParam := strings.TrimSpace(r.URL.Query().Get("version"))
	if versionParam == "" {
		http.Error(w, "version param required: ?version=N", http.StatusBadRequest)
		return
	}
	targetVersion, err := strconv.Atoi(versionParam)
	if err != nil || targetVersion < 1 {
		http.Error(w, "version must be a positive integer", http.StatusBadRequest)
		return
	}

	datasetFolder, err := combine.ResolveDatasetFolder("test-api")
	if err != nil {
		log.Println("resolve dataset error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	datasetID, err := combine.ResolveDatasetID(datasetFolder)
	if err != nil {
		http.Error(w, "resolve dataset id: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := versioning.Rollback(datasetID, targetVersion); err != nil {
		log.Println("rollback error:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte("done — rolled back to v" + versionParam + "\n"))
}

func datasetsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID := strings.TrimSpace(r.URL.Query().Get("user_id"))
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	rows, err := db.Get().Query(`
		SELECT
			d.dataset_id,
			d.data_name,
			COALESCE(d.alias, d.data_name) AS alias,
			COALESCE(d.tag, '') AS tag,
			d.visibility,
			d.is_frozen,
			d.is_cloned,
			d.created_at,
			d.active_version,
			COALESCE(dv.created_at, d.created_at) AS last_refresh,
			COALESCE(
				(SELECT q.status FROM queue q
				 JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
				 WHERE du.dataset_id = d.dataset_id
				 AND q.status NOT IN ('done', 'failed')
				 LIMIT 1),
				(SELECT rq.status FROM reddit_queue rq
				 JOIN datasets_url du ON du.dataset_url_id = rq.dataset_url_id
				 WHERE du.dataset_id = d.dataset_id
				 AND rq.status NOT IN ('done', 'failed')
				 LIMIT 1),
				'done'
			) AS queue_status,
			COALESCE(
				(SELECT GROUP_CONCAT(dv2.version_number ORDER BY dv2.version_number DESC)
				 FROM dataset_versions dv2
				 WHERE dv2.dataset_id = d.dataset_id),
				''
			) AS versions,
			CASE
				WHEN ds.source = 'reddit' THEN 'reddit'
				WHEN EXISTS (
					SELECT 1 FROM dataset_subreddits dsr WHERE dsr.dataset_id = d.dataset_id
				) THEN 'reddit'
				ELSE 'web'
			END AS source
		FROM datasets d
		LEFT JOIN dataset_versions dv
			ON dv.dataset_id = d.dataset_id
			AND dv.version_number = d.active_version
		LEFT JOIN dataset_schema ds ON ds.dataset_id = d.dataset_id
		WHERE d.user_id = ?
		ORDER BY d.created_at DESC
	`, userID)
	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type DatasetRow struct {
		DatasetID     int64     `json:"dataset_id"`
		Name          string    `json:"name"`
		Alias         string    `json:"alias"`
		Tag           string    `json:"tag"`
		Visibility    string    `json:"visibility"`
		IsFrozen      bool      `json:"is_frozen"`
		IsCloned      bool      `json:"is_cloned"`
		CreatedAt     time.Time `json:"created_at"`
		ActiveVersion int       `json:"version"`
		LastRefresh   time.Time `json:"last_refresh"`
		Status        string    `json:"status"`
		Versions      []int     `json:"versions"`
		DatasetType   string    `json:"dataset_type"`
	}

	var datasets []DatasetRow

	for rows.Next() {
		var d DatasetRow
		var alias, tag, versionsStr, queueStatus, source sql.NullString
		var activeVersion sql.NullInt64
		var isFrozen, isCloned int

		if err := rows.Scan(
			&d.DatasetID,
			&d.Name,
			&alias,
			&tag,
			&d.Visibility,
			&isFrozen,
			&isCloned,
			&d.CreatedAt,
			&activeVersion,
			&d.LastRefresh,
			&queueStatus,
			&versionsStr,
			&source,
		); err != nil {
			log.Printf("[datasets] scan error: %v", err)
			continue
		}

		d.Alias = alias.String
		if d.Alias == "" {
			d.Alias = d.Name
		}
		d.Tag = tag.String
		d.IsFrozen = isFrozen == 1
		d.IsCloned = isCloned == 1

		if activeVersion.Valid {
			d.ActiveVersion = int(activeVersion.Int64)
		} else {
			d.ActiveVersion = 0
		}

		d.DatasetType = source.String
		if d.DatasetType == "" {
			d.DatasetType = "web"
		}

		hasVersions := versionsStr.String != ""
		if versionsStr.String != "" {
			for _, v := range strings.Split(versionsStr.String, ",") {
				v = strings.TrimSpace(v)
				if n, err := strconv.Atoi(v); err == nil {
					d.Versions = append(d.Versions, n)
				}
			}
		}
		if d.Versions == nil {
			d.Versions = []int{}
		}

		switch {
		case d.IsFrozen:
			d.Status = "frozen"
		case queueStatus.String != "done":
			d.Status = "processing"
		case !hasVersions:
			d.Status = "processing"
		default:
			d.Status = "active"
		}

		datasets = append(datasets, d)
	}

	if datasets == nil {
		datasets = []DatasetRow{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"datasets": datasets,
	})
}

func freezeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DatasetID int64 `json:"dataset_id"`
		Freeze    bool  `json:"freeze"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	val := 0
	if req.Freeze {
		val = 1
	}
	_, err := db.Get().Exec(`
		UPDATE datasets SET is_frozen = ? WHERE dataset_id = ?
	`, val, req.DatasetID)
	if err != nil {
		http.Error(w, "update failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
}

func rollbackHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DatasetID     int64 `json:"dataset_id"`
		VersionNumber int   `json:"version_number"`
		Freeze        bool  `json:"freeze"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	_, err := db.Get().Exec(`
		UPDATE datasets SET active_version = ? WHERE dataset_id = ?
	`, req.VersionNumber, req.DatasetID)
	if err != nil {
		http.Error(w, "rollback failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if req.Freeze {
		_, err = db.Get().Exec(`
			UPDATE datasets SET is_frozen = 1 WHERE dataset_id = ?
		`, req.DatasetID)
		if err != nil {
			http.Error(w, "freeze failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
}

// ---------------------------------------------------------------- result file helper --

type resultFile struct {
	Entities []json.RawMessage `json:"entities"`
	Posts    []json.RawMessage `json:"posts"`
	Total    int               `json:"total"`
}

func readResultFile(filePath string) (entityCount int, fileSize int64) {
	if filePath == "" {
		return 0, 0
	}
	b, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("[readResultFile] cannot read %s: %v", filePath, err)
		return 0, 0
	}
	fileSize = int64(len(b))
	var rf resultFile
	if err := json.Unmarshal(b, &rf); err != nil {
		log.Printf("[readResultFile] cannot parse %s: %v", filePath, err)
		return 0, fileSize
	}
	if rf.Total > 0 {
		return rf.Total, fileSize
	}
	if len(rf.Posts) > 0 {
		return len(rf.Posts), fileSize
	}
	return len(rf.Entities), fileSize
}

func datasetViewHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idStr := strings.TrimSpace(r.URL.Query().Get("dataset_id"))
	if idStr == "" {
		http.Error(w, "dataset_id is required", http.StatusBadRequest)
		return
	}
	datasetID, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "invalid dataset_id", http.StatusBadRequest)
		return
	}

	var d struct {
		DatasetID     int64          `json:"dataset_id"`
		Name          string         `json:"name"`
		Description   sql.NullString `json:"-"`
		Intent        string         `json:"intent"`
		Tag           sql.NullString `json:"-"`
		Visibility    string         `json:"visibility"`
		IsFrozen      int            `json:"-"`
		IsCloned      int            `json:"-"`
		Nightly       int            `json:"-"`
		ActiveVersion sql.NullInt64  `json:"-"`
		CreatedAt     time.Time      `json:"created_at"`
	}
	err = db.Get().QueryRow(`
		SELECT dataset_id, data_name, description, intent, tag, visibility,
		       is_frozen, is_cloned, nightly, active_version, created_at
		FROM datasets WHERE dataset_id = ?
	`, datasetID).Scan(
		&d.DatasetID, &d.Name, &d.Description, &d.Intent, &d.Tag,
		&d.Visibility, &d.IsFrozen, &d.IsCloned, &d.Nightly,
		&d.ActiveVersion, &d.CreatedAt,
	)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var fieldsJSON string
	var includeLinks, includeFiles, includeImages int
	err = db.Get().QueryRow(`
		SELECT fields, include_links, include_files, include_images
		FROM dataset_schema WHERE dataset_id = ?
	`, datasetID).Scan(&fieldsJSON, &includeLinks, &includeFiles, &includeImages)
	if err != nil {
		http.Error(w, "query schema: "+err.Error(), http.StatusInternalServerError)
		return
	}
	var schemaFields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(fieldsJSON), &schemaFields); err != nil {
		http.Error(w, "parse schema: "+err.Error(), http.StatusInternalServerError)
		return
	}

	vRows, err := db.Get().Query(`
		SELECT version_number, file_path, created_at, is_active
		FROM dataset_versions WHERE dataset_id = ?
		ORDER BY version_number ASC
	`, datasetID)
	if err != nil {
		http.Error(w, "query versions: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer vRows.Close()

	type VersionRow struct {
		VersionNumber int       `json:"version_number"`
		FilePath      string    `json:"file_path"`
		CreatedAt     time.Time `json:"created_at"`
		IsActive      bool      `json:"is_active"`
		EntityCount   int       `json:"entity_count"`
		FileSize      int64     `json:"file_size_bytes"`
	}
	var versions []VersionRow
	for vRows.Next() {
		var v VersionRow
		var isActive int
		if err := vRows.Scan(&v.VersionNumber, &v.FilePath, &v.CreatedAt, &isActive); err != nil {
			continue
		}
		v.IsActive = isActive == 1
		v.EntityCount, v.FileSize = readResultFile(v.FilePath)
		log.Printf("[view] version=%d file=%s entities=%d size=%d", v.VersionNumber, v.FilePath, v.EntityCount, v.FileSize)
		versions = append(versions, v)
	}
	if versions == nil {
		versions = []VersionRow{}
	}

	uRows, err := db.Get().Query(`
		SELECT
			du.dataset_url_id,
			du.url,
			du.source_type,
			du.folder_path,
			COALESCE(
				(SELECT q.status FROM queue q WHERE q.dataset_url_id = du.dataset_url_id ORDER BY q.queue_id DESC LIMIT 1),
				(SELECT rq.status FROM reddit_queue rq WHERE rq.dataset_url_id = du.dataset_url_id ORDER BY rq.reddit_queue_id DESC LIMIT 1),
				'pending'
			) AS queue_status
		FROM datasets_url du
		WHERE du.dataset_id = ?
		ORDER BY du.dataset_url_id ASC
	`, datasetID)
	if err != nil {
		http.Error(w, "query urls: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer uRows.Close()

	type URLRow struct {
		DatasetURLID int64          `json:"dataset_url_id"`
		URL          string         `json:"url"`
		SourceType   string         `json:"source_type"`
		QueueStatus  string         `json:"queue_status"`
		FolderPath   sql.NullString `json:"-"`
		FolderOut    *string        `json:"folder_path"`
	}
	var urls []URLRow
	for uRows.Next() {
		var u URLRow
		if err := uRows.Scan(&u.DatasetURLID, &u.URL, &u.SourceType, &u.FolderPath, &u.QueueStatus); err != nil {
			continue
		}
		if u.FolderPath.Valid {
			u.FolderOut = &u.FolderPath.String
		}
		urls = append(urls, u)
	}
	if urls == nil {
		urls = []URLRow{}
	}

	status := "active"
	if d.IsFrozen == 1 {
		status = "frozen"
	} else {
		for _, u := range urls {
			if u.QueueStatus != "done" && u.QueueStatus != "failed" {
				status = "processing"
				break
			}
		}
	}

	lastRefresh := d.CreatedAt
	if len(versions) > 0 {
		lastRefresh = versions[len(versions)-1].CreatedAt
	}

	activeVersion := int(d.ActiveVersion.Int64)
	entityCount := 0
	fileSizeBytes := int64(0)
	for _, v := range versions {
		if v.VersionNumber == activeVersion {
			entityCount = v.EntityCount
			fileSizeBytes = v.FileSize
			break
		}
	}

	type URLOut struct {
		DatasetURLID int64   `json:"dataset_url_id"`
		URL          string  `json:"url"`
		SourceType   string  `json:"source_type"`
		QueueStatus  string  `json:"queue_status"`
		FolderPath   *string `json:"folder_path"`
	}
	var urlsOut []URLOut
	for _, u := range urls {
		urlsOut = append(urlsOut, URLOut{
			DatasetURLID: u.DatasetURLID,
			URL:          u.URL,
			SourceType:   u.SourceType,
			QueueStatus:  u.QueueStatus,
			FolderPath:   u.FolderOut,
		})
	}
	if urlsOut == nil {
		urlsOut = []URLOut{}
	}

	resp := map[string]interface{}{
		"dataset_id":      d.DatasetID,
		"name":            d.Name,
		"description":     d.Description.String,
		"intent":          d.Intent,
		"tag":             d.Tag.String,
		"visibility":      d.Visibility,
		"is_frozen":       d.IsFrozen == 1,
		"is_cloned":       d.IsCloned == 1,
		"nightly":         d.Nightly == 1,
		"status":          status,
		"active_version":  activeVersion,
		"created_at":      d.CreatedAt,
		"last_refresh":    lastRefresh,
		"schema":          schemaFields,
		"include_links":   includeLinks == 1,
		"include_images":  includeImages == 1,
		"include_files":   includeFiles == 1,
		"versions":        versions,
		"urls":            urlsOut,
		"entity_count":    entityCount,
		"file_size_bytes": fileSizeBytes,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func datasetResultHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	datasetID, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("dataset_id")))
	if err != nil || datasetID < 1 {
		http.Error(w, "invalid dataset_id", http.StatusBadRequest)
		return
	}
	versionNum, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("version_id")))
	if err != nil || versionNum < 1 {
		http.Error(w, "invalid version_id", http.StatusBadRequest)
		return
	}

	var filePath string
	err = db.Get().QueryRow(`
		SELECT file_path FROM dataset_versions
		WHERE dataset_id = ? AND version_number = ?
	`, datasetID, versionNum).Scan(&filePath)
	if err == sql.ErrNoRows {
		http.Error(w, "version not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query version: "+err.Error(), http.StatusInternalServerError)
		return
	}

	b, err := os.ReadFile(filePath)
	if err != nil {
		http.Error(w, "read file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var rf resultFile
	if err := json.Unmarshal(b, &rf); err != nil {
		http.Error(w, "parse file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	entities := make([]map[string]interface{}, 0, len(rf.Entities))
	for _, raw := range rf.Entities {
		var m map[string]interface{}
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}
		entities = append(entities, m)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"entities": entities,
		"version":  versionNum,
		"total":    len(entities),
	})
}

func datasetDiffHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	datasetID, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("dataset_id")))
	if err != nil || datasetID < 1 {
		http.Error(w, "invalid dataset_id", http.StatusBadRequest)
		return
	}
	v1Num, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("v1")))
	if err != nil || v1Num < 1 {
		http.Error(w, "invalid v1", http.StatusBadRequest)
		return
	}
	v2Num, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("v2")))
	if err != nil || v2Num < 1 {
		http.Error(w, "invalid v2", http.StatusBadRequest)
		return
	}

	getPath := func(vNum int) (string, error) {
		var filePath string
		err := db.Get().QueryRow(`
			SELECT file_path FROM dataset_versions
			WHERE dataset_id = ? AND version_number = ?
		`, datasetID, vNum).Scan(&filePath)
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("version %d not found", vNum)
		}
		return filePath, err
	}

	path1, err := getPath(v1Num)
	if err != nil {
		http.Error(w, "v1: "+err.Error(), http.StatusNotFound)
		return
	}
	path2, err := getPath(v2Num)
	if err != nil {
		http.Error(w, "v2: "+err.Error(), http.StatusNotFound)
		return
	}

	readEntities := func(filePath string) ([]map[string]interface{}, error) {
		b, err := os.ReadFile(filePath)
		if err != nil {
			return nil, err
		}
		var rf resultFile
		if err := json.Unmarshal(b, &rf); err != nil {
			return nil, err
		}
		out := make([]map[string]interface{}, 0, len(rf.Entities))
		for _, raw := range rf.Entities {
			var m map[string]interface{}
			if err := json.Unmarshal(raw, &m); err != nil {
				continue
			}
			out = append(out, m)
		}
		return out, nil
	}

	entities1, err := readEntities(path1)
	if err != nil {
		http.Error(w, "read v1: "+err.Error(), http.StatusInternalServerError)
		return
	}
	entities2, err := readEntities(path2)
	if err != nil {
		http.Error(w, "read v2: "+err.Error(), http.StatusInternalServerError)
		return
	}

	hashEntity := func(e map[string]interface{}) string {
		keys := make([]string, 0, len(e))
		for k := range e {
			if k != "_source" {
				keys = append(keys, k)
			}
		}
		for i := 0; i < len(keys); i++ {
			for j := i + 1; j < len(keys); j++ {
				if keys[j] < keys[i] {
					keys[i], keys[j] = keys[j], keys[i]
				}
			}
		}
		h := sha256.New()
		for _, k := range keys {
			v, _ := json.Marshal(e[k])
			h.Write([]byte(k))
			h.Write(v)
		}
		return hex.EncodeToString(h.Sum(nil))
	}

	hashField := func(val interface{}) string {
		b, _ := json.Marshal(val)
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:])
	}

	fieldSimilarity := func(a, b map[string]interface{}) int {
		score := 0
		for k, av := range a {
			if k == "_source" || av == nil {
				continue
			}
			bv, ok := b[k]
			if !ok || bv == nil {
				continue
			}
			if hashField(av) == hashField(bv) {
				score++
			}
		}
		return score
	}

	changedFields := func(v1, v2 map[string]interface{}) []string {
		seen := make(map[string]bool)
		for k := range v1 {
			if k != "_source" {
				seen[k] = true
			}
		}
		for k := range v2 {
			if k != "_source" {
				seen[k] = true
			}
		}
		var changed []string
		for k := range seen {
			if hashField(v1[k]) != hashField(v2[k]) {
				changed = append(changed, k)
			}
		}
		for i := 0; i < len(changed); i++ {
			for j := i + 1; j < len(changed); j++ {
				if changed[j] < changed[i] {
					changed[i], changed[j] = changed[j], changed[i]
				}
			}
		}
		return changed
	}

	bySource1 := make(map[string][]map[string]interface{})
	for _, e := range entities1 {
		src, _ := e["_source"].(string)
		bySource1[src] = append(bySource1[src], e)
	}
	bySource2 := make(map[string][]map[string]interface{})
	for _, e := range entities2 {
		src, _ := e["_source"].(string)
		bySource2[src] = append(bySource2[src], e)
	}

	allSources := make(map[string]bool)
	for src := range bySource1 {
		allSources[src] = true
	}
	for src := range bySource2 {
		allSources[src] = true
	}

	type FieldDiff struct {
		Field string      `json:"field"`
		V1    interface{} `json:"v1"`
		V2    interface{} `json:"v2"`
	}

	type DiffRecord struct {
		ID         string                 `json:"id"`
		ChangeType string                 `json:"change_type"`
		Source     string                 `json:"source"`
		V1         map[string]interface{} `json:"v1"`
		V2         map[string]interface{} `json:"v2"`
		FieldDiffs []FieldDiff            `json:"field_diffs"`
	}

	var records []DiffRecord
	added, subtracted, modified := 0, 0, 0
	recordID := 0
	nextID := func() string {
		recordID++
		return fmt.Sprintf("rec_%04d", recordID)
	}

	for src := range allSources {
		group1 := bySource1[src]
		group2 := bySource2[src]

		if len(group1) > 0 && len(group2) == 0 {
			for _, e := range group1 {
				subtracted++
				records = append(records, DiffRecord{
					ID:         nextID(),
					ChangeType: "subtracted",
					Source:     src,
					V1:         e,
					V2:         nil,
				})
			}
			continue
		}

		if len(group1) == 0 && len(group2) > 0 {
			for _, e := range group2 {
				added++
				records = append(records, DiffRecord{
					ID:         nextID(),
					ChangeType: "added",
					Source:     src,
					V1:         nil,
					V2:         e,
				})
			}
			continue
		}

		hash1 := make(map[string]map[string]interface{}, len(group1))
		for _, e := range group1 {
			hash1[hashEntity(e)] = e
		}
		hash2 := make(map[string]map[string]interface{}, len(group2))
		for _, e := range group2 {
			hash2[hashEntity(e)] = e
		}

		usedHash1 := make(map[string]bool)
		usedHash2 := make(map[string]bool)
		for h := range hash1 {
			if _, ok := hash2[h]; ok {
				usedHash1[h] = true
				usedHash2[h] = true
			}
		}

		var orphans1 []map[string]interface{}
		for h, e := range hash1 {
			if !usedHash1[h] {
				orphans1 = append(orphans1, e)
			}
		}
		var orphans2 []map[string]interface{}
		for h, e := range hash2 {
			if !usedHash2[h] {
				orphans2 = append(orphans2, e)
			}
		}

		pairedO1 := make(map[int]int)
		pairedO2 := make(map[int]bool)

		for i, e1 := range orphans1 {
			bestScore := -1
			bestJ := -1
			for j, e2 := range orphans2 {
				if pairedO2[j] {
					continue
				}
				s := fieldSimilarity(e1, e2)
				if s > bestScore {
					bestScore = s
					bestJ = j
				}
			}
			if bestJ >= 0 {
				pairedO1[i] = bestJ
				pairedO2[bestJ] = true
			}
		}

		for i, e1 := range orphans1 {
			j, paired := pairedO1[i]
			if !paired {
				continue
			}
			e2 := orphans2[j]
			cf := changedFields(e1, e2)

			var fieldDiffs []FieldDiff
			for _, f := range cf {
				fieldDiffs = append(fieldDiffs, FieldDiff{
					Field: f,
					V1:    e1[f],
					V2:    e2[f],
				})
			}

			modified++
			records = append(records, DiffRecord{
				ID:         nextID(),
				ChangeType: "modified",
				Source:     src,
				V1:         e1,
				V2:         e2,
				FieldDiffs: fieldDiffs,
			})
		}

		for i, e1 := range orphans1 {
			if _, paired := pairedO1[i]; !paired {
				subtracted++
				records = append(records, DiffRecord{
					ID:         nextID(),
					ChangeType: "subtracted",
					Source:     src,
					V1:         e1,
					V2:         nil,
				})
			}
		}

		for j, e2 := range orphans2 {
			if !pairedO2[j] {
				added++
				records = append(records, DiffRecord{
					ID:         nextID(),
					ChangeType: "added",
					Source:     src,
					V1:         nil,
					V2:         e2,
				})
			}
		}
	}

	log.Printf("[diff] dataset_id=%d v1=%d v2=%d added=%d subtracted=%d modified=%d total_records=%d",
		datasetID, v1Num, v2Num, added, subtracted, modified, len(records))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"added":      added,
		"subtracted": subtracted,
		"modified":   modified,
		"total_v1":   len(entities1),
		"total_v2":   len(entities2),
		"records":    records,
	})
}

// ---------------------------------------------------------------- types --

type EditDatasetRequest struct {
	DatasetID   int64         `json:"dataset_id"`
	UserID      string        `json:"user_id"`
	Description string        `json:"description"`
	Tag         string        `json:"tag"`
	Visibility  string        `json:"visibility"`
	Nightly     string        `json:"nightly"`
	Schema      []SchemaInput `json:"schema"`
	URLs        []string      `json:"urls"`
}

func editDatasetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req EditDatasetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	var ve []string
	if req.DatasetID < 1 {
		ve = append(ve, "dataset_id is required")
	}
	if strings.TrimSpace(req.UserID) == "" {
		ve = append(ve, "user_id is required")
	}
	if strings.TrimSpace(req.Description) == "" {
		ve = append(ve, "description is required")
	}
	if req.Visibility != "public" && req.Visibility != "private" {
		ve = append(ve, "visibility must be 'public' or 'private'")
	}
	if req.Nightly != "yes" && req.Nightly != "no" {
		ve = append(ve, "nightly must be 'yes' or 'no'")
	}
	if len(req.Schema) == 0 {
		ve = append(ve, "at least one schema field is required")
	}
	for i, f := range req.Schema {
		if strings.TrimSpace(f.Type) == "" || strings.TrimSpace(f.Description) == "" {
			ve = append(ve, fmt.Sprintf("schema field %d must have a type and description", i+1))
		}
	}
	if len(ve) > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "validation failed", "fields": ve})
		return
	}

	var ownerID string
	err := db.Get().QueryRow(`
		SELECT user_id FROM datasets WHERE dataset_id = ?
	`, req.DatasetID).Scan(&ownerID)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if ownerID != req.UserID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	var existingFieldsJSON string
	err = db.Get().QueryRow(`
		SELECT fields FROM dataset_schema WHERE dataset_id = ?
	`, req.DatasetID).Scan(&existingFieldsJSON)
	if err != nil {
		http.Error(w, "load schema: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var existingFields map[string]map[string]string
	if err := json.Unmarshal([]byte(existingFieldsJSON), &existingFields); err != nil {
		http.Error(w, "parse existing schema: "+err.Error(), http.StatusInternalServerError)
		return
	}

	incomingFields := make(map[string]map[string]string, len(req.Schema))
	for _, f := range req.Schema {
		key := strings.TrimSpace(f.Type)
		incomingFields[key] = map[string]string{
			"type":        key,
			"description": strings.TrimSpace(f.Description),
		}
	}

	incomingJSON, _ := json.Marshal(incomingFields)
	log.Printf("[edit] existing schema: %s", existingFieldsJSON)
	log.Printf("[edit] incoming schema: %s", string(incomingJSON))

	schemaChanged := false
	if len(incomingFields) != len(existingFields) {
		schemaChanged = true
	} else {
		for key, incoming := range incomingFields {
			existing, ok := existingFields[key]
			if !ok {
				schemaChanged = true
				break
			}
			if existing["type"] != incoming["type"] || existing["description"] != incoming["description"] {
				schemaChanged = true
				break
			}
		}
	}
	log.Printf("[edit] dataset_id=%d schema_changed=%v", req.DatasetID, schemaChanged)

	tx, err := db.Get().Begin()
	if err != nil {
		http.Error(w, "begin tx: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	nightly := 0
	if req.Nightly == "yes" {
		nightly = 1
	}

	_, err = tx.Exec(`
		UPDATE datasets
		SET description = ?, tag = ?, visibility = ?, nightly = ?
		WHERE dataset_id = ?
	`, strings.TrimSpace(req.Description), strings.TrimSpace(req.Tag),
		req.Visibility, nightly, req.DatasetID)
	if err != nil {
		http.Error(w, "update dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if schemaChanged {
		newSchemaJSON, err := json.Marshal(incomingFields)
		if err != nil {
			http.Error(w, "marshal schema: "+err.Error(), http.StatusInternalServerError)
			return
		}

		_, err = tx.Exec(`
			UPDATE dataset_schema SET fields = ? WHERE dataset_id = ?
		`, string(newSchemaJSON), req.DatasetID)
		if err != nil {
			http.Error(w, "update schema: "+err.Error(), http.StatusInternalServerError)
			return
		}

		res, err := tx.Exec(`
			UPDATE queue q
			JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
			SET q.status = 'proceed-extract'
			WHERE du.dataset_id = ?
			  AND q.status = 'done'
			  AND du.folder_path IS NOT NULL
		`, req.DatasetID)
		if err != nil {
			http.Error(w, "requeue urls: "+err.Error(), http.StatusInternalServerError)
			return
		}
		affected, _ := res.RowsAffected()
		log.Printf("[edit] schema changed — requeued %d urls for re-extraction", affected)
	}

	newURLsAdded := 0
	for _, rawURL := range req.URLs {
		rawURL = strings.TrimSpace(rawURL)
		if rawURL == "" || !strings.HasPrefix(rawURL, "http") {
			continue
		}

		var exists int
		err := tx.QueryRow(`
			SELECT COUNT(*) FROM datasets_url
			WHERE dataset_id = ? AND url = ?
		`, req.DatasetID, rawURL).Scan(&exists)
		if err != nil {
			log.Printf("[edit] url exists check failed for %s: %v", rawURL, err)
			continue
		}
		if exists > 0 {
			log.Printf("[edit] skipping duplicate url: %s", rawURL)
			continue
		}

		res, err := tx.Exec(`
			INSERT INTO datasets_url (dataset_id, url, rendered_by, source_type)
			VALUES (?, ?, 'browserless', 'import')
		`, req.DatasetID, rawURL)
		if err != nil {
			log.Printf("[edit] insert url failed %s: %v", rawURL, err)
			continue
		}
		urlID, _ := res.LastInsertId()

		if isRedditURL(rawURL) {
			_, err = tx.Exec(`
				INSERT INTO reddit_queue (dataset_url_id, status) VALUES (?, 'pending')
			`, urlID)
		} else {
			_, err = tx.Exec(`
				INSERT INTO queue (dataset_url_id, status) VALUES (?, 'pending')
			`, urlID)
		}
		if err != nil {
			log.Printf("[edit] insert queue failed for url_id %d: %v", urlID, err)
			continue
		}
		newURLsAdded++
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "commit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[edit] done — dataset_id=%d new_urls=%d schema_changed=%v",
		req.DatasetID, newURLsAdded, schemaChanged)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":             true,
		"schema_changed": schemaChanged,
		"new_urls_added": newURLsAdded,
	})
}


func buildRedditURLs(subreddits []string, keywords []string, cap int, dateFrom string, dateTo string) []string {
	var urls []string

	// convert date strings to unix timestamps
	afterTS := ""
	beforeTS := ""
	layout := "2006-01-02"
	if dateFrom != "" {
		if t, err := time.Parse(layout, dateFrom); err == nil {
			afterTS = fmt.Sprintf("%d", t.Unix())
		}
	}
	if dateTo != "" {
		if t, err := time.Parse(layout, dateTo); err == nil {
			beforeTS = fmt.Sprintf("%d", t.Unix())
		}
	}

	// build date query params
	dateParams := ""
	if afterTS != "" {
		dateParams += fmt.Sprintf("&after=t3_%s", afterTS)
	}
	if beforeTS != "" {
		dateParams += fmt.Sprintf("&before=t3_%s", beforeTS)
	}

	query := ""
	if len(keywords) > 0 {
		var cleaned []string
		for _, k := range keywords {
			k = strings.TrimSpace(k)
			if k != "" {
				cleaned = append(cleaned, k)
			}
		}
		query = url.QueryEscape(strings.Join(cleaned, " "))
	}

	if len(subreddits) > 0 {
		for _, sub := range subreddits {
			sub = strings.TrimSpace(sub)
			if sub == "" {
				continue
			}
			if query != "" {
				urls = append(urls, fmt.Sprintf(
					"https://www.reddit.com/r/%s/search.json?q=%s&restrict_sr=1&sort=new&limit=%d%s",
					sub, query, cap, dateParams,
				))
			} else {
				// no keywords — /new/ doesn't support date filters natively
				// so we append date params and let the pipeline filter on its end
				urls = append(urls, fmt.Sprintf(
					"https://www.reddit.com/r/%s/new/.json?limit=%d&sort=new%s",
					sub, cap, dateParams,
				))
			}
		}
	} else if query != "" {
		urls = append(urls, fmt.Sprintf(
			"https://www.reddit.com/search.json?q=%s&sort=new&limit=%d%s",
			query, cap, dateParams,
		))
	}

	return urls
}


// ---------------------------------------------------------------- /queue/reddit --

type QueueRedditRequest struct {
	UserID      string   `json:"user_id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tag         string   `json:"tag"`
	Visibility  string   `json:"visibility"`
	Nightly     string   `json:"nightly"`
	Subreddits  []string `json:"subreddits"`
	Keywords    []string `json:"keywords"`
	URLs        string   `json:"urls"`
	DateFrom    string   `json:"date_from"`
	DateTo      string   `json:"date_to"`
	Cap         int      `json:"cap"`
}

func queueRedditHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req QueueRedditRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	var ve []string
	if strings.TrimSpace(req.UserID) == "" {
		ve = append(ve, "user_id is required")
	}
	if strings.TrimSpace(req.Name) == "" {
		ve = append(ve, "name is required")
	}
	if strings.TrimSpace(req.Description) == "" {
		ve = append(ve, "description is required")
	}
	if req.Visibility != "public" && req.Visibility != "private" {
		ve = append(ve, "visibility must be 'public' or 'private'")
	}
	if req.Nightly != "yes" && req.Nightly != "no" {
		ve = append(ve, "nightly must be 'yes' or 'no'")
	}

	hasURLs := strings.TrimSpace(req.URLs) != ""
	hasSubs := len(req.Subreddits) > 0
	hasKeywords := len(req.Keywords) > 0
	if !hasURLs && !hasSubs && !hasKeywords {
		ve = append(ve, "at least one source is required: urls, subreddits, or keywords")
	}

	if req.DateFrom == "" {
		ve = append(ve, "date_from is required")
	}
	if req.DateTo == "" {
		ve = append(ve, "date_to is required")
	}
	if req.Cap < 50 || req.Cap > 1000 {
		ve = append(ve, "cap must be between 50 and 1000")
	}
	if len(ve) > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":  "validation failed",
			"fields": ve,
		})
		return
	}

	var importURLs []string
	for _, line := range strings.Split(req.URLs, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && strings.HasPrefix(line, "http") {
			importURLs = append(importURLs, line)
		}
	}

	// build all reddit URLs (subreddits + keywords + date range)
	allRedditURLs := buildRedditURLs(req.Subreddits, req.Keywords, req.Cap, req.DateFrom, req.DateTo)

	nightly := 0
	if req.Nightly == "yes" {
		nightly = 1
	}

	tx, err := db.Get().Begin()
	if err != nil {
		http.Error(w, "begin tx: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	res, err := tx.Exec(`
		INSERT INTO datasets
			(user_id, data_name, alias, description, tag, visibility, nightly, is_cloned, date_from, date_to, cap)
		VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
	`, req.UserID, req.Name, req.Name,
		req.Description, req.Tag,
		req.Visibility, nightly,
		req.DateFrom, req.DateTo, req.Cap)
	if err != nil {
		http.Error(w, "insert dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	datasetID, _ := res.LastInsertId()
	log.Printf("[queue/reddit] dataset inserted — id: %d", datasetID)

	_, err = tx.Exec(`
		INSERT INTO dataset_schema (dataset_id, fields, source)
		VALUES (?, '{}', 'reddit')
	`, datasetID)
	if err != nil {
		http.Error(w, "insert schema: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// insert subreddits
	for _, sub := range req.Subreddits {
		sub = strings.TrimSpace(sub)
		if sub == "" {
			continue
		}
		if _, err := tx.Exec(`
			INSERT INTO dataset_subreddits (dataset_id, subreddit) VALUES (?, ?)
		`, datasetID, sub); err != nil {
			http.Error(w, "insert subreddit: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// insert keywords
	for _, kw := range req.Keywords {
		kw = strings.TrimSpace(kw)
		if kw == "" {
			continue
		}
		if _, err := tx.Exec(`
			INSERT INTO dataset_keywords (dataset_id, keyword) VALUES (?, ?)
		`, datasetID, kw); err != nil {
			http.Error(w, "insert keyword: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// queue all built reddit URLs
	for _, u := range allRedditURLs {
		res, err := tx.Exec(`
			INSERT INTO datasets_url (dataset_id, url, rendered_by, source_type)
			VALUES (?, ?, 'reddit-api', 'reddit')
		`, datasetID, u)
		if err != nil {
			http.Error(w, "insert reddit url: "+err.Error(), http.StatusInternalServerError)
			return
		}
		urlID, _ := res.LastInsertId()
		if _, err := tx.Exec(`
			INSERT INTO reddit_queue (dataset_url_id, status) VALUES (?, 'pending')
		`, urlID); err != nil {
			http.Error(w, "insert reddit_queue: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// queue import URLs
	for _, u := range importURLs {
		res, err := tx.Exec(`
			INSERT INTO datasets_url (dataset_id, url, rendered_by, source_type)
			VALUES (?, ?, 'reddit-api', 'reddit')
		`, datasetID, u)
		if err != nil {
			http.Error(w, "insert import url: "+err.Error(), http.StatusInternalServerError)
			return
		}
		urlID, _ := res.LastInsertId()
		if _, err := tx.Exec(`
			INSERT INTO reddit_queue (dataset_url_id, status) VALUES (?, 'pending')
		`, urlID); err != nil {
			http.Error(w, "insert reddit_queue import: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "commit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[queue/reddit] committed — dataset_id: %d subreddits: %d keywords: %d import_urls: %d total_urls: %d",
		datasetID, len(req.Subreddits), len(req.Keywords), len(importURLs), len(allRedditURLs)+len(importURLs))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "queued",
		"dataset":     req.Name,
		"dataset_id":  datasetID,
		"subreddits":  len(req.Subreddits),
		"keywords":    len(req.Keywords),
		"import_urls": len(importURLs),
		"total_urls":  len(allRedditURLs) + len(importURLs),
	})

	go func() {
		if err := reddit.RunFromQueue(reddit.QueuedRequest{
			DatasetID: datasetID,
			UserID:    req.UserID,
			DataName:  req.Name,
			Cap:       req.Cap,
		}); err != nil {
			log.Printf("[queue/reddit] pipeline error dataset_id=%d: %v", datasetID, err)
		}
	}()
}
// ---------------------------------------------------------------- /dataset/refresh --

func refreshHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DatasetID int64  `json:"dataset_id"`
		UserID    string `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.DatasetID < 1 || strings.TrimSpace(req.UserID) == "" {
		http.Error(w, "dataset_id and user_id are required", http.StatusBadRequest)
		return
	}

	var ownerID, dataName string
	var nightly, isFrozen int
	var cap sql.NullInt64
	err := db.Get().QueryRow(`
		SELECT user_id, data_name, nightly, is_frozen, cap
		FROM datasets WHERE dataset_id = ?
	`, req.DatasetID).Scan(&ownerID, &dataName, &nightly, &isFrozen, &cap)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if ownerID != req.UserID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if isFrozen == 1 {
		http.Error(w, "dataset is frozen", http.StatusBadRequest)
		return
	}

	var source string
	err = db.Get().QueryRow(`
		SELECT COALESCE(source, 'web') FROM dataset_schema WHERE dataset_id = ?
	`, req.DatasetID).Scan(&source)
	if err != nil {
		http.Error(w, "query schema: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if source == "reddit" {
		var subCount int
		db.Get().QueryRow(`
			SELECT COUNT(*) FROM dataset_subreddits WHERE dataset_id = ?
		`, req.DatasetID).Scan(&subCount)

		if subCount == 0 {
			http.Error(w, "no subreddits to refresh — dataset only has import URLs", http.StatusBadRequest)
			return
		}

		capVal := 200
		if cap.Valid {
			capVal = int(cap.Int64)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":     "refresh queued",
			"dataset_id": req.DatasetID,
			"type":       "reddit",
		})

		go func() {
			if _, err := reddit.Trigger(reddit.TriggerRequest{
				DatasetID: req.DatasetID,
				UserID:    req.UserID,
				DataName:  dataName,
				Cap:       capVal,
			}); err != nil {
				log.Printf("[refresh] reddit trigger error dataset_id=%d: %v", req.DatasetID, err)
			}
		}()

	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":     "refresh queued",
			"dataset_id": req.DatasetID,
			"type":       "web",
		})

		go func() {
			cfg := crawl.Config{BrowserlessKey: os.Getenv("BROWSERLESS_KEYS")}

			// mark all queue rows for this dataset as nightly-recrawl
			_, _ = db.Get().Exec(`
				UPDATE queue q
				JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
				SET q.crawl_type = 'nightly-recrawl'
				WHERE du.dataset_id = ?
			`, req.DatasetID)

			extractSchema, formatSchema, err := loadSchema(req.DatasetID)
			if err != nil {
				log.Printf("[refresh] load schema error dataset_id=%d: %v", req.DatasetID, err)
				return
			}
			if err := diff.Run(req.DatasetID, cfg, extractSchema, formatSchema); err != nil {
				log.Printf("[refresh] web diff error dataset_id=%d: %v", req.DatasetID, err)
			}
		}()
	}
}

// ---------------------------------------------------------------- /nightly --

func nightlyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Get().Query(`
		SELECT d.dataset_id, d.data_name, d.user_id,
			COALESCE(d.cap, 200) AS cap,
			COALESCE(ds.source, 'web') AS source
		FROM datasets d
		LEFT JOIN dataset_schema ds ON ds.dataset_id = d.dataset_id
		WHERE d.nightly = 1
		  AND d.is_frozen = 0
	`)
	if err != nil {
		http.Error(w, "query datasets: "+err.Error(), http.StatusInternalServerError)
		return
	}

	type nightlyRow struct {
		DatasetID int64
		DataName  string
		UserID    string
		Cap       int
		Source    string
	}

	var datasets []nightlyRow
	for rows.Next() {
		var d nightlyRow
		if err := rows.Scan(&d.DatasetID, &d.DataName, &d.UserID, &d.Cap, &d.Source); err != nil {
			log.Printf("[nightly] scan error: %v", err)
			continue
		}
		datasets = append(datasets, d)
	}
	rows.Close()

	log.Printf("[nightly] triggering %d datasets", len(datasets))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "nightly triggered",
		"datasets": len(datasets),
	})

	for _, d := range datasets {
		d := d
		if d.Source == "reddit" {
			var subCount int
			db.Get().QueryRow(`
				SELECT COUNT(*) FROM dataset_subreddits WHERE dataset_id = ?
			`, d.DatasetID).Scan(&subCount)

			if subCount == 0 {
				log.Printf("[nightly] skipping dataset_id=%d — no subreddits", d.DatasetID)
				continue
			}

			go func() {
				if _, err := reddit.Trigger(reddit.TriggerRequest{
					DatasetID: d.DatasetID,
					UserID:    d.UserID,
					DataName:  d.DataName,
					Cap:       d.Cap,
				}); err != nil {
					log.Printf("[nightly] reddit trigger error dataset_id=%d: %v", d.DatasetID, err)
				}
			}()

		} else {
    go func() {
        cfg := crawl.Config{BrowserlessKey: os.Getenv("BROWSERLESS_KEYS")}

        // mark all queue rows for this dataset as nightly-recrawl
        _, _ = db.Get().Exec(`
            UPDATE queue q
            JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
            SET q.crawl_type = 'nightly-recrawl'
            WHERE du.dataset_id = ?
        `, d.DatasetID)

        extractSchema, formatSchema, err := loadSchema(d.DatasetID)
        if err != nil {
            log.Printf("[nightly] load schema error dataset_id=%d: %v", d.DatasetID, err)
            return
        }
        if err := diff.Run(d.DatasetID, cfg, extractSchema, formatSchema); err != nil {
            log.Printf("[nightly] web diff error dataset_id=%d: %v", d.DatasetID, err)
        }
    }()
}
	}
}



func testRetryHandler(w http.ResponseWriter, r *http.Request) {
    cfg := crawl.Config{BrowserlessKey: os.Getenv("BROWSERLESS_KEYS")}
    if err := crawl.Retry(cfg, "test-api"); err != nil {
        log.Println("retry error:", err)
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    log.Println("retry complete")
    w.Write([]byte("done\n"))
}




func redditDatasetViewHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idStr := strings.TrimSpace(r.URL.Query().Get("dataset_id"))
	if idStr == "" {
		http.Error(w, "dataset_id is required", http.StatusBadRequest)
		return
	}
	datasetID, err := strconv.Atoi(idStr)
	if err != nil || datasetID < 1 {
		http.Error(w, "invalid dataset_id", http.StatusBadRequest)
		return
	}

	// ── Core dataset row ──
	var d struct {
		DatasetID     int64
		Name          string
		Alias         sql.NullString
		Description   sql.NullString
		Intent        sql.NullString
		Tag           sql.NullString
		Visibility    string
		IsFrozen      int
		IsCloned      int
		Nightly       int
		ActiveVersion sql.NullInt64
		CreatedAt     time.Time
		DateFrom      sql.NullString
		DateTo        sql.NullString
		Cap           sql.NullInt64
	}
	err = db.Get().QueryRow(`
		SELECT dataset_id, data_name, alias, description, intent, tag,
		       visibility, is_frozen, is_cloned, nightly, active_version,
		       created_at, date_from, date_to, cap
		FROM datasets WHERE dataset_id = ?
	`, datasetID).Scan(
		&d.DatasetID, &d.Name, &d.Alias, &d.Description, &d.Intent, &d.Tag,
		&d.Visibility, &d.IsFrozen, &d.IsCloned, &d.Nightly, &d.ActiveVersion,
		&d.CreatedAt, &d.DateFrom, &d.DateTo, &d.Cap,
	)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// ── Subreddits ──
	subRows, err := db.Get().Query(`
		SELECT subreddit FROM dataset_subreddits WHERE dataset_id = ? ORDER BY subreddit_id ASC
	`, datasetID)
	if err != nil {
		http.Error(w, "query subreddits: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer subRows.Close()
	var subreddits []string
	for subRows.Next() {
		var s string
		if err := subRows.Scan(&s); err == nil {
			subreddits = append(subreddits, s)
		}
	}
	if subreddits == nil {
		subreddits = []string{}
	}

	// ── Versions ──
	vRows, err := db.Get().Query(`
		SELECT version_number, file_path, created_at, is_active
		FROM dataset_versions WHERE dataset_id = ?
		ORDER BY version_number ASC
	`, datasetID)
	if err != nil {
		http.Error(w, "query versions: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer vRows.Close()

	type VersionOut struct {
		VersionNumber int       `json:"version_number"`
		FilePath      string    `json:"file_path"`
		CreatedAt     time.Time `json:"created_at"`
		IsActive      bool      `json:"is_active"`
		PostCount     int       `json:"post_count"`
		FileSizeBytes int64     `json:"file_size_bytes"`
	}
	var versions []VersionOut
	for vRows.Next() {
		var v VersionOut
		var isActive int
		if err := vRows.Scan(&v.VersionNumber, &v.FilePath, &v.CreatedAt, &isActive); err != nil {
			continue
		}
		v.IsActive = isActive == 1
		v.PostCount, v.FileSizeBytes = readResultFile(v.FilePath)
		versions = append(versions, v)
	}
	if versions == nil {
		versions = []VersionOut{}
	}

	// ── URLs + queue status ──
	uRows, err := db.Get().Query(`
		SELECT
			du.dataset_url_id,
			du.url,
			du.source_type,
			du.folder_path,
			COALESCE(
				(SELECT rq.status FROM reddit_queue rq
				 WHERE rq.dataset_url_id = du.dataset_url_id
				 ORDER BY rq.reddit_queue_id DESC LIMIT 1),
				'pending'
			) AS queue_status
		FROM datasets_url du
		WHERE du.dataset_id = ?
		ORDER BY du.dataset_url_id ASC
	`, datasetID)
	if err != nil {
		http.Error(w, "query urls: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer uRows.Close()

	type URLOut struct {
		DatasetURLID int64   `json:"dataset_url_id"`
		URL          string  `json:"url"`
		SourceType   string  `json:"source_type"`
		QueueStatus  string  `json:"queue_status"`
		FolderPath   *string `json:"folder_path"`
	}
	var urls []URLOut
	for uRows.Next() {
		var u URLOut
		var folderPath sql.NullString
		if err := uRows.Scan(&u.DatasetURLID, &u.URL, &u.SourceType, &folderPath, &u.QueueStatus); err != nil {
			continue
		}
		if folderPath.Valid {
			u.FolderPath = &folderPath.String
		}
		urls = append(urls, u)
	}
	if urls == nil {
		urls = []URLOut{}
	}

	// ── Derive status ──
	status := "active"
	if d.IsFrozen == 1 {
		status = "frozen"
	} else {
		for _, u := range urls {
			if u.QueueStatus != "done" && u.QueueStatus != "failed" {
				status = "processing"
				break
			}
		}
		if status == "active" && len(versions) == 0 {
			status = "processing"
		}
	}

	// ── Last refresh ──
	lastRefresh := d.CreatedAt
	if len(versions) > 0 {
		lastRefresh = versions[len(versions)-1].CreatedAt
	}

	// ── Active version stats ──
	activeVersion := int(d.ActiveVersion.Int64)
	postCount := 0
	fileSizeBytes := int64(0)
	for _, v := range versions {
		if v.VersionNumber == activeVersion {
			postCount = v.PostCount
			fileSizeBytes = v.FileSizeBytes
			break
		}
	}

	// ── Display name: alias falls back to data_name ──
	displayName := d.Name
	if d.Alias.Valid && strings.TrimSpace(d.Alias.String) != "" {
		displayName = d.Alias.String
	}

	cap := 200
	if d.Cap.Valid {
		cap = int(d.Cap.Int64)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"dataset_id":      d.DatasetID,
		"name":            displayName,
		"description":     d.Description.String,
		"intent":          d.Intent.String,
		"tag":             d.Tag.String,
		"visibility":      d.Visibility,
		"is_frozen":       d.IsFrozen == 1,
		"is_cloned":       d.IsCloned == 1,
		"nightly":         d.Nightly == 1,
		"status":          status,
		"active_version":  activeVersion,
		"created_at":      d.CreatedAt,
		"last_refresh":    lastRefresh,
		"date_from":       d.DateFrom.String,
		"date_to":         d.DateTo.String,
		"cap":             cap,
		"subreddits":      subreddits,
		"versions":        versions,
		"urls":            urls,
		"post_count":      postCount,
		"file_size_bytes": fileSizeBytes,
	})
}


func redditDatasetDiffHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	datasetID, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("dataset_id")))
	if err != nil || datasetID < 1 {
		http.Error(w, "invalid dataset_id", http.StatusBadRequest)
		return
	}
	v1Num, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("v1")))
	if err != nil || v1Num < 1 {
		http.Error(w, "invalid v1", http.StatusBadRequest)
		return
	}
	v2Num, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("v2")))
	if err != nil || v2Num < 1 {
		http.Error(w, "invalid v2", http.StatusBadRequest)
		return
	}

	getPath := func(vNum int) (string, error) {
		var filePath string
		err := db.Get().QueryRow(`
			SELECT file_path FROM dataset_versions
			WHERE dataset_id = ? AND version_number = ?
		`, datasetID, vNum).Scan(&filePath)
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("version %d not found", vNum)
		}
		return filePath, err
	}

	path1, err := getPath(v1Num)
	if err != nil {
		http.Error(w, "v1: "+err.Error(), http.StatusNotFound)
		return
	}
	path2, err := getPath(v2Num)
	if err != nil {
		http.Error(w, "v2: "+err.Error(), http.StatusNotFound)
		return
	}

	type redditResultFile struct {
		Posts []map[string]interface{} `json:"posts"`
		Total int                      `json:"total"`
	}

	readPosts := func(filePath string) ([]map[string]interface{}, error) {
		b, err := os.ReadFile(filePath)
		if err != nil {
			return nil, err
		}
		var rf redditResultFile
		if err := json.Unmarshal(b, &rf); err != nil {
			return nil, err
		}
		return rf.Posts, nil
	}

	posts1, err := readPosts(path1)
	if err != nil {
		http.Error(w, "read v1: "+err.Error(), http.StatusInternalServerError)
		return
	}
	posts2, err := readPosts(path2)
	if err != nil {
		http.Error(w, "read v2: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Index by URL (Reddit post identity key)
	hashField := func(val interface{}) string {
		b, _ := json.Marshal(val)
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:])
	}

	hashPost := func(p map[string]interface{}) string {
		// Hash stable fields only (exclude comments, fetched_at, score since those change)
		stableKeys := []string{"url", "title", "body", "author", "subreddit", "created_utc"}
		h := sha256.New()
		for _, k := range stableKeys {
			v, _ := json.Marshal(p[k])
			h.Write([]byte(k))
			h.Write(v)
		}
		return hex.EncodeToString(h.Sum(nil))
	}

	changedFields := func(v1, v2 map[string]interface{}) []string {
		seen := make(map[string]bool)
		for k := range v1 {
			seen[k] = true
		}
		for k := range v2 {
			seen[k] = true
		}
		var changed []string
		for k := range seen {
			if hashField(v1[k]) != hashField(v2[k]) {
				changed = append(changed, k)
			}
		}
		// sort
		for i := 0; i < len(changed); i++ {
			for j := i + 1; j < len(changed); j++ {
				if changed[j] < changed[i] {
					changed[i], changed[j] = changed[j], changed[i]
				}
			}
		}
		return changed
	}

	// Build URL-keyed maps
	byURL1 := make(map[string]map[string]interface{}, len(posts1))
	for _, p := range posts1 {
		if u, ok := p["url"].(string); ok && u != "" {
			byURL1[u] = p
		}
	}
	byURL2 := make(map[string]map[string]interface{}, len(posts2))
	for _, p := range posts2 {
		if u, ok := p["url"].(string); ok && u != "" {
			byURL2[u] = p
		}
	}

	type FieldDiff struct {
		Field string      `json:"field"`
		V1    interface{} `json:"v1"`
		V2    interface{} `json:"v2"`
	}

	type DiffRecord struct {
		ID         string                 `json:"id"`
		ChangeType string                 `json:"change_type"`
		Source     string                 `json:"source"`
		V1         map[string]interface{} `json:"v1"`
		V2         map[string]interface{} `json:"v2"`
		FieldDiffs []FieldDiff            `json:"field_diffs"`
	}

	var records []DiffRecord
	added, subtracted, modified := 0, 0, 0

	// Posts in v1
	for url, p1 := range byURL1 {
		p2, exists := byURL2[url]
		if !exists {
			subtracted++
			records = append(records, DiffRecord{
				ID:         url,
				ChangeType: "subtracted",
				Source:     url,
				V1:         p1,
				V2:         nil,
			})
			continue
		}
		// Both exist — check if anything changed
		if hashPost(p1) == hashPost(p2) {
			// stable fields unchanged — still check mutable fields like score/comments
			cf := changedFields(p1, p2)
			if len(cf) == 0 {
				continue
			}
		}
		cf := changedFields(p1, p2)
		if len(cf) == 0 {
			continue
		}
		var fieldDiffs []FieldDiff
		for _, f := range cf {
			fieldDiffs = append(fieldDiffs, FieldDiff{
				Field: f,
				V1:    p1[f],
				V2:    p2[f],
			})
		}
		modified++
		records = append(records, DiffRecord{
			ID:         url,
			ChangeType: "modified",
			Source:     url,
			V1:         p1,
			V2:         p2,
			FieldDiffs: fieldDiffs,
		})
	}

	// Posts only in v2 (added)
	for url, p2 := range byURL2 {
		if _, exists := byURL1[url]; !exists {
			added++
			records = append(records, DiffRecord{
				ID:         url,
				ChangeType: "added",
				Source:     url,
				V1:         nil,
				V2:         p2,
			})
		}
	}

	// Sort records: subtracted first, then added, then modified
	order := map[string]int{"subtracted": 0, "added": 1, "modified": 2}
	for i := 0; i < len(records); i++ {
		for j := i + 1; j < len(records); j++ {
			if order[records[j].ChangeType] < order[records[i].ChangeType] {
				records[i], records[j] = records[j], records[i]
			}
		}
	}

	log.Printf("[reddit-diff] dataset_id=%d v1=%d v2=%d added=%d subtracted=%d modified=%d",
		datasetID, v1Num, v2Num, added, subtracted, modified)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"added":      added,
		"subtracted": subtracted,
		"modified":   modified,
		"total_v1":   len(posts1),
		"total_v2":   len(posts2),
		"records":    records,
	})
}

func redditDatasetResultHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	datasetID, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("dataset_id")))
	if err != nil || datasetID < 1 {
		http.Error(w, "invalid dataset_id", http.StatusBadRequest)
		return
	}
	versionNum, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("version_id")))
	if err != nil || versionNum < 1 {
		http.Error(w, "invalid version_id", http.StatusBadRequest)
		return
	}

	var filePath string
	err = db.Get().QueryRow(`
		SELECT file_path FROM dataset_versions
		WHERE dataset_id = ? AND version_number = ?
	`, datasetID, versionNum).Scan(&filePath)
	if err == sql.ErrNoRows {
		http.Error(w, "version not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query version: "+err.Error(), http.StatusInternalServerError)
		return
	}

	b, err := os.ReadFile(filePath)
	if err != nil {
		http.Error(w, "read file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var rf struct {
		Posts []json.RawMessage `json:"posts"`
		Total int               `json:"total"`
	}
	if err := json.Unmarshal(b, &rf); err != nil {
		http.Error(w, "parse file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	posts := make([]map[string]interface{}, 0, len(rf.Posts))
	for _, raw := range rf.Posts {
		var m map[string]interface{}
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}
		posts = append(posts, m)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"posts":   posts,
		"version": versionNum,
		"total":   len(posts),
	})
}



func redditEditDatasetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DatasetID     int64    `json:"dataset_id"`
		UserID        string   `json:"user_id"`
		Alias         string   `json:"alias"`
		Description   string   `json:"description"`
		Tag           string   `json:"tag"`
		Visibility    string   `json:"visibility"`
		Nightly       string   `json:"nightly"`
		DateFrom      string   `json:"date_from"`
		DateTo        string   `json:"date_to"`
		Cap           int      `json:"cap"`
		NewSubreddits []string `json:"new_subreddits"`
		NewKeywords   []string `json:"new_keywords"`
		NewURLs       []string `json:"new_urls"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	// ── Validation ────────────────────────────────────────────────────────────

	var ve []string
	if req.DatasetID < 1 {
		ve = append(ve, "dataset_id is required")
	}
	if strings.TrimSpace(req.UserID) == "" {
		ve = append(ve, "user_id is required")
	}
	if strings.TrimSpace(req.Alias) == "" {
		ve = append(ve, "alias is required")
	}
	if strings.TrimSpace(req.Description) == "" {
		ve = append(ve, "description is required")
	}
	if req.Visibility != "public" && req.Visibility != "private" {
		ve = append(ve, "visibility must be 'public' or 'private'")
	}
	if req.Nightly != "yes" && req.Nightly != "no" {
		ve = append(ve, "nightly must be 'yes' or 'no'")
	}
	if strings.TrimSpace(req.DateFrom) == "" {
		ve = append(ve, "date_from is required")
	}
	if strings.TrimSpace(req.DateTo) == "" {
		ve = append(ve, "date_to is required")
	}
	if req.Cap < 50 || req.Cap > 1000 {
		ve = append(ve, "cap must be between 50 and 1000")
	}
	if len(ve) > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":  "validation failed",
			"fields": ve,
		})
		return
	}

	// ── Ownership check ───────────────────────────────────────────────────────

	var ownerID string
	err := db.Get().QueryRow(`
		SELECT user_id FROM datasets WHERE dataset_id = ?
	`, req.DatasetID).Scan(&ownerID)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if ownerID != req.UserID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	// ── Fetch existing subreddits and keywords for dedup ──────────────────────

	existingSubs := make(map[string]bool)
	subRows, err := db.Get().Query(`
		SELECT subreddit FROM dataset_subreddits WHERE dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		http.Error(w, "query subreddits: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer subRows.Close()
	for subRows.Next() {
		var s string
		if err := subRows.Scan(&s); err == nil {
			existingSubs[strings.ToLower(strings.TrimSpace(s))] = true
		}
	}

	existingKws := make(map[string]bool)
	kwRows, err := db.Get().Query(`
		SELECT keyword FROM dataset_keywords WHERE dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		http.Error(w, "query keywords: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer kwRows.Close()
	for kwRows.Next() {
		var k string
		if err := kwRows.Scan(&k); err == nil {
			existingKws[strings.ToLower(strings.TrimSpace(k))] = true
		}
	}

	// ── Transaction ───────────────────────────────────────────────────────────

	nightly := 0
	if req.Nightly == "yes" {
		nightly = 1
	}

	tx, err := db.Get().Begin()
	if err != nil {
		http.Error(w, "begin tx: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	// Update core dataset fields + alias
	_, err = tx.Exec(`
		UPDATE datasets
		SET alias       = ?,
		    description = ?,
		    tag         = ?,
		    visibility  = ?,
		    nightly     = ?,
		    date_from   = ?,
		    date_to     = ?,
		    cap         = ?
		WHERE dataset_id = ?
	`,
		strings.TrimSpace(req.Alias),
		strings.TrimSpace(req.Description),
		strings.TrimSpace(req.Tag),
		req.Visibility,
		nightly,
		req.DateFrom,
		req.DateTo,
		req.Cap,
		req.DatasetID,
	)
	if err != nil {
		http.Error(w, "update dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Insert new subreddits (skip duplicates)
	newSubsAdded := 0
	for _, sub := range req.NewSubreddits {
		sub = strings.TrimSpace(sub)
		if sub == "" {
			continue
		}
		if existingSubs[strings.ToLower(sub)] {
			log.Printf("[reddit-edit] skipping duplicate subreddit: %s", sub)
			continue
		}
		if _, err := tx.Exec(`
			INSERT INTO dataset_subreddits (dataset_id, subreddit) VALUES (?, ?)
		`, req.DatasetID, sub); err != nil {
			http.Error(w, "insert subreddit: "+err.Error(), http.StatusInternalServerError)
			return
		}
		newSubsAdded++
	}

	// Insert new keywords (skip duplicates)
	newKwsAdded := 0
	for _, kw := range req.NewKeywords {
		kw = strings.TrimSpace(kw)
		if kw == "" {
			continue
		}
		if existingKws[strings.ToLower(kw)] {
			log.Printf("[reddit-edit] skipping duplicate keyword: %s", kw)
			continue
		}
		if _, err := tx.Exec(`
			INSERT INTO dataset_keywords (dataset_id, keyword) VALUES (?, ?)
		`, req.DatasetID, kw); err != nil {
			http.Error(w, "insert keyword: "+err.Error(), http.StatusInternalServerError)
			return
		}
		newKwsAdded++
	}

	// Insert new URLs (skip duplicates)
	newURLsAdded := 0
	for _, rawURL := range req.NewURLs {
		rawURL = strings.TrimSpace(rawURL)
		if rawURL == "" || !strings.HasPrefix(rawURL, "http") {
			continue
		}

		var exists int
		err := tx.QueryRow(`
			SELECT COUNT(*) FROM datasets_url
			WHERE dataset_id = ? AND url = ?
		`, req.DatasetID, rawURL).Scan(&exists)
		if err != nil {
			log.Printf("[reddit-edit] url exists check failed for %s: %v", rawURL, err)
			continue
		}
		if exists > 0 {
			log.Printf("[reddit-edit] skipping duplicate url: %s", rawURL)
			continue
		}

		res, err := tx.Exec(`
			INSERT INTO datasets_url (dataset_id, url, rendered_by, source_type)
			VALUES (?, ?, 'reddit-api', 'reddit')
		`, req.DatasetID, rawURL)
		if err != nil {
			log.Printf("[reddit-edit] insert url failed %s: %v", rawURL, err)
			continue
		}
		urlID, _ := res.LastInsertId()

		if _, err := tx.Exec(`
			INSERT INTO reddit_queue (dataset_url_id, status) VALUES (?, 'pending')
		`, urlID); err != nil {
			log.Printf("[reddit-edit] insert reddit_queue failed for url_id %d: %v", urlID, err)
			continue
		}
		newURLsAdded++
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "commit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[reddit-edit] done — dataset_id=%d alias=%q new_subs=%d new_kws=%d new_urls=%d",
		req.DatasetID, req.Alias, newSubsAdded, newKwsAdded, newURLsAdded)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":             true,
		"new_subs_added": newSubsAdded,
		"new_kws_added":  newKwsAdded,
		"new_urls_added": newURLsAdded,
	})
}

func deleteDatasetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DatasetID int64  `json:"dataset_id"`
		UserID    string `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.DatasetID < 1 {
		http.Error(w, "dataset_id is required", http.StatusBadRequest)
		return
	}

	var ownerID string
	err := db.Get().QueryRow(`
		SELECT user_id FROM datasets WHERE dataset_id = ?
	`, req.DatasetID).Scan(&ownerID)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if req.UserID != "" && ownerID != req.UserID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	_, err = db.Get().Exec(`
		DELETE FROM datasets WHERE dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		http.Error(w, "delete failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[delete] dataset_id=%d deleted", req.DatasetID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
}