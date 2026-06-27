package main

import (
  "bytes"
  "bufio"
	"crypto/sha256"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/var-raphael/vexaro-engine/ai"
	"github.com/var-raphael/vexaro-engine/crawl"
	"github.com/var-raphael/vexaro-engine/db"
	"github.com/var-raphael/vexaro-engine/diff"
	"github.com/var-raphael/vexaro-engine/download"
	"github.com/joho/godotenv"
	"github.com/var-raphael/vexaro-engine/heal"
	"github.com/var-raphael/vexaro-engine/notification"
	"github.com/var-raphael/vexaro-engine/serp"
	"github.com/var-raphael/vexaro-engine/storage"
	"github.com/var-raphael/vexaro-engine/worker"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("[env] no .env file found — relying on system env")
	}

	storage.Init()
	
	if err := initAuth(); err != nil {
        log.Fatal("failed to init auth: ", err)
  }

	if err := crawl.LoadCrawlProxies("proxies.txt"); err != nil {
		log.Println("[main] no crawl proxy file found — running direct")
	}


	defer db.Close()
	go worker.Start()
	heal.BillingSweepFn = func() {
    RunBillingExpirySweep()
    RunDatasetLimitSweep()
  }
  go heal.Start()
	notification.Init(db.Get())

  http.HandleFunc("/test/crawl", corsMiddleware(testCrawlHandler))
	http.HandleFunc("/dataset/alternate/delete", corsMiddleware(authMiddleware(webAlternateDeleteHandler)))
	http.HandleFunc("/dataset/alternate/result", corsMiddleware(webAlternateResultHandler))
	http.HandleFunc("/dataset/alternate/save", corsMiddleware(authMiddleware(webAlternateSaveHandler)))
	http.HandleFunc("/dataset/clone", corsMiddleware(authMiddleware(cloneDatasetHandler)))
	http.HandleFunc("/dataset/delete", corsMiddleware(authMiddleware(deleteDatasetHandler)))
	http.HandleFunc("/dataset/diff", corsMiddleware(datasetDiffHandler))
	http.HandleFunc("/dataset/download", corsMiddleware(download.Handler(db.Get())))
	http.HandleFunc("/dataset/edit", corsMiddleware(authMiddleware(editDatasetHandler)))
	http.HandleFunc("/dataset/freeze", corsMiddleware(authMiddleware(freezeHandler)))
	http.HandleFunc("/dataset/amazon/edit", corsMiddleware(authMiddleware(amazonEditDatasetHandler)))
	http.HandleFunc("/dataset/refresh", corsMiddleware(authMiddleware(refreshHandler)))
	http.HandleFunc("/dataset/result", corsMiddleware(datasetResultHandler))
	http.HandleFunc("/dataset/rollback", corsMiddleware(authMiddleware(rollbackHandler)))
	http.HandleFunc("/dataset/view", corsMiddleware(datasetViewHandler))
	http.HandleFunc("/dataset-market", corsMiddleware(datasetMarketHandler))
	http.HandleFunc("/datasets", corsMiddleware(authMiddleware(datasetsHandler)))
	http.HandleFunc("/nightly", corsMiddleware(nightlyHandler))
	http.HandleFunc("/ping/", PingHandler)
	http.HandleFunc("/queue", corsMiddleware(authMiddleware(queueHandler)))
	http.HandleFunc("/profile/", corsMiddleware(profileHandler))
	http.HandleFunc("/api/", apiCorsMiddleware(apiDatasetHandler))
	http.HandleFunc("/profile/update", corsMiddleware(authMiddleware(profileUpdateHandler)))
	http.HandleFunc("/notifications", corsMiddleware(authMiddleware(notificationsHandler)))
	http.HandleFunc("/notifications/read", corsMiddleware(authMiddleware(notificationsReadHandler)))
	http.HandleFunc("/auth/sync", corsMiddleware(authSyncHandler))
	http.HandleFunc("/amazon-new", corsMiddleware(authMiddleware(amazonQueueHandler)))
http.HandleFunc("/keepalive", keepAliveHandler)
	http.HandleFunc("/dataset/refresh/batch", corsMiddleware(batchRefreshHandler))
	http.HandleFunc("/webhook/register", corsMiddleware(authMiddleware(webhookRegisterHandler)))
	http.HandleFunc("/webhook/delete", corsMiddleware(authMiddleware(webhookDeleteHandler)))
	http.HandleFunc("/webhook/view", corsMiddleware(authMiddleware(webhookViewHandler)))
	http.HandleFunc("/dataset/regenerate/ping-key", corsMiddleware(authMiddleware(regeneratePingKeyHandler)))
	http.HandleFunc("/dataset/regenerate/private-key", corsMiddleware(authMiddleware(regeneratePrivateKeyHandler)))

	mcpServer := setupMCPServer()
	registerMCPRoutes(mcpServer)
	
	RegisterBillingRoutes()

	port := ":8080"
	log.Println("server running on", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal(err)
	}
}

// ---------------------------------------------------------------- cors --

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		origin := os.Getenv("ALLOWED_ORIGIN")
		if origin == "" {
			origin = "http://localhost:3000"
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func apiCorsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

// ---------------------------------------------------------------- helpers --

func keepAliveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var dummy int
	if err := db.Get().QueryRow(`SELECT 1`).Scan(&dummy); err != nil {
		log.Printf("[keepalive] db check failed: %v", err)
		http.Error(w, "db unreachable", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":   true,
		"pong": time.Now().UTC(),
	})
}

func generateKey() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		h := sha256.Sum256([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
		return hex.EncodeToString(h[:])
	}
	return hex.EncodeToString(b)
}

func parsePage(r *http.Request) (page, limit int) {
	page = 1
	limit = 100
	if p := strings.TrimSpace(r.URL.Query().Get("page")); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			page = n
		}
	}
	if l := strings.TrimSpace(r.URL.Query().Get("limit")); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			if n > 500 {
				n = 500
			}
			limit = n
		}
	}
	return
}

func parsePageAlt(r *http.Request) (page, limit int) {
	page = 1
	limit = 100
	if p := strings.TrimSpace(r.URL.Query().Get("page")); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			page = n
		}
	}
	if l := strings.TrimSpace(r.URL.Query().Get("limit")); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			if n > 10000 {
				n = 10000
			}
			limit = n
		}
	}
	return
}

func totalPages(total, limit int) int {
	if limit <= 0 {
		return 1
	}
	return (total + limit - 1) / limit
}

func streamEntities(filePath, arrayKey string, page, limit int) ([]json.RawMessage, int, error) {
	data, err := storage.Read(filePath)
	if err != nil {
		return nil, 0, fmt.Errorf("read: %w", err)
	}

	dec := json.NewDecoder(bytes.NewReader(data))

	if _, err := dec.Token(); err != nil {
		return nil, 0, fmt.Errorf("read open brace: %w", err)
	}

	offset := (page - 1) * limit
	var result []json.RawMessage
	total := 0

	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return nil, 0, fmt.Errorf("read key: %w", err)
		}
		key, ok := keyToken.(string)
		if !ok {
			var skip json.RawMessage
			dec.Decode(&skip)
			continue
		}

		if key != arrayKey {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, 0, fmt.Errorf("skip key %q: %w", key, err)
			}
			continue
		}

		if _, err := dec.Token(); err != nil {
			return nil, 0, fmt.Errorf("read array open: %w", err)
		}

		idx := 0
		for dec.More() {
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return nil, 0, fmt.Errorf("decode item %d: %w", idx, err)
			}
			total++
			if idx >= offset && idx < offset+limit {
				result = append(result, raw)
			}
			idx++
		}

		if _, err := dec.Token(); err != nil {
			return nil, 0, fmt.Errorf("read array close: %w", err)
		}

		break
	}

	if result == nil {
		result = []json.RawMessage{}
	}
	return result, total, nil
}

func isRedditURL(u string) bool {
	unwrapped := unwrapGoogleURL(u)
	return strings.Contains(unwrapped, "reddit.com") ||
		strings.Contains(unwrapped, "old.reddit.com") ||
		strings.Contains(unwrapped, "redd.it")
}

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
		DataType    string `json:"data_type"`
	}
	if err := json.Unmarshal([]byte(fieldsJSON), &rawFields); err != nil {
		return nil, nil, fmt.Errorf("parse schema: %w", err)
	}

	extractSchema := map[string]*ai.SchemaField{}
	formatSchema := map[string]json.RawMessage{}
	for key, f := range rawFields {
		extractSchema[key] = &ai.SchemaField{
			Type:        f.Type,
			Description: f.Description,
			DataType:    f.DataType,
		}
		formatSchema[key] = json.RawMessage(`"` + f.Type + `"`)
	}

	return extractSchema, formatSchema, nil
}

func readResultFile(filePath string) (entityCount int, fileSize int64) {
	if filePath == "" {
		return 0, 0
	}
	b, err := storage.Read(filePath)
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
func triggerRefresh(datasetID int64, dataName string, source string, userID string) error {
	res, err := db.Get().Exec(`
		UPDATE queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		SET q.status = 'pending-diff',
		    q.crawl_type = 'nightly-recrawl',
		    q.cluster_role = 'solo',
		    q.primary_dataset_url_id = NULL,
		    q.cluster_attempts = 0,
		    q.diff_cluster_role = 'solo',
		    q.diff_primary_dataset_url_id = NULL
		WHERE du.dataset_id = ?
		AND q.status IN ('done', 'failed')
		AND q.fail_count < 7
	`, datasetID)
	if err != nil {
		return fmt.Errorf("reset queue: %w", err)
	}
	affected, _ := res.RowsAffected()
	log.Printf("[triggerRefresh] reset %d urls to pending-diff dataset_id=%d", affected, datasetID)

	cfg := crawl.Config{BrowserlessKey: os.Getenv("BROWSERLESS_KEYS")}
	if err := diff.Run(datasetID, cfg); err != nil {
		return fmt.Errorf("diff run: %w", err)
	}
	log.Printf("[triggerRefresh] done — dataset_id=%d source=%s", datasetID, source)

	if userID != "" {
		notification.Notify(
			userID,
			&datasetID,
			"refresh_complete",
			fmt.Sprintf("Dataset \"%s\" has a new version ready.", dataName),
		)
	}

	return nil
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

// ---------------------------------------------------------------- types --

type resultFile struct {
	Entities []json.RawMessage `json:"entities"`
	Posts    []json.RawMessage `json:"posts"`
	Total    int               `json:"total"`
}

type EditDatasetRequest struct {
	DatasetID   int64         `json:"dataset_id"`
	Description string        `json:"description"`
	Tag         string        `json:"tag"`
	Visibility  string        `json:"visibility"`
	Nightly     string        `json:"nightly"`
	Schema      []SchemaInput `json:"schema"`
	URLs        []string      `json:"urls"`
	IsPremium   bool          `json:"is_premium"`
	Price       float64       `json:"price"`
}

type QueueRequest struct {
	Name          string        `json:"name"`
	Description   string        `json:"description"`
	Tag           string        `json:"tag"`
	Visibility    string        `json:"visibility"`
	Nightly       string        `json:"nightly"`
	Intent        string        `json:"intent"`
	ExtractIntent string        `json:"extractIntent"`
	Schema        []SchemaInput `json:"schema"`
	URLs          string        `json:"urls"`
	DryRun        bool          `json:"dry_run"`
	IncludeLinks  bool          `json:"includeLinks"`
}

type QueueRedditRequest struct {
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

type SchemaInput struct {
	ID          int    `json:"id"`
	Type        string `json:"type"`
	Description string `json:"description"`
	DataType    string `json:"data_type"`
}

type ClassifiedURLs struct {
	SERP   []string
	Import []string
	Reddit []string
}

// ---------------------------------------------------------------- handlers --

func cloneDatasetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		SourceDatasetID    int64  `json:"source_dataset_id"`
		Name               string `json:"name"`
		Description        string `json:"description"`
		ExtractDescription string `json:"extract_description"`
		Tag                string `json:"tag"`
		Nightly            string `json:"nightly"`
		Visibility         string `json:"visibility"`
		IsPremiumClone     bool   `json:"is_premium_clone"`
		PaymentRef         string `json:"payment_ref"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	var ve []string
	if req.SourceDatasetID < 1 {
		ve = append(ve, "source_dataset_id is required")
	}
	if strings.TrimSpace(req.Name) == "" {
		ve = append(ve, "name is required")
	}
	if req.Visibility != "public" && req.Visibility != "private" {
		ve = append(ve, "visibility must be 'public' or 'private'")
	}
	if req.Nightly != "yes" && req.Nightly != "no" {
		ve = append(ve, "nightly must be 'yes' or 'no'")
	}
	if req.IsPremiumClone && strings.TrimSpace(req.PaymentRef) == "" {
		ve = append(ve, "payment_ref is required for premium clone")
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

	plan := GetUserPlan(userID)
	if CountUserDatasets(userID) >= plan.Datasets {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": fmt.Sprintf("dataset limit reached (%d). Upgrade your plan to clone more.", plan.Datasets),
		})
		return
	}

	if req.IsPremiumClone {
		var existingCount int
		err := db.Get().QueryRow(`
			SELECT COUNT(*) FROM dataset_purchases
			WHERE user_id = ? AND dataset_id = ?
		`, userID, req.SourceDatasetID).Scan(&existingCount)
		if err != nil {
			http.Error(w, "purchase check failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if existingCount > 0 {
			http.Error(w, "already purchased", http.StatusConflict)
			return
		}

		var refCount int
		err = db.Get().QueryRow(`
			SELECT COUNT(*) FROM dataset_purchases WHERE order_id = ?
		`, req.PaymentRef).Scan(&refCount)
		if err != nil {
			http.Error(w, "ref check failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if refCount > 0 {
			http.Error(w, "payment reference already used", http.StatusConflict)
			return
		}
	}

	var src struct {
		DatasetID     int64
		Name          string
		Intent        sql.NullString
		ExtractIntent sql.NullString
		IsFrozen      int
		Visibility    string
		IsPremium     int
		Cap           int
		DateFrom      sql.NullString
		DateTo        sql.NullString
		IncludeLinks  int
	}
	err := db.Get().QueryRow(`
		SELECT dataset_id, data_name, intent, extract_intent, is_frozen, visibility, is_premium, cap, date_from, date_to, include_links
		FROM datasets WHERE dataset_id = ?
	`, req.SourceDatasetID).Scan(
		&src.DatasetID, &src.Name, &src.Intent, &src.ExtractIntent,
		&src.IsFrozen, &src.Visibility, &src.IsPremium,
		&src.Cap, &src.DateFrom, &src.DateTo, &src.IncludeLinks,
	)
	if err == sql.ErrNoRows {
		http.Error(w, "source dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query source dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if src.Visibility != "public" {
		http.Error(w, "cannot clone a private dataset", http.StatusForbidden)
		return
	}
	if src.IsPremium == 1 && !req.IsPremiumClone {
		http.Error(w, "this dataset requires payment to clone", http.StatusForbidden)
		return
	}

	var srcFilePath string
	var srcEntityCount int
	var srcAltFilePath sql.NullString
	var srcVersionNumber int
	err = db.Get().QueryRow(`
		SELECT version_number, file_path, COALESCE(entity_count, 0), alt_file_path
		FROM dataset_versions
		WHERE dataset_id = ?
		ORDER BY version_number DESC
		LIMIT 1
	`, req.SourceDatasetID).Scan(
		&srcVersionNumber, &srcFilePath, &srcEntityCount, &srcAltFilePath,
	)
	if err == sql.ErrNoRows {
		http.Error(w, "source dataset has no versions yet", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, "query source version: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var schemaFields string
	var schemaSource string
	err = db.Get().QueryRow(`
		SELECT fields, COALESCE(source, 'web')
		FROM dataset_schema WHERE dataset_id = ?
	`, req.SourceDatasetID).Scan(&schemaFields, &schemaSource)
	if err != nil {
		http.Error(w, "query source schema: "+err.Error(), http.StatusInternalServerError)
		return
	}

	uRows, err := db.Get().Query(`
		SELECT
			du.url, du.rendered_by, du.source_type, du.url_type,
			COALESCE(du.folder_path, '') AS folder_path,
			COALESCE(
				(SELECT q.status FROM queue q
				 WHERE q.dataset_url_id = du.dataset_url_id
				 ORDER BY q.queue_id DESC LIMIT 1),
				(SELECT rq.status FROM reddit_queue rq
				 WHERE rq.dataset_url_id = du.dataset_url_id
				 ORDER BY rq.reddit_queue_id DESC LIMIT 1),
				'done'
			) AS queue_status
		FROM datasets_url du
		WHERE du.dataset_id = ?
	`, req.SourceDatasetID)
	if err != nil {
		http.Error(w, "query source urls: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer uRows.Close()

	type urlRow struct {
		URL         string
		RenderedBy  string
		SourceType  string
		URLType     string
		FolderPath  string
		QueueStatus string
	}
	var urls []urlRow
	for uRows.Next() {
		var u urlRow
		if err := uRows.Scan(&u.URL, &u.RenderedBy, &u.SourceType, &u.URLType, &u.FolderPath, &u.QueueStatus); err != nil {
			continue
		}
		urls = append(urls, u)
	}
	uRows.Close()

	sRows, err := db.Get().Query(`
		SELECT subreddit FROM dataset_subreddits WHERE dataset_id = ?
	`, req.SourceDatasetID)
	if err != nil {
		http.Error(w, "query source subreddits: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer sRows.Close()
	var subreddits []string
	for sRows.Next() {
		var s string
		if err := sRows.Scan(&s); err == nil {
			subreddits = append(subreddits, s)
		}
	}

	kRows, err := db.Get().Query(`
		SELECT keyword FROM dataset_keywords WHERE dataset_id = ?
	`, req.SourceDatasetID)
	if err != nil {
		http.Error(w, "query source keywords: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer kRows.Close()
	var keywords []string
	for kRows.Next() {
		var k string
		if err := kRows.Scan(&k); err == nil {
			keywords = append(keywords, k)
		}
	}

	nightly := 0
	if req.Nightly == "yes" {
		nightly = 1
	}

	extractIntent := strings.TrimSpace(req.ExtractDescription)
	if extractIntent == "" {
		extractIntent = src.ExtractIntent.String
	}

	tx, err := db.Get().Begin()
	if err != nil {
		http.Error(w, "begin tx: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	pingKey := generateKey()
	privateKey := generateKey()

	res, err := tx.Exec(`
		INSERT INTO datasets
			(user_id, data_name, alias, description, intent, extract_intent, tag, visibility, nightly, is_cloned, cloned_from, active_version, is_premium, ping_key, private_key, cap, date_from, date_to, include_links)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 1, ?, ?, ?, ?, ?, ?, ?)
	`,
		userID, req.Name, req.Name,
		strings.TrimSpace(req.Description),
		src.Intent.String,
		extractIntent,
		strings.TrimSpace(req.Tag),
		req.Visibility, nightly,
		req.SourceDatasetID,
		req.IsPremiumClone,
		pingKey, privateKey,
		src.Cap,
		sql.NullString{String: src.DateFrom.String, Valid: src.DateFrom.Valid},
		sql.NullString{String: src.DateTo.String, Valid: src.DateTo.Valid},
		src.IncludeLinks,
	)
	if err != nil {
		http.Error(w, "insert dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	newDatasetID, err := res.LastInsertId()
	if err != nil {
		http.Error(w, "get new dataset id: "+err.Error(), http.StatusInternalServerError)
		return
	}

	newDir := fmt.Sprintf("data/%s-%d", req.Name, newDatasetID)
	newFilePath := fmt.Sprintf("%s/result-v1.json", newDir)
	newAltFilePath := ""

	var copiedKeys []string
	rollbackCopied := func() {
		for _, key := range copiedKeys {
			if err := storage.Delete(key); err != nil {
				log.Printf("[clone] rollback delete %s: %v", key, err)
			}
		}
	}

	srcData, err := storage.Read(srcFilePath)
	if err != nil {
		tx.Rollback()
		http.Error(w, "read source file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := storage.Write(newFilePath, srcData); err != nil {
		tx.Rollback()
		http.Error(w, "write clone file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	copiedKeys = append(copiedKeys, newFilePath)

	if srcAltFilePath.Valid && srcAltFilePath.String != "" {
		newAltFilePath = fmt.Sprintf("%s/alt-result-v1.json", newDir)
		altData, err := storage.Read(srcAltFilePath.String)
		if err != nil {
			rollbackCopied()
			tx.Rollback()
			http.Error(w, "read source alt file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if err := storage.Write(newAltFilePath, altData); err != nil {
			rollbackCopied()
			tx.Rollback()
			http.Error(w, "write clone alt file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		copiedKeys = append(copiedKeys, newAltFilePath)
	}

	patchResultFile := func(path string) {
		b, err := storage.Read(path)
		if err != nil {
			log.Printf("[clone] patch read error %s: %v", path, err)
			return
		}
		var m map[string]interface{}
		if err := json.Unmarshal(b, &m); err != nil {
			log.Printf("[clone] patch unmarshal error %s: %v", path, err)
			return
		}
		m["dataset_id"] = newDatasetID
		m["version"] = 1
		m["data_name"] = req.Name
		m["generated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
		patched, err := json.MarshalIndent(m, "", "  ")
		if err != nil {
			log.Printf("[clone] patch marshal error %s: %v", path, err)
			return
		}
		if err := storage.Write(path, patched); err != nil {
			log.Printf("[clone] patch write error %s: %v", path, err)
		}
	}

	patchResultFile(newFilePath)
	if newAltFilePath != "" {
		patchResultFile(newAltFilePath)
	}

	_, err = tx.Exec(`
		INSERT INTO dataset_schema (dataset_id, fields, source)
		VALUES (?, ?, ?)
	`, newDatasetID, schemaFields, schemaSource)
	if err != nil {
		rollbackCopied()
		http.Error(w, "insert schema: "+err.Error(), http.StatusInternalServerError)
		return
	}

	for _, u := range urls {
		var folderPathVal interface{}
		if u.FolderPath != "" {
			folderPathVal = u.FolderPath
		}
		urlRes, err := tx.Exec(`
			INSERT INTO datasets_url (dataset_id, url, rendered_by, source_type, url_type, folder_path)
			VALUES (?, ?, ?, ?, ?, ?)
		`, newDatasetID, u.URL, u.RenderedBy, u.SourceType, u.URLType, folderPathVal)
		if err != nil {
			log.Printf("[clone] insert url failed %s: %v", u.URL, err)
			continue
		}
		newURLID, _ := urlRes.LastInsertId()
		if u.SourceType == "reddit" {
			_, err = tx.Exec(`
				INSERT INTO reddit_queue (dataset_url_id, status) VALUES (?, ?)
			`, newURLID, u.QueueStatus)
		} else {
			_, err = tx.Exec(`
				INSERT INTO queue (dataset_url_id, status, crawl_type) VALUES (?, ?, 'fresh')
			`, newURLID, u.QueueStatus)
		}
		if err != nil {
			log.Printf("[clone] insert queue failed url_id=%d: %v", newURLID, err)
		}
	}

	for _, sub := range subreddits {
		if _, err := tx.Exec(`
			INSERT INTO dataset_subreddits (dataset_id, subreddit) VALUES (?, ?)
		`, newDatasetID, sub); err != nil {
			log.Printf("[clone] insert subreddit failed %s: %v", sub, err)
		}
	}

	for _, kw := range keywords {
		if _, err := tx.Exec(`
			INSERT INTO dataset_keywords (dataset_id, keyword) VALUES (?, ?)
		`, newDatasetID, kw); err != nil {
			log.Printf("[clone] insert keyword failed %s: %v", kw, err)
		}
	}

	_, err = tx.Exec(`
		INSERT INTO dataset_versions (dataset_id, version_number, file_path, is_active, entity_count, alt_file_path)
		VALUES (?, 1, ?, 1, ?, ?)
	`, newDatasetID, newFilePath, srcEntityCount, sql.NullString{
		String: newAltFilePath,
		Valid:  newAltFilePath != "",
	})
	if err != nil {
		rollbackCopied()
		http.Error(w, "insert version: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		UPDATE datasets SET clone_count = clone_count + 1 WHERE dataset_id = ?
	`, req.SourceDatasetID)
	if err != nil {
		rollbackCopied()
		http.Error(w, "update clone_count: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		rollbackCopied()
		http.Error(w, "commit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if req.IsPremiumClone && req.PaymentRef != "" {
		_, err = db.Get().Exec(`
			INSERT INTO dataset_purchases (user_id, dataset_id, order_id)
			VALUES (?, ?, ?)
		`, userID, req.SourceDatasetID, req.PaymentRef)
		if err != nil {
			log.Printf("[clone] purchase record failed dataset_id=%d ref=%s: %v",
				newDatasetID, req.PaymentRef, err)
		} else {
			log.Printf("[clone] purchase recorded — user=%s source_dataset_id=%d ref=%s",
				userID, req.SourceDatasetID, req.PaymentRef)
		}
	}

	log.Printf("[clone] source_dataset_id=%d (v%d) cloned by user=%s new_dataset_id=%d name=%q premium=%v alt_copied=%v extract_intent_overridden=%v",
		req.SourceDatasetID, srcVersionNumber, userID, newDatasetID, req.Name, req.IsPremiumClone, newAltFilePath != "", req.ExtractDescription != "")

	notification.Notify(
		userID,
		&newDatasetID,
		"clone_created",
		fmt.Sprintf("Dataset \"%s\" has been cloned and is ready.", req.Name),
	)

	if src.Visibility == "public" {
		var sourceOwnerID string
		db.Get().QueryRow(`
			SELECT user_id FROM datasets WHERE dataset_id = ?
		`, req.SourceDatasetID).Scan(&sourceOwnerID)
		if sourceOwnerID != "" && sourceOwnerID != userID {
			sourceDatasetID := req.SourceDatasetID
			notification.Notify(
				sourceOwnerID,
				&sourceDatasetID,
				"dataset_cloned",
				fmt.Sprintf("Your dataset was cloned by another user."),
			)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":             true,
		"new_dataset_id": newDatasetID,
		"name":           req.Name,
		"alt_copied":     newAltFilePath != "",
	})
}

func buildFieldWeights(entities []map[string]interface{}) map[string]float64 {
	fieldValues := make(map[string]map[string]bool)

	for _, e := range entities {
		for k, v := range e {
			if k == "_source" {
				continue
			}
			if fieldValues[k] == nil {
				fieldValues[k] = make(map[string]bool)
			}
			b, _ := json.Marshal(v)
			fieldValues[k][string(b)] = true
		}
	}

	weights := make(map[string]float64)
	total := float64(len(entities))
	if total == 0 {
		return weights
	}
	for field, vals := range fieldValues {
		weights[field] = float64(len(vals)) / total
	}
	return weights
}

func fieldSimilarityWeighted(a, b map[string]interface{}, weights map[string]float64) float64 {
	score := 0.0
	for k, av := range a {
		if k == "_source" {
			continue
		}
		bv, ok := b[k]
		if !ok || bv == nil {
			continue
		}
		ab, _ := json.Marshal(av)
		bb, _ := json.Marshal(bv)
		if string(ab) == string(bb) {
			w := weights[k]
			if w < 0.1 {
				w = 0.1
			}
			score += w
		}
	}
	return score
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
		b, err := storage.Read(filePath)
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

	population := entities1
	if len(entities2) > len(entities1) {
		population = entities2
	}
	weights := buildFieldWeights(population)

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
					ID: nextID(), ChangeType: "subtracted",
					Source: src, V1: e, V2: nil,
				})
			}
			continue
		}
		if len(group1) == 0 && len(group2) > 0 {
			for _, e := range group2 {
				added++
				records = append(records, DiffRecord{
					ID: nextID(), ChangeType: "added",
					Source: src, V1: nil, V2: e,
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

		const minSimilarityThreshold = 0.3

		pairedO1 := make(map[int]int)
		pairedO2 := make(map[int]bool)

		for i, e1 := range orphans1 {
			bestScore := -1.0
			bestJ := -1
			for j, e2 := range orphans2 {
				if pairedO2[j] {
					continue
				}
				s := fieldSimilarityWeighted(e1, e2, weights)
				if s > bestScore {
					bestScore = s
					bestJ = j
				}
			}
			if bestJ >= 0 && bestScore >= minSimilarityThreshold {
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
					Field: f, V1: e1[f], V2: e2[f],
				})
			}

			modified++
			records = append(records, DiffRecord{
				ID: nextID(), ChangeType: "modified",
				Source: src, V1: e1, V2: e2,
				FieldDiffs: fieldDiffs,
			})
		}

		for i, e1 := range orphans1 {
			if _, paired := pairedO1[i]; !paired {
				subtracted++
				records = append(records, DiffRecord{
					ID: nextID(), ChangeType: "subtracted",
					Source: src, V1: e1, V2: nil,
				})
			}
		}

		for j, e2 := range orphans2 {
			if !pairedO2[j] {
				added++
				records = append(records, DiffRecord{
					ID: nextID(), ChangeType: "added",
					Source: src, V1: nil, V2: e2,
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

func datasetMarketHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Get().Query(`
		SELECT
    d.dataset_id,
    d.data_name,
    COALESCE(d.alias, d.data_name) AS alias,
    COALESCE(d.description, '') AS description,
    COALESCE(d.extract_intent, '') AS extract_intent,
    COALESCE(d.tag, '') AS tag,
    d.is_premium,
    d.price,
    d.clone_count,
    d.api_hit_count,
    d.active_version,
    d.created_at,
    CASE
        WHEN d.dataset_type = 'amazon' THEN 'amazon'
        WHEN EXISTS (
            SELECT 1 FROM dataset_subreddits dsr WHERE dsr.dataset_id = d.dataset_id
        ) THEN 'reddit'
        WHEN ds.source = 'reddit' THEN 'reddit'
        ELSE 'web'
    END AS dataset_type,
    CASE
        WHEN EXISTS (
            SELECT 1 FROM dataset_versions dv
            WHERE dv.dataset_id = d.dataset_id
            AND dv.version_number = d.active_version
            AND dv.alt_file_path IS NOT NULL
            AND dv.alt_file_path != ''
        ) THEN 1
        ELSE 0
    END AS has_alt,
    COALESCE(
        (SELECT dv.entity_count FROM dataset_versions dv
         WHERE dv.dataset_id = d.dataset_id
         AND dv.version_number = d.active_version
         LIMIT 1), 0
    ) AS entity_count
FROM datasets d
LEFT JOIN dataset_schema ds ON ds.dataset_id = d.dataset_id
WHERE d.visibility = 'public'
  AND d.active_version IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM dataset_versions dv2
      WHERE dv2.dataset_id = d.dataset_id
  )
ORDER BY (
    (d.clone_count * 3) +
    (d.api_hit_count * 1) +
    (DATEDIFF(NOW(), d.created_at) * -0.5)
) DESC
	`)
	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type MarketDataset struct {
		DatasetID     int64     `json:"dataset_id"`
		Name          string    `json:"name"`
		Description   string    `json:"description"`
		ExtractIntent string    `json:"extract_intent"`
		Tag           string    `json:"tag"`
		IsPremium     bool      `json:"is_premium"`
		Price         float64   `json:"price"`
		CloneCount    int       `json:"clone_count"`
		ApiHitCount   int       `json:"api_hit_count"`
		ActiveVersion int       `json:"active_version"`
		CreatedAt     time.Time `json:"created_at"`
		DatasetType   string    `json:"dataset_type"`
		HasAlt        bool      `json:"has_alt"`
		EntityCount   int       `json:"entity_count"`
	}

	var datasets []MarketDataset

	for rows.Next() {
		var d MarketDataset
		var isPremium, hasAlt int
		var alias string

		if err := rows.Scan(
			&d.DatasetID,
			&d.Name,
			&alias,
			&d.Description,
			&d.ExtractIntent,
			&d.Tag,
			&isPremium,
			&d.Price,
			&d.CloneCount,
			&d.ApiHitCount,
			&d.ActiveVersion,
			&d.CreatedAt,
			&d.DatasetType,
			&hasAlt,
			&d.EntityCount,
		); err != nil {
			log.Printf("[market] scan error: %v", err)
			continue
		}

		if alias != "" {
			d.Name = alias
		}

		d.IsPremium = isPremium == 1
		d.HasAlt = hasAlt == 1
		datasets = append(datasets, d)
	}

	if datasets == nil {
		datasets = []MarketDataset{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"datasets": datasets,
	})
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

	page, limit := parsePage(r)

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

	entities, total, err := streamEntities(filePath, "entities", page, limit)
	if err != nil {
		http.Error(w, "read file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"entities":    entities,
		"version":     versionNum,
		"total":       total,
		"page":        page,
		"limit":       limit,
		"total_pages": totalPages(total, limit),
	})
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
		UserID        string         `json:"user_id"`
		Name          string         `json:"name"`
		Description   sql.NullString `json:"-"`
		Intent        sql.NullString `json:"-"`
		ExtractIntent sql.NullString `json:"-"`
		Tag           sql.NullString `json:"-"`
		Visibility    string         `json:"visibility"`
		IsFrozen      int            `json:"-"`
		IsCloned      int            `json:"-"`
		Nightly       int            `json:"-"`
		ActiveVersion sql.NullInt64  `json:"-"`
		CreatedAt     time.Time      `json:"created_at"`
		DatasetType   string         `json:"-"`
	}
	err = db.Get().QueryRow(`
		SELECT dataset_id, user_id, data_name, description, intent, extract_intent, tag, visibility,
		       is_frozen, is_cloned, nightly, active_version, created_at, dataset_type
		FROM datasets WHERE dataset_id = ?
	`, datasetID).Scan(
		&d.DatasetID, &d.UserID, &d.Name, &d.Description, &d.Intent, &d.ExtractIntent, &d.Tag,
		&d.Visibility, &d.IsFrozen, &d.IsCloned, &d.Nightly,
		&d.ActiveVersion, &d.CreatedAt, &d.DatasetType,
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
	err = db.Get().QueryRow(`
		SELECT fields FROM dataset_schema WHERE dataset_id = ?
	`, datasetID).Scan(&fieldsJSON)
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
		SELECT version_number, file_path, COALESCE(alt_file_path, ''), created_at, is_active, entity_count
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
		AltFilePath   *string   `json:"alt_file_path"`
		CreatedAt     time.Time `json:"created_at"`
		IsActive      bool      `json:"is_active"`
		EntityCount   int       `json:"entity_count"`
		FileSizeBytes int64     `json:"file_size_bytes"`
	}

	var versions []VersionRow
	for vRows.Next() {
		var v VersionRow
		var isActive int
		var altFilePath string
		if err := vRows.Scan(&v.VersionNumber, &v.FilePath, &altFilePath, &v.CreatedAt, &isActive, &v.EntityCount); err != nil {
			continue
		}
		v.IsActive = isActive == 1
		if altFilePath != "" {
			v.AltFilePath = &altFilePath
		}
		if sz, err := storage.Size(v.FilePath); err == nil {
			v.FileSizeBytes = sz
		}
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
	for _, v := range versions {
		if v.VersionNumber == activeVersion {
			entityCount = v.EntityCount
			break
		}
	}

	fileSizeBytes := int64(0)
	for _, v := range versions {
		if v.VersionNumber == activeVersion {
			fileSizeBytes = v.FileSizeBytes
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
		"dataset_id":     d.DatasetID,
		"user_id":        d.UserID,
		"name":           d.Name,
		"description":    d.Description.String,
		"intent":         d.Intent.String,
		"extract_intent": d.ExtractIntent.String,
		"tag":            d.Tag.String,
		"visibility":     d.Visibility,
		"is_frozen":      d.IsFrozen == 1,
		"is_cloned":      d.IsCloned == 1,
		"nightly":        d.Nightly == 1,
		"status":         status,
		"active_version": activeVersion,
		"created_at":     d.CreatedAt,
		"last_refresh":   lastRefresh,
		"schema":         schemaFields,
		"versions":       versions,
		"urls":           urlsOut,
		"entity_count":   entityCount,
		"file_size_bytes": fileSizeBytes,
		"dataset_type":   d.DatasetType,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func datasetsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
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
			d.is_premium,
			d.is_cloned,
			d.nightly,
			d.created_at,
			d.active_version,
			d.ping_key,
			d.private_key,
			d.clone_count,
			d.api_hit_count,
			COALESCE(dv.created_at, d.created_at) AS last_refresh,
			COALESCE(dv.entity_count, 0) AS entity_count,
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
    WHEN d.dataset_type = 'amazon' THEN 'amazon'
    WHEN ds.source = 'reddit' THEN 'reddit'
    WHEN EXISTS (
        SELECT 1 FROM dataset_subreddits dsr WHERE dsr.dataset_id = d.dataset_id
    ) THEN 'reddit'
    ELSE 'web'
    END AS source,
			CASE
				WHEN EXISTS (
					SELECT 1 FROM dataset_versions dv3
					WHERE dv3.dataset_id = d.dataset_id
					AND dv3.version_number = d.active_version
					AND dv3.alt_file_path IS NOT NULL
					AND dv3.alt_file_path != ''
				) THEN 1
				ELSE 0
			END AS has_alt
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
		IsPremium     bool      `json:"is_premium"`
		Nightly       bool      `json:"nightly"`
		CreatedAt     time.Time `json:"created_at"`
		PingKey       string    `json:"ping_key"`
		PrivateKey    string    `json:"private_key"`
		CloneCount    int       `json:"clone_count"`
		ApiHitCount   int       `json:"api_hit_count"`
		EntityCount   int       `json:"entity_count"`
		ActiveVersion int       `json:"version"`
		LastRefresh   time.Time `json:"last_refresh"`
		Status        string    `json:"status"`
		Versions      []int     `json:"versions"`
		DatasetType   string    `json:"dataset_type"`
		HasAlt        bool      `json:"has_alt"`
	}

	var datasets []DatasetRow

	for rows.Next() {
		var d DatasetRow
		var alias, tag, versionsStr, queueStatus, source sql.NullString
		var pingKey, privateKey sql.NullString
		var activeVersion sql.NullInt64
		var isFrozen, isCloned, hasAlt, isPremium, nightly int
		var cloneCount, apiHitCount, entityCount int

		if err := rows.Scan(
			&d.DatasetID,
			&d.Name,
			&alias,
			&tag,
			&d.Visibility,
			&isFrozen,
			&isPremium,
			&isCloned,
			&nightly,
			&d.CreatedAt,
			&activeVersion,
			&pingKey,
			&privateKey,
			&cloneCount,
			&apiHitCount,
			&d.LastRefresh,
			&entityCount,
			&queueStatus,
			&versionsStr,
			&source,
			&hasAlt,
		); err != nil {
			log.Printf("[datasets] scan error: %v", err)
			continue
		}

		d.HasAlt = hasAlt == 1
		d.IsFrozen = isFrozen == 1
		d.IsCloned = isCloned == 1
		d.IsPremium = isPremium == 1
		d.Nightly = nightly == 1
		d.CloneCount = cloneCount
		d.ApiHitCount = apiHitCount
		d.EntityCount = entityCount

		d.PingKey = pingKey.String
		if d.Visibility == "private" {
			d.PrivateKey = privateKey.String
		}

		d.Alias = alias.String
		if d.Alias == "" {
			d.Alias = d.Name
		}
		d.Tag = tag.String

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

func deleteDatasetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		DatasetID int64 `json:"dataset_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.DatasetID < 1 {
		http.Error(w, "dataset_id is required", http.StatusBadRequest)
		return
	}

	var ownerID, dataName string
	err := db.Get().QueryRow(`
		SELECT user_id, data_name FROM datasets WHERE dataset_id = ?
	`, req.DatasetID).Scan(&ownerID, &dataName)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if ownerID != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	folderRows, err := db.Get().Query(`
		SELECT DISTINCT folder_path FROM datasets_url
		WHERE dataset_id = ? AND folder_path IS NOT NULL AND folder_path != ''
	`, req.DatasetID)
	if err != nil {
		log.Printf("[delete] query folder_paths dataset_id=%d: %v", req.DatasetID, err)
	}
	var folderPaths []string
	if folderRows != nil {
		for folderRows.Next() {
			var fp string
			if folderRows.Scan(&fp) == nil && strings.TrimSpace(fp) != "" {
				folderPaths = append(folderPaths, fp)
			}
		}
		folderRows.Close()
	}

	versionRows, err := db.Get().Query(`
		SELECT file_path, COALESCE(alt_file_path, '') FROM dataset_versions
		WHERE dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		log.Printf("[delete] query version_paths dataset_id=%d: %v", req.DatasetID, err)
	}
	var versionPaths []string
	if versionRows != nil {
		for versionRows.Next() {
			var fp, altFp string
			if versionRows.Scan(&fp, &altFp) == nil {
				if strings.TrimSpace(fp) != "" {
					versionPaths = append(versionPaths, fp)
				}
				if strings.TrimSpace(altFp) != "" {
					versionPaths = append(versionPaths, altFp)
				}
			}
		}
		versionRows.Close()
	}

	tx, err := db.Get().Begin()
	if err != nil {
		http.Error(w, "begin tx: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		DELETE q FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE du.dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		http.Error(w, "delete queue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		DELETE rq FROM reddit_queue rq
		JOIN datasets_url du ON du.dataset_url_id = rq.dataset_url_id
		WHERE du.dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		http.Error(w, "delete reddit_queue: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		DELETE FROM datasets_url WHERE dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		http.Error(w, "delete datasets_url: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		DELETE FROM dataset_schema WHERE dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		http.Error(w, "delete dataset_schema: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		DELETE FROM dataset_versions WHERE dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		http.Error(w, "delete dataset_versions: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		DELETE FROM dataset_keywords WHERE dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		http.Error(w, "delete dataset_keywords: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		DELETE FROM dataset_subreddits WHERE dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		http.Error(w, "delete dataset_subreddits: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		DELETE FROM dataset_purchases WHERE dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		http.Error(w, "delete dataset_purchases: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		DELETE FROM datasets WHERE dataset_id = ?
	`, req.DatasetID)
	if err != nil {
		http.Error(w, "delete dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "commit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Delete all version files (including alt files) from storage
	for _, fp := range versionPaths {
		if err := storage.Delete(fp); err != nil {
			log.Printf("[delete] remove version file %s: %v", fp, err)
		}
	}

	// Delete folder-level files from storage
	for _, fp := range folderPaths {
		if err := storage.Delete(fp); err != nil {
			log.Printf("[delete] remove folder path %s: %v", fp, err)
		}
	}

	log.Printf("[delete] dataset_id=%d name=%q deleted by user=%s folders_cleaned=%d",
		req.DatasetID, dataName, userID, len(folderPaths))

	subject, html := notification.DatasetDeletedHTML(dataName)
	notification.NotifyEmail(
		ownerID,
		nil,
		"dataset_deleted",
		fmt.Sprintf("Dataset \"%s\" has been permanently deleted.", dataName),
		subject,
		html,
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
}

func editDatasetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		DatasetID          int64         `json:"dataset_id"`
		Description        string        `json:"description"`
		ExtractDescription string        `json:"extract_description"`
		Tag                string        `json:"tag"`
		Visibility         string        `json:"visibility"`
		Nightly            string        `json:"nightly"`
		Schema             []SchemaInput `json:"schema"`
		URLs               []string      `json:"urls"`
		URLsToDelete       []int64       `json:"urls_to_delete"`
		IsPremium          bool          `json:"is_premium"`
		Price              float64       `json:"price"`
		IncludeLinks       bool          `json:"include_links"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	var ve []string
	if req.DatasetID < 1 {
		ve = append(ve, "dataset_id is required")
	}
	if strings.TrimSpace(req.Description) == "" {
		ve = append(ve, "description is required")
	}
	extractDesc := strings.TrimSpace(req.ExtractDescription)
	if extractDesc == "" {
		ve = append(ve, "extract_description is required")
	} else if len([]rune(extractDesc)) < 20 {
		ve = append(ve, fmt.Sprintf("extract_description must be at least 20 characters (got %d)", len([]rune(extractDesc))))
	} else if len([]rune(extractDesc)) > 500 {
		ve = append(ve, fmt.Sprintf("extract_description must be at most 500 characters (got %d)", len([]rune(extractDesc))))
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
	var isFrozen int
	err := db.Get().QueryRow(`
		SELECT user_id, is_frozen FROM datasets WHERE dataset_id = ?
	`, req.DatasetID).Scan(&ownerID, &isFrozen)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if ownerID != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if isFrozen == 1 {
		http.Error(w, "dataset is frozen — unfreeze it before editing", http.StatusForbidden)
		return
	}

	// ── Processing check ──────────────────────────────────────────────────────
	var processingCount int
	err = db.Get().QueryRow(`
		SELECT COUNT(*) FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE du.dataset_id = ?
		AND q.status NOT IN ('done', 'failed')
	`, req.DatasetID).Scan(&processingCount)
	if err != nil {
		http.Error(w, "processing check failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if processingCount > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "dataset is currently processing — please wait until it completes before editing",
		})
		return
	}

	if len(req.URLsToDelete) > 0 {
		for _, urlID := range req.URLsToDelete {
			var count int
			err := db.Get().QueryRow(`
				SELECT COUNT(*) FROM datasets_url
				WHERE dataset_url_id = ? AND dataset_id = ?
			`, urlID, req.DatasetID).Scan(&count)
			if err != nil {
				http.Error(w, "validate url ownership: "+err.Error(), http.StatusInternalServerError)
				return
			}
			if count == 0 {
				http.Error(w, fmt.Sprintf("url_id %d does not belong to this dataset", urlID), http.StatusForbidden)
				return
			}
		}
	}

	// ── Plan URL limit check ──────────────────────────────────────────────────
	if len(req.URLs) > 0 {
		plan := GetUserPlan(userID)
		currentURLCount := CountDatasetURLs(req.DatasetID)
		remaining := plan.URLs - currentURLCount
		if remaining <= 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": fmt.Sprintf("URL limit reached (%d). Upgrade your plan to add more.", plan.URLs),
			})
			return
		}
		if len(req.URLs) > remaining {
			req.URLs = req.URLs[:remaining]
		}
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
			"data_type":   strings.TrimSpace(f.DataType),
		}
	}

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
			if existing["type"] != incoming["type"] ||
				existing["description"] != incoming["description"] ||
				existing["data_type"] != incoming["data_type"] {
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
		SET description = ?, extract_intent = ?, tag = ?, visibility = ?, nightly = ?, is_premium = ?, price = ?, include_links = ?
		WHERE dataset_id = ?
	`,
		strings.TrimSpace(req.Description),
		extractDesc,
		strings.TrimSpace(req.Tag),
		req.Visibility,
		nightly,
		req.IsPremium,
		req.Price,
		req.IncludeLinks,
		req.DatasetID,
	)
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

	urlsDeleted := 0
	for _, urlID := range req.URLsToDelete {
		var folderPath sql.NullString
		err := tx.QueryRow(`
			SELECT folder_path FROM datasets_url
			WHERE dataset_url_id = ? AND dataset_id = ?
		`, urlID, req.DatasetID).Scan(&folderPath)
		if err != nil {
			log.Printf("[edit] fetch folder_path for url_id=%d: %v", urlID, err)
		}

		_, err = tx.Exec(`DELETE FROM queue WHERE dataset_url_id = ?`, urlID)
		if err != nil {
			log.Printf("[edit] delete queue for url_id=%d: %v", urlID, err)
		}

		_, err = tx.Exec(`DELETE FROM reddit_queue WHERE dataset_url_id = ?`, urlID)
		if err != nil {
			log.Printf("[edit] delete reddit_queue for url_id=%d: %v", urlID, err)
		}

		res, err := tx.Exec(`
			DELETE FROM datasets_url WHERE dataset_url_id = ? AND dataset_id = ?
		`, urlID, req.DatasetID)
		if err != nil {
			log.Printf("[edit] delete url_id=%d: %v", urlID, err)
			continue
		}
		n, _ := res.RowsAffected()
		urlsDeleted += int(n)

		if folderPath.Valid && strings.TrimSpace(folderPath.String) != "" {
			if err := db.Get().QueryRow(`SELECT 1`).Scan(new(int)); err == nil {
				log.Printf("[edit] folder cleanup skipped for url_id=%d — handled by storage layer", urlID)
			}
		}
	}
	log.Printf("[edit] deleted %d urls for dataset_id=%d", urlsDeleted, req.DatasetID)

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

	log.Printf("[edit] done — dataset_id=%d new_urls=%d urls_deleted=%d schema_changed=%v",
		req.DatasetID, newURLsAdded, urlsDeleted, schemaChanged)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":             true,
		"schema_changed": schemaChanged,
		"new_urls_added": newURLsAdded,
		"urls_deleted":   urlsDeleted,
	})
}

 func freezeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
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
	if ownerID != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	if !req.Freeze {
		plan := GetUserPlan(userID)
		var activeCount int
		db.Get().QueryRow(`
			SELECT COUNT(*) FROM datasets WHERE user_id = ? AND is_frozen = 0
		`, userID).Scan(&activeCount)
		if activeCount >= plan.Datasets {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": fmt.Sprintf("cannot unfreeze — you're at your %s plan limit of %d active datasets. Upgrade to unlock more.", plan.Name, plan.Datasets),
			})
			return
		}
	}

	val := 0
	if req.Freeze {
		val = 1
	}
	_, err = db.Get().Exec(`
		UPDATE datasets SET is_frozen = ? WHERE dataset_id = ?
	`, val, req.DatasetID)
	if err != nil {
		http.Error(w, "update failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
}

func nightlyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Get().Query(`
		SELECT d.dataset_id, d.data_name, d.user_id, COALESCE(ds.source, 'web')
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
		Source    string
	}

	var datasets []nightlyRow
	for rows.Next() {
		var d nightlyRow
		if err := rows.Scan(&d.DatasetID, &d.DataName, &d.UserID, &d.Source); err != nil {
			log.Printf("[nightly] scan error: %v", err)
			continue
		}
		datasets = append(datasets, d)
	}
	rows.Close()

	log.Printf("[nightly] triggering %d datasets", len(datasets))

	if err := diff.BuildDiffClusterMap(); err != nil {
		log.Printf("[nightly] build diff cluster map error: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "nightly triggered",
		"datasets": len(datasets),
	})

	for _, d := range datasets {
		d := d
		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					log.Printf("[nightly] panic dataset_id=%d: %v", d.DatasetID, rec)
				}
			}()
			log.Printf("[nightly] starting dataset_id=%d", d.DatasetID)
			if err := triggerRefresh(d.DatasetID, d.DataName, d.Source, d.UserID); err != nil {
				log.Printf("[nightly] error dataset_id=%d: %v", d.DatasetID, err)
			} else {
				log.Printf("[nightly] done dataset_id=%d", d.DatasetID)
			}
		}()
	}
}

func PingHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := strings.TrimPrefix(r.URL.Path, "/ping/")
	key = strings.TrimSpace(key)
	if key == "" {
		http.Error(w, "ping key is required", http.StatusBadRequest)
		return
	}

	var datasetID int64
	var dataName, source, ownerID string
	var isFrozen int
	err := db.Get().QueryRow(`
		SELECT d.dataset_id, d.data_name, d.user_id, d.is_frozen, COALESCE(ds.source, 'web')
		FROM datasets d
		LEFT JOIN dataset_schema ds ON ds.dataset_id = d.dataset_id
		WHERE d.ping_key = ?
	`, key).Scan(&datasetID, &dataName, &ownerID, &isFrozen, &source)
	if err == sql.ErrNoRows {
		http.Error(w, "invalid ping key", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// ── Plan check ────────────────────────────────────────────────────────────
	plan := GetUserPlan(ownerID)
	if !plan.PingURL {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "ping URL is not available on this dataset's owner plan. Upgrade to use this feature.",
		})
		return
	}

	if isFrozen == 1 {
		http.Error(w, "dataset is frozen", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":           true,
		"dataset_id":   datasetID,
		"dataset_name": dataName,
		"status":       "refresh queued",
	})

	go func() {
		if err := triggerRefresh(datasetID, dataName, source, ""); err != nil {
			log.Printf("[ping] error dataset_id=%d: %v", datasetID, err)
		}
	}()
}

func queueHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req QueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	serpIntent := strings.TrimSpace(req.Intent)
	extractIntent := strings.TrimSpace(req.ExtractIntent)
	hasURLs := strings.TrimSpace(req.URLs) != ""

	var validationErrors []string
	if strings.TrimSpace(req.Name) == "" {
		validationErrors = append(validationErrors, "name is required")
	}
	if strings.TrimSpace(req.Description) == "" {
		validationErrors = append(validationErrors, "description is required")
	}
	if serpIntent != "" && len(serpIntent) < 20 {
		validationErrors = append(validationErrors, fmt.Sprintf("serp intent must be at least 20 characters if provided (got %d)", len(serpIntent)))
	}
	if len(extractIntent) < 20 {
		validationErrors = append(validationErrors, fmt.Sprintf("extract intent must be at least 20 characters (got %d)", len(extractIntent)))
	}
	if serpIntent == "" && !hasURLs {
		validationErrors = append(validationErrors, "provide a SERP intent, import URLs, or both")
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

	// ── Plan checks ───────────────────────────────────────────────────────────
	plan := GetUserPlan(userID)
	if CountUserDatasets(userID) >= plan.Datasets {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": fmt.Sprintf("dataset limit reached (%d). Upgrade your plan to create more.", plan.Datasets),
		})
		return
	}

	// ── SSE setup ─────────────────────────────────────────────────────────────
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok2 := w.(http.Flusher)
	if !ok2 {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	send := func(event, data string) {
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
		flusher.Flush()
	}

	sendProgress := func(step, detail string) {
		b, _ := json.Marshal(map[string]string{"step": step, "detail": detail})
		send("progress", string(b))
	}

	sendError := func(userMsg string, internalErr error) {
		if internalErr != nil {
			log.Printf("[queue] error user=%s name=%q: %v", userID, req.Name, internalErr)
		}
		b, _ := json.Marshal(map[string]string{"message": userMsg})
		send("error", string(b))
	}

	ctx := r.Context()

	// ── SERP ──────────────────────────────────────────────────────────────────
	var allURLs []string
	var serpEntries []serp.URLEntry

	if serpIntent != "" {
		sendProgress("serp", "Searching for URLs...")
		serpResult, err := serp.Fetch(serp.SERPRequest{
			UserID:   userID,
			DataName: req.Name,
			Intent:   serpIntent,
			Limit:    60,
		}, func(step, detail string) {
			if ctx.Err() != nil {
				return
			}
			sendProgress(step, detail)
		})
		if err != nil {
			log.Printf("[queue] serp error user=%s name=%q: %v", userID, req.Name, err)
			if !hasURLs {
				sendError("Failed to find URLs for your intent. Try rephrasing it.", err)
				return
			}
			sendProgress("serp_warn", "Search failed — proceeding with imported URLs only")
		} else {
			allURLs = append(allURLs, serpResult.URLs...)
			serpEntries = serpResult.Entries
			sendProgress("serp_done", fmt.Sprintf("Found %d URLs from search", len(serpResult.URLs)))
		}
	}

	if ctx.Err() != nil {
		log.Printf("[queue] client disconnected before import — user=%s", userID)
		return
	}

	// ── Import URLs ───────────────────────────────────────────────────────────
	var importURLs []string
	for _, line := range strings.Split(req.URLs, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && strings.HasPrefix(line, "http") {
			importURLs = append(importURLs, line)
		}
	}
	allURLs = append(allURLs, importURLs...)

	if len(allURLs) == 0 {
		sendError("No URLs found to process. Try a different intent or add import URLs.", nil)
		return
	}

	// ── Dedup ─────────────────────────────────────────────────────────────────
	seen := make(map[string]bool)
	var dedupedURLs []string
	for _, u := range allURLs {
		normalized := strings.TrimRight(strings.ToLower(strings.TrimSpace(u)), "/")
		if normalized == "" || seen[normalized] {
			continue
		}
		seen[normalized] = true
		dedupedURLs = append(dedupedURLs, u)
	}

	serpURLType := make(map[string]string)
	for _, e := range serpEntries {
		serpURLType[e.URL] = e.URLType
	}

	serpSet := make(map[string]bool)
	for _, e := range serpEntries {
		serpSet[strings.TrimRight(strings.ToLower(strings.TrimSpace(e.URL)), "/")] = true
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

	// ── Plan limits ───────────────────────────────────────────────────────────
	if len(classified.SERP) > plan.SERP {
		classified.SERP = classified.SERP[:plan.SERP]
	}
	urlBudget := plan.URLs
	if len(classified.Import) > urlBudget {
		classified.Import = classified.Import[:urlBudget]
	}
	urlBudget -= len(classified.Import)
	if urlBudget < 0 {
		urlBudget = 0
	}
	if len(classified.Reddit) > urlBudget {
		classified.Reddit = classified.Reddit[:urlBudget]
	}

	sendProgress("classified", fmt.Sprintf(
		"Classified %d URLs — %d from search, %d imported, %d reddit",
		len(classified.SERP)+len(classified.Import)+len(classified.Reddit),
		len(classified.SERP), len(classified.Import), len(classified.Reddit),
	))

	if ctx.Err() != nil {
		log.Printf("[queue] client disconnected before db insert — user=%s", userID)
		return
	}

	// ── Dry run ───────────────────────────────────────────────────────────────
	if req.DryRun {
		b, _ := json.Marshal(map[string]interface{}{
			"dry_run":    true,
			"dataset_id": -1,
			"name":       req.Name,
		})
		send("done", string(b))
		log.Printf("[queue] dry-run — user=%s name=%q serp=%d import=%d reddit=%d",
			userID, req.Name, len(classified.SERP), len(classified.Import), len(classified.Reddit))
		return
	}

	// ── DB insert ─────────────────────────────────────────────────────────────
	sendProgress("saving", "Saving dataset...")

	pingKey := generateKey()
	privateKey := generateKey()

	schemaFields := make(map[string]map[string]string)
	for _, f := range req.Schema {
		schemaFields[f.Type] = map[string]string{
			"type":        f.Type,
			"description": f.Description,
			"data_type":   f.DataType,
		}
	}
	schemaJSON, err := json.Marshal(schemaFields)
	if err != nil {
		sendError("Failed to save your dataset. Please try again.", err)
		return
	}

	nightly := 0
	if req.Nightly == "yes" {
		nightly = 1
	}

	tx, err := db.Get().Begin()
	if err != nil {
		sendError("Failed to save your dataset. Please try again.", err)
		return
	}
	defer tx.Rollback()

	res, err := tx.Exec(`
		INSERT INTO datasets (user_id, data_name, alias, description, intent, extract_intent, tag, visibility, nightly, is_cloned, ping_key, private_key, include_links)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
	`, userID, req.Name, req.Name, req.Description, serpIntent, extractIntent, req.Tag, req.Visibility, nightly, pingKey, privateKey, req.IncludeLinks)
	if err != nil {
		sendError("Failed to save your dataset. Please try again.", err)
		return
	}
	datasetID, err := res.LastInsertId()
	if err != nil {
		sendError("Failed to save your dataset. Please try again.", err)
		return
	}

	if _, err := tx.Exec(`
		INSERT INTO dataset_schema (dataset_id, fields)
		VALUES (?, ?)
	`, datasetID, string(schemaJSON)); err != nil {
		sendError("Failed to save your dataset. Please try again.", err)
		return
	}

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

	for _, entry := range entries {
		urlType := "extraction"
		if t, ok := serpURLType[entry.url]; ok {
			urlType = t
		}
		if entry.sourceType == "import" {
			urlType = "extraction"
		}

		res, err := tx.Exec(`
			INSERT INTO datasets_url (dataset_id, url, rendered_by, source_type, url_type)
			VALUES (?, ?, 'browserless', ?, ?)
		`, datasetID, entry.url, entry.sourceType, urlType)
		if err != nil {
			sendError("Failed to save your dataset. Please try again.", err)
			return
		}
		datasetURLID, err := res.LastInsertId()
		if err != nil {
			sendError("Failed to save your dataset. Please try again.", err)
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
			sendError("Failed to save your dataset. Please try again.", err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		sendError("Failed to save your dataset. Please try again.", err)
		return
	}

	sendProgress("saved", fmt.Sprintf("Dataset saved with %d URLs queued", len(entries)))

	// ── Notifications ─────────────────────────────────────────────────────────
	var datasetCount int
	db.Get().QueryRow(`
		SELECT COUNT(*) FROM datasets WHERE user_id = ?
	`, userID).Scan(&datasetCount)

	if datasetCount == 1 {
		subject, html := notification.FirstDatasetHTML(req.Name)
		notification.NotifyEmail(
			userID,
			&datasetID,
			"first_dataset",
			fmt.Sprintf("Your first dataset \"%s\" is being processed.", req.Name),
			subject,
			html,
		)
	} else {
		notification.Notify(
			userID,
			&datasetID,
			"dataset_created",
			fmt.Sprintf("Your dataset \"%s\" has been created and is being processed.", req.Name),
		)
	}

	// ── Done ──────────────────────────────────────────────────────────────────
	b, _ := json.Marshal(map[string]interface{}{
		"dataset_id": datasetID,
		"name":       req.Name,
	})
	send("done", string(b))

	log.Printf("[queue] done — user=%s dataset_id=%d name=%q urls=%d", userID, datasetID, req.Name, len(entries))
}


func refreshHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		DatasetID int64 `json:"dataset_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.DatasetID < 1 {
		http.Error(w, "dataset_id is required", http.StatusBadRequest)
		return
	}

	var ownerID, dataName string
	var isFrozen int
	var source string
	err := db.Get().QueryRow(`
		SELECT d.user_id, d.data_name, d.is_frozen, COALESCE(ds.source, 'web')
		FROM datasets d
		LEFT JOIN dataset_schema ds ON ds.dataset_id = d.dataset_id
		WHERE d.dataset_id = ?
	`, req.DatasetID).Scan(&ownerID, &dataName, &isFrozen, &source)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if ownerID != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if isFrozen == 1 {
		http.Error(w, "dataset is frozen", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "refresh queued",
		"dataset_id": req.DatasetID,
		"type":       source,
	})

	go func() {
		if err := triggerRefresh(req.DatasetID, dataName, source, userID); err != nil {
			log.Printf("[refresh] error dataset_id=%d: %v", req.DatasetID, err)
		}
	}()
}

func rollbackHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
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
	if req.DatasetID < 1 || req.VersionNumber < 1 {
		http.Error(w, "dataset_id and version_number are required", http.StatusBadRequest)
		return
	}

	var ownerID string
	var isFrozen int
	err := db.Get().QueryRow(`
		SELECT user_id, is_frozen FROM datasets WHERE dataset_id = ?
	`, req.DatasetID).Scan(&ownerID, &isFrozen)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if ownerID != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if isFrozen == 1 {
		http.Error(w, "dataset is frozen — unfreeze it before rolling back", http.StatusForbidden)
		return
	}

	var versionExists int
	db.Get().QueryRow(`
		SELECT COUNT(*) FROM dataset_versions WHERE dataset_id = ? AND version_number = ?
	`, req.DatasetID, req.VersionNumber).Scan(&versionExists)
	if versionExists == 0 {
		http.Error(w, "version not found", http.StatusNotFound)
		return
	}

	_, err = db.Get().Exec(`
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

	log.Printf("[rollback] dataset_id=%d user=%s rolled back to version=%d freeze=%v",
		req.DatasetID, userID, req.VersionNumber, req.Freeze)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
}

func webAlternateDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		DatasetID int64 `json:"dataset_id"`
		Version   int   `json:"version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.DatasetID < 1 || req.Version < 1 {
		http.Error(w, "dataset_id and version are required", http.StatusBadRequest)
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
	if ownerID != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	var altFilePath string
	err = db.Get().QueryRow(`
		SELECT COALESCE(alt_file_path, '') FROM dataset_versions
		WHERE dataset_id = ? AND version_number = ?
	`, req.DatasetID, req.Version).Scan(&altFilePath)
	if err == sql.ErrNoRows {
		http.Error(w, "version not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query version: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if altFilePath != "" {
		if err := storage.Delete(altFilePath); err != nil {
			log.Printf("[web-alternate/delete] delete %s: %v", altFilePath, err)
		}
	}

	_, err = db.Get().Exec(`
		UPDATE dataset_versions
		SET alt_file_path = NULL
		WHERE dataset_id = ? AND version_number = ?
	`, req.DatasetID, req.Version)
	if err != nil {
		http.Error(w, "update version: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[web-alternate/delete] dataset_id=%d version=%d path=%s", req.DatasetID, req.Version, altFilePath)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
}

func webAlternateResultHandler(w http.ResponseWriter, r *http.Request) {
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

	page, limit := parsePageAlt(r)

	var dataName string
	err = db.Get().QueryRow(`
		SELECT COALESCE(NULLIF(TRIM(alias), ''), data_name)
		FROM datasets
		WHERE dataset_id = ?
	`, datasetID).Scan(&dataName)
	if err != nil {
		dataName = fmt.Sprintf("Dataset %d", datasetID)
	}

	var filePath string
	var altFilePath sql.NullString
	err = db.Get().QueryRow(`
		SELECT file_path, alt_file_path FROM dataset_versions
		WHERE dataset_id = ? AND version_number = ?
	`, datasetID, versionNum).Scan(&filePath, &altFilePath)
	if err == sql.ErrNoRows {
		http.Error(w, "version not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query version: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if !altFilePath.Valid || altFilePath.String == "" {
    http.Error(w, "no alternate version exists", http.StatusNotFound)
    return
}

	entities, total, err := streamEntities(altFilePath.String, "entities", page, limit)
	if err != nil {
		http.Error(w, "read alt file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"entities":    entities,
		"data_name":   dataName,
		"version":     versionNum,
		"total":       total,
		"page":        page,
		"limit":       limit,
		"total_pages": totalPages(total, limit),
	})
}

func webAlternateSaveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		DatasetID int64             `json:"dataset_id"`
		Version   int               `json:"version"`
		Entities  []json.RawMessage `json:"entities"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.DatasetID < 1 || req.Version < 1 || len(req.Entities) == 0 {
		http.Error(w, "dataset_id, version, and entities are required", http.StatusBadRequest)
		return
	}

	var ownerID string
	var isFrozen int
	err := db.Get().QueryRow(`
		SELECT user_id, is_frozen FROM datasets WHERE dataset_id = ?
	`, req.DatasetID).Scan(&ownerID, &isFrozen)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if ownerID != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if isFrozen == 1 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "dataset is frozen — unfreeze it before saving an alt version",
		})
		return
	}

	// ── Processing check ──────────────────────────────────────────────────────
	var processingCount int
	err = db.Get().QueryRow(`
		SELECT COUNT(*) FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE du.dataset_id = ?
		AND q.status NOT IN ('done', 'failed')
	`, req.DatasetID).Scan(&processingCount)
	if err != nil {
		http.Error(w, "processing check failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if processingCount > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "dataset is currently processing — please wait until it completes before saving an alt version",
		})
		return
	}

	var filePath string
	err = db.Get().QueryRow(`
		SELECT file_path FROM dataset_versions
		WHERE dataset_id = ? AND version_number = ?
	`, req.DatasetID, req.Version).Scan(&filePath)
	if err == sql.ErrNoRows {
		http.Error(w, "version not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query version: "+err.Error(), http.StatusInternalServerError)
		return
	}

	dir := filePath[:strings.LastIndex(filePath, "/")+1]
	base := filePath[strings.LastIndex(filePath, "/")+1:]
	altFilePath := dir + "alt-" + base

	payload := map[string]interface{}{
		"dataset_id":   req.DatasetID,
		"version":      req.Version,
		"total":        len(req.Entities),
		"generated_at": time.Now().UTC().Format(time.RFC3339Nano),
		"entities":     req.Entities,
	}
	b, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		http.Error(w, "marshal: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := storage.Write(altFilePath, b); err != nil {
		http.Error(w, "write file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = db.Get().Exec(`
		UPDATE dataset_versions
		SET alt_file_path = ?
		WHERE dataset_id = ? AND version_number = ?
	`, altFilePath, req.DatasetID, req.Version)
	if err != nil {
		http.Error(w, "update version: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[web-alternate/save] dataset_id=%d version=%d entities=%d path=%s",
		req.DatasetID, req.Version, len(req.Entities), altFilePath)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":            true,
		"alt_file_path": altFilePath,
		"entity_count":  len(req.Entities),
	})
}

func schemaKeyList(keys map[string]bool) []string {
	list := make([]string, 0, len(keys))
	for k := range keys {
		list = append(list, k)
	}
	for i := 0; i < len(list); i++ {
		for j := i + 1; j < len(list); j++ {
			if list[j] < list[i] {
				list[i], list[j] = list[j], list[i]
			}
		}
	}
	return list
}

func batchRefreshHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DatasetIDs []int64 `json:"dataset_ids"`
		UserID     string  `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if len(req.DatasetIDs) == 0 {
		http.Error(w, "dataset_ids is required", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.UserID) == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	type result struct {
		DatasetID int64  `json:"dataset_id"`
		Status    string `json:"status"`
		Error     string `json:"error,omitempty"`
	}

	var results []result

	for _, id := range req.DatasetIDs {
		var ownerID, dataName, source string
		var isFrozen int
		err := db.Get().QueryRow(`
			SELECT d.user_id, d.data_name, d.is_frozen, COALESCE(ds.source, 'web')
			FROM datasets d
			LEFT JOIN dataset_schema ds ON ds.dataset_id = d.dataset_id
			WHERE d.dataset_id = ?
		`, id).Scan(&ownerID, &dataName, &isFrozen, &source)
		if err == sql.ErrNoRows {
			results = append(results, result{DatasetID: id, Status: "error", Error: "not found"})
			continue
		}
		if err != nil {
			results = append(results, result{DatasetID: id, Status: "error", Error: err.Error()})
			continue
		}
		if ownerID != req.UserID {
			results = append(results, result{DatasetID: id, Status: "error", Error: "forbidden"})
			continue
		}
		if isFrozen == 1 {
			results = append(results, result{DatasetID: id, Status: "error", Error: "dataset is frozen"})
			continue
		}

		id, dataName, source := id, dataName, source
		go func() {
			if err := triggerRefresh(id, dataName, source, req.UserID); err != nil {
				log.Printf("[batch-refresh] error dataset_id=%d: %v", id, err)
			}
		}()

		results = append(results, result{DatasetID: id, Status: "queued"})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"results": results,
	})
}

func profileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	username := strings.TrimPrefix(r.URL.Path, "/profile/")
	username = strings.TrimSpace(username)
	if username == "" {
		http.Error(w, "username is required", http.StatusBadRequest)
		return
	}

	var u struct {
		UserID   string
		UserName string
		Bio      sql.NullString
		RegDate  time.Time
		IsAdmin  int
		InfoText sql.NullString
	}
	err := db.Get().QueryRow(`
		SELECT user_id, user_name, bio, reg_date, is_admin, info_text
		FROM user
		WHERE user_name = ?
	`, username).Scan(
		&u.UserID, &u.UserName,
		&u.Bio, &u.RegDate, &u.IsAdmin, &u.InfoText,
	)
	if err == sql.ErrNoRows {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query user: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var publicDatasetCount int
	db.Get().QueryRow(`
		SELECT COUNT(*) FROM datasets
		WHERE user_id = ? AND visibility = 'public'
	`, u.UserID).Scan(&publicDatasetCount)

	var totalClonesReceived int
	db.Get().QueryRow(`
		SELECT COALESCE(SUM(clone_count), 0) FROM datasets
		WHERE user_id = ?
	`, u.UserID).Scan(&totalClonesReceived)

	var totalVersionsSaved int
	db.Get().QueryRow(`
		SELECT COUNT(*) FROM dataset_versions dv
		JOIN datasets d ON d.dataset_id = dv.dataset_id
		WHERE d.user_id = ?
	`, u.UserID).Scan(&totalVersionsSaved)

	var totalAPIHits int
	db.Get().QueryRow(`
		SELECT COALESCE(SUM(api_hit_count), 0) FROM datasets
		WHERE user_id = ?
	`, u.UserID).Scan(&totalAPIHits)

	rows, err := db.Get().Query(`
		SELECT
			d.dataset_id,
			COALESCE(d.alias, d.data_name) AS display_name,
			COALESCE(d.description, '') AS description,
			COALESCE(d.tag, '') AS tag,
			d.visibility,
			d.is_premium,
			d.price,
			d.clone_count,
			d.api_hit_count,
			d.active_version,
			d.created_at,
			CASE
				WHEN EXISTS (
					SELECT 1 FROM dataset_subreddits dsr WHERE dsr.dataset_id = d.dataset_id
				) THEN 'reddit'
				WHEN ds.source = 'reddit' THEN 'reddit'
				ELSE 'web'
			END AS dataset_type,
			CASE
				WHEN EXISTS (
					SELECT 1 FROM dataset_versions dv
					WHERE dv.dataset_id = d.dataset_id
					AND dv.version_number = d.active_version
					AND dv.alt_file_path IS NOT NULL
					AND dv.alt_file_path != ''
				) THEN 1
				ELSE 0
			END AS has_alt,
			COALESCE(
				(SELECT dv2.entity_count FROM dataset_versions dv2
				 WHERE dv2.dataset_id = d.dataset_id
				 AND dv2.version_number = d.active_version
				 LIMIT 1), 0
			) AS entity_count
		FROM datasets d
		LEFT JOIN dataset_schema ds ON ds.dataset_id = d.dataset_id
		WHERE d.user_id = ?
		  AND d.visibility = 'public'
		  AND d.active_version IS NOT NULL
		ORDER BY d.created_at DESC
	`, u.UserID)
	if err != nil {
		http.Error(w, "query datasets: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type ProfileDataset struct {
		DatasetID     int64     `json:"dataset_id"`
		Name          string    `json:"name"`
		Description   string    `json:"description"`
		Tag           string    `json:"tag"`
		Visibility    string    `json:"visibility"`
		IsPremium     bool      `json:"is_premium"`
		Price         float64   `json:"price"`
		CloneCount    int       `json:"clone_count"`
		ApiHitCount   int       `json:"api_hit_count"`
		ActiveVersion int       `json:"active_version"`
		CreatedAt     time.Time `json:"created_at"`
		DatasetType   string    `json:"dataset_type"`
		HasAlt        bool      `json:"has_alt"`
		EntityCount   int       `json:"entity_count"`
	}

	var datasets []ProfileDataset
	for rows.Next() {
		var d ProfileDataset
		var isPremium, hasAlt int
		var activeVersion sql.NullInt64

		if err := rows.Scan(
			&d.DatasetID, &d.Name, &d.Description, &d.Tag,
			&d.Visibility, &isPremium, &d.Price,
			&d.CloneCount, &d.ApiHitCount, &activeVersion,
			&d.CreatedAt, &d.DatasetType, &hasAlt, &d.EntityCount,
		); err != nil {
			log.Printf("[profile] scan error: %v", err)
			continue
		}
		d.IsPremium = isPremium == 1
		d.HasAlt = hasAlt == 1
		if activeVersion.Valid {
			d.ActiveVersion = int(activeVersion.Int64)
		}
		datasets = append(datasets, d)
	}
	if datasets == nil {
		datasets = []ProfileDataset{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"user": map[string]interface{}{
			"user_id":   u.UserID,
			"username":  u.UserName,
			"bio":       u.Bio.String,
			"joined_at": u.RegDate,
			"is_admin":  u.IsAdmin == 1,
			"info_text": u.InfoText.String,
		},
		"stats": map[string]interface{}{
			"public_datasets": publicDatasetCount,
			"clones_received": totalClonesReceived,
			"versions_saved":  totalVersionsSaved,
			"total_api_hits":  totalAPIHits,
		},
		"datasets": datasets,
	})
}

func sanitizeBio(bio string) string {
	htmlTagRe := regexp.MustCompile(`<[^>]*>`)
	bio = htmlTagRe.ReplaceAllString(bio, "")

	controlRe := regexp.MustCompile(`[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]`)
	bio = controlRe.ReplaceAllString(bio, "")

	bio = strings.TrimSpace(bio)
	runes := []rune(bio)
	if len(runes) > 500 {
		bio = string(runes[:500])
	}

	return bio
}

func profileUpdateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		Bio      string `json:"bio"`
		InfoText string `json:"info_text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	var exists int
	err := db.Get().QueryRow(`
		SELECT COUNT(*) FROM user WHERE user_id = ?
	`, userID).Scan(&exists)
	if err != nil || exists == 0 {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	cleanBio := sanitizeBio(req.Bio)

	htmlTagRe := regexp.MustCompile(`<[^>]*>`)
	cleanInfoText := htmlTagRe.ReplaceAllString(req.InfoText, "")
	controlRe := regexp.MustCompile(`[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]`)
	cleanInfoText = controlRe.ReplaceAllString(cleanInfoText, "")
	cleanInfoText = strings.TrimSpace(cleanInfoText)

	_, err = db.Get().Exec(`
		UPDATE user SET bio = ?, info_text = ? WHERE user_id = ?
	`, cleanBio, cleanInfoText, userID)
	if err != nil {
		http.Error(w, "update failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[profile/update] user_id=%s bio_len=%d", userID, len(cleanBio))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":        true,
		"bio":       cleanBio,
		"info_text": cleanInfoText,
	})
}

func notificationsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	rows, err := db.Get().Query(`
		SELECT notification_id, user_id, dataset_id, kind, message, is_read, emailed, created_at
		FROM notifications
		WHERE user_id = ?
		ORDER BY created_at DESC
		LIMIT 50
	`, userID)
	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type Notification struct {
		NotificationID int64     `json:"notification_id"`
		UserID         string    `json:"user_id"`
		DatasetID      *int64    `json:"dataset_id"`
		Kind           string    `json:"kind"`
		Message        string    `json:"message"`
		IsRead         bool      `json:"is_read"`
		Emailed        bool      `json:"emailed"`
		CreatedAt      time.Time `json:"created_at"`
	}

	var notifications []Notification
	for rows.Next() {
		var n Notification
		var isRead, emailed int
		if err := rows.Scan(
			&n.NotificationID,
			&n.UserID,
			&n.DatasetID,
			&n.Kind,
			&n.Message,
			&isRead,
			&emailed,
			&n.CreatedAt,
		); err != nil {
			log.Printf("[notifications] scan error: %v", err)
			continue
		}
		n.IsRead = isRead == 1
		n.Emailed = emailed == 1
		notifications = append(notifications, n)
	}

	if notifications == nil {
		notifications = []Notification{}
	}

	var unreadCount int
	db.Get().QueryRow(`
		SELECT COUNT(*) FROM notifications WHERE user_id = ? AND is_read = 0
	`, userID).Scan(&unreadCount)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"notifications": notifications,
		"unread_count":  unreadCount,
	})
}

func notificationsReadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		NotificationID *int64 `json:"notification_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.NotificationID != nil {
		_, err := db.Get().Exec(`
			UPDATE notifications SET is_read = 1
			WHERE notification_id = ? AND user_id = ?
		`, *req.NotificationID, userID)
		if err != nil {
			http.Error(w, "update failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		_, err := db.Get().Exec(`
			UPDATE notifications SET is_read = 1 WHERE user_id = ?
		`, userID)
		if err != nil {
			http.Error(w, "update failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
}

func authSyncHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		UserID   string `json:"user_id"`
		Email    string `json:"email"`
		FullName string `json:"full_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	req.UserID = strings.TrimSpace(req.UserID)
	req.Email = strings.TrimSpace(req.Email)
	req.FullName = strings.TrimSpace(req.FullName)

	if req.UserID == "" || req.Email == "" {
		http.Error(w, "user_id and email are required", http.StatusBadRequest)
		return
	}

	username := strings.ToLower(strings.Split(req.Email, "@")[0])
	usernameRe := regexp.MustCompile(`[^a-z0-9_]`)
	username = usernameRe.ReplaceAllString(username, "_")
	if len(username) > 50 {
		username = username[:50]
	}

	var existingUserID string
	err := db.Get().QueryRow(`
		SELECT user_id FROM user WHERE user_id = ?
	`, req.UserID).Scan(&existingUserID)

	isNewUser := false

	if err == sql.ErrNoRows {
		baseUsername := username
		for i := 1; ; i++ {
			var count int
			db.Get().QueryRow(`
				SELECT COUNT(*) FROM user WHERE user_name = ?
			`, username).Scan(&count)
			if count == 0 {
				break
			}
			username = fmt.Sprintf("%s_%d", baseUsername, i)
		}

		_, err = db.Get().Exec(`
			INSERT INTO user (user_id, user_name, email, is_admin)
			VALUES (?, ?, ?, 0)
		`, req.UserID, username, req.Email)
		if err != nil {
			log.Printf("[auth/sync] insert error user_id=%s: %v", req.UserID, err)
			http.Error(w, "failed to create user: "+err.Error(), http.StatusInternalServerError)
			return
		}

		isNewUser = true
		log.Printf("[auth/sync] new user created user_id=%s username=%s", req.UserID, username)
	} else if err != nil {
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	} else {
		db.Get().QueryRow(`
			SELECT user_name FROM user WHERE user_id = ?
		`, req.UserID).Scan(&username)
		log.Printf("[auth/sync] existing user user_id=%s username=%s", req.UserID, username)
	}

	var user struct {
		UserID   string         `json:"user_id"`
		UserName string         `json:"username"`
		Email    string         `json:"email"`
		Bio      sql.NullString `json:"-"`
		IsAdmin  int            `json:"is_admin"`
		RegDate  string         `json:"created_at"`
	}
	err = db.Get().QueryRow(`
		SELECT user_id, user_name, email, bio, is_admin, reg_date
		FROM user WHERE user_id = ?
	`, req.UserID).Scan(
		&user.UserID, &user.UserName, &user.Email,
		&user.Bio, &user.IsAdmin, &user.RegDate,
	)
	if err != nil {
		http.Error(w, "fetch user error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if isNewUser {
		notification.Notify(
			req.UserID,
			nil,
			"welcome",
			"Welcome to Vexaro! Start by creating your first dataset.",
		)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":         true,
		"user_id":    user.UserID,
		"username":   user.UserName,
		"email":      user.Email,
		"bio":        user.Bio.String,
		"is_admin":   user.IsAdmin == 1,
		"created_at": user.RegDate,
	})
}

func amazonQueueHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		Name        string   `json:"name"`
		Description string   `json:"description"`
		Tag         string   `json:"tag"`
		Visibility  string   `json:"visibility"`
		Nightly     string   `json:"nightly"`
		Marketplace string   `json:"marketplace"`
		ASINs       []string `json:"asins"`
		Fields      []string `json:"fields"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	var ve []string
	if strings.TrimSpace(req.Name) == "" {
		ve = append(ve, "name is required")
	}
	if strings.TrimSpace(req.Description) == "" {
		ve = append(ve, "description is required")
	}
	if strings.TrimSpace(req.Marketplace) == "" {
		ve = append(ve, "marketplace is required")
	}
	if len(req.ASINs) == 0 {
		ve = append(ve, "at least one ASIN is required")
	}
	if len(req.Fields) == 0 {
		ve = append(ve, "at least one field is required")
	}
	if req.Visibility != "public" && req.Visibility != "private" {
		ve = append(ve, "visibility must be 'public' or 'private'")
	}
	if req.Nightly != "yes" && req.Nightly != "no" {
		ve = append(ve, "nightly must be 'yes' or 'no'")
	}

	asinRe := regexp.MustCompile(`^[A-Z0-9]{10}$`)
	var validASINs []string
	for _, asin := range req.ASINs {
		asin = strings.TrimSpace(strings.ToUpper(asin))
		if asinRe.MatchString(asin) {
			validASINs = append(validASINs, asin)
		}
	}
	if len(validASINs) == 0 {
		ve = append(ve, "no valid ASINs provided — ASINs must be 10 alphanumeric characters")
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

	// ── Plan checks ───────────────────────────────────────────────────────────
	plan := GetUserPlan(userID)
	if CountUserDatasets(userID) >= plan.Datasets {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": fmt.Sprintf("dataset limit reached (%d). Upgrade your plan to create more.", plan.Datasets),
		})
		return
	}
	if len(validASINs) > plan.URLs {
		validASINs = validASINs[:plan.URLs]
	}

	schemaFields := make(map[string]map[string]string, len(req.Fields))
	for _, f := range req.Fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		schemaFields[f] = map[string]string{
			"type":        f,
			"description": f,
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

	pingKey := generateKey()
	privateKey := generateKey()

	res, err := db.Get().Exec(`
		INSERT INTO datasets
			(user_id, data_name, alias, description, tag, visibility, nightly, is_cloned, dataset_type, marketplace, ping_key, private_key)
		VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'amazon', ?, ?, ?)
	`,
		userID,
		strings.TrimSpace(req.Name),
		strings.TrimSpace(req.Name),
		strings.TrimSpace(req.Description),
		strings.TrimSpace(req.Tag),
		req.Visibility,
		nightly,
		strings.TrimSpace(req.Marketplace),
		pingKey,
		privateKey,
	)
	if err != nil {
		http.Error(w, "insert dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	datasetID, err := res.LastInsertId()
	if err != nil {
		http.Error(w, "get dataset id: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err := db.Get().Exec(`
		INSERT INTO dataset_schema (dataset_id, fields, source)
		VALUES (?, ?, 'amazon')
	`, datasetID, string(schemaJSON)); err != nil {
		http.Error(w, "insert schema: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[amazon/queue] dataset inserted — id=%d marketplace=%s asins=%d fields=%d",
		datasetID, req.Marketplace, len(validASINs), len(req.Fields))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":         true,
		"dataset_id": datasetID,
	})

	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[amazon/queue/bg] panic dataset_id=%d: %v", datasetID, rec)
			}
		}()

		tx, err := db.Get().Begin()
		if err != nil {
			log.Printf("[amazon/queue/bg] begin tx error dataset_id=%d: %v", datasetID, err)
			return
		}
		defer tx.Rollback()

		queued := 0
		for _, asin := range validASINs {
			productURL := fmt.Sprintf("https://www.amazon.%s/dp/%s", req.Marketplace, asin)

			urlRes, err := tx.Exec(`
				INSERT INTO datasets_url (dataset_id, url, rendered_by, source_type, url_type)
				VALUES (?, ?, 'amazon-api', 'import', 'extraction')
			`, datasetID, productURL)
			if err != nil {
				log.Printf("[amazon/queue/bg] insert url error asin=%s: %v", asin, err)
				continue
			}
			datasetURLID, err := urlRes.LastInsertId()
			if err != nil {
				log.Printf("[amazon/queue/bg] url id error asin=%s: %v", asin, err)
				continue
			}

			if _, err := tx.Exec(`
				INSERT INTO queue (dataset_url_id, status, crawl_type)
				VALUES (?, 'pending', 'fresh')
			`, datasetURLID); err != nil {
				log.Printf("[amazon/queue/bg] insert queue error asin=%s: %v", asin, err)
				continue
			}

			queued++
		}

		if err := tx.Commit(); err != nil {
			log.Printf("[amazon/queue/bg] commit error dataset_id=%d: %v", datasetID, err)
			return
		}

		log.Printf("[amazon/queue/bg] done — dataset_id=%d queued=%d", datasetID, queued)

		notification.Notify(
			userID,
			&datasetID,
			"dataset_created",
			fmt.Sprintf("Your Amazon dataset \"%s\" has been created and is being processed.", req.Name),
		)
	}()
}

func amazonEditDatasetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		DatasetID    int64    `json:"dataset_id"`
		Alias        string   `json:"alias"`
		Description  string   `json:"description"`
		Tag          string   `json:"tag"`
		Visibility   string   `json:"visibility"`
		Nightly      string   `json:"nightly"`
		Fields       []string `json:"fields"`
		NewASINs     []string `json:"new_asins"`
		URLsToDelete []int64  `json:"urls_to_delete"`
		IsPremium    bool     `json:"is_premium"`
		Price        float64  `json:"price"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	var ve []string
	if req.DatasetID < 1 {
		ve = append(ve, "dataset_id is required")
	}
	if strings.TrimSpace(req.Alias) == "" {
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
	if len(req.Fields) == 0 {
		ve = append(ve, "at least one field is required")
	}
	if len(ve) > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "validation failed", "fields": ve})
		return
	}

	var ownerID string
	var marketplace string
	err := db.Get().QueryRow(`
		SELECT user_id, COALESCE(marketplace, 'com')
		FROM datasets WHERE dataset_id = ?
	`, req.DatasetID).Scan(&ownerID, &marketplace)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if ownerID != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	// ── Processing check ──────────────────────────────────────────────────────
	var processingCount int
	err = db.Get().QueryRow(`
		SELECT COUNT(*) FROM queue q
		JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
		WHERE du.dataset_id = ?
		AND q.status NOT IN ('done', 'failed')
	`, req.DatasetID).Scan(&processingCount)
	if err != nil {
		http.Error(w, "processing check failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if processingCount > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "dataset is currently processing — please wait until it completes before editing",
		})
		return
	}

	// ── Plan URL limit check ──────────────────────────────────────────────────
	if len(req.NewASINs) > 0 {
		plan := GetUserPlan(userID)
		currentCount := CountDatasetURLs(req.DatasetID)
		remaining := plan.URLs - currentCount
		if remaining <= 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": fmt.Sprintf("ASIN limit reached (%d). Upgrade your plan to add more.", plan.URLs),
			})
			return
		}
		if len(req.NewASINs) > remaining {
			req.NewASINs = req.NewASINs[:remaining]
		}
	}

	for _, urlID := range req.URLsToDelete {
		var count int
		err := db.Get().QueryRow(`
			SELECT COUNT(*) FROM datasets_url
			WHERE dataset_url_id = ? AND dataset_id = ?
		`, urlID, req.DatasetID).Scan(&count)
		if err != nil {
			http.Error(w, "validate url ownership: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if count == 0 {
			http.Error(w, fmt.Sprintf("url_id %d does not belong to this dataset", urlID), http.StatusForbidden)
			return
		}
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

	incomingFields := make(map[string]map[string]string, len(req.Fields))
	for _, f := range req.Fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		incomingFields[f] = map[string]string{
			"type":        f,
			"description": f,
		}
	}

	schemaChanged := false
	if len(incomingFields) != len(existingFields) {
		schemaChanged = true
	} else {
		for key := range incomingFields {
			if _, ok := existingFields[key]; !ok {
				schemaChanged = true
				break
			}
		}
	}
	log.Printf("[amazon-edit] dataset_id=%d schema_changed=%v", req.DatasetID, schemaChanged)

	schemaJSON, err := json.Marshal(incomingFields)
	if err != nil {
		http.Error(w, "marshal schema: "+err.Error(), http.StatusInternalServerError)
		return
	}

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

	_, err = tx.Exec(`
		UPDATE datasets
		SET alias       = ?,
		    description = ?,
		    tag         = ?,
		    visibility  = ?,
		    nightly     = ?,
		    is_premium  = ?,
		    price       = ?
		WHERE dataset_id = ?
	`,
		strings.TrimSpace(req.Alias),
		strings.TrimSpace(req.Description),
		strings.TrimSpace(req.Tag),
		req.Visibility,
		nightly,
		req.IsPremium,
		req.Price,
		req.DatasetID,
	)
	if err != nil {
		http.Error(w, "update dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		UPDATE dataset_schema SET fields = ? WHERE dataset_id = ?
	`, string(schemaJSON), req.DatasetID)
	if err != nil {
		http.Error(w, "update schema: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if schemaChanged {
		res, err := tx.Exec(`
			UPDATE queue q
			JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
			SET q.status = 'proceed-format'
			WHERE du.dataset_id = ?
			  AND q.status = 'done'
			  AND du.folder_path IS NOT NULL
		`, req.DatasetID)
		if err != nil {
			http.Error(w, "requeue for re-format: "+err.Error(), http.StatusInternalServerError)
			return
		}
		affected, _ := res.RowsAffected()
		log.Printf("[amazon-edit] schema changed — requeued %d urls for re-format", affected)
	}

	urlsDeleted := 0
	for _, urlID := range req.URLsToDelete {
		_, _ = tx.Exec(`DELETE FROM queue WHERE dataset_url_id = ?`, urlID)
		res, err := tx.Exec(`
			DELETE FROM datasets_url WHERE dataset_url_id = ? AND dataset_id = ?
		`, urlID, req.DatasetID)
		if err != nil {
			log.Printf("[amazon-edit] delete url_id=%d: %v", urlID, err)
			continue
		}
		n, _ := res.RowsAffected()
		urlsDeleted += int(n)
	}

	asinsAdded := 0
	for _, asin := range req.NewASINs {
		asin = strings.TrimSpace(strings.ToUpper(asin))
		if len(asin) != 10 {
			continue
		}

		productURL := fmt.Sprintf("https://www.amazon.%s/dp/%s", marketplace, asin)

		var exists int
		tx.QueryRow(`
			SELECT COUNT(*) FROM datasets_url WHERE dataset_id = ? AND url = ?
		`, req.DatasetID, productURL).Scan(&exists)
		if exists > 0 {
			log.Printf("[amazon-edit] skipping duplicate asin=%s", asin)
			continue
		}

		urlRes, err := tx.Exec(`
			INSERT INTO datasets_url (dataset_id, url, rendered_by, source_type, url_type)
			VALUES (?, ?, 'amazon-api', 'import', 'extraction')
		`, req.DatasetID, productURL)
		if err != nil {
			log.Printf("[amazon-edit] insert url asin=%s: %v", asin, err)
			continue
		}
		urlID, _ := urlRes.LastInsertId()

		if _, err := tx.Exec(`
			INSERT INTO queue (dataset_url_id, status, crawl_type)
			VALUES (?, 'pending', 'fresh')
		`, urlID); err != nil {
			log.Printf("[amazon-edit] insert queue asin=%s: %v", asin, err)
			continue
		}
		asinsAdded++
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "commit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[amazon-edit] done — dataset_id=%d alias=%q schema_changed=%v asins_added=%d urls_deleted=%d",
		req.DatasetID, req.Alias, schemaChanged, asinsAdded, urlsDeleted)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":             true,
		"schema_changed": schemaChanged,
		"asins_added":    asinsAdded,
		"urls_deleted":   urlsDeleted,
	})
}

func webhookRegisterHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		DatasetID int64  `json:"dataset_id"`
		URL       string `json:"url"`
		Secret    string `json:"secret"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.DatasetID < 1 || strings.TrimSpace(req.URL) == "" {
		http.Error(w, "dataset_id and url are required", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(req.URL, "http") {
		http.Error(w, "url must be a valid http/https URL", http.StatusBadRequest)
		return
	}

	// ── Plan check ────────────────────────────────────────────────────────────
	plan := GetUserPlan(userID)
	if !plan.Webhook {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "webhooks are not available on your current plan. Upgrade to use this feature.",
		})
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
	if ownerID != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	var secret interface{}
	if strings.TrimSpace(req.Secret) != "" {
		secret = strings.TrimSpace(req.Secret)
	}

	_, err = db.Get().Exec(`
		INSERT INTO dataset_webhooks (dataset_id, user_id, url, secret, is_active)
		VALUES (?, ?, ?, ?, 1)
		ON DUPLICATE KEY UPDATE url = VALUES(url), secret = VALUES(secret), is_active = 1, last_fired_at = NULL, last_status = NULL
	`, req.DatasetID, userID, strings.TrimSpace(req.URL), secret)
	if err != nil {
		http.Error(w, "save webhook: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[webhook] registered dataset_id=%d user_id=%s url=%s", req.DatasetID, userID, req.URL)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
}

func webhookDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		DatasetID int64 `json:"dataset_id"`
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
	if ownerID != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	_, err = db.Get().Exec(`
		DELETE FROM dataset_webhooks WHERE dataset_id = ? AND user_id = ?
	`, req.DatasetID, userID)
	if err != nil {
		http.Error(w, "delete webhook: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[webhook] deleted dataset_id=%d user_id=%s", req.DatasetID, userID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
}

func webhookViewHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	datasetID, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("dataset_id")))
	if err != nil || datasetID < 1 {
		http.Error(w, "invalid dataset_id", http.StatusBadRequest)
		return
	}

	var ownerID string
	err = db.Get().QueryRow(`
		SELECT user_id FROM datasets WHERE dataset_id = ?
	`, datasetID).Scan(&ownerID)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if ownerID != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	var webhookURL, secret sql.NullString
	var isActive int
	var lastFiredAt sql.NullTime
	var lastStatus sql.NullInt64
	var createdAt time.Time

	err = db.Get().QueryRow(`
		SELECT url, secret, is_active, last_fired_at, last_status, created_at
		FROM dataset_webhooks WHERE dataset_id = ?
	`, datasetID).Scan(&webhookURL, &secret, &isActive, &lastFiredAt, &lastStatus, &createdAt)
	if err == sql.ErrNoRows {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"has_webhook": false})
		return
	}
	if err != nil {
		http.Error(w, "query webhook: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resp := map[string]interface{}{
		"has_webhook":   true,
		"url":           webhookURL.String,
		"has_secret":    secret.Valid && secret.String != "",
		"is_active":     isActive == 1,
		"last_fired_at": nil,
		"last_status":   nil,
		"created_at":    createdAt,
	}
	if lastFiredAt.Valid {
		resp["last_fired_at"] = lastFiredAt.Time
	}
	if lastStatus.Valid {
		resp["last_status"] = lastStatus.Int64
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func regeneratePingKeyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		DatasetID int64 `json:"dataset_id"`
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
	if ownerID != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	newKey := generateKey()
	_, err = db.Get().Exec(`
		UPDATE datasets SET ping_key = ? WHERE dataset_id = ?
	`, newKey, req.DatasetID)
	if err != nil {
		http.Error(w, "update failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[regenerate] ping_key regenerated dataset_id=%d user_id=%s", req.DatasetID, userID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":       true,
		"ping_key": newKey,
	})
}

func regeneratePrivateKeyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		DatasetID int64 `json:"dataset_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.DatasetID < 1 {
		http.Error(w, "dataset_id is required", http.StatusBadRequest)
		return
	}

	var ownerID, visibility string
	err := db.Get().QueryRow(`
		SELECT user_id, visibility FROM datasets WHERE dataset_id = ?
	`, req.DatasetID).Scan(&ownerID, &visibility)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if ownerID != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if visibility != "private" {
		http.Error(w, "private key only applies to private datasets", http.StatusBadRequest)
		return
	}

	newKey := generateKey()
	_, err = db.Get().Exec(`
		UPDATE datasets SET private_key = ? WHERE dataset_id = ?
	`, newKey, req.DatasetID)
	if err != nil {
		http.Error(w, "update failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[regenerate] private_key regenerated dataset_id=%d user_id=%s", req.DatasetID, userID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":          true,
		"private_key": newKey,
	})
}



func testCrawlHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cfg := crawl.Config{BrowserlessKey: os.Getenv("BROWSERLESS_KEYS")}

	// Read URLs from test-urls.txt
	f, err := os.Open("test-urls.txt")
	if err != nil {
		http.Error(w, "open test-urls.txt: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	var urls []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		urls = append(urls, line)
	}

	os.MkdirAll("data/test", 0755)

	type result struct {
		URL         string `json:"url"`
		Status      string `json:"status"`
		ContentType string `json:"content_type"`
		ContentLen  int    `json:"content_length"`
		Error       string `json:"error,omitempty"`
	}

	var results []result

	for _, u := range urls {
		// Reject check
		if crawl.IsRejectedURL(u) {
			results = append(results, result{URL: u, Status: "rejected"})
			continue
		}

		html, err := crawl.FetchRaw(cfg, u)
		if err != nil {
			results = append(results, result{URL: u, Status: "failed", Error: err.Error()})
			continue
		}

		// Run extract with includeLinks false for now
		data := crawl.ExtractPublic(u, html, "test", true)

		// Save to data/test/<hash>.txt
		hash := fmt.Sprintf("%x", sha256.Sum256([]byte(u)))[:12]
		outPath := fmt.Sprintf("data/test/%s.txt", hash)
		content := fmt.Sprintf("URL: %s\nTITLE: %s\nLAYER: %s\n\n%s", 
			data.URL, data.Title, data.LayerUsed, data.Content)
		os.WriteFile(outPath, []byte(content), 0644)

		results = append(results, result{
			URL:         u,
			Status:      "ok",
			ContentType: crawl.DetectContentTypePublic(html),
			ContentLen:  len(data.Content),
		})

		log.Printf("[test/crawl] done url=%s len=%d file=%s", u, len(data.Content), outPath)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total":   len(urls),
		"results": results,
	})
}