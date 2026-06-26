package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/var-raphael/vexaro-engine/db"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/var-raphael/vexaro-engine/storage"
)

// ── Rate limiter ──────────────────────────────────────────────────────────────

type rateLimitEntry struct {
	count     int
	windowStart time.Time
}

type rateLimiter struct {
	mu      sync.Mutex
	entries map[string]*rateLimitEntry

	publicLimit  int
	privateLimit int
	windowSize   time.Duration
}

var apiRateLimiter = &rateLimiter{
	entries:      make(map[string]*rateLimitEntry),
	publicLimit:  100,
	privateLimit: 300,
	windowSize:   time.Minute,
}

func init() {
	// Periodically clean up stale entries to prevent memory leak
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			apiRateLimiter.cleanup()
		}
	}()
}

func (rl *rateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	for key, entry := range rl.entries {
		if now.Sub(entry.windowStart) > rl.windowSize {
			delete(rl.entries, key)
		}
	}
}

// allow returns true if the request is within the rate limit.
// key should be "ip" for public or "ip:private" for private requests.
func (rl *rateLimiter) allow(key string, isPrivate bool) (bool, int, time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	limit := rl.publicLimit
	if isPrivate {
		limit = rl.privateLimit
	}

	now := time.Now()
	entry, ok := rl.entries[key]
	if !ok || now.Sub(entry.windowStart) > rl.windowSize {
		rl.entries[key] = &rateLimitEntry{count: 1, windowStart: now}
		return true, limit - 1, rl.windowSize
	}

	entry.count++
	remaining := limit - entry.count
	retryAfter := rl.windowSize - now.Sub(entry.windowStart)

	if entry.count > limit {
		return false, 0, retryAfter
	}
	return true, remaining, retryAfter
}

func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For first (if behind a proxy/load balancer)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if ip := strings.TrimSpace(parts[0]); ip != "" {
			return ip
		}
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	// Fall back to RemoteAddr, strip port
	ip := r.RemoteAddr
	if i := strings.LastIndex(ip, ":"); i != -1 {
		ip = ip[:i]
	}
	return ip
}

// ── Handler ───────────────────────────────────────────────────────────────────

func apiDatasetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/")
	path = strings.Trim(path, "/")
	segments := strings.Split(path, "/")

	if len(segments) < 3 {
		http.Error(w, "invalid path — expected /api/{id}/{name}/{version}/", http.StatusBadRequest)
		return
	}

	datasetIDStr := segments[0]
	nameSlug := segments[1]
	versionStr := segments[2]
	isAlt := (len(segments) >= 4 && segments[3] == "alt") ||
		strings.ToLower(strings.TrimSpace(r.URL.Query().Get("alt"))) == "true"

	datasetID, err := strconv.Atoi(datasetIDStr)
	if err != nil || datasetID < 1 {
		http.Error(w, "invalid dataset id", http.StatusBadRequest)
		return
	}

	var d struct {
		DatasetID     int64
		DataName      string
		Alias         sql.NullString
		Visibility    string
		PrivateKey    string
		IsFrozen      int
		ActiveVersion sql.NullInt64
	}
	err = db.Get().QueryRow(`
		SELECT dataset_id, data_name, alias, visibility, COALESCE(private_key, ''), is_frozen, active_version
		FROM datasets WHERE dataset_id = ?
	`, datasetID).Scan(
		&d.DatasetID, &d.DataName, &d.Alias,
		&d.Visibility, &d.PrivateKey,
		&d.IsFrozen, &d.ActiveVersion,
	)
	if err == sql.ErrNoRows {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query dataset: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if d.IsFrozen == 1 {
		http.Error(w, "dataset is frozen", http.StatusGone)
		return
	}

	displayName := d.DataName
	if d.Alias.Valid && strings.TrimSpace(d.Alias.String) != "" {
		displayName = d.Alias.String
	}
	if nameSlug != slugify(displayName) {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}

	// ── Auth check for private datasets ──────────────────────────────────────
	isPrivateAuth := false
	if d.Visibility == "private" {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "authorization required", http.StatusUnauthorized)
			return
		}
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			http.Error(w, "invalid authorization format — expected: Bearer {key}", http.StatusUnauthorized)
			return
		}
		if parts[1] != d.PrivateKey {
			http.Error(w, "invalid private key", http.StatusForbidden)
			return
		}
		isPrivateAuth = true
	}

	// ── Rate limiting ─────────────────────────────────────────────────────────
	clientIP := getClientIP(r)
	rateLimitKey := clientIP
	if isPrivateAuth {
		rateLimitKey = clientIP + ":private"
	}
	allowed, remaining, retryAfter := apiRateLimiter.allow(rateLimitKey, isPrivateAuth)
	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(func() int {
		if isPrivateAuth {
			return apiRateLimiter.privateLimit
		}
		return apiRateLimiter.publicLimit
	}()))
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
	w.Header().Set("X-RateLimit-Reset", strconv.Itoa(int(time.Now().Add(retryAfter).Unix())))
	if !allowed {
		w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())))
		http.Error(w, "rate limit exceeded — try again later", http.StatusTooManyRequests)
		return
	}

	// ── Version resolution ────────────────────────────────────────────────────
	var versionNumber int
	switch versionStr {
	case "active":
		if !d.ActiveVersion.Valid {
			http.Error(w, "dataset has no active version", http.StatusNotFound)
			return
		}
		versionNumber = int(d.ActiveVersion.Int64)
	case "latest":
		err = db.Get().QueryRow(`
			SELECT MAX(version_number) FROM dataset_versions WHERE dataset_id = ?
		`, datasetID).Scan(&versionNumber)
		if err != nil || versionNumber < 1 {
			http.Error(w, "dataset has no versions", http.StatusNotFound)
			return
		}
	default:
		if !strings.HasPrefix(versionStr, "v") {
			http.Error(w, "invalid version — use 'active', 'latest', or 'vN'", http.StatusBadRequest)
			return
		}
		n, err := strconv.Atoi(strings.TrimPrefix(versionStr, "v"))
		if err != nil || n < 1 {
			http.Error(w, "invalid version number", http.StatusBadRequest)
			return
		}
		versionNumber = n
	}

	var filePath string
	var altFilePath sql.NullString
	err = db.Get().QueryRow(`
		SELECT file_path, alt_file_path
		FROM dataset_versions
		WHERE dataset_id = ? AND version_number = ?
	`, datasetID, versionNumber).Scan(&filePath, &altFilePath)
	if err == sql.ErrNoRows {
		http.Error(w, fmt.Sprintf("version %s not found", versionStr), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "query version: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if isAlt && (!altFilePath.Valid || strings.TrimSpace(altFilePath.String) == "") {
		http.Error(w, "no alt version exists for this version", http.StatusNotFound)
		return
	}

	targetPath := filePath
	if isAlt {
		targetPath = altFilePath.String
	}

	// ── Query params ──────────────────────────────────────────────────────────
	q := r.URL.Query()

	format := strings.ToLower(strings.TrimSpace(q.Get("format")))
	if format == "" {
		format = "json"
	}
	validFormats := map[string]bool{
		"json": true, "jsonl": true, "csv": true,
		"tsv": true, "xml": true, "parquet": true,
	}
	if !validFormats[format] {
		http.Error(w, "invalid format — supported: json, jsonl, csv, tsv, xml, parquet", http.StatusBadRequest)
		return
	}

	dedup         := strings.ToLower(q.Get("dedup")) == "true"
	denull        := strings.ToLower(q.Get("denull")) == "true"
	flattenFields := strings.ToLower(q.Get("flatten")) == "true"
	includeSource := strings.ToLower(q.Get("include_source")) != "false"
	pretty        := strings.ToLower(q.Get("pretty")) != "false"
	countOnly     := strings.ToLower(q.Get("count")) == "true"

	var dedupKeys []string
	if dk := strings.TrimSpace(q.Get("dedup_key")); dk != "" {
		for _, f := range strings.Split(dk, ",") {
			if t := strings.TrimSpace(f); t != "" {
				dedupKeys = append(dedupKeys, t)
			}
		}
	}

	var keepFields []string
	if kf := strings.TrimSpace(q.Get("keep_field")); kf != "" {
		for _, f := range strings.Split(kf, ",") {
			if t := strings.TrimSpace(f); t != "" {
				keepFields = append(keepFields, t)
			}
		}
	}

	var dropFields []string
	if df := strings.TrimSpace(q.Get("drop_field")); df != "" {
		for _, f := range strings.Split(df, ",") {
			if t := strings.TrimSpace(f); t != "" {
				dropFields = append(dropFields, t)
			}
		}
	}

	type fieldFilter struct{ field, value string }
	var exactFilters []fieldFilter
	for _, fv := range q["filter"] {
		parts := strings.SplitN(fv, ":", 2)
		if len(parts) == 2 {
			exactFilters = append(exactFilters, fieldFilter{
				field: strings.TrimSpace(parts[0]),
				value: strings.ToLower(strings.TrimSpace(parts[1])),
			})
		}
	}

	var containsFilters []fieldFilter
	for _, fv := range q["filter_contains"] {
		parts := strings.SplitN(fv, ":", 2)
		if len(parts) == 2 {
			containsFilters = append(containsFilters, fieldFilter{
				field: strings.TrimSpace(parts[0]),
				value: strings.ToLower(strings.TrimSpace(parts[1])),
			})
		}
	}

	var keywords []string
	keywordsAND := strings.ToLower(strings.TrimSpace(q.Get("keywords_mode"))) == "and"
	if kw := strings.TrimSpace(q.Get("keywords")); kw != "" {
		for _, k := range strings.Split(kw, ",") {
			if t := strings.ToLower(strings.TrimSpace(k)); t != "" {
				keywords = append(keywords, t)
			}
		}
	}

	type sortSpec struct{ field, dir string }
	var sortBy *sortSpec
	if sv := strings.TrimSpace(q.Get("sort")); sv != "" {
		parts := strings.SplitN(sv, ":", 2)
		dir := "asc"
		if len(parts) == 2 && strings.ToLower(parts[1]) == "desc" {
			dir = "desc"
		}
		sortBy = &sortSpec{field: strings.TrimSpace(parts[0]), dir: dir}
	}

	limit, offset, sample := 0, 0, 0
	if lv := strings.TrimSpace(q.Get("limit")); lv != "" {
		if n, err := strconv.Atoi(lv); err == nil && n > 0 {
			limit = n
		}
	}
	if ov := strings.TrimSpace(q.Get("offset")); ov != "" {
		if n, err := strconv.Atoi(ov); err == nil && n >= 0 {
			offset = n
		}
	}
	if sv := strings.TrimSpace(q.Get("sample")); sv != "" {
		if n, err := strconv.Atoi(sv); err == nil && n > 0 {
			sample = n
		}
	}

	// ── Load file ─────────────────────────────────────────────────────────────
	fileBytes, err := storage.Read(targetPath)
	if err != nil {
		log.Printf("[api] read file error path=%s: %v", targetPath, err)
		http.Error(w, "could not read dataset file", http.StatusInternalServerError)
		return
	}

	var raw struct {
		Entities []json.RawMessage `json:"entities"`
		Posts    []json.RawMessage `json:"posts"`
	}
	if err := json.Unmarshal(fileBytes, &raw); err != nil {
		http.Error(w, "could not parse dataset file", http.StatusInternalServerError)
		return
	}
	items := raw.Entities
	if len(items) == 0 {
		items = raw.Posts
	}

	entities := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		var m map[string]interface{}
		if err := json.Unmarshal(item, &m); err != nil {
			continue
		}
		entities = append(entities, m)
	}

	// ── Transformations ───────────────────────────────────────────────────────

	// 1. strip _source
	if !includeSource {
		for _, e := range entities {
			delete(e, "_source")
		}
	}

	// 2. denull — recursive, preserves structure
	if denull {
		for i, e := range entities {
			if cleaned, ok := apiDenull(e).(map[string]interface{}); ok {
				entities[i] = cleaned
			}
		}
	}

	// 3. keep_field — dot-notation aware
	if len(keepFields) > 0 {
		for i, e := range entities {
			entities[i] = apiKeepFields(e, keepFields)
		}
	}

	// 4. drop_field — dot-notation aware
	if len(dropFields) > 0 {
		for i, e := range entities {
			entities[i] = apiDropFields(e, dropFields)
		}
	}

	// 5. exact filters — dot-notation, fans out across arrays
	if len(exactFilters) > 0 {
		out := entities[:0]
		for _, e := range entities {
			ok := true
			for _, f := range exactFilters {
				vals := apiGetNestedValues(e, f.field)
				if len(vals) == 0 {
					ok = false
					break
				}
				matched := false
				for _, v := range vals {
					if strings.ToLower(fmt.Sprintf("%v", v)) == f.value {
						matched = true
						break
					}
				}
				if !matched {
					ok = false
					break
				}
			}
			if ok {
				out = append(out, e)
			}
		}
		entities = out
	}

	// 6. contains filters — dot-notation, fans out across arrays
	if len(containsFilters) > 0 {
		out := entities[:0]
		for _, e := range entities {
			ok := true
			for _, f := range containsFilters {
				vals := apiGetNestedValues(e, f.field)
				if len(vals) == 0 {
					ok = false
					break
				}
				matched := false
				for _, v := range vals {
					if strings.Contains(strings.ToLower(fmt.Sprintf("%v", v)), f.value) {
						matched = true
						break
					}
				}
				if !matched {
					ok = false
					break
				}
			}
			if ok {
				out = append(out, e)
			}
		}
		entities = out
	}

	// 7. keywords — recurses through all values at every depth
	if len(keywords) > 0 {
		out := entities[:0]
		for _, e := range entities {
			haystack := strings.ToLower(apiCollectAllStrings(e))
			var matched bool
			if keywordsAND {
				matched = true
				for _, kw := range keywords {
					if !strings.Contains(haystack, kw) {
						matched = false
						break
					}
				}
			} else {
				for _, kw := range keywords {
					if strings.Contains(haystack, kw) {
						matched = true
						break
					}
				}
			}
			if matched {
				out = append(out, e)
			}
		}
		entities = out
	}

	// 8. sort — top-level fields only, skips gracefully if value is nested
	if sortBy != nil {
		field := sortBy.field
		asc := sortBy.dir == "asc"
		sort.SliceStable(entities, func(i, j int) bool {
			vi := fmt.Sprintf("%v", entities[i][field])
			vj := fmt.Sprintf("%v", entities[j][field])
			ni, erri := strconv.ParseFloat(vi, 64)
			nj, errj := strconv.ParseFloat(vj, 64)
			var less bool
			if erri == nil && errj == nil {
				less = ni < nj
			} else {
				less = vi < vj
			}
			return asc == less
		})
	}

	// 9. dedup — top-level only
	if dedup {
		seen := make(map[string]struct{}, len(entities))
		out := entities[:0]
		for _, e := range entities {
			var h string
			if len(dedupKeys) > 0 {
				hh := sha256.New()
				for _, k := range dedupKeys {
					v, _ := json.Marshal(e[k])
					hh.Write([]byte(k))
					hh.Write(v)
				}
				h = hex.EncodeToString(hh.Sum(nil))
			} else {
				h = apiEntityHash(e)
			}
			if _, exists := seen[h]; !exists {
				seen[h] = struct{}{}
				out = append(out, e)
			}
		}
		entities = out
	}

	// 10. sample
	if sample > 0 && sample < len(entities) {
		if err := cryptoShuffle(entities, sample); err != nil {
			log.Printf("[api] sample shuffle error: %v", err)
		}
		entities = entities[:sample]
	}

	// 11. offset + limit
	if offset > 0 {
		if offset >= len(entities) {
			entities = []map[string]interface{}{}
		} else {
			entities = entities[offset:]
		}
	}
	if limit > 0 && limit < len(entities) {
		entities = entities[:limit]
	}

	if countOnly {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"count":      len(entities),
			"dataset_id": datasetID,
			"version":    versionNumber,
			"alt":        isAlt,
		})
		go incrementAPIHit(int64(datasetID))
		return
	}

	// 12. flatten — for formats that require it or explicit request
	needsFlatten := flattenFields || format == "csv" || format == "tsv" || format == "parquet"
	var flatEntities []map[string]interface{}
	if needsFlatten {
		flatEntities = make([]map[string]interface{}, len(entities))
		for i, e := range entities {
			flatEntities[i] = apiFlattenFull(e, "")
		}
	}

	// ── Response headers ──────────────────────────────────────────────────────
	w.Header().Set("X-Dataset-ID", fmt.Sprintf("%d", datasetID))
	w.Header().Set("X-Dataset-Version", fmt.Sprintf("v%d", versionNumber))
	w.Header().Set("X-Dataset-Alt", fmt.Sprintf("%v", isAlt))
	w.Header().Set("X-Total-Count", fmt.Sprintf("%d", len(entities)))

	// ── Format output ─────────────────────────────────────────────────────────
	switch format {
	case "jsonl":
		w.Header().Set("Content-Type", "application/x-ndjson")
		enc := json.NewEncoder(w)
		for _, e := range entities {
			if err := enc.Encode(e); err != nil {
				break
			}
		}

	case "csv":
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", fmt.Sprintf(
			`attachment; filename="%s-v%d.csv"`, slugify(displayName), versionNumber,
		))
		apiWriteDelimited(w, flatEntities, ',')

	case "tsv":
		w.Header().Set("Content-Type", "text/tab-separated-values")
		w.Header().Set("Content-Disposition", fmt.Sprintf(
			`attachment; filename="%s-v%d.tsv"`, slugify(displayName), versionNumber,
		))
		apiWriteDelimited(w, flatEntities, '\t')

	case "xml":
		w.Header().Set("Content-Type", "application/xml")
		apiWriteXML(w, entities, displayName)

	case "parquet":
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", fmt.Sprintf(
			`attachment; filename="%s-v%d.parquet"`, slugify(displayName), versionNumber,
		))
		b, err := apiEncodeParquet(flatEntities)
		if err != nil {
			http.Error(w, "parquet encode failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(b)

	default: // json
		w.Header().Set("Content-Type", "application/json")
		out := entities
		if flattenFields {
			out = flatEntities
		}
		if pretty {
			b, _ := json.MarshalIndent(out, "", "  ")
			w.Write(b)
		} else {
			json.NewEncoder(w).Encode(out)
		}
	}

	log.Printf("[api] dataset_id=%d version=v%d alt=%v format=%s count=%d dedup=%v dedup_keys=%v denull=%v keywords=%v sort=%v flatten=%v ip=%s",
		datasetID, versionNumber, isAlt, format, len(entities), dedup, dedupKeys, denull, keywords, sortBy, flattenFields, clientIP)

	go incrementAPIHit(int64(datasetID))
}

// ── Transformation helpers ────────────────────────────────────────────────────

// apiDenull recursively removes null values and empty strings at every depth.
// Preserves original structure — objects stay objects, arrays stay arrays.
func apiDenull(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(val))
		for k, v2 := range val {
			if v2 == nil {
				continue
			}
			cleaned := apiDenull(v2)
			if cleaned == nil {
				continue
			}
			out[k] = cleaned
		}
		return out
	case []interface{}:
		out := make([]interface{}, 0, len(val))
		for _, v2 := range val {
			if v2 == nil {
				continue
			}
			cleaned := apiDenull(v2)
			if cleaned == nil {
				continue
			}
			out = append(out, cleaned)
		}
		if len(out) == 0 {
			return nil
		}
		return out
	case string:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return val
	default:
		return v
	}
}

// apiGetNestedValues resolves a dot-notation path against a value.
// Fans out across arrays — "reviews.rating" returns all rating values
// from every object in the reviews array.
func apiGetNestedValues(v interface{}, path string) []interface{} {
	parts := strings.SplitN(path, ".", 2)
	key := parts[0]
	rest := ""
	if len(parts) == 2 {
		rest = parts[1]
	}

	switch val := v.(type) {
	case map[string]interface{}:
		child, ok := val[key]
		if !ok {
			return nil
		}
		if rest == "" {
			return []interface{}{child}
		}
		return apiGetNestedValues(child, rest)
	case []interface{}:
		var out []interface{}
		for _, item := range val {
			out = append(out, apiGetNestedValues(item, path)...)
		}
		return out
	default:
		return nil
	}
}

// apiKeepFields filters an entity to only include the specified dot-notation paths.
// Nested paths like "reviews.rating" keep the reviews array structure but strip
// all other fields inside each review object.
func apiKeepFields(e map[string]interface{}, fields []string) map[string]interface{} {
	type keepNode struct {
		keepAll  bool
		children map[string]*keepNode
	}

	root := &keepNode{children: make(map[string]*keepNode)}
	for _, f := range fields {
		parts := strings.Split(f, ".")
		node := root
		for i, p := range parts {
			if node.children[p] == nil {
				node.children[p] = &keepNode{children: make(map[string]*keepNode)}
			}
			node = node.children[p]
			if i == len(parts)-1 {
				node.keepAll = true
			}
		}
	}

	var filterValue func(v interface{}, node *keepNode) interface{}
	filterValue = func(v interface{}, node *keepNode) interface{} {
		if node.keepAll {
			return v
		}
		switch val := v.(type) {
		case map[string]interface{}:
			out := make(map[string]interface{})
			for k, child := range node.children {
				if cv, ok := val[k]; ok {
					result := filterValue(cv, child)
					if result != nil {
						out[k] = result
					}
				}
			}
			return out
		case []interface{}:
			out := make([]interface{}, 0, len(val))
			for _, item := range val {
				result := filterValue(item, node)
				if result != nil {
					out = append(out, result)
				}
			}
			return out
		default:
			return v
		}
	}

	out := make(map[string]interface{})
	for k, child := range root.children {
		if cv, ok := e[k]; ok {
			result := filterValue(cv, child)
			if result != nil {
				out[k] = result
			}
		}
	}
	return out
}

// apiDropFields removes dot-notation paths from an entity recursively.
// e.g. drop_field=reviews.content removes content from every review object.
func apiDropFields(e map[string]interface{}, fields []string) map[string]interface{} {
	type dropNode struct {
		dropAll  bool
		children map[string]*dropNode
	}

	root := &dropNode{children: make(map[string]*dropNode)}
	for _, f := range fields {
		parts := strings.Split(f, ".")
		node := root
		for i, p := range parts {
			if node.children[p] == nil {
				node.children[p] = &dropNode{children: make(map[string]*dropNode)}
			}
			node = node.children[p]
			if i == len(parts)-1 {
				node.dropAll = true
			}
		}
	}

	var dropValue func(v interface{}, node *dropNode) interface{}
	dropValue = func(v interface{}, node *dropNode) interface{} {
		if node.dropAll {
			return nil
		}
		switch val := v.(type) {
		case map[string]interface{}:
			out := make(map[string]interface{}, len(val))
			for k, cv := range val {
				if childNode, shouldProcess := node.children[k]; shouldProcess {
					result := dropValue(cv, childNode)
					if result != nil {
						out[k] = result
					}
				} else {
					out[k] = cv
				}
			}
			return out
		case []interface{}:
			out := make([]interface{}, 0, len(val))
			for _, item := range val {
				result := dropValue(item, node)
				if result != nil {
					out = append(out, result)
				}
			}
			return out
		default:
			return v
		}
	}

	out := make(map[string]interface{}, len(e))
	for k, cv := range e {
		if childNode, shouldProcess := root.children[k]; shouldProcess {
			result := dropValue(cv, childNode)
			if result != nil {
				out[k] = result
			}
		} else {
			out[k] = cv
		}
	}
	return out
}

// apiCollectAllStrings recursively collects all leaf values at any depth
// into a single string for keyword search.
func apiCollectAllStrings(v interface{}) string {
	var sb strings.Builder
	var collect func(interface{})
	collect = func(v interface{}) {
		switch val := v.(type) {
		case map[string]interface{}:
			for _, cv := range val {
				collect(cv)
				sb.WriteByte(' ')
			}
		case []interface{}:
			for _, item := range val {
				collect(item)
				sb.WriteByte(' ')
			}
		case nil:
			// skip
		default:
			sb.WriteString(fmt.Sprintf("%v", val))
			sb.WriteByte(' ')
		}
	}
	collect(v)
	return sb.String()
}

// apiFlattenFull recursively flattens a map including arrays of objects.
// Arrays of primitives are JSON-encoded into a single string cell.
// Arrays of objects are expanded with indexed keys: reviews_0_rating, reviews_1_rating etc.
func apiFlattenFull(m map[string]interface{}, prefix string) map[string]interface{} {
	out := make(map[string]interface{})
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + "_" + k
		}
		switch val := v.(type) {
		case map[string]interface{}:
			for fk, fv := range apiFlattenFull(val, key) {
				out[fk] = fv
			}
		case []interface{}:
			allPrimitive := true
			for _, item := range val {
				if _, isMap := item.(map[string]interface{}); isMap {
					allPrimitive = false
					break
				}
			}
			if allPrimitive {
				b, _ := json.Marshal(val)
				out[key] = string(b)
			} else {
				for i, item := range val {
					indexedKey := fmt.Sprintf("%s_%d", key, i)
					switch itemVal := item.(type) {
					case map[string]interface{}:
						for fk, fv := range apiFlattenFull(itemVal, indexedKey) {
							out[fk] = fv
						}
					default:
						out[indexedKey] = itemVal
					}
				}
			}
		default:
			out[key] = v
		}
	}
	return out
}

// ── Output format helpers ─────────────────────────────────────────────────────

func incrementAPIHit(datasetID int64) {
	if _, err := db.Get().Exec(`
		UPDATE datasets SET api_hit_count = api_hit_count + 1 WHERE dataset_id = ?
	`, datasetID); err != nil {
		log.Printf("[api] increment api_hit_count error dataset_id=%d: %v", datasetID, err)
	}
}

func cryptoShuffle(entities []map[string]interface{}, n int) error {
	length := len(entities)
	for i := 0; i < n; i++ {
		rangSize := int64(length - i)
		rb := make([]byte, 8)
		if _, err := rand.Read(rb); err != nil {
			return err
		}
		var randUint uint64
		for _, b := range rb {
			randUint = randUint<<8 | uint64(b)
		}
		j := i + int(randUint%uint64(rangSize))
		entities[i], entities[j] = entities[j], entities[i]
	}
	return nil
}

func apiEntityHash(e map[string]interface{}) string {
	keys := make([]string, 0, len(e))
	for k := range e {
		if k != "_source" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		v, _ := json.Marshal(e[k])
		h.Write([]byte(k))
		h.Write(v)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func apiWriteDelimited(w http.ResponseWriter, entities []map[string]interface{}, delimiter rune) {
	if len(entities) == 0 {
		return
	}
	seen := make(map[string]bool)
	var cols []string
	for _, e := range entities {
		for k := range e {
			if !seen[k] {
				seen[k] = true
				cols = append(cols, k)
			}
		}
	}
	sort.Strings(cols)

	cw := csv.NewWriter(w)
	cw.Comma = delimiter
	cw.Write(cols)
	for _, e := range entities {
		row := make([]string, len(cols))
		for i, col := range cols {
			v := e[col]
			if v == nil {
				row[i] = ""
			} else {
				switch val := v.(type) {
				case string:
					row[i] = val
				case float64:
					if val == float64(int64(val)) {
						row[i] = fmt.Sprintf("%d", int64(val))
					} else {
						row[i] = fmt.Sprintf("%g", val)
					}
				default:
					b, _ := json.Marshal(v)
					row[i] = string(b)
				}
			}
		}
		cw.Write(row)
	}
	cw.Flush()
}

func apiWriteXML(w http.ResponseWriter, entities []map[string]interface{}, datasetName string) {
	safeName := apiXMLEscape(datasetName)
	w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>` + "\n"))
	w.Write([]byte(fmt.Sprintf(`<dataset name="%s">`, safeName) + "\n"))
	for _, e := range entities {
		w.Write([]byte("  <entity>\n"))
		keys := make([]string, 0, len(e))
		for k := range e {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			apiWriteXMLValue(w, k, e[k], 2)
		}
		w.Write([]byte("  </entity>\n"))
	}
	w.Write([]byte("</dataset>\n"))
}

func apiWriteXMLValue(w http.ResponseWriter, tag string, v interface{}, depth int) {
	indent := strings.Repeat("  ", depth)
	safeTag := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, tag)
	if len(safeTag) > 0 && safeTag[0] >= '0' && safeTag[0] <= '9' {
		safeTag = "_" + safeTag
	}

	switch val := v.(type) {
	case map[string]interface{}:
		w.Write([]byte(fmt.Sprintf("%s<%s>\n", indent, safeTag)))
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			apiWriteXMLValue(w, k, val[k], depth+1)
		}
		w.Write([]byte(fmt.Sprintf("%s</%s>\n", indent, safeTag)))
	case []interface{}:
		singular := strings.TrimSuffix(safeTag, "s")
		if singular == safeTag {
			singular = "item"
		}
		w.Write([]byte(fmt.Sprintf("%s<%s>\n", indent, safeTag)))
		for _, item := range val {
			apiWriteXMLValue(w, singular, item, depth+1)
		}
		w.Write([]byte(fmt.Sprintf("%s</%s>\n", indent, safeTag)))
	case nil:
		w.Write([]byte(fmt.Sprintf("%s<%s/>\n", indent, safeTag)))
	default:
		w.Write([]byte(fmt.Sprintf("%s<%s>%s</%s>\n",
			indent, safeTag, apiXMLEscape(fmt.Sprintf("%v", val)), safeTag)))
	}
}

func apiXMLEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, `"`, "&quot;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	return s
}

func apiEncodeParquet(entities []map[string]interface{}) ([]byte, error) {
	if len(entities) == 0 {
		return nil, fmt.Errorf("no entities")
	}
	seen := make(map[string]bool)
	var cols []string
	for _, e := range entities {
		for k := range e {
			if !seen[k] {
				seen[k] = true
				cols = append(cols, k)
			}
		}
	}
	sort.Strings(cols)

	groupFields := make(parquet.Group)
	for _, col := range cols {
		groupFields[col] = parquet.Leaf(parquet.String().Type())
	}
	schema := parquet.NewSchema("dataset", groupFields)

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[map[string]string](&buf, schema)
	for _, e := range entities {
		row := make(map[string]string, len(cols))
		for _, col := range cols {
			if v := e[col]; v == nil {
				row[col] = ""
			} else {
				row[col] = fmt.Sprintf("%v", v)
			}
		}
		if _, err := writer.Write([]map[string]string{row}); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func slugify(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == ' ' || r == '-' || r == '_':
			b.WriteRune('-')
		}
	}
	result := regexp.MustCompile(`-+`).ReplaceAllString(b.String(), "-")
	return strings.Trim(result, "-")
}