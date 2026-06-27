package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/var-raphael/vexaro-engine/db"
	"github.com/var-raphael/vexaro-engine/storage"
)

// ---------------------------------------------------------------- token helpers --

func generateMCPToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func validateMCPToken(token string) (string, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return "", fmt.Errorf("token required")
	}

	var userID string
	var isActive int
	err := db.Get().QueryRow(`
		SELECT user_id, is_active FROM mcp_tokens WHERE token = ?
	`, token).Scan(&userID, &isActive)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("invalid token")
	}
	if err != nil {
		return "", fmt.Errorf("db error: %w", err)
	}
	if isActive != 1 {
		return "", fmt.Errorf("token revoked")
	}

	db.Get().Exec(`
		UPDATE mcp_tokens SET last_used_at = NOW() WHERE token = ?
	`, token)

	return userID, nil
}

func extractBearerToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return ""
	}
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

// ---------------------------------------------------------------- token HTTP handlers --

func mcpTokenGenerateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
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

	token, err := generateMCPToken()
	if err != nil {
		http.Error(w, "failed to generate token", http.StatusInternalServerError)
		return
	}

	_, err = db.Get().Exec(`
		INSERT INTO mcp_tokens (user_id, token, is_active)
		VALUES (?, ?, 1)
		ON DUPLICATE KEY UPDATE token = VALUES(token), is_active = 1, last_used_at = NULL, created_at = NOW()
	`, userID, token)
	if err != nil {
		http.Error(w, "failed to save token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[mcp/token] generated token for user_id=%s", userID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":         true,
		"token":      token,
		"created_at": time.Now().UTC(),
	})
}

func mcpTokenRevokeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	_, err := db.Get().Exec(`
		UPDATE mcp_tokens SET is_active = 0 WHERE user_id = ?
	`, userID)
	if err != nil {
		http.Error(w, "revoke failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[mcp/token] revoked token for user_id=%s", userID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
}

func mcpTokenViewHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := authedUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var token string
	var isActive int
	var lastUsedAt sql.NullTime
	var createdAt time.Time

	err := db.Get().QueryRow(`
		SELECT token, is_active, last_used_at, created_at
		FROM mcp_tokens WHERE user_id = ?
	`, userID).Scan(&token, &isActive, &lastUsedAt, &createdAt)
	if err == sql.ErrNoRows {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"has_token": false,
		})
		return
	}
	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resp := map[string]interface{}{
		"has_token":  true,
		"token":      token,
		"is_active":  isActive == 1,
		"created_at": createdAt,
	}
	if lastUsedAt.Valid {
		resp["last_used_at"] = lastUsedAt.Time
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ---------------------------------------------------------------- query pipeline --

type mcpQueryParams struct {
	Format          string
	Dedup           bool
	DedupKeys       []string
	Denull          bool
	FlattenFields   bool
	IncludeSource   bool
	Pretty          bool
	CountOnly       bool
	KeepFields      []string
	DropFields      []string
	ExactFilters    []mcpFieldFilter
	ContainsFilters []mcpFieldFilter
	Keywords        []string
	KeywordsAND     bool
	SortField       string
	SortDir         string
	Limit           int
	Offset          int
	Sample          int
}

type mcpFieldFilter struct {
	Field string
	Value string
}

func runQueryPipeline(entities []map[string]interface{}, p mcpQueryParams) []map[string]interface{} {
	// 1. strip _source
	if !p.IncludeSource {
		for _, e := range entities {
			delete(e, "_source")
		}
	}

	// 2. denull — recursive, preserves structure
	if p.Denull {
		for i, e := range entities {
			if cleaned, ok := apiDenull(e).(map[string]interface{}); ok {
				entities[i] = cleaned
			}
		}
	}

	// 3. keep_field — dot-notation aware
	if len(p.KeepFields) > 0 {
		for i, e := range entities {
			entities[i] = apiKeepFields(e, p.KeepFields)
		}
	}

	// 4. drop_field — dot-notation aware
	if len(p.DropFields) > 0 {
		for i, e := range entities {
			entities[i] = apiDropFields(e, p.DropFields)
		}
	}

	// 5. exact filters — dot-notation, fans out across arrays
	if len(p.ExactFilters) > 0 {
		out := entities[:0]
		for _, e := range entities {
			ok := true
			for _, f := range p.ExactFilters {
				vals := apiGetNestedValues(e, f.Field)
				if len(vals) == 0 {
					ok = false
					break
				}
				matched := false
				for _, v := range vals {
					if strings.ToLower(fmt.Sprintf("%v", v)) == f.Value {
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
	if len(p.ContainsFilters) > 0 {
		out := entities[:0]
		for _, e := range entities {
			ok := true
			for _, f := range p.ContainsFilters {
				vals := apiGetNestedValues(e, f.Field)
				if len(vals) == 0 {
					ok = false
					break
				}
				matched := false
				for _, v := range vals {
					if strings.Contains(strings.ToLower(fmt.Sprintf("%v", v)), f.Value) {
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
	if len(p.Keywords) > 0 {
		out := entities[:0]
		for _, e := range entities {
			haystack := strings.ToLower(apiCollectAllStrings(e))
			var matched bool
			if p.KeywordsAND {
				matched = true
				for _, kw := range p.Keywords {
					if !strings.Contains(haystack, kw) {
						matched = false
						break
					}
				}
			} else {
				for _, kw := range p.Keywords {
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

	// 8. sort — top-level fields only
	if p.SortField != "" {
		asc := p.SortDir == "asc"
		sort.SliceStable(entities, func(i, j int) bool {
			vi := fmt.Sprintf("%v", entities[i][p.SortField])
			vj := fmt.Sprintf("%v", entities[j][p.SortField])
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
	if p.Dedup {
		seen := make(map[string]struct{}, len(entities))
		out := entities[:0]
		for _, e := range entities {
			var h string
			if len(p.DedupKeys) > 0 {
				hh := sha256.New()
				for _, k := range p.DedupKeys {
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
	if p.Sample > 0 && p.Sample < len(entities) {
		if err := cryptoShuffle(entities, p.Sample); err != nil {
			log.Printf("[mcp/query] sample shuffle error: %v", err)
		}
		entities = entities[:p.Sample]
	}

	// 11. offset + limit
	if p.Offset > 0 {
		if p.Offset >= len(entities) {
			return []map[string]interface{}{}
		}
		entities = entities[p.Offset:]
	}
	if p.Limit > 0 && p.Limit < len(entities) {
		entities = entities[:p.Limit]
	}

	return entities
}

func formatEntities(entities []map[string]interface{}, format string, datasetName string, flatten bool) (string, error) {
	switch format {
	case "jsonl":
		var sb strings.Builder
		enc := json.NewEncoder(&sb)
		for _, e := range entities {
			if err := enc.Encode(e); err != nil {
				return "", err
			}
		}
		return sb.String(), nil

	case "csv", "tsv":
		flat := make([]map[string]interface{}, len(entities))
		for i, e := range entities {
			flat[i] = apiFlattenFull(e, "")
		}
		delimiter := ','
		if format == "tsv" {
			delimiter = '\t'
		}
		var buf bytes.Buffer
		cw := csv.NewWriter(&buf)
		cw.Comma = rune(delimiter)
		if len(flat) > 0 {
			seen := make(map[string]bool)
			var cols []string
			for _, e := range flat {
				for k := range e {
					if !seen[k] {
						seen[k] = true
						cols = append(cols, k)
					}
				}
			}
			sort.Strings(cols)
			cw.Write(cols)
			for _, e := range flat {
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
		return buf.String(), nil

	case "xml":
		var sb strings.Builder
		sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
		sb.WriteString(fmt.Sprintf(`<dataset name="%s">`, apiXMLEscape(datasetName)) + "\n")
		for _, e := range entities {
			sb.WriteString("  <entity>\n")
			keys := make([]string, 0, len(e))
			for k := range e {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				mcpWriteXMLValue(&sb, k, e[k], 2)
			}
			sb.WriteString("  </entity>\n")
		}
		sb.WriteString("</dataset>\n")
		return sb.String(), nil

	case "parquet":
		flat := make([]map[string]interface{}, len(entities))
		for i, e := range entities {
			flat[i] = apiFlattenFull(e, "")
		}
		b, err := apiEncodeParquet(flat)
		if err != nil {
			return "", err
		}
		return string(b), nil

	default: // json
		out := entities
		if flatten {
			out = make([]map[string]interface{}, len(entities))
			for i, e := range entities {
				out[i] = apiFlattenFull(e, "")
			}
		}
		b, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}

func mcpWriteXMLValue(sb *strings.Builder, tag string, v interface{}, depth int) {
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
		sb.WriteString(fmt.Sprintf("%s<%s>\n", indent, safeTag))
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			mcpWriteXMLValue(sb, k, val[k], depth+1)
		}
		sb.WriteString(fmt.Sprintf("%s</%s>\n", indent, safeTag))
	case []interface{}:
		singular := strings.TrimSuffix(safeTag, "s")
		if singular == safeTag {
			singular = "item"
		}
		sb.WriteString(fmt.Sprintf("%s<%s>\n", indent, safeTag))
		for _, item := range val {
			mcpWriteXMLValue(sb, singular, item, depth+1)
		}
		sb.WriteString(fmt.Sprintf("%s</%s>\n", indent, safeTag))
	case nil:
		sb.WriteString(fmt.Sprintf("%s<%s/>\n", indent, safeTag))
	default:
		sb.WriteString(fmt.Sprintf("%s<%s>%s</%s>\n",
			indent, safeTag, apiXMLEscape(fmt.Sprintf("%v", val)), safeTag))
	}
}

// ---------------------------------------------------------------- load entities from file --

func loadEntitiesFromFile(filePath string) ([]map[string]interface{}, error) {
	fileBytes, err := storage.Read(filePath)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	var raw struct {
		Entities []json.RawMessage `json:"entities"`
		Posts    []json.RawMessage `json:"posts"`
	}
	if err := json.Unmarshal(fileBytes, &raw); err != nil {
		return nil, fmt.Errorf("parse file: %w", err)
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
	return entities, nil
}

// ---------------------------------------------------------------- resolve version file --

func resolveVersionFile(datasetID int64, version string, useAlt bool) (string, int, error) {
	var versionNumber int
	var filePath string
	var altFilePath sql.NullString

	switch version {
	case "active", "":
		var activeVersion sql.NullInt64
		err := db.Get().QueryRow(`
			SELECT active_version FROM datasets WHERE dataset_id = ?
		`, datasetID).Scan(&activeVersion)
		if err != nil || !activeVersion.Valid {
			return "", 0, fmt.Errorf("no active version found")
		}
		versionNumber = int(activeVersion.Int64)
	case "latest":
		err := db.Get().QueryRow(`
			SELECT MAX(version_number) FROM dataset_versions WHERE dataset_id = ?
		`, datasetID).Scan(&versionNumber)
		if err != nil || versionNumber < 1 {
			return "", 0, fmt.Errorf("no versions found")
		}
	default:
		n, err := strconv.Atoi(strings.TrimPrefix(version, "v"))
		if err != nil || n < 1 {
			return "", 0, fmt.Errorf("invalid version: %s", version)
		}
		versionNumber = n
	}

	err := db.Get().QueryRow(`
		SELECT file_path, alt_file_path FROM dataset_versions
		WHERE dataset_id = ? AND version_number = ?
	`, datasetID, versionNumber).Scan(&filePath, &altFilePath)
	if err == sql.ErrNoRows {
		return "", 0, fmt.Errorf("version %d not found", versionNumber)
	}
	if err != nil {
		return "", 0, fmt.Errorf("query version: %w", err)
	}

	if useAlt {
		if !altFilePath.Valid || strings.TrimSpace(altFilePath.String) == "" {
			return "", 0, fmt.Errorf("no alt version exists for version %d", versionNumber)
		}
		return altFilePath.String, versionNumber, nil
	}

	return filePath, versionNumber, nil
}

// ---------------------------------------------------------------- MCP server setup --

func setupMCPServer() *server.MCPServer {
	s := server.NewMCPServer(
		"Quorel",
		"1.0.0",
		server.WithToolCapabilities(true),
	)

	// ── Tool: list_datasets ───────────────────────────────────────────────────
	s.AddTool(
		mcp.NewTool("list_datasets",
			mcp.WithDescription("List all datasets owned by the authenticated user. Returns dataset metadata including name, type, entity count, and last refresh time."),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			userID, ok := ctx.Value(mcpUserIDKey).(string)
			if !ok || userID == "" {
				return mcp.NewToolResultError("unauthorized"), nil
			}

			rows, err := db.Get().Query(`
				SELECT d.dataset_id, COALESCE(d.alias, d.data_name), d.dataset_type,
				       d.active_version, dv.entity_count, dv.created_at
				FROM datasets d
				LEFT JOIN dataset_versions dv
				  ON d.dataset_id = dv.dataset_id AND d.active_version = dv.version_number
				WHERE d.user_id = ?
				ORDER BY d.dataset_id DESC
			`, userID)
			if err != nil {
				return mcp.NewToolResultError("query failed: " + err.Error()), nil
			}
			defer rows.Close()

			type datasetRow struct {
				DatasetID     int64         `json:"dataset_id"`
				Name          string        `json:"name"`
				DatasetType   string        `json:"dataset_type"`
				ActiveVersion sql.NullInt64 `json:"-"`
				EntityCount   sql.NullInt64 `json:"-"`
				CreatedAt     sql.NullTime  `json:"-"`
				Version       interface{}   `json:"active_version"`
				Count         interface{}   `json:"entity_count"`
				LastRefresh   interface{}   `json:"last_refresh"`
			}

			var datasets []datasetRow
			for rows.Next() {
				var d datasetRow
				if err := rows.Scan(&d.DatasetID, &d.Name, &d.DatasetType,
					&d.ActiveVersion, &d.EntityCount, &d.CreatedAt); err != nil {
					continue
				}
				if d.ActiveVersion.Valid {
					d.Version = d.ActiveVersion.Int64
				}
				if d.EntityCount.Valid {
					d.Count = d.EntityCount.Int64
				}
				if d.CreatedAt.Valid {
					d.LastRefresh = d.CreatedAt.Time.UTC().Format(time.RFC3339)
				}
				datasets = append(datasets, d)
			}

			if datasets == nil {
				datasets = []datasetRow{}
			}

			result := map[string]interface{}{
				"datasets": datasets,
				"total":    len(datasets),
			}
			b, _ := json.MarshalIndent(result, "", "  ")
			return mcp.NewToolResultText(string(b)), nil
		},
	)

	// ── Tool: get_dataset_schema ──────────────────────────────────────────────
	s.AddTool(
		mcp.NewTool("get_dataset_schema",
			mcp.WithDescription("Get the field schema of a dataset. Call this before query_dataset to understand what fields are available for filtering and sorting."),
			mcp.WithNumber("dataset_id", mcp.Required(), mcp.Description("The dataset ID to inspect")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			userID, ok := ctx.Value(mcpUserIDKey).(string)
			if !ok || userID == "" {
				return mcp.NewToolResultError("unauthorized"), nil
			}

			args, ok := req.Params.Arguments.(map[string]interface{})
			if !ok {
				return mcp.NewToolResultError("invalid arguments"), nil
			}

			datasetID := int64(args["dataset_id"].(float64))

			var ownerID, dataName, datasetType string
			var activeVersion sql.NullInt64
			err := db.Get().QueryRow(`
				SELECT user_id, COALESCE(alias, data_name), dataset_type, active_version
				FROM datasets WHERE dataset_id = ?
			`, datasetID).Scan(&ownerID, &dataName, &datasetType, &activeVersion)
			if err == sql.ErrNoRows {
				return mcp.NewToolResultError("dataset not found"), nil
			}
			if err != nil {
				return mcp.NewToolResultError("query failed: " + err.Error()), nil
			}
			if ownerID != userID {
				return mcp.NewToolResultError("access denied"), nil
			}

			var fieldsJSON string
			var includeLinks, includeFiles, includeImages int
			err = db.Get().QueryRow(`
				SELECT fields, include_links, include_files, include_images
				FROM dataset_schema WHERE dataset_id = ?
			`, datasetID).Scan(&fieldsJSON, &includeLinks, &includeFiles, &includeImages)
			if err != nil {
				return mcp.NewToolResultError("schema not found"), nil
			}

			var rawFields map[string]map[string]string
			if err := json.Unmarshal([]byte(fieldsJSON), &rawFields); err != nil {
				return mcp.NewToolResultError("parse schema failed"), nil
			}

			var fields []string
			for k := range rawFields {
				fields = append(fields, k)
			}
			sort.Strings(fields)

			entityCount := 0
			if activeVersion.Valid {
				db.Get().QueryRow(`
					SELECT COALESCE(entity_count, 0) FROM dataset_versions
					WHERE dataset_id = ? AND version_number = ?
				`, datasetID, activeVersion.Int64).Scan(&entityCount)
			}

			result := map[string]interface{}{
				"dataset_id":     datasetID,
				"name":           dataName,
				"dataset_type":   datasetType,
				"fields":         fields,
				"field_details":  rawFields,
				"entity_count":   entityCount,
				"include_links":  includeLinks == 1,
				"include_images": includeImages == 1,
				"include_files":  includeFiles == 1,
			}
			b, _ := json.MarshalIndent(result, "", "  ")
			return mcp.NewToolResultText(string(b)), nil
		},
	)

	// ── Tool: query_dataset ───────────────────────────────────────────────────
	s.AddTool(
		mcp.NewTool("query_dataset",
			mcp.WithDescription("Query a dataset with filters, keywords, sorting, and pagination. Supports all formats: json, jsonl, csv, tsv, xml, parquet."),
			mcp.WithNumber("dataset_id", mcp.Required(), mcp.Description("The dataset ID to query")),
			mcp.WithString("version", mcp.Description("Version to query: 'active' (default), 'latest', or 'v1', 'v2' etc")),
			mcp.WithString("format", mcp.Description("Output format: json (default), jsonl, csv, tsv, xml, parquet")),
			mcp.WithString("keywords", mcp.Description("Comma-separated keywords to search across all fields including nested")),
			mcp.WithString("keywords_mode", mcp.Description("'and' requires all keywords match, 'or' (default) requires any")),
			mcp.WithString("filter", mcp.Description("Exact field match, dot-notation supported: 'reviews.rating:5'")),
			mcp.WithString("filter_contains", mcp.Description("Partial field match, dot-notation supported: 'reviews.author:john'")),
			mcp.WithString("keep_field", mcp.Description("Comma-separated fields to keep, dot-notation supported: 'title,reviews.rating'")),
			mcp.WithString("drop_field", mcp.Description("Comma-separated fields to remove, dot-notation supported: 'reviews.content'")),
			mcp.WithString("sort", mcp.Description("Sort by top-level field: 'field:asc' or 'field:desc'")),
			mcp.WithNumber("limit", mcp.Description("Max number of results to return")),
			mcp.WithNumber("offset", mcp.Description("Number of results to skip")),
			mcp.WithNumber("sample", mcp.Description("Return N random results")),
			mcp.WithBoolean("dedup", mcp.Description("Remove duplicate entities")),
			mcp.WithString("dedup_key", mcp.Description("Comma-separated top-level fields to use for dedup")),
			mcp.WithBoolean("denull", mcp.Description("Recursively remove null and empty string fields at all depths")),
			mcp.WithBoolean("flatten", mcp.Description("Flatten nested objects and arrays into top-level fields")),
			mcp.WithBoolean("include_source", mcp.Description("Include _source field (default true)")),
			mcp.WithBoolean("count", mcp.Description("Return only the count of matching entities")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			userID, ok := ctx.Value(mcpUserIDKey).(string)
			if !ok || userID == "" {
				return mcp.NewToolResultError("unauthorized"), nil
			}

			args, ok := req.Params.Arguments.(map[string]interface{})
			if !ok {
				return mcp.NewToolResultError("invalid arguments"), nil
			}

			datasetID := int64(args["dataset_id"].(float64))

			var ownerID, dataName string
			err := db.Get().QueryRow(`
				SELECT user_id, COALESCE(alias, data_name) FROM datasets WHERE dataset_id = ?
			`, datasetID).Scan(&ownerID, &dataName)
			if err == sql.ErrNoRows {
				return mcp.NewToolResultError("dataset not found"), nil
			}
			if err != nil {
				return mcp.NewToolResultError("query failed: " + err.Error()), nil
			}
			if ownerID != userID {
				return mcp.NewToolResultError("access denied"), nil
			}

			version := "active"
			if v, ok := args["version"].(string); ok && v != "" {
				version = v
			}

			filePath, versionNumber, err := resolveVersionFile(datasetID, version, false)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}

			entities, err := loadEntitiesFromFile(filePath)
			if err != nil {
				return mcp.NewToolResultError("load file: " + err.Error()), nil
			}

			p := mcpQueryParams{
				IncludeSource: true,
				Pretty:        true,
				Format:        "json",
			}

			if f, ok := args["format"].(string); ok && f != "" {
				p.Format = strings.ToLower(f)
			}
			if v, ok := args["dedup"].(bool); ok {
				p.Dedup = v
			}
			if v, ok := args["denull"].(bool); ok {
				p.Denull = v
			}
			if v, ok := args["flatten"].(bool); ok {
				p.FlattenFields = v
			}
			if v, ok := args["include_source"].(bool); ok {
				p.IncludeSource = v
			}
			if v, ok := args["count"].(bool); ok {
				p.CountOnly = v
			}
			if v, ok := args["limit"].(float64); ok && v > 0 {
				p.Limit = int(v)
			}
			if v, ok := args["offset"].(float64); ok && v > 0 {
				p.Offset = int(v)
			}
			if v, ok := args["sample"].(float64); ok && v > 0 {
				p.Sample = int(v)
			}
			if v, ok := args["dedup_key"].(string); ok && v != "" {
				for _, k := range strings.Split(v, ",") {
					if t := strings.TrimSpace(k); t != "" {
						p.DedupKeys = append(p.DedupKeys, t)
					}
				}
			}
			if v, ok := args["keep_field"].(string); ok && v != "" {
				for _, k := range strings.Split(v, ",") {
					if t := strings.TrimSpace(k); t != "" {
						p.KeepFields = append(p.KeepFields, t)
					}
				}
			}
			if v, ok := args["drop_field"].(string); ok && v != "" {
				for _, k := range strings.Split(v, ",") {
					if t := strings.TrimSpace(k); t != "" {
						p.DropFields = append(p.DropFields, t)
					}
				}
			}
			if v, ok := args["filter"].(string); ok && v != "" {
				parts := strings.SplitN(v, ":", 2)
				if len(parts) == 2 {
					p.ExactFilters = append(p.ExactFilters, mcpFieldFilter{
						Field: strings.TrimSpace(parts[0]),
						Value: strings.ToLower(strings.TrimSpace(parts[1])),
					})
				}
			}
			if v, ok := args["filter_contains"].(string); ok && v != "" {
				parts := strings.SplitN(v, ":", 2)
				if len(parts) == 2 {
					p.ContainsFilters = append(p.ContainsFilters, mcpFieldFilter{
						Field: strings.TrimSpace(parts[0]),
						Value: strings.ToLower(strings.TrimSpace(parts[1])),
					})
				}
			}
			if v, ok := args["keywords"].(string); ok && v != "" {
				for _, k := range strings.Split(v, ",") {
					if t := strings.ToLower(strings.TrimSpace(k)); t != "" {
						p.Keywords = append(p.Keywords, t)
					}
				}
			}
			if v, ok := args["keywords_mode"].(string); ok {
				p.KeywordsAND = strings.ToLower(v) == "and"
			}
			if v, ok := args["sort"].(string); ok && v != "" {
				parts := strings.SplitN(v, ":", 2)
				p.SortField = strings.TrimSpace(parts[0])
				p.SortDir = "asc"
				if len(parts) == 2 && strings.ToLower(parts[1]) == "desc" {
					p.SortDir = "desc"
				}
			}

			if p.FlattenFields || p.Format == "csv" || p.Format == "tsv" || p.Format == "parquet" {
				for i, e := range entities {
					entities[i] = apiFlattenFull(e, "")
				}
			}

			entities = runQueryPipeline(entities, p)

			if p.CountOnly {
				b, _ := json.MarshalIndent(map[string]interface{}{
					"count":      len(entities),
					"dataset_id": datasetID,
					"version":    versionNumber,
				}, "", "  ")
				return mcp.NewToolResultText(string(b)), nil
			}

			out, err := formatEntities(entities, p.Format, dataName, p.FlattenFields)
			if err != nil {
				return mcp.NewToolResultError("format error: " + err.Error()), nil
			}

			go incrementAPIHit(datasetID)

			return mcp.NewToolResultText(out), nil
		},
	)

	// ── Tool: pull_for_edit ───────────────────────────────────────────────────
	s.AddTool(
		mcp.NewTool("pull_for_edit",
			mcp.WithDescription("Pull the full unfiltered entity list from a dataset version for AI processing. Can pull original or alt version. Use push_alt_version to save processed results back."),
			mcp.WithNumber("dataset_id", mcp.Required(), mcp.Description("The dataset ID to pull from")),
			mcp.WithString("version", mcp.Description("Version to pull: 'active' (default), 'latest', or 'v1', 'v2' etc")),
			mcp.WithBoolean("use_alt", mcp.Description("Set true to pull the alt version instead of the original")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			userID, ok := ctx.Value(mcpUserIDKey).(string)
			if !ok || userID == "" {
				return mcp.NewToolResultError("unauthorized"), nil
			}

			args, ok := req.Params.Arguments.(map[string]interface{})
			if !ok {
				return mcp.NewToolResultError("invalid arguments"), nil
			}

			datasetID := int64(args["dataset_id"].(float64))

			var ownerID string
			err := db.Get().QueryRow(`
				SELECT user_id FROM datasets WHERE dataset_id = ?
			`, datasetID).Scan(&ownerID)
			if err == sql.ErrNoRows {
				return mcp.NewToolResultError("dataset not found"), nil
			}
			if err != nil {
				return mcp.NewToolResultError("query failed: " + err.Error()), nil
			}
			if ownerID != userID {
				return mcp.NewToolResultError("access denied"), nil
			}

			version := "active"
			if v, ok := args["version"].(string); ok && v != "" {
				version = v
			}
			useAlt := false
			if v, ok := args["use_alt"].(bool); ok {
				useAlt = v
			}

			filePath, versionNumber, err := resolveVersionFile(datasetID, version, useAlt)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}

			entities, err := loadEntitiesFromFile(filePath)
			if err != nil {
				return mcp.NewToolResultError("load file: " + err.Error()), nil
			}

			result := map[string]interface{}{
				"dataset_id":   datasetID,
				"version":      versionNumber,
				"is_alt":       useAlt,
				"entity_count": len(entities),
				"entities":     entities,
			}
			b, _ := json.MarshalIndent(result, "", "  ")
			return mcp.NewToolResultText(string(b)), nil
		},
	)

	// ── Tool: push_alt_version ────────────────────────────────────────────────
	s.AddTool(
    mcp.NewTool("push_alt_version",
        mcp.WithDescription("Push AI-processed entities back as the alt version of a dataset. Always writes to alt, never overwrites the original. Use pull_for_edit first to get entities."),
        mcp.WithNumber("dataset_id", mcp.Required(), mcp.Description("The dataset ID to push to")),
        mcp.WithNumber("version", mcp.Required(), mcp.Description("The version number to attach the alt to")),
        mcp.WithString("entities", mcp.Required(), mcp.Description("JSON array of processed entities to save as alt")),
    ),
    func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
        userID, ok := ctx.Value(mcpUserIDKey).(string)
        if !ok || userID == "" {
            return mcp.NewToolResultError("unauthorized"), nil
        }

        args, ok := req.Params.Arguments.(map[string]interface{})
        if !ok {
            return mcp.NewToolResultError("invalid arguments"), nil
        }

        datasetID := int64(args["dataset_id"].(float64))
        versionNumber := int(args["version"].(float64))
        entitiesJSON, ok := args["entities"].(string)
        if !ok || strings.TrimSpace(entitiesJSON) == "" {
            return mcp.NewToolResultError("entities is required"), nil
        }

        var ownerID string
        var isFrozen int
        err := db.Get().QueryRow(`
            SELECT user_id, is_frozen FROM datasets WHERE dataset_id = ?
        `, datasetID).Scan(&ownerID, &isFrozen)
        if err == sql.ErrNoRows {
            return mcp.NewToolResultError("dataset not found"), nil
        }
        if err != nil {
            return mcp.NewToolResultError("query failed: " + err.Error()), nil
        }
        if ownerID != userID {
            return mcp.NewToolResultError("access denied"), nil
        }
        if isFrozen == 1 {
            return mcp.NewToolResultError("dataset is frozen — unfreeze it before pushing an alt version"), nil
        }

        // ── Processing check ──────────────────────────────────────────────────
        var processingCount int
        err = db.Get().QueryRow(`
            SELECT COUNT(*) FROM queue q
            JOIN datasets_url du ON du.dataset_url_id = q.dataset_url_id
            WHERE du.dataset_id = ?
            AND q.status NOT IN ('done', 'failed')
        `, datasetID).Scan(&processingCount)
        if err != nil {
            return mcp.NewToolResultError("processing check failed: " + err.Error()), nil
        }
        if processingCount > 0 {
            return mcp.NewToolResultError("dataset is currently processing — please wait until it completes before pushing an alt version"), nil
        }

        var entities []json.RawMessage
        if err := json.Unmarshal([]byte(entitiesJSON), &entities); err != nil {
            return mcp.NewToolResultError("invalid entities JSON: " + err.Error()), nil
        }
        if len(entities) == 0 {
            return mcp.NewToolResultError("entities array is empty"), nil
        }

        var filePath string
        err = db.Get().QueryRow(`
            SELECT file_path FROM dataset_versions
            WHERE dataset_id = ? AND version_number = ?
        `, datasetID, versionNumber).Scan(&filePath)
        if err == sql.ErrNoRows {
            return mcp.NewToolResultError(fmt.Sprintf("version %d not found", versionNumber)), nil
        }
        if err != nil {
            return mcp.NewToolResultError("query version: " + err.Error()), nil
        }

        dir := filePath[:strings.LastIndex(filePath, "/")+1]
        base := filePath[strings.LastIndex(filePath, "/")+1:]
        altFilePath := dir + "alt-" + base

        payload := map[string]interface{}{
            "dataset_id":   datasetID,
            "version":      versionNumber,
            "total":        len(entities),
            "generated_at": time.Now().UTC().Format(time.RFC3339Nano),
            "entities":     entities,
        }
        b, err := json.MarshalIndent(payload, "", "  ")
        if err != nil {
            return mcp.NewToolResultError("marshal error: " + err.Error()), nil
        }

        if err := storage.Write(altFilePath, b); err != nil {
            return mcp.NewToolResultError("write file error: " + err.Error()), nil
        }

        _, err = db.Get().Exec(`
            UPDATE dataset_versions SET alt_file_path = ?
            WHERE dataset_id = ? AND version_number = ?
        `, altFilePath, datasetID, versionNumber)
        if err != nil {
            return mcp.NewToolResultError("update version: " + err.Error()), nil
        }

        log.Printf("[mcp/push_alt] dataset_id=%d version=%d entities=%d path=%s",
            datasetID, versionNumber, len(entities), altFilePath)

        result := map[string]interface{}{
            "ok":           true,
            "dataset_id":   datasetID,
            "version":      versionNumber,
            "entity_count": len(entities),
            "alt_path":     altFilePath,
        }
        b, _ = json.MarshalIndent(result, "", "  ")
        return mcp.NewToolResultText(string(b)), nil
    },
)

	return s
}

// ---------------------------------------------------------------- context key --

type contextKey string

const mcpUserIDKey contextKey = "mcp_user_id"

// ---------------------------------------------------------------- SSE handler --

func mcpSSEHandler(mcpServer *server.MCPServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.URL.Path, "/mcp/")
		token = strings.TrimSuffix(token, "/sse")
		token = strings.Trim(token, "/")

		if token == "" {
			token = extractBearerToken(r)
		}

		userID, err := validateMCPToken(token)
		if err != nil {
			http.Error(w, "unauthorized: "+err.Error(), http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), mcpUserIDKey, userID)
		r = r.WithContext(ctx)

		log.Printf("[mcp/sse] user_id=%s connected", userID)

		sseServer := server.NewSSEServer(mcpServer,
    server.WithBaseURL("https://quorel.onrender.com"),
    server.WithDynamicBasePath(func(r *http.Request, sessionID string) string {
        return fmt.Sprintf("/mcp/%s", token)
    }),
)

		sseServer.SSEHandler().ServeHTTP(w, r)
	}
}

// ---------------------------------------------------------------- register routes --

func registerMCPRoutes(mcpServer *server.MCPServer) {
	sseServer := server.NewSSEServer(mcpServer,
		server.WithBaseURL("https://quorel.onrender.com"),
		server.WithDynamicBasePath(func(r *http.Request, sessionID string) string {
			token := strings.TrimPrefix(r.URL.Path, "/mcp/")
			token = strings.Split(token, "/")[0]
			return fmt.Sprintf("/mcp/%s", token)
		}),
	)

	http.HandleFunc("/mcp/token", corsMiddleware(authMiddleware(mcpTokenGenerateHandler)))
	http.HandleFunc("/mcp/token/revoke", corsMiddleware(authMiddleware(mcpTokenRevokeHandler)))
	http.HandleFunc("/mcp/token/view", corsMiddleware(authMiddleware(mcpTokenViewHandler)))
	http.HandleFunc("/mcp/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/mcp/"), "/")
		if len(parts) == 0 {
			http.Error(w, "invalid path", http.StatusBadRequest)
			return
		}
		token := parts[0]
		userID, err := validateMCPToken(token)
		if err != nil {
			http.Error(w, "unauthorized: "+err.Error(), http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), mcpUserIDKey, userID)
		r = r.WithContext(ctx)
		log.Printf("[mcp/sse] user_id=%s connected", userID)

		if len(parts) > 1 && parts[1] == "message" {
			sseServer.MessageHandler().ServeHTTP(w, r)
		} else {
			sseServer.SSEHandler().ServeHTTP(w, r)
		}
	})
}
