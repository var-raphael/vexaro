package download

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/xuri/excelize/v2"
	"github.com/var-raphael/vexaro-engine/storage"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

func collectColumns(entities []map[string]interface{}) []string {
	seen := make(map[string]bool)
	var cols []string
	for _, e := range entities {
		for k := range e {
			if k == "_source" {
				continue
			}
			if !seen[k] {
				seen[k] = true
				cols = append(cols, k)
			}
		}
	}
	sort.Strings(cols)
	return cols
}

func cellString(val interface{}) string {
	if val == nil {
		return ""
	}
	switch v := val.(type) {
	case string:
		return v
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%g", v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		b, _ := json.Marshal(val)
		return string(b)
	}
}

func readEntitiesFromFile(filePath string) ([]map[string]interface{}, error) {
	b, err := storage.Read(filePath)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	var raw struct {
		Entities []json.RawMessage `json:"entities"`
		Posts    []json.RawMessage `json:"posts"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("parse file: %w", err)
	}

	items := raw.Entities
	if len(items) == 0 {
		items = raw.Posts
	}

	out := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		var m map[string]interface{}
		if err := json.Unmarshal(item, &m); err != nil {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

// flattenEntity recursively flattens nested maps and arrays into a single-level
// map with underscore-separated keys.
// Arrays of primitives are JSON-encoded into a single string cell.
// Arrays of objects are expanded with indexed keys: reviews_0_rating, reviews_1_rating, etc.
func flattenEntity(m map[string]interface{}, prefix string) map[string]interface{} {
	out := make(map[string]interface{})
	for k, v := range m {
		if k == "_source" {
			continue
		}
		key := k
		if prefix != "" {
			key = prefix + "_" + k
		}
		switch val := v.(type) {
		case map[string]interface{}:
			for fk, fv := range flattenEntity(val, key) {
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
						for fk, fv := range flattenEntity(itemVal, indexedKey) {
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

func flattenEntities(entities []map[string]interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, len(entities))
	for i, e := range entities {
		out[i] = flattenEntity(e, "")
	}
	return out
}

// ── Format encoders ───────────────────────────────────────────────────────────

func encodeJSON(entities []map[string]interface{}) ([]byte, error) {
	return json.MarshalIndent(entities, "", "  ")
}

func encodeJSONL(entities []map[string]interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, e := range entities {
		if err := enc.Encode(e); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func encodeCSV(entities []map[string]interface{}, delimiter rune) ([]byte, error) {
	flat := flattenEntities(entities)
	cols := collectColumns(flat)
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.Comma = delimiter

	if err := w.Write(cols); err != nil {
		return nil, err
	}
	for _, e := range flat {
		row := make([]string, len(cols))
		for i, col := range cols {
			row[i] = cellString(e[col])
		}
		if err := w.Write(row); err != nil {
			return nil, err
		}
	}
	w.Flush()
	return buf.Bytes(), w.Error()
}

func encodeXML(entities []map[string]interface{}, datasetName string) ([]byte, error) {
	type Field struct {
		XMLName xml.Name
		Value   string `xml:",chardata"`
	}
	type Entity struct {
		XMLName xml.Name `xml:"entity"`
		Fields  []Field
	}
	type Root struct {
		XMLName  xml.Name `xml:"dataset"`
		Name     string   `xml:"name,attr"`
		Entities []Entity `xml:"entities>entity"`
	}

	cols := collectColumns(entities)
	root := Root{Name: datasetName}

	for _, e := range entities {
		var fields []Field
		for _, col := range cols {
			fields = append(fields, Field{
				XMLName: xml.Name{Local: col},
				Value:   cellString(e[col]),
			})
		}
		root.Entities = append(root.Entities, Entity{Fields: fields})
	}

	out, err := xml.MarshalIndent(root, "", "  ")
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), out...), nil
}

func encodeExcel(entities []map[string]interface{}, datasetName string) ([]byte, error) {
	flat := flattenEntities(entities)
	cols := collectColumns(flat)
	f := excelize.NewFile()
	defer f.Close()

	sheet := "Data"
	f.SetSheetName("Sheet1", sheet)

	headerStyle, _ := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true, Color: "FFFFFF"},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"1E3A5F"}, Pattern: 1},
		Border: []excelize.Border{
			{Type: "bottom", Color: "FFFFFF", Style: 2},
		},
	})

	for i, col := range cols {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		f.SetCellValue(sheet, cell, col)
		f.SetCellStyle(sheet, cell, cell, headerStyle)
	}

	for rowIdx, e := range flat {
		for colIdx, col := range cols {
			cell, _ := excelize.CoordinatesToCellName(colIdx+1, rowIdx+2)
			val := e[col]
			switch v := val.(type) {
			case float64:
				f.SetCellValue(sheet, cell, v)
			case bool:
				f.SetCellValue(sheet, cell, v)
			case nil:
				f.SetCellValue(sheet, cell, "")
			default:
				f.SetCellValue(sheet, cell, cellString(val))
			}
		}
	}

	for i, col := range cols {
		colLetter, _ := excelize.ColumnNumberToName(i + 1)
		maxLen := len(col)
		for _, e := range flat {
			l := len(cellString(e[col]))
			if l > maxLen {
				maxLen = l
			}
		}
		width := float64(maxLen) + 4
		if width > 60 {
			width = 60
		}
		f.SetColWidth(sheet, colLetter, colLetter, width)
	}

	f.SetPanes(sheet, &excelize.Panes{
		Freeze:      true,
		Split:       false,
		XSplit:      0,
		YSplit:      1,
		TopLeftCell: "A2",
		ActivePane:  "bottomLeft",
	})

	var buf bytes.Buffer
	if err := f.Write(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeParquet(entities []map[string]interface{}) ([]byte, error) {
	if len(entities) == 0 {
		return nil, fmt.Errorf("no entities to encode")
	}

	flat := flattenEntities(entities)
	cols := collectColumns(flat)

	groupFields := make(parquet.Group)
	for _, col := range cols {
		groupFields[col] = parquet.Leaf(parquet.String().Type())
	}
	schema := parquet.NewSchema("dataset", groupFields)

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[map[string]string](&buf, schema)

	for _, e := range flat {
		row := make(map[string]string, len(cols))
		for _, col := range cols {
			row[col] = cellString(e[col])
		}
		if _, err := writer.Write([]map[string]string{row}); err != nil {
			return nil, fmt.Errorf("write row: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close writer: %w", err)
	}
	return buf.Bytes(), nil
}

// ── ZIP builder ───────────────────────────────────────────────────────────────

func buildZip(files map[string][]byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, data := range files {
		fw, err := zw.Create(name)
		if err != nil {
			return nil, err
		}
		if _, err := fw.Write(data); err != nil {
			return nil, err
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ── Content type + extension ──────────────────────────────────────────────────

func formatMeta(format string) (contentType, ext string) {
	switch strings.ToLower(format) {
	case "json":
		return "application/json", "json"
	case "jsonl":
		return "application/x-ndjson", "jsonl"
	case "csv":
		return "text/csv", "csv"
	case "tsv":
		return "text/tab-separated-values", "tsv"
	case "xml":
		return "application/xml", "xml"
	case "excel":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "xlsx"
	case "parquet":
		return "application/octet-stream", "parquet"
	default:
		return "application/json", "json"
	}
}

// ── Encode dispatcher ─────────────────────────────────────────────────────────

func encodeEntities(entities []map[string]interface{}, format, datasetName string) ([]byte, error) {
	switch strings.ToLower(format) {
	case "json":
		return encodeJSON(entities)
	case "jsonl":
		return encodeJSONL(entities)
	case "csv":
		return encodeCSV(entities, ',')
	case "tsv":
		return encodeCSV(entities, '\t')
	case "xml":
		return encodeXML(entities, datasetName)
	case "excel":
		return encodeExcel(entities, datasetName)
	case "parquet":
		return encodeParquet(entities)
	default:
		return encodeJSON(entities)
	}
}

// ── Handler ───────────────────────────────────────────────────────────────────

func Handler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

		format := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
		if format == "" {
			format = "json"
		}

		validFormats := map[string]bool{
			"json": true, "jsonl": true, "csv": true,
			"tsv": true, "xml": true, "excel": true, "parquet": true,
		}
		if !validFormats[format] {
			http.Error(w, "invalid format — supported: json, jsonl, csv, tsv, xml, excel, parquet", http.StatusBadRequest)
			return
		}

		source := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("source")))
		if source == "" {
			source = "original"
		}
		if source != "original" && source != "alt" && source != "both" {
			http.Error(w, "invalid source — use: original, alt, both", http.StatusBadRequest)
			return
		}

		var datasetName string
		err = db.QueryRow(`
			SELECT COALESCE(NULLIF(TRIM(alias), ''), data_name)
			FROM datasets WHERE dataset_id = ?
		`, datasetID).Scan(&datasetName)
		if err != nil {
			http.Error(w, "dataset not found", http.StatusNotFound)
			return
		}

		var filePath string
		var altFilePath *string
		err = db.QueryRow(`
			SELECT file_path, alt_file_path
			FROM dataset_versions
			WHERE dataset_id = ? AND version_number = ?
		`, datasetID, versionNum).Scan(&filePath, &altFilePath)
		if err != nil {
			http.Error(w, "version not found", http.StatusNotFound)
			return
		}

		if (source == "alt" || source == "both") && (altFilePath == nil || *altFilePath == "") {
			http.Error(w, "no alternate exists for this version", http.StatusNotFound)
			return
		}

		safeName := strings.ReplaceAll(strings.ToLower(datasetName), " ", "-")
		safeName = strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
				return r
			}
			return -1
		}, safeName)
		if safeName == "" {
			safeName = fmt.Sprintf("dataset-%d", datasetID)
		}

		baseName := fmt.Sprintf("%s-v%d", safeName, versionNum)
		_, ext := formatMeta(format)

		// ── Both — build ZIP ──────────────────────────────────────────────
		if source == "both" {
			origEntities, err := readEntitiesFromFile(filePath)
			if err != nil {
				log.Printf("[download] read original dataset_id=%d: %v", datasetID, err)
				http.Error(w, "failed to read original file", http.StatusInternalServerError)
				return
			}
			altEntities, err := readEntitiesFromFile(*altFilePath)
			if err != nil {
				log.Printf("[download] read alt dataset_id=%d: %v", datasetID, err)
				http.Error(w, "failed to read alt file", http.StatusInternalServerError)
				return
			}

			origBytes, err := encodeEntities(origEntities, format, datasetName)
			if err != nil {
				http.Error(w, "encode original: "+err.Error(), http.StatusInternalServerError)
				return
			}
			altBytes, err := encodeEntities(altEntities, format, datasetName)
			if err != nil {
				http.Error(w, "encode alt: "+err.Error(), http.StatusInternalServerError)
				return
			}

			zipBytes, err := buildZip(map[string][]byte{
				fmt.Sprintf("%s-original.%s", baseName, ext): origBytes,
				fmt.Sprintf("%s-alt.%s", baseName, ext):      altBytes,
			})
			if err != nil {
				http.Error(w, "build zip: "+err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/zip")
			w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.zip"`, baseName))
			w.Header().Set("Content-Length", strconv.Itoa(len(zipBytes)))
			w.Write(zipBytes)

			log.Printf("[download] zip dataset_id=%d version=%d format=%s orig=%d alt=%d entities",
				datasetID, versionNum, format, len(origEntities), len(altEntities))
			return
		}

		// ── Single file ───────────────────────────────────────────────────
		targetPath := filePath
		fileLabel := "original"
		if source == "alt" {
			targetPath = *altFilePath
			fileLabel = "alt"
		}

		entities, err := readEntitiesFromFile(targetPath)
		if err != nil {
			log.Printf("[download] read %s dataset_id=%d: %v", fileLabel, datasetID, err)
			http.Error(w, "failed to read file", http.StatusInternalServerError)
			return
		}

		encoded, err := encodeEntities(entities, format, datasetName)
		if err != nil {
			log.Printf("[download] encode %s dataset_id=%d format=%s: %v", fileLabel, datasetID, format, err)
			http.Error(w, "encode failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		contentType, _ := formatMeta(format)
		filename := fmt.Sprintf("%s-%s.%s", baseName, fileLabel, ext)
		if source == "original" {
			filename = fmt.Sprintf("%s.%s", baseName, ext)
		}

		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
		w.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
		w.Write(encoded)

		log.Printf("[download] served dataset_id=%d version=%d format=%s source=%s entities=%d",
			datasetID, versionNum, format, source, len(entities))
	}
}