package format

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/var-raphael/vexaro-engine/clean"
)

type FieldType string

const (
	FieldTypeText   FieldType = "text"
	FieldTypeNumber FieldType = "number"
	FieldTypeLink   FieldType = "link"
	FieldTypeImage  FieldType = "image"
	FieldTypeFile   FieldType = "file"
)

type FieldDef struct {
	Type        FieldType `json:"type"`
	Description string    `json:"description,omitempty"`
	Formats     []string  `json:"format,omitempty"`
}

type Schema struct {
	Fields map[string]*FieldDef
}

func ParseSchema(raw map[string]json.RawMessage) (*Schema, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("schema must have at least one field")
	}

	schema := &Schema{Fields: make(map[string]*FieldDef, len(raw))}

	for name, v := range raw {
		def, err := parseField(name, v)
		if err != nil {
			return nil, err
		}
		schema.Fields[name] = def
	}

	return schema, nil
}

func parseField(name string, raw json.RawMessage) (*FieldDef, error) {
	var simple string
	if err := json.Unmarshal(raw, &simple); err == nil {
		ft, err := validateType(name, simple)
		if err != nil {
			return nil, err
		}
		return &FieldDef{Type: ft}, nil
	}

	var rich struct {
		Type        string   `json:"type"`
		Description string   `json:"description"`
		Formats     []string `json:"format"`
	}
	if err := json.Unmarshal(raw, &rich); err != nil {
		return nil, fmt.Errorf("field %q: must be a type string or object with 'type' key", name)
	}

	ft, err := validateType(name, rich.Type)
	if err != nil {
		return nil, err
	}

	def := &FieldDef{
		Type:        ft,
		Description: rich.Description,
	}

	if len(rich.Formats) > 0 {
		if ft != FieldTypeFile {
			return nil, fmt.Errorf("field %q: 'format' restriction is only valid for file type", name)
		}
		for i, ext := range rich.Formats {
			if !strings.HasPrefix(ext, ".") {
				rich.Formats[i] = "." + ext
			}
			rich.Formats[i] = strings.ToLower(rich.Formats[i])
		}
		def.Formats = rich.Formats
	}

	return def, nil
}

func validateType(name, t string) (FieldType, error) {
	switch FieldType(t) {
	case FieldTypeText, FieldTypeNumber, FieldTypeLink, FieldTypeImage, FieldTypeFile:
		return FieldType(t), nil
	default:
		return "", fmt.Errorf("field %q: unknown type %q (valid: text, number, link, image, file)", name, t)
	}
}

func FilterBySchema(cleaned *clean.CleanedData, schema *Schema) map[string]interface{} {
    out := map[string]interface{}{}

    // always carry source_url through
    if cleaned.URL != "" {
        out["source_url"] = cleaned.URL
    }

	var hasText, hasNumber, hasLink, hasImage, hasFile bool
	for _, def := range schema.Fields {
		switch def.Type {
		case FieldTypeText:
			hasText = true
		case FieldTypeNumber:
			hasNumber = true
		case FieldTypeLink:
			hasLink = true
		case FieldTypeImage:
			hasImage = true
		case FieldTypeFile:
			hasFile = true
		}
	}

	if hasText || hasNumber {
		if strings.TrimSpace(cleaned.Content) != "" {
			out["content"] = cleaned.Content
		}
	}

	if hasLink && len(cleaned.Links) > 0 {
		out["links"] = cleaned.Links
	}

	if hasImage && len(cleaned.Images) > 0 {
		out["images"] = cleaned.Images
	}

	if hasFile {
		for _, def := range schema.Fields {
			if def.Type == FieldTypeFile {
				matched := filterFiles(cleaned.Downloads, def.Formats)
				if len(matched) > 0 {
					out["files"] = matched
				}
				break
			}
		}
	}

	return out
}

func filterFiles(files []string, formats []string) []string {
	if len(files) == 0 {
		return nil
	}
	if len(formats) == 0 {
		return files
	}

	var matched []string
	for _, f := range files {
		lower := strings.ToLower(f)
		for _, ext := range formats {
			if strings.HasSuffix(lower, ext) || strings.Contains(lower, ext+"?") {
				matched = append(matched, f)
				break
			}
		}
	}
	return matched
}