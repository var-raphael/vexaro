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
)

type FieldDef struct {
	Type        FieldType `json:"type"`
	Description string    `json:"description,omitempty"`
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
		Type        string `json:"type"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal(raw, &rich); err != nil {
		return nil, fmt.Errorf("field %q: must be a type string or object with 'type' key", name)
	}

	ft, err := validateType(name, rich.Type)
	if err != nil {
		return nil, err
	}

	return &FieldDef{
		Type:        ft,
		Description: rich.Description,
	}, nil
}

func validateType(name, t string) (FieldType, error) {
	switch FieldType(t) {
	case FieldTypeText, FieldTypeNumber:
		return FieldType(t), nil
	default:
		return FieldTypeText, nil
	}
}

func FilterBySchema(cleaned *clean.CleanedData, schema *Schema) map[string]interface{} {
	out := map[string]interface{}{}

	if cleaned.URL != "" {
		out["source_url"] = cleaned.URL
	}

	var hasText, hasNumber bool
	for _, def := range schema.Fields {
		switch def.Type {
		case FieldTypeText:
			hasText = true
		case FieldTypeNumber:
			hasNumber = true
		}
	}

	if hasText || hasNumber {
		if strings.TrimSpace(cleaned.Content) != "" {
			out["content"] = cleaned.Content
		}
	}

	return out
}