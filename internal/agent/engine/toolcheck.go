package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// validateToolCall judges one tool call of an answer against the tool that was
// offered: its type is "function", its name is the offered tool's, and its
// arguments are a JSON object (not null, not another JSON value, not
// malformed) that conforms to the tool's parameter schema.
//
// Only the subset of JSON Schema the offered tools use is understood: an
// object schema whose "properties" declare a "type" of string, number,
// integer, boolean, object or array, and whose "required" names properties
// that must be present. A required property must be present; every property
// that is present and listed must have its declared type (null is none of
// them); properties the schema does not list are not constrained. A schema
// outside the subset is an error, never a pass. This is the agent's own
// minimal validator, not a JSON Schema implementation (standard library only).
func validateToolCall(offered tool, callType, name, arguments string) error {
	if callType != "function" {
		return reasoned(agentapi.ReasonWrongTool, "the call's type is %q, not \"function\"", callType)
	}
	if name != offered.Function.Name {
		return reasoned(agentapi.ReasonWrongTool, "the answer calls %q, not %q", name, offered.Function.Name)
	}
	schema, err := parseSchema(offered.Function.Parameters)
	if err != nil {
		return err
	}
	args, err := parseArguments(arguments)
	if err != nil {
		return reasoned(agentapi.ReasonBadToolArguments, "%v", err)
	}
	for _, key := range schema.Required {
		if _, ok := args[key]; !ok {
			return reasoned(agentapi.ReasonBadToolArguments, "the arguments miss the required %q", key)
		}
	}
	keys := make([]string, 0, len(schema.Properties))
	for key := range schema.Properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		v, ok := args[key]
		if !ok {
			continue
		}
		if want := schema.Properties[key].Type; !hasJSONType(v, want) {
			return reasoned(agentapi.ReasonBadToolArguments, "the argument %q is %s, not %s", key, describeJSON(v), want)
		}
	}
	return nil
}

// parameterSchema is the subset of a tool's parameter schema validateToolCall
// reads.
type parameterSchema struct {
	Type       string                    `json:"type"`
	Properties map[string]propertySchema `json:"properties"`
	Required   []string                  `json:"required"`
}

type propertySchema struct {
	Type string `json:"type"`
}

var schemaTypes = map[string]bool{"string": true, "number": true, "integer": true, "boolean": true, "object": true, "array": true}

func parseSchema(raw json.RawMessage) (parameterSchema, error) {
	var s parameterSchema
	if len(raw) == 0 {
		return s, nil // a tool with no declared parameters: any object
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return s, fmt.Errorf("the offered tool's schema is not understood: %v", err)
	}
	if s.Type != "" && s.Type != "object" {
		return s, fmt.Errorf("the offered tool's schema has type %q: only object is understood", s.Type)
	}
	for key, p := range s.Properties {
		if !schemaTypes[p.Type] {
			return s, fmt.Errorf("the offered tool's property %q has type %q: not understood", key, p.Type)
		}
	}
	return s, nil
}

// parseArguments reads the arguments of a call as a JSON object. Numbers stay
// json.Number, so an integer is told from a fraction.
func parseArguments(arguments string) (map[string]any, error) {
	if strings.TrimSpace(arguments) == "" {
		return nil, errors.New("the tool call has no arguments")
	}
	dec := json.NewDecoder(strings.NewReader(arguments))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("the tool call's arguments are not valid JSON: %v", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("the tool call's arguments are not valid JSON: data after the value")
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("the tool call's arguments are %s, not a JSON object", describeJSON(v))
	}
	return obj, nil
}

// hasJSONType reports whether v, decoded with UseNumber, has the JSON Schema
// type. An integer is a number with no fractional part, as in JSON Schema.
func hasJSONType(v any, typ string) bool {
	switch typ {
	case "string":
		_, ok := v.(string)
		return ok
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "object":
		_, ok := v.(map[string]any)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	case "number", "integer":
		n, ok := v.(json.Number)
		if !ok {
			return false
		}
		f, err := n.Float64()
		if err != nil || math.IsInf(f, 0) {
			return false
		}
		return typ == "number" || f == math.Trunc(f)
	}
	return false
}

// describeJSON names the JSON type of a decoded value.
func describeJSON(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case string:
		return "a string"
	case bool:
		return "a boolean"
	case json.Number:
		return "a number"
	case map[string]any:
		return "an object"
	case []any:
		return "an array"
	}
	return fmt.Sprintf("a %T", v)
}
