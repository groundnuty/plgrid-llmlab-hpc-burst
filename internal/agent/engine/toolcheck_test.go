package engine

import (
	"encoding/json"
	"strings"
	"testing"
)

// The validator's cases are written by hand: each is a call an engine could
// make, and whether it is a valid call of the weather tool.
func TestValidateToolCallOfTheWeatherTool(t *testing.T) {
	for _, tc := range []struct {
		name, typ, fn, args string
		wantErr             string // "" for a valid call, else a part of the message
	}{
		{"a valid call", "function", "get_weather", `{"city":"Krakow"}`, ""},
		{"a valid call with extra argument", "function", "get_weather", `{"city":"Krakow","units":"C"}`, ""},
		{"a valid call, spaced", "function", "get_weather", ` { "city" : "Krakow" } `, ""},
		{"null arguments", "function", "get_weather", `null`, "null, not a JSON object"},
		{"empty object", "function", "get_weather", `{}`, `miss the required "city"`},
		{"wrong type", "function", "get_weather", `{"city":42}`, `"city" is a number, not string`},
		{"null city", "function", "get_weather", `{"city":null}`, `"city" is null, not string`},
		{"malformed JSON", "function", "get_weather", `{"city": `, "not valid JSON"},
		{"no arguments", "function", "get_weather", ``, "no arguments"},
		{"blank arguments", "function", "get_weather", "  \n", "no arguments"},
		{"an array", "function", "get_weather", `["Krakow"]`, "an array, not a JSON object"},
		{"a string", "function", "get_weather", `"Krakow"`, "a string, not a JSON object"},
		{"data after the object", "function", "get_weather", `{"city":"Krakow"} {}`, "data after the value"},
		{"another tool", "function", "get_time", `{"city":"Krakow"}`, `calls "get_time", not "get_weather"`},
		{"not a function", "tool", "get_weather", `{"city":"Krakow"}`, `type is "tool"`},
		{"no type", "", "get_weather", `{"city":"Krakow"}`, `type is ""`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateToolCall(weatherTool, tc.typ, tc.fn, tc.args)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Errorf("rejected: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Errorf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

// The subset of JSON Schema: each declared type, and the required list.
func TestValidateToolCallSchemaSubset(t *testing.T) {
	multi := tool{Type: "function", Function: toolFunction{Name: "f", Parameters: json.RawMessage(`{
		"type": "object",
		"properties": {
			"s": {"type": "string"}, "n": {"type": "number"}, "i": {"type": "integer"},
			"b": {"type": "boolean"}, "o": {"type": "object"}, "a": {"type": "array"}
		},
		"required": ["s", "i"]
	}`)}}
	for _, tc := range []struct {
		args    string
		wantErr string
	}{
		{`{"s":"x","i":3}`, ""},
		{`{"s":"x","i":3,"n":1.5,"b":true,"o":{"k":1},"a":[1,"two"]}`, ""},
		{`{"s":"x","i":3.0}`, ""}, // 3.0 is an integer in JSON Schema
		{`{"s":"x","i":1e2}`, ""}, // so is 1e2
		{`{"s":"x","i":3,"n":2}`, ""},
		{`{"s":"x"}`, `miss the required "i"`},
		{`{"i":3}`, `miss the required "s"`},
		{`{"s":"x","i":3.5}`, `"i" is a number, not integer`},
		{`{"s":"x","i":"3"}`, `"i" is a string, not integer`},
		{`{"s":"x","i":3,"n":"1"}`, `"n" is a string, not number`},
		{`{"s":"x","i":3,"b":1}`, `"b" is a number, not boolean`},
		{`{"s":"x","i":3,"b":"true"}`, `"b" is a string, not boolean`},
		{`{"s":"x","i":3,"o":[]}`, `"o" is an array, not object`},
		{`{"s":"x","i":3,"a":{}}`, `"a" is an object, not array`},
		{`{"s":"x","i":3,"a":null}`, `"a" is null, not array`},
		{`{"s":true,"i":3}`, `"s" is a boolean, not string`},
	} {
		err := validateToolCall(multi, "function", "f", tc.args)
		switch {
		case tc.wantErr == "" && err != nil:
			t.Errorf("%s: rejected: %v", tc.args, err)
		case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
			t.Errorf("%s: error = %v, want one containing %q", tc.args, err, tc.wantErr)
		}
	}
}

// A schema outside the subset is an error, never a pass, and a tool with no
// parameters takes any object.
func TestValidateToolCallSchemasOutsideTheSubset(t *testing.T) {
	with := func(schema string) tool {
		return tool{Type: "function", Function: toolFunction{Name: "f", Parameters: json.RawMessage(schema)}}
	}
	for schema, wantErr := range map[string]string{
		`{"type":"array"}`:                                     `has type "array"`,
		`{"type":"object","properties":{"x":{}}}`:              `property "x" has type ""`,
		`{"type":"object","properties":{"x":{"type":"null"}}}`: `property "x" has type "null"`,
		`{"type":"object","properties":3}`:                     "not understood",
	} {
		if err := validateToolCall(with(schema), "function", "f", `{"x":1}`); err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Errorf("schema %s: error = %v, want one containing %q", schema, err, wantErr)
		}
	}
	noParams := tool{Type: "function", Function: toolFunction{Name: "f"}}
	if err := validateToolCall(noParams, "function", "f", `{"anything":[1]}`); err != nil {
		t.Errorf("a tool with no parameters rejected an object: %v", err)
	}
	if err := validateToolCall(noParams, "function", "f", `null`); err == nil {
		t.Error("a tool with no parameters accepted null")
	}
}

// The non-streamed check uses the validator: the answer must call the tool.
func TestWellFormedToolCall(t *testing.T) {
	answer := func(calls string) chatResponse {
		var r chatResponse
		if err := json.Unmarshal([]byte(`{"choices":[{"message":{"content":null,"tool_calls":`+calls+`},"finish_reason":"tool_calls"}]}`), &r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	good := `{"id":"c","type":"function","function":{"name":"get_weather","arguments":"{\"city\": \"Krakow\"}"}}`
	for name, tc := range map[string]struct {
		calls   string
		wantErr string
	}{
		"valid":           {`[` + good + `]`, ""},
		"no calls":        {`[]`, "calls no tool"},
		"null arguments":  {`[{"type":"function","function":{"name":"get_weather","arguments":"null"}}]`, "null, not a JSON object"},
		"empty object":    {`[{"type":"function","function":{"name":"get_weather","arguments":"{}"}}]`, "required"},
		"wrong type":      {`[{"type":"function","function":{"name":"get_weather","arguments":"{\"city\":42}"}}]`, "not string"},
		"no arguments":    {`[{"type":"function","function":{"name":"get_weather"}}]`, "no arguments"},
		"second call bad": {`[` + good + `,{"type":"function","function":{"name":"get_weather","arguments":"{"}}]`, "not valid JSON"},
	} {
		err := wellFormedToolCall(answer(tc.calls))
		switch {
		case tc.wantErr == "" && err != nil:
			t.Errorf("%s: rejected: %v", name, err)
		case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
			t.Errorf("%s: error = %v, want one containing %q", name, err, tc.wantErr)
		}
	}
	if err := wellFormedToolCall(chatResponse{}); err == nil {
		t.Error("an answer with no choice passed")
	}
}
