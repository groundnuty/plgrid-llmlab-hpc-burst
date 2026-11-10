package rawjson

import (
	"encoding/json"
	"reflect"
	"testing"
)

// bs is one backslash. Escape fixtures are built from it so this file holds no escape sequence
// that an editor could decode.
var bs = string(rune(92))

func mustParse(t *testing.T, s string) *Node {
	t.Helper()
	n, err := Parse([]byte(s))
	if err != nil {
		t.Fatalf("Parse(%s): %v", s, err)
	}
	return n
}

func TestParseKeepsRawSpans(t *testing.T) {
	in := ` { "b" : 1.50 , "a":[ "x` + bs + `u00e9" , true,null ] } `
	n := mustParse(t, in)
	if got := string(n.Raw); got != `{ "b" : 1.50 , "a":[ "x`+bs+`u00e9" , true,null ] }` {
		t.Fatalf("root raw = %q", got)
	}
	if n.Members[0].Key != "b" || string(n.Members[0].Value.Raw) != "1.50" {
		t.Fatalf("first member = %q %q", n.Members[0].Key, n.Members[0].Value.Raw)
	}
	if got := string(Lookup(n, "a[0]").Raw); got != `"x`+bs+`u00e9"` {
		t.Fatalf("a[0] raw = %q", got)
	}
	if Lookup(n, "a[2]").Kind != Null || Lookup(n, "a[3]") != nil || Lookup(n, "c") != nil {
		t.Fatal("Lookup on a null, past the end, or a missing key")
	}
}

func TestParseRefusesInvalid(t *testing.T) {
	for _, s := range []string{``, `{`, `{"a":}`, `[1,]`, `{"a":1}x`, `"` + bs + `x"`} {
		if _, err := Parse([]byte(s)); err == nil {
			t.Errorf("Parse(%q) accepted", s)
		}
	}
}

func TestIdenticalBytesHaveNoDiff(t *testing.T) {
	s := `{"model":"m","messages":[{"role":"user","content":"hi"}],"seed":9223372036854775807}`
	if d := Compare(mustParse(t, s), mustParse(t, s)); len(d) != 0 {
		t.Fatalf("diffs on identical input: %+v", d)
	}
}

func TestOrderAndWhitespaceAreNotDiffs(t *testing.T) {
	a := `{"a":1,"b":{"c":[1,2],"d":"x"}}`
	b := "{ \"b\": {\"d\":\"x\", \"c\":[1, 2]},\n \"a\":1 }"
	if d := Compare(mustParse(t, a), mustParse(t, b)); len(d) != 0 {
		t.Fatalf("diffs from member order and whitespace: %+v", d)
	}
}

// The verdicts below are written out by hand from the definitions in compare.go's Verdict comments.
func TestVerdicts(t *testing.T) {
	cases := []struct {
		name, sent, recv string
		want             []Diff
	}{
		{"dropped nested field", `{"tools":[{"function":{"name":"f","strict":true}}]}`, `{"tools":[{"function":{"name":"f"}}]}`,
			[]Diff{{Path: "tools[0].function.strict", Verdict: Dropped, Sent: "true"}}},
		{"injected default", `{"model":"m"}`, `{"model":"m","stream":false}`,
			[]Diff{{Path: "stream", Verdict: Injected, Received: "false"}}},
		{"injected null", `{"prompt":"p"}`, `{"prompt":"p","max_tokens":null}`,
			[]Diff{{Path: "max_tokens", Verdict: Injected, Received: "null"}}},
		{"array content to string", `{"content":[{"type":"text","text":"s"}]}`, `{"content":""}`,
			[]Diff{{Path: "content", Verdict: Retyped, Sent: `[{"type":"text","text":"s"}]`, Received: `""`}}},
		{"int to float literal", `{"temperature":1}`, `{"temperature":1.0}`,
			[]Diff{{Path: "temperature", Verdict: Retyped, Sent: "1", Received: "1.0"}}},
		{"float spelled otherwise", `{"top_p":1e-3}`, `{"top_p":0.001}`,
			[]Diff{{Path: "top_p", Verdict: Reencoded, Sent: "1e-3", Received: "0.001"}}},
		{"float32 widening", `{"top_p":0.9}`, `{"top_p":0.8999999761581421}`,
			[]Diff{{Path: "top_p", Verdict: Changed, Sent: "0.9", Received: "0.8999999761581421"}}},
		{"int64 through float64", `{"seed":9223372036854775807}`, `{"seed":9223372036854775808}`,
			[]Diff{{Path: "seed", Verdict: Changed, Sent: "9223372036854775807", Received: "9223372036854775808"}}},
		{"escape re-encoded", `{"s":"caf` + bs + `u00e9"}`, `{"s":"café"}`,
			[]Diff{{Path: "s", Verdict: Reencoded, Sent: `"caf` + bs + `u00e9"`, Received: `"café"`}}},
		{"string value changed", `{"s":"a"}`, `{"s":"b"}`,
			[]Diff{{Path: "s", Verdict: Changed, Sent: `"a"`, Received: `"b"`}}},
		{"bool changed", `{"b":true}`, `{"b":false}`,
			[]Diff{{Path: "b", Verdict: Changed, Sent: "true", Received: "false"}}},
		{"renamed alias", `{"role":"assistant","reasoning_content":"r"}`, `{"role":"assistant","reasoning":"r"}`,
			[]Diff{{Path: "reasoning_content", Verdict: Renamed, Sent: `"r"`, Received: `"r"`, To: "reasoning"}}},
		{"dropped whole object", `{"m":[{"role":"user","x_ext":{"a":1}}]}`, `{"m":[{"role":"user"}]}`,
			[]Diff{{Path: "m[0].x_ext", Verdict: Dropped, Sent: `{"a":1}`}}},
		{"dropped array element", `{"stop":["a","b"]}`, `{"stop":["a"]}`,
			[]Diff{{Path: "stop[1]", Verdict: Dropped, Sent: `"b"`}}},
		{"duplicate removed, last kept", `{"a":1,"a":2}`, `{"a":2}`,
			[]Diff{{Path: "a", Verdict: DuplicateRemoved, Sent: `"a":1,"a":2`, Received: `"a":2`}}},
		{"duplicate removed, first kept", `{"a":1,"a":2}`, `{"a":1}`,
			[]Diff{{Path: "a", Verdict: DuplicateRemoved, Sent: `"a":1,"a":2`, Received: `"a":1`},
				{Path: "a", Verdict: Changed, Sent: "2", Received: "1"}}},
		{"duplicate added", `{"a":2}`, `{"a":1,"a":2}`,
			[]Diff{{Path: "a", Verdict: DuplicateAdded, Sent: `"a":2`, Received: `"a":1,"a":2`}}},
		{"nested key re-escaped", `{"x":{"` + bs + `u0061":1}}`, `{"x":{"a":1}}`,
			[]Diff{{Path: "x.a", Verdict: KeyReencoded, Sent: `"` + bs + `u0061"`, Received: `"a"`}}},
		{"top-level key re-escaped, value kept", `{"` + bs + `u0074emperature":0.3}`, `{"temperature":0.3}`,
			[]Diff{{Path: "temperature", Verdict: KeyReencoded, Sent: `"` + bs + `u0074emperature"`, Received: `"temperature"`}}},
	}
	for _, c := range cases {
		got := Compare(mustParse(t, c.sent), mustParse(t, c.recv))
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s:\n got %+v\nwant %+v", c.name, got, c.want)
		}
	}
}

func TestAtTakesTheWorstRelatedDiff(t *testing.T) {
	diffs := []Diff{
		{Path: "messages[0].content", Verdict: Retyped},
		{Path: "messages[1]", Verdict: Dropped},
		{Path: "tools[0].function.description", Verdict: Reencoded},
		{Path: "tools[0].function.strict", Verdict: Dropped},
		{Path: "stream", Verdict: Injected},
	}
	for path, want := range map[string]Verdict{
		"messages[0].content":      Retyped,
		"messages[0]":              Retyped, // a diff inside it
		"messages[1].name":         Dropped, // its parent was dropped
		"messages[10]":             Kept,    // a prefix of the text, not of the path
		"tools[0].function":        Dropped,
		"tools[0].function.name":   Kept,
		"tools[0].function.strict": Dropped,
		"stream_options":           Kept,
		"stream":                   Injected,
	} {
		if got, _ := At(diffs, path); got != want {
			t.Errorf("At(%q) = %s, want %s", path, got, want)
		}
	}
}

// TestLossyProxyIsSeen is the comparator's positive control: a body re-marshalled through
// encoding/json's generic model, with one nested field deleted, must show exactly the losses that
// re-marshalling causes, and the deletion, and nothing more.
func TestLossyProxyIsSeen(t *testing.T) {
	sent := `{"model":"m","seed":9007199254740993,"temperature":1,"user":"<a&b>",` +
		`"tools":[{"type":"function","function":{"name":"f","parameters":{},"strict":true}}]}`
	var v map[string]any
	if err := json.Unmarshal([]byte(sent), &v); err != nil {
		t.Fatal(err)
	}
	delete(v["tools"].([]any)[0].(map[string]any)["function"].(map[string]any), "strict")
	recv, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Verdict{}
	for _, d := range Compare(mustParse(t, sent), mustParse(t, string(recv))) {
		got[d.Path] = d.Verdict
	}
	want := map[string]Verdict{
		"seed":                     Changed,   // float64 cannot hold 2^53+1
		"user":                     Reencoded, // encoding/json writes < > & as escapes
		"tools[0].function.strict": Dropped,
	}
	// temperature 1 stays 1 through float64 and back, so it is not a diff.
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lossy proxy: got %v, want %v (re-marshalled: %s)", got, want, recv)
	}
}
