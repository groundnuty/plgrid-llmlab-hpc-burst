package deploymentid_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// The golden vectors' directories (testdata/README.md).
var canonVectors = []string{
	"01-hbep-example", "02-managed-with-revision", "03-managed-no-revision-unicode", "04-flags-env-repeat-json",
	"05-unicode-order", "06-template-from-tokenizer-config", "07-bytewise-paths-no-gpu",
}

func vectorFile(t *testing.T, dir, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "vectors", dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestCanonicalizeGivesTheCommittedBytes: Canonicalize of each vector's hand-written object, keys in any
// order, gives the committed RFC 8785 bytes, which neither this package nor Canonicalize produced
// (testdata/README.md). So does Canonicalize of the object as the records' writers store it, through
// encoding/json, which escapes <, >, & and U+2028 in vector 05: the stored text differs from the canonical
// bytes there, and the result does not.
func TestCanonicalizeGivesTheCommittedBytes(t *testing.T) {
	escaped := 0
	for _, dir := range canonVectors {
		want := vectorFile(t, dir, "canonical.json")
		stored, err := json.Marshal(struct {
			Canonical json.RawMessage `json:"canonical"`
		}{json.RawMessage(want)})
		if err != nil {
			t.Fatal(err)
		}
		stored = stored[len(`{"canonical":`) : len(stored)-1]
		if !bytes.Equal(stored, want) {
			escaped++
		}
		for name, in := range map[string][]byte{"expected-object.json": vectorFile(t, dir, "expected-object.json"), "the stored form": stored} {
			got, err := deploymentid.Canonicalize(in)
			if err != nil {
				t.Errorf("%s, %s: %v", dir, name, err)
				continue
			}
			if !bytes.Equal(got, want) {
				t.Errorf("%s, %s: Canonicalize gives\n%s\nwant canonical.json's\n%s", dir, name, got, want)
			}
		}
	}
	if escaped == 0 {
		t.Fatal("no vector's stored form differs from its canonical bytes: the case this test is for is gone")
	}
}

// TestCanonicalizeRefuses: what is not a JSON value of scheme v1's types, or not one RFC 8785 can write,
// is an error, never a silent rewrite.
func TestCanonicalizeRefuses(t *testing.T) {
	for name, in := range map[string]string{
		"a number":                  `{"a":1}`,
		"a boolean":                 `{"a":true}`,
		"null":                      `{"a":null}`,
		"a duplicate key":           `{"a":"x","a":"y"}`,
		"a duplicate key, escaped":  `{"a":"x","\u0061":"y"}`,
		"invalid UTF-8":             "{\"a\":\"\xff\"}",
		"a lone high surrogate":     `{"a":"\ud83d"}`,
		"a lone low surrogate":      `{"a":"\ude00"}`,
		"a high one, then a letter": `{"a":"\ud83dx"}`,
		"a high one, then a high":   `{"a":"\ud83d\ud83d"}`,
		"a surrogate in a key":      `{"\ud83d":"x"}`,
		"trailing data":             `{"a":"x"} {}`,
		"not JSON":                  `{"a":`,
	} {
		if got, err := deploymentid.Canonicalize([]byte(in)); err == nil {
			t.Errorf("%s: Canonicalize(%q) = %q, want an error", name, in, got)
		}
	}
}

// TestCanonicalizeKeepsAPairAndEscapes: an escaped surrogate pair is the character it encodes, written as
// UTF-8 (RFC 8785 section 3.2.2.2), and an escaped backslash before a "u" starts no escape.
func TestCanonicalizeKeepsAPairAndEscapes(t *testing.T) {
	for in, want := range map[string]string{
		`{"b":"\ud83d\ude00","a":"\u003c\u2028"}`: "{\"a\":\"<\u2028\",\"b\":\"\U0001F600\"}",
		`{"a":"\\ud83d"}`:                         `{"a":"\\ud83d"}`,
		`["x",[],{}]`:                             `["x",[],{}]`,
	} {
		got, err := deploymentid.Canonicalize([]byte(in))
		if err != nil || string(got) != want {
			t.Errorf("Canonicalize(%s) = %q, %v; want %q", in, got, err, want)
		}
	}
}
