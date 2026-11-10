package cases

import (
	"bufio"
	"os"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/rawjson"
)

type field struct{ endpoint, path, source string }

// readFields reads the enumeration that scripts/enumerate_fields.py wrote from vLLM's source.
func readFields(t *testing.T) []field {
	t.Helper()
	f, err := os.Open("../fields/vllm-v0.32.0.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []field
	sc := bufio.NewScanner(f)
	sc.Scan() // header
	for sc.Scan() {
		cols := strings.Split(sc.Text(), "\t")
		if len(cols) != 7 {
			t.Fatalf("bad row %q", sc.Text())
		}
		out = append(out, field{cols[0], cols[1], cols[6]})
	}
	if len(out) < 200 {
		t.Fatalf("only %d fields; the enumeration looks truncated", len(out))
	}
	return out
}

func TestEveryEnumeratedFieldIsCovered(t *testing.T) {
	covered := map[string]map[Endpoint]bool{}
	for _, c := range All() {
		for _, cv := range c.Covers {
			if covered[cv.Field] == nil {
				covered[cv.Field] = map[Endpoint]bool{}
			}
			covered[cv.Field][c.Endpoint] = true
		}
	}
	known := map[string]bool{}
	for _, f := range readFields(t) {
		known[f.path] = true
		if _, ok := Excluded[f.path]; ok {
			continue
		}
		var want []Endpoint
		switch f.endpoint {
		case "chat":
			want = []Endpoint{Chat}
		case "completions":
			want = []Endpoint{Completions}
		case "both":
			want = []Endpoint{Chat, Completions}
		default:
			t.Fatalf("endpoint %q", f.endpoint)
		}
		for _, e := range want {
			if !covered[f.path][e] {
				t.Errorf("%s (%s) is not covered on %s", f.path, f.source, e)
			}
		}
	}
	for field := range covered {
		if !strings.HasPrefix(field, "ext:") && !known[field] {
			t.Errorf("a case covers %q, which the enumeration lacks", field)
		}
	}
	for path := range Excluded {
		if !known[path] {
			t.Errorf("Excluded names %q, which the enumeration lacks", path)
		}
	}
}

func TestBodiesAndCovers(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range All() {
		if seen[c.Key()] {
			t.Errorf("duplicate case %s", c.Key())
		}
		seen[c.Key()] = true
		plain, err := rawjson.Parse([]byte(c.Body))
		if err != nil {
			t.Errorf("%s: %v in %s", c.Key(), err, c.Body)
			continue
		}
		if plain.Get("model") == nil {
			t.Errorf("%s: no model", c.Key())
		}
		stream := plain.Get("stream")
		switch c.Stream {
		case "both":
			if stream != nil {
				t.Errorf("%s: a both-ways case must not set stream itself", c.Key())
			}
		case "only":
			if stream == nil || string(stream.Raw) != "true" {
				t.Errorf("%s: a stream-only case must set stream to true", c.Key())
			}
		case "never":
		default:
			t.Errorf("%s: Stream %q", c.Key(), c.Stream)
		}
		for _, cv := range c.Covers {
			if rawjson.Lookup(plain, cv.At) == nil {
				t.Errorf("%s: covers %s at %s, which its body lacks", c.Key(), cv.Field, cv.At)
			}
		}
		for _, v := range c.Variants() {
			n, err := rawjson.Parse(v.Body)
			if err != nil {
				t.Errorf("%s: %v", v.Key(), err)
				continue
			}
			if s := n.Get("stream"); v.Stream != (s != nil && string(s.Raw) == "true") {
				t.Errorf("%s: stream is %v, variant says %v", v.Key(), s, v.Stream)
			}
			if c.Stream == "both" && v.Stream && string(v.Body) == c.Body {
				t.Errorf("%s: the streamed body is the plain one", v.Key())
			}
		}
	}
}

// The escape cases must carry the escapes as bytes on the wire, not the characters they stand for.
func TestEscapeCasesCarryEscapes(t *testing.T) {
	b := string(rune(92))
	for _, c := range All() {
		if c.ID != "enc/string-escapes" {
			continue
		}
		for _, esc := range []string{b + "u00e9", b + "ud83d" + b + "ude00", b + "/", b + "u0001", b + "t"} {
			if !strings.Contains(c.Body, esc) {
				t.Errorf("%s lacks the escape %q", c.Key(), esc)
			}
		}
	}
}
