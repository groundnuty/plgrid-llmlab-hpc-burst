package convkey

import (
	"net/http"
	"strings"
	"testing"
)

// Number serialisation follows ECMAScript's Number::toString (RFC 8785
// section 3.2.2.3). The expected strings are RFC 8785 Appendix B's examples
// and ECMAScript's rule for the exponent switch at 1e21 and 1e-7, written by
// hand, not produced by this code.
func TestNumbersInES6Form(t *testing.T) {
	cases := map[string]string{
		"0":                       "0",
		"-0":                      "0",
		"5e-324":                  "5e-324",
		"-5e-324":                 "-5e-324",
		"1.7976931348623157e308":  "1.7976931348623157e+308",
		"9007199254740992":        "9007199254740992",
		"-9007199254740992":       "-9007199254740992",
		"295147905179352830000":   "295147905179352830000",
		"9.999999999999997e+22":   "9.999999999999997e+22",
		"1e23":                    "1e+23",
		"999999999999999700000":   "999999999999999700000",
		"1e21":                    "1e+21",
		"100000000000000000000":   "100000000000000000000",
		"9.999999999999997e-7":    "9.999999999999997e-7",
		"0.000001":                "0.000001",
		"0.0000001":               "1e-7",
		"1424953923781206.2":      "1424953923781206.2",
		"4.50":                    "4.5",
		"2e-3":                    "0.002",
		"-1.5E+2":                 "-150",
	}
	for in, want := range cases {
		got, err := canonical([]byte(in))
		if err != nil {
			t.Errorf("%s: %v", in, err)
			continue
		}
		if string(got) != want {
			t.Errorf("%s -> %s, want %s", in, got, want)
		}
	}
}

func TestStringsKeysAndLiterals(t *testing.T) {
	cases := map[string]string{
		// RFC 8785 section 3.2.2.2: only the quote, the backslash and
		// U+0000..U+001F are escaped; '/' and U+2028 are written as is.
		`"a\/b é\u001f\t"`: "\"a/b é\\u001f\\t\"",
		`{"b":1,"a":[true,false,null]}`: `{"a":[true,false,null],"b":1}`,
		// Keys sort as UTF-16 code units: U+1F600 (surrogates D83D DE00)
		// sorts before U+FFFD.
		`{"�":1,"😀":2}`: `{"😀":2,"` + "�" + `":1}`,
	}
	for in, want := range cases {
		got, err := canonical([]byte(in))
		if err != nil {
			t.Errorf("%s: %v", in, err)
			continue
		}
		if string(got) != want {
			t.Errorf("%s -> %s, want %s", in, got, want)
		}
	}
}

func TestDuplicateKeyIsRefused(t *testing.T) {
	if _, err := canonical([]byte(`{"a":1,"a":2}`)); err == nil {
		t.Error("a duplicate key must be an error (I-JSON)")
	}
}

func TestKeySources(t *testing.T) {
	h := http.Header{}
	h.Set("X-Session-Id", "abc")
	if k, src := Key(h, DefaultSessionHeaders, []byte(`[{"role":"user","content":"x"}]`)); k != "abc" || src != SourceHeader {
		t.Errorf("header: %s %s", k, src)
	}
	h2 := http.Header{}
	h2.Set("X-Other", "o")
	h2.Set("X-Session-Id", "s")
	if k, _ := Key(h2, []string{"x-other", "x-session-id"}, nil); k != "o" {
		t.Errorf("the first honoured header in order wins: got %s", k)
	}
	if k, src := Key(http.Header{"X-Session-Id": {""}}, DefaultSessionHeaders, []byte(`[{"role":"user","content":"x"}]`)); src != SourceOpening || !strings.HasPrefix(k, OpeningPrefix) {
		t.Errorf("an empty header does not count: %s %s", k, src)
	}
	k1, src := Key(http.Header{}, DefaultSessionHeaders, nil)
	k2, _ := Key(http.Header{}, DefaultSessionHeaders, nil)
	if src != SourceRandom || !strings.HasPrefix(k1, RandomPrefix) || len(k1) != len(RandomPrefix)+32 || k1 == k2 {
		t.Errorf("random keys: %s %s (%s)", k1, k2, src)
	}
	if _, src := Key(http.Header{}, DefaultSessionHeaders, []byte(`"not an array"`)); src != SourceRandom {
		t.Errorf("an unhashable opening gets a fresh key, got %s", src)
	}
}

// TestOpeningIgnoresToolsNamesAndToolCalls: only role and content are
// hashed, so these two openings share a key.
func TestOpeningIgnoresToolsNamesAndToolCalls(t *testing.T) {
	a, err := Opening([]byte(`[{"role":"system","content":"S","name":"n"},{"role":"user","content":"U","tool_calls":[]}]`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Opening([]byte(`[{"content":"S","role":"system"},{"content":"U","role":"user"}]`))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("%s != %s", a, b)
	}
}
