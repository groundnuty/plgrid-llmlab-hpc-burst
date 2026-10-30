package deploymentid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"testing/quick"
)

func mustJCS(t *testing.T, v jvalue) []byte {
	t.Helper()
	got, err := canonicalize(v)
	if err != nil {
		t.Fatalf("canonicalize: %v", err)
	}
	return got
}

// The sorting sample of RFC 8785 section 3.2.3 and the expected order printed
// there. Bytewise UTF-8 order would put the Hebrew letter before the emoji;
// UTF-16 order puts the emoji (a surrogate pair, D83D DE00) before it (FB33).
func TestJCSRFC8785SortingSample(t *testing.T) {
	obj := jobject{
		{"\xe2\x82\xac", jstring("Euro Sign")},                       // U+20AC
		{"\r", jstring("Carriage Return")},                           // U+000D
		{"\xef\xac\xb3", jstring("Hebrew Letter Dalet With Dagesh")}, // U+FB33
		{"1", jstring("One")},
		{"\xf0\x9f\x98\x80", jstring("Emoji: Grinning Face")},        // U+1F600
		{"\xc2\x80", jstring("Control")},                             // U+0080
		{"\xc3\xb6", jstring("Latin Small Letter O With Diaeresis")}, // U+00F6
	}
	want := `{"\r":"Carriage Return","1":"One","` + "\xc2\x80" + `":"Control","` + "\xc3\xb6" +
		`":"Latin Small Letter O With Diaeresis","` + "\xe2\x82\xac" + `":"Euro Sign","` + "\xf0\x9f\x98\x80" +
		`":"Emoji: Grinning Face","` + "\xef\xac\xb3" + `":"Hebrew Letter Dalet With Dagesh"}`
	if got := mustJCS(t, obj); string(got) != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

// The string of RFC 8785 section 3.2.2, "€$\u000F\u000aA'B"\\\\"\/"
// once parsed, and its serialisation as the hex dump of section 3.2.4 prints it.
func TestJCSRFC8785StringSample(t *testing.T) {
	in := "\xe2\x82\xac$\x0f\nA'B\"\\\\\"/"
	want := []byte{
		0x22, 0xe2, 0x82, 0xac, 0x24, 0x5c, 0x75, 0x30, 0x30, 0x30, 0x66, 0x5c, 0x6e,
		0x41, 0x27, 0x42, 0x5c, 0x22, 0x5c, 0x5c, 0x5c, 0x5c, 0x5c, 0x22, 0x2f, 0x22,
	}
	if got := mustJCS(t, jstring(in)); !bytes.Equal(got, want) {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestJCSStringEscapes(t *testing.T) {
	short := map[byte]string{'\b': `\b`, '\t': `\t`, '\n': `\n`, '\f': `\f`, '\r': `\r`}
	for c := byte(0); c < 0x20; c++ {
		want, ok := short[c]
		if !ok {
			want = fmt.Sprintf(`\u%04x`, c) // lowercase hex, RFC 8785 section 3.2.2.2
		}
		if got := mustJCS(t, jstring(string([]byte{c}))); string(got) != `"`+want+`"` {
			t.Errorf("U+%04X: got %q, want %q", c, got, `"`+want+`"`)
		}
	}
	// Spot checks written out, not computed.
	literal := map[string]string{
		"\x00": `"\u0000"`, "\x0b": `"\u000b"`, "\x1f": `"\u001f"`, "\x1b": `"\u001b"`,
		`"`: `"\""`, `\`: `"\\"`,
		// Written as is: '/', DEL, the HTML characters, U+2028, U+2029.
		"/": `"/"`, "\x7f": "\"\x7f\"", "<>&": `"<>&"`,
		"\xe2\x80\xa8": "\"\xe2\x80\xa8\"", "\xe2\x80\xa9": "\"\xe2\x80\xa9\"",
		"": `""`,
	}
	for in, want := range literal {
		if got := mustJCS(t, jstring(in)); string(got) != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
}

func TestJCSEmptyAndNilContainers(t *testing.T) {
	cases := []struct {
		v    jvalue
		want string
	}{
		{jobject(nil), `{}`}, {jobject{}, `{}`}, {jarray(nil), `[]`}, {jarray{}, `[]`},
		{jobject{{"a", jobject(nil)}, {"b", jarray(nil)}}, `{"a":{},"b":[]}`},
	}
	for _, c := range cases {
		if got := mustJCS(t, c.v); string(got) != c.want {
			t.Errorf("got %q, want %q", got, c.want)
		}
	}
}

func TestJCSSortsRecursivelyAndKeepsArrayOrder(t *testing.T) {
	v := jobject{
		{"b", jarray{jobject{{"y", jstring("1")}, {"x", jstring("2")}}, jstring("z"), jstring("a")}},
		{"a", jobject{{"2", jstring("")}, {"10", jstring("")}, {"", jstring("e")}}},
	}
	want := `{"a":{"":"e","10":"","2":""},"b":[{"x":"2","y":"1"},"z","a"]}`
	if got := mustJCS(t, v); string(got) != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestJCSPrefixSortsBeforeLonger(t *testing.T) {
	v := jobject{{"ab", jstring("3")}, {"a", jstring("1")}, {"aa", jstring("2")}, {"", jstring("0")}}
	want := `{"":"0","a":"1","aa":"2","ab":"3"}`
	if got := mustJCS(t, v); string(got) != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestJCSErrors(t *testing.T) {
	cases := map[string]jvalue{
		"invalid UTF-8 value":    jstring("a\xffb"),
		"surrogate half (UTF-8)": jstring("\xed\xa0\x80"),
		"invalid UTF-8 key":      jobject{{"\xc3", jstring("v")}},
		"duplicate key":          jobject{{"k", jstring("1")}, {"k", jstring("2")}},
		"nested invalid":         jarray{jobject{{"k", jstring("\xff")}}},
	}
	for name, v := range cases {
		if got, err := canonicalize(v); err == nil {
			t.Errorf("%s: no error, got %q", name, got)
		}
	}
}

// Any valid UTF-8 string, whatever it holds, must come back unchanged through
// a standard JSON parser, and must not contain a raw control character.
func TestJCSStringRoundTrip(t *testing.T) {
	check := func(s string) bool {
		out, err := canonicalize(jstring(s))
		if err != nil {
			return false
		}
		for _, c := range out {
			if c < 0x20 {
				return false
			}
		}
		var back string
		return json.Unmarshal(out, &back) == nil && back == s
	}
	cfg := &quick.Config{MaxCount: 2000, Rand: rand.New(rand.NewSource(8785))}
	if err := quick.Check(check, cfg); err != nil {
		t.Error(err)
	}
	for _, s := range []string{"", "\x00\x1f\x7f", "\xe2\x80\xa8<>&/\\\"", "\xf0\x9f\x98\x80\xef\xac\xb3"} {
		if !check(s) {
			t.Errorf("round trip failed for %q", s)
		}
	}
}
