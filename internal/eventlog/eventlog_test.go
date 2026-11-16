package eventlog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewTokenIsThirtyTwoRandomBytesInHex(t *testing.T) {
	a, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewToken()
	if len(a) != 64 || !ValidToken(a) {
		t.Fatalf("token %q: want 64 lower-case hex digits", a)
	}
	if a == b {
		t.Fatalf("two tokens are equal: %q", a)
	}
	for _, bad := range []string{"", strings.Repeat("a", 63), strings.Repeat("a", 65), strings.Repeat("A", 64), strings.Repeat("g", 64)} {
		if ValidToken(bad) {
			t.Errorf("ValidToken(%q) is true", bad)
		}
	}
}

// ConfText is the launcher's file, so its text is written out here by hand: sync_interval inside
// the jetstream block (the only place nats-server reads it), both listeners on loopback, a token.
func TestConfTextIsExactlyHBEP8s(t *testing.T) {
	tok := strings.Repeat("ab", 32)
	got, err := ConfText("/run/st/eventlog", 28496, 28497, tok)
	if err != nil {
		t.Fatal(err)
	}
	want := "listen: 127.0.0.1:28496\n" +
		"http: 127.0.0.1:28497\n" +
		"authorization {\n  token: \"" + tok + "\"\n}\n" +
		"jetstream {\n  store_dir: \"/run/st/eventlog\"\n  sync_interval: always\n}\n"
	if got != want {
		t.Fatalf("conf text:\n%s\nwant:\n%s", got, want)
	}
}

func TestConfTextRefusesWhatItCannotCarry(t *testing.T) {
	tok := strings.Repeat("ab", 32)
	for name, c := range map[string]struct {
		dir          string
		client, mon  int
		token        string
		wantInReason string
	}{
		"relative store":    {"eventlog", 28496, 28497, tok, "absolute"},
		"quote in store":    {`/run/"st"`, 28496, 28497, tok, "character"},
		"newline in store":  {"/run/st\n/x", 28496, 28497, tok, "character"},
		"backslash":         {`/run/st\x`, 28496, 28497, tok, "character"},
		"port 0":            {"/run/st", 0, 28497, tok, "port"},
		"port too high":     {"/run/st", 28496, 70000, tok, "port"},
		"same port twice":   {"/run/st", 28496, 28496, tok, "both"},
		"short token":       {"/run/st", 28496, 28497, "abc", "token"},
		"upper-case token":  {"/run/st", 28496, 28497, strings.Repeat("AB", 32), "token"},
		"non-hex token":     {"/run/st", 28496, 28497, strings.Repeat("zz", 32), "token"},
		"token with quote":  {"/run/st", 28496, 28497, strings.Repeat(`a"`, 32), "token"},
		"empty store":       {"", 28496, 28497, tok, "absolute"},
		"store with delete": {"/run/st\x7f", 28496, 28497, tok, "character"},
	} {
		_, err := ConfText(c.dir, c.client, c.mon, c.token)
		if err == nil || !strings.Contains(err.Error(), c.wantInReason) {
			t.Errorf("%s: err %v, want one naming %q", name, err, c.wantInReason)
		}
	}
}

func TestCapsRoundTripAndAreStrict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, CapsFile)
	if err := WriteCaps(path, Caps{Events: 2 << 30, Usage: 64 << 20}); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("caps.json: %v, mode %v, want 0600", err, fi)
	}
	// The file's text is written out by hand: the names are the contract with the process.
	b, _ := os.ReadFile(path)
	if want := `{"events":2147483648,"usage":67108864}` + "\n"; string(b) != want {
		t.Fatalf("caps.json holds %q, want %q", b, want)
	}
	got, err := ReadCaps(path)
	if err != nil || got != (Caps{Events: 2147483648, Usage: 67108864}) || got.Sum() != 2147483648+67108864 {
		t.Fatalf("ReadCaps: %+v, %v", got, err)
	}
	for name, text := range map[string]string{
		"an unknown field":  `{"events":1,"usage":1,"extra":2}`,
		"a zero cap":        `{"events":0,"usage":1}`,
		"a negative cap":    `{"events":1,"usage":-1}`,
		"a missing cap":     `{"events":1}`,
		"two values":        `{"events":1,"usage":1}{"events":1,"usage":1}`,
		"not json":          `events=1`,
		"a string for caps": `{"events":"2GiB","usage":1}`,
	} {
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadCaps(path); err == nil {
			t.Errorf("%s: ReadCaps accepted %q", name, text)
		}
	}
	if err := WriteCaps(path, Caps{Events: 0, Usage: 1}); err == nil {
		t.Error("WriteCaps accepted a zero cap")
	}
}
