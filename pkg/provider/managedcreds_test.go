package provider

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The expected values below are written out by hand from the file format in
// ManagedCredentials' documentation, never produced by Encode.

const testKey = "sk-example-0123456789abcdef"

func writeCreds(t *testing.T, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "llmlab.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadManagedCredentialsReadsTheDocumentedForm(t *testing.T) {
	path := writeCreds(t, `{"key": "`+testKey+`", "baseURL": "https://gateway.example/v1", "expiry": "2027-01-31T01:00:00+01:00"}`, 0o600)
	c, err := ReadManagedCredentials(path)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2027, 1, 31, 0, 0, 0, 0, time.UTC)
	if c.Key != testKey || c.BaseURL != "https://gateway.example/v1" || c.Expiry == nil || !c.Expiry.Equal(want) || c.Expiry.Location() != time.UTC {
		t.Fatalf("got %+v (expiry %v), want the file's values with the expiry in UTC", c, c.Expiry)
	}
	noExpiry := writeCreds(t, `{"key":"`+testKey+`","baseURL":"https://gateway.example/v1"}`, 0o400)
	if c, err := ReadManagedCredentials(noExpiry); err != nil || c.Expiry != nil {
		t.Fatalf("a file without expiry (mode 0400): %+v, %v; want no expiry and no error", c, err)
	}
}

func TestEncodeIsTheCanonicalForm(t *testing.T) {
	exp := time.Date(2027, 1, 31, 1, 0, 0, 0, time.FixedZone("CET", 3600))
	b, err := ManagedCredentials{Key: testKey, BaseURL: "https://gateway.example/v1", Expiry: &exp}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	want := "{\n  \"key\": \"" + testKey + "\",\n  \"baseURL\": \"https://gateway.example/v1\",\n  \"expiry\": \"2027-01-31T00:00:00Z\"\n}\n"
	if string(b) != want {
		t.Fatalf("Encode =\n%s\nwant\n%s", b, want)
	}
}

func TestParseManagedCredentialsRefuses(t *testing.T) {
	for _, tc := range []struct{ name, content, want string }{
		{"an unknown member (a misspelt expiry)", `{"key":"example-key-0001","baseURL":"https://g.example/v1","expires":"2027-01-31T00:00:00Z"}`, `unknown member "expires"`},
		{"baseUrl spelt in another case", `{"key":"example-key-0001","baseUrl":"https://g.example/v1"}`, `unknown member "baseUrl"`},
		{"key spelt in upper case", `{"KEY":"example-key-0001","baseURL":"https://g.example/v1"}`, `unknown member "KEY"`},
		{"a repeated member", `{"key":"example-key-0001","baseURL":"https://g.example/v1","baseURL":"https://h.example/v1"}`, `"baseURL" appears twice`},
		{"a key that is not a string", `{"key":1,"baseURL":"https://g.example/v1"}`, "wrong JSON type"},
		{"an array", `[]`, "want one JSON object"},
		{"no key", `{"baseURL":"https://g.example/v1"}`, "key is empty"},
		{"a key with a space", `{"key":"example key 0001","baseURL":"https://g.example/v1"}`, "not printable ASCII"},
		{"a key with a newline", `{"key":"example-key-0001\n","baseURL":"https://g.example/v1"}`, "not printable ASCII"},
		{"a key above the limit", `{"key":"` + strings.Repeat("k", MaxManagedKeyBytes+1) + `","baseURL":"https://g.example/v1"}`, "above"},
		{"no base URL", `{"key":"example-key-0001"}`, "not a URL"},
		{"plain http to a remote host", `{"key":"example-key-0001","baseURL":"http://g.example/v1"}`, "want https"},
		{"another scheme", `{"key":"example-key-0001","baseURL":"ftp://g.example/v1"}`, "want https"},
		{"user information", `{"key":"example-key-0001","baseURL":"https://user:pw@g.example/v1"}`, "user information"},
		{"a query", `{"key":"example-key-0001","baseURL":"https://g.example/v1?a=b"}`, "query"},
		{"a fragment", `{"key":"example-key-0001","baseURL":"https://g.example/v1#x"}`, "fragment"},
		{"a trailing slash", `{"key":"example-key-0001","baseURL":"https://g.example/v1/"}`, "ends with '/'"},
		{"no version segment", `{"key":"example-key-0001","baseURL":"https://g.example"}`, "does not end with the API version segment /v1"},
		{"another version segment", `{"key":"example-key-0001","baseURL":"https://g.example/v2"}`, "/v1"},
		{"v1 only as the end of a longer segment", `{"key":"example-key-0001","baseURL":"https://g.example/apiv1"}`, "/v1"},
		{"v1 in an escaped slash", `{"key":"example-key-0001","baseURL":"https://g.example/api%2Fv1"}`, "/v1"},
		{"a date without a time", `{"key":"example-key-0001","baseURL":"https://g.example/v1","expiry":"2027-01-31"}`, "RFC 3339"},
		{"two objects", `{"key":"example-key-0001","baseURL":"https://g.example/v1"}{}`, "data after"},
		{"not JSON", `key=example-key-0001`, "want one JSON object"},
		{"a truncated object", `{"key":"example-key-0001","baseURL":`, "invalid JSON"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseManagedCredentials([]byte(tc.content))
			if err == nil || !errors.Is(err, ErrCredentials) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want ErrCredentials mentioning %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "example-key-0001") || strings.Contains(err.Error(), "pw") {
				t.Fatalf("the error %q holds a secret", err)
			}
		})
	}
	for _, ok := range []string{"http://127.0.0.1:29340/v1", "http://localhost:29340/v1", "http://[::1]:29340/v1", "https://gateway.example/v1", "https://gateway.example:8443/api/v1"} {
		if _, err := ParseManagedCredentials([]byte(`{"key":"example-key-0001","baseURL":"` + ok + `"}`)); err != nil {
			t.Errorf("baseURL %s refused: %v", ok, err)
		}
	}
}

// A key shorter than MinManagedKeyBytes is a placeholder or a truncated paste
// (`@hpc-burst-science-agent`, 31 Oct 2026). The refusal wraps ErrCredentials,
// which the managed controller reads as CredentialsInvalid, and it gives the
// key's length only. Parse, Validate and Encode agree with Read, so the
// operator's tool never writes a file the controller then refuses.
func TestManagedKeyBelowSixteenBytesIsRefused(t *testing.T) {
	const short, least = "canary-key-0123", "canary-key-01234" // 15 and 16 bytes
	if len(short) != 15 || len(least) != 16 || MinManagedKeyBytes != 16 {
		t.Fatalf("fixtures are %d and %d bytes, minimum %d; want 15, 16 and 16", len(short), len(least), MinManagedKeyBytes)
	}
	file := func(key string) string { return `{"key":"` + key + `","baseURL":"https://gateway.example/v1"}` }

	_, err := ReadManagedCredentials(writeCreds(t, file(short), 0o600))
	if !errors.Is(err, ErrCredentials) {
		t.Fatalf("a 15-byte key: err = %v, want ErrCredentials", err)
	}
	if !strings.Contains(err.Error(), "15 bytes") || strings.Contains(err.Error(), short) {
		t.Errorf("a 15-byte key: err = %q, want its length (15 bytes) and no part of the key", err)
	}
	c, err := ReadManagedCredentials(writeCreds(t, file(least), 0o600))
	if err != nil || c.Key != least {
		t.Fatalf("a 16-byte key: key %q, err %v, want it accepted", c.Key, err)
	}

	// The same limit through the other entry points.
	if _, err := ParseManagedCredentials([]byte(file(short))); !errors.Is(err, ErrCredentials) {
		t.Errorf("Parse of a 15-byte key: err = %v, want ErrCredentials", err)
	}
	if _, err := (ManagedCredentials{Key: short, BaseURL: "https://gateway.example/v1"}).Encode(); !errors.Is(err, ErrCredentials) {
		t.Errorf("Encode of a 15-byte key: err = %v, want ErrCredentials", err)
	}
	if _, err := (ManagedCredentials{Key: least, BaseURL: "https://gateway.example/v1"}).Encode(); err != nil {
		t.Errorf("Encode of a 16-byte key: %v", err)
	}
	// An empty key keeps its own message.
	if _, err := ParseManagedCredentials([]byte(file(""))); err == nil || !strings.Contains(err.Error(), "key is empty") {
		t.Errorf("an empty key: err = %v, want \"key is empty\"", err)
	}
}

func TestReadManagedCredentialsChecksTheFile(t *testing.T) {
	good := `{"key":"` + testKey + `","baseURL":"https://gateway.example/v1"}`
	for _, mode := range []os.FileMode{0o640, 0o604, 0o644, 0o200} {
		_, err := ReadManagedCredentials(writeCreds(t, good, mode))
		if !errors.Is(err, ErrCredentials) || strings.Contains(err.Error(), testKey) {
			t.Errorf("mode %04o: err = %v, want ErrCredentials without the key", mode, err)
		}
	}
	target := writeCreds(t, good, 0o600)
	link := filepath.Join(t.TempDir(), "link.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadManagedCredentials(link); !errors.Is(err, ErrCredentials) {
		t.Errorf("a symlink: err = %v, want ErrCredentials", err)
	}
	if _, err := ReadManagedCredentials(t.TempDir()); !errors.Is(err, ErrCredentials) {
		t.Errorf("a directory: err = %v, want ErrCredentials", err)
	}
	_, err := ReadManagedCredentials(filepath.Join(t.TempDir(), "missing.json"))
	if !errors.Is(err, ErrCredentials) || !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a missing file: err = %v, want ErrCredentials and fs.ErrNotExist", err)
	}
	big := writeCreds(t, good+strings.Repeat(" ", MaxManagedCredentialsBytes), 0o600)
	if _, err := ReadManagedCredentials(big); !errors.Is(err, ErrCredentials) || !strings.Contains(err.Error(), "bytes") {
		t.Errorf("a file above the limit: err = %v", err)
	}
	bad := writeCreds(t, `{"key":"`+testKey+`","baseURL":"http://g.example/v1"}`, 0o600)
	if _, err := ReadManagedCredentials(bad); err == nil || strings.Contains(err.Error(), testKey) {
		t.Errorf("an invalid file: err = %v, want an error without the key", err)
	}
}

func TestManagedCredentialsURLs(t *testing.T) {
	c := ManagedCredentials{Key: testKey, BaseURL: "https://gateway.example/api/v1"}
	for path, want := range map[string]string{
		"/v1/chat/completions": "https://gateway.example/api/v1/chat/completions",
		"/v1/models":           "https://gateway.example/api/v1/models",
		"/v1/responses/r%2F1":  "https://gateway.example/api/v1/responses/r%2F1",
		"/v1/embeddings":       "https://gateway.example/api/v1/embeddings",
		"/v1/completions":      "https://gateway.example/api/v1/completions",
	} {
		if got, err := c.URLFor(path); err != nil || got != want {
			t.Errorf("URLFor(%q) = %q, %v; want %q", path, got, err, want)
		}
	}
	for _, path := range []string{"/v2/models", "/models", "/v1/", "v1/models", "/v1/models?x=1", ""} {
		if got, err := c.URLFor(path); err == nil {
			t.Errorf("URLFor(%q) = %q, want an error", path, got)
		}
	}
	if got := c.CatalogURL(); got != "https://gateway.example/api/v1/models" {
		t.Errorf("CatalogURL = %q", got)
	}
}

func TestExpiringIsADayBeforeTheExpiry(t *testing.T) {
	exp := time.Date(2027, 1, 31, 0, 0, 0, 0, time.UTC)
	c := ManagedCredentials{Key: testKey, BaseURL: "https://gateway.example/v1", Expiry: &exp}
	for _, tc := range []struct {
		now  time.Time
		want bool
	}{
		{time.Date(2027, 1, 29, 23, 59, 59, 0, time.UTC), false},
		{time.Date(2027, 1, 30, 0, 0, 0, 0, time.UTC), true},
		{time.Date(2027, 2, 1, 0, 0, 0, 0, time.UTC), true},
	} {
		if got := c.Expiring(tc.now); got != tc.want {
			t.Errorf("Expiring(%s) = %v, want %v", tc.now, got, tc.want)
		}
	}
	c.Expiry = nil
	if c.Expiring(exp) {
		t.Error("a key with no known expiry is never expiring")
	}
}

// No refusal echoes a supplied value: a key put into baseURL's query, pasted
// as baseURL, or pasted as a member name never reaches the error, which the
// Provider's controller copies into a condition and an event (from the review
// of PR #85).
func TestParseManagedCredentialsEchoesNoSuppliedValue(t *testing.T) {
	const canary = "sk-canary-7f3a9c0d1e2b"
	for _, tc := range []struct{ name, body, want string }{
		{"a key in baseURL's query", `{"key":"example-key-0001","baseURL":"https://g.example/v1?api_key=` + canary + `"}`, "query"},
		{"a key in baseURL's fragment", `{"key":"example-key-0001","baseURL":"https://g.example/v1#` + canary + `"}`, "fragment"},
		{"a key pasted as baseURL", `{"key":"example-key-0001","baseURL":"` + canary + `"}`, "want https"},
		{"a key as baseURL's host over http", `{"key":"example-key-0001","baseURL":"http://` + canary + `/v1"}`, "want https"},
		{"a key as baseURL's path", `{"key":"example-key-0001","baseURL":"https://g.example/` + canary + `"}`, "/v1"},
		{"a key with a trailing slash", `{"key":"example-key-0001","baseURL":"https://g.example/` + canary + `/"}`, "ends with '/'"},
		{"a key as baseURL with no host", `{"key":"example-key-0001","baseURL":"https:///` + canary + `"}`, "no host"},
		{"a key as an opaque baseURL", `{"key":"example-key-0001","baseURL":"https:` + canary + `"}`, "baseURL"},
		{"a key that does not parse as a URL", `{"key":"example-key-0001","baseURL":"https://g.example/%zz` + canary + `"}`, "not a URL"},
		{"a key as a member name", `{"key":"example-key-0001","baseURL":"https://g.example/v1","` + canary + `":1}`, "unknown member (name not shown)"},
		{"a key as a repeated member name", `{"` + canary + `":1,"` + canary + `":2}`, "(name not shown)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseManagedCredentials([]byte(tc.body))
			if err == nil || !errors.Is(err, ErrCredentials) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want ErrCredentials mentioning %q", err, tc.want)
			}
			if strings.Contains(err.Error(), canary) || strings.Contains(err.Error(), "7f3a9c") {
				t.Fatalf("the error %q holds the supplied value", err)
			}
		})
	}
	// A short, name-like member is still named, so a misspelling is plain.
	if _, err := ParseManagedCredentials([]byte(`{"key":"example-key-0001","baseURL":"https://g.example/v1","expires":"x"}`)); err == nil || !strings.Contains(err.Error(), `unknown member "expires"`) {
		t.Fatalf("err = %v, want it to name expires", err)
	}
}
