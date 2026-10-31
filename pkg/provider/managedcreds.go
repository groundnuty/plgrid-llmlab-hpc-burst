package provider

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"
)

// ManagedCredentials is a managed Provider's credentials file (HBEP-4, Provider
// settings: "it holds the key, the gateway's base URL, and the key's expiry
// date when one is known"). Two components read it: the managed Provider's
// controller (#69), which lists the catalog with the key and sets
// CredentialsExpiring from the expiry, and the front (#64), which sends a
// managed target's requests to the base URL with the key (HBEP-5, Request
// path, step 7; The LLMLab route). Both read it with ReadManagedCredentials,
// so they agree on every byte.
//
// The file is one JSON object, UTF-8, with exactly these members; an unknown
// member is an error (a misspelt "expiry" would otherwise silently disable the
// expiry warning):
//
//	{
//	  "key": "<the upstream key>",
//	  "baseURL": "https://gateway.example/v1",
//	  "expiry": "2027-01-31T00:00:00Z"
//	}
//
// It is mode 600 (no bit for group or others) and owned by the controller's
// user (HBEP-11, Credentials). The operator writes it; Encode gives the
// canonical form.
type ManagedCredentials struct {
	// Key is the upstream key, sent as "Authorization: Bearer <Key>". It is
	// printable ASCII without spaces, at most MaxManagedKeyBytes. It never
	// reaches a log, an object or a command line.
	Key string `json:"key"`

	// BaseURL is the gateway's OpenAI-compatible base URL, as an OpenAI client
	// is configured with it, the API version segment included: for example
	// https://gateway.example/v1. A request for the front's path /v1/<rest>
	// goes to BaseURL + "/<rest>" (URLFor); the catalog is BaseURL +
	// "/models" (CatalogURL, HBEP-4's GET /v1/models). It is https, or http
	// only on a loopback host (tests and local doubles); it has no user
	// information, query or fragment, and no trailing "/". Its path ends with
	// the segment "/v1" (from the review of the HBEPs, #74).
	BaseURL string `json:"baseURL"`

	// Expiry is when the key expires, an RFC 3339 time, absent when it is not
	// known. A day before it the Provider gets CredentialsExpiring (Expiring).
	Expiry *time.Time `json:"expiry,omitempty"`
}

// Limits of a managed credentials file.
const (
	// MaxManagedCredentialsBytes is the largest file ReadManagedCredentials
	// reads.
	MaxManagedCredentialsBytes = 64 << 10
	// MaxManagedKeyBytes is the longest key.
	MaxManagedKeyBytes = 4096
	// CredentialsExpiringWindow is how long before the expiry the Provider
	// gets CredentialsExpiring (HBEP-4: "a day before the expiry date").
	CredentialsExpiringWindow = 24 * time.Hour
)

// ParseManagedCredentials decodes and validates a managed credentials file's
// content. Member names match exactly, in case too, and none may repeat:
// encoding/json alone would take "baseUrl" for "baseURL" and keep the last of
// two "expiry" members. Its errors wrap ErrCredentials and never contain the
// key.
func ParseManagedCredentials(b []byte) (ManagedCredentials, error) {
	c, err := decodeManagedCredentials(b)
	if err != nil {
		return ManagedCredentials{}, err
	}
	if err := c.Validate(); err != nil {
		return ManagedCredentials{}, err
	}
	if c.Expiry != nil {
		t := c.Expiry.UTC()
		c.Expiry = &t
	}
	return c, nil
}

func decodeManagedCredentials(b []byte) (ManagedCredentials, error) {
	var c ManagedCredentials
	dec := json.NewDecoder(bytes.NewReader(b))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return c, credErr("not a managed credentials object: want one JSON object")
	}
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return c, credErr("not a managed credentials object: %s", jsonErrText(err))
		}
		name, _ := tok.(string)
		if seen[name] {
			return c, credErr("member %q appears twice", name)
		}
		seen[name] = true
		switch name {
		case "key":
			err = dec.Decode(&c.Key)
		case "baseURL":
			err = dec.Decode(&c.BaseURL)
		case "expiry":
			err = dec.Decode(&c.Expiry)
		default:
			return c, credErr("unknown member %q: want key, baseURL and expiry", name)
		}
		if err != nil {
			return c, credErr("member %q: %s", name, jsonErrText(err))
		}
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return c, credErr("not a managed credentials object: %s", jsonErrText(err))
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return c, credErr("data after the managed credentials object")
	}
	return c, nil
}

// jsonErrText describes a decoding error without echoing the input.
func jsonErrText(err error) string {
	var syn *json.SyntaxError
	var typ *json.UnmarshalTypeError
	var tim *time.ParseError
	switch {
	case err == nil:
		return "invalid JSON"
	case errors.As(err, &tim):
		return "not an RFC 3339 time, such as 2027-01-31T00:00:00Z"
	case errors.As(err, &syn):
		return fmt.Sprintf("invalid JSON at byte %d", syn.Offset)
	case errors.As(err, &typ):
		return "wrong JSON type"
	default:
		return "invalid JSON"
	}
}

// Validate returns an error, wrapping ErrCredentials, unless c is a complete,
// well-formed managed credentials file. It never contains the key.
func (c ManagedCredentials) Validate() error {
	switch {
	case c.Key == "":
		return credErr("key is empty")
	case len(c.Key) > MaxManagedKeyBytes:
		return credErr("key is %d bytes, above %d", len(c.Key), MaxManagedKeyBytes)
	}
	for i := 0; i < len(c.Key); i++ {
		if c.Key[i] <= ' ' || c.Key[i] > '~' {
			return credErr("key holds a byte that is not printable ASCII at position %d", i)
		}
	}
	return checkBaseURL(c.BaseURL)
}

func checkBaseURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || raw == "" {
		return credErr("baseURL %q is not a URL", raw)
	}
	host := u.Hostname()
	switch {
	case u.Scheme != "https" && !(u.Scheme == "http" && isLoopback(host)):
		return credErr("baseURL %q: want https (http only on a loopback host)", raw)
	case host == "":
		return credErr("baseURL %q has no host", raw)
	case u.User != nil:
		return credErr("baseURL has user information; put the key in key")
	case u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#"):
		return credErr("baseURL %q has a query or a fragment", raw)
	case strings.HasSuffix(u.Path, "/"):
		return credErr("baseURL %q ends with '/'", raw)
	case u.Opaque != "":
		return credErr("baseURL %q is not hierarchical", raw)
	case !strings.HasSuffix(u.EscapedPath(), "/v1"):
		return credErr("baseURL %q does not end with the API version segment /v1, as in https://gateway.example/v1", raw)
	}
	return nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Encode returns the canonical file content: the object as indented JSON
// with a final newline, the expiry in UTC.
func (c ManagedCredentials) Encode() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c.Expiry != nil {
		t := c.Expiry.UTC()
		c.Expiry = &t
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// URLFor returns the upstream URL for a request the front received at path,
// which starts with "/v1/": BaseURL followed by path without its "/v1". The
// caller appends the request's query, if any. path is the request's path as
// it arrived, already escaped.
func (c ManagedCredentials) URLFor(path string) (string, error) {
	rest, ok := strings.CutPrefix(path, "/v1/")
	if !ok || rest == "" {
		return "", fmt.Errorf("path %q is not under /v1/", path)
	}
	if strings.ContainsAny(rest, "?#") {
		return "", fmt.Errorf("path %q holds a query or a fragment; pass the path alone", path)
	}
	return c.BaseURL + "/" + rest, nil
}

// CatalogURL is the catalog listing's URL, the gateway's GET /v1/models
// (HBEP-4, Managed provider in alpha).
func (c ManagedCredentials) CatalogURL() string { return c.BaseURL + "/models" }

// Expiring reports whether the Provider has CredentialsExpiring at now: the
// expiry is known and now is at most CredentialsExpiringWindow before it, or
// past it.
func (c ManagedCredentials) Expiring(now time.Time) bool {
	return c.Expiry != nil && !now.Before(c.Expiry.Add(-CredentialsExpiringWindow))
}

// ReadManagedCredentials reads and validates a managed Provider's credentials
// file: a regular file (not a symlink), owned by the calling user, with no
// permission bit for group or others, of at most MaxManagedCredentialsBytes.
// Every error wraps ErrCredentials (the Provider then gets
// v1alpha1.ConditionCredentialsInvalid) except a missing file, which also
// wraps fs.ErrNotExist; none contains the file's content.
func ReadManagedCredentials(path string) (ManagedCredentials, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return ManagedCredentials{}, fmt.Errorf("%w: %w", ErrCredentials, err)
	}
	if err := checkOwnerOnly(path, info); err != nil {
		return ManagedCredentials{}, err
	}
	f, err := os.Open(path)
	if err != nil {
		return ManagedCredentials{}, fmt.Errorf("%w: %w", ErrCredentials, err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, MaxManagedCredentialsBytes+1))
	if err != nil {
		return ManagedCredentials{}, fmt.Errorf("%w: read %s: %w", ErrCredentials, path, err)
	}
	if len(b) > MaxManagedCredentialsBytes {
		return ManagedCredentials{}, credErr("%s is above %d bytes", path, MaxManagedCredentialsBytes)
	}
	c, err := ParseManagedCredentials(b)
	if err != nil {
		return ManagedCredentials{}, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

// checkOwnerOnly refuses a file that is not regular, that another user owns,
// or that grants group or others any permission (HBEP-11, Credentials).
func checkOwnerOnly(path string, info os.FileInfo) error {
	if !info.Mode().IsRegular() {
		return credErr("%s is not a regular file (%s)", path, info.Mode().Type())
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 || perm&0o400 == 0 {
		return credErr("%s has mode %04o; want 0600: readable by its owner only", path, perm)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return credErr("%s: cannot read its owner", path)
	}
	if int(st.Uid) != os.Geteuid() {
		return credErr("%s is owned by uid %d, not the controller's user (uid %d)", path, st.Uid, os.Geteuid())
	}
	return nil
}

func credErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCredentials, fmt.Sprintf(format, args...))
}
