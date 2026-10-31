package hubfiles

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
)

// Expected bytes, blob ids and SHA-256 values in this file are the literals of
// fakehub_test.go (computed once with git hash-object and sha256sum, outside
// this package) or constants written here. Nothing is derived with the
// package's own download or hashing code.

// fixtureNamed is the fixture of fakehub_test.go with that path.
func fixtureNamed(path string) fixture {
	for _, f := range fixtures {
		if f.path == path {
			return f
		}
	}
	panic("no fixture " + path)
}

// sameSize returns a string as long as s that differs from it.
func sameSize(s string) string { return "X" + s[1:] }

// A small LFS file: its content, and the lfs.oid that sha256sum gives for it.
const (
	lfsABC = "abc" // sha256ABC
	lfsABD = "abd" // another content of the same size
)

// withFiles is allFiles plus the extra files; a file with the path of a
// fixture replaces it, since a listing may not hold a path twice.
func withFiles(extra ...fakeFile) []fakeFile {
	files := allFiles()
	for _, x := range extra {
		if i := slices.IndexFunc(files, func(f fakeFile) bool { return f.path == x.path }); i >= 0 {
			files[i] = x
		} else {
			files = append(files, x)
		}
	}
	return files
}

// leakyTransport fails every download with an error whose text holds the
// request's Authorization header, the way a careless lower layer might.
func leakyTransport() roundTripFunc {
	return func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/resolve/") {
			return nil, fmt.Errorf("connection dropped while sending %q: %w", r.Header.Get("Authorization"), io.ErrUnexpectedEOF)
		}
		return http.DefaultTransport.RoundTrip(r)
	}
}

func TestContentReturnsTheExactBytesOfAFileNotInLFS(t *testing.T) {
	h := newFakeHub(t, allFiles())
	// A small page size spreads the listing over several pages: the file is
	// found wherever it is.
	h.pageSize = 4
	c := h.client(t, Config{})
	for _, path := range []string{"config.json", "tokenizer/special/map.json", "dir with space/file #1.txt", "é.txt", ".gitattributes"} {
		got, err := c.Content(context.Background(), testRepo, testRev, path)
		if err != nil {
			t.Errorf("%s: %v", path, err)
			continue
		}
		if want := fixtureNamed(path).content; string(got) != want {
			t.Errorf("%s = %q, want %q", path, got, want)
		}
	}
	tree, resolves := h.requests()
	// The listing holds 17 entries (13 files and 4 directories), so 5 pages of
	// 4; every call reads all of them, and downloads only the file asked for
	// (Files would have downloaded every file that is not in LFS).
	if len(tree) != 5*5 {
		t.Errorf("%d tree requests for 5 calls, want 5 pages per call", len(tree))
	}
	want := []string{"config.json", "tokenizer/special/map.json", "dir with space/file #1.txt", "é.txt", ".gitattributes"}
	if !slices.Equal(resolves, want) {
		t.Errorf("downloads = %q, want %q", resolves, want)
	}
}

func TestContentReturnsAnEmptyFileAsEmptyNotNil(t *testing.T) {
	h := newFakeHub(t, withFiles(lfsContentFile("empty.safetensors", "", sha256Empty)))
	for _, path := range []string{"empty.txt", "empty.safetensors"} {
		got, err := h.client(t, Config{}).Content(context.Background(), testRepo, testRev, path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if got == nil || len(got) != 0 {
			t.Errorf("%s = %#v, want a non-nil empty slice (present and empty is not absent)", path, got)
		}
	}
}

func TestContentReturnsTheExactBytesOfAnLFSFile(t *testing.T) {
	h := newFakeHub(t, withFiles(lfsContentFile("small.safetensors", lfsABC, sha256ABC)))
	got, err := h.client(t, Config{}).Content(context.Background(), testRepo, testRev, "small.safetensors")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "abc" {
		t.Errorf("content = %q, want %q", got, "abc")
	}
	if _, resolves := h.requests(); !slices.Equal(resolves, []string{"small.safetensors"}) {
		t.Errorf("downloads = %q, want only the file asked for", resolves)
	}
}

func TestContentRefusesAFileWhoseBlobIDDiffersFromTheListing(t *testing.T) {
	cfg := fixtureNamed("config.json")
	files := allFiles()
	for i := range files {
		if files[i].path == "config.json" {
			// The listing keeps the real blob id and size; the Hub serves other
			// bytes of the same length.
			files[i].content = sameSize(cfg.content)
		}
	}
	h := newFakeHub(t, files)
	got, err := h.client(t, Config{}).Content(context.Background(), testRepo, testRev, "config.json")
	if !errors.Is(err, ErrBlobMismatch) {
		t.Fatalf("err = %v, want ErrBlobMismatch", err)
	}
	if !strings.Contains(err.Error(), "config.json") {
		t.Errorf("the error does not name the file: %v", err)
	}
	if got != nil {
		t.Errorf("the altered bytes were returned: %q", got)
	}
}

func TestContentRefusesAnLFSFileWhoseSHA256DiffersFromTheListing(t *testing.T) {
	// The listing's lfs.oid is the SHA-256 of "abc"; the Hub serves "abd".
	h := newFakeHub(t, withFiles(lfsContentFile("small.safetensors", lfsABD, sha256ABC)))
	got, err := h.client(t, Config{}).Content(context.Background(), testRepo, testRev, "small.safetensors")
	if !errors.Is(err, ErrBlobMismatch) {
		t.Fatalf("err = %v, want ErrBlobMismatch", err)
	}
	if got != nil {
		t.Errorf("the altered bytes were returned: %q", got)
	}
}

func TestContentRefusesAContentThatDiffersFromTheListedSize(t *testing.T) {
	for name, f := range map[string]fakeFile{
		"not in LFS, the listing says longer":  withSize(plainFile(fixtureNamed("config.json")), 100),
		"not in LFS, the listing says shorter": withSize(plainFile(fixtureNamed("config.json")), 3),
		"LFS, the listing says longer":         withSize(lfsContentFile("small.safetensors", lfsABC, sha256ABC), 100),
		"LFS, the listing says shorter":        withSize(lfsContentFile("small.safetensors", lfsABC, sha256ABC), 2),
	} {
		h := newFakeHub(t, withFiles(f))
		got, err := h.client(t, Config{}).Content(context.Background(), testRepo, testRev, f.path)
		if !errors.Is(err, ErrSizeMismatch) {
			t.Errorf("%s: err = %v, want ErrSizeMismatch", name, err)
		}
		if got != nil {
			t.Errorf("%s: bytes were returned: %q", name, got)
		}
	}
}

// withSize returns f with the size its listing gives.
func withSize(f fakeFile, size int64) fakeFile {
	f.size = size
	return f
}

// countingBody is a download body of unknown length that counts what is read
// from it. limit < 0 means it never ends.
type countingBody struct {
	n, limit int64
}

func (b *countingBody) Read(p []byte) (int, error) {
	if b.limit >= 0 {
		if b.n >= b.limit {
			return 0, io.EOF
		}
		p = p[:min(int64(len(p)), b.limit-b.n)]
	}
	for i := range p {
		p[i] = 'x'
	}
	b.n += int64(len(p))
	return len(p), nil
}

func (b *countingBody) Close() error { return nil }

// bodyTransport answers the listing with listing and every download with body,
// which has no Content-Length.
func bodyTransport(listing string, body io.ReadCloser) http.RoundTripper {
	return lengthTransport(listing, body, -1)
}

// lengthTransport is bodyTransport with the Content-Length the response
// claims (-1: none).
func lengthTransport(listing string, body io.ReadCloser, length int64) http.RoundTripper {
	tree := handlerTransport(serveBody(listing))
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if !strings.Contains(r.URL.Path, "/resolve/") {
			return tree.RoundTrip(r)
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: http.Header{}, ContentLength: length, Body: body, Request: r}, nil
	})
}

func TestContentNeverReadsMoreThanTheListedSizePlusOne(t *testing.T) {
	cfg := fixtureNamed("config.json")
	size := int64(len(cfg.content))
	listing := listOf(fmt.Sprintf(`{"type":"file","oid":%q,"size":%d,"path":"config.json"}`, cfg.blob, size))
	for name, tc := range map[string]struct {
		body     *countingBody
		wantErr  error
		wantRead int64
	}{
		"a body that never ends":     {&countingBody{limit: -1}, ErrSizeMismatch, size + 1},
		"a body one byte too long":   {&countingBody{limit: size + 1}, ErrSizeMismatch, size + 1},
		"a body one byte too short":  {&countingBody{limit: size - 1}, ErrSizeMismatch, size - 1},
		"the right length, bad data": {&countingBody{limit: size}, ErrBlobMismatch, size}, // every byte is 'x'
	} {
		c, err := New(Config{BaseURL: "http://127.0.0.1:1", HTTPClient: &http.Client{Transport: bodyTransport(listing, tc.body)}})
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.Content(context.Background(), testRepo, testRev, "config.json")
		if !errors.Is(err, tc.wantErr) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.wantErr)
		}
		if got != nil {
			t.Errorf("%s: bytes were returned", name)
		}
		if tc.body.n != tc.wantRead {
			t.Errorf("%s: %d bytes were read, want %d (the listed size is %d)", name, tc.body.n, tc.wantRead, size)
		}
	}
}

// failingBody fails if it is read.
type failingBody struct{ read bool }

func (b *failingBody) Read([]byte) (int, error) {
	b.read = true
	return 0, errors.New("the body must not be read")
}
func (b *failingBody) Close() error { return nil }

func TestContentRefusesALengthHeaderThatDiffersFromTheListingWithoutReadingTheBody(t *testing.T) {
	cfg := fixtureNamed("config.json")
	size := int64(len(cfg.content))
	listing := listOf(fmt.Sprintf(`{"type":"file","oid":%q,"size":%d,"path":"config.json"}`, cfg.blob, size))
	for _, claimed := range []int64{size - 1, size + 1, 0} {
		body := &failingBody{}
		c, err := New(Config{BaseURL: "http://127.0.0.1:1", HTTPClient: &http.Client{Transport: lengthTransport(listing, body, claimed)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Content(context.Background(), testRepo, testRev, "config.json"); !errors.Is(err, ErrSizeMismatch) {
			t.Errorf("Content-Length %d for %d listed bytes: err = %v, want ErrSizeMismatch", claimed, size, err)
		}
		if body.read {
			t.Errorf("Content-Length %d: the body was read", claimed)
		}
	}
}

func TestContentFailsForAPathTheRevisionDoesNotHave(t *testing.T) {
	h := newFakeHub(t, allFiles())
	c := h.client(t, Config{})
	for name, path := range map[string]string{
		"absent":                "no-such-file.json",
		"a directory":           "tokenizer",
		"another case":          "Config.json",
		"not at the root":       "config.json/x",
		"a longer name":         "config.json.bak",
		"only the base name":    "map.json",
		"a missing sub-path":    "tokenizer/map.json",
		"a trailing slash":      "config.json/",
		"a leading slash":       "/config.json",
		"an empty segment":      "tokenizer//tokenizer.json",
		"a parent segment":      "tokenizer/../config.json",
		"a dot segment":         "./config.json",
		"empty":                 "",
		"a control character":   "config.json\n",
		"a backslash":           "tokenizer\\tokenizer.json",
		"not valid UTF-8":       "\xff.json",
		"an escaped slash form": "tokenizer%2Ftokenizer.json",
	} {
		got, err := c.Content(context.Background(), testRepo, testRev, path)
		if !errors.Is(err, ErrNoSuchFile) {
			t.Errorf("%s (%q): err = %v, want ErrNoSuchFile", name, path, err)
		}
		if got != nil {
			t.Errorf("%s: bytes were returned", name)
		}
	}
	if _, resolves := h.requests(); len(resolves) != 0 {
		t.Errorf("a file was downloaded for a path the revision does not have: %q", resolves)
	}
	// ErrNoSuchFile is its own error: not the Hub's 404.
	_, err := c.Content(context.Background(), testRepo, testRev, "no-such-file.json")
	if errors.Is(err, ErrNotFound) || Retryable(err) {
		t.Errorf("err = %v: ErrNoSuchFile must not match ErrNotFound and is not retryable", err)
	}
}

func TestContentRefusesAPathThatCannotBeListedBeforeAnyRequest(t *testing.T) {
	h := newFakeHub(t, allFiles())
	for _, path := range []string{"", "/config.json", "tokenizer//tokenizer.json", "tokenizer/../config.json", "./config.json", "config.json\n", "\xff.json"} {
		if _, err := h.client(t, Config{}).Content(context.Background(), testRepo, testRev, path); !errors.Is(err, ErrNoSuchFile) {
			t.Errorf("%q: err = %v, want ErrNoSuchFile", path, err)
		}
	}
	if got := h.authHeaders(); len(got) != 0 {
		t.Errorf("%d requests were made for paths that no listing can hold", len(got))
	}
}

func TestContentRefusesAFileAboveTheLimitWithoutDownloadingIt(t *testing.T) {
	files := withFiles(lfsContentFile("small.safetensors", lfsABC, sha256ABC))
	for i := range files {
		if files[i].path == "config.json" || files[i].path == "small.safetensors" {
			files[i].size = 10_000 // the listing says so; the bytes served are small
		}
	}
	h := newFakeHub(t, files)
	c := h.client(t, Config{MaxFileBytes: 1000})
	for _, path := range []string{"config.json", "small.safetensors"} {
		got, err := c.Content(context.Background(), testRepo, testRev, path)
		if !errors.Is(err, ErrFileTooLarge) {
			t.Errorf("%s: err = %v, want ErrFileTooLarge", path, err)
		}
		if got != nil {
			t.Errorf("%s: bytes were returned", path)
		}
	}
	if _, resolves := h.requests(); len(resolves) != 0 {
		t.Errorf("an oversized file was downloaded: %q", resolves)
	}
	// At exactly the limit the file is read.
	files = allFiles()
	h = newFakeHub(t, files)
	size := int64(len(fixtureNamed("config.json").content))
	if _, err := h.client(t, Config{MaxFileBytes: size}).Content(context.Background(), testRepo, testRev, "config.json"); err != nil {
		t.Errorf("a file of exactly MaxFileBytes: %v", err)
	}
	if _, err := h.client(t, Config{MaxFileBytes: size - 1}).Content(context.Background(), testRepo, testRev, "config.json"); !errors.Is(err, ErrFileTooLarge) {
		t.Errorf("a file one byte over MaxFileBytes: err = %v, want ErrFileTooLarge", err)
	}
}

func TestContentRefusesABadRevisionOrRepositoryBeforeAnyRequest(t *testing.T) {
	h := newFakeHub(t, allFiles())
	c := h.client(t, Config{})
	for _, rev := range []string{"", "main", testRev[:7], strings.ToUpper(testRev), testRev + "0"} {
		if _, err := c.Content(context.Background(), testRepo, rev, "config.json"); !errors.Is(err, ErrInvalidRevision) {
			t.Errorf("revision %q: err = %v, want ErrInvalidRevision", rev, err)
		}
	}
	for _, repo := range []string{"", "../x", "a/b/c", "owner/", "owner/na me"} {
		if _, err := c.Content(context.Background(), repo, testRev, "config.json"); !errors.Is(err, ErrInvalidRepository) {
			t.Errorf("repository %q: err = %v, want ErrInvalidRepository", repo, err)
		}
	}
	if got := h.authHeaders(); len(got) != 0 {
		t.Errorf("%d requests were made for refused arguments", len(got))
	}
}

func TestContentRefusesAStoredEncodingItCannotCheck(t *testing.T) {
	h := newFakeHub(t, allFiles())
	var acceptEncoding []string
	inner := h.srv.Config.Handler
	h.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/resolve/") {
			acceptEncoding = append(acceptEncoding, r.Header.Get("Accept-Encoding"))
			w.Header().Set("Content-Encoding", "gzip")
		}
		inner.ServeHTTP(w, r)
	})
	got, err := h.client(t, Config{}).Content(context.Background(), testRepo, testRev, "config.json")
	if err == nil || !strings.Contains(err.Error(), "Content-Encoding") {
		t.Fatalf("err = %v, want a refusal that names Content-Encoding", err)
	}
	if got != nil {
		t.Errorf("bytes were returned: %q", got)
	}
	if !slices.Equal(acceptEncoding, []string{"identity"}) {
		t.Errorf("Accept-Encoding of the download = %q, want identity", acceptEncoding)
	}
}

func TestContentSendsTheTokenOnlyInTheAuthorizationHeader(t *testing.T) {
	h := newFakeHub(t, allFiles())
	h.token = testToken
	h.pageSize = 8
	path := writeToken(t, testToken+"\n", 0o600)
	got, err := h.client(t, Config{TokenFile: path}).Content(context.Background(), testRepo, testRev, "config.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != fixtureNamed("config.json").content {
		t.Errorf("content = %q", got)
	}
	auths := h.authHeaders()
	tree, resolves := h.requests()
	if len(auths) != len(tree)+len(resolves) || len(resolves) != 1 || len(tree) < 2 {
		t.Fatalf("%d requests recorded for %d tree and %d download requests", len(auths), len(tree), len(resolves))
	}
	for i, a := range auths {
		if a != "Bearer "+testToken {
			t.Errorf("request %d carried %q, want the bearer token", i, a)
		}
	}
	for _, u := range append(tree, resolves...) {
		if strings.Contains(u, testToken) {
			t.Errorf("the token is in a URL: %s", u)
		}
	}
}

func TestContentDropsTheTokenOnADownloadRedirectedToAnotherHost(t *testing.T) {
	other := recordingOrigin(t)
	h := newFakeHub(t, allFiles())
	h.token = testToken
	h.redirectTo = other.URL
	path := writeToken(t, testToken, 0o600)
	got, err := h.client(t, Config{TokenFile: path}).Content(context.Background(), testRepo, testRev, "config.json")
	if err != nil {
		t.Fatalf("Content through a redirect: %v", err)
	}
	if string(got) != fixtureNamed("config.json").content {
		t.Errorf("content = %q", got)
	}
	auths := other.authHeaders()
	if len(auths) != 1 {
		t.Fatalf("the other host served %d requests, want the one download", len(auths))
	}
	if auths[0] != "" {
		t.Errorf("the other host received an Authorization header")
	}
}

func TestContentReadsTheTokenFileOnEveryCall(t *testing.T) {
	h := newFakeHub(t, allFiles())
	h.token = testToken
	path := writeToken(t, "hf_stale_token_value", 0o600)
	c := h.client(t, Config{TokenFile: path})
	if _, err := c.Content(context.Background(), testRepo, testRev, "config.json"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("with the stale token: err = %v, want ErrUnauthorized", err)
	}
	if err := os.WriteFile(path, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Content(context.Background(), testRepo, testRev, "config.json"); err != nil {
		t.Errorf("after the token file was rotated: %v", err)
	}
}

func TestContentRefusesATokenFileThatIsTooOpenBeforeAnyRequest(t *testing.T) {
	h := newFakeHub(t, allFiles())
	_, err := h.client(t, Config{TokenFile: writeToken(t, testToken, 0o644)}).Content(context.Background(), testRepo, testRev, "config.json")
	if !errors.Is(err, ErrTokenFile) {
		t.Fatalf("err = %v, want ErrTokenFile", err)
	}
	if got := h.authHeaders(); len(got) != 0 {
		t.Errorf("%d requests were made with a token file that is too open", len(got))
	}
}

func TestContentErrorsNeverHoldTheToken(t *testing.T) {
	// A 401 on the download, from a Hub that echoes the Authorization header
	// in a header and in the body.
	h := newFakeHub(t, allFiles())
	h.denyDownloads = true
	_, err := h.client(t, Config{TokenFile: writeToken(t, testToken, 0o600)}).Content(context.Background(), testRepo, testRev, "config.json")
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
	leakCheck(t, "401 on the download", err, testToken, "Bearer")

	// A lower layer whose own error text holds the Authorization header: the
	// last line of defence blanks it and keeps the classification.
	// First prove the transport does leak, or the check below would pass for
	// a transport that never repeated anything.
	probe, _ := http.NewRequest(http.MethodGet, h.srv.URL+"/"+testRepo+"/resolve/"+testRev+"/config.json", nil)
	probe.Header.Set("Authorization", "Bearer "+testToken)
	if _, rawErr := leakyTransport()(probe); rawErr == nil || !strings.Contains(rawErr.Error(), testToken) {
		t.Fatalf("the transport does not leak the header (%v); the check below would be vacuous", rawErr)
	}
	h2 := newFakeHub(t, allFiles())
	c := h2.client(t, Config{TokenFile: writeToken(t, testToken, 0o600), HTTPClient: &http.Client{Transport: leakyTransport()}})
	got, err := c.Content(context.Background(), testRepo, testRev, "config.json")
	if err == nil || got != nil {
		t.Fatalf("Content = %q, %v; want the transport's failure", got, err)
	}
	if !strings.Contains(err.Error(), redacted) {
		t.Errorf("the error does not show the token was blanked: %v", err)
	}
	leakCheck(t, "transport error", err, testToken, "Bearer "+testToken)
	if !Retryable(err) {
		t.Errorf("the scrubbed error lost its classification (a cut connection is retryable): %v", err)
	}
}
