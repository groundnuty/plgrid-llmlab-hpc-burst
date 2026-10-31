package hubfiles

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

// wantPlain is the expected Entry of a fixture, from the pasted constants.
func wantPlain(f fixture) Entry {
	e := Entry{BlobID: f.blob, Size: int64(len(f.content))}
	e.Path = f.path
	e.SHA256 = f.sha256
	return e
}

// wantAll is the expected result for allFiles(): sorted bytewise by path.
func wantAll() []Entry {
	var want []Entry
	for _, f := range fixtures {
		want = append(want, wantPlain(f))
	}
	for _, l := range []struct {
		path, sha string
		size      int64
	}{{lfsPath1, sha256ABC, 30_000_000_000}, {lfsPath2, sha256Empty, 6_000_000_000}} {
		e := Entry{BlobID: pointerOID, Size: l.size, LFS: true}
		e.Path = l.path
		e.SHA256 = l.sha
		want = append(want, e)
	}
	slices.SortFunc(want, func(a, b Entry) int { return strings.Compare(a.Path, b.Path) })
	return want
}

func paths(entries []Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Path
	}
	return out
}

func TestFilesFollowsAListingSplitAcrossThreePages(t *testing.T) {
	h := newFakeHub(t, allFiles())
	h.pageSize = 6 // 13 files and 4 directories: 17 entries, so pages of 6, 6 and 5
	entries, err := h.client(t, Config{}).Files(context.Background(), testRepo, testRev)
	if err != nil {
		t.Fatalf("Files: %v", err)
	}

	tree, _ := h.requests()
	if len(tree) != 3 {
		t.Fatalf("tree requests = %d, want 3 (one per page): %v", len(tree), tree)
	}
	// Each follow-up request is the previous page's Link, byte for byte: the
	// cursor holds %3D%3D and must not be re-encoded.
	for i := 1; i < 3; i++ {
		if want := strings.TrimPrefix(h.links[i-1], h.srv.URL); tree[i] != want {
			t.Errorf("request %d = %q, want the Link of page %d, %q", i+1, tree[i], i, want)
		}
	}
	if !strings.Contains(h.links[0], "%3D") {
		t.Fatalf("the fake's cursor %q lost its escaped padding; the verbatim check proves nothing", h.links[0])
	}
	// Page 3 must have contributed files, and they must be in the result.
	last := h.pages[2]
	if len(last) == 0 {
		t.Fatalf("page 3 holds no file; the test would pass without following it")
	}
	for _, p := range last {
		if !slices.Contains(paths(entries), p) {
			t.Errorf("file %q of page 3 is missing from the result", p)
		}
	}
	if got, want := entries, wantAll(); !slices.Equal(got, want) {
		t.Errorf("entries differ from the expected list:\n got  %+v\n want %+v", got, want)
	}
}

func TestFilesSendsRecursiveAndPageSize(t *testing.T) {
	h := newFakeHub(t, allFiles())
	if _, err := h.client(t, Config{PageSize: 7}).Files(context.Background(), testRepo, testRev); err != nil {
		t.Fatal(err)
	}
	tree, _ := h.requests()
	if !strings.Contains(tree[0], "recursive=true") || !strings.Contains(tree[0], "limit=7") {
		t.Errorf("first request = %q, want recursive=true and limit=7", tree[0])
	}
	h = newFakeHub(t, allFiles())
	if _, err := h.client(t, Config{}).Files(context.Background(), testRepo, testRev); err != nil {
		t.Fatal(err)
	}
	tree, _ = h.requests()
	if len(tree) != 1 || strings.Contains(tree[0], "limit=") {
		t.Errorf("default requests = %v, want one request without limit", tree)
	}
}

func TestFilesSortsBytewise(t *testing.T) {
	h := newFakeHub(t, allFiles())
	entries, err := h.client(t, Config{PageSize: 5}).Files(context.Background(), testRepo, testRev)
	if err != nil {
		t.Fatal(err)
	}
	// Bytewise: uppercase before lowercase, '-' (0x2d) and '.' (0x2e) before
	// '/' (0x2f), '/' before digits and letters, and non-ASCII (UTF-8 bytes
	// above 0x7f) last. A segment-aware sort would put "a/b.json" before
	// "a.json". Written out by hand, not derived from the sort the package uses.
	want := []string{
		".gitattributes",
		"B.json",
		"a-b.json",
		"a.json",
		"a/b.json",
		"config.json",
		"dir with space/file #1.txt",
		"empty.txt",
		lfsPath1,
		lfsPath2,
		"tokenizer/special/map.json",
		"tokenizer/tokenizer.json",
		"é.txt",
	}
	if got := paths(entries); !slices.Equal(got, want) {
		t.Errorf("order = %q\nwant    %q", got, want)
	}
}

func TestFilesDropsDirectories(t *testing.T) {
	h := newFakeHub(t, allFiles())
	entries, err := h.client(t, Config{}).Files(context.Background(), testRepo, testRev)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"a", "tokenizer", "tokenizer/special", "dir with space"} {
		if slices.Contains(paths(entries), d) {
			t.Errorf("directory %q is in the result", d)
		}
	}
	if len(entries) != len(allFiles()) {
		t.Errorf("%d entries, want %d files", len(entries), len(allFiles()))
	}
}

func TestLFSEntryUsesLFSOIDAndIsNeverDownloaded(t *testing.T) {
	h := newFakeHub(t, allFiles())
	entries, err := h.client(t, Config{}).Files(context.Background(), testRepo, testRev)
	if err != nil {
		t.Fatal(err)
	}
	_, resolves := h.requests()
	for _, r := range resolves {
		if r == lfsPath1 || r == lfsPath2 {
			t.Errorf("the LFS file %q was downloaded", r)
		}
	}
	if len(resolves) != len(fixtures) {
		t.Errorf("%d downloads, want %d (the non-LFS files only): %v", len(resolves), len(fixtures), resolves)
	}
	for _, e := range entries {
		switch e.Path {
		case lfsPath1:
			if e.SHA256 != sha256ABC || !e.LFS || e.BlobID != pointerOID {
				t.Errorf("%s = %+v, want lfs.oid %s", e.Path, e, sha256ABC)
			}
		case lfsPath2:
			if e.SHA256 != sha256Empty || !e.LFS {
				t.Errorf("%s = %+v, want lfs.oid %s", e.Path, e, sha256Empty)
			}
		}
	}
}

func TestFilesDownloadsEscapedPaths(t *testing.T) {
	h := newFakeHub(t, allFiles())
	if _, err := h.client(t, Config{Concurrency: 1}).Files(context.Background(), testRepo, testRev); err != nil {
		t.Fatal(err)
	}
	_, resolves := h.requests()
	// A name with a space and '#' must arrive whole; unescaped, '#' would cut
	// the URL and the fake would answer 404.
	for _, want := range []string{"dir with space/file #1.txt", "é.txt", "a/b.json"} {
		if !slices.Contains(resolves, want) {
			t.Errorf("no download of %q in %q", want, resolves)
		}
	}
}

func TestFilesRefusesAnyRevisionButAFullCommitBeforeAnyRequest(t *testing.T) {
	for _, rev := range []string{
		"main",
		"refs/heads/main",
		"v1.0",
		"0123456", // a 7-character id
		"0123456789abcdef0123456789abcdef0123456",   // 39 hex
		"0123456789abcdef0123456789abcdef012345678", // 41 hex
		"0123456789ABCDEF0123456789ABCDEF01234567",  // 40 hex, uppercase
		" 0123456789abcdef0123456789abcdef01234567",
		"0123456789abcdef0123456789abcdef01234567\n",
		strings.Repeat("a", 64), // a 64-hex id is not a Hub commit id
		"",
	} {
		h := newFakeHub(t, allFiles())
		_, err := h.client(t, Config{}).Files(context.Background(), testRepo, rev)
		if !errors.Is(err, ErrInvalidRevision) {
			t.Errorf("revision %q: err = %v, want ErrInvalidRevision", rev, err)
		}
		if tree, resolves := h.requests(); len(tree)+len(resolves) != 0 {
			t.Errorf("revision %q: %d requests reached the server", rev, len(tree)+len(resolves))
		}
	}
}

func TestFilesRefusesABadRepositoryBeforeAnyRequest(t *testing.T) {
	for _, repo := range []string{"", "..", "../x", "a/../b", "a/b/c", "a b/c", "a/b?x=1", "/a/b", "a/b/", "a//b", "-a/b", "a/.hidden"} {
		h := newFakeHub(t, allFiles())
		_, err := h.client(t, Config{}).Files(context.Background(), repo, testRev)
		if !errors.Is(err, ErrInvalidRepository) {
			t.Errorf("repository %q: err = %v, want ErrInvalidRepository", repo, err)
		}
		if tree, resolves := h.requests(); len(tree)+len(resolves) != 0 {
			t.Errorf("repository %q: %d requests reached the server", repo, len(tree)+len(resolves))
		}
	}
}

func TestFilesChecksTheBlobIDOfEveryDownload(t *testing.T) {
	files := allFiles()
	for i := range files {
		if files[i].path == "config.json" {
			// The listing's oid is another file's blob id: the content does
			// not match the Hub's own claim about it.
			files[i].oid = fixtures[0].blob
		}
	}
	h := newFakeHub(t, files)
	entries, err := h.client(t, Config{}).Files(context.Background(), testRepo, testRev)
	if !errors.Is(err, ErrBlobMismatch) {
		t.Fatalf("err = %v, want ErrBlobMismatch", err)
	}
	if !strings.Contains(err.Error(), "config.json") {
		t.Errorf("the error does not name the file: %v", err)
	}
	if entries != nil {
		t.Errorf("a failed call returned a partial list: %+v", entries)
	}
}

func TestFilesRefusesAContentThatDiffersFromTheListedSize(t *testing.T) {
	for name, listed := range map[string]int64{"listing says longer": 100, "listing says shorter": 3} {
		files := allFiles()
		for i := range files {
			if files[i].path == "config.json" {
				files[i].size = listed
			}
		}
		h := newFakeHub(t, files)
		_, err := h.client(t, Config{}).Files(context.Background(), testRepo, testRev)
		if !errors.Is(err, ErrSizeMismatch) {
			t.Errorf("%s: err = %v, want ErrSizeMismatch", name, err)
		}
	}
}

func TestFilesRefusesALargeFileThatIsNotInLFSWithoutDownloadingIt(t *testing.T) {
	files := allFiles()
	for i := range files {
		if files[i].path == "config.json" {
			files[i].size = 10_000
		}
	}
	h := newFakeHub(t, files)
	_, err := h.client(t, Config{MaxFileBytes: 1000}).Files(context.Background(), testRepo, testRev)
	if !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("err = %v, want ErrFileTooLarge", err)
	}
	if _, resolves := h.requests(); slices.Contains(resolves, "config.json") {
		t.Error("the oversized file was downloaded")
	}
}

func TestFilesAnswers401ForAGatedRepoWithoutAToken(t *testing.T) {
	h := newFakeHub(t, allFiles())
	h.token = "hf_gated_secret_value_0123456789"
	_, err := h.client(t, Config{}).Files(context.Background(), testRepo, testRev)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != http.StatusUnauthorized || he.TokenSent {
		t.Errorf("HTTPError = %+v, want status 401 with no token sent", he)
	}
	if !strings.Contains(err.Error(), "token") || !strings.Contains(err.Error(), "GatedRepo") {
		t.Errorf("the error does not say a token is needed: %v", err)
	}
	if got := h.authHeaders(); len(got) != 1 {
		t.Errorf("%d requests reached the server, want 1 (the refused listing): %q", len(got), got)
	}
}

func TestFilesReportsStatusSentinels(t *testing.T) {
	h := newFakeHub(t, allFiles())
	_, err := h.client(t, Config{}).Files(context.Background(), "example-org/other", testRev)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown repository: err = %v, want ErrNotFound", err)
	}
	for status, want := range map[int]error{http.StatusForbidden: ErrForbidden, http.StatusUnauthorized: ErrUnauthorized, http.StatusNotFound: ErrNotFound} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		c, err := New(Config{BaseURL: srv.URL})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Files(context.Background(), testRepo, testRev)
		srv.Close()
		if !errors.Is(err, want) {
			t.Errorf("status %d: err = %v, want %v", status, err, want)
		}
	}
}

func TestFilesIsWholeOrNothing(t *testing.T) {
	// A failure in one download fails the call; the result is nil, not a list
	// with a hole that would hash to another deployment ID.
	files := allFiles()
	files = append(files, fakeFile{path: "zz-missing.json", oid: pointerOID, size: 4, missing: true})
	h := newFakeHub(t, files)
	entries, err := h.client(t, Config{}).Files(context.Background(), testRepo, testRev)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound for the file the Hub lists but cannot serve", err)
	}
	if entries != nil {
		t.Errorf("partial result: %+v", entries)
	}
}

func TestFilesHonoursContextCancellation(t *testing.T) {
	h := newFakeHub(t, allFiles())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := h.client(t, Config{}).Files(ctx, testRepo, testRev)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestFilesRefusesAnUntrustworthyListing(t *testing.T) {
	good := plainFile(fixtures[1])
	for name, bad := range map[string]fakeFile{
		"parent segment":        {path: "../escape.json", oid: fixtures[1].blob, size: -1, content: fixtures[1].content},
		"absolute path":         {path: "/etc/passwd", oid: fixtures[1].blob, size: -1},
		"empty segment":         {path: "a//b.json", oid: fixtures[1].blob, size: -1},
		"dot segment":           {path: "./x.json", oid: fixtures[1].blob, size: -1},
		"backslash":             {path: `a\b.json`, oid: fixtures[1].blob, size: -1},
		"duplicate path":        {path: good.path, content: good.content, oid: good.oid, size: -1},
		"lfs.oid not 64 hex":    {path: "m.safetensors", oid: pointerOID, size: 5, lfsOID: "ABC"},
		"lfs.oid uppercase":     {path: "m.safetensors", oid: pointerOID, size: 5, lfsOID: strings.ToUpper(sha256ABC)},
		"blob id not 40 hex":    {path: "x.json", oid: "abc", size: -1},
		"blob id missing":       {path: "x.json", oid: "", size: -1},
		"blob id uppercase hex": {path: "x.json", oid: strings.ToUpper(fixtures[1].blob), size: -1},
	} {
		h := newFakeHub(t, []fakeFile{good, bad})
		_, err := h.client(t, Config{}).Files(context.Background(), testRepo, testRev)
		if !errors.Is(err, ErrInvalidListing) {
			t.Errorf("%s: err = %v, want ErrInvalidListing", name, err)
		}
		if _, resolves := h.requests(); len(resolves) != 0 {
			t.Errorf("%s: a download was made before the listing was validated: %v", name, resolves)
		}
	}
}

func TestFilesRefusesAnEmptyListingAndAnUnknownEntryType(t *testing.T) {
	h := newFakeHub(t, nil)
	if _, err := h.client(t, Config{}).Files(context.Background(), testRepo, testRev); !errors.Is(err, ErrInvalidListing) {
		t.Errorf("empty listing: err = %v, want ErrInvalidListing", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"type":"submodule","oid":"` + pointerOID + `","size":0,"path":"vendor/x"}]`))
	}))
	defer srv.Close()
	c, err := New(Config{BaseURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Files(context.Background(), testRepo, testRev); !errors.Is(err, ErrInvalidListing) {
		t.Errorf("unknown type: err = %v, want ErrInvalidListing", err)
	}
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`<html>not json`)) }))
	defer srv2.Close()
	c2, _ := New(Config{BaseURL: srv2.URL})
	if _, err := c2.Files(context.Background(), testRepo, testRev); !errors.Is(err, ErrInvalidListing) {
		t.Errorf("non-JSON listing: err = %v, want ErrInvalidListing", err)
	}
}

func TestFilesRefusesAPageChainThatLoopsOrNeverEnds(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Every page points to itself.
		w.Header().Set("Link", "<"+srv.URL+r.URL.RequestURI()+`>; rel="next"`)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	c, err := New(Config{BaseURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Files(context.Background(), testRepo, testRev); !errors.Is(err, ErrTooManyPages) {
		t.Errorf("loop: err = %v, want ErrTooManyPages", err)
	}

	// Distinct links forever: the page cap stops it.
	n := 0
	var srv2 *httptest.Server
	srv2 = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		w.Header().Set("Link", "<"+srv2.URL+"/api/models/x/tree/y?page="+strings.Repeat("p", n)+`>; rel="next"`)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv2.Close()
	c2, err := New(Config{BaseURL: srv2.URL, MaxPages: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c2.Files(context.Background(), testRepo, testRev); !errors.Is(err, ErrTooManyPages) {
		t.Errorf("endless chain: err = %v, want ErrTooManyPages", err)
	}
	if n != 5 {
		t.Errorf("%d pages fetched, want exactly the cap of 5", n)
	}
}

func TestNextLinkParsing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		want   string
	}{
		{"plain", []string{`<https://h/p?c=1%3D%3D>; rel="next"`}, "https://h/p?c=1%3D%3D"},
		{"unquoted rel", []string{`<https://h/p>; rel=next`}, "https://h/p"},
		{"other relation only", []string{`<https://h/p>; rel="prev"`}, ""},
		{"two links in one value", []string{`<https://h/a>; rel="prev", <https://h/b>; rel="next"`}, "https://h/b"},
		{"two header lines", []string{`<https://h/a>; rel="prev"`, `<https://h/b>; rel="next"`}, "https://h/b"},
		{"rel with several values", []string{`<https://h/a>; rel="next last"`}, "https://h/a"},
		{"extra parameter first", []string{`<https://h/a>; title="x"; rel="next"`}, "https://h/a"},
		{"none", nil, ""},
		{"garbage", []string{`not a link`}, ""},
	} {
		if got := nextLink(tc.values); got != tc.want {
			t.Errorf("%s: nextLink(%q) = %q, want %q", tc.name, tc.values, got, tc.want)
		}
	}
}

func TestNewValidatesTheConfiguration(t *testing.T) {
	for name, cfg := range map[string]Config{
		"plain http, remote host": {BaseURL: "http://hub.example.org"},
		"ftp":                     {BaseURL: "ftp://hub.example.org"},
		"a path":                  {BaseURL: "https://hub.example.org/api"},
		"a query":                 {BaseURL: "https://hub.example.org?x=1"},
		"user info":               {BaseURL: "https://user:pw@hub.example.org"},
		"no host":                 {BaseURL: "https://"},
		"page size below 0":       {PageSize: -1},
		"page size above 1000":    {PageSize: 1001},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: New accepted %+v", name, cfg)
		}
	}
	for name, cfg := range map[string]Config{
		"default":            {},
		"loopback http":      {BaseURL: "http://127.0.0.1:8080"},
		"localhost http":     {BaseURL: "http://localhost:8080"},
		"ipv6 loopback http": {BaseURL: "http://[::1]:8080"},
		"trailing slash":     {BaseURL: "https://hub.example.org/"},
	} {
		if _, err := New(cfg); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
