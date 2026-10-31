package hubfiles

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fixture is a small file with its git blob id and SHA-256. The values were
// computed once with git hash-object and sha256sum, outside this package, and
// pasted here.
type fixture struct {
	path    string
	content string
	blob    string
	sha256  string
}

var fixtures = []fixture{
	{".gitattributes", "*.safetensors filter=lfs diff=lfs merge=lfs -text\n", "580d310c2d211aca166288a207e972ced9bb0100", "ee05350c7fda0af4da116020dd7b976d731de3beed4f5e717d5bcc95ead24550"},
	{"config.json", "{\"model_type\": \"toy\"}\n", "beafd0b1303476cd8c3cd7c92e31bb3dbb077681", "44e3a3fc8c348d245903a5c19d94ac69019b51f843772795924b8b975206d653"},
	{"tokenizer/tokenizer.json", "{\"version\": \"1.0\"}\n", "92bdb0aff34a95a7c00546c3d608c984294164ed", "e0e77b70ca77d0be8f321590c0deb449d364cc3c4edaeaa6eef40636d56a298e"},
	{"tokenizer/special/map.json", "{\"eos\": 2}\n", "a2402f8862033cf3ddd79cc64398b8e599975d7d", "1326768391e6720393a04ad6a667f8a2d66e26365ef2d31435d37a106ede62f1"},
	{"B.json", "B\n", "223b7836fb19fdf64ba2d3cd6173c6a283141f78", "c0cde77fa8fef97d476c10aad3d2d54fcc2f336140d073651c2dcccf1e379fd6"},
	{"a-b.json", "a-b\n", "7f07527a80bd8c2b1c5087d7ccfe61073b068374", "7d17362cca32429c54dcaf0ffe6e48a16d6ae8f404b46da4f518281c532757e9"},
	{"a.json", "a\n", "78981922613b2afb6025042ff6bd878ac1994e85", "87428fc522803d31065e7bce3cf03fe475096631e5e07bbd7a0fde60c4cf25c7"},
	{"a/b.json", "a/b\n", "0ee729686ab2a0074639c5f64930648571e7c4b2", "241714c62f70695b332fd13a5feeb58b25bf8f3899a0e41d80d2dd6892d8f87a"},
	{"é.txt", "e-acute\n", "1790b5177aa5b07b7eaaa219eecfc864c7120a56", "e5a9e9791231dcb8555026125e3c00f0e99ad566739487560936d6704c1ccd52"},
	{"dir with space/file #1.txt", "spaced\n", "bd4269ff9d6818e647e89bacacf357bc8b8eb33c", "96faa18568f8de6d2be0927265d4f317324564b41ca02188ba5430234a87860d"},
	{"empty.txt", "", "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
}

// The SHA-256 of "abc" and of the empty string, from sha256sum. The fake lists
// two LFS shards under these values; the content is never served.
const (
	sha256ABC   = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	sha256Empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	// A blob id for an LFS pointer file; any 40 lowercase hex will do.
	pointerOID = "0123456789abcdef0123456789abcdef01234567"
)

const (
	testRepo = "example-org/example-1b"
	testRev  = "0123456789abcdef0123456789abcdef01234567"
	lfsPath1 = "model-00001-of-00002.safetensors"
	lfsPath2 = "model-00002-of-00002.safetensors"
)

// fakeFile is one file the fake Hub lists.
type fakeFile struct {
	path    string
	content string // served for a non-LFS file
	oid     string // the git blob id the listing gives
	size    int64  // the listed size; -1 means len(content)
	lfsOID  string // non-empty makes it an LFS entry
	missing bool   // listed, but the resolve endpoint answers 404
}

func plainFile(f fixture) fakeFile {
	return fakeFile{path: f.path, content: f.content, oid: f.blob, size: -1}
}

func lfsFile(path, sha string, size int64) fakeFile {
	return fakeFile{path: path, oid: pointerOID, size: size, lfsOID: sha}
}

// allFiles is every fixture plus two LFS shards.
func allFiles() []fakeFile {
	files := make([]fakeFile, 0, len(fixtures)+2)
	for _, f := range fixtures {
		files = append(files, plainFile(f))
	}
	return append(files, lfsFile(lfsPath1, sha256ABC, 30_000_000_000), lfsFile(lfsPath2, sha256Empty, 6_000_000_000))
}

// fakeHub is an httptest stand-in for the Hub's tree and resolve endpoints. It
// lists directories as the Hub does (so a client must drop them), in an order
// that is not sorted, and cuts the listing into pages of at most pageSize
// entries when pageSize is set, whatever limit the client asks for (the way a
// Hub with a small maximum page behaves).
type fakeHub struct {
	t        *testing.T
	srv      *httptest.Server
	repo     string
	rev      string
	files    []fakeFile
	pageSize int
	// reorder arranges the listing's entries before they are cut into pages.
	reorder func([]map[string]any)
	// token, when set, is required as a bearer token on every request.
	token string
	// linkTo, when set, replaces the host of every Link the fake emits.
	linkTo string
	// denyDownloads makes every resolve request answer 401 (with the echo of
	// the Authorization header that a token-checking Hub would also give).
	denyDownloads bool
	// redirectTo, when set, makes every resolve request a 302 to that origin.
	redirectTo string
	// treeRedirectTo, when set, makes every tree request a 302 to that origin,
	// with the same path and query.
	treeRedirectTo string

	mu       sync.Mutex
	tree     []string   // RequestURI of each tree request
	links    []string   // each Link header the fake emitted
	pages    [][]string // the file paths on each page served
	resolves []string   // decoded path of each resolve request
	auths    []string   // the Authorization header of each request
}

func newFakeHub(t *testing.T, files []fakeFile) *fakeHub {
	t.Helper()
	h := &fakeHub{t: t, repo: testRepo, rev: testRev, files: files, reorder: reverseByPath}
	h.srv = httptest.NewServer(http.HandlerFunc(h.handle))
	t.Cleanup(h.srv.Close)
	return h
}

// reverseByPath puts the entries in reverse path order: directories and files
// interleaved, and the opposite of the order the client must return.
func reverseByPath(entries []map[string]any) {
	sort.Slice(entries, func(i, j int) bool { return entries[i]["path"].(string) > entries[j]["path"].(string) })
}

// rotate returns a reorder function that moves the first n entries to the end
// of the path-sorted list: another order, and with it another page split.
func rotate(n int) func([]map[string]any) {
	return func(entries []map[string]any) {
		sort.Slice(entries, func(i, j int) bool { return entries[i]["path"].(string) < entries[j]["path"].(string) })
		n := n % len(entries)
		rotated := append(append([]map[string]any{}, entries[n:]...), entries[:n]...)
		copy(entries, rotated)
	}
}

func (h *fakeHub) entries(recursive bool) []map[string]any {
	var out []map[string]any
	dirs := map[string]bool{}
	for _, f := range h.files {
		if !recursive && strings.Contains(f.path, "/") {
			dir := f.path[:strings.Index(f.path, "/")]
			if !dirs[dir] {
				dirs[dir] = true
				out = append(out, map[string]any{"type": "directory", "oid": pointerOID, "size": 0, "path": dir})
			}
			continue
		}
		size := int64(len(f.content))
		if f.size >= 0 {
			size = f.size
		}
		e := map[string]any{"type": "file", "oid": f.oid, "size": size, "path": f.path}
		if f.lfsOID != "" {
			e["lfs"] = map[string]any{"oid": f.lfsOID, "size": size, "pointerSize": 135}
		}
		out = append(out, e)
		if !recursive {
			continue
		}
		// A recursive listing also lists every directory above the file.
		for i := 0; i < len(f.path); i++ {
			if f.path[i] == '/' {
				if dir := f.path[:i]; !dirs[dir] {
					dirs[dir] = true
					out = append(out, map[string]any{"type": "directory", "oid": pointerOID, "size": 0, "path": dir})
				}
			}
		}
	}
	return out
}

func (h *fakeHub) handle(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.auths = append(h.auths, r.Header.Get("Authorization"))
	h.mu.Unlock()
	isDownload := strings.Contains(r.URL.Path, "/resolve/")
	if h.token != "" && r.Header.Get("Authorization") != "Bearer "+h.token || h.denyDownloads && isDownload {
		// Echo what we received in the places a careless client might copy
		// into an error: a header and the body.
		got := r.Header.Get("Authorization")
		w.Header().Set("X-Error-Code", "GatedRepo")
		w.Header().Set("X-Error-Message", "Invalid credentials: "+got)
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Invalid credentials: " + got})
		return
	}
	if h.treeRedirectTo != "" && !isDownload {
		http.Redirect(w, r, h.treeRedirectTo+r.URL.RequestURI(), http.StatusFound)
		return
	}
	if h.redirectTo != "" && isDownload {
		rel := (&url.URL{Path: "/" + strings.TrimPrefix(r.URL.Path, "/"+h.repo+"/resolve/"+h.rev+"/")}).EscapedPath()
		http.Redirect(w, r, h.redirectTo+rel, http.StatusFound)
		return
	}
	switch {
	case r.URL.Path == "/api/models/"+h.repo+"/tree/"+h.rev:
		h.serveTree(w, r)
	case strings.HasPrefix(r.URL.Path, "/"+h.repo+"/resolve/"+h.rev+"/"):
		h.serveFile(w, r, strings.TrimPrefix(r.URL.Path, "/"+h.repo+"/resolve/"+h.rev+"/"))
	default:
		http.NotFound(w, r)
	}
}

func (h *fakeHub) serveTree(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	entries := h.entries(q.Get("recursive") == "true")
	h.reorder(entries)
	limit := 0
	if v, err := strconv.Atoi(q.Get("limit")); err == nil && v > 0 {
		limit = v
	}
	if h.pageSize > 0 && (limit == 0 || h.pageSize < limit) {
		limit = h.pageSize
	}
	offset := 0
	if c := q.Get("cursor"); c != "" {
		raw, err := base64.StdEncoding.DecodeString(c)
		if err == nil {
			offset, err = strconv.Atoi(string(raw))
		}
		if err != nil || offset < 0 || offset > len(entries) {
			http.Error(w, "bad cursor", http.StatusBadRequest)
			return
		}
	}
	end := len(entries)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	page := entries[offset:end]
	h.mu.Lock()
	h.tree = append(h.tree, r.RequestURI)
	var paths []string
	for _, e := range page {
		if e["type"] == "file" {
			paths = append(paths, e["path"].(string))
		}
	}
	h.pages = append(h.pages, paths)
	h.mu.Unlock()
	if end < len(entries) {
		cursor := url.QueryEscape(base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end))))
		host := h.srv.URL
		if h.linkTo != "" {
			host = h.linkTo
		}
		link := host + "/api/models/" + h.repo + "/tree/" + h.rev + "?cursor=" + cursor + "&limit=" + strconv.Itoa(limit) + "&recursive=true"
		h.mu.Lock()
		h.links = append(h.links, link)
		h.mu.Unlock()
		w.Header().Set("Link", "<"+link+`>; rel="next"`)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(page)
}

func (h *fakeHub) serveFile(w http.ResponseWriter, r *http.Request, path string) {
	h.mu.Lock()
	h.resolves = append(h.resolves, path)
	h.mu.Unlock()
	for _, f := range h.files {
		if f.path == path && f.lfsOID == "" && !f.missing {
			_, _ = w.Write([]byte(f.content))
			return
		}
	}
	http.NotFound(w, r)
}

// requests returns copies of the fake's records.
func (h *fakeHub) requests() (tree, resolves []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.tree...), append([]string(nil), h.resolves...)
}

// authHeaders returns the Authorization header of every request that reached
// the fake, including the refused ones, in arrival order.
func (h *fakeHub) authHeaders() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.auths...)
}

// client returns a Client for the fake, with cfg's other fields.
func (h *fakeHub) client(t *testing.T, cfg Config) *Client {
	t.Helper()
	cfg.BaseURL = h.srv.URL
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}
