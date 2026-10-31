package hubfiles

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// treeServer is a server for tests that need to say exactly what the Hub
// sends, byte for byte. It counts the requests that reach it.
type treeServer struct {
	*httptest.Server
	mu   sync.Mutex
	uris []string // RequestURI of every request, in arrival order
	auth []string // Authorization header of every request
}

func newTreeServer(t *testing.T, handle http.HandlerFunc) *treeServer {
	t.Helper()
	s := &treeServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.uris = append(s.uris, r.URL.RequestURI())
		s.auth = append(s.auth, r.Header.Get("Authorization"))
		s.mu.Unlock()
		handle(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *treeServer) requests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.uris)
}

func (s *treeServer) authHeaders() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.auth...)
}

func (s *treeServer) client(t *testing.T, cfg Config) *Client {
	t.Helper()
	cfg.BaseURL = s.URL
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// serveBody answers every request with body.
func serveBody(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

// lfsJSON is a listing entry for an LFS file at path: the Hub's shape, with
// the keys this package does not read.
func lfsJSON(path string) string {
	p, _ := json.Marshal(path)
	return `{"type":"file","oid":"` + pointerOID + `","size":3,"path":` + string(p) +
		`,"lfs":{"oid":"` + sha256ABC + `","size":3,"pointerSize":135},"xetHash":"x"}`
}

// listOf is a page that holds the given entries.
func listOf(entries ...string) string { return "[" + strings.Join(entries, ",") + "]" }

// treePath is the path of the recursive listing of the test repository.
const treePath = "/api/models/" + testRepo + "/tree/" + testRev
