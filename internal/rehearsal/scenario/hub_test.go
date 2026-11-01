package scenario_test

// The local Hub double: the Hugging Face endpoints the controller lists a
// checkpoint with (GET /api/models/<repo>/tree/<rev>) and the agent downloads
// from (GET /<repo>/resolve/<rev>/<path>), for one small repository. The
// listing's ids are computed from the content as the Hub defines them: a git
// blob id, sha1("blob <size>\0" + content), for every file, and the SHA-256 as
// lfs.oid for an LFS file (pkg/hubfiles/download.go).

import (
	"crypto/sha1" // the git blob id is a SHA-1 by definition
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
)

type hubFile struct {
	path    string
	content []byte
	lfs     bool
}

// hubFiles is the rehearsal checkpoint: a config and a small weights file in
// LFS, as a real checkpoint has.
var hubFiles = []hubFile{
	{path: "config.json", content: []byte(`{"model_type":"rehearsal","max_position_embeddings":4096}` + "\n")},
	{path: "generation_config.json", content: []byte(`{"temperature":0.7}` + "\n")},
	{path: "model.safetensors", content: []byte(strings.Repeat("rehearsal weights\n", 512)), lfs: true},
}

type hub struct {
	url string
	srv *http.Server

	mu       sync.Mutex
	resolves []string // the path of each resolve request, in order
}

func gitBlobID(b []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(b))
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func startHub(t *testing.T) *hub {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := &hub{url: "http://" + ln.Addr().String()}
	h.srv = &http.Server{Handler: http.HandlerFunc(h.handle)}
	go func() { _ = h.srv.Serve(ln) }() // ErrServerClosed at Close
	t.Cleanup(func() { _ = h.srv.Close() })
	return h
}

func (h *hub) handle(w http.ResponseWriter, r *http.Request) {
	tree := "/api/models/" + checkpointRepo + "/tree/" + checkpointRev
	resolve := "/" + checkpointRepo + "/resolve/" + checkpointRev + "/"
	switch {
	case r.URL.Path == tree:
		var entries []map[string]any
		for _, f := range hubFiles {
			e := map[string]any{"type": "file", "oid": gitBlobID(f.content), "size": len(f.content), "path": f.path}
			if f.lfs {
				e["lfs"] = map[string]any{"oid": sha256Hex(f.content), "size": len(f.content), "pointerSize": 135}
			}
			entries = append(entries, e)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(entries)
	case strings.HasPrefix(r.URL.Path, resolve):
		p := strings.TrimPrefix(r.URL.Path, resolve)
		h.mu.Lock()
		h.resolves = append(h.resolves, p)
		h.mu.Unlock()
		for _, f := range hubFiles {
			if f.path == p {
				w.Header().Set("Content-Length", fmt.Sprint(len(f.content)))
				_, _ = w.Write(f.content)
				return
			}
		}
		http.NotFound(w, r)
	default:
		http.NotFound(w, r)
	}
}

// resolved is a copy of the paths downloaded so far.
func (h *hub) resolved() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.resolves...)
}
