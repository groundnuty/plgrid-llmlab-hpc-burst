package lifecycle

// The Hub origin the lifecycle controller lists checkpoints from (issue #146,
// ruling of 1 Nov 2026): Options.HubEndpoint, which hpcburst-controller sets
// from HF_ENDPOINT, and https://huggingface.co when it is empty. Expected
// values are the Hub's own address and the listing a loopback double served.

import (
	"context"
	"crypto/sha1" // the git blob id is a SHA-1 by definition
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestCheckpointSourceListsFromHubEndpoint(t *testing.T) {
	const repo, rev = "example-org/example-model", "0123456789abcdef0123456789abcdef01234567"
	content := []byte(`{"model_type":"example"}` + "\n")
	blob := sha1.New()
	fmt.Fprintf(blob, "blob %d\x00", len(content))
	blob.Write(content)
	var mu sync.Mutex
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		switch r.URL.Path {
		case "/api/models/" + repo + "/tree/" + rev:
			_ = json.NewEncoder(w).Encode([]map[string]any{{"type": "file", "oid": hex.EncodeToString(blob.Sum(nil)), "size": len(content), "path": "config.json"}})
		case "/" + repo + "/resolve/" + rev + "/config.json":
			_, _ = w.Write(content)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	src := checkpointSource(Options{HubEndpoint: srv.URL}, nil)
	files, err := src.Files(context.Background(), v1alpha1.CheckpointSpec{Repository: repo, Revision: rev})
	if err != nil {
		t.Fatalf("Files from the Hub at %s: %v", srv.URL, err)
	}
	sum := sha256.Sum256(content)
	if len(files) != 1 || files[0].Path != "config.json" || files[0].SHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("files %+v, want config.json with its SHA-256", files)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) == 0 || paths[0] != "/api/models/"+repo+"/tree/"+rev {
		t.Errorf("the endpoint served %v, want the listing first", paths)
	}
}

func TestCheckpointSourceDefaultsToTheHub(t *testing.T) {
	src, ok := checkpointSource(Options{}, nil).(*cachedSource)
	if !ok {
		t.Fatalf("checkpointSource with no options is %T, want the cached Hub source", src)
	}
	hub, ok := src.inner.(*HubSource)
	if !ok || hub.endpoint() != "https://huggingface.co" {
		t.Errorf("the default source %#v lists from %q, want https://huggingface.co", src.inner, hub.endpoint())
	}
	given := &cachedSource{}
	if got := checkpointSource(Options{Checkpoints: given, HubEndpoint: "http://127.0.0.1:1"}, nil); got != given {
		t.Errorf("Options.Checkpoints was replaced by %T", got)
	}
}
