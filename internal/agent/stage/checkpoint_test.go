package stage

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// With RehashOnly nothing is downloaded, moved, locked or created: a missing
// file is absent, a changed one a mismatch with its hash, even on a restage,
// and the checkpoint directory is not made (Opus B3: a verification under a
// running engine downloaded the files it found missing).
func TestRehashOnlyChangesNothing(t *testing.T) {
	s, hub, _ := newStager(t, map[string]string{
		resolvePath("config.json"): "hello weights\n",
		resolvePath("tok.json"):    "hello weights\n",
	})
	req := agent.CheckpointRequest{
		Repository: "org/model", Revision: rev, Restage: true, RehashOnly: true,
		Files: []agentapi.FileDigest{
			{Path: "config.json", SHA256: weightsSum},
			{Path: "good.json", SHA256: weightsSum},
			{Path: "tok.json", SHA256: weightsSum},
		},
	}
	res, err := s.Checkpoint(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range res.Files {
		if f.Outcome != agentapi.FileAbsent {
			t.Errorf("%+v, want absent", f)
		}
	}
	if _, err := os.Stat(res.Dir); !os.IsNotExist(err) {
		t.Errorf("RehashOnly created the checkpoint directory (%v)", err)
	}

	dir := ckptDir(s)
	os.MkdirAll(dir, 0o700)
	os.WriteFile(filepath.Join(dir, "config.json"), []byte("tampered\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "good.json"), []byte("hello weights\n"), 0o600)
	before := listDir(t, dir)
	res, err = s.Checkpoint(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	o := outcomes(res)
	if f := o["config.json"]; f.Outcome != agentapi.FileMismatch || f.SHA256 != tamperedSum {
		t.Errorf("config.json: %+v", f)
	}
	if o["good.json"].Outcome != agentapi.FileMatch || o["tok.json"].Outcome != agentapi.FileAbsent {
		t.Errorf("good.json, tok.json: %+v %+v", o["good.json"], o["tok.json"])
	}
	if after := listDir(t, dir); !slices.Equal(before, after) {
		t.Errorf("the directory changed: %q, then %q", before, after)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "config.json")); string(b) != "tampered\n" {
		t.Errorf("config.json holds %q", b)
	}
	if gets, _ := hub.requests(); len(gets) != 0 {
		t.Errorf("downloaded %q", gets)
	}
}

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err == nil {
			out = append(out, p)
		}
		return nil
	})
	return out
}

// A restage moves the changed file aside, downloads it again and removes the
// copy moved aside once the new file is in place (Opus disk-use nit).
func TestRestageRemovesTheCopyMovedAside(t *testing.T) {
	s, hub, _ := newStager(t, map[string]string{resolvePath("config.json"): "hello weights\n"})
	dir := ckptDir(s)
	os.MkdirAll(dir, 0o700)
	var asides atomic.Int32
	hub.setIntercept(func(int, http.ResponseWriter, *http.Request) bool {
		m, _ := filepath.Glob(filepath.Join(dir, "*.mismatch-*"))
		asides.Store(int32(len(m)))
		return false
	})
	os.WriteFile(filepath.Join(dir, "config.json"), []byte("tampered\n"), 0o600)
	res, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev, Restage: true,
		Files: []agentapi.FileDigest{{Path: "config.json", SHA256: weightsSum}},
	})
	if err != nil || res.Files[0].Outcome != agentapi.FileMatch {
		t.Fatalf("%+v %v", res, err)
	}
	if n := asides.Load(); n != 1 {
		t.Errorf("%d copies moved aside while the download ran, want 1", n)
	}
	if m, _ := filepath.Glob(filepath.Join(dir, "*.mismatch-*")); len(m) != 0 {
		t.Errorf("copies left: %q", m)
	}
}

// A restage whose download fails leaves the file where it was, not moved aside.
func TestFailedRestagePutsTheFileBack(t *testing.T) {
	s, _, _ := newStager(t, nil) // the Hub has nothing: HTTP 404
	dir := ckptDir(s)
	os.MkdirAll(dir, 0o700)
	os.WriteFile(filepath.Join(dir, "config.json"), []byte("tampered\n"), 0o600)
	_, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev, Restage: true,
		Files: []agentapi.FileDigest{{Path: "config.json", SHA256: weightsSum}},
	})
	if err == nil {
		t.Fatal("the download failed but Checkpoint succeeded")
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "config.json")); string(b) != "tampered\n" {
		t.Errorf("config.json holds %q", b)
	}
	if m, _ := filepath.Glob(filepath.Join(dir, "*.mismatch-*")); len(m) != 0 {
		t.Errorf("copies left: %q", m)
	}
}

// Download files left by a run that died are removed once an hour old, in the
// checkpoint's subdirectories too; younger ones, other files and a RehashOnly
// call's view are left alone.
func TestStalePartFilesAreRemoved(t *testing.T) {
	s, _, _ := newStager(t, nil)
	dir := ckptDir(s)
	os.MkdirAll(filepath.Join(dir, "tok"), 0o700)
	old := time.Now().Add(-2 * time.Hour)
	mk := func(rel string, mtime time.Time) string {
		p := filepath.Join(dir, rel)
		os.WriteFile(p, []byte("x"), 0o600)
		os.Chtimes(p, mtime, mtime)
		return p
	}
	stale := []string{mk(".model.safetensors.part-111", old), mk("tok/.tokenizer.json.part-222", old)}
	keep := []string{mk(".model.safetensors.part-333", time.Now().Add(-5*time.Minute)), mk("notes.part-1", old)}
	req := agent.CheckpointRequest{Repository: "org/model", Revision: rev, RehashOnly: true}
	if _, err := s.Checkpoint(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	for _, p := range stale {
		if _, err := os.Lstat(p); err != nil {
			t.Errorf("a RehashOnly call removed %s", filepath.Base(p))
		}
	}
	req.RehashOnly = false
	if _, err := s.Checkpoint(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	for _, p := range stale {
		if _, err := os.Lstat(p); !os.IsNotExist(err) {
			t.Errorf("%s was not removed (%v)", filepath.Base(p), err)
		}
	}
	for _, p := range keep {
		if _, err := os.Lstat(p); err != nil {
			t.Errorf("%s was removed: %v", filepath.Base(p), err)
		}
	}
}

// The token goes to the Hub's host only. The Hub redirects an LFS file to its
// CDN, a subdomain, where Go would keep the Authorization header (Opus nit).
func TestTokenIsNotSentToAnotherHost(t *testing.T) {
	s, hub, _ := newStager(t, map[string]string{"/blob/abc": "hello weights\n"})
	hub.setIntercept(func(_ int, w http.ResponseWriter, r *http.Request) bool {
		if r.Host == "hub.test" {
			http.Redirect(w, r, "http://cdn-lfs.hub.test/blob/abc", http.StatusFound)
			return true
		}
		return false
	})
	res, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev, HubToken: "hf_secret",
		Files: []agentapi.FileDigest{{Path: "model.safetensors", SHA256: weightsSum}},
	})
	if err != nil || res.Files[0].Outcome != agentapi.FileMatch {
		t.Fatalf("%+v %v", res, err)
	}
	want := []hubReq{
		{Path: resolvePath("model.safetensors"), Host: "hub.test", Auth: "Bearer hf_secret"},
		{Path: "/blob/abc", Host: "cdn-lfs.hub.test", Auth: ""},
	}
	if got := hub.all(); !slices.Equal(got, want) {
		t.Errorf("requests %+v, want %+v", got, want)
	}
}

// A redirect loop is cut at ten redirects, as net/http's own policy does.
func TestRedirectLoopIsCut(t *testing.T) {
	s, hub, _ := newStager(t, nil)
	hub.setIntercept(func(_ int, w http.ResponseWriter, r *http.Request) bool {
		http.Redirect(w, r, r.URL.Path, http.StatusFound)
		return true
	})
	_, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev,
		Files: []agentapi.FileDigest{{Path: "config.json", SHA256: weightsSum}},
	})
	if err == nil || !strings.Contains(err.Error(), "10 redirects") {
		t.Fatalf("err %v, want a stop after 10 redirects", err)
	}
}

// A download that fails half way and is tried again counts its bytes once.
func TestProgressDoesNotCountARetriedDownloadTwice(t *testing.T) {
	const content = "hello weights\n"
	s, hub, _ := newStager(t, map[string]string{resolvePath("config.json"): content})
	hub.setIntercept(func(n int, w http.ResponseWriter, r *http.Request) bool {
		if n > 1 {
			return false
		}
		w.Header().Set("Content-Length", "14")
		w.Write([]byte(content[:7]))
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler) // the connection drops half way
	})
	type report struct{ done, total int64 }
	var mu sync.Mutex
	var reports []report
	res, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev,
		Files: []agentapi.FileDigest{{Path: "config.json", SHA256: weightsSum}},
		Progress: func(done, total int64) {
			mu.Lock()
			reports = append(reports, report{done, total})
			mu.Unlock()
		},
	})
	if err != nil || res.Files[0].Outcome != agentapi.FileMatch {
		t.Fatalf("%+v %v", res, err)
	}
	if gets, _ := hub.requests(); len(gets) != 2 {
		t.Fatalf("%d requests, want the failed try and the retry", len(gets))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reports) == 0 {
		t.Fatal("no progress reported")
	}
	for _, r := range reports {
		if r.total > 14 || r.done > r.total {
			t.Errorf("report %+v is beyond the 14-byte file", r)
		}
	}
	if last := reports[len(reports)-1]; last != (report{14, 14}) {
		t.Errorf("final report %+v, want {14 14}", last)
	}
}
