package stage

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestMain(m *testing.M) {
	enginedouble.Dispatch()
	// No process a test starts inherits a descriptor this binary got, such
	// as a build lock's (an orphan would hold it).
	if err := agent.CloseInheritedFDs(); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// Hashes from sha256sum, never from the code under test.
const (
	// printf 'hello weights\n' | sha256sum
	weightsSum = "62e1cb3d236c6ee44130c54bf720f87b2ba1b0d58965634af9a3c3e541a462ef"
	// printf 'tampered\n' | sha256sum
	tamperedSum = "92e78d0b032962f47792a9fa95fd981ef63e1e3ef074d536d6304c75eddbe29f"
	// head -c 9437184 /dev/zero | sha256sum (9 MiB: more than one 8 MiB read)
	zeros9MiBSum = "d2ee4703cd9698945ca7b9fe1689ea3095597eac1a0afd8dba00cac7894fdc43"
	rev          = "0123456789abcdef0123456789abcdef01234567"
	digest       = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

// fakeHub serves /<repo>/resolve/<rev>/<path> from a map, over a Unix socket.
type fakeHub struct {
	mu    sync.Mutex
	files map[string]string
	gets  []string
	auth  []string
}

func (h *fakeHub) requests() (gets, auth []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.gets...), append([]string(nil), h.auth...)
}

func startHub(t *testing.T, root string, files map[string]string) (*fakeHub, *http.Client) {
	t.Helper()
	h := &fakeHub{files: files}
	sock := filepath.Join(root, "hub.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.gets = append(h.gets, r.URL.Path)
		h.auth = append(h.auth, r.Header.Get("Authorization"))
		c, ok := h.files[r.URL.Path]
		h.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		io.WriteString(w, c)
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return h, &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}}
}

func newStager(t *testing.T, files map[string]string) (*Stager, *fakeHub, string) {
	t.Helper()
	root, err := os.MkdirTemp("", "hbs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	hub, client := startHub(t, root, files)
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := enginedouble.Install(bin); err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{
		StagingDir:  filepath.Join(root, "staging"),
		Apptainer:   filepath.Join(bin, "apptainer"),
		TmpDir:      root,
		HubEndpoint: "http://hub.test",
		HTTPClient:  client,
		BaseEnv:     append(os.Environ(), "HB_FAKE_APPTAINER_LOG="+filepath.Join(root, "apptainer.jsonl")),
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, hub, root
}

func resolvePath(p string) string { return "/org/model/resolve/" + rev + "/" + p }

func ckptDir(s *Stager) string {
	return filepath.Join(s.c.StagingDir, agentapi.StagedCheckpointDir("org/model", rev))
}

func outcomes(r agent.CheckpointResult) map[string]agentapi.FileResult {
	m := map[string]agentapi.FileResult{}
	for _, f := range r.Files {
		m[f.Path] = f
	}
	return m
}

// A missing file is downloaded at the full commit id, checked, and renamed
// into place; a nested path gets its directory; no temporary file stays; the
// token goes in the Authorization header.
func TestDownloadsMissingFiles(t *testing.T) {
	s, hub, _ := newStager(t, map[string]string{
		resolvePath("model.safetensors"):  "hello weights\n",
		resolvePath("tok/tokenizer.json"): "hello weights\n",
	})
	res, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev, HubToken: "hf_secret",
		Files: []agentapi.FileDigest{{Path: "model.safetensors", SHA256: weightsSum}, {Path: "tok/tokenizer.json", SHA256: weightsSum}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for p, f := range outcomes(res) {
		if f.Outcome != agentapi.FileMatch {
			t.Errorf("%s: %+v", p, f)
		}
		b, err := os.ReadFile(filepath.Join(ckptDir(s), p))
		if err != nil || string(b) != "hello weights\n" {
			t.Errorf("%s holds %q (%v)", p, b, err)
		}
	}
	left, _ := filepath.Glob(filepath.Join(ckptDir(s), "*part*"))
	if len(left) != 0 {
		t.Errorf("temporary files left: %q", left)
	}
	_, auth := hub.requests()
	for _, a := range auth {
		if a != "Bearer hf_secret" {
			t.Errorf("Authorization %q", a)
		}
	}
	if res.Dir != ckptDir(s) {
		t.Errorf("dir %q", res.Dir)
	}
}

// A download whose content does not match the record fails and leaves no file.
func TestRefusesADownloadThatDoesNotMatch(t *testing.T) {
	s, _, _ := newStager(t, map[string]string{resolvePath("config.json"): "tampered\n"})
	_, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev,
		Files: []agentapi.FileDigest{{Path: "config.json", SHA256: weightsSum}},
	})
	if err == nil || !strings.Contains(err.Error(), tamperedSum) {
		t.Fatalf("err %v, want a mismatch naming the download's hash", err)
	}
	if entries, _ := os.ReadDir(ckptDir(s)); len(entries) != 0 {
		t.Fatalf("files left after a refused download: %v", entries)
	}
}

// A present file is rehashed: a match is a match, a changed file a mismatch
// with its hash, and nothing is downloaded at a first start. On a restage the
// changed file is moved aside and downloaded again.
func TestRehashAndRestage(t *testing.T) {
	s, hub, _ := newStager(t, map[string]string{resolvePath("config.json"): "hello weights\n"})
	dir := ckptDir(s)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "config.json"), []byte("tampered\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "zeros.safetensors"), make([]byte, 9<<20), 0o600)
	files := []agentapi.FileDigest{{Path: "config.json", SHA256: weightsSum}, {Path: "zeros.safetensors", SHA256: zeros9MiBSum}}
	res, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{Repository: "org/model", Revision: rev, Files: files})
	if err != nil {
		t.Fatal(err)
	}
	o := outcomes(res)
	if f := o["config.json"]; f.Outcome != agentapi.FileMismatch || f.SHA256 != tamperedSum {
		t.Errorf("config.json: %+v", f)
	}
	if f := o["zeros.safetensors"]; f.Outcome != agentapi.FileMatch {
		t.Errorf("zeros.safetensors: %+v", f)
	}
	if gets, _ := hub.requests(); len(gets) != 0 {
		t.Fatalf("a first start downloaded %q", gets)
	}

	res, err = s.Checkpoint(context.Background(), agent.CheckpointRequest{Repository: "org/model", Revision: rev, Files: files, Restage: true})
	if err != nil {
		t.Fatal(err)
	}
	if f := outcomes(res)["config.json"]; f.Outcome != agentapi.FileMatch {
		t.Errorf("after restage config.json: %+v", f)
	}
	aside, _ := filepath.Glob(filepath.Join(dir, "config.json.mismatch-*"))
	if len(aside) != 1 {
		t.Fatalf("moved aside: %q", aside)
	}
	if b, _ := os.ReadFile(aside[0]); string(b) != "tampered\n" {
		t.Errorf("the moved-aside copy holds %q", b)
	}
}

// Beside safetensors, the other weight formats and original/ are not
// downloaded (vLLM's default loader does not load them) and are absent; one
// that is present is still rehashed.
func TestSkipsWeightsTheEngineDoesNotLoad(t *testing.T) {
	s, hub, _ := newStager(t, map[string]string{resolvePath("model.safetensors"): "hello weights\n"})
	dir := ckptDir(s)
	os.MkdirAll(dir, 0o700)
	os.WriteFile(filepath.Join(dir, "present.bin"), []byte("tampered\n"), 0o600)
	res, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev,
		Files: []agentapi.FileDigest{
			{Path: "model.safetensors", SHA256: weightsSum},
			{Path: "pytorch_model.bin", SHA256: weightsSum},
			{Path: "original/consolidated.00.pth", SHA256: weightsSum},
			{Path: "present.bin", SHA256: weightsSum},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	o := outcomes(res)
	if o["pytorch_model.bin"].Outcome != agentapi.FileAbsent || o["original/consolidated.00.pth"].Outcome != agentapi.FileAbsent {
		t.Errorf("skipped files: %+v", o)
	}
	if f := o["present.bin"]; f.Outcome != agentapi.FileMismatch || f.SHA256 != tamperedSum {
		t.Errorf("present.bin: %+v", f)
	}
	if gets, _ := hub.requests(); len(gets) != 1 || gets[0] != resolvePath("model.safetensors") {
		t.Errorf("downloads %q", gets)
	}
}

// Without safetensors, .bin weights are what the engine loads, and are staged.
func TestStagesBinWeightsWithoutSafetensors(t *testing.T) {
	s, _, _ := newStager(t, map[string]string{resolvePath("pytorch_model.bin"): "hello weights\n"})
	res, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev,
		Files: []agentapi.FileDigest{{Path: "pytorch_model.bin", SHA256: weightsSum}},
	})
	if err != nil || outcomes(res)["pytorch_model.bin"].Outcome != agentapi.FileMatch {
		t.Fatalf("%+v %v", res, err)
	}
}

// No list: the checkpoint is treated as pre-staged, its directory exists for
// the bind, and nothing is fetched.
func TestNoListStagesNothing(t *testing.T) {
	s, hub, _ := newStager(t, nil)
	res, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{Repository: "org/model", Revision: rev})
	if err != nil || len(res.Files) != 0 {
		t.Fatalf("%+v %v", res, err)
	}
	if fi, err := os.Stat(res.Dir); err != nil || !fi.IsDir() {
		t.Fatalf("checkpoint dir: %v", err)
	}
	if gets, _ := hub.requests(); len(gets) != 0 {
		t.Fatalf("fetched %q", gets)
	}
}

func TestRefusesUnsafeInputs(t *testing.T) {
	s, _, _ := newStager(t, nil)
	for _, req := range []agent.CheckpointRequest{
		{Repository: "org/model", Revision: rev, Files: []agentapi.FileDigest{{Path: "../escape", SHA256: weightsSum}}},
		{Repository: "org/model", Revision: rev, Files: []agentapi.FileDigest{{Path: "/abs", SHA256: weightsSum}}},
		{Repository: "org/model", Revision: rev, Files: []agentapi.FileDigest{{Path: "a/./b", SHA256: weightsSum}}},
		{Repository: "org/model", Revision: rev, Files: []agentapi.FileDigest{{Path: "ok", SHA256: "XYZ"}}},
		{Repository: "org/model", Revision: "main"},
		{Repository: "../model", Revision: rev},
	} {
		if _, err := s.Checkpoint(context.Background(), req); err == nil {
			t.Errorf("accepted %+v", req)
		}
	}
	if _, err := s.Image(context.Background(), "docker.io/vllm/vllm-openai", "sha256:short", agentapi.ImageFormatSIF); err == nil {
		t.Error("accepted a short digest")
	}
	if _, err := s.Image(context.Background(), "docker.io/vllm/vllm-openai@sha256:x", digest, agentapi.ImageFormatSIF); err == nil {
		t.Error("accepted an image with a digest in it")
	}
}

// The image is pulled as a SIF or built as a sandbox from the same digest, at
// agentapi.StagedImagePath, once: a second call reuses it.
func TestImageFormats(t *testing.T) {
	for _, tc := range []struct {
		format agentapi.ImageFormat
		cmd    string
		dir    bool
	}{
		{agentapi.ImageFormatSIF, "pull", false},
		{agentapi.ImageFormatSandbox, "build", true},
	} {
		t.Run(string(tc.format), func(t *testing.T) {
			s, _, root := newStager(t, nil)
			res, err := s.Image(context.Background(), "docker.io/vllm/vllm-openai", digest, tc.format)
			if err != nil {
				t.Fatal(err)
			}
			p := res.Path
			if want := filepath.Join(s.c.StagingDir, agentapi.StagedImagePath(digest, tc.format)); p != want {
				t.Fatalf("path %q, want %q", p, want)
			}
			if fi, err := os.Stat(p); err != nil || fi.IsDir() != tc.dir {
				t.Fatalf("staged image: %v", err)
			}
			if !res.Verified {
				t.Error("a fresh pull or build is not reported as verified")
			}
			// The positive control for the integrity check: an intact image
			// passes it, is reported verified, and is not pulled again.
			again, err := s.Image(context.Background(), "docker.io/vllm/vllm-openai", digest, tc.format)
			if err != nil || !again.Verified || again.Path != p {
				t.Fatalf("second call: %+v, %v", again, err)
			}
			b, _ := os.ReadFile(filepath.Join(root, "apptainer.jsonl"))
			lines := strings.Split(strings.TrimSpace(string(b)), "\n")
			if len(lines) != 1 {
				t.Fatalf("%d apptainer invocations, want 1: %q", len(lines), lines)
			}
			var rec struct{ Argv []string }
			json.Unmarshal([]byte(lines[0]), &rec)
			want := "docker://docker.io/vllm/vllm-openai@" + digest
			if len(rec.Argv) == 0 || rec.Argv[0] != tc.cmd || rec.Argv[len(rec.Argv)-1] != want {
				t.Fatalf("invocation %q, want %s ... %s", rec.Argv, tc.cmd, want)
			}
		})
	}
}

// The hasher reads through its buffer: the FIPS 180-2 vector for "abc".
func TestHashFileKnownVector(t *testing.T) {
	p := filepath.Join(t.TempDir(), "abc")
	os.WriteFile(p, []byte("abc"), 0o600)
	got, err := hashFile(context.Background(), p, make([]byte, 2))
	if err != nil || got != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("%s %v", got, err)
	}
}
