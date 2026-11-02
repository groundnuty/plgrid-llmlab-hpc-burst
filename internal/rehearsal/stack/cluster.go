package stack

// The stand-in cluster: the fake Slurm in run mode behind the rehearsal's
// login node, and the Hub double. Copied from internal/rehearsal/scenario's
// harness_test.go (startCluster) and hub_test.go (issue #251 moves the
// scenario onto it); the partitions and QOSes are the caller's, so a stand-in
// can have Helios's shape.

import (
	"crypto/sha1" // the git blob id is a SHA-1 by definition
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// ClusterUser is the fake cluster's own user, never this host's.
const ClusterUser = "example-user"

// Cluster is the fake Slurm in run mode and its login node.
type Cluster struct {
	Plat    *fakeslurm.Platform
	Login   *rehearsal.LoginNode
	Root    string // RunMode.Root: the cluster's shared file system
	Home    string // the cluster user's HOME
	Scratch string
	Fakes   string // the engine double's fake apptainer
}

// ClusterShape is the stand-in cluster's partitions and QOSes; the zero shape
// is the rehearsal's, one GPU-only partition named Partition.
type ClusterShape struct {
	Partitions []fakeslurm.Partition
	QOS        []fakeslurm.QOS
	// FirstJobID is the first job ID the cluster gives; 0 keeps the fake's own.
	// A run that finds its processes by SLURM_JOB_ID on this shared host sets one
	// no other fake cluster here uses.
	FirstJobID int
}

// StartCluster starts the fake cluster in dir, with jobEnv added to every
// job's environment, and its login node, and registers their Close.
func StartCluster(t T, dir string, shape ClusterShape, jobEnv map[string]string) *Cluster {
	t.Helper()
	c := &Cluster{Root: filepath.Join(dir, "c"), Fakes: filepath.Join(dir, "fk")}
	c.Home, c.Scratch = filepath.Join(c.Root, "home", ClusterUser), filepath.Join(c.Root, "scratch", ClusterUser)
	for _, d := range []string{c.Home, c.Scratch, c.Fakes} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// The login node's directory holds the relay socket, which runs commands
	// on the fake cluster: mode 700.
	if err := os.Mkdir(filepath.Join(dir, "ln"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := enginedouble.Install(c.Fakes); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"HPCBURST_APPTAINER": filepath.Join(c.Fakes, "apptainer"),
		// No GPU on this host: the agent's GPU probe only warns without one.
		"HPCBURST_NVIDIA_SMI": filepath.Join(c.Fakes, "no-nvidia-smi"),
	}
	for k, v := range jobEnv {
		env[k] = v
	}
	if len(shape.Partitions) == 0 {
		shape.Partitions = []fakeslurm.Partition{{Name: Partition, GPUOnly: true}}
	}
	c.Plat = fakeslurm.New(slurm.RealClock, fakeslurm.Config{
		Partitions: shape.Partitions, QOS: shape.QOS, FirstJobID: shape.FirstJobID,
		// KillWait 20 s: longer than the agent's own shutdown (the profile's
		// 5 s stopGrace, its 10 s margin and a 5 s last report), shorter than
		// Slurm's 30 s default. Slots stay unlimited, so no ended job's slot
		// goes to a phantom job.
		Run: &fakeslurm.RunMode{Root: c.Root, JobDir: filepath.Join(dir, "j"), Env: env, KillWait: 20 * time.Second},
	})
	t.Cleanup(func() {
		if err := c.Plat.Close(); err != nil {
			t.Errorf("the fake cluster's Close: %v", err)
		}
	})
	login, err := rehearsal.StartLoginNode(rehearsal.LoginConfig{
		Dir: filepath.Join(dir, "ln"), Platform: c.Plat,
		Login: fakeslurm.Login{User: ClusterUser, Env: map[string]string{"HOME": c.Home, "SCRATCH": c.Scratch}},
	})
	if err != nil {
		t.Fatalf("the login node: %v", err)
	}
	t.Cleanup(func() {
		if err := login.Close(); err != nil {
			t.Errorf("the login node's Close: %v", err)
		}
	})
	c.Login = login
	return c
}

// ShortDir is a scratch directory with a short path: k3s's sockets bound the
// state directory to 80 bytes and an engine's unix socket in a job's TMPDIR to
// 108. It is removed at the end of the test.
func ShortDir(t T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "hr")
	if err != nil {
		t.Fatal(err)
	}
	if len(d) > 48 {
		t.Fatalf("scratch directory %s is longer than 48 bytes; set TMPDIR to a shorter path", d)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

// hubFile is one file of the Hub double's repository.
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

// Hub is the local Hub double: the Hugging Face endpoints the controller lists
// a checkpoint with (GET /api/models/<repo>/tree/<rev>) and the agent downloads
// from (GET /<repo>/resolve/<rev>/<path>), for one small repository. The
// listing's ids are computed from the content as the Hub defines them: a git
// blob id, sha1("blob <size>\0" + content), for every file, and the SHA-256 as
// lfs.oid for an LFS file (pkg/hubfiles/download.go).
type Hub struct {
	URL string
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

// StartHub serves the double on a kernel-chosen loopback port until the test ends.
func StartHub(t T) *Hub {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := &Hub{URL: "http://" + ln.Addr().String()}
	h.srv = &http.Server{Handler: http.HandlerFunc(h.handle)}
	go func() { _ = h.srv.Serve(ln) }() // ErrServerClosed at Close
	t.Cleanup(func() { _ = h.srv.Close() })
	return h
}

func (h *Hub) handle(w http.ResponseWriter, r *http.Request) {
	tree := "/api/models/" + CheckpointRepo + "/tree/" + CheckpointRev
	resolve := "/" + CheckpointRepo + "/resolve/" + CheckpointRev + "/"
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

// Resolved is a copy of the paths downloaded so far.
func (h *Hub) Resolved() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.resolves...)
}
