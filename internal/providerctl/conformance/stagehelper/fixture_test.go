package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/stage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestMain(m *testing.M) {
	// The fake Apptainer is this test binary under the name apptainer
	// (enginedouble.Install): it runs as the fake and exits.
	enginedouble.Dispatch()
	// No process a test starts inherits a descriptor this binary got, such as a
	// build lock's (an orphan would hold it).
	if err := agent.CloseInheritedFDs(); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// An image and a digest of the shape a real one has, and no more: the fake
// Apptainer pulls and builds any docker://<repository>@sha256:<64 hex>.
const (
	testImage  = "example.org/engine/server"
	testDigest = "sha256:abababababababababababababababababababababababababababababababab"
	testUID    = "1234567" // a Slurm job ID of the right shape
)

// syncBuf is a stderr that the stager's and Apptainer's writers share.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// fixture is one compute node's side of an Allocation: its directory with
// agent.json and the credential, the staging directory, the job's TMPDIR, and
// the fake Apptainer.
type fixture struct {
	root, alloc, staging, tmp, bin, appLog string
	// env is the job's environment, as getenv answers it.
	env map[string]string
}

// newFixture lays out an Allocation whose agent.json names hubURL as the Hub.
// It sets HB_FAKE_APPTAINER_LOG in this process's environment, which the
// stager passes to Apptainer (BaseEnv is os.Environ()), so no test that uses a
// fixture runs in parallel.
func newFixture(t *testing.T, hubURL string) *fixture {
	t.Helper()
	root, err := os.MkdirTemp("", "hs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	fx := &fixture{
		root:    root,
		alloc:   filepath.Join(root, "alloc"),
		staging: filepath.Join(root, "staging"),
		tmp:     filepath.Join(root, "tmp"),
		bin:     filepath.Join(root, "bin"),
		appLog:  filepath.Join(root, "apptainer.jsonl"),
	}
	for _, d := range []string{fx.alloc, fx.staging, fx.tmp, fx.bin} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := enginedouble.Install(fx.bin); err != nil {
		t.Fatal(err)
	}
	t.Setenv(enginedouble.EnvApptainerLog, fx.appLog)
	cfg := agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion,
		Tag:             "hb-example-abcdef012345",
		Mode:            agentapi.DialHomeDirect,
		Endpoint:        "127.0.0.1:29261",
		CertSPKISHA256:  strings.Repeat("a", 64),
		FrpcDigest:      strings.Repeat("b", 64),
		StagingDir:      fx.staging,
		DialHomeDir:     filepath.Join(root, "dial-home"),
		HubEndpoint:     hubURL,
	}
	b, err := cfg.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fx.alloc, agentapi.ConfigFileName), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := agentapi.WriteKeyFile(filepath.Join(fx.alloc, agent.CredentialFileName), strings.Repeat("0123456789abcdef", 4)); err != nil {
		t.Fatal(err)
	}
	fx.env = map[string]string{
		"SLURM_JOB_ID":       testUID,
		"TMPDIR":             fx.tmp,
		"HPCBURST_APPTAINER": filepath.Join(fx.bin, "apptainer"),
	}
	return fx
}

func (fx *fixture) getenv(k string) string { return fx.env[k] }

// run runs the helper over the fixture with in and returns its exit code and
// what it wrote to stderr.
func (fx *fixture) run(t *testing.T, in inputs) (int, string) {
	t.Helper()
	var errs syncBuf
	code := runInputs(context.Background(), in, []string{fx.alloc}, fx.getenv, &errs)
	return code, errs.String()
}

// result is the result file's content, which must be one line.
func (fx *fixture) result(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(fx.alloc, resultName))
	if err != nil {
		t.Fatalf("the helper left no result file: %v", err)
	}
	line, ok := strings.CutSuffix(string(b), "\n")
	if !ok || strings.Contains(line, "\n") {
		t.Fatalf("the result file is not one newline-ended line: %q", b)
	}
	return line
}

// goodInputs are inputs for the Hub double of package stack.
func goodInputs() inputs {
	return inputs{image: testImage, imageDigest: testDigest, checkpointRepo: stack.CheckpointRepo, checkpointRev: stack.CheckpointRev, loadFormat: "auto"}
}

// apptainerCalls is the first argument of every invocation the fake Apptainer
// recorded: "pull" or "build". It is a local equivalent of package stage's
// test helper of the same name, which is in a _test.go file and so cannot be
// imported.
func apptainerCalls(t *testing.T, fx *fixture) []string {
	t.Helper()
	b, err := os.ReadFile(fx.appLog)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var rec struct{ Argv []string }
		if err := json.Unmarshal([]byte(l), &rec); err != nil || len(rec.Argv) == 0 {
			t.Fatalf("the fake Apptainer's log line %q: %v", l, err)
		}
		out = append(out, rec.Argv[0])
	}
	return out
}

// hubGet reads one URL of the Hub double over plain net/http.
func hubGet(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d, %v", url, resp.StatusCode, err)
	}
	return b
}

// expectedFiles is the identity record's file list for the Hub double's
// checkpoint, built here from what the double serves and by nothing the helper
// uses: the listing's paths, and the SHA-256 of every file's served bytes taken
// by crypto/sha256 in this test (neither pkg/hubfiles nor the helper's code
// builds it). It sorts the list bytewise by path, as the record does. Its
// requests raise the double's download count, so a caller reads that count
// after calling it.
func expectedFiles(t *testing.T, hubURL string) []agentapi.FileDigest {
	t.Helper()
	var listing []struct {
		Path string `json:"path"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal(hubGet(t, hubURL+"/api/models/"+stack.CheckpointRepo+"/tree/"+stack.CheckpointRev), &listing); err != nil {
		t.Fatal(err)
	}
	var files []agentapi.FileDigest
	for _, e := range listing {
		if e.Type != "file" {
			continue
		}
		sum := sha256.Sum256(hubGet(t, hubURL+"/"+stack.CheckpointRepo+"/resolve/"+stack.CheckpointRev+"/"+e.Path))
		files = append(files, agentapi.FileDigest{Path: e.Path, SHA256: hex.EncodeToString(sum[:])})
	}
	slices.SortFunc(files, func(a, b agentapi.FileDigest) int { return strings.Compare(a.Path, b.Path) })
	if len(files) == 0 || !slices.ContainsFunc(files, func(f agentapi.FileDigest) bool { return f.Path == "model.safetensors" }) {
		t.Fatalf("the Hub double's listing holds no weights file: %+v", files)
	}
	return files
}

// agentLikeStager is a Stager configured as cmd/hb-agent configures its own,
// over the fixture's Allocation directory: the job read from agent.json and the
// job's environment again, the run directory made as the agent makes it, and
// the agent's fields. It is written out here, not built by the helper's code.
// built is set when its Image starts a build.
func agentLikeStager(t *testing.T, fx *fixture) (s *stage.Stager, built *atomic.Bool, ctx context.Context) {
	t.Helper()
	job, err := agent.LoadJob(fx.alloc, fx.getenv)
	if err != nil {
		t.Fatal(err)
	}
	runDir, err := agent.MakeRunDir(job.TmpDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(runDir) })
	var log syncBuf
	s, err = stage.New(stage.Config{
		StagingDir:  job.Config.StagingDir,
		Apptainer:   fx.env["HPCBURST_APPTAINER"],
		TmpDir:      runDir,
		HubEndpoint: job.Config.HubEndpoint, // agent.json names it, so the agent uses it
		BaseEnv:     os.Environ(),
		Log:         &log,
	})
	if err != nil {
		t.Fatal(err)
	}
	built = new(atomic.Bool)
	ctx = agent.WithImageBuild(context.Background(), func(string) { built.Store(true) })
	return s, built, ctx
}
