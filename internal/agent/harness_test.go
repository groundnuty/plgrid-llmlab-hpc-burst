package agent_test

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/engine"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/stage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// TestMain lets this test binary also play Apptainer, ssh and the engine,
// which the real agent starts as processes (package enginedouble).
func TestMain(m *testing.M) {
	enginedouble.Dispatch()
	// No process a test starts inherits a descriptor this binary got, such
	// as a build lock's (an orphan would hold it).
	if err := agent.CloseInheritedFDs(); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// The harness's identity of the Allocation and its job.
const (
	hTag        = "hb-example-0123456789ab"
	hAllocUID   = "11111111-2222-3333-4444-555555555555"
	hInstUID    = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	hJobID      = "4242"
	hCredential = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	hDigest     = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	hServed     = "example-model"
)

// timedBackend records when each report reached the Backend, which
// fake.Backend does not: the outage test reads the gaps.
type timedBackend struct {
	agentapi.Backend
	mu    sync.Mutex
	times []time.Time
}

func (b *timedBackend) Report(ctx context.Context, c agentapi.Caller, r agentapi.Report) error {
	err := b.Backend.Report(ctx, c, r)
	if err == nil {
		b.mu.Lock()
		b.times = append(b.times, time.Now())
		b.mu.Unlock()
	}
	return err
}

func (b *timedBackend) reportTimes() []time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]time.Time(nil), b.times...)
}

type harness struct {
	t          *testing.T
	backend    *fake.Backend
	timed      *timedBackend
	gate       *fake.Gate
	api        *backendAPI
	root       string // short temp dir: run, staging, logs
	runDir     string
	stagingDir string
	cacheDir   string // the Allocation's cache directory (Options.CacheDir)
	appLog     string // the fake apptainer's invocations
	engLog     string // the engine double's starts and requests
	aid        string
	agent      *agent.Agent
	cancel     context.CancelFunc
	done       chan error
	logs       *syncBuffer
	hubMu      sync.Mutex
	hubHits    int
	hubAuth    []string
}

type harnessOptions struct {
	credential string // presented credential; hCredential when empty
	jobID      string // presented job ID; hJobID when empty
	restart    int32
	hub        map[string]string // files the fake Hub serves, by URL path
	// hubIntercept sees every Hub request first and returns true when it
	// has answered it.
	hubIntercept func(w http.ResponseWriter, r *http.Request) bool
	// apptainer replaces the fake Apptainer with another program.
	apptainer string
	// engineWrap, stagerWrap and backendWrap wrap the real engine client,
	// the real stager and the fake Backend.
	engineWrap  func(agent.Engine) agent.Engine
	stagerWrap  func(agent.Stager) agent.Stager
	backendWrap func(agentapi.Backend) agentapi.Backend
	// tunnel is told which Instance the agent serves.
	tunnel agent.Tunnel
	// home, when set, is the agent's HOME in place of the test process's.
	home string
}

// newHarness starts a real agent against a fake Backend behind a fake Gate,
// with the real engine client and stager, the fake Apptainer and the engine
// double as processes.
func newHarness(t *testing.T, o harnessOptions) *harness {
	t.Helper()
	root, err := os.MkdirTemp("", "hba")
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, root: root, done: make(chan error, 1), logs: &syncBuffer{}}
	t.Cleanup(func() { os.RemoveAll(root) })
	bin := filepath.Join(root, "bin")
	h.runDir = filepath.Join(root, "run")
	h.stagingDir = filepath.Join(root, "staging")
	for _, d := range []string{bin, h.runDir, h.stagingDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := enginedouble.Install(bin); err != nil {
		t.Fatal(err)
	}
	h.appLog = filepath.Join(root, "apptainer.jsonl")
	h.engLog = filepath.Join(root, "engine.jsonl")

	h.backend = fake.NewBackend()
	h.timed = &timedBackend{Backend: h.backend}
	var inner agentapi.Backend = h.timed
	if o.backendWrap != nil {
		inner = o.backendWrap(inner)
	}
	h.gate = fake.NewGate(inner)
	h.gate.Add(hTag, fake.GateAllocation{Namespace: "ns", Name: "alloc", UID: hAllocUID, Credential: hCredential, JobID: hJobID})
	cred, job := o.credential, o.jobID
	if cred == "" {
		cred = hCredential
	}
	if job == "" {
		job = hJobID
	}
	h.aid, err = agentapi.NewAgentInstanceID()
	if err != nil {
		t.Fatal(err)
	}
	h.api = &backendAPI{
		b:         h.gate.As(cred, hTag, job),
		caller:    agentapi.Caller{AgentInstanceID: h.aid},
		pollEvery: 10 * time.Millisecond,
		longPoll:  300 * time.Millisecond,
	}
	apptainer := filepath.Join(bin, "apptainer")
	if o.apptainer != "" {
		apptainer = o.apptainer
	}
	baseEnv := append(os.Environ(), "HB_FAKE_APPTAINER_LOG="+h.appLog, noExitSleep)
	if o.home != "" {
		baseEnv = slices.DeleteFunc(baseEnv, func(kv string) bool { return strings.HasPrefix(kv, "HOME=") })
		baseEnv = append(baseEnv, "HOME="+o.home)
	}
	h.cacheDir = filepath.Join(root, agentapi.AllocationCacheDirName)
	files := map[string]string{"/org/model/resolve/" + strings.Repeat("c", 40) + "/" + defaultFile.Path: defaultContent}
	for k, v := range o.hub {
		files[k] = v
	}
	hubClient, hubURL := h.startHub(files, o.hubIntercept)
	st, err := stage.New(stage.Config{
		StagingDir:  h.stagingDir,
		Apptainer:   apptainer,
		TmpDir:      h.runDir,
		HubEndpoint: hubURL,
		HTTPClient:  hubClient,
		BaseEnv:     baseEnv,
	})
	if err != nil {
		t.Fatal(err)
	}
	var stager agent.Stager = st
	if o.stagerWrap != nil {
		stager = o.stagerWrap(st)
	}
	engines := func(socket, key string, l agentapi.Launch) agent.Engine {
		e := agent.Engine(engine.New(socket, key, engine.WithReasoning(agent.DeclaresReasoningParser(l.Command))))
		if o.engineWrap != nil {
			e = o.engineWrap(e)
		}
		return e
	}
	h.agent, err = agent.New(agent.Options{
		API:                  h.api,
		Engines:              engines,
		Stager:               stager,
		Apptainer:            apptainer,
		RunDir:               h.runDir,
		LogDir:               root,
		CacheDir:             h.cacheDir,
		Registration:         agentapi.RegisterRequest{RestartCount: o.restart, Node: "node-test", CPUArch: "x86_64"},
		BaseEnv:              baseEnv,
		Tunnel:               o.tunnel,
		Logger:               slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		ReportInterval:       200 * time.Millisecond,
		StartupProbeInterval: 50 * time.Millisecond,
		MinBackoff:           20 * time.Millisecond,
		MaxBackoff:           200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { h.done <- h.agent.Run(ctx) }()
	t.Cleanup(h.stop)
	return h
}

// stop ends the agent (as a job end does) and waits for it, which stops every
// worker.
func (h *harness) stop() {
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(30 * time.Second):
		h.t.Errorf("the agent did not stop within 30 s")
	}
	if h.t.Failed() {
		h.t.Logf("agent log:\n%s", h.logs.String())
	}
}

// noExitSleep stops a race-built fake from sleeping a second at its exit
// (the race detector's atexit_sleep_ms), which under go test -race would make
// a TERM-honouring engine and its fake Apptainer outlast a short stop grace.
const noExitSleep = "GORACE=atexit_sleep_ms=0"

// launch is the Instance's launch in the harness: the engine double, with its
// modes, through the fake Apptainer.
func launch(modes string, engLog string) agentapi.Launch {
	env := []agentapi.EnvVar{{Name: "HB_ENGINE_DOUBLE_LOG", Value: engLog}, {Name: "GORACE", Value: strings.TrimPrefix(noExitSleep, "GORACE=")}}
	if modes != "" {
		env = append(env, agentapi.EnvVar{Name: "HB_ENGINE_DOUBLE", Value: modes})
	}
	return agentapi.Launch{
		Image:       "docker.io/vllm/vllm-openai",
		ImageDigest: hDigest,
		Command: []string{"vllm", "serve", agentapi.ContainerModelDir,
			"--uds=" + agentapi.ContainerEngineSocket(agentapi.WorkerName(0)),
			"--served-model-name=" + hServed, "--max-model-len=4096", "--max-num-seqs=8"},
		Env:        env,
		KeyEnv:     "VLLM_API_KEY",
		Checkpoint: agentapi.Checkpoint{Repository: "org/model", Revision: strings.Repeat("c", 40)},
		GPUs:       1,
		MaxNumSeqs: 8,
	}
}

// The identity record's default file list: one file the fake Hub serves.
// printf '{}\n' | sha256sum
var (
	defaultContent = "{}\n"
	defaultFile    = agentapi.FileDigest{Path: "config.json", SHA256: "ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356"}
	defaultFiles   = []agentapi.FileDigest{defaultFile}
)

// runState is a desired state that runs the harness's Instance. Without
// checks it asks for verification with the default file list, which every
// start instruction must carry.
func (h *harness) runState(modes string, instGen, workerGen int64, checks *agentapi.Checks) agentapi.DesiredState {
	if checks == nil {
		checks = verifyChecks(1, nil)
	}
	return agentapi.DesiredState{
		Action: agentapi.ActionRun,
		Instance: &agentapi.DesiredInstance{
			Name: "inst", UID: hInstUID, DeploymentID: strings.Repeat("d", 64),
			Launch:     launch(modes, h.engLog),
			Generation: instGen,
			Workers: []agentapi.DesiredWorker{{
				Name: agentapi.WorkerName(0), Generation: workerGen,
				EngineSocket: agentapi.ContainerEngineSocket(agentapi.WorkerName(0)),
			}},
			Checks: checks,
		},
		Settings: agentapi.AgentSettings{
			ProbeInterval: agentapi.Duration(100 * time.Millisecond),
			StopGrace:     agentapi.Duration(600 * time.Millisecond),
		},
	}
}

func (h *harness) set(d agentapi.DesiredState) { h.backend.SetDesired(hAllocUID, d) }

// waitReport waits until the Backend's latest report satisfies ok.
func (h *harness) waitReport(what string, timeout time.Duration, ok func(agentapi.Report) bool) agentapi.Report {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if r, found := h.backend.LastReport(hAllocUID); found && ok(r) {
			return r
		}
		if time.Now().After(deadline) {
			r, _ := h.backend.LastReport(hAllocUID)
			b, _ := json.MarshalIndent(r.Redacted(), "", "  ")
			h.t.Fatalf("timed out after %s waiting for %s; last report:\n%s", timeout, what, b)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// worker0 returns worker-0's report, or nil.
func worker0(r agentapi.Report) *agentapi.WorkerReport {
	if r.Instance == nil {
		return nil
	}
	for i := range r.Instance.Workers {
		if r.Instance.Workers[i].Name == agentapi.WorkerName(0) {
			return &r.Instance.Workers[i]
		}
	}
	return nil
}

func running(r agentapi.Report) bool {
	w := worker0(r)
	return w != nil && w.Phase == agentapi.PartRunning
}

// appInvocation is one line of the fake Apptainer's log
// (enginedouble.ExecRecord): Argv starts after the program name.
type appInvocation struct {
	Argv []string `json:"argv"`
	Env  []string `json:"env"`
	PID  int      `json:"pid"`
}

// execs returns the fake Apptainer's exec invocations, oldest first.
func (h *harness) execs() []appInvocation {
	var out []appInvocation
	for _, line := range readLines(h.t, h.appLog) {
		var inv appInvocation
		if err := json.Unmarshal([]byte(line), &inv); err != nil {
			h.t.Fatalf("apptainer log line %q: %v", line, err)
		}
		if len(inv.Argv) > 0 && inv.Argv[0] == "exec" {
			out = append(out, inv)
		}
	}
	return out
}

// appCommands returns the subcommand of every fake Apptainer invocation.
func (h *harness) appCommands() []string {
	var out []string
	for _, line := range readLines(h.t, h.appLog) {
		var inv appInvocation
		if err := json.Unmarshal([]byte(line), &inv); err == nil && len(inv.Argv) > 0 {
			out = append(out, inv.Argv[0])
		}
	}
	return out
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		if s := strings.TrimSpace(sc.Text()); s != "" {
			lines = append(lines, s)
		}
	}
	return lines
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
