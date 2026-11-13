package agent

// Several workers' checks (HBEP-7, Several workers; the science agent's ruling on #439, Q6): each worker's
// checks, asked in DesiredWorker.Checks, run against that worker's own socket, never through the router,
// and each is keyed by that worker's process alone, so one worker's restart never drops another's
// results: each worker is verified on its own. runChecks is driven the way the loop drives it, over
// staged and running workers; every wait is on a unit's own completion event, never on a clock.

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// heldEngine is one socket's Engine whose verification waits until released, or until it is cancelled.
type heldEngine struct {
	Engine
	socket  string
	started chan<- string
	release <-chan struct{}
}

func (e heldEngine) Verify(ctx context.Context, _ agentapi.Sampling) agentapi.VerifyResult {
	e.started <- e.socket
	select {
	case <-e.release:
		return agentapi.VerifyResult{Fingerprint: "fp-" + e.socket}
	case <-ctx.Done():
		return agentapi.VerifyResult{}
	}
}

// heldAgent is an agent whose engines are heldEngines, one release per socket.
type heldAgent struct {
	*Agent
	started  chan string
	mu       sync.Mutex
	releases map[string]chan struct{}
}

func newHeldAgent(t *testing.T) *heldAgent {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h := &heldAgent{started: make(chan string, 8), releases: map[string]chan struct{}{}}
	h.Agent = &Agent{
		ctx: ctx, log: slog.New(slog.NewTextHandler(io.Discard, nil)), events: make(chan func(), 64),
		o: Options{Engines: func(socket, _ string, _ agentapi.Launch) Engine {
			h.mu.Lock()
			defer h.mu.Unlock()
			r := h.releases[socket]
			if r == nil {
				r = make(chan struct{})
				h.releases[socket] = r
			}
			return heldEngine{socket: socket, started: h.started, release: r}
		}},
	}
	t.Cleanup(func() { cancel(); h.wg.Wait() })
	return h
}

func (h *heldAgent) release(socket string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	close(h.releases[socket])
}

// next takes one event from the loop's queue, as the loop does.
func (h *heldAgent) next(t *testing.T, what string) {
	t.Helper()
	select {
	case f := <-h.events:
		f()
	case <-time.After(20 * time.Second):
		t.Fatalf("no event: %s", what)
	}
}

func (h *heldAgent) startedOn(t *testing.T) string {
	t.Helper()
	select {
	case s := <-h.started:
		return s
	case <-time.After(20 * time.Second):
		t.Fatal("no verification started")
	}
	return ""
}

func twoWorkerChecks(dir string) (*instance, agentapi.DesiredInstance) {
	verify := func() *agentapi.Checks {
		return &agentapi.Checks{VerificationGeneration: 1, Verify: &agentapi.VerifyRequest{}}
	}
	in := &instance{
		launch: testLaunch(), staged: true, ckptDir: dir, generation: 1,
		workers: []*worker{
			{name: "worker-0", socket: "/run/worker-0.sock", phase: agentapi.PartRunning, proc: &procgroup.Group{}, generation: 1, instGen: 1},
			{name: "worker-1", socket: "/run/worker-1.sock", phase: agentapi.PartRunning, proc: &procgroup.Group{}, generation: 1, instGen: 1},
		},
	}
	i := agentapi.DesiredInstance{Generation: 1, Workers: []agentapi.DesiredWorker{
		{Name: "worker-0", Generation: 1, Checks: verify()},
		{Name: "worker-1", Generation: 1, Checks: verify()},
	}}
	return in, i
}

// Each worker's verification runs against its own socket, at the same time.
func TestEachWorkersChecksRunOnItsOwnSocket(t *testing.T) {
	h := newHeldAgent(t)
	dir := t.TempDir()
	writeGenerationConfig(t, dir, `{}`)
	in, i := twoWorkerChecks(dir)
	h.inst = in
	h.runChecks(in, i)
	got := map[string]bool{h.startedOn(t): true, h.startedOn(t): true}
	if !got["/run/worker-0.sock"] || !got["/run/worker-1.sock"] {
		t.Fatalf("verifications ran on %v, want one on each worker's own socket", got)
	}
	if in.ck.unit != nil || in.ck.checks != nil {
		t.Errorf("the Instance's own checks ran though only the workers' were asked: %+v", in.ck.checks)
	}
	for _, s := range []string{"/run/worker-0.sock", "/run/worker-1.sock"} {
		h.release(s)
		h.next(t, "a verification ended")
	}
	for _, name := range []string{"worker-0", "worker-1"} {
		ck := in.wck[name]
		if ck == nil || ck.checks == nil || ck.checks.Verify == nil || ck.checks.Verify.Fingerprint != "fp-/run/"+name+".sock" {
			t.Errorf("%s's verification result: %+v, want its own socket's", name, ck)
		}
	}
}

// worker-1 restarts while worker-0's verification runs: worker-1's unit is cancelled and its result
// dropped, worker-0's runs on, and its result lands. A key over every worker would cancel worker-0's too.
func TestOneWorkersRestartLeavesAnothersChecks(t *testing.T) {
	h := newHeldAgent(t)
	dir := t.TempDir()
	writeGenerationConfig(t, dir, `{}`)
	in, i := twoWorkerChecks(dir)
	h.inst = in
	h.runChecks(in, i)
	h.startedOn(t)
	h.startedOn(t)
	// worker-1 restarts: its generation is raised, and a new process runs for it.
	i.Workers[1].Generation = 2
	in.workers[1].generation = 2
	h.runChecks(in, i)
	if u := in.wck["worker-1"].unit; u == nil || !u.cancelled {
		t.Fatalf("worker-1's unit after its restart: %+v, want cancelled", u)
	}
	if u := in.wck["worker-0"].unit; u == nil || u.cancelled {
		t.Fatalf("worker-0's unit after worker-1's restart: %+v, want still running", u)
	}
	h.next(t, "worker-1's cancelled unit returned")
	h.release("/run/worker-0.sock")
	h.next(t, "worker-0's verification ended")
	if ck := in.wck["worker-0"]; ck.checks.Verify == nil || ck.checks.Verify.Fingerprint != "fp-/run/worker-0.sock" {
		t.Errorf("worker-0's result after worker-1's restart: %+v, want its verification", ck.checks.Verify)
	}
	if ck := in.wck["worker-1"]; ck.checks.Verify != nil {
		t.Errorf("worker-1's result from its old process landed: %+v", ck.checks.Verify)
	}
}

// rehashCounter is a Stager that counts the rehashes a verification asks for.
type rehashCounter struct {
	Stager
	n *atomic.Int32
}

func (s rehashCounter) Checkpoint(context.Context, CheckpointRequest) (CheckpointResult, error) {
	s.n.Add(1)
	return CheckpointResult{}, nil
}

// A staging's rehash serves each worker's first verification after it: two workers started together
// rehash the checkpoint no more, and the second verification of either does rehash.
func TestAStagingsRehashServesEachWorkersFirstVerification(t *testing.T) {
	h := newHeldAgent(t)
	var rehashes atomic.Int32
	h.o.Stager = rehashCounter{n: &rehashes}
	dir := t.TempDir()
	writeGenerationConfig(t, dir, `{}`)
	in, i := twoWorkerChecks(dir)
	files := []agentapi.FileDigest{{Path: "config.json", SHA256: strings.Repeat("ab", 32)}}
	staged := []agentapi.FileResult{{Path: "config.json", Outcome: agentapi.FileMatch, SHA256: strings.Repeat("ab", 32)}}
	in.fileResults, in.filesKey, in.stagingSeq = staged, filesKey(files), 1
	for w := range i.Workers {
		i.Workers[w].Checks.Verify.Files = files
	}
	h.inst = in
	h.runChecks(in, i)
	h.startedOn(t)
	h.startedOn(t)
	h.release("/run/worker-0.sock")
	h.release("/run/worker-1.sock")
	h.next(t, "a verification ended")
	h.next(t, "the other verification ended")
	if n := rehashes.Load(); n != 0 {
		t.Errorf("the first verifications after a staging rehashed %d times, want none: the staging's rehash serves each", n)
	}
	for _, name := range []string{"worker-0", "worker-1"} {
		if v := in.wck[name].checks.Verify; v == nil || len(v.Files) != 1 || v.Files[0].Outcome != agentapi.FileMatch {
			t.Errorf("%s's verification carries %+v, want the staging's rehash", name, v)
		}
	}
	// A new verification generation of worker-0 rehashes: the staging served its first one.
	i.Workers[0].Checks = &agentapi.Checks{VerificationGeneration: 2, Verify: &agentapi.VerifyRequest{Files: files}}
	h.releases["/run/worker-0.sock"] = make(chan struct{})
	h.runChecks(in, i)
	h.startedOn(t)
	h.release("/run/worker-0.sock")
	h.next(t, "worker-0's second verification ended")
	if n := rehashes.Load(); n != 1 {
		t.Errorf("worker-0's second verification rehashed %d times, want once", n)
	}
}
