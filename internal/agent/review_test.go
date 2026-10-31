package agent_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Tests for the fixes of PR #87's review round. Each fails on the code the
// reviewers read.

func staging(r agentapi.Report) *agentapi.Staging {
	if r.Instance == nil {
		return nil
	}
	return r.Instance.Staging
}

func filesAwaited(r agentapi.Report) bool {
	st := staging(r)
	return st != nil && st.Phase == agentapi.StagingInProgress && strings.Contains(st.Message, "file list awaited")
}

// A start instruction without the identity record's file list starts no
// worker: staging stays in progress with a message saying the list is
// awaited, nothing is pulled or downloaded, and a verification without Files
// does not count. The list then starts it (Opus B3; `@hpc-burst-science-agent`,
// 31 Oct 2026).
func TestNoStartWithoutTheFileList(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, &agentapi.Checks{VerificationGeneration: 1}))
	h.waitReport("the file list awaited", 10*time.Second, filesAwaited)
	h.set(h.runState("", 1, 0, &agentapi.Checks{VerificationGeneration: 1, Verify: &agentapi.VerifyRequest{}}))
	time.Sleep(800 * time.Millisecond)
	r := mustLast(t, h)
	if !filesAwaited(r) {
		t.Fatalf("a verification without Files started staging: %+v", staging(r))
	}
	if cmds := h.appCommands(); len(cmds) != 0 {
		t.Fatalf("apptainer ran without a file list: %q", cmds)
	}
	if n := h.hubRequests(); n != 0 {
		t.Fatalf("%d downloads without a file list", n)
	}
	if len(r.Instance.Workers) != 0 {
		t.Fatalf("workers without a file list: %+v", r.Instance.Workers)
	}
	h.set(h.runState("", 1, 0, nil))
	h.waitReport("worker-0 running", 20*time.Second, running)
}

// A whole-Instance restart (rung 2) stages again, and waits for the file list
// of its own generation: the generation-1 list does not count for 2.
func TestRestageWaitsForItsGenerationsFileList(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, nil))
	h.waitReport("worker-0 running", 20*time.Second, running)
	h.set(h.runState("", 2, 0, &agentapi.Checks{VerificationGeneration: 2}))
	h.waitReport("the generation-2 file list awaited", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return filesAwaited(r) && w != nil && w.Phase == agentapi.PartStopped
	})
	time.Sleep(500 * time.Millisecond)
	if n := len(h.execs()); n != 1 {
		t.Fatalf("%d starts before the generation-2 file list came, want 1", n)
	}
	h.set(h.runState("", 2, 0, verifyChecks(2, nil)))
	h.waitReport("worker-0 running at instance generation 2", 20*time.Second, func(r agentapi.Report) bool {
		return running(r) && r.Instance.Generation == 2
	})
}

// After the start, verification only rehashes: a listed file that is missing
// is absent, and nothing is downloaded under the running engine, even when
// the Hub has it (Opus B3).
func TestVerificationAfterTheStartOnlyRehashes(t *testing.T) {
	rev := strings.Repeat("c", 40)
	// printf 'late\n' | sha256sum
	late := agentapi.FileDigest{Path: "late.json", SHA256: "f152945b358aa26a9e72e25381deff94e254c547089bd690dccd218e9414d148"}
	h := newHarness(t, harnessOptions{hub: map[string]string{"/org/model/resolve/" + rev + "/late.json": "late\n"}})
	h.set(h.runState("", 1, 0, nil))
	h.waitReport("verification 1", 30*time.Second, verified(1))
	before := h.hubRequests()
	h.set(h.runState("", 1, 0, verifyChecks(2, []agentapi.FileDigest{defaultFile, late})))
	r := h.waitReport("verification 2", 30*time.Second, verified(2))
	got := map[string]agentapi.FileOutcome{}
	for _, f := range r.Instance.Checks.Verify.Files {
		got[f.Path] = f.Outcome
	}
	if got["late.json"] != agentapi.FileAbsent || got["config.json"] != agentapi.FileMatch {
		t.Fatalf("files %+v, want config.json match and late.json absent", got)
	}
	if n := h.hubRequests(); n != before {
		t.Fatalf("verification after the start downloaded %d files", n-before)
	}
}

// startsAndChats returns, from the engine's record, the index of each start
// line and of each chat request, in order. The double records chat requests
// only; verification sends one (the fingerprint), and no probe does.
func startsAndChats(t *testing.T, h *harness) (starts, chats []int) {
	t.Helper()
	for i, line := range readLines(t, h.engLog) {
		var ev struct {
			Event string `json:"event"`
			Path  string `json:"path"`
		}
		if json.Unmarshal([]byte(line), &ev) != nil {
			continue
		}
		switch {
		case ev.Event == "start":
			starts = append(starts, i)
		case ev.Path == "/v1/chat/completions":
			chats = append(chats, i)
		}
	}
	return starts, chats
}

// A verification generation the controller sent before raising the worker's
// generation is first taken against the old process; once the worker
// restarts under the same verification generation, the old result is dropped
// and verification runs again against the new process (Opus B1, CE-A).
func TestVerificationTakenBeforeARestartIsTakenAgain(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, nil))
	first := h.waitReport("verification 1", 30*time.Second, verified(1))
	pid := worker0(first).PID
	h.set(h.runState("", 1, 0, verifyChecks(2, nil)))
	h.waitReport("verification 2 of the old process", 30*time.Second, verified(2))
	h.set(h.runState("", 1, 1, verifyChecks(2, nil)))
	r := h.waitReport("verification 2 of the restarted process", 30*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return verified(2)(r) && w.Generation == 1 && w.PID != pid
	})
	_ = r
	starts, chats := startsAndChats(t, h)
	if len(starts) != 2 {
		t.Fatalf("%d engine starts, want 2", len(starts))
	}
	after := false
	for _, c := range chats {
		after = after || c > starts[1]
	}
	if !after {
		t.Fatal("the restarted engine got no verification request: verification 2 was carried over from the old process")
	}
	// No report shows generation 2's verification for a worker at
	// generation 1 before that worker runs.
	for _, rep := range h.backend.Reports(hAllocUID) {
		c, w := checksOf(rep), worker0(rep)
		if c != nil && c.VerificationGeneration == 2 && c.Verify != nil && w != nil && w.Generation == 1 && w.Phase != agentapi.PartRunning {
			t.Fatalf("a report shows verification 2 while the generation-1 worker is %s", w.Phase)
		}
	}
}

// A file that changed after one verification fails the next: the staged
// rehash serves only the first verification after the staging (Astra B2,
// Opus B1, CE-B).
func TestAFileChangedBetweenGenerationsIsRehashed(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, nil))
	r := h.waitReport("verification 1", 30*time.Second, verified(1))
	if f := r.Instance.Checks.Verify.Files; len(f) != 1 || f[0].Outcome != agentapi.FileMatch {
		t.Fatalf("verification 1 files %+v", f)
	}
	p := filepath.Join(h.stagingDir, agentapi.StagedCheckpointDir("org/model", strings.Repeat("c", 40)), "config.json")
	if err := os.WriteFile(p, []byte("changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// printf 'changed\n' | sha256sum
	const changedSum = "7f8b1dfc466b6249f06cbe55c9174df2578e7754da793fded244ef5cba2a38f1"
	h.set(h.runState("", 1, 0, verifyChecks(2, nil)))
	r = h.waitReport("verification 2", 30*time.Second, verified(2))
	f := r.Instance.Checks.Verify.Files
	if len(f) != 1 || f[0].Outcome != agentapi.FileMismatch {
		t.Fatalf("verification 2 files %+v, want a mismatch for the changed file", f)
	}
	if f[0].SHA256 != changedSum {
		t.Fatalf("the mismatch reports %s, want the changed file's %s", f[0].SHA256, changedSum)
	}
}

// pulledDigest returns the digest the fake Apptainer was asked to pull.
func pulledDigest(t *testing.T, h *harness) string {
	t.Helper()
	for _, line := range readLines(t, h.appLog) {
		var inv appInvocation
		if json.Unmarshal([]byte(line), &inv) == nil && len(inv.Argv) > 0 && (inv.Argv[0] == "pull" || inv.Argv[0] == "build") {
			ref := inv.Argv[len(inv.Argv)-1]
			if i := strings.LastIndex(ref, "@"); i >= 0 {
				return ref[i+1:]
			}
		}
	}
	t.Fatal("the fake Apptainer pulled nothing")
	return ""
}

// unverifiedStager reports every image as not checked.
type unverifiedStager struct{ agent.Stager }

func (s unverifiedStager) Image(ctx context.Context, image, digest string, f agentapi.ImageFormat) (agent.ImageResult, error) {
	r, err := s.Stager.Image(ctx, image, digest, f)
	r.Verified = false
	return r, err
}

// The image digest is reported only for an image whose integrity check
// passed: an unchecked image gets none, which the controller fails, instead of
// the declared digest copied back (Astra B1, Opus B5).
func TestAnUncheckedImageGetsNoDigest(t *testing.T) {
	h := newHarness(t, harnessOptions{stagerWrap: func(s agent.Stager) agent.Stager { return unverifiedStager{s} }})
	h.set(h.runState("", 1, 0, nil))
	r := h.waitReport("verification 1", 30*time.Second, verified(1))
	if d := r.Instance.Checks.Verify.ImageDigest; d != "" {
		t.Fatalf("image digest %q for an image that was not checked", d)
	}
}

// slowCancelEngine makes a cancelled warm-up round take a while to return, as
// requests unwinding do, and records when it returned.
type slowCancelEngine struct {
	agent.Engine
	returned *atomic.Int64 // unix nanos
}

func (e slowCancelEngine) WarmUpRound(ctx context.Context, n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
	r := e.Engine.WarmUpRound(ctx, n, req)
	if ctx.Err() != nil {
		time.Sleep(500 * time.Millisecond)
		e.returned.Store(time.Now().UnixNano())
	}
	return r
}

// WarmUp.Done is published only after the cancelled round has returned, so no
// report says warm-up is done while its requests are still unwinding (Astra
// B7).
func TestWarmUpDoneOnlyAfterTheCancelledRoundReturned(t *testing.T) {
	var returned atomic.Int64
	h := newHarness(t, harnessOptions{engineWrap: func(e agent.Engine) agent.Engine { return slowCancelEngine{e, &returned} }})
	h.set(h.runState("stall", 1, 0, warmUpChecks(1, 2)))
	h.waitReport("warm-up started", 30*time.Second, func(r agentapi.Report) bool {
		c := checksOf(r)
		return c != nil && c.WarmUp != nil
	})
	time.Sleep(300 * time.Millisecond)
	h.set(h.runState("stall", 1, 0, &agentapi.Checks{VerificationGeneration: 1}))
	h.waitReport("warm-up done", 20*time.Second, warmDone)
	reps, times := h.backend.Reports(hAllocUID), h.timed.reportTimes()
	if len(reps) != len(times) {
		t.Fatalf("%d reports, %d times", len(reps), len(times))
	}
	for k, r := range reps {
		if warmDone(r) {
			if ret := returned.Load(); ret == 0 || times[k].UnixNano() < ret {
				t.Fatalf("warm-up was reported done at %s, before the cancelled round returned (%d)", times[k].Format(time.StampMicro), ret)
			}
			return
		}
	}
	t.Fatal("no report says warm-up is done")
}

// neverKeeps accepts every registration and then answers every poll as not
// registered, as a controller that turns an agent away might.
type neverKeeps struct {
	agentapi.Backend
	registers atomic.Int64
}

func (b *neverKeeps) Register(ctx context.Context, c agentapi.Caller, r agentapi.RegisterRequest) (agentapi.RegisterResponse, error) {
	b.registers.Add(1)
	return b.Backend.Register(ctx, c, r)
}

func (b *neverKeeps) Desired(context.Context, agentapi.Caller) (agentapi.DesiredState, error) {
	return agentapi.DesiredState{}, agentapi.Errorf(agentapi.CodeNotRegistered, "turned away")
}

// A not-registered answer right after a fresh registration is retried with
// backoff and logged once, not in a hot loop (Opus B6, CE-F: 59,410 calls in a
// second).
func TestNotRegisteredRightAfterRegisteringBacksOff(t *testing.T) {
	nk := &neverKeeps{}
	h := newHarness(t, harnessOptions{backendWrap: func(b agentapi.Backend) agentapi.Backend { nk.Backend = b; return nk }})
	time.Sleep(time.Second)
	if n := nk.registers.Load(); n > 20 {
		t.Fatalf("%d registrations in a second: a hot loop", n)
	}
	if n := strings.Count(h.logs.String(), "right after a registration"); n != 1 {
		t.Fatalf("the condition was logged %d times, want once", n)
	}
}

// A malformed desired state is logged once per Version, not at every converge.
func TestAMalformedDesiredStateIsLoggedOnce(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(agentapi.DesiredState{Action: agentapi.ActionRun})
	time.Sleep(1500 * time.Millisecond)
	if n := strings.Count(h.logs.String(), "malformed desired state ignored"); n != 1 {
		t.Fatalf("logged %d times, want once", n)
	}
}

// The Hub token reaches the Hub, in the Authorization header of the download.
func TestTheHubTokenReachesTheHub(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	d := h.runState("", 1, 0, nil)
	d.Instance.HubToken = "hf_knowntoken"
	h.set(d)
	h.waitReport("verification 1", 30*time.Second, verified(1))
	h.hubMu.Lock()
	auth := append([]string(nil), h.hubAuth...)
	h.hubMu.Unlock()
	if len(auth) == 0 || auth[0] != "Bearer hf_knowntoken" {
		t.Fatalf("the Hub saw Authorization %q", auth)
	}
}

// stubApptainer writes an Apptainer whose exec starts a child and exits at
// once, as a parent that leaves a child behind. The child either handles
// TERM (it records it and exits) or ignores it. Each exec records whether the
// previous child still ran when it started, and the child's PID.
func stubApptainer(t *testing.T, root string, childIgnoresTerm bool) (path, log string) {
	t.Helper()
	path = filepath.Join(root, "stub-apptainer")
	log = filepath.Join(root, "stub.log")
	child := `(trap 'echo graceful >> LOG; exit 0' TERM; while :; do sleep 0.05; done) &`
	if childIgnoresTerm {
		child = `(trap '' TERM; exec sleep 300) &`
	}
	src := `#!/bin/sh
case "$1" in
pull) echo fake-sif > "$2"; exit 0 ;;
build) mkdir -p "$3"; exit 0 ;;
exec)
  if [ -f PREV ] && kill -0 "$(cat PREV)" 2>/dev/null; then echo overlap >> LOG; fi
  ` + child + `
  echo $! > PREV
  echo "child $!" >> LOG
  exit 0 ;;
esac
exit 2
`
	src = strings.ReplaceAll(src, "LOG", log)
	src = strings.ReplaceAll(src, "PREV", filepath.Join(root, "prev.pid"))
	if err := os.WriteFile(path, []byte(src), 0o700); err != nil {
		t.Fatal(err)
	}
	return path, log
}

// A worker whose leader exits without being told to, leaving a child behind,
// is reported exited at once; the rest of its group gets the stop grace (TERM
// first, so a child that handles TERM ends cleanly), then KILL; and a restart
// starts only once the group is gone (Astra B4).
func TestALeftoverWorkerChildGetsItsGraceAndBlocksTheRestart(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ignores bool
	}{{"child handles TERM", false}, {"child ignores TERM", true}} {
		t.Run(tc.name, func(t *testing.T) {
			root, err := os.MkdirTemp("", "hbw")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(root)
			stub, log := stubApptainer(t, root, tc.ignores)
			h := newHarness(t, harnessOptions{apptainer: stub})
			h.set(h.runState("", 1, 0, nil))
			h.waitReport("worker-0 exited", 20*time.Second, func(r agentapi.Report) bool {
				w := worker0(r)
				return w != nil && w.Phase == agentapi.PartExited
			})
			h.set(h.runState("", 1, 1, nil))
			deadline := time.Now().Add(20 * time.Second)
			for strings.Count(readFile(log), "child ") < 2 {
				if time.Now().After(deadline) {
					t.Fatalf("no restart; stub log:\n%s", readFile(log))
				}
				time.Sleep(20 * time.Millisecond)
			}
			text := readFile(log)
			if strings.Contains(text, "overlap") {
				t.Fatalf("the restart started while the old group's child still ran:\n%s", text)
			}
			if !tc.ignores && !strings.Contains(text, "graceful") {
				t.Fatalf("the leftover child was killed without TERM first:\n%s", text)
			}
			killChildren(t, text)
		})
	}
}

func readFile(p string) string {
	b, _ := os.ReadFile(p)
	return string(b)
}

// killChildren kills the stub's children that still run, by PID and start
// time, at the end of a test.
func killChildren(t *testing.T, log string) {
	t.Helper()
	for _, line := range strings.Split(log, "\n") {
		if v, ok := strings.CutPrefix(line, "child "); ok {
			if pid, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
				if id, err := procgroup.IDOf(pid); err == nil && id.Alive() {
					_ = syscall.Kill(pid, syscall.SIGKILL)
				}
			}
		}
	}
}
