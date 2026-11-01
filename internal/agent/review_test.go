package agent_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

// heldRounds controls a warm-up round from the test, with gates and no timers (#184). Before the round calls
// the engine it waits for admit, which only the test's admission helper closes, so no request reaches the
// engine until the test has chosen to let it; and when the round comes back cancelled it announces that
// (cancelled, at cancelledAt) and does not return to the agent until the test closes release, so the agent
// has a cancelled round that is still unwinding for as long as the test wants.
type heldRounds struct {
	admit       chan struct{}
	release     chan struct{}
	cancelled   chan struct{}
	cancelledAt atomic.Int64 // unix nanos, when the cancelled round came back from the engine
	returned    atomic.Int64 // unix nanos, when it returned to the agent
	cancelOnce  sync.Once
	admitOnce   sync.Once
	releaseOnce sync.Once
}

func newHeldRounds(t *testing.T) *heldRounds {
	return &heldRounds{admit: make(chan struct{}), release: make(chan struct{}), cancelled: make(chan struct{})}
}

// letGo registers the clean-up that opens the gate and releases the round. A test that fails before it lets
// the round go must not leave the agent waiting for it when the harness stops the agent, so this is
// registered after the harness: clean-ups run last-in first-out, and this one runs first.
func (hr *heldRounds) letGo(t *testing.T) {
	t.Cleanup(func() {
		hr.openGate()
		hr.releaseRound()
	})
}

func (hr *heldRounds) openGate()     { hr.admitOnce.Do(func() { close(hr.admit) }) }
func (hr *heldRounds) releaseRound() { hr.releaseOnce.Do(func() { close(hr.release) }) }

// letAdmit opens the gate and waits for the engine to hold a warm-up request in flight: the engine
// double's "admitted" line of a streaming request (warm-up requests stream; verification's does not) with
// no end yet. It is the only thing that opens the gate, so a test that skips it never has a request admitted.
func (hr *heldRounds) letAdmit(t *testing.T, h *harness) {
	t.Helper()
	hr.openGate()
	eventually(t, 30*time.Second, "the engine to hold a warm-up request in flight", func() bool {
		admitted, ended := streamingCounts(t, h)
		return admitted > ended
	})
}

// heldRoundEngine is the agent's engine client with every warm-up round passed through the heldRounds.
type heldRoundEngine struct {
	agent.Engine
	hr *heldRounds
}

func (e heldRoundEngine) WarmUpRound(ctx context.Context, n int32, req agentapi.WarmUpRequest, s agentapi.Sampling) agentapi.WarmUpRound {
	select {
	case <-e.hr.admit:
	case <-ctx.Done():
	}
	r := e.Engine.WarmUpRound(ctx, n, req, s)
	if ctx.Err() != nil {
		e.hr.cancelledAt.Store(time.Now().UnixNano())
		e.hr.cancelOnce.Do(func() { close(e.hr.cancelled) })
		<-e.hr.release
		e.hr.returned.Store(time.Now().UnixNano())
	}
	return r
}

// WarmUp.Done is published only after the cancelled round has returned, so no report says warm-up is done
// while its requests are still unwinding (Astra B7). Nothing here is timed:
//
//   - The round cannot reach the engine until the test's admission helper (letAdmit) opens the gate, and
//     the helper returns when the engine double holds an admitted streaming request with no end. The drop
//     comes after that, so a request is in flight when it is cancelled; a test that skipped the helper would
//     drop warm-up while the gate is shut, and no request would ever be admitted to be cancelled.
//   - The cancelled round is then held: it does not return to the agent until the test releases it. The
//     test waits until the round has seen the cancellation, and then for a report that the agent took after
//     that, by its own clock (Report.Time). The agent takes every snapshot in its one loop, so that report
//     comes after the loop has handled the drop, and it must not say Done: the round has not returned. An
//     agent that published Done when it cancelled the round would say it here, in every such report.
//   - Only then is the round released, and Done must come, with a report taken after the round returned.
//   - The engine must have logged the admitted warm-up request as cancelled by its client.
func TestWarmUpDoneOnlyAfterTheCancelledRoundReturned(t *testing.T) {
	hr := newHeldRounds(t)
	h := newHarness(t, harnessOptions{engineWrap: func(e agent.Engine) agent.Engine { return heldRoundEngine{e, hr} }})
	hr.letGo(t)
	h.set(h.runState("stall", 1, 0, warmUpChecks(1, 2)))
	h.waitReport("warm-up started", 30*time.Second, func(r agentapi.Report) bool {
		c := checksOf(r)
		return c != nil && c.WarmUp != nil
	})
	hr.letAdmit(t, h)
	h.set(h.runState("stall", 1, 0, &agentapi.Checks{VerificationGeneration: 1}))
	select {
	case <-hr.cancelled:
	case <-time.After(30 * time.Second):
		t.Fatal("the warm-up round never saw its cancellation")
	}
	cancelledAt := time.Unix(0, hr.cancelledAt.Load())
	var after []agentapi.Report
	eventually(t, 30*time.Second, "a report that the agent took after the round saw its cancellation", func() bool {
		after = after[:0]
		for _, r := range h.backend.Reports(hAllocUID) {
			if r.Time.After(cancelledAt) {
				after = append(after, r)
			}
		}
		return len(after) > 0
	})
	for _, r := range after {
		if warmDone(r) {
			t.Fatalf("a report taken at %s, after the cancellation at %s, says warm-up is done while the cancelled round has not returned", r.Time.Format(time.StampMicro), cancelledAt.Format(time.StampMicro))
		}
	}
	hr.releaseRound()
	h.waitReport("warm-up done", 20*time.Second, warmDone)
	eventually(t, 15*time.Second, "the engine to log the admitted warm-up request cancelled by its client", func() bool {
		var admitted, cancelled int
		for _, ev := range engineEvents(t, h) {
			switch {
			case ev.Event == "admitted" && ev.Stream:
				admitted++
			case ev.Event == "request" && ev.Stream && ev.Outcome == "cancelled":
				cancelled++
			}
		}
		return admitted > 0 && cancelled > 0
	})
	reps, times := h.backend.Reports(hAllocUID), h.timed.reportTimes()
	if len(reps) != len(times) {
		t.Fatalf("%d reports, %d times", len(reps), len(times))
	}
	for k, r := range reps {
		if warmDone(r) {
			if ret := hr.returned.Load(); ret == 0 || times[k].UnixNano() < ret {
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

// stubApptainer writes an Apptainer whose exec starts a child and exits, as a
// parent that leaves a child behind. The child either handles TERM (it
// records "graceful <its PID>" and exits) or ignores it. The parent exits only
// once the child has set its TERM disposition, which the child says through a
// FIFO: the parent's exit is what makes the agent stop the group, and a TERM
// that comes before the child's trap kills it as if it had none (CI run
// 41236080709). Each exec logs whether the previous child still ran when it
// started, the child's PID, and the time just before it exits ("exiting
// <Unix ns>"); it also writes the child's PID and start time, taken while the
// child surely runs, to the children file.
func stubApptainer(t *testing.T, root string, childIgnoresTerm bool) (path, log, children string) {
	t.Helper()
	path = filepath.Join(root, "stub-apptainer")
	log = filepath.Join(root, "stub.log")
	children = filepath.Join(root, "children")
	// In a subshell $$ is the parent's PID; /proc/self is the subshell's.
	child := `(read -r me _ < /proc/self/stat; trap 'echo "graceful $me" >> LOG; exit 0' TERM; echo > READY; while :; do sleep 0.05; done) &`
	if childIgnoresTerm {
		child = `(trap '' TERM; echo > READY; exec sleep 300) &`
	}
	src := `#!/bin/sh
case "$1" in
pull) echo fake-sif > "$2"; exit 0 ;;
build) mkdir -p "$3"; exit 0 ;;
exec)
  if [ -f PREV ] && kill -0 "$(cat PREV)" 2>/dev/null; then echo overlap >> LOG; fi
  rm -f READY; mkfifo READY
  ` + child + `
  echo $! > PREV
  echo "$! $(cut -d' ' -f22 /proc/$!/stat)" >> CHILDREN
  echo "child $!" >> LOG
  read -r ready < READY
  echo "exiting $(date +%s%N)" >> LOG
  exit 0 ;;
esac
exit 2
`
	src = strings.ReplaceAll(src, "LOG", log)
	src = strings.ReplaceAll(src, "PREV", filepath.Join(root, "prev.pid"))
	src = strings.ReplaceAll(src, "READY", filepath.Join(root, "ready.fifo"))
	src = strings.ReplaceAll(src, "CHILDREN", children)
	if err := os.WriteFile(path, []byte(src), 0o700); err != nil {
		t.Fatal(err)
	}
	return path, log, children
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
			// Cleanups run last registered first: the harness stops the
			// agent, then every child the stub recorded is killed, whatever
			// the agent did, and only then does the directory go.
			t.Cleanup(func() { os.RemoveAll(root) })
			stub, log, children := stubApptainer(t, root, tc.ignores)
			t.Cleanup(func() { killChildren(children) })
			h := newHarness(t, harnessOptions{apptainer: stub})
			run := h.runState("", 1, 0, nil)
			grace := time.Duration(run.Settings.StopGrace)
			// The leftover is watched from the moment the stub records it,
			// apart from the agent's reports, which come only every 200 ms.
			const watchFor = 30 * time.Second
			watch, stopWatch := context.WithTimeout(context.Background(), watchFor)
			t.Cleanup(stopWatch)
			gone := make(chan time.Time, 1)
			go func() { gone <- leftoverGoneAt(watch, children) }()
			h.set(run)
			h.waitReport("worker-0 exited", 20*time.Second, func(r agentapi.Report) bool {
				w := worker0(r)
				return w != nil && w.Phase == agentapi.PartExited
			})
			h.set(h.runState("", 1, 1, nil))
			if tc.ignores {
				// TERM went to the group after the parent's exit, so the
				// child that ignores it runs on for the grace, to its KILL.
				at := <-gone
				if at.IsZero() {
					t.Fatalf("the leftover child that ignores TERM still ran %s after the start: its group's KILL did not end it", watchFor)
				}
				ns, err := strconv.ParseInt(lineAfter(readFile(log), "exiting "), 10, 64)
				if err != nil {
					t.Fatalf("no exit time in the stub log: %v\n%s", err, readFile(log))
				}
				if lived := at.Sub(time.Unix(0, ns)); lived < grace*5/6 {
					t.Fatalf("the leftover child that ignores TERM was gone %s after its parent exited, before most of the %s stop grace", lived, grace)
				}
			}
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
			// The leftover is the first exec's child: the restarted child's
			// graceful exit must not count for it.
			leftover := lineAfter(text, "child ")
			if !tc.ignores && !strings.Contains(text, "\ngraceful "+leftover+"\n") {
				t.Fatalf("the leftover child %s was killed without TERM first:\n%s", leftover, text)
			}
		})
	}
}

func readFile(p string) string {
	b, _ := os.ReadFile(p)
	return string(b)
}

// lineAfter returns the rest of the first line of text that starts with
// prefix, or "" when none does.
func lineAfter(text, prefix string) string {
	for _, line := range strings.Split(text, "\n") {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			return v
		}
	}
	return ""
}

// recordedChildren reads the stub's children file: one "<PID> <start time>"
// line per child, taken while the child ran.
func recordedChildren(path string) []procgroup.ID {
	var ids []procgroup.ID
	for _, line := range strings.Split(readFile(path), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		pid, err := strconv.Atoi(f[0])
		if err != nil {
			continue
		}
		start, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, procgroup.ID{PID: pid, StartTime: start})
	}
	return ids
}

// leftoverGoneAt waits for the stub's first child to be recorded, then polls
// it and returns when it was first seen gone, which is never before it ended;
// the zero time if it still ran when ctx ended.
func leftoverGoneAt(ctx context.Context, children string) time.Time {
	var ids []procgroup.ID
	for ids = recordedChildren(children); len(ids) == 0 || ids[0].Alive(); {
		select {
		case <-ctx.Done():
			return time.Time{}
		case <-time.After(5 * time.Millisecond):
		}
		if len(ids) == 0 {
			ids = recordedChildren(children)
		}
	}
	return time.Now()
}

// killChildren kills the stub's children that still run, by the PID and
// start time the stub recorded, so that a reused PID is never signalled.
func killChildren(children string) {
	for _, id := range recordedChildren(children) {
		if id.Alive() {
			_ = syscall.Kill(id.PID, syscall.SIGKILL)
		}
	}
}

// An engine that answers every request with a server error shows it in the
// health sample's responses by status, as the engine labels them (vLLM's
// grouped "5xx"), which agentapi.ServerErrors counts (Opus test nit).
func TestServerErrorsShowInTheHealthSample(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("errors-only", 1, 0, nil))
	r := h.waitReport("worker-0 running", 20*time.Second, running)
	sock := strings.TrimPrefix(r.Instance.ServingEndpoint, "unix:")
	for range 3 {
		if code := postChat(t, sock, r.Instance.Key); code != 500 {
			t.Fatalf("the errors-only engine answered %d", code)
		}
	}
	h.waitReport("server errors in the health sample", 10*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Health != nil && agentapi.ServerErrors(w.Health.ResponsesByStatus) >= 3
	})
}

// withReasoning declares a reasoning parser in the launch, with a context
// limit of 16,384 tokens: a check's answer budget plus the 4,096 reasoning
// tokens must fit beside its prompt, which the harness's 4,096 does not allow.
func withReasoning(d agentapi.DesiredState) agentapi.DesiredState {
	var cmd []string
	for _, a := range d.Instance.Launch.Command {
		if strings.HasPrefix(a, "--max-model-len=") {
			a = "--max-model-len=16384"
		}
		cmd = append(cmd, a)
	}
	d.Instance.Launch.Command = append(cmd, "--reasoning-parser=deepseek_r1")
	return d
}

// A reasoning model that reasons first passes every check, judged on its
// final answer, and its warm-up timings count from the first reasoning token,
// with the reasoning counted as output (Astra B11, Opus B2;
// `@hpc-burst-science-agent`, 31 Oct 2026).
func TestAReasoningEngineQualifiesAndWarmsUp(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	checks := qualifyChecks(1, true)
	checks.WarmUp = &agentapi.WarmUpRequest{Shapes: hShapes, TargetSessions: 1}
	h.set(withReasoning(h.runState("reasoning-first=50", 1, 0, checks)))
	r := h.waitReport("qualification and a warm-up round", 60*time.Second, func(r agentapi.Report) bool {
		c := checksOf(r)
		return qualified(1)(r) && c.WarmUp != nil && len(c.WarmUp.Rounds) >= 1
	})
	for _, c := range r.Instance.Checks.Qualify.Checks {
		if !c.Passed {
			t.Errorf("check %s failed on a healthy reasoning engine: %s", c.Name, c.Message)
		}
	}
	for _, q := range r.Instance.Checks.WarmUp.Rounds[0].Requests {
		if q.Error != "" {
			t.Errorf("%s warm-up request failed: %s", q.Shape, q.Error)
		}
	}
}

// A reasoning model that spends the whole budget reasoning and gives no final
// answer fails the behaviour check with its own reason, not as a wrong answer.
func TestReasoningThatExhaustsTheBudgetFailsWithItsOwnReason(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(withReasoning(h.runState("reasoning-forever", 1, 0, qualifyChecks(1, false))))
	r := h.waitReport("qualification", 60*time.Second, qualified(1))
	c, ok := outcome(*r.Instance.Checks.Qualify, agentapi.CheckBehaviour)
	if !ok || c.Passed || c.Reason != agentapi.ReasonReasoningBudgetExhausted {
		t.Fatalf("behaviour %+v, want a failure whose reason is ReasoningBudgetExhausted", c)
	}
	if c.ContextTokens <= 0 {
		t.Fatalf("behaviour outcome without the context it ran at: %+v", c)
	}
}

// An engine whose probe starts failing is reported with the failures counted
// in a row, and stays running: restarting it is the controller's call (Opus
// test nit; HBEP-7, Health and restarts).
func TestFailingProbesAreCountedAndNothingRestarts(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("health-fail-after=800ms", 1, 0, nil))
	pid := worker0(h.waitReport("worker-0 running", 20*time.Second, running)).PID
	r := h.waitReport("three failed probes in a row", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Health != nil && !w.Health.ProbeOK && w.Health.ConsecutiveProbeFailures >= 3
	})
	if w := worker0(r); w.Phase != agentapi.PartRunning || w.PID != pid {
		t.Fatalf("the worker changed on failing probes: %+v", w)
	}
	if n := len(h.execs()); n != 1 {
		t.Fatalf("%d starts: the agent restarted on failing probes", n)
	}
}

// failingMetrics reads the engine's metrics once, then fails every read.
type failingMetrics struct {
	agent.Engine
	reads *atomic.Int64
}

func (e failingMetrics) Sample(ctx context.Context) (agentapi.Health, error) {
	if e.reads.Add(1) > 1 {
		return agentapi.Health{}, context.DeadlineExceeded
	}
	return e.Engine.Sample(ctx)
}

// When the metrics read fails, the sample leaves the gauges out (unknown,
// never zero), keeps MetricsReadAt at the last read that succeeded, and still
// carries a fresh probe result; the controller then takes no gauge-based
// action (agentapi.Health.MetricsFresh; `@hpc-burst-science-agent`, 31 Oct
// 2026; Opus nit, CE-E: stale gauges with a fresh time).
func TestAFailedMetricsReadLeavesTheGaugesOut(t *testing.T) {
	var reads atomic.Int64
	h := newHarness(t, harnessOptions{engineWrap: func(e agent.Engine) agent.Engine { return failingMetrics{e, &reads} }})
	h.set(h.runState("", 1, 0, nil))
	first := h.waitReport("a sample with metrics", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Health != nil && w.Health.MetricsReadAt != nil && w.Health.RunningRequests != nil
	})
	// Through the real agent, the engine double's lines for the preemption
	// counter and the prefix-cache counters come out as known values (0 or
	// more), so the later nil is the failed read's doing, not a missing line.
	if fh := worker0(first).Health; fh.Preemptions == nil || fh.PrefixCacheQueriedTokens == nil || fh.PrefixCacheHitTokens == nil {
		t.Fatalf("the first sample lacks the engine counters: %+v", fh)
	}
	readAt := *worker0(first).Health.MetricsReadAt
	r := h.waitReport("a later sample after the metrics failed", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Health != nil && w.Health.Time.After(readAt.Add(300*time.Millisecond))
	})
	hl := worker0(r).Health
	if hl.RunningRequests != nil || hl.WaitingRequests != nil || hl.GeneratedTokens != nil || hl.KVCacheUsage != nil || hl.ResponsesByStatus != nil ||
		hl.Preemptions != nil || hl.PrefixCacheQueriedTokens != nil || hl.PrefixCacheHitTokens != nil ||
		hl.TimePerOutputTokenSecondsSum != nil || hl.TimePerOutputTokenCount != nil {
		t.Fatalf("gauges reported after the metrics read failed: %+v", hl)
	}
	if hl.MetricsReadAt == nil || !hl.MetricsReadAt.Equal(readAt) {
		t.Fatalf("MetricsReadAt %v, want the last successful read %v", hl.MetricsReadAt, readAt)
	}
	if !hl.ProbeOK {
		t.Fatal("the probe result is not fresh")
	}
}
