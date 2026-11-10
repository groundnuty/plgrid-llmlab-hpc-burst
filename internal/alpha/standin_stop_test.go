package alpha

// The stack's own stop, for a stand-in whose scenario is meant to fail (PR #317's review). Run joins a failed close into the run's error,
// where every text such a stand-in is checked for still matches, and stack.Host.DownErr runs once, so the stack's registered clean-up
// neither retries nor reports it: a stand-in could pass with its stack still up. So Close keeps what it saw of the stop apart (stackStop),
// and those stand-ins check it before anything the scenario said. The check is the rehearsal's step (g)'s: the whole stack's processes
// as hpcburst status reports them before down, then none of them and none of the stack's ports left after it (stack.Host.Processes,
// stack.MissingComponents, stack.Left), looked at within the time the clean-ups after it leave (stopBy).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// stopWait bounds the look at what is left of the stack once down has returned, as step (g)'s wait does: down has stopped its processes
// by then, or failed.
const stopWait = 30 * time.Second

// stopReserve is the test's time the look at what is left keeps for what comes after it, before the test's deadline: Close's cancel of the
// run's jobs (EndJobs, a look every 5 s) and the stop of its ssh master, then the registered clean-ups (the fake cluster's KILL of every
// job, its ssh servers, the root k3s check). It is stack.CleanupMargin less down's default grace of 40 s, which that margin was sized with
// (harness.go): what the margin holds for after down. A look that would reach into it does not happen (PR #317's delta review).
const stopReserve = stack.CleanupMargin - 40*time.Second

// stopBy is when the look at what is left after down must end (stack.Left returns by it): wait from now, or stopReserve before the test's
// deadline when that is sooner. ok is false when that leaves less than the port read's own time (stack.LeftPortRead): then there is no
// look, which is no pass.
func stopBy(now time.Time, wait time.Duration, deadline time.Time, hasDeadline bool) (by time.Time, ok bool) {
	by = now.Add(wait)
	if last := deadline.Add(-stopReserve); hasDeadline && last.Before(by) {
		by = last
	}
	return by, by.Sub(now) >= stack.LeftPortRead
}

// stackStop is what Close saw of the stack's stop: the processes hpcburst status reported before down, whether down ran, its own error,
// and what was left after it, or why that was not looked at. The zero value is a stop nothing saw, which problem reports.
type stackStop struct {
	procs      []stack.Proc
	statusErr  error
	ran        bool
	downErr    error
	left       []string
	notChecked string
	// wait is how long the look at what is left may take; 0 is stopWait.
	wait time.Duration
	// deadline is the test's (t.Deadline) unless a test sets it.
	deadline func() (time.Time, bool)
}

// before reads the stack's processes, for after to compare; nothing on a nil stackStop, a run that does not check its stop.
func (s *stackStop) before(h *stack.Host) {
	if s != nil {
		s.procs, s.statusErr = h.Processes()
	}
}

// after keeps down's own result, and what is left of the stack within the time stopBy gives; nothing on a nil stackStop.
func (s *stackStop) after(t *testing.T, h *stack.Host, downErr error) {
	if s == nil {
		return
	}
	wait, deadline := s.wait, s.deadline
	if wait == 0 {
		wait = stopWait
	}
	if deadline == nil {
		deadline = t.Deadline
	}
	s.ran, s.downErr = h.Downed(), downErr
	dl, has := deadline()
	if by, ok := stopBy(time.Now(), wait, dl, has); ok {
		s.left = stack.Left(s.procs, h.Ports.All(), by)
	} else {
		s.notChecked = fmt.Sprintf("not checked: no time left (%s to the test's deadline, and what follows keeps %s of it)", time.Until(dl).Round(time.Second), stopReserve)
	}
	names := make([]string, 0, len(s.procs))
	for _, p := range s.procs {
		names = append(names, p.Name+"="+strconv.Itoa(p.PID))
	}
	t.Logf("the stack's stop: %d of its processes before down %v; down ran %t, its error %v; left after it %q%s", len(s.procs), names, s.ran, downErr, s.left, s.notChecked)
}

// problem is what kept the stop from being clean, nil when it was: the whole stack read before down (stack.MissingComponents, the set
// step (g) requires), down run without an error of its own, and none of those processes nor the stack's ports left after it, looked at.
func (s stackStop) problem() error {
	var errs []error
	if s.statusErr != nil {
		errs = append(errs, fmt.Errorf("the stack's processes before down: %w", s.statusErr))
	}
	if missing := stack.MissingComponents(s.procs); len(missing) > 0 {
		errs = append(errs, fmt.Errorf("hpcburst status before down reported no %s, so what is left of it after down is not known", strings.Join(missing, ", no ")))
	}
	if !s.ran {
		errs = append(errs, errors.New("hpcburst down did not run"))
	}
	if s.downErr != nil {
		errs = append(errs, fmt.Errorf("the stack's own shutdown failed: %w", s.downErr))
	}
	if s.notChecked != "" {
		errs = append(errs, errors.New("what is left after hpcburst down was "+s.notChecked))
	}
	if len(s.left) > 0 {
		errs = append(errs, fmt.Errorf("left after hpcburst down: %s", strings.Join(s.left, ", ")))
	}
	return errors.Join(errs...)
}

// catchUpFailedTexts are what the run's error must say when its result catch-up's LIST of Allocations failed: the catch-up, the reason,
// the injected failure and what was read of the second job.
var catchUpFailedTexts = []string{"result: whether the watch has caught up with the API is not known", "the API could not be listed", errAllocationList.Error(), ErrSecondNotStarted.Error()}

// catchUpFailed is the verdict on a run whose result catch-up's LIST of Allocations failed (lists, the count of failed LISTs): nil for a FAIL
// of the run itself, from that catch-up, on a stack that stopped cleanly. The stop is checked first and alone, so that a failed stop
// cannot pass inside an error the scenario was meant to give; then the run's error must be the scenario's alone, with no close's in it.
func (s secondHeldRun) catchUpFailed(lists int32) error {
	if err := s.stop.problem(); err != nil {
		return fmt.Errorf("the stack did not stop cleanly, whatever the run ended with: %w", err)
	}
	if lists == 0 {
		return fmt.Errorf("no LIST of Allocations was made, so no catch-up failed (failures %v, err %v)", s.failures, s.err)
	}
	if got := rowOutcome(s.row, s.failures, s.err, nil); got != "FAIL" || len(s.failures) != 0 || s.err == nil {
		return fmt.Errorf("the row ends %s (failures %v, err %v); want a FAIL of the run itself, from its result catch-up", got, s.failures, s.err)
	}
	var errs []error
	if strings.Contains(s.err.Error(), "closing the steps") {
		errs = append(errs, fmt.Errorf("err = %v, which carries a failure of the steps' close", s.err))
	}
	for _, want := range catchUpFailedTexts {
		if !strings.Contains(s.err.Error(), want) {
			errs = append(errs, fmt.Errorf("err = %v, which lacks %q", s.err, want))
		}
	}
	return errors.Join(errs...)
}

// catchUpFailsSteps is a handover whose scenario ends as the stand-ins' does when their result catch-up's LIST of Allocations fails: the
// real catch-up, over a real Recorder and a client whose every LIST of Allocations fails, beside a second job that never started.
type catchUpFailsSteps struct{ *liveRun }

func (s catchUpFailsSteps) Handover(ctx context.Context, _ Plan) (HandoverObs, error) {
	// syncWatch's body, written out: a function of this package that calls syncWatch is in the watch's inventory under its name
	// (stack.WatchUses), and one more Handover there would stand in for liveRun's own.
	const what = "the handover's result"
	err := s.watch.CatchUp(ctx, s.t, s.c, s.w.md.Namespace, stack.KindInstances|stack.KindAllocations, what).Err(what)
	if err == nil {
		s.t.Fatal("the catch-up told, with every LIST of Allocations failing")
	}
	return HandoverObs{}, notKnownBeside(err, &SecondNotStartedError{ReplacementLate: LateRaised})
}

// stackComponents are the processes the double's status names, one of this test's own each: k3s, recorded, and the supervised four.
var stackComponents = []string{"k3s", "supervisor", "controller", "front", "frps"}

// startSleepers starts one process of this test's own for each name, reaped once it ends, and killed at the test's end.
func startSleepers(t *testing.T, names []string) map[string]int {
	t.Helper()
	pids := map[string]int{}
	for _, name := range names {
		c := exec.Command("sleep", "600")
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		reaped := make(chan struct{})
		go func() {
			_ = c.Wait() // its exit is the double's kill, or the clean-up's
			close(reaped)
		}()
		t.Cleanup(func() {
			_ = c.Process.Kill() // it may have ended already
			<-reaped
		})
		pids[name] = c.Process.Pid
	}
	return pids
}

// statusOf is hpcburst status --json for the processes, as the launcher writes it, without the one omit names.
func statusOf(t *testing.T, pids map[string]int, omit string) []byte {
	t.Helper()
	entry := func(name string) map[string]any {
		return map[string]any{"name": name, "running": true, "pid": pids[name]}
	}
	st := map[string]any{"processes": []map[string]any{{"name": "k3s", "pid": pids["k3s"], "verdict": "ours"}}}
	if omit != "supervisor" {
		st["supervisor"] = map[string]any{"running": true, "pid": pids["supervisor"]}
	}
	var ctl []map[string]any
	for _, name := range []string{"controller", "front", "frps"} {
		if name != omit {
			ctl = append(ctl, entry(name))
		}
	}
	st["controllers"] = ctl
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The review of PR 317, and its delta: a stand-in whose run is meant to FAIL must not pass when the stack's own stop failed, or was not
// seen whole, though every text it checks the run's error for still matches. This runs the stand-ins' verdict (catchUpFailed) on what Run
// and liveRun.Close make, as they are, with no stack. hpcburst is a double: its status names a process of this test's own for each of
// k3s, the supervisor, the controller, the front and frps, and its down stops them all but one it spares, then exits as the case says.
// The cases are a down that fails; one that leaves the controller running; a status that omits the controller, which down then leaves
// running unseen; no time left before the test's deadline for the look at what is left; and the control, a down that stops the stack.
// The stack's ports are issue #303's 29880-29892, where nothing listens.
func TestAStandInWhoseStackDidNotStopFails(t *testing.T) {
	for _, c := range []struct {
		name        string
		omit, spare string // a component status leaves out, and one down leaves running
		exit        string // the double's down, after its kill
		wait        time.Duration
		near        bool     // the test's deadline 20 s off, inside stopReserve
		want        []string // what the verdict's error says; none for the control
	}{
		{"down fails", "", "", "echo went wrong; exit 3", 0, false, []string{"the stack's own shutdown failed: hpcburst down: exit status 3\nwent wrong"}},
		// The double's down returns once what it stopped has ended, and the controller never ends: so one look is all it takes (the port
		// read's time), and there is no window to race.
		{"down leaves a process", "", "controller", "exit 0", stack.LeftPortRead, false, []string{"left after hpcburst down: controller=", " still runs"}},
		{"status omits the controller, which down leaves running", "controller", "controller", "exit 0", 0, false, []string{"hpcburst status before down reported no running controller"}},
		{"no time left for the look", "", "controller", "exit 0", 0, true, []string{"what is left after hpcburst down was not checked: no time left"}},
		{"down stops the stack", "", "", "exit 0", 0, false, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			pids := startSleepers(t, stackComponents)
			var kill []string
			for _, name := range stackComponents {
				if name != c.spare {
					kill = append(kill, strconv.Itoa(pids[name]))
				}
			}
			dir := t.TempDir()
			state := filepath.Join(dir, "st")
			if err := os.Mkdir(state, 0o700); err != nil {
				t.Fatal(err)
			}
			status := filepath.Join(dir, "status.json")
			if err := os.WriteFile(status, statusOf(t, pids, c.omit), 0o600); err != nil {
				t.Fatal(err)
			}
			bin := filepath.Join(dir, "hpcburst")
			// As hpcburst down does, the double's down returns only once each process it stopped has ended: a zombie (this test reaps it)
			// or gone. It is bounded by down's own bound (stack.DownTimeout).
			script := `#!/bin/sh
case "$1" in
status) cat "$HB_TEST_STATUS" ;;
down)
	kill $HB_TEST_KILL
	for p in $HB_TEST_KILL; do
		while st=$(cut -d' ' -f3 "/proc/$p/stat" 2>/dev/null) && [ "$st" != Z ]; do sleep 0.05; done
	done
	` + c.exit + ` ;;
*) exit 2 ;;
esac
`
			if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			env := append(os.Environ(), "HB_TEST_STATUS="+status, "HB_TEST_KILL="+strings.Join(kill, " "))
			h := &stack.Host{T: t, B: stack.Binaries{HPCBurst: bin}, State: state, Env: env, Ports: stack.Ports{Base: 29880}}
			rec, _ := watchWithResets(t, resetPlan{})
			lists := new(atomic.Int32)
			stop := &stackStop{wait: c.wait}
			if c.near {
				stop.deadline = func() (time.Time, bool) { return time.Now().Add(20 * time.Second), true }
			}
			r := &liveRun{t: t, guardFail: make(chan struct{}), h: h, watch: rec, stop: stop,
				c: allocationListFails{Client: crfake.NewClientBuilder().WithScheme(eventScheme(t)).Build(), failed: lists}}
			r.w.md = &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Name: "alpha-0a1b2c3d4e", Namespace: "alpha-rehearsal"}}
			row := rowOf("alpha-athena-handover")
			cfg := Config{Row: row, Out: filepath.Join(t.TempDir(), "out"), PendingMax: 10 * time.Minute, FrontLabel: testFrontLabel}
			tags, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			failures, runErr := Run(context.Background(), cfg, catchUpFailsSteps{r}, tags, t.Logf)
			s := secondHeldRun{row: row, cfg: cfg, failures: failures, err: runErr, stop: *r.stop}
			// What the stand-in checked before this review: the row ends FAIL, and every text of the run's error is there.
			if got := rowOutcome(row, failures, runErr, nil); got != "FAIL" {
				t.Fatalf("the row ends %s (failures %v, err %v), want FAIL", got, failures, runErr)
			}
			for _, want := range catchUpFailedTexts {
				if !strings.Contains(runErr.Error(), want) {
					t.Errorf("err = %v, which lacks %q", runErr, want)
				}
			}
			verdict := s.catchUpFailed(lists.Load())
			if c.want == nil {
				if verdict != nil {
					t.Errorf("a stack that stopped: the verdict is %v, want none", verdict)
				}
				return
			}
			if verdict == nil {
				t.Fatalf("the verdict passes a run whose stack did not stop, or was not seen whole (the run's error: %v)", runErr)
			}
			for _, want := range c.want {
				if !strings.Contains(verdict.Error(), want) {
					t.Errorf("the verdict is %v, which lacks %q", verdict, want)
				}
			}
			if !strings.Contains(verdict.Error(), "the stack did not stop cleanly") {
				t.Errorf("the verdict is %v, want it to fail on the stop, before the scenario", verdict)
			}
		})
	}
}

// The look at what is left after down never takes the time kept for what follows it (PR #317's delta review): it ends stopReserve before
// the test's deadline at the latest, or does not happen. The last case is the review's: a step that gave up at the wait's cap
// (stack.CleanupMargin before the deadline), then a down that ran to its bound (stack.DownTimeout), leaves no time for it.
func TestTheStopCheckKeepsTheCleanupsReserve(t *testing.T) {
	now := time.Now()
	for _, c := range []struct {
		name     string
		deadline time.Time
		has      bool
		wantOK   bool
		wantBy   time.Time
	}{
		{"no deadline", time.Time{}, false, true, now.Add(stopWait)},
		{"a deadline far off", now.Add(time.Hour), true, true, now.Add(stopWait)},
		{"a deadline that shortens the look", now.Add(stopReserve + 10*time.Second), true, true, now.Add(10 * time.Second)},
		{"a deadline that leaves less than the port read", now.Add(stopReserve + stack.LeftPortRead - time.Second), true, false, time.Time{}},
		{"the wait's cap, then a down to its bound", now.Add(stack.CleanupMargin - stack.DownTimeout), true, false, time.Time{}},
	} {
		by, ok := stopBy(now, stopWait, c.deadline, c.has)
		if ok != c.wantOK || (ok && !by.Equal(c.wantBy)) {
			t.Errorf("%s: stopBy = %s after now, %t; want %s after now, %t", c.name, by.Sub(now), ok, c.wantBy.Sub(now), c.wantOK)
		}
		if ok && c.has && by.After(c.deadline.Add(-stopReserve)) {
			t.Errorf("%s: the look ends %s before the deadline, inside the %s kept for what follows", c.name, c.deadline.Sub(by), stopReserve)
		}
	}
}
