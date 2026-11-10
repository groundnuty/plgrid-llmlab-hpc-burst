package alpha

// The stack's own stop, for a stand-in whose scenario is meant to fail (PR #317's review). Run joins a failed close into the run's error,
// where every text such a stand-in is checked for still matches, and stack.Host.DownErr runs once, so the stack's registered clean-up
// neither retries nor reports it: a stand-in could pass with its stack still up. So Close keeps what it saw of the stop apart (stackStop),
// and those stand-ins check it before anything the scenario said. The check is the rehearsal's step (g)'s: the stack's processes as
// hpcburst status reports them before down, then none of them and none of the stack's ports left after it (stack.Host.Processes,
// stack.Left).

import (
	"context"
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

// stopWait bounds the wait for the stack's processes to end once down has returned, as step (g)'s does: down has stopped them by then,
// or failed.
const stopWait = 30 * time.Second

// stackStop is what Close saw of the stack's stop: the processes hpcburst status reported before down, whether down ran, its own error,
// and what was left after it. The zero value is a stop nothing saw, which problem reports.
type stackStop struct {
	procs     []stack.Proc
	statusErr error
	ran       bool
	downErr   error
	left      []string
	// wait is how long after down the processes may take to end; 0 is stopWait.
	wait time.Duration
}

// before reads the stack's processes, for after to compare; nothing on a nil stackStop, a run that does not check its stop.
func (s *stackStop) before(h *stack.Host) {
	if s != nil {
		s.procs, s.statusErr = h.Processes()
	}
}

// after keeps down's own result and what is left of the stack; nothing on a nil stackStop.
func (s *stackStop) after(t *testing.T, h *stack.Host, downErr error) {
	if s == nil {
		return
	}
	wait := s.wait
	if wait == 0 {
		wait = stopWait
	}
	s.ran, s.downErr = h.Downed(), downErr
	s.left = stack.Left(s.procs, h.Ports.All(), wait)
	names := make([]string, 0, len(s.procs))
	for _, p := range s.procs {
		names = append(names, p.Name+"="+strconv.Itoa(p.PID))
	}
	t.Logf("the stack's stop: %d of its processes before down %v; down ran %t, its error %v; left after it %q", len(s.procs), names, s.ran, downErr, s.left)
}

// problem is what kept the stop from being clean, nil when it was: the stack's processes read before down, down run without an error of
// its own, and none of those processes nor the stack's ports left after it.
func (s stackStop) problem() error {
	var errs []error
	if s.statusErr != nil {
		errs = append(errs, fmt.Errorf("the stack's processes before down: %w", s.statusErr))
	}
	if len(s.procs) == 0 {
		errs = append(errs, errors.New("hpcburst status reported no process of the stack before down, so nothing left after it would prove nothing"))
	}
	if !s.ran {
		errs = append(errs, errors.New("hpcburst down did not run"))
	}
	if s.downErr != nil {
		errs = append(errs, fmt.Errorf("the stack's own shutdown failed: %w", s.downErr))
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

// The review of PR 317: a stand-in whose run is meant to FAIL must not pass when the stack's own stop failed, though every text it checks
// the run's error for still matches. This runs the stand-ins' verdict (catchUpFailed) on what Run and liveRun.Close make, as they are,
// with no stack: hpcburst is a double whose status names a process of this test's own as k3s, and whose down fails (it stops the process,
// then exits 3), ends well but leaves the process running, or stops it, the control. The stack's ports are issue #303's 29880-29892, where
// nothing listens.
func TestAStandInWhoseStackDidNotStopFails(t *testing.T) {
	for _, c := range []struct {
		name, down string
		wait       time.Duration
		want       []string // what the verdict's error says; none for the control
	}{
		{"down fails", `kill "$HB_TEST_K3S_PID"; echo went wrong; exit 3`, 0, []string{"the stack's own shutdown failed: hpcburst down: exit status 3\nwent wrong"}},
		// The process never ends, so one look after down is all it takes, and there is no window to race.
		{"down leaves a process", `exit 0`, time.Nanosecond, []string{"left after hpcburst down: k3s=", " still runs"}},
		{"down stops the stack", `kill "$HB_TEST_K3S_PID"; exit 0`, 0, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			k3s := exec.Command("sleep", "600")
			if err := k3s.Start(); err != nil {
				t.Fatal(err)
			}
			reaped := make(chan struct{})
			go func() {
				_ = k3s.Wait() // its exit is the double's kill, or the clean-up's
				close(reaped)
			}()
			t.Cleanup(func() {
				_ = k3s.Process.Kill() // it may have ended already
				<-reaped
			})
			pid := strconv.Itoa(k3s.Process.Pid)
			dir := t.TempDir()
			state := filepath.Join(dir, "st")
			if err := os.Mkdir(state, 0o700); err != nil {
				t.Fatal(err)
			}
			bin := filepath.Join(dir, "hpcburst")
			script := "#!/bin/sh\ncase \"$1\" in\n" +
				`status) printf '{"processes":[{"name":"k3s","pid":%s,"verdict":"ours"}]}\n' "$HB_TEST_K3S_PID" ;;` + "\n" +
				"down) " + c.down + " ;;\n*) exit 2 ;;\nesac\n"
			if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			h := &stack.Host{T: t, B: stack.Binaries{HPCBurst: bin}, State: state, Env: append(os.Environ(), "HB_TEST_K3S_PID="+pid), Ports: stack.Ports{Base: 29880}}
			rec, _ := watchWithResets(t, resetPlan{})
			lists := new(atomic.Int32)
			r := &liveRun{t: t, guardFail: make(chan struct{}), h: h, watch: rec, stop: &stackStop{wait: c.wait},
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
				t.Fatalf("the verdict passes a run whose stack did not stop (the run's error: %v)", runErr)
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
