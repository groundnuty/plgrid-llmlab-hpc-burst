package conformance

// The start-wait bound, tested without a platform: the wait loop on a fake
// clock, and a job the fake Slurm holds pending past the bound, which the run
// must cancel by its tag and end as NOT RUN. The expected outcomes come from
// the issue (a job that does not start within 10 minutes is cancelled by its
// tag and the row is reported NOT RUN, not failed), not from the code.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func TestReadSettingsStartWait(t *testing.T) {
	s, err := readSettings(env(full()))
	if err != nil || s.startWait != 0 {
		t.Fatalf("unset: startWait %v, err %v; want 0 and no error", s.startWait, err)
	}
	m := full()
	m["HPCBURST_CONF_START_WAIT"] = "10m"
	if s, err = readSettings(env(m)); err != nil || s.startWait != 10*time.Minute {
		t.Errorf("10m: startWait %v, err %v; want 10m", s.startWait, err)
	}
	for _, bad := range []string{"10", "ten minutes", "30s", "0m", "-5m", "31m", "2h"} {
		m["HPCBURST_CONF_START_WAIT"] = bad
		if _, err := readSettings(env(m)); err == nil || !strings.Contains(err.Error(), "HPCBURST_CONF_START_WAIT is not a duration from 1m to 30m0s") {
			t.Errorf("%q: error = %v, want a refusal naming the setting and its range", bad, err)
		}
	}
}

// fakeTime is a clock that only moves when the wait sleeps.
type fakeTime struct{ c *fake.Clock }

func (f fakeTime) now() time.Time        { return f.c.Now() }
func (f fakeTime) sleep(d time.Duration) { f.c.Advance(d) }
func newFakeTime() fakeTime {
	return fakeTime{fake.NewClock(time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC))}
}
func (f fakeTime) since(t time.Time) time.Duration { return f.now().Sub(t) }

// A job that never starts is given up on at the bound and no later; one that
// starts before the bound, or at it, is a start; and a job never accepted is
// told apart from one that is accepted and waiting.
func TestWaitStartStopsAtTheBound(t *testing.T) {
	const bound, every = 10 * time.Minute, 3 * time.Second
	for _, tc := range []struct {
		name       string
		startsAt   time.Duration // the fake time after which the job runs; <0: never
		acceptedAt time.Duration // the fake time after which a job is accepted; <0: never
		want       startState
		wantWithin [2]time.Duration // the time waited
	}{
		{"never starts", -1, 0, jobDidNotStart, [2]time.Duration{bound, bound + every}},
		{"starts early", 9 * time.Second, 0, jobStarted, [2]time.Duration{9 * time.Second, 9*time.Second + every}},
		{"starts just before the bound", bound - every, 0, jobStarted, [2]time.Duration{bound - every, bound}},
		{"starts at the bound", bound, 0, jobStarted, [2]time.Duration{bound - every, bound + every}},
		{"never accepted", -1, -1, jobNotAccepted, [2]time.Duration{bound, bound + every}},
		{"accepted late and not started", -1, 8 * time.Minute, jobDidNotStart, [2]time.Duration{bound, bound + every}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ft := newFakeTime()
			since := ft.now()
			polls := 0
			got := waitStart(ft.now, ft.sleep, since, bound, every, func() (bool, bool) {
				polls++
				el := ft.since(since)
				return tc.startsAt >= 0 && el >= tc.startsAt, tc.acceptedAt >= 0 && el >= tc.acceptedAt
			})
			if got != tc.want {
				t.Errorf("outcome %d, want %d", got, tc.want)
			}
			if w := ft.since(since); w < tc.wantWithin[0] || w > tc.wantWithin[1] {
				t.Errorf("waited %s, want between %s and %s", w, tc.wantWithin[0], tc.wantWithin[1])
			}
			if polls < 1 {
				t.Error("the probe was never called")
			}
		})
	}
	// A bound that has passed before the wait begins still looks once.
	ft := newFakeTime()
	polls := 0
	if got := waitStart(ft.now, ft.sleep, ft.now().Add(-time.Hour), bound, every, func() (bool, bool) { polls++; return true, true }); got != jobStarted || polls != 1 {
		t.Errorf("a passed bound: outcome %d after %d polls, want a start after exactly one", got, polls)
	}
}

// stopRecorder stands in for a test whose Fatalf and Skipf end it, as testing's
// do, and records which one did.
type stopRecorder struct {
	testing.TB
	failed, skipped string
	errors          []string
}

func (r *stopRecorder) Helper() {}
func (r *stopRecorder) Fatalf(f string, a ...any) {
	r.failed = fmt.Sprintf(f, a...)
	runtime.Goexit()
}
func (r *stopRecorder) Skipf(f string, a ...any) {
	r.skipped = fmt.Sprintf(f, a...)
	runtime.Goexit()
}
func (r *stopRecorder) Logf(string, ...any)       {}
func (r *stopRecorder) Errorf(f string, a ...any) { r.errors = append(r.errors, fmt.Sprintf(f, a...)) }

// ends runs fn as the test body, in a goroutine, so that Fatalf and Skipf can
// end it.
func ends(fn func()) {
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	<-done
}

// held is a fake Slurm with two held jobs on it: one this run tagged, one a
// foreign job with a tag of the same form that the run never wrote down.
type held struct {
	w               *world
	rec             *stopRecorder
	sl              *fakeslurm.Platform
	ft              fakeTime
	ours, foreign   string // job IDs
	ourTag, foreTag string
	dir             string
	cancelFails     *bool
	queueFails      *bool
}

type cancelFault struct {
	slurm.Runner
	fail, queueFail *bool
}

func (c cancelFault) Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error) {
	if *c.fail && strings.HasPrefix(cmd.Line, "scancel") {
		return slurm.Result{}, fmt.Errorf("scancel: connection reset")
	}
	if *c.queueFail && strings.HasPrefix(cmd.Line, "squeue") {
		return slurm.Result{}, fmt.Errorf("squeue: connection reset")
	}
	return c.Runner.Run(ctx, cmd)
}

func newHeld(t *testing.T) *held {
	t.Helper()
	ft := newFakeTime()
	sl := fakeslurm.New(ft.c, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: "plgrid"}}})
	session := sl.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": "/home/example-user", "SCRATCH": "/scratch/example-user"}})
	pp, part, err := loadProfile(profilesDir(), "helios", "plgrid", "$SCRATCH/example-run")
	if err != nil {
		t.Fatal(err)
	}
	fail, queueFail := new(bool), new(bool)
	b := slurm.NewBatch(cancelFault{session, fail, queueFail}, provider.Profile{Name: pp.Name, Spec: pp.Spec, Partition: part},
		provider.Settings{Provider: "example", Account: "example-account", Partition: "plgrid"}, slurm.Options{Clock: ft.c})
	dir := t.TempDir()
	rec := &stopRecorder{TB: t}
	w := &world{t: rec, p: newPrivate(t, dir), batch: b, jobIDs: map[string]bool{}, now: ft.now, sleep: ft.sleep}
	w.s.account, w.s.startWait = "example-account", 10*time.Minute
	digest := sha256.Sum256([]byte("example agent"))
	params := v1alpha1.BatchParameters{Partition: "plgrid", TimeLimit: "5m", CPUsPerTask: 1, Memory: resource.MustParse("1Gi"), Nodes: 1, AgentDigest: hex.EncodeToString(digest[:])}
	h := &held{w: w, rec: rec, sl: sl, ft: ft, dir: dir, cancelFails: fail, queueFails: queueFail}
	submit := func(name, uid string, ours bool) (string, string) {
		tag := v1alpha1.JobTag("conformance", name, types.UID("uid-"+uid))
		if ours {
			w.p.tag(tag)
		}
		id, outcome, err := b.Submit(context.Background(), heldRequest(w.s, tag, params))
		if err != nil || outcome != provider.Accepted {
			t.Fatalf("submit %s: %s %v", name, outcome, err)
		}
		return id, tag
	}
	h.ours, h.ourTag = submit("conformance-held-submit-and-cancel-on-delete", "1", true)
	h.foreign, h.foreTag = submit("someone-elses-job", "2", false)
	return h
}

func (h *held) state(t *testing.T, id string) (state string, started bool) {
	t.Helper()
	n, _ := strconv.Atoi(id)
	j, ok := h.sl.Job(n)
	if !ok {
		t.Fatalf("the fake holds no job %s", id)
	}
	return string(j.State), !j.Start.IsZero()
}

func (h *held) pending(name string) func(string) *v1alpha1.Allocation {
	return func(string) *v1alpha1.Allocation {
		return &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{v1alpha1.AnnotationJobID: h.ours}},
			Status: v1alpha1.AllocationStatus{AtProvider: v1alpha1.AllocationObservation{Phase: v1alpha1.AllocationPending}}}
	}
}

func running(a *v1alpha1.Allocation) bool {
	return a.Status.AtProvider.Phase == v1alpha1.AllocationRunning
}

// A job the fake holds pending past the bound: the wait ends at the bound, the
// run's own job is cancelled by its tag and the foreign job beside it is not,
// the marker is written without a job ID or a tag, and the test is skipped,
// not failed.
func TestAJobHeldPastTheBoundIsCancelledByItsTagAndTheRunIsNotRun(t *testing.T) {
	h := newHeld(t)
	h.w.poll = h.pending("step")
	began := h.ft.now()
	ends(func() { h.w.untilRuns("step 1", "step", "the job runs", h.w.at(), "", running) })

	if h.rec.failed != "" {
		t.Fatalf("the run failed: %s", h.rec.failed)
	}
	if !strings.HasPrefix(h.rec.skipped, "NOT RUN: step 1: a job did not start within 10m0s of its acceptance") {
		t.Fatalf("skipped = %q, want a NOT RUN for step 1 with the bound", h.rec.skipped)
	}
	if waited := h.ft.since(began); waited < 10*time.Minute || waited > 11*time.Minute {
		t.Errorf("the wait lasted %s, want the bound of 10m plus the cancel's few seconds", waited)
	}
	if st, started := h.state(t, h.ours); st != string(fakeslurm.Cancelled) || started {
		t.Errorf("the run's job is %s (started %v), want cancelled and never started", st, started)
	}
	if st, _ := h.state(t, h.foreign); st == string(fakeslurm.Cancelled) {
		t.Errorf("the foreign job was cancelled: only a job whose tag the run wrote down is the run's")
	}
	if n := h.sl.Count("scancel"); n != 1 {
		t.Errorf("%d scancel calls, want exactly the run's one job", n)
	}
	raw, err := os.ReadFile(filepath.Join(h.dir, notRunFile))
	if err != nil {
		t.Fatalf("no marker: %v", err)
	}
	if info, _ := os.Stat(filepath.Join(h.dir, notRunFile)); info.Mode().Perm() != 0o600 {
		t.Errorf("the marker has mode %v, want 600", info.Mode().Perm())
	}
	for _, leak := range []string{h.ours, h.ourTag, h.foreTag} {
		if strings.Contains(string(raw), leak) {
			t.Errorf("the marker names %q", leak)
		}
	}
	if !strings.Contains(string(raw), "step 1") || !strings.Contains(string(raw), "10m0s") {
		t.Errorf("marker = %q, want it to name the step and the bound", raw)
	}
}

// A run with no start wait waits for the wait in the queue instead (HPCBURST_CONF_PENDING_MAX_SECONDS; the science
// agent's ruling of 8 Nov 2026, #272): a job held past it is cancelled by its tag and the run is NOT RUN, with the
// marker saying it was the wait in the queue, not the start-wait bound. The expected bound is written out.
func TestAJobHeldPastTheWaitInTheQueueIsCancelledByItsTagAndTheRunIsNotRun(t *testing.T) {
	h := newHeld(t)
	h.w.s.startWait, h.w.s.pendingMax = 0, 20*time.Minute
	h.w.poll = h.pending("step")
	began := h.ft.now()
	ends(func() { h.w.untilRuns("step 1", "step", "the job runs", h.w.at(), "", running) })

	if h.rec.failed != "" {
		t.Fatalf("the run failed: %s", h.rec.failed)
	}
	if !strings.HasPrefix(h.rec.skipped, "NOT RUN: step 1: a job was still pending after 20m0s in the queue") {
		t.Fatalf("skipped = %q, want a NOT RUN for step 1 naming the wait in the queue", h.rec.skipped)
	}
	if waited := h.ft.since(began); waited < 20*time.Minute || waited > 21*time.Minute {
		t.Errorf("the wait lasted %s, want the wait in the queue of 20m plus the cancel's few seconds", waited)
	}
	if st, started := h.state(t, h.ours); st != string(fakeslurm.Cancelled) || started {
		t.Errorf("the run's job is %s (started %v), want cancelled and never started", st, started)
	}
	if st, _ := h.state(t, h.foreign); st == string(fakeslurm.Cancelled) {
		t.Errorf("the foreign job was cancelled")
	}
	raw, err := os.ReadFile(filepath.Join(h.dir, notRunFile))
	if err != nil {
		t.Fatalf("no marker: %v", err)
	}
	if !strings.Contains(string(raw), "still pending after 20m0s in the queue") || strings.Contains(string(raw), "start-wait") {
		t.Errorf("marker = %q, want it to name the wait in the queue and not the start-wait bound", raw)
	}
	// The same wait with a job never accepted, and with a cancel that fails, stays a failure.
	t.Run("never accepted", func(t *testing.T) {
		h := newHeld(t)
		h.w.s.startWait, h.w.s.pendingMax = 0, 20*time.Minute
		h.w.poll = func(name string) *v1alpha1.Allocation {
			return &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: name}}
		}
		ends(func() { h.w.untilRuns("step 1", "step", "the job runs", h.w.at(), "", running) })
		if !strings.Contains(h.rec.failed, "step 1: no job was accepted within the wait in the queue of 20m0s") || h.rec.skipped != "" {
			t.Errorf("failed %q, skipped %q; want a failure for a job never accepted", h.rec.failed, h.rec.skipped)
		}
	})
	t.Run("the cancel fails", func(t *testing.T) {
		h := newHeld(t)
		h.w.s.startWait, h.w.s.pendingMax = 0, 20*time.Minute
		*h.cancelFails = true
		h.w.poll = h.pending("step")
		ends(func() { h.w.untilRuns("step 1", "step", "the job runs", h.w.at(), "", running) })
		if !strings.Contains(h.rec.failed, "cancelling this run's jobs by tag failed") || h.rec.skipped != "" {
			t.Errorf("failed %q, skipped %q; want a failure naming the cancel", h.rec.failed, h.rec.skipped)
		}
		if _, err := os.Stat(filepath.Join(h.dir, notRunFile)); err == nil {
			t.Error("a marker was written although a job may be live")
		}
	})
}

// A job that starts in time is a start: nothing is cancelled and no marker is
// written. A job that was never accepted is a failure, with nothing cancelled
// (nothing of the run's was submitted by the step) and no marker.
func TestAJobThatStartsInTimeOrWasNeverAcceptedIsNotANotRun(t *testing.T) {
	t.Run("starts in time", func(t *testing.T) {
		h := newHeld(t)
		polls := 0
		h.w.poll = func(name string) *v1alpha1.Allocation {
			polls++
			a := h.pending(name)(name)
			if polls >= 5 {
				a.Status.AtProvider.Phase = v1alpha1.AllocationRunning
			}
			return a
		}
		var got *v1alpha1.Allocation
		ends(func() { got = h.w.untilRuns("step 1", "step", "the job runs", h.w.at(), "", running) })
		if h.rec.failed != "" || h.rec.skipped != "" || got == nil || !running(got) {
			t.Fatalf("failed %q, skipped %q, allocation %v; want a plain start", h.rec.failed, h.rec.skipped, got)
		}
		if n := h.sl.Count("scancel"); n != 0 {
			t.Errorf("%d scancel calls, want none", n)
		}
		if _, err := os.Stat(filepath.Join(h.dir, notRunFile)); err == nil {
			t.Error("a marker was written for a job that started")
		}
	})
	t.Run("never accepted", func(t *testing.T) {
		h := newHeld(t)
		h.w.poll = func(name string) *v1alpha1.Allocation {
			return &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: name}}
		}
		ends(func() { h.w.untilRuns("step 1", "step", "the job runs", h.w.at(), "", running) })
		if !strings.Contains(h.rec.failed, "step 1: no job was accepted within the start-wait bound of 10m0s") || h.rec.skipped != "" {
			t.Errorf("failed %q, skipped %q; want a failure for a job never accepted", h.rec.failed, h.rec.skipped)
		}
		if _, err := os.Stat(filepath.Join(h.dir, notRunFile)); err == nil {
			t.Error("a marker was written for a failure")
		}
	})
}

// A cancel that fails leaves a job on the platform: that is a failure with no
// marker, never a NOT RUN.
func TestAFailedCancelIsAFailureNotANotRun(t *testing.T) {
	h := newHeld(t)
	*h.cancelFails = true
	h.w.poll = h.pending("step")
	ends(func() { h.w.untilRuns("step 3", "step", "the job runs", h.w.at(), h.ours, running) })
	if !strings.Contains(h.rec.failed, "cancelling this run's jobs by tag failed") || h.rec.skipped != "" {
		t.Errorf("failed %q, skipped %q; want a failure naming the cancel", h.rec.failed, h.rec.skipped)
	}
	if _, err := os.Stat(filepath.Join(h.dir, notRunFile)); err == nil {
		t.Error("a marker was written although a job may be live")
	}
}

// jobNumber is the fake's number for a job ID.
func jobNumber(t *testing.T, id string) int {
	t.Helper()
	n, err := strconv.Atoi(id)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// lifted lifts the hold of the run's job, so that the fake can start it, and returns its number.
func (h *held) lifted(t *testing.T) int {
	t.Helper()
	if err := h.w.batch.LiftHold(context.Background(), h.ours); err != nil {
		t.Fatal(err)
	}
	return jobNumber(t, h.ours)
}

// startedOnTheFake lifts the hold and starts the run's job on the fake, and returns its number.
func (h *held) startedOnTheFake(t *testing.T) int {
	t.Helper()
	n := h.lifted(t)
	if err := h.sl.Start(n); err != nil {
		t.Fatal(err)
	}
	return n
}

// allocationIn is the Allocation of the run's job as a poll reads it, in a phase, with a change to it.
func (h *held) allocationIn(phase v1alpha1.AllocationPhase, change func(*v1alpha1.Allocation)) func(string) *v1alpha1.Allocation {
	return func(name string) *v1alpha1.Allocation {
		a := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{v1alpha1.AnnotationJobID: h.ours}},
			Status: v1alpha1.AllocationStatus{AtProvider: v1alpha1.AllocationObservation{Phase: phase, Reason: "the reason"}}}
		if change != nil {
			change(a)
		}
		return a
	}
}

// B1 of the review of PR 284: NOT RUN means the job affirmatively never started (the ruling of 8 Nov 2026, #272). At the
// bound the run reads the scheduler, past the cached listing and the Allocation's status, which a logged reconcile error
// leaves stale. A job that started is a failure, and so is a job the read cannot tell about: missing evidence is
// neither "never started" nor a pass. Every case goes through the real waiter, with the fake Slurm behind it, and each
// must fail, name what it found, write no marker and cancel nothing by a NOT RUN.
func TestAJobThatStartedOrCannotBeReadAtTheBoundIsNeverNotRun(t *testing.T) {
	for _, c := range []struct {
		name string
		// arrange sets the world up before the wait; the acceptedID it returns is the waiter's.
		arrange func(t *testing.T, h *held) string
		want    string
		// atOnce is a phase from which no job starts: the wait ends there, not at the bound.
		atOnce bool
	}{
		{"it started and failed between two polls", func(t *testing.T, h *held) string {
			h.w.poll = h.allocationIn(v1alpha1.AllocationFailed, nil)
			return ""
		}, "the Allocation is Failed", true},
		{"it was refused", func(t *testing.T, h *held) string {
			h.w.poll = h.allocationIn(v1alpha1.AllocationRefused, nil)
			return ""
		}, "the Allocation is Refused", true},
		{"it was lost", func(t *testing.T, h *held) string {
			h.w.poll = h.allocationIn(v1alpha1.AllocationLost, nil)
			return ""
		}, "the Allocation is Lost", true},
		{"the status is a stale Pending and the scheduler says it runs", func(t *testing.T, h *held) string {
			h.startedOnTheFake(t)
			h.w.poll = h.pending("step")
			return ""
		}, "the scheduler shows it Running", false},
		{"the status is a stale Pending and the job ran, ended and has left the queue", func(t *testing.T, h *held) string {
			if err := h.sl.End(h.startedOnTheFake(t), string(fakeslurm.Failed)); err != nil {
				t.Fatal(err)
			}
			h.w.poll = h.pending("step")
			return ""
		}, "the queue does not list it", false},
		{"the status is a stale Pending and the job was requeued, so it is pending again after it ran", func(t *testing.T, h *held) string {
			if err := h.sl.Requeue(h.startedOnTheFake(t)); err != nil {
				t.Fatal(err)
			}
			h.w.poll = h.pending("step")
			return ""
		}, "the scheduler shows it Failed", false},
		{"the queue cannot be read at the bound", func(t *testing.T, h *held) string {
			*h.queueFails = true
			h.w.poll = h.pending("step")
			return ""
		}, "its state could not be read", false},
		{"the scheduler no longer lists the job", func(t *testing.T, h *held) string {
			h.w.poll = h.allocationIn(v1alpha1.AllocationPending, func(a *v1alpha1.Allocation) { a.Annotations[v1alpha1.AnnotationJobID] = "999999" })
			return ""
		}, "the queue does not list it", false},
		{"the Allocation cannot be read", func(t *testing.T, h *held) string {
			h.w.poll = func(string) *v1alpha1.Allocation { return nil }
			return h.ours
		}, "its Allocation could not be read", false},
		{"the status says the job runs, but not the one the step waits for", func(t *testing.T, h *held) string {
			h.w.poll = h.allocationIn(v1alpha1.AllocationRunning, nil)
			return ""
		}, "its Allocation is Running", false},
		{"the status says the job ended", func(t *testing.T, h *held) string {
			h.w.poll = h.allocationIn(v1alpha1.AllocationEnded, nil)
			return ""
		}, "its Allocation is Ended", false},
		{"the status has a start time, whatever its phase", func(t *testing.T, h *held) string {
			now := metav1.NewTime(h.ft.now())
			h.w.poll = h.allocationIn(v1alpha1.AllocationPending, func(a *v1alpha1.Allocation) { a.Status.AtProvider.StartTime = &now })
			return ""
		}, "with a start time", false},
		{"there is no platform to read", func(t *testing.T, h *held) string {
			h.w.batch = nil
			h.w.poll = h.pending("step")
			return ""
		}, "no platform to read it on", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHeld(t)
			accepted := c.arrange(t, h)
			began := h.ft.now()
			// The condition never holds, as for step 3 when the job that runs is not the one the step waits for.
			never := func(*v1alpha1.Allocation) bool { return false }
			ends(func() { h.w.untilRuns("step 1", "step", "the job runs", h.w.at(), accepted, never) })
			if h.rec.skipped != "" {
				t.Errorf("the run ended NOT RUN: %s", h.rec.skipped)
			}
			if !strings.Contains(h.rec.failed, c.want) {
				t.Errorf("failed = %q, want a failure that says %q", h.rec.failed, c.want)
			}
			if _, err := os.Stat(filepath.Join(h.dir, notRunFile)); err == nil {
				t.Error("a marker was written for a job that cannot be called one that never started")
			}
			if c.atOnce {
				if waited := h.ft.since(began); waited >= 10*time.Minute {
					t.Errorf("the wait lasted %s: a job in this phase will not start, so the wait must end there", waited)
				}
			} else if waited := h.ft.since(began); waited < 10*time.Minute {
				t.Errorf("the wait lasted %s, want the bound of 10m: the status may be read again until then", waited)
			}
		})
	}
}

// The other side of the same read: what the scheduler affirmatively shows waiting stays NOT RUN, held or merely
// pending, with a status that says so, and with no status yet for a job accepted before the wait (step 3's direct
// sbatch, not yet adopted, whose ID only the step knows). Without these the test above would pass on a run that
// never reports NOT RUN at all.
func TestAJobTheSchedulerShowsWaitingAtTheBoundIsNotRun(t *testing.T) {
	for _, c := range []struct {
		name    string
		arrange func(t *testing.T, h *held) string
	}{
		{"held", func(t *testing.T, h *held) string {
			h.w.poll = h.allocationIn(v1alpha1.AllocationHeld, nil)
			return ""
		}},
		{"pending", func(t *testing.T, h *held) string {
			h.lifted(t)
			h.w.poll = h.pending("step")
			return ""
		}},
		{"accepted before the wait and not yet adopted", func(t *testing.T, h *held) string {
			h.w.poll = func(name string) *v1alpha1.Allocation {
				return &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: name}}
			}
			return h.ours
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHeld(t)
			accepted := c.arrange(t, h)
			ends(func() { h.w.untilRuns("step 3", "step", "the job runs", h.w.at(), accepted, running) })
			if h.rec.failed != "" || !strings.HasPrefix(h.rec.skipped, "NOT RUN: step 3:") {
				t.Errorf("failed %q, skipped %q; want a NOT RUN", h.rec.failed, h.rec.skipped)
			}
			if _, err := os.Stat(filepath.Join(h.dir, notRunFile)); err != nil {
				t.Errorf("no marker: %v", err)
			}
		})
	}
}

// The two waits for a job to run in TestConformance (step 1, and step 3's
// adopted job) are the ones the bound applies to; a wait written with until
// has the 30-minute deadline only. A wait on the Running phase must therefore
// go through untilRuns, and both of them must.
func TestTheStepsWaitForARunningJobThroughUntilRuns(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "conformance_test.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	viaUntilRuns, settles := 0, 0
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "TestConformance" {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if ok && sel.Sel.Name == "settleDuplicates" {
				settles++
			}
			if !ok || (sel.Sel.Name != "until" && sel.Sel.Name != "untilRuns") {
				return true
			}
			waitsForRunning := false
			for _, arg := range call.Args {
				ast.Inspect(arg, func(m ast.Node) bool {
					if id, ok := m.(*ast.SelectorExpr); ok && id.Sel.Name == "AllocationRunning" {
						waitsForRunning = true
					}
					return true
				})
			}
			switch {
			case sel.Sel.Name == "untilRuns":
				viaUntilRuns++
			case waitsForRunning:
				t.Error("TestConformance waits for the Running phase with until, which the start-wait bound does not apply to; use untilRuns")
			}
			return true
		})
	}
	if viaUntilRuns != 2 {
		t.Errorf("TestConformance calls untilRuns %d times, want 2 (step 1 and step 3)", viaUntilRuns)
	}
	if settles != 1 {
		t.Errorf("TestConformance calls settleDuplicates %d times, want once, after step 5", settles)
	}
}
