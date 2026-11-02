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
}

type cancelFault struct {
	slurm.Runner
	fail *bool
}

func (c cancelFault) Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error) {
	if *c.fail && strings.HasPrefix(cmd.Line, "scancel") {
		return slurm.Result{}, fmt.Errorf("scancel: connection reset")
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
	fail := new(bool)
	b := slurm.NewBatch(cancelFault{session, fail}, provider.Profile{Name: pp.Name, Spec: pp.Spec, Partition: part},
		provider.Settings{Provider: "example", Account: "example-account", Partition: "plgrid"}, slurm.Options{Clock: ft.c})
	dir := t.TempDir()
	rec := &stopRecorder{TB: t}
	w := &world{t: rec, p: newPrivate(t, dir), batch: b, jobIDs: map[string]bool{}, now: ft.now, sleep: ft.sleep}
	w.s.account, w.s.startWait = "example-account", 10*time.Minute
	digest := sha256.Sum256([]byte("example agent"))
	params := v1alpha1.BatchParameters{Partition: "plgrid", TimeLimit: "5m", CPUsPerTask: 1, Memory: resource.MustParse("1Gi"), Nodes: 1, AgentDigest: hex.EncodeToString(digest[:])}
	h := &held{w: w, rec: rec, sl: sl, ft: ft, dir: dir, cancelFails: fail}
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
	ends(func() { h.w.untilRuns("step 1", "step", "the job runs", h.w.at(), false, running) })

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
		ends(func() { got = h.w.untilRuns("step 1", "step", "the job runs", h.w.at(), false, running) })
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
		ends(func() { h.w.untilRuns("step 1", "step", "the job runs", h.w.at(), false, running) })
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
	ends(func() { h.w.untilRuns("step 3", "step", "the job runs", h.w.at(), true, running) })
	if !strings.Contains(h.rec.failed, "cancelling this run's jobs by tag failed") || h.rec.skipped != "" {
		t.Errorf("failed %q, skipped %q; want a failure naming the cancel", h.rec.failed, h.rec.skipped)
	}
	if _, err := os.Stat(filepath.Join(h.dir, notRunFile)); err == nil {
		t.Error("a marker was written although a job may be live")
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
