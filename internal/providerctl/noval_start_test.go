package providerctl

// A job cancelled before it started, as Slurm 24.11's sacct reports it: its
// time.start is the bare NO_VAL, 4294967294 (issue #267). It used to read as a
// start time in 2106, and everything downstream that takes a start time for
// "the job started" took the job for one that ran: the accounting read made for
// a started job (allocatedRead), the adoption ranking (choose), and the record
// kept after the Allocation (keepAccounting).
//
// The job in these tests comes through the real parser, from the fixture of the
// real reply's shape (internal/slurm/testdata/v0.0.42/sacct-never-started.json),
// never from a provider.Job built by hand: a Job with a nil StartTime passes
// with or without the fix, and a fixture whose time.start is 0 or absent would
// prove nothing either. The fake Slurm writes 0 for a start that never was, so
// the lifecycle test shows its cancelled jobs the way Slurm 24.11 does.

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// recordingRunner is a fixtureRunner that keeps the command lines it was asked.
type recordingRunner struct {
	fixtureRunner

	mu    sync.Mutex
	lines []string
}

func (r *recordingRunner) Run(ctx context.Context, c slurm.Command) (slurm.Result, error) {
	r.mu.Lock()
	r.lines = append(r.lines, c.Line)
	r.mu.Unlock()
	return r.fixtureRunner.Run(ctx, c)
}

// count is how many commands started with prefix.
func (r *recordingRunner) count(prefix string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, l := range r.lines {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

// neverStartedJob is job 4311 of the never-started fixture, read through the
// real Batch as the controller reads a recorded job that the queue listing no
// longer shows: from accounting.
func neverStartedJob(t *testing.T) (*slurm.Batch, *recordingRunner, provider.Job) {
	t.Helper()
	sacct, err := os.ReadFile("../slurm/testdata/v0.0.42/sacct-never-started.json")
	if err != nil {
		t.Fatal(err)
	}
	squeue, err := os.ReadFile("../slurm/testdata/v0.0.42/squeue-empty.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sacct), `"start": 4294967294`) {
		t.Fatal("precondition: the fixture's time.start is not the bare NO_VAL 4294967294, so the tests below prove nothing")
	}
	r := &recordingRunner{fixtureRunner: fixtureRunner{"squeue --me --json": squeue, "sacct --json -X -D -j 4311": sacct}}
	profile := provider.Profile{Spec: v1alpha1.PlatformProfileSpec{JSONDataParser: "v0.0.42"}}
	b := slurm.NewBatch(r, profile, provider.Settings{}, slurm.Options{})
	job, found, err := b.Observe(context.Background(), "4311")
	if err != nil || !found || job.Source != provider.SourceAccounting || job.EndTime == nil {
		t.Fatalf("precondition: Observe = %+v, %v, %v; want the ended job from accounting", job, found, err)
	}
	return b, r, job
}

// allocatedRead is the accounting read made once a job is first seen started,
// for the CPUs and GPUs it was allocated; it is not made for a job that has not
// started, nor for one that accounting already gave. A job that never started
// has no allocated hours: its status block stays unset (they are unknown,
// never 0), and with a start in 2106 it was set, with an elapsed time of 0 s.
//
// Two shapes of the same job. As accounting gives it, it carries its Accounting
// already, so allocatedRead returns early whatever its start is: only the
// StartTime and the status block it leads to are checked there. As a job
// without Accounting, the shape of one the queue listing shows, the start time
// is the only thing between it and a read; the read is the call that a job
// taken for started would make.
func TestAllocatedReadGivesNoHoursForAJobWhoseStartIsNoVal(t *testing.T) {
	for _, tc := range []struct {
		name         string
		noAccounting bool
	}{
		{"as accounting gives the job", false},
		{"without Accounting, as a listing gives it", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, r, job := neverStartedJob(t)
			if tc.noAccounting {
				job.Accounting = nil
			}
			e := &external{s: &shared{}, p: b}
			own := r.count("sacct")
			prior := &v1alpha1.AllocationObservation{}
			got, read, err := e.allocatedRead(context.Background(), job, prior, at(12, 0, 0))
			if err != nil {
				t.Fatal(err)
			}
			if read || r.count("sacct") != own {
				t.Errorf("allocatedRead read accounting (read %v, %d sacct calls, %d from the observe); a job that never started has nothing allocated to read", read, r.count("sacct"), own)
			}
			if got.StartTime != nil {
				t.Errorf("StartTime = %s, want none: the job never started", got.StartTime)
			}
			if a := provider.MirrorAccounting(got, 1, prior, at(12, 0, 0), read); a != nil {
				t.Errorf("the status accounting block = %+v, want none: no allocated hours", a)
			}
		})
	}
}

// choose ranks the jobs of a tag when the Allocation has none recorded (HBEP-7,
// Adoption): a live job, then a job that ran. A cancelled job that never
// started did not run, so a job that did, with a higher ID, comes first. With a
// start in 2106 the lower ID won.
func TestChooseDoesNotTakeANoValStartForAJobThatRan(t *testing.T) {
	_, _, never := neverStartedJob(t) // 4311
	start, end := at(11, 0, 0), at(11, 5, 0)
	ran := provider.Job{ID: "4312", State: []string{"COMPLETED"}, Phase: v1alpha1.AllocationEnded, StartTime: &start, EndTime: &end}
	if got := choose([]provider.Job{never, ran}); got.ID != "4312" {
		t.Errorf("choose = job %s, want 4312, the job that ran, over 4311, which was cancelled before it started", got.ID)
	}
}

// noValRunner shows an ended job that never started the way Slurm 24.11's sacct
// does, with the bare NO_VAL for time.start where the fake Slurm writes 0. It
// touches only a CANCELLED record whose start is 0, the case of the real reply.
type noValRunner struct{ inner slurm.Runner }

func (n noValRunner) Generation() uint64 { return n.inner.Generation() }

func (n noValRunner) Run(ctx context.Context, c slurm.Command) (slurm.Result, error) {
	res, err := n.inner.Run(ctx, c)
	if err != nil || !strings.HasPrefix(c.Line, "sacct ") {
		return res, err
	}
	var top map[string]any
	if json.Unmarshal(res.Stdout, &top) != nil {
		return res, err
	}
	changed := false
	jobs, _ := top["jobs"].([]any)
	for _, j := range jobs {
		rec, _ := j.(map[string]any)
		state, _ := rec["state"].(map[string]any)
		cur, _ := state["current"].([]any)
		tm, _ := rec["time"].(map[string]any)
		if len(cur) > 0 && cur[0] == "CANCELLED" && tm["start"] == float64(0) {
			tm["start"] = json.Number("4294967294")
			changed = true
		}
	}
	if !changed {
		return res, err
	}
	out, mErr := json.Marshal(top)
	if mErr != nil {
		return res, mErr
	}
	res.Stdout = out
	return res, err
}

// withNoValStarts puts a noValRunner between the Batch and the fake Slurm.
func withNoValStarts() realOption {
	return func(e *realEnv) {
		e.opts.NewPlatform = func(v slurm.Validated) (Platform, io.Closer, error) {
			e.batch = slurm.NewBatch(noValRunner{inner: e.session}, v.Profile, v.Settings, slurm.Options{Clock: e.clock})
			return e.batch, io.NopCloser(nil), nil
		}
	}
}

// The Allocation's whole life with a job that is cancelled while queued, over
// the real Batch: no accounting in the status while it waits, and after the
// delete a record that names the start time unknown, with no allocated hours.
// The control is the fake's own 0, which every version of the parser reads as
// no start; the NO_VAL row is the one that fails without the fix.
func TestAJobCancelledBeforeItStartedLeavesAnUnknownStartInItsRecord(t *testing.T) {
	for _, tc := range []struct {
		name string
		mods []realOption
	}{
		{"control: start 0, as the fake writes it", nil},
		{"start NO_VAL, as Slurm 24.11 writes it", []realOption{withNoValStarts()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", tc.mods...)
			e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 2)
			id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
			clock.Advance(30 * time.Second)
			e.run("alloc-a", 1)
			if a := e.accStatus("alloc-a"); a != nil {
				t.Fatalf("a queued job has accounting %+v, want none", a)
			}
			head := head(e, "alloc-a", "1001")
			e.del("alloc-a")
			e.run("alloc-a", 1) // cancelled at 12:00:30
			if j := slurmJob(t, sl, id); j.State != fakeslurm.Cancelled || !j.Start.IsZero() {
				t.Fatalf("precondition: job %s is %s, started %v; want CANCELLED and never started", id, j.State, j.Start)
			}
			clock.Advance(10 * time.Second)
			e.run("alloc-a", 1)
			if !e.gone("alloc-a") {
				t.Fatalf("the Allocation is still there; its job is %s", slurmJob(t, sl, id).State)
			}
			e.wantRecord("alloc-a", head+`,"endTime":"2026-10-30T12:00:30Z","basis":"allocated",`+
				`"unknown":["allocatedCPUs","allocatedGPUs","elapsedSeconds","startTime"]}`)
		})
	}
}
