package providerctl

// The read-back runs on the first successful observe of the recorded job, keyed
// on its own marker (review of PR 76: Opus R1, Astra B6 first part), and
// compares accounting's GPU count when accounting reports one (Astra B6 second
// part). Over the real Batch and the fake Slurm, which scripts a job
// established with another time limit (fakeslurm.AsTimeLimit).

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const wantLimitDiff = "timeLimit: requested 30m0s, got 10m0s"

func submittedAsOther(a *v1alpha1.Allocation) (corev1.ConditionStatus, string) {
	c := a.GetCondition(v1alpha1.ConditionSubmittedAsOther)
	return c.Status, c.Message
}

// Opus R1, transport error: the first observe after the Create fails, and
// crossplane still writes the status, which then names the job. The read-back
// must run on the next successful observe. The control has no failure.
func TestReadBackRunsAfterAFailedFirstObserve(t *testing.T) {
	for _, inject := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "first observe fails"}[inject], func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
			sl.Script(fakeslurm.AsTimeLimit(10))
			e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 1) // Observe (missing), Create, sbatch, record, settle
			if e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID] == "" {
				t.Fatal("precondition: no job recorded after the first reconcile")
			}
			if inject {
				sl.FailNext(1, slurm.ErrTransport)
				e.run("alloc-a", 1)
				if got := e.get("alloc-a").Status.AtProvider.JobID; got == "" {
					t.Fatal("precondition: the failed observe did not persist the job ID, so it cannot hide the read-back")
				}
			}
			clock.Advance(40 * time.Second)
			e.run("alloc-a", 3)
			if st, msg := submittedAsOther(e.get("alloc-a")); st != corev1.ConditionTrue || !strings.Contains(msg, wantLimitDiff) {
				t.Errorf("SubmittedAsOther %s %q, want True naming %q", st, msg, wantLimitDiff)
			}
		})
	}
}

// Opus R1, empty first observe: the job ended at once, the listing dropped it
// (Helios) and accounting did not have it yet, so the first observe found
// nothing (and persisted the job ID with absentSince). Once accounting shows
// it, the read-back runs on that accounting record.
func TestReadBackRunsAfterAnEmptyFirstObserve(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: fakeslurm.NotListedHelios})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	sl.Script(fakeslurm.AsTimeLimit(10))
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 1)
	id, err := strconv.Atoi(e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID])
	if err != nil {
		t.Fatalf("precondition: no job recorded: %v", err)
	}
	if err := sl.End(id, fakeslurm.Failed); err != nil {
		t.Fatal(err)
	}
	if err := sl.AcctHide(id); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Second)
	e.run("alloc-a", 1)
	if o := e.get("alloc-a").Status.AtProvider; o.AbsentSince == nil {
		t.Fatalf("precondition: the first observe found the job (phase %s); want it absent from both reads", o.Phase)
	}
	if err := sl.AcctShow(id); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Second)
	e.run("alloc-a", 2)
	if st, msg := submittedAsOther(e.get("alloc-a")); st != corev1.ConditionTrue || !strings.Contains(msg, wantLimitDiff) {
		t.Errorf("SubmittedAsOther %s %q, want True naming %q", st, msg, wantLimitDiff)
	}
}

// The marker is written once the job matched, and a later change is never
// read back: an administrator's extension after the first observe raises
// nothing (HBEP-4, Read-back).
func TestReadBackMarkerStopsLaterComparisons(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	a := e.get("alloc-a")
	id := a.Annotations[v1alpha1.AnnotationJobID]
	if got := a.Annotations[v1alpha1.AnnotationReadBack]; id == "" || got != id {
		t.Fatalf("read-back marker %q, want the job ID %q after a matching first observe", got, id)
	}
	n, _ := strconv.Atoi(id)
	if err := sl.SetTimeLimit(n, 90); err != nil {
		t.Fatal(err)
	}
	clock.Advance(40 * time.Second)
	e.run("alloc-a", 2)
	if st, msg := submittedAsOther(e.get("alloc-a")); st == corev1.ConditionTrue {
		t.Errorf("SubmittedAsOther True %q after an extension made after the read-back", msg)
	}
}

// Astra B6, second part: accounting's GPU count is compared too. The record is
// the real Athena reply (1 GPU, testdata/v0.0.40-athena-real), read through the
// real Batch, against a request for 2 GPUs.
func TestReadBackComparesAccountingGPUs(t *testing.T) {
	sacct, err := os.ReadFile("../slurm/testdata/v0.0.40-athena-real/sacct-completed.json")
	if err != nil {
		t.Fatal(err)
	}
	squeue, err := os.ReadFile("../slurm/testdata/v0.0.40/squeue-empty.json")
	if err != nil {
		t.Fatal(err)
	}
	r := fixtureRunner{"squeue --me --json": squeue, "sacct --json -X -D -j 4501": sacct}
	profile := provider.Profile{Spec: v1alpha1.PlatformProfileSpec{JSONDataParser: "v0.0.40"}}
	b := slurm.NewBatch(r, profile, provider.Settings{}, slurm.Options{})
	job, found, err := b.Observe(context.Background(), "4501")
	if err != nil || !found || job.Source != provider.SourceAccounting {
		t.Fatalf("precondition: Observe = %+v, %v, %v; want the accounting record", job, found, err)
	}
	req := &v1alpha1.BatchParameters{Partition: "plgrid-gpu-a100", TimeLimit: "5m", GPUs: 2}
	diffs := readBackDiffs(req, job)
	if len(diffs) != 1 || diffs[0] != "gpus: requested 2, got 1" {
		t.Errorf("diffs %q, want only the GPU count, requested 2, got 1", diffs)
	}
	req.GPUs = 1
	if diffs := readBackDiffs(req, job); len(diffs) != 0 {
		t.Errorf("diffs %q for the request the job was made with, want none", diffs)
	}
}

// An accounting record without a gres entry reads as 0 GPUs: not reported, so
// not compared. A queue record always carries the count, so 0 there differs.
func TestReadBackDiffsAccountingGPUsOnlyWhenReported(t *testing.T) {
	req := &v1alpha1.BatchParameters{Partition: "p", TimeLimit: "30m", GPUs: 2}
	for _, tc := range []struct {
		source provider.Source
		gpus   int32
		differ bool
	}{
		{provider.SourceAccounting, 0, false},
		{provider.SourceAccounting, 1, true},
		{provider.SourceAccounting, 2, false},
		{provider.SourceQueue, 0, true},
		{provider.SourceQueue, 2, false},
	} {
		job := provider.Job{Source: tc.source, GPUs: tc.gpus, Partition: "p", TimeLimit: 30 * time.Minute}
		if got := len(readBackDiffs(req, job)) > 0; got != tc.differ {
			t.Errorf("%s with %d GPUs against 2 requested: differs %v, want %v", tc.source, tc.gpus, got, tc.differ)
		}
	}
}

// fixtureRunner answers fixed command lines with fixed replies.
type fixtureRunner map[string][]byte

func (f fixtureRunner) Run(_ context.Context, c slurm.Command) (slurm.Result, error) {
	out, ok := f[c.Line]
	if !ok {
		return slurm.Result{ExitCode: 1, Stderr: []byte("unexpected command")}, nil
	}
	return slurm.Result{Stdout: out}, nil
}

func (fixtureRunner) Generation() uint64 { return 1 }
