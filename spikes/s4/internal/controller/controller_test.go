package controller

import (
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/internal/slurm"
)

// Submission records as they sit in the annotation, written by hand.
const (
	inFlight  = `{"n":2,"token":"tok2","state":"InFlight","by":"reconciler-a","at":"2026-10-30T09:00:00Z"}`
	unknownN2 = `{"n":2,"token":"tok2","state":"Unknown","by":"reconciler-a","at":"2026-10-30T09:00:00Z","reason":"timeout"}`
	doneN2    = `{"n":2,"token":"tok2","state":"Done","by":"reconciler-a","at":"2026-10-30T09:00:00Z","jobId":1001}`
	rejected1 = `{"n":1,"token":"tok1","state":"Rejected","by":"reconciler-a","at":"2026-10-30T08:00:00Z","reason":"Invalid account"}`
	abandoned = `{"n":2,"token":"tok2","state":"Abandoned","by":"reconciler-a","at":"2026-10-30T09:00:00Z"}`
)

// allocation returns an Allocation with the given annotations. Namespace
// "ns", name "alloc" and UID "uid-1234" make its tag "hb-alloc-a7656bce1293"
// (the first 12 hex digits of sha256("ns/alloc/uid-1234"), computed with
// sha256sum, not by Tag).
func allocation(an map[string]string) *v1alpha1.Allocation {
	return &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ns", Name: "alloc", UID: "uid-1234", Annotations: an,
	}}
}

const wantTag = "hb-alloc-a7656bce1293"

func TestReadRecord(t *testing.T) {
	t.Run("no annotations is the zero record", func(t *testing.T) {
		for _, an := range []map[string]string{nil, {}, {"unrelated": "x"}} {
			rec, err := readRecord(allocation(an))
			if err != nil || rec != (record{}) {
				t.Errorf("annotations %v: got %+v, %v; want the zero record", an, rec, err)
			}
		}
	})
	t.Run("valid record", func(t *testing.T) {
		rec, err := readRecord(allocation(map[string]string{
			AnnoSubmission: inFlight, AnnoJobID: "1001", AnnoReleased: "1001",
		}))
		if err != nil {
			t.Fatal(err)
		}
		if rec.attempt == nil {
			t.Fatal("no attempt parsed")
		}
		a := rec.attempt
		if a.N != 2 || a.Token != "tok2" || a.State != "InFlight" || a.By != "reconciler-a" ||
			!a.At.Equal(time.Date(2026, 10, 30, 9, 0, 0, 0, time.UTC)) {
			t.Errorf("attempt %+v", *a)
		}
		if rec.jobID != 1001 || rec.released != 1001 {
			t.Errorf("jobID %d released %d, want 1001 and 1001", rec.jobID, rec.released)
		}
		if got := rec.nextN(); got != 3 {
			t.Errorf("nextN %d after attempt 2, want 3", got)
		}
	})
	t.Run("job id and released only", func(t *testing.T) {
		rec, err := readRecord(allocation(map[string]string{AnnoJobID: "7", AnnoReleased: "7"}))
		if err != nil || rec.attempt != nil || rec.jobID != 7 || rec.released != 7 || rec.nextN() != 1 {
			t.Errorf("got %+v, %v", rec, err)
		}
	})
	bad := []struct{ name, key, value string }{
		{"malformed submission", AnnoSubmission, `{"n":2,"token":`},
		{"submission is not an object", AnnoSubmission, `[1,2]`},
		{"job-id not a number", AnnoJobID, "abc"},
		{"job-id zero", AnnoJobID, "0"},
		{"job-id negative", AnnoJobID, "-5"},
		{"released not a number", AnnoReleased, "abc"},
		{"released zero", AnnoReleased, "0"},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			if rec, err := readRecord(allocation(map[string]string{c.key: c.value})); err == nil {
				t.Errorf("%s=%q accepted as %+v; a malformed record must be an error, never \"no record\"", c.key, c.value, rec)
			}
		})
	}
}

// slurmJob builds a job as squeue or sacct would report it.
func slurmJob(id int64, source string, held bool, started bool, states ...string) slurm.Job {
	j := slurm.Job{ID: id, Source: source, State: states, Held: held}
	if started {
		j.Start = time.Unix(1761800100, 0).UTC()
	}
	return j
}

func TestChoose(t *testing.T) {
	cases := []struct {
		name string
		jobs []slurm.Job
		want int64
	}{
		{"two held jobs: the lowest ID",
			[]slurm.Job{slurmJob(1001, "squeue", true, false, "PENDING"), slurmJob(1002, "squeue", true, false, "PENDING")}, 1001},
		// Only an established job is ever released, so a pending job that is
		// not held proves an earlier establishment whose record was lost.
		{"held 1001, released 1002: the released one wins",
			[]slurm.Job{slurmJob(1001, "squeue", true, false, "PENDING"), slurmJob(1002, "squeue", false, false, "PENDING")}, 1002},
		{"cancelled before it started 1001, running 1002",
			[]slurm.Job{slurmJob(1001, "sacct", false, false, "CANCELLED"), slurmJob(1002, "squeue", false, true, "RUNNING")}, 1002},
		{"cancelled before it started 1001, completed 1002",
			[]slurm.Job{slurmJob(1001, "sacct", false, false, "CANCELLED"), slurmJob(1002, "sacct", false, true, "COMPLETED")}, 1002},
		{"a single held job", []slurm.Job{slurmJob(1003, "squeue", true, false, "PENDING")}, 1003},
		{"running 1001 and a held extra 1002",
			[]slurm.Job{slurmJob(1001, "squeue", false, true, "RUNNING"), slurmJob(1002, "squeue", true, false, "PENDING")}, 1001},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := choose(c.jobs); got.ID != c.want {
				t.Errorf("chose job %d, want %d", got.ID, c.want)
			}
		})
	}
}

func TestMergeJobsKeepsFirstRecordAndSortsByID(t *testing.T) {
	first := []slurm.Job{slurmJob(1004, "squeue", false, true, "RUNNING"), slurmJob(1002, "squeue", false, true, "RUNNING")}
	second := []slurm.Job{
		slurmJob(1003, "sacct", false, true, "COMPLETED"),
		slurmJob(1002, "sacct", false, true, "COMPLETED"), // also in first: squeue's record wins
		slurmJob(1001, "sacct", false, true, "COMPLETED"),
	}
	got := mergeJobs(first, second)
	wantIDs := []int64{1001, 1002, 1003, 1004}
	wantSrc := []string{"sacct", "squeue", "sacct", "squeue"}
	if len(got) != 4 {
		t.Fatalf("got %d jobs, want 4: %+v", len(got), got)
	}
	for i, j := range got {
		if j.ID != wantIDs[i] || j.Source != wantSrc[i] {
			t.Errorf("position %d: job %d from %s, want job %d from %s", i, j.ID, j.Source, wantIDs[i], wantSrc[i])
		}
	}
	if got[1].HasState("COMPLETED") || !got[1].HasState("RUNNING") {
		t.Errorf("job 1002 has state %v, want the first list's [RUNNING]", got[1].State)
	}
	if first[0].ID != 1004 || first[1].ID != 1002 || len(first) != 2 {
		t.Errorf("mergeJobs reordered or grew its first argument: %+v", first)
	}
}

func TestUnion(t *testing.T) {
	cases := []struct {
		name string
		a, b []int64
		want []int64
	}{
		{"unsorted with an overlap", []int64{1003, 1001}, []int64{1002, 1003}, []int64{1001, 1002, 1003}},
		{"duplicates in the second list", nil, []int64{5, 5, 3}, []int64{3, 5}},
		{"identical lists", []int64{1, 2}, []int64{2, 1}, []int64{1, 2}},
		{"empty", nil, nil, nil},
		// The result is sorted and deduplicated, whichever list repeats a value.
		{"duplicates in the first list", []int64{1, 1}, nil, []int64{1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := slices.Clone(c.a)
			if got := union(c.a, c.b); !slices.Equal(got, c.want) {
				t.Errorf("union(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
			}
			if !slices.Equal(a, c.a) {
				t.Errorf("union changed its first argument: %v -> %v", a, c.a)
			}
		})
	}
}

func TestTagIsHashOfNamespaceNameUID(t *testing.T) {
	if got := Tag(allocation(nil)); got != wantTag {
		t.Errorf("Tag = %q, want %q", got, wantTag)
	}
	other := allocation(nil)
	other.SetUID("uid-5678")
	if Tag(other) == wantTag {
		t.Error("a different UID gave the same tag; a recreated Allocation would adopt its predecessor's job")
	}
}

func TestCanonical(t *testing.T) {
	named := func(name string) *v1alpha1.Allocation {
		return allocation(map[string]string{"crossplane.io/external-name": name})
	}
	if err := canonical(named(wantTag)); err != nil {
		t.Errorf("the canonical tag was refused: %v", err)
	}
	if err := canonical(allocation(nil)); err == nil {
		t.Error("an empty external name was accepted")
	}
	if err := canonical(named("")); err == nil {
		t.Error("an empty external-name annotation was accepted")
	}
	err := canonical(named("hb-hand-edited-000000000000"))
	if err == nil {
		t.Fatal("a different external name was accepted")
	}
	for _, want := range []string{"hb-hand-edited-000000000000", wantTag} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
}

func TestPhase(t *testing.T) {
	t.Run("held pending job", func(t *testing.T) {
		j := slurmJob(1001, "squeue", true, false, "PENDING")
		j.Reason = "JobHeldUser"
		p, reason := phase(j, false)
		if p != v1alpha1.PhasePending || !strings.Contains(strings.ToLower(reason), "held") {
			t.Errorf("phase %q reason %q, want Pending and a reason mentioning held", p, reason)
		}
	})
	t.Run("requeued", func(t *testing.T) {
		for _, state := range []string{"PENDING", "RUNNING", "COMPLETED"} {
			j := slurmJob(1001, "squeue", false, false, state)
			j.RestartCount, j.Records = 1, 2
			p, reason := phase(j, true)
			if p != v1alpha1.PhaseFailed || !strings.HasPrefix(reason, "Requeued") {
				t.Errorf("state %s: phase %q reason %q, want Failed and a reason starting \"Requeued\"", state, p, reason)
			}
		}
	})
	states := []struct{ state, phase string }{
		{"PENDING", v1alpha1.PhasePending},
		{"RUNNING", v1alpha1.PhaseRunning},
		{"COMPLETED", v1alpha1.PhaseSucceeded},
		{"FAILED", v1alpha1.PhaseFailed},
		{"CANCELLED", v1alpha1.PhaseFailed},
		{"TIMEOUT", v1alpha1.PhaseFailed},
		{"NODE_FAIL", v1alpha1.PhaseFailed},
	}
	for _, c := range states {
		t.Run(c.state, func(t *testing.T) {
			if p, _ := phase(slurmJob(1001, "squeue", false, true, c.state), false); p != c.phase {
				t.Errorf("state %s: phase %q, want %q", c.state, p, c.phase)
			}
		})
	}
	t.Run("pending job that is not held has no reason", func(t *testing.T) {
		if p, reason := phase(slurmJob(1001, "squeue", false, false, "PENDING"), false); p != v1alpha1.PhasePending || reason != "" {
			t.Errorf("phase %q reason %q, want Pending and no reason", p, reason)
		}
	})
}

func TestPollIntervalHook(t *testing.T) {
	const poll = time.Minute
	hook := PollIntervalHook(Options{UnresolvedPoll: time.Second})
	deleted := func(a *v1alpha1.Allocation) *v1alpha1.Allocation {
		now := metav1.Now()
		a.SetDeletionTimestamp(&now)
		return a
	}
	sub := func(s string) map[string]string { return map[string]string{AnnoSubmission: s} }
	cases := []struct {
		name string
		a    *v1alpha1.Allocation
		poll time.Duration
		want time.Duration
	}{
		{"attempt in flight", allocation(sub(inFlight)), poll, time.Second},
		{"attempt unknown", allocation(sub(unknownN2)), poll, time.Second},
		{"attempt done", allocation(sub(doneN2)), poll, poll},
		{"attempt rejected", allocation(sub(rejected1)), poll, poll},
		{"attempt abandoned", allocation(sub(abandoned)), poll, poll},
		{"no attempt", allocation(nil), poll, poll},
		{"being deleted, attempt done", deleted(allocation(sub(doneN2))), poll, time.Second},
		{"being deleted, no attempt", deleted(allocation(nil)), poll, time.Second},
		{"being deleted, attempt in flight", deleted(allocation(sub(inFlight))), poll, time.Second},
		// The hook shortens the interval; it never lengthens a shorter one.
		{"in flight, poll already shorter", allocation(sub(inFlight)), 500 * time.Millisecond, 500 * time.Millisecond},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := hook(c.a, c.poll); got != c.want {
				t.Errorf("interval %v, want %v", got, c.want)
			}
		})
	}
}
