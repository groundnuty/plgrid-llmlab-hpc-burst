package slurm

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Athena's real v0.0.40 replies (testdata/v0.0.40-athena-real/README.md).
// The expected values come from the request the conformance run made (1 GPU,
// a 5 min limit, plgrid-gpu-a100, held) and from HBEP-4's dialect rules, not
// from the parser: the epoch times are the reply's own numbers, written out.

func realReply(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "v0.0.40-athena-real", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func realQueueJob(t *testing.T, name string, id int64) provider.Job {
	t.Helper()
	recs, v, err := parseQueue(realReply(t, name), "v0.0.40")
	if err != nil || v != "v0.0.40" {
		t.Fatalf("%s: version %q, %v", name, v, err)
	}
	for _, r := range recs {
		if r.JobID == id {
			j, err := r.job(v)
			if err != nil {
				t.Fatal(err)
			}
			return j
		}
	}
	t.Fatalf("%s: no job %d", name, id)
	return provider.Job{}
}

func TestRealAthenaHeldJob(t *testing.T) {
	j := realQueueJob(t, "squeue-held.json", 4501)
	if j.Phase != v1alpha1.AllocationHeld || j.Hold != provider.HoldUser || j.Reason != "JobHeldUser" {
		t.Errorf("phase %s hold %q reason %q, want Held by its user (priority 0, JobHeldUser)", j.Phase, j.Hold, j.Reason)
	}
	if j.Partition != "plgrid-gpu-a100" || j.TimeLimit != 5*time.Minute || j.GPUs != 1 {
		t.Errorf("read-back fields %s %s %d, want plgrid-gpu-a100, 5m, 1 GPU", j.Partition, j.TimeLimit, j.GPUs)
	}
	if j.StartTime != nil || j.StartEstimate != nil {
		t.Errorf("a held job has no start and no estimate: %v %v", j.StartTime, j.StartEstimate)
	}
}

func TestRealAthenaRunningJob(t *testing.T) {
	j := realQueueJob(t, "squeue-running.json", 4501)
	if j.Phase != v1alpha1.AllocationRunning || j.Held() {
		t.Fatalf("phase %s held %v, want Running", j.Phase, j.Held())
	}
	if j.StartTime == nil || j.HardEnd == nil || j.HardEnd.Sub(*j.StartTime) != 5*time.Minute {
		t.Errorf("start %v hard end %v: want the projected end, start plus the 5 min limit", j.StartTime, j.HardEnd)
	}
	if j.StartTime.Location() != time.UTC {
		t.Error("the start is not UTC")
	}
}

func TestRealAthenaEndedJobStillListed(t *testing.T) {
	j := realQueueJob(t, "squeue-completed-listed.json", 4501)
	if j.Phase != v1alpha1.AllocationEnded || j.EndTime == nil || j.Source != provider.SourceQueue {
		t.Errorf("phase %s end %v source %s, want Ended from the listing with an end time", j.Phase, j.EndTime, j.Source)
	}
	other := realQueueJob(t, "squeue-completed-listed.json", 4502)
	if other.Phase != v1alpha1.AllocationPending {
		t.Errorf("the second job: phase %s, want Pending", other.Phase)
	}
}

func TestRealAthenaAccounting(t *testing.T) {
	for _, name := range []string{"sacct-completed.json", "sacct-completed-after-hold.json"} {
		recs, v, err := parseAccounting(realReply(t, name), "v0.0.40")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		jobs, err := mergeAccounting(recs, v)
		if err != nil || len(jobs) != 1 {
			t.Fatalf("%s: %d jobs, %v", name, len(jobs), err)
		}
		j := jobs[0]
		if j.Phase != v1alpha1.AllocationEnded || j.Held() || j.Requeued || j.RestartCount != 0 {
			t.Errorf("%s: phase %s held %v requeued %v restarts %d, want Ended, not held, not requeued", name, j.Phase, j.Held(), j.Requeued, j.RestartCount)
		}
		if j.EndTime == nil || j.StartTime == nil || j.SubmitTime.IsZero() || j.TimeLimit != 5*time.Minute || j.GPUs != 1 {
			t.Errorf("%s: times %v %v %v limit %s GPUs %d", name, j.SubmitTime, j.StartTime, j.EndTime, j.TimeLimit, j.GPUs)
		}
	}
	// The second job's numbers, as Slurm gave them: submitted 1793452614,
	// started 1793452734, ended 1793452825.
	recs, v, _ := parseAccounting(realReply(t, "sacct-completed-after-hold.json"), "v0.0.40")
	jobs, _ := mergeAccounting(recs, v)
	j := jobs[0]
	if !j.SubmitTime.Equal(time.Unix(1793452614, 0)) || !j.StartTime.Equal(time.Unix(1793452734, 0)) || !j.EndTime.Equal(time.Unix(1793452825, 0)) {
		t.Errorf("times %v %v %v", j.SubmitTime, j.StartTime, j.EndTime)
	}
	// A v0.0.42 profile refuses these replies: the dialect check.
	if _, _, err := parseAccounting(realReply(t, "sacct-completed.json"), "v0.0.42"); err == nil {
		t.Error("a v0.0.40 reply was read under a v0.0.42 profile")
	}
}

// REQUEUED in any position of the state, the base included, is a requeue
// (pkg/provider.SlurmPhase, as spike S4 accepts it): Failed, Requeued, and
// the job still counts as live for cancelling.
func TestRequeuedBaseState(t *testing.T) {
	for _, state := range [][]string{{"REQUEUED"}, {"PENDING", "REQUEUED"}} {
		q := queueRecord{JobID: 7, Name: "hb-x-000000000000", JobState: state}
		j, err := q.job("v0.0.42")
		if err != nil {
			t.Fatalf("%v: %v", state, err)
		}
		if j.Phase != v1alpha1.AllocationFailed || !j.Requeued || j.EndTime != nil {
			t.Errorf("%v: phase %s requeued %v end %v, want Failed, requeued, live", state, j.Phase, j.Requeued, j.EndTime)
		}
	}
}
