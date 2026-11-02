package fakeslurm

// New. #198: Platform.HoldCompleting, the control of the tests that run no
// processes. A held job is COMPLETING as run mode's own flag is: squeue lists
// its end state with the flag in both NotListed modes, the purge skips it however
// old its end, sacct keeps the bare end state, and once it is released the
// mode's rules apply, MinJobAge counted from the end.

import (
	"strings"
	"testing"
	"time"
)

func TestHoldCompletingKeepsAnEndedJobListedUnpurgedAndOutOfAccounting(t *testing.T) {
	for _, mode := range []NotListedMode{NotListedHelios, NotListedAthena} {
		t.Run(string(mode), func(t *testing.T) {
			r := newRig(t, Config{NotListed: mode, MinJobAge: 10 * time.Second})
			id := r.sbatch()
			r.start(id)
			r.end(id, Cancelled)
			if err := r.p.HoldCompleting(id, true); err != nil {
				t.Fatal(err)
			}
			r.clock.Advance(time.Hour) // far past end + MinJobAge
			if got := r.squeueJob(id)["job_state"]; !equalStrings(got, Cancelled, Completing) {
				t.Errorf("held job_state %v an hour after the end, want [CANCELLED COMPLETING]", got)
			}
			if j, _ := r.p.Job(id); !j.Completing {
				t.Errorf("snapshot Completing false while held")
			}
			recs := jobsOf(t, r.sacct("--jobs="+itoa(id)))
			if len(recs) != 1 || !equalStrings(recs[0]["state"].(map[string]any)["current"], Cancelled) {
				t.Errorf("sacct while held: %v, want one record with the bare end state [CANCELLED]", recs)
			}
			if err := r.p.HoldCompleting(id, false); err != nil {
				t.Fatal(err)
			}
			// Released an hour after the end: the purge, which counts from the
			// end, takes the job at once in both modes.
			if _, stderr, code := r.run("squeue --me --json --jobs=" + itoa(id)); code != 1 || !strings.Contains(stderr, "Invalid job id specified") {
				t.Errorf("released an hour after the end: exit %d stderr %q, want exit 1 Invalid job id", code, stderr)
			}
		})
	}
}

func TestHoldCompletingNeedsAnEndedJobAndNoRunMode(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch()
	if err := r.p.HoldCompleting(id, true); err == nil || !strings.Contains(err.Error(), "only an ended job") {
		t.Errorf("HoldCompleting on a pending job: %v, want a refusal naming that only an ended job can complete", err)
	}
	if err := r.p.HoldCompleting(999999, true); err == nil {
		t.Error("HoldCompleting on an unknown job: nil, want an error")
	}
	rr := newRunRig(t, RunMode{})
	if err := rr.p.HoldCompleting(rr.submit("true"), true); err == nil || !strings.Contains(err.Error(), "run mode decides") {
		t.Errorf("HoldCompleting in run mode: %v, want a refusal", err)
	}
}

// A job requeued while its nodes clean up is listed ["PENDING","COMPLETING"]
// with its restart count above 0; cancelling it turns it CANCELLED, the flag
// still held (HBEP-4, Phases, A completing job, Adoption and cancel).
func TestHoldCompletingOnARequeuedPendingJobThenCancel(t *testing.T) {
	r := newRig(t, Config{NotListed: NotListedAthena, MinJobAge: 10 * time.Minute})
	id := r.sbatch()
	r.start(id)
	if err := r.p.Requeue(id); err != nil {
		t.Fatal(err)
	}
	if err := r.p.HoldCompleting(id, true); err != nil {
		t.Fatalf("HoldCompleting on a requeued pending job: %v", err)
	}
	j := r.squeueJob(id)
	if !equalStrings(j["job_state"], Pending, Completing) || j["restart_cnt"].(float64) != 1 {
		t.Errorf("job_state %v restart_cnt %v, want [PENDING COMPLETING] and 1", j["job_state"], j["restart_cnt"])
	}
	r.must("scancel " + itoa(id))
	if got := r.squeueJob(id)["job_state"]; !equalStrings(got, Cancelled, Completing) {
		t.Errorf("after scancel: job_state %v, want [CANCELLED COMPLETING]", got)
	}
}
