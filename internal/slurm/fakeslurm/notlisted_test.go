package fakeslurm

// New. The two "not listed" shapes S7 measured, with explicit clock steps. The
// timings are S7's: MinJobAge 10 s; Helios answers an empty list and then the
// error, Athena keeps listing the ended job (FAILED, NonZeroExitCode).

import (
	"testing"
	"time"
)

// queueAnswer runs squeue for the one job and returns the number of listed jobs
// and the raw outcome.
func queueAnswer(t *testing.T, r *rig, id int) (n, code int, stdout, stderr string) {
	t.Helper()
	stdout, stderr, code = r.run("squeue --me --json --jobs=" + itoa(id))
	if code == 0 {
		n = len(jobsOf(t, decode(t, stdout)))
	}
	return n, code, stdout, stderr
}

// listKind is what squeue answers for one job at one moment.
type listKind int

const (
	listed    listKind = iota // the job, in the state step.state
	emptyList                 // exit 0 and "jobs": []
	invalidID                 // exit 1, "Invalid job id specified"
)

// step moves the clock to t0+at, ends the job first when end is set, and says
// what squeue --me and squeue --me --jobs=<id> must answer then.
type step struct {
	name  string
	at    time.Duration
	end   string
	kind  listKind
	state string
}

// runTimeline follows one job on one clock through the steps, in order: the
// job runs from t0 and ends at t0+20 s; MinJobAge is 10 s, so it is purged from
// t0+30 s.
func runTimeline(t *testing.T, mode NotListedMode, steps []step) {
	t.Helper()
	r := newRig(t, Config{NotListed: mode})
	id := r.sbatch("--time=10")
	r.start(id)
	for _, s := range steps {
		r.clock.Set(t0.Add(s.at))
		if s.end != "" {
			r.end(id, s.end)
		}
		me := idsOf(t, r.squeue())
		stdout, stderr, code := r.run("squeue --me --json --jobs=" + itoa(id))
		switch s.kind {
		case listed:
			js := jobsOf(t, decode(t, stdout))
			if code != 0 || len(js) != 1 || !equalInts(me, []int{id}) {
				t.Fatalf("%s: --jobs exit %d with %d jobs, --me lists %v, want the job in both", s.name, code, len(js), me)
			}
			wantStrings(t, s.name+" job_state", js[0]["job_state"], s.state)
			wantListedFields(t, s, js[0])
		case emptyList:
			if code != 0 || len(jobsOf(t, decode(t, stdout))) != 0 || len(me) != 0 {
				t.Errorf("%s: --jobs exit %d stdout %q, --me lists %v, want exit 0, an empty list, and nothing in --me", s.name, code, stdout, me)
			}
		case invalidID:
			const want = "slurm_load_jobs error: Invalid job id specified\n"
			if code != 1 || stdout != "" || stderr != want || len(me) != 0 {
				t.Errorf("%s: --jobs exit %d stdout %q stderr %q, --me lists %v, want exit 1, the error, and nothing in --me", s.name, code, stdout, stderr, me)
			}
		}
	}
}

// wantListedFields checks an ended job's listing: S7's Athena capture shows
// FAILED with the reason NonZeroExitCode, and the actual start and end.
func wantListedFields(t *testing.T, s step, j map[string]any) {
	t.Helper()
	if s.state != Failed {
		return
	}
	if j["state_reason"] != "NonZeroExitCode" {
		t.Errorf("%s: state_reason = %v, want NonZeroExitCode (S7)", s.name, j["state_reason"])
	}
	wantNV(t, s.name+" end_time", j["end_time"], true, float64(t0.Unix()+20))
	wantNV(t, s.name+" start_time", j["start_time"], true, float64(t0.Unix()))
}

// S7: Helios shows three phases in order: listed while the job runs; after the
// end an empty list with exit 0 (the --me listing drops the job at once); and,
// from MinJobAge after the end, the error. The boundary is inclusive.
func TestNotListedHeliosThreePhases(t *testing.T) {
	runTimeline(t, NotListedHelios, []step{
		{name: "phase 1: running", at: time.Second, kind: listed, state: Running},
		{name: "phase 1: running, just before the end", at: 19 * time.Second, kind: listed, state: Running},
		{name: "phase 2: at the end", at: 20 * time.Second, end: Completed, kind: emptyList},
		{name: "phase 2: end + 1 s", at: 21 * time.Second, kind: emptyList},
		{name: "phase 2: end + 9 s", at: 29 * time.Second, kind: emptyList},
		{name: "phase 3: end + 10 s", at: 30 * time.Second, kind: invalidID},
		{name: "phase 3: long after", at: 10 * time.Minute, kind: invalidID},
	})
}

func TestNotListedHeliosListedBeforeTheEnd(t *testing.T) {
	r := newRig(t, Config{NotListed: NotListedHelios})
	id := r.sbatch("--time=10")
	if n, code, _, _ := queueAnswer(t, r, id); code != 0 || n != 1 {
		t.Errorf("pending: exit %d with %d jobs, want 1", code, n)
	}
	if got := idsOf(t, r.squeue()); !equalInts(got, []int{id}) {
		t.Errorf("pending: squeue --me lists %v, want [%d]", got, id)
	}
}

// S7: Athena keeps listing the ended job in its end state until the purge
// (FAILED with NonZeroExitCode); then --me drops it and --jobs gives the error.
func TestNotListedAthena(t *testing.T) {
	runTimeline(t, NotListedAthena, []step{
		{name: "running", at: time.Second, kind: listed, state: Running},
		{name: "ended: at the end", at: 20 * time.Second, end: Failed, kind: listed, state: Failed},
		{name: "ended: end + 5 s", at: 25 * time.Second, kind: listed, state: Failed},
		{name: "ended: end + 9 s", at: 29 * time.Second, kind: listed, state: Failed},
		{name: "purged: end + 10 s", at: 30 * time.Second, kind: invalidID},
		{name: "purged: long after", at: time.Hour, kind: invalidID},
	})
}

func TestNotListedUnknownAndOtherUsersJobs(t *testing.T) {
	for _, mode := range []NotListedMode{NotListedHelios, NotListedAthena} {
		r := newRig(t, Config{NotListed: mode})
		bobJob := r.sbatchAs(loginAs(r.p, "bob"))
		if _, code, _, stderr := queueAnswer(t, r, 9999); code != 1 || stderr != "slurm_load_jobs error: Invalid job id specified\n" {
			t.Errorf("%s: an unknown ID: exit %d stderr %q, want the error", mode, code, stderr)
		}
		// Another user's job is known to slurmctld: --me filters it out, no error.
		if n, code, _, _ := queueAnswer(t, r, bobJob); code != 0 || n != 0 {
			t.Errorf("%s: another user's job: exit %d with %d jobs, want an empty list", mode, code, n)
		}
	}
}

func TestSqueueTerminalStatesAndReasons(t *testing.T) {
	r := newRig(t, Config{NotListed: NotListedAthena})
	want := map[string]string{Completed: "None", Failed: "NonZeroExitCode", Cancelled: "None", Timeout: "TimeLimit", NodeFail: "NodeDown"}
	for state, reason := range want {
		id := r.sbatch()
		r.start(id)
		r.end(id, state)
		j := r.squeueJob(id)
		wantStrings(t, state+" job_state", j["job_state"], state)
		if j["state_reason"] != reason {
			t.Errorf("%s: state_reason = %v, want %s", state, j["state_reason"], reason)
		}
		rc, status := 1.0, "ERROR"
		if state == Completed {
			rc, status = 0, "SUCCESS"
		}
		wantExitCode(t, state+" exit_code", j["exit_code"], status, true, rc)
	}
}
