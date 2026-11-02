package fakeslurm

// New, with the scenarios of spikes/s4/fakeslurm/scancel_test.go and
// holds_test.go (scontrol release). The expected error lines are S4's and the
// brief's, taken from Slurm's scancel and scontrol sources.

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// S4 test 7: scancel on a pending, a finished-but-listed and a purged job.
func TestScancel(t *testing.T) {
	r := newRig(t, Config{NotListed: NotListedAthena})
	pending := r.sbatch("--job-name=pending")
	done := r.sbatch("--job-name=done")
	running := r.sbatch("--job-name=running")
	r.start(done)
	r.start(running)
	r.clock.Advance(time.Second)
	r.end(done, Completed)

	// Pending and running jobs are cancelled silently.
	r.clock.Advance(time.Second)
	cancelledAt := r.clock.Now()
	for _, id := range []int{pending, running} {
		if out, errOut, code := r.run("scancel " + itoa(id)); code != 0 || out != "" || errOut != "" {
			t.Errorf("scancel %d: exit %d stdout %q stderr %q, want a silent exit 0", id, code, out, errOut)
		}
	}
	cancelled := jobsOf(t, r.squeue("--jobs="+itoa(pending)+","+itoa(running)))
	if len(cancelled) != 2 {
		t.Fatalf("squeue lists %d cancelled jobs, want 2 (Athena lists ended jobs until the purge)", len(cancelled))
	}
	for _, j := range cancelled {
		wantStrings(t, "job_state", j["job_state"], "CANCELLED")
		wantNV(t, "end_time", j["end_time"], true, float64(cancelledAt.Unix()))
	}

	// Ended but not purged, an unknown ID, and a purged job: real Slurm 24.11
	// prints nothing and exits 0 for all three (#199), and touches nothing.
	for _, id := range []int{done, pending} {
		if out, errOut, code := r.run("scancel " + itoa(id)); code != 0 || out != "" || errOut != "" {
			t.Errorf("scancel %d of an ended job: exit %d stdout %q stderr %q, want a silent exit 0", id, code, out, errOut)
		}
	}
	if j, _ := r.p.Job(done); j.State != Completed {
		t.Errorf("job %d is %s after a scancel of the ended job, want COMPLETED", done, j.State)
	}
	if out, errOut, code := r.run("scancel 9999"); code != 0 || out != "" || errOut != "" {
		t.Errorf("scancel of an unknown ID: exit %d stdout %q stderr %q, want a silent exit 0", code, out, errOut)
	}
	// Purged: the job `done` ended at t0+1 and MinJobAge is 10 s.
	r.clock.Set(t0.Add(11 * time.Second))
	if out, errOut, code := r.run("scancel " + itoa(done)); code != 0 || out != "" || errOut != "" {
		t.Errorf("scancel of a purged job: exit %d stdout %q stderr %q, want a silent exit 0", code, out, errOut)
	}

	// Several IDs: an unknown one changes nothing, and the others are cancelled.
	r.clock.Set(t0.Add(time.Minute))
	again := r.sbatch("--job-name=again")
	if out, errOut, code := r.run("scancel 9999 " + itoa(again)); code != 0 || out != "" || errOut != "" {
		t.Errorf("scancel 9999 %d: exit %d stdout %q stderr %q, want a silent exit 0", again, code, out, errOut)
	}
	if j, _ := r.p.Job(again); j.State != Cancelled {
		t.Errorf("job %d state %s after a mixed scancel, want CANCELLED", again, j.State)
	}
	for _, line := range []string{"scancel", "scancel -u someone", "scancel --state=PENDING"} {
		if _, _, code := r.run(line); code != 1 {
			t.Errorf("%q: exit %d, want 1", line, code)
		}
	}
}

// The same ID twice in one call: the first cancels the job, the second finds it ended, which
// real scancel does not mention.
func TestScancelTwiceInOneCall(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch()
	if out, errOut, code := r.run("scancel " + itoa(id) + " " + itoa(id)); code != 0 || out != "" || errOut != "" {
		t.Errorf("exit %d stdout %q stderr %q, want a silent exit 0", code, out, errOut)
	}
	if j, _ := r.p.Job(id); j.State != Cancelled {
		t.Errorf("job %d state %s, want CANCELLED", id, j.State)
	}
}

// Config.NoisyScancel brings back the error text the fake printed before #199, for a test of the
// plugin against a Slurm that is noisy. Only the two texts real scancel keeps quiet come back;
// another user's job still gives "Access/permission denied" either way.
func TestNoisyScancelPrintsTheErrorsAndExitsOne(t *testing.T) {
	r := newRig(t, Config{NoisyScancel: true})
	done := r.sbatch("--job-name=done")
	r.start(done)
	r.clock.Advance(time.Second)
	r.end(done, Completed)

	_, errOut, code := r.run("scancel " + itoa(done))
	if want := "scancel: error: Kill job error on job id " + itoa(done) + ": Job/step already completing or completed\n"; code != 1 || errOut != want {
		t.Errorf("scancel of an ended job: exit %d stderr %q, want exit 1 and %q", code, errOut, want)
	}
	_, errOut, code = r.run("scancel 9999")
	if want := "scancel: error: Kill job error on job id 9999: Invalid job id specified\n"; code != 1 || errOut != want {
		t.Errorf("scancel of an unknown ID: exit %d stderr %q, want exit 1 and %q", code, errOut, want)
	}
	r.clock.Advance(11 * time.Second) // MinJobAge is 10 s
	_, errOut, code = r.run("scancel " + itoa(done))
	if want := "scancel: error: Kill job error on job id " + itoa(done) + ": Invalid job id specified\n"; code != 1 || errOut != want {
		t.Errorf("scancel of a purged job: exit %d stderr %q, want exit 1 and %q", code, errOut, want)
	}
	again := r.sbatch("--job-name=again")
	_, errOut, code = r.run("scancel 9999 " + itoa(again) + " " + itoa(again))
	if code != 1 || !strings.Contains(errOut, "job id 9999: Invalid job id specified") || !strings.Contains(errOut, "Job/step already completing or completed") {
		t.Errorf("scancel 9999 %d %d: exit %d stderr %q", again, again, code, errOut)
	}
	if j, _ := r.p.Job(again); j.State != Cancelled {
		t.Errorf("job %d state %s after a mixed scancel, want CANCELLED", again, j.State)
	}
}

// S4 test 3: scontrol release, one case per Slurm outcome. The error lines are
// S4's, apart from the unknown job, which the brief writes as slurm_load_jobs.
func TestScontrolRelease(t *testing.T) {
	r := newRig(t, Config{})
	userHeld := r.sbatch("--hold")
	adminHeld := r.sbatch("--hold")
	if err := r.p.HoldAdmin(adminHeld); err != nil {
		t.Fatal(err)
	}
	free := r.sbatch()
	running := r.sbatch()
	r.start(running)
	ended := r.sbatch()
	r.end(ended, Completed)
	cancelledHeld := r.sbatch("--hold")
	r.run("scancel " + itoa(cancelledHeld))

	const (
		invalid = "slurm_load_jobs error: Invalid job id specified\n"
		done    = "scontrol: error: Job has already finished\n"
		denied  = "scontrol: error: Access/permission denied\n"
	)
	for _, tc := range []struct {
		name   string
		arg    string
		exit   int
		stderr string
		check  bool // look at squeue afterwards
		held   bool
		reason string
	}{
		{"a user hold is cleared", itoa(userHeld), 0, "", true, false, "None"},
		{"releasing again is ignored", itoa(userHeld), 0, "", true, false, "None"},
		{"an admin hold is denied", itoa(adminHeld), 1, denied, true, true, "JobHeldAdmin"},
		{"not held (pending)", itoa(free), 0, "", true, false, "None"},
		{"not held (running)", itoa(running), 0, "", true, false, "None"},
		{"an ended job", itoa(ended), 1, done, false, false, ""},
		{"a cancelled held job", itoa(cancelledHeld), 1, done, false, false, ""},
		{"an unknown ID", "9999", 1, invalid, false, false, ""},
		{"not a number", "abc", 1, invalid, false, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, errOut, code := r.run("scontrol release " + tc.arg)
			if code != tc.exit || errOut != tc.stderr || out != "" {
				t.Errorf("exit %d stdout %q stderr %q, want exit %d stderr %q", code, out, errOut, tc.exit, tc.stderr)
			}
			if !tc.check {
				return
			}
			id, _ := strconv.Atoi(tc.arg)
			sq := r.squeueJob(id)
			if sq["hold"] != tc.held || sq["state_reason"] != tc.reason {
				t.Errorf("after release: hold %v reason %v, want %v and %s", sq["hold"], sq["state_reason"], tc.held, tc.reason)
			}
			if tc.held {
				wantNV(t, "priority", sq["priority"], true, 0)
			} else {
				wantNV(t, "priority", sq["priority"], true, 1000)
			}
		})
	}
	// The released job can start now; the admin-held one cannot.
	r.start(userHeld)
	if err := r.p.Start(adminHeld); err == nil || !strings.Contains(err.Error(), "held") {
		t.Errorf("Start of an admin-held job = %v, want a refusal that says it is held", err)
	}
}

func TestScontrolReleasePurgedJob(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch("--hold")
	r.run("scancel " + itoa(id)) // cancelled at t0, MinJobAge 10 s
	r.clock.Advance(10 * time.Second)
	_, errOut, code := r.run("scontrol release " + itoa(id))
	if want := "slurm_load_jobs error: Invalid job id specified\n"; code != 1 || errOut != want {
		t.Errorf("purged job: exit %d stderr %q, want exit 1 and %q", code, errOut, want)
	}
}

// S4: comma lists and separate arguments both work; a failure of one ID does
// not stop the others.
func TestScontrolReleaseSeveralIDs(t *testing.T) {
	r := newRig(t, Config{})
	a, b, c, d := r.sbatch("--hold"), r.sbatch("--hold"), r.sbatch("--hold"), r.sbatch("--hold")
	if _, errOut, code := r.run("scontrol release " + itoa(a) + "," + itoa(b)); code != 0 || errOut != "" {
		t.Fatalf("comma list: exit %d stderr %q", code, errOut)
	}
	for _, id := range []int{a, b} {
		if j, _ := r.p.Job(id); j.Held {
			t.Errorf("job %d is still held after a comma list", id)
		}
	}
	_, errOut, code := r.run("scontrol release 9999 " + itoa(c) + " " + itoa(d) + ",9998")
	if want := strings.Repeat("slurm_load_jobs error: Invalid job id specified\n", 2); code != 1 || errOut != want {
		t.Errorf("mixed IDs: exit %d stderr %q, want exit 1 and two invalid-ID lines", code, errOut)
	}
	for _, id := range []int{c, d} {
		if j, _ := r.p.Job(id); j.Held {
			t.Errorf("job %d is still held after a mixed release", id)
		}
	}
}

func TestScontrolOtherSubcommandsAreRefused(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch("--hold")
	for _, line := range []string{
		"scontrol", "scontrol show job " + itoa(id), "scontrol hold " + itoa(id),
		"scontrol update JobId=" + itoa(id) + " Priority=0", "scontrol requeue " + itoa(id), "scontrol release",
	} {
		if out, errOut, code := r.run(line); code != 1 || out != "" || errOut == "" {
			t.Errorf("%q: exit %d stdout %q stderr %q, want exit 1, a usage error, no stdout", line, code, out, errOut)
		}
	}
	if j, _ := r.p.Job(id); !j.Held {
		t.Errorf("a refused subcommand changed the hold")
	}
}

// Another user's job: the answer is "Access/permission denied", and nothing changes.
func TestOtherUsersJobsAreDenied(t *testing.T) {
	r := newRig(t, Config{})
	bob := loginAs(r.p, "bob")
	held := r.sbatch("--hold")
	running := r.sbatch()
	r.start(running)
	for _, line := range []string{"scancel " + itoa(running), "scontrol release " + itoa(held)} {
		_, errOut, code := r.runAs(bob, line, nil)
		if code != 1 || !strings.Contains(errOut, "Access/permission denied") {
			t.Errorf("bob's %q: exit %d stderr %q, want exit 1 and Access/permission denied", line, code, errOut)
		}
	}
	if j, _ := r.p.Job(running); j.State != Running {
		t.Errorf("bob's scancel changed job %d to %s", running, j.State)
	}
	if j, _ := r.p.Job(held); !j.Held {
		t.Errorf("bob released job %d", held)
	}
}
