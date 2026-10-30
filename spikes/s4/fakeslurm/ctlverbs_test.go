package fakeslurm

import (
	"strconv"
	"testing"
	"time"
)

// 5. hold-admin and requeue-hold.
func TestCtlHoldAdmin(t *testing.T) {
	r := newRig(t)
	pending := r.sbatch()
	running := r.sbatch()
	r.ctl("start", strconv.Itoa(running))
	userHeld := r.sbatch("-H")

	r.ctl("hold-admin", strconv.Itoa(pending))
	if ev := r.lastEvent("ctl"); ev["job_id"] != float64(pending) || ev["exit"] != 0.0 {
		t.Errorf("hold-admin event %v", ev)
	}
	sq := r.squeueJob(pending)
	wantStrings(t, "job_state", sq["job_state"], "PENDING")
	wantHold(t, "admin-held job", sq, true, "JobHeldAdmin")
	if r.stateJob(pending)["held"] != true {
		t.Errorf("state Held = false after hold-admin")
	}

	// a running job cannot be held; an unknown one neither
	for _, id := range []string{strconv.Itoa(running), "9999", "x"} {
		if _, _, code := r.run("fakeslurmctl", "hold-admin", id); code != 1 {
			t.Errorf("hold-admin %s: exit %d, want 1", id, code)
		}
	}
	wantHold(t, "running job after a refused hold-admin", r.squeueJob(running), false, "None")

	// an admin can turn a user hold into an admin hold
	r.ctl("hold-admin", strconv.Itoa(userHeld))
	wantHold(t, "user hold turned admin", r.squeueJob(userHeld), true, "JobHeldAdmin")
	_, errOut, code := r.run("scontrol", "release", strconv.Itoa(userHeld))
	if code != 1 || errOut != "scontrol: error: Access/permission denied\n" {
		t.Errorf("release of the admin hold: exit %d stderr %q", code, errOut)
	}
}

func TestCtlRequeueHold(t *testing.T) {
	r := newRig(t)
	id := r.sbatch("--job-name=rh")
	r.clk.Advance(5 * time.Second)
	r.ctl("start", strconv.Itoa(id))
	r.clk.Advance(5 * time.Second)
	r.ctl("requeue-hold", strconv.Itoa(id))
	if ev := r.lastEvent("ctl"); ev["job_id"] != float64(id) || ev["exit"] != 0.0 {
		t.Errorf("requeue-hold event %v", ev)
	}

	sq := r.squeueJob(id)
	wantStrings(t, "job_state", sq["job_state"], "PENDING")
	wantHold(t, "requeue-held job", sq, true, "JobHeldUser")
	if sq["restart_cnt"] != 1.0 {
		t.Errorf("restart_cnt %v, want 1", sq["restart_cnt"])
	}
	if _, _, code := r.run("fakeslurmctl", "start", strconv.Itoa(id)); code != 1 {
		t.Errorf("start of a requeue-held job: exit %d, want 1", code)
	}
	// the user releases it and the new execution starts
	if _, _, code := r.run("scontrol", "release", strconv.Itoa(id)); code != 0 {
		t.Fatalf("release: exit %d", code)
	}
	wantHold(t, "after release", r.squeueJob(id), false, "None")
	r.ctl("start", strconv.Itoa(id))
	wantStrings(t, "job_state", r.squeueJob(id)["job_state"], "RUNNING")

	// a plain requeue of a held job clears the hold: the new execution is free
	h := r.sbatch("-H")
	r.ctl("requeue", strconv.Itoa(h))
	wantHold(t, "plain requeue of a held job", r.squeueJob(h), false, "BeginTime")
}

// 5. acct-hide / acct-show: slurmdbd does not have the record (yet).
func TestAcctHideShow(t *testing.T) {
	r := newRig(t)
	a := r.sbatch("--job-name=a")
	b := r.sbatch("--job-name=b")
	r.ctl("start", strconv.Itoa(a))
	r.ctl("requeue", strconv.Itoa(a)) // a has two records with -D

	r.ctl("acct-hide", strconv.Itoa(a))
	if ev := r.lastEvent("ctl"); ev["job_id"] != float64(a) || ev["exit"] != 0.0 {
		t.Errorf("acct-hide event %v", ev)
	}
	for _, args := range [][]string{nil, {"-D"}, {"-j", strconv.Itoa(a)}, {"-a", "-X"}, {"--name=a"}} {
		for _, id := range ids(t, r.sacct(args...)) {
			if id == a {
				t.Errorf("sacct %v lists hidden job %d", args, a)
			}
		}
	}
	if got := ids(t, r.sacct("-D")); !sameInts(got, []int{b}) {
		t.Errorf("sacct -D lists %v, want only the visible job %d", got, b)
	}
	if got := ids(t, r.squeue()); !sameInts(got, []int{a, b}) {
		t.Errorf("squeue lists %v, want both jobs (squeue is unaffected)", got)
	}

	r.ctl("acct-show", strconv.Itoa(a))
	if got := ids(t, r.sacct("-D")); !sameInts(got, []int{a, a, b}) {
		t.Errorf("sacct -D after acct-show lists %v, want [%d %d %d]", got, a, a, b)
	}
	for _, args := range [][]string{{"acct-hide", "9999"}, {"acct-show", "9999"}, {"acct-hide"}} {
		if _, _, code := r.run(append([]string{"fakeslurmctl"}, args...)...); code != 1 {
			t.Errorf("fakeslurmctl %v: exit %d, want 1", args, code)
		}
	}
}

// 5. forget-requeue: an accounting purge of the earlier records.
func TestForgetRequeue(t *testing.T) {
	for _, version := range []string{"v0.0.42", "v0.0.40"} {
		t.Run(version, func(t *testing.T) {
			r := newRig(t)
			r.env["FAKE_SLURM_DATA_PARSER"] = version
			id := r.sbatch("--job-name=fr")
			r.clk.Advance(time.Second)
			r.ctl("start", strconv.Itoa(id))
			r.clk.Advance(time.Second)
			r.ctl("requeue", strconv.Itoa(id))
			r.clk.Advance(time.Second)
			r.ctl("start", strconv.Itoa(id))
			r.clk.Advance(time.Second)
			r.ctl("requeue", strconv.Itoa(id))
			latestSubmit := r.clk.Now().Unix()

			if n := len(jobs(t, r.sacct("-D"))); n != 3 {
				t.Fatalf("before the purge sacct -D lists %d records, want 3", n)
			}
			if rc := r.squeueJob(id)["restart_cnt"]; rc != 2.0 {
				t.Fatalf("before the purge restart_cnt %v, want 2", rc)
			}

			r.ctl("forget-requeue", strconv.Itoa(id))
			if ev := r.lastEvent("ctl"); ev["job_id"] != float64(id) || ev["exit"] != 0.0 {
				t.Errorf("forget-requeue event %v", ev)
			}

			sq := r.squeueJob(id)
			if sq["restart_cnt"] != 0.0 {
				t.Errorf("squeue restart_cnt %v, want 0", sq["restart_cnt"])
			}
			wantStrings(t, "squeue job_state", sq["job_state"], "PENDING")
			wantNV(t, "squeue submit_time", sq["submit_time"], true, float64(latestSubmit))
			recs := jobs(t, r.sacct("-D"))
			if len(recs) != 1 {
				t.Fatalf("after the purge sacct -D lists %d records, want 1", len(recs))
			}
			wantStrings(t, "sacct state.current", recs[0]["state"].(map[string]any)["current"], "PENDING")
			if rc, has := recs[0]["restart_cnt"]; version == "v0.0.42" && (!has || rc != 0.0) {
				t.Errorf("sacct restart_cnt %v (present %v), want 0", rc, has)
			} else if version == "v0.0.40" && has {
				t.Errorf("v0.0.40 sacct record has restart_cnt")
			}

			// the job is still the same job and can run and be requeued again
			r.ctl("start", strconv.Itoa(id))
			wantStrings(t, "job_state", r.squeueJob(id)["job_state"], "RUNNING")
			r.ctl("requeue", strconv.Itoa(id))
			if rc := r.squeueJob(id)["restart_cnt"]; rc != 1.0 {
				t.Errorf("restart_cnt %v after a new requeue, want 1", rc)
			}
		})
	}
	r := newRig(t)
	if _, _, code := r.run("fakeslurmctl", "forget-requeue", "9999"); code != 1 {
		t.Errorf("forget-requeue of an unknown job: exit %d, want 1", code)
	}
}
