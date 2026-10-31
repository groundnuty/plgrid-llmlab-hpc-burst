package fakeslurm

// New, with the cases of spikes/s4/fakeslurm/ctlverbs_test.go (hold-admin,
// requeue-hold) and the controls this fake adds. The slot rules come from the
// brief: a partition with Slots runs at most that many jobs; when one of the
// platform's running jobs ends, a phantom job of another user takes its slot.

import (
	"strings"
	"testing"
	"time"
)

func TestStartRefusals(t *testing.T) {
	r := newRig(t, Config{})
	held := r.sbatch("--hold")
	admin := r.sbatch()
	if err := r.p.HoldAdmin(admin); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{held, admin} {
		if err := r.p.Start(id); err == nil || !strings.Contains(err.Error(), "held") {
			t.Errorf("Start(%d) of a held job = %v, want a refusal that says it is held", id, err)
		}
		if j, _ := r.p.Job(id); j.State != Pending {
			t.Errorf("a refused Start changed job %d to %s", id, j.State)
		}
	}
	free := r.sbatch()
	r.start(free)
	if err := r.p.Start(free); err == nil {
		t.Errorf("Start of a running job succeeded")
	}
	if err := r.p.Start(9999); err == nil {
		t.Errorf("Start of an unknown job succeeded")
	}
	// Released, the user-held job starts.
	r.must("scontrol release " + itoa(held))
	r.start(held)
	wantStrings(t, "job_state", r.squeueJob(held)["job_state"], "RUNNING")
}

func TestEnd(t *testing.T) {
	r := newRig(t, Config{})
	pending := r.sbatch()
	running := r.sbatch()
	r.start(running)
	r.clock.Advance(40 * time.Second)
	r.end(running, Timeout)
	r.end(pending, NodeFail) // a pending job can end without having started
	j, _ := r.p.Job(running)
	if j.State != Timeout || !j.End.Equal(t0.Add(40*time.Second)) {
		t.Errorf("job = %+v, want TIMEOUT ended at t0+40 s", j)
	}
	if j, _ := r.p.Job(pending); !j.Start.IsZero() || j.State != NodeFail {
		t.Errorf("ended pending job = %+v, want NODE_FAIL with no start", j)
	}
	for _, state := range []string{"RUNNING", "PENDING", "REQUEUED", "done", ""} {
		if err := r.p.End(running, state); err == nil {
			t.Errorf("End to the non-terminal state %q succeeded", state)
		}
	}
	if err := r.p.End(running, Completed); err == nil {
		t.Errorf("End of an ended job succeeded")
	}
	if err := r.p.End(9999, Completed); err == nil {
		t.Errorf("End of an unknown job succeeded")
	}
}

// S4: requeue-hold and a plain requeue.
func TestRequeueAndRequeueHold(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch("--job-name=rh")
	r.clock.Advance(5 * time.Second)
	r.start(id)
	r.clock.Advance(5 * time.Second)
	if err := r.p.RequeueHold(id); err != nil {
		t.Fatal(err)
	}
	sq := r.squeueJob(id)
	wantStrings(t, "job_state", sq["job_state"], "PENDING")
	if sq["hold"] != true || sq["state_reason"] != "JobHeldUser" || sq["restart_cnt"] != 1.0 {
		t.Errorf("requeue-held job: hold %v reason %v restart_cnt %v", sq["hold"], sq["state_reason"], sq["restart_cnt"])
	}
	if err := r.p.Start(id); err == nil {
		t.Errorf("Start of a requeue-held job succeeded")
	}
	// The user releases it and the new execution starts.
	r.must("scontrol release " + itoa(id))
	r.start(id)
	wantStrings(t, "job_state", r.squeueJob(id)["job_state"], "RUNNING")
	if j, _ := r.p.Job(id); j.Executions != 2 || j.RestartCount != 1 {
		t.Errorf("job = %+v, want 2 executions and restart count 1", j)
	}
	// A plain requeue of a held job clears the hold: the new execution is free.
	h := r.sbatch("--hold")
	if err := r.p.Requeue(h); err != nil {
		t.Fatal(err)
	}
	if sq := r.squeueJob(h); sq["hold"] != false || sq["state_reason"] != "BeginTime" {
		t.Errorf("plain requeue of a held job: hold %v reason %v, want false and BeginTime", sq["hold"], sq["state_reason"])
	}
}

// S4: hold-admin turns a pending job, even one on a user hold, into an admin hold.
func TestHoldAdmin(t *testing.T) {
	r := newRig(t, Config{})
	pending := r.sbatch()
	running := r.sbatch()
	r.start(running)
	userHeld := r.sbatch("--hold")
	if err := r.p.HoldAdmin(pending); err != nil {
		t.Fatal(err)
	}
	sq := r.squeueJob(pending)
	if sq["hold"] != true || sq["state_reason"] != "JobHeldAdmin" {
		t.Errorf("admin-held job: hold %v reason %v", sq["hold"], sq["state_reason"])
	}
	if err := r.p.HoldAdmin(running); err == nil {
		t.Errorf("HoldAdmin of a running job succeeded")
	}
	if err := r.p.HoldAdmin(9999); err == nil {
		t.Errorf("HoldAdmin of an unknown job succeeded")
	}
	if err := r.p.HoldAdmin(userHeld); err != nil {
		t.Fatal(err)
	}
	_, errOut, code := r.run("scontrol release " + itoa(userHeld))
	if code != 1 || errOut != "scontrol: error: Access/permission denied\n" {
		t.Errorf("release of the admin hold: exit %d stderr %q", code, errOut)
	}
}

func TestSetTimeLimitAndSnapshots(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch("--time=30")
	if err := r.p.SetTimeLimit(id, 45); err != nil {
		t.Fatal(err)
	}
	est := t0.Add(time.Hour)
	if err := r.p.SetEstimate(id, &est); err != nil {
		t.Fatal(err)
	}
	est = est.Add(time.Hour) // the caller's variable is not the stored one
	j, _ := r.p.Job(id)
	if j.TimeLimit != 45 || j.Estimate == nil || !j.Estimate.Equal(t0.Add(time.Hour)) {
		t.Errorf("job = %+v, want limit 45 and the estimate t0+1h", j)
	}
	*j.Estimate = t0 // nor is a snapshot's
	if j2, _ := r.p.Job(id); !j2.Estimate.Equal(t0.Add(time.Hour)) {
		t.Errorf("the estimate changed through a snapshot")
	}
	if all := r.p.Jobs(); len(all) != 1 || all[0].ID != id {
		t.Errorf("Jobs = %+v", all)
	}
	for _, err := range []error{r.p.SetTimeLimit(9999, 1), r.p.SetEstimate(9999, nil)} {
		if err == nil {
			t.Errorf("a control on an unknown job succeeded")
		}
	}
	if _, ok := r.p.Job(9999); ok {
		t.Errorf("Job(9999) found a job")
	}
}

// ---- slots and phantom jobs ----

func slotRig(t *testing.T) *rig {
	t.Helper()
	return newRig(t, Config{Partitions: []Partition{{Name: "full", Slots: 1}, {Name: "open"}, {Name: "other", Slots: 2}}})
}

func TestFreedSlotGoesToAPhantom(t *testing.T) {
	r := slotRig(t)
	a := r.sbatch("--partition=full")
	b := r.sbatch("--partition=full")
	r.start(a)
	// The one slot is taken by a: b cannot start.
	if err := r.p.Start(b); err == nil || !strings.Contains(err.Error(), "slots") {
		t.Fatalf("Start(b) with the slot taken = %v, want a refusal about slots", err)
	}
	// a ends and its slot goes at once to a phantom job of another user.
	r.end(a, Completed)
	if n := r.p.Phantoms("full"); n != 1 {
		t.Fatalf("Phantoms = %d, want 1", n)
	}
	if err := r.p.Start(b); err == nil {
		t.Errorf("Start(b) after the slot went to a phantom succeeded: a start must never be assumed")
	}
	// A phantom is another user's job: it is in no listing of ours, and the
	// ended job a has left the Helios listing.
	if got := idsOf(t, r.squeue()); !equalInts(got, []int{b}) {
		t.Errorf("squeue lists %v, want only the pending job %d", got, b)
	}
	r.p.FreePhantoms("other") // the wrong partition changes nothing
	if r.p.Phantoms("full") != 1 {
		t.Errorf("FreePhantoms(other) freed the slot of full")
	}
	r.p.FreePhantoms()
	if n := r.p.Phantoms("full"); n != 0 {
		t.Errorf("Phantoms after FreePhantoms = %d, want 0", n)
	}
	r.start(b)
}

func TestSlotRules(t *testing.T) {
	r := slotRig(t)
	// scancel of a running job, like an end, hands the slot to a phantom.
	a := r.sbatch("--partition=full")
	r.start(a)
	r.must("scancel " + itoa(a))
	if r.p.Phantoms("full") != 1 {
		t.Errorf("a cancelled running job left %d phantoms, want 1", r.p.Phantoms("full"))
	}
	r.p.FreePhantoms("full")

	// A pending job that ends holds no slot: no phantom.
	p := r.sbatch("--partition=full")
	r.must("scancel " + itoa(p))
	if r.p.Phantoms("full") != 0 {
		t.Errorf("a cancelled pending job made %d phantoms, want 0", r.p.Phantoms("full"))
	}

	// A requeue of a running job frees its slot to a phantom too.
	q := r.sbatch("--partition=full")
	r.start(q)
	if err := r.p.Requeue(q); err != nil {
		t.Fatal(err)
	}
	if r.p.Phantoms("full") != 1 {
		t.Errorf("a requeued running job left %d phantoms, want 1", r.p.Phantoms("full"))
	}
	r.p.FreePhantoms()

	// A partition without Slots, or not listed, has no phantoms and no limit.
	for i := 0; i < 3; i++ {
		id := r.sbatch("--partition=open")
		r.start(id)
		r.end(id, Completed)
	}
	if r.p.Phantoms("open") != 0 {
		t.Errorf("an unlimited partition made phantoms")
	}

	// Slots=2 allows two at once, and the phantoms count against the limit.
	x, y, z := r.sbatch("--partition=other"), r.sbatch("--partition=other"), r.sbatch("--partition=other")
	r.start(x)
	r.start(y)
	if err := r.p.Start(z); err == nil {
		t.Errorf("a third start in a 2-slot partition succeeded")
	}
	r.end(x, Completed)
	r.end(y, Completed)
	if r.p.Phantoms("other") != 2 {
		t.Errorf("Phantoms(other) = %d, want 2", r.p.Phantoms("other"))
	}
	r.p.FreePhantoms("other")
	r.start(z)
}
