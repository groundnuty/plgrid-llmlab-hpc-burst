package fakeslurm

// New. #198 (Opus nit on PR 217): a completing job holds its partition slot
// until the flag clears. Slurm frees the job's submit count and its GPUs when it
// sets the flag, but a node only once its epilog has completed: make_node_comp
// keeps the node's completing count up and make_node_idle sets it idle only when
// the count reaches 0 (src/slurmctld/node_mgr.c, Slurm 24.11.5, lines 4599-4646
// and 4752-4845), and node selection takes no COMPLETING node for a new job
// (_pick_best_nodes, src/slurmctld/node_scheduler.c, lines 1832-1846). Then the
// slot goes to a phantom job of another user, as an ended job's always does, and
// FreePhantoms releases it. The submit slot and the GPU cap stay freed at the
// flag (runepilog_test.go).

import (
	"os"
	"strings"
	"testing"
	"time"
)

// Run mode, a one-slot partition: job A is cancelled and its processes ignore
// TERM, then they end and the epilog runs for 10 minutes. Job B, pending for
// the slot, does not start at any of the three moments the old fake started it
// (FreePhantoms while A's processes live, while its epilog runs, and after it),
// except the last, once the slot has become a phantom and is freed.
func TestCompletingJobHoldsItsPartitionSlotUntilTheEpilogEnds(t *testing.T) {
	r := newEpilogRig(t, NotListedHelios, 10*time.Second, 10*time.Minute, Config{Partitions: []Partition{{Name: "gh", GPUOnly: true, Slots: 1}}})
	up, goFile, goB := r.home+"/up", r.home+"/go", r.home+"/goB"
	a := r.submit("trap \"\" TERM; echo up > "+up+"; "+waitLoop(goFile), "--time=6000", "--partition=gh", "--gres=gpu:1")
	r.waitFile(up)
	b := r.submit(waitLoop(goB), "--time=6000", "--partition=gh", "--gres=gpu:1")
	if j := r.job(b); j.State != Pending {
		t.Fatalf("precondition: job B is %s beside the running job A in a one-slot partition, want PENDING", j.State)
	}
	r.must("scancel " + itoa(a))
	waitFor(t, "the TERM to the processes of job A", func() bool { return !r.job(a).TermAt.IsZero() })
	if j := r.job(a); j.State != Cancelled || !j.Completing || j.Finished {
		t.Fatalf("precondition: job A is %s, Completing %v, Finished %v; want CANCELLED, true, false", j.State, j.Completing, j.Finished)
	}
	r.p.FreePhantoms("gh")
	if j := r.job(b); j.State != Pending {
		t.Fatalf("job B is %s while job A's processes still live, want PENDING: its node is not free", j.State)
	}
	if err := os.WriteFile(goFile, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if j := r.waitFinished(a); !j.Completing {
		t.Fatalf("precondition: job A is not completing with its processes gone, but its epilog runs for 10 minutes")
	}
	r.p.FreePhantoms("gh")
	if j := r.job(b); j.State != Pending {
		t.Fatalf("job B is %s while job A's epilog runs, want PENDING: Slurm frees a node when its epilog has completed", j.State)
	}
	if n := r.p.Phantoms("gh"); n != 0 {
		t.Fatalf("%d phantom jobs while job A's epilog runs, want 0: the slot is still job A's", n)
	}
	r.clock.Advance(10*time.Minute + time.Second) // the epilog has ended
	if n := r.p.Phantoms("gh"); n != 1 {
		t.Fatalf("%d phantom jobs after job A's epilog, want 1: the freed slot went to another user's job", n)
	}
	if j := r.job(b); j.State != Pending {
		t.Fatalf("job B is %s with the freed slot taken by a phantom, want PENDING", j.State)
	}
	r.p.FreePhantoms("gh")
	waitFor(t, "job B to start once the phantom's slot is freed", func() bool { return r.job(b).State == Running })
	if err := os.WriteFile(goB, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	r.waitFinished(b)
}

// The same without processes: HoldCompleting keeps an ended job's node busy
// until it is released, and Start for a pending job is refused meanwhile.
func TestHeldCompletingJobHoldsItsPartitionSlot(t *testing.T) {
	r := newRig(t, Config{Partitions: []Partition{{Name: "p", Slots: 1}}})
	a := r.sbatch("--partition=p")
	b := r.sbatch("--partition=p")
	r.start(a)
	r.end(a, Cancelled)
	if err := r.p.HoldCompleting(a, true); err != nil {
		t.Fatal(err)
	}
	r.p.FreePhantoms("p")
	if err := r.p.Start(b); err == nil || !strings.Contains(err.Error(), "slots of partition p are taken") {
		t.Fatalf("Start of B beside the completing job A: %v, want the refusal for a taken slot", err)
	}
	if err := r.p.HoldCompleting(a, false); err != nil {
		t.Fatal(err)
	}
	if n := r.p.Phantoms("p"); n != 1 {
		t.Fatalf("%d phantom jobs after A stopped completing, want 1", n)
	}
	r.p.FreePhantoms("p")
	if err := r.p.Start(b); err != nil {
		t.Fatalf("Start of B once A's node is free and the phantom is gone: %v", err)
	}
}
