package fakeslurm

// New. The paths of run mode that the PR 153 review found untested (N5 and N7)
// and the contract of Finished and Close. Every wait is on observable state:
// a file, a job's snapshot, a process's state in /proc.

import (
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// scancel reaches a process in a process group of its own, as the engine of
// hb-agent is: the TERM goes to the job's session and its descendants, not
// only to the batch shell's group. The shell traps TERM and carries on, so it
// is the helper that has to get TERM and write its file; if only the shell
// were signalled, the helper would live until the grace ended.
func TestRunModeScancelReachesAChildInItsOwnProcessGroup(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r := newRunRig(t, RunMode{KillWait: time.Hour, Env: map[string]string{helperEnv: "setpgid-term"}})
	child, term, release := r.home+"/child", r.home+"/term", r.home+"/release"
	id := r.submit("trap : TERM; " + exe + " " + child + " " + term + " & " + waitLoop(release))
	f := strings.Fields(r.waitFile(child))
	if len(f) != 2 {
		t.Fatalf("child file %q, want a PID and a process group", f)
	}
	if f[1] == itoa(r.job(id).PID) {
		t.Fatalf("the child shares the batch shell's process group %s; the test needs one of its own", f[1])
	}
	r.must("scancel " + itoa(id))
	if got := r.waitFile(term); got != "term\n" { // the child got TERM and wrote its file
		t.Errorf("the child wrote %q on TERM", got)
	}
	if err := os.WriteFile(release, nil, 0o644); err != nil { // the shell's loop ends
		t.Fatal(err)
	}
	j := r.waitFinished(id)
	if j.State != Cancelled || j.TermAt.IsZero() || !j.KillAt.IsZero() {
		t.Errorf("state %s, TermAt %v, KillAt %v; want CANCELLED, TERM sent, no KILL (the grace is an hour)", j.State, j.TermAt, j.KillAt)
	}
}

// End(NODE_FAIL) kills at once, as a failed node does: KILL with no TERM, and
// a job that ignores TERM is gone all the same.
func TestRunModeEndNodeFailKillsAtOnce(t *testing.T) {
	r := newRunRig(t, RunMode{KillWait: time.Hour})
	up := r.home + "/up"
	id := r.submit("trap \"\" TERM; echo up > " + up + "; " + waitLoop(r.home+"/never"))
	r.waitFile(up)
	if err := r.p.End(id, NodeFail); err != nil {
		t.Fatal(err)
	}
	j := r.waitFinished(id)
	if j.State != NodeFail || !j.TermAt.IsZero() || j.KillAt.IsZero() || j.ExitSignal != syscall.SIGKILL {
		t.Errorf("state %s, TermAt %v, KillAt %v, signal %v; want NODE_FAIL, KILL only, SIGKILL", j.State, j.TermAt, j.KillAt, j.ExitSignal)
	}
}

// Standard output and error go to slurm-<job ID>.out in the submit directory
// when --output and --error are not given, both to the one file; a separate
// --error is its own file, and a relative --output is relative to the submit
// directory.
func TestRunModeOutputFiles(t *testing.T) {
	r := newRunRig(t, RunMode{})
	both := r.submit("echo out; echo err >&2")
	split := r.submit("echo out; echo err >&2", "--output=o.txt", "--error=e.txt")
	r.waitFinished(both)
	r.waitFinished(split)
	for name, want := range map[string]string{
		"slurm-" + itoa(both) + ".out": "out\nerr\n",
		"o.txt":                        "out\n",
		"e.txt":                        "err\n",
	} {
		if got, err := os.ReadFile(r.home + "/" + name); err != nil || string(got) != want {
			t.Errorf("%s holds %q (%v), want %q", name, got, err, want)
		}
	}
}

// A requeued execution appends to the output of the first instead of
// truncating it, with SLURM_RESTART_COUNT set.
func TestRunModeRequeueAppendsToTheOutput(t *testing.T) {
	r := newRunRig(t, RunMode{})
	out, goFile := r.home+"/o.txt", r.home+"/go"
	id := r.submit("echo run-${SLURM_RESTART_COUNT:-0}; "+waitLoop(goFile), "--output="+out)
	waitFor(t, "the first run's line", func() bool { b, _ := os.ReadFile(out); return string(b) == "run-0\n" })
	if err := r.p.Requeue(id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the second run's line after the first's", func() bool { b, _ := os.ReadFile(out); return string(b) == "run-0\nrun-1\n" })
	if err := os.WriteFile(goFile, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	r.waitFinished(id)
}

// The new execution of a requeued job starts only once the old one's
// processes are gone, not when it is requeued. The old execution's sweep is
// held while its shell is a zombie; the job must stay PENDING with no process
// until the sweep ends.
func TestRunModeRequeuedExecutionWaitsForTheOldOne(t *testing.T) {
	r := newRunRig(t, RunMode{})
	var old atomic.Int64
	var held atomic.Bool
	inSweep, release := make(chan struct{}), make(chan struct{})
	r.p.knobs.proc.scanned = func(leader int) {
		if int64(leader) == old.Load() && procState(leader) == "Z" && held.CompareAndSwap(false, true) {
			close(inSweep)
			<-release
		}
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	id := r.submit(waitLoop(r.home + "/never"))
	first := r.job(id).PID
	old.Store(int64(first))
	if err := r.p.Requeue(id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the old execution's sweep to be held", func() bool {
		select {
		case <-inSweep:
			return true
		default:
			return false
		}
	})
	if j := r.job(id); j.State != Pending || j.PID != 0 || j.Executions != 2 {
		t.Errorf("while the old execution is cleared: state %s, PID %d, %d executions; want PENDING, no process, 2", j.State, j.PID, j.Executions)
	}
	close(release)
	waitFor(t, "the new execution to start", func() bool { j := r.job(id); return j.State == Running && j.PID > 0 })
	if j := r.job(id); j.PID == first {
		t.Errorf("the new execution has the old PID %d", first)
	}
}

// A partition with one slot holds the next job until the slot is free, and an
// ended job's slot goes to a phantom job of another user until FreePhantoms
// (HBEP-4's QoS model): the pending job starts within FreePhantoms.
func TestRunModeSlotsAndPhantoms(t *testing.T) {
	r := newRunRigCfg(t, Config{
		Run: &RunMode{}, NotListed: NotListedAthena,
		Partitions: []Partition{{Name: "example-part", Slots: 1}},
	}, true)
	goA, outB := r.home+"/go-a", r.home+"/b"
	a := r.submit(waitLoop(goA), "--partition=example-part")
	b := r.submit("echo b > "+outB, "--partition=example-part")
	if j := r.job(a); j.State != Running {
		t.Fatalf("job a: state %s, want RUNNING", j.State)
	}
	if j := r.job(b); j.State != Pending || j.PID != 0 {
		t.Fatalf("job b while a holds the slot: state %s, PID %d; want PENDING, no process", j.State, j.PID)
	}
	if err := os.WriteFile(goA, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	r.waitFinished(a)
	if j := r.job(b); j.State != Pending || j.PID != 0 || r.p.Phantoms("example-part") != 1 {
		t.Errorf("after a ended: b is %s with PID %d, %d phantoms; want PENDING, no process, 1 phantom holding a's slot", j.State, j.PID, r.p.Phantoms("example-part"))
	}
	r.p.FreePhantoms()
	if j := r.job(b); j.PID <= 0 {
		t.Fatalf("after FreePhantoms: b is %s with PID %d; want it started within the call", j.State, j.PID)
	}
	if j := r.waitFinished(b); j.State != Completed {
		t.Errorf("job b ended %s, want COMPLETED", j.State)
	}
	if got, _ := os.ReadFile(outB); string(got) != "b\n" {
		t.Errorf("job b wrote %q", got)
	}
}

// Finished is true once the execution is terminal and none of its processes is
// left: at once for an execution that never had a process (cancelled while
// held, ended by a control), and only after the processes are gone for one
// that did. Outside run mode it is zero.
func TestRunModeFinishedMeansNoProcessLeft(t *testing.T) {
	r := newRunRig(t, RunMode{KillWait: time.Hour})
	held := r.submit("true", "--hold")
	if j := r.job(held); j.Finished || j.State != Pending {
		t.Errorf("a held job: finished %v, state %s; want PENDING, not finished", j.Finished, j.State)
	}
	r.must("scancel " + itoa(held))
	if j := r.job(held); !j.Finished || j.State != Cancelled || j.PID != 0 {
		t.Errorf("a held job cancelled: finished %v, state %s, PID %d; want finished, CANCELLED, no process", j.Finished, j.State, j.PID)
	}
	ended := r.submit("true", "--hold")
	if err := r.p.End(ended, NodeFail); err != nil {
		t.Fatal(err)
	}
	if j := r.job(ended); !j.Finished || j.State != NodeFail {
		t.Errorf("a pending job ended by a control: finished %v, state %s; want finished, NODE_FAIL", j.Finished, j.State)
	}

	// A job with a process is terminal at the scancel but not Finished until
	// the process is gone: the terminator is held before it sends TERM.
	entered, release := make(chan struct{}), make(chan struct{})
	r.p.knobs.terminateEnter = func(*jobRun) { close(entered); <-release }
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	goFile := r.home + "/go"
	running := r.submit(waitLoop(goFile))
	r.must("scancel " + itoa(running))
	waitFor(t, "the terminator to start", func() bool {
		select {
		case <-entered:
			return true
		default:
			return false
		}
	})
	j := r.job(running)
	pids, err := r.p.Processes(running)
	if j.State != Cancelled || j.Finished || err != nil || len(pids) == 0 {
		t.Errorf("a running job cancelled: state %s, finished %v, processes %v (%v); want CANCELLED, not finished, a process", j.State, j.Finished, pids, err)
	}
	close(release)
	if err := os.WriteFile(goFile, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if j := r.waitFinished(running); j.State != Cancelled {
		t.Errorf("state %s, want CANCELLED", j.State)
	}

	m := newRig(t, Config{})
	id := m.sbatch("--hold")
	if err := m.p.End(id, Cancelled); err != nil {
		t.Fatal(err)
	}
	if j, _ := m.p.Job(id); j.Finished {
		t.Errorf("outside run mode a terminal job reads Finished")
	}
}

// Close kills what runs and the job ends FAILED, killed by a signal, as it
// does when anything kills its shell; a job that had ended keeps its state.
func TestRunModeCloseLeavesRunningJobsFailed(t *testing.T) {
	r := newRunRig(t, RunMode{})
	up := r.home + "/up"
	done := r.submit("true")
	r.waitFinished(done)
	running := r.submit("echo up > " + up + "; " + waitLoop(r.home+"/never"))
	r.waitFile(up)
	if err := r.p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if j := r.job(running); j.State != Failed || j.ExitSignal != syscall.SIGKILL || !j.Finished {
		t.Errorf("a job running at Close: state %s, signal %v, finished %v; want FAILED, SIGKILL, finished", j.State, j.ExitSignal, j.Finished)
	}
	if j := r.job(done); j.State != Completed {
		t.Errorf("a job that had ended before Close: state %s, want COMPLETED", j.State)
	}
}
