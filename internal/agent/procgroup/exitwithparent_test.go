package procgroup_test

// Spec.ExitWithParent (#514): a process started with it dies with the process that started it,
// however that dies, as a test binary killed by go test's -timeout runs no t.Cleanup. The starter is
// a child test binary (this binary, on one test, testenv.StartChild), killed through its pidfd; the
// process it started is followed through its own identity (PID, start time, boot ID) by a pidfd taken
// before the kill, never by name.

import (
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// pgChildGuardEnv says whether the child starts its process with ExitWithParent ("1") or without.
const pgChildGuardEnv = "HPCBURST_PROCGROUP_CHILD_GUARD"

// TestProcgroupChild is run only as a child of TestExitWithParentKillsTheProcessWithItsStarter, and
// returns at once otherwise. It starts sleep through procgroup, writes sleep's identity for its
// parent, and waits to be killed; the 2 minutes only end a child that nobody kills.
func TestProcgroupChild(t *testing.T) {
	if _, ok := testenv.ChildRecordPath(); !ok {
		return
	}
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Fatal(err)
	}
	g, err := procgroup.Start(procgroup.Spec{Path: sleep, Args: []string{"300"}, ExitWithParent: os.Getenv(pgChildGuardEnv) == "1"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	id, err := testenv.IdentityOf(g.PID())
	if err != nil {
		t.Fatal(err)
	}
	testenv.WriteChildRecord(t, id)
	select {
	case <-time.After(2 * time.Minute):
		t.Fatal("the child was not killed")
	case <-g.Done():
		t.Fatalf("sleep exited under the child: code %d", g.ExitCode())
	}
}

// TestExitWithParentKillsTheProcessWithItsStarter: with ExitWithParent, the process exits once the
// test binary that started it is killed. The control, without it, shows the check can see a process
// that outlives its starter: sleep still runs 500 ms after the child has been reaped (a Pdeathsig is
// sent while the child exits, before it can be reaped, so a signalled sleep dies within
// milliseconds), and is then killed through its verified pidfd.
func TestExitWithParentKillsTheProcessWithItsStarter(t *testing.T) {
	for _, c := range []struct {
		name  string
		guard bool
	}{
		{"with ExitWithParent", true},
		{"without it (the control)", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			guard := "0"
			if c.guard {
				guard = "1"
			}
			child := testenv.StartChild(t, "TestProcgroupChild", pgChildGuardEnv+"="+guard)
			var id testenv.ProcID
			child.WaitRecord(time.Minute, &id)
			p, err := testenv.OpenProc(id)
			if err != nil {
				t.Fatalf("the child's sleep %+v is not running before the kill, so this check could not see a leak: %v", id, err)
			}
			defer p.Close()
			if p.WaitExit(0) {
				t.Fatalf("the child's sleep %+v had already exited before the kill", id)
			}
			child.Kill()
			if c.guard {
				if !p.WaitExit(20 * time.Second) {
					_ = p.Kill()
					t.Errorf("sleep %+v outlived the test binary that started it with ExitWithParent", id)
				}
				return
			}
			outlived := !p.WaitExit(500 * time.Millisecond)
			_ = p.Kill()
			p.WaitExit(20 * time.Second)
			if !outlived {
				t.Error("without ExitWithParent sleep still died with its starter: the check above cannot tell")
			}
		})
	}
}

// TestExitWithParentOutlivesTheCallersThread: the process is tied to procgroup's own locked thread,
// not to the caller's. The kernel sends Pdeathsig when the thread that forked the child exits
// (golang/go#27505), and a goroutine that locks its thread and returns ends that thread. Here the
// caller is such a goroutine: sleep, started by it with ExitWithParent, must still run 500 ms after
// the caller's thread has ended (a Pdeathsig kills within milliseconds), and is then stopped.
func TestExitWithParentOutlivesTheCallersThread(t *testing.T) {
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Fatal(err)
	}
	type started struct {
		g   *procgroup.Group
		err error
	}
	res := make(chan started, 1)
	go func() {
		runtime.LockOSThread() // never unlocked: this goroutine's return ends its thread
		g, err := procgroup.Start(procgroup.Spec{Path: sleep, Args: []string{"300"}, ExitWithParent: true}, time.Now())
		res <- started{g, err}
	}()
	r := <-res
	if r.err != nil {
		t.Fatal(r.err)
	}
	id, err := testenv.IdentityOf(r.g.PID())
	if err != nil {
		t.Fatalf("sleep is not running right after its start: %v", err)
	}
	defer func() {
		// Stopped through a pidfd on its verified identity, never by group.
		if p, err := testenv.OpenProc(id); err == nil {
			_ = p.Kill()
			p.WaitExit(20 * time.Second)
			p.Close()
		}
		<-r.g.Done()
	}()
	select {
	case <-r.g.Exited():
		t.Fatalf("sleep died with the thread of the goroutine that started it: code %d", r.g.ExitCode())
	case <-time.After(500 * time.Millisecond):
	}
}
