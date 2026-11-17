package main

// A killed test process leaves a DEAD mount, never a wedged one, even when it is killed while one of its own threads writes to its own
// volume (issue #658). That is the case a volume served in the test's own process cannot survive. The write is a request the server has
// already read, and a thread that waits for the answer to such a request waits uninterruptibly: the kernel finishes the process only
// when the answer comes, or the connection ends. When the server is a thread of the same process, SIGKILL ends it too, nobody answers,
// the process keeps /dev/fuse, and the connection never ends: the mount is wedged, and every stat of it blocks for good. With the server
// in another process (capfs_volume_test.go), which the kernel kills when its test process dies, the connection ends, the waiting thread
// returns, and the process finishes dying.
//
// The test process that is killed is a VICTIM: this test binary run on one test alone (testenv.StartChild), which makes its volume with
// newCapVolume, as every test does, and then writes to it while its server holds the write. The victim is killed through its pidfd, by
// the test that started it, after the server has recorded that it holds the write, so that the kill cannot land before the request is
// read or after it is answered. This test then shows what the issue asks: the victim has ended, and so has the volume's server, no
// thread of either is left (the pidfd of a process signals when its whole thread group has gone), no fusermount3 is left, a bounded stat
// of the mount fails with ENOTCONN (or the mount is gone, where auto_unmount unmounts a dead one), and the next newCapVolume sweeps it.
//
// A volume served in the victim's own process makes this test fail, at the victim's wait: it never finishes dying, and the failure names
// its threads and what they wait in. That mutant also leaves the wedged mount, and a process that only a connection abort can end, on
// whatever machine runs it.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// capVictimTest is the victim's test, which does nothing unless it was started as a child (testenv.ChildRecordPath).
const capVictimTest = "TestCapVictimWritesToItsVolume"

// capVictimRecord is what the victim writes before it writes to its volume.
type capVictimRecord struct {
	Victim testenv.ProcID `json:"victim"` // the victim itself
	Server testenv.ProcID `json:"server"` // the process that serves its volume
	Mount  string         `json:"mount"`
	Ctl    string         `json:"ctl"` // the volume's control directory, outside the mount
	// Scratch is the directory that holds the victim's temporary directories (the server's output file is in one of them). The victim is
	// killed, so the testing package never removes it, and the test that killed the victim does. No mount is under it: a volume's directory
	// is made beside it, by os.MkdirTemp.
	Scratch string `json:"scratch"`
}

// TestCapVictimWritesToItsVolume is the victim of TestAKilledTestProcessMidWriteLeavesADeadMountNotAWedgedOne: it makes a volume, records
// who it is and who serves the volume, asks the server to hold every write, and writes. The write is bounded by nothing, on purpose: this
// is the test process that waits on its own volume, and it is killed there.
func TestCapVictimWritesToItsVolume(t *testing.T) {
	if _, ok := testenv.ChildRecordPath(); !ok {
		return // an ordinary run of the tests: this is no victim
	}
	scratch := filepath.Dir(t.TempDir())
	v := newCapVolume(t, capChildLimit)
	victim, err := testenv.IdentityOf(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	server, err := testenv.IdentityOf(v.serverPID)
	if err != nil {
		t.Fatal(err)
	}
	testenv.WriteChildRecord(t, capVictimRecord{Victim: victim, Server: server, Mount: v.mount, Ctl: v.ctl.dir, Scratch: scratch})
	v.stall()
	err = os.WriteFile(filepath.Join(v.mount, "victim"), []byte("written by the victim"), 0o600)
	t.Fatalf("the victim's write returned (%v), so the victim was not killed while its volume held the write", err)
}

// capThreadStates is a line for each thread of the process: its id, its state letter (D is an uninterruptible sleep) and what it waits in.
// Reads of /proc/<pid>/task/<tid>/stat and wchan never block on a mount.
func capThreadStates(pid int) string {
	dir := fmt.Sprintf("/proc/%d/task", pid)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return "no thread list (" + err.Error() + ")"
	}
	var parts []string
	for _, e := range ents {
		stat, err := os.ReadFile(filepath.Join(dir, e.Name(), "stat"))
		if err != nil {
			continue
		}
		s := string(stat)
		fields := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
		if len(fields) == 0 {
			continue
		}
		wchan, _ := os.ReadFile(filepath.Join(dir, e.Name(), "wchan"))
		parts = append(parts, fmt.Sprintf("thread %s: %s in %s", e.Name(), fields[0], strings.TrimSpace(string(wchan))))
	}
	return strings.Join(parts, "; ")
}

// capCleanUpAfterVictim ends what the victim may have left, whatever state the test left it in: the server of its volume, if it still
// runs (through a pidfd that was checked against its start time), a fusermount3 left for the mount point, and a mount left dead, with the
// directory, and the victim's scratch directory, which holds no mount. A mount that is not dead is reported and left, with its directory.
func capCleanUpAfterVictim(t *testing.T, rec capVictimRecord) {
	t.Helper()
	if rec.Mount == "" {
		return // the victim never made its volume
	}
	defer func() {
		// Only the directory the victim's testing package made for it: under the temp directory, and named for the victim's test.
		if rec.Scratch != "" && filepath.Dir(rec.Scratch) == filepath.Clean(os.TempDir()) && strings.HasPrefix(filepath.Base(rec.Scratch), capVictimTest) {
			if err := os.RemoveAll(rec.Scratch); err != nil {
				t.Errorf("removing the victim's scratch directory: %v", err)
			}
		}
	}()
	if p, err := testenv.OpenProc(rec.Server); err == nil {
		_ = p.Kill()
		if !p.WaitExit(capChildBound) {
			t.Errorf("the victim's volume server did not end after SIGKILL")
		}
		p.Close()
	}
	capKillLeftFusermount3(t, rec.Mount)
	if capUnmountIfDead(t, rec.Mount, capChildBound) {
		if err := capRemoveDir(filepath.Dir(rec.Mount)); err != nil {
			t.Errorf("%v", err)
		}
	}
}

// The test of the issue (#658): a test process killed WHILE one of its threads writes to its volume, a write the server has read and
// holds, leaves a DEAD mount. The victim ends (and so does the server: the kernel kills it when the victim dies); no thread of either is
// left; no fusermount3 is left; a bounded stat of the mount fails with ENOTCONN, or the mount is gone; and the next newCapVolume sweeps
// it and works. Each wait is bounded, and none touches the mount before the victim has ended, so that a failure never adds a thread
// that is stuck on the mount.
func TestAKilledTestProcessMidWriteLeavesADeadMountNotAWedgedOne(t *testing.T) {
	capFuseOrSkip(t)
	var rec capVictimRecord
	t.Cleanup(func() { capCleanUpAfterVictim(t, rec) }) // runs after the child's own clean-up, which only kills the victim
	child := testenv.StartChild(t, capVictimTest)
	child.WaitRecord(2*capChildBound, &rec)
	held := filepath.Join(rec.Ctl, capCtlStalled)
	capWaitFor(t, capChildBound, "the volume's server to hold the victim's write (it has read the request, and not answered it)", func() bool {
		_, err := os.Stat(held)
		return err == nil
	})

	victim, err := testenv.OpenProc(rec.Victim)
	if err != nil {
		t.Fatalf("the victim, whose write the server holds, is not running: %v\n%s", err, child.Output())
	}
	defer victim.Close()
	if err := victim.Kill(); err != nil {
		t.Fatalf("SIGKILL to the victim: %v", err)
	}
	if !victim.WaitExit(capChildBound) {
		t.Fatalf("the killed test process did not finish dying within %s: a thread of it waits on its own volume for a request the server has "+
			"read, and nothing ends that wait. Its mount %s is wedged, and its server must not be in its own process. Its threads: %s",
			capChildBound, rec.Mount, capThreadStates(rec.Victim.PID))
	}

	server, err := testenv.OpenProc(rec.Server)
	switch {
	case errors.Is(err, testenv.ErrProcGone):
	case err != nil:
		t.Errorf("the victim's volume server cannot be looked at: %v", err)
	default:
		if !server.WaitExit(capChildBound) {
			t.Errorf("the victim's volume server was still running %s after the victim died; it is killed. Its threads: %s", capChildBound, capThreadStates(rec.Server.PID))
			_ = server.Kill()
		}
		server.Close()
	}

	capWaitFor(t, capChildBound, "the victim's fusermount3 to exit", func() bool { return len(capFusermount3For(rec.Mount)) == 0 })
	capAwaitDeadMount(t, rec.Mount)
	v := newCapVolume(t, capChildLimit)
	if capListed(rec.Mount) {
		t.Errorf("%s, the mount of the killed test process, is still listed after newCapVolume", rec.Mount)
	}
	writeVolume(t, v)
}
