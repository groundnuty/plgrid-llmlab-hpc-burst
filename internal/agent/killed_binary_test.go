package agent_test

// The dial-home ssh forward, and the tunnel rig's frps, die with the test binary that started them
// (#514). A test binary that go test's -timeout panics runs no t.Cleanup, and each, in a process group
// of its own, then outlived it: the forward on a port and with keys of a temp directory nobody
// removes, frps on the rig's fixed ports. Tests set SSHForward.ExitWithParent, and the rig starts
// frps with procgroup's. The forward runs in a child test binary (this binary, on one test,
// testenv.StartChild) that the parent kills through its pidfd; the forward's ssh is followed through
// the identity the forward recorded (OnStart), by a pidfd taken before the kill, never by name.

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// sshChildGuardEnv says whether the child's forward sets ExitWithParent ("1") or not.
const sshChildGuardEnv = "HPCBURST_SSH_FORWARD_CHILD_GUARD"

// TestSSHForwardChild is run only as a child of TestAKilledTestBinaryLeavesNoSSHForward, and returns
// at once otherwise. It runs a forward through the fake ssh until it listens, writes the identity of
// the ssh the forward recorded for its parent, and waits to be killed; the 2 minutes only end a
// child that nobody kills.
func TestSSHForwardChild(t *testing.T) {
	if _, ok := testenv.ChildRecordPath(); !ok {
		return
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := enginedouble.Install(bin); err != nil {
		t.Fatal(err)
	}
	started := make(chan agent.ProcessID, 4)
	fwd := &agent.SSHForward{
		SSH: filepath.Join(bin, "ssh"), RemotePort: 29299, Config: sshConfig(t, root), Log: io.Discard,
		ExitWithParent: os.Getenv(sshChildGuardEnv) == "1",
		OnStart:        func(id agent.ProcessID) { started <- id },
	}
	go func() { _ = fwd.Run(context.Background()) }()
	for end := time.Now().Add(time.Minute); ; time.Sleep(5 * time.Millisecond) {
		if st, _ := fwd.State(); st.Listening {
			break
		}
		if time.Now().After(end) {
			t.Fatal("the fake ssh's forward never listened")
		}
	}
	id := <-started
	pid, err := testenv.IdentityOf(id.PID)
	if err != nil {
		t.Fatal(err)
	}
	testenv.WriteChildRecord(t, pid)
	time.Sleep(2 * time.Minute)
	t.Fatal("the child was not killed")
}

// TestAKilledTestBinaryLeavesNoSSHForward: with ExitWithParent, the forward's ssh exits once the test
// binary that ran the forward is killed. The control, without it, shows the check can see an ssh that
// outlives its starter: it still runs 500 ms after the child has been reaped (a Pdeathsig is sent
// while the child exits, before it can be reaped), and is then killed through its verified pidfd.
func TestAKilledTestBinaryLeavesNoSSHForward(t *testing.T) {
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
			child := testenv.StartChild(t, "TestSSHForwardChild", sshChildGuardEnv+"="+guard)
			var id testenv.ProcID
			child.WaitRecord(2*time.Minute, &id)
			p, err := testenv.OpenProc(id)
			if err != nil {
				t.Fatalf("the child's ssh %+v is not running before the kill, so this check could not see a leak: %v", id, err)
			}
			defer p.Close()
			if p.WaitExit(0) {
				t.Fatalf("the child's ssh %+v had already exited before the kill", id)
			}
			child.Kill()
			if c.guard {
				if !p.WaitExit(20 * time.Second) {
					_ = p.Kill()
					t.Errorf("the forward's ssh %+v outlived the test binary that ran it with ExitWithParent", id)
				}
				return
			}
			outlived := !p.WaitExit(500 * time.Millisecond)
			_ = p.Kill()
			p.WaitExit(20 * time.Second)
			if !outlived {
				t.Error("without ExitWithParent the ssh still died with its starter: the check above cannot tell")
			}
		})
	}
}

// TestTunnelRigFrpsChild is run only as a child of TestAKilledTestBinaryLeavesNoTunnelRigFrps, and
// returns at once otherwise. It starts the tunnel rig's frps as the tunnel tests do (startFrps),
// writes frps's identity for its parent, and waits to be killed.
func TestTunnelRigFrpsChild(t *testing.T) {
	if _, ok := testenv.ChildRecordPath(); !ok {
		return
	}
	frpsBin, _ := frpBins(t)
	f := startFrps(t, frpsBin, t.TempDir(), newProcWatch())
	id, err := testenv.IdentityOf(f.g.PID())
	if err != nil {
		t.Fatal(err)
	}
	testenv.WriteChildRecord(t, id)
	time.Sleep(2 * time.Minute)
	t.Fatal("the child was not killed")
}

// TestAKilledTestBinaryLeavesNoTunnelRigFrps: the tunnel rig's frps exits once the test binary that
// started it is killed, so it holds none of the rig's fixed ports (29280-29289) for the next run.
// TestExitWithParentKillsTheProcessWithItsStarter's control shows the check can see a survivor.
func TestAKilledTestBinaryLeavesNoTunnelRigFrps(t *testing.T) {
	frpBins(t)
	child := testenv.StartChild(t, "TestTunnelRigFrpsChild")
	var id testenv.ProcID
	child.WaitRecord(2*time.Minute, &id)
	p, err := testenv.OpenProc(id)
	if err != nil {
		t.Fatalf("the child's frps %+v is not running before the kill, so this check could not see a leak: %v", id, err)
	}
	defer p.Close()
	if p.WaitExit(0) {
		t.Fatalf("the child's frps %+v had already exited before the kill", id)
	}
	child.Kill()
	if !p.WaitExit(20 * time.Second) {
		_ = p.Kill()
		t.Errorf("the tunnel rig's frps %+v outlived the test binary that started it", id)
	}
}
