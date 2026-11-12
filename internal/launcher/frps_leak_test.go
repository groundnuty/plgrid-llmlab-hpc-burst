package launcher

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A frps that an earlier test binary left running holds the rig's ports and answers /healthz, and
// the next rig on them fails naming the wrong cause (#484). These tests run the frps rig in a child
// test binary (this binary, on one test, as TestEnvoyBinaryProbe does), so that what the rig does
// at its start is what the test sees, and not a copy of it.

// frpsRigChildBaseEnv names the port base the child's rig takes.
const frpsRigChildBaseEnv = "HPCBURST_FRPS_RIG_CHILD_BASE"

// frpsRigChildTest is the test the children below run.
const frpsRigChildTest = "TestFrpsRigChild"

// TestFrpsRigChild is run only as a child of the tests below, and returns at once otherwise. It
// starts the frps rig on the port base it is given, writes the launcher's record of that frps
// (name, PID, start time, boot ID) for its parent, and waits to be killed. The 2 minutes only end a
// child that nobody kills.
func TestFrpsRigChild(t *testing.T) {
	if _, ok := testenv.ChildRecordPath(); !ok {
		return
	}
	base, err := strconv.Atoi(os.Getenv(frpsRigChildBaseEnv))
	if err != nil {
		t.Fatalf("%s: %v", frpsRigChildBaseEnv, err)
	}
	r := newFrpsRig(t, base, 0)
	testenv.WriteChildRecord(t, r.proc.Rec())
	select {
	case <-time.After(2 * time.Minute):
		t.Fatal("the child was not killed")
	case <-r.proc.Done():
		t.Fatalf("frps exited under the child: %s", r.proc.ExitStatus())
	}
}

// A rig refuses a port that something holds, whichever of its four ports it is, before it starts
// frps, and fails naming the port: not with the /healthz of whatever answers there. The holder here
// answers nothing: the case a check of /healthz cannot see. Each case holds one port and runs the
// real rig in a child; the rig's start must fail with that port in its message and no sign that it
// went on to start frps.
func TestFrpsRigRefusesAPortSomethingHolds(t *testing.T) {
	testenv.FrpBinaries(t) // the child needs them; without them this test skips as the rigs do
	host := agentapi.Host{PortBase: frpsRigBase2}
	for name, addr := range map[string]string{
		"the plugin's": host.FrpsPluginAddr(), "frps's bind": host.FrpsBindAddr(),
		"frps's tcpmux": host.FrpsTCPMuxAddr(), "frps's web": host.FrpsWebAddr(),
	} {
		t.Run(name, func(t *testing.T) {
			ln, err := net.Listen("tcp", addr)
			if err != nil {
				t.Fatalf("this test cannot hold %s: %v", addr, err)
			}
			defer ln.Close()
			child := testenv.StartChild(t, frpsRigChildTest, frpsRigChildBaseEnv+"="+strconv.Itoa(frpsRigBase2))
			code, out := child.Wait(2 * time.Minute)
			_, port, _ := net.SplitHostPort(addr)
			if code == 0 {
				t.Fatalf("the rig started with %s held:\n%s", addr, out)
			}
			if !strings.Contains(out, addr) || !strings.Contains(out, "port "+port) {
				t.Errorf("the failure does not name %s and its port %s:\n%s", addr, port, out)
			}
			if strings.Contains(out, "never answered /healthz") || strings.Contains(out, "frps exited at start") {
				t.Errorf("the rig went on to start frps, and failed on that:\n%s", out)
			}
		})
	}
}

// The leak itself: a test binary that is killed, as `go test -timeout` and an outer time limit kill
// one, runs no t.Cleanup, and the frps it started was left running on the rig's ports. The test runs
// the real rig in a child, waits for the child's record of its frps (the launcher's own: PID, start
// time, boot ID), takes it by a pidfd, which proves it is running and is the recorded process, kills
// the child through ITS pidfd, and requires frps to exit: by the pidfd's readiness, not by a sleep,
// and by the record, never by a name. A frps that does not exit is stopped through its verified
// pidfd, so a failing run does not leave it for the next one.
func TestAKilledTestBinaryLeavesNoFrps(t *testing.T) {
	testenv.FrpBinaries(t)
	child := testenv.StartChild(t, frpsRigChildTest, frpsRigChildBaseEnv+"="+strconv.Itoa(frpsRigBase))
	var id testenv.ProcID
	child.WaitRecord(2*time.Minute, &id)
	frps, err := testenv.OpenProc(id)
	if err != nil {
		t.Fatalf("the child's frps %+v is not running before the kill, so this check could not see a leak: %v", id, err)
	}
	defer frps.Close()
	if frps.WaitExit(0) {
		t.Fatalf("the child's frps %+v had already exited before the kill", id)
	}
	child.Kill()
	if !frps.WaitExit(20 * time.Second) {
		_ = frps.Kill()
		t.Fatalf("frps %+v outlived the test binary that started it and was killed", id)
	}
}

// Pdeathsig is tied to the thread that created the child, not to the process: the kernel signals
// the child when that thread exits (golang/go#27505), and a goroutine that locks its thread and
// returns ends it. StartFrps with ExitWithParent therefore creates the child, and waits for it, on a
// goroutine of its own that locks its thread, so that a caller whose own thread ends does not take
// frps with it. The test starts frps from a goroutine that locks its thread and returns, waits until
// that thread is gone from /proc/self/task (a state, not a time), and requires frps still to be
// running. The program is a shell script that sleeps, standing in for frps for this one fact: it is
// started the way StartFrps starts frps and signalled by no one. frps is held for 500 ms after the
// thread is gone: a child the kernel signalled at the thread's exit was signalled before that exit
// can be seen, and dies within milliseconds, so a child that is alive then is not one that was.
func TestFrpsWithParentDeathSurvivesItsStartersThreadEnding(t *testing.T) {
	dir := shortDir(t)
	script := filepath.Join(dir, "frps-stand-in")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 300\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(script)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	cfg := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(cfg, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	type started struct {
		p    *FrpsProc
		tid  int
		main bool
		err  error
	}
	// The process's main thread is never ended by the runtime, so a starter that returns on it shows
	// nothing. A starter that finds itself there parks on it, locked, until the test ends, and the
	// next one has to run on another thread.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var r started
	for attempt := 0; attempt < 4 && (attempt == 0 || r.main); attempt++ {
		res := make(chan started, 1)
		go func() {
			runtime.LockOSThread() // never unlocked: the thread ends when this goroutine returns
			tid := unix.Gettid()
			if tid == os.Getpid() {
				res <- started{tid: tid, main: true}
				<-release
				return
			}
			p, err := StartFrps(FrpsSpec{Bin: FrpsBinary{Version: "test", Path: script, file: f}, Config: cfg, Log: filepath.Join(dir, "out.log"), Dir: dir, ExitWithParent: true})
			res <- started{p, tid, false, err}
		}()
		r = <-res
	}
	if r.main {
		t.Fatal("every starter ran on the process's main thread, which the runtime never ends, so this test cannot show what a thread's end does")
	}
	if r.err != nil {
		t.Fatalf("start: %v", r.err)
	}
	t.Cleanup(func() { _, _ = r.p.Stop(context.Background(), 2*time.Second) })
	for end := time.Now().Add(20 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		if _, err := os.Stat("/proc/self/task/" + strconv.Itoa(r.tid)); errors.Is(err, os.ErrNotExist) {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("the starter's thread %d never ended, so this test cannot show what its end does", r.tid)
		}
	}
	if waitExited(context.Background(), []*procHandle{r.p.h}, 500*time.Millisecond) {
		t.Fatalf("frps stand-in exited after its starter's thread ended: %s", r.p.ExitStatus())
	}
}
