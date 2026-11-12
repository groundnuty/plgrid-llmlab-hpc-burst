package launcher

import (
	"context"
	"errors"
	"fmt"
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

// frpsRigChildBaseEnv names the port base the child's rig takes, and frpsRigChildModeEnv how it
// starts frps: empty is the rig as it is, "detached" starts it without ExitWithParent and then lets
// go of it (Release), as a caller does that leaves frps to be found again from its record.
const (
	frpsRigChildBaseEnv = "HPCBURST_FRPS_RIG_CHILD_BASE"
	frpsRigChildModeEnv = "HPCBURST_FRPS_RIG_CHILD_MODE"
)

// frpsRigChildTest is the test the children below run.
const frpsRigChildTest = "TestFrpsRigChild"

// frpsRigRecord is what the child writes for its parent: the launcher's record of its frps (name,
// PID, start time, boot ID), and the identity of the frpc it started, when it started one.
type frpsRigRecord struct {
	Frps ProcRec         `json:"frps"`
	Frpc *testenv.ProcID `json:"frpc,omitempty"`
}

// TestFrpsRigChild is run only as a child of the tests below, and returns at once otherwise. It
// starts the frps rig on the port base it is given, and, unless it is told to detach, a frpc that logs
// in to that frps; writes the records of what it started for its parent; and waits to be killed. The
// 2 minutes only end a child that nobody kills.
func TestFrpsRigChild(t *testing.T) {
	if _, ok := testenv.ChildRecordPath(); !ok {
		return
	}
	base, err := strconv.Atoi(os.Getenv(frpsRigChildBaseEnv))
	if err != nil {
		t.Fatalf("%s: %v", frpsRigChildBaseEnv, err)
	}
	detached := os.Getenv(frpsRigChildModeEnv) == "detached"
	if detached {
		frpsRigExitWithParent = false
	}
	r := newFrpsRig(t, base, 0)
	rec := frpsRigRecord{Frps: r.proc.Rec()}
	if detached {
		r.proc.Release()
	} else {
		_, frpc := testenv.FrpBinaries(t)
		cfg := filepath.Join(r.state, "frpc-child.toml")
		conf := fmt.Sprintf("serverAddr = \"127.0.0.1\"\nserverPort = %d\nloginFailExit = false\nlog.to = %q\nmetadatas.token = \"tok-child\"\ntransport.protocol = \"tcp\"\n",
			r.host.FrpsBindPort(), filepath.Join(r.state, "frpc-child.log"))
		if err := os.WriteFile(cfg, []byte(conf), 0o600); err != nil {
			t.Fatal(err)
		}
		_, pid := startFrpcProc(t, frpc, cfg)
		id, err := testenv.IdentityOf(pid)
		if err != nil {
			t.Fatal(err)
		}
		rec.Frpc = &id
	}
	testenv.WriteChildRecord(t, rec)
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
// one, runs no t.Cleanup, and the frps and the frpc it started were left running. The test runs the
// real rig in a child, waits for the child's records (the launcher's own for frps: PID, start time,
// boot ID; the same three values for frpc), takes each process by a pidfd, which proves it is running
// and is the recorded one, kills the child through ITS pidfd, and requires both to exit: by the
// pidfd's readiness, not by a sleep, and by the record, never by a name. A process that does not exit
// is killed through its verified pidfd, so a failing run does not leave it for the next one.
func TestAKilledTestBinaryLeavesNoFrpsAndNoFrpc(t *testing.T) {
	testenv.FrpBinaries(t)
	child := testenv.StartChild(t, frpsRigChildTest, frpsRigChildBaseEnv+"="+strconv.Itoa(frpsRigBase))
	var rec frpsRigRecord
	child.WaitRecord(2*time.Minute, &rec)
	if rec.Frpc == nil {
		t.Fatalf("the child recorded no frpc: %+v", rec)
	}
	ids := map[string]testenv.ProcID{
		"frps": {PID: rec.Frps.PID, StartTicks: rec.Frps.StartTicks, BootID: rec.Frps.BootID},
		"frpc": *rec.Frpc,
	}
	held := map[string]*testenv.Proc{}
	for name, id := range ids {
		p, err := testenv.OpenProc(id)
		if err != nil {
			t.Fatalf("the child's %s %+v is not running before the kill, so this check could not see a leak: %v", name, id, err)
		}
		defer p.Close()
		if p.WaitExit(0) {
			t.Fatalf("the child's %s %+v had already exited before the kill", name, id)
		}
		held[name] = p
	}
	child.Kill()
	for name, p := range held {
		if !p.WaitExit(20 * time.Second) {
			_ = p.Kill()
			t.Errorf("%s %+v outlived the test binary that started it and was killed", name, ids[name])
		}
	}
}

// What ExitWithParent changes, and nothing else: a frps started WITHOUT it outlives its starter, as
// StartFrps and Release have always allowed, so that a caller that exits can leave frps to be found
// again from its record (StopFrps). The child starts the rig's frps without the field, lets go of it
// (Release), and is killed through its pidfd. frps is taken by a pidfd before that, and must still be
// running 500 ms after the child has been reaped (the signal a Pdeathsig would send is sent when the
// child's thread exits, so a frps that was signalled dies within milliseconds, and one that is alive
// then was not). StopFrps with the child's record must find it, verify it as ours, and stop it with
// TERM, which is the use the field leaves alone. frps that is still running at the end is killed
// through its verified pidfd.
func TestFrpsStartedWithoutExitWithParentOutlivesItsReleasedStarter(t *testing.T) {
	testenv.FrpBinaries(t)
	child := testenv.StartChild(t, frpsRigChildTest, frpsRigChildBaseEnv+"="+strconv.Itoa(frpsRigBase), frpsRigChildModeEnv+"=detached")
	var rec frpsRigRecord
	child.WaitRecord(2*time.Minute, &rec)
	p, err := testenv.OpenProc(testenv.ProcID{PID: rec.Frps.PID, StartTicks: rec.Frps.StartTicks, BootID: rec.Frps.BootID})
	if err != nil {
		t.Fatalf("the child's frps %+v is not running before the kill: %v", rec.Frps, err)
	}
	defer p.Close()
	defer func() { _ = p.Kill() }() // a no-op once it has exited
	child.Kill()
	if p.WaitExit(500 * time.Millisecond) {
		t.Fatalf("a frps started without ExitWithParent exited when its released starter was killed: %+v", rec.Frps)
	}
	rep, err := StopFrps(context.Background(), rec.Frps, 10*time.Second)
	if err != nil {
		t.Fatalf("StopFrps with the record of a frps that outlived its starter: %v (%+v)", err, rep)
	}
	if rep.Before != Ours.String() || !strings.Contains(rep.Signals, "TERM") {
		t.Errorf("StopFrps found it %q and sent %q; want it verified as ours and stopped with TERM: %+v", rep.Before, rep.Signals, rep)
	}
	if !p.WaitExit(10 * time.Second) {
		t.Errorf("frps %+v did not exit after StopFrps", rec.Frps)
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
