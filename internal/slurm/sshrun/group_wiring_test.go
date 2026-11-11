package sshrun

// Tests for how group.go is wired into the transport (issue 363): killMaster's
// and Cancel's signals reach the sink, a refusal leaves the real master alone,
// and a scan of the sources keeps killGroup the only numeric signal and the
// barrier the only way to reap. The guards themselves are in group_test.go,
// which also has the helpers; the stop, start and probe tests are in
// group_startstop_test.go.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// killMaster sends SIGTERM and then, for a master that ignores it, SIGKILL,
// each through the guarded function. The leader is a shell that ignores TERM,
// with a group member (sleep, which inherits the ignored TERM) that only a
// GROUP signal reaches; the test holds the member's pidfd to see it end.
func TestKillMasterSendsTermThenKillToTheGroup(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	rec := &sinkRecorder{forward: killGroup}
	s.signalGroup = rec.sink
	s.killGrace = 100 * time.Millisecond

	cmd := exec.Command("/bin/sh", "-c", `trap "" TERM; sleep 120 >/dev/null & echo $!; echo ready; wait`)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	leader := newGroupLeader(s.logger(), s.signalGroup)
	if err := leader.start(cmd); err != nil {
		t.Fatal(err)
	}
	rd := bufio.NewReader(out)
	var memberPid int
	var ready string
	if _, err := fmt.Fscanln(rd, &memberPid); err != nil {
		t.Fatalf("no member PID from the shell: %v", err)
	}
	if _, err := fmt.Fscanln(rd, &ready); err != nil || ready != "ready" {
		t.Fatalf("the shell is not ready: %q %v", ready, err)
	}
	memberFd, err := unix.PidfdOpen(memberPid, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// If the test failed before the group was killed, end what it started.
		_ = unix.PidfdSendSignal(memberFd, syscall.SIGKILL, nil, 0)
		_ = unix.Close(memberFd)
	})
	m := newMaster(cmd, leader)
	t.Cleanup(func() {
		endThroughPidfd(leader)
		<-m.done
	})

	pgid := cmd.Process.Pid
	s.killMaster(m)

	want := []sinkCall{{pgid, syscall.SIGTERM}, {pgid, syscall.SIGKILL}}
	got := rec.got()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("the sink saw %+v, want %+v", got, want)
	}
	select {
	case <-m.done:
	default:
		t.Error("killMaster returned before the master was reaped")
	}
	if cmd.ProcessState == nil {
		t.Fatal("the master was never reaped")
	}
	if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || ws.Signal() != syscall.SIGKILL {
		t.Errorf("the master did not end by SIGKILL: %v", cmd.ProcessState)
	}
	waitExited(t, memberFd, 10*time.Second) // the group member died with it
}

// The real master of a transport (the fake ssh) is pinned at its start, and
// Close's stop goes through the sink: exactly one SIGTERM, to the PGID of the
// master's own process, which the fake obeys.
func TestCloseSignalsTheMastersGroupThroughTheSink(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	rec := &sinkRecorder{forward: killGroup}
	s.signalGroup = rec.sink
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	m := s.master
	s.mu.Unlock()
	pgid := m.cmd.Process.Pid
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	want := []sinkCall{{pgid, syscall.SIGTERM}}
	if got := rec.got(); len(got) != 1 || got[0] != want[0] {
		t.Errorf("the sink saw %+v, want %+v", got, want)
	}
	select {
	case <-m.done:
	default:
		t.Error("Close returned before the master was reaped")
	}
	if n := len(fakeLogLines(r.dir, "master-stopped")); n != 1 {
		t.Errorf("the fake logged %d stops, want 1 (it stops on SIGTERM)", n)
	}
}

// runStalledCheck starts s.check with a context that only the test ends, and
// returns once the stall is OBSERVED (the fake logged "check-hang"): the cut
// that follows can not come before the client stalled, however slow the
// machine. cut ends the check's context; the result arrives on the channel.
func runStalledCheck(t *testing.T, r *rig, s *SSH) (cut func(), result <-chan bool) {
	t.Helper()
	s.o.CheckTimeout = time.Minute // only the test's cut ends the check
	r.hangChecks()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan bool, 1)
	go func() { done <- s.check(ctx) }()
	r.waitForLog("check-hang", 30*time.Second)
	if n := len(fakeLogLines(r.dir, "check-hang")); n < 1 {
		t.Fatalf("%d stalled checks were logged, want at least one", n)
	}
	return cancel, done
}

// Cancel's SIGKILL: a stalled "ssh -O check", cut after its stall was
// observed, is killed by one group signal to the PGID of that very client (the
// sink reads its command line, which names the check).
func TestStalledCheckIsKilledThroughTheSink(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	var cmdline string
	rec := &sinkRecorder{forward: killGroup, onCall: func(pgid int) {
		b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pgid))
		cmdline = strings.ReplaceAll(string(b), "\x00", " ")
	}}
	s.signalGroup = rec.sink
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("the connect's own checks signalled: %+v", got)
	}
	cut, result := runStalledCheck(t, r, s)
	cut()
	select {
	case ok := <-result:
		if ok {
			t.Fatal("a stalled check reported success")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the stalled check did not end after it was cut")
	}
	got := rec.got()
	if len(got) != 1 || got[0].sig != syscall.SIGKILL || got[0].pgid <= 1 {
		t.Fatalf("the sink saw %+v, want one SIGKILL to a PGID above 1", got)
	}
	if !strings.Contains(cmdline, "-O check") {
		t.Errorf("the signalled group's leader was not the check client: %q", cmdline)
	}
}

// A guard that refuses Cancel's group signal: Cancel sends nothing and returns
// the refusal, and exec's WaitDelay ends the client through its own handle,
// so the check still returns instead of waiting for a client that stalls for
// an hour. The refusal is real: the start-time check cannot read /proc.
func TestStalledCheckStillEndsWhenAGuardRefusesTheGroupSignal(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	log, logs := capturedLog()
	s.log = log
	rec := &sinkRecorder{forward: killGroup}
	s.signalGroup = rec.sink
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	cut, result := runStalledCheck(t, r, s)
	swap(t, &procRead, procFailing) // the stall was observed; now the check client's pin cannot be read
	cut()
	select {
	case ok := <-result:
		if ok {
			t.Fatal("a stalled check reported success")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the stalled check did not end, with its group signal refused")
	}
	if got := rec.got(); len(got) != 0 {
		t.Errorf("a refused signal reached the sink: %+v", got)
	}
	if out := logs.String(); !strings.Contains(out, "guard="+guardCannotTell) {
		t.Errorf("the refusal is not in the log: %s", out)
	}
}

// A group signal that fails in the sink (it returns an error; it is called
// once): Cancel returns that error, and WaitDelay still ends the client.
func TestStalledCheckStillEndsWhenTheGroupSignalFails(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	s.log, _ = capturedLog()
	rec := &sinkRecorder{forward: func(int, syscall.Signal) error { return errors.New("example: failed in the sink") }}
	s.signalGroup = rec.sink
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	cut, result := runStalledCheck(t, r, s)
	cut()
	select {
	case ok := <-result:
		if ok {
			t.Fatal("a stalled check reported success")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the stalled check did not end, with its group signal failing")
	}
	if got := rec.got(); len(got) != 1 {
		t.Errorf("the sink was called %d times, want the one attempt: %+v", len(got), got)
	}
}

// The package's only numeric signal is killGroup, reached only through the
// sink New sets. The pin failure's kill goes through the leader's own pidfd,
// and only the barrier (groupLeader.reap), the failed pin's bounded cleanup
// and the probe reap a child. A new kill(2), a signal through an os.Process, a
// process lookup by PID, a pidfd signal or a reap anywhere else fails this
// scan. It reads the package's non-test sources, so it needs no signal to run.
func TestNoUnguardedSignalsInThePackage(t *testing.T) {
	// Where each name may appear: enclosing function -> its count.
	identUses := map[string]map[string]int{
		"killGroup":       {"New": 1},
		"pidfdSendSignal": {"start": 1, "probePidfd": 1, "probeChild": 1},
	}
	// Calls named Wait on a command: only these functions reap.
	waitIn := map[string]bool{"reap": true, "reapBounded": true, "start": true, "probePidfd": true}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	uses := map[string]map[string]int{}
	killCalls := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok {
				return true
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					if _, watched := identUses[id.Name]; watched && fn.Name.Name != id.Name {
						if uses[id.Name] == nil {
							uses[id.Name] = map[string]int{}
						}
						uses[id.Name][fn.Name.Name]++
					}
				}
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				recv, member := types.ExprString(sel.X), sel.Sel.Name
				if member == "Wait" && (recv == "cmd" || strings.HasSuffix(recv, ".cmd")) && !waitIn[fn.Name.Name] {
					t.Errorf("%s: %s.Wait in %s reaps outside the barrier", name, recv, fn.Name.Name)
				}
				isSignal := (member == "Kill" || member == "Tgkill" || member == "Tkill" || member == "Killpg" || member == "PidfdSendSignal") && (recv == "syscall" || recv == "unix") ||
					(member == "Kill" || member == "Signal") && strings.HasSuffix(recv, ".Process") ||
					member == "FindProcess" && recv == "os"
				if !isSignal {
					return true
				}
				if fn.Name.Name == "killGroup" && recv == "unix" && member == "Kill" {
					killCalls++
					return true
				}
				t.Errorf("%s: %s.%s in %s signals outside the guarded function", name, recv, member, fn.Name.Name)
				return true
			})
			return false
		})
	}
	if killCalls != 1 {
		t.Errorf("killGroup makes %d kill calls, want exactly the one", killCalls)
	}
	for ident, want := range identUses {
		for fn, n := range uses[ident] {
			if want[fn] != n {
				t.Errorf("%s names %s %d times, want %d: it may be named only in %v", fn, ident, n, want[fn], want)
			}
		}
		for fn, n := range want {
			if uses[ident][fn] != n {
				t.Errorf("%s names %s %d times, want %d", fn, ident, uses[ident][fn], n)
			}
		}
	}
}
