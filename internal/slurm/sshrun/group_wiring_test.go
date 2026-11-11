package sshrun

// Tests for how group.go is wired into the transport (issue 363): killMaster's
// and Cancel's signals reach the sink, a refusal leaves the real master alone,
// New refuses a kernel without pidfd, and a scan of the sources keeps
// killGroup the only numeric signal. The guards themselves are in
// group_test.go, which also has the helpers.

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

// A guard that refuses the master's group stops killMaster at once: nothing is
// sent, not another way either, and the master is still running afterwards.
// The pin is moved one tick, so the start-time guard refuses.
func TestCloseSendsNothingWhenAGuardRefusesTheMaster(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	log, logs := capturedLog()
	s.log = log
	rec := &sinkRecorder{forward: killGroup}
	s.signalGroup = rec.sink
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	m := s.master
	s.mu.Unlock()
	t.Cleanup(func() { // the guard refused, so end the master by hand, through its pidfd
		endThroughPidfd(m.leader)
		<-m.done
	})
	pgid := m.cmd.Process.Pid
	m.leader.ticks++

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	if err := within(t, "Close with a refused master", closed, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := rec.got(); len(got) != 0 {
		t.Errorf("a refused signal reached the sink: %+v", got)
	}
	if out := logs.String(); !strings.Contains(out, "guard="+guardStartTime) || !strings.Contains(out, fmt.Sprintf("pgid=%d", pgid)) {
		t.Errorf("the log does not name the guard and the PGID: %s", out)
	}
	select {
	case <-m.done:
		t.Error("the master ended, although nothing was sent to it")
	default:
	}
	if exited, err := waitReadable(m.leader.fd, 0); err != nil || exited {
		t.Errorf("the master's pidfd shows an exit: exited=%v err=%v", exited, err)
	}
}

// Cancel's SIGKILL: a stalled "ssh -O check" is cut at CheckTimeout by one
// group signal to the PGID of that very client (the sink reads its command
// line, which names the check).
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
	// Only now the short bound: the connect above needed the default one on a
	// slow machine (a race build re-executes a slow fake ssh).
	s.o.CheckTimeout = time.Second
	r.hangChecks()
	done := make(chan error, 1)
	go func() {
		if s.check(context.Background()) {
			done <- errors.New("a stalled check reported success")
			return
		}
		done <- nil
	}()
	if err := within(t, "the stalled check", done, 20*time.Second); err != nil {
		t.Fatal(err)
	}
	got := rec.got()
	if len(got) != 1 || got[0].sig != syscall.SIGKILL || got[0].pgid <= 1 {
		t.Fatalf("the sink saw %+v, want one SIGKILL to a PGID above 1", got)
	}
	if !strings.Contains(cmdline, "-O check") {
		t.Errorf("the signalled group's leader was not the check client: %q", cmdline)
	}
	if n := len(fakeLogLines(r.dir, "check-hang")); n > 1 {
		t.Errorf("%d checks stalled, want at most the one that was killed", n)
	}
}

// When the guarded signal fails or is refused, Cancel returns its error and
// sends nothing, and exec's WaitDelay ends the client through its own handle:
// the check still returns, instead of waiting for a client that stalls for an
// hour. No guard-bypassing fallback is involved: the sink is never called.
func TestStalledCheckStillEndsWhenTheGroupSignalIsRefused(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	if err := s.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.o.CheckTimeout = time.Second // the connect above needed the default one
	rec := &sinkRecorder{forward: func(int, syscall.Signal) error { return errors.New("example: refused by the sink") }}
	s.signalGroup = rec.sink
	s.log, _ = capturedLog()
	r.hangChecks()
	done := make(chan error, 1)
	go func() {
		if s.check(context.Background()) {
			done <- errors.New("a stalled check reported success")
			return
		}
		done <- nil
	}()
	if err := within(t, "the stalled check whose group signal failed", done, 20*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := rec.got(); len(got) != 1 {
		t.Errorf("the sink was called %d times, want the one attempt: %+v", len(got), got)
	}
}

// A kernel without pidfd gets no transport: nothing could be pinned.
func TestNewRefusesAKernelWithoutPidfd(t *testing.T) {
	orig := pidfdOpen
	pidfdOpen = func(int, int) (int, error) { return -1, syscall.ENOSYS }
	t.Cleanup(func() { pidfdOpen = orig })
	r := newRig(t)
	_, err := New(Options{Name: "prov-a", Config: exampleConfig, ControlDir: r.ctl})
	if err == nil || !strings.Contains(err.Error(), "pidfd") {
		t.Errorf("New on a kernel without pidfd: got %v, want an error naming pidfd", err)
	}
}

// The package's only numeric signal is killGroup, reached only through the
// sink New sets, and the only other signal sent at all is the pin failure's,
// through the leader's pidfd. A new
// kill(2), a signal through an os.Process, a process lookup by PID or a pidfd
// signal anywhere else, say an unguarded one beside a guarded call, fails this
// scan. It reads the
// package's non-test sources, so it needs no signal to run.
func TestNoUnguardedSignalsInThePackage(t *testing.T) {
	allowed := map[string][2]string{ // enclosing function -> the one call (package, name) it may make
		"killGroup": {"unix", "Kill"},
		"start":     {"unix", "PidfdSendSignal"},
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	// killGroup itself is named only where New makes it the default sink.
	killGroupUses := map[string]int{}
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
				if id, ok := n.(*ast.Ident); ok && id.Name == "killGroup" && fn.Name.Name != "killGroup" {
					killGroupUses[fn.Name.Name]++
				}
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				recv, member := types.ExprString(sel.X), sel.Sel.Name
				isSignal := (member == "Kill" || member == "Tgkill" || member == "Tkill" || member == "Killpg" || member == "PidfdSendSignal") && (recv == "syscall" || recv == "unix") ||
					(member == "Kill" || member == "Signal") && strings.HasSuffix(recv, ".Process") ||
					member == "FindProcess" && recv == "os"
				if !isSignal {
					return true
				}
				if allowed[fn.Name.Name] != [2]string{recv, member} {
					t.Errorf("%s: %s.%s in %s signals outside the guarded function", name, recv, member, fn.Name.Name)
				}
				seen[fn.Name.Name]++
				return true
			})
			return false
		})
	}
	for fn, what := range allowed {
		if seen[fn] != 1 {
			t.Errorf("%s makes %d signal calls, want exactly the one %s.%s", fn, seen[fn], what[0], what[1])
		}
	}
	for fn, n := range killGroupUses {
		if fn != "New" || n != 1 {
			t.Errorf("%s names killGroup %d times: only New may, once, as the default sink", fn, n)
		}
	}
}
