package procgroup

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// script writes a shell script and returns its path. The script's child, a
// sleep that ignores TERM, writes its PID to child.pid.
func script(t *testing.T, body string) (path, childPIDFile string) {
	t.Helper()
	dir := t.TempDir()
	childPIDFile = filepath.Join(dir, "child.pid")
	path = filepath.Join(dir, "s.sh")
	src := "#!/bin/sh\n" + strings.ReplaceAll(body, "CHILDPID", childPIDFile) + "\n"
	if err := os.WriteFile(path, []byte(src), 0o700); err != nil {
		t.Fatal(err)
	}
	return path, childPIDFile
}

// childOf waits for the script's child PID and returns its ID.
func childOf(t *testing.T, file string) ID {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		b, err := os.ReadFile(file)
		if pid, perr := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && perr == nil {
			id, err := IDOf(pid)
			if err == nil {
				t.Cleanup(func() {
					if id.Alive() {
						_ = syscall.Kill(id.PID, syscall.SIGKILL)
					}
				})
				return id
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the script's child did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func start(t *testing.T, path string) *Group {
	t.Helper()
	g, err := Start(Spec{Path: path}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		g.Kill()
		select {
		case <-g.Done():
		case <-time.After(10 * time.Second):
		}
	})
	return g
}

func closedWithin(c <-chan struct{}, d time.Duration) bool {
	select {
	case <-c:
		return true
	case <-time.After(d):
		return false
	}
}

// A leader that exits at once, leaving a child that ignores TERM: the leader's
// exit is seen, but the group is not done while the child lives. Stop then
// TERMs the child, which ignores it, and KILLs it after the grace; only then
// is the group done (PR #87 review, Astra B4).
func TestALeaderThatExitsEarlyLeavesTheGroupUntilItsChildIsGone(t *testing.T) {
	path, cf := script(t, "trap '' TERM\nsleep 300 &\necho $! > CHILDPID\nexit 0")
	g := start(t, path)
	child := childOf(t, cf)
	if !closedWithin(g.Exited(), 5*time.Second) {
		t.Fatal("the leader's exit was not seen")
	}
	if g.ExitCode() != 0 {
		t.Fatalf("exit code %d, want 0", g.ExitCode())
	}
	if closedWithin(g.Done(), 300*time.Millisecond) {
		t.Fatal("the group is done while a member still runs")
	}
	if !child.Alive() {
		t.Fatal("the child died with no signal sent")
	}
	const grace = 400 * time.Millisecond
	stopped := time.Now()
	g.Stop(grace)
	if !closedWithin(g.Done(), 10*time.Second) {
		t.Fatal("the group did not end after Stop")
	}
	if took := time.Since(stopped); took < grace {
		t.Fatalf("done after %s, before the %s grace: the TERM-ignoring child was killed early", took, grace)
	}
	if child.Alive() {
		t.Fatalf("child %+v still runs", child)
	}
}

// The leader stays unreaped, a zombie holding the group's ID, until the group
// is empty, so no process can take the ID while the group is signalled.
func TestTheLeaderStaysUnreapedUntilTheGroupIsEmpty(t *testing.T) {
	path, cf := script(t, "trap '' TERM\nsleep 300 &\necho $! > CHILDPID\nexit 0")
	g := start(t, path)
	childOf(t, cf)
	<-g.Exited()
	st, err := readStat(g.PID())
	if err != nil || st.state != 'Z' {
		t.Fatalf("leader state %q (%v), want a zombie while its group lives", st.state, err)
	}
	g.Kill()
	if !closedWithin(g.Done(), 10*time.Second) {
		t.Fatal("not done after KILL")
	}
	if _, err := readStat(g.PID()); err == nil {
		if st, _ := readStat(g.PID()); st.start == g.ID().StartTime {
			t.Fatal("the leader is still not reaped after its group ended")
		}
	}
}

// An intentional stop of a leader that exits on TERM, with a child that
// ignores it: the child keeps its grace and is killed after it, not at the
// leader's exit.
func TestStopGivesSurvivingChildrenTheirGrace(t *testing.T) {
	path, cf := script(t, "(trap '' TERM; sleep 300) &\necho $! > CHILDPID\nwhile :; do sleep 1; done")
	g := start(t, path)
	child := childOf(t, cf)
	time.Sleep(100 * time.Millisecond)
	const grace = 500 * time.Millisecond
	stopped := time.Now()
	g.Stop(grace)
	if !closedWithin(g.Exited(), 5*time.Second) {
		t.Fatal("the leader did not exit on TERM")
	}
	if time.Since(stopped) < grace && !child.Alive() {
		t.Fatal("the child was killed at the leader's exit, before its grace")
	}
	if !closedWithin(g.Done(), 10*time.Second) {
		t.Fatal("not done")
	}
	if took := time.Since(stopped); took < grace {
		t.Fatalf("done after %s, before the grace", took)
	}
	if child.Alive() {
		t.Fatal("the child survived the KILL")
	}
}

// A group whose members all honour TERM ends well inside the grace.
func TestACooperativeGroupStopsAtOnce(t *testing.T) {
	path, cf := script(t, "sleep 300 &\necho $! > CHILDPID\nwhile :; do sleep 1; done")
	g := start(t, path)
	child := childOf(t, cf)
	stopped := time.Now()
	g.Stop(5 * time.Second)
	if !closedWithin(g.Done(), 3*time.Second) {
		t.Fatalf("a TERM-honouring group was not done within 3 s of a 5 s grace")
	}
	if g.ExitCode() != KilledExitCode {
		t.Fatalf("exit code %d, want %d for a leader ended by TERM", g.ExitCode(), KilledExitCode)
	}
	if child.Alive() {
		t.Fatal("the child survived")
	}
	_ = stopped
}

func TestExitCode(t *testing.T) {
	path, _ := script(t, "exit 3")
	g := start(t, path)
	if !closedWithin(g.Done(), 5*time.Second) {
		t.Fatal("not done")
	}
	if g.ExitCode() != 3 {
		t.Fatalf("exit code %d, want 3", g.ExitCode())
	}
	// Signals after the end are no-ops.
	g.Stop(time.Second)
	g.Kill()
}

// An ID is a PID with its start time: another start time is another process.
func TestIDTellsAReusedPIDApart(t *testing.T) {
	self, err := IDOf(os.Getpid())
	if err != nil || !self.Alive() {
		t.Fatalf("own ID %+v %v", self, err)
	}
	other := ID{PID: self.PID, StartTime: self.StartTime + 1}
	if other.Alive() {
		t.Fatal("a PID with another start time counts as the same process")
	}
}
