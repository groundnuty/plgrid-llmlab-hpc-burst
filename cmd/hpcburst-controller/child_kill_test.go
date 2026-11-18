package main

// child.kill (child_test.go) on real processes.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// waitStatusText says how a process ended, in words ("exit status 1" or the
// signal's name), where %v of a syscall.WaitStatus prints a bare number.
func waitStatusText(ws syscall.WaitStatus) string {
	switch {
	case ws.Signaled():
		return "signal: " + ws.Signal().String()
	case ws.Exited():
		return fmt.Sprintf("exit status %d", ws.ExitStatus())
	}
	return fmt.Sprintf("wait status %#x", uint32(ws))
}

// startSh starts `sh -c script` as a test child the way launchChild does: its
// stderr in a file and its Pdeathsig set. It is not reaped in the background
// yet (watch), and nothing ends it at the end of the test.
func startSh(t *testing.T, name, script string) *child {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "child-"+name+".stderr")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close() // the child holds its own descriptor
	c := &child{name: name, stderrPath: logPath, exited: make(chan struct{})}
	c.cmd = exec.Command("sh", "-c", script)
	c.cmd.Stderr = logFile
	c.cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err := c.cmd.Start(); err != nil {
		t.Fatalf("start child %s: %v", name, err)
	}
	return c
}

// TestChildKillWaitsForAProcessThatIsAlreadyDone is #737's logged failure: the
// test's own SIGKILL ended the process, os.Process knew it (Signal returned
// os.ErrProcessDone) before the goroutine that reaps it had closed exited, and
// kill then reported "SIGKILL to child A: os: process already finished". kill
// waits for exited and returns the status; it does not report the signal.
func TestChildKillWaitsForAProcessThatIsAlreadyDone(t *testing.T) {
	c := startSh(t, "A", "exec sleep 60")
	if err := c.cmd.Process.Signal(syscall.SIGKILL); err != nil { // the test's own kill
		t.Fatal(err)
	}
	var exitErr *exec.ExitError
	if err := c.cmd.Wait(); !errors.As(err, &exitErr) { // what watch's goroutine does, up to closing exited
		t.Fatalf("Wait: %v, want the exit error of a killed process", err)
	}
	if err := c.cmd.Process.Signal(syscall.SIGKILL); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("Signal to a process that Wait has returned for: %v, want os.ErrProcessDone; the row's premise does not hold", err)
	}
	time.AfterFunc(300*time.Millisecond, func() { close(c.exited) })
	if !c.running() {
		t.Fatal("exited is closed already; the row's premise does not hold")
	}
	ws := c.kill(t) // fails the test if it reports the finished process
	if !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Errorf("kill returned %s, want the process's own end by SIGKILL", waitStatusText(ws))
	}
}
