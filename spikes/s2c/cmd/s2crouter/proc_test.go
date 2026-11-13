package main

import (
	"bufio"
	"errors"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// `s2crouter signal|stop` run as children of this test process, so the processes this test starts are
// "the job's own" (their parent is the launcher's parent); a grandchild is not, unless reached as a
// descendant with -tree.

func startChild(t *testing.T, name string, args ...string) (*exec.Cmd, <-chan error) {
	t.Helper()
	c := exec.Command(name, args...)
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	t.Cleanup(func() { _ = c.Process.Kill() }) // through the child's pidfd (os/exec on Linux)
	return c, done
}

func waitEnd(t *testing.T, done <-chan error, within time.Duration) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(within):
		t.Fatalf("the process did not end within %v", within)
		return nil
	}
}

func signalledBy(err error, sig syscall.Signal) bool {
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		return false
	}
	ws, ok := ee.Sys().(syscall.WaitStatus)
	return ok && ws.Signaled() && ws.Signal() == sig
}

func TestSignalReachesTheJobsOwnChild(t *testing.T) {
	c, done := startChild(t, "sleep", "300")
	if out, err := self("signal", "-sig", "TERM", strconv.Itoa(c.Process.Pid)).CombinedOutput(); err != nil {
		t.Fatalf("signal: %v %s", err, out)
	}
	if err := waitEnd(t, done, 10*time.Second); !signalledBy(err, syscall.SIGTERM) {
		t.Fatalf("the child ended %v, want by SIGTERM", err)
	}
}

func TestSignalRefusesAProcessThatIsNotTheJobsOwn(t *testing.T) {
	// sh is our child; its sleep is a grandchild, not the job's own as a root.
	c := exec.Command("sh", "-c", "sleep 300 & echo $!; wait")
	stdout, err := c.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	t.Cleanup(func() { _ = c.Process.Kill() })
	sc := bufio.NewScanner(stdout)
	if !sc.Scan() {
		t.Fatal("no PID from sh")
	}
	grand, err := strconv.Atoi(strings.TrimSpace(sc.Text()))
	if err != nil {
		t.Fatal(err)
	}
	out, err := self("signal", "-sig", "TERM", strconv.Itoa(grand)).CombinedOutput()
	if err == nil || !strings.Contains(string(out), "not the job's own") {
		t.Fatalf("a grandchild as root: %v %s, want a refusal", err, out)
	}
	if _, _, _, err := procStat(grand); err != nil {
		t.Fatalf("the refused grandchild is gone: %v", err)
	}
	// As a descendant of the job's own sh it is reached, with -tree.
	if out, err := self("stop", "-grace", "5s", strconv.Itoa(c.Process.Pid)).CombinedOutput(); err != nil {
		t.Fatalf("stop: %v %s", err, out)
	}
	waitEnd(t, done, 10*time.Second)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if st, _, _, err := procStat(grand); err != nil || st == 'Z' {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the grandchild outlived stop of its parent's tree")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestStopKillsWhatIgnoresTerm(t *testing.T) {
	c := exec.Command("sh", "-c", `trap "" TERM; echo ready; while :; do sleep 1; done`)
	stdout, err := c.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	t.Cleanup(func() { _ = c.Process.Kill() })
	if sc := bufio.NewScanner(stdout); !sc.Scan() || sc.Text() != "ready" {
		t.Fatal("sh did not report its trap set")
	}
	t0 := time.Now()
	if out, err := self("stop", "-grace", "1s", strconv.Itoa(c.Process.Pid)).CombinedOutput(); err != nil {
		t.Fatalf("stop: %v %s", err, out)
	}
	if err := waitEnd(t, done, 15*time.Second); !signalledBy(err, syscall.SIGKILL) {
		t.Fatalf("sh, which ignores TERM, ended %v, want by SIGKILL after the grace", err)
	}
	if time.Since(t0) > 12*time.Second {
		t.Fatalf("stop took %v with a 1 s grace", time.Since(t0))
	}
}
