package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// startCLI starts `hpcburst args...` without waiting for it.
func startCLI(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = leakyEnv()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd
}

// TestKilledUpIsCleanedByDown: `up` is killed (SIGKILL, so it cannot clean up after itself) while
// k3s is still starting. The record was written before the wait for readiness, so down finds k3s
// and stops it.
func TestKilledUpIsCleanedByDown(t *testing.T) {
	h := newHost(t)
	cmd := startCLI(t, h.upArgs(29220)...)
	waitFor(t, 30*time.Second, "the record to appear", func() bool {
		_, err := os.Stat(filepath.Join(h.dir, "state.json"))
		return err == nil
	})
	st := readState(t, h.dir)
	pid := st.Processes[0].PID
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	if !running(pid) {
		t.Fatal("k3s did not survive the death of the command that started it")
	}
	r := cli(t, h.args("down", "--grace", "20s")...)
	if r.code != exitOK {
		t.Fatalf("down exited %d", r.code)
	}
	if !waitDone(pid) {
		t.Fatalf("k3s (pid %d) still runs after down", pid)
	}
	if rows := listeningInRange(t, launcherPortsLo, launcherPortsHi); len(rows) != 0 {
		t.Errorf("listeners remain: %v", rows)
	}
}

// TestInterruptedUpStopsWhatItStarted: Ctrl-C (SIGINT) during `up` stops k3s and exits non-zero.
func TestInterruptedUpStopsWhatItStarted(t *testing.T) {
	h := newHost(t)
	cmd := startCLI(t, h.upArgs(29220)...)
	waitFor(t, 30*time.Second, "the record to appear", func() bool {
		_, err := os.Stat(filepath.Join(h.dir, "state.json"))
		return err == nil
	})
	pid := readState(t, h.dir).Processes[0].PID
	time.Sleep(1500 * time.Millisecond) // let k3s get going
	if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("an interrupted up exited 0")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("up did not exit after SIGINT")
	}
	if !waitDone(pid) {
		t.Fatalf("k3s (pid %d) still runs after the interrupted up", pid)
	}
	if rows := listeningInRange(t, launcherPortsLo, launcherPortsHi); len(rows) != 0 {
		t.Errorf("listeners remain: %v", rows)
	}
	if r := cli(t, h.args("status")...); r.code != exitStopped {
		t.Errorf("status after an interrupted up: exit %d: %s", r.code, strings.TrimSpace(r.out))
	}
}
