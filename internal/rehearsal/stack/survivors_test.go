package stack

// Processes and Left (survivors.go) on a double of hpcburst status, two processes of this test's own, and a listener on issue #303's
// block (29880-29899): 29893 alone, since internal/alpha's test of the same check reads 29880-29892.

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// statusDouble is an hpcburst that answers status --state-dir <state> --json --no-api with out and exit code rc, and nothing else.
func statusDouble(t *testing.T, state, out string, rc int) string {
	t.Helper()
	dir := t.TempDir()
	answer := filepath.Join(dir, "status.json")
	if err := os.WriteFile(answer, []byte(out), 0o600); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "hpcburst")
	script := fmt.Sprintf("#!/bin/sh\n[ \"$*\" = \"status --state-dir %s --json --no-api\" ] || exit 2\ncat %s\nexit %d\n", state, answer, rc)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

// sleeper is a process of this test's own that runs until it is killed, and is reaped once it ends.
func sleeper(t *testing.T) *exec.Cmd {
	t.Helper()
	c := exec.Command("sleep", "600")
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_ = c.Wait() // its exit is the kill's
		close(done)
	}()
	t.Cleanup(func() {
		_ = c.Process.Kill() // it may have ended already
		<-done
	})
	return c
}

func TestProcessesAndLeft(t *testing.T) {
	sa, sb := sleeper(t), sleeper(t)
	a, b := sa.Process.Pid, sb.Process.Pid
	state := filepath.Join(t.TempDir(), "st")
	// k3s is ours; a record judged gone is not; the supervisor is k3s's PID, one process; the front runs; frps does not.
	out := fmt.Sprintf(`{"processes":[{"name":"k3s","pid":%d,"verdict":"ours"},{"name":"old","pid":%d,"verdict":"gone"}],`+
		`"supervisor":{"running":true,"pid":%d},"controllers":[{"name":"front","running":true,"pid":%d},{"name":"frps","running":false}]}`, a, b, a, b)
	h := &Host{T: t, B: Binaries{HPCBurst: statusDouble(t, state, out, 0)}, State: state, Env: os.Environ()}
	procs, err := h.Processes()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(procs), fmt.Sprintf("[k3s=%d front=%d]", a, b); got != want {
		t.Fatalf("Processes = %s, want %s", got, want)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:29893")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	// While both run and the port listens, all three are left, after one look (no wait).
	want := []string{fmt.Sprintf("k3s=%d still runs", a), fmt.Sprintf("front=%d still runs", b), "port 29893 still listens"}
	if left := Left(procs, []int{29893}, 0); !slices.Equal(left, want) {
		t.Errorf("Left = %q, want %q", left, want)
	}
	// Another process under the PID is not the one status named.
	if other := (Proc{Name: "k3s", PID: a, start: procs[0].start + 1}); !other.Gone() {
		t.Errorf("%s with another start time is not gone", other)
	}
	// Once both end and the listener closes, nothing is left: Left waits for the processes' end, on their state.
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []*exec.Cmd{sa, sb} {
		if err := c.Process.Kill(); err != nil {
			t.Fatal(err)
		}
	}
	if left := Left(procs, []int{29893}, WaitDeadline); len(left) != 0 {
		t.Errorf("Left = %q after both ended, want nothing", left)
	}
}

// A status that names a process that does not run, or that cannot be read, is an error: a list of what ran before down is then no
// baseline for what is left after it.
func TestProcessesFailsOnWhatItCannotRead(t *testing.T) {
	state := filepath.Join(t.TempDir(), "st")
	const noSuchPID = 1 << 30 // above Linux's largest pid_max, 2^22
	for name, c := range map[string]struct {
		out  string
		rc   int
		want string
	}{
		"a process that does not run": {fmt.Sprintf(`{"processes":[{"name":"k3s","pid":%d,"verdict":"ours"}]}`, noSuchPID), 0, fmt.Sprintf("k3s=%d does not run", noSuchPID)},
		"a status that is not JSON":   {"no state here\n", 1, "hpcburst status --json (exit status 1): "},
	} {
		h := &Host{T: t, B: Binaries{HPCBurst: statusDouble(t, state, c.out, c.rc)}, State: state, Env: os.Environ()}
		if _, err := h.Processes(); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error with %q", name, err, c.want)
		}
	}
}
