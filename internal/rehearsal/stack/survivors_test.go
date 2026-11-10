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
	"strconv"
	"strings"
	"testing"
	"time"
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

// procsOf is each Proc as name=pid, with a + for one the launcher records.
func procsOf(procs []Proc) string {
	var b strings.Builder
	for _, p := range procs {
		b.WriteString(" " + p.Name + "=" + strconv.Itoa(p.PID))
		if p.Recorded {
			b.WriteString("+")
		}
	}
	return strings.TrimSpace(b.String())
}

// Gone tells a live process from one that ended, by its PID and start time, on a child this test starts and stops by its PID (the
// rehearsal's TestProcIDGone, moved here with the helper it tests, issue #251).
func TestProcGone(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() }) // ended already when the test got that far
	pid := cmd.Process.Pid
	start, ok := procStart(pid)
	if !ok {
		t.Fatalf("procStart(%d) of a running child: not found", pid)
	}
	p := Proc{Name: "sleep", PID: pid, start: start}
	if p.Gone() {
		t.Fatalf("a running child %d counts as gone", pid)
	}
	if (Proc{Name: "sleep", PID: pid, start: start + 1}).Gone() == false {
		t.Errorf("a process under the same PID with another start time counts as the same one")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	// A zombie until reaped, which holds nothing: the kill is delivered asynchronously, so this waits on the process's state.
	WaitFor(t, 30*time.Second, "the killed, unreaped child to count as gone", p.Gone)
	_ = cmd.Wait()
	if !p.Gone() {
		t.Errorf("a reaped child %d does not count as gone", pid)
	}
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
	if got, want := procsOf(procs), fmt.Sprintf("k3s=%d+ front=%d", a, b); got != want {
		t.Fatalf("Processes = %s, want %s", got, want)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:29893")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	// While both run and the port listens, all three are left, after one look (no wait, only the port read's time).
	want := []string{fmt.Sprintf("k3s=%d still runs", a), fmt.Sprintf("front=%d still runs", b), "port 29893 still listens"}
	if left := Left(procs, []int{29893}, time.Now().Add(LeftPortRead)); !slices.Equal(left, want) {
		t.Errorf("Left = %q, want %q", left, want)
	}
	// Another process under the PID is not the one status named.
	if other := (Proc{Name: "k3s", PID: a, start: procs[0].start + 1}); !other.Gone() {
		t.Errorf("k3s=%d with another start time is not gone", a)
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
	if left := Left(procs, []int{29893}, time.Now().Add(WaitDeadline)); len(left) != 0 {
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

// Left returns by the time it is given, with a process that never ends, so that its caller's time for what follows (alpha's Close: the
// cancel of the run's jobs and the clean-ups) is kept: the process is left, and the port read still happens within that time. The bound
// is checked on the clock, with a second's slack for one pause of the wait and the scheduler, since Left's sleeps end at its time.
func TestLeftEndsByItsTime(t *testing.T) {
	c := sleeper(t)
	start, ok := procStart(c.Process.Pid)
	if !ok {
		t.Fatal("the sleeper does not run")
	}
	procs := []Proc{{Name: "k3s", PID: c.Process.Pid, Recorded: true, start: start}}
	by := time.Now().Add(LeftPortRead + 500*time.Millisecond)
	left := Left(procs, []int{29893}, by)
	if over := time.Since(by); over > time.Second {
		t.Errorf("Left returned %s after its time", over)
	}
	if want := []string{fmt.Sprintf("k3s=%d still runs", c.Process.Pid)}; !slices.Equal(left, want) {
		t.Errorf("Left = %q, want %q: the process, and a port read that succeeded", left, want)
	}
}

// MissingComponents names each part of the stack a list of its processes lacks: the four supervised entries and a recorded process.
func TestMissingComponents(t *testing.T) {
	whole := []Proc{{Name: "k3s", Recorded: true}, {Name: "supervisor"}, {Name: "controller"}, {Name: "front"}, {Name: "frps"}}
	if m := MissingComponents(whole); len(m) != 0 {
		t.Errorf("the whole stack lacks %q", m)
	}
	for i, p := range whole {
		part := slices.Delete(slices.Clone(whole), i, i+1)
		want := "running " + p.Name
		if p.Recorded {
			want = "recorded process of ours (k3s)"
		}
		if m := MissingComponents(part); !slices.Equal(m, []string{want}) {
			t.Errorf("without %s: %q, want %q", p.Name, m, want)
		}
	}
	if m := MissingComponents(nil); len(m) != 5 {
		t.Errorf("no process at all lacks %q, want all five", m)
	}
}
