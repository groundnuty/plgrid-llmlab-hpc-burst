package rehearsal

// Issue #556: the process watcher of the rig's tests recorded a process's name at its first sample and never again, so a child sampled between
// its fork and its exec, which still has its parent's name, was recorded under that name: on main's CI it saw "sshd rehearsal.test" and never the
// forward's "ssh", and failed TestCloseLeavesNoProcess. The test below holds a child in that window until the watcher has recorded it, lets it exec
// a program named ssh, and asks the watcher what it saw, with no wait that guesses at a window: each step waits on what the watcher or /proc shows.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The pre-exec child: this test binary run with envPreExec set to a program, which waits, under this binary's name, for the file named by
// envPreExecGo, then execs the program with the file named by envPreExecDone as its argument.
const (
	envPreExec     = "HB_REHEARSAL_TEST_PREEXEC"
	envPreExecGo   = "HB_REHEARSAL_TEST_PREEXEC_GO"
	envPreExecDone = "HB_REHEARSAL_TEST_PREEXEC_DONE"
)

// preExecChild is TestMain's first step: in the pre-exec child it never returns.
func preExecChild() {
	target := os.Getenv(envPreExec)
	if target == "" {
		return
	}
	deadline := time.Now().Add(waitDeadline)
	for {
		if _, err := os.Stat(os.Getenv(envPreExecGo)); err == nil {
			break
		}
		if time.Now().After(deadline) {
			os.Exit(3)
		}
		time.Sleep(5 * time.Millisecond)
	}
	err := syscall.Exec(target, []string{target, os.Getenv(envPreExecDone)}, os.Environ())
	os.Stderr.WriteString("rehearsal: the pre-exec child could not exec: " + err.Error() + "\n")
	os.Exit(4)
}

// waitWatch waits for cond, failing the test with what at the deadline.
func waitWatch(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitDeadline)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %s", what, waitDeadline)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestTheWatcherRecordsAChildByTheNameItExecsTo(t *testing.T) {
	dir, err := os.MkdirTemp("", "rw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	// A program named ssh: its process's name is ssh once it is exec'd (the kernel names a process by the file it execs), and it ends when
	// told to.
	ssh, goFile, doneFile := filepath.Join(dir, "ssh"), filepath.Join(dir, "go"), filepath.Join(dir, "done")
	if err := os.WriteFile(ssh, []byte("#!/bin/sh\nwhile [ ! -e \"$1\" ]; do sleep 0.01; done\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	w := startWatch(t, dir)
	w.wantSSH = false // the rig's own check (sshd and ssh both seen) is not this test's; it asks the watcher directly
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), envPreExec+"="+ssh, envPreExecGo+"="+goFile, envPreExecDone+"="+doneFile)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	exited := false
	t.Cleanup(func() {
		if !exited {
			_ = os.WriteFile(doneFile, nil, 0o600)
			_ = os.WriteFile(goFile, nil, 0o600)
			_ = cmd.Wait()
		}
	})
	comm := func() string {
		b, _ := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/comm")
		return strings.TrimSpace(string(b))
	}
	preExec := comm()
	if preExec == "" || preExec == "ssh" {
		t.Fatalf("the child's name before its exec is %q, want this binary's", preExec)
	}
	// 1. The watcher records the child while it is still this binary, before its exec.
	waitWatch(t, "the watcher records the child before its exec", func() bool { return w.recorded(pid) })
	if got := comm(); got != preExec {
		t.Fatalf("the child is %q, not %q, when the watcher has recorded it: it exec'd too early", got, preExec)
	}
	// 2. The child execs ssh.
	if err := os.WriteFile(goFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitWatch(t, "the child execs ssh", func() bool { return comm() == "ssh" })
	// 3. Two more samples while it is ssh, then it ends.
	n := w.samplesTaken()
	waitWatch(t, "two samples after the exec", func() bool { return w.samplesTaken() >= n+2 })
	if err := os.WriteFile(doneFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("the child: %v", err)
	}
	exited = true
	w.finish()
	if !w.saw("ssh") {
		t.Errorf("the watcher never recorded the child as ssh, the name it exec'd to: it keeps the name it first saw, %q", preExec)
	}
	if !w.saw(preExec) {
		t.Errorf("the watcher did not record the child's name before its exec, %q: the window was not caught", preExec)
	}
}

// record keeps a process by its PID and its start time: a PID the kernel has given to another process is another record, and both names are
// kept; and one process sampled twice under two names keeps both, the latest as its name. Rows and names are given, not read.
func TestTheWatcherKeepsEachProcessByItsPIDAndStartTime(t *testing.T) {
	w := &watch{seen: map[procKey]seenProc{}}
	names := map[procKey]string{}
	comm := func(r procRow) func(int) string { return func(int) string { return names[procKey{r.pid, r.ticks}] } }
	first, reused := procRow{pid: 4242, ppid: 1, ticks: 100}, procRow{pid: 4242, ppid: 1, ticks: 200}
	names[procKey{first.pid, first.ticks}] = "rehearsal.test"
	w.record([]procRow{first}, comm(first))
	names[procKey{first.pid, first.ticks}] = "ssh"
	w.record([]procRow{first}, comm(first))
	names[procKey{reused.pid, reused.ticks}] = "sshd"
	w.record([]procRow{reused}, comm(reused))
	if len(w.seen) != 2 {
		t.Fatalf("%d records, want 2: the process and the one that reused its PID", len(w.seen))
	}
	if p := w.seen[procKey{4242, 100}]; p.comm != "ssh" || !p.comms["rehearsal.test"] || !p.comms["ssh"] {
		t.Errorf("the first process is %q with names %v, want ssh, having been rehearsal.test", p.comm, p.comms)
	}
	if p := w.seen[procKey{4242, 200}]; p.row != reused || p.comm != "sshd" {
		t.Errorf("the process that reused the PID is %+v as %q, want its own row as sshd", p.row, p.comm)
	}
	// A name that cannot be read (the process has gone) keeps what was read before.
	names[procKey{first.pid, first.ticks}] = ""
	w.record([]procRow{first}, comm(first))
	if p := w.seen[procKey{4242, 100}]; p.comm != "ssh" {
		t.Errorf("an unreadable name replaced the process's name: %q", p.comm)
	}
}
