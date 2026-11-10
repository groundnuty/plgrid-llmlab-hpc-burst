package rehearsal

// New. Issue #308: the pool's stop always returns, and what a node started
// stays below the test process (a child subreaper) and is still stopped and
// reaped after its parent has gone. No test here sends a real signal: the
// processes they start end when their input closes.

import (
	"bytes"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// standIn writes a stand-in for sshd that lives until its input (the
// connection) closes, and so ends without any signal.
func standIn(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sshd-stand-in")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nread -r line\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

// An sshd whose pidfd cannot be taken (ENOSYS, EPERM) gets neither TERM nor
// KILL from signalProcs. Close still returns, with that error and the bound's,
// having ended the sshd through its os.Process, here the sink, so the stand-in
// lives on until the test closes its connection.
func TestCloseReturnsWhenAnSSHDCannotBePinned(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.ENOSYS, syscall.EPERM} {
		t.Run(errno.Error(), func(t *testing.T) {
			log, err := os.Create(filepath.Join(t.TempDir(), "sshd.log"))
			if err != nil {
				t.Fatal(err)
			}
			var rec sink
			api := rec.fake(hostProcs())
			pinned := api.open
			sshds := map[int]bool{}
			api.started = func(pid int) { rec.mu.Lock(); sshds[pid] = true; rec.mu.Unlock() }
			api.open = func(pid int) (int, error) {
				rec.mu.Lock()
				own := sshds[pid]
				rec.mu.Unlock()
				if own {
					return 0, errno
				}
				return pinned(pid)
			}
			pool := newPool(standIn(t), os.DevNull, log)
			pool.api, pool.wait = api, 200*time.Millisecond
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			pool.wg.Add(1)
			go pool.serve(ln)
			g := &Gateway{srv: &sshdServer{ln: ln, pool: pool, log: log}}
			c, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			var p *sshdProc
			for deadline := time.Now().Add(waitDeadline); p == nil; time.Sleep(5 * time.Millisecond) {
				pool.mu.Lock()
				if len(pool.procs) > 0 {
					p = pool.procs[0]
				}
				pool.mu.Unlock()
				if p == nil && time.Now().After(deadline) {
					t.Fatal("the stand-in sshd was never stored")
				}
			}
			closed := make(chan error, 1)
			go func() { closed <- g.Close() }()
			var cerr error
			select {
			case cerr = <-closed:
			case <-time.After(waitDeadline):
				c.Close() // the stand-in ends, and Close with it
				<-closed
				t.Fatalf("Close did not return within %v while the sshd could not be pinned", waitDeadline)
			}
			if l := refusedFor(cerr, p.self.pid); !strings.Contains(l, "pidfd_open") || !strings.Contains(l, errno.Error()) {
				t.Errorf("Close: %v; want the sshd's pidfd error (%v)", cerr, errno)
			}
			if cerr == nil || !strings.Contains(cerr.Error(), "still running") {
				t.Errorf("Close: %v; want the bound's error", cerr)
			}
			if _, killed := rec.snapshot(); !slices.Equal(killed, []int{p.self.pid}) {
				t.Errorf("killed through os.Process: %v, want the sshd %d", killed, p.self.pid)
			}
			if sent, _ := rec.snapshot(); len(sent) != 0 {
				t.Errorf("signalled %v, want nothing", sent)
			}
			c.Close()
			select {
			case <-p.done:
			case <-time.After(waitDeadline):
				t.Fatal("the stand-in sshd did not end when its connection closed")
			}
		})
	}
}

// statOf is a process's state letter and row, read for a test.
func statOf(t *testing.T, pid int) (string, procRow) {
	t.Helper()
	state, r, gone, err := readStat(pid)
	if err != nil || gone {
		t.Fatalf("read process %d: gone %v, %v", pid, gone, err)
	}
	return state, r
}

// O1: the test process is a child subreaper, so an orphan of what a node
// started is reparented to it. A process recorded below a parent that has
// since ended is still below the test, and the stop signals it; once it has
// exited, the stop reaps it, by its PID alone. The orphan is real: a cat that
// a shell started and left, which ends when its pipe closes.
func TestAnOrphanOfTheNodeIsStoppedAndReaped(t *testing.T) {
	// What a node does before its first process. The orphan's new parent,
	// below, is what shows it took.
	if err := ensureSubreaper(); err != nil {
		t.Fatal(err)
	}
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pw.Close() })
	// The cat holds no pipe of Output's, or Output would wait for it.
	sh := exec.Command("sh", "-c", "cat <&3 >/dev/null 2>&1 & echo $!")
	sh.ExtraFiles = []*os.File{pr}
	out, err := sh.Output()
	pr.Close()
	if err != nil {
		t.Fatal(err)
	}
	cat, err := strconv.Atoi(string(bytes.TrimSpace(out)))
	if err != nil {
		t.Fatalf("the shell printed %q, want the orphan's PID", out)
	}
	t.Cleanup(func() {
		pw.Close()
		// Reaped here if the stop did not: by its PID alone.
		var ws syscall.WaitStatus
		for end := time.Now().Add(waitDeadline); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
			if got, err := syscall.Wait4(cat, &ws, syscall.WNOHANG, nil); err != nil || got == cat {
				return
			}
		}
	})
	var row procRow
	for end := time.Now().Add(waitDeadline); ; time.Sleep(5 * time.Millisecond) {
		_, r := statOf(t, cat)
		if r.ppid == os.Getpid() {
			row = r
			break
		}
		if time.Now().After(end) {
			t.Fatalf("the orphan %d has parent %d, want this process %d", cat, r.ppid, os.Getpid())
		}
	}
	// As it was recorded while its shell lived: below the shell.
	row.ppid = sh.Process.Pid
	stopWith := func(rec *sink) error {
		pool := &sshdPool{api: rec.fake(hostProcs()), wait: 200 * time.Millisecond}
		done := make(chan struct{})
		close(done) // the shell, the recorded root, is reaped
		pool.procs = []*sshdProc{{self: procRow{pid: sh.Process.Pid, ppid: os.Getpid(), ticks: 1}, done: done, below: map[int]procRow{cat: row}}}
		return pool.stop(nil)
	}
	var live sink
	if err := stopWith(&live); err != nil {
		t.Errorf("stop with the orphan alive: %v", err)
	}
	if sent, _ := live.snapshot(); !slices.Contains(sent, cat) {
		t.Errorf("signalled %v, want the orphan %d, which is below the test", sent, cat)
	}
	pw.Close()
	for end := time.Now().Add(waitDeadline); ; time.Sleep(5 * time.Millisecond) {
		if state, _ := statOf(t, cat); state == "Z" {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("the orphan %d did not exit when its pipe closed", cat)
		}
	}
	var dead sink
	if err := stopWith(&dead); err != nil {
		t.Errorf("stop with the orphan a zombie: %v", err)
	}
	var info unix.Siginfo
	if err := unix.Waitid(unix.P_PID, cat, &info, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT, nil); !errors.Is(err, syscall.ECHILD) {
		t.Errorf("waitid(%d) after the stop: %v; want ECHILD, the zombie reaped by the stop", cat, err)
	}
}
