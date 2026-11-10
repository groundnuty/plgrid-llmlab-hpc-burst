package rehearsal

// New. Issue #308: the pool's stop always returns, signals each process
// before its parent, names what is still alive at its deadline, and reports
// what has left the test's tree without signalling it; a forced command dies
// with its sshd. No test here sends a signal: the processes they start end
// when their input closes, and the one SIGKILL is the kernel's, at a
// stand-in sshd's exit, to the forced command that asked for it.

import (
	"bufio"
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
// having ended the sshd through its os.Process, here the sink, so the
// stand-in lives on until the test closes its connection.
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
			pool := newPool("gateway", standIn(t), os.DevNull, log)
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

// The stop signals each recorded process before its parent, and an sshd
// after everything recorded below it, so it never orphans what it has yet to
// signal: extra first, then the deepest, then the sshd. The start times are
// far above any real process's, so no row names one.
func TestStopSignalsEachProcessBeforeItsParent(t *testing.T) {
	const far = uint64(1) << 50
	var rec sink
	pool := &sshdPool{api: rec.fake(hostProcs()), signal: rec.unchecked, wait: 100 * time.Millisecond}
	done := make(chan struct{})
	close(done) // reaped: nothing more is recorded
	sshd := procRow{pid: 900, ppid: os.Getpid(), ticks: far + 9}
	pool.procs = []*sshdProc{{self: sshd, done: done, below: map[int]procRow{
		901: {pid: 901, ppid: 900, ticks: far + 10}, // the session process
		902: {pid: 902, ppid: 901, ticks: far + 11}, // the forced command
		903: {pid: 903, ppid: 902, ticks: far + 9},  // below it, though it started in an earlier tick
		904: {pid: 904, ppid: 900, ticks: far + 12}, // another child of the sshd
	}}}
	if err := pool.stop([]procRow{{pid: 950, ppid: 1, ticks: far + 20}}); err != nil {
		t.Fatalf("stop: %v", err)
	}
	sent, _ := rec.snapshot()
	want := []int{950, 903, 902, 904, 901, 900}
	if len(sent) < len(want) || !slices.Equal(sent[:len(want)], want) {
		t.Errorf("the TERM pass signalled %v, want %v first: deepest first, the sshd last", sent, want)
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

// orphan starts a cat that a shell starts and leaves, so it is reparented away
// from the test, and that ends when the returned pipe closes. It returns the
// cat's row as it would have been recorded below the shell.
func orphan(t *testing.T) (procRow, *os.File) {
	t.Helper()
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
	for end := time.Now().Add(waitDeadline); ; time.Sleep(5 * time.Millisecond) {
		if _, r := statOf(t, cat); r.ppid != sh.Process.Pid {
			r.ppid = sh.Process.Pid
			return r, pw
		}
		if time.Now().After(end) {
			t.Fatalf("the orphan %d was never reparented", cat)
		}
	}
}

// D4: a recorded process that has left the test's tree (here a real orphan,
// reparented to PID 1 or a subreaper outside the test) is never signalled.
// Close returns an error that names it by command, start time and node, and
// it counts as a leak.
func TestCloseReportsAProcessThatLeftTheTreeAndDoesNotSignalIt(t *testing.T) {
	cat, pw := orphan(t)
	var rec sink
	log, err := os.Create(filepath.Join(t.TempDir(), "sshd.log"))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pool := &sshdPool{name: "gateway", log: log, api: rec.fake(hostProcs()), wait: 100 * time.Millisecond}
	done := make(chan struct{})
	close(done)
	pool.procs = []*sshdProc{{self: procRow{pid: cat.ppid, ppid: os.Getpid(), ticks: 1}, done: done, below: map[int]procRow{cat.pid: cat}}}
	g := &Gateway{srv: &sshdServer{ln: ln, pool: pool, log: log}}
	cerr := g.Close()
	l := refusedFor(cerr, cat.pid)
	for _, w := range []string{"the gateway left process", "(cat, started at tick " + strconv.FormatUint(cat.ticks, 10) + ")", "outside the test's tree", "not signalled"} {
		if !strings.Contains(l, w) {
			t.Errorf("Close: %q; want it to say %q", l, w)
		}
	}
	if sent, _ := rec.snapshot(); slices.Contains(sent, cat.pid) {
		t.Errorf("signalled the orphan %d, which is outside the test's tree", cat.pid)
	}
	if n := rec.leaks.Load(); n != 1 {
		t.Errorf("counted %d leaks, want 1", n)
	}
	pw.Close()
}

// D5: a recorded process still alive at the stop's last deadline, here one
// the sink did not end, is an error that names it, not a nil.
func TestStopNamesAProcessStillAliveAtItsDeadline(t *testing.T) {
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cat := exec.Command("cat")
	cat.Stdin = pr
	if err := cat.Start(); err != nil {
		t.Fatal(err)
	}
	pr.Close()
	t.Cleanup(func() { pw.Close(); _ = cat.Wait() })
	_, r := statOf(t, cat.Process.Pid)
	var rec sink
	pool := &sshdPool{name: "login node", api: rec.fake(hostProcs()), wait: 100 * time.Millisecond}
	done := make(chan struct{})
	close(done)
	gone := procRow{pid: 1 << 22, ppid: os.Getpid(), ticks: uint64(1) << 50} // an sshd that is no more: above pid_max, and no start time matches it
	pool.procs = []*sshdProc{{self: gone, done: done, below: map[int]procRow{r.pid: r}}}
	err = pool.stop(nil)
	if sent, _ := rec.snapshot(); !slices.Contains(sent, r.pid) {
		t.Fatalf("signalled %v, want the cat %d, which is below the test", sent, r.pid)
	}
	if err == nil || !strings.Contains(err.Error(), "still alive") || !strings.Contains(err.Error(), strconv.Itoa(r.pid)+" (cat, started at tick") {
		t.Errorf("stop: %v; want it to name the cat %d as still alive", err, r.pid)
	}
}

// D2: the forced command's guard asks for SIGKILL at its parent's exit, then
// goes on only under an sshd (sshd, or OpenSSH 9.8's sshd-session).
func TestTheRelayGuardTiesTheForcedCommandToItsSSHD(t *testing.T) {
	for _, c := range []struct {
		parent string
		ask    error
		want   int
	}{
		{"sshd", nil, 0},
		{"sshd-session", nil, 0},
		{"systemd", nil, failExit},
		{"bash", nil, failExit},
		{"sshd", syscall.EINVAL, failExit},
	} {
		asked := false
		got := relayGuard(func() error { asked = true; return c.ask }, func() int { return 4242 }, func(pid int) string {
			if pid != 4242 {
				t.Errorf("the command name of %d was read, want the parent 4242's", pid)
			}
			return c.parent
		})
		if got != c.want || !asked {
			t.Errorf("under %q (request: %v): exit %d, asked %v; want %d, asked", c.parent, c.ask, got, asked, c.want)
		}
	}
}

// D2 for real: the test binary as a forced command, under a stand-in sshd (a
// shell started by the name sshd), dies when the stand-in exits, by the
// kernel's SIGKILL that it asked for. That it has passed its guard is shown by
// its dial of the relay socket, which comes after; that it has died, by the
// end of that connection.
func TestAForcedCommandDiesWithItsSSHD(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	sshd := filepath.Join(dir, "sshd")
	if err := os.Symlink(sh, sshd); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(dir, "r.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	hold, release, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer release.Close()
	// The stand-in starts the forced command in the background, says its PID,
	// and waits until the test releases it.
	cmd := exec.Command(sshd, "-c", `"$0" </dev/null >/dev/null 2>&1 3<&- & echo $!; read -r x <&3`, exe)
	cmd.ExtraFiles = []*os.File{hold}
	cmd.Env = append(os.Environ(), EnvRole+"="+RoleRelay, EnvRelay+"="+sock, "SSH_ORIGINAL_COMMAND=srun --jobid=1 true")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	hold.Close()
	t.Cleanup(func() { release.Close(); _ = cmd.Wait() })
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		t.Fatalf("the stand-in sshd said no PID: %v", err)
	}
	forced, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatalf("the stand-in sshd said %q, want a PID", line)
	}
	_ = ln.(*net.UnixListener).SetDeadline(time.Now().Add(waitDeadline))
	conn, err := ln.Accept()
	if err != nil {
		t.Fatalf("the forced command %d never dialed the relay socket: %v", forced, err)
	}
	defer conn.Close() // without the kernel's kill, this ends it: its relay goes away
	if _, r := statOf(t, forced); r.ppid != cmd.Process.Pid {
		t.Fatalf("the forced command %d has parent %d, want the stand-in sshd %d", forced, r.ppid, cmd.Process.Pid)
	}
	release.Close() // the stand-in sshd exits
	_ = conn.SetReadDeadline(time.Now().Add(waitDeadline))
	buf := make([]byte, 64<<10)
	for {
		if _, err := conn.Read(buf); err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatalf("the forced command %d outlived its sshd by %v", forced, waitDeadline)
			}
			break // its end of the connection closed: it has exited
		}
	}
	for end := time.Now().Add(waitDeadline); ; time.Sleep(5 * time.Millisecond) {
		if _, ok := readProcRow(forced); !ok {
			return
		}
		if time.Now().After(end) {
			t.Fatalf("the forced command %d is still alive after its sshd exited", forced)
		}
	}
}
