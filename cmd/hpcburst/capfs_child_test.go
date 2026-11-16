package main

// The sweep of dead capfs mounts against real ones (issue #648; the sweep is capfs_sweep_test.go's). A capfs server runs in a CHILD process, the
// test binary run again with HPCBURST_TEST_CAPFS_SERVER set (TestMain), so that the test can kill it the way a killed test is killed: with SIGKILL,
// through a pidfd, which runs no clean-up. What is shown, on real FUSE mounts: the killed server's mount stays, with a dead endpoint (stat fails
// with ENOTCONN); the next newCapVolume sweeps it; a live mount owned by another child is not touched; and what fusermount3's auto_unmount does
// through go-fuse.
//
// Each test cleans up its own children and mounts without the code under test: it closes the child's stdin (which makes a live child unmount and
// exit), kills a child that does not, and unmounts its own mount point directly if it is still there, so that a broken sweep leaves nothing behind.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const (
	// capfsServerEnv makes the test binary a capfs server instead of running tests: the value is the directory it makes its volume in. It serves
	// until its stdin is closed (the test is done, or the test process is gone), then unmounts and exits; a SIGKILL ends it with no clean-up.
	capfsServerEnv = "HPCBURST_TEST_CAPFS_SERVER"
	// capfsAutoUnmountEnv, set to 1, mounts the child's volume with fusermount3's auto_unmount.
	capfsAutoUnmountEnv = "HPCBURST_TEST_CAPFS_AUTO_UNMOUNT"

	capChildLimit = 1 << 20
)

// capfsServerMain is the child process: it mounts a volume under dir and serves it until its stdin closes.
func capfsServerMain(dir string) {
	v, err := mountCapVolume(dir, capChildLimit, os.Getenv(capfsAutoUnmountEnv) == "1")
	if err != nil {
		fmt.Fprintln(os.Stderr, "capfs server:", err)
		os.Exit(3)
	}
	fmt.Println("mounted", v.mount)
	_, _ = io.Copy(io.Discard, os.Stdin) // returns when the parent closes the pipe, or dies
	if err := v.server.Unmount(); err != nil {
		fmt.Fprintln(os.Stderr, "capfs server: unmount:", err)
		os.Exit(4)
	}
	v.server.Wait()
	os.Exit(0)
}

// capWaitFor polls cond every 20 ms until it holds, failing the test after d with what it waited for.
func capWaitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(d); !cond(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out after %s waiting for %s", d, what)
		}
	}
}

// capListed says whether the mountinfo lists a capfs mount at the mount point.
func capListed(point string) bool {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	for _, m := range parseCapMounts(string(data)) {
		if m.point == point {
			return true
		}
	}
	return false
}

// capErrno is the stat of a mount point as the test itself reads it, not through the sweep: 0 when it answers, else the errno.
func capErrno(point string) syscall.Errno {
	var st syscall.Stat_t
	if err := syscall.Stat(point, &st); err != nil {
		var errno syscall.Errno
		if errors.As(err, &errno) {
			return errno
		}
		return syscall.EIO
	}
	return 0
}

// capChild is a capfs server in a child process.
type capChild struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	out     *bytes.Buffer
	dir     string
	mount   string
	done    chan struct{} // closed when the child has been waited for
	waitErr error
}

// launchCapServer starts the child and returns at once, with the test's clean-up registered: it closes the child's stdin, kills it if it does not
// stop, and unmounts its mount point itself if it is still listed.
func launchCapServer(t *testing.T, autoUnmount bool) *capChild {
	t.Helper()
	dir, err := os.MkdirTemp("", "cs")
	if err != nil {
		t.Fatal(err)
	}
	if real, err := filepath.EvalSymlinks(dir); err == nil {
		dir = real // mountinfo names the mount point by its real path
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	c := &capChild{dir: dir, mount: filepath.Join(dir, "mnt"), out: &bytes.Buffer{}, done: make(chan struct{})}
	c.cmd = exec.Command(self)
	c.cmd.Env = append(os.Environ(), capfsServerEnv+"="+dir)
	if autoUnmount {
		c.cmd.Env = append(c.cmd.Env, capfsAutoUnmountEnv+"=1")
	}
	c.cmd.Stdout, c.cmd.Stderr = c.out, c.out
	if c.stdin, err = c.cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	if err := c.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { c.waitErr = c.cmd.Wait(); close(c.done) }()
	t.Cleanup(func() {
		c.stdin.Close()
		select {
		case <-c.done:
		case <-time.After(15 * time.Second):
			t.Errorf("the capfs child did not stop when its stdin closed; killing it. Its output: %s", c.out.String())
			c.sigkill()
			<-c.done
		}
		if capListed(c.mount) { // its own mount, left dead by a kill: unmounted here directly, whatever the sweep did or did not do
			if out, err := exec.Command("fusermount3", "-u", c.mount).CombinedOutput(); err != nil {
				t.Errorf("the test's own unmount of %s failed: %v: %s", c.mount, err, out)
			}
		}
		if err := os.RemoveAll(c.dir); err != nil {
			t.Errorf("removing %s: %v", c.dir, err)
		}
	})
	return c
}

// startCapServer launches the child and returns once its mount is listed and answers a write.
func startCapServer(t *testing.T, autoUnmount bool) *capChild {
	t.Helper()
	c := launchCapServer(t, autoUnmount)
	capWaitFor(t, 20*time.Second, "the child's mount to be listed and answer", func() bool {
		select {
		case <-c.done:
			t.Fatalf("the capfs child ended before its mount was ready: %v: %s", c.waitErr, c.out.String())
		default:
		}
		if !capListed(c.mount) || capErrno(c.mount) != 0 {
			return false
		}
		return os.WriteFile(filepath.Join(c.mount, ".ready"), []byte("x"), 0o600) == nil
	})
	return c
}

// sigkill sends SIGKILL to the child through a pidfd.
func (c *capChild) sigkill() {
	fd, err := unix.PidfdOpen(c.cmd.Process.Pid, 0)
	if err != nil {
		return // gone already
	}
	defer unix.Close(fd)
	_ = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
}

// kill ends the child with SIGKILL through its pidfd, which runs none of its clean-up, and waits for it to be reaped.
func (c *capChild) kill(t *testing.T) {
	t.Helper()
	c.sigkill()
	select {
	case <-c.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("the capfs child did not end after SIGKILL")
	}
	var ee *exec.ExitError
	if !errors.As(c.waitErr, &ee) || ee.ExitCode() != -1 {
		t.Fatalf("the capfs child ended %v, want killed by a signal", c.waitErr)
	}
}

// write writes a file through the child's mount and reads it back: the mount is live.
func (c *capChild) write(t *testing.T, name, content string) {
	t.Helper()
	p := filepath.Join(c.mount, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", p, err)
	}
	if b, err := os.ReadFile(p); err != nil || string(b) != content {
		t.Fatalf("reading %s back: %q, %v", p, b, err)
	}
}

// killLeavingADeadMount starts a child, shows its mount live, kills it, and returns once the test has seen the state the issue describes: the mount is
// still listed, and its endpoint is dead (a stat fails with ENOTCONN).
func killLeavingADeadMount(t *testing.T) *capChild {
	t.Helper()
	c := startCapServer(t, false)
	c.write(t, "before-the-kill", "served")
	c.kill(t)
	capWaitFor(t, 10*time.Second, "the killed child's mount to answer ENOTCONN", func() bool { return capErrno(c.mount) == syscall.ENOTCONN })
	if !capListed(c.mount) {
		t.Fatalf("the mount of the killed child is not listed: the state of the issue, a mount left behind, was not reached")
	}
	return c
}

// The state of the issue, and its repair: a capfs server killed with SIGKILL leaves its mount behind with a dead endpoint, and the next newCapVolume
// sweeps it, so that a killed store-full test no longer leaves a stale mount for later runs and clean-ups.
func TestTheNextCapVolumeSweepsAMountLeftByAKilledServer(t *testing.T) {
	dead := killLeavingADeadMount(t)
	if err := os.WriteFile(filepath.Join(dead.mount, "x"), []byte("x"), 0o600); !errors.Is(err, syscall.ENOTCONN) {
		t.Fatalf("a write through the killed server's mount: %v, want ENOTCONN", err)
	}
	v := newCapVolume(t, capChildLimit)
	if capListed(dead.mount) {
		t.Errorf("%s, the mount of the killed server, is still listed after newCapVolume", dead.mount)
	}
	if e := capErrno(dead.mount); e == syscall.ENOTCONN {
		t.Errorf("%s still answers ENOTCONN after newCapVolume", dead.mount)
	}
	if err := os.WriteFile(filepath.Join(v.mount, "new"), []byte("new"), 0o600); err != nil {
		t.Errorf("the new volume does not work: %v", err)
	}
}

// A live mount owned by another process is not touched by a new volume: it is still listed, still answers, and keeps what it held.
func TestTheNextCapVolumeLeavesAnotherProcessesLiveMountAlone(t *testing.T) {
	live := startCapServer(t, false)
	live.write(t, "kept", "the other test's data")
	v := newCapVolume(t, capChildLimit)
	if !capListed(live.mount) || capErrno(live.mount) != 0 {
		t.Fatalf("the live mount of another process was touched: listed %t, stat errno %v", capListed(live.mount), capErrno(live.mount))
	}
	if b, err := os.ReadFile(filepath.Join(live.mount, "kept")); err != nil || string(b) != "the other test's data" {
		t.Errorf("the live mount lost its data: %q, %v", b, err)
	}
	live.write(t, "after", "still served")
	if err := os.WriteFile(filepath.Join(v.mount, "new"), []byte("new"), 0o600); err != nil {
		t.Errorf("the new volume does not work: %v", err)
	}
}

// Both at once: with one killed server's mount dead and another's live, the sweep takes the dead one and not the live one, and logs each decision.
func TestTheSweepTakesTheDeadMountAndLeavesTheLiveOneOfAnotherChild(t *testing.T) {
	dead := killLeavingADeadMount(t)
	live := startCapServer(t, false)
	live.write(t, "kept", "served")
	var logged []string
	swept := sweepDeadCapMounts(realSweepEnv(func(f string, a ...any) { logged = append(logged, fmt.Sprintf(f, a...)) }))
	// Any other dead capfs mount of this user's on the machine is swept too, so the dead one is looked for in the list, and the live one must not be in it.
	var sweptDead, sweptLive bool
	for _, p := range swept {
		sweptDead = sweptDead || p == dead.mount
		sweptLive = sweptLive || p == live.mount
	}
	if !sweptDead || sweptLive {
		t.Errorf("swept %v: the dead mount %s swept %t, the live mount %s swept %t, want true and false", swept, dead.mount, sweptDead, live.mount, sweptLive)
	}
	if capListed(dead.mount) {
		t.Errorf("the dead mount %s is still listed", dead.mount)
	}
	if !capListed(live.mount) || capErrno(live.mount) != 0 {
		t.Errorf("the live mount %s was touched: listed %t, stat errno %v", live.mount, capListed(live.mount), capErrno(live.mount))
	}
	live.write(t, "after", "still served")
	all := strings.Join(logged, "\n")
	if !strings.Contains(all, "unmounted "+dead.mount) || !strings.Contains(all, live.mount+" answers") {
		t.Errorf("the sweep did not log both decisions: %q", logged)
	}
}
