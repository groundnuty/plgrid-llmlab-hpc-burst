package main

// The mount, its watcher and the sweep against real FUSE mounts (issue #648; the sweep is capfs_sweep_test.go's, the mount and the
// watcher capfs_mount_test.go's). A capfs server runs in a CHILD process, the test binary run again with HPCBURST_TEST_CAPFS_SERVER set
// (TestMain), so that the test can end it the way a killed test ends: SIGKILL through a pidfd, which runs no clean-up. What is shown, on
// real mounts: a killed server's mount is unmounted by its watcher; one with no watcher is left with a dead endpoint, and the next
// newCapVolume sweeps it; a live mount of another process is touched by neither; a wedged one (open, unserved) is reported, with its
// connection, and left alone, promptly; and neither the mount's descriptor nor the watcher's pipe reaches a process started later.
//
// No test touches a mount without a bound: every stat, read and write through one runs in a goroutine that is abandoned at its deadline,
// and a directory is removed only once nothing is mounted under it (capRemoveDir). A child ends however the test ends: it exits when its
// stdin closes, which it watches from before it mounts, and the kernel kills it when the test binary ends (testenv.StartDyingWithParent).
// Each test cleans up its children and mounts itself: it closes the child's stdin, kills a child that does not exit, and unmounts its
// own mount point if it is left dead, so a broken sweep or watcher leaves nothing behind.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

const (
	// capfsServerEnv makes the test binary a capfs server instead of running tests: the value is the directory it makes its mount in.
	// It serves until its stdin closes, then unmounts and exits; a SIGKILL ends it with no clean-up.
	capfsServerEnv = "HPCBURST_TEST_CAPFS_SERVER"
	// capfsServerWatchEnv, set to 1, gives the child's volume a watcher, as newCapVolume's has.
	capfsServerWatchEnv = "HPCBURST_TEST_CAPFS_WITH_WATCHER"
	// capfsWedgeEnv, set to 1, makes the child mount (named capFsWedgeName) and then hold the descriptor without ever serving it: the
	// mount is open, and a stat of it does not return, until the child exits, which leaves it dead.
	capfsWedgeEnv = "HPCBURST_TEST_CAPFS_WEDGE"

	capChildLimit = 1 << 20
	// capChildBound bounds each wait of these tests: on a child, on its mount, and on a file operation through a mount.
	capChildBound = 20 * time.Second
)

// capfsServerMain is the child process: it mounts under dir and serves until its stdin closes. It watches stdin from the start, so that
// a parent that is gone ends it even while it mounts.
func capfsServerMain(dir string) {
	stdinClosed := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, os.Stdin); close(stdinClosed) }()
	if os.Getenv(capfsWedgeEnv) == "1" {
		point := filepath.Join(dir, "mnt")
		if err := os.Mkdir(point, 0o700); err != nil {
			fmt.Fprintln(os.Stderr, "capfs server:", err)
			os.Exit(3)
		}
		conn, err := fusermountCapfs(point, capFsWedgeName)
		if err != nil {
			fmt.Fprintln(os.Stderr, "capfs server:", err)
			os.Exit(3)
		}
		fmt.Println("mounted", point, "wedged")
		<-stdinClosed
		runtime.KeepAlive(conn) // exiting closes the descriptor, which ends the connection
		os.Exit(0)
	}
	v, err := mountCapVolume(dir, capChildLimit, capFsName, os.Getenv(capfsServerWatchEnv) == "1")
	if err != nil {
		fmt.Fprintln(os.Stderr, "capfs server:", err)
		os.Exit(3)
	}
	fmt.Println("mounted", v.mount)
	<-stdinClosed
	if err := v.unmount(capChildBound); err != nil {
		fmt.Fprintln(os.Stderr, "capfs server: unmount:", err)
		os.Exit(4)
	}
	os.Exit(0)
}

// capBounded runs op in a goroutine and waits at most d for it. An op that has not returned is abandoned, and the error says so; its
// result goes only through the channel, so an op that returns late writes nothing the caller reads.
func capBounded[T any](d time.Duration, what string, op func() (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	res := make(chan result, 1)
	go func() { v, err := op(); res <- result{v, err} }()
	select {
	case r := <-res:
		return r.v, r.err
	case <-time.After(d):
		var zero T
		return zero, fmt.Errorf("%s did not return within %s", what, d)
	}
}

// capDo is capBounded for an op with no result.
func capDo(d time.Duration, what string, op func() error) error {
	_, err := capBounded(d, what, func() (struct{}, error) { return struct{}{}, op() })
	return err
}

// capStatErrno is the stat of a mount point as the test itself reads it, not through the sweep, bounded by d: 0 when it answers, else
// the errno, and false when it did not return in time.
func capStatErrno(point string, d time.Duration) (syscall.Errno, bool) {
	errno, err := capBounded(d, "a stat of "+point, func() (syscall.Errno, error) {
		var st syscall.Stat_t
		err := syscall.Stat(point, &st)
		var errno syscall.Errno
		if err != nil && !errors.As(err, &errno) {
			errno = syscall.EIO
		}
		return errno, nil
	})
	return errno, err == nil
}

// fileFd is f's descriptor, read without os.File.Fd, which would switch a non-blocking file to blocking.
func fileFd(t *testing.T, f *os.File) int {
	t.Helper()
	rc, err := f.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	fd := -1
	if err := rc.Control(func(u uintptr) { fd = int(u) }); err != nil {
		t.Fatal(err)
	}
	return fd
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

// capListed says whether the mountinfo lists a capfs mount, of either name, at the mount point.
func capListed(point string) bool {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	for _, m := range parseCapMounts(string(data), capFsName, capFsWedgeName) {
		if m.point == point {
			return true
		}
	}
	return false
}

// capMountAt is the capfs mount listed at the mount point, read from the mountinfo by the test.
func capMountAt(t *testing.T, point string) capMount {
	t.Helper()
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range parseCapMounts(string(data), capFsName, capFsWedgeName) {
		if m.point == point {
			return m
		}
	}
	t.Fatalf("no capfs mount is listed at %s", point)
	return capMount{}
}

// capUnmountIfDead is the tests' own clean-up of a mount they made: it waits up to d for the mount to go, and unmounts it while it is
// dead, with a plain unmount, without the sweep. A mount that answers, or does not, is reported and left; the result says whether the
// mount is gone.
func capUnmountIfDead(t *testing.T, point string, d time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for capListed(point) {
		if errno, answered := capStatErrno(point, capChildBound); answered && errno == syscall.ENOTCONN {
			if err := capUnmount(point, capUnmountTimeout); err != nil && capListed(point) {
				t.Errorf("the test's own unmount of its dead mount: %v", err)
				return false
			}
			continue
		} else if time.Now().After(deadline) {
			t.Errorf("the mount %s is still listed, and is not dead (stat answered %t, errno %v): it is left", point, answered, errno)
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
	return true
}

// capFuseOrSkip mounts a volume in this process, as the store-full tests do, so that a test of real mounts skips (or, with
// HPCBURST_REQUIRE_INTEGRATION=1, fails) with newCapVolume's reason where FUSE cannot be mounted.
func capFuseOrSkip(t *testing.T) { t.Helper(); newCapVolume(t, capChildLimit) }

// capChildMode is how a child mounts: with a watcher, and wedged (mounted and never served).
type capChildMode struct{ watch, wedge bool }

// capChild is a capfs server in a child process.
type capChild struct {
	mode  capChildMode
	cmd   *exec.Cmd
	stdin io.WriteCloser
	out   string // the file the child's output goes to
	dir   string
	mount string
	pidfd int
	done  <-chan struct{} // closed when the child has been reaped
}

func (c *capChild) output() string { b, _ := os.ReadFile(c.out); return string(b) }

// watchLog is what the child's watcher wrote.
func (c *capChild) watchLog() string {
	b, _ := os.ReadFile(filepath.Join(c.dir, "watch.log"))
	return string(b)
}

func (c *capChild) ended(d time.Duration) bool {
	select {
	case <-c.done:
		return true
	case <-time.After(d):
		return false
	}
}

// launchCapServer starts the child and returns at once, with the test's clean-up registered.
func launchCapServer(t *testing.T, mode capChildMode) *capChild {
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
	c := &capChild{mode: mode, dir: dir, mount: filepath.Join(dir, "mnt"), out: filepath.Join(t.TempDir(), "output"), pidfd: -1}
	out, err := os.Create(c.out)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close() // the child has its own copy
	c.cmd = exec.Command(self)
	c.cmd.Env = append(os.Environ(), capfsServerEnv+"="+dir)
	if mode.watch {
		c.cmd.Env = append(c.cmd.Env, capfsServerWatchEnv+"=1")
	}
	if mode.wedge {
		c.cmd.Env = append(c.cmd.Env, capfsWedgeEnv+"=1")
	}
	c.cmd.Stdout, c.cmd.Stderr = out, out
	if c.stdin, err = c.cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	c.cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &c.pidfd}
	if c.done, err = testenv.StartDyingWithParent(c.cmd); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.cleanUp(t) })
	return c
}

// cleanUp ends the child and removes its mount and directory, whatever state the test left them in: it closes the child's stdin, which
// makes it unmount and exit, kills it if it does not, and then unmounts a mount it left dead. One that is not dead is reported and left,
// with its directory.
func (c *capChild) cleanUp(t *testing.T) {
	c.stdin.Close()
	if !c.ended(capChildBound) {
		t.Errorf("the capfs child did not exit when its stdin closed; killing it. Its output: %s", c.output())
		c.sigkill()
		if !c.ended(capChildBound) {
			t.Errorf("the capfs child did not end after SIGKILL; its mount %s and directory are left", c.mount)
			return
		}
	}
	if c.pidfd >= 0 {
		unix.Close(c.pidfd)
		c.pidfd = -1
	}
	if capUnmountIfDead(t, c.mount, capChildBound) {
		if err := capRemoveDir(c.dir); err != nil {
			t.Errorf("%v", err)
		}
	}
}

// startCapServer launches the child and returns once its mount is listed, and, unless it is wedged, answers a write.
func startCapServer(t *testing.T, mode capChildMode) *capChild {
	t.Helper()
	c := launchCapServer(t, mode)
	capWaitFor(t, capChildBound, "the child's mount to be listed", func() bool {
		select {
		case <-c.done:
			t.Fatalf("the capfs child ended before its mount was ready: %v: %s", c.cmd.ProcessState, c.output())
		default:
		}
		return strings.Contains(c.output(), "mounted "+c.mount) && capListed(c.mount)
	})
	if !mode.wedge {
		c.write(t, ".ready", "x")
	}
	return c
}

// sigkill sends SIGKILL to the child through its pidfd.
func (c *capChild) sigkill() {
	if c.pidfd >= 0 {
		_ = unix.PidfdSendSignal(c.pidfd, unix.SIGKILL, nil, 0)
	}
}

// kill ends the child with SIGKILL through its pidfd, which runs none of its clean-up, and waits for it to be reaped.
func (c *capChild) kill(t *testing.T) {
	t.Helper()
	c.sigkill()
	if !c.ended(capChildBound) {
		t.Fatalf("the capfs child did not end after SIGKILL")
	}
	if ws, ok := c.cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("the capfs child ended %v, want killed by SIGKILL", c.cmd.ProcessState)
	}
}

// write writes a file through the child's mount and reads it back, bounded: the mount is live.
func (c *capChild) write(t *testing.T, name, content string) {
	t.Helper()
	p := filepath.Join(c.mount, name)
	got, err := capBounded(capChildBound, "a write and read of "+p, func() ([]byte, error) {
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			return nil, err
		}
		return os.ReadFile(p)
	})
	if err != nil || string(got) != content {
		t.Fatalf("writing %s and reading it back: %q, %v", p, got, err)
	}
}

// writeVolume writes a file through a volume of this process's, bounded: the volume works.
func writeVolume(t *testing.T, v *capVolume) {
	t.Helper()
	p := filepath.Join(v.mount, "new")
	if err := capDo(capChildBound, "a write to "+p, func() error { return os.WriteFile(p, []byte("new"), 0o600) }); err != nil {
		t.Errorf("the new volume does not work: %v", err)
	}
}

// killLeavingADeadMount starts a child whose mount has no watcher, shows its mount live, kills it, and returns once the test has seen
// the state of the issue: the mount is still listed, and its endpoint is dead (a stat fails with ENOTCONN).
func killLeavingADeadMount(t *testing.T) *capChild {
	t.Helper()
	c := startCapServer(t, capChildMode{})
	c.write(t, "before-the-kill", "served")
	c.kill(t)
	capWaitFor(t, capChildBound, "the killed child's mount to answer ENOTCONN", func() bool {
		errno, answered := capStatErrno(c.mount, capChildBound)
		return answered && errno == syscall.ENOTCONN
	})
	if !capListed(c.mount) {
		t.Fatalf("the mount of the killed child is not listed: the state of the issue, a mount left behind, was not reached")
	}
	return c
}

// The repair at the source: a server killed with SIGKILL through its pidfd has its mount unmounted by its watcher, with no sweep run,
// and the next newCapVolume works.
func TestAKilledServersMountIsUnmountedByItsWatcher(t *testing.T) {
	capFuseOrSkip(t)
	c := startCapServer(t, capChildMode{watch: true})
	c.write(t, "before-the-kill", "served")
	c.kill(t)
	capWaitFor(t, capChildBound, "the watcher to unmount the killed server's mount, before any sweep", func() bool { return !capListed(c.mount) })
	if log := c.watchLog(); !strings.Contains(log, "unmounted "+c.mount) {
		t.Errorf("the watcher's log does not say it unmounted %s: %q", c.mount, log)
	}
	writeVolume(t, newCapVolume(t, capChildLimit))
}

// A watcher whose pipe ends while its server still serves (a leaked write end that closed, or a parent that let go early) finds the
// mount live, and leaves it: it is still listed, still answers, and its watcher has exited saying so.
func TestAWatcherWhosePipeEndsLeavesALiveMountAlone(t *testing.T) {
	capFuseOrSkip(t)
	dir, err := os.MkdirTemp("", "cw")
	if err != nil {
		t.Fatal(err)
	}
	v, err := mountCapVolume(dir, capChildLimit, capFsName, true)
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	t.Cleanup(func() {
		if err := v.unmount(capChildBound); err != nil {
			t.Errorf("unmount %s: %v", v.mount, err)
		}
		if capUnmountIfDead(t, v.mount, capChildBound) {
			if err := capRemoveDir(dir); err != nil {
				t.Errorf("%v", err)
			}
		}
	})
	if err := v.releaseWatcher(); err != nil {
		t.Fatalf("%v", err)
	}
	if errno, answered := capStatErrno(v.mount, capChildBound); !capListed(v.mount) || !answered || errno != 0 {
		t.Fatalf("the live mount %s was touched by its watcher: listed %t, stat answered %t, errno %v", v.mount, capListed(v.mount), answered, errno)
	}
	writeVolume(t, v)
	log, _ := os.ReadFile(filepath.Join(filepath.Dir(v.mount), "watch.log"))
	if !strings.Contains(string(log), v.mount+" answers") {
		t.Errorf("the watcher's log does not say it left the live mount alone: %q", log)
	}
}

// The state of the issue, and its repair by the sweep: a capfs server whose mount has no watcher, killed with SIGKILL, leaves its mount
// behind with a dead endpoint, and the next newCapVolume sweeps it.
func TestTheNextCapVolumeSweepsAMountLeftByAKilledServer(t *testing.T) {
	capFuseOrSkip(t)
	dead := killLeavingADeadMount(t)
	p := filepath.Join(dead.mount, "x")
	if err := capDo(capChildBound, "a write to "+p, func() error { return os.WriteFile(p, []byte("x"), 0o600) }); !errors.Is(err, syscall.ENOTCONN) {
		t.Fatalf("a write through the killed server's mount: %v, want ENOTCONN", err)
	}
	v := newCapVolume(t, capChildLimit)
	if capListed(dead.mount) {
		t.Errorf("%s, the mount of the killed server, is still listed after newCapVolume", dead.mount)
	}
	writeVolume(t, v)
}

// A live mount owned by another process is not touched by a new volume: it is still listed, still answers, and keeps what it held.
func TestTheNextCapVolumeLeavesAnotherProcessesLiveMountAlone(t *testing.T) {
	capFuseOrSkip(t)
	live := startCapServer(t, capChildMode{watch: true})
	live.write(t, "kept", "the other test's data")
	v := newCapVolume(t, capChildLimit)
	if errno, answered := capStatErrno(live.mount, capChildBound); !capListed(live.mount) || !answered || errno != 0 {
		t.Fatalf("the live mount of another process was touched: listed %t, stat answered %t, errno %v", capListed(live.mount), answered, errno)
	}
	p := filepath.Join(live.mount, "kept")
	if kept, err := capBounded(capChildBound, "a read of "+p, func() ([]byte, error) { return os.ReadFile(p) }); err != nil || string(kept) != "the other test's data" {
		t.Errorf("the live mount lost its data: %q, %v", kept, err)
	}
	live.write(t, "after", "still served")
	writeVolume(t, v)
}

// Both at once: with one killed server's mount dead and another's live, the sweep takes the dead one and not the live one, and logs each
// decision. A sweep of another run on this machine may take the dead mount first; the live one must be untouched either way.
func TestTheSweepTakesTheDeadMountAndLeavesTheLiveOneOfAnotherChild(t *testing.T) {
	capFuseOrSkip(t)
	dead := killLeavingADeadMount(t)
	live := startCapServer(t, capChildMode{watch: true})
	live.write(t, "kept", "served")
	var logged []string
	res := sweepDeadCapMounts(realSweepEnv(func(f string, a ...any) { logged = append(logged, fmt.Sprintf(f, a...)) }))
	var sweptDead, sweptLive bool
	for _, p := range res.swept {
		sweptDead = sweptDead || p == dead.mount
		sweptLive = sweptLive || p == live.mount
	}
	if sweptLive {
		t.Errorf("swept %v, which includes the live mount %s", res.swept, live.mount)
	}
	if capListed(dead.mount) {
		t.Errorf("the dead mount %s is still listed after the sweep (swept %v)", dead.mount, res.swept)
	}
	if errno, answered := capStatErrno(live.mount, capChildBound); !capListed(live.mount) || !answered || errno != 0 {
		t.Errorf("the live mount %s was touched: listed %t, stat answered %t, errno %v", live.mount, capListed(live.mount), answered, errno)
	}
	live.write(t, "after", "still served")
	all := strings.Join(logged, "\n")
	if !strings.Contains(all, live.mount+" answers") || (sweptDead && !strings.Contains(all, "unmounted "+dead.mount)) {
		t.Errorf("the sweep did not log its decisions: %q", logged)
	}
}

// A wedged mount (its descriptor held by a child that never serves it, so a stat of it never returns) is reported, with its FUSE
// connection, and left alone, and the sweep returns within its bound. A second sweep does not stat it again while the first stat is
// stuck: it reports it at once. Only the wedged mount's own line of the real mountinfo is given to the sweep, so that other runs' mounts
// on this machine do not change what it reports.
func TestTheSweepLeavesAWedgedMountAloneAndReturnsPromptly(t *testing.T) {
	capFuseOrSkip(t)
	w := startCapServer(t, capChildMode{wedge: true})
	if _, answered := capStatErrno(w.mount, 2*time.Second); answered {
		t.Fatalf("a stat of the wedged child's mount %s returned: the wedged state was not reached", w.mount)
	}
	m := capMountAt(t, w.mount)
	env := realSweepEnv(t.Logf)
	env.failsOnWedged = func(capMount) bool { return true }
	env.mountinfo = func() ([]byte, error) {
		data, err := os.ReadFile("/proc/self/mountinfo")
		var keep []string
		for _, line := range strings.Split(string(data), "\n") {
			if ms := parseCapMounts(line, capFsWedgeName); len(ms) == 1 && ms[0].point == w.mount {
				keep = append(keep, line)
			}
		}
		return []byte(strings.Join(keep, "\n")), err
	}
	sweep := func() (sweepResult, time.Duration) {
		start := time.Now()
		resc := make(chan sweepResult, 1)
		go func() { resc <- sweepDeadCapMounts(env) }()
		select {
		case res := <-resc:
			return res, time.Since(start)
		case <-time.After(capStatTimeout + capChildBound):
			t.Fatalf("the sweep did not return within %s of a wedged mount", capStatTimeout+capChildBound) // the clean-up ends the child, and the stuck stat with it
		}
		return sweepResult{}, 0
	}
	res, took := sweep()
	if took > capStatTimeout+5*time.Second {
		t.Errorf("the sweep took %s with one wedged mount, want about its stat bound, %s", took, capStatTimeout)
	}
	if len(res.swept) != 0 || len(res.wedged) != 1 || res.wedged[0].point != w.mount {
		t.Fatalf("swept %v and found wedged %v, want only %s wedged", res.swept, res.wedged, w.mount)
	}
	if m.connDir() == "" || !strings.Contains(res.failure(), m.connDir()) || !strings.Contains(res.failure(), "FUSE connection "+m.dev) {
		t.Errorf("the failure does not name the mount's connection %s (%s): %s", m.dev, m.connDir(), res.failure())
	}
	if !capListed(w.mount) {
		t.Errorf("the wedged mount %s is no longer listed: the sweep touched it", w.mount)
	}
	res2, took2 := sweep()
	if took2 > capStatTimeout/2 || len(res2.wedged) != 1 || !strings.Contains(res2.failure(), "has not returned") {
		t.Errorf("a second sweep took %s and reported %q, want it at once, from the first sweep's stat that has not returned", took2, res2.failure())
	}
}

// The mount's descriptor is close-on-exec from the moment this process has it, so a process it starts later (k3s, the CLI, another
// mount's helper) does not hold it, and cannot keep the connection open after the server has gone. The mount here is never served, and
// is ended by closing the descriptor, which leaves it dead, for the test's own unmount.
func TestTheMountsDescriptorIsCloseOnExec(t *testing.T) {
	capFuseOrSkip(t)
	dir, err := os.MkdirTemp("", "cx")
	if err != nil {
		t.Fatal(err)
	}
	if real, err := filepath.EvalSymlinks(dir); err == nil {
		dir = real
	}
	point := filepath.Join(dir, "mnt")
	if err := os.Mkdir(point, 0o700); err != nil {
		t.Fatal(err)
	}
	conn, err := fusermountCapfs(point, capFsWedgeName)
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	t.Cleanup(func() {
		unix.Close(conn.fd) // ends the connection
		if capUnmountIfDead(t, point, capChildBound) {
			if err := capRemoveDir(dir); err != nil {
				t.Errorf("%v", err)
			}
		}
	})
	flags, err := unix.FcntlInt(uintptr(conn.fd), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Errorf("the /dev/fuse descriptor (fd %d) has flags %#x (%v), want FD_CLOEXEC", conn.fd, flags, err)
	}
	for fd, target := range capHelperFds(t) {
		if target == "/dev/fuse" {
			t.Errorf("a process started after the mount holds /dev/fuse as its fd %s", fd)
		}
	}
}

// The mount's helper gets only its own end of the socket, as its fd 3, and not this process's end. This needs no FUSE: the helper here
// is ls, listing its own descriptors.
func TestTheMountHelperGetsOnlyItsOwnEndOfTheSocket(t *testing.T) {
	ls, err := exec.LookPath("ls")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	h, err := startCommHelper(ls, []string{"-l", "/proc/self/fd"}, &out)
	if err != nil {
		t.Fatal(err)
	}
	defer h.comm.Close()
	select {
	case <-h.done:
	case <-time.After(capChildBound):
		t.Fatal("ls did not finish")
	}
	var st unix.Stat_t
	if err := unix.Fstat(fileFd(t, h.comm), &st); err != nil {
		t.Fatal(err)
	}
	mine := fmt.Sprintf("socket:[%d]", st.Ino)
	fds := parseFdListing(out.String())
	if !strings.HasPrefix(fds["3"], "socket:[") || fds["3"] == mine {
		t.Errorf("the helper's fd 3 is %q, want its own end of the socket (this process's is %s): %s", fds["3"], mine, out.String())
	}
	for fd, target := range fds {
		if target == mine {
			t.Errorf("the helper holds this process's end of the socket, %s, as its fd %s: %s", mine, fd, out.String())
		}
	}
}

// startTestWatcher starts a watcher of a mount point where nothing is mounted, with its log in a directory of the test's, and releases
// it at the test's end. This needs no FUSE.
func startTestWatcher(t *testing.T, env ...string) (*capWatcher, string) {
	t.Helper()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "watch.log")
	w, err := startCapWatcher(filepath.Join(dir, "mnt"), logPath, env...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.release(capWatchTimeout); err != nil {
			t.Errorf("%v", err)
		}
	})
	return w, logPath
}

// The watcher's pipe ends only when this process lets go of it, or dies: its write end is close-on-exec, so neither the watcher itself
// (which would then wait on its own write end for good, as fusermount3 did on go-fuse's socket) nor a process started later (k3s, the
// CLI) holds it; the watcher holds the read end, as its stdin, and nothing else of the pipe.
func TestTheWatchersPipeReachesNoOtherProcess(t *testing.T) {
	w, logPath := startTestWatcher(t)
	fd := fileFd(t, w.pipe)
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Errorf("the watcher's pipe (fd %d) has flags %#x (%v), want FD_CLOEXEC", fd, flags, err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		t.Fatal(err)
	}
	pipe := fmt.Sprintf("pipe:[%d]", st.Ino)
	dir := fmt.Sprintf("/proc/%d/fd", w.cmd.Process.Pid) // its child, not yet reaped, so the pid is still the watcher's
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		target, err := os.Readlink(filepath.Join(dir, e.Name()))
		if err == nil && target == pipe && e.Name() != "0" {
			t.Errorf("the watcher holds its own pipe as fd %s, besides its stdin: it would never see the pipe end", e.Name())
		}
	}
	for n, target := range capHelperFds(t) {
		if target == pipe {
			t.Errorf("a process started after the watcher holds its pipe as fd %s", n)
		}
	}
	if err := w.release(capChildBound); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(logPath); !strings.Contains(string(b), "is not mounted: nothing to do") {
		t.Errorf("the watcher's log: %q, want it to say that nothing is mounted", b)
	}
}

// The watcher ignores SIGTERM, SIGINT and SIGHUP, so that a stopped scope, which sends SIGTERM to every process in it at once, does not
// end it before the death of its parent does; and a SIGTERM sent to it then does not end it. It leads a session of its own, so that a
// signal to the test's process group (timeout(1)'s SIGKILL after -k, say) does not reach it.
func TestTheWatcherIgnoresTheSignalsOfAStoppedScope(t *testing.T) {
	w, logPath := startTestWatcher(t)
	pidfd, err := unix.PidfdOpen(w.cmd.Process.Pid, 0) // its child, not yet reaped, so the pid is still the watcher's
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(pidfd)
	if sid, err := unix.Getsid(w.cmd.Process.Pid); err != nil || sid != w.cmd.Process.Pid {
		t.Errorf("the watcher (pid %d) is in session %d (%v), want a session of its own", w.cmd.Process.Pid, sid, err)
	}
	want := uint64(1)<<(uint(syscall.SIGTERM)-1) | uint64(1)<<(uint(syscall.SIGINT)-1) | uint64(1)<<(uint(syscall.SIGHUP)-1)
	status := fmt.Sprintf("/proc/%d/status", w.cmd.Process.Pid)
	capWaitFor(t, capChildBound, "the watcher to ignore SIGTERM, SIGINT and SIGHUP (SigIgn in "+status+")", func() bool {
		b, err := os.ReadFile(status)
		if err != nil {
			return false
		}
		for _, line := range strings.Split(string(b), "\n") {
			if v, ok := strings.CutPrefix(line, "SigIgn:"); ok {
				n, err := strconv.ParseUint(strings.TrimSpace(v), 16, 64)
				return err == nil && n&want == want
			}
		}
		return false
	})
	if err := unix.PidfdSendSignal(pidfd, unix.SIGTERM, nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.release(capWatchTimeout); err != nil {
		t.Fatal(err)
	}
	if !w.cmd.ProcessState.Success() {
		t.Errorf("the watcher ended %v after a SIGTERM, want a clean exit", w.cmd.ProcessState)
	}
	if b, _ := os.ReadFile(logPath); !strings.Contains(string(b), "nothing to do") {
		t.Errorf("the watcher's log: %q, want it to have gone on after the SIGTERM", b)
	}
}

// A watcher whose pipe never ends gives up after its deadline, and leaves the mount as it is, so that a leaked write end does not keep it
// alive for good.
func TestAWatcherGivesUpAfterItsDeadline(t *testing.T) {
	w, logPath := startTestWatcher(t, capfsWatchMaxEnv+"=1s")
	select {
	case <-w.done:
	case <-time.After(capChildBound):
		t.Fatalf("the watcher, given 1s, did not give up within %s", capChildBound)
	}
	if b, _ := os.ReadFile(logPath); !strings.Contains(string(b), "gives up") {
		t.Errorf("the watcher's log: %q, want it to say it gave up", b)
	}
}

// What a watcher does once its pipe has ended, for each state of the mount: only a dead one is unmounted; a live one is left at once; one
// that is first unknown (the dying server's descriptor still closing) and then dead is unmounted; one that stays wedged is left after the
// settle time; and one that is not mounted needs nothing.
func TestTheWatcherUnmountsOnlyADeadMount(t *testing.T) {
	// settle is long where the mount turns dead in time, so that a slow machine does not make the watcher give up first, and short where
	// it never does.
	for _, c := range []struct {
		name      string
		listed    bool
		states    []capState
		settle    time.Duration
		unmounted bool
		says      string
	}{
		{"dead", true, []capState{capDead}, time.Minute, true, "unmounted /m"},
		{"live", true, []capState{capLive}, time.Minute, false, "answers"},
		{"unknown, then dead", true, []capState{capUnknown, capWedged, capDead}, time.Minute, true, "unmounted /m"},
		{"wedged", true, []capState{capWedged}, 300 * time.Millisecond, false, "was wedged"},
		{"not mounted", false, nil, time.Minute, false, "not mounted"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var probes, unmounts int
			says := capWatchAfterEnd("/m", c.settle,
				func(string) bool { return c.listed && unmounts == 0 },
				func(string) (capState, error) {
					s := c.states[min(probes, len(c.states)-1)]
					probes++
					return s, errors.New("the probe's reason")
				},
				func(string) error { unmounts++; return nil })
			if (unmounts == 1) != c.unmounted || unmounts > 1 {
				t.Errorf("unmounted %d times, want %t", unmounts, c.unmounted)
			}
			if !strings.Contains(says, c.says) {
				t.Errorf("the watcher says %q, want it to say %q", says, c.says)
			}
			if c.name == "live" && probes != 1 {
				t.Errorf("the watcher probed a live mount %d times, want once: it leaves it at once", probes)
			}
		})
	}
}

// capHelperFds lists the descriptors of a process this one starts now, with nothing passed to it on purpose.
func capHelperFds(t *testing.T) map[string]string {
	t.Helper()
	out, err := exec.Command("ls", "-l", "/proc/self/fd").Output()
	if err != nil {
		t.Fatalf("ls -l /proc/self/fd: %v", err)
	}
	return parseFdListing(string(out))
}

// parseFdListing reads an ls -l of /proc/self/fd into each descriptor's target.
func parseFdListing(s string) map[string]string {
	fds := map[string]string{}
	for _, line := range strings.Split(s, "\n") {
		left, target, ok := strings.Cut(line, " -> ")
		if !ok {
			continue
		}
		f := strings.Fields(left)
		if len(f) == 0 {
			continue
		}
		fds[f[len(f)-1]] = target
	}
	return fds
}
