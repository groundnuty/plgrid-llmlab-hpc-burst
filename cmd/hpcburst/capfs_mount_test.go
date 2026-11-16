package main

// How a capfs volume is mounted, and what unmounts it when its test is killed (issue #648).
//
// The FUSE server of a volume runs in the test's own process, so a killed test never runs the clean-up that unmounts it. Two ways of
// unmounting at the source were looked at, and neither works on this machine's fusermount3 (3.14.0), so a watcher process of our own
// does it.
//
// go-fuse v2.12.1 can pass fusermount3's auto_unmount, but its mount is then wedged at once. It runs fusermount3 itself
// (fuse/mount_linux.go, callFusermount) and waits for it to EXIT before it reads the /dev/fuse descriptor from their socket, but with
// auto_unmount fusermount3 does not exit: it stays, to unmount when the socket closes. Its socketpair is not close-on-exec either, so
// fusermount3 inherits the mounting end as well as its own, and never sees the socket close. The mount is made, its descriptor waits
// unread in the socket, nothing serves it, and every stat of it blocks; when the process is killed, fusermount3 still holds both ends,
// and the mount stays open and unserved for good. That wedged two sessions on 16 Nov 2026, when an earlier test of this issue mounted
// with auto_unmount through go-fuse.
//
// Mounting the way libfuse does (below) avoids that, but fusermount3 3.14.0 still does not unmount: when the socket closes, it opens the
// mount point as root to check that the mount is dead (should_auto_unmount), and FUSE refuses root a mount made without allow_other,
// with EACCES; only ENOTCONN counts, so it exits and leaves the mount. libfuse fixed that later (recheck_ENOTCONN_as_owner, in 3.16.2).
// allow_other needs user_allow_other in /etc/fuse.conf, which a user cannot set.
//
// So: the volume is mounted with a close-on-exec socketpair, fusermount3 gets only its own end (fd 3, _FUSE_COMMFD=3), and the
// descriptor is received at once with MSG_CMSG_CLOEXEC, so that no process this one starts later (k3s, the CLI, another mount's helper)
// inherits it and keeps the connection open after the server has gone; go-fuse serves it through its /dev/fd/N mount point. And before
// the mount, a watcher is started: this test binary again (capfsWatchMain), as this user, holding the read end of a close-on-exec pipe
// whose write end only this process holds. When this process dies, however it dies (SIGKILL, a crash, a -test.timeout panic, a stopped
// scope's SIGTERM), the pipe ends, and the watcher unmounts the mount with a plain fusermount3 -u, but only once a bounded stat of it
// fails with ENOTCONN: a mount that answers, or does not, is left alone. The watcher ignores SIGTERM, SIGINT and SIGHUP, runs in its own
// session, and gives up after capWatchMax, so it never lingers.
//
// What it does not cover: a SIGKILL of the whole scope or cgroup kills the watcher too, and leaves the mount with a dead endpoint. The
// sweep of dead mounts (capfs_sweep_test.go) removes those, at the next newCapVolume.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	// capMaxWrite is the largest write the server takes, which is also the mount's max_read: go-fuse sizes its request buffers by it, so
	// the kernel must not be told it may send more. 128 KiB is go-fuse's default.
	capMaxWrite = 128 << 10
	// capMountTimeout bounds fusermount3's mount.
	capMountTimeout = 30 * time.Second
	// capUnmountTimeout bounds one fusermount3 -u.
	capUnmountTimeout = 30 * time.Second

	// capfsWatchEnv makes the test binary a watcher instead of running tests (TestMain): the value is the mount point it watches.
	capfsWatchEnv = "HPCBURST_TEST_CAPFS_WATCH"
	// capfsWatchMaxEnv overrides capWatchMax, for the test of it.
	capfsWatchMaxEnv = "HPCBURST_TEST_CAPFS_WATCH_MAX"
	// capWatchMax is how long a watcher waits for its pipe to end before it gives up and exits, leaving the mount as it is: longer
	// than any one test, so that only a leaked write end, which would never end, reaches it.
	capWatchMax = 2 * time.Hour
	// capWatchSettle is how long a watcher, once its pipe has ended, keeps looking at a mount that is neither dead nor live: the dying
	// process's descriptor may still be closing, so a first stat may wait, or fail with ECONNABORTED.
	capWatchSettle = 30 * time.Second
	// capWatchProbe bounds each of the watcher's stats.
	capWatchProbe = 2 * time.Second
	// capWatchTimeout bounds the wait for a watcher to exit, once its pipe has ended.
	capWatchTimeout = capWatchSettle + capUnmountTimeout + 10*time.Second
)

// capMountOptions are fusermount3's -o options for a capfs mount: the names the sweep finds it by in /proc/self/mountinfo (source and
// type), and max_read, which go-fuse would pass if it mounted itself.
func capMountOptions(source string) string {
	return strings.Join([]string{"fsname=" + source, "subtype=capfs", fmt.Sprintf("max_read=%d", capMaxWrite)}, ",")
}

// fusermountBinary is the helper go-fuse would run: fusermount3, else fusermount; on PATH, then in /bin.
func fusermountBinary() (string, error) {
	for _, name := range []string{"fusermount3", "fusermount"} {
		if p, err := exec.LookPath(name); err == nil {
			return p, nil
		}
		if p, err := exec.LookPath("/bin/" + name); err == nil {
			return p, nil
		}
	}
	return "", errors.New("neither fusermount3 nor fusermount is installed")
}

// commHelper is a helper process started with one end of a socketpair as its fd 3, as fusermount3 expects, and the end this process
// keeps.
type commHelper struct {
	comm   *os.File      // this process's end; close-on-exec and non-blocking
	cmd    *exec.Cmd     //
	done   chan struct{} // closed once the helper has exited and been reaped
	stderr bytes.Buffer  // what the helper wrote to stderr; read it only after done is closed
}

// startCommHelper starts bin with args, and with the far end of a new close-on-exec socketpair as its fd 3 (_FUSE_COMMFD=3). The helper
// inherits no descriptor of this process's but that end: everything else this process opens is close-on-exec (Go opens its files so,
// and both ends here are), and exec makes fd 3 a copy without it. stdout is where the helper's output goes (nil: nowhere).
func startCommHelper(bin string, args []string, stdout io.Writer) (*commHelper, error) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("socketpair: %w", err)
	}
	// Only this process's end is non-blocking, so that its read can be bounded; fusermount3 blocks on its own end.
	if err := unix.SetNonblock(fds[0], true); err != nil {
		unix.Close(fds[0])
		unix.Close(fds[1])
		return nil, fmt.Errorf("socketpair: %w", err)
	}
	comm, remote := os.NewFile(uintptr(fds[0]), "capfs-comm"), os.NewFile(uintptr(fds[1]), "capfs-comm-helper")
	h := &commHelper{comm: comm, done: make(chan struct{})}
	h.cmd = exec.Command(bin, args...)
	h.cmd.Env = []string{"_FUSE_COMMFD=3"}
	h.cmd.ExtraFiles = []*os.File{remote}
	h.cmd.Stdout, h.cmd.Stderr = stdout, &h.stderr
	err = h.cmd.Start()
	remote.Close() // the helper has its copy; this process holds only its own end, so it sees the helper exit
	if err != nil {
		comm.Close()
		return nil, fmt.Errorf("start %s: %w", bin, err)
	}
	go func() { _ = h.cmd.Wait(); close(h.done) }()
	return h, nil
}

// recvFuseFd reads the /dev/fuse descriptor fusermount3 sends over the socket, waiting at most d. It is received close-on-exec
// (MSG_CMSG_CLOEXEC), atomically: set afterwards, a process this one started in between would inherit it, and hold the connection open
// after the server has gone.
func recvFuseFd(comm *os.File, d time.Duration) (int, error) {
	rc, err := comm.SyscallConn()
	if err != nil {
		return -1, err
	}
	if err := comm.SetReadDeadline(time.Now().Add(d)); err != nil {
		return -1, err
	}
	defer comm.SetReadDeadline(time.Time{})
	var (
		buf     [1]byte
		oob     = make([]byte, unix.CmsgSpace(4))
		n, oobn int
		rerr    error
	)
	err = rc.Read(func(fd uintptr) bool {
		n, oobn, _, _, rerr = unix.Recvmsg(int(fd), buf[:], oob, unix.MSG_CMSG_CLOEXEC)
		return rerr != unix.EAGAIN
	})
	if err != nil {
		return -1, fmt.Errorf("waiting for the descriptor: %w", err)
	}
	if rerr != nil {
		return -1, fmt.Errorf("receiving the descriptor: %w", rerr)
	}
	if n == 0 && oobn == 0 {
		return -1, errors.New("the helper closed the socket without sending a descriptor")
	}
	msgs, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return -1, fmt.Errorf("parsing the control message: %w", err)
	}
	var fds []int
	for i := range msgs {
		got, err := unix.ParseUnixRights(&msgs[i])
		if err != nil {
			continue
		}
		fds = append(fds, got...)
	}
	if len(fds) != 1 {
		for _, fd := range fds {
			unix.Close(fd)
		}
		return -1, fmt.Errorf("expected one descriptor, got %d", len(fds))
	}
	return fds[0], nil
}

// capConn is a FUSE connection mounted by fusermount3 and not yet served.
type capConn struct {
	point string
	fd    int // the /dev/fuse descriptor, close-on-exec; go-fuse owns it once it serves it
}

// fusermountCapfs mounts a FUSE file system named source at point through fusermount3, and returns the connection.
func fusermountCapfs(point, source string) (*capConn, error) {
	bin, err := fusermountBinary()
	if err != nil {
		return nil, err
	}
	h, err := startCommHelper(bin, []string{point, "-o", capMountOptions(source)}, nil)
	if err != nil {
		return nil, err
	}
	defer h.comm.Close()
	fd, err := recvFuseFd(h.comm, capMountTimeout)
	if err != nil {
		h.comm.Close()
		select {
		case <-h.done:
			return nil, fmt.Errorf("%s %s: %w: %v: %s", bin, point, err, h.cmd.ProcessState, strings.TrimSpace(h.stderr.String()))
		case <-time.After(capUnmountTimeout):
			return nil, fmt.Errorf("%s %s: %w, and it did not exit", bin, point, err)
		}
	}
	select { // fusermount3 exits once it has sent the descriptor
	case <-h.done:
	case <-time.After(capUnmountTimeout):
		unix.Close(fd)
		return nil, fmt.Errorf("%s %s sent the descriptor but did not exit", bin, point)
	}
	return &capConn{point: point, fd: fd}, nil
}

// capUnmountArgs are the arguments of an unmount: -u, a plain unmount, which fails with EBUSY while the mount is in use. Never -z, a lazy
// unmount, which would detach a live mount from under its user.
func capUnmountArgs(point string) []string { return []string{"-u", point} }

// capUnmount unmounts point with fusermount3 -u, waiting at most d. A helper that does not return in time is left running, and reported.
func capUnmount(point string, d time.Duration) error {
	bin, err := fusermountBinary()
	if err != nil {
		return err
	}
	type result struct {
		out []byte
		err error
	}
	res := make(chan result, 1)
	go func() {
		out, err := exec.Command(bin, capUnmountArgs(point)...).CombinedOutput()
		res <- result{out, err}
	}()
	select {
	case r := <-res:
		if r.err != nil {
			return fmt.Errorf("%s -u %s: %w: %s", bin, point, r.err, strings.TrimSpace(string(r.out)))
		}
		return nil
	case <-time.After(d):
		return fmt.Errorf("%s -u %s did not return within %s", bin, point, d)
	}
}

// capWatcher is the watcher of one mount: the write end of its pipe, which only this process holds, and the process.
type capWatcher struct {
	point string
	pipe  *os.File      // close-on-exec; closing it, or this process's death, ends the watcher's wait
	cmd   *exec.Cmd     //
	done  chan struct{} // closed once the watcher has exited and been reaped
}

// startCapWatcher starts the watcher of the mount at point, writing what it does to logPath. Start it before the mount, so that no moment
// of the mount's life is unwatched. env is added to the watcher's own small environment.
func startCapWatcher(point, logPath string, env ...string) (*capWatcher, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	r, w, err := os.Pipe() // both ends close-on-exec: the watcher gets the read end as its stdin, and nobody else gets either
	if err != nil {
		return nil, err
	}
	logf, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		r.Close()
		w.Close()
		return nil, err
	}
	cw := &capWatcher{point: point, pipe: w, done: make(chan struct{})}
	cw.cmd = exec.Command(self)
	cw.cmd.Env = append([]string{capfsWatchEnv + "=" + point, "PATH=" + os.Getenv("PATH")}, env...)
	cw.cmd.Stdin, cw.cmd.Stdout, cw.cmd.Stderr = r, logf, logf
	// Its own session, so that a signal to this process's group does not reach it; no Pdeathsig, since it must outlive this process.
	cw.cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	err = cw.cmd.Start()
	r.Close() // the watcher has its copy
	logf.Close()
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("start the watcher of %s: %w", point, err)
	}
	go func() { _ = cw.cmd.Wait(); close(cw.done) }()
	return cw, nil
}

// release ends the watcher's wait and waits at most d for it to exit. Release it only after the mount is unmounted: a watcher released
// while the server still serves leaves the mount alone, but the mount is then unwatched.
func (cw *capWatcher) release(d time.Duration) error {
	cw.pipe.Close()
	select {
	case <-cw.done:
		return nil
	case <-time.After(d):
		return fmt.Errorf("the watcher of %s did not exit within %s of its pipe closing", cw.point, d)
	}
}

// capfsWatchMain is the watcher: it waits for its stdin to end, which happens when the process that started it is gone, then unmounts
// the mount if it is dead (capWatchAfterEnd), says what it did, and exits.
func capfsWatchMain(point string) {
	signal.Ignore(syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP) // a stopped scope sends SIGTERM to every process in it, the test first among them
	max := capWatchMax
	if v := os.Getenv(capfsWatchMaxEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			max = d
		}
	}
	ended := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, os.Stdin); close(ended) }()
	select {
	case <-ended:
	case <-time.After(max):
		fmt.Printf("capfs watcher: its pipe did not end within %s, so it gives up: %s is left as it is\n", max, point)
		os.Exit(0)
	}
	fmt.Println(capWatchAfterEnd(point, capWatchSettle, capListed,
		func(p string) (capState, error) { return probeCapMount(p, capWatchProbe) },
		func(p string) error { return capUnmount(p, capUnmountTimeout) }))
	os.Exit(0)
}

// capWatchAfterEnd is what a watcher does once its pipe has ended: it unmounts the mount at point, with a plain unmount, only when a stat
// of it fails with ENOTCONN. A mount that answers is someone's, and is left at once; one that is neither dead nor live is looked at again
// until settle has passed, and then left. It returns what it did.
func capWatchAfterEnd(point string, settle time.Duration, listed func(string) bool, probe func(string) (capState, error), unmount func(string) error) string {
	deadline := time.Now().Add(settle)
	for {
		if !listed(point) {
			return "capfs watcher: " + point + " is not mounted: nothing to do"
		}
		state, why := probe(point)
		switch state {
		case capDead:
			if err := unmount(point); err != nil {
				if !listed(point) {
					return "capfs watcher: " + point + " went away meanwhile: nothing to do"
				}
				return fmt.Sprintf("capfs watcher: %s is dead, and could not be unmounted: %v", point, err)
			}
			return "capfs watcher: unmounted " + point + ", whose server is gone"
		case capLive:
			return "capfs watcher: " + point + " answers, so someone serves it: left alone"
		}
		if time.Now().After(deadline) {
			return fmt.Sprintf("capfs watcher: %s was %v (%v) for %s: left alone", point, state, why, settle)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
