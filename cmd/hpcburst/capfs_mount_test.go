package main

// How a capfs volume is mounted, so that a killed test leaves a DEAD mount, never a wedged one (issue #648).
//
// The FUSE server of a volume runs in the test's own process, so a killed test never runs the clean-up that unmounts it. What matters is
// what it leaves. If no process holds the connection's /dev/fuse descriptor any more, the kernel ends the connection when the server
// dies: the mount is dead, a stat of it fails at once with ENOTCONN, and the sweep of dead mounts (capfs_sweep_test.go) unmounts it at
// the next newCapVolume. If some other process still holds the descriptor, or the socket it travels in, the connection stays open and
// nobody serves it: the mount is wedged, and every stat of it blocks for good. That wedged two sessions on 16 Nov 2026.
//
// The cause was go-fuse v2.12.1's own mount (fuse/mount_linux.go, callFusermount), with fusermount3's auto_unmount, which an earlier
// test of this issue passed. go-fuse waits for fusermount3 to EXIT before it reads the descriptor from their socket, but with
// auto_unmount fusermount3 does not exit: it stays, to unmount when the socket closes. And its socketpair is not close-on-exec, so
// fusermount3 inherits the mounting end as well as its own, and never sees the socket close. The mount is made, its descriptor waits
// unread in the socket that fusermount3 holds, nothing serves it, and that outlives the test. (go-fuse's master of 8 Nov 2026 has both.)
//
// So the volume is mounted here the way libfuse's fuse_mount_fusermount does it. The socketpair is close-on-exec, and fusermount3 gets
// only its own end, as fd 3 (_FUSE_COMMFD=3). The descriptor is read at once, with MSG_CMSG_CLOEXEC, so that no process this one starts
// later (k3s, the CLI, another mount's fusermount3) inherits it and holds the connection open after the server has gone. go-fuse serves
// it through its /dev/fd/N mount point, which skips go-fuse's poll hack, so capPollHack runs it here.
//
// With auto_unmount, fusermount3 stays until this process's end of the socket closes, at the clean-up or at this process's death, and
// then exits. On fusermount3 3.16 and later it unmounts the mount first, if it is dead. Before 3.16 it cannot: it opens the mount point
// as root to check that the mount is dead (should_auto_unmount), FUSE refuses root a mount made without allow_other with EACCES, only
// ENOTCONN counts, and it exits leaving the mount (libfuse added recheck_ENOTCONN_as_owner later; allow_other needs user_allow_other in
// /etc/fuse.conf, which a user cannot set). Either way it leaves no process behind, and the sweep removes a mount it left.
//
// What none of this covers: a process that cannot finish dying. A thread of it waiting on its own mount for a request its server had
// already read (a test hung on its volume, then killed) waits on uninterruptibly, so the process's descriptors stay open, and the mount
// stays wedged until its connection is aborted (/sys/fs/fuse/connections/N/abort); every other run's sweep reports it, and none touches
// it. That happened once, in a loaded run on 16 Nov 2026, from the deadlock that capPollHack now prevents.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
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
	// capUnmountTimeout bounds one fusermount3 -u, and the wait for an auto_unmount fusermount3 to exit.
	capUnmountTimeout = 30 * time.Second
)

// capMountOptions are fusermount3's -o options for a capfs mount: the names the sweep finds it by in /proc/self/mountinfo (source and
// type), and max_read, which go-fuse would pass if it mounted itself.
func capMountOptions(source string, autoUnmount bool) string {
	opts := []string{"fsname=" + source, "subtype=capfs", fmt.Sprintf("max_read=%d", capMaxWrite)}
	if autoUnmount {
		opts = append([]string{"auto_unmount"}, opts...)
	}
	return strings.Join(opts, ",")
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

// fusermountVersion is the installed helper's version, from its --version ("fusermount3 version: 3.14.0").
func fusermountVersion() (major, minor int, err error) {
	bin, err := fusermountBinary()
	if err != nil {
		return 0, 0, err
	}
	out, err := exec.Command(bin, "--version").CombinedOutput()
	if err != nil {
		return 0, 0, fmt.Errorf("%s --version: %w: %s", bin, err, out)
	}
	_, v, ok := strings.Cut(strings.TrimSpace(string(out)), "version: ")
	parts := strings.SplitN(v, ".", 3)
	if !ok || len(parts) < 2 {
		return 0, 0, fmt.Errorf("%s --version printed %q, not a version", bin, out)
	}
	if major, err = strconv.Atoi(parts[0]); err == nil {
		minor, err = strconv.Atoi(parts[1])
	}
	if err != nil {
		return 0, 0, fmt.Errorf("%s --version printed %q, not a version", bin, out)
	}
	return major, minor, nil
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
	remote.Close() // the helper has its copy; this process holds only its own end, so the helper sees it close
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
	point  string
	fd     int         // the /dev/fuse descriptor, close-on-exec; go-fuse owns it once it serves it
	helper *commHelper // fusermount3; with auto_unmount it stays until its socket closes (closeComm)
}

// fusermountCapfs mounts a FUSE file system named source at point through fusermount3, and returns the connection. With autoUnmount,
// fusermount3 stays until this process's end of the socket closes (closeComm, or this process's death), and then exits, unmounting a
// dead mount first where it can (fusermount3 3.16 and later).
func fusermountCapfs(point, source string, autoUnmount bool) (*capConn, error) {
	bin, err := fusermountBinary()
	if err != nil {
		return nil, err
	}
	h, err := startCommHelper(bin, []string{point, "-o", capMountOptions(source, autoUnmount)}, nil)
	if err != nil {
		return nil, err
	}
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
	c := &capConn{point: point, fd: fd, helper: h}
	if !autoUnmount { // fusermount3 exits once it has sent the descriptor
		if err := c.closeComm(capUnmountTimeout); err != nil {
			unix.Close(fd)
			return nil, err
		}
	}
	return c, nil
}

// closeComm closes this process's end of the socket and waits at most d for fusermount3 to exit. One that does not exit in time is
// killed: it is this process's child, not yet reaped, so its pid is still its own.
func (c *capConn) closeComm(d time.Duration) error {
	c.helper.comm.Close()
	select {
	case <-c.helper.done:
		return nil
	case <-time.After(d):
	}
	_ = c.helper.cmd.Process.Kill()
	select {
	case <-c.helper.done:
	case <-time.After(capUnmountTimeout):
	}
	return fmt.Errorf("fusermount3 for %s did not exit within %s of its socket closing, and was killed", c.point, d)
}

// capPollHackName is go-fuse's pollHackName (fuse/poll.go): a file of the root that its server answers itself.
const capPollHackName = ".go-fuse-epoll-hack"

// capPollHack is go-fuse's pollHack (fuse/poll_unix.go), which its WaitMount runs, but not for a /dev/fd/N mount point. It opens the
// server's own file and polls it once, so that the server answers the kernel's POLL with ENOSYS, after which the kernel sends no POLL
// again. Without it, Go's runtime, which registers every file this process opens with epoll, has the kernel send a POLL for a file on
// the mount, and when epoll has taken the last of GOMAXPROCS, no thread is left to answer it: the process deadlocks on its own mount,
// which a loaded -count=50 run of these tests did once before this was added (the cause is go-fuse's own comment on the hack; the
// deadlock itself was not traced). The open is a raw one, as go-fuse's is, so that the runtime does not register it with epoll itself;
// and it is bounded by d.
func capPollHack(point string, d time.Duration) error {
	return capDo(d, "the poll hack on "+point, func() error {
		fd, err := syscall.Open(point+"/"+capPollHackName, syscall.O_RDONLY, 0)
		if err != nil {
			return err
		}
		_, _ = unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN | unix.POLLPRI | unix.POLLOUT}}, 0)
		return syscall.Close(fd)
	})
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
