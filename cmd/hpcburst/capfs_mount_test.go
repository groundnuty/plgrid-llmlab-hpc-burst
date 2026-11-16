package main

// How a capfs volume is mounted, so that fusermount3's auto_unmount works (issue #648).
//
// go-fuse v2.12.1 runs fusermount3 itself (fuse/mount_linux.go, callFusermount), and two things there make auto_unmount worse than
// useless. It waits for fusermount3 to EXIT before it reads the /dev/fuse descriptor from their socket, but with auto_unmount fusermount3
// does not exit: it stays, to unmount when the socket closes. And its socketpair is not close-on-exec, so fusermount3 inherits the
// mounting end as well as its own, and never sees the socket close. So the mount is made, its descriptor waits unread in the socket,
// nothing serves it, and every stat of it blocks; when the mounting process is killed, fusermount3 still holds both ends, and the mount
// stays open and unserved for good. That wedged two sessions on 16 Nov 2026, when an earlier test of this issue mounted with
// auto_unmount through go-fuse.
//
// So the volume is mounted here the way libfuse's fuse_mount_fusermount does it. The socketpair is close-on-exec, and fusermount3 gets
// only its own end, as fd 3 (_FUSE_COMMFD=3). The descriptor is read at once, with MSG_CMSG_CLOEXEC, so that no process started later by
// this one inherits it. The mounting end stays open for the life of the server, and go-fuse serves the descriptor through its /dev/fd/N
// mount point. With auto_unmount, fusermount3 stays to watch the socket: when this process dies, however it dies (SIGKILL, a crash, a
// -test.timeout panic), the kernel closes the mounting end and the descriptor, and fusermount3 unmounts the mount.
//
// What that does not cover: fusermount3 runs in this process's systemd scope, so a stopped scope, or a cgroup kill, takes it too, and the
// mount is left with a dead endpoint. A mount made without auto_unmount is also left. The sweep of dead mounts (capfs_sweep_test.go)
// removes those, at the next newCapVolume.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	// capMaxWrite is the largest write the server takes, which is also the mount's max_read: go-fuse sizes its request buffers by it, so
	// the kernel must not be told it may send more. 128 KiB is go-fuse's default.
	capMaxWrite = 128 << 10
	// capMountTimeout bounds fusermount3's mount: past it, the mount is given up, and the mounting end is closed, which makes an
	// auto_unmount fusermount3 unmount whatever it mounted.
	capMountTimeout = 30 * time.Second
	// capUnmountTimeout bounds one fusermount3 -u.
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
	remote.Close() // the helper has its copy; this process must hold only its own end, so that the helper sees it close
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
	helper *commHelper // fusermount3; with auto_unmount it watches comm until it closes
	auto   bool
}

// fusermountCapfs mounts a FUSE file system named source at point through fusermount3, and returns the connection. With autoUnmount,
// fusermount3 stays to unmount it when the mounting end of the socket closes: at closeComm, or when this process dies.
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
		h.comm.Close() // an auto_unmount fusermount3 that did mount now unmounts
		select {
		case <-h.done:
			return nil, fmt.Errorf("%s %s: %w: %v: %s", bin, point, err, h.cmd.ProcessState, strings.TrimSpace(h.stderr.String()))
		case <-time.After(capUnmountTimeout):
			return nil, fmt.Errorf("%s %s: %w, and it did not exit", bin, point, err)
		}
	}
	c := &capConn{point: point, fd: fd, helper: h, auto: autoUnmount}
	if !autoUnmount {
		c.closeComm(capUnmountTimeout) // fusermount3 exits once it has sent the descriptor
	}
	return c, nil
}

// closeComm closes the mounting end, and waits at most d for fusermount3 to exit. With auto_unmount, it unmounts the mount first if it
// is still there, so a live one must be unmounted before this.
func (c *capConn) closeComm(d time.Duration) error {
	c.helper.comm.Close()
	select {
	case <-c.helper.done:
		return nil
	case <-time.After(d):
		return fmt.Errorf("fusermount3 for %s did not exit within %s of its socket closing", c.point, d)
	}
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
