package rehearsal

// New. Peer hang-up detection for the relay server's streams (relaystream.go).
// The frame reader of a stream notices a client that went away by reading its
// end of file, but it is not always reading: when the step has stopped taking
// its input and the input queue is full, the reader waits to queue the next
// frame, and the end of file stays unread behind the frames the client sent. A
// hang-up is therefore watched for apart from the reader, with poll(2), which
// reports it even while data waits unread, and without reading a byte.

import (
	"errors"
	"net"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// watchHangup calls onHangup, once, when the peer of c has hung up: it closed
// its end or shut down its writing side, which poll reports as POLLRDHUP or
// POLLHUP whether or not data waits unread, or the socket is in error
// (POLLERR). It reads nothing from c and does not hold c's descriptor (a held
// descriptor would make c.Close wait for the watch). It watches a duplicate of
// the descriptor instead, which stays open, and keeps c's socket open with it,
// until stop is called; stop ends the watch and waits until it has ended, and
// may be called again. A poll that fails for any other reason is taken for a
// hang-up, since the peer can no longer be watched.
func watchHangup(c net.Conn, onHangup func()) (stop func(), err error) {
	sc, ok := c.(syscall.Conn)
	if !ok {
		return nil, errors.New("the connection has no descriptor to watch")
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return nil, err
	}
	dup := -1
	var dupErr error
	if err := rc.Control(func(fd uintptr) { dup, dupErr = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 0) }); err != nil {
		return nil, err
	}
	if dupErr != nil {
		return nil, dupErr
	}
	var wake [2]int // the write end is closed to end the watch
	if err := unix.Pipe2(wake[:], unix.O_CLOEXEC); err != nil {
		_ = unix.Close(dup)
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer unix.Close(dup)
		defer unix.Close(wake[0])
		// Only POLLRDHUP is asked for: POLLHUP, POLLERR and POLLNVAL are always
		// reported, and POLLIN is left out so that frames waiting unread do not
		// end the wait.
		fds := []unix.PollFd{{Fd: int32(dup), Events: unix.POLLRDHUP}, {Fd: int32(wake[0]), Events: unix.POLLIN}}
		for {
			if _, err := unix.Poll(fds, -1); err != nil {
				if errors.Is(err, unix.EINTR) {
					continue
				}
				onHangup()
				return
			}
			if fds[1].Revents != 0 {
				return // stop closed the write end
			}
			if fds[0].Revents != 0 { // anything poll reports without being asked is an end
				onHangup()
				return
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { _ = unix.Close(wake[1]) })
		<-done
	}, nil
}
