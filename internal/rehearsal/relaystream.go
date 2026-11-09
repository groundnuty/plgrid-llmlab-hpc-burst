package rehearsal

// New. The streaming half of the relay: the forced command's side and the
// server's side of an srun line (wire.go, Streaming). The relay of HBEP-6's
// host-dialed mode runs "srun -u --overlap --jobid=<id> --ntasks=1 <agent>
// relay <channel>" through this login node and carries one multiplexed
// session over the step's standard input and output for the job's whole life,
// which the one request and one reply of relay.go cannot carry.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// isSrunLine reports whether the command's first word is srun, the one command
// the login node streams.
func isSrunLine(line string) bool {
	f := strings.Fields(line)
	return len(f) > 0 && f[0] == "srun"
}

// relayStreamClient is the forced command for an srun line. It is full duplex:
// standard input goes to the server as it arrives, and the step's output comes
// back to standard output and error as it arrives. On the end of standard
// input it sends an end frame and goes on reading, so the output of a step
// that has seen its input end is not lost. It returns the step's exit code, or
// 255 when the server's socket closes or ssh has gone away.
func relayStreamClient(sock, line string) int {
	// A write to an ssh that went away must fail with EPIPE, so the exit is
	// 255, instead of ending this process with SIGPIPE.
	signal.Ignore(syscall.SIGPIPE)
	conn, err := net.DialTimeout("unix", sock, dialTimeout)
	if err != nil {
		return clientFail("the test process is not listening: %v", err)
	}
	defer conn.Close()
	if err := writeStreamRequest(conn, line); err != nil {
		return clientFail("send the command: %v", err)
	}
	go copyStdin(conn)
	go watchSession(conn)
	for {
		kind, p, err := readFrame(conn)
		if err != nil {
			return clientFail("no more frames from the test process: %v", err)
		}
		var out *os.File
		switch kind {
		case frameStdout:
			out = os.Stdout
		case frameStderr:
			out = os.Stderr
		case frameExit:
			if len(p) != 4 {
				return clientFail("an exit frame of %d bytes, want 4", len(p))
			}
			return int(int32(binary.BigEndian.Uint32(p))) & 0xff
		default:
			return clientFail("a frame of kind %d from the test process", kind)
		}
		if _, err := out.Write(p); err != nil {
			return clientFail("ssh has gone away: %v", err)
		}
	}
}

// copyStdin sends standard input to the server in frames as it arrives, and an
// end frame when it ends (an end or a read error: either way no more bytes
// come). It is the only writer of the connection after the request.
func copyStdin(conn net.Conn) {
	buf := make([]byte, maxFrame)
	for {
		n, err := os.Stdin.Read(buf)
		if n > 0 {
			if writeFrame(conn, frameStdin, buf[:n]) != nil {
				return // the server is gone; the reading side will say so
			}
		}
		if err != nil {
			_ = writeFrame(conn, frameStdinEOF, nil)
			return
		}
	}
}

// watchSession closes conn when ssh has gone away, which is when the pipes
// that sshd gave this process lose their reader: a write end of a pipe polls
// as in error when no one reads it. A step that writes nothing would never
// show it by a write, and the end of standard input does not tell it from a
// client that merely closed its input (half-close). Closing conn is what the
// server reads as "the session ended", and it kills the step.
func watchSession(conn net.Conn) {
	fds := []unix.PollFd{{Fd: 1, Events: unix.POLLRDHUP}, {Fd: 2, Events: unix.POLLRDHUP}}
	for len(fds) > 0 {
		if _, err := unix.Poll(fds, -1); err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return
		}
		live := fds[:0]
		for _, f := range fds {
			switch {
			case f.Revents&unix.POLLNVAL != 0:
				// not open here: nothing to watch
			case f.Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLRDHUP) != 0:
				_ = conn.Close()
				return
			default:
				live = append(live, f)
			}
		}
		fds = live
	}
}

// handleStream runs an srun line for one client. The step lives as long as
// the connection: when the client's side closes (ssh went away), the step's
// context ends and the fake kills it. close() ends it the same way. The
// hang-up is watched for apart from the frame reader (watchHangup), which
// cannot see it while the step has stopped taking its input and the reader
// waits to queue a frame.
func (r *relayServer) handleStream(c net.Conn, line string) {
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()
	fw := &frameWriter{w: c}
	stopWatch, err := watchHangup(c, cancel)
	if err != nil {
		// Without the watch a step could outlive its session: refuse the line.
		sendStreamEnd(fw, toReply(slurm.Result{}, fmt.Errorf("watch the connection for a hang-up: %w", err)))
		return
	}
	defer stopWatch()
	in, inW := io.Pipe()
	go readClientFrames(ctx, c, inW, cancel, r.backpressureHook())
	run := func() (slurm.Result, error) {
		code, err := r.sess.Stream(ctx, line, in, fw.data(frameStdout), fw.data(frameStderr))
		return slurm.Result{ExitCode: code}, err
	}
	var res slurm.Result
	if r.around != nil {
		res, err = r.around(line, run)
	} else {
		res, err = run()
	}
	_ = in.Close() // a Read of the step's input that is still blocked returns
	if ctx.Err() != nil {
		return // the client is gone or the server closes: there is nobody to answer
	}
	sendStreamEnd(fw, toReply(res, err))
}

// sendStreamEnd sends what the step left on standard output and error, then
// the exit frame, the last frame of a stream. A write that fails ends it: the
// client is gone.
func sendStreamEnd(fw *frameWriter, rep reply) {
	for _, f := range []struct {
		kind byte
		p    []byte
	}{{frameStdout, rep.stdout}, {frameStderr, rep.stderr}} {
		if len(f.p) > 0 {
			if _, err := fw.data(f.kind).Write(f.p); err != nil {
				return
			}
		}
	}
	var code [4]byte
	binary.BigEndian.PutUint32(code[:], uint32(rep.exit))
	_ = fw.frame(frameExit, code[:])
}

// readClientFrames feeds the step's input from the client's frames and ends
// the stream when the client goes away. It reads on after the end frame, since
// that is how a client that has gone shows it, and a frame of the wrong kind or
// one after the end frame ends the stream. The input is queued to a goroutine
// of its own, so a step that stops reading its input does not keep this one
// from seeing the connection close; once the queue is full it waits, which is
// the backpressure. A reader that waits does not read the end of file either,
// so a hang-up then is watchHangup's to find: it ends ctx, which frees the
// wait. onFull, when set, is called when the queue is full and the reader is
// about to wait (a test's way to know that it does).
func readClientFrames(ctx context.Context, c net.Conn, inW *io.PipeWriter, cancel context.CancelFunc, onFull func()) {
	queue := make(chan []byte, 64)
	go func() {
		failed := false
		for p := range queue {
			if !failed {
				if _, err := inW.Write(p); err != nil {
					failed = true // keep draining, so the reader is never blocked on us
				}
			}
		}
		_ = inW.Close() // the end of the client's input; a no-op after an error close
	}()
	queueClosed := false
	defer func() {
		if !queueClosed {
			close(queue)
		}
	}()
	for {
		kind, p, err := readFrame(c)
		if err != nil {
			cancel() // before the input ends, so the step is killed, not given an end of input
			_ = inW.CloseWithError(err)
			return
		}
		switch {
		case kind == frameStdin && !queueClosed:
			select {
			case queue <- p:
			default: // full: wait for room, or for the end of the stream
				if onFull != nil {
					onFull()
				}
				select {
				case queue <- p:
				case <-ctx.Done():
					return
				}
			}
		case kind == frameStdinEOF && !queueClosed:
			queueClosed = true
			close(queue)
		default:
			cancel()
			_ = inW.CloseWithError(errors.New("rehearsal login node: a frame of the wrong kind or after the end of input"))
			return
		}
	}
}
