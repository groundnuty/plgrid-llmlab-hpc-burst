package rehearsal

// New. The relay: the forced command of the login node (a re-run of the test
// binary, through Dispatch) and the server in the test process that runs each
// command line against the fake Slurm platform.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// The environment of the forced command.
const (
	// EnvRole selects the role; RoleRelay is the login node's forced command.
	EnvRole   = "HB_REHEARSAL_ROLE"
	RoleRelay = "slurm-relay"
	// EnvRelay is the unix socket of the relay server in the test process.
	EnvRelay = "HB_REHEARSAL_RELAY"
)

// Dispatch makes a test binary the login node's forced command. Call it first
// thing in TestMain, before the test flags are parsed. When HB_REHEARSAL_ROLE
// is "slurm-relay", it relays the command sshd put in SSH_ORIGINAL_COMMAND,
// with all of standard input, to the test process, writes the command's
// output, and exits with its exit code. Otherwise it returns, and the tests
// run.
func Dispatch() {
	if os.Getenv(EnvRole) == RoleRelay {
		os.Exit(relayClient(os.Getenv(EnvRelay)))
	}
}

// failExit is the exit code of ssh's own failures, and of a command the relay
// could not run. sshrun reads it as a transport error.
const failExit = 255

// dialTimeout bounds the connection to the relay server; the server is a
// local socket that is either listening or gone.
const dialTimeout = 10 * time.Second

// relayClient is the forced command: it returns the process's exit code.
func relayClient(sock string) int {
	fail := func(format string, a ...any) int {
		fmt.Fprintf(os.Stderr, "rehearsal login node: "+format+"\n", a...)
		return failExit
	}
	line, ok := os.LookupEnv("SSH_ORIGINAL_COMMAND")
	switch {
	case sock == "":
		return fail("%s is not set", EnvRelay)
	case !ok:
		return fail("no command: only commands are offered, not a shell")
	}
	stdin, err := io.ReadAll(io.LimitReader(os.Stdin, maxField+1))
	if err != nil {
		return fail("read standard input: %v", err)
	}
	if len(stdin) > maxField {
		return fail("standard input is above the limit of %d bytes", maxField)
	}
	conn, err := net.DialTimeout("unix", sock, dialTimeout)
	if err != nil {
		return fail("the test process is not listening: %v", err)
	}
	defer conn.Close()
	if err := writeRequest(conn, request{line: line, stdin: stdin}); err != nil {
		return fail("send the command: %v", err)
	}
	p, err := readReply(conn)
	if err != nil {
		return fail("no reply from the test process: %v", err)
	}
	// A write error here means ssh went away; there is nobody to tell.
	_, _ = os.Stdout.Write(p.stdout)
	_, _ = os.Stderr.Write(p.stderr)
	return int(p.exit) & 0xff
}

// relayServer runs the requests it receives as the session of one user on the
// platform. It never closes the platform.
type relayServer struct {
	ln     net.Listener
	sess   *fakeslurm.Session
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

// readTimeout bounds how long the server waits for a request that a client
// started and did not finish.
const readTimeout = 60 * time.Second

func startRelay(sock string, sess *fakeslurm.Session) (*relayServer, error) {
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return nil, fmt.Errorf("listen on the relay socket: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &relayServer{ln: ln, sess: sess, ctx: ctx, cancel: cancel, conns: map[net.Conn]struct{}{}}
	r.wg.Add(1)
	go r.accept()
	return r, nil
}

func (r *relayServer) accept() {
	defer r.wg.Done()
	for {
		c, err := r.ln.Accept()
		if err != nil {
			return
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			c.Close()
			return
		}
		r.conns[c] = struct{}{}
		r.wg.Add(1)
		r.mu.Unlock()
		go r.handle(c)
	}
}

func (r *relayServer) handle(c net.Conn) {
	defer r.wg.Done()
	defer func() {
		r.mu.Lock()
		delete(r.conns, c)
		r.mu.Unlock()
		c.Close()
	}()
	_ = c.SetReadDeadline(time.Now().Add(readTimeout))
	q, err := readRequest(c)
	if err != nil {
		return // the client went away or sent garbage: no reply to give
	}
	_ = c.SetReadDeadline(time.Time{})
	var stdin []byte
	if len(q.stdin) > 0 {
		stdin = q.stdin
	}
	res, err := r.sess.Run(r.ctx, slurm.Command{Line: q.line, Stdin: stdin})
	_ = writeReply(c, toReply(res, err))
}

// toReply turns a runner outcome into the reply: the command's own result, or
// exit 255 and one line of standard error for a runner error.
func toReply(res slurm.Result, err error) reply {
	if err == nil {
		return reply{exit: int32(res.ExitCode), stdout: res.Stdout, stderr: res.Stderr}
	}
	kind := "runner error"
	switch {
	case errors.Is(err, slurm.ErrNotStarted):
		kind = "command never started"
	case errors.Is(err, slurm.ErrTimeout):
		kind = "command timed out"
	case errors.Is(err, slurm.ErrTransport):
		kind = "transport error"
	}
	msg := strings.Join(strings.Fields(err.Error()), " ")
	return reply{exit: failExit, stderr: []byte("rehearsal login node: " + kind + ": " + msg + "\n")}
}

// close stops the listener, ends every request (one blocked at a gate comes
// free) and waits for the handlers.
func (r *relayServer) close() {
	r.mu.Lock()
	r.closed = true
	for c := range r.conns {
		c.Close()
	}
	r.mu.Unlock()
	r.cancel()
	r.ln.Close()
	r.wg.Wait()
}
