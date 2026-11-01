package main

// A test-only transport that carries slurm.Runner calls over loopback TCP, so
// that real controller processes (child_test.go) can share one in-memory fake
// Slurm (internal/slurm/fakeslurm) that lives in the test process.
//
// Protocol: one TCP connection per call. The client writes one JSON line, the
// request, and reads one JSON line, the reply. A reply that never comes is
// what a dropped ssh connection looks like, so a server may withhold it
// (runnerHooks) and the client then waits, as it would wait on ssh.
//
// What the client reports follows slurm.Runner's contract (runner.go) and what
// internal/slurm/sshrun does for the same cases:
//
//   - the connection cannot be made: ErrNotStarted, nothing reached the server;
//   - the connection fails after it was made, or ends before a reply (sshrun's
//     exit 255 or a signal): ErrTransport, since the command may have run;
//   - the caller's context ends while the command runs: ErrTransport;
//   - the command's own time-out ends it: ErrTimeout.
//
// An error the server's runner returns keeps its identity over the wire:
// encodeError lists which of the sentinels (wireKinds) it matches under
// errors.Is, and decodeError rebuilds an error that matches the same ones, so
// the provider's Unknown-or-Rejected decision (slurm.classifySubmit) is the one
// the in-process fake would have caused.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

const (
	wireOpRun        = "run"
	wireOpGeneration = "generation"
)

// wireRequest is the client's line.
type wireRequest struct {
	Op   string `json:"op"`
	Line string `json:"line,omitempty"`
	// Stdin is the command's standard input; HasStdin keeps nil (none) apart
	// from empty.
	Stdin    []byte `json:"stdin,omitempty"`
	HasStdin bool   `json:"hasStdin,omitempty"`
}

// wireResponse is the server's line.
type wireResponse struct {
	Stdout     []byte `json:"stdout,omitempty"`
	Stderr     []byte `json:"stderr,omitempty"`
	ExitCode   int    `json:"exitCode"`
	Generation uint64 `json:"generation,omitempty"`
	// Failed says the runner returned an error: Kinds are the wireKinds codes
	// it matches and Msg is its text.
	Failed bool     `json:"failed,omitempty"`
	Kinds  []string `json:"kinds,omitempty"`
	Msg    string   `json:"msg,omitempty"`
}

// wireKinds are the errors whose identity crosses the wire.
var wireKinds = []struct {
	code string
	err  error
}{
	{"not-started", slurm.ErrNotStarted},
	{"timeout", slurm.ErrTimeout},
	{"transport", slurm.ErrTransport},
	{"canceled", context.Canceled},
	{"deadline", context.DeadlineExceeded},
}

// encodeError returns the codes of every wireKinds error that err matches, and
// its text. An error that matches none travels as text alone.
func encodeError(err error) (kinds []string, msg string) {
	for _, k := range wireKinds {
		if errors.Is(err, k.err) {
			kinds = append(kinds, k.code)
		}
	}
	return kinds, err.Error()
}

// wireError is an error rebuilt from the wire: it has the text of the original
// and matches, under errors.Is, the sentinels the original matched.
type wireError struct {
	msg   string
	kinds []error
}

func (e *wireError) Error() string   { return e.msg }
func (e *wireError) Unwrap() []error { return e.kinds }

// decodeError is encodeError's inverse. A code it does not know is named in the
// text, never dropped silently.
func decodeError(kinds []string, msg string) error {
	e := &wireError{msg: msg}
	for _, code := range kinds {
		known := false
		for _, k := range wireKinds {
			if k.code == code {
				e.kinds = append(e.kinds, k.err)
				known = true
			}
		}
		if !known {
			e.msg += fmt.Sprintf(" [unknown error kind %q on the wire]", code)
		}
	}
	return e
}

// runnerHooks let a test hold a command at the server. A hook that returns
// true withholds the reply: the connection stays open, with no answer, until
// the client goes away or the server closes.
type runnerHooks struct {
	// before runs when a command arrives, before the runner sees it; a
	// withheld command is never run.
	before func(cmd slurm.Command) (withhold bool)
	// after runs once the runner has run the command (the job is registered),
	// before the reply is sent.
	after func(cmd slurm.Command, res slurm.Result, err error) (withhold bool)
}

// runnerServer serves a slurm.Runner on a loopback port the kernel chose.
type runnerServer struct {
	ln     net.Listener
	runner slurm.Runner
	hooks  runnerHooks
	// ctx is the server's lifetime: every command runs under a context
	// derived from it, which also ends when its client's connection does.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// serveRunner starts a server for r and closes it at the end of the test.
func serveRunner(t testing.TB, r slurm.Runner, h runnerHooks) *runnerServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &runnerServer{ln: ln, runner: r, hooks: h}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.wg.Add(1)
	go s.accept()
	t.Cleanup(s.Close)
	return s
}

// Addr is the host:port the server listens on.
func (s *runnerServer) Addr() string { return s.ln.Addr().String() }

// Close stops the server and waits for every handler, the withholding ones
// included.
func (s *runnerServer) Close() {
	s.cancel()
	_ = s.ln.Close()
	s.wg.Wait()
}

func (s *runnerServer) accept() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(conn)
		}()
	}
}

func (s *runnerServer) handle(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	line, err := br.ReadBytes('\n')
	if err != nil {
		return // the client left before a whole request: nothing ran
	}
	var req wireRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return
	}
	// The client sends nothing after its request, so this read ends only when
	// it closes the connection or dies: that ends the command's context.
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	go func() {
		_, _ = br.ReadByte()
		cancel()
	}()
	var resp wireResponse
	switch req.Op {
	case wireOpGeneration:
		resp.Generation = s.runner.Generation()
	case wireOpRun:
		cmd := slurm.Command{Line: req.Line}
		if req.HasStdin {
			cmd.Stdin = req.Stdin
			if cmd.Stdin == nil {
				cmd.Stdin = []byte{}
			}
		}
		if s.hooks.before != nil && s.hooks.before(cmd) {
			<-ctx.Done()
			return
		}
		res, err := s.runner.Run(ctx, cmd)
		if s.hooks.after != nil && s.hooks.after(cmd, res, err) {
			<-ctx.Done()
			return
		}
		resp = wireResponse{Stdout: res.Stdout, Stderr: res.Stderr, ExitCode: res.ExitCode}
		if err != nil {
			resp.Failed = true
			resp.Kinds, resp.Msg = encodeError(err)
		}
	default:
		resp = wireResponse{Failed: true, Msg: fmt.Sprintf("unknown operation %q", req.Op)}
	}
	_ = json.NewEncoder(conn).Encode(resp)
}

// remoteRunner is a slurm.Runner that calls a runnerServer.
type remoteRunner struct{ addr string }

var _ slurm.Runner = (*remoteRunner)(nil)

// call makes one round trip. dialed is false when the connection could not be
// made, so that nothing reached the server.
func (r *remoteRunner) call(ctx context.Context, req wireRequest) (resp wireResponse, dialed bool, err error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", r.addr)
	if err != nil {
		return resp, false, err
	}
	defer conn.Close()
	// The context's end closes the connection, which frees a blocked read.
	defer context.AfterFunc(ctx, func() { _ = conn.Close() })()
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return resp, true, err
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return resp, true, err
	}
	return resp, true, json.Unmarshal(line, &resp)
}

// Run implements slurm.Runner, with the error kinds the package comment lists.
func (r *remoteRunner) Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error) {
	timeout := cmd.Timeout
	if timeout <= 0 {
		timeout = provider.CommandTimeout
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, dialed, err := r.call(cctx, wireRequest{Op: wireOpRun, Line: cmd.Line, Stdin: cmd.Stdin, HasStdin: cmd.Stdin != nil})
	switch {
	case err != nil && !dialed:
		return slurm.Result{}, fmt.Errorf("%w: %v", slurm.ErrNotStarted, err)
	case err != nil && errors.Is(cctx.Err(), context.DeadlineExceeded) && ctx.Err() == nil:
		return slurm.Result{}, fmt.Errorf("%w after %s", slurm.ErrTimeout, timeout)
	case err != nil && ctx.Err() != nil:
		return slurm.Result{}, fmt.Errorf("%w: %v", slurm.ErrTransport, ctx.Err())
	case err != nil:
		return slurm.Result{}, fmt.Errorf("%w: the connection to the login node failed", slurm.ErrTransport)
	}
	res := slurm.Result{Stdout: resp.Stdout, Stderr: resp.Stderr, ExitCode: resp.ExitCode}
	if resp.Failed {
		return res, decodeError(resp.Kinds, resp.Msg)
	}
	return res, nil
}

// Generation implements slurm.Runner: the server's runner's generation, and 0
// when the server cannot be asked. Batch.Paths never caches under 0, so every
// path is then resolved again.
func (r *remoteRunner) Generation() uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, _, err := r.call(ctx, wireRequest{Op: wireOpGeneration})
	if err != nil || resp.Failed {
		return 0
	}
	return resp.Generation
}
