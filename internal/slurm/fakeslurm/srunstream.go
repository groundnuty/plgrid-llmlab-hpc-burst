package fakeslurm

// New. Stream: an srun line that runs as a duplex stream instead of one
// request and one reply (issue #298, HBEP-6's host-dialed mode: a relay runs
// "srun -u --overlap --jobid=<id> --ntasks=1 <agent> relay <channel>" on the
// login node and carries one multiplexed session over the step's standard
// input and output for the job's whole life). The step is the one srunstep.go
// starts; only its input, its output and its end differ from the buffered
// srun of Run.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// streamChunk is the most one read of a step's streams or of Stream's input
// takes, so nothing is buffered beyond one read.
const streamChunk = 32 << 10

// maxHeldLine bounds what a step's standard output may leave without a
// newline before the fake sends it on anyway (Stream, without -u). A program
// that never writes a newline would otherwise grow the held bytes without
// limit; real srun's own limit is not modelled.
const maxHeldLine = 1 << 20

// stepStream is what a streamed step reads and writes.
type stepStream struct {
	ctx      context.Context // the Stream call's: once it ends, a stuck consumer is let go
	in       io.Reader
	out, err io.Writer
	// unbuf is -u: standard output is passed on as it arrives. Without it,
	// the bytes after the last newline are held until the next newline or the
	// end of standard output.
	unbuf bool
	// afterRead is runKnobs.streamRead.
	afterRead func(fd, n int)
}

// streamState is the part of a streamed step that its copy goroutines and
// its finish share.
type streamState struct {
	draining, abandoned atomic.Bool
	stdinBytes          atomic.Int64 // bytes the step's standard input got

	mu            sync.Mutex
	inErr, outErr error // the first read error of the input, the first write error of an output
}

func (x *streamState) fail(slot *error, err error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if *slot == nil {
		*slot = err
	}
}

func (x *streamState) firstErr() error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.inErr != nil {
		return fmt.Errorf("reading the step's standard input: %w", x.inErr)
	}
	if x.outErr != nil {
		return fmt.Errorf("writing the step's output: %w", x.outErr)
	}
	return nil
}

// pumpIn feeds the step's standard input from r as bytes arrive, and closes
// it at r's end. A Read of r that is blocked when the step has ended is not
// waited for: finish closes the pipe, so the next write ends the copy.
func (st *srunStep) pumpIn(w *os.File, r io.Reader) {
	defer close(st.inDone)
	defer w.Close()
	b := make([]byte, streamChunk)
	for {
		n, err := r.Read(b)
		if n > 0 {
			if _, werr := w.Write(b[:n]); werr != nil {
				return // the program ended or closed its input: nothing is wrong with r
			}
			st.sx.stdinBytes.Add(int64(n))
		}
		if err != nil {
			if err != io.EOF {
				st.sx.fail(&st.sx.inErr, err)
			}
			return
		}
	}
}

// pumpOut copies the step's output stream (fd 1 or 2) to w as it arrives. With
// hold, it keeps the bytes after the last newline back until a newline
// arrives or the stream ends, as srun line-buffers standard output when it is
// not given -u. It returns when the stream ends: end of file, or, once the
// step's processes are gone (draining), a pause of stepPipeGrace with nothing
// to read, which a process that escaped the sweep and holds the pipe causes.
func (st *srunStep) pumpOut(r *os.File, w io.Writer, hold bool, fd int, done chan<- struct{}) {
	defer close(done)
	b := make([]byte, streamChunk)
	var held []byte
	failed := false
	send := func(p []byte) {
		if len(p) == 0 || failed || st.sx.abandoned.Load() {
			return
		}
		if _, err := w.Write(p); err != nil {
			failed = true // keep reading, so the step is not blocked on a full pipe
			st.sx.fail(&st.sx.outErr, err)
		}
	}
	for {
		if st.sx.draining.Load() {
			_ = r.SetReadDeadline(time.Now().Add(stepPipeGrace)) // progress renews it
		}
		n, err := r.Read(b)
		if n > 0 {
			chunk := b[:n]
			switch i := bytes.LastIndexByte(chunk, '\n'); {
			case !hold:
				send(chunk)
			case i < 0:
				held = append(held, chunk...)
				if len(held) >= maxHeldLine {
					send(held)
					held = held[:0]
				}
			default:
				held = append(held, chunk[:i+1]...)
				send(held)
				held = append(held[:0], chunk[i+1:]...)
			}
			if h := st.stream.afterRead; h != nil {
				h(fd, n)
			}
		}
		if err != nil {
			break // end of file, the deadline, or finish closed the pipe
		}
	}
	send(held) // the stream ended: what is held goes now
}

// closeStream is closeAll for a streamed step, called once the step's
// processes are gone (or not known to be, which the outcome says). It lets the
// output copies deliver what the pipes still hold and ends them, then closes
// the fake's ends of the pipes. It waits for both output copies, but not for a
// Read of the input that is blocked. A consumer that has stopped reading is
// waited for only until the Stream call's context has ended and stepPipeGrace
// has passed; its copy is then abandoned: no later write to it starts, though
// one that is in flight may still complete.
func (st *srunStep) closeStream() {
	st.sx.draining.Store(true)
	dl := time.Now().Add(stepPipeGrace)
	_ = st.outR.SetReadDeadline(dl)
	_ = st.errR.SetReadDeadline(dl)
	dry := make(chan struct{})
	go func() {
		<-st.outDone
		<-st.errDone
		close(dry)
	}()
	select {
	case <-dry:
	case <-st.stream.ctx.Done():
		grace := time.NewTimer(stepPipeGrace)
		defer grace.Stop()
		select {
		case <-dry:
		case <-grace.C:
			st.sx.abandoned.Store(true)
		}
	}
	_ = st.stdin.Close()
	_ = st.outR.Close()
	_ = st.errR.Close()
}

// Stream runs one srun line as a duplex stream. It is Run for the one command
// the relay of HBEP-6's host-dialed mode needs, with a step that lives as long
// as its caller wants it:
//
//   - The line is exactly one simple command whose first word is srun:
//     "srun [-u] --overlap --jobid=<id> [--ntasks=1] [--time=<t>] <program>
//     [args...]" (doc.go has the grammar). Anything else is refused with
//     the fake's own codes and a message on stderr, and nothing runs: a line
//     that does not parse is exit 2, one that is a chain (" && "), carries a
//     redirection or whose first word is not srun is exit 1. A refused line is
//     not in the call log. Faults (FailNext) and gates (Gate("srun")) apply as
//     for Run, and so do srun's own checks (run mode, the job, the program),
//     which refuse as they do there, with their exit codes on stderr.
//   - The step's standard input is fed from stdin as bytes arrive, in reads of
//     at most 32 KiB, and the end of stdin closes the step's input: half-close
//     semantics, the step's output goes on. A nil stdin is an empty one. Stream
//     does not wait for a Read of stdin that is blocked after the step has
//     ended; a caller that needs it to return closes its source. The step's
//     standard output and error are written to stdout and stderr as they
//     arrive, with no cap on the total, from two goroutines of their own (so
//     the two must not be one writer that is unsafe for concurrent use). A nil
//     writer discards. A write that fails does not stop the step: its output
//     is read on and dropped, and Stream returns the first failure as a runner
//     error wrapping slurm.ErrTransport once the step has ended, as it does a
//     read of stdin that failed (other than with io.EOF).
//   - Without -u, standard output is line-buffered, as srun's is: the bytes
//     after the last newline are held until a newline arrives or the step's
//     standard output ends (a held line is sent on at 1 MiB, to bound the
//     memory). This is what the probe of 9 Nov 2026 found on a real cluster:
//     without -u, srun held output that ends without a newline until the
//     stream closed. With -u nothing is held. Standard error passes straight
//     through either way.
//   - There is no Command.Timeout and no time bound but --time. The call ends
//     when the program exits (its exit status is the result's, 128 plus the
//     signal for a signal); when ctx ends (the step and everything left in its
//     session get KILL, and the error is that of the context, as Run's:
//     slurm.ErrTimeout for a deadline, slurm.ErrTransport for a cancellation);
//     when the job's batch shell exits (exit 1, "srun: error: Job step
//     aborted"); or when --time passes (exit 1, "STEP CANCELLED DUE TO TIME
//     LIMIT"), in RunMode.Minute.
//   - mu is not held while the step runs. Stream returns only when no process
//     of the step is left, with the limit of a job's (srun's doc.go entry),
//     and both output copies have finished. The one exception is a consumer
//     that stops reading while ctx ends: its copy is let go after two seconds
//     (closeStream), so a stuck consumer cannot hold Stream for ever; a write
//     already in flight to it may complete after Stream has returned, and no
//     later one starts.
//
// The exit code is -1 whenever the error is not nil. The call's Call has the
// line's words; its StdinLen is the number of bytes the step's standard input
// has got, final when Stream returns (0 while the stream runs).
func (s *Session) Stream(ctx context.Context, line string, stdin io.Reader, stdout, stderr io.Writer) (exitCode int, err error) {
	p := s.p
	p.enter()
	defer p.leave()
	if stdin == nil {
		stdin = bytes.NewReader(nil)
	}
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	if err := ctx.Err(); err != nil {
		return -1, fmt.Errorf("%w: %v", slurm.ErrNotStarted, err)
	}
	if err := p.takeFault(s, line); err != nil {
		return -1, err
	}
	cmds, perr := parseLine(line)
	if perr != nil {
		fmt.Fprintf(stderr, "bash: %s\n", perr)
		return 2, nil
	}
	if msg := streamRefusal(cmds); msg != "" {
		fmt.Fprintf(stderr, "fakeslurm: Stream: %s\n", msg)
		return 1, nil
	}
	if err := p.waitGate(ctx, "srun"); err != nil {
		return -1, err
	}
	var res streamResult
	func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		res = p.streamLocked(ctx, s, cmds[0], &stepStream{ctx: ctx, in: stdin, out: stdout, err: stderr})
	}()
	if res.err != nil {
		return -1, res.err
	}
	if !res.abandoned {
		// What a refusal or the end of the step said. A failing write is the
		// consumer's: the step's result stands.
		if len(res.stdout) > 0 {
			_, _ = stdout.Write(res.stdout)
		}
		if len(res.stderr) > 0 {
			_, _ = stderr.Write(res.stderr)
		}
	}
	return res.code, nil
}

// streamRefusal is why a parsed line is not one simple srun command, or "".
func streamRefusal(cmds []simpleCmd) string {
	switch {
	case len(cmds) == 0:
		return "an empty line: only one srun command streams"
	case len(cmds) > 1:
		return "a chain of commands (&&): only one srun command streams"
	case cmds[0].words[0] != "srun":
		return fmt.Sprintf("%q is not srun: only srun streams", cmds[0].words[0])
	case cmds[0].redirected:
		return "a redirection: only a plain srun command streams"
	}
	return ""
}

// streamResult is what a streamed line leaves: its exit code, the messages the
// fake itself wrote (standard output stays empty today), a runner error, and
// whether a consumer's copy was let go.
type streamResult struct {
	code           int
	stdout, stderr []byte
	err            error
	abandoned      bool
}

// streamLocked logs the call and runs the line with mu held, which the step
// lets go of while it runs.
func (p *Platform) streamLocked(ctx context.Context, s *Session, c simpleCmd, sio *stepStream) streamResult {
	sh := &shell{p: p, s: s, ctx: ctx, now: p.clock.Now(), umask: 0o022}
	p.logCall(s, c, 0, sh.now)
	idx := len(p.calls) - 1
	code, st := sh.srunStream(c.words[1:], sio)
	var res streamResult
	if st != nil {
		p.calls[idx].StdinLen = int(st.sx.stdinBytes.Load())
		res.abandoned = st.sx.abandoned.Load()
	}
	res.code, res.err = code, sh.runErr
	res.stdout, res.stderr = sh.stdout.Bytes(), sh.stderr.Bytes()
	return res
}

// srunStream is srun for a streamed step. It returns the exit code and the
// step, nil when nothing was started.
func (sh *shell) srunStream(args []string, sio *stepStream) (int, *srunStep) {
	pl, code, ok := sh.planStep(args)
	if !ok {
		return code, nil
	}
	p := sh.p
	sio.unbuf, sio.afterRead = pl.a.unbuf, p.knobs.streamRead
	spec := pl.spec(p)
	spec.stream = sio
	st, err := startStep(spec)
	if err != nil {
		return sh.fail(1, "srun: error: the step could not start: %v", err), nil
	}
	var out stepOutcome
	func() {
		p.mu.Unlock() // the step runs for as long as its caller wants
		defer p.mu.Lock()
		out = st.finish(sh.ctx, 0, time.Duration(pl.a.minutes)*p.minute(), pl.r.exited)
	}()
	sh.now = p.clock.Now()
	code = sh.stepResult(out, 0)
	if err := st.sx.firstErr(); err != nil && sh.runErr == nil {
		sh.runErr = fmt.Errorf("%w: fakeslurm: srun: %v", slurm.ErrTransport, err)
	}
	return code, st
}
