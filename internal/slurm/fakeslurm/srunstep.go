package fakeslurm

// New. Run mode: the process of an srun step. It is started in a session of
// its own, as a job is, and ended the way the job's batch shell is
// (runwait.go): wait for its exit without reaping it, so the PID stays the
// session ID, then kill whatever is left in the session (sweepSession), then
// reap. Its standard streams are pipes the fake reads itself, with a cap,
// rather than files or a bytes.Buffer behind exec's copying goroutines, which
// wait for a grandchild that holds the other end open.

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// maxStepOutput bounds what a step may write to each of its standard output
// and error: the VM has no swap, and a program that never stops writing would
// fill the memory before its deadline. A step over it is killed.
const maxStepOutput = 16 << 20

// stepPipeGrace is how long the fake waits, after the step's processes are
// gone, for the pipes to run dry. A process that escaped the sweep (it called
// setsid and its parent died) can hold a pipe open for ever; the fake then
// closes its end and returns what it has.
const stepPipeGrace = 2 * time.Second

// stepEnd says why a step ended.
type stepEnd int

const (
	stepExited     stepEnd = iota // the program exited, or a signal ended it
	stepCtxEnded                  // the Run call's context or Command.Timeout ended
	stepLimited                   // --time passed
	stepJobEnded                  // the job's batch shell exited
	stepOverOutput                // the step wrote more than maxStepOutput
)

// stepSpec is what startStep needs, all read from the job under mu.
type stepSpec struct {
	path, argv0 string
	args        []string
	dir         string
	env         []string
	// stdin is the step's whole standard input, for a buffered step; stream,
	// when set, replaces it and the output caps (srunstream.go).
	stdin  []byte
	stream *stepStream

	proc                      *procAPI
	sweepLimit, sweepInterval time.Duration
}

// stepOutcome is what a finished step leaves.
type stepOutcome struct {
	end            stepEnd
	status         syscall.WaitStatus // for stepExited
	stdout, stderr []byte
	// err is why the step's processes are not known to be gone (a sweep that
	// failed, a wait that failed).
	err error
}

// streamCap collects one stream of a step, up to maxStepOutput bytes; it keeps
// reading after that, and drops what it reads, so the program is never
// blocked on a full pipe.
type streamCap struct {
	mu   sync.Mutex
	buf  []byte
	over bool
}

func (c *streamCap) bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.buf...)
}

// drain reads r to its end or its close, then closes done. onOver is called
// once, when the cap is first passed.
func (c *streamCap) drain(r *os.File, onOver func(), done chan<- struct{}) {
	defer close(done)
	b := make([]byte, 32<<10)
	for {
		n, err := r.Read(b)
		if n > 0 {
			c.mu.Lock()
			first := false
			if room := maxStepOutput - len(c.buf); n <= room {
				c.buf = append(c.buf, b[:n]...)
			} else {
				c.buf = append(c.buf, b[:room]...)
				first, c.over = !c.over, true
			}
			c.mu.Unlock()
			if first {
				onOver()
			}
		}
		if err != nil {
			return // EOF, or the fake closed its end
		}
	}
}

// srunStep is a started step.
type srunStep struct {
	spec           stepSpec
	cmd            *exec.Cmd
	pid            int
	stdin          *os.File // the fake's end of the step's standard input
	outR           *os.File // the fake's ends of its standard output and error
	errR           *os.File
	outCap, errCap streamCap

	overOnce                 sync.Once
	over                     chan struct{} // closed when a stream passes the cap
	inDone, outDone, errDone chan struct{}

	// A streamed step (srunstream.go): spec.stream is set, outCap and errCap
	// stay empty, and over never closes.
	stream *stepStream
	sx     streamState
}

// startStep starts the program in a session of its own, with the given
// environment and working directory and the given standard input. The caller
// holds mu, which keeps the job from ending between its check and the start.
func startStep(spec stepSpec) (*srunStep, error) {
	inR, inW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		return nil, errors.Join(err, inR.Close(), inW.Close())
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		return nil, errors.Join(err, inR.Close(), inW.Close(), outR.Close(), outW.Close())
	}
	cmd := exec.Command(spec.path)
	cmd.Args = append([]string{spec.argv0}, spec.args...)
	cmd.Dir, cmd.Env = spec.dir, spec.env
	cmd.Stdin, cmd.Stdout, cmd.Stderr = inR, outW, errW // files: exec starts no copying goroutine
	cmd.SysProcAttr = sessionAttr()
	startErr := startProc(cmd)
	_ = errors.Join(inR.Close(), outW.Close(), errW.Close()) // the child has its own copies
	if startErr != nil {
		return nil, errors.Join(startErr, inW.Close(), outR.Close(), errR.Close())
	}
	st := &srunStep{
		spec: spec, cmd: cmd, pid: cmd.Process.Pid, stdin: inW, outR: outR, errR: errR,
		over:   make(chan struct{}),
		inDone: make(chan struct{}), outDone: make(chan struct{}), errDone: make(chan struct{}),
		stream: spec.stream,
	}
	if st.stream != nil {
		go st.pumpIn(inW, st.stream.in)
		go st.pumpOut(outR, st.stream.out, !st.stream.unbuf, 1, st.outDone)
		go st.pumpOut(errR, st.stream.err, false, 2, st.errDone)
		return st, nil
	}
	onOver := func() { st.overOnce.Do(func() { close(st.over) }) }
	go func() { // a program that never reads its input leaves this blocked until finish closes the pipe
		defer close(st.inDone)
		_, _ = inW.Write(spec.stdin)
		_ = inW.Close()
	}()
	go st.outCap.drain(outR, onOver, st.outDone)
	go st.errCap.drain(errR, onOver, st.errDone)
	return st, nil
}

// finish follows the step to its end and cleans up, and returns what it left.
// It is called with mu released. The step ends when its program exits, when
// ctx or timeout (0 for none, a streamed step's) ends the call, when limit
// (--time, 0 for none) passes, when jobGone is closed (the job's batch shell
// exited: a Slurm step does not outlive its job) or when it passes the output
// cap. Whatever the reason, no process of the step is left when it returns,
// unless out.err says so.
func (st *srunStep) finish(parent context.Context, timeout, limit time.Duration, jobGone <-chan struct{}) stepOutcome {
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, timeout)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}
	defer cancel()
	exited := make(chan error, 1)
	go func() { exited <- waitExit(st.pid) }()
	var limitC <-chan time.Time
	if limit > 0 {
		t := time.NewTimer(limit)
		defer t.Stop()
		limitC = t.C
	}
	out := stepOutcome{end: stepExited}
	joined := false
	select {
	case err := <-exited:
		joined = true
		if err != nil {
			return st.abandon(out, err)
		}
	case <-ctx.Done():
		out.end = stepCtxEnded
	case <-limitC:
		out.end = stepLimited
	case <-jobGone:
		out.end = stepJobEnded
	case <-st.over:
		out.end = stepOverOutput
	}
	// Kill what is left in the session, the program itself included when it
	// still runs. The program stays a zombie, or alive until this kills it, so
	// its PID is the session ID throughout.
	if err := sweepSession(st.spec.proc, st.pid, st.spec.sweepLimit, st.spec.sweepInterval); err != nil {
		out.err = err
		st.closeAll()
		return out // the program may live on, so it is not waited for or reaped
	}
	if !joined {
		if err := <-exited; err != nil { // killed, so the wait returns
			return st.abandon(out, err)
		}
	}
	_ = st.cmd.Wait() // reaps; the status is in ProcessState
	if ps := st.cmd.ProcessState; ps != nil {
		out.status, _ = ps.Sys().(syscall.WaitStatus)
	}
	st.closeAll()
	out.stdout, out.stderr = st.outCap.bytes(), st.errCap.bytes()
	if out.end == stepExited && (st.outCap.over || st.errCap.over) {
		out.end = stepOverOutput
	}
	return out
}

// abandon ends a step whose exit could not be waited for: the program is
// killed through its handle and reaped, and nothing is known about the rest of
// its session (a zombie that was not reaped is the only anchor of the session
// ID, and this is not that case), so the outcome says so. As with the batch
// shell's waitFailed, waitid on a child does not fail and no test reaches it.
func (st *srunStep) abandon(out stepOutcome, cause error) stepOutcome {
	_ = st.cmd.Process.Kill()
	_ = st.cmd.Wait()
	st.closeAll()
	out.err = cause
	return out
}

// closeAll closes the fake's ends of the pipes, which unblocks the three
// goroutines on them, and waits for those goroutines, after a short grace for
// the streams to run dry when the step's processes have all exited.
func (st *srunStep) closeAll() {
	if st.stream != nil {
		st.closeStream()
		return
	}
	dry := make(chan struct{})
	go func() {
		<-st.outDone
		<-st.errDone
		close(dry)
	}()
	grace := time.NewTimer(stepPipeGrace)
	defer grace.Stop()
	select {
	case <-dry:
	case <-grace.C:
	}
	_ = st.stdin.Close()
	_ = st.outR.Close()
	_ = st.errR.Close()
	<-st.inDone
	<-dry
}
