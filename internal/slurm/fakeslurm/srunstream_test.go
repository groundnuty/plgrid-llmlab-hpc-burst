package fakeslurm

// New. Tests of Session.Stream and of -u (srunstream.go, srun.go). The
// expected values come from the brief of issue #298 (a stream crosses both ways
// before any end of input, standard output is line-buffered without -u and
// not with it, no cap, how a stream ends, what it refuses), from the shell's
// conventions for exit codes and from the 9 Nov 2026 probe of real srun's
// buffering; none is read back from the fake. Every wait is on observable
// state (a file a step wrote, the copy's own count of what it has handled, a
// PID gone) with a generous deadline, never on a sleep. The hold of standard
// output is proved by the count the copy reports after it has handled a
// chunk: a marker file shows only that the program wrote, and a check made
// right after it could pass because the copy has not read yet.

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// streamLine is the line of HBEP-6's relay (agentapi.RelayLine), with -u or
// without, and the program's words after it.
func streamLine(id int, dashU bool, program string) string {
	u := ""
	if dashU {
		u = "-u "
	}
	return "srun " + u + "--overlap --jobid=" + itoa(id) + " --ntasks=1 " + program
}

// shScript is "sh -c" with the script as one quoted word.
func shScript(script string) string { return "sh -c " + quote(script) }

// sink records each Write as one chunk.
type sink struct {
	mu     sync.Mutex
	chunks []string
}

func (s *sink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks = append(s.chunks, string(p))
	return len(p), nil
}

func (s *sink) list() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.chunks...)
}

func (s *sink) text() string { return strings.Join(s.list(), "") }

// hashSink digests and counts what it is given and keeps none of it.
type hashSink struct {
	mu sync.Mutex
	d  hash.Hash
	n  int
}

func newHashSink() *hashSink { return &hashSink{d: sha256.New()} }

func (s *hashSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n += len(p)
	return s.d.Write(p)
}

func (s *hashSink) sum() ([]byte, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.d.Sum(nil), s.n
}

type streamMsg struct {
	code int
	err  error
}

// streamRun is a Stream call in a goroutine, with its standard input as a
// pipe the test writes to.
type streamRun struct {
	in          *io.PipeWriter
	out, errOut *sink
	done        <-chan streamMsg
	cancel      context.CancelFunc
}

// startStream runs the line as s and returns at once. The cleanup ends the
// call, so a failed test strands no step.
func startStream(t *testing.T, s *Session, line string) *streamRun {
	t.Helper()
	pr, pw := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	run := &streamRun{in: pw, out: &sink{}, errOut: &sink{}, cancel: cancel}
	done := make(chan streamMsg, 1)
	run.done = done
	t.Cleanup(func() {
		cancel()
		pw.Close()
	})
	go func() {
		code, err := s.Stream(ctx, line, pr, run.out, run.errOut)
		pr.Close()
		done <- streamMsg{code, err}
	}()
	return run
}

func (sr *streamRun) send(t *testing.T, s string) {
	t.Helper()
	if _, err := io.WriteString(sr.in, s); err != nil {
		t.Fatalf("writing %q to the step's input: %v", s, err)
	}
}

// result waits for Stream to return.
func (sr *streamRun) result(t *testing.T) streamMsg {
	t.Helper()
	m := recvChan(t, sr.done)
	return m
}

func timeAfterDeadline() <-chan time.Time { return time.After(waitDeadline) }

func recvChan[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-timeAfterDeadline():
		t.Fatalf("Stream did not return within %s", waitDeadline)
		var zero T
		return zero
	}
}

// returned reports, without waiting, whether Stream has returned.
func (sr *streamRun) returned() bool {
	select {
	case m := <-sr.done:
		sr.done = repeat(m)
		return true
	default:
		return false
	}
}

// repeat is a channel that always has m.
func repeat[T any](m T) <-chan T {
	ch := make(chan T, 1)
	ch <- m
	return ch
}

// reads counts the bytes a streamed step's copies have handled, by fd.
type reads struct{ out, err atomic.Int64 }

// countReads installs the knob that counts them; call it before the stream
// starts.
func (r *runRig) countReads() *reads {
	rd := &reads{}
	r.p.knobs.streamRead = func(fd, n int) {
		if fd == 1 {
			rd.out.Add(int64(n))
		} else {
			rd.err.Add(int64(n))
		}
	}
	return rd
}

// Bytes cross both ways before any end of input: what the test writes comes
// back from the step while the step still runs and Stream has not returned,
// and the end of input then ends the step with exit 0 (half-close).
func TestStreamCrossesBothWaysBeforeTheEndOfInput(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	sr := startStream(t, r.s, streamLine(id, true, "cat"))
	for _, piece := range []string{"hello", " ", "world\n"} {
		before := len(sr.out.text())
		sr.send(t, piece)
		waitFor(t, "the step to echo "+piece, func() bool { return len(sr.out.text()) >= before+len(piece) })
		if sr.returned() {
			t.Fatalf("Stream returned after %q, before the end of input", piece)
		}
	}
	if got := sr.out.text(); got != "hello world\n" {
		t.Fatalf("the step echoed %q before the end of input, want %q", got, "hello world\n")
	}
	if err := sr.in.Close(); err != nil { // the end of input
		t.Fatal(err)
	}
	if m := sr.result(t); m.err != nil || m.code != 0 {
		t.Errorf("Stream returned %+v, want exit 0 and no error", m)
	}
	if got := sr.out.text(); got != "hello world\n" || sr.errOut.text() != "" {
		t.Errorf("after the end: stdout %q, stderr %q; want %q and nothing", got, sr.errOut.text(), "hello world\n")
	}
}

// With -u a chunk that has no newline reaches the consumer while the step
// runs; without it, standard output is line-buffered and the chunk is held
// until a newline arrives (the 9 Nov 2026 probe: without -u, srun held
// output that ends without a newline until the stream closed). The copy's own
// count says it has handled the chunk before the test looks, so "nothing
// arrived" cannot be a copy that has not read yet.
func TestStreamHoldsStandardOutputWithoutDashU(t *testing.T) {
	for _, c := range []struct {
		name         string
		dashU        bool
		whileRunning []string // the consumer's chunks after the step wrote "a"
		afterNewline []string // and at the end, when the step has written "b\n"
	}{
		{"with -u", true, []string{"a"}, []string{"a", "b\n"}},
		{"without -u", false, nil, []string{"ab\n"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRunRig(t, RunMode{})
			id := r.holdJob(r.s, r.root+"/release")
			rd := r.countReads()
			marker := r.root + "/wrote"
			sr := startStream(t, r.s, streamLine(id, c.dashU,
				shScript(`printf a; echo w > `+marker+`; read g; printf "b\n"`)))
			r.waitFile(marker)
			waitFor(t, "the copy to handle the first chunk", func() bool { return rd.out.Load() >= 1 })
			if got := sr.out.list(); !sameChunks(got, c.whileRunning) {
				t.Fatalf("while the step runs, the consumer has %q; want %q", got, c.whileRunning)
			}
			sr.send(t, "go\n")
			if m := sr.result(t); m.err != nil || m.code != 0 {
				t.Fatalf("Stream returned %+v, want exit 0 and no error", m)
			}
			if got := sr.out.list(); !sameChunks(got, c.afterNewline) {
				t.Errorf("at the end the consumer has %q; want %q", got, c.afterNewline)
			}
		})
	}
}

func sameChunks(got, want []string) bool {
	return len(got) == 0 && len(want) == 0 || reflect.DeepEqual(got, want)
}

// What the line buffer holds when the step's standard output ends goes out
// then, with or without -u: no byte is lost.
func TestStreamSendsHeldBytesWhenStandardOutputEnds(t *testing.T) {
	for _, dashU := range []bool{false, true} {
		r := newRunRig(t, RunMode{})
		id := r.holdJob(r.s, r.root+"/release")
		sr := startStream(t, r.s, streamLine(id, dashU, shScript("printf abc")))
		if m := sr.result(t); m.err != nil || m.code != 0 {
			t.Fatalf("-u %v: Stream returned %+v, want exit 0", dashU, m)
		}
		if got := sr.out.text(); got != "abc" {
			t.Errorf("-u %v: the consumer got %q, want %q", dashU, got, "abc")
		}
	}
}

// Standard error passes straight through even without -u: a chunk with no
// newline arrives while the step runs, and standard output is not touched.
func TestStreamPassesStandardErrorStraightThrough(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	rd := r.countReads()
	marker := r.root + "/wrote"
	sr := startStream(t, r.s, streamLine(id, false, shScript(`printf e >&2; echo w > `+marker+`; read g`)))
	r.waitFile(marker)
	waitFor(t, "the copy to handle the chunk", func() bool { return rd.err.Load() >= 1 })
	if got := sr.errOut.list(); !reflect.DeepEqual(got, []string{"e"}) || sr.out.text() != "" {
		t.Errorf("while the step runs: stderr chunks %q, stdout %q; want [\"e\"] and nothing", got, sr.out.text())
	}
	sr.send(t, "go\n")
	if m := sr.result(t); m.err != nil || m.code != 0 {
		t.Errorf("Stream returned %+v, want exit 0", m)
	}
}

// Nothing caps a stream: 33 MiB through the step and back (the buffered
// srun's cap is 16 MiB per stream) arrive byte for byte, in order. The
// digests are of an independent source, the seeded generator that wrote the
// input, against what the consumer received.
func TestStreamIsNotCapped(t *testing.T) {
	const total = 33<<20 + 7
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	pr, pw := io.Pipe()
	sent := sha256.New()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		defer pw.Close()
		rnd := rand.New(rand.NewSource(7))
		buf := make([]byte, 64<<10)
		for n := 0; n < total; {
			k := min(len(buf), total-n)
			rnd.Read(buf[:k])
			sent.Write(buf[:k])
			if _, err := pw.Write(buf[:k]); err != nil {
				return
			}
			n += k
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*waitDeadline)
	defer cancel()
	got := newHashSink()
	code, err := r.s.Stream(ctx, streamLine(id, true, "cat"), pr, got, nil)
	pr.Close()
	<-writerDone
	if err != nil || code != 0 {
		t.Fatalf("Stream returned exit %d, %v; want 0, nil", code, err)
	}
	sum, n := got.sum()
	if n != total || !reflect.DeepEqual(sum, sent.Sum(nil)) {
		t.Errorf("the consumer got %d bytes (digest equal: %v), want %d bytes with the digest of what was sent", n, reflect.DeepEqual(sum, sent.Sum(nil)), total)
	}
}

// A script that records its own PID and its background child's.
const treeScript = `echo $$ > PIDS; sleep 300 & echo $! >> PIDS; wait`

func treeStep(pids string) string { return shScript(strings.ReplaceAll(treeScript, "PIDS", pids)) }

// Cancelling the context kills the step and what it started, each checked by
// PID at the moment Stream returns, and Stream returns the error of the
// context, as Run does for a cancelled one. The job is untouched.
func TestStreamIsKilledWhenTheContextEnds(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	pids := r.root + "/pids"
	sr := startStream(t, r.s, streamLine(id, true, treeStep(pids)))
	waitPids(t, pids, 2)
	sr.cancel()
	m := sr.result(t)
	if !errors.Is(m.err, slurm.ErrTransport) || m.code != -1 {
		t.Errorf("Stream returned %+v, want exit -1 and slurm.ErrTransport", m)
	}
	wantNoneLeft(t, pids, 2)
	if j := r.job(id); j.State != Running {
		t.Errorf("the job is %s after its step was cancelled, want RUNNING", j.State)
	}
}

// The end of the job's batch shell ends the step, as a Slurm step does not
// outlive its job: exit 1 and the Job step aborted line, and no process of
// the step is left, by PID.
func TestStreamEndsWithItsJob(t *testing.T) {
	r := newRunRig(t, RunMode{})
	release := r.root + "/release"
	id := r.holdJob(r.s, release)
	pids := r.root + "/pids"
	sr := startStream(t, r.s, streamLine(id, true, treeStep(pids)))
	waitPids(t, pids, 2)
	if err := os.WriteFile(release, nil, 0o644); err != nil { // the job's shell exits
		t.Fatal(err)
	}
	m := sr.result(t)
	if m.err != nil || m.code != 1 || !strings.Contains(sr.errOut.text(), "srun: error: Job step aborted") {
		t.Errorf("Stream returned %+v with stderr %q; want exit 1 and a Job step aborted line", m, sr.errOut.text())
	}
	wantNoneLeft(t, pids, 2)
}

// --time bounds a stream as it does a buffered step.
func TestStreamHonoursItsTimeLimit(t *testing.T) {
	// The step must write both its PIDs before its minute passes, as in
	// TestSrunHonoursItsTimeLimit, which takes the same room.
	r := newRunRig(t, RunMode{Minute: 3 * time.Second})
	id := r.holdJob(r.s, r.root+"/release")
	pids := r.root + "/pids"
	sr := startStream(t, r.s, streamLine(id, true, "--time=1 "+treeStep(pids)))
	m := sr.result(t)
	if m.err != nil || m.code != 1 || !strings.Contains(sr.errOut.text(), "TIME LIMIT") {
		t.Errorf("Stream returned %+v with stderr %q; want exit 1 and a TIME LIMIT line", m, sr.errOut.text())
	}
	wantNoneLeft(t, pids, 2)
}

// A consumer that stops reading does not hold Stream for ever once the
// context has ended: the copy that is stuck in its Write is let go, and
// Stream returns with the step's processes gone. The write is released at the
// end, and nothing more reaches the consumer from the step.
func TestStreamIsNotHeldByAConsumerThatStoppedReading(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	rd := r.countReads()
	pids := r.root + "/pids"
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	stuck := writerFunc(func(p []byte) (int, error) {
		once.Do(func() { close(entered) })
		<-release // stops reading: this write does not return
		return len(p), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr, pw := io.Pipe()
	defer pw.Close()
	done := make(chan streamMsg, 1)
	script := strings.ReplaceAll(`echo $$ > PIDS; sleep 300 & echo $! >> PIDS; printf x; printf y; wait`, "PIDS", pids)
	go func() {
		code, err := r.s.Stream(ctx, streamLine(id, true, shScript(script)), pr, stuck, nil)
		done <- streamMsg{code, err}
	}()
	waitPids(t, pids, 2)
	await(t, "the consumer's Write to be entered", entered)
	cancel()
	m := recvChan(t, done)
	if !errors.Is(m.err, slurm.ErrTransport) {
		t.Errorf("Stream returned %+v, want slurm.ErrTransport", m)
	}
	wantNoneLeft(t, pids, 2)
	close(release)
	waitFor(t, "the let-go copy to finish its write", func() bool { return rd.out.Load() >= 1 })
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func await(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-timeAfterDeadline():
		t.Fatalf("timed out after %s waiting for %s", waitDeadline, what)
	}
}

// A process that escaped the step's session and holds the output open does
// not hold Stream: it returns, with what the step wrote, once the processes it
// can find are gone and the streams have had a moment to run dry. The escape
// is ordered by a handshake, as TestSrunReturnsWhileAnEscapeeHoldsItsOutput's.
func TestStreamReturnsWhileAnEscapeeHoldsItsOutput(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	esc, gate := r.root+"/escapee", r.root+"/go"
	script := "echo before\n" +
		"sh -c 'while [ ! -e " + gate + " ]; do sleep 0.01; done; exec setsid sh -c \"echo \\$\\$ > " + esc + ".tmp && mv " + esc + ".tmp " + esc + "; exec sleep 300\"' &\n" +
		"while [ ! -s " + esc + " ]; do : > " + gate + "; sleep 0.01; done\n" +
		"exit 0\n"
	pr, pw := io.Pipe()
	go func() { io.WriteString(pw, script); pw.Close() }()
	out := &sink{}
	code, err := r.s.Stream(t.Context(), streamLine(id, true, "bash -s"), pr, out, nil)
	pr.Close()
	if out.text() != "before\n" || code != 0 || err != nil {
		t.Errorf("Stream gave stdout %q, exit %d, %v; want %q, 0, nil", out.text(), code, err, "before\n")
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(r.waitFile(esc)))
	if pid <= 0 || procGone(pid) {
		t.Fatalf("the escapee %d is not running; the test would prove nothing", pid)
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the escapee to be gone", func() bool { return procGone(pid) })
}

// The exit status is the program's, or 128 plus the signal.
func TestStreamExitStatus(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	for _, c := range []struct {
		prog string
		want int
	}{
		{"sh -c 'exit 7'", 7},
		{"sh -c 'kill -KILL $$'", 128 + int(syscall.SIGKILL)},
		{"true", 0},
		{"false", 1},
		{"no-such-program", 127},
	} {
		code, err := r.s.Stream(t.Context(), streamLine(id, true, c.prog), nil, nil, nil)
		if err != nil || code != c.want {
			t.Errorf("%q: exit %d, %v; want exit %d", c.prog, code, err, c.want)
		}
	}
}

// Stream takes exactly one simple srun command: other commands, chains and
// redirections are refused with the fake's own codes, and nothing runs. The
// same step on a plain srun line is the control, so an absent marker is not a
// wrong path.
func TestStreamRefusesWhatIsNotOneSrun(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	marker := r.root + "/marker"
	step := shScript("echo ran > " + marker)
	if code, err := r.s.Stream(t.Context(), streamLine(id, true, step), nil, nil, nil); err != nil || code != 0 {
		t.Fatalf("control: exit %d, %v", code, err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("control: no marker (%v)", err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	srunBefore, squeueBefore := r.p.Count("srun"), r.p.Count("squeue")
	for _, c := range []struct {
		name, line, want string
		code             int
	}{
		{"squeue", "squeue --me --json", `"squeue" is not srun`, 1},
		{"printenv", "printenv HOME", `"printenv" is not srun`, 1},
		{"a chain", streamLine(id, true, step) + " && true", "a chain of commands", 1},
		{"srun after another command", "true && " + streamLine(id, true, step), "a chain of commands", 1},
		{"a redirection", streamLine(id, true, step) + " > " + r.root + "/out", "a redirection", 1},
		{"cat to a file", "cat > " + r.root + "/out", `"cat" is not srun`, 1},
		{"an empty line", "", "an empty line", 1},
		{"a syntax error", "srun -u --overlap $HOME", "syntax error", 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			var stdout, stderr strings.Builder
			code, err := r.s.Stream(t.Context(), c.line, strings.NewReader("ignored"), &stdout, &stderr)
			if err != nil || code != c.code || stdout.Len() != 0 || !strings.Contains(stderr.String(), c.want) {
				t.Errorf("%q: exit %d, %v, stdout %q, stderr %q; want exit %d, a message holding %q", c.line, code, err, stdout.String(), stderr.String(), c.code, c.want)
			}
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("the step ran: marker stat = %v", err)
			}
			if _, err := os.Stat(r.root + "/out"); err == nil {
				t.Error("the redirection wrote a file")
			}
		})
	}
	if got, want := r.p.Count("srun"), srunBefore; got != want || r.p.Count("squeue") != squeueBefore {
		t.Errorf("a refused line is in the call log: srun %d (was %d), squeue %d (was %d)", got, want, r.p.Count("squeue"), squeueBefore)
	}
}

// srun's own checks refuse a streamed line as they do a buffered one: the
// message and the exit code come back on the streams, and nothing runs.
func TestStreamRunsSrunsOwnChecks(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	for _, c := range []struct {
		name, line, want string
		code             int
	}{
		{"unknown option", "srun -u --overlap --jobid=" + itoa(id) + " --exclusive true", `not "--exclusive"`, 1},
		{"no job", "srun -u --overlap --ntasks=1 true", "--jobid=<id> is required", 1},
		{"unknown job", "srun -u --overlap --jobid=9999999 true", "Invalid job id specified", 1},
		{"no program", streamLine(id, true, "no-such-program"), "No such file or directory", 127},
	} {
		var stderr strings.Builder
		code, err := r.s.Stream(t.Context(), c.line, nil, nil, &stderr)
		if err != nil || code != c.code || !strings.HasPrefix(stderr.String(), "srun: error: ") || !strings.Contains(stderr.String(), c.want) {
			t.Errorf("%s: exit %d, %v, stderr %q; want exit %d and an srun error holding %q", c.name, code, err, stderr.String(), c.code, c.want)
		}
	}
}

// Faults and gates apply as for Run: a fault is returned without running and
// is not in Calls; a gate holds the command, which is not in Calls until it
// is released.
func TestStreamFaultsAndGatesApply(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	marker := r.root + "/marker"
	line := streamLine(id, true, shScript("echo ran > "+marker))
	before := r.p.Count("srun")

	r.p.FailNext(1, slurm.ErrTimeout)
	if code, err := r.s.Stream(t.Context(), line, nil, nil, nil); !errors.Is(err, slurm.ErrTimeout) || code != -1 {
		t.Errorf("after FailNext: exit %d, %v; want -1 and slurm.ErrTimeout", code, err)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Error("the step ran after a fault")
	}
	if f := r.p.Faulted(); len(f) != 1 || f[0].Words[0] != "srun" || r.p.Count("srun") != before {
		t.Errorf("faulted %+v, srun calls %d (was %d); want one srun fault and no call", f, r.p.Count("srun"), before)
	}

	g := r.p.Gate("srun")
	defer g.Release()
	sr := startStream(t, r.s, line)
	await(t, "the gate to hold the command", g.Blocked())
	if r.p.Count("srun") != before || sr.returned() {
		t.Errorf("a gated command is logged (%d calls, was %d) or has returned", r.p.Count("srun"), before)
	}
	g.Release()
	if m := sr.result(t); m.err != nil || m.code != 0 {
		t.Errorf("after the gate: %+v, want exit 0", m)
	}
	if r.p.Count("srun") != before+1 {
		t.Errorf("%d srun calls after the gate, want %d", r.p.Count("srun"), before+1)
	}
}

// The call log has the line's words, and its StdinLen is what the step's
// input got, final when Stream returns. The control: Run's own log entry for
// a buffered srun keeps its length as before.
func TestStreamLogsTheCall(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	line := streamLine(id, true, "cat")
	pr, pw := io.Pipe()
	go func() { io.WriteString(pw, "12345"); io.WriteString(pw, "6789"); pw.Close() }()
	if code, err := r.s.Stream(t.Context(), line, pr, nil, nil); err != nil || code != 0 {
		t.Fatalf("exit %d, %v", code, err)
	}
	pr.Close()
	var last Call
	for _, c := range r.p.Calls() {
		if c.Words[0] == "srun" {
			last = c
		}
	}
	if want := strings.Fields(line); !reflect.DeepEqual(last.Words, want) || last.StdinLen != 9 || last.User != "example-user" {
		t.Errorf("logged call %+v, want words %v, stdin length 9, user example-user", last, want)
	}
}

// A failing consumer or input is not lost silently: the step runs to its end
// and Stream returns the failure as a transport error.
func TestStreamReportsAFailingConsumerOrInput(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	boom := errors.New("example write failure")
	code, err := r.s.Stream(t.Context(), streamLine(id, true, shScript("printf one; printf two")), nil,
		writerFunc(func([]byte) (int, error) { return 0, boom }), nil)
	if !errors.Is(err, slurm.ErrTransport) || !strings.Contains(err.Error(), boom.Error()) || code != -1 {
		t.Errorf("a failing consumer: exit %d, %v; want -1 and a transport error that names the failure", code, err)
	}
	failing := io.MultiReader(strings.NewReader("abc"), errReader{errors.New("example read failure")})
	var out strings.Builder
	code, err = r.s.Stream(t.Context(), streamLine(id, true, "cat"), failing, &out, nil)
	if !errors.Is(err, slurm.ErrTransport) || !strings.Contains(err.Error(), "example read failure") || code != -1 || out.String() != "abc" {
		t.Errorf("a failing input: exit %d, %v, stdout %q; want -1, a transport error and the bytes that did arrive", code, err, out.String())
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// While a stream runs, the platform answers other commands: mu is not held.
func TestStreamDoesNotBlockThePlatform(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	sr := startStream(t, r.s, streamLine(id, true, "cat"))
	sr.send(t, "up\n")
	waitFor(t, "the step to echo", func() bool { return sr.out.text() == "up\n" })
	answered := make(chan jobView, 1)
	go func() { answered <- r.view(id) }() // blocks for good if the stream holds the lock
	select {
	case v := <-answered:
		if v.job.State != Running {
			t.Errorf("while the stream runs: job %s, want RUNNING", v.job.State)
		}
	case <-timeAfterDeadline():
		t.Fatalf("squeue, sacct and Processes did not answer in %s while a stream ran", waitDeadline)
	}
	sr.in.Close()
	if m := sr.result(t); m.err != nil || m.code != 0 {
		t.Errorf("Stream returned %+v, want exit 0", m)
	}
}

// -u is a known srun option: it is read in any order with the others, and
// only before the program, where "-u" is the program's own word.
func TestParseSrunTakesDashU(t *testing.T) {
	for _, c := range []struct {
		name  string
		args  []string
		unbuf bool
		prog  string
		rest  []string
	}{
		{"first", []string{"-u", "--overlap", "--jobid=5", "--ntasks=1", "agent", "relay", "control"}, true, "agent", []string{"relay", "control"}},
		{"middle", []string{"--overlap", "-u", "--jobid=5", "prog"}, true, "prog", nil},
		{"last", []string{"--jobid=5", "--time=2", "--overlap", "-u", "prog", "a"}, true, "prog", []string{"a"}},
		{"absent", []string{"--overlap", "--jobid=5", "prog"}, false, "prog", nil},
		{"after the program", []string{"--overlap", "--jobid=5", "prog", "-u"}, false, "prog", []string{"-u"}},
	} {
		a, msg := parseSrun(c.args)
		if msg != "" || a.unbuf != c.unbuf || a.jobID != 5 || a.program != c.prog || !sameChunks(a.args, c.rest) {
			t.Errorf("%s: parseSrun(%q) = %+v, %q; want unbuffered %v, job 5, program %q, args %q", c.name, c.args, a, msg, c.unbuf, c.prog, c.rest)
		}
	}
	for _, bad := range []string{"-u=1", "-uu", "-unbuffered", "--unbuffered", "-n1", "-i"} {
		if _, msg := parseSrun([]string{bad, "--overlap", "--jobid=5", "prog"}); !strings.Contains(msg, `not "`+bad+`"`) {
			t.Errorf("parseSrun with %q: message %q; want it refused by name", bad, msg)
		}
	}
}

// Run accepts -u, and an unknown option beside it is still refused.
func TestRunAcceptsDashU(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	if out, errOut, code := r.run("srun -u --overlap --jobid=" + itoa(id) + " --ntasks=1 sh -c 'echo ok'"); out != "ok\n" || errOut != "" || code != 0 {
		t.Errorf("Run with -u: stdout %q, stderr %q, exit %d; want ok, none, 0", out, errOut, code)
	}
	if _, errOut, code := r.run("srun -u --overlap --jobid=" + itoa(id) + " --exclusive true"); code != 1 || !strings.Contains(errOut, `not "--exclusive"`) {
		t.Errorf("Run with -u and an unknown option: stderr %q, exit %d; want it refused", errOut, code)
	}
}
