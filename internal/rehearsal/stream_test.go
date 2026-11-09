package rehearsal

// New. The streaming relay (relaystream.go, wire.go), through the real sshd and
// the system ssh: an srun line streams both ways before any end of input, the
// bytes arrive exact, the step ends when the ssh client dies, and the other
// lines are untouched. A test that called the server's handler directly, with
// no ssh or sshd between, would prove nothing about the forced command, the
// pipes sshd gives it or the session ending, so none does. The expected values
// come from the brief of issue #298 and from the bytes the tests write; none is
// read back from the relay. Every wait is on observable state (bytes arrived, a
// PID file, a process gone) with a generous deadline.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
)

// lockedBuf is a buffer that the ssh client's copying goroutine writes while
// the test reads.
type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// hashBuf digests and counts what it receives and keeps none of it.
type hashBuf struct {
	mu sync.Mutex
	h  hash.Hash
	n  int
}

func newHashBuf() *hashBuf { return &hashBuf{h: sha256.New()} }

func (b *hashBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.n += len(p)
	return b.h.Write(p)
}

func (b *hashBuf) sum() (string, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return hex.EncodeToString(b.h.Sum(nil)), b.n
}

// sshStep is a system ssh process running one line on the login node, with
// its standard input as a pipe.
type sshStep struct {
	cmd         *exec.Cmd
	in          io.WriteCloser
	out, errOut *lockedBuf
	done        chan error
}

// startSSH runs line through a plain ssh of its own (no ControlMaster), so
// killing the process closes the connection to sshd. stdout, when set,
// replaces the step's out buffer. The cleanup kills the process, so a failed
// test strands none.
func (r *rig) startSSH(t *testing.T, line string, stdout io.Writer) *sshStep {
	t.Helper()
	st := &sshStep{out: &lockedBuf{}, errOut: &lockedBuf{}, done: make(chan error, 1)}
	if stdout == nil {
		stdout = st.out
	}
	st.cmd = exec.Command("ssh", "-F", filepath.Join(r.base, "creds"),
		"-o", "BatchMode=yes", "-o", "IdentityAgent=none", "-o", "ConnectTimeout=20", "-T", alias, line)
	var err error
	if st.in, err = st.cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	st.cmd.Stdout, st.cmd.Stderr = stdout, st.errOut
	if err := st.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { st.done <- st.cmd.Wait() }()
	t.Cleanup(func() {
		_ = st.cmd.Process.Kill() // after the exit this is an error nobody needs
		select {
		case err := <-st.done:
			st.done <- err
		case <-time.After(waitDeadline):
			t.Errorf("ssh process %d did not end after a kill", st.cmd.Process.Pid)
		}
	})
	return st
}

func (st *sshStep) send(t *testing.T, s string) {
	t.Helper()
	if _, err := io.WriteString(st.in, s); err != nil {
		t.Fatalf("writing %q to ssh's standard input: %v (stderr %q)", s, err, st.errOut.String())
	}
}

// exited reports, without waiting, whether the ssh process has ended.
func (st *sshStep) exited() bool {
	select {
	case err := <-st.done:
		st.done <- err
		return true
	default:
		return false
	}
}

// exitCode waits for the ssh process to end and returns its exit code, -1 for
// a signal.
func (st *sshStep) exitCode(t *testing.T) int {
	t.Helper()
	select {
	case err := <-st.done:
		st.done <- err
		var ee *exec.ExitError
		switch {
		case err == nil:
			return 0
		case errors.As(err, &ee):
			return ee.ExitCode()
		}
		t.Fatalf("ssh: %v", err)
	case <-time.After(waitDeadline):
		t.Fatalf("ssh did not end within %s (stderr %q)", waitDeadline, st.errOut.String())
	}
	return -1
}

// waitLoop is a shell loop that waits until the file exists, for at most
// about 60 s, so a job that nobody releases still ends on its own.
func waitLoop(file string) string {
	return "i=0; while [ ! -e " + file + " ] && [ $i -lt 1200 ]; do sleep 0.05; i=$((i+1)); done"
}

// holdJob submits a job that keeps running and waits until its shell does,
// and returns its ID.
func holdJob(t *testing.T, r *rig, s *sshrun.SSH) int {
	t.Helper()
	id := atoi(t, must(t, s, "sbatch --parsable '--wrap="+waitLoop(filepath.Join(r.root, "release"))+"'"))
	waitFor(t, "the job to run", func() bool { j, _ := r.plat.Job(id); return j.PID > 0 })
	return id
}

func srunUnbuffered(id int, program string) string {
	return "srun -u --overlap --jobid=" + strconv.Itoa(id) + " --ntasks=1 " + program
}

// alive reports whether the process is still the live one a PID file named.
func alive(pid int) bool { _, ok := readProcRow(pid); return ok }

// readPID reads the PID a step wrote to its file.
func readPID(t *testing.T, file string) int {
	t.Helper()
	var pid int
	waitFor(t, file+" to hold a PID", func() bool {
		b, err := os.ReadFile(file)
		n, cerr := strconv.Atoi(strings.TrimSpace(string(b)))
		pid = n
		return err == nil && cerr == nil && n > 0
	})
	return pid
}

// An srun line streams both ways before any end of input: through the real
// sshd and ssh, what the test writes comes back from the step while ssh still
// runs, and the end of input then ends the step with exit 0 (half-close: the
// output kept coming after the input had no more).
func TestSrunStreamsBothWaysBeforeTheEndOfInput(t *testing.T) {
	r, s := newRig(t)
	id := holdJob(t, r, s)
	st := r.startSSH(t, srunUnbuffered(id, "cat"), nil)
	for _, piece := range []string{"ping\n", "pong"} { // the second has no newline
		before := len(st.out.String())
		st.send(t, piece)
		waitFor(t, "the step to echo "+strconv.Quote(piece), func() bool { return len(st.out.String()) >= before+len(piece) })
		if st.exited() {
			t.Fatalf("ssh ended after %q, before the end of input (stderr %q)", piece, st.errOut.String())
		}
	}
	if got := st.out.String(); got != "ping\npong" {
		t.Fatalf("the step echoed %q before the end of input, want %q", got, "ping\npong")
	}
	if err := st.in.Close(); err != nil { // the end of input
		t.Fatal(err)
	}
	if code := st.exitCode(t); code != 0 || st.out.String() != "ping\npong" || st.errOut.String() != "" {
		t.Errorf("after the end of input: exit %d, stdout %q, stderr %q; want 0, %q, none", code, st.out.String(), st.errOut.String(), "ping\npong")
	}
	if got := r.plat.CountFor(clusterUser, "srun"); got != 1 {
		t.Errorf("the platform logged %d srun calls, want 1", got)
	}
}

// The bytes cross exact and in order, both ways at once, with nothing added or
// dropped: 3 MiB of seeded random bytes with NULs, carriage returns and no
// trailing newline go through the step and come back with the digest of what
// was sent, which the writer computes from its own generator.
func TestSrunStreamIsByteExact(t *testing.T) {
	r, s := newRig(t)
	id := holdJob(t, r, s)
	const total = 3<<20 + 7
	got := newHashBuf()
	st := r.startSSH(t, srunUnbuffered(id, "cat"), got)
	sent := sha256.New()
	writerDone := make(chan error, 1)
	go func() {
		rnd := rand.New(rand.NewSource(11))
		buf := make([]byte, 100<<10) // not a multiple of the frame size
		for n := 0; n < total; {
			k := min(len(buf), total-n)
			rnd.Read(buf[:k])
			for i := 0; i < k; i += 509 {
				buf[i] = 0
			}
			for i := 3; i < k; i += 997 {
				buf[i] = '\r'
			}
			sent.Write(buf[:k])
			if _, err := st.in.Write(buf[:k]); err != nil {
				writerDone <- err
				return
			}
			n += k
		}
		writerDone <- st.in.Close()
	}()
	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatalf("writing to ssh: %v (stderr %q)", err, st.errOut.String())
		}
	case <-time.After(waitDeadline):
		t.Fatalf("the input was not all written within %s", waitDeadline)
	}
	if code := st.exitCode(t); code != 0 {
		t.Fatalf("ssh exit %d (stderr %q)", code, st.errOut.String())
	}
	sum, n := got.sum()
	if want := hex.EncodeToString(sent.Sum(nil)); n != total || sum != want {
		t.Errorf("the step returned %d bytes with digest %s, want %d bytes with digest %s", n, sum, total, want)
	}
}

// Killing the local ssh client with SIGKILL ends the step: its process is gone
// within the deadline, though the step is silent (it writes nothing and ignores
// its input), so only the session going away can tell the relay, and the job
// is untouched. The step is alive just before the kill, so its absence after
// it is the kill's doing.
func TestSrunStepEndsWhenTheSSHClientIsKilled(t *testing.T) {
	r, s := newRig(t)
	id := holdJob(t, r, s)
	pidFile := filepath.Join(r.root, "step.pid")
	st := r.startSSH(t, srunUnbuffered(id, "sh -c 'echo $$ > "+pidFile+"; exec sleep 300'"), nil)
	pid := readPID(t, pidFile)
	if !alive(pid) || st.exited() {
		t.Fatalf("before the kill: step %d alive %v, ssh ended %v; the check after it would prove nothing", pid, alive(pid), st.exited())
	}
	if err := st.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if code := st.exitCode(t); code != -1 {
		t.Errorf("the killed ssh client ended with exit %d, want a signal", code)
	}
	waitFor(t, "the step to be gone", func() bool { return !alive(pid) })
	if j, _ := r.plat.Job(id); j.State != "RUNNING" || j.Finished {
		t.Errorf("the job is %s (finished %v) after its step was cancelled, want RUNNING", j.State, j.Finished)
	}
}

// The controller's own transport ends a step the same way: a command that runs
// past its Timeout is cut by the ssh client's kill, which is what ends the
// step now that the stream has no Timeout of its own.
func TestSrunStepEndsWhenTheControllersCommandTimesOut(t *testing.T) {
	r, s := newRig(t)
	id := holdJob(t, r, s)
	pidFile := filepath.Join(r.root, "step.pid")
	line := "srun --overlap --jobid=" + strconv.Itoa(id) + " --ntasks=1 sh -c 'echo $$ > " + pidFile + "; exec sleep 300'"
	errc := make(chan error, 1)
	go func() {
		_, err := s.Run(context.Background(), slurm.Command{Line: line, Timeout: 3 * time.Second})
		errc <- err
	}()
	pid := readPID(t, pidFile)
	select {
	case err := <-errc:
		if !errors.Is(err, slurm.ErrTimeout) {
			t.Errorf("the command that ran past its Timeout returned %v, want slurm.ErrTimeout", err)
		}
	case <-time.After(waitDeadline):
		t.Fatalf("the command did not return within %s", waitDeadline)
	}
	waitFor(t, "the step to be gone", func() bool { return !alive(pid) })
}

// The Provider's use of srun, through the controller's transport, is as it
// was: a script on standard input, the output and error apart, and the exit
// code. Without -u the output that ends without a newline arrives at the end.
func TestSrunThroughTheControllersTransportStillAnswers(t *testing.T) {
	r, s := newRig(t)
	id := holdJob(t, r, s)
	line := "srun --overlap --jobid=" + strconv.Itoa(id) + " --ntasks=1 --time=1 bash -s"
	res := run(t, s, line, []byte("echo out\necho err >&2\nprintf tail\nexit 3\n"))
	if string(res.Stdout) != "out\ntail" || string(res.Stderr) != "err\n" || res.ExitCode != 3 {
		t.Errorf("srun gave stdout %q, stderr %q, exit %d; want %q, %q, 3", res.Stdout, res.Stderr, res.ExitCode, "out\ntail", "err\n")
	}
	// A refusal comes back as it did: the message on standard error, exit 1.
	res = run(t, s, "srun --overlap --jobid=999999 --ntasks=1 true", nil)
	if res.ExitCode != 1 || !strings.Contains(string(res.Stderr), "Invalid job id specified") {
		t.Errorf("srun on an unknown job: exit %d, stderr %q; want 1 and the Slurm error", res.ExitCode, res.Stderr)
	}
	// Content through standard input, byte for byte: the digest of 3 MiB.
	content := make([]byte, 3<<20+7)
	rand.New(rand.NewSource(5)).Read(content)
	sum := sha256.Sum256(content)
	res = run(t, s, "srun --overlap --jobid="+strconv.Itoa(id)+" --ntasks=1 sha256sum", content)
	if want := hex.EncodeToString(sum[:]) + "  -\n"; string(res.Stdout) != want || res.ExitCode != 0 {
		t.Errorf("sha256sum of the input gave %q, exit %d; want %q, 0", res.Stdout, res.ExitCode, want)
	}
}

// An ordinary command answers as before, and while a stream is open: the
// stream does not hold the platform, and goes on afterwards.
func TestOrdinaryCommandsAnswerWhileAStreamIsOpen(t *testing.T) {
	r, s := newRig(t)
	id := holdJob(t, r, s)
	st := r.startSSH(t, srunUnbuffered(id, "cat"), nil)
	st.send(t, "one\n")
	waitFor(t, "the step to echo", func() bool { return st.out.String() == "one\n" })
	q := decodeSqueue(t, []byte(must(t, s, "squeue --me --json")))
	if len(q.Jobs) != 1 || q.Jobs[0].JobID != id || q.Jobs[0].JobState[0] != "RUNNING" {
		t.Errorf("squeue while the stream is open listed %+v, want the running job %d", q.Jobs, id)
	}
	if got := strings.TrimSpace(must(t, s, "printenv HOME")); got != r.home {
		t.Errorf("printenv HOME while the stream is open: %q, want %q", got, r.home)
	}
	st.send(t, "two\n")
	waitFor(t, "the step to echo again", func() bool { return st.out.String() == "one\ntwo\n" })
	_ = st.in.Close()
	if code := st.exitCode(t); code != 0 {
		t.Errorf("the stream ended with exit %d, want 0", code)
	}
}

// A live stream does not outlive the login node: Close ends it, the step is
// killed (its PID is gone when Close returns) and ssh exits 255, which the
// controller reads as a transport failure.
func TestCloseEndsALiveStream(t *testing.T) {
	r, s := newRig(t)
	id := holdJob(t, r, s)
	pidFile := filepath.Join(r.root, "step.pid")
	st := r.startSSH(t, srunUnbuffered(id, "sh -c 'echo $$ > "+pidFile+"; exec cat'"), nil)
	pid := readPID(t, pidFile)
	st.send(t, "x")
	waitFor(t, "the step to echo", func() bool { return st.out.String() == "x" })
	if err := r.node.Close(); err != nil {
		t.Fatal(err)
	}
	if alive(pid) {
		t.Errorf("the step %d is alive after Close returned", pid)
	}
	if code := st.exitCode(t); code != 255 {
		t.Errorf("ssh ended with exit %d after Close, want 255", code)
	}
}

// The ordinary request and reply keep their format byte for byte, and a
// streaming request is told from them by its first eight bytes. These are the
// encodings written out by hand.
func TestWireFormats(t *testing.T) {
	var b bytes.Buffer
	if err := writeRequest(&b, request{line: "ab", stdin: []byte("xyz")}); err != nil {
		t.Fatal(err)
	}
	want := append(append(append([]byte{0, 0, 0, 0, 0, 0, 0, 2}, "ab"...), 0, 0, 0, 0, 0, 0, 0, 3), "xyz"...)
	if !bytes.Equal(b.Bytes(), want) {
		t.Errorf("an ordinary request is % x, want % x", b.Bytes(), want)
	}
	q, err := readRequest(bytes.NewReader(want))
	if err != nil || q.stream || q.line != "ab" || string(q.stdin) != "xyz" {
		t.Errorf("reading it back: %+v, %v", q, err)
	}

	b.Reset()
	if err := writeReply(&b, reply{exit: 3, stdout: []byte("o"), stderr: []byte("ee")}); err != nil {
		t.Fatal(err)
	}
	want = append(append(append([]byte{0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1}, "o"...), 0, 0, 0, 0, 0, 0, 0, 2), "ee"...)
	if !bytes.Equal(b.Bytes(), want) {
		t.Errorf("a reply is % x, want % x", b.Bytes(), want)
	}

	b.Reset()
	if err := writeStreamRequest(&b, "srun x"); err != nil {
		t.Fatal(err)
	}
	want = append([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 6}, "srun x"...)
	if !bytes.Equal(b.Bytes(), want) {
		t.Errorf("a streaming request is % x, want % x", b.Bytes(), want)
	}
	if q, err := readRequest(bytes.NewReader(want)); err != nil || !q.stream || q.line != "srun x" {
		t.Errorf("reading it back: %+v, %v", q, err)
	}

	b.Reset()
	if err := writeFrame(&b, frameStdout, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	if want := []byte{3, 0, 0, 0, 2, 'h', 'i'}; !bytes.Equal(b.Bytes(), want) {
		t.Errorf("a frame is % x, want % x", b.Bytes(), want)
	}
	if k, p, err := readFrame(bytes.NewReader(b.Bytes())); err != nil || k != frameStdout || string(p) != "hi" {
		t.Errorf("reading it back: %d %q %v", k, p, err)
	}
}

// A frame is bounded on both ends, and a long write goes out as several frames
// that, read in order, are the bytes written.
func TestFramesAreBounded(t *testing.T) {
	var b bytes.Buffer
	if err := writeFrame(&b, frameStdin, make([]byte, maxFrame+1)); err == nil {
		t.Error("writeFrame accepted a payload above maxFrame")
	}
	var h [5]byte
	h[0] = frameStdout
	binary.BigEndian.PutUint32(h[1:], maxFrame+1)
	if _, _, err := readFrame(bytes.NewReader(h[:])); err == nil {
		t.Error("readFrame accepted a length above maxFrame")
	}
	data := make([]byte, 2*maxFrame+5)
	rand.New(rand.NewSource(3)).Read(data)
	b.Reset()
	fw := &frameWriter{w: &b}
	if n, err := fw.data(frameStdout).Write(data); err != nil || n != len(data) {
		t.Fatalf("Write: %d, %v", n, err)
	}
	var got []byte
	frames := 0
	for b.Len() > 0 {
		k, p, err := readFrame(&b)
		if err != nil || k != frameStdout || len(p) == 0 || len(p) > maxFrame {
			t.Fatalf("frame %d: kind %d, %d bytes, %v", frames, k, len(p), err)
		}
		got = append(got, p...)
		frames++
	}
	if frames != 3 || !bytes.Equal(got, data) {
		t.Errorf("%d frames, content equal %v; want 3 frames holding the %d bytes written", frames, bytes.Equal(got, data), len(data))
	}
}
