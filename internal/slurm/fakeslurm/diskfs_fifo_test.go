package fakeslurm

// New. B3 of the PR 153 review: a FIFO given as --output or --error, or one
// swapped in for a regular file, must not block the fake. Every open that can
// meet one is non-blocking and checked with fstat, and a launch holds
// Platform.mu, so a blocked open would block every reader of the platform and
// Close too. Each case runs under a deadline that fails the test cleanly: at
// the deadline the test opens the FIFO's other end itself, so the blocked
// call returns and the platform can still be closed.

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// fifoDeadline is how long a call that must return at once may take before
// the test calls it blocked. It is generous: it only fails a test.
const fifoDeadline = 20 * time.Second

// returnsAtOnce runs fn and fails the test if it has not returned by the
// deadline; unblock is then called so that fn can return and the test can end.
func returnsAtOnce(t *testing.T, what string, unblock func(), fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
		return
	case <-time.After(fifoDeadline):
	}
	unblock()
	select {
	case <-done:
	case <-time.After(fifoDeadline):
		t.Log("the blocked call did not return after the FIFO was opened for reading")
	}
	t.Fatalf("%s blocked: it had not returned after %s", what, fifoDeadline)
}

// openOtherEnd opens the read end of the FIFO without blocking, as the
// unblock of a test, and returns a function that closes it.
func openOtherEnd(t *testing.T, fifo string) (closeEnd func()) {
	t.Helper()
	fd, err := syscall.Open(fifo, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	return func() { syscall.Close(fd) }
}

func mkfifo(t *testing.T, name string) {
	t.Helper()
	if err := syscall.Mkfifo(name, 0o644); err != nil {
		t.Fatal(err)
	}
}

// The open of a name that is not a regular file returns at once, whether or
// not the other end of a FIFO is open, and a directory is refused too.
func TestDiskFSOpenRegularRefusesWhatIsNotARegularFile(t *testing.T) {
	root := t.TempDir()
	d := newTestDiskFS(t, root)
	mkfifo(t, root+"/fifo")
	if err := os.Mkdir(root+"/dir", 0o755); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name   string
		flag   int
		reader bool // the FIFO's read end is open
		want   error
	}{
		{"fifo to read, no writer", os.O_RDONLY, false, errNotRegular},
		{"fifo to write, no reader", os.O_WRONLY, false, syscall.ENXIO},
		{"fifo to write, a reader", os.O_WRONLY, true, errNotRegular},
		{"directory", os.O_RDONLY, false, errNotRegular},
	} {
		t.Run(c.name, func(t *testing.T) {
			name := "fifo"
			if c.name == "directory" {
				name = "dir"
			}
			if c.reader {
				defer openOtherEnd(t, root+"/fifo")()
			}
			var err error
			returnsAtOnce(t, "openRegular", func() { openOtherEnd(t, root+"/fifo") }, func() {
				var f *os.File
				if f, err = d.openRegular(name, c.flag, 0); err == nil {
					f.Close()
				}
			})
			if !errors.Is(err, c.want) {
				t.Errorf("openRegular: %v, want %v", err, c.want)
			}
		})
	}
}

// submitRaw runs sbatch for a wrap command from a goroutine that may block,
// and returns the job ID from its reply.
func (r *runRig) submitRaw(line string) (id int, err error) {
	res, err := r.s.Run(context.Background(), slurm.Command{Line: line})
	if err != nil {
		return 0, err
	}
	if res.ExitCode != 0 {
		return 0, errors.New("exit " + strconv.Itoa(res.ExitCode) + ": " + string(res.Stderr))
	}
	return strconv.Atoi(strings.TrimSpace(string(res.Stdout)))
}

// A FIFO as --output or --error fails the job at once: with no reader its
// open would block for ever, and with a reader it is not a file. The platform
// answers meanwhile, and the next job runs.
func TestRunModeFifoOutputFailsTheJobAtOnce(t *testing.T) {
	for _, opt := range []string{"--output", "--error"} {
		for _, reader := range []bool{false, true} {
			name := opt + " with no reader"
			if reader {
				name = opt + " with a reader"
			}
			t.Run(name, func(t *testing.T) {
				r := newRunRig(t, RunMode{})
				fifo, ran := r.home+"/fifo", r.home+"/ran"
				mkfifo(t, fifo)
				if reader {
					defer openOtherEnd(t, fifo)()
				}
				line := "sbatch --parsable " + opt + "=" + fifo + " '--wrap=echo ran > " + ran + "'"
				var id int
				var err error
				returnsAtOnce(t, "sbatch with a FIFO output", func() { openOtherEnd(t, fifo) }, func() { id, err = r.submitRaw(line) })
				if err != nil {
					t.Fatal(err)
				}
				j := r.job(id) // takes Platform.mu: a launch that blocked under it would block here
				if j.State != Failed || j.StartErr == nil || j.PID != 0 || !j.Finished {
					t.Errorf("state %s, StartErr %v, PID %d, finished %v; want FAILED at once with an error and no process", j.State, j.StartErr, j.PID, j.Finished)
				}
				if _, err := os.Stat(ran); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("the job ran: %v", err)
				}
				if next := r.submit("true"); r.waitFinished(next).State != Completed {
					t.Errorf("the next job did not complete")
				}
			})
		}
	}
}

// The job's output descriptor is a blocking one: the open is non-blocking,
// and the flag is cleared before the child inherits the descriptor. The job
// reads its own descriptor's flags from /proc, where fdinfo prints them in
// octal.
func TestRunModeOutputDescriptorIsBlocking(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.submit("cat /proc/self/fdinfo/1; cat /proc/self/fdinfo/2 >&2", "--error="+r.home+"/err.out")
	r.waitFinished(id)
	for _, name := range []string{r.home + "/slurm-" + itoa(id) + ".out", r.home + "/err.out"} {
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		var flags int64 = -1
		for _, l := range strings.Split(string(b), "\n") {
			if v, ok := strings.CutPrefix(l, "flags:"); ok {
				if flags, err = strconv.ParseInt(strings.TrimSpace(v), 8, 64); err != nil {
					t.Fatal(err)
				}
			}
		}
		if flags < 0 {
			t.Fatalf("%s has no flags line: %q", name, b)
		}
		if flags&syscall.O_NONBLOCK != 0 {
			t.Errorf("%s: the child's descriptor has flags %#o, with O_NONBLOCK set", name, flags)
		}
	}
}
