package sshrun

// Tests for group.go's reap barrier and its fail-closed checks (PR 365 review
// round). The barrier test is an interleaving that is forced, not hoped for:
// the sink blocks while the leader exits, and the test waits on observable
// states only (the exit shown by the pidfd, the reaper parked on the mutex the
// signal holds), with a generous deadline that only a broken barrier reaches.

import (
	"errors"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// parkedOnMutex reports whether a goroutine that is inside fn is blocked on a
// mutex, read from the goroutine dump: the state a reap is in while a signal
// holds the mutex it needs.
func parkedOnMutex(fn string) bool {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for _, g := range strings.Split(string(buf), "\n\n") {
		head, _, _ := strings.Cut(g, "\n")
		if strings.Contains(g, fn) && (strings.Contains(head, "sync.Mutex.Lock") || strings.Contains(head, "semacquire")) {
			return true
		}
	}
	return false
}

// A signal that passed its guards holds the mutex through its kill, so the
// reap, which comes after the leader's exit, waits for it: the leader stays an
// unreaped zombie, and its group's number stays ours, until the sink returns.
// A signal after the reap began is refused. The leader here is exited by the
// test (its input closes), not by the signal, and the reap is the production
// one (groupLeader.reap, as the master's waiter and check use it).
func TestAReapWaitsForASignalThatIsUnderWay(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var enter sync.Once
	rec := &sinkRecorder{forward: func(int, syscall.Signal) error {
		enter.Do(func() { close(entered) })
		<-release
		return nil
	}}
	log, logs := capturedLog()
	cmd := exec.Command("cat")
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	g := newGroupLeader(log, rec.sink)
	if err := g.start(cmd); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = in.Close() })
	reaped := make(chan error, 1)
	go func() { reaped <- g.reap(cmd) }()
	signalled := make(chan error, 1)
	go func() { signalled <- g.signal(syscall.SIGTERM) }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the signal never reached the sink")
	}

	// The leader exits while the sink holds the mutex.
	_ = in.Close()
	waitExited(t, g.fd, 10*time.Second)
	deadline := time.Now().Add(10 * time.Second)
	for !parkedOnMutex("(*groupLeader).reap") {
		if time.Now().After(deadline) {
			t.Fatal("the reap is not waiting for the signal's mutex: it went on (or never saw the exit)")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := pidfdWaitid(g.fd, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT); err != nil {
		t.Errorf("the leader was reaped while a signal was under way: %v", err)
	}
	select {
	case err := <-reaped:
		t.Fatalf("the reap returned while a signal was under way: %v", err)
	default:
	}

	close(release)
	if err := <-signalled; err != nil {
		t.Errorf("the signal under way: %v", err)
	}
	select {
	case <-reaped:
	case <-time.After(10 * time.Second):
		t.Fatal("the reap did not go on after the signal returned")
	}
	// Reaping is set: a later signal is refused and sends nothing.
	err = g.signal(syscall.SIGKILL)
	var ge *groupGuardError
	if !errors.As(err, &ge) || ge.Guard != guardLeaderGone || !strings.Contains(ge.Detail, "being reaped") {
		t.Errorf("a signal after the reap began: got %v, want a refusal by %q (being reaped)", err, guardLeaderGone)
	}
	if got := rec.got(); len(got) != 1 {
		t.Errorf("the sink saw %+v, want only the signal that was under way", got)
	}
	if out := logs.String(); !strings.Contains(out, "guard="+guardLeaderGone) {
		t.Errorf("the refusal is not in the log: %s", out)
	}
}

// Reaping set, the guards refuse: the leader is alive and ours, so only this
// guard does.
func TestGroupSignalRefusesALeaderBeingReaped(t *testing.T) {
	rec := &sinkRecorder{}
	c := startCat(t, rec)
	c.g.markReaping()
	err := refused(t, c.g, c.logs, rec, guardLeaderGone)
	if !strings.Contains(err.Error(), "being reaped") {
		t.Errorf("not the reaping refusal: %v", err)
	}
}

// The checks that fail fail closed: with no pidfd group signal to fall back
// on, a check that cannot tell is a refusal, not a pass. Each fixture is a
// live pinned leader whose other checks all pass, with one call made to fail.
func TestGroupSignalRefusesWhenACheckFails(t *testing.T) {
	t.Run("poll_fails", func(t *testing.T) {
		rec := &sinkRecorder{}
		c := startCat(t, rec)
		swap(t, &pidfdPoll, func([]unix.PollFd, int) (int, error) { return -1, syscall.EBADF })
		err := refused(t, c.g, c.logs, rec, guardCannotTell)
		if !strings.Contains(err.Error(), "poll") {
			t.Errorf("not the poll refusal: %v", err)
		}
	})
	t.Run("waitid_fails_unexpectedly", func(t *testing.T) {
		rec := &sinkRecorder{}
		c := startCat(t, rec)
		swap(t, &pidfdWaitid, func(int, int) error { return syscall.EINVAL })
		err := refused(t, c.g, c.logs, rec, guardCannotTell)
		if !strings.Contains(err.Error(), "waitid") {
			t.Errorf("not the waitid refusal: %v", err)
		}
	})
	t.Run("start_time_unreadable", func(t *testing.T) {
		rec := &sinkRecorder{}
		c := startCat(t, rec)
		swap(t, &procRead, procFailing) // after the start: the pin was read already
		err := refused(t, c.g, c.logs, rec, guardCannotTell)
		if !strings.Contains(err.Error(), "start time") {
			t.Errorf("not the start-time refusal: %v", err)
		}
	})
}
