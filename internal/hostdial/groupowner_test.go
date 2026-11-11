package hostdial_test

// Owner-level tests for the default path's guarded stop (issue #370), over the
// real Owner and relay doubles (shell scripts, as options_test.go's): the stop
// of alpha's relays goes through the group signal sink, one TERM to each
// relay's own group, and a stop whose signals a guard refuses is reported and
// leaves the relays running, without Drop hanging. The seams (export_test.go)
// are swapped before the Owner starts and restored after it stopped.

import (
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
)

type groupCall struct {
	pgid int
	sig  syscall.Signal
}

// recordGroupSignals swaps the sink for one that records each call and forwards
// it to the real group signal; the swap is undone at the test's end.
func recordGroupSignals(t *testing.T) func() []groupCall {
	t.Helper()
	var mu sync.Mutex
	var calls []groupCall
	t.Cleanup(hostdial.SwapGroupSink(func(pgid int, sig syscall.Signal) error {
		mu.Lock()
		calls = append(calls, groupCall{pgid, sig})
		mu.Unlock()
		return hostdial.KillGroup(pgid, sig)
	}))
	return func() []groupCall {
		mu.Lock()
		defer mu.Unlock()
		return append([]groupCall(nil), calls...)
	}
}

const sleepRelaySh = "exec sleep 120\n"

// Drop stops each of alpha's relays with one TERM, sent through the sink to
// the PGID of that relay's own group (its PID: the relay leads its group),
// and the relays are gone when Drop returns.
func TestAnOwnersDefaultStopSignalsEachRelaysGroupThroughTheSink(t *testing.T) {
	calls := recordGroupSignals(t)
	dir := t.TempDir()
	jobs := shellJobs{}
	tg := shellTarget(1, "hb-relay-sink-0123456789ab", writeScript(t, dir, "relay.sh", sleepRelaySh), dir, "", jobs)
	own := shellOwner(t, hostdial.OwnerOptions{})
	own.Want(tg)
	up := waitUp(t, own, tg.Tag)
	own.Drop(tg.Tag)

	got := map[int][]syscall.Signal{}
	for _, c := range calls() {
		got[c.pgid] = append(got[c.pgid], c.sig)
	}
	for ch, r := range up {
		if sigs := got[r.PID]; len(sigs) != 1 || sigs[0] != syscall.SIGTERM {
			t.Errorf("the %s relay %d got %v through the sink, want one SIGTERM", ch, r.PID, sigs)
		}
		if alive(r.PID) {
			t.Errorf("the %s relay %d outlived Drop", ch, r.PID)
		}
	}
	if len(got) != len(up) {
		t.Errorf("the sink saw %d groups, want the %d relays' own: %v", len(got), len(up), got)
	}
}

// A stop that a guard refuses (the start-time guard cannot read /proc) sends
// nothing and is reported; the relays run on, and Drop still returns: nothing
// waits for a process that was not signalled. The test ends them by hand,
// through pidfds opened on the processes the Owner started.
func TestAnOwnersStopThatIsRefusedLeavesTheRelaysRunningAndDropReturns(t *testing.T) {
	calls := recordGroupSignals(t)
	var failing atomic.Bool
	t.Cleanup(hostdial.SwapGroupProcRead(func(name string) ([]byte, error) {
		if failing.Load() && strings.HasPrefix(name, "/proc/") && strings.HasSuffix(name, "/stat") {
			return nil, syscall.EMFILE
		}
		return os.ReadFile(name)
	}))
	t.Cleanup(hostdial.SwapStopRetryPause(10 * time.Millisecond))
	dir := t.TempDir()
	jobs := shellJobs{}
	tg := shellTarget(2, "hb-relay-refuse-0123456789ab", writeScript(t, dir, "relay.sh", sleepRelaySh), dir, "", jobs)
	own := shellOwner(t, hostdial.OwnerOptions{})
	own.Want(tg)
	up := waitUp(t, own, tg.Tag)
	for ch, r := range up {
		fd, err := unix.PidfdOpen(r.PID, 0)
		if err != nil {
			t.Fatalf("pidfd for the %s relay %d: %v", ch, r.PID, err)
		}
		t.Cleanup(func() {
			_ = unix.PidfdSendSignal(fd, syscall.SIGKILL, nil, 0)
			_ = unix.Close(fd)
		})
	}

	failing.Store(true)
	dropped := make(chan struct{})
	go func() { own.Drop(tg.Tag); close(dropped) }()
	select {
	case <-dropped:
	case <-time.After(60 * time.Second):
		t.Fatal("Drop did not return with its relays' stops refused")
	}
	if got := calls(); len(got) != 0 {
		t.Errorf("a refused signal reached the sink: %+v", got)
	}
	for ch, r := range up {
		if !alive(r.PID) {
			t.Errorf("the %s relay %d ended, although nothing was sent to it", ch, r.PID)
		}
	}
	failing.Store(false)
}
