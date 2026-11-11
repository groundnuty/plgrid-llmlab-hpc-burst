package hostdial_test

// A relay whose stop was refused stays the Owner's until its process is reaped (issue #370, the review of
// PR 378). Target.OnExit is read by the relay keeper as "the process is gone" (runner.go: a dropped relay's
// process is listed as up until it is), and the Allocation's slots count the relays on a login node, so a stop
// that returns with the ssh alive must not clear its PID, give back its slots, start a successor beside it or
// call OnExit: all of that waits for the reap.

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// exitLog counts the OnExit calls of each channel.
type exitLog struct {
	mu sync.Mutex
	n  map[agentapi.RelayChannel]int
}

func (e *exitLog) add(ch agentapi.RelayChannel) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.n == nil {
		e.n = map[agentapi.RelayChannel]int{}
	}
	e.n[ch]++
}

func (e *exitLog) of(ch agentapi.RelayChannel) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.n[ch]
}

func (e *exitLog) total() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	t := 0
	for _, n := range e.n {
		t += n
	}
	return t
}

// procRefusal makes the reads of /proc/<pid>/stat of the chosen processes fail, as an exhausted descriptor table
// does: the start-time guard cannot read, so every group signal to those processes is refused (leader-checkable).
// A process it does not name is read as usual, so a successor's start (which pins the process by reading it)
// is not refused.
type procRefusal struct {
	mu   sync.Mutex
	pids map[string]bool
}

func (r *procRefusal) refuse(pids ...int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pids == nil {
		r.pids = map[string]bool{}
	}
	for _, p := range pids {
		r.pids["/proc/"+strconv.Itoa(p)+"/stat"] = true
	}
}

// allow ends every refusal.
func (r *procRefusal) allow() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pids = nil
}

func (r *procRefusal) refused(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pids[name]
}

func refuseProcReads(t *testing.T) *procRefusal {
	t.Helper()
	r := &procRefusal{}
	t.Cleanup(hostdial.SwapGroupProcRead(func(name string) ([]byte, error) {
		if r.refused(name) {
			return nil, syscall.EMFILE
		}
		return os.ReadFile(name)
	}))
	t.Cleanup(hostdial.SwapStopRetryPause(10 * time.Millisecond))
	return r
}

// pidsOf lists the relays' processes.
func pidsOf(up map[agentapi.RelayChannel]hostdial.RelayState) []int {
	var out []int
	for _, r := range up {
		out = append(out, r.PID)
	}
	return out
}

// pidfdsOf opens a pidfd on each relay's process, to end it by hand through a handle that cannot name another
// process; the descriptors close, and a process still alive is killed, when the test ends.
func pidfdsOf(t *testing.T, up map[agentapi.RelayChannel]hostdial.RelayState) map[agentapi.RelayChannel]int {
	t.Helper()
	fds := map[agentapi.RelayChannel]int{}
	for ch, r := range up {
		fd, err := unix.PidfdOpen(r.PID, 0)
		if err != nil {
			t.Fatalf("pidfd for the %s relay %d: %v", ch, r.PID, err)
		}
		fds[ch] = fd
		t.Cleanup(func() {
			_ = unix.PidfdSendSignal(fd, syscall.SIGKILL, nil, 0)
			_ = unix.Close(fd)
		})
	}
	return fds
}

// With every stop of an Allocation's relays refused, Drop returns and the relays run on; until they are reaped
// their slots stay taken (the login node's other Allocation cannot have them) and OnExit is not called. Once
// they are reaped, OnExit is called once per relay and the slots come back.
func TestADroppedRelayWhoseStopWasRefusedKeepsItsSlotsAndOnExitUntilItIsReaped(t *testing.T) {
	refusing := refuseProcReads(t)
	slots := hostdial.NewSlots(2)
	dir := t.TempDir()
	jobs := shellJobs{}
	exits := &exitLog{}
	tg := shellTarget(5, "hb-relay-stray-0123456789ab", writeScript(t, dir, "relay.sh", sleepRelaySh), dir, "", jobs)
	tg.OnExit = exits.add
	own := shellOwner(t, hostdial.OwnerOptions{Slots: slots})
	own.Want(tg)
	up := waitUp(t, own, tg.Tag)
	fds := pidfdsOf(t, up)

	refusing.refuse(pidsOf(up)...)
	dropped := make(chan struct{})
	go func() { own.Drop(tg.Tag); close(dropped) }()
	select {
	case <-dropped:
	case <-time.After(60 * time.Second):
		t.Fatal("Drop did not return with its relays' stops refused")
	}
	for ch, r := range up {
		if !alive(r.PID) {
			t.Fatalf("the %s relay %d ended, although nothing was sent to it", ch, r.PID)
		}
	}
	if n := exits.total(); n != 0 {
		t.Errorf("OnExit was called %d times for relays whose processes still run", n)
	}
	// The slots: the pair is not free while the relays run. The wait is for an answer that must not come, so
	// it is bounded; a release before this point would have answered at once.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	if release, err := slots.Acquire(ctx, tg.Login, 2); err == nil {
		release()
		t.Error("the login node's slots were given back while the relays still run")
	}
	cancel()

	// They are reaped: OnExit is called once for each, and the slots come back.
	refusing.allow()
	for ch, fd := range fds {
		if err := unix.PidfdSendSignal(fd, syscall.SIGKILL, nil, 0); err != nil {
			t.Fatalf("KILL to the %s relay: %v", ch, err)
		}
	}
	waitFor(t, 30*time.Second, "OnExit for both relays", func() bool { return exits.total() >= 2 })
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	release, err := slots.Acquire(ctx, tg.Login, 2)
	if err != nil {
		t.Fatalf("the slots did not come back after the relays were reaped: %v", err)
	}
	release()
	for _, ch := range agentapi.RelayChannels {
		if n := exits.of(ch); n != 1 {
			t.Errorf("the %s relay's OnExit was called %d times, want once", ch, n)
		}
	}
}

// closedOutRelaySh ends the session at once (it closes its standard output, so the host side reads the end of
// the session) and keeps running, as a relay whose ssh outlives its session does.
const closedOutRelaySh = "exec 1>&-\nexec sleep 120\n"

// A relay whose session ended and whose stop was then refused is not started again beside its process: its PID
// stays listed, no successor starts, and OnExit is not called until the process is reaped; then the successor
// starts, and OnExit was called for the first.
func TestARelayWhoseStopWasRefusedGetsNoSuccessorBesideItsProcess(t *testing.T) {
	refusing := refuseProcReads(t)
	dir := t.TempDir()
	jobs := shellJobs{}
	exits := &exitLog{}
	tg := shellTarget(6, "hb-relay-beside-0123456789ab", writeScript(t, dir, "relay.sh", closedOutRelaySh), dir, "", jobs)
	tg.OnExit = exits.add
	own := shellOwner(t, hostdial.OwnerOptions{})
	own.Want(tg)
	// The refusal starts once both relays are started and pinned (a start-time read that fails refuses the
	// start itself); the Owner then waits its stop grace, 5 s, before it stops a relay whose session ended.
	waitFor(t, time.Minute, "both relays started", func() bool {
		return relayOf(own, tg.Tag, agentapi.RelayControl).PID != 0 && relayOf(own, tg.Tag, agentapi.RelayServing).PID != 0
	})
	started := map[agentapi.RelayChannel]hostdial.RelayState{}
	for _, ch := range agentapi.RelayChannels {
		started[ch] = relayOf(own, tg.Tag, ch)
	}
	refusing.refuse(pidsOf(started)...)
	pids := map[agentapi.RelayChannel]int{}
	waitFor(t, time.Minute, "both relays' sessions ended and their stops refused", func() bool {
		for _, ch := range agentapi.RelayChannels {
			r := relayOf(own, tg.Tag, ch)
			if r.PID == 0 || !strings.Contains(r.LastEnd, "may still run") {
				return false
			}
			pids[ch] = r.PID
		}
		return true
	})
	fds := pidfdsOf(t, map[agentapi.RelayChannel]hostdial.RelayState{
		agentapi.RelayControl: relayOf(own, tg.Tag, agentapi.RelayControl),
		agentapi.RelayServing: relayOf(own, tg.Tag, agentapi.RelayServing),
	})
	// The backoff here is at most 200 ms: a successor started beside the process would show within this
	// window. A bounded wait for something that must not happen.
	for end := time.Now().Add(1500 * time.Millisecond); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		for _, ch := range agentapi.RelayChannels {
			r := relayOf(own, tg.Tag, ch)
			if r.PID != pids[ch] || r.Starts != 1 {
				t.Fatalf("the %s relay: PID %d and %d starts while its process %d runs after a refused stop; want that PID and 1 start", ch, r.PID, r.Starts, pids[ch])
			}
		}
		if n := exits.total(); n != 0 {
			t.Fatalf("OnExit was called %d times for relays whose processes still run", n)
		}
	}

	refusing.allow()
	for ch, fd := range fds {
		if err := unix.PidfdSendSignal(fd, syscall.SIGKILL, nil, 0); err != nil {
			t.Fatalf("KILL to the %s relay: %v", ch, err)
		}
	}
	waitFor(t, 30*time.Second, "a successor of each relay", func() bool {
		for _, ch := range agentapi.RelayChannels {
			if r := relayOf(own, tg.Tag, ch); r.Starts < 2 {
				return false
			}
		}
		return true
	})
	for _, ch := range agentapi.RelayChannels {
		if n := exits.of(ch); n < 1 {
			t.Errorf("the %s relay's first process was reaped and OnExit was not called", ch)
		}
	}
}
