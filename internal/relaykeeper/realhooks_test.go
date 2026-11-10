package relaykeeper

// New for issue #343 (PR 3): the hooks of the Runner's tests
// (realrunner_test.go): hookRunner passes every call on to the real Runner
// and wraps each Want's callbacks, the keeper's own inside, to record them
// and, where a test asks, to hold one at its point.

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// latch holds its waiters until it is opened, or the test ends.
type latch struct {
	c chan struct{}
	o sync.Once
}

func newLatch() *latch { return &latch{c: make(chan struct{})} }
func (l *latch) open() { l.o.Do(func() { close(l.c) }) }
func (l *latch) wait(done <-chan struct{}) {
	select {
	case <-l.c:
	case <-done:
	}
}

// shiftClock is the real clock moved forward by what a test adds: its
// readings keep their monotonic part, as the keeper's expiries need.
type shiftClock struct{ off atomic.Int64 }

func (c *shiftClock) Now() time.Time          { return time.Now().Add(time.Duration(c.off.Load())) }
func (c *shiftClock) advance(d time.Duration) { c.off.Add(int64(d)) }

// gateCall is one answer of the keeper's gate.
type gateCall struct {
	ch         agentapi.RelayChannel
	epoch, seq int64
	ok         bool
}

// startCall is one OnStart: its (E, N), and what the Runner's Relays showed
// of that relay just before the keeper was told (duty 3).
type startCall struct {
	ch         agentapi.RelayChannel
	epoch, seq int64
	shown      bool // listed up, with this N, before OnStart reached the keeper
	pid        int  // the PID it was listed with
}

// hookedWant is one Want as the keeper made it, with the test's hooks. Its
// latches are set by the rig's plan, before the Want reaches the Runner.
type hookedWant struct {
	n    int
	p    string
	host string // the set's HostName
	w    Want   // the keeper's own
	r    *realRig

	beforeList *latch                           // every listing waits for it, before the keeper's
	afterList  *latch                           // every listing that showed the job waits for it, after the keeper's
	afterGate  map[agentapi.RelayChannel]*latch // every start of the channel the gate allowed waits for it, before the Runner goes on
	finals     *latch                           // the Runner's read of the seeds waits for it
	// awaitTrap holds each OnStart, before the keeper is told and so before
	// hostdial's stop can signal the process, until the relay's lingering ssh
	// has set its TERM to be ignored (rkWorld.lingerSeq).
	awaitTrap bool

	mu        sync.Mutex
	lists     int // listings asked
	listed    int // listings that showed the job
	refusals  int // listings that did not
	afterWait int // listings waiting after the keeper's
	gates     []gateCall
	atGate    map[agentapi.RelayChannel]int // allowed starts waiting after the gate
	starts    []startCall
	failed    map[agentapi.RelayChannel]int
	finalEnds []string
}

func (hw *hookedWant) count(f func() int) int {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return f()
}

// gatesOf are the gate's answers so far.
func (hw *hookedWant) gatesOf() []gateCall {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return slices.Clone(hw.gates)
}

// startsOf are the OnStarts so far.
func (hw *hookedWant) startsOf() []startCall {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return slices.Clone(hw.starts)
}

// waitingAtGate is how many allowed starts of ch wait after the gate.
func (hw *hookedWant) waitingAtGate(ch agentapi.RelayChannel) int {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return hw.atGate[ch]
}

// hookedJobs is a Want's listing, wrapped.
type hookedJobs struct{ hw *hookedWant }

func (j hookedJobs) ListByTag(ctx context.Context, tag string) ([]provider.Job, error) {
	hw := j.hw
	hw.mu.Lock()
	hw.lists++
	hw.mu.Unlock()
	if hw.beforeList != nil {
		hw.beforeList.wait(hw.r.done)
	}
	jobs, err := hw.w.Jobs.ListByTag(ctx, tag)
	shown := err == nil && len(jobs) > 0
	hw.mu.Lock()
	if shown {
		hw.listed++
	} else {
		hw.refusals++
	}
	hw.mu.Unlock()
	if shown && hw.afterList != nil {
		hw.mu.Lock()
		hw.afterWait++
		hw.mu.Unlock()
		hw.afterList.wait(hw.r.done)
		hw.mu.Lock()
		hw.afterWait--
		hw.mu.Unlock()
	}
	return jobs, err
}

// wrapped is the Want the Runner gets: the keeper's callbacks inside the
// test's.
func (hw *hookedWant) wrapped() Want {
	w := hw.w
	out := w
	out.Jobs = hookedJobs{hw}
	out.Finals = func() map[agentapi.RelayChannel]string {
		if hw.finals != nil {
			hw.finals.wait(hw.r.done)
		}
		return w.Finals()
	}
	out.BeforeStart = func(ch agentapi.RelayChannel, epoch, seq int64) bool {
		ok := w.BeforeStart(ch, epoch, seq)
		hw.mu.Lock()
		hw.gates = append(hw.gates, gateCall{ch: ch, epoch: epoch, seq: seq, ok: ok})
		l := hw.afterGate[ch]
		if ok && l != nil {
			hw.atGate[ch]++
		}
		hw.mu.Unlock()
		if ok && l != nil {
			l.wait(hw.r.done)
			hw.mu.Lock()
			hw.atGate[ch]--
			hw.mu.Unlock()
		}
		return ok
	}
	out.OnStart = func(ch agentapi.RelayChannel, epoch, seq int64) {
		sc := startCall{ch: ch, epoch: epoch, seq: seq}
		for _, r := range hw.r.rr.Relays(hw.p) {
			if r.Tag == w.Entry.Tag && r.Channel == ch && r.State == wire.StateUp && r.Seq == seq {
				sc.shown, sc.pid = true, r.PID
			}
		}
		hw.mu.Lock()
		hw.starts = append(hw.starts, sc)
		hw.mu.Unlock()
		if hw.awaitTrap {
			end := time.Now().Add(20 * time.Second)
			for !hw.r.w.trapped(seq) && time.Now().Before(end) {
				select {
				case <-hw.r.done:
					end = time.Now()
				case <-time.After(10 * time.Millisecond):
				}
			}
		}
		w.OnStart(ch, epoch, seq)
	}
	out.StartFailed = func(ch agentapi.RelayChannel) {
		hw.mu.Lock()
		hw.failed[ch]++
		hw.mu.Unlock()
		w.StartFailed(ch)
	}
	out.OnFinal = func(ch agentapi.RelayChannel, kind hostdial.EndKind, cause string, epoch, seq int64) {
		hw.mu.Lock()
		hw.finalEnds = append(hw.finalEnds, string(ch)+" "+kind.String())
		hw.mu.Unlock()
		w.OnFinal(ch, kind, cause, epoch, seq)
	}
	return out
}

// hookRunner is the real Runner with the test's hooks on each Want.
type hookRunner struct {
	*OwnerRunner
	r *realRig

	mu    sync.Mutex
	wants []*hookedWant
	plan  func(hw *hookedWant)
}

func (h *hookRunner) Want(p string, s wire.SSH, w Want) {
	hw := &hookedWant{p: p, host: s.HostName, w: w, r: h.r, afterGate: map[agentapi.RelayChannel]*latch{},
		atGate: map[agentapi.RelayChannel]int{}, failed: map[agentapi.RelayChannel]int{}}
	h.mu.Lock()
	hw.n = len(h.wants)
	h.wants = append(h.wants, hw)
	plan := h.plan
	h.mu.Unlock()
	if plan != nil {
		plan(hw)
	}
	h.OwnerRunner.Want(p, s, hw.wrapped())
}
