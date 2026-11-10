package relaykeeper

// New for issue #343 (PR 3): a Keeper with its real Runner (OwnerRunner) on
// the real path (realworld_test.go), for the Runner's tests. The sets reach
// the keeper through Handle, in-process. Between the keeper and the
// OwnerRunner sits hookRunner, which passes every call on and wraps each
// Want's callbacks (its listing, its gate, OnStart, StartFailed, OnFinal and
// the seeds' read) to record them and, where a test asks, to hold one at its
// point: so a test scripts the window between a listing and its gate, or
// between the gate and the process start, without a fixed sleep. The
// keeper's own callbacks run unchanged inside the wrappers.
//
// Every oracle is the keeper's or the job side's, never the code under test's
// return value: what the keeper holds (its in-flight starts, read under its
// lock), whether another Provider's set for the host is accepted, what the
// state file holds, what the fake ran (srun lines) and the bytes that cross.

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The Providers: rkProv's relays are the tests'; rkOther, the older
// Provider, probes whether the login host is free, since it wins a freed host
// (claims.go, oldestWanting).
const (
	rkProv  = "example-b"
	rkOther = "example-a"
)

// The generations a test's entries carry.
const (
	genOne = "1a2b3c4d5e6f7a8b"
	genTwo = "2b3c4d5e6f7a8b9c"
)

// latch holds its waiters until it is opened, or the test ends.
type latch struct {
	c chan struct{}
	o sync.Once
}

func newLatch() *latch { return &latch{c: make(chan struct{})} }
func (l *latch) open() { l.o.Do(func() { close(l.c) }) }
func (l *latch) isOpen() bool {
	select {
	case <-l.c:
		return true
	default:
		return false
	}
}

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

// realRig is a Keeper with the real Runner on a world.
type realRig struct {
	t     *testing.T
	w     *rkWorld
	k     *Keeper
	hr    *hookRunner
	rr    *OwnerRunner
	slots *hostdial.Slots
	clock *shiftClock
	state string
	logs  *logRecorder
	done  chan struct{}
	// restoreClaim, when set, puts back a claim a test took from rkProv.
	restoreClaim func()
}

// rigOptions change a rig's Runner and keeper.
type rigOptions struct {
	runner     func(*OwnerRunnerOptions)
	claimLapse time.Duration
}

// newRealRig opens a keeper on a fresh state file whose Runner is the real
// one, with short backoffs, the test's Slots of HBEP-4's 4 per login host,
// and the keeper's -F files in the world's directory. At the test's end
// every hook's wait ends, then the Runner stops every relay through its
// pidfd.
func newRealRig(t *testing.T, w *rkWorld, ro rigOptions) *realRig {
	t.Helper()
	rg := &realRig{t: t, w: w, slots: hostdial.NewSlots(hostdial.DefaultRelaysPerLogin), clock: &shiftClock{},
		state: filepath.Join(t.TempDir(), "relaykeeper", "state.json"), logs: &logRecorder{t: t}, done: make(chan struct{})}
	log := slog.New(rg.logs)
	k, err := Open(Options{State: rg.state, Now: rg.clock.Now, ClaimLapse: ro.claimLapse, Logger: log,
		Runner: func(e int64) (Runner, error) {
			o := OwnerRunnerOptions{Epoch: e, Ports: w.ports(), Slots: rg.slots, SSHDir: filepath.Join(w.base, "k"),
				MinBackoff: 100 * time.Millisecond, MaxBackoff: 400 * time.Millisecond, StopGrace: 2 * time.Second, Logger: log}
			if ro.runner != nil {
				ro.runner(&o)
			}
			rr, err := NewOwnerRunner(context.Background(), o)
			if err != nil {
				return nil, err
			}
			rg.rr = rr
			rg.hr = &hookRunner{OwnerRunner: rr, r: rg}
			return rg.hr, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	rg.k = k
	t.Cleanup(func() {
		stopped := make(chan struct{})
		go func() { rg.rr.Stop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(60 * time.Second):
			t.Error("the Runner did not stop within 60 s")
		}
		if t.Failed() {
			rg.logs.dump()
		}
	})
	t.Cleanup(func() { close(rg.done) }) // runs first: no hook waits past the test
	return rg
}

// plan sets the hooks of each Want from now on.
func (rg *realRig) plan(f func(hw *hookedWant)) {
	rg.hr.mu.Lock()
	defer rg.hr.mu.Unlock()
	rg.hr.plan = f
}

// set is a valid set of p for the world's login node with entries, valid
// for MaxValidFor from now.
func (rg *realRig) set(p string, complete bool, entries ...wire.Entry) wire.DesiredSet {
	s := set(p, 1, complete, "", entries...)
	s.SSH = rg.w.ssh()
	s.ValidForMS = wire.MaxValidFor.Milliseconds()
	return s
}

// host is the login host as the keeper resolved it.
func (rg *realRig) host() string { return rg.rr.LoginHost(rkProv, rg.w.ssh()) }

// probe is rkOther's set for the login host, with one Allocation that
// carries RelayEndedForGood: it claims the host when it is free, and starts
// nothing.
func (rg *realRig) probe() wire.DesiredSet {
	e := wire.Entry{UID: "0a1b2c3d-0000-4000-9200-000000000001", Tag: "hb-rk-probe-0123456789ab", JobID: "77",
		AllocDir: "/stage/alloc/hb-rk-probe-0123456789ab", AgentPath: "/stage/agent/e/hb-agent", Gen: genOne, EndedForGood: true}
	return rg.set(rkOther, true, e)
}

// probeAccepted reports whether rkOther's probe set is accepted.
func (rg *realRig) probeAccepted() bool {
	kind, _ := send(rg.t, rg.k, wire.KindSet, rg.probe())
	return kind == wire.KindReport
}

// holds are the keeper's starts in flight, read under its lock.
func (rg *realRig) holds() int {
	rg.k.mu.Lock()
	defer rg.k.mu.Unlock()
	return len(rg.k.inflight)
}

// upOf are p's relays the Runner lists up.
func (rg *realRig) upOf(p string) []wire.RelayState {
	var out []wire.RelayState
	for _, r := range rg.rr.Relays(p) {
		if r.State == wire.StateUp {
			out = append(out, r)
		}
	}
	return out
}

// waitPair waits until both relays of tag are listed up, each with a PID,
// and returns them.
func (rg *realRig) waitPair(p, tag string) map[agentapi.RelayChannel]wire.RelayState {
	rg.t.Helper()
	return rg.waitPairWhere(p, tag, func(wire.RelayState) bool { return true })
}

// waitPairWhere is waitPair for the relays keep accepts.
func (rg *realRig) waitPairWhere(p, tag string, keep func(wire.RelayState) bool) map[agentapi.RelayChannel]wire.RelayState {
	rg.t.Helper()
	out := map[agentapi.RelayChannel]wire.RelayState{}
	rkWaitFor(rg.t, time.Minute, "both relays of "+tag+" up", func() bool {
		clear(out)
		for _, r := range rg.upOf(p) {
			if r.Tag == tag && r.PID != 0 && keep(r) {
				out[r.Channel] = r
			}
		}
		return len(out) == 2
	})
	return out
}

// ownerRelay is the Provider's Owner's own state of a relay of tag on ch.
func (rg *realRig) ownerRelay(p, tag string, ch agentapi.RelayChannel) hostdial.RelayState {
	rg.rr.mu.Lock()
	pr := rg.rr.provs[p]
	rg.rr.mu.Unlock()
	if pr == nil || pr.own == nil {
		return hostdial.RelayState{}
	}
	for _, r := range pr.own.Relays() {
		if r.Tag == tag && r.Channel == ch {
			return r
		}
	}
	return hostdial.RelayState{}
}

// hooked is the first Want of tag at gen.
func (rg *realRig) hooked(tag, gen string) *hookedWant {
	rg.t.Helper()
	rg.hr.mu.Lock()
	defer rg.hr.mu.Unlock()
	for _, hw := range rg.hr.wants {
		if hw.w.Entry.Tag == tag && hw.w.Entry.Gen == gen {
			return hw
		}
	}
	rg.t.Fatalf("no Want of %s at %s", tag, gen)
	return nil
}

// ends are the final ends the state file holds; a state file the keeper's
// own LoadState refuses fails the test.
func (rg *realRig) ends() []EndRecord {
	rg.t.Helper()
	s, err := LoadState(rg.state)
	if err != nil {
		rg.t.Errorf("the keeper's state file does not load: %v", err)
		return nil
	}
	return s.Ends
}

// rawEnds are the ends the state file holds, read without the state's
// checks.
func (rg *realRig) rawEnds() []EndRecord {
	rg.t.Helper()
	b, err := os.ReadFile(rg.state)
	if err != nil {
		rg.t.Fatal(err)
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		rg.t.Fatal(err)
	}
	return s.Ends
}

// noEnds fails the test when the state file holds an end, or rkProv's
// report names one.
func (rg *realRig) noEnds() {
	rg.t.Helper()
	if e := rg.ends(); len(e) != 0 {
		rg.t.Errorf("the state file holds ends %+v, want none: nothing is marked", e)
	}
	rg.k.mu.Lock()
	rep := rg.k.report(rkProv, rg.host(), false)
	rg.k.mu.Unlock()
	if len(rep.Ends) != 0 {
		rg.t.Errorf("rkProv's report names ends %+v, want none", rep.Ends)
	}
}

// doubleResolutions are the keeper's warnings of a start resolved with none
// in flight: a Runner that resolved a start twice, or one the gate never
// allowed.
func (rg *realRig) doubleResolutions() int {
	return len(rg.logs.records("a start was resolved with none in flight"))
}

// freeAfter is the closing oracle of a test: rkProv's complete set with no
// Allocation stops its relays and lets its claim lapse; once its processes
// are gone, no start of any Provider is in flight, and rkOther's set for the
// host is accepted.
func (rg *realRig) freeAfter() {
	rg.t.Helper()
	accepted(rg.t, rg.k, rg.set(rkProv, true))
	rkWaitFor(rg.t, time.Minute, "rkProv's relays gone", func() bool { return len(rg.upOf(rkProv)) == 0 })
	if n := rg.holds(); n != 0 {
		rg.t.Errorf("%d starts in flight once every relay is gone, want 0", n)
	}
	if !rg.probeAccepted() {
		rg.t.Error("another Provider's set for the host was refused with no relay up and no start in flight there: the host was not freed")
	}
}
