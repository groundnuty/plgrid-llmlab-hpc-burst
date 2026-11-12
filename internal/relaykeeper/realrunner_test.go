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
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
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
func (rg *realRig) host() string {
	rg.t.Helper()
	host, err := rg.rr.LoginHost(rkProv, rg.w.ssh())
	if err != nil {
		rg.t.Fatalf("the login host: %v", err)
	}
	return host
}

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
	// A start of a relay the set drops may still be in flight until the
	// Drop lands; one that never resolves keeps the count above 0, and fails
	// the wait.
	rkWaitFor(rg.t, time.Minute, "rkProv's relays gone and no start in flight", func() bool { return len(rg.upOf(rkProv)) == 0 && rg.holds() == 0 })
	if !rg.probeAccepted() {
		rg.t.Error("another Provider's set for the host was refused with no relay up and no start in flight there: the host was not freed")
	}
}
