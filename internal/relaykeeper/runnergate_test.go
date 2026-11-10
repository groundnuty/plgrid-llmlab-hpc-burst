package relaykeeper

// New for issue #343 (PR 3): the keeper's gate on the real path (path 5,
// and the science agent's check items 1, 2 and 4 for ruling B). The window
// between a listing and its gate is scripted by holding the listing's
// return (afterList); the window between the gate and the process start, by
// holding the Want's gate callback after the keeper's gate allowed the start
// (afterGate): the keeper has taken the start's hold, and the Runner has not
// gone on to the process start yet. The Runner's own BeforeStart, which
// marks the start pending, runs after the hold is let go.

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// refusedAtTheGate waits until each channel of hw has been answered by the
// gate, and fails the test unless every answer refused, no process started,
// and no srun line carries any N the gate saw for hw.
func refusedAtTheGate(t *testing.T, rg *realRig, hw *hookedWant, job rkJob) {
	t.Helper()
	rkWaitFor(t, time.Minute, "both channels' gates answered", func() bool {
		seen := map[agentapi.RelayChannel]bool{}
		for _, g := range hw.gatesOf() {
			seen[g.ch] = true
		}
		return len(seen) == 2
	})
	for _, g := range hw.gatesOf() {
		if g.ok {
			t.Errorf("the gate allowed the old attempt: %+v", g)
		}
		if s := rg.w.srunsWithSeq(job.id, g.seq); len(s) != 0 {
			t.Errorf("the fake ran the refused attempt's srun line: %q", s)
		}
		for _, r := range rg.logs.records("relay started") {
			if r["seq"] == strconv.FormatInt(g.seq, 10) {
				t.Errorf("the refused attempt's ssh started: %v", r)
			}
		}
	}
	if s := hw.startsOf(); len(s) != 0 {
		t.Errorf("the refused attempt was told started: %+v", s)
	}
}

// Path 5 (a). A replacement generation arrives in the window between the
// listing and the gate: the old attempt is refused at the gate and never
// launches (no ssh, no srun line with its N); the replacement starts and
// carries bytes, every srun line the fake ran is the replacement's, nothing
// is recorded, and the host is freed afterwards.
func TestAReplacementInTheWindowKeepsTheOldAttemptFromLaunching(t *testing.T) {
	w := newRKWorld(t)
	j := w.submit("hb-rk-repl-0123456789ab", 30*time.Second)
	rg := newRealRig(t, w, rigOptions{})
	hold := newLatch()
	rg.plan(func(hw *hookedWant) {
		if hw.w.Entry.Gen == genOne {
			hw.afterList = hold
		}
	})
	accepted(t, rg.k, rg.set(rkProv, true, w.entry(j, genOne)))
	old := rg.hooked(j.tag, genOne)
	rkWaitFor(t, time.Minute, "both listings passed and waiting", func() bool { return old.count(func() int { return old.afterWait }) == 2 })
	accepted(t, rg.k, rg.set(rkProv, true, w.entry(j, genTwo)))
	hold.open()
	refusedAtTheGate(t, rg, old, j)
	nu := rg.hooked(j.tag, genTwo)
	rg.waitPair(rkProv, j.tag)
	for _, ch := range agentapi.RelayChannels {
		rkRoundTrip(t, j, ch, 16<<10)
	}
	allowed := map[string]bool{}
	for _, g := range nu.gatesOf() {
		if g.ok {
			allowed[strconv.FormatInt(g.seq, 10)] = true
		}
	}
	for _, l := range w.sruns(j.id) {
		f := strings.Fields(l)
		if !allowed[f[len(f)-1]] {
			t.Errorf("the fake ran %q, not a start the replacement's gate allowed", l)
		}
	}
	rg.noEnds()
	rg.freeAfter()
}

// Path 5 (c). A Drop arrives in the window (a complete set without the
// Allocation): the attempt is refused at the gate and never launches, the
// keeper holds nothing, nothing is recorded, and another Provider's set for
// the host is accepted at once.
func TestADropInTheWindowKeepsTheAttemptFromLaunching(t *testing.T) {
	w := newRKWorld(t)
	j := w.submit("hb-rk-drop-0123456789ab", 30*time.Second)
	rg := newRealRig(t, w, rigOptions{})
	hold := newLatch()
	rg.plan(func(hw *hookedWant) { hw.afterList = hold })
	accepted(t, rg.k, rg.set(rkProv, true, w.entry(j, genOne)))
	hw := rg.hooked(j.tag, genOne)
	rkWaitFor(t, time.Minute, "both listings passed and waiting", func() bool { return hw.count(func() int { return hw.afterWait }) == 2 })
	accepted(t, rg.k, rg.set(rkProv, true))
	hold.open()
	refusedAtTheGate(t, rg, hw, j)
	if s := w.sruns(j.id); len(s) != 0 {
		t.Errorf("the fake ran srun for the dropped Allocation: %q", s)
	}
	if n := rg.holds(); n != 0 {
		t.Errorf("the keeper holds %d starts, want none", n)
	}
	rg.noEnds()
	if !rg.probeAccepted() {
		t.Error("another Provider's set for the host was refused after the dropped attempt was refused at the gate")
	}
}

// Path 5 (d). The Provider moves to another login host in the window: the
// attempt on the first host is refused at the gate and never launches, the
// keeper holds nothing on the first host, and another Provider's set for it
// is accepted. (The moved relays dial a host where nothing listens.)
func TestAMoveInTheWindowKeepsTheAttemptFromLaunching(t *testing.T) {
	w := newRKWorld(t)
	j := w.submit("hb-rk-move-0123456789ab", 30*time.Second)
	rg := newRealRig(t, w, rigOptions{})
	hold := newLatch()
	rg.plan(func(hw *hookedWant) {
		if hw.n == 0 {
			hw.afterList = hold
		}
	})
	e := w.entry(j, genOne)
	accepted(t, rg.k, rg.set(rkProv, true, e))
	hw := rg.hooked(j.tag, genOne)
	host := rg.host()
	rkWaitFor(t, time.Minute, "both listings passed and waiting", func() bool { return hw.count(func() int { return hw.afterWait }) == 2 })
	moved := rg.set(rkProv, true, e)
	moved.SSH.HostName = "127.0.0.2"
	accepted(t, rg.k, moved)
	hold.open()
	refusedAtTheGate(t, rg, hw, j)
	if s := w.sruns(j.id); len(s) != 0 {
		t.Errorf("the fake ran srun on the first host after the move: %q", s)
	}
	rg.k.mu.Lock()
	for key := range rg.k.inflight {
		if key.host == host {
			t.Errorf("the keeper holds a start on the first host: %+v", key)
		}
	}
	rg.k.mu.Unlock()
	rg.noEnds()
	if !rg.probeAccepted() {
		t.Error("another Provider's set for the first host was refused after the Provider moved")
	}
}

// The science agent's check item 1: a gate refusal is no start, even when it
// repeats. The Runner's Slots here allow 6 relays per login host, so a third
// Allocation's relays get their slots and reach the gate, whose own count of
// HBEP-4's 4 refuses them, attempt after attempt: no srun line for them, no
// start, never final, nothing held, no end recorded or reported. Once one of
// the two Allocations up is dropped, the gate allows the third's starts, and
// they carry bytes.
func TestAGateRefusalIsNoStartOnTheRealPath(t *testing.T) {
	w := newRKWorld(t)
	a := w.submit("hb-rk-gra-0123456789ab", 30*time.Second)
	b := w.submit("hb-rk-grb-0123456789ab", 30*time.Second)
	c := w.submit("hb-rk-grc-0123456789ab", 30*time.Second)
	rg := newRealRig(t, w, rigOptions{runner: func(o *OwnerRunnerOptions) { o.Slots = hostdial.NewSlots(6) }})
	ea, eb, ec := w.entry(a, genOne), w.entry(b, genOne), w.entry(c, genOne)
	accepted(t, rg.k, rg.set(rkProv, true, ea, eb))
	rg.waitPair(rkProv, a.tag)
	rg.waitPair(rkProv, b.tag)
	for _, j := range []rkJob{a, b} {
		for _, ch := range agentapi.RelayChannels {
			rkRoundTrip(t, j, ch, 16<<10)
		}
	}
	accepted(t, rg.k, rg.set(rkProv, true, ea, eb, ec))
	hc := rg.hooked(c.tag, genOne)
	rkWaitFor(t, time.Minute, "three gate refusals of each of the third Allocation's channels", func() bool {
		n := map[agentapi.RelayChannel]int{}
		for _, g := range hc.gatesOf() {
			if !g.ok {
				n[g.ch]++
			}
		}
		return n[agentapi.RelayControl] >= 3 && n[agentapi.RelayServing] >= 3
	})
	for _, j := range []rkJob{a, b} {
		for _, ch := range agentapi.RelayChannels {
			rkRoundTrip(t, j, ch, 16<<10)
		}
	}
	for _, g := range hc.gatesOf() {
		if g.ok {
			t.Fatalf("the gate allowed a fifth relay: %+v", g)
		}
	}
	if s := w.sruns(c.id); len(s) != 0 {
		t.Errorf("the fake ran srun for refused starts: %q", s)
	}
	if s := hc.startsOf(); len(s) != 0 {
		t.Errorf("a refused start was told started: %+v", s)
	}
	for _, ch := range agentapi.RelayChannels {
		r := rg.ownerRelay(rkProv, c.tag, ch)
		if r.Starts != 0 || r.PID != 0 || r.Final || !strings.Contains(r.LastRefusal, "not allowed") {
			t.Errorf("the third Allocation's %s relay after refused starts: %+v, want no start, not final, refused by the gate", ch, r)
		}
	}
	if n := len(relaySSHs(t, a.id, b.id, c.id)); n != 4 {
		t.Errorf("%d relay ssh clients run, want 4", n)
	}
	rg.k.mu.Lock()
	for key := range rg.k.inflight {
		if key.tag == c.tag {
			t.Errorf("the keeper holds a start of the refused Allocation: %+v", key)
		}
	}
	rg.k.mu.Unlock()
	rg.noEnds()
	accepted(t, rg.k, rg.set(rkProv, true, eb, ec))
	rg.waitPair(rkProv, c.tag)
	for _, ch := range agentapi.RelayChannels {
		rkRoundTrip(t, c, ch, 16<<10)
	}
	rg.noEnds()
	rg.freeAfter()
}

// The science agent's check item 4: a start the gate allowed that never
// resolves keeps holding its host, past the set's expiry and the claim's
// lapse, and past a Drop: another Provider's set for the host is refused
// throughout, failing toward refusal. hostdial's process start cannot be made
// to hang through the Runner (cmd.Start returns once the child has exec'd),
// so the start is held between the keeper's gate and the process start, in
// the Want's gate callback, which the keeper cannot tell apart. Once the
// start is let go, it resolves, and the host is freed.
func TestAGatePassedStartThatNeverResolvesKeepsHolding(t *testing.T) {
	w := newRKWorld(t)
	j := w.submit("hb-rk-hang-0123456789ab", 30*time.Second)
	rg := newRealRig(t, w, rigOptions{claimLapse: time.Second})
	hold := newLatch()
	rg.plan(func(hw *hookedWant) {
		hw.afterGate[agentapi.RelayControl], hw.afterGate[agentapi.RelayServing] = hold, hold
	})
	accepted(t, rg.k, rg.set(rkProv, true, w.entry(j, genOne)))
	hw := rg.hooked(j.tag, genOne)
	rkWaitFor(t, time.Minute, "both starts allowed and held", func() bool {
		return hw.waitingAtGate(agentapi.RelayControl) == 1 && hw.waitingAtGate(agentapi.RelayServing) == 1
	})
	if n := rg.holds(); n != 2 {
		t.Fatalf("the keeper holds %d starts, want 2", n)
	}
	rg.clock.advance(wire.MaxValidFor + time.Second)
	rg.clock.advance(2 * time.Second) // and the claim's lapse of 1 s after it
	if rg.probeAccepted() {
		t.Fatal("another Provider's set was accepted past the set's expiry and the claim's lapse with two starts in flight")
	}
	accepted(t, rg.k, rg.set(rkProv, true))
	rg.clock.advance(2 * time.Second)
	if rg.probeAccepted() {
		t.Fatal("another Provider's set was accepted after a Drop with two starts in flight")
	}
	if n := rg.holds(); n != 2 {
		t.Errorf("the keeper holds %d starts after the expiry, the lapse and the Drop, want 2", n)
	}
	hold.open()
	rkWaitFor(t, time.Minute, "both starts resolved", func() bool { return rg.holds() == 0 })
	rkWaitFor(t, time.Minute, "the dropped relays' processes gone", func() bool { return len(rg.upOf(rkProv)) == 0 })
	if !rg.probeAccepted() {
		t.Error("another Provider's set was refused once the held starts resolved and their processes were gone")
	}
}
