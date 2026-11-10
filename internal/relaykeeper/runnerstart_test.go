package relaykeeper

// New for issue #343 (PR 3): the Runner's success path on the real path
// (realrunner_test.go): a start the gate allows is shown up, with its PID,
// before the keeper's OnStart is told (duty 3), each is resolved exactly once
// per channel (duty 2), and OnStart carries the (E, N) the gate saw.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Path 7, duties 2 and 3. Both channels' starts pass the gate and are held
// there, so both are in flight at once: the keeper holds two starts, and
// another Provider's set for the host is refused. Control's start is let go
// first: the Runner lists it up, with a PID and the N the gate saw, before
// the keeper's OnStart is told; then the keeper holds one start, serving's,
// and the host is still refused. Then serving's. Each channel is resolved
// exactly once (one OnStart, no StartFailed, no resolution of a start with
// none in flight), OnStart carries the gate's (E, N) with E the keeper's, and
// bytes cross each channel. Afterwards the host is freed.
func TestAStartIsShownUpBeforeOnStartAndResolvedOncePerChannel(t *testing.T) {
	w := newRKWorld(t)
	j := w.submit("hb-rk-start-0123456789ab", 30*time.Second)
	rg := newRealRig(t, w, rigOptions{})
	ctl, srv := newLatch(), newLatch()
	rg.plan(func(hw *hookedWant) {
		hw.afterGate[agentapi.RelayControl], hw.afterGate[agentapi.RelayServing] = ctl, srv
	})
	accepted(t, rg.k, rg.set(rkProv, true, w.entry(j, genOne)))
	hw := rg.hooked(j.tag, genOne)
	rkWaitFor(t, time.Minute, "both channels' starts allowed and held after the gate", func() bool {
		return hw.waitingAtGate(agentapi.RelayControl) == 1 && hw.waitingAtGate(agentapi.RelayServing) == 1
	})
	if n := rg.holds(); n != 2 {
		t.Fatalf("the keeper holds %d starts with both channels allowed and not launched, want 2", n)
	}
	if rg.probeAccepted() {
		t.Fatal("another Provider's set was accepted while two starts were in flight on the host")
	}
	ctl.open()
	rkWaitFor(t, time.Minute, "control's OnStart", func() bool { return len(hw.startsOf()) == 1 })
	if n := rg.holds(); n != 1 {
		t.Errorf("the keeper holds %d starts once control's resolved and serving's is in flight, want 1", n)
	}
	if rg.probeAccepted() {
		t.Error("another Provider's set was accepted while serving's start was still in flight")
	}
	srv.open()
	up := rg.waitPair(rkProv, j.tag)
	rkWaitFor(t, time.Minute, "serving's OnStart", func() bool { return len(hw.startsOf()) == 2 })
	for _, ch := range agentapi.RelayChannels {
		rkRoundTrip(t, j, ch, 64<<10)
	}
	gates := map[agentapi.RelayChannel]gateCall{}
	for _, g := range hw.gatesOf() {
		if !g.ok {
			t.Errorf("the gate refused a start: %+v", g)
		}
		gates[g.ch] = g
	}
	seen := map[agentapi.RelayChannel]int{}
	for _, s := range hw.startsOf() {
		seen[s.ch]++
		g := gates[s.ch]
		if s.epoch != rg.k.Epoch() || s.epoch != g.epoch || s.seq != g.seq {
			t.Errorf("the %s channel's OnStart carried (%d, %d), want the gate's (%d, %d) with E %d", s.ch, s.epoch, s.seq, g.epoch, g.seq, rg.k.Epoch())
		}
		if !s.shown || s.pid == 0 || s.pid != up[s.ch].PID {
			t.Errorf("the %s channel's relay was not listed up with its PID %d before the keeper's OnStart (shown %v, PID %d)", s.ch, up[s.ch].PID, s.shown, s.pid)
		}
		if up[s.ch].Seq != s.seq || up[s.ch].Epoch != s.epoch {
			t.Errorf("the %s relay is listed with (%d, %d), want OnStart's (%d, %d)", s.ch, up[s.ch].Epoch, up[s.ch].Seq, s.epoch, s.seq)
		}
	}
	for _, ch := range agentapi.RelayChannels {
		if seen[ch] != 1 || hw.count(func() int { return hw.failed[ch] }) != 0 {
			t.Errorf("the %s channel was resolved by %d OnStarts and %d StartFaileds, want exactly one OnStart", ch, seen[ch], hw.failed[ch])
		}
	}
	if n := rg.doubleResolutions(); n != 0 {
		t.Errorf("the keeper saw %d resolutions of a start with none in flight, want 0", n)
	}
	if n := rg.holds(); n != 0 {
		t.Errorf("the keeper holds %d starts with both relays up, want 0", n)
	}
	if rg.probeAccepted() {
		t.Error("another Provider's set was accepted while both relays run on the host")
	}
	rg.noEnds()
	rg.freeAfter()
}
