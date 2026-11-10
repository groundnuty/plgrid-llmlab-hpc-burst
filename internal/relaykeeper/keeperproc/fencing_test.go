package keeperproc

// HBEP-6's keeper items for E and the Lease fence (Test plan, The relay
// keeper): "an agent fenced at an alpha epoch accepts the keeper's first
// relays, at 2^31 plus the start count"; "a keeper restart's relays carry a
// higher E"; "a set or an acknowledgement from a lower Lease epoch is refused,
// including a stale controller that reconnects first after a keeper restart".
// And "A keeper restart cuts every host-dialed stream on the host": the
// restart test's stream across the restart is cut, as expected, and a new one
// crosses the restarted keeper's relays.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// An agent fenced at an alpha epoch: an alpha relay owner of Lease epoch 5,
// in the test process, relays the job first, so the job side keeps (5, N) on
// each channel; its relays are then dropped, as an alpha controller's are at
// a switch to the keeper. A serving stream opened with no session waits for
// one, and the keeper's first relays, at E = 2^31 + 1, are accepted: the
// stream crosses them, whole, and the job side shows a second session on
// each channel and no refusal.
//
// Mutant (process.go): the Runner's "Epoch: epoch" made "Epoch: 1": the
// keeper's relays carry (1, N), the agent refuses them as superseded, and
// the stream never gets a session.
func TestAnAgentFencedAtAnAlphaEpochAcceptsTheKeepersFirstRelays(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-kp-alfa-0123456789ab", 30*time.Second)
	own := w.alphaOwner(5)
	own.Want(w.target(j))
	waitFor(t, time.Minute, "the alpha relays' sessions", func() bool {
		st := j.status(t)
		return st[control].Connected && st[serving].Connected
	})
	for _, r := range own.Relays() {
		if r.Epoch != 5 || r.PID == 0 {
			t.Fatalf("the alpha %s relay %+v: want it up at epoch 5", r.Channel, r)
		}
	}
	w.roundTrip(j, serving, 16<<10)
	own.Drop(j.tag)
	waitFor(t, time.Minute, "the alpha sessions ended", func() bool {
		st := j.status(t)
		return !st[control].Connected && !st[serving].Connected
	})
	k := w.startKeeper("")
	s := w.connect(j, serving)
	if err := s.write(s.chunk(16 << 10)); err != nil {
		t.Fatal(err)
	}
	lead := w.startDouble("leader", k.sock)
	mustReport(t, lead.lead(w.set(provA, 1, true, w.entry(j, gen1))), "the leader's set")
	if err := s.greet(45 * time.Second); err != nil {
		t.Fatalf("the serving stream opened before the keeper's relays: %v", err)
	}
	s.mustEcho(64<<10, "over the keeper's relay")
	up := k.waitUp(provA, j)
	for _, ch := range agentapi.RelayChannels {
		if up[ch].Epoch != firstE {
			t.Errorf("the keeper's %s relay carries E %d, want 2^31+1 = %d", ch, up[ch].Epoch, firstE)
		}
	}
	st := j.status(t)
	for _, ch := range agentapi.RelayChannels {
		if st[ch].Sessions != 2 || st[ch].Refused != 0 || !st[ch].Connected {
			t.Errorf("the job side's %s channel %+v: want the alpha session and the keeper's, none refused", ch, st[ch])
		}
	}
	w.roundTrip(j, control, 16<<10)
	s.whole()
}

// A keeper restart: a stale double (Lease epoch 4) leads first, and a
// hand-over makes the leader epoch 5, so the keeper's fence for the Provider,
// on disk, is 5. The keeper restarts (its input ends; it stops every relay)
// on the same state directory. The serving stream across the restart is cut,
// which is expected: a keeper restart cuts every host-dialed stream on the
// host. The restarted keeper takes E = 2^31 + 2. The stale double reconnects
// first and is refused, naming the fence; the real leader is then accepted,
// and its relays carry the higher E, which the job side, fenced at the first
// run's (E, N), accepts: a new stream crosses them, whole.
//
// Mutants: process.go's "Epoch: epoch" made a constant 2^31 + 1, so the
// restarted keeper's relays carry the first run's E with a lower N and are
// refused as superseded; keeper.go's fence "if seen && epoch <
// f.HighestEpoch" made false, so the stale double's set is accepted.
func TestAKeeperRestartTakesAHigherEpochAndRefusesAStaleDouble(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-kp-rest-0123456789ab", 30*time.Second)
	k := w.startKeeper("")
	stale := w.startDouble("stale", k.sock)
	mustReport(t, stale.lead(w.set(provA, 4, true, w.entry(j, gen1))), "the first leader's set")
	k.waitUp(provA, j)
	lead := w.startDouble("leader", k.sock)
	mustReport(t, lead.lead(w.set(provA, 5, true, w.entry(j, gen1))), "the new leader's set")
	stale.pause()
	up := k.waitUp(provA, j)
	s := w.openStream(j, serving)
	s.mustEcho(64<<10, "before the keeper's restart")
	lead.pause()
	k2 := k.restart()
	if k2.epoch != firstE+1 {
		t.Fatalf("the restarted keeper took E %d, want 2^31+2 = %d", k2.epoch, firstE+1)
	}
	if !s.cut() {
		t.Error("the serving stream still echoes across the keeper's restart, which stops every relay")
	}
	if rs := k2.relaysOf(provA, j.tag); len(rs) != 0 {
		t.Errorf("the restarted keeper shows relays before any set: %+v", rs)
	}
	ref := mustRefuse(t, stale.set(w.set(provA, 4, true, w.entry(j, gen1))), "the stale double's set after the restart")
	if ref.Reason != wire.RefusedEpoch || ref.HighestEpoch != 5 {
		t.Errorf("the stale double's refusal %+v: want %s, naming the highest epoch 5", ref, wire.RefusedEpoch)
	}
	if r := mustReport(t, lead.lead(w.set(provA, 5, true, w.entry(j, gen1))), "the leader's set after the restart"); r.KeeperEpoch != firstE+1 {
		t.Errorf("the report names E %d, want %d", r.KeeperEpoch, firstE+1)
	}
	up2 := k2.waitUp(provA, j, up[control].PID, up[serving].PID)
	for _, ch := range agentapi.RelayChannels {
		if up2[ch].Epoch != firstE+1 {
			t.Errorf("the restarted keeper's %s relay carries E %d, want %d", ch, up2[ch].Epoch, firstE+1)
		}
	}
	st := j.status(t)
	for _, ch := range agentapi.RelayChannels {
		if st[ch].Sessions != 2 || st[ch].Refused != 0 {
			t.Errorf("the job side's %s channel %+v: want one session per keeper run, none refused", ch, st[ch])
		}
	}
	s2 := w.openStream(j, serving)
	s2.mustEcho(64<<10, "after the keeper's restart")
	s2.whole()
	if ps, _ := k2.provider(provA); ps.Refused[wire.RefusedEpoch] != 1 {
		t.Errorf("the restarted keeper counts %d refusals by epoch, want the stale double's 1", ps.Refused[wire.RefusedEpoch])
	}
}
