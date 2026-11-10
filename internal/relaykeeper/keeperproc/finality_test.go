package keeperproc

// HBEP-6's keeper items for final ends (Test plan, The relay keeper): "a
// final-ended channel whose end was acknowledged isn't restarted after a
// keeper restart, nor on a new credentials generation"; "an Allocation with
// endedForGood gets no relay". Once a final end is held for an Allocation,
// neither of its relays starts again (keeper.go's package doc, jobs: the
// listing is by tag, not by channel), and the relays that run are kept.

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// An acknowledged final end: the fake's step-limit refusal makes the control
// relay's restart fail, a final end the keeper reports for the mark while
// the serving stream runs on. The leader acknowledges it as marked, and it is
// reported no more. The step limit is then lifted, so a start would now
// succeed. After a keeper restart (which cuts the stream, as every keeper
// restart cuts every host-dialed stream) and on a new credentials generation,
// neither of the Allocation's relays starts: the keeper's log shows the
// control channel seeded final and the serving one's starts refused, and no
// srun line for the job reaches the login node. The positive control is a
// second Allocation in the same sets: its relays start after the restart and
// restart on the new generation, and carry bytes.
//
// Mutant (keeper.go, jobs.authorized): "&& len(k.endsOf(j.provider, e)) ==
// 0" dropped, so the serving relay starts after the restart.
func TestAnAcknowledgedFinalEndIsNotRestarted(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-kp-fina-0123456789ab", 30*time.Second)
	j2 := w.submit("hb-kp-finb-0123456789ab", 30*time.Second)
	k := w.startKeeper("")
	lead := w.startDouble("leader", k.sock)
	first := w.set(provA, 1, true, w.entry(j, gen1), w.entry(j2, gen1))
	mustReport(t, lead.lead(first), "the leader's set")
	up := k.waitUp(provA, j)
	k.waitUp(provA, j2)
	s := w.openStream(j, serving)
	s.mustEcho(64<<10, "before the failed end")
	waitFor(t, time.Minute, "both relays' srun steps logged", func() bool { return len(w.sruns(j.id)) == 2 })
	if err := w.plat.SetStepLimit(atoi(t, j.id), 2); err != nil {
		t.Fatal(err)
	}
	k.pins(map[agentapi.RelayChannel]wire.RelayState{control: up[control]})[control].kill(t, "the control relay's ssh")
	var end wire.FinalEnd
	waitFor(t, time.Minute, "the keeper's report of the control relay's failed end", func() bool {
		r := mustReport(t, lead.lead(first), "the leader's set")
		for _, e := range r.Ends {
			if e.Tag == j.tag && e.Channel == control {
				end = e
				return true
			}
		}
		return false
	})
	if end.Kind != wire.EndFailed || end.UID != j.uid || end.JobID != j.id || !strings.Contains(end.Cause, "step limit") || end.Epoch != firstE {
		t.Errorf("the reported end %+v: want the control channel's failed end at the step limit, at E %d", end, firstE)
	}
	s.mustEcho(32<<10, "after the control relay's failed end")
	a := lead.ack(wire.Ack{Provider: provA.name, LeaseEpoch: 1, UID: j.uid, JobID: j.id, Channel: control, Epoch: end.Epoch, Seq: end.Seq, Outcome: wire.OutcomeMarked})
	if a.Kind != string(wire.KindAcked) || a.Acked == nil || !a.Acked.Decided {
		t.Fatalf("the acknowledgement was answered %+v, want decided", a)
	}
	if r := mustReport(t, lead.lead(first), "the leader's set after the acknowledgement"); len(r.Ends) != 0 {
		t.Errorf("a decided end is still reported: %+v", r.Ends)
	}
	s.mustEcho(32<<10, "after the acknowledgement")
	s.whole()
	if err := w.plat.SetStepLimit(atoi(t, j.id), 0); err != nil {
		t.Fatal(err)
	}
	lead.pause()
	k2 := k.restart()
	if !s.cut() {
		t.Error("the serving stream still echoes across the keeper's restart")
	}
	runs := len(w.sruns(j.id))
	m := k2.log.mark()
	mustReport(t, lead.lead(first), "the leader's set after the restart")
	up2 := k2.waitUp(provA, j2)
	w.roundTrip(j2, serving, 16<<10)
	k2.log.waitLine(t, m, "the control relay seeded final after the restart", relayLog(logSeeded, j.tag, control)...)
	k2.log.waitLine(t, m, "the serving relay's start refused after the restart", append(relayLog(logNotStarted, j.tag, serving), whyNotListed)...)
	if n := len(w.sruns(j.id)); n != runs {
		t.Errorf("%d srun lines for the job after the keeper's restart, want none: a held end's Allocation starts no relay", n-runs)
	}
	m2 := k2.log.mark()
	mustReport(t, lead.lead(w.set(provA, 1, true, w.entry(j, gen2), w.entry(j2, gen2))), "the leader's set of a new generation")
	k2.waitUp(provA, j2, up2[control].PID, up2[serving].PID)
	w.roundTrip(j2, serving, 16<<10)
	k2.log.waitLine(t, m2, "the control relay seeded final on the new generation", relayLog(logSeeded, j.tag, control)...)
	k2.log.waitLine(t, m2, "the serving relay's start refused on the new generation", append(relayLog(logNotStarted, j.tag, serving), whyNotListed)...)
	if n := len(w.sruns(j.id)); n != runs {
		t.Errorf("%d srun lines for the job on a new generation, want none", n-runs)
	}
	for _, r := range k2.relaysOf(provA, j.tag) {
		if r.State == wire.StateUp {
			t.Errorf("the held end's Allocation's %s relay %+v is up", r.Channel, r)
		}
	}
	if got := k2.log.since(m, logStarted, "tag="+j.tag); len(got) != 0 {
		t.Errorf("a relay of the held end's Allocation started: %q", got)
	}
}

// An Allocation with endedForGood gets no relay. The first Allocation's
// relays run, with a serving stream, before a set marks it endedForGood:
// they are kept as they are, and the stream runs on, whole; but its control
// relay's ssh, ended through its verified pin, is not restarted (its start
// is refused, and no srun line follows). The second Allocation, endedForGood
// in its first set, never gets a relay: no srun line, no relay in the status,
// and a connection gets no session within its job's wait. A new credentials
// generation changes none of it.
//
// Mutant (keeper.go, handleSet): "if e.EndedForGood { continue }" dropped,
// so the marked entry is wanted as a new identity: the first Allocation's
// relays are replaced, and the second's started.
func TestAnAllocationEndedForGoodGetsNoRelay(t *testing.T) {
	w := newWorld(t)
	j1 := w.submit("hb-kp-efga-0123456789ab", 30*time.Second)
	j2 := w.submit("hb-kp-efgb-0123456789ab", 5*time.Second)
	k := w.startKeeper("")
	lead := w.startDouble("leader", k.sock)
	mustReport(t, lead.lead(w.set(provA, 1, true, w.entry(j1, gen1))), "the leader's set")
	up := k.waitUp(provA, j1)
	s := w.openStream(j1, serving)
	s.mustEcho(64<<10, "before the mark")
	before := j1.status(t)
	mustReport(t, lead.lead(w.set(provA, 1, true, endedForGood(w.entry(j1, gen1)), endedForGood(w.entry(j2, gen1)))), "the set with both marked")
	for range 3 {
		s.mustEcho(32<<10, "after the mark")
	}
	for _, ch := range agentapi.RelayChannels {
		if r, ok := k.upOf(provA, j1.tag, ch); !ok || r.PID != up[ch].PID || r.Starts != 1 {
			t.Errorf("the marked Allocation's %s relay %+v, want it kept as it was, PID %d", ch, r, up[ch].PID)
		}
	}
	if after := j1.status(t); after[serving] != before[serving] {
		t.Errorf("the marked job's serving channel %+v, want the session it kept, %+v", after[serving], before[serving])
	}
	m := k.log.mark()
	runs := w.srunsOn(j1.id, control)
	k.pins(map[agentapi.RelayChannel]wire.RelayState{control: up[control]})[control].kill(t, "the marked Allocation's control relay's ssh")
	k.log.waitLine(t, m, "the marked Allocation's control restart refused", append(relayLog(logNotStarted, j1.tag, control), whyNotListed)...)
	s.mustEcho(32<<10, "after the control relay's end")
	w.noSession(j2, serving, "for an Allocation marked endedForGood in its first set")
	if n := len(w.sruns(j2.id)); n != 0 {
		t.Errorf("%d srun lines for the Allocation marked in its first set, want none", n)
	}
	if rs := k.relaysOf(provA, j2.tag); len(rs) != 0 {
		t.Errorf("the Allocation marked in its first set has relays: %+v", rs)
	}
	m2 := k.log.mark()
	mustReport(t, lead.lead(w.set(provA, 1, true, endedForGood(w.entry(j1, gen2)), endedForGood(w.entry(j2, gen2)))), "the marked set of a new generation")
	k.log.waitLine(t, m2, "the marked Allocation's control restart refused on the new generation", append(relayLog(logNotStarted, j1.tag, control), whyNotListed)...)
	s.mustEcho(32<<10, "on the new generation")
	if r, ok := k.upOf(provA, j1.tag, serving); !ok || r.PID != up[serving].PID {
		t.Errorf("the marked Allocation's serving relay %+v on the new generation, want it kept, PID %d", r, up[serving].PID)
	}
	if n := w.srunsOn(j1.id, control); n != runs {
		t.Errorf("%d srun lines for the marked Allocation's control channel, want none", n-runs)
	}
	if n := len(w.sruns(j2.id)); n != 0 {
		t.Errorf("%d srun lines for the Allocation marked in its first set on the new generation, want none", n)
	}
	s.whole()
}
