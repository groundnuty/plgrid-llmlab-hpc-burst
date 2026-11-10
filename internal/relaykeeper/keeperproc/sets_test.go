package keeperproc

// HBEP-6's keeper items for the sets' completeness and expiry (Test plan, The
// relay keeper): "a new leader's incomplete set stops no relay"; "an expired
// set starts and restarts nothing. That holds after a keeper restart, during
// a leader outage, and for an Allocation deleted during that outage". "Not
// restarted" is never read from an absence alone: each is the keeper's log of
// a start it refused (hostdial's "a relay did not start", naming why), and
// the fake's own count of srun lines, kept in the test process, unchanged.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A new leader's incomplete set stops no relay: two Allocations' relays run
// under the old leader's complete set, with a serving stream on the second.
// The old leader stops sending, and the new one's first set, incomplete,
// names only the first. The second's relays, pinned and verified as the
// keeper's children, run on, and so does the stream, whole. The positive
// control: the new leader's complete set that leaves the second out stops
// its relays (their pins show the exit) and cuts its stream, and the first's
// relays run on.
//
// Mutant (keeper.go, handleSet): "if s.Complete {" made "if true {", so the
// incomplete set drops the second Allocation's relays.
func TestANewLeadersIncompleteSetStopsNoRelay(t *testing.T) {
	w := newWorld(t)
	j1 := w.submit("hb-kp-inca-0123456789ab", 30*time.Second)
	j2 := w.submit("hb-kp-incb-0123456789ab", 30*time.Second)
	k := w.startKeeper("")
	old := w.startDouble("old-leader", k.sock)
	mustReport(t, old.lead(w.set(provA, 1, true, w.entry(j1, gen1), w.entry(j2, gen1))), "the old leader's set")
	up1 := k.waitUp(provA, j1)
	up2 := k.waitUp(provA, j2)
	pins2 := k.pins(up2)
	s := w.openStream(j2, serving)
	s.mustEcho(64<<10, "before the hand-over")
	old.pause()
	nu := w.startDouble("new-leader", k.sock)
	mustReport(t, nu.lead(w.set(provA, 2, false, w.entry(j1, gen1))), "the new leader's incomplete set")
	for range 4 {
		s.mustEcho(32<<10, "after the incomplete set")
	}
	for ch, p := range pins2 {
		if p.exited() {
			t.Errorf("the second Allocation's %s relay's ssh exited at an incomplete set", ch)
		}
		if r, ok := k.upOf(provA, j2.tag, ch); !ok || r.PID != p.pid {
			t.Errorf("the second Allocation's %s relay %+v after the incomplete set, want it up as it was, PID %d", ch, r, p.pid)
		}
	}
	if st := j2.status(t); st[serving].Sessions != 1 || !st[serving].Connected {
		t.Errorf("the second job's serving channel %+v: want the one session it had", st[serving])
	}
	s.whole()
	// The positive control: a complete set that leaves the second out.
	mustReport(t, nu.lead(w.set(provA, 2, true, w.entry(j1, gen1))), "the new leader's complete set")
	for ch, p := range pins2 {
		p.waitExit(t, time.Minute, "the dropped Allocation's "+string(ch)+" relay's ssh")
	}
	if !s.cut() {
		t.Error("the dropped Allocation's serving stream still echoes")
	}
	waitFor(t, time.Minute, "the dropped Allocation's relays gone from the status", func() bool { return len(k.relaysOf(provA, j2.tag)) == 0 })
	for _, ch := range agentapi.RelayChannels {
		if r, ok := k.upOf(provA, j1.tag, ch); !ok || r.PID != up1[ch].PID {
			t.Errorf("the kept Allocation's %s relay %+v, want it up as it was, PID %d", ch, r, up1[ch].PID)
		}
	}
	w.roundTrip(j1, serving, 16<<10)
}

// After a keeper restart, with no set yet, nothing starts: the stream across
// the restart is cut (a keeper restart cuts every host-dialed stream), and
// the restarted keeper shows no relay. The leader's set that has expired on
// arrival changes nothing: no relay is wanted. Its set that is accepted but
// expires at once (aged) makes the relays wanted, and each start is refused
// for the expiry, in the keeper's log: a new serving connection gets no
// session within the job's wait, and no srun line reaches the login node.
// Then a fresh set starts both relays, at the restarted keeper's E, and a new
// stream crosses them, whole.
//
// Mutant (keeper.go, jobs.authorized): the expiry's case, "case
// !k.o.Now().Before(hs.expires):", dropped, so the aged set starts the
// relays.
func TestAfterAKeeperRestartOnlyAFreshSetStartsRelays(t *testing.T) {
	w := newWorld(t)
	j := w.submit("hb-kp-rsex-0123456789ab", 5*time.Second)
	k := w.startKeeper("")
	lead := w.startDouble("leader", k.sock)
	full := w.set(provA, 1, true, w.entry(j, gen1))
	mustReport(t, lead.lead(full), "the leader's set")
	k.waitUp(provA, j)
	s := w.openStream(j, serving)
	s.mustEcho(64<<10, "before the keeper's restart")
	lead.pause()
	k2 := k.restart()
	if !s.cut() {
		t.Error("the serving stream still echoes across the keeper's restart")
	}
	runs := len(w.sruns(j.id))
	m := k2.log.mark()
	if rs := k2.relaysOf(provA, j.tag); len(rs) != 0 {
		t.Errorf("the restarted keeper shows relays before any set: %+v", rs)
	}
	if r := mustReport(t, lead.set(expired(w.set(provA, 1, true, w.entry(j, gen1)))), "the expired set"); !r.Expired || len(r.Relays) != 0 {
		t.Errorf("the report of a set expired on arrival: expired %v, relays %+v; want expired and none", r.Expired, r.Relays)
	}
	if rs := k2.relaysOf(provA, j.tag); len(rs) != 0 {
		t.Errorf("a set expired on arrival made relays: %+v", rs)
	}
	if r := mustReport(t, lead.set(aged(w.set(provA, 1, true, w.entry(j, gen1)))), "the aged set"); r.Expired {
		t.Error("the aged set was expired on arrival: it must be accepted, and expire after")
	}
	for _, ch := range agentapi.RelayChannels {
		k2.log.waitLine(t, m, "the "+string(ch)+" relay's start refused for the expiry", append(relayLog(logNotStarted, j.tag, ch), whyExpired)...)
	}
	w.noSession(j, serving, "under an expired set after the keeper's restart")
	if n := len(w.sruns(j.id)); n != runs {
		t.Errorf("%d srun lines for the job reached the login node under an expired set, want none", n-runs)
	}
	if got := k2.log.since(m, logStarted, "tag="+j.tag); len(got) != 0 {
		t.Errorf("a relay started under an expired set: %q", got)
	}
	mustReport(t, lead.lead(w.set(provA, 1, true, w.entry(j, gen1))), "the fresh set")
	up := k2.waitUp(provA, j)
	for _, ch := range agentapi.RelayChannels {
		if up[ch].Epoch != firstE+1 {
			t.Errorf("the %s relay carries E %d, want the restarted keeper's %d", ch, up[ch].Epoch, firstE+1)
		}
	}
	s2 := w.openStream(j, serving)
	s2.mustEcho(64<<10, "under the fresh set")
	s2.whole()
}

// During a leader outage an expired set restarts nothing. Two Allocations
// run, a serving stream on each. The positive control first: under a fresh
// set, the first's control relay's ssh, ended through its verified pin, is
// restarted. Then the outage: the leader's last set expires at once (aged),
// and the leader sends no more.
//   - The first's control relay's ssh ends again: its restart is refused for
//     the expiry, and no srun line follows, while its serving stream runs on.
//   - The second Allocation is deleted during the outage. The keeper cannot
//     know: its relays and stream run on (HBEP-6's stated limit) until its job
//     ends, which ends them (their pins show the exit); their restarts are
//     refused for the expiry, and no srun line follows.
//
// Then a fresh set, complete, without the second: the first's control relay
// is restarted and carries bytes, its serving relay and stream run on, whole,
// and the second's relays are dropped, never restarted.
//
// Mutant (keeper.go, jobs.authorized): the expiry's case dropped, so both
// ends are restarted during the outage.
func TestDuringALeaderOutageAnExpiredSetRestartsNothing(t *testing.T) {
	w := newWorld(t)
	j1 := w.submit("hb-kp-outa-0123456789ab", 30*time.Second)
	j2 := w.submit("hb-kp-outb-0123456789ab", 30*time.Second)
	k := w.startKeeper("")
	lead := w.startDouble("leader", k.sock)
	mustReport(t, lead.lead(w.set(provA, 1, true, w.entry(j1, gen1), w.entry(j2, gen1))), "the leader's set")
	up1 := k.waitUp(provA, j1)
	up2 := k.waitUp(provA, j2)
	s1 := w.openStream(j1, serving)
	s1.mustEcho(64<<10, "before the outage")
	s2 := w.openStream(j2, serving)
	s2.mustEcho(64<<10, "before the outage")
	// The positive control.
	k.pins(map[agentapi.RelayChannel]wire.RelayState{control: up1[control]})[control].kill(t, "the first control relay's ssh")
	var again wire.RelayState
	waitFor(t, time.Minute, "the first control relay restarted under a fresh set", func() bool {
		r, ok := k.upOf(provA, j1.tag, control)
		again = r
		return ok && r.PID != up1[control].PID
	})
	w.roundTrip(j1, control, 16<<10)
	// The outage.
	mustReport(t, lead.set(aged(w.set(provA, 1, true, w.entry(j1, gen1), w.entry(j2, gen1)))), "the leader's last set")
	m := k.log.mark()
	runs1 := w.srunsOn(j1.id, control)
	k.pins(map[agentapi.RelayChannel]wire.RelayState{control: again})[control].kill(t, "the first control relay's ssh during the outage")
	k.log.waitLine(t, m, "the first control relay's restart refused for the expiry", append(relayLog(logNotStarted, j1.tag, control), whyExpired)...)
	s1.mustEcho(32<<10, "during the outage")
	if r, ok := k.upOf(provA, j1.tag, control); ok {
		t.Errorf("the first control relay %+v runs during the outage", r)
	}
	if n := w.srunsOn(j1.id, control); n != runs1 {
		t.Errorf("%d srun lines for the first control relay during the outage, want none", n-runs1)
	}
	// The second Allocation, deleted during the outage.
	s2.mustEcho(32<<10, "after its Allocation's deletion, which the keeper cannot know")
	pins2 := k.pins(up2)
	m2 := k.log.mark()
	runs2 := len(w.sruns(j2.id))
	w.end(j2)
	for ch, p := range pins2 {
		p.waitExit(t, time.Minute, "the second Allocation's "+string(ch)+" relay's ssh at its job's end")
	}
	for _, ch := range agentapi.RelayChannels {
		k.log.waitLine(t, m2, "the second Allocation's "+string(ch)+" restart refused for the expiry", append(relayLog(logNotStarted, j2.tag, ch), whyExpired)...)
	}
	if !s2.cut() {
		t.Error("the ended job's serving stream still echoes")
	}
	if n := len(w.sruns(j2.id)); n != runs2 {
		t.Errorf("%d srun lines for the deleted Allocation's job during the outage, want none", n-runs2)
	}
	// A fresh set, without the deleted Allocation.
	mustReport(t, lead.lead(w.set(provA, 1, true, w.entry(j1, gen1))), "the leader's fresh set")
	waitFor(t, time.Minute, "the first control relay restarted under the fresh set", func() bool {
		r, ok := k.upOf(provA, j1.tag, control)
		return ok && r.PID != up1[control].PID && r.PID != again.PID
	})
	w.roundTrip(j1, control, 16<<10)
	if r, ok := k.upOf(provA, j1.tag, serving); !ok || r.PID != up1[serving].PID {
		t.Errorf("the first serving relay %+v, want it up as it was, PID %d", r, up1[serving].PID)
	}
	s1.mustEcho(32<<10, "after the fresh set")
	s1.whole()
	waitFor(t, time.Minute, "the deleted Allocation's relays dropped", func() bool { return len(k.relaysOf(provA, j2.tag)) == 0 })
	if n := len(w.sruns(j2.id)); n != runs2 {
		t.Errorf("%d srun lines for the deleted Allocation's job after the fresh set, want none", n-runs2)
	}
}
