package keeperproc

// HBEP-6's keeper items for final ends with REAL controller processes (Test
// plan, The relay keeper; #343's PR 4, Stage B):
//   - "a final end survives a controller restart, and the next leader writes
//     RelayEndedForGood and acknowledges it";
//   - "a final end whose Allocation was deleted before the replay is
//     acknowledged as refused, and no longer reported";
//   - "a crash after the mark but before the acknowledgement raises
//     RelayFailed once".
//
// The final end is the fake's step-limit refusal of the control relay's
// restart (finality_test.go's), while a serving stream runs on. What the
// tests read: the keeper's reports and its answers to acknowledgements, off
// the wire by the tap, by sender; the mark on the Allocation and the
// RelayFailed Events regarding it, from the API server. The expected values
// are HBEP-6's (the mark's reason RelayFailed, the acknowledgement's outcome
// and reason) and the test's own (the step-limited channel, the job, the
// Allocation's UID, the Lease epochs as written).

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// finalRun is a final-end test's start: one running Allocation adopted by A,
// which leads, B standing by, both relays up and pinned, and a serving stream
// across them.
type finalRun struct {
	t      *testing.T
	g      *rig
	p      prov
	a      *v1alpha1.Allocation
	j      job
	ca, cb *realCtl
	up     map[agentapi.RelayChannel]wire.RelayState
	pins   map[agentapi.RelayChannel]*pinned
	s      *stream
	own    int
	before leaseRecord
}

func startFinal(t *testing.T, name string, rule func(a *realCtl) func(r *tapReq) tapAction) *finalRun {
	t.Helper()
	f := &finalRun{t: t, g: newRig(t)}
	g := f.g
	// The final end comes from a relay restart after A is dead, which the
	// keeper makes only under A's last unexpired set. With the sets' validity
	// lengthened to the 2 s poll plus 60 s, that set outlives A by about 55 s
	// (its age carries the 5 s exchange bound), not about 8 s, so the refused
	// restart cannot miss it under load; the Lease's own timings are
	// stageBTimings'.
	tm := stageBTimings
	tm.SetRenewMS = 60_000
	g.timings = &tm
	f.p = g.provider("example-a")
	f.a, f.j = g.running(f.p, name)
	f.own = g.w.allSbatches()
	f.ca = g.startController("a", f.p.name, g.tap.path)
	if rule != nil {
		g.tap.setRule(rule(f.ca))
	}
	_, f.up = g.leads(f.ca, f.p, f.j)
	g.adopted(f.a, f.j)
	f.before = g.firstLease(f.p.name)
	f.pins = g.k.pins(f.up)
	f.s = g.w.openStream(f.j, serving)
	f.s.mustEcho(64<<10, "before the final end")
	f.cb = g.startController("b", f.p.name, g.tap.path)
	f.cb.waitLog(logAcquiring)
	waitFor(t, time.Minute, "both relays' srun steps logged", func() bool { return len(g.w.sruns(f.j.id)) == 2 })
	if err := g.w.plat.SetStepLimit(atoi(t, f.j.id), 2); err != nil {
		t.Fatal(err)
	}
	return f
}

// endControl ends the control relay's ssh through its verified pin: its
// restart is refused at the step limit, a final end.
func (f *finalRun) endControl() {
	f.t.Helper()
	f.pins[control].kill(f.t, "the control relay's ssh")
}

// waitFinal waits until the keeper's status shows the control relay final.
func (f *finalRun) waitFinal() {
	f.t.Helper()
	waitFor(f.t, time.Minute, "the control relay final in the keeper's status", func() bool {
		for _, r := range f.g.k.relaysOf(f.p, f.j.tag) {
			if r.Channel == control && r.State == wire.StateFinal {
				return true
			}
		}
		return false
	})
}

// endWhileLeaderless kills A through its pidfd and then produces the final
// end, which the keeper records while no controller leads: A is gone, and B
// has sent nothing and does not hold the Lease.
func (f *finalRun) endWhileLeaderless() {
	f.t.Helper()
	f.ca.kill()
	f.endControl()
	f.waitFinal()
	if n := len(f.g.tap.requests(from(f.cb.pid()))); n != 0 {
		f.t.Fatalf("B sent %d requests before the final end was recorded: it led", n)
	}
	if l := f.g.api.lease(f.t, f.p.name); l.holder != f.before.holder {
		f.t.Fatalf("the Lease %+v changed hands before the final end was recorded, was %+v", l, f.before)
	}
	f.s.mustEcho(32<<10, "after the final end, with no leader")
}

// endIn is the control channel's final end of uid in a report, and whether
// the report has one.
func endIn(r *wire.Report, uid string) (wire.FinalEnd, bool) {
	if r == nil {
		return wire.FinalEnd{}, false
	}
	for _, e := range r.Ends {
		if e.UID == uid && e.Channel == control {
			return e, true
		}
	}
	return wire.FinalEnd{}, false
}

// replay waits for B's first set answered with the control relay's final end,
// checks the end, and returns it.
func (f *finalRun) replay() wire.FinalEnd {
	f.t.Helper()
	b := f.cb.pid()
	end := time.Now().Add(2 * time.Minute)
	var r *tapReq
	for r == nil {
		got := f.g.tap.requests(func(r *tapReq) bool { _, ok := endIn(r.report, f.j.uid); return r.pid == b && ok })
		if len(got) > 0 {
			r = &got[0]
			break
		}
		if time.Now().After(end) {
			f.t.Fatal("no set of B's was answered with the control relay's final end within 2 min")
		}
		f.s.mustEcho(8<<10, "while B takes over")
		time.Sleep(200 * time.Millisecond)
	}
	e, _ := endIn(r.report, f.j.uid)
	if e.Kind != wire.EndFailed || e.JobID != f.j.id || e.Tag != f.j.tag || !strings.Contains(e.Cause, "step limit") || e.Epoch != firstE {
		f.t.Errorf("the final end reported to B %+v: want the control channel's failed end at the step limit, job %s, at E %d", e, f.j.id, firstE)
	}
	return e
}

// ackOf waits for an acknowledgement of e from pid that the keeper answered,
// and returns it.
func (f *finalRun) ackOf(pid int, e wire.FinalEnd, what string) tapReq {
	f.t.Helper()
	r := f.g.tap.waitReq(f.t, time.Minute, what, func(r *tapReq) bool {
		return r.pid == pid && r.ack != nil && r.ack.UID == e.UID && r.ack.JobID == e.JobID && r.ack.Channel == e.Channel &&
			r.ack.Epoch == e.Epoch && r.ack.Seq == e.Seq && r.replyKind != ""
	})
	return f.g.tap.snapshot(r)
}

// reportedNoMore waits for three of B's sets after the acknowledgement ack,
// each answered with a report, and checks that none of B's reports after it
// names the end: the keeper reports it no more. With echo, the stream carries
// bytes meanwhile.
func (f *finalRun) reportedNoMore(ack tapReq, echo bool) {
	f.t.Helper()
	b := f.cb.pid()
	after := func(r *tapReq) bool { return r.pid == b && r.n > ack.n && r.set != nil && r.report != nil }
	waitFor(f.t, time.Minute, "three of B's sets answered after the acknowledgement", func() bool {
		if echo {
			f.s.mustEcho(4<<10, "after the acknowledgement")
		}
		return len(f.g.tap.requests(after)) >= 3
	})
	for _, r := range f.g.tap.requests(after) {
		if e, ok := endIn(r.report, f.j.uid); ok {
			f.t.Errorf("the keeper still reports the decided end %+v, in its answer to B's %s", e, r.describe())
		}
	}
}

// newEpoch is B's Lease epoch: the Lease as written once B holds it, checked
// against client-go's rule, one up from A's on the holder change.
func (f *finalRun) newEpoch() int64 {
	f.t.Helper()
	l := f.g.api.waitLease(f.t, f.p.name, "B holds the Lease", time.Minute, func(l leaseRecord) bool { return l.holder != "" && l.holder != f.before.holder })
	want := f.before.transitions + 1
	if l.transitions != want {
		f.t.Errorf("the Lease's leaseTransitions after B took it is %d, want %d (it was %d under A)", l.transitions, want, f.before.transitions)
	}
	return want
}

// A final end survives a controller restart: the keeper records it while no
// controller leads (A KILLed, B not leading yet), and replays it to B, the
// next leader, which writes RelayEndedForGood on the Allocation, raises
// RelayFailed once, and acknowledges the end as marked; the keeper then
// reports it no more. The serving relay and its stream run on, whole.
func TestAFinalEndSurvivesARealControllerRestart(t *testing.T) {
	f := startFinal(t, "kp-fins", nil)
	f.endWhileLeaderless()
	e := f.replay()
	epoch := f.newEpoch()
	ack := f.ackOf(f.cb.pid(), e, "B's acknowledgement of the replayed end")
	if ack.ack.Outcome != wire.OutcomeMarked || ack.ack.Reason != "" || ack.ack.LeaseEpoch != epoch || ack.acked == nil || !ack.acked.Decided {
		t.Errorf("B's acknowledgement %+v was answered %+v %+v, want marked at Lease epoch %d and decided", *ack.ack, ack.acked, ack.refused, epoch)
	}
	al, ok := f.g.api.allocation(t, allocNS, f.a.Name)
	if !ok {
		t.Fatal("the Allocation is gone")
	}
	st, reason, msg := mark(al)
	if st != "True" || reason != string(v1alpha1.ReasonRelayFailed) || !strings.Contains(msg, "control relay") || !strings.Contains(msg, "step limit") {
		t.Errorf("the Allocation's RelayEndedForGood is %s, %s, %q; want True, %s, naming the control relay and the step limit", st, reason, msg, v1alpha1.ReasonRelayFailed)
	}
	waitFor(t, time.Minute, "a RelayFailed Event regarding the Allocation", func() bool { return f.g.api.relayFailedEvents(t, allocNS, string(f.a.UID)) >= 1 })
	f.reportedNoMore(ack, true)
	if n := f.g.api.relayFailedEvents(t, allocNS, string(f.a.UID)); n != 1 {
		t.Errorf("%d RelayFailed Events regarding the Allocation, want 1", n)
	}
	f.g.epochsOf("B", f.cb.pid(), epoch)
	if f.pins[serving].exited() {
		t.Error("the serving relay's ssh, held through its pidfd, exited")
	}
	for range 3 {
		f.s.mustEcho(32<<10, "after the mark")
	}
	f.g.ownSbatches(f.own)
	f.s.whole()
}

// A final end whose Allocation was deleted before the replay: the end is
// recorded while no controller leads, and the Allocation is then deleted (its
// finalizer keeps it, being deleted); meanwhile the keeper keeps its relays
// and the stream runs on (HBEP-6, No leader). B, the next leader, acknowledges
// the end as refused, deleted, writes no mark and raises no RelayFailed, and
// the keeper reports the end no more. B's first reconcile of the deleting
// Allocation cancels its job, which then cuts the stream.
func TestAFinalEndOfADeletedAllocationIsRefusedByTheRealNextLeader(t *testing.T) {
	f := startFinal(t, "kp-find", nil)
	f.endWhileLeaderless()
	if err := f.g.api.c.Delete(context.Background(), f.a); err != nil {
		t.Fatal(err)
	}
	al, ok := f.g.api.allocation(t, allocNS, f.a.Name)
	if !ok || al.DeletionTimestamp == nil {
		t.Fatalf("the Allocation after its deletion: %v, want it there, being deleted, kept by its finalizer", ok)
	}
	for range 3 {
		f.s.mustEcho(32<<10, "after the Allocation's deletion, with no leader")
	}
	e := f.replay()
	epoch := f.newEpoch()
	ack := f.ackOf(f.cb.pid(), e, "B's acknowledgement of the deleted Allocation's end")
	if ack.ack.Outcome != wire.OutcomeRefused || ack.ack.Reason != wire.AckDeleted || ack.ack.LeaseEpoch != epoch || ack.acked == nil || !ack.acked.Decided {
		t.Errorf("B's acknowledgement %+v was answered %+v %+v, want refused, %s, at Lease epoch %d, and decided", *ack.ack, ack.acked, ack.refused, wire.AckDeleted, epoch)
	}
	f.reportedNoMore(ack, false)
	if al, ok := f.g.api.allocation(t, allocNS, f.a.Name); ok {
		if st, _, msg := mark(al); st == "True" {
			t.Errorf("the deleted Allocation carries RelayEndedForGood: %q", msg)
		}
	}
	if n := f.g.api.relayFailedEvents(t, allocNS, string(f.a.UID)); n != 0 {
		t.Errorf("%d RelayFailed Events regarding the deleted Allocation, want none", n)
	}
	f.g.epochsOf("B", f.cb.pid(), epoch)
	// Then whatever the stream shows: B cancels the deleting Allocation's
	// job, which ends the relays.
	waitFor(t, 2*time.Minute, "the deleted Allocation's job ended", func() bool {
		jb, ok := f.g.w.plat.Job(atoi(t, f.j.id))
		return ok && jb.State != "RUNNING"
	})
	if !f.s.cut() {
		t.Error("the serving stream still echoes after the deleted Allocation's job ended")
	}
	f.g.ownSbatches(f.own)
}

// A crash after the mark but before the acknowledgement: the tap holds every
// acknowledgement from A, the leader, so A writes the mark and raises
// RelayFailed (keeper.go's ackEnd: mark, event, acknowledgement), and its
// acknowledgement never reaches the keeper; A is then KILLed through its
// pidfd. B, the next leader, gets the end replayed, finds the mark there,
// acknowledges it as marked and raises no second event: RelayFailed is
// counted once in the API server, and the keeper reports the end no more.
//
// The KILL waits for A's event to be in the API server: the recorder sends
// events from a queue of its own, so a crash before the queue is sent loses
// the event, which no acknowledgement order can prevent.
func TestACrashAfterTheMarkRaisesRelayFailedOnceWithRealControllers(t *testing.T) {
	f := startFinal(t, "kp-finc", func(a *realCtl) func(r *tapReq) tapAction {
		pid := a.pid()
		return func(r *tapReq) tapAction {
			if r.pid == pid && r.kind == wire.KindAck {
				return tapHold
			}
			return tapPass
		}
	})
	f.endControl()
	held := f.g.tap.waitReq(t, time.Minute, "A's acknowledgement held by the tap", func(r *tapReq) bool {
		return r.pid == f.ca.pid() && r.ack != nil && r.action == tapHold
	})
	h := f.g.tap.snapshot(held)
	if h.ack.UID != f.j.uid || h.ack.Channel != control || h.ack.Outcome != wire.OutcomeMarked {
		t.Fatalf("A's held acknowledgement %+v: want the control channel's end of the Allocation, marked", *h.ack)
	}
	waitFor(t, time.Minute, "A's mark on the Allocation", func() bool {
		al, ok := f.g.api.allocation(t, allocNS, f.a.Name)
		if !ok {
			return false
		}
		st, _, _ := mark(al)
		return st == "True"
	})
	waitFor(t, time.Minute, "A's RelayFailed Event in the API server", func() bool { return f.g.api.relayFailedEvents(t, allocNS, string(f.a.UID)) >= 1 })
	f.ca.kill()
	f.s.mustEcho(32<<10, "after A's KILL")
	e := f.replay()
	if e.Epoch != h.ack.Epoch || e.Seq != h.ack.Seq {
		t.Errorf("the end replayed to B %+v is not the one A acknowledged, %+v", e, *h.ack)
	}
	epoch := f.newEpoch()
	ack := f.ackOf(f.cb.pid(), e, "B's acknowledgement of the replayed end")
	if ack.ack.Outcome != wire.OutcomeMarked || ack.ack.Reason != "" || ack.ack.LeaseEpoch != epoch || ack.acked == nil || !ack.acked.Decided {
		t.Errorf("B's acknowledgement %+v was answered %+v %+v, want marked at Lease epoch %d and decided", *ack.ack, ack.acked, ack.refused, epoch)
	}
	f.reportedNoMore(ack, true)
	if n := f.g.api.relayFailedEvents(t, allocNS, string(f.a.UID)); n != 1 {
		t.Errorf("%d RelayFailed Events regarding the Allocation, want 1: the crash after the mark raised it again", n)
	}
	for _, r := range f.g.tap.requests(func(r *tapReq) bool { return r.pid == f.ca.pid() && r.ack != nil }) {
		if !r.forwarded.IsZero() {
			t.Errorf("A's %s reached the keeper: the crash was not before the acknowledgement", r.describe())
		}
	}
	if f.pins[serving].exited() {
		t.Error("the serving relay's ssh, held through its pidfd, exited")
	}
	for range 3 {
		f.s.mustEcho(32<<10, "after B's acknowledgement")
	}
	f.g.ownSbatches(f.own)
	f.s.whole()
}
