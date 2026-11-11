package keeperproc

// HBEP-6's keeper items for the Lease fence with REAL controller processes
// (Test plan, The relay keeper; #343's PR 4, Stage B):
//   - "a set or an acknowledgement from a lower Lease epoch is refused,
//     including a stale controller that reconnects first after a keeper
//     restart" (the stale-copy half: the copy is a real controller, paused
//     while another takes its Lease, then resumed);
//   - Fencing's beta half: "a copy that loses the lead stops sending sets and
//     acknowledgements before it releases the Lease".
// The refusals are read off the wire by the tap, by the sender's PID, and from
// the keeper's own status counters; the Lease from the API server.

import (
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A stale real controller: the sets it sends after it resumes, and its
// acknowledgement held in flight across its pause, are refused for their
// Lease epoch, one of those new sets when it is the first request to reach a
// restarted keeper, and none of them changes a relay.
//
// How A becomes stale: its Lease is taken from under it while it is paused.
// A is stopped by SIGSTOP through its pidfd, the test writes a release
// record on its Lease (as client-go's elector writes one: no holder,
// leaseTransitions kept), and B, standing by, takes the Lease at once, one
// epoch up, and leads. A is then resumed by SIGCONT through its pidfd. Its
// renewals fail, but its renew deadline (30 min in this test) is far off, so it
// goes on leading, and sending, at its old epoch. A Lease
// that expired instead would not do: client-go's renew deadline is below its
// lease duration, so a copy paused past its successor's take-over finds its
// renew deadline passed at once on resume, and whether it sends before its
// elector notices is a race.
//
// The Provider has two running Allocations. The first's control relay gets a
// final end (the fake's step limit): A writes its mark, and the tap holds
// A's acknowledgement, in flight when A is paused. The second's relays run,
// pinned, with a serving stream and bytes across the control relay.
//   - After B's take-over and its own acknowledgement of the end, A's held
//     acknowledgement reaches the keeper: refused, epoch, naming B's epoch.
//   - A resumed: the sets A sends after the resume (the second built after
//     it too), seen at the tap, are refused, epoch; the keeper counts them; the pinned relays are the same processes
//     with the same E and N, and bytes cross both channels, whole.
//   - The keeper is restarted on its state directory (which cuts the stream)
//     while the tap drops B's requests: the first request to reach it is a
//     new set of A's, refused, epoch, the fence kept in the state file; no
//     relay starts and no srun line follows.
//   - B's next set is accepted by the restarted keeper and starts the second
//     Allocation's relays at a higher E: bytes cross both channels.
//
// Nothing A sends after B's take-over is accepted by either keeper.
func TestAStaleRealControllersAcknowledgementAndSetAreRefusedForTheirEpoch(t *testing.T) {
	g := newRig(t)
	// A's renew deadline, 30 min, is above the sum of the bounds of every
	// wait between A's pause and the last request the test needs from it
	// (about 15 min), so A, correct, cannot stop leading before it; the Lease
	// duration must exceed it. A set's validity takes a renew deadline of
	// its own, 60 s, under the keeper's 2 min bound.
	tm := stageBTimings
	tm.LeaseDurationMS, tm.RenewDeadlineMS, tm.SetRenewMS = 31*60_000, 30*60_000, 60_000
	g.timings = &tm
	p := g.provider("example-a")
	a1, j1 := g.running(p, "kp-stla")
	a2, j2 := g.running(p, "kp-stlb")
	own := g.w.allSbatches()
	ca := g.startController("a", p.name, g.tap.path)
	apid := ca.pid()
	g.tap.setRule(func(r *tapReq) tapAction {
		if r.pid == apid && r.kind == wire.KindAck {
			return tapHold
		}
		return tapPass
	})
	_, up1 := g.leads(ca, p, j1)
	_, up2 := g.leads(ca, p, j2)
	g.adopted(a1, j1)
	g.adopted(a2, j2)
	before := g.firstLease(p.name)
	pins1 := g.k.pins(up1)
	pins2 := g.k.pins(up2)
	s := g.w.openStream(j2, serving)
	s.mustEcho(64<<10, "before the final end")
	g.w.roundTrip(j2, control, 16<<10)
	// The first Allocation's relays carry bytes too, before its control
	// relay's final end, and its serving relay after it.
	g.w.roundTrip(j1, control, 16<<10)
	g.w.roundTrip(j1, serving, 16<<10)
	cb := g.startController("b", p.name, g.tap.path)
	bpid := cb.pid()
	cb.waitLog(logAcquiring)
	waitFor(t, time.Minute, "both relays' srun steps of the first job logged", func() bool { return len(g.w.sruns(j1.id)) == 2 })
	if err := g.w.plat.SetStepLimit(atoi(t, j1.id), 2); err != nil {
		t.Fatal(err)
	}
	pins1[control].kill(t, "the first Allocation's control relay's ssh")
	heldAck := g.tap.waitReq(t, time.Minute, "A's acknowledgement held", func(r *tapReq) bool { return r.pid == apid && r.ack != nil && r.action == tapHold })
	if err := ca.c.signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("STOP A through its pidfd: %v", err)
	}
	waitFor(t, 30*time.Second, "A stopped", func() bool { return procState(apid) == "T" })
	t.Cleanup(func() {
		// A paused does not stop at its input's end: it is KILLed through
		// its pidfd, unless it is gone.
		if !ca.c.wait(0) {
			_ = ca.c.signal(syscall.SIGKILL)
		}
	})

	// The Lease is taken from under A, and B takes it, while the stream
	// runs.
	released := time.Now()
	g.api.releaseUnder(t, p.name)
	after := g.api.waitLease(t, p.name, "B holds the Lease", time.Minute, func(l leaseRecord) bool { return l.holder != "" && l.holder != before.holder })
	want := before.transitions + 1
	if after.transitions != want {
		t.Errorf("the Lease's leaseTransitions after B took it is %d, want %d (it was %d under A)", after.transitions, want, before.transitions)
	}
	end := time.Now().Add(2 * time.Minute)
	for len(g.tap.requests(reportedSetFrom(bpid))) == 0 {
		if time.Now().After(end) {
			t.Fatal("no set of B's was accepted within 2 min of A's pause")
		}
		s.mustEcho(8<<10, "while B takes over")
		time.Sleep(200 * time.Millisecond)
	}
	g.tap.waitReq(t, time.Minute, "B's acknowledgement of the end answered", func(r *tapReq) bool {
		return r.pid == bpid && r.ack != nil && r.ack.UID == j1.uid && r.acked != nil
	})
	s.mustEcho(32<<10, "after B's acknowledgement")

	// A's acknowledgement, in flight across its pause, reaches the keeper.
	ra := g.tap.let(t, heldAck, tapPass)
	if ra.refused == nil || ra.refused.Reason != wire.RefusedEpoch || ra.refused.HighestEpoch != want {
		t.Errorf("A's acknowledgement was answered %s %+v %+v, want refused, %s, naming epoch %d", ra.replyKind, ra.acked, ra.refused, wire.RefusedEpoch, want)
	}
	if ps, _ := g.k.provider(p); ps.Refused[wire.RefusedEpoch] != 1 {
		t.Errorf("the keeper counts %d refusals for the epoch, want 1", ps.Refused[wire.RefusedEpoch])
	}

	// A resumes, stale, and sends a new set.
	resumed := time.Now()
	if err := ca.c.signal(syscall.SIGCONT); err != nil {
		t.Fatalf("CONT A through its pidfd: %v", err)
	}
	fresh := g.tap.snapshot(g.tap.waitReq(t, time.Minute, "a set A sent after its resume, answered", func(r *tapReq) bool {
		return r.pid == apid && r.set != nil && r.at.After(resumed) && r.replyKind != ""
	}))
	if fresh.refused == nil || fresh.refused.Reason != wire.RefusedEpoch || fresh.refused.HighestEpoch != want || fresh.set.LeaseEpoch != before.transitions {
		t.Errorf("A's set after its resume, at Lease epoch %d, was answered %s %+v, want refused, %s, naming epoch %d", fresh.set.LeaseEpoch, fresh.replyKind, fresh.refused, wire.RefusedEpoch, want)
	}
	// And one A built after its resume too, not only sent after it: its
	// sender sends one set at a time, a send interval apart.
	again := g.tap.snapshot(g.tap.waitReq(t, time.Minute, "a second set A sent after its resume, answered", func(r *tapReq) bool {
		return r.pid == apid && r.set != nil && r.n > fresh.n && r.replyKind != ""
	}))
	if again.refused == nil || again.refused.Reason != wire.RefusedEpoch || again.refused.HighestEpoch != want {
		t.Errorf("A's second set after its resume was answered %s %+v, want refused, %s, naming epoch %d", again.replyKind, again.refused, wire.RefusedEpoch, want)
	}
	t.Logf("A resumed at %s; its first answered sets after it: %s; %s", resumed.Format(time.RFC3339Nano), fresh.describe(), again.describe())
	if ps, _ := g.k.provider(p); ps.Refused[wire.RefusedEpoch] < 3 {
		t.Errorf("the keeper counts %d refusals for the epoch, want 3 or more", ps.Refused[wire.RefusedEpoch])
	}
	if ca.c.wait(0) {
		t.Fatal("A is gone after its resume: it is not a stale copy that still runs")
	}
	s.mustEcho(32<<10, "after A's stale set")
	g.w.roundTrip(j2, control, 16<<10)
	for _, ch := range agentapi.RelayChannels {
		if pins2[ch].exited() {
			t.Errorf("the second Allocation's %s relay's ssh, held through its pidfd, exited", ch)
		}
		r, ok := g.k.upOf(p, j2.tag, ch)
		if !ok || r.PID != up2[ch].PID || r.Epoch != up2[ch].Epoch || r.Seq != up2[ch].Seq || r.Starts != 1 {
			t.Errorf("the second Allocation's %s relay %+v after A's stale requests, want it as it was: PID %d, (%d, %d), 1 start",
				ch, r, up2[ch].PID, up2[ch].Epoch, up2[ch].Seq)
		}
	}
	if pins1[serving].exited() {
		t.Error("the first Allocation's serving relay's ssh, held through its pidfd, exited")
	}
	g.w.roundTrip(j1, serving, 16<<10)
	s.mustEcho(32<<10, "before the keeper's restart")
	s.whole()

	// The keeper restarts while B's requests are dropped: the first request
	// to reach it is a new set of A's.
	g.tap.setRule(func(r *tapReq) tapAction {
		if r.pid == bpid {
			return tapDrop
		}
		return tapPass
	})
	sock, dir := g.k.sock, g.k.dir
	g.k.stop()
	stopped := time.Now()
	g.k = g.w.startKeeper(dir)
	if g.k.sock != sock {
		t.Fatalf("the restarted keeper serves %s, not the tap's upstream %s", g.k.sock, sock)
	}
	if g.k.epoch != firstE+1 {
		t.Errorf("the restarted keeper took E %d, want %d", g.k.epoch, firstE+1)
	}
	runs1, runs2 := len(g.w.sruns(j1.id)), len(g.w.sruns(j2.id))
	reached := func(r *tapReq) bool { return r.forwarded.After(stopped) && r.replyKind != "" }
	first := g.tap.snapshot(g.tap.waitReq(t, time.Minute, "a request answered by the restarted keeper", reached))
	if first.pid != apid || first.set == nil || first.refused == nil || first.refused.Reason != wire.RefusedEpoch || first.refused.HighestEpoch != want {
		t.Errorf("the first request the restarted keeper answered is %s, %+v; want a set of A's refused, %s, naming epoch %d",
			first.describe(), first.refused, wire.RefusedEpoch, want)
	}
	ps, _ := g.k.provider(p)
	if ps.Refused[wire.RefusedEpoch] < 1 || len(ps.Relays) != 0 {
		t.Errorf("the restarted keeper's status: %d refusals for the epoch and relays %+v, want some and none", ps.Refused[wire.RefusedEpoch], ps.Relays)
	}
	if n1, n2 := len(g.w.sruns(j1.id)), len(g.w.sruns(j2.id)); n1 != runs1 || n2 != runs2 {
		t.Errorf("%d and %d srun lines after A's stale set reached the restarted keeper, want none", n1-runs1, n2-runs2)
	}
	if got := g.k.log.since(0, logStarted); len(got) != 0 {
		t.Errorf("a relay started on A's stale set: %q", got)
	}
	// The stream was cut by the restart (checked here, after the last
	// request the test needs from A: cut's own bound is long).
	if !s.cut() {
		t.Error("the serving stream still echoes across the keeper's restart")
	}

	// B's next set is accepted, and starts the second Allocation's relays.
	g.tap.setRule(nil)
	g.tap.waitReq(t, time.Minute, "B's set accepted by the restarted keeper", func(r *tapReq) bool {
		return r.pid == bpid && r.set != nil && r.report != nil && r.forwarded.After(first.forwarded)
	})
	up := g.k.waitUp(p, j2)
	for ch, r := range up {
		if r.Epoch != firstE+1 {
			t.Errorf("the second Allocation's %s relay under the restarted keeper is at E %d, want %d", ch, r.Epoch, firstE+1)
		}
	}
	g.w.roundTrip(j2, serving, 16<<10)
	g.w.roundTrip(j2, control, 16<<10)
	for _, r := range g.tap.requests(func(r *tapReq) bool { return r.pid == apid && r.at.After(released) && r.replyKind != "" }) {
		if r.refused == nil || r.refused.Reason != wire.RefusedEpoch {
			t.Errorf("A's %s after B's take-over was answered %s %+v, want refused for its epoch", r.describe(), r.replyKind, r.refused)
		}
	}
	g.epochsOf("A", apid, before.transitions)
	g.epochsOf("B", bpid, want)
	g.ownSbatches(own)
	ca.kill()
}

// A leader that loses the lead stops sending before it releases the Lease,
// with a set in flight.
func TestARealLeaderStopsSendingBeforeItReleasesTheLease(t *testing.T) {
	leaderStopsBeforeRelease(t, false)
}

// A leader that loses the lead stops sending before it releases the Lease,
// with an acknowledgement in flight: the fake's step limit gives the control
// relay a final end, A writes its mark, and the tap holds the acknowledgement
// A then sends; its sender waits behind it.
func TestARealLeaderWithAnAcknowledgementInFlightStopsSendingBeforeItReleasesTheLease(t *testing.T) {
	leaderStopsBeforeRelease(t, true)
}

// leaderStopsBeforeRelease: the tap holds A's next set, or its first
// acknowledgement (ack), in flight, and A is then stopped by SIGTERM through
// its pidfd. While the request is held, A's manager waits for its
// leader-election runnables (its log shows the wait begun, its Allocation
// workers finished, and not the next step), and A's Lease, read from the API
// server, is still A's. The tap then lets the request through, and the
// keeper's answer reaches A. The order: the answer was written back before
// the release record's own time (its renewTime), A exited cleanly, and no
// request from A's process (by SO_PEERCRED) came after the held one, so none
// after the release. The relays and the stream run on, whole.
//
// The hold is bounded by the controller's exchange time-out (5 s): a hold
// that outlasts it releases the Lease all the same, so the test checks the
// order, not that the Lease outlasts any hold.
func leaderStopsBeforeRelease(t *testing.T, ack bool) {
	g := newRig(t)
	p := g.provider("example-a")
	al, j := g.running(p, "kp-rels")
	own := g.w.allSbatches()
	a := g.startController("a", p.name, g.tap.path)
	_, up := g.leads(a, p, j)
	g.adopted(al, j)
	pins := g.k.pins(up)
	s := g.w.openStream(j, serving)
	s.mustEcho(64<<10, "before the stop")
	g.w.roundTrip(j, control, 16<<10)
	idA := g.firstLease(p.name).holder
	var armed atomic.Bool
	armed.Store(true)
	apid := a.pid()
	g.tap.setRule(func(r *tapReq) tapAction {
		if r.pid == apid && ((ack && r.ack != nil) || (!ack && r.set != nil)) && armed.CompareAndSwap(true, false) {
			return tapHold
		}
		return tapPass
	})
	if ack {
		waitFor(t, time.Minute, "both relays' srun steps logged", func() bool { return len(g.w.sruns(j.id)) == 2 })
		if err := g.w.plat.SetStepLimit(atoi(t, j.id), 2); err != nil {
			t.Fatal(err)
		}
		pins[control].kill(t, "the control relay's ssh")
	}
	held := g.tap.waitReq(t, time.Minute, "A's request held in flight", func(r *tapReq) bool {
		return r.pid == apid && r.action == tapHold
	})
	if err := a.c.signal(syscall.SIGTERM); err != nil {
		t.Fatalf("TERM A through its pidfd: %v", err)
	}
	a.waitLog(logStopLeaderRun)
	a.waitLog(logWorkersDone)
	if l := g.api.lease(t, p.name); l.holder != idA {
		t.Errorf("A's Lease %+v is not A's while A's request is in flight", l)
	}
	if a.logged(logStopCaches) {
		t.Error("A's manager went past its leader-election runnables while its request was in flight")
	}
	s.mustEcho(32<<10, "while A's request is held")
	if l := g.api.lease(t, p.name); l.holder != idA {
		t.Errorf("A's Lease %+v is not A's while A's request is in flight", l)
	}
	h := g.tap.let(t, held, tapPass)
	answered := h.report != nil
	if ack {
		answered = h.ack != nil && h.acked != nil && h.acked.Decided
	}
	if h.answered.IsZero() || !answered {
		t.Fatalf("A's held %s was not answered as it should be (%s %+v %+v; %s): A gave up on it, the hold outlasted its exchange's time-out",
			h.kind, h.replyKind, h.acked, h.refused, h.err)
	}
	a.waitClean()
	rel := g.api.waitLease(t, p.name, "A's release of the Lease", time.Minute, func(l leaseRecord) bool { return l.found && l.holder == "" })
	t.Logf("A's %s in flight: held at %s, its answer written back from %s; the release record's time %s",
		h.kind, h.at.Format(time.RFC3339Nano), h.replying.Format(time.RFC3339Nano), rel.renew.Format(time.RFC3339Nano))
	if !h.replying.Before(rel.renew) {
		t.Errorf("the keeper's answer to A's %s in flight was written back to A from %s, not before the release record's time %s",
			h.kind, h.replying.Format(time.RFC3339Nano), rel.renew.Format(time.RFC3339Nano))
	}
	for _, r := range g.tap.requests(from(apid)) {
		if r.at.After(h.at) {
			t.Errorf("A sent %s after its %s in flight (the release was at %s)", r.describe(), h.kind, rel.renew.Format(time.RFC3339Nano))
		}
	}
	for range 3 {
		s.mustEcho(32<<10, "after A's release")
	}
	if !ack {
		// The control relay carries bytes after the stop too (in the
		// acknowledgement's case it has ended for good).
		g.w.roundTrip(j, control, 16<<10)
	}
	for _, ch := range agentapi.RelayChannels {
		if (!ack || ch == serving) && pins[ch].exited() {
			t.Errorf("the %s relay's ssh, held through its pidfd, exited", ch)
		}
	}
	g.ownSbatches(own)
	s.whole()
}
