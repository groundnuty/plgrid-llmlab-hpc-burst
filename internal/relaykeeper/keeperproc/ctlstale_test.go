package keeperproc

// HBEP-6's keeper items for the Lease fence with REAL controller processes
// (Test plan, The relay keeper; #343's PR 4, Stage B):
//   - "a set or an acknowledgement from a lower Lease epoch is refused,
//     including a stale controller that reconnects first after a keeper
//     restart" (the stale-copy half: the copy is a real controller, held
//     paused while another takes its Lease);
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

// A stale real controller's acknowledgement and set are refused for their
// Lease epoch, the set when it is the first request to reach a restarted
// keeper, and neither changes a relay.
//
// The Provider has two running Allocations: the first's control relay gets a
// final end (the fake's step limit), so A, the leader, writes its mark and
// acknowledges it; the second's relays run, pinned, with a serving stream.
// The tap holds A's acknowledgement, and then A's next set (which names the
// second Allocation, so a fresh keeper would start its relays), and A is
// paused with SIGSTOP through its pidfd, both in flight. B takes the Lease
// once it expires (epoch one up, read from the Lease as written), and the
// keeper accepts B's sets, so its fence is B's epoch.
//   - A's acknowledgement then reaches the keeper and is refused, epoch,
//     naming B's epoch; the keeper counts it; the pinned relays are the same
//     processes with the same E and N, and the stream runs on, whole.
//   - The keeper is restarted on its state directory (which cuts the
//     stream) while the tap drops B's requests, and A's held set is the first
//     request to reach it: refused, epoch, the fence kept in the state file;
//     no relay starts and no srun line follows.
//   - B's next set is accepted by the restarted keeper and starts the second
//     Allocation's relays at a higher E: a new stream crosses them.
func TestAStaleRealControllersAcknowledgementAndSetAreRefusedForTheirEpoch(t *testing.T) {
	g := newRig(t)
	p := g.provider("example-a")
	a1, j1 := g.running(p, "kp-stla")
	a2, j2 := g.running(p, "kp-stlb")
	own := g.w.allSbatches()
	var holdSets atomic.Bool
	ca := g.startController("a", p.name, g.tap.path)
	apid := ca.pid()
	g.tap.setRule(func(r *tapReq) tapAction {
		switch {
		case r.pid != apid:
			return tapPass
		case r.kind == wire.KindAck:
			holdSets.Store(true)
			return tapHold
		case holdSets.Load():
			return tapHold
		}
		return tapPass
	})
	_, up1 := g.leads(ca, p, j1)
	_, up2 := g.leads(ca, p, j2)
	g.adopted(a1, j1)
	g.adopted(a2, j2)
	before := g.api.lease(t, p.name)
	pins1 := g.k.pins(up1)
	pins2 := g.k.pins(up2)
	s := g.w.openStream(j2, serving)
	s.mustEcho(64<<10, "before the final end")
	cb := g.startController("b", p.name, g.tap.path)
	cb.waitLog(logAcquiring)
	waitFor(t, time.Minute, "both relays' srun steps of the first job logged", func() bool { return len(g.w.sruns(j1.id)) == 2 })
	if err := g.w.plat.SetStepLimit(atoi(t, j1.id), 2); err != nil {
		t.Fatal(err)
	}
	pins1[control].kill(t, "the first Allocation's control relay's ssh")
	heldAck := g.tap.waitReq(t, time.Minute, "A's acknowledgement held", func(r *tapReq) bool { return r.pid == apid && r.ack != nil && r.action == tapHold })
	heldSet := g.tap.waitReq(t, time.Minute, "A's next set held", func(r *tapReq) bool { return r.pid == apid && r.set != nil && r.action == tapHold })
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
	hs := g.tap.snapshot(heldSet)
	if e, ok := entryIn(hs.set, j2.tag); !ok || !hs.set.Complete || e.JobID != j2.id {
		t.Fatalf("A's held set %+v does not name the second Allocation's job %s in a complete set", hs.set, j2.id)
	}
	// B takes the Lease while the stream runs, and acknowledges the end.
	end := time.Now().Add(2 * time.Minute)
	for len(g.tap.requests(reportedSetFrom(cb.pid()))) == 0 {
		if time.Now().After(end) {
			t.Fatal("no set of B's was accepted within 2 min of A's pause")
		}
		s.mustEcho(8<<10, "while B takes over")
		time.Sleep(200 * time.Millisecond)
	}
	after := g.api.waitLease(t, p.name, "B holds the Lease", time.Minute, func(l leaseRecord) bool { return l.holder != "" && l.holder != before.holder })
	want := before.transitions + 1
	if after.transitions != want {
		t.Errorf("the Lease's leaseTransitions after B took it is %d, want %d (it was %d under A)", after.transitions, want, before.transitions)
	}
	g.tap.waitReq(t, time.Minute, "B's acknowledgement of the end answered", func(r *tapReq) bool {
		return r.pid == cb.pid() && r.ack != nil && r.ack.UID == j1.uid && r.acked != nil
	})
	g.epochsOf("A", apid, before.transitions)
	g.epochsOf("B", cb.pid(), want)
	s.mustEcho(32<<10, "after B's acknowledgement")

	// A's acknowledgement reaches the running keeper.
	ra := g.tap.let(t, heldAck, tapPass)
	if ra.refused == nil || ra.refused.Reason != wire.RefusedEpoch || ra.refused.HighestEpoch != want {
		t.Errorf("A's stale acknowledgement was answered %s %+v %+v, want refused, %s, naming epoch %d", ra.replyKind, ra.acked, ra.refused, wire.RefusedEpoch, want)
	}
	if ps, _ := g.k.provider(p); ps.Refused[wire.RefusedEpoch] != 1 {
		t.Errorf("the keeper counts %d refusals for the epoch, want 1", ps.Refused[wire.RefusedEpoch])
	}
	s.mustEcho(32<<10, "after A's stale acknowledgement")
	for _, ch := range agentapi.RelayChannels {
		if pins2[ch].exited() {
			t.Errorf("the second Allocation's %s relay's ssh, held through its pidfd, exited", ch)
		}
		r, ok := g.k.upOf(p, j2.tag, ch)
		if !ok || r.PID != up2[ch].PID || r.Epoch != up2[ch].Epoch || r.Seq != up2[ch].Seq || r.Starts != 1 {
			t.Errorf("the second Allocation's %s relay %+v after A's stale acknowledgement, want it as it was: PID %d, (%d, %d), 1 start",
				ch, r, up2[ch].PID, up2[ch].Epoch, up2[ch].Seq)
		}
	}
	if pins1[serving].exited() {
		t.Error("the first Allocation's serving relay's ssh, held through its pidfd, exited")
	}
	s.mustEcho(32<<10, "before the keeper's restart")
	s.whole()

	// The keeper restarts while B's requests are dropped: A's held set is
	// the first request to reach it.
	bpid := cb.pid()
	g.tap.setRule(func(r *tapReq) tapAction {
		switch r.pid {
		case apid:
			return tapHold
		case bpid:
			return tapDrop
		}
		return tapPass
	})
	restarted := time.Now()
	sock := g.k.sock
	g.k = g.k.restart()
	if g.k.sock != sock {
		t.Fatalf("the restarted keeper serves %s, not the tap's upstream %s", g.k.sock, sock)
	}
	if g.k.epoch != firstE+1 {
		t.Errorf("the restarted keeper took E %d, want %d", g.k.epoch, firstE+1)
	}
	if !s.cut() {
		t.Error("the serving stream still echoes across the keeper's restart")
	}
	runs1, runs2 := len(g.w.sruns(j1.id)), len(g.w.sruns(j2.id))
	m := g.k.log.mark()
	rs := g.tap.let(t, heldSet, tapPass)
	if rs.refused == nil || rs.refused.Reason != wire.RefusedEpoch || rs.refused.HighestEpoch != want {
		t.Errorf("A's stale set was answered by the restarted keeper %s %+v, want refused, %s, naming epoch %d", rs.replyKind, rs.refused, wire.RefusedEpoch, want)
	}
	for _, r := range g.tap.requests(func(r *tapReq) bool { return !r.forwarded.IsZero() && r.forwarded.After(restarted) && r.n != rs.n }) {
		if r.forwarded.Before(rs.forwarded) {
			t.Errorf("%s reached the restarted keeper before A's stale set", r.describe())
		}
	}
	ps, _ := g.k.provider(p)
	if ps.Refused[wire.RefusedEpoch] != 1 || len(ps.Relays) != 0 {
		t.Errorf("the restarted keeper's status: %d refusals for the epoch and relays %+v, want 1 and none", ps.Refused[wire.RefusedEpoch], ps.Relays)
	}
	if n1, n2 := len(g.w.sruns(j1.id)), len(g.w.sruns(j2.id)); n1 != runs1 || n2 != runs2 {
		t.Errorf("%d and %d srun lines after A's stale set reached the restarted keeper, want none", n1-runs1, n2-runs2)
	}
	if got := g.k.log.since(m, logStarted); len(got) != 0 {
		t.Errorf("a relay started on A's stale set: %q", got)
	}

	// B's next set is accepted, and starts the second Allocation's relays.
	g.tap.setRule(func(r *tapReq) tapAction {
		if r.pid == apid {
			return tapHold
		}
		return tapPass
	})
	g.tap.waitReq(t, time.Minute, "B's set accepted by the restarted keeper", func(r *tapReq) bool {
		return r.pid == bpid && r.set != nil && r.report != nil && r.forwarded.After(rs.forwarded)
	})
	up := g.k.waitUp(p, j2)
	for ch, r := range up {
		if r.Epoch != firstE+1 {
			t.Errorf("the second Allocation's %s relay under the restarted keeper is at E %d, want %d", ch, r.Epoch, firstE+1)
		}
	}
	g.w.roundTrip(j2, serving, 16<<10)
	g.ownSbatches(own)
	ca.kill()
}

// A leader that loses the lead stops sending before it releases the Lease.
// The tap holds A's next set in flight, and A is then stopped by SIGTERM
// through its pidfd. While the set is held, A's manager waits for its
// leader-election runnables (its log shows the wait begun, its Allocation
// workers finished, and not the next step), and A's Lease, read from the API
// server, is still A's. The tap then lets the set through, and the keeper's
// answer reaches A. The order: the answer came before the release record
// was written (its renewTime, the release's own time), A exited cleanly, and
// no request from A's process (by SO_PEERCRED) came after the held set, so
// none after the release. The relays and the stream run on, whole.
//
// The hold is bounded by the controller's exchange time-out (5 s): a hold
// that outlasts it releases the Lease all the same, so the test checks the
// order, not that the Lease outlasts any hold.
func TestARealLeaderStopsSendingBeforeItReleasesTheLease(t *testing.T) {
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
	idA := g.api.lease(t, p.name).holder
	if idA == "" {
		t.Fatal("the Lease has no holder while A leads")
	}
	var armed atomic.Bool
	armed.Store(true)
	apid := a.pid()
	g.tap.setRule(func(r *tapReq) tapAction {
		if r.pid == apid && r.set != nil && armed.CompareAndSwap(true, false) {
			return tapHold
		}
		return tapPass
	})
	held := g.tap.waitReq(t, time.Minute, "A's set held in flight", func(r *tapReq) bool { return r.pid == apid && r.set != nil && r.action == tapHold })
	if err := a.c.signal(syscall.SIGTERM); err != nil {
		t.Fatalf("TERM A through its pidfd: %v", err)
	}
	a.waitLog(logStopLeaderRun)
	a.waitLog(logWorkersDone)
	if l := g.api.lease(t, p.name); l.holder != idA {
		t.Errorf("A's Lease %+v is not A's while A's set is in flight", l)
	}
	if a.logged(logStopCaches) {
		t.Error("A's manager went past its leader-election runnables while its set was in flight")
	}
	s.mustEcho(32<<10, "while A's set is held")
	if l := g.api.lease(t, p.name); l.holder != idA {
		t.Errorf("A's Lease %+v is not A's while A's set is in flight", l)
	}
	h := g.tap.let(t, held, tapPass)
	if h.answered.IsZero() || h.report == nil {
		t.Fatalf("A's held set was not answered (%s): A gave up on it, the hold outlasted its exchange's time-out", h.err)
	}
	a.waitClean()
	rel := g.api.waitLease(t, p.name, "A's release of the Lease", time.Minute, func(l leaseRecord) bool { return l.found && l.holder == "" })
	t.Logf("A's set in flight: held at %s, its answer written back from %s; the release record's time %s",
		h.at.Format(time.RFC3339Nano), h.replying.Format(time.RFC3339Nano), rel.renew.Format(time.RFC3339Nano))
	if !h.replying.Before(rel.renew) {
		t.Errorf("the keeper's answer to A's set in flight was written back to A from %s, not before the release record's time %s",
			h.replying.Format(time.RFC3339Nano), rel.renew.Format(time.RFC3339Nano))
	}
	for _, r := range g.tap.requests(from(apid)) {
		if r.at.After(h.at) {
			t.Errorf("A sent %s after its set in flight (the release was at %s)", r.describe(), rel.renew.Format(time.RFC3339Nano))
		}
	}
	for range 3 {
		s.mustEcho(32<<10, "after A's release")
	}
	for _, ch := range agentapi.RelayChannels {
		if pins[ch].exited() {
			t.Errorf("the %s relay's ssh, held through its pidfd, exited", ch)
		}
	}
	g.ownSbatches(own)
	s.whole()
}
