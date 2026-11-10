package keeperproc

// HBEP-6's keeper item with REAL controller processes (Test plan, The relay
// keeper; #343's PR 4, Stage B): "a controller restart and a leadership
// hand-over while streams run cut no serving stream, and the relays' E
// doesn't change". Two real controllers of one Provider (ctlrole_test.go)
// and a keeper process; the controllers reach the keeper through the tap,
// which reads each set's Lease epoch off the wire. The expected epochs come
// from client-go's rule, checked against the Provider's Lease as the API
// server holds it, read before and after: the first leader's elector creates
// the record with leaseTransitions 0, the count goes up by one when the
// holder changes, and a copy's epoch is the count at its own acquisition.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A clean hand-over: A leads and B stands by while a serving stream runs; A
// is stopped by SIGTERM through its pidfd, releases the Lease, and B takes
// it.
func TestARealControllersHandOverCutsNoStream(t *testing.T) { realHandOver(t, false) }

// A restart: A is SIGKILLed through its pidfd, so its Lease is not released,
// and B takes it once it has expired, while the serving stream runs.
func TestARealControllersRestartCutsNoStream(t *testing.T) { realHandOver(t, true) }

func realHandOver(t *testing.T, kill bool) {
	g := newRig(t)
	p := g.provider("example-a")
	al, j := g.running(p, "kp-hand")
	own := g.w.allSbatches()
	a := g.startController("a", p.name, g.tap.path)
	setA, up := g.leads(a, p, j)
	g.adopted(al, j)
	before := g.firstLease(p.name)
	if setA.set.LeaseEpoch != before.transitions {
		t.Errorf("A's set carries Lease epoch %d; the Lease as written while A alone holds it has leaseTransitions %d", setA.set.LeaseEpoch, before.transitions)
	}
	held := g.k.pins(up) // the relays' ssh, held through pidfds across the event
	jobBefore := j.status(t)
	s := g.w.openStream(j, serving)
	s.mustEcho(64<<10, "before the event")
	b := g.startController("b", p.name, g.tap.path)
	b.waitLog(logAcquiring)
	s.mustEcho(32<<10, "with B standing by")
	if l := g.api.lease(t, p.name); l.holder != before.holder || l.transitions != before.transitions {
		t.Fatalf("the Lease %+v changed while A leads and B stands by, was %+v", l, before)
	}
	if n := len(g.tap.requests(from(b.pid()))); n != 0 {
		t.Errorf("B sent %d requests to the keeper while it stood by", n)
	}
	m := g.k.log.mark()
	if kill {
		a.kill()
	} else {
		a.term()
	}
	// The stream runs across the event: bytes cross until B's first set is
	// answered.
	end := time.Now().Add(2 * time.Minute)
	for len(g.setsFrom(b.pid())) == 0 {
		if time.Now().After(end) {
			t.Fatal("B sent no set the keeper answered within 2 min of A's end")
		}
		s.mustEcho(8<<10, "during the take-over")
		time.Sleep(200 * time.Millisecond)
	}
	g.leads(b, p, j)
	after := g.api.waitLease(t, p.name, "B holds the Lease", time.Minute, func(l leaseRecord) bool { return l.holder != "" && l.holder != before.holder })
	want := before.transitions + 1 // client-go: one up on a holder change
	if after.transitions != want {
		t.Errorf("the Lease's leaseTransitions after B took it is %d, want %d (it was %d under A)", after.transitions, want, before.transitions)
	}
	g.epochsOf("A", a.pid(), before.transitions)
	g.epochsOf("B", b.pid(), want)
	for _, r := range g.tap.requests(from(b.pid())) {
		if r.refused != nil {
			t.Errorf("the keeper refused B's %s: %+v", r.describe(), *r.refused)
		}
	}
	for range 4 {
		s.mustEcho(32<<10, "after B's complete set")
	}
	listed := map[agentapi.RelayChannel]int{}
	for _, r := range g.k.relaysOf(p, j.tag) {
		listed[r.Channel]++
	}
	for _, ch := range agentapi.RelayChannels {
		if listed[ch] != 1 {
			t.Errorf("the keeper lists the %s relay %d times after the event, want once", ch, listed[ch])
		}
		if held[ch].exited() {
			t.Errorf("the %s relay's ssh, held through its pidfd, exited across the event: it was replaced", ch)
		}
		r, ok := g.k.upOf(p, j.tag, ch)
		switch {
		case !ok:
			t.Errorf("the %s relay is not up after the event", ch)
		case r.PID != up[ch].PID || r.Epoch != up[ch].Epoch || r.Seq != up[ch].Seq || r.Starts != 1:
			t.Errorf("the %s relay after the event: PID %d, (E, N) (%d, %d), %d starts; want it adopted: PID %d, (%d, %d), 1 start",
				ch, r.PID, r.Epoch, r.Seq, r.Starts, up[ch].PID, up[ch].Epoch, up[ch].Seq)
		case r.Epoch != firstE:
			t.Errorf("the %s relay's E is %d, want %d", ch, r.Epoch, firstE)
		}
		for _, msg := range []string{logStarted, logNotStarted} {
			if got := g.k.log.since(m, relayLog(msg, j.tag, ch)...); len(got) != 0 {
				t.Errorf("the keeper's log across the event, the %s relay: %q; want no start and no refused one", ch, got)
			}
		}
	}
	if got := g.k.log.since(m, whyGate); len(got) != 0 {
		t.Errorf("a start was refused at the keeper's gate across the event: %q", got)
	}
	jobAfter := j.status(t)
	for _, ch := range agentapi.RelayChannels {
		if jobAfter[ch] != jobBefore[ch] || !jobAfter[ch].Connected || jobAfter[ch].Sessions != 1 {
			t.Errorf("the job side's %s channel %+v after the event, want the one session it kept, %+v", ch, jobAfter[ch], jobBefore[ch])
		}
	}
	g.ownSbatches(own)
	s.whole()
}
