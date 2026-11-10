package relaykeeper

// New for issue #343 (PR 3): HBEP-4's 4 relays per login host on the real
// path (path 1): one hostdial.Slots, the Runner's, for every Provider's
// Owner. Every count of relays is paired with bytes that cross.

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Path 1. Two Allocations up hold the login host's 4 slots: their four
// relays run (four ssh clients, four slots in use) and carry bytes. A third
// Allocation's relays wait for their pair of slots: no process, no start, no
// srun line, and a connection on its listener gets no byte back. Once one of
// the two is dropped (a complete set without it), the third's relays start
// and carry bytes, and four run again.
func TestAThirdAllocationWaitsForItsSlotsAndStartsOnceOneIsDropped(t *testing.T) {
	w := newRKWorld(t)
	a := w.submit("hb-rk-slota-0123456789ab", 3*time.Second)
	b := w.submit("hb-rk-slotb-0123456789ab", 3*time.Second)
	c := w.submit("hb-rk-slotc-0123456789ab", 3*time.Second)
	rg := newRealRig(t, w, rigOptions{})
	ea, eb, ec := w.entry(a, genOne), w.entry(b, genOne), w.entry(c, genOne)
	accepted(t, rg.k, rg.set(rkProv, true, ea, eb))
	upA, upB := rg.waitPair(rkProv, a.tag), rg.waitPair(rkProv, b.tag)
	for _, j := range []rkJob{a, b} {
		for _, ch := range agentapi.RelayChannels {
			rkRoundTrip(t, j, ch, 32<<10)
		}
	}
	accepted(t, rg.k, rg.set(rkProv, true, ea, eb, ec))
	rkWaitFor(t, time.Minute, "the third Allocation's relays waiting for their slots", func() bool {
		for _, ch := range agentapi.RelayChannels {
			if r := rg.ownerRelay(rkProv, c.tag, ch); r.Tag != c.tag || !r.Waiting {
				return false
			}
		}
		return true
	})
	for _, j := range []rkJob{a, b} {
		for _, ch := range agentapi.RelayChannels {
			rkRoundTrip(t, j, ch, 32<<10)
		}
	}
	if n := len(relaySSHs(t, a.id, b.id, c.id)); n != 4 {
		t.Errorf("%d relay ssh clients run with the third Allocation waiting, want 4", n)
	}
	if n := len(rg.upOf(rkProv)); n != 4 {
		t.Errorf("the Runner lists %d relays up, want 4", n)
	}
	if n := rg.slots.InUse(rg.host()); n != 4 {
		t.Errorf("%d of the Runner's slots are in use on the login host, want 4", n)
	}
	for _, ch := range agentapi.RelayChannels {
		if cn, err := rkDial(c, ch, 15*time.Second); err == nil {
			cn.close()
			t.Errorf("a connection on the waiting third Allocation's %s listener got a greeting: a fifth relay ran", ch)
		}
		if r := rg.ownerRelay(rkProv, c.tag, ch); r.Starts != 0 || r.PID != 0 {
			t.Errorf("the waiting %s relay started: %+v", ch, r)
		}
	}
	if s := w.sruns(c.id); len(s) != 0 {
		t.Errorf("the fake ran srun for the waiting Allocation: %q", s)
	}
	accepted(t, rg.k, rg.set(rkProv, true, eb, ec))
	rg.waitPair(rkProv, c.tag)
	for _, j := range []rkJob{b, c} {
		for _, ch := range agentapi.RelayChannels {
			rkRoundTrip(t, j, ch, 32<<10)
		}
	}
	for ch, r := range upA {
		if rkAlive(r.PID) {
			t.Errorf("the dropped Allocation's %s relay %d still runs", ch, r.PID)
		}
	}
	for ch, r := range rg.waitPair(rkProv, b.tag) {
		if r.PID != upB[ch].PID {
			t.Errorf("the kept Allocation's %s relay changed from %d to %d", ch, upB[ch].PID, r.PID)
		}
	}
	if n := len(relaySSHs(t, a.id, b.id, c.id)); n != 4 {
		t.Errorf("%d relay ssh clients run after the hand-over of slots, want 4", n)
	}
	if n := rg.slots.InUse(rg.host()); n != 4 {
		t.Errorf("%d slots in use after the hand-over, want 4", n)
	}
	rg.noEnds()
	rg.freeAfter()
}

// Path 1, across a generation change. Two Allocations up hold the 4 slots.
// The first's relay processes outlive their stop's TERM and their ssh
// connections, until the test lets them end (rkWorld.lingerAll), as
// processes whose end has not landed yet. Its new generation's pair waits
// for its slots while the old pair's processes are still there: they are
// listed up, with their own N and PID, and four of the Runner's relay
// processes run, while the other Allocation carries bytes. Only once the old
// processes are gone does the new pair start, with higher N, and carry
// bytes. A sampler of the Runner's relay processes over the whole change
// never sees more than 4.
func TestANewGenerationsPairWaitsUntilTheOldPairsProcessesAreGone(t *testing.T) {
	w := newRKWorld(t)
	a := w.submit("hb-rk-gena-0123456789ab", 30*time.Second)
	b := w.submit("hb-rk-genb-0123456789ab", 30*time.Second)
	ssh := w.lingeringSSH()
	rg := newRealRig(t, w, rigOptions{runner: func(o *OwnerRunnerOptions) { o.SSHBinary, o.StopGrace = ssh, 30*time.Second }})
	w.lingerAll(true)
	accepted(t, rg.k, rg.set(rkProv, true, w.entry(a, genOne)))
	old := rg.waitPair(rkProv, a.tag)
	w.lingerAll(false)
	accepted(t, rg.k, rg.set(rkProv, true, w.entry(a, genOne), w.entry(b, genOne)))
	upB := rg.waitPair(rkProv, b.tag)
	for _, j := range []rkJob{a, b} {
		for _, ch := range agentapi.RelayChannels {
			rkRoundTrip(t, j, ch, 16<<10)
		}
	}
	var most atomic.Int64
	stop, sampled := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			if n := int64(len(relaySSHs(t, a.id, b.id))); n > most.Load() {
				most.Store(n)
			}
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	accepted(t, rg.k, rg.set(rkProv, true, w.entry(a, genTwo), w.entry(b, genOne)))
	rkWaitFor(t, time.Minute, "the new generation's pair waiting for its slots", func() bool {
		for _, ch := range agentapi.RelayChannels {
			r := rg.ownerRelay(rkProv, a.tag, ch)
			if !strings.HasPrefix(r.Gen, genTwo+"/") || !r.Waiting {
				return false
			}
		}
		return true
	})
	// The old pair lingers past its TERM: the window.
	for _, ch := range agentapi.RelayChannels {
		rkRoundTrip(t, b, ch, 16<<10)
	}
	listed := map[int]wire.RelayState{}
	for _, r := range rg.upOf(rkProv) {
		listed[r.PID] = r
	}
	for ch, r := range old {
		if got, ok := listed[r.PID]; !ok || got.Seq != r.Seq || got.Tag != a.tag {
			t.Errorf("the old %s relay %d, still running, is not listed up with its own N %d: %+v", ch, r.PID, r.Seq, rg.upOf(rkProv))
		}
	}
	if n := len(listed); n != 4 {
		t.Errorf("the Runner lists %d relays up in the window, want 4: two old ones still running and the other Allocation's two", n)
	}
	if n := len(relaySSHs(t, a.id, b.id)); n != 4 {
		t.Errorf("%d of the Runner's relay processes run in the window, want 4", n)
	}
	for _, ch := range agentapi.RelayChannels {
		if r := rg.ownerRelay(rkProv, a.tag, ch); r.Starts != 0 || r.PID != 0 {
			t.Errorf("the new generation's %s relay started while the old pair's processes ran: %+v", ch, r)
		}
	}
	for ch, r := range old {
		if !rkAlive(r.PID) {
			t.Fatalf("the old %s relay %d ended before the window's checks were done: they prove nothing", ch, r.PID)
		}
	}
	w.release() // the old pair's processes end now
	nu := rg.waitPairWhere(rkProv, a.tag, func(r wire.RelayState) bool { return r.PID != old[r.Channel].PID })
	for ch, r := range old {
		if rkAlive(r.PID) {
			t.Errorf("the new %s relay started while the old one, %d, still ran", ch, r.PID)
		}
		if nu[ch].Seq <= r.Seq {
			t.Errorf("the new %s relay's N %d is not above the old one's, %d", ch, nu[ch].Seq, r.Seq)
		}
	}
	for _, j := range []rkJob{a, b} {
		for _, ch := range agentapi.RelayChannels {
			rkRoundTrip(t, j, ch, 16<<10)
		}
	}
	close(stop)
	<-sampled
	if n := most.Load(); n > 4 || n < 4 {
		t.Errorf("the sampler saw at most %d of the Runner's relay processes over the change, want 4: never more than HBEP-4's 4", n)
	}
	for ch, r := range rg.waitPair(rkProv, b.tag) {
		if r.PID != upB[ch].PID {
			t.Errorf("the other Allocation's %s relay changed from %d to %d", ch, upB[ch].PID, r.PID)
		}
	}
	rg.noEnds()
	rg.freeAfter()
}

// runningJobs is a listing that always shows an entry's job running under
// its tag, for a Runner with no keeper.
type runningJobs struct{ e wire.Entry }

func (j runningJobs) ListByTag(_ context.Context, tag string) ([]provider.Job, error) {
	return []provider.Job{{ID: j.e.JobID, Name: tag, Phase: v1alpha1.AllocationRunning}}, nil
}

// bareWant is a Want with no keeper behind it: its listing always shows the
// job, its gate always allows, and its callbacks do nothing.
func bareWant(e wire.Entry, uid string) Want {
	return Want{Entry: e, ProviderUID: uid, Jobs: runningJobs{e},
		Finals:      func() map[agentapi.RelayChannel]string { return nil },
		OnFinal:     func(agentapi.RelayChannel, hostdial.EndKind, string, int64, int64) {},
		OnStart:     func(agentapi.RelayChannel, int64, int64) {},
		BeforeStart: func(agentapi.RelayChannel, int64, int64) bool { return true },
		StartFailed: func(agentapi.RelayChannel) {}}
}

// Path 1, across Providers. The keeper lets one Provider use a login host at
// a time (claims.go), so this is the Runner alone, with no keeper: two
// Providers' Owners on one login host share the Runner's one Slots. One
// Allocation of the first Provider and one of the second hold the 4 slots
// and carry bytes; the second Provider's other Allocation waits, with no
// process; once the first Provider's is dropped, it starts and carries
// bytes.
func TestEveryProvidersOwnerSharesTheLoginHostsSlots(t *testing.T) {
	w := newRKWorld(t)
	a := w.submit("hb-rk-shra-0123456789ab", 3*time.Second)
	b := w.submit("hb-rk-shrb-0123456789ab", 3*time.Second)
	c := w.submit("hb-rk-shrc-0123456789ab", 3*time.Second)
	slots := hostdial.NewSlots(hostdial.DefaultRelaysPerLogin)
	rr, err := NewOwnerRunner(context.Background(), OwnerRunnerOptions{Epoch: EpochBase + 1, Ports: w.ports(), Slots: slots,
		SSHDir: w.base + "/k", MinBackoff: 100 * time.Millisecond, MaxBackoff: 400 * time.Millisecond, StopGrace: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rr.Stop)
	const uidA, uidB = "0a1b2c3d-0000-4000-8000-00000000000a", "0a1b2c3d-0000-4000-8000-00000000000b"
	rr.Want(rkProv, w.ssh(), bareWant(w.entry(a, genOne), uidB))
	rr.Want(rkOther, w.ssh(), bareWant(w.entry(b, genOne), uidA))
	pairUp := func(p, tag string) bool {
		n := 0
		for _, r := range rr.Relays(p) {
			if r.Tag == tag && r.State == wire.StateUp && r.PID != 0 {
				n++
			}
		}
		return n == 2
	}
	rkWaitFor(t, time.Minute, "both Providers' pairs up", func() bool { return pairUp(rkProv, a.tag) && pairUp(rkOther, b.tag) })
	rr.Want(rkOther, w.ssh(), bareWant(w.entry(c, genOne), uidA))
	waiting := func() bool {
		rr.mu.Lock()
		pr := rr.provs[rkOther]
		rr.mu.Unlock()
		n := 0
		for _, r := range pr.own.Relays() {
			if r.Tag == c.tag && r.Waiting && r.Starts == 0 {
				n++
			}
		}
		return n == 2
	}
	rkWaitFor(t, time.Minute, "the second Provider's other Allocation waiting for its slots", waiting)
	for _, j := range []rkJob{a, b} {
		for _, ch := range agentapi.RelayChannels {
			rkRoundTrip(t, j, ch, 16<<10)
		}
	}
	if n := len(relaySSHs(t, a.id, b.id, c.id)); n != 4 {
		t.Errorf("%d relay ssh clients run on the login host for two Providers, want 4", n)
	}
	if n := slots.InUse(rr.LoginHost(rkProv, w.ssh())); n != 4 {
		t.Errorf("%d of the Runner's slots are in use, want 4", n)
	}
	for _, ch := range agentapi.RelayChannels {
		if cn, err := rkDial(c, ch, 15*time.Second); err == nil {
			cn.close()
			t.Errorf("the waiting Allocation's %s listener got a greeting: a fifth relay ran", ch)
		}
	}
	if !waiting() {
		t.Error("the waiting Allocation's relays started while the other two Allocations held the slots")
	}
	rr.Drop(rkProv, a.tag)
	rkWaitFor(t, time.Minute, "the waiting Allocation's pair up", func() bool { return pairUp(rkOther, c.tag) })
	for _, ch := range agentapi.RelayChannels {
		rkRoundTrip(t, c, ch, 16<<10)
		rkRoundTrip(t, b, ch, 16<<10)
	}
	if n := len(relaySSHs(t, a.id, b.id, c.id)); n != 4 {
		t.Errorf("%d relay ssh clients run after the hand-over, want 4", n)
	}
}
