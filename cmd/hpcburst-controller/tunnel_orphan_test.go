package main

// TunnelOrphaned (HBEP-8, Reasons in alpha; HBEP-6, The inventory): an online proxy that is no
// current Instance's UID gets a degraded event from the lifecycle controller, regarding the
// Allocation whose credential admitted the tunnel, else the lifecycle Lease, under HBEP-8's
// hourly gate for a repeating condition (Repeating conditions: when it starts, then at most
// once an hour while it lasts, never on every read).
//
// The clock is the test's: the gate reads pathState.now, and no test waits an hour. The
// expected values are HBEP-8's, written out: the type string, the reason, the regarding
// objects' kinds and names, and the hour. The stub Reader answers with the orphans the test
// spells out, never ones computed from the writer.

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
)

// orphanGate is HBEP-8's hour, and orphanGrace TunnelOrphanGrace (`@hpc-burst-science-agent`,
// 1 Nov 2026, for #167: an orphan only after every read for 2 minutes listed it so), both written
// out here, not read from the code under test.
const (
	orphanGate  = time.Hour
	orphanGrace = 2 * time.Minute
)

// orphanSteps is a Reader whose answer the test changes between reads.
type orphanSteps struct {
	orphans []string
	err     error
}

func (o *orphanSteps) Read(_ context.Context, uids []string) frpsplugin.Report {
	rep := frpsplugin.Report{Conditions: map[string]metav1.Condition{}, Orphans: o.orphans, Err: o.err}
	if o.err != nil {
		rep.Orphans = nil
	}
	for _, u := range uids {
		rep.Conditions[u] = cond(metav1.ConditionTrue, "ProxyOnline", "up")
	}
	return rep
}

func orphanAllocation(name, uid string) *v1alpha1.Allocation {
	a := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: name, UID: types.UID(uid)}}
	return a
}

// orphanRig is a writer over fake clients holding the given objects, with the test's clock and
// the plugin's admissions.
type orphanRig struct {
	t     *testing.T
	state *pathState
	rd    *orphanSteps
	rec   *lifecycle.MemoryRecorder
	adm   *admissions
	base  time.Time // the clock's start; every read says how far from it it runs
	now   time.Time
}

func newOrphanRig(t *testing.T, objs ...client.Object) *orphanRig {
	t.Helper()
	c := tunnelScheme(t).WithObjects(objs...).Build()
	r := &orphanRig{t: t, rd: &orphanSteps{}, rec: &lifecycle.MemoryRecorder{}, adm: newAdmissions(frpsplugin.NewTrigger()),
		base: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	r.now = r.base
	r.state = stateFor(c, c, r.rd, nil)
	r.state.rec, r.state.admitted, r.state.now = r.rec, r.adm, func() time.Time { return r.now }
	return r
}

// read runs one read of the list at the clock's start plus offset.
func (r *orphanRig) read(offset time.Duration) {
	r.t.Helper()
	r.now = r.base.Add(offset)
	r.state.readOnce(r.t.Context())
}

// events returns the TunnelOrphaned events: HBEP-8's type degraded and the reason alone.
func (r *orphanRig) events() []lifecycle.RecordedEvent {
	r.t.Helper()
	evs := r.rec.Find("degraded", "TunnelOrphaned")
	if all := r.rec.Events(); len(evs) != len(all) {
		r.t.Fatalf("events %+v: every event here must be degraded/TunnelOrphaned", all)
	}
	for _, e := range evs {
		if !e.Warning {
			r.t.Fatalf("event %+v: degraded is a Warning event", e)
		}
	}
	return evs
}

func (r *orphanRig) wantEvents(step string, n int) []lifecycle.RecordedEvent {
	r.t.Helper()
	evs := r.events()
	if len(evs) != n {
		r.t.Fatalf("%s: %d TunnelOrphaned events %+v, want %d", step, len(evs), evs, n)
	}
	return evs
}

func (r *orphanRig) wantRegarding(e lifecycle.RecordedEvent, kind, ns, name string) {
	r.t.Helper()
	if e.Kind != kind || e.Namespace != ns || e.Name != name {
		r.t.Fatalf("the event regards %s %s/%s, want %s %s/%s", e.Kind, e.Namespace, e.Name, kind, ns, name)
	}
}

// One event when the orphan is first seen, then at most one an hour while it lasts: not on every
// read, and not early. Regarding the Allocation whose credential admitted the tunnel.
func TestOrphanEventsFollowHBEP8sHourlyGate(t *testing.T) {
	r := newOrphanRig(t, orphanAllocation("al-a", "alloc-uid-1"), orphanAllocation("al-b", "alloc-uid-2"))
	r.rd.orphans = []string{"gone-instance"}
	// The plugin admitted the proxy through Allocation al-b, not the first one in the list.
	r.adm.ProxyChanged(frpsplugin.OpNewProxy, "gone-instance", "alloc-uid-2")

	r.read(-orphanGrace)
	r.wantEvents("first seen, the grace starts", 0)
	r.read(0)
	first := r.wantEvents("listed for the whole grace", 1)[0]
	r.wantRegarding(first, "*v1alpha1.Allocation", "team-a", "al-b")

	// The gate holds for the hour: at the read period, in a burst, and one second short of it.
	for i := 1; i <= 5; i++ {
		r.read(time.Duration(i) * 10 * time.Second)
	}
	r.wantEvents("a burst of reads in the first minute", 1)
	r.read(orphanGate - time.Second)
	r.wantEvents("one second short of the hour", 1)

	r.read(orphanGate)
	r.wantEvents("at the hour", 2)
	r.read(orphanGate + time.Minute)
	r.read(orphanGate + 31*time.Minute)
	r.read(2*orphanGate - time.Second)
	r.wantEvents("one second short of the second hour", 2)
	r.read(2 * orphanGate)
	evs := r.wantEvents("at the second hour", 3)
	for _, e := range evs {
		r.wantRegarding(e, "*v1alpha1.Allocation", "team-a", "al-b")
	}
}

// An orphan that is gone from a read leaves the gate: when it is back it is a new start, and the
// event comes at once, inside the hour of the first.
func TestOrphanEventRestartsWhenTheOrphanWentAwayAndCameBack(t *testing.T) {
	r := newOrphanRig(t, orphanAllocation("al-a", "alloc-uid-1"))
	r.adm.ProxyChanged(frpsplugin.OpNewProxy, "gone-instance", "alloc-uid-1")
	r.rd.orphans = []string{"gone-instance"}
	r.read(-orphanGrace)
	r.read(0)
	r.wantEvents("an orphan after the grace", 1)

	r.rd.orphans = nil // frps no longer lists it online
	r.read(10 * time.Minute)
	r.wantEvents("gone", 1)

	// Back: its grace starts again, and then the event comes, inside the hour of the first.
	r.rd.orphans = []string{"gone-instance"}
	r.read(20 * time.Minute)
	r.wantEvents("back after 20 minutes, inside a new grace", 1)
	back := 20*time.Minute + orphanGrace
	r.read(back)
	r.wantEvents("back for a whole grace", 2)
	// The new start has its own hour.
	r.read(back + orphanGate - time.Second)
	r.wantEvents("one second short of an hour after the new start", 2)
	r.read(back + orphanGate)
	r.wantEvents("an hour after the new start", 3)
}

// A read of the list that fails is no evidence that the orphan went away: the hourly gate holds
// through it (it is not a new start), and nothing is emitted. It restarts the grace's clock
// (TestOrphanGraceAnUnreadableListRestartsTheClock), so the orphan is one again a grace later,
// still inside the hour of the first event, which the gate keeps quiet.
func TestOrphanGateSurvivesAnUnreadableList(t *testing.T) {
	r := newOrphanRig(t, orphanAllocation("al-a", "alloc-uid-1"))
	r.adm.ProxyChanged(frpsplugin.OpNewProxy, "gone-instance", "alloc-uid-1")
	r.rd.orphans = []string{"gone-instance"}
	r.read(-orphanGrace)
	r.read(0)
	r.wantEvents("an orphan after the grace", 1)

	r.rd.err = errors.New("frps's web server is down")
	r.read(10 * time.Minute)
	r.wantEvents("the list unreadable", 1)

	r.rd.err = nil
	r.read(20 * time.Minute)
	r.read(20*time.Minute + orphanGrace)
	evs := r.wantEvents("readable again for a grace, inside the hour of the first", 1)
	r.wantRegarding(evs[0], "*v1alpha1.Allocation", "team-a", "al-a") // the admission was kept through the outage
	r.read(orphanGate)
	evs = r.wantEvents("the hour after the first event", 2)
	r.wantRegarding(evs[1], "*v1alpha1.Allocation", "team-a", "al-a")
}

// An unreadable list restarts the grace's clock (`@hpc-burst-science-agent`, 1 Nov 2026, review
// of #186): an interval nobody observed must not count toward the grace. The science agent's
// timeline, written out: listed at 0 s (and each read to 100 s), unreadable at 110 s, listed at
// 130 s: no event then, nor at 240 s; one at 250 s, a whole grace after 130 s.
func TestOrphanGraceAnUnreadableListRestartsTheClock(t *testing.T) {
	r := newOrphanRig(t, orphanAllocation("al-a", "alloc-uid-1"))
	r.adm.ProxyChanged(frpsplugin.OpNewProxy, "lingering", "alloc-uid-1")
	r.rd.orphans = []string{"lingering"}
	r.readEvery(0, 100*time.Second)
	r.rd.err = errors.New("frps's web server is down")
	r.read(110 * time.Second)
	r.rd.err = nil
	r.read(130 * time.Second)
	r.wantEvents("listed at 130 s after an unreadable read at 110 s", 0)
	r.readEvery(140*time.Second, 130*time.Second+orphanGrace-10*time.Second)
	r.wantEvents("one read short of a grace after 130 s", 0)
	r.read(130*time.Second + orphanGrace)
	r.wantRegarding(r.wantEvents("a whole grace after 130 s", 1)[0], "*v1alpha1.Allocation", "team-a", "al-a")
}

// Without the plugin's admission (after a restart the map is empty) or with the Allocation gone
// from the cache, the event regards the lifecycle Lease, in hpcburst-system. Each orphan has its
// own gate.
func TestOrphanEventRegardsTheLifecycleLeaseWhenNoAllocationIsKnown(t *testing.T) {
	r := newOrphanRig(t, orphanAllocation("al-a", "alloc-uid-1"))
	r.rd.orphans = []string{"unadmitted"}
	r.read(-orphanGrace)
	r.read(0)
	r.wantRegarding(r.wantEvents("no admission known", 1)[0], "*v1.Lease", v1alpha1.ControllerNamespace, v1alpha1.LeaseLifecycle)
	if v1alpha1.ControllerNamespace != "hpcburst-system" || v1alpha1.LeaseLifecycle != "lifecycle" {
		t.Fatalf("the Lease is %s/%s, HBEP-8 says hpcburst-system/lifecycle", v1alpha1.ControllerNamespace, v1alpha1.LeaseLifecycle)
	}

	// The admitting Allocation is not in the cache any more: the Lease again.
	r.adm.ProxyChanged(frpsplugin.OpNewProxy, "from-a-deleted-allocation", "alloc-uid-deleted")
	r.rd.orphans = []string{"unadmitted", "from-a-deleted-allocation"}
	r.read(time.Minute)
	r.wantEvents("a second proxy, inside its grace", 1)
	r.read(time.Minute + orphanGrace)
	evs := r.wantEvents("a second orphan, its Allocation gone", 2)
	r.wantRegarding(evs[1], "*v1.Lease", v1alpha1.ControllerNamespace, v1alpha1.LeaseLifecycle)

	// And one whose Allocation is there regards it, beside them.
	r.adm.ProxyChanged(frpsplugin.OpNewProxy, "admitted", "alloc-uid-1")
	r.rd.orphans = []string{"admitted", "from-a-deleted-allocation", "unadmitted"}
	r.read(10 * time.Minute)
	r.wantEvents("a third proxy, inside its grace", 2)
	r.read(10*time.Minute + orphanGrace)
	evs = r.wantEvents("a third orphan, its Allocation known", 3)
	r.wantRegarding(evs[2], "*v1alpha1.Allocation", "team-a", "al-a")
}

// The admission follows the plugin's calls only: a CloseProxy names no Allocation and changes
// nothing, and an admitted proxy's entry is forgotten once the proxy is neither a live Instance's
// nor an orphan.
func TestAdmissionsKeepWhatMayBecomeAnOrphanAndForgetTheRest(t *testing.T) {
	adm := newAdmissions(frpsplugin.NewTrigger())
	adm.ProxyChanged(frpsplugin.OpNewProxy, "p1", "alloc-1")
	adm.ProxyChanged(frpsplugin.OpNewProxy, "p2", "alloc-2")
	adm.ProxyChanged(frpsplugin.OpCloseProxy, "p1", "")
	if uid, ok := adm.allocationOf("p1"); !ok || uid != "alloc-1" {
		t.Fatalf("after a CloseProxy p1 is admitted by %q, %t; want alloc-1 kept", uid, ok)
	}
	adm.retain(map[string]struct{}{"p2": {}})
	if _, ok := adm.allocationOf("p1"); ok {
		t.Error("p1 is neither live nor an orphan and is still remembered")
	}
	if uid, ok := adm.allocationOf("p2"); !ok || uid != "alloc-2" {
		t.Errorf("p2 admitted by %q, %t; want alloc-2 kept", uid, ok)
	}
	select {
	case <-adm.trigger.C():
	default:
		t.Error("the plugin's calls no longer reach the trigger")
	}
}

// An ended Instance keeps its last TunnelConnected, which can be True (HBEP-6 reads the
// condition for the inventory; nothing routes or decides on an ended Instance): the writer
// neither asks about it nor writes it, a later read that would change it touches nothing, and its
// proxy, if frps still lists it online, is an orphan and raises TunnelOrphaned, regarding the
// Allocation that admitted it.
//
// The Reader here answers as frps's list does, from what the test sets: for an Instance it is
// asked about, True while the proxy is online and False when it is not; and the proxy, online
// and asked about by no one, is an orphan. So a writer that asked about the ended Instance
// would write False the moment the proxy went offline, and the stored True would change.
func TestEndedInstanceKeepsItsConditionAndItsOnlineProxyIsAnOrphan(t *testing.T) {
	alloc := orphanAllocation("al-a", "alloc-uid-1")
	in := unitInstance("in-1", "inst-uid-1", 1)
	in.Status.Phase = v1alpha1.InstanceReady
	in.Status.Conditions = []metav1.Condition{{Type: "TunnelConnected", Status: metav1.ConditionTrue, Reason: "ProxyOnline", Message: "up", LastTransitionTime: metav1.Now()}}
	k := &counted{}
	c := tunnelScheme(t).WithObjects(alloc, in).WithInterceptorFuncs(k.funcs()).Build()
	r := &orphanRig{t: t, rec: &lifecycle.MemoryRecorder{}, adm: newAdmissions(frpsplugin.NewTrigger()), base: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	r.now = r.base
	var asked []string
	proxyOnline := true
	rd := readerFunc(func(_ context.Context, uids []string) frpsplugin.Report {
		asked = append(asked, uids...)
		rep := frpsplugin.Report{Conditions: map[string]metav1.Condition{}}
		for _, u := range uids {
			if proxyOnline {
				rep.Conditions[u] = cond(metav1.ConditionTrue, "ProxyOnline", "up")
			} else {
				rep.Conditions[u] = cond(metav1.ConditionFalse, "ProxyOffline", "frps does not list a proxy for the Instance")
			}
		}
		if proxyOnline && !slices.Contains(uids, "inst-uid-1") {
			rep.Orphans = []string{"inst-uid-1"}
		}
		return rep
	})
	r.state = stateFor(c, c, rd, nil)
	r.state.rec, r.state.admitted, r.state.now = r.rec, r.adm, func() time.Time { return r.now }
	r.adm.ProxyChanged(frpsplugin.OpNewProxy, "inst-uid-1", "alloc-uid-1")

	r.read(0)
	r.wantEvents("the Instance lives", 0)
	if len(asked) != 1 || asked[0] != "inst-uid-1" || k.updates.Load() != 0 {
		t.Fatalf("while it lives: asked %v, %d writes; want it asked about, and nothing to write (True already)", asked, k.updates.Load())
	}

	// The Instance ends, the proxy stays online.
	got := getInstance(t, c, "in-1")
	got.Status.Phase = v1alpha1.InstanceEnded
	got.Status.EndReason = v1alpha1.ReasonDrained
	if err := c.Status().Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	k.updates.Store(0)
	asked = nil
	r.read(10 * time.Second)
	r.wantEvents("its proxy still online right after the end, inside the grace", 0)
	r.read(10*time.Second + orphanGrace)
	evs := r.wantEvents("its proxy still online a whole grace after the end", 1)
	r.wantRegarding(evs[0], "*v1alpha1.Allocation", "team-a", "al-a")

	// The proxy goes offline: a read that would turn a live Instance's condition False.
	proxyOnline = false
	r.read(20*time.Second + orphanGrace)
	r.read(30*time.Second + orphanGrace)

	if len(asked) != 0 {
		t.Errorf("after it ended the reader was asked about %v, want no UID", asked)
	}
	if k.updates.Load() != 0 {
		t.Errorf("%d status writes after the Instance ended, want none", k.updates.Load())
	}
	end := getInstance(t, c, "in-1")
	if tc := meta.FindStatusCondition(end.Status.Conditions, "TunnelConnected"); tc == nil || tc.Status != metav1.ConditionTrue || tc.Reason != "ProxyOnline" {
		t.Errorf("the ended Instance's TunnelConnected = %+v, want the True/ProxyOnline it had at the end", tc)
	}
	r.wantEvents("the proxy offline: no orphan any more", 1)
}

// readEvery reads at from, then every 10 s (HBEP-6's read period) up to and including to.
func (r *orphanRig) readEvery(from, to time.Duration) {
	r.t.Helper()
	for at := from; at <= to; at += 10 * time.Second {
		r.read(at)
	}
}

// TunnelOrphanGrace (`@hpc-burst-science-agent`, 1 Nov 2026, for #167): a normal drain. The
// Instance has ended and its frpc still serves the proxy for a while; it goes offline within the
// grace, so no read ever made it an orphan, and nothing is raised, then or later.
func TestOrphanGraceANormalDrainRaisesNothing(t *testing.T) {
	r := newOrphanRig(t, orphanAllocation("al-a", "alloc-uid-1"))
	r.adm.ProxyChanged(frpsplugin.OpNewProxy, "drained-instance", "alloc-uid-1")
	r.rd.orphans = []string{"drained-instance"}
	r.readEvery(0, orphanGrace-10*time.Second) // listed on every read for 1 m 50 s
	r.rd.orphans = nil                         // frpc stopped: frps lists it offline
	r.readEvery(orphanGrace, orphanGrace+orphanGate)
	r.wantEvents("a drain whose proxy went offline within the grace", 0)
}

// TunnelOrphanGrace: a proxy still online after the grace raises exactly one event, at the first
// read a whole grace after it was first listed, not one read earlier, and then only on the hour.
func TestOrphanGraceStillOnlineAfterTheGraceRaisesExactlyOne(t *testing.T) {
	r := newOrphanRig(t, orphanAllocation("al-a", "alloc-uid-1"))
	r.adm.ProxyChanged(frpsplugin.OpNewProxy, "lingering", "alloc-uid-1")
	r.rd.orphans = []string{"lingering"}
	r.readEvery(0, orphanGrace-10*time.Second)
	r.wantEvents("one read short of the grace", 0)
	r.read(orphanGrace)
	r.wantRegarding(r.wantEvents("a whole grace", 1)[0], "*v1alpha1.Allocation", "team-a", "al-a")
	r.readEvery(orphanGrace+10*time.Second, orphanGrace+30*time.Minute)
	r.wantEvents("half an hour on", 1)
}

// TunnelOrphanGrace: a proxy that flaps offline and back starts its clock again. Listed for a
// minute, missing from one read, listed again: two minutes after it was FIRST listed it is still
// inside the grace of its return, and the event comes a whole grace after the return.
func TestOrphanGraceAFlapRestartsTheClock(t *testing.T) {
	r := newOrphanRig(t, orphanAllocation("al-a", "alloc-uid-1"))
	r.adm.ProxyChanged(frpsplugin.OpNewProxy, "flapping", "alloc-uid-1")
	r.rd.orphans = []string{"flapping"}
	r.readEvery(0, time.Minute)
	r.rd.orphans = nil
	r.read(time.Minute + 10*time.Second) // one read without it
	r.rd.orphans = []string{"flapping"}
	back := time.Minute + 20*time.Second
	r.readEvery(back, back+orphanGrace-10*time.Second)
	r.wantEvents("two minutes after it was first listed, inside the grace of its return", 0)
	r.read(back + orphanGrace)
	r.wantEvents("a whole grace after its return", 1)
}
