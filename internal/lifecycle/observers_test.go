package lifecycle

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The tests in this file cover HBEP-8, Observers, and Repeating conditions:
// the lifecycle controller judges each observer's heartbeat by its age and
// raises failing/HeartbeatMissed when it goes stale, then at most once an
// hour while it stays stale, never on every check. The expected times are
// worked out by hand from the rules, not from the code: a Provider read at T is
// stale once older than ProviderReadyStaleness (5 min), the front's Lease once
// its renewTime is older than its leaseDurationSeconds (30 s), an agent once it
// has sent no report for health.agentTimeout (2 min); the gate is an hour. Each
// "never" case has a control in the same rig that fires.

// heartbeatTimes runs check every tick for total and returns, as offsets from
// from, the times at which a new HeartbeatMissed event regarding the object
// appeared.
func heartbeatTimes(t *testing.T, rg *rig, kind, name string, from time.Time, tick, total time.Duration, check func()) []time.Duration {
	t.Helper()
	var out []time.Duration
	n := len(heartbeatEvents(rg, kind, name))
	for end := rg.clock.Now().Add(total); rg.clock.Now().Before(end); {
		rg.clock.Advance(tick)
		check()
		if m := len(heartbeatEvents(rg, kind, name)); m > n {
			if m > n+1 {
				t.Fatalf("%d HeartbeatMissed events on %s %s in one check", m-n, kind, name)
			}
			out = append(out, rg.clock.Now().Sub(from))
			n = m
		}
	}
	return out
}

// heartbeatEvents returns the failing/HeartbeatMissed events regarding one
// object, by its Go type's name and its name.
func heartbeatEvents(rg *rig, kind, name string) []RecordedEvent {
	var out []RecordedEvent
	for _, e := range rg.rec.Find("failing", "HeartbeatMissed") {
		if e.Kind == kind && e.Name == name {
			if !e.Warning {
				rg.t.Fatalf("HeartbeatMissed on %s %s is not a Warning", kind, name)
			}
			out = append(out, e)
		}
	}
	return out
}

// checkHourly asserts HBEP-8's rate rule on the times a condition that lasted
// three hours or more fired at: at the first check it was stale (first), then
// at least an hour apart, at most four times in all and at least twice (an
// ungated or a once-only emitter fails it).
func checkHourly(t *testing.T, got []time.Duration, first time.Duration) {
	t.Helper()
	if len(got) < 2 || len(got) > 4 {
		t.Fatalf("fired at %v: want 2 to 4 events over three stale hours (once at the start, then at most hourly)", got)
	}
	if got[0] != first {
		t.Fatalf("first event at +%s, want +%s, the first check after the heartbeat went stale", got[0], first)
	}
	for i := 1; i < len(got); i++ {
		if got[i]-got[i-1] < time.Hour {
			t.Fatalf("events at %v: +%s and +%s are less than an hour apart", got, got[i-1], got[i])
		}
	}
}

func (rg *rig) checkHeartbeats() {
	rg.t.Helper()
	if err := rg.r.checkHeartbeats(rg.ctx); err != nil {
		rg.t.Fatalf("checkHeartbeats: %v", err)
	}
}

// TestHeartbeatMissedStaleProviderOnceThenHourly: the Provider controller reads
// once at 08:00:00 and stops, so its stored Ready stays True. Checked every
// minute, the Provider is stale from 08:06 (6 min > 5 min; 5 min is not), and
// fires then, while Ready still says True, and then at most hourly over three
// hours. A fresh read clears it; going stale again fires at once, not an hour
// after the last event.
func TestHeartbeatMissedStaleProviderOnceThenHourly(t *testing.T) {
	rg := newRig(t)
	if err := rg.prov.Sync(rg.ctx); err != nil {
		t.Fatal(err)
	}
	read := rg.clock.Now()
	if got := rg.provider().Status.LastSuccessfulStatusRead; got == nil || !got.Time.Equal(read) {
		t.Fatalf("lastSuccessfulStatusRead %v after the read, want %s", got, read)
	}
	const kind = "*v1alpha1.Provider"
	var readyAtFirst *bool
	got := heartbeatTimes(t, rg, kind, rigProvider, read, time.Minute, 3*time.Hour+6*time.Minute, func() {
		rg.checkHeartbeats()
		if readyAtFirst == nil && len(heartbeatEvents(rg, kind, rigProvider)) == 1 {
			v := apimeta.IsStatusConditionTrue(rg.provider().Status.Conditions, v1alpha1.ConditionProviderReady)
			readyAtFirst = &v
		}
	})
	checkHourly(t, got, 6*time.Minute)
	if readyAtFirst == nil || !*readyAtFirst {
		t.Fatal("the stored Ready was not True when HeartbeatMissed fired; the test does not show a judgement by age")
	}
	// A fresh read clears the condition: no event while it is fresh, and the
	// next staleness fires at its start.
	if err := rg.prov.Sync(rg.ctx); err != nil {
		t.Fatal(err)
	}
	again := rg.clock.Now()
	got = heartbeatTimes(t, rg, kind, rigProvider, again, time.Minute, 10*time.Minute, rg.checkHeartbeats)
	if len(got) != 1 || got[0] != 6*time.Minute {
		t.Fatalf("after a fresh read, then silence: fired at %v, want once at +6m", got)
	}
}

// TestHeartbeatMissedBatchedProviderReads: reads in batches against a check
// every minute that follows the read. Reads every 5 or 6 min leave the read at
// most 4 or 5 min old at a check, within the 5 min rule (5 min itself is not
// stale), and never fire (HBEP-8, Test plan, Batched heartbeats); the control,
// reads every 7 min, leaves it 6 min old and fires.
func TestHeartbeatMissedBatchedProviderReads(t *testing.T) {
	for _, tc := range []struct {
		every time.Duration
		fires bool
	}{{5 * time.Minute, false}, {6 * time.Minute, false}, {7 * time.Minute, true}} {
		t.Run(tc.every.String(), func(t *testing.T) {
			rg := newRig(t)
			minutes := 0
			got := heartbeatTimes(t, rg, "*v1alpha1.Provider", rigProvider, rg.clock.Now(), time.Minute, 2*time.Hour, func() {
				if time.Duration(minutes)*time.Minute%tc.every == 0 {
					if err := rg.prov.Sync(rg.ctx); err != nil {
						t.Fatal(err)
					}
				}
				minutes++
				rg.checkHeartbeats()
			})
			if fired := len(got) > 0; fired != tc.fires {
				t.Fatalf("reads every %s: fired at %v, want fired=%v", tc.every, got, tc.fires)
			}
		})
	}
}

// TestHeartbeatMissedProviderNeverRead (`@hpc-burst-science-agent`, 31 Oct
// 2026, for #111): a Provider with no successful read at all is stale once its
// creation is older than 5 min, and fires at +6 min, then hourly.
func TestHeartbeatMissedProviderNeverRead(t *testing.T) {
	rg := newRig(t)
	created := rg.clock.Now()
	rg.create(&v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: "never-read"},
		Spec:       v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: rigProfile}, AllowedPartitions: []string{rigPartition}},
	})
	got := heartbeatTimes(t, rg, "*v1alpha1.Provider", "never-read", created, time.Minute, 3*time.Hour+6*time.Minute, func() {
		if err := rg.prov.Sync(rg.ctx); err != nil { // the rig's own Provider stays fresh
			t.Fatal(err)
		}
		rg.checkHeartbeats()
	})
	checkHourly(t, got, 6*time.Minute)
	if n := len(heartbeatEvents(rg, "*v1alpha1.Provider", rigProvider)); n != 0 {
		t.Fatalf("%d HeartbeatMissed events on the Provider that reads every minute", n)
	}
}

// frontLease is the front's Lease as the front writes it (internal/front/
// lease.go): renewed every 10 s, leaseDurationSeconds 30.
func frontLease(renew time.Time, seconds int32) *coordinationv1.Lease {
	t := metav1.NewMicroTime(renew)
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: v1alpha1.ControllerNamespace, Name: "front"},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: ptr.To("hpcburst-front/1"), LeaseDurationSeconds: ptr.To(seconds), AcquireTime: &t, RenewTime: &t},
	}
}

func (rg *rig) renewFront(at time.Time) {
	rg.t.Helper()
	var l coordinationv1.Lease
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: "front"}, &l); err != nil {
		rg.t.Fatal(err)
	}
	t := metav1.NewMicroTime(at)
	l.Spec.RenewTime = &t
	if err := rg.c.Update(rg.ctx, &l); err != nil {
		rg.t.Fatal(err)
	}
}

// TestHeartbeatMissedFrontLease: with no front Lease nothing is judged
// (`@hpc-burst-science-agent`, 31 Oct 2026, for #111). A front that renews
// every 10 s never fires. Once it stops, its Lease is stale when its renewTime
// is more than 30 s old: not at 30 s, at 31 s, and then at most hourly over
// three hours. A Lease of 60 s is judged by its own duration.
func TestHeartbeatMissedFrontLease(t *testing.T) {
	const kind = "*v1.Lease"
	rg := newRig(t)
	if got := heartbeatTimes(t, rg, kind, "front", rg.clock.Now(), 10*time.Second, 10*time.Minute, rg.checkHeartbeats); len(got) != 0 {
		t.Fatalf("no front Lease: fired at %v", got)
	}
	rg.create(frontLease(rg.clock.Now(), 30))
	got := heartbeatTimes(t, rg, kind, "front", rg.clock.Now(), 10*time.Second, 10*time.Minute, func() {
		rg.renewFront(rg.clock.Now())
		rg.checkHeartbeats()
	})
	if len(got) != 0 {
		t.Fatalf("a front renewing every 10 s: fired at %v", got)
	}
	stopped := rg.clock.Now()
	rg.clock.Set(stopped.Add(30 * time.Second))
	rg.checkHeartbeats()
	if n := len(heartbeatEvents(rg, kind, "front")); n != 0 {
		t.Fatalf("renewTime exactly 30 s old: %d events, want none", n)
	}
	got = heartbeatTimes(t, rg, kind, "front", stopped, time.Second, 3*time.Hour+time.Second, rg.checkHeartbeats)
	checkHourly(t, got, 31*time.Second)
	for _, e := range heartbeatEvents(rg, kind, "front") {
		if e.Namespace != v1alpha1.ControllerNamespace {
			t.Fatalf("HeartbeatMissed regards %s/%s, want the front's Lease in %s", e.Namespace, e.Name, v1alpha1.ControllerNamespace)
		}
	}

	// Its own duration: 60 s is stale at 61 s, not at 31 s.
	rg2 := newRig(t)
	rg2.create(frontLease(rg2.clock.Now(), 60))
	got = heartbeatTimes(t, rg2, kind, "front", rg2.clock.Now(), time.Second, 2*time.Minute, rg2.checkHeartbeats)
	if len(got) != 1 || got[0] != 61*time.Second {
		t.Fatalf("a 60 s Lease: fired at %v, want once at +61s", got)
	}
}

// statusWrite is one Instance status write on the shared sequence.
type statusWrite struct {
	seq         int
	name        string
	phase       v1alpha1.InstancePhase
	endReason   string
	unreachable bool
}

// sequencedRig is a rig whose recorder and Instance status writes number
// their calls from one Sequence (HBEP-8: the order of a recorder call against
// a status write is checked on one shared sequence).
func sequencedRig(t *testing.T, opts ...rigOption) (*rig, *[]statusWrite) {
	seq := &Sequence{}
	writes := &[]statusWrite{}
	opts = append(opts, withInterceptors(interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, o ...client.SubResourceUpdateOption) error {
		if err := c.SubResource(sub).Update(ctx, obj, o...); err != nil {
			return err
		}
		if in, ok := obj.(*v1alpha1.Instance); ok && sub == "status" {
			*writes = append(*writes, statusWrite{seq: seq.Next(), name: in.Name, phase: in.Status.Phase, endReason: in.Status.EndReason,
				unreachable: isTrue(in.Status.Conditions, v1alpha1.ConditionAgentUnreachable)})
		}
		return nil
	}}))
	rg := newRig(t, opts...)
	rg.rec.Seq = seq
	return rg, writes
}

// TestHeartbeatMissedAgentUnreachable: an agent that stops reporting makes its
// Instance AgentUnreachable True at its last report + 2 min, and one
// failing/HeartbeatMissed regarding the Instance follows that status write on
// the shared sequence, never before it. Silent for three hours, it fires at
// most hourly. Reports resuming clear it; a second silence fires at its start.
func TestHeartbeatMissedAgentUnreachable(t *testing.T) {
	rg, writes := sequencedRig(t)
	rg.create(mdSpec("md"))
	in, alloc := startReady(rg)
	const kind = "*v1alpha1.Instance"
	if n := len(heartbeatEvents(rg, kind, in.Name)); n != 0 {
		t.Fatalf("%d HeartbeatMissed events while the agent reports", n)
	}
	ag := rg.agentOf(alloc).agent
	last, _ := ag.LastReport()
	ag.Silence(true)
	got := heartbeatTimes(t, rg, kind, in.Name, last.Time, startTick, 3*time.Hour+2*time.Minute, func() { rg.Step() })
	checkHourly(t, got, 2*time.Minute)
	first := heartbeatEvents(rg, kind, in.Name)[0]
	turned := 0
	for _, w := range *writes {
		if w.name == in.Name && w.unreachable {
			turned = w.seq
			break
		}
	}
	if turned == 0 || turned > first.Seq {
		t.Fatalf("AgentUnreachable True written at seq %d, the first HeartbeatMissed at seq %d; the event follows the condition's write", turned, first.Seq)
	}
	if ph := rg.instance(in.Name).Status.Phase; ph != v1alpha1.InstanceReady {
		t.Fatalf("phase %s after three silent hours; nothing restarts on unreachability alone", ph)
	}

	ag.Silence(false)
	startStep(rg, "md", startTick)
	if isTrue(rg.instance(in.Name).Status.Conditions, v1alpha1.ConditionAgentUnreachable) {
		t.Fatal("AgentUnreachable still True after the reports resumed")
	}
	last, _ = ag.LastReport()
	ag.Silence(true)
	got = heartbeatTimes(t, rg, kind, in.Name, last.Time, startTick, 5*time.Minute, func() { rg.Step() })
	if len(got) != 1 || got[0] != 2*time.Minute {
		t.Fatalf("a second silence: fired at %v, want once at +2m", got)
	}
}

// heldKeys returns the repeat-gate keys with a prefix, sorted: the conditions
// the gate still remembers.
func heldKeys(rg *rig, prefix string) []string {
	mem := rg.r.memory()
	mem.mu.Lock()
	defer mem.mu.Unlock()
	var out []string
	for k := range mem.repeats {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

// TestHeartbeatKeysOfGoneObjectsAreForgotten: the repeat gate keeps a key per
// object whose heartbeat is stale, and only a check of that object clears it.
// The keys of a Provider deleted while stale, and of an Instance that left
// without passing through end(), would stay for the life of the process; a
// check that lists the objects successfully forgets the keys of those that are
// absent, keeps the others', and a list that fails forgets none.
func TestHeartbeatKeysOfGoneObjectsAreForgotten(t *testing.T) {
	failList := false
	rg := newRig(t, withInterceptors(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if failList {
			return errors.New("the list is down")
		}
		return c.List(ctx, list, opts...)
	}}))
	provider := func(name string) *v1alpha1.Provider {
		return &v1alpha1.Provider{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: rigProfile}, AllowedPartitions: []string{rigPartition}},
		}
	}
	rg.create(provider("gone-provider"))
	rg.create(provider("kept-provider"))
	rg.create(mdSpec("md"))
	in, alloc := startReady(rg)
	// A silent agent: its Instance goes AgentUnreachable and holds a key.
	rg.agentOf(alloc).agent.Silence(true)
	rg.until(10*time.Minute, startTick, "AgentUnreachable", func() bool {
		return isTrue(rg.instance(in.Name).Status.Conditions, v1alpha1.ConditionAgentUnreachable)
	})
	var gone, kept v1alpha1.Provider
	for name, p := range map[string]*v1alpha1.Provider{"gone-provider": &gone, "kept-provider": &kept} {
		if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: name}, p); err != nil {
			t.Fatal(err)
		}
	}
	// Past ProviderReadyStaleness for the two Providers that nothing reads;
	// the rig's own Provider is read, so it stays fresh and holds no key.
	rg.clock.Advance(6 * time.Minute)
	if err := rg.prov.Sync(rg.ctx); err != nil {
		t.Fatal(err)
	}
	rg.checkHeartbeats()
	wantProviders := []string{providerHeartbeatKey(&gone), providerHeartbeatKey(&kept)}
	slices.Sort(wantProviders)
	if got := heldKeys(rg, providerHeartbeatPrefix); !slices.Equal(got, wantProviders) {
		t.Fatalf("Provider keys %v after the stale check, want %v (the control: both stale Providers are held)", got, wantProviders)
	}
	instKey := instanceHeartbeatKey(rg.instance(in.Name))
	if got := heldKeys(rg, instanceHeartbeatPrefix); !slices.Equal(got, []string{instKey}) {
		t.Fatalf("Instance keys %v, want [%s] (the control: the unreachable Instance is held)", got, instKey)
	}

	// Both objects go without a pass over them: the Provider is deleted, and
	// the Instance loses its finalizers and is deleted.
	if err := rg.c.Delete(rg.ctx, &gone); err != nil {
		t.Fatal(err)
	}
	live := rg.instance(in.Name)
	live.Finalizers = nil
	if err := rg.c.Update(rg.ctx, live); err != nil {
		t.Fatal(err)
	}
	if err := rg.c.Delete(rg.ctx, live); err != nil {
		t.Fatal(err)
	}

	// A list that fails forgets nothing, and the check reports it.
	failList = true
	if err := rg.r.checkHeartbeats(rg.ctx); err == nil {
		t.Fatal("checkHeartbeats returned no error while every list fails")
	}
	failList = false
	if got := heldKeys(rg, providerHeartbeatPrefix); !slices.Equal(got, wantProviders) {
		t.Fatalf("Provider keys %v after a check whose lists failed, want %v untouched", got, wantProviders)
	}
	if got := heldKeys(rg, instanceHeartbeatPrefix); !slices.Equal(got, []string{instKey}) {
		t.Fatalf("Instance keys %v after a check whose lists failed, want [%s] untouched", got, instKey)
	}

	// A successful list forgets the keys of what is gone and keeps the other's.
	rg.checkHeartbeats()
	if got := heldKeys(rg, providerHeartbeatPrefix); !slices.Equal(got, []string{providerHeartbeatKey(&kept)}) {
		t.Fatalf("Provider keys %v after the check, want only the kept Provider's %s", got, providerHeartbeatKey(&kept))
	}
	if got := heldKeys(rg, instanceHeartbeatPrefix); len(got) != 0 {
		t.Fatalf("Instance keys %v after the Instance was deleted, want none", got)
	}
}
