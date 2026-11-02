package scenario_test

// The TunnelOrphaned check of a real teardown (steps c and g, and issue
// #193's end), judged once the controller has observably finished every read
// of frps's list that could raise one (review of PR #247), never after a
// fixed wait.

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// witnessName is the tunnel witness: an Instance no ModelDeployment owns.
const witnessName = "rehearsal-tunnel-witness"

// assertNoOrphans fails on any TunnelOrphaned event about the recorded
// proxies, read past two barriers.
//
// The controller raises TunnelOrphaned only in a read of frps's list
// (cmd/hpcburst-controller, tunnel_state.go: readOnce, then reportOrphans),
// for a proxy that read lists online with no current Instance. The plugin
// admits no proxy for an ended Instance (internal/frpsplugin, index.go), so a
// proxy that frps has listed offline never comes back: every event about it
// is raised by a read that fetched the list before the test saw it offline.
//
//   - The read barrier: the witness, an Instance with no owner and no
//     model-deployment label, which the lifecycle controller therefore never
//     reads, created after the last proxy end. Tunnel-state writes
//     TunnelConnected on every Instance that has not ended, from a read that
//     listed it (lifecycle.WriteTunnelConnected). Once the witness carries the
//     condition, a read that began after the witness existed has finished;
//     the reads run one at a time and each raises its events before it writes
//     a condition, so every read that could raise an event about the proxies
//     has raised it.
//   - The delivery barrier: an event this test writes after that, the marker;
//     the events are judged from a list that holds it, so no event the API
//     server had stored before the marker is missed.
//
// What no API object shows is the controller's own queue: client-go's recorder
// posts an event a moment after it is raised, so an event raised just before
// the witness's condition and still queued when the list is read would be
// missed. That residual is the recorder's, not a wait of this test's.
func (r *rig) assertNoOrphans(steps string) {
	r.t.Helper()
	var last time.Time
	for _, end := range r.proxyEnds {
		if end.After(last) {
			last = end
		}
	}
	if last.IsZero() {
		r.t.Fatal("no proxy end was recorded: the TunnelOrphaned check would be vacuous")
	}
	online := r.frpsProxies()
	for uid := range r.proxyEnds {
		if online[string(uid)] {
			r.t.Errorf("frps lists the proxy %s online again after it was seen offline", uid)
		}
	}
	w, created, seen := r.tunnelWitness()
	marker := r.markerEvent(w)
	evs := r.eventsThrough(marker)
	var orphans []eventsv1.Event
	for _, e := range evs {
		if e.Reason == v1alpha1.EventReasonTunnelOrphaned {
			orphans = append(orphans, e)
			r.t.Logf("FINDING step (%s): %s regarding %s %s: %.300s", steps, e.Reason, e.Regarding.Kind, e.Regarding.Name, e.Note)
		}
	}
	r.t.Logf("TunnelOrphaned check (%s): the last of %d proxies seen offline at %s; the witness %s created %s and given TunnelConnected by a read by %s; %d events listed through the marker %s, %d TunnelOrphaned",
		steps, len(r.proxyEnds), ts(last), w.Name, ts(created), ts(seen), len(evs), marker, len(orphans))
	if len(orphans) > 0 {
		r.t.Errorf("%d TunnelOrphaned events across steps (%s): a real teardown left a proxy online with no current Instance", len(orphans), steps)
	}
	r.deleteWitness(w)
}

// tunnelWitness creates the witness, with the spec of the first Instance the
// watch saw and no owner, labels or status, and waits until tunnel-state has
// written its TunnelConnected condition. It returns the witness, when it was
// created, and when the condition was first seen.
func (r *rig) tunnelWitness() (*v1alpha1.Instance, time.Time, time.Time) {
	r.t.Helper()
	r.watch.mu.Lock()
	spec := r.watch.firstSpec
	r.watch.mu.Unlock()
	if spec == nil {
		r.t.Fatal("the watch saw no Instance: the tunnel witness has no spec to copy")
	}
	w := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: witnessName, Namespace: namespace}, Spec: *spec.DeepCopy()}
	ctx, cancel := callCtx(r.t)
	err := r.c.Create(ctx, w)
	cancel()
	if err != nil {
		r.t.Fatalf("create the tunnel witness: %v", err)
	}
	created := time.Now()
	// A test that fails before the check deletes it while the API serves.
	r.t.Cleanup(func() {
		if !r.h.downed {
			r.deleteWitness(w)
		}
	})
	var seen time.Time
	waitState(r.t, waitDeadline, "the controller's tunnel-state to write TunnelConnected on the witness "+w.Name, func() (bool, string) {
		var cur v1alpha1.Instance
		if !r.get(&cur, w.Name, namespace) {
			return false, "the witness cannot be read"
		}
		if c := meta.FindStatusCondition(cur.Status.Conditions, v1alpha1.ConditionTunnelConnected); c != nil {
			seen = time.Now()
			return true, ""
		}
		return false, fmt.Sprintf("the witness has conditions %v and phase %q", cur.Status.Conditions, cur.Status.Phase)
	})
	return w, created, seen
}

// deleteWitness deletes the witness and waits until it is gone; a witness
// already gone is no error.
func (r *rig) deleteWitness(w *v1alpha1.Instance) {
	r.t.Helper()
	ctx, cancel := callCtx(r.t)
	err := r.c.Delete(ctx, &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: w.Name, Namespace: namespace}})
	cancel()
	if err != nil && !apierrors.IsNotFound(err) {
		r.t.Errorf("delete the tunnel witness: %v", err)
		return
	}
	waitFor(r.t, waitDeadline, "the tunnel witness to be gone", func() bool {
		var cur v1alpha1.Instance
		return !r.get(&cur, w.Name, namespace)
	})
}

// markerEvent writes the delivery barrier's event, regarding the witness, and
// returns its name.
func (r *rig) markerEvent(w *v1alpha1.Instance) string {
	r.t.Helper()
	name := fmt.Sprintf("rehearsal-marker-%d", time.Now().UnixNano())
	e := &eventsv1.Event{
		ObjectMeta:          metav1.ObjectMeta{Name: name, Namespace: namespace},
		EventTime:           metav1.NewMicroTime(time.Now()),
		ReportingController: "rehearsal-test",
		ReportingInstance:   "rehearsal-test",
		Action:              "marker",
		Reason:              "RehearsalMarker",
		Type:                provider.EventKindOrdinary,
		Regarding:           corev1.ObjectReference{APIVersion: v1alpha1.GroupVersion.String(), Kind: "Instance", Namespace: namespace, Name: w.Name, UID: w.UID},
		Note:                "the rehearsal's delivery barrier: the TunnelOrphaned check reads a list that holds this event",
	}
	ctx, cancel := callCtx(r.t)
	defer cancel()
	if err := r.c.Create(ctx, e); err != nil {
		r.t.Fatalf("write the marker event: %v", err)
	}
	return name
}

// eventsThrough lists the events.k8s.io events until the list holds the
// marker, and returns that list.
func (r *rig) eventsThrough(marker string) []eventsv1.Event {
	r.t.Helper()
	var out []eventsv1.Event
	waitState(r.t, waitDeadline, "an event list that holds the marker "+marker, func() (bool, string) {
		var l eventsv1.EventList
		ctx, cancel := callCtx(r.t)
		defer cancel()
		if err := r.c.List(ctx, &l); err != nil {
			return false, err.Error()
		}
		for _, e := range l.Items {
			if e.Name == marker {
				out = l.Items
				return true, ""
			}
		}
		return false, fmt.Sprintf("%d events listed, not the marker", len(l.Items))
	})
	return out
}
