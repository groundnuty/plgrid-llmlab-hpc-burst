package scenario_test

// The TunnelOrphaned check of a real teardown (steps c and g, and issue
// #193's end), proven on the lifecycle controller's raised-events counter by
// HBEP-8's four conditions (absence_test.go; #256), never after a fixed wait
// and never from a list of Events, which the recorder fills asynchronously.

import (
	"fmt"
	"time"

	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
)

// witnessName is the tunnel witness: an Instance no ModelDeployment owns.
const witnessName = "rehearsal-tunnel-witness"

// orphanQuiet is condition 4 for TunnelOrphaned: one loop raises it.
const orphanQuiet = "TunnelOrphaned has one source, the tunnel-state reader (cmd/hpcburst-controller, tunnel_state.go: readOnce, then reportOrphans), " +
	"which raises it for any proxy frps lists online with no current Instance, whatever the Instance"

// assertNoOrphans proves that no TunnelOrphaned event was raised across the
// steps since r.orphanBase, the first read, which a test takes once the
// controller it covers serves and before any Instance ends.
//
// The controller raises TunnelOrphaned only in a read of frps's list for a
// proxy that read lists online with no current Instance, once that has lasted
// tunnelOrphanGrace. The plugin admits no proxy for an ended Instance
// (internal/frpsplugin, index.go), so a proxy frps has listed offline never
// comes back.
//
//   - Condition 1: every recorded proxy end is after the first read, and at the
//     first read frps listed no proxy online for an Instance that had ended.
//   - Condition 3, the barrier: the witness, an Instance with no owner and no
//     model-deployment label, which the lifecycle controller never reads, is
//     created once every proxy has been seen offline and every recorded
//     Instance has been ended for tunnelOrphanGrace: after any TunnelOrphaned
//     was due. Tunnel-state writes TunnelConnected on every Instance that has
//     not ended, from a read that listed it (lifecycle.WriteTunnelConnected),
//     and raises a read's events before it writes its conditions. A read that
//     listed the witness read frps after the witness existed, so once the
//     witness carries the condition such a read has finished emitting; the
//     reads run one at a time, so every earlier read has too.
func (r *rig) assertNoOrphans(steps string) {
	r.t.Helper()
	base := r.orphanBase
	if base == nil {
		r.t.Fatal("the TunnelOrphaned check has no first read taken before the steps that end Instances: it would be inconclusive")
	}
	var last, due time.Time
	var unmet []string
	if len(base.orphanable) > 0 {
		unmet = append(unmet, fmt.Sprintf("condition 1: at the first read frps listed proxies online for ended or gone Instances %v", base.orphanable))
	}
	for uid, end := range r.proxyEnds {
		if !end.After(base.change) {
			unmet = append(unmet, fmt.Sprintf("condition 1: the proxy %s was seen offline at %s, not after the first read at %s", uid, ts(end), ts(base.change)))
		}
		if end.After(last) {
			last = end
		}
		ended, ok := r.instanceEnded(uid)
		if !ok || ended.After(end) {
			ended = end // the Instance ended before its proxy went offline
		}
		if d := ended.Add(tunnelOrphanGrace); d.After(due) {
			due = d
		}
	}
	if last.IsZero() {
		r.t.Fatal("no proxy end was recorded: the TunnelOrphaned check would be vacuous")
	}
	if last.After(due) {
		due = last
	}
	online := r.frpsProxies()
	for uid := range r.proxyEnds {
		if online[string(uid)] {
			r.t.Errorf("frps lists the proxy %s online again after it was seen offline", uid)
		}
	}
	waitFor(r.t, waitDeadline, "the time any TunnelOrphaned would have been due, "+ts(due), func() bool { return time.Now().After(due) })
	w, created, seen := r.tunnelWitness()
	barrier := eventcount.Barrier{
		What:      "a tunnel-state read that listed the witness " + w.Name + " and wrote its TunnelConnected",
		PassStart: created, Observed: seen, Due: due,
	}
	r.t.Logf("TunnelOrphaned check (%s): the last of %d proxies seen offline at %s, due by %s; the witness %s created %s and given TunnelConnected by a read by %s",
		steps, len(r.proxyEnds), ts(last), ts(due), w.Name, ts(created), ts(seen))
	if v := r.judgeAbsence(base, keyTunnelOrphaned, base.change, unmet, []eventcount.Barrier{barrier}, orphanQuiet); v == eventcount.Raised {
		r.logOrphanEvents(steps)
	}
	r.deleteWitness(w)
}

// logOrphanEvents lists the TunnelOrphaned events the API holds, for the
// report of a raised one; it proves nothing about absence.
func (r *rig) logOrphanEvents(steps string) {
	r.t.Helper()
	var l eventsv1.EventList
	ctx, cancel := callCtx(r.t)
	defer cancel()
	if err := r.c.List(ctx, &l); err != nil {
		r.t.Logf("list events for the TunnelOrphaned finding: %v", err)
		return
	}
	for _, e := range l.Items {
		if e.Reason == v1alpha1.EventReasonTunnelOrphaned {
			r.t.Logf("FINDING step (%s): %s regarding %s %s, raised %s, stored %s: %.300s", steps, e.Reason, e.Regarding.Kind, e.Regarding.Name,
				ts(e.EventTime.Time), e.CreationTimestamp.UTC().Format(time.RFC3339), e.Note)
		}
	}
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
