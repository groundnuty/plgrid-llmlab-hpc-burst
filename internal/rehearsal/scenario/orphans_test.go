package scenario_test

// The TunnelOrphaned check of a real teardown (steps c and g, and issue
// #193's end), proven on the lifecycle controller's raised-events counter by
// HBEP-8's four conditions (absence_test.go; #256), never after a fixed wait
// and never from a list of Events, which the recorder fills asynchronously.

import (
	"fmt"
	"sort"
	"time"

	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
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
//   - Condition 1: every recorded proxy's Instance had not ended at the first
//     read (orphanFirstReadProblems), every recorded proxy end is after the
//     first read, and at the first read frps listed no proxy online for an
//     Instance that had ended.
//   - Condition 3, the barrier: the witness, an Instance with no owner and no
//     model-deployment label, which the lifecycle controller never reads, is
//     created once every proxy has been seen offline and every recorded
//     Instance has been ended for tunnelOrphanGrace: after any TunnelOrphaned
//     was due. Tunnel-state writes TunnelConnected on every Instance that has
//     not ended, from a read that listed it (lifecycle.WriteTunnelConnected),
//     and raises a read's events before it writes its conditions. A read that
//     listed the witness read frps after the witness existed, so once the
//     witness carries a condition of a successful read (fromSuccessfulRead)
//     such a read has judged every proxy and finished emitting; the reads run
//     one at a time, so every earlier read has too. A read of frps that failed
//     writes Unknown/ProxyListUnreadable and judges no proxy, so that
//     condition is no barrier.
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
	unmet = append(unmet, orphanFirstReadProblems(r.proxyEnds, base.live, r.watch.createdAt, base.first)...)
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
	stack.WaitFor(r.t, stack.WaitDeadline, "the time any TunnelOrphaned would have been due, "+ts(due), func() bool { return time.Now().After(due) })
	w, created, cond, seen := r.tunnelWitness()
	barrier := witnessBarrier(w.Name, created, cond, seen, due)
	r.t.Logf("TunnelOrphaned check (%s): the last of %d proxies seen offline at %s, due by %s; the witness %s created %s and given TunnelConnected %s by a read by %s",
		steps, len(r.proxyEnds), ts(last), ts(due), w.Name, ts(created), conditionString(cond), ts(seen))
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
	ctx, cancel := stack.CallCtx(r.t)
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

// orphanFirstReadProblems is the part of condition 1 that the recorded proxy
// ends carry: the Instance of each recorded proxy had not ended at the first
// read, answered at first. An Instance in live, the Instances not ended in a
// list taken just after the first read, had not ended at it either, since an
// end is final. One that list lacks counts only when its creationTimestamp
// (cut to the second, so never later than the creation) is after the first
// read: it did not exist then. Any other may have ended before the first read,
// with its proxy online past tunnelOrphanGrace and then offline before the
// list of frps's proxies, so a TunnelOrphaned raised for it would be in both
// counts.
func orphanFirstReadProblems(ends map[types.UID]time.Time, live map[string]bool, created func(types.UID) (time.Time, bool), first time.Time) []string {
	var out []string
	for uid := range ends {
		if live[string(uid)] {
			continue
		}
		c, ok := created(uid)
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("condition 1: the Instance %s was not live at the first read (%s), and its creation time is unknown", uid, ts(first)))
		case !c.After(first):
			out = append(out, fmt.Sprintf("condition 1: the Instance %s was not live at the first read (%s), and was created at %s, before it: it may have ended before the first read", uid, ts(first), ts(c)))
		}
	}
	sort.Strings(out)
	return out
}

// fromSuccessfulRead reports whether a TunnelConnected condition was written
// from a successful read of frps's list: True with ProxyOnline, or False with
// ProxyOffline (frpsplugin.Reader). A read that failed writes Unknown with
// ProxyListUnreadable, and reportOrphans returns at once on it, judging no
// proxy; any other condition is not one tunnel-state writes from a read, so it
// is no evidence either.
func fromSuccessfulRead(c *metav1.Condition) bool {
	if c == nil {
		return false
	}
	return c.Status == metav1.ConditionTrue && c.Reason == v1alpha1.ReasonProxyOnline ||
		c.Status == metav1.ConditionFalse && c.Reason == v1alpha1.ReasonProxyOffline
}

// witnessBarrier is condition 3's evidence from the witness, created at
// created and seen with the condition c at seen: a pass only when c comes from
// a successful read; otherwise it has no pass, and the check is inconclusive.
func witnessBarrier(name string, created time.Time, c *metav1.Condition, seen, due time.Time) eventcount.Barrier {
	b := eventcount.Barrier{
		What:      "a tunnel-state read that listed the witness " + name + " and wrote its TunnelConnected from a successful read of frps",
		PassStart: created, Due: due,
	}
	if fromSuccessfulRead(c) {
		b.Observed = seen
	} else {
		b.What += ", which it does not carry (" + conditionString(c) + ")"
	}
	return b
}

// conditionString is a condition's status and reason, for a report.
func conditionString(c *metav1.Condition) string {
	if c == nil {
		return "none"
	}
	return string(c.Status) + "/" + c.Reason
}

// tunnelWitness creates the witness, with the spec of the first Instance the
// watch saw and no owner, labels or status, and waits until tunnel-state has
// written its TunnelConnected condition from a successful read of frps. It
// returns the witness, when it was created, the last condition seen, and when
// a successful read's condition was first seen: zero when none came within
// waitDeadline, which makes the check inconclusive (witnessBarrier).
func (r *rig) tunnelWitness() (*v1alpha1.Instance, time.Time, *metav1.Condition, time.Time) {
	r.t.Helper()
	r.watch.mu.Lock()
	spec := r.watch.firstSpec
	r.watch.mu.Unlock()
	if spec == nil {
		r.t.Fatal("the watch saw no Instance: the tunnel witness has no spec to copy")
	}
	w := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: witnessName, Namespace: stack.Namespace}, Spec: *spec.DeepCopy()}
	ctx, cancel := stack.CallCtx(r.t)
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
	// A failed read's Unknown is written once and stays until a successful
	// read changes it, so the wait goes on past it.
	deadline, cut := stack.BoundedDeadline(r.t, stack.WaitDeadline)
	var last *metav1.Condition
	for {
		var cur v1alpha1.Instance
		if r.get(&cur, w.Name, stack.Namespace) {
			if c := meta.FindStatusCondition(cur.Status.Conditions, v1alpha1.ConditionTunnelConnected); c != nil {
				last = c.DeepCopy()
				if fromSuccessfulRead(c) {
					return w, created, last, time.Now()
				}
			}
		}
		if time.Now().After(deadline) {
			if cut {
				r.t.Fatalf("gave up waiting for a successful read's TunnelConnected on the witness %s %s before the test's deadline, so that its clean-up runs", w.Name, stack.CleanupMargin)
			}
			r.t.Logf("no successful read of frps wrote TunnelConnected on the witness %s within %s (last %s)", w.Name, stack.WaitDeadline, conditionString(last))
			return w, created, last, time.Time{}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// deleteWitness deletes the witness and waits until it is gone; a witness
// already gone is no error.
func (r *rig) deleteWitness(w *v1alpha1.Instance) {
	r.t.Helper()
	ctx, cancel := stack.CallCtx(r.t)
	err := r.c.Delete(ctx, &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: w.Name, Namespace: stack.Namespace}})
	cancel()
	if err != nil && !apierrors.IsNotFound(err) {
		r.t.Errorf("delete the tunnel witness: %v", err)
		return
	}
	stack.WaitFor(r.t, stack.WaitDeadline, "the tunnel witness to be gone", func() bool {
		var cur v1alpha1.Instance
		return !r.get(&cur, w.Name, stack.Namespace)
	})
}
