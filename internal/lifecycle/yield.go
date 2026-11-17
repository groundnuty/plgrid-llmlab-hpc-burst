package lifecycle

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// A pending extra yields its submit slot (beta; HBEP-7, Over-allocation and qualification (beta), Its checks, "a
// pending extra yields its slot": the science agent's ruling on #616's PR 4, 17 Nov 2026, amendment #674).
//
// When a missing primary, of any deployment, is refused on a Provider only for its submit limit, the newest pending
// extra there is cancelled, so the primary submits in that slot. An extra is insurance, and a primary that can't be
// submitted at all is serving capacity short; a pending extra costs only its queue age, and cancelling it is
// over-allocation's own act, with no approval. A started extra is never cancelled for this.
//
// The deployment B whose primary is refused decides, at its slot reservation, under the Provider's lock (slotAsk.onLimit):
// it marks the newest pending surplus Instance there that no other primary's mark names, by the annotation
// annotationYieldTo, "<ns>/<B> <RFC3339 time of the marking pass>" and the label labelYieldMark, and the Instance watch
// wakes the owner. B deletes nothing of another deployment's. It marks only while its shortage (shortageOf: the
// primaries it lacks by Allocation presence, which counts a cut-short Instance as missing) exceeds its marks
// outstanding on every Provider, and only when the cancel could admit it (the count that refused is no more than the
// limit). The owner's own step 7 acts (settleYields, then scaleUnits): it re-reads B and cancels the Instance as
// over-allocation's trim, with the release cause "yielded its submit slot to <B>", or clears the mark. B keeps its marks
// honest (clearYieldMarks), on every Provider, including one it no longer lists: it clears the marks its pass no longer
// supports, the oldest extras' first.
//
// What the owner reads, and why it is not B's Degraded: SubmitLimitReached is the ninth of degradedOrder, so
// VerificationFailed, RestartLoop, SubmissionRejected, NoOverlapSlot and others mask it, and an owner that cleared
// a mark on any other top reason would have B re-mark on every pass, a loop that starves B's primary. The owner reads
// B's state and the mark's survival instead: B's status.lastReconcileTime, which the marking pass writes at the
// time the mark carries, and its observedGeneration, so a B whose spec changed since its last pass is waited for.

// labelYieldMark is on every Instance that carries annotationYieldTo, with the value "true", so the marks naming a
// deployment can be listed in one List across every Provider (the yield protocol's own, internal to this package and
// free to change: docs/operations.md says so; #708's fix round, Y2).
const labelYieldMark = v1alpha1.Group + "/yield-mark"

// annotationYieldTo is on a pending surplus Instance that yields its submit slot: "<ns>/<deployment> <RFC3339>", the
// deployment whose missing primary was refused for the Provider's submit limit alone and the time of the marking
// pass (the time B's status.lastReconcileTime then carries). A lifecycle-owned annotation, proposed for
// api/v1alpha1/constants.go beside annotationRankedOut.
const annotationYieldTo = v1alpha1.Group + "/yield-to"

// yieldRecheck is how soon a pass that waits on a deployment's next pass asks to run again.
const yieldRecheck = 10 * time.Second

// yieldValue is the mark's value.
func yieldValue(deployment string, at time.Time) string {
	return deployment + " " + at.UTC().Format(time.RFC3339)
}

// parseYield reads a mark: the deployment (namespace/name) and the time. ok is false for a malformed one.
func parseYield(v string) (deployment string, at time.Time, ok bool) {
	d, ts, found := strings.Cut(v, " ")
	if !found || !strings.Contains(d, "/") {
		return "", time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return "", time.Time{}, false
	}
	return d, t, true
}

// yieldsTo is the deployment an Instance's mark names, or "".
func yieldsTo(in *v1alpha1.Instance) string {
	d, _, ok := parseYield(in.Annotations[annotationYieldTo])
	if !ok {
		return ""
	}
	return d
}

// deploymentKeyOf is this pass's deployment, namespace/name.
func (p *pass) deploymentKeyOf() string { return p.key.Namespace + "/" + p.key.Name }

// shortageOf is how many primaries a deployment lacks, counted by ALLOCATION PRESENCE and never by the selection's
// choice (Y1): its desired count less the units, one per hedge, that countable Instances make and that HAVE an
// Allocation. A cut-short Instance (made, its Allocation's create failed, retried in ensureAllocation) adds nothing,
// so it counts as missing; and the selection's tie between same-second pending Instances (select.go: the UID) can
// never turn a missing primary into "chosen". The pass, its clearing of marks, the owner's re-check of the deployment
// that asked and the extra's classification in ensureAllocation all read this one function.
func shortageOf(sel selection, desired int) int {
	has := 0
	for _, u := range sel.units() {
		for _, m := range u.members {
			if sel.allocOf(m) != nil {
				has++
				break
			}
		}
	}
	return max(desired-has, 0)
}

// shortage is shortageOf this pass's Instances and Allocations.
func (p *pass) shortage() int { return shortageOf(p.selection(nil), p.desiredCount()) }

// limitRefusal is what a pass saw when a missing primary was refused on a Provider for its submit limit alone: the
// count that refused it (listed + expected + reserved) and the limit.
type limitRefusal struct{ total, limit int }

// yieldMarkOf is one of this deployment's marks: the Instance that carries it, its Provider and whether it is
// still live (countable) or a cancelled extra whose Allocation is being deleted and still holds the slot.
type yieldMarkOf struct {
	in       *v1alpha1.Instance
	provider string
	live     bool
}

// marksOfMine reads, through the uncached Reader and with ONE List by the mark's label (every namespace, every
// Provider), the marks that name this deployment and are OUTSTANDING: their Allocation still holds a slot and either
// the Instance is live or the Allocation is being deleted (a cancelled extra's mark counts until its Allocation's
// deletion completes, as the slot its primary is waiting for; an extra that ended for another reason and keeps its
// Allocation counts for nothing).
func (p *pass) marksOfMine(ctx context.Context) ([]yieldMarkOf, error) {
	var il v1alpha1.InstanceList
	if err := p.r.Reader.List(ctx, &il, client.MatchingLabels{labelYieldMark: "true"}); err != nil {
		return nil, fmt.Errorf("list the yield marks: %w", err)
	}
	me := p.deploymentKeyOf()
	var out []yieldMarkOf
	for i := range il.Items {
		in := &il.Items[i]
		if yieldsTo(in) != me {
			continue
		}
		var a v1alpha1.Allocation
		switch err := p.r.Reader.Get(ctx, client.ObjectKey{Namespace: in.Namespace, Name: in.Spec.AllocationRef.Name}, &a); {
		case apierrors.IsNotFound(err):
			continue
		case err != nil:
			return nil, fmt.Errorf("read the Allocation of yield mark %s: %w", in.Name, err)
		}
		if !holdsSlot(&a) || !(countable(in) || !a.DeletionTimestamp.IsZero()) {
			continue
		}
		out = append(out, yieldMarkOf{in: in, provider: in.Labels[v1alpha1.LabelProvider], live: countable(in)})
	}
	return out, nil
}

// newestMarkFirst orders marks newest first, by the Instance's creation time, then by name: the marks left after a
// clearing are on the newest pending extras, as #674 has the newest pending extra yield.
func newestMarkFirst(ms []yieldMarkOf) {
	sort.SliceStable(ms, func(i, j int) bool {
		a, b := ms[i].in, ms[j].in
		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			return b.CreationTimestamp.Before(&a.CreationTimestamp)
		}
		return a.Name < b.Name
	})
}

// yieldOnLimit is the callback of a missing primary's reservation (slotAsk.onLimit), called under the Provider's
// lock when the reservation is refused for the submit limit alone, by the submit step and by ensureAllocation's retry
// of a cut-short primary: it records the refusal for this pass (the count that refused and the limit), and marks one
// more pending extra on the Provider when
//   - the cancel could admit this primary (Y3): the count that refused, which holds the marked extras still, is no
//     more than the limit. Each slot one of this deployment's earlier marks frees is taken by another of its refused
//     primaries, so its own marks net to zero, and this primary needs one more: it is never `mine` that is
//     subtracted. Other deployments' marks free slots for them;
//   - and this deployment's outstanding marks on EVERY Provider, plus those made earlier in this pass (deduplicated by
//     UID, since the Reader may lag), are fewer than its shortage (Y1, Y2).
//
// rd carries the pending surplus Instances of every deployment on the Provider, newest first (D8's views).
func (p *pass) yieldOnLimit(pv *providerView) func(ctx context.Context, rd slotReads) error {
	return func(ctx context.Context, rd slotReads) error {
		limit := int(*pv.limits.MaxSubmitPerUser)
		if p.limitRefused == nil {
			p.limitRefused = map[string]limitRefusal{}
		}
		p.limitRefused[pv.name] = limitRefusal{total: rd.total, limit: limit}
		if rd.total > limit {
			return nil // no cancel can admit this primary: the count would still be over the limit
		}
		marks, err := p.marksOfMine(ctx)
		if err != nil {
			return err
		}
		count := map[types.UID]bool{}
		for _, m := range marks {
			count[m.in.UID] = true
		}
		for uid := range p.marked {
			count[uid] = true
		}
		if len(count) >= p.shortage() {
			p.at(p.now.Add(yieldRecheck)) // its marks are outstanding: the deployment is asked again
			return nil
		}
		me := p.deploymentKeyOf()
		for _, in := range rd.pendingSurplus {
			if in.Annotations[annotationYieldTo] != "" {
				continue // another deployment's primary is already served by this one
			}
			if err := p.patchYield(ctx, in, yieldValue(me, p.now)); err != nil {
				return err
			}
			if p.marked == nil {
				p.marked = map[types.UID]bool{}
			}
			p.marked[in.UID] = true
			p.at(p.now.Add(yieldRecheck))
			return nil
		}
		return nil // no pending extra there to yield: the primary stays refused
	}
}

// patchYield sets (annotation and label) or, with "", clears (both) the mark on the Instance, by one merge patch.
func (p *pass) patchYield(ctx context.Context, in *v1alpha1.Instance, value string) error {
	orig := in.DeepCopy()
	if value == "" {
		delete(in.Annotations, annotationYieldTo)
		delete(in.Labels, labelYieldMark)
	} else {
		if in.Annotations == nil {
			in.Annotations = map[string]string{}
		}
		if in.Labels == nil {
			in.Labels = map[string]string{}
		}
		in.Annotations[annotationYieldTo] = value
		in.Labels[labelYieldMark] = "true"
	}
	if err := p.r.Client.Patch(ctx, in, client.MergeFrom(orig), owner); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("patch the yield mark of Instance %s: %w", in.Name, err)
	}
	return nil
}

// clearYieldMarks is this deployment's own housekeeping, after its placements: it clears the marks naming it that this
// pass's refusals no longer support, on EVERY Provider, including one it no longer lists (Y4). A mark goes when
//   - this pass saw no limit-alone refusal on its Provider: the deployment placed, or is not trying (paused, gated,
//     scaled down), or no longer lists the Provider. A refusal in ensureAllocation counts;
//   - on a Provider where it was refused, it holds more marks than the cancels can admit: the live marks left there are
//     the live ones less the count's excess over the limit (a cancelled extra's mark counts on both sides and is never
//     cleared: its Allocation still holds the slot);
//   - across all Providers, it holds more live marks than its shortage less the cancelled ones.
//
// The marks cleared are the OLDEST extras', so the marks left are on the NEWEST pending extras. Each clear is made under
// its mark's Provider's lock, like the marking, and re-reads the Instance there. It asks to run again while a mark of
// its own stays.
func (p *pass) clearYieldMarks() error {
	marks, err := p.marksOfMine(p.ctx)
	if err != nil {
		return err
	}
	committed := 0
	byProvider := map[string][]yieldMarkOf{}
	for _, m := range marks {
		if m.live {
			byProvider[m.provider] = append(byProvider[m.provider], m)
		} else {
			committed++
		}
	}
	var keep []yieldMarkOf
	for prov, ms := range byProvider {
		newestMarkFirst(ms)
		r, refused := p.limitRefused[prov]
		n := 0
		if refused {
			n = min(max(len(ms)-max(r.total-r.limit, 0), 0), len(ms))
		}
		keep = append(keep, ms[:n]...)
		if err := p.clearMarks(prov, ms[n:]); err != nil {
			return err
		}
	}
	newestMarkFirst(keep)
	room := max(p.shortage()-committed, 0)
	if len(keep) > room {
		byProv := map[string][]yieldMarkOf{}
		for _, m := range keep[room:] {
			byProv[m.provider] = append(byProv[m.provider], m)
		}
		for prov, ms := range byProv {
			if err := p.clearMarks(prov, ms); err != nil {
				return err
			}
		}
		keep = keep[:room]
	}
	if len(keep) > 0 {
		p.at(p.now.Add(yieldRecheck))
	}
	return nil
}

// clearMarks clears the marks on one Provider's Instances under that Provider's lock, re-reading each Instance there:
// one that is no longer live, or no longer names this deployment, is left alone.
func (p *pass) clearMarks(provider string, ms []yieldMarkOf) error {
	if len(ms) == 0 {
		return nil
	}
	me := p.deploymentKeyOf()
	return p.r.slots().underLock(p.ctx, provider, func(ctx context.Context) error {
		for _, m := range ms {
			var cur v1alpha1.Instance
			switch err := p.r.Reader.Get(ctx, client.ObjectKeyFromObject(m.in), &cur); {
			case apierrors.IsNotFound(err):
				continue
			case err != nil:
				return fmt.Errorf("read Instance %s to clear its yield mark: %w", m.in.Name, err)
			}
			if yieldsTo(&cur) != me || !countable(&cur) {
				continue
			}
			if err := p.patchYield(ctx, &cur, ""); err != nil {
				return err
			}
		}
		return nil
	})
}

// settleYields is the owner's step 7 for the yield marks on its own Instances, before it cancels any
// (scaleUnits reads p.yielding). For each pending surplus Instance that carries a mark:
//   - its job started, it is no longer surplus, the mark is malformed, or the deployment it names is gone, being
//     deleted, past its end time, scaled to zero, or has no shortage any more (shortageOf: it got a slot elsewhere): the
//     mark is cleared and nothing is cancelled; a started extra is never cancelled for this;
//   - the named deployment's status.lastReconcileTime is no later than the mark's time (no later pass of it has run
//     since the mark: the marking pass writes its own time there), or its status.observedGeneration lags its
//     metadata.generation (its spec changed since its last pass, a Provider dropped or a count lowered, and only its
//     next pass re-evaluates the mark): WAIT, the mark stays, nothing is cancelled, and the pass asks to run again;
//   - otherwise a later pass of the named deployment, at its current spec, was refused again for the limit alone and
//     kept the mark (it clears the marks of a pass that was not): the Instance yields, and is cancelled as
//     over-allocation's trim.
func (p *pass) settleYields() error {
	p.yielding = nil
	_, surplus := selectUnits(p.selection(nil), p.desiredCount())
	isSurplus := map[*v1alpha1.Instance]bool{}
	for _, u := range surplus {
		for _, m := range u.members {
			isSurplus[m] = true
		}
	}
	for _, in := range p.insts {
		v := in.Annotations[annotationYieldTo]
		if v == "" {
			continue
		}
		if !countable(in) {
			// A cancelled extra's tombstone keeps its mark while its Allocation is being deleted: that Allocation still
			// holds the slot the named deployment's primary waits for, and the guard counts the mark (marksOfMine).
			// Cleared here, a second extra would be marked for the same primary.
			if a := p.allocOf(in); a != nil && !a.DeletionTimestamp.IsZero() {
				continue
			}
			if err := p.patchYield(p.ctx, in, ""); err != nil {
				return err
			}
			continue
		}
		b, at, ok := parseYield(v)
		if !ok || !isSurplus[in] || p.stale[in.UID] || jobStarted(p.allocOf(in)) {
			if err := p.patchYield(p.ctx, in, ""); err != nil {
				return err
			}
			continue
		}
		st, err := p.needsSlot(b)
		if err != nil {
			return err
		}
		switch {
		case !st.needs:
			if err := p.patchYield(p.ctx, in, ""); err != nil {
				return err
			}
		case st.last == nil || !st.last.After(at) || st.lags:
			p.at(p.now.Add(yieldRecheck))
		default:
			if p.yielding == nil {
				p.yielding = map[types.UID]string{}
			}
			p.yielding[in.UID] = b
		}
	}
	return nil
}

// slotNeed is what the owner reads of the deployment a mark names.
type slotNeed struct {
	needs bool       // it still lacks a primary by Allocation presence (shortageOf), and exists, serves and is not ending
	last  *time.Time // its status.lastReconcileTime
	lags  bool       // its status.observedGeneration is not its metadata.generation: its next pass has yet to see its spec
}

// needsSlot reads the named deployment: whether it still lacks a primary (it exists, is not being deleted, is not past its
// end time, and shortageOf its Instances and Allocations is above zero), its status.lastReconcileTime, and whether its
// last pass saw its current spec.
func (p *pass) needsSlot(deployment string) (slotNeed, error) {
	ns, name, _ := strings.Cut(deployment, "/")
	var md v1alpha1.ModelDeployment
	switch err := p.r.Reader.Get(p.ctx, client.ObjectKey{Namespace: ns, Name: name}, &md); {
	case apierrors.IsNotFound(err):
		return slotNeed{}, nil
	case err != nil:
		return slotNeed{}, fmt.Errorf("read ModelDeployment %s for a yield mark: %w", deployment, err)
	}
	if !md.DeletionTimestamp.IsZero() || md.Spec.Source.Served == nil || md.Spec.Instances == 0 ||
		(md.Spec.EndTime != nil && !p.now.Before(md.Spec.EndTime.Time)) {
		return slotNeed{}, nil // gone, being deleted, scaled to zero or past its end time: its pass makes no primary
	}
	v := deploymentView{key: deployment, allocs: map[string]*v1alpha1.Allocation{}}
	var il v1alpha1.InstanceList
	if err := p.r.Reader.List(p.ctx, &il, client.InNamespace(ns), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(name)}); err != nil {
		return slotNeed{}, fmt.Errorf("list Instances of %s for a yield mark: %w", deployment, err)
	}
	for i := range il.Items {
		v.insts = append(v.insts, &il.Items[i])
	}
	var al v1alpha1.AllocationList
	if err := p.r.Reader.List(p.ctx, &al, client.InNamespace(ns), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(name)}); err != nil {
		return slotNeed{}, fmt.Errorf("list Allocations of %s for a yield mark: %w", deployment, err)
	}
	for i := range al.Items {
		v.allocs[al.Items[i].Name] = &al.Items[i]
	}
	need := slotNeed{needs: shortageOf(selectionOfView(&md, v), int(md.Spec.Instances)) > 0, lags: md.Status.ObservedGeneration != md.Generation}
	if md.Status.LastReconcileTime != nil {
		t := md.Status.LastReconcileTime.Time
		need.last = &t
	}
	return need, nil
}

// newestPending sorts the pending surplus Instances newest first, by creation time, then by name.
func newestPending(in []*v1alpha1.Instance) {
	sort.SliceStable(in, func(i, j int) bool {
		if !in[i].CreationTimestamp.Equal(&in[j].CreationTimestamp) {
			return in[j].CreationTimestamp.Before(&in[i].CreationTimestamp)
		}
		return in[i].Name < in[j].Name
	})
}
