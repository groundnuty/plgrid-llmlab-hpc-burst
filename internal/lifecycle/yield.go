package lifecycle

import (
	"context"
	"fmt"
	"slices"
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
// The deployment B whose primary is refused decides, at its slot reservation, under the yield lock (slotTable.yieldMu) and the
// Provider's lock (slotAsk.onLimit): it marks the newest pending surplus Instance there that no primary's mark names (any
// deployment's; one whose Allocation is being deleted or that has none is never a candidate), by the annotation
// annotationYieldTo, "<ns>/<B> <RFC3339 time of the marking pass>" and the label labelYieldMark, and the Instance watch
// wakes the owner. B deletes nothing of another deployment's. It marks only while its shortage (shortageOf: the primaries it
// lacks by Allocation presence, which counts a cut-short Instance as missing) exceeds its live marks on every Provider plus
// the yields in flight for it (Allocations being deleted whose release cause names it), and only when the cancel could
// admit it (the count that refused is no more than the limit).
//
// The owner's own step 7 decides what to do with a mark on its Instance (settleYields, then scaleDown): it clears a mark that
// cannot hold (the job started, no longer surplus), and COMMITS the cancel of the rest (commitYield) under the yield lock and
// the extra's Provider's lock, after an uncached re-check of the mark, of B and of the count, or clears the mark, or waits. The
// cancel is over-allocation's trim, with the release cause "yielded its submit slot to <B>". B keeps its marks honest
// (clearYieldMarks), on every Provider, including one it no longer lists: it clears the marks its pass no longer supports,
// the oldest extras' first. Its clearing is no longer what keeps the cancel safe: the owner's commit is.
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

// yieldReleaseWhy is the words of the release cause of an extra that yielded its slot to a deployment (ns/name).
func yieldReleaseWhy(deployment string) string { return "yielded its submit slot to " + deployment }

// inFlightFor counts the yields in flight for a deployment (C2): the Allocations, of any deployment on any Provider,
// that are being DELETED and whose release cause says they yielded their submit slot to it. Their slots are coming to the
// deployment's primaries, so they are never a reason to yield again, and they count against its shortage. It is one List
// through the uncached Reader.
func (p *pass) inFlightFor(ctx context.Context, deployment string) (int, error) {
	var al v1alpha1.AllocationList
	if err := p.r.Reader.List(ctx, &al); err != nil {
		return 0, fmt.Errorf("list the Allocations being released for the yields in flight: %w", err)
	}
	n := 0
	for i := range al.Items {
		a := &al.Items[i]
		if !a.DeletionTimestamp.IsZero() && strings.HasSuffix(a.Annotations[v1alpha1.AnnotationReleaseCause], yieldReleaseWhy(deployment)) {
			n++
		}
	}
	return n, nil
}

// yieldOnLimit is the callback of a missing primary's reservation (slotAsk.onLimit), called under yieldMu and the
// Provider's lock when the reservation is refused for the submit limit alone, by the submit step and by
// ensureAllocation's retry of a cut-short primary: it records the refusal for this pass, and marks one more pending
// extra on the Provider when
//   - the cancel could admit this primary (Y3): the count that refused, which holds the marked extras still, is no more
//     than the limit. Each slot one of this deployment's earlier marks frees is taken by another of its refused
//     primaries, so its own marks, and the yields in flight, net to zero, and this primary needs one more: never
//     subtracted from the count. Other deployments' marks free slots for them;
//   - and this deployment's live marks on EVERY Provider, plus those made earlier in this pass (deduplicated by UID, since
//     the Reader may lag), plus the yields in flight for it (inFlightFor), are fewer than its shortage (Y1, Y2, C3).
//
// The candidate is the newest of rd's pending surplus Instances (every deployment's, on the Provider, whose Allocation holds
// a slot, is not being deleted and has no job started) that carries no mark.
func (p *pass) yieldOnLimit(pv *providerView) func(ctx context.Context, rd slotReads) error {
	return func(ctx context.Context, rd slotReads) error {
		limit := int(*pv.limits.MaxSubmitPerUser)
		if p.limitRefused == nil {
			p.limitRefused = map[string]bool{}
		}
		p.limitRefused[pv.name] = true
		if rd.total > limit {
			return nil // no cancel can admit this primary: the count would still be over the limit
		}
		marks, err := p.marksOfMine(ctx)
		if err != nil {
			return err
		}
		flying, err := p.inFlightFor(ctx, p.deploymentKeyOf())
		if err != nil {
			return err
		}
		count := map[types.UID]bool{}
		for _, m := range marks {
			if m.live {
				count[m.in.UID] = true
			}
		}
		for uid := range p.marked {
			count[uid] = true
		}
		if len(count)+flying >= p.shortage() {
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

// reserveYielding is reserveSlotWith for a missing primary, whose refusal for the limit alone may yield: it holds yieldMu
// across the reservation (taken before the Provider's lock, which the reservation takes), so no cancel's decision runs
// between the count that refused and the mark.
func (p *pass) reserveYielding(pv *providerView, name, holder string, takers []string, params *v1alpha1.BatchParameters, quiet bool, alsoLeave int, onLimit func(context.Context, slotReads) error) (ok, holds bool, err error) {
	if onLimit != nil {
		p.r.slots().yieldMu.Lock()
		defer p.r.slots().yieldMu.Unlock()
	}
	return p.reserveSlotWith(pv, name, holder, takers, params, quiet, alsoLeave, onLimit)
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

// clearYieldMarks is this deployment's own housekeeping, after its placements, and no longer what keeps the cancel safe
// (that is the owner's commit, commitYield): it clears the live marks naming it on EVERY Provider, including one it no
// longer lists (Y4), when
//   - this pass saw no limit-alone refusal on their Provider: the deployment placed, or is not trying (paused, gated,
//     scaled down), or no longer lists it. A refusal in ensureAllocation counts;
//   - it holds more live marks than its shortage less the yields in flight for it (C5): the OLDEST extras' go first, so the
//     marks left are on the NEWEST pending extras.
//
// A cancelled extra's mark is never cleared here: its Allocation, deleting, is a yield in flight. Each clear is made
// under yieldMu and its mark's Provider's lock, re-reading the Instance there. It asks to run again while a mark of its
// own stays.
func (p *pass) clearYieldMarks() error {
	marks, err := p.marksOfMine(p.ctx)
	if err != nil {
		return err
	}
	var live []yieldMarkOf
	for _, m := range marks {
		if m.live {
			live = append(live, m)
		}
	}
	if len(live) == 0 {
		return nil
	}
	flying, err := p.inFlightFor(p.ctx, p.deploymentKeyOf())
	if err != nil {
		return err
	}
	var keep, drop []yieldMarkOf
	for _, m := range live {
		if p.limitRefused[m.provider] {
			keep = append(keep, m)
		} else {
			drop = append(drop, m)
		}
	}
	newestMarkFirst(keep)
	if room := max(p.shortage()-flying, 0); len(keep) > room {
		drop = append(drop, keep[room:]...)
		keep = keep[:room]
	}
	if err := p.clearMarks(drop); err != nil {
		return err
	}
	if len(keep) > 0 {
		p.at(p.now.Add(yieldRecheck))
	}
	return nil
}

// clearMarks clears the marks, each under yieldMu and its Provider's lock, re-reading each Instance there: one that is no
// longer live, or no longer names this deployment, is left alone.
func (p *pass) clearMarks(ms []yieldMarkOf) error {
	if len(ms) == 0 {
		return nil
	}
	p.r.slots().yieldMu.Lock()
	defer p.r.slots().yieldMu.Unlock()
	me := p.deploymentKeyOf()
	for _, m := range ms {
		err := p.r.slots().underLock(p.ctx, m.provider, func(ctx context.Context) error {
			var cur v1alpha1.Instance
			switch err := p.r.Reader.Get(ctx, client.ObjectKeyFromObject(m.in), &cur); {
			case apierrors.IsNotFound(err):
				return nil
			case err != nil:
				return fmt.Errorf("read Instance %s to clear its yield mark: %w", m.in.Name, err)
			}
			if yieldsTo(&cur) != me || !countable(&cur) {
				return nil
			}
			return p.patchYield(ctx, &cur, "")
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// settleYields is the owner's step 7 for the yield marks on its own Instances, before it cancels any (scaleDown reads
// p.yielding, and commits each candidate through commitYield). For each Instance that carries a mark:
//   - one that is no longer live: a cancelled extra's tombstone keeps its mark while its Allocation is being deleted, as a
//     yield in flight, and otherwise the mark is cleared;
//   - a mark that is malformed, an Instance that is no longer surplus or is stale, or whose job started: the mark is
//     cleared and nothing is cancelled (a started extra is never cancelled for this);
//   - otherwise it is a CANDIDATE: the cancel is decided by commitYield, under the locks, from fresh reads.
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
			if a := p.allocOf(in); a != nil && !a.DeletionTimestamp.IsZero() {
				continue
			}
			if err := p.patchYield(p.ctx, in, ""); err != nil {
				return err
			}
			continue
		}
		b, _, ok := parseYield(v)
		if !ok || !isSurplus[in] || p.stale[in.UID] || jobStarted(p.allocOf(in)) {
			if err := p.patchYield(p.ctx, in, ""); err != nil {
				return err
			}
			continue
		}
		if p.yielding == nil {
			p.yielding = map[types.UID]string{}
		}
		p.yielding[in.UID] = b
	}
	return nil
}

// yieldVerdict is what the owner's re-check decides about a candidate.
type yieldVerdict int

const (
	yieldWait   yieldVerdict = iota // keep the mark, cancel nothing, ask to run again
	yieldClear                      // clear the mark: the need is gone
	yieldCancel                     // commit the cancel
)

// commitYield is the owner's COMMIT of a yielded extra's cancel (C4), in its own step 7 through the trim path. It takes
// yieldMu, then the extra's Provider's lock, and re-reads everything uncached under them; it cancels only when
//   - the named deployment B exists, is not being deleted, still serves and still allows this Provider;
//   - B's status.lastReconcileTime is later than the mark's time, the mark is still there, and B's observedGeneration is
//     its generation (B's spec is the one its last pass saw);
//   - B's shortage exceeds the yields in flight for it (inFlightFor): the slots coming do not already meet it;
//   - freeing this extra's slot admits a primary: the Provider's count, with this extra and the yields in flight still
//     listed, is no more than the limit.
//
// B gone, not allowing the Provider, or its shortage no more than the yields in flight: the mark is CLEARED (and a release
// cause written for the cancel and left by a restart is removed). Any other failure: WAIT. When it cancels it writes the
// release cause "yielded its submit slot to B", deletes the Allocation and ends the Instance cleanly, under both locks;
// the Instance's memory and finalizer follow after they are released.
func (p *pass) commitYield(t scaleTarget, b string) error {
	in := t.instance
	pv := p.pvOf(in.Labels)
	_, at, ok := parseYield(in.Annotations[annotationYieldTo])
	if pv == nil || pv.profile == nil || pv.limits.MaxSubmitPerUser == nil || !ok {
		return p.patchYield(p.ctx, in, "")
	}
	yl := &p.r.slots().yieldMu
	yl.Lock()
	defer yl.Unlock()
	done := false
	err := p.r.slots().underLockCounted(p.ctx, p.r.Reader, pv.name, &pv.profile.Spec, p.reserveTime(), func(ctx context.Context, total int) error {
		cur, alloc, verdict, err := p.recheckYield(ctx, in, b, at, pv, total)
		if err != nil {
			return err
		}
		switch verdict {
		case yieldWait:
			p.at(p.now.Add(yieldRecheck))
			return nil
		case yieldClear:
			if cur == nil {
				return nil
			}
			return p.clearYield(ctx, cur, alloc, b)
		}
		*in = *cur
		if old := p.allocs[alloc.Name]; old != nil {
			defer func() { *old = *alloc }() // the pass reads its Allocation as the commit leaves it: deleting
		}
		why := yieldReleaseWhy(b)
		if gone, err := p.writeReleaseCause(alloc, releaseCause(endCause(in, v1alpha1.ReasonDrained, why))); err != nil || gone {
			return err
		}
		if err := p.endStatus(in, v1alpha1.ReasonDrained, why); err != nil {
			return err
		}
		if err := p.endRelease(in, alloc, v1alpha1.ReasonDrained, why, true); err != nil {
			return err
		}
		done = true
		return nil
	})
	if err != nil || !done {
		return err
	}
	p.mem.clear(instanceHeartbeatKey(in))
	return p.removeDrainFinalizer(in)
}

// recheckYield is commitYield's re-check, under both locks, from uncached reads. It returns the Instance and its Allocation
// as it read them (nil when gone) and the verdict.
func (p *pass) recheckYield(ctx context.Context, in *v1alpha1.Instance, b string, at time.Time, pv *providerView, total int) (*v1alpha1.Instance, *v1alpha1.Allocation, yieldVerdict, error) {
	var cur v1alpha1.Instance
	switch err := p.r.Reader.Get(ctx, client.ObjectKeyFromObject(in), &cur); {
	case apierrors.IsNotFound(err):
		return nil, nil, yieldClear, nil
	case err != nil:
		return nil, nil, 0, fmt.Errorf("read Instance %s to commit its yield: %w", in.Name, err)
	}
	if !countable(&cur) || cur.Annotations[annotationYieldTo] != in.Annotations[annotationYieldTo] {
		return nil, nil, yieldClear, nil // the mark is gone or changed, or the Instance ended: nothing to cancel
	}
	var alloc v1alpha1.Allocation
	switch err := p.r.Reader.Get(ctx, client.ObjectKey{Namespace: cur.Namespace, Name: cur.Spec.AllocationRef.Name}, &alloc); {
	case apierrors.IsNotFound(err):
		return &cur, nil, yieldClear, nil
	case err != nil:
		return nil, nil, 0, fmt.Errorf("read the Allocation of Instance %s to commit its yield: %w", in.Name, err)
	}
	if !alloc.DeletionTimestamp.IsZero() || jobStarted(&alloc) {
		return &cur, &alloc, yieldClear, nil // a started extra is never cancelled for a yield
	}
	ns, name, _ := strings.Cut(b, "/")
	var md v1alpha1.ModelDeployment
	switch err := p.r.Reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &md); {
	case apierrors.IsNotFound(err):
		return &cur, &alloc, yieldClear, nil
	case err != nil:
		return nil, nil, 0, fmt.Errorf("read ModelDeployment %s to commit a yield: %w", b, err)
	}
	if !md.DeletionTimestamp.IsZero() || md.Spec.Source.Served == nil || md.Spec.Instances == 0 ||
		(md.Spec.EndTime != nil && !p.now.Before(md.Spec.EndTime.Time)) {
		return &cur, &alloc, yieldClear, nil // gone, being deleted, scaled to zero or past its end time
	}
	var provs v1alpha1.ProviderList
	if err := p.r.Reader.List(ctx, &provs); err != nil {
		return nil, nil, 0, fmt.Errorf("list Providers to commit a yield: %w", err)
	}
	allowed, err := selectedProviderNames(md.Spec.Providers, provs.Items)
	if err != nil || !slices.Contains(allowed, pv.name) {
		return &cur, &alloc, yieldClear, nil // B no longer allows this Provider: it could not use the slot
	}
	short, err := p.shortageOfDeployment(ctx, &md)
	if err != nil {
		return nil, nil, 0, err
	}
	flying, err := p.inFlightFor(ctx, b)
	if err != nil {
		return nil, nil, 0, err
	}
	if short <= flying {
		return &cur, &alloc, yieldClear, nil // the slots already coming meet B's shortage
	}
	last := md.Status.LastReconcileTime
	if last == nil || !last.After(at) || md.Status.ObservedGeneration != md.Generation {
		return &cur, &alloc, yieldWait, nil // B has not passed again since the mark, at its current spec
	}
	if total > int(*pv.limits.MaxSubmitPerUser) {
		return &cur, &alloc, yieldWait, nil // freeing this extra's slot would not admit a primary
	}
	return &cur, &alloc, yieldCancel, nil
}

// clearYield clears a mark that the owner's re-check found no longer needed, and the release cause that a cancel cut short by
// a restart may have written on its Allocation.
func (p *pass) clearYield(ctx context.Context, in *v1alpha1.Instance, alloc *v1alpha1.Allocation, b string) error {
	if err := p.patchYield(ctx, in, ""); err != nil {
		return err
	}
	if alloc != nil && alloc.DeletionTimestamp.IsZero() && strings.HasSuffix(alloc.Annotations[v1alpha1.AnnotationReleaseCause], yieldReleaseWhy(b)) {
		next := alloc.DeepCopy()
		delete(next.Annotations, v1alpha1.AnnotationReleaseCause)
		if err := p.r.Client.Update(ctx, next, owner); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("clear the yield's release cause of Allocation %s: %w", alloc.Name, err)
		}
	}
	return nil
}

// shortageOfDeployment is shortageOf the named deployment's own Instances and Allocations, read uncached: what the owner
// re-reads of the deployment that asked (C4).
func (p *pass) shortageOfDeployment(ctx context.Context, md *v1alpha1.ModelDeployment) (int, error) {
	v := deploymentView{key: md.Namespace + "/" + md.Name, allocs: map[string]*v1alpha1.Allocation{}}
	var il v1alpha1.InstanceList
	if err := p.r.Reader.List(ctx, &il, client.InNamespace(md.Namespace), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md.Name)}); err != nil {
		return 0, fmt.Errorf("list Instances of %s for a yield: %w", v.key, err)
	}
	for i := range il.Items {
		v.insts = append(v.insts, &il.Items[i])
	}
	var al v1alpha1.AllocationList
	if err := p.r.Reader.List(ctx, &al, client.InNamespace(md.Namespace), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md.Name)}); err != nil {
		return 0, fmt.Errorf("list Allocations of %s for a yield: %w", v.key, err)
	}
	for i := range al.Items {
		v.allocs[al.Items[i].Name] = &al.Items[i]
	}
	return shortageOf(selectionOfView(md, v), int(md.Spec.Instances)), nil
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
