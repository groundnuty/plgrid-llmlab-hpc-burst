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
// it marks the newest pending surplus Instance there that no other primary's mark names (yieldMark), by the
// annotation annotationYieldTo, "<ns>/<B> <RFC3339 time of the marking pass>", and the Instance watch wakes the owner. B
// deletes nothing of another deployment's. The owner's own step 7 acts (settleYields, then scaleUnits): it re-reads
// B and cancels the Instance as over-allocation's trim, with the release cause "yielded its submit slot to <B>", or
// clears the mark. B keeps its marks honest (clearYieldMarks): a pass that saw no refusal for the limit alone of a
// missing primary on a mark's Provider clears the marks naming B there.
//
// What the owner reads, and why it is not B's Degraded: SubmitLimitReached is the ninth of degradedOrder, so
// VerificationFailed, RestartLoop, SubmissionRejected, NoOverlapSlot and others mask it, and an owner that cleared
// a mark on any other top reason would have B re-mark on every pass, a loop that starves B's primary. The owner reads
// B's state and the mark's survival instead: B's status.lastReconcileTime, which the marking pass writes at the
// time the mark carries.

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

// missingPrimaries is how many primaries the deployment still lacks: its desired count less the chosen ones.
func (p *pass) missingPrimaries() int {
	return max(p.desiredCount()-len(p.primaries()), 0)
}

// yieldOnLimit is the callback of a missing primary's reservation (slotAsk.onLimit), called under the Provider's
// lock when the reservation is refused for the submit limit alone: it records the refusal for this pass, and marks
// one more pending extra on the Provider while the extras already marked for this deployment there number fewer
// than the deployment's missing primaries: the marks outstanding are the Instances carrying a mark that names it
// while their Allocation still holds a slot, a cancelled extra's included until its deletion completes. rd carries
// the pending surplus Instances of every deployment on the Provider, newest first (D8's views).
func (p *pass) yieldOnLimit(pv *providerView) func(ctx context.Context, rd slotReads) error {
	return func(ctx context.Context, rd slotReads) error {
		if p.limitRefused == nil {
			p.limitRefused = map[string]bool{}
		}
		p.limitRefused[pv.name] = true
		me := p.deploymentKeyOf()
		marked := 0
		for _, in := range rd.yieldMarks {
			if yieldsTo(in) == me {
				marked++
			}
		}
		if marked >= p.missingPrimaries() {
			p.at(p.now.Add(yieldRecheck)) // its marks are outstanding: the deployment is asked again
			return nil
		}
		for _, in := range rd.pendingSurplus {
			if in.Annotations[annotationYieldTo] != "" {
				continue // another deployment's primary is already served by this one
			}
			if err := p.patchYield(ctx, in, yieldValue(me, p.now)); err != nil {
				return err
			}
			p.at(p.now.Add(yieldRecheck))
			return nil
		}
		return nil // no pending extra there to yield: the primary stays refused
	}
}

// patchYield sets or, with "", clears the annotation on the Instance, by a merge patch of that annotation alone.
func (p *pass) patchYield(ctx context.Context, in *v1alpha1.Instance, value string) error {
	orig := in.DeepCopy()
	if value == "" {
		delete(in.Annotations, annotationYieldTo)
	} else {
		if in.Annotations == nil {
			in.Annotations = map[string]string{}
		}
		in.Annotations[annotationYieldTo] = value
	}
	if err := p.r.Client.Patch(ctx, in, client.MergeFrom(orig), owner); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("patch the yield mark of Instance %s: %w", in.Name, err)
	}
	return nil
}

// clearYieldMarks is this deployment's own housekeeping, after its placements: for each Provider with a submit
// limit where this pass saw NO refusal for the limit alone of a missing primary, the marks naming this deployment
// there are cleared, under the Provider's lock like the marking. That covers the deployment missing nothing, its
// primary placed, refused for another reason, or not trying (paused or gated). A pass that did see such a refusal
// leaves its marks as they are and refreshes no time. It asks to run again while a mark of its own stays.
func (p *pass) clearYieldMarks() error {
	me := p.deploymentKeyOf()
	for _, pv := range p.providers {
		if pv.limits.MaxSubmitPerUser == nil {
			continue
		}
		var il v1alpha1.InstanceList
		if err := p.r.Client.List(p.ctx, &il, client.MatchingLabels{v1alpha1.LabelProvider: pv.name}); err != nil {
			return fmt.Errorf("list Instances of Provider %s for yield marks: %w", pv.name, err)
		}
		var mine []*v1alpha1.Instance
		for i := range il.Items {
			if yieldsTo(&il.Items[i]) == me {
				mine = append(mine, &il.Items[i])
			}
		}
		if len(mine) == 0 {
			continue
		}
		if p.limitRefused[pv.name] {
			p.at(p.now.Add(yieldRecheck))
			continue
		}
		err := p.r.slots().underLock(p.ctx, pv.name, func(ctx context.Context) error {
			for _, in := range mine {
				if err := p.patchYield(ctx, in, ""); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// settleYields is the owner's step 7 for the yield marks on its own Instances, before it cancels any
// (scaleUnits reads p.yielding). For each pending surplus Instance that carries a mark:
//   - its job started, it is no longer surplus, the mark is malformed, or the deployment it names is gone, being
//     deleted, or has no missing primary any more (it scaled down, or got a slot elsewhere): the mark is cleared and
//     nothing is cancelled; a started extra is never cancelled for this;
//   - the named deployment's status.lastReconcileTime is no later than the mark's time: no later pass of it has run
//     since the mark (the marking pass writes its own time there), so WAIT: the mark stays, nothing is cancelled, and
//     the pass asks to run again;
//   - otherwise a later pass of the named deployment was refused again for the limit alone and kept the mark (it
//     clears the marks of a pass that was not): the Instance yields, and is cancelled as over-allocation's trim.
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
			// holds the slot the named deployment's primary waits for, and the guard counts the mark (readReserved).
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
		needs, last, err := p.needsSlot(b)
		if err != nil {
			return err
		}
		switch {
		case !needs:
			if err := p.patchYield(p.ctx, in, ""); err != nil {
				return err
			}
		case last == nil || !last.After(at):
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

// needsSlot reads the named deployment: whether it still lacks a primary (it exists, is not being deleted, and its
// countable chosen primaries are fewer than its desired count: the selection its own pass makes), and its
// status.lastReconcileTime.
func (p *pass) needsSlot(deployment string) (needs bool, last *time.Time, err error) {
	ns, name, _ := strings.Cut(deployment, "/")
	var md v1alpha1.ModelDeployment
	switch err := p.r.Reader.Get(p.ctx, client.ObjectKey{Namespace: ns, Name: name}, &md); {
	case apierrors.IsNotFound(err):
		return false, nil, nil
	case err != nil:
		return false, nil, fmt.Errorf("read ModelDeployment %s for a yield mark: %w", deployment, err)
	}
	if !md.DeletionTimestamp.IsZero() || md.Spec.Source.Served == nil || md.Spec.Instances == 0 ||
		(md.Spec.EndTime != nil && !p.now.Before(md.Spec.EndTime.Time)) {
		return false, nil, nil // gone, being deleted, scaled to zero or past its end time: its pass makes no primary
	}
	v := deploymentView{key: deployment, allocs: map[string]*v1alpha1.Allocation{}}
	var il v1alpha1.InstanceList
	if err := p.r.Reader.List(p.ctx, &il, client.InNamespace(ns), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(name)}); err != nil {
		return false, nil, fmt.Errorf("list Instances of %s for a yield mark: %w", deployment, err)
	}
	for i := range il.Items {
		v.insts = append(v.insts, &il.Items[i])
	}
	var al v1alpha1.AllocationList
	if err := p.r.Reader.List(p.ctx, &al, client.InNamespace(ns), client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(name)}); err != nil {
		return false, nil, fmt.Errorf("list Allocations of %s for a yield mark: %w", deployment, err)
	}
	for i := range al.Items {
		v.allocs[al.Items[i].Name] = &al.Items[i]
	}
	chosen, _ := selectUnits(selectionOfView(&md, v), int(md.Spec.Instances))
	if md.Status.LastReconcileTime != nil {
		t := md.Status.LastReconcileTime.Time
		last = &t
	}
	return len(chosen) < int(md.Spec.Instances), last, nil
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
