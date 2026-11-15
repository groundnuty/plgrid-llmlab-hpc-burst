package lifecycle

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// A serving primary's replacement slot is reserved (HBEP-7, Provisioning, Per-user limits; owner decision, 15 Nov
// 2026: "Reserve the handover slot", "Can always reserve the slots without my permission"; issue #598).
//
// A primary that is Running and will need a replacement, and that has no live replacement yet, holds one slot on
// its Provider for that replacement. The count a new Allocation checks is the Allocations that hold a slot, plus
// the expectations, plus these reserved slots, so a new primary of any deployment is created only when a slot is
// left beyond them. A replacement takes its predecessor's reserved slot: the reservation it is made under names
// its predecessors as takers, and their reservations are not counted against it. In no-overlap mode, where the
// effective submit limit is 1, the slot its predecessor frees is held for the successor likewise, so it goes to
// the successor before any other deployment's primary. On the one-slot path the successor is created only once
// that slot is free (step 7, from the gap's carrier, nooverlap.go), so until then the out-of-service carrier
// holds it: the primary that drained in the mode, or a successor of it that failed before it was ready, whose
// retry takes the slot. And any live Instance whose Allocation is not made yet (a pass cut short between the two
// creates, by a failed create or a restart) holds its own, so a replacement made but not yet submitted never
// loses its slot to another deployment (#599's review).
//
// The reserved slots are worked out under the Provider's lock, from reads made there, as the listed count is: so
// the reservation is as atomic as the count (slots.go), and two workers never both see a slot left beyond it.

// reservedSlot is one slot held for a handover on a Provider.
type reservedSlot struct {
	// key is the Allocation (namespace/name) the slot is held under: a primary's, for its replacement; a gap
	// carrier's, for its successor; an Instance's own, for its Allocation not yet made.
	key string
	// deployment is the ModelDeployment (namespace/name) whose handover holds it.
	deployment string
	kind       slotKind
}

// slotKind is why a slot is reserved.
type slotKind int

const (
	// slotForReplacement is a serving primary's, for its replacement.
	slotForReplacement slotKind = iota
	// slotForSuccessor is an out-of-service gap carrier's, for the successor step 7 makes of it.
	slotForSuccessor
	// slotForOwnAllocation is a live Instance's, for its own Allocation not made yet: once that Allocation is
	// reserved (an expectation) or listed, the slot is that, and no longer counted as reserved.
	slotForOwnAllocation
)

// reservedOn returns the slots reserved on the Provider, from reads made under its lock: the Instances that carry
// its label, and the ModelDeployments of the primaries among them. allocs are the Provider's Allocations the
// count's own list returned (listProviderAllocations), by namespace/name; profile is the Provider's, for the
// drain start, and with none no primary's slot is reserved. The slots come in a fixed order.
//
// A primary holds one when it is countable (not draining, ended or deleting), no countable Instance is its
// predecessor, no live Instance names it as its predecessor, its Allocation on this Provider is Running, and its
// deployment, not being deleted, serves past its drain start (servesPast). Each Instance step 7 would make a
// no-overlap successor of holds one for that successor: the set is noOverlapCarriers' own, read by the same
// helper (outOfServiceCarriers; nooverlap.go), the primary that drained in the mode or a successor of it that
// failed before it was ready, when its deployment is not being deleted (a deleted deployment's pass makes no
// successor). Its slot is keyed by its own Allocation, which its successor's reservation takes (takersOf). Every
// countable Instance whose Allocation does not exist holds one under that Allocation's key: a pass cut short
// between the Instance and its Allocation, by a failed create or a restart, keeps its slot. The Instances of a
// deployment recreated under the same name are not told apart here (noOverlapCarriers drops them as stale),
// since a carrier's name is random and a stale successor naming it is not a case that arises.
func reservedOn(ctx context.Context, r client.Reader, provider string, profile *v1alpha1.PlatformProfileSpec, allocs map[string]*v1alpha1.Allocation) ([]reservedSlot, error) {
	var il v1alpha1.InstanceList
	if err := r.List(ctx, &il, client.MatchingLabels{v1alpha1.LabelProvider: provider}); err != nil {
		return nil, fmt.Errorf("list Instances for Provider %s's reserved slots: %w", provider, err)
	}
	sort.Slice(il.Items, func(i, j int) bool {
		return il.Items[i].Namespace+"/"+il.Items[i].Name < il.Items[j].Namespace+"/"+il.Items[j].Name
	})
	insts := make([]*v1alpha1.Instance, len(il.Items))
	countableNames, replaced := map[string]bool{}, map[string]bool{}
	for i := range il.Items {
		in := &il.Items[i]
		insts[i] = in
		if countable(in) {
			countableNames[in.Namespace+"/"+in.Name] = true
		}
		if pred := in.Annotations[v1alpha1.AnnotationPredecessor]; pred != "" && !ended(in) {
			replaced[in.Namespace+"/"+pred] = true
		}
	}
	carriers := map[*v1alpha1.Instance]bool{}
	for _, g := range outOfServiceCarriers(insts, func(*v1alpha1.Instance) bool { return true }) {
		carriers[g.in] = true
	}
	mds := map[string]*v1alpha1.ModelDeployment{}
	var out []reservedSlot
	for i := range il.Items {
		in := &il.Items[i]
		allocKey := in.Namespace + "/" + in.Spec.AllocationRef.Name
		if carriers[in] {
			md, err := deploymentOf(ctx, r, in, mds)
			if err != nil {
				return nil, err
			}
			if md != nil && md.DeletionTimestamp.IsZero() {
				out = append(out, reservedSlot{key: allocKey, deployment: md.Namespace + "/" + md.Name, kind: slotForSuccessor})
			}
			continue
		}
		if _, made := allocs[allocKey]; countable(in) && !made {
			out = append(out, reservedSlot{key: allocKey, deployment: deploymentKey(in), kind: slotForOwnAllocation})
			continue
		}
		if profile == nil || !countable(in) || replaced[in.Namespace+"/"+in.Name] {
			continue
		}
		if pred := in.Annotations[v1alpha1.AnnotationPredecessor]; pred != "" && countableNames[in.Namespace+"/"+pred] {
			continue // a replacement whose predecessor still serves: the predecessor is the primary
		}
		a := allocs[allocKey]
		if a == nil || a.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
			continue
		}
		leaseEnd := in.Status.LeaseEnd
		if leaseEnd == nil {
			leaseEnd = a.Status.AtProvider.HardEnd
		}
		if leaseEnd == nil {
			continue
		}
		md, err := deploymentOf(ctx, r, in, mds)
		if err != nil {
			return nil, err
		}
		if md == nil || !md.DeletionTimestamp.IsZero() {
			continue
		}
		ds, err := v1alpha1.DrainStart(leaseEnd.Time, *profile, md.Spec.Routing)
		if err != nil || !servesPast(md, ds) {
			continue
		}
		out = append(out, reservedSlot{key: allocKey, deployment: md.Namespace + "/" + md.Name, kind: slotForReplacement})
	}
	return out, nil
}

// deploymentKey names the Instance's ModelDeployment (namespace/name), from its controlling owner reference, or
// from its label when it has none.
func deploymentKey(in *v1alpha1.Instance) string {
	for _, o := range in.OwnerReferences {
		if o.Kind == "ModelDeployment" && o.Controller != nil && *o.Controller {
			return in.Namespace + "/" + o.Name
		}
	}
	return in.Namespace + "/" + in.Labels[v1alpha1.LabelModelDeployment]
}

// deploymentOf reads the Instance's ModelDeployment, once per deployment (mds caches it by namespace/name). One
// that is gone is nil: its Instances end, and nothing is reserved for them.
func deploymentOf(ctx context.Context, r client.Reader, in *v1alpha1.Instance, mds map[string]*v1alpha1.ModelDeployment) (*v1alpha1.ModelDeployment, error) {
	key := deploymentKey(in)
	if md, ok := mds[key]; ok {
		return md, nil
	}
	name := key[len(in.Namespace)+1:]
	var md v1alpha1.ModelDeployment
	switch err := r.Get(ctx, client.ObjectKey{Namespace: in.Namespace, Name: name}, &md); {
	case apierrors.IsNotFound(err):
		mds[key] = nil
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read ModelDeployment %s for its reserved slot: %w", key, err)
	}
	mds[key] = &md
	return &md, nil
}

// counted returns the reserved slots a reservation or a count counts: not those held for key, the Allocation
// being reserved, or for any of takers, whose slot the new Allocation takes, and not an Instance's own slot whose
// Allocation is already expected (an expectation counts it). The deployments holding them come back too, in
// order, each once, for the message.
func counted(slots []reservedSlot, key string, takers []string, expected map[string]time.Time) (n int, by []string) {
	take := map[string]bool{key: true}
	for _, k := range takers {
		take[k] = true
	}
	seenBy := map[string]bool{}
	for _, s := range slots {
		if take[s.key] {
			continue
		}
		if _, ok := expected[s.key]; s.kind == slotForOwnAllocation && ok {
			continue
		}
		n++
		if !seenBy[s.deployment] {
			seenBy[s.deployment] = true
			by = append(by, s.deployment)
		}
	}
	return n, by
}

// reservedText names the handovers whose reserved slots a count holds, for a
// message: "" when there are none.
func reservedText(n int, by []string) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("; %d more slots are reserved for the handovers of %s (HBEP-7, Provisioning, Per-user limits)", n, strings.Join(by, ", "))
}

// takersOf returns the Allocations (namespace/name) whose reserved slots an
// Allocation for a replacement of pred takes: pred's, and those of the
// Instances before it in its chain, through any failed replacements, back to
// the primary that holds the slot. nil for a new primary.
func (p *pass) takersOf(pred *v1alpha1.Instance) []string {
	var out []string
	for cur, n := pred, 0; cur != nil && n <= len(p.insts); n++ {
		out = append(out, p.key.Namespace+"/"+cur.Spec.AllocationRef.Name)
		next := cur.Annotations[v1alpha1.AnnotationPredecessor]
		cur = nil
		for _, in := range p.insts {
			if next != "" && in.Name == next {
				cur = in
				break
			}
		}
	}
	return out
}

// instanceNamed returns the deployment's Instance of that name, or nil.
func (p *pass) instanceNamed(name string) *v1alpha1.Instance {
	if name == "" {
		return nil
	}
	for _, in := range p.insts {
		if in.Name == name {
			return in
		}
	}
	return nil
}
