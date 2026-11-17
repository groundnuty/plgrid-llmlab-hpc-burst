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
// retry takes the slot. And a live replacement or successor whose Allocation is not made yet (a pass cut short
// between the two creates, by a failed create or a restart) holds its own: the slot reserved for it passes to it
// with its Instance and is used up once its Allocation is reserved or listed, so it never loses that slot to
// another deployment (#599's review). A deployment's first primary, which no handover reserved a slot for, gives
// its slot back when its creation fails, as HBEP-7 says (slots.go).
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
	// created and uid order the own slots of Instances that wait for their Allocations: the older takes a slot
	// before the younger (before).
	created time.Time
	uid     string
}

// before reports whether s's Instance is older than o's: by creation time, then by UID. Of two replacements
// waiting for their Allocations on a Provider with no slot for both, possible only after a leader hand-over
// race, the older takes the slot and the younger waits, so neither waits for the other for ever (the science
// agent's ruling on #599).
func (s reservedSlot) before(o reservedSlot) bool {
	if !s.created.Equal(o.created) {
		return s.created.Before(o.created)
	}
	return s.uid < o.uid
}

// slotKind is why a slot is reserved.
type slotKind int

const (
	// slotForReplacement is a serving primary's, for its replacement.
	slotForReplacement slotKind = iota
	// slotForSuccessor is an out-of-service gap carrier's, for the successor step 7 makes of it.
	slotForSuccessor
	// slotForOwnAllocation is a live replacement's or successor's, for its own Allocation not made yet: once
	// that Allocation is reserved (an expectation) or listed, the slot is that, and no longer counted as reserved.
	slotForOwnAllocation
)

// reservedOn returns the slots reserved on the Provider, from reads made under its lock: the Instances that carry
// its label, the Instances on every Provider of each deployment with one among them (deploymentViews), and the
// ModelDeployments of the primaries among them. Only the Instances on this Provider hold slots here, but whether
// one is replaced, whether its predecessor still serves, a gap's chain and a hedge's winner are read across every
// Provider (#617): a primary whose replacement went to another Provider holds no slot for it. allocs are the
// Provider's Allocations the
// count's own list returned (listProviderAllocations), by namespace/name; profile is the Provider's, for the
// drain start, and with none no primary's slot is reserved. The slots come in a fixed order.
//
// A primary holds one when it is countable (not draining, ended or deleting), it is not surplus in its
// deployment's selection (surplusOf: an extra, or a primary a lowered count left over, holds none, #616), no
// countable Instance is its predecessor, no live Instance names it as its predecessor, its Allocation on this Provider is Running, and its
// deployment, not being deleted, serves past its drain start (servesPast). So does a hedge's winner whose
// predecessor still serves on another Provider, from its win: it is ready, and its hedge's winner (hedgeWinner over
// the hedge's countable members on every Provider), so another deployment's primary can't take the slot its own
// replacement will need while it serves beside its predecessor (HBEP-7, Hedging (beta), Interactions). Each Instance step 7 would make a
// no-overlap successor of holds one for that successor: the set is noOverlapCarriers' own, read by the same
// helper (outOfServiceCarriers; nooverlap.go), the primary that drained in the mode or a successor of it that
// failed before it was ready, when its deployment is not being deleted (a deleted deployment's pass makes no
// successor). Its slot is keyed by its own Allocation, which its successor's reservation takes (takersOf). Every
// countable Instance that names a predecessor (a replacement or a successor) and whose Allocation does not exist
// holds one under that Allocation's key: a handover cut short between the Instance and its Allocation, by a
// failed create or a restart, keeps the slot reserved for it. An Instance with no predecessor, a deployment's
// first primary or step 7's new one, holds none: no handover reserved its slot, and a failed creation of it gives
// its slot back (HBEP-7, Slots are reserved atomically). The Instances of a
// deployment recreated under the same name are not told apart here (noOverlapCarriers drops them as stale),
// since a carrier's name is random and a stale successor naming it is not a case that arises.
func reservedOn(ctx context.Context, r client.Reader, provider string, profile *v1alpha1.PlatformProfileSpec, allocs map[string]*v1alpha1.Allocation) ([]reservedSlot, error) {
	slots, _, _, err := readReserved(ctx, r, provider, profile, allocs)
	return slots, err
}

// readReserved is reservedOn, and it returns, from the same reads, the pending surplus Instances of every
// deployment on the Provider (the yield's candidates): countable surplus Instances whose Allocation exists, holds a
// slot and has no job started, newest first; and every Instance on the Provider that carries a yield mark while its
// Allocation still holds a slot, whatever its phase (the marks outstanding: a cancelled extra's, until its Allocation's
// deletion completes, still counts as the slot its refused primary is waiting for).
func readReserved(ctx context.Context, r client.Reader, provider string, profile *v1alpha1.PlatformProfileSpec, allocs map[string]*v1alpha1.Allocation) ([]reservedSlot, []*v1alpha1.Instance, []*v1alpha1.Instance, error) {
	var il v1alpha1.InstanceList
	if err := r.List(ctx, &il, client.MatchingLabels{v1alpha1.LabelProvider: provider}); err != nil {
		return nil, nil, nil, fmt.Errorf("list Instances for Provider %s's reserved slots: %w", provider, err)
	}
	views, err := deploymentViews(ctx, r, il.Items)
	if err != nil {
		return nil, nil, nil, err
	}
	// Every read below but the slots' own is of the deployments' Instances on every Provider (#617): a primary
	// whose replacement or hedge member is on another Provider is replaced all the same, and a gap's chain and a
	// hedge's members may span Providers. Only the Instances on this Provider hold slots here.
	countableNames, replaced, byName := map[string]bool{}, map[string]bool{}, map[string]*v1alpha1.Instance{}
	var all, here []*v1alpha1.Instance
	winners := map[*v1alpha1.Instance]bool{}
	for _, v := range views {
		for _, in := range v.insts {
			all = append(all, in)
			byName[in.Namespace+"/"+in.Name] = in
			if countable(in) {
				countableNames[in.Namespace+"/"+in.Name] = true
			}
			if pred := in.Annotations[v1alpha1.AnnotationPredecessor]; pred != "" && !ended(in) {
				replaced[in.Namespace+"/"+pred] = true
			}
			if in.Labels[v1alpha1.LabelProvider] == provider {
				here = append(here, in)
			}
		}
		// The winner rule, over each hedge's countable members on every Provider, as the pass reads it (hedge.go). A
		// resolved hedge's one member that has served stays its winner through a restart, which makes it verifying
		// for a while, so its slot does not lapse for the restart's length (#627's review).
		for _, g := range groupHedges(v.insts, countable) {
			if w := hedgeWinner(g.members); w != nil {
				winners[w] = true
			} else if len(g.members) == 1 && g.members[0].Status.ReadyTime != nil {
				winners[g.members[0]] = true
			}
		}
	}
	sort.Slice(here, func(i, j int) bool { return here[i].Namespace+"/"+here[i].Name < here[j].Namespace+"/"+here[j].Name })
	carriers := map[*v1alpha1.Instance]bool{}
	for _, g := range outOfServiceCarriers(all, func(*v1alpha1.Instance) bool { return true }) {
		carriers[g.in] = true
	}
	mds := map[string]*v1alpha1.ModelDeployment{}
	// The selection of each deployment (selectPrimaries, as its own pass makes it): a surplus Instance holds no
	// reserved slot and gets no replacement (HBEP-7, How it counts), so a started extra on a Running Allocation
	// reserves nothing, as it would otherwise block another deployment's primary and be counted twice in its own
	// deployment's extra check (#616 PR 4). The surplus Instances on this Provider that wait, with a slot held and
	// no job started, are also the yield's candidates (yield.go).
	surplus := map[*v1alpha1.Instance]bool{}
	var pending []*v1alpha1.Instance
	for _, v := range views {
		set, err := surplusOf(ctx, r, v, mds)
		if err != nil {
			return nil, nil, nil, err
		}
		for in := range set {
			surplus[in] = true
			a := allocs[in.Namespace+"/"+in.Spec.AllocationRef.Name]
			if in.Labels[v1alpha1.LabelProvider] == provider && countable(in) && a != nil && a.DeletionTimestamp.IsZero() && holdsSlot(a) && a.Status.AtProvider.StartTime == nil {
				pending = append(pending, in)
			}
		}
	}
	newestPending(pending)
	var out []reservedSlot
	for _, in := range here {
		allocKey := in.Namespace + "/" + in.Spec.AllocationRef.Name
		if carriers[in] {
			md, err := deploymentOf(ctx, r, in, mds)
			if err != nil {
				return nil, nil, nil, err
			}
			if md != nil && md.DeletionTimestamp.IsZero() {
				out = append(out, reservedSlot{key: allocKey, deployment: md.Namespace + "/" + md.Name, kind: slotForSuccessor})
			}
			continue
		}
		if _, made := allocs[allocKey]; countable(in) && !made && in.Annotations[v1alpha1.AnnotationPredecessor] != "" {
			out = append(out, reservedSlot{key: allocKey, deployment: deploymentKey(in), kind: slotForOwnAllocation, created: in.CreationTimestamp.Time, uid: string(in.UID)})
			continue
		}
		if profile == nil || !countable(in) || replaced[in.Namespace+"/"+in.Name] || surplus[in] {
			continue
		}
		if pred := in.Annotations[v1alpha1.AnnotationPredecessor]; pred != "" && countableNames[in.Namespace+"/"+pred] {
			// A replacement whose predecessor still serves: the predecessor is the primary, and holds the slot on its
			// own Provider. Its hedge's winner on another Provider holds one here from its win (HBEP-7, Hedging (beta),
			// Interactions, The winner's own slot; a hedge of one placed on another Provider too, the science agent's
			// ruling on #604, Q4): the rule below applies to it as to a primary.
			if pi := byName[in.Namespace+"/"+pred]; pi == nil || pi.Labels[v1alpha1.LabelProvider] == provider || !winners[in] {
				continue
			}
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
			return nil, nil, nil, err
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
	var outstanding []*v1alpha1.Instance
	for _, in := range here {
		if a := allocs[in.Namespace+"/"+in.Spec.AllocationRef.Name]; in.Annotations[annotationYieldTo] != "" && a != nil && holdsSlot(a) {
			outstanding = append(outstanding, in)
		}
	}
	return out, pending, outstanding, nil
}

// deploymentView is one deployment's Instances on every Provider.
type deploymentView struct {
	key   string // namespace/the model-deployment label's value
	insts []*v1alpha1.Instance
	// allocs are the deployment's Allocations on every Provider by name, one List by the same label (#616 PR 4):
	// what the selection reads (a hedge's representative, an Instance's job start). Empty for a view of one
	// Instance that has no label.
	allocs map[string]*v1alpha1.Allocation
}

// deploymentViews reads, for each deployment with an Instance among onProvider, its Instances on every Provider
// and its Allocations, each by its model-deployment label in its namespace, one List per deployment each (#617;
// shared with #616's selection, which reads both).
// An Instance of onProvider that its deployment's List does not show (one with no label, or a cache between the
// two Lists) is kept as listed, so the reserved count never reads fewer Instances than the Provider's own List.
// The views come in the order of their deployments' first Instance in onProvider.
func deploymentViews(ctx context.Context, r client.Reader, onProvider []v1alpha1.Instance) ([]deploymentView, error) {
	at := map[string]int{}
	var out []deploymentView
	for i := range onProvider {
		in := &onProvider[i]
		label := in.Labels[v1alpha1.LabelModelDeployment]
		key := in.Namespace + "/" + label
		if label == "" {
			key = in.Namespace + "//" + in.Name // no deployment to read: a view of itself
		}
		j, ok := at[key]
		if !ok {
			j = len(out)
			at[key] = j
			v := deploymentView{key: key, allocs: map[string]*v1alpha1.Allocation{}}
			if label != "" {
				var il v1alpha1.InstanceList
				if err := r.List(ctx, &il, client.InNamespace(in.Namespace), client.MatchingLabels{v1alpha1.LabelModelDeployment: label}); err != nil {
					return nil, fmt.Errorf("list Instances of deployment %s for the reserved slots: %w", key, err)
				}
				for k := range il.Items {
					v.insts = append(v.insts, &il.Items[k])
				}
				var al v1alpha1.AllocationList
				if err := r.List(ctx, &al, client.InNamespace(in.Namespace), client.MatchingLabels{v1alpha1.LabelModelDeployment: label}); err != nil {
					return nil, fmt.Errorf("list Allocations of deployment %s for the reserved slots: %w", key, err)
				}
				for k := range al.Items {
					v.allocs[al.Items[k].Name] = &al.Items[k]
				}
			}
			out = append(out, v)
		}
		found := false
		for _, o := range out[j].insts {
			if o.Namespace == in.Namespace && o.Name == in.Name {
				found = true
				break
			}
		}
		if !found {
			out[j].insts = append(out[j].insts, in)
		}
	}
	return out, nil
}

// surplusOf returns the Instances the deployment's selection leaves out: every member of each hedge that
// selectPrimaries does not choose, at the deployment's desired count: spec.instances, or 0 when it is being
// deleted. It reads the objects its pass reads (the Instances it owns or that an earlier deployment of the same
// name owned, in the pass's order, and the deployment's Allocations), so the two make one selection (a parity
// test pins it). A deployment that is gone has no ModelDeployment to read, and its Instances hold nothing here.
func surplusOf(ctx context.Context, r client.Reader, v deploymentView, mds map[string]*v1alpha1.ModelDeployment) (map[*v1alpha1.Instance]bool, error) {
	if len(v.insts) == 0 || deploymentKey(v.insts[0]) == v.insts[0].Namespace+"/" {
		return nil, nil // no Instance, or one that names no deployment: nothing to select over
	}
	md, err := deploymentOf(ctx, r, v.insts[0], mds)
	if err != nil || md == nil {
		return nil, err
	}
	desired := 0
	if md.DeletionTimestamp.IsZero() {
		desired = int(md.Spec.Instances)
	}
	_, out := selectUnits(selectionOfView(md, v), desired)
	set := map[*v1alpha1.Instance]bool{}
	for _, u := range out {
		for _, m := range u.members {
			set[m] = true
		}
	}
	return set, nil
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
	var self *reservedSlot
	for i := range slots {
		if slots[i].key == key && slots[i].kind == slotForOwnAllocation {
			self = &slots[i]
		}
	}
	seenBy := map[string]bool{}
	for _, s := range slots {
		if take[s.key] {
			continue
		}
		if _, ok := expected[s.key]; s.kind == slotForOwnAllocation && ok {
			continue
		}
		if self != nil && s.kind == slotForOwnAllocation && self.before(s) {
			continue // a younger waiting replacement yields to this older one
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

// lastingCause returns the reason and the words to end a waiting replacement with when its Provider can never
// take its Allocation (ensureAllocation): the Provider dropped from the deployment's list or managed
// (AllocationLost), or its partition too short for the lease (SpecUnsupported). "" for none: a Provider problem,
// such as one not Ready, can pass, so the replacement waits.
func lastingCause(pv *providerView) (reason, why string) {
	switch {
	case pv.extra:
		return v1alpha1.ReasonAllocationLost, fmt.Sprintf("Provider %s is no longer in the deployment's list", pv.name)
	case pv.managed:
		return v1alpha1.ReasonAllocationLost, fmt.Sprintf("Provider %s is on a managed platform", pv.name)
	case pv.tooShort != "":
		return v1alpha1.ReasonSpecUnsupported, pv.tooShort
	}
	return "", ""
}
