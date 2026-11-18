package lifecycle

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// slotSnapshot is the ONE ordered read of Allocations and Instances that a reservation makes under a Provider's lock
// (#751's delta review): every view the count, the reserved slots, the extra's unit set and the admission headroom
// need is derived from it, never from a list of its own, so no two of them read the world at different moments.
//
// The Allocations of every namespace are read FIRST and the Instances of every namespace SECOND, each once, under the
// reservation's one deadline. The order is the guarantee: an Allocation that the first list shows has its Instance in the
// second, and an Instance the second shows without an Allocation in the first is IN FLIGHT (its Allocation is made after
// the first list, or not yet), which the unit set counts as a unit. Both readings err toward counting more, never toward
// releasing a slot. The ModelDeployments and the Providers are read after both (the Gets of deploymentOf, the headroom's
// lists, the sizing's Provider), and only name what the snapshot's objects are judged by.
type slotSnapshot struct {
	// allocs are the Allocations of every namespace, read first.
	allocs []*v1alpha1.Allocation
	// insts are the Instances of every namespace, read second, and nil when the reservation needs none (a count of
	// the Provider's Allocations alone).
	insts []*v1alpha1.Instance
}

// readSlotSnapshot lists the Allocations of every namespace and then, with withInstances, the Instances of every
// namespace. provider only words an error.
func readSlotSnapshot(ctx context.Context, r client.Reader, provider string, withInstances bool) (slotSnapshot, error) {
	var s slotSnapshot
	var al v1alpha1.AllocationList
	if err := r.List(ctx, &al); err != nil {
		return slotSnapshot{}, fmt.Errorf("list Allocations for Provider %s's submit limit: %w", provider, err)
	}
	for i := range al.Items {
		s.allocs = append(s.allocs, &al.Items[i])
	}
	if !withInstances {
		return s, nil
	}
	var il v1alpha1.InstanceList
	if err := r.List(ctx, &il); err != nil {
		return slotSnapshot{}, fmt.Errorf("list Instances for Provider %s's reserved slots: %w", provider, err)
	}
	for i := range il.Items {
		s.insts = append(s.insts, &il.Items[i])
	}
	return s, nil
}

// providerAllocations returns the snapshot's Allocations whose providerConfigRef names the Provider: seen holds all of
// them, listed those that hold a slot (holdsSlot), and allocs the Allocations themselves, all by namespace/name. A
// deleting Allocation counts until its deletion completes.
func (s slotSnapshot) providerAllocations(provider string) (seen, listed map[string]bool, allocs map[string]*v1alpha1.Allocation) {
	seen, listed, allocs = map[string]bool{}, map[string]bool{}, map[string]*v1alpha1.Allocation{}
	for _, a := range s.allocs {
		if ref := a.Spec.ProviderConfigReference; ref == nil || ref.Name != provider {
			continue
		}
		k := a.Namespace + "/" + a.Name
		seen[k] = true
		allocs[k] = a
		if holdsSlot(a) {
			listed[k] = true
		}
	}
	return seen, listed, allocs
}

// onProvider returns the snapshot's Instances that carry the Provider's label, in the list's order.
func (s slotSnapshot) onProvider(provider string) []*v1alpha1.Instance {
	var out []*v1alpha1.Instance
	for _, in := range s.insts {
		if in.Labels[v1alpha1.LabelProvider] == provider {
			out = append(out, in)
		}
	}
	return out
}

// deploymentView returns one deployment's Instances on every Provider and its Allocations by name, the snapshot's by the
// model-deployment label in the namespace (the view the selection reads, #616 PR 4). An empty label has no deployment to
// read: its view is empty.
func (s slotSnapshot) deploymentView(namespace, label string) deploymentView {
	v := deploymentView{key: namespace + "/" + label, allocs: map[string]*v1alpha1.Allocation{}}
	if label == "" {
		return v
	}
	for _, in := range s.insts {
		if in.Namespace == namespace && in.Labels[v1alpha1.LabelModelDeployment] == label {
			v.insts = append(v.insts, in)
		}
	}
	for _, a := range s.allocs {
		if a.Namespace == namespace && a.Labels[v1alpha1.LabelModelDeployment] == label {
			v.allocs[a.Name] = a
		}
	}
	return v
}

// deploymentViews returns, for each deployment with an Instance among onProvider, its view (deploymentView), in the order
// of the deployments' first Instance in onProvider (#617; shared with #616's selection, which reads both). An Instance of
// onProvider that its deployment's view does not show (one with no label) is kept as listed, so the reserved count never
// reads fewer Instances than the Provider's own.
func (s slotSnapshot) deploymentViews(onProvider []*v1alpha1.Instance) []deploymentView {
	at := map[string]int{}
	var out []deploymentView
	for _, in := range onProvider {
		label := in.Labels[v1alpha1.LabelModelDeployment]
		key := in.Namespace + "/" + label
		if label == "" {
			key = in.Namespace + "//" + in.Name // no deployment to read: a view of itself
		}
		j, ok := at[key]
		if !ok {
			j = len(out)
			at[key] = j
			v := s.deploymentView(in.Namespace, label)
			v.key = key
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
	return out
}
