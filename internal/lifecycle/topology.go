package lifecycle

import (
	"context"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// providerView is one allowed Provider as the reconcile pass sees it.
type providerView struct {
	name      string
	prov      *v1alpha1.Provider
	profile   *v1alpha1.PlatformProfile
	settings  settings
	partition v1alpha1.PartitionProfile
	// hasClass is true when the profile names the partition, so class is
	// known.
	hasClass bool
	class    v1alpha1.AcceleratorClass
	// managed is true for a managed platform's Provider, which a served
	// deployment ignores (HBEP-7, Reconcile loop, step 1).
	managed bool
	// problem, when set, says why the Provider takes no new Allocation: it is
	// missing, not Ready, has no partition, or no agent build for its CPU
	// architecture (Degraded, ProviderUnavailable).
	problem     string
	agentDigest string
	// extra marks a Provider the deployment no longer allows but whose
	// Instances or Allocations still exist: it is read for their drain window
	// and retention, and takes nothing new.
	extra bool
}

func (p *providerView) usable() bool { return p.problem == "" && p.hasClass && !p.managed && !p.extra }

// selectedProviderNames returns the allowed Provider names in HBEP-7's order:
// those listed by name, in order, then those only the selector matches, by
// name (HBEP-7, What the controller reads).
func selectedProviderNames(sel v1alpha1.ProviderSelection, all []v1alpha1.Provider) ([]string, error) {
	out := append([]string(nil), sel.Names...)
	seen := map[string]bool{}
	for _, n := range out {
		seen[n] = true
	}
	if sel.Selector != nil {
		s, err := metav1.LabelSelectorAsSelector(sel.Selector)
		if err != nil {
			return nil, fmt.Errorf("providers.selector: %w", err)
		}
		var matched []string
		for i := range all {
			p := &all[i]
			if !seen[p.Name] && s.Matches(labels.Set(p.Labels)) {
				matched = append(matched, p.Name)
				seen[p.Name] = true
			}
		}
		slices.Sort(matched)
		out = append(out, matched...)
	}
	return out, nil
}

// loadProviders resolves the deployment's allowed Providers with their
// profiles and partitions, then the extra ones its objects still name. A
// missing object is a problem of that Provider, not an error of the pass.
func (r *Reconciler) loadProviders(ctx context.Context, md *v1alpha1.ModelDeployment, extra []string) ([]*providerView, error) {
	var all v1alpha1.ProviderList
	if err := r.Client.List(ctx, &all); err != nil {
		return nil, fmt.Errorf("list Providers: %w", err)
	}
	names, err := selectedProviderNames(md.Spec.Providers, all.Items)
	if err != nil {
		return nil, err
	}
	byName := map[string]*v1alpha1.Provider{}
	for i := range all.Items {
		byName[all.Items[i].Name] = &all.Items[i]
	}
	allowed := len(names)
	for _, n := range extra {
		if n != "" && !slices.Contains(names, n) {
			names = append(names, n)
		}
	}
	var out []*providerView
	for i, n := range names {
		pv := &providerView{name: n, extra: i >= allowed}
		out = append(out, pv)
		p, ok := byName[n]
		if !ok {
			pv.problem = "Provider " + n + " does not exist"
			continue
		}
		pv.prov = p
		var prof v1alpha1.PlatformProfile
		if err := r.Client.Get(ctx, client.ObjectKey{Name: p.Spec.PlatformProfileRef.Name}, &prof); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("get PlatformProfile %s: %w", p.Spec.PlatformProfileRef.Name, err)
			}
			pv.problem = fmt.Sprintf("Provider %s names PlatformProfile %s, which does not exist", n, p.Spec.PlatformProfileRef.Name)
			continue
		}
		pv.profile = &prof
		if prof.Spec.ProviderKind == v1alpha1.PlatformManaged {
			pv.managed = true
			continue
		}
		if prof.Spec.ProviderKind != v1alpha1.PlatformBatch {
			pv.problem = fmt.Sprintf("Provider %s is on a %s platform; alpha provisions on batch platforms only", n, prof.Spec.ProviderKind)
			continue
		}
		s, err := resolveSettings(prof.Spec)
		if err != nil {
			pv.problem = fmt.Sprintf("Provider %s: %v", n, err)
			continue
		}
		pv.settings = s
		if len(p.Spec.AllowedPartitions) == 0 {
			pv.problem = fmt.Sprintf("Provider %s allows no partition", n)
			continue
		}
		part := p.Spec.AllowedPartitions[0]
		idx := slices.IndexFunc(prof.Spec.Partitions, func(pp v1alpha1.PartitionProfile) bool { return pp.Name == part })
		if idx < 0 {
			pv.problem = fmt.Sprintf("Provider %s allows partition %s, which PlatformProfile %s does not describe", n, part, prof.Name)
			continue
		}
		pv.partition = prof.Spec.Partitions[idx]
		pv.class, pv.hasClass = pv.partition.AcceleratorClass, true
		if !providerReady(p) {
			pv.problem = fmt.Sprintf("Provider %s is not Ready", n)
			continue
		}
		digest, err := r.Config.AgentDigest(pv.class.CPUArch)
		if err != nil {
			pv.problem = fmt.Sprintf("Provider %s: no allocation agent build for %s: %v", n, pv.class.CPUArch, err)
			continue
		}
		pv.agentDigest = digest
	}
	return out, nil
}

func providerReady(p *v1alpha1.Provider) bool {
	return apimeta.IsStatusConditionTrue(p.Status.Conditions, v1alpha1.ConditionProviderReady)
}

// classes returns the accelerator classes the allowed batch partitions offer,
// in the order of their first Provider.
func classes(pvs []*providerView) []v1alpha1.AcceleratorClass {
	var out []v1alpha1.AcceleratorClass
	for _, p := range pvs {
		if p.hasClass && !p.managed && !slices.Contains(out, p.class) {
			out = append(out, p.class)
		}
	}
	return out
}

// partitionSizing returns an Allocation's submission parameters on a Provider
// for an Instance of gpus GPUs (HBEP-7, Provisioning), or an *unsupported
// error: a GPU count above gpusPerNode, or a profile without the per-GPU
// values or a time limit.
func partitionSizing(pv *providerView, gpus int32, leaseLimit *v1alpha1.Duration) (v1alpha1.BatchParameters, error) {
	part := pv.partition
	if part.GPUsPerNode == nil || part.CPUsPerGPU == nil || part.MemPerGPU == nil {
		return v1alpha1.BatchParameters{}, fmt.Errorf("PlatformProfile %s does not give gpusPerNode, cpusPerGpu and memPerGpu for partition %s", pv.profile.Name, part.Name)
	}
	if gpus > *part.GPUsPerNode {
		return v1alpha1.BatchParameters{}, refuse("the serving spec needs %d GPUs (tensor-parallel-size times pipeline-parallel-size), above the %d per node of partition %s; alpha Instances are single-node (HBEP-7, Provisioning)", gpus, *part.GPUsPerNode, part.Name)
	}
	var maxT, lease *time.Duration
	if part.MaxTime != nil {
		d, err := part.MaxTime.AsDuration()
		if err != nil {
			return v1alpha1.BatchParameters{}, fmt.Errorf("partition %s maxTime: %w", part.Name, err)
		}
		maxT = &d
	}
	if leaseLimit != nil {
		d, err := leaseLimit.AsDuration()
		if err != nil {
			return v1alpha1.BatchParameters{}, refuse("leaseLimit %q: %v", *leaseLimit, err)
		}
		lease = &d
	}
	lim, ok := timeLimit(maxT, lease)
	if !ok {
		return v1alpha1.BatchParameters{}, fmt.Errorf("partition %s has no maxTime and the deployment no leaseLimit of a minute or more", part.Name)
	}
	mem := part.MemPerGPU.DeepCopy()
	mem.Mul(int64(gpus))
	return v1alpha1.BatchParameters{
		Partition: part.Name, TimeLimit: formatMinutes(lim), GPUs: gpus,
		CPUsPerTask: gpus * *part.CPUsPerGPU, Memory: mem, Nodes: 1, AgentDigest: pv.agentDigest,
	}, nil
}
