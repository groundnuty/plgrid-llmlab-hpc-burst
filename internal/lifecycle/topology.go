package lifecycle

import (
	"context"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	// limits are the QOS limits that apply to the Provider's Allocations:
	// its partition's qos and its own qos, in Slurm's order
	// (v1alpha1.EffectiveQOSLimits; HBEP-4, QOS limits).
	limits v1alpha1.QOSLimits
	// tooShort, when set, says why the Provider's time limit cannot host an
	// Instance: it is not longer than the lead an Instance needs, so the
	// Provider takes no new Allocation (Degraded, PartitionTooShort; HBEP-7,
	// Provisioning, A partition too short).
	tooShort string
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
		pv.settings = s
		if err != nil {
			pv.problem = fmt.Sprintf("Provider %s: %v", n, err)
			continue
		}
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
		limits, err := v1alpha1.EffectiveQOSLimits(&prof.Spec, pv.partition, p.Spec.QOS)
		if err != nil {
			pv.problem = fmt.Sprintf("Provider %s: PlatformProfile %s: %v", n, prof.Name, err)
			continue
		}
		pv.limits = limits
		if !v1alpha1.ProviderReady(p, r.Clock.Now()) {
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
// error: a GPU count above the smaller of gpusPerNode and the QOS limits'
// maxGpusPerUser (refused, never capped, since it is the engine's parallel
// size), or a profile without the per-GPU values or a time limit. CPUs and
// memory, gpus times cpusPerGpu and memPerGpu, are capped at the QOS limits'
// maxCpusPerUser and maxMemPerUser: they are an allotment in proportion, so a
// cap shrinks them and leaves the GPU count as it is (HBEP-4, QOS limits).
func partitionSizing(pv *providerView, gpus int32, leaseLimit *v1alpha1.Duration) (v1alpha1.BatchParameters, error) {
	part := pv.partition
	maxGPUs, known := v1alpha1.MaxAllocationGPUs(part, pv.limits)
	if !known || part.CPUsPerGPU == nil || part.MemPerGPU == nil {
		return v1alpha1.BatchParameters{}, fmt.Errorf("PlatformProfile %s does not give gpusPerNode (nor does a QOS maxGpusPerUser), cpusPerGpu and memPerGpu for partition %s", pv.profile.Name, part.Name)
	}
	if gpus > maxGPUs {
		// An effective maxGpusPerUser of 0 refuses every GPU count here: the
		// Provider cannot host the Instance (`@hpc-burst-science-agent`,
		// 1 Nov 2026, from PR 1's review).
		if q := pv.limits.MaxGPUsPerUser; q != nil && (part.GPUsPerNode == nil || *q < *part.GPUsPerNode) {
			return v1alpha1.BatchParameters{}, refuse("the serving spec needs %d GPUs (tensor-parallel-size times pipeline-parallel-size), above the %s %d of QOS %s on Provider %s (partition %s); a parallel size is refused, never capped (HBEP-7, Provisioning)", gpus, v1alpha1.LimitMaxGPUsPerUser, *q, pv.limits.From[v1alpha1.LimitMaxGPUsPerUser], pv.name, part.Name)
		}
		return v1alpha1.BatchParameters{}, refuse("the serving spec needs %d GPUs (tensor-parallel-size times pipeline-parallel-size), above the %d per node of partition %s; alpha Instances are single-node (HBEP-7, Provisioning)", gpus, *part.GPUsPerNode, part.Name)
	}
	lim, _, err := allocationTimeLimit(pv, leaseLimit)
	if err != nil {
		return v1alpha1.BatchParameters{}, err
	}
	cpus := gpus * *part.CPUsPerGPU
	if q := pv.limits.MaxCPUsPerUser; q != nil && cpus > *q {
		if *q < 1 {
			return v1alpha1.BatchParameters{}, fmt.Errorf("QOS %s on Provider %s sets %s %d, which leaves an Allocation no CPU", pv.limits.From[v1alpha1.LimitMaxCPUsPerUser], pv.name, v1alpha1.LimitMaxCPUsPerUser, *q)
		}
		cpus = *q
	}
	mem := part.MemPerGPU.DeepCopy()
	mem.Mul(int64(gpus))
	if q := pv.limits.MaxMemPerUser; q != nil && mem.Cmp(*q) > 0 {
		if q.Sign() <= 0 {
			// --mem=0 asks Slurm for a node's whole memory: never send it.
			return v1alpha1.BatchParameters{}, fmt.Errorf("QOS %s on Provider %s sets %s %s, which leaves an Allocation no memory", pv.limits.From[v1alpha1.LimitMaxMemPerUser], pv.name, v1alpha1.LimitMaxMemPerUser, q.String())
		}
		mem = q.DeepCopy()
	}
	return v1alpha1.BatchParameters{
		Partition: part.Name, TimeLimit: formatMinutes(lim), GPUs: gpus,
		CPUsPerTask: cpus, Memory: mem, Nodes: 1, AgentDigest: pv.agentDigest,
	}, nil
}

// allocationTimeLimit is an Allocation's time limit on the Provider and the
// bound that set it: the smallest of the partition's maxTime, the QOS limits'
// maxWall and the deployment's leaseLimit, in whole minutes
// (v1alpha1.AllocationTimeLimit; HBEP-4, QOS limits; HBEP-7, Provisioning).
func allocationTimeLimit(pv *providerView, leaseLimit *v1alpha1.Duration) (time.Duration, v1alpha1.TimeLimitBound, error) {
	part := pv.partition
	var maxT, lease *time.Duration
	if part.MaxTime != nil {
		d, err := part.MaxTime.AsDuration()
		if err != nil {
			return 0, "", fmt.Errorf("partition %s maxTime: %w", part.Name, err)
		}
		maxT = &d
	}
	if leaseLimit != nil {
		d, err := leaseLimit.AsDuration()
		if err != nil {
			return 0, "", refuse("leaseLimit %q: %v", *leaseLimit, err)
		}
		lease = &d
	}
	lim, bound, ok := v1alpha1.AllocationTimeLimit(maxT, pv.limits.MaxWall, lease)
	if !ok {
		return 0, "", fmt.Errorf("partition %s has no maxTime, its QOS no maxWall and the deployment no leaseLimit of a minute or more", part.Name)
	}
	return lim, bound, nil
}
