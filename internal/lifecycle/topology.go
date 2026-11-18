package lifecycle

import (
	"context"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
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
	// hostDialed is whether the controller runs in host-dialed mode, which caps the Provider's submit limit
	// at HostDialedMaxAllocations (hostDialedLimits).
	hostDialed bool
	// jobLimits and scope are set only on a view resolved for a GPU count (forGPUs) of a Provider whose partition
	// sets qosByGpus: the QOS limits of the job's QOS, which size an Allocation, and how the per-user limits that
	// belong to that QOS are counted (qosscope.go). Then limits holds only the Provider-wide ones: the partition
	// QOS's and the host-dialed ceiling. Both are nil for every other Provider, whose limits are as they were.
	jobLimits *v1alpha1.QOSLimits
	scope     *qosScope
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
		partition, err := allowedPartition(p, &prof)
		if err != nil {
			pv.problem = err.Error()
			continue
		}
		pv.partition = partition
		pv.class, pv.hasClass = pv.partition.AcceleratorClass, true
		jobQOS := p.Spec.QOS
		if len(partition.QOSByGPUs) > 0 {
			// The QOS has one source (HBEP-4, The Slurm dialect): the partition's qosByGpus. A Provider that also
			// sets qos is refused by ReadProfile, so no Allocation could be submitted on it: say so here.
			if jobQOS != "" {
				pv.problem = fmt.Sprintf("Provider %s sets qos %q, but partition %s of PlatformProfile %s sets qosByGpus, which chooses the QOS by GPU count: the QOS has one source (HBEP-4, The Slurm dialect)", n, jobQOS, partition.Name, prof.Name)
				continue
			}
			// With no QOS chosen yet, only the partition QOS's limits apply (forGPUs adds the job QOS's).
		}
		limits, err := v1alpha1.EffectiveQOSLimits(&prof.Spec, pv.partition, jobQOS)
		if err != nil {
			pv.problem = fmt.Sprintf("Provider %s: PlatformProfile %s: %v", n, prof.Name, err)
			continue
		}
		pv.limits = limits
		pv.hostDialed = r.Config.HostDialed
		if r.Config.HostDialed {
			pv.limits = hostDialedLimits(limits)
		}
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

// allowedPartition returns the profile's entry for the Provider's partition,
// the first it allows. The error says why there is none, in the words of
// providerView.problem.
func allowedPartition(p *v1alpha1.Provider, prof *v1alpha1.PlatformProfile) (v1alpha1.PartitionProfile, error) {
	if len(p.Spec.AllowedPartitions) == 0 {
		return v1alpha1.PartitionProfile{}, fmt.Errorf("Provider %s allows no partition", p.Name)
	}
	part := p.Spec.AllowedPartitions[0]
	idx := slices.IndexFunc(prof.Spec.Partitions, func(pp v1alpha1.PartitionProfile) bool { return pp.Name == part })
	if idx < 0 {
		return v1alpha1.PartitionProfile{}, fmt.Errorf("Provider %s allows partition %s, which PlatformProfile %s does not describe", p.Name, part, prof.Name)
	}
	return prof.Spec.Partitions[idx], nil
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
// cap shrinks them and leaves the GPU count as it is (HBEP-4, QOS limits). A
// memory request is capped at the maxMemPerUser in whole MiB, rounded down.
func partitionSizing(pv *providerView, gpus int32, leaseLimit *v1alpha1.Duration) (v1alpha1.BatchParameters, error) {
	return partitionSizingFor(pv, gpus, 1, leaseLimit)
}

// partitionSizingFor is partitionSizing for an Instance of workers workers of perWorker GPUs each: the
// Allocation's GPU count is their product, on one node, refused, never capped, above the same limits
// (HBEP-7, Several workers, Sizing). With one worker it is partitionSizing, its refusals word for word.
func partitionSizingFor(pv *providerView, perWorker, workers int32, leaseLimit *v1alpha1.Duration) (v1alpha1.BatchParameters, error) {
	// Each worker's rung and the router's and the whole Instance's must fit the restarts status.restarts
	// keeps, so each part's spent rung stays visible (HBEP-7, Several workers, Health and restarts). With
	// one worker it always fits: (1 + 2) x maxAttemptsPerRung is 24.
	if a := pv.settings.attemptsPerRung; (workers+2)*a > maxRestartsKept {
		return v1alpha1.BatchParameters{}, refuse("workersPerInstance %d on Provider %s, whose profile sets restart.attemptsPerRung %d: (workers + 2) x attempts is %d restarts, above the %d that status.restarts keeps (HBEP-7, Several workers)", workers, pv.name, a, (workers+2)*a, maxRestartsKept)
	}
	gpus := perWorker * workers
	needs := fmt.Sprintf("the serving spec needs %d GPUs (tensor-parallel-size times pipeline-parallel-size)", gpus)
	if workers > 1 {
		needs = fmt.Sprintf("the deployment needs %d GPUs (workersPerInstance %d times %d, tensor-parallel-size times pipeline-parallel-size)", gpus, workers, perWorker)
	}
	// The job's QOS comes first: where the partition sets qosByGpus the GPU count chooses it, and its limits, its
	// 3-day wall among them, are the ones that bound the Allocation (HBEP-4, QOS limits; #745). A count above the
	// list's last entry has no QOS and is refused, never capped.
	pv, err := pv.forGPUs(gpus)
	if err != nil {
		return v1alpha1.BatchParameters{}, refuse("%s, %v (HBEP-4, The Slurm dialect)", needs, err)
	}
	lim := pv.sizingLimits()
	part := pv.partition
	bound, known := v1alpha1.AllocationGPUBound(part, lim)
	if !known || part.CPUsPerGPU == nil || part.MemPerGPU == nil {
		return v1alpha1.BatchParameters{}, fmt.Errorf("PlatformProfile %s does not give gpusPerNode (nor does a QOS maxGpusPerUser), cpusPerGpu and memPerGpu for partition %s", pv.profile.Name, part.Name)
	}
	if gpus > bound.Most {
		// An effective maxGpusPerUser of 0 refuses every GPU count here: the Provider cannot host the Instance
		// (`@hpc-burst-science-agent`, 1 Nov 2026, from PR 1's review).
		switch bound.By {
		case v1alpha1.LimitMaxGPUsPerUser, v1alpha1.LimitMaxGPUsPerJob:
			return v1alpha1.BatchParameters{}, refuse("%s, above the %s %d of QOS %s on Provider %s (partition %s); a parallel size is refused, never capped (HBEP-7, Provisioning)", needs, bound.By, bound.Most, lim.From[bound.By], pv.name, part.Name)
		case v1alpha1.GPUBoundQOSByGPUs:
			return v1alpha1.BatchParameters{}, refuse("%s, above the %d of the last qosByGpus entry of partition %s (HBEP-4, The Slurm dialect)", needs, bound.Most, part.Name)
		}
		return v1alpha1.BatchParameters{}, refuse("%s, above the %d per node of partition %s; alpha Instances are single-node (HBEP-7, Provisioning)", needs, bound.Most, part.Name)
	}
	tl, tlBound, err := allocationTimeLimit(pv, leaseLimit)
	if err != nil {
		return v1alpha1.BatchParameters{}, err
	}
	if tl < time.Minute {
		// A known limit under a minute is PartitionTooShort (checkTimeLimits);
		// no Allocation is ever sized for it, whatever the caller does.
		return v1alpha1.BatchParameters{}, fmt.Errorf("the time limit %s, set by %s on partition %s, is under a minute: Slurm counts whole minutes", tl, tlBound, part.Name)
	}
	// CPUs and memory are capped at the effective per-job and per-user caps (HBEP-4, Submission, GPU sizing; #745).
	cpus := gpus * *part.CPUsPerGPU
	for _, c := range []struct {
		cap  *int32
		name string
	}{{lim.MaxCPUsPerUser, v1alpha1.LimitMaxCPUsPerUser}, {lim.MaxCPUsPerJob, v1alpha1.LimitMaxCPUsPerJob}} {
		if c.cap != nil && cpus > *c.cap {
			if *c.cap < 1 {
				return v1alpha1.BatchParameters{}, fmt.Errorf("QOS %s on Provider %s sets %s %d, which leaves an Allocation no CPU", lim.From[c.name], pv.name, c.name, *c.cap)
			}
			cpus = *c.cap
		}
	}
	mem := part.MemPerGPU.DeepCopy()
	mem.Mul(int64(gpus))
	for _, c := range []struct {
		cap  *resource.Quantity
		name string
	}{{lim.MaxMemPerUser, v1alpha1.LimitMaxMemPerUser}, {lim.MaxMemPerJob, v1alpha1.LimitMaxMemPerJob}} {
		if c.cap == nil {
			continue
		}
		// Slurm counts memory in whole MiB. The provider rounds a request up and
		// a cap down (internal/slurm: a job never gets less than it asked for
		// and never more than the cap), so a request is checked against the cap
		// in whole MiB, rounded down: a request equal to a decimal-unit cap such
		// as 100G rounds up above the cap's 95,367 MiB and would be refused for
		// ever (review of PR 171, Astra B2).
		capMiB := c.cap.Value() >> mibShift
		if capMiB < 1 {
			// --mem=0 asks Slurm for a node's whole memory: never send it.
			return v1alpha1.BatchParameters{}, fmt.Errorf("QOS %s on Provider %s sets %s %s, which leaves an Allocation no memory", lim.From[c.name], pv.name, c.name, c.cap.String())
		}
		if mem.Value() > capMiB<<mibShift {
			mem = *resource.NewQuantity(capMiB<<mibShift, resource.BinarySI)
		}
	}
	return v1alpha1.BatchParameters{
		Partition: part.Name, TimeLimit: formatMinutes(tl), GPUs: gpus,
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
	maxWall := pv.sizingLimits().MaxWall
	lim, bound, ok := v1alpha1.AllocationTimeLimit(maxT, maxWall, lease)
	switch {
	case bound == "":
		// None of the three is set: the profile cannot give a time limit.
		return 0, "", fmt.Errorf("partition %s has no maxTime, its QOS no maxWall and the deployment no leaseLimit", part.Name)
	case !ok:
		// A limit is set but rounds to under a minute (maxWall 30s, say). It is
		// a known, unusably short limit, not an absent one: the caller marks
		// the Provider too short, naming the bound and its raw value (review
		// of PR 171, Astra B3).
		return rawBound(bound, maxT, maxWall, lease), bound, nil
	}
	return lim, bound, nil
}

// rawBound returns the value of the named bound, before rounding.
func rawBound(bound v1alpha1.TimeLimitBound, maxTime, maxWall, leaseLimit *time.Duration) time.Duration {
	var d *time.Duration
	switch bound {
	case v1alpha1.TimeLimitMaxTime:
		d = maxTime
	case v1alpha1.TimeLimitMaxWall:
		d = maxWall
	case v1alpha1.TimeLimitLeaseLimit:
		d = leaseLimit
	}
	if d == nil {
		return 0
	}
	return *d
}

// mibShift converts bytes to MiB.
const mibShift = 20

// memMiBUp is a memory request in the whole MiB Slurm charges for it: rounded up, as the plugin sends it (--mem=<MiB>M,
// internal/slurm checkRequest). The aggregate checks sum these, never raw bytes.
func memMiBUp(q resource.Quantity) int64 { return (q.Value() + (1<<mibShift - 1)) >> mibShift }
