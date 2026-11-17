package lifecycle

// The fixture of the placement-order rows (HBEP-7, Provisioning, Where; #604 PR 3): the rig's Provider A (a 4 h
// partition) beside B, of A's accelerator class, and C, of another one, each with a 12 h partition. A deployment
// first runs alone on B, and on C, and is lowered to no Instance each time, which leaves that Provider's Allocation
// kept as warm capacity (Keep, the default); then its primary goes on A. The warm Allocations' hard ends are 12 h
// after their starts, about 20:00, so each drains hours after the primary's 13:00:20 hard end and is later than it by
// any lead, however it is set (HBEP-7, Hedging (beta), Started losers are kept).
//
// The shared timeline, worked out from handover_test.go's header (start-to-ready 50 s, no queue wait but the jobs'
// own 0 s, handoverMargin 30 min, drain window 2 h 10 min): the primary is made at 09:00:10 and its job starts at
// 09:00:20 with a hard end of 13:00:20, so its drain start is 10:50:20, its replacement is planned at 10:50:20 -
// (0 + 50 s + 30 min) = 10:19:30, and its replacement is late after 10:50:20 - 50 s = 10:49:30. Every expected
// Provider and Allocation is named in the row that expects it, from what the row built, never from the order the
// code computes.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	ordB     = "example-b"
	ordC     = "example-c"
	ordCGPU  = "example-c100"
	ordBSlot = "example-b1"
	ordASlot = "example-a1"
)

// ordRig is the rig with Providers B and C beside A.
type ordRig struct {
	*rig
	b, c *rigExtra
	// warm is the warm Allocation each Provider got from warmUp, by Provider name.
	warm map[string]string
}

// ordOpts are what a row varies: A's and B's submit limits (0: none; the limit is declared in the profile's QOS and
// enforced by the fake Slurm), and whether C exists.
type ordOpts struct {
	aLimit, bLimit int32
	withC          bool
}

func newOrdRig(t *testing.T, o ordOpts, opts ...rigOption) *ordRig {
	t.Helper()
	var qos []v1alpha1.QOSProfile
	ropts := []rigOption{withMaxTime(4 * time.Hour)}
	if o.aLimit > 0 {
		qos = append(qos, v1alpha1.QOSProfile{Name: ordASlot, MaxSubmitPerUser: i32(o.aLimit)})
		ropts = append(ropts, withProviderQOS(ordASlot), withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser = int(o.aLimit) }))
	}
	if o.bLimit > 0 {
		qos = append(qos, v1alpha1.QOSProfile{Name: ordBSlot, MaxSubmitPerUser: i32(o.bLimit)})
	}
	if len(qos) > 0 {
		ropts = append(ropts, withProfile(func(s *v1alpha1.PlatformProfileSpec) { s.QOS = qos }))
	}
	rg := newRig(t, append(ropts, opts...)...)
	r := &ordRig{rig: rg, warm: map[string]string{}}
	r.b = rg.addProvider(ordB, "gpu-b", 12*time.Hour,
		func(pp *v1alpha1.PartitionProfile) {
			if o.bLimit > 0 {
				pp.QOS = ordBSlot
			}
		},
		func(c *pfake.Config) {
			if o.bLimit > 0 {
				c.MaxSubmitPerUser = int(o.bLimit)
			}
		})
	if o.withC {
		r.c = rg.addProvider(ordC, "gpu-c", 12*time.Hour,
			func(pp *v1alpha1.PartitionProfile) { pp.AcceleratorClass.GPUModel = ordCGPU },
			func(c *pfake.Config) { c.Partitions[0].GPUModel = ordCGPU })
	}
	return r
}

// ordMD is the deployment of the rows: one hedge member at most (hedgeWidth 1, so no growth adds a member unless the
// row is about one), no Instance yet, on no Provider; the row names them.
func ordMD(name string) *v1alpha1.ModelDeployment {
	md := mdSpec(name)
	md.Spec.HedgeWidth = ptrTo(int32(1))
	md.Spec.Instances = 0
	return md
}

// allowOn sets the deployment's Providers, in order, and how many Instances it wants.
func (o *ordRig) allowOn(md string, instances int32, provs ...string) {
	o.t.Helper()
	o.mutateMD(md, func(m *v1alpha1.ModelDeployment) {
		m.Spec.Providers.Names = provs
		m.Spec.Instances = instances
	})
}

// warmUp runs the deployment alone on the Provider until its Instance is ready, then lowers it to no Instance, which
// leaves the Allocation kept and warm, and returns that Allocation's name. It fails when the Provider does not end
// with exactly one kept Allocation of the deployment, or when its job did not run: the row would prove nothing.
func (o *ordRig) warmUp(md, prov string) string {
	o.t.Helper()
	o.allowOn(md, 1, prov)
	o.until(10*time.Minute, 10*time.Second, "ready on "+prov, func() bool { return o.readyCount(md) == 1 })
	o.allowOn(md, 0, prov)
	o.until(10*time.Minute, 10*time.Second, "scaled to zero", func() bool { return len(o.liveInstances(md)) == 0 })
	var kept []string
	for _, a := range o.allocations(md) {
		if a.Labels[v1alpha1.LabelProvider] == prov && a.DeletionTimestamp.IsZero() {
			kept = append(kept, a.Name)
		}
	}
	if len(kept) != 1 {
		o.t.Fatalf("%d kept Allocations on %s after its warm-up, want exactly one: %v", len(kept), prov, kept)
	}
	if a, _ := o.allocation(kept[0]); a == nil || a.Status.AtProvider.Phase != v1alpha1.AllocationRunning || a.Status.AtProvider.HardEnd == nil {
		o.t.Fatalf("the warm Allocation %s of %s is not a running one with a hard end: %+v", kept[0], prov, a)
	}
	o.warm[prov] = kept[0]
	return kept[0]
}

// primaryOnA puts the deployment's primary on A at 09:00:10 and steps to its ready time, 09:01:10, with lease end
// 13:00:20 (the shared timeline), then names the Providers the deployment allows from here on, in order. Both the
// primary and its Allocation are returned. The warm-ups must be done before 09:00.
func (o *ordRig) primaryOnA(md string, provs ...string) *v1alpha1.Instance {
	o.t.Helper()
	if !o.clock.Now().Before(hoT(9, 0, 0)) {
		o.t.Fatalf("the warm-ups ended at %s, after 09:00:00", o.clock.Now().Format(time.TimeOnly))
	}
	hoStepTo(o.rig, hoT(9, 0, 0), 5*time.Minute, nil)
	o.allowOn(md, 1, rigProvider)
	hoStepTo(o.rig, hoT(9, 1, 20), 10*time.Second, nil)
	live := o.liveInstances(md)
	if len(live) != 1 || live[0].Status.Phase != v1alpha1.InstanceReady || live[0].Labels[v1alpha1.LabelProvider] != rigProvider ||
		live[0].Status.LeaseEnd == nil || !live[0].Status.LeaseEnd.Time.Equal(hoT(13, 0, 20)) {
		o.t.Fatalf("want one ready primary on %s with lease end 13:00:20 at 09:01:20, got %+v", rigProvider, live)
	}
	o.allowOn(md, 1, provs...)
	p := live[0]
	return &p
}

// replacementAt steps to just after the primary's replacement time, 10:19:30, and returns the primary's one
// replacement, which the row's order has just placed.
func (o *ordRig) replacementAt(p *v1alpha1.Instance) v1alpha1.Instance {
	o.t.Helper()
	hoStepTo(o.rig, hoT(10, 19, 20), 5*time.Minute, nil)
	hoStepTo(o.rig, hoT(10, 19, 40), 10*time.Second, nil)
	reps := hoRepl(o.rig, p.Name)
	if len(reps) != 1 {
		o.t.Fatalf("%d replacements of %s at 10:19:40, want one (planned 10:19:30)", len(reps), p.Name)
	}
	return reps[0]
}

// subs is the count of sbatch calls each Provider's fake Slurm has received: A, B and C (0 without C).
func (o *ordRig) subs() (a, b, c int) {
	a, b = o.batch.Count("sbatch"), o.b.batch.Count("sbatch")
	if o.c != nil {
		c = o.c.batch.Count("sbatch")
	}
	return a, b, c
}

// where names the Provider an Instance is on and the Allocation it runs in.
func where(in v1alpha1.Instance) (prov, alloc string) {
	return in.Labels[v1alpha1.LabelProvider], in.Spec.AllocationRef.Name
}
