package lifecycle

// The alpha two-job rows' guard (internal/alpha, TwoJobGuard) against this
// controller, under long queue waits (review of PR 255, B5; the science
// agent's ruling on PR 255: "a test shows that no third Allocation is ever
// created"). The guard is the live steps' own: it looks at the cluster (the
// rig's clock, then the run's jobs in the fake Slurm) and sets the
// deployment's end it gives, and it looks only between rounds, never inside
// one, so a job's start, the controller's first sight of it and the plan of
// its replacement can all fall in one round, as in a live pass. Each case is
// also run with the guard the review found losing that race (an end set only
// once the successor has a lease end, the run's lastJob of 0baf5a7), which
// must create a third Allocation: the case reaches the race, so the guard's
// pass is not one that a short wait would give anyway.

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// alphaGuardMD is the run's deployment, named as a run names it.
const alphaGuardMD = "alpha-0a1b2c3d4e"

// endGuard sets the deployment's end before a round.
type endGuard interface {
	before(rg *rig)
}

// twoJobGuard is the live steps' guard: a look (the cluster's clock, then the run's jobs), then the end it gives.
type twoJobGuard struct {
	t *testing.T
	g *alpha.TwoJobGuard
}

func (g *twoJobGuard) look(rg *rig) alpha.Look {
	l := alpha.Look{Clock: rg.clock.Now()}
	for _, a := range rg.allocations(alphaGuardMD) {
		js, err := rg.batch.FindByTag(context.Background(), v1alpha1.JobTag(a.Namespace, a.Name, a.UID), rigStart.Add(-time.Hour))
		if err != nil {
			g.t.Fatal(err)
		}
		l.Jobs = append(l.Jobs, js...)
	}
	return l
}

func (g *twoJobGuard) before(rg *rig) {
	end, _, err := g.g.Next(g.look(rg))
	if err != nil {
		g.t.Fatalf("at %s the guard stopped the run: %v", rg.clock.Now().Format(time.TimeOnly), err)
	}
	setEnd(rg, end)
}

// leaseEndGuard is the guard of 0baf5a7 (guardSuccessor and lastJob): it keeps the predecessor as it first saw
// it with a lease end, sets no end until the predecessor's successor has a lease end, then sets the successor's
// drain start (EndOfTwo).
type leaseEndGuard struct {
	dw       time.Duration
	pred     string
	predDS   time.Time
	finished bool
}

func (g *leaseEndGuard) before(rg *rig) {
	if g.finished {
		return
	}
	for _, in := range rg.instances(alphaGuardMD) {
		if g.pred == "" && in.Annotations[v1alpha1.AnnotationPredecessor] == "" && in.Status.LeaseEnd != nil {
			g.pred, g.predDS = in.Name, in.Status.LeaseEnd.Add(-g.dw)
		}
	}
	if g.pred == "" {
		return
	}
	for _, in := range rg.instances(alphaGuardMD) {
		if in.Annotations[v1alpha1.AnnotationPredecessor] == g.pred && in.Status.LeaseEnd != nil {
			if end, err := alpha.EndOfTwo(g.predDS, in.Status.LeaseEnd.Add(-g.dw)); err == nil {
				setEnd(rg, end)
				g.finished = true
			}
		}
	}
}

func setEnd(rg *rig, end time.Time) {
	rg.t.Helper()
	md := rg.md(alphaGuardMD)
	if md.Spec.EndTime != nil && md.Spec.EndTime.Time.Equal(end) {
		return
	}
	rg.mutateMD(alphaGuardMD, func(md *v1alpha1.ModelDeployment) { e := metav1.NewTime(end); md.Spec.EndTime = &e })
}

// guardCase is one run: the row, the first job's queue wait, the successor's own (0 for none), and whether the
// Provider has Helios's one-GPU QOS (no-overlap mode's queued path).
type guardCase struct {
	name, row      string
	firstWait      time.Duration
	succWait       time.Duration
	noOverlapQueue bool
}

// The live profiles' times (profiles/helios.yaml, profiles/athena.yaml): a 15 min drain window (the 5 min
// request deadline and the 10 min drainMargin, under the 2 h 10 min drainWindow), a 30 min handoverMargin;
// the rows' 1 h 46 min lease, so 1 h 31 min from a start to its drain start, and 61 min from a start to its
// drain start less the handoverMargin. A first wait of 61 min or more (with start-to-ready above 0) or 90 min
// makes a successor's replacement due the moment it starts (review of PR 255: Opus's 61 min, Astra's 90 min).
// The deployment is the row's (alpha.Row.Plan), as the live steps create it.
func alphaGuardRig(t *testing.T, c guardCase) (*rig, *v1alpha1.ModelDeployment) {
	row, ok := alpha.RowByName(c.row)
	if !ok {
		t.Fatalf("no row %s", c.row)
	}
	opts := []rigOption{withProfile(func(p *v1alpha1.PlatformProfileSpec) {
		p.DrainWindow, p.DrainMargin, p.HandoverMargin = dur(130*time.Minute), dur(10*time.Minute), dur(30*time.Minute)
	})}
	if c.noOverlapQueue {
		// Helios's shape: the partition's QOS allows 2,000 jobs, the Provider's QOS (now) one GPU.
		qos := []v1alpha1.QOSProfile{{Name: "example-plgrid", MaxSubmitPerUser: i32(2000)}, {Name: "example-now", MaxSubmitPerUser: i32(1), MaxGPUsPerUser: i32(1)}}
		opts = append(opts, qosOpts(qos, "example-plgrid", "example-now")...)
		opts = append(opts, withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser, c.MaxGPUsPerUser = 2000, 1 }))
	}
	rg := newRig(t, opts...)
	rg.batch.Enqueue(pfake.Script{QueueWait: c.firstWait}, pfake.Script{QueueWait: c.succWait})
	md := mdSpec(alphaGuardMD)
	plan := row.Plan()
	md.Spec.LeaseLimit = dur(plan.LeaseLimit)
	md.Spec.Routing = &v1alpha1.Routing{MaxRequestDuration: dur(plan.MaxRequestDuration)}
	if plan.ReleaseWhenIdle {
		md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(plan.IdleLimit)}
	}
	return rg, md
}

// guardRun is what a run showed: every Allocation the deployment ever had (by name: when it was first seen
// and when its job started), whether a successor became ready, and the deployment's end at the run's end.
type guardRun struct {
	allocs    map[string]string
	succReady bool
	end       time.Time
}

// runGuarded runs the case with the guard, in 10 s rounds with the guard before each, until 10 minutes past
// the deployment's end, or, with untilThird, until a third Allocation is created (the control, whose run
// never ends: each successor gets its own replacement).
func runGuarded(t *testing.T, c guardCase, untilThird bool, guard func(rg *rig) endGuard) guardRun {
	t.Helper()
	rg, md := alphaGuardRig(t, c)
	g := guard(rg)
	// The live steps create the deployment with the guard's first end, from the clock read before it.
	if tg, ok := g.(*twoJobGuard); ok {
		e, _, err := tg.g.Next(alpha.Look{Clock: rg.clock.Now()})
		if err != nil {
			t.Fatal(err)
		}
		mt := metav1.NewTime(e)
		md.Spec.EndTime = &mt
	}
	rg.create(md)
	r := guardRun{allocs: map[string]string{}}
	limit := rigStart.Add(c.firstWait + c.succWait + 5*time.Hour)
	for rg.clock.Now().Before(limit) {
		g.before(rg)
		rg.clock.Advance(10 * time.Second)
		rg.Step()
		for _, a := range rg.allocations(alphaGuardMD) {
			seen := "seen " + rg.clock.Now().Format(time.TimeOnly)
			if st := a.Status.AtProvider.StartTime; st != nil {
				seen += ", started " + st.Format(time.TimeOnly)
			}
			if old, ok := r.allocs[a.Name]; !ok || (!strings.Contains(old, "started") && strings.Contains(seen, "started")) {
				if ok {
					seen = old + "; " + seen
				}
				r.allocs[a.Name] = seen
			}
		}
		for _, in := range rg.instances(alphaGuardMD) {
			if in.Annotations[v1alpha1.AnnotationPredecessor] != "" && in.Status.Phase == v1alpha1.InstanceReady {
				r.succReady = true
			}
		}
		if untilThird && len(r.allocs) >= 3 {
			return r
		}
		if cur := rg.md(alphaGuardMD); cur.Spec.EndTime != nil && !rg.clock.Now().Before(cur.Spec.EndTime.Add(10*time.Minute)) {
			r.end = cur.Spec.EndTime.Time
			return r
		}
	}
	for _, a := range rg.allocations(alphaGuardMD) {
		t.Logf("allocation %s: phase %s start %v end %v", a.Name, a.Status.AtProvider.Phase, a.Status.AtProvider.StartTime, a.Status.AtProvider.EndTime)
	}
	for _, in := range rg.instances(alphaGuardMD) {
		t.Logf("instance %s: pred %q phase %s leaseEnd %v", in.Name, in.Annotations[v1alpha1.AnnotationPredecessor], in.Status.Phase, in.Status.LeaseEnd)
	}
	if cur := rg.md(alphaGuardMD); cur.Spec.EndTime != nil {
		t.Logf("md end %s; conditions %+v", cur.Spec.EndTime.Format(time.TimeOnly), cur.Status.Conditions)
	}
	t.Fatalf("the deployment did not reach its end by %s (a guarded run: %T): %v", limit.Format(time.TimeOnly), g, r.allocs)
	return r
}

func TestTheTwoJobGuardKeepsALongWaitToTwoAllocations(t *testing.T) {
	dw, limit := 15*time.Minute, 106*time.Minute
	for _, c := range []guardCase{
		{name: "no-overlap, a 90 min first wait", row: "alpha-helios-nooverlap", firstWait: 90 * time.Minute, noOverlapQueue: true},
		{name: "no-overlap, a 61 min first wait", row: "alpha-helios-nooverlap", firstWait: 61 * time.Minute, noOverlapQueue: true},
		{name: "handover, a 90 min first wait, the successor starting after the drain start", row: "alpha-athena-handover", firstWait: 90 * time.Minute, succWait: 95 * time.Minute},
		{name: "handover, a 61 min first wait, the successor starting after the drain start", row: "alpha-athena-handover", firstWait: 61 * time.Minute, succWait: 95 * time.Minute},
	} {
		t.Run(c.name, func(t *testing.T) {
			var tg *alpha.TwoJobGuard
			r := runGuarded(t, c, false, func(rg *rig) endGuard {
				tg = &alpha.TwoJobGuard{TimeLimit: limit, DrainWindow: dw}
				return &twoJobGuard{t: t, g: tg}
			})
			if len(r.allocs) > 2 {
				t.Errorf("the guarded run created %d Allocations, want at most 2: %v", len(r.allocs), r.allocs)
			}
			if !r.succReady {
				t.Error("the guarded run's successor never became ready: the end stopped the handover")
			}
			if _, final, err := tg.Next(alpha.Look{Clock: r.end}); err != nil || !final {
				t.Errorf("the guard never reached the successor's drain start (final %v, %v)", final, err)
			}
			// Control: the end set only once the successor has a lease end loses the race here.
			if cr := runGuarded(t, c, true, func(rg *rig) endGuard { return &leaseEndGuard{dw: dw} }); len(cr.allocs) < 3 {
				t.Errorf("the control (an end set at the successor's lease end) created %d Allocations: the case does not reach the race it is for: %v", len(cr.allocs), cr.allocs)
			} else {
				t.Logf("the control created %d Allocations: %v", len(cr.allocs), cr.allocs)
			}
		})
	}
}
