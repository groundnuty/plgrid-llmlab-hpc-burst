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
//
// Both tests, and every case in them, run in parallel (#282): they are the
// package's slowest CPU-bound tests, walking hours of the manual clock in 10 s
// rounds, and the package stands close to go test's 10-minute timeout. Each
// case's rig has its own fake client, clock, fake Slurm and temporary directory;
// a rig opens no port, starts no process and sets no environment variable. Go
// resumes a top-level parallel test only once every sequential test has ended,
// so the package's control planes, and the tests that swap newLeaseLock, are
// gone by then. These are the package's only parallel tests: one that joins them
// must keep all of this true.

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

// first is the end the live steps create the deployment with: the guard's, from the clock read before it.
func (g *twoJobGuard) first(rg *rig) time.Time {
	e, _, err := g.g.Next(alpha.Look{Clock: rg.clock.Now()})
	if err != nil {
		g.t.Fatal(err)
	}
	return e
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

// stoppingGuard is the live steps' guard until the step dies (when says so, before a round), and nothing after:
// the deployment keeps the last end the guard set (last).
type stoppingGuard struct {
	inner   *twoJobGuard
	when    func(rg *rig) bool
	stopped bool
	last    time.Time
}

func (g *stoppingGuard) first(rg *rig) time.Time { return g.inner.first(rg) }

func (g *stoppingGuard) before(rg *rig) {
	if !g.stopped && g.when(rg) {
		g.stopped = true
		if md := rg.md(alphaGuardMD); md.Spec.EndTime != nil {
			g.last = md.Spec.EndTime.Time
		}
	}
	if !g.stopped {
		g.inner.before(rg)
	}
}

// guardRun is what a run showed: every Allocation the deployment ever had (by name: when it was first seen
// and when its job started, and the round it was first seen in), whether a successor became ready, the
// deployment's end at the run's end, and the run's jobs that the fake Slurm still shows alive then.
type guardRun struct {
	allocs    map[string]string
	firstSeen map[string]time.Time
	jobs      map[string]string
	succReady bool
	end       time.Time
	liveJobs  []string
}

// runGuarded runs the case with the guard, in 10 s rounds with the guard before each, until 10 minutes past
// the deployment's end, or, with untilThird, until a third Allocation is created (the control, whose run
// never ends: each successor gets its own replacement).
func runGuarded(t *testing.T, c guardCase, untilThird bool, guard func(rg *rig) endGuard) guardRun {
	t.Helper()
	rg, md := alphaGuardRig(t, c)
	g := guard(rg)
	// The live steps create the deployment with the guard's first end, from the clock read before it.
	if fg, ok := g.(interface{ first(*rig) time.Time }); ok {
		mt := metav1.NewTime(fg.first(rg))
		md.Spec.EndTime = &mt
	}
	rg.create(md)
	r := guardRun{allocs: map[string]string{}, firstSeen: map[string]time.Time{}, jobs: map[string]string{}}
	limit := rigStart.Add(c.firstWait + c.succWait + 5*time.Hour)
	for rg.clock.Now().Before(limit) {
		g.before(rg)
		rg.clock.Advance(10 * time.Second)
		rg.Step()
		for _, a := range rg.allocations(alphaGuardMD) {
			if _, ok := r.firstSeen[a.Name]; !ok {
				r.firstSeen[a.Name] = rg.clock.Now()
			}
			if id := a.Status.AtProvider.JobID; id != "" {
				r.jobs[a.Name] = id
			}
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
			for alloc, id := range r.jobs {
				if j, ok := rg.batch.Job(id); !ok || j.EndTime == nil {
					r.liveJobs = append(r.liveJobs, alloc)
				}
			}
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
	t.Parallel()
	dw, limit := 15*time.Minute, 106*time.Minute
	for _, c := range []guardCase{
		{name: "no-overlap, a 90 min first wait", row: "alpha-helios-nooverlap", firstWait: 90 * time.Minute, noOverlapQueue: true},
		{name: "no-overlap, a 61 min first wait", row: "alpha-helios-nooverlap", firstWait: 61 * time.Minute, noOverlapQueue: true},
		{name: "handover, a 90 min first wait, the successor starting after the drain start", row: "alpha-athena-handover", firstWait: 90 * time.Minute, succWait: 95 * time.Minute},
		{name: "handover, a 61 min first wait, the successor starting after the drain start", row: "alpha-athena-handover", firstWait: 61 * time.Minute, succWait: 95 * time.Minute},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
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

// The step dies mid-wait (the science agent's requirement on PR 255): the guard stops patching, at three
// moments of the 90-minute cases. The deployment then ends at the last end the guard set, which is no later
// than the drain start of any job not yet started, so the controller never creates a third Allocation, nor any
// after the end, and it ends every job of the run (the row itself fails: hack/livechecks shows the script's
// FAIL and its cleanup by the run's prefix).
func TestADeadStepLeavesTheDeploymentAtItsLastEnd(t *testing.T) {
	t.Parallel()
	dw, limit := 15*time.Minute, 106*time.Minute
	started := func(rg *rig) int {
		n := 0
		for _, a := range rg.allocations(alphaGuardMD) {
			if a.Status.AtProvider.StartTime != nil {
				n++
			}
		}
		return n
	}
	for _, c := range []struct {
		name string
		gc   guardCase
		when func(rg *rig) bool
	}{
		{"while the first job pends", guardCase{row: "alpha-athena-handover", firstWait: 90 * time.Minute, succWait: 95 * time.Minute},
			func(rg *rig) bool { return !rg.clock.Now().Before(rigStart.Add(30 * time.Minute)) }},
		{"as the predecessor starts", guardCase{row: "alpha-athena-handover", firstWait: 90 * time.Minute, succWait: 95 * time.Minute},
			func(rg *rig) bool { return started(rg) > 0 }},
		{"while the successor waits for the GPU", guardCase{row: "alpha-helios-nooverlap", firstWait: 90 * time.Minute, noOverlapQueue: true},
			func(rg *rig) bool { return len(rg.allocations(alphaGuardMD)) == 2 && started(rg) == 1 }},
		{"while the successor waits in the queue", guardCase{row: "alpha-athena-handover", firstWait: 90 * time.Minute, succWait: 95 * time.Minute},
			func(rg *rig) bool { return len(rg.allocations(alphaGuardMD)) == 2 && started(rg) == 1 }},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var sg *stoppingGuard
			r := runGuarded(t, c.gc, false, func(rg *rig) endGuard {
				sg = &stoppingGuard{inner: &twoJobGuard{t: t, g: &alpha.TwoJobGuard{TimeLimit: limit, DrainWindow: dw}}, when: c.when}
				return sg
			})
			if !sg.stopped {
				t.Fatal("the step never died: the case does not test what it names")
			}
			if !r.end.Equal(sg.last) {
				t.Errorf("the deployment ended at %s, want the guard's last end %s", r.end.Format(time.TimeOnly), sg.last.Format(time.TimeOnly))
			}
			if len(r.allocs) > 2 {
				t.Errorf("%d Allocations, want at most 2: %v", len(r.allocs), r.allocs)
			}
			for name, at := range r.firstSeen {
				if at.After(r.end) {
					t.Errorf("Allocation %s was created at %s, after the deployment's end %s", name, at.Format(time.TimeOnly), r.end.Format(time.TimeOnly))
				}
			}
			if len(r.liveJobs) > 0 {
				t.Errorf("jobs of %v are alive 10 minutes after the deployment's end", r.liveJobs)
			}
		})
	}
}
