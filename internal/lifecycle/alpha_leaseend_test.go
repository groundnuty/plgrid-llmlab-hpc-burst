package lifecycle

// A moved lease end in front of the real lifecycle controller (#541, the follow-up to #534 and #535).
//
// Slurm moves a running job's end_time later by the time its configuration took: end_time becomes the moment the
// configuration ended plus the time limit, and start_time and time_limit stay as they were (row B's live run of 13 Nov
// 2026 saw one second of it; internal/alpha/jobcount_slurm_test.go reproduces it through the parser into the guard).
// #535's guard derives every end it sets from a job's start plus its time limit (TwoJobGuard.drainStartOf), never from
// the lease end it reads, so that the end is no later than the drain start the controller holds, whichever lease end
// the Provider last read (copyLeaseEnd). The lifecycle tests of alpha_jobcount_test.go only have a lease end of exactly
// start plus the time limit. This test puts a job whose lease end is 3 s later in front of the real controller, with
// the live steps' guard, on the handover row.
//
// How the moved end is made. The fake Slurm (pfake.Batch) has no configuration time: a running job's hard end is its
// start plus its time limit, and the one control that moves it, SetTimeLimit, also lists the longer time limit, which
// the guard refuses (planned). So the test calls SetTimeLimit(limit + 3 s) 5 s after a job's start (the first look after
// it; Slurm's configuration ended 3 s after the start), which moves the job's end and makes the fake job end there, as
// Slurm ends a job at its end_time, and the guard's look lists the planned time limit again, as squeue does.
//
// What the controller can hold. The controller's drain start follows the lease end the Provider last read, and that
// read can be older than the guard's look: the Provider's reads are not tied to the lifecycle controller's passes (a
// gap of 30 s is inside the minute that v1alpha1.ProviderPollInterval allows an idle Provider to go without one).
// A job that starts is first read at its start, before its configuration ends, so its lease end is start plus the limit;
// the guard's look comes after, and lists the moved one. A successor is a replacement, and counts as a primary, which
// gets a replacement of its own, only once its predecessor no longer counts (primaries: draining or ended), so the race
// is open only for a successor that starts after its predecessor's drain start, the row's "successor starting after the
// drain start" shape (a first wait of 61 min and a successor wait of 95 min). The guard's final end is then the
// successor's start plus the limit less the drain window: at or before the drain start of either lease end, so
// servesPast (strictly after) is false and no replacement of the successor is planned. An end derived from the listed
// lease end is 3 s later, so it is after the drain start of the old lease end. With the Provider's next read a round
// after the start (a poll every 10 s) the controller holds the moved lease end before it passes again, and the two
// ends are equal there: the mutant of #541 plans no third Allocation in that case, and only the assertions on the
// deployment's end fail it, since its end is 3 s later than the successor's start + limit - drain window. With a poll
// every 30 s the controller holds the old lease end for two passes with the final end already set: the mutant's end is
// after that drain start, which plans a third Allocation, and that case is the one that reaches the race. A third case has a successor
// that starts at once and is ready before the predecessor drains, the handover in time: the successor is then a
// replacement, the race is not open, and the assertions on the timeline pin the handover's time.
//
// Like alpha_jobcount_test.go's, the cases run in parallel, and a rig opens no port, starts no process and sets no
// environment variable.

import (
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

const (
	// movedConfiguration is how long a job's configuration took: its end_time moves this far past start + time limit.
	movedConfiguration = 3 * time.Second
	// movedLimit and movedDrain are the live profiles' times of alpha_jobcount_test.go: the rows' lease of 1 h 46 min,
	// and the 15 min drain window (the 5 min request deadline and the 10 min drainMargin).
	movedLimit = 106 * time.Minute
	movedDrain = 15 * time.Minute
)

// movedGuard is the live steps' guard (alpha.TwoJobGuard) with a look that lists a job's time limit as planned.
type movedGuard struct {
	t       *testing.T
	g       *alpha.TwoJobGuard
	final   bool
	finalAt time.Time
}

// movedSlurmJobs is the run's jobs in the fake Slurm, by Allocation name, as the fake lists them.
func movedSlurmJobs(rg *rig) map[string]provider.Job {
	rg.t.Helper()
	out := map[string]provider.Job{}
	for _, a := range rg.allocations(alphaGuardMD) {
		js, err := rg.batch.FindByTag(rg.ctx, v1alpha1.JobTag(a.Namespace, a.Name, a.UID), rigStart.Add(-time.Hour))
		if err != nil {
			rg.t.Fatal(err)
		}
		if len(js) == 1 {
			out[a.Name] = js[0]
		}
	}
	return out
}

// look is the guard's look: the cluster's clock, then the run's jobs. The fake lists a moved job's time limit as moved
// (SetTimeLimit); squeue keeps the time limit and moves only end_time, so the look lists the planned one.
func (g *movedGuard) look(rg *rig) alpha.Look {
	l := alpha.Look{Clock: rg.clock.Now()}
	for _, j := range movedSlurmJobs(rg) {
		j.TimeLimit = movedLimit
		l.Jobs = append(l.Jobs, j)
	}
	return l
}

func (g *movedGuard) first(rg *rig) time.Time {
	e, _, err := g.g.Next(alpha.Look{Clock: rg.clock.Now()})
	if err != nil {
		g.t.Fatal(err)
	}
	return e
}

func (g *movedGuard) before(rg *rig) {
	end, final, err := g.g.Next(g.look(rg))
	if err != nil {
		g.t.Fatalf("at %s the guard stopped the run: %v", rg.clock.Now().Format(time.TimeOnly), err)
	}
	if final && !g.final {
		g.final, g.finalAt = true, rg.clock.Now()
	}
	setEnd(rg, end)
}

// movedSlurmConfigures is Slurm's configuration ending for every job that has started and not yet been moved: its end_time
// moves to start + time limit + movedConfiguration. A job is moved at the first look after its start, 5 s after it,
// later than the configuration's 3 s.
func movedSlurmConfigures(rg *rig, moved map[string]bool) {
	rg.t.Helper()
	for _, j := range movedSlurmJobs(rg) {
		if j.StartTime == nil || j.EndTime != nil || moved[j.ID] {
			continue
		}
		if rg.clock.Now().Before(j.StartTime.Add(movedConfiguration)) {
			continue
		}
		rg.batch.SetTimeLimit(j.ID, movedLimit+movedConfiguration)
		moved[j.ID] = true
	}
}

// movedStepWithoutProvider is rig.Step in a round that the Provider's controller does not poll in.
func (rg *rig) movedStepWithoutProvider() {
	rg.t.Helper()
	rg.syncAgents()
	rg.reconcileAll()
	if err := rg.r.checkHeartbeats(rg.ctx); err != nil {
		rg.t.Fatalf("checkHeartbeats: %v", err)
	}
}

// movedSeenInstance is what a run showed of one Instance: its distinct lease ends in order, and the clock of the first
// round that it was ready, draining and ended.
type movedSeenInstance struct {
	leaseEnds                 []time.Time
	readyAt, drainAt, endedAt time.Time
}

// movedRun is what a run showed.
type movedRun struct {
	insts map[string]*movedSeenInstance
	// order is the Instances' names, oldest first: the predecessor, then the successor.
	order []string
	// everAlloc are the Allocations the run ever showed, oldest first, kept after an Allocation is gone; third is set
	// to them when there are more than two, with the clock of the round that showed the third.
	everAlloc []string
	third     []string
	thirdAt   time.Time
	// starts are the jobs' starts in the fake Slurm, by Allocation name; leaseSlacks are the guard's (pred, succ).
	starts      map[string]time.Time
	allocOrder  []string
	finalAt     time.Time
	staleAfter  []time.Time
	end         time.Time
	submissions int
	predSlack   *time.Duration
	succSlack   *time.Duration
}

// runMoved runs the handover row of alphaGuardRig, with a first wait of 61 min and the case's successor wait, at
// overAllocation 0, in rounds of 10 s: the guard's look 5 s after each round, then the Provider's read (every
// w.pollEvery rounds; the others run the agents and the controller only), until 10 minutes past the deployment's end.
func runMoved(t *testing.T, w movedWant) movedRun {
	t.Helper()
	rg, md := alphaGuardRig(t, guardCase{row: "alpha-athena-handover", firstWait: 61 * time.Minute, succWait: w.succWait})
	noOverAllocation(md)
	tg := &alpha.TwoJobGuard{TimeLimit: movedLimit, DrainWindow: movedDrain}
	g := &movedGuard{t: t, g: tg}
	first := metav1.NewTime(g.first(rg))
	md.Spec.EndTime = &first
	rg.create(md)

	r := movedRun{insts: map[string]*movedSeenInstance{}, starts: map[string]time.Time{}}
	moved := map[string]bool{}
	limit := rigStart.Add(61*time.Minute + w.succWait + 5*time.Hour)
	for round := 1; rg.clock.Now().Before(limit); round++ {
		rg.clock.Advance(5 * time.Second)
		movedSlurmConfigures(rg, moved)
		g.before(rg)
		rg.clock.Advance(5 * time.Second)
		if round%w.pollEvery == 0 {
			rg.Step()
		} else {
			rg.movedStepWithoutProvider()
		}
		now := rg.clock.Now()
		for name, j := range movedSlurmJobs(rg) {
			if j.StartTime != nil {
				if _, ok := r.starts[name]; !ok {
					r.allocOrder = append(r.allocOrder, name)
				}
				r.starts[name] = *j.StartTime
			}
		}
		for _, a := range rg.allocations(alphaGuardMD) {
			if !slices.Contains(r.everAlloc, a.Name) {
				r.everAlloc = append(r.everAlloc, a.Name)
			}
		}
		if len(r.everAlloc) > 2 {
			r.third, r.thirdAt = slices.Clone(r.everAlloc), now
			break
		}
		for _, in := range rg.instances(alphaGuardMD) {
			s, ok := r.insts[in.Name]
			if !ok {
				s = &movedSeenInstance{}
				r.insts[in.Name] = s
				r.order = append(r.order, in.Name)
			}
			if le := in.Status.LeaseEnd; le != nil && (len(s.leaseEnds) == 0 || !s.leaseEnds[len(s.leaseEnds)-1].Equal(le.Time)) {
				s.leaseEnds = append(s.leaseEnds, le.Time)
			}
			switch in.Status.Phase {
			case v1alpha1.InstanceReady:
				if s.readyAt.IsZero() {
					s.readyAt = now
				}
			case v1alpha1.InstanceDraining:
				if s.drainAt.IsZero() {
					s.drainAt = now
				}
			case v1alpha1.InstanceEnded:
				if s.endedAt.IsZero() {
					s.endedAt = now
				}
			}
			// The successor holds the lease end from before the move while the guard's final end is set.
			if g.final && in.Annotations[v1alpha1.AnnotationPredecessor] != "" && in.Status.LeaseEnd != nil {
				if st, ok := r.starts[in.Spec.AllocationRef.Name]; ok && in.Status.LeaseEnd.Time.Equal(st.Add(movedLimit)) {
					r.staleAfter = append(r.staleAfter, now)
				}
			}
		}
		if cur := rg.md(alphaGuardMD); g.final && cur.Spec.EndTime != nil && !now.Before(cur.Spec.EndTime.Add(10*time.Minute)) {
			r.end = cur.Spec.EndTime.Time
			break
		}
	}
	r.finalAt = g.finalAt
	r.submissions = rg.submissions()
	r.predSlack, r.succSlack = tg.LeaseSlacks()
	if r.end.IsZero() && r.third == nil {
		t.Fatalf("the deployment did not reach its end by %s: Allocations %v", rg.clock.Now().Format(time.TimeOnly), r.allocOrder)
	}
	return r
}

// movedAt is a clock time of the rig's day.
func movedAt(hms string) time.Time {
	tm, err := time.Parse(time.TimeOnly, hms)
	if err != nil {
		panic(err)
	}
	return time.Date(rigStart.Year(), rigStart.Month(), rigStart.Day(), tm.Hour(), tm.Minute(), tm.Second(), 0, time.UTC)
}

func movedAts(hms ...string) []time.Time {
	var out []time.Time
	for _, h := range hms {
		out = append(out, movedAt(h))
	}
	return out
}

func movedTimesEqual(a, b []time.Time) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

func movedClockList(ts []time.Time) []string {
	out := []string{}
	for _, t := range ts {
		out = append(out, t.Format(time.TimeOnly))
	}
	return out
}

// movedWant is one case's expected timeline, every time worked out by hand and written as a literal (none comes from
// the controller or the guard). The rig's clock starts at 08:00:00 and moves in rounds of 10 s; the guard looks 5 s
// after each round, and the Provider reads in the round itself, every pollEvery rounds. The inputs are the two jobs'
// starts in the fake Slurm, which fall on a round, so a job is first read at its start, 3 s before its configuration
// ends. The first job waits 61 min, and the successor succWait. A replacement is due the moment its primary starts:
// replacementTime is the drain start less the observed wait, the 50 s start-to-ready time and the 30 min handover
// margin, which for a wait of 61 min is 91 min less 91 min 50 s from a start, before the start.
//
// From a start S the lease end is S + 1 h 46 min, and 3 s later once moved; the drain window is 15 min, so a drain
// start is the lease end less 15 min; the guard's final end, the successor's S + 1 h 46 min - 15 min, is S + 1 h 31 min.
// An Instance is first draining in the first round at or after its drain start, and ready 50 s after its job's start
// (the rig's start-to-ready time; handover_test.go's header).
type movedWant struct {
	name      string
	pollEvery int
	// the successor's queue wait; and whether it starts in time to be ready before the predecessor drains.
	succWait time.Duration
	handover bool
	// the jobs' starts, and the clock of the look that makes the guard's end final (the successor's start + 5 s).
	predStart, succStart, finalAt string
	// the lease ends the Instances showed, in order: start + 1 h 46 min, then 3 s later.
	predLease, succLease []string
	// the round the predecessor first drains in (the drain start of its moved lease end, rounded up to a round), and
	// the round it ended in; the round the successor was ready in; and the deployment's end, which is also the round
	// the successor drains in: the deployment ends before the successor's drain start.
	predDrain, predEnded, succReady, end string
	// the rounds in which the successor held its old lease end beside the final end.
	stale []string
}

// The first case: the Provider reads every round (10 s). The successor's start is read at its start, the lease end not
// moved yet, and the round after holds the moved lease end, before the controller passes again; a final end set from the
// listed lease end would equal the drain start and plan nothing. The mutant of #541 plans no third Allocation here, and
// is failed by the assertions on the deployment's end (12:07:43 for 12:07:40) and on the successor's drain.
//
//	pred: starts 09:01:20, which is 08:00:20 (the rig's first start with no wait; handover_test.go's header) + 61 min.
//	      Lease end 09:01:20 + 1:46:00 = 10:47:20, then 10:47:23. Drain start 10:47:23 - 0:15 = 10:32:23, so the first
//	      round at or after it is 10:32:30 (the unmoved lease end would give 10:32:20); it ends in the next round, 10:32:40.
//	succ: the look of 09:01:25 raises the end past pred's drain start, the controller creates it at 09:01:30 and the
//	      Provider submits it at 09:01:40; it starts 95 min after, at 10:36:40, after pred ended. Lease end 10:36:40 +
//	      1:46:00 = 12:22:40, then 12:22:43; ready 10:36:40 + 50 s = 10:37:30. Its look is at 10:36:45, the first after
//	      its start, and sets the end 10:36:40 + 1:31:00 = 12:07:40, which equals the drain start of the old lease end
//	      (12:22:40 - 0:15) and is 3 s before that of the moved one (12:07:43); it drains at the end, 12:07:40.
//
// The second: the Provider reads every third round (30 s), at the rig's clock of 08:00:30, 08:01:00 and so on.
//
//	pred: starts 09:02:00 (08:01:00 + 61 min). Lease end 10:48:00, then 10:48:03. Drain start 10:48:03 - 0:15 = 10:33:03,
//	      so 10:33:10 (the unmoved lease end: 10:33:00); ended 10:33:20.
//	succ: the look of 09:02:05 raises the end, the controller creates it at 09:02:10 and the Provider submits it at its
//	      next read, 09:02:30; it starts at 10:37:30. Lease end 12:23:30, then 12:23:33; ready 10:38:20. Its look at 10:37:35
//	      sets the end 10:37:30 + 1:31:00 = 12:08:30, the drain start of the old lease end (12:23:30 - 0:15), 3 s before
//	      that of the moved one (12:08:33). The Provider reads the successor at 10:37:30 (start, lease end not moved) and
//	      at 10:38:00 (moved), so the rounds of 10:37:40 and 10:37:50 hold the old lease end with the final end set. A
//	      final end from the listed lease end would be 12:08:33, after the old drain start 12:08:30 that the controller
//	      holds in those rounds: it plans a third Allocation at 10:37:40.
//
// The third: the successor starts at once, no wait, and the Provider reads every round. This is the handover in time.
//
//	pred: as in the first case (start 09:01:20; lease end 10:47:20, then 10:47:23; draining 10:32:30, ended 10:32:40).
//	succ: created 09:01:30, submitted and started 09:01:40 with no wait; lease end 09:01:40 + 1:46:00 = 10:47:40, then
//	      10:47:43; ready 09:01:40 + 50 s = 09:02:30, 1 h 30 min before pred's drain start, so it takes over at 10:32:30.
//	      The look of 09:01:45 sets the end 09:01:40 + 1:31:00 = 10:32:40, 10 s after pred's drain, which is where the
//	      successor drains.
var movedCases = []movedWant{
	{name: "the Provider reads every round", pollEvery: 1, succWait: 95 * time.Minute,
		predStart: "09:01:20", succStart: "10:36:40", finalAt: "10:36:45",
		predLease: []string{"10:47:20", "10:47:23"}, succLease: []string{"12:22:40", "12:22:43"},
		predDrain: "10:32:30", predEnded: "10:32:40", succReady: "10:37:30", end: "12:07:40"},
	{name: "the Provider reads every 30 s", pollEvery: 3, succWait: 95 * time.Minute,
		predStart: "09:02:00", succStart: "10:37:30", finalAt: "10:37:35",
		predLease: []string{"10:48:00", "10:48:03"}, succLease: []string{"12:23:30", "12:23:33"},
		predDrain: "10:33:10", predEnded: "10:33:20", succReady: "10:38:20", end: "12:08:30",
		stale: []string{"10:37:40", "10:37:50"}},
	{name: "the successor starts at once and is ready before the predecessor drains", pollEvery: 1, handover: true,
		predStart: "09:01:20", succStart: "09:01:40", finalAt: "09:01:45",
		predLease: []string{"10:47:20", "10:47:23"}, succLease: []string{"10:47:40", "10:47:43"},
		predDrain: "10:32:30", predEnded: "10:32:40", succReady: "09:02:30", end: "10:32:40"},
}

// A job whose lease end Slurm moved 3 s later, with the live steps' guard, in front of the real controller. The
// deployment's end, the Instances' lease ends and drain starts stay consistent, no third Allocation or job is planned,
// and the predecessor drains at the drain start of its moved lease end, the successor serving from its start to the
// deployment's end.
func TestAMovedLeaseEndKeepsTheTwoJobGuardToTwoAllocations(t *testing.T) {
	t.Parallel()
	for _, w := range movedCases {
		t.Run(w.name, func(t *testing.T) {
			t.Parallel()
			r := runMoved(t, w)
			logMovedRun(t, r)
			// The first thing: a third Allocation is the failure the guard exists to prevent.
			if r.third != nil {
				t.Fatalf("a third Allocation was planned at %s (Allocations %v, %d submissions): the deployment's end was after the drain start the controller held for the successor",
					r.thirdAt.Format(time.TimeOnly), r.third, r.submissions)
			}
			if len(r.everAlloc) != 2 || len(r.allocOrder) != 2 || len(r.order) != 2 || r.submissions != 2 {
				t.Fatalf("%d Allocations ever, %d started, %d Instances, %d submissions, want 2 of each", len(r.everAlloc), len(r.allocOrder), len(r.order), r.submissions)
			}
			pred, succ := r.insts[r.order[0]], r.insts[r.order[1]]
			if got := r.starts[r.allocOrder[0]]; !got.Equal(movedAt(w.predStart)) {
				t.Errorf("the predecessor's job started at %s, want %s: the case's input moved", got.Format(time.TimeOnly), w.predStart)
			}
			if got := r.starts[r.allocOrder[1]]; !got.Equal(movedAt(w.succStart)) {
				t.Errorf("the successor's job started at %s, want %s: the case's input moved", got.Format(time.TimeOnly), w.succStart)
			}
			if !r.finalAt.Equal(movedAt(w.finalAt)) {
				t.Errorf("the guard's end became final at the look of %s, want %s", r.finalAt.Format(time.TimeOnly), w.finalAt)
			}
			// The guard saw both jobs' lease ends 3 s past their start plus the time limit.
			if r.predSlack == nil || *r.predSlack != movedConfiguration || r.succSlack == nil || *r.succSlack != movedConfiguration {
				t.Errorf("the guard's lease slacks are %v and %v, want 3s for both", r.predSlack, r.succSlack)
			}
			// The lease ends the controller held.
			if !movedTimesEqual(pred.leaseEnds, movedAts(w.predLease...)) {
				t.Errorf("the predecessor's lease ends were %v, want %v", movedClockList(pred.leaseEnds), w.predLease)
			}
			if !movedTimesEqual(succ.leaseEnds, movedAts(w.succLease...)) {
				t.Errorf("the successor's lease ends were %v, want %v", movedClockList(succ.leaseEnds), w.succLease)
			}
			// The deployment's end is the successor's start + limit - drain window, at or before the drain start
			// of either lease end the controller held for it.
			if !r.end.Equal(movedAt(w.end)) {
				t.Errorf("the deployment's end is %s, want %s", r.end.Format(time.TimeOnly), w.end)
			}
			for _, le := range movedAts(w.succLease...) {
				if ds := le.Add(-movedDrain); r.end.After(ds) {
					t.Errorf("the deployment's end %s is after the successor's drain start %s of lease end %s",
						r.end.Format(time.TimeOnly), ds.Format(time.TimeOnly), le.Format(time.TimeOnly))
				}
			}
			// The rounds the successor held the old lease end beside the final end: the case reaches the race, or,
			// with none expected, it does not.
			if !movedTimesEqual(r.staleAfter, movedAts(w.stale...)) {
				t.Errorf("the successor held its old lease end with the final end set in the rounds %v, want %v", movedClockList(r.staleAfter), w.stale)
			}
			// The timeline: the predecessor drains at the first round at or after the drain start of its moved lease
			// end and ends a round later, before the successor starts; the successor is ready 50 s after its start and
			// drains at the deployment's end.
			if !pred.drainAt.Equal(movedAt(w.predDrain)) {
				t.Errorf("the predecessor drained at %s, want %s (the drain start of its moved lease end %s, rounded up to a round)",
					pred.drainAt.Format(time.TimeOnly), w.predDrain, movedAts(w.predLease...)[1].Add(-movedDrain).Format(time.TimeOnly))
			}
			if !pred.endedAt.Equal(movedAt(w.predEnded)) {
				t.Errorf("the predecessor ended at %s, want %s", pred.endedAt.Format(time.TimeOnly), w.predEnded)
			}
			if w.handover && !succ.readyAt.Before(pred.drainAt) {
				t.Errorf("the successor was ready at %s, the predecessor drained at %s: want the handover in time, ready first",
					succ.readyAt.Format(time.TimeOnly), pred.drainAt.Format(time.TimeOnly))
			}
			if !succ.readyAt.Equal(movedAt(w.succReady)) {
				t.Errorf("the successor was ready at %s, want %s", succ.readyAt.Format(time.TimeOnly), w.succReady)
			}
			if !succ.drainAt.Equal(movedAt(w.end)) {
				t.Errorf("the successor drained at %s, want the deployment's end %s", succ.drainAt.Format(time.TimeOnly), w.end)
			}
		})
	}
}

// logMovedRun writes the run's timeline, for a failure to be read from.
func logMovedRun(t *testing.T, r movedRun) {
	t.Helper()
	t.Logf("the guard's end final at the look of %s, the deployment's end %s, %d submissions, lease slacks %v and %v",
		r.finalAt.Format(time.TimeOnly), r.end.Format(time.TimeOnly), r.submissions, r.predSlack, r.succSlack)
	for _, n := range r.allocOrder {
		t.Logf("Allocation %s: job started %s", n, r.starts[n].Format(time.TimeOnly))
	}
	for _, n := range r.order {
		s := r.insts[n]
		t.Logf("Instance %s: lease ends %v, ready %s, draining %s, ended %s", n, movedClockList(s.leaseEnds),
			s.readyAt.Format(time.TimeOnly), s.drainAt.Format(time.TimeOnly), s.endedAt.Format(time.TimeOnly))
	}
	t.Logf("rounds in which the successor held its old lease end beside the final end: %v", movedClockList(r.staleAfter))
}
