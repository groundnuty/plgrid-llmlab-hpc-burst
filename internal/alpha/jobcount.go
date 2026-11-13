package alpha

// The job counts of the stack rows (issue #245; the science agent's rulings on
// PR 255, comment 6691221974, and the review of PR 255, B5). With no queue wait
// observed, the lifecycle controller creates a primary's replacement at the
// primary's start, and a successor's own replacement before the predecessor's
// drain start (HBEP-7, Handover); with a long queue wait it can create the
// successor's replacement in the very pass that first sees the successor run.
// A tunnel row would hold two jobs and a handover row three, where the runbook
// promises one and two. A deployment that ends at or before an Instance's
// drain start gives it no replacement (internal/lifecycle/handover.go,
// replacements), so the live steps place the deployment's end with these
// functions, on the cluster's own clock, the clock of the jobs' times that the
// controller compares the end with.

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// FirstEndOfOne is the end a deployment of one job is created with: the
// earliest drain start its job can have, the job's time limit less the drain
// window after clusterNow, a reading of the cluster's clock taken before the
// deployment exists, so that no replacement is planned at the job's start,
// whenever it comes: the job is submitted after the deployment's creation, and
// a job's lease end is no earlier than its start plus its time limit, on the
// cluster's clock (TwoJobGuard.planned).
// API times are whole seconds.
func FirstEndOfOne(clusterNow time.Time, timeLimit, drainWindow time.Duration) time.Time {
	return clusterNow.Add(timeLimit - drainWindow).Truncate(time.Second)
}

// EndOfOne is the end of a deployment of one job once its Instance's drain
// start is known: a minute before it, so that no rounding puts it after.
func EndOfOne(drainStart time.Time) time.Time {
	return drainStart.Add(-time.Minute).Truncate(time.Second)
}

// EndOfTwo is the end of a deployment of two jobs once its successor's drain
// start is known: that drain start, where the successor's lease drains it
// anyway. An end at or before the predecessor's drain start would stop the
// handover, so that is an error.
func EndOfTwo(predDrainStart, succDrainStart time.Time) (time.Time, error) {
	end := succDrainStart.Truncate(time.Second)
	if !end.After(predDrainStart) {
		return time.Time{}, fmt.Errorf("the successor's drain start %s is not after the predecessor's %s: the deployment keeps no end",
			end.UTC().Format(time.RFC3339), predDrainStart.UTC().Format(time.RFC3339))
	}
	return end, nil
}

// ErrQueueWaitPastEnd is a row of one job whose job was still pending a minute
// before its deployment's first end (FirstEndOfOne): the deployment would end
// before the job started, so the run cancelled the job by its tag. It ends the
// run as NOT RUN, exit 3, on both tunnel rows (the science agent's ruling on
// PR 255): the job did not start in time, the consequence of #222.
var ErrQueueWaitPastEnd = errors.New("a tunnel row's job was still pending a minute before its deployment's first end (its time limit less the drain window after the deployment's creation), and was cancelled by its tag: a longer lease (LIVE_TUNNEL_LEASE_MINUTES) leaves it more time")

// FirstClock is the run's first clock, the longest its first job may wait in
// the queue: the wait the operator allows (pendingMax), and for a row of one
// job no later than a minute before its deployment's first end, the time limit
// less the drain window, so that the job has started, and the run has moved
// the end, before the deployment ends. pastEnd says the end, not the operator's
// wait, is what bounds it. A row of two jobs has no such end: its end follows
// the cluster's clock while no job of its runs (TwoJobGuard).
//
// A row with a start wait (Row.StartWait, the plgrid-now rows of issue #376)
// has it as its first clock, from the deployment's creation, and takes no
// other: the operator's wait is not read (hack/live-checks.sh, "A row with a
// start wait has the start wait as its first clock and takes no other"). The
// deployment's first end still bounds it when that comes first.
func FirstClock(row Row, pendingMax, timeLimit, drainWindow time.Duration) (bound time.Duration, pastEnd bool) {
	wait := row.firstWait(pendingMax)
	if row.MostJobs == 1 {
		if c := timeLimit - drainWindow - time.Minute; c < wait {
			return c, true
		}
	}
	return wait, false
}

// firstWait is the wait in the queue each job of the row may take: its start wait, else the operator's.
func (r Row) firstWait(pendingMax time.Duration) time.Duration {
	if r.StartWait > 0 {
		return r.StartWait
	}
	return pendingMax
}

// NotStartedError is a run whose first job was still pending when its first
// clock (FirstClock) ran out, after the run cancelled its jobs by their tags
// and saw none left. It is ErrNotStarted, and ErrQueueWaitPastEnd too when the
// deployment's first end was the bound. StartWait says the bound was the
// row's start wait.
type NotStartedError struct {
	Bound     time.Duration
	PastEnd   bool
	StartWait bool
}

func (e *NotStartedError) Error() string {
	switch {
	case e.PastEnd:
		return fmt.Sprintf("%v (after %s)", ErrQueueWaitPastEnd, e.Bound)
	case e.StartWait:
		return fmt.Sprintf("%v (after the row's start wait of %s)", ErrNotStarted, e.Bound)
	}
	return fmt.Sprintf("%v (after %s)", ErrNotStarted, e.Bound)
}

// Is makes the error ErrNotStarted, and ErrQueueWaitPastEnd when the
// deployment's first end was the bound.
func (e *NotStartedError) Is(target error) bool {
	return target == ErrNotStarted || (e.PastEnd && target == ErrQueueWaitPastEnd)
}

// WaitingToStart is what makes a job one that never started, in a read made at a
// bound (the science agent's ruling of 8 Nov 2026 on #272): the scheduler lists it
// held or pending, with no start and no end. A requeued job is pending again after
// it ran, so it is not. Everything else is not a job that never started, and a read
// that failed, or that does not list the job, is no evidence of it either: the run
// then ends as a failure that names what it found, never as NOT RUN.
func WaitingToStart(j provider.Job) bool {
	return j.StartTime == nil && j.EndTime == nil && !j.Requeued && (j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld)
}

// The run's own names (review of PR 255, B3). hack/live-checks.sh draws a run
// ID for each stack row it runs and passes it on (HPCBURST_ALPHA_RUN_ID); the
// run's Provider and deployment are named after it (RunName), the lifecycle
// controller names every Allocation after its Provider
// (internal/lifecycle/provision.go, randomName(pv.name)), and a job's tag
// after its Allocation (v1alpha1.JobTag), so every job of the run has
// TagPrefix. The script's cleanup and the steps' Close find the run's jobs by
// it in the queue, whatever the recorder holds.

// runIDRE is the shape of a run ID: 10 lowercase hex digits, 40 random bits.
var runIDRE = regexp.MustCompile(`^[0-9a-f]{10}$`)

// NewRunID draws a run ID, for a run that no script started (a stand-in).
func NewRunID() (string, error) {
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("a run ID: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// CheckRunID refuses a run ID of another shape.
func CheckRunID(id string) error {
	if !runIDRE.MatchString(id) {
		return errors.New("a run ID must be 10 lowercase hex digits")
	}
	return nil
}

// RunName is the name of the run's Provider and of its ModelDeployment.
func RunName(runID string) string { return "alpha-" + runID }

// TagPrefix is how every job tag of the run begins: "hb-", the Provider's
// name and the dash before the Allocation's random suffix. The Allocation's
// name is 22 bytes, under the 40 that a tag keeps of it, and a tag 38, under
// the 56 that Helios's job names allow (profiles/helios.yaml, jobName).
func TagPrefix(runID string) string { return "hb-" + RunName(runID) + "-" }

// ClusterClockLine reads the cluster's clock on its login node: whole seconds
// since the epoch. The controller compares a deployment's end with the jobs'
// times that the scheduler gives on the cluster's clock, so the ends are
// placed on it, never on this host's (review of PR 255, nit).
const ClusterClockLine = "date -u +%s"

// ParseClusterClock reads ClusterClockLine's output: one line of digits.
func ParseClusterClock(out []byte) (time.Time, error) {
	s := strings.TrimSpace(string(out))
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || strings.TrimLeft(s, "0123456789") != "" {
		return time.Time{}, errors.New("the cluster's clock did not read as whole seconds since the epoch")
	}
	return time.Unix(n, 0).UTC(), nil
}

// Look is one look at the cluster for the job-count guard: the cluster's own
// clock, read just before a fresh queue listing (Clock), and the run's jobs in
// that listing (Jobs). Every job of the run that had started by Clock is in
// it: a job is listed from its submission, and an ended one for MinJobAge.
type Look struct {
	Clock time.Time
	Jobs  []provider.Job
}

// TwoJobGuard keeps a row of two jobs (MostJobs 2) to two, from the
// deployment's creation (review of PR 255, B5). The successor's replacement
// is planned in the pass that first sees the successor run, so the
// deployment's end must be no later than the successor's drain start BEFORE
// that pass, and strictly after the predecessor's drain start once a
// successor may exist, so that the handover happens. A job's drain start is
// its lease end less the drain window, on the cluster's clock. Its lease end is
// its start plus its time limit, or later by the time its configuration took
// (planned), and the controller's drain start follows the lease end the
// Provider last read (internal/lifecycle, copyLeaseEnd), which may be either.
// So the guard reckons each job's drain start as the earliest of them, its
// start plus the time limit less the drain window (drainStartOf): at or before
// every drain start the controller can have for the job. So:
//
//   - While no job of the run has started, and while only the predecessor
//     has, the end is the latest look's cluster clock plus the time limit less
//     the drain window: no later than the drain start of any job that starts
//     after that look. The deployment is created with it (Next on the first
//     look), so the predecessor's start meets an end at or before its drain
//     start, which plans no successor yet; the first look whose clock is past
//     the predecessor's start raises the end past the predecessor's drain
//     start, and only then can the successor be created, after that look's
//     clock, so its drain start is no earlier than the end. The end moves on
//     with every look, so it is never reached while a job waits in the queue.
//   - Once the successor has started, the end is its drain start so reckoned
//     (EndOfTwo), for good: its lease's drain start, or earlier than that by
//     the time its configuration took.
//
// A third job of the run that starts, a started job whose time limit is not
// the one planned with or whose lease end is earlier than its start plus it
// (planned), and an EndOfTwo error are errors: the run must stop.
// The guard never lowers the end: an older end came from an earlier clock and
// is still no later than any later start's drain start.
type TwoJobGuard struct {
	// TimeLimit is the jobs' time limit (v1alpha1.AllocationTimeLimit), and
	// DrainWindow the controller's (v1alpha1.InstanceDrainWindow).
	TimeLimit, DrainWindow time.Duration

	pred, succ     string
	predDrainStart time.Time
	end            time.Time
	final          bool
}

// Next is the end the deployment must have after the look l, and whether it
// is final (the successor's drain start, as drainStartOf reckons it). An error
// must stop the run.
func (g *TwoJobGuard) Next(l Look) (end time.Time, final bool, err error) {
	var started []provider.Job
	for _, j := range l.Jobs {
		if !jobStarted(j) {
			continue
		}
		if err := g.planned(j); err != nil {
			return time.Time{}, false, err
		}
		started = append(started, j)
	}
	sort.SliceStable(started, func(a, b int) bool { return started[a].StartTime.Before(*started[b].StartTime) })
	if g.pred == "" && len(started) > 0 {
		g.pred, g.predDrainStart = started[0].ID, g.drainStartOf(started[0])
	}
	var others []provider.Job
	for _, j := range started {
		if j.ID != g.pred && j.ID != g.succ {
			others = append(others, j)
		}
	}
	switch {
	case len(others) > 0 && (g.final || len(others) > 1):
		return time.Time{}, false, errors.New("a third job of the run has started: the row holds at most two")
	case g.final:
		return g.end, true, nil
	case len(others) == 1:
		end, err := EndOfTwo(g.predDrainStart, g.drainStartOf(others[0]))
		if err != nil {
			return time.Time{}, false, err
		}
		g.succ, g.end, g.final = others[0].ID, end, true
		return g.end, true, nil
	}
	// While the look's clock is not yet past the predecessor's start, this
	// end is at or before its drain start and plans no successor yet; the
	// next look raises it past.
	if e := l.Clock.Add(g.TimeLimit - g.DrainWindow).Truncate(time.Second); e.After(g.end) {
		g.end = e
	}
	return g.end, false, nil
}

// drainStartOf is the earliest drain start a started job that planned accepted can have: its start plus the planned
// time limit, less the drain window. Its lease end is there or later in every listing, the guard's own and the
// Provider's, and the guard's look can read a later one than the Provider last did. An end derived from the lease end
// the guard read could then be after the drain start the controller has, which plans a replacement
// (internal/lifecycle, replacements); an end derived from the start cannot. Where the lease end is the start plus the
// time limit, the two agree.
func (g *TwoJobGuard) drainStartOf(j provider.Job) time.Time {
	return j.StartTime.Add(g.TimeLimit - g.DrainWindow)
}

// planned checks that a started job has a lease the guard plans with. Its lease end may be later than its start plus
// the time limit, never earlier: when a job's configuration (its prolog, its nodes made ready) ends a whole second or
// more after its start, Slurm sets its end_time to that moment plus the time limit and leaves its start as it was
// (Slurm 23.11.7, src/slurmctld/job_mgr.c: job_config_fini, _job_time_limit_incr). Row B's live run of 13 Nov 2026
// saw one second of it. A later lease end only puts the job's drain start later than an end given before the job
// started assumes, so that end stays at or before it. The guard takes a later one only when the listing gives the
// job's own time limit and it is the planned one; with none listed, the lease end must be its start plus the time
// limit exactly, since a longer time limit could not be told from a configuration.
func (g *TwoJobGuard) planned(j provider.Job) error {
	switch {
	case j.HardEnd == nil:
		return errors.New("a job of the run started with no lease end")
	case j.HardEnd.Before(j.StartTime.Add(g.TimeLimit)):
		return fmt.Errorf("a job of the run started with a lease end before its start plus the time limit %s the guard plans with", g.TimeLimit)
	case j.TimeLimit != 0 && j.TimeLimit != g.TimeLimit:
		return fmt.Errorf("a job of the run is listed with the time limit %s, not the %s the guard plans with", j.TimeLimit, g.TimeLimit)
	case j.TimeLimit == 0 && !j.HardEnd.Equal(j.StartTime.Add(g.TimeLimit)):
		return fmt.Errorf("a job of the run started with a lease end later than its start plus the time limit %s the guard plans with, and no time limit listed to say why", g.TimeLimit)
	}
	return nil
}

// jobStarted reports whether a listed job has started: it has a start time,
// and it did not end at that very time, as a job cancelled while pending does.
func jobStarted(j provider.Job) bool { return JobStarted(j) }

// JobStarted is jobStarted, for the rows of other packages.
func JobStarted(j provider.Job) bool {
	return j.StartTime != nil && (j.EndTime == nil || j.EndTime.After(*j.StartTime))
}
