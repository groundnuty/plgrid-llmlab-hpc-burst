package conformance

// The first clock of a job: the bound within which it must start. A job under a one-job QOS can queue for a long
// time, and the run holds the slot while it waits; a job in a long queue holds nothing but is no use to wait for
// without end. A job that has not started when the bound passes, counted from the moment the wait began (the
// Allocation is created and reconciled at once, or the direct sbatch has just returned), stops the whole run: the
// job is cancelled by its tag, and the run ends as NOT RUN, which hack/live-checks.sh reports apart from a failure,
// on every row and whichever bound it was (the science agent's ruling of 8 Nov 2026, #272: a queue wait tests
// nothing, so it is never a FAIL). The bound is the row's start wait (HPCBURST_CONF_START_WAIT) when it has one,
// else the wait in the queue (HPCBURST_CONF_PENDING_MAX_SECONDS, 30 minutes when unset). A job that was never
// accepted is a failure.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// maxStartWait is the longest bound HPCBURST_CONF_START_WAIT can name, the default of the wait in the queue.
const maxStartWait = 30 * time.Minute

// notRunFile is the marker, in the private output directory, that tells
// hack/live-checks.sh the run stopped on the bound. It holds the step and the
// bound, never a job ID. The script reads it only together with a skipped test
// that has no failure.
const notRunFile = "not-run.txt"

// startState is what a wait for a job to start ended with.
type startState int

const (
	jobStarted     startState = iota
	jobNotAccepted            // the bound passed and no job had been accepted
	jobDidNotStart            // the bound passed, a job was accepted and is not running
)

// waitStart calls probe, then sleeps every, until probe says the job runs or
// bound has passed since since. probe is always called at least once, and a
// job that runs at the bound counts as started.
func waitStart(now func() time.Time, sleep func(time.Duration), since time.Time, bound, every time.Duration, probe func() (started, accepted bool)) startState {
	deadline := since.Add(bound)
	for {
		started, accepted := probe()
		switch {
		case started:
			return jobStarted
		case !now().Before(deadline):
			if !accepted {
				return jobNotAccepted
			}
			return jobDidNotStart
		}
		sleep(every)
	}
}

// clock is the world's time for the start wait: the wall clock, unless a test
// sets its own.
func (w *world) clock() (now func() time.Time, sleep func(time.Duration)) {
	now, sleep = w.now, w.sleep
	if now == nil {
		now = time.Now
	}
	if sleep == nil {
		sleep = time.Sleep
	}
	return now, sleep
}

// pollAllocation reconciles the Allocation once and reads it back, unless a
// test supplies its own poll.
func (w *world) pollAllocation(name string) *v1alpha1.Allocation {
	if w.poll != nil {
		return w.poll(name)
	}
	w.reconcile(name)
	return w.get(name)
}

// at is the start-wait clock's now.
func (w *world) at() time.Time {
	now, _ := w.clock()
	return now()
}

// firstClock is the bound a job's start is waited for, and what a message calls it: the row's start wait when it has
// one, else the wait in the queue.
func (w *world) firstClock() (bound time.Duration, name string) {
	if w.s.startWait > 0 {
		return w.s.startWait, "the start-wait bound"
	}
	return w.s.pendingMax, "the wait in the queue"
}

// untilRuns waits until cond holds for the Allocation, within the first clock (see the top of this file). since is
// when the wait began, accepted says the job was accepted before it did (step 3's direct sbatch), step names the
// step in the output.
func (w *world) untilRuns(step, name, what string, since time.Time, accepted bool, cond func(*v1alpha1.Allocation) bool) *v1alpha1.Allocation {
	w.t.Helper()
	bound, boundName := w.firstClock()
	var last *v1alpha1.Allocation
	probe := func() (bool, bool) {
		last = w.pollAllocation(name)
		return last != nil && cond(last), accepted || (last != nil && last.Annotations[v1alpha1.AnnotationJobID] != "")
	}
	now, sleep := w.clock()
	switch waitStart(now, sleep, since, bound, 3*time.Second, probe) {
	case jobStarted:
		return last
	case jobNotAccepted:
		w.t.Fatalf("%s: no job was accepted within %s of %s", step, boundName, bound)
	default:
		w.notRun(step)
	}
	return nil
}

// notRun ends the run as NOT RUN: it cancels every job this run tagged, by its
// tag, checks that none is left live, writes the marker and skips the test. A
// cancel that fails, or a job left live, is a failure instead, because a job
// would be left on the platform. The marker and the skip say which bound ran out.
func (w *world) notRun(step string) {
	w.t.Helper()
	bound, boundName := w.firstClock()
	ctx := context.Background()
	if err := w.cancelByTag(ctx); err != nil {
		w.t.Fatalf("%s: the job did not start within %s of %s, and cancelling this run's jobs by tag failed: %v", step, boundName, bound, err)
	}
	note := fmt.Sprintf("%s: a job did not start within the start-wait bound of %s; this run's jobs were cancelled by tag\n", step, bound)
	skip := fmt.Sprintf("NOT RUN: %s: a job did not start within %s of its acceptance; it was cancelled by its tag", step, bound)
	if w.s.startWait == 0 {
		note = fmt.Sprintf("%s: a job was still pending after %s in the queue; this run's jobs were cancelled by tag\n", step, bound)
		skip = fmt.Sprintf("NOT RUN: %s: a job was still pending after %s in the queue (the wait in the queue); it was cancelled by its tag", step, bound)
	}
	if err := os.WriteFile(filepath.Join(w.p.dir, notRunFile), []byte(note), 0o600); err != nil {
		w.t.Fatalf("%s: the job did not start within %s of %s, but the not-run marker could not be written: %v", step, boundName, bound, err)
	}
	w.t.Skipf("%s", skip)
}

// cancelByTag cancels every live job whose name is one of the tags this run
// wrote down (job-tags.txt), then reads them again and fails if one is still
// live. It lists the queue only: a job that has not started is in it.
func (w *world) cancelByTag(ctx context.Context) error {
	if w.batch == nil {
		return nil // no platform yet: nothing was submitted
	}
	_, sleep := w.clock()
	for try := 0; try < 10; try++ {
		var ids []string
		for _, tag := range w.p.tags {
			jobs, err := w.batch.ListByTag(ctx, tag)
			if err != nil {
				return fmt.Errorf("list by tag: %w", err)
			}
			for _, j := range jobs {
				if j.EndTime == nil {
					ids = append(ids, j.ID)
					w.jobIDs[j.ID] = true
				}
			}
		}
		if len(ids) == 0 {
			return nil
		}
		for _, r := range w.batch.Cancel(ctx, ids) {
			w.p.log("not-run cancel %s: %s %v", r.JobID, r.Outcome, r.Err)
			if r.Outcome == provider.CancelError {
				return fmt.Errorf("cancel: %w", r.Err)
			}
		}
		sleep(3 * time.Second) // a cancelled job can show as completing for a few seconds
	}
	return fmt.Errorf("a job of this run is still live after 10 cancels")
}

// duplicateWait is how long the run gives the controller to cancel a late
// duplicate at the end, before it calls it a failure.
const duplicateWait = 2 * time.Minute

// settleDuplicates checks, for every tag this run recorded, that at most one
// job of the tag ever started and that none is still live. A live job of a
// recorded tag at the end of the run is a late duplicate: an ambiguous
// submission was retried after the in-flight window and the original registered
// late. Production keeps HBEP-4's rules (the tag resolves it; a held job found
// beside the recorded one is cancelled, HeldDuplicateCancelled), and the run
// asserts that this happened, within duplicateWait, reconciling the Allocations
// in names meanwhile (those with a job recorded: reconciling one without would
// submit). Whatever is live after that is a failure, and the cleanup cancels
// it by its tag.
func (w *world) settleDuplicates(names []string, within time.Duration) {
	w.t.Helper()
	if w.batch == nil {
		return
	}
	now, sleep := w.clock()
	deadline := now().Add(within)
	for {
		live := 0
		for _, tag := range w.p.tags {
			jobs, err := w.batch.FindByTag(context.Background(), tag, w.began)
			if err != nil {
				w.t.Errorf("reading the jobs of a tag at the end of the run: %v", err)
				return
			}
			started := 0
			for _, j := range jobs {
				if j.StartTime != nil {
					started++
				}
				if j.EndTime == nil {
					live++
					w.jobIDs[j.ID] = true
				}
			}
			if started > 1 {
				w.t.Errorf("%d jobs of one of this run's tags started: an ambiguous submission was retried and both ran", started)
				return
			}
		}
		if live == 0 {
			return
		}
		if !now().Before(deadline) {
			w.t.Errorf("%d job(s) of this run's tags are still live at the end, %s after the last step: a late duplicate was not cancelled; the cleanup cancels them by tag", live, within)
			return
		}
		for _, name := range names {
			w.reconcile(name)
		}
		sleep(3 * time.Second)
	}
}
