package alpha

// What the wait for the run's jobs to end says when it gives up (issue #503; the science agent's ruling, 13 Nov 2026). Row A's live run
// timed out after 12 minutes waiting "for every job of the run to end in the cluster's record", and the failure named nothing: not which
// job lacked an end, not whether the ledger had erred or listed none, and not why the controller had not released the Allocation. The
// state below is what the wait saw last, for the failure's text: each job's state, phase and reason with no job ID in it, each
// Allocation's phase, whether it is being deleted, how long it has been idle and the retention it follows, and each Instance's phase.
// It is a text for a post, so it holds no ID, name, path, host or login: jobs, Allocations and Instances are counted in the order given.

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// EndWaitState is the state of the run's jobs, Allocations and Instances at a moment, as a line of text. jobs and ledgerErr are the
// ledger's read of the run's tags; allocs and insts are the API's objects of the run, and apiErr is why they could not be read when
// they could not (a state with no Allocation must not be the state of a failed read). now is the host's clock, which the Allocation's
// idle time is measured against (the controller writes the idle-since anchor from its own, on this host in a run).
func EndWaitState(jobs []provider.Job, ledgerErr error, allocs []v1alpha1.Allocation, insts []v1alpha1.Instance, apiErr error, now time.Time) string {
	if apiErr != nil {
		// The error's text is in the test's private log, not here.
		return strings.Join([]string{jobsState(jobs, ledgerErr), "the Allocations and Instances could not be read from the API"}, "; ")
	}
	return strings.Join([]string{jobsState(jobs, ledgerErr), allocationsState(allocs, now), instancesState(insts)}, "; ")
}

// jobsState is the ledger's answer: an error, no job at all, or each job's state.
func jobsState(jobs []provider.Job, err error) string {
	switch {
	case errors.Is(err, ErrNoLedger):
		return "the run made no batch client, so the cluster's record was never read"
	case err != nil:
		// The error's text can hold a host or a path: it is in the test's private log, not here.
		return "the cluster's record could not be read (the ledger's read failed)"
	case len(jobs) == 0:
		return "the cluster's record lists no job for the run's tags"
	}
	ended := 0
	var each []string
	for i, j := range jobs {
		if j.EndTime != nil {
			ended++
		}
		each = append(each, fmt.Sprintf("job %d: state %s, phase %s, reason %s, ended %s", i+1, stateOf(j), phaseOf(j), reasonOf(j), yesNo(j.EndTime != nil)))
	}
	return fmt.Sprintf("%s in the cluster's record, %d ended: %s", plural(len(jobs), "job"), ended, strings.Join(each, "; "))
}

// stateOf is the scheduler's state of a job with its flags, joined with a plus (RUNNING+COMPLETING), or "unknown" when it gave none.
func stateOf(j provider.Job) string {
	if len(j.State) == 0 {
		return "unknown"
	}
	return strings.Join(j.State, "+")
}

func phaseOf(j provider.Job) string {
	if j.Phase == "" {
		return "unknown"
	}
	return string(j.Phase)
}

// reasonOf is the scheduler's reason for the job, with the job's own ID, wherever it stands in the text, replaced by [job].
func reasonOf(j provider.Job) string {
	if j.Reason == "" {
		return "none given"
	}
	if j.ID == "" {
		return j.Reason
	}
	return strings.ReplaceAll(j.Reason, j.ID, "[job]")
}

// allocationsState is each Allocation's phase, deletion, idle time and retention, in the order of its creation then its name.
func allocationsState(allocs []v1alpha1.Allocation, now time.Time) string {
	if len(allocs) == 0 {
		return "no Allocation"
	}
	sorted := slices.Clone(allocs)
	slices.SortFunc(sorted, func(a, b v1alpha1.Allocation) int {
		if c := a.CreationTimestamp.Time.Compare(b.CreationTimestamp.Time); c != 0 {
			return c
		}
		return strings.Compare(a.Name, b.Name)
	})
	var each []string
	for i := range sorted {
		a := &sorted[i]
		each = append(each, fmt.Sprintf("Allocation %d: phase %s, deleting %s, %s, retention %s", i+1, phaseOfAllocation(a),
			yesNo(!a.DeletionTimestamp.IsZero()), idleOf(a, now), retentionOf(a)))
	}
	return fmt.Sprintf("%s: %s", plural(len(sorted), "Allocation"), strings.Join(each, "; "))
}

func phaseOfAllocation(a *v1alpha1.Allocation) string {
	if a.Status.AtProvider.Phase == "" {
		return "unknown"
	}
	return string(a.Status.AtProvider.Phase)
}

// idleOf says how long the Allocation has been idle, from its idle-since annotation, or that it is not idle (it carries none: it hosts
// an Instance, or the controller has not yet written the anchor).
func idleOf(a *v1alpha1.Allocation, now time.Time) string {
	t, ok, err := v1alpha1.IdleSince(a)
	switch {
	case err != nil:
		return "its idle-since annotation is not a time"
	case !ok:
		return "not idle (no idle-since)"
	}
	return "idle for " + now.Sub(t).Round(time.Second).String()
}

// retentionOf is the retention the Allocation follows: its mode (an unset one is Keep), with the idle limit under ReleaseWhenIdle and
// the date under Keep.
func retentionOf(a *v1alpha1.Allocation) string {
	rp := a.Spec.RetentionPolicy
	mode := rp.EffectiveMode()
	switch {
	case mode == v1alpha1.RetentionReleaseWhenIdle && rp != nil && rp.IdleLimit != nil:
		if d, err := rp.IdleLimit.AsDuration(); err == nil {
			return fmt.Sprintf("%s (idle limit %s)", mode, d)
		}
		return fmt.Sprintf("%s (idle limit not a duration)", mode)
	case mode == v1alpha1.RetentionReleaseWhenIdle:
		return fmt.Sprintf("%s (no idle limit)", mode)
	case rp != nil && rp.Until != nil:
		return fmt.Sprintf("%s (until a date)", mode)
	}
	return string(mode)
}

// instancesState is each Instance's phase with its end reason, in the order of its creation then its name.
func instancesState(insts []v1alpha1.Instance) string {
	if len(insts) == 0 {
		return "no Instance"
	}
	sorted := slices.Clone(insts)
	slices.SortFunc(sorted, func(a, b v1alpha1.Instance) int {
		if c := a.CreationTimestamp.Time.Compare(b.CreationTimestamp.Time); c != 0 {
			return c
		}
		return strings.Compare(a.Name, b.Name)
	})
	var each []string
	for i := range sorted {
		in := &sorted[i]
		phase := string(in.Status.Phase)
		if phase == "" {
			phase = "unknown"
		}
		if in.Status.EndReason != "" {
			phase += " (" + in.Status.EndReason + ")"
		}
		each = append(each, fmt.Sprintf("Instance %d: phase %s, deleting %s", i+1, phase, yesNo(!in.DeletionTimestamp.IsZero())))
	}
	return fmt.Sprintf("%s: %s", plural(len(sorted), "Instance"), strings.Join(each, "; "))
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
