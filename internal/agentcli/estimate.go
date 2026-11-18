package agentcli

import (
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The estimate of a target is Slurm's advisory start time for the same job submitted now (HBEP-10, The
// release cost). It has three states, ruled by the science agent on #661 (17 Nov 2026) and derived here:
//
//   - pending: no entry yet for the plan's ID and the target's Provider, and the plan is at most 60 s old
//     (status.plan.observedAt);
//   - unknown: no entry after those 60 s, or an entry whose start carries an unknown reason, which is a
//     final unknown, never pending again;
//   - known: the entry's time, advisory, with its age: now minus the read time of the entry.
//
// docs/agent-cli.md states the rule for a reader of the API.

// estimateWindow is how long a plan waits for its estimate before it reads as unknown.
const estimateWindow = 60 * time.Second

// noEstimateReason is the reason of an estimate that did not come within the window.
const noEstimateReason = "no estimate within 60 s"

// estimateEntry is the entry of status.estimates[] that the Provider's controller writes (#661): provider,
// plan ID, the targets it covers by UID, and a start, either a time or an unknown reason, with when it was
// read. estimateEntries maps the API's field into this, and nothing else reads the API's own shape.
type estimateEntry struct {
	Provider string
	PlanID   string
	// Targets are the UIDs of the plan targets the entry covers. An entry that lists none covers every
	// target of its Provider.
	Targets []types.UID
	Start   estimateStart
}

// estimateStart is an entry's answer: Time, or Unknown with its reason, read at ObservedAt.
type estimateStart struct {
	Time       *time.Time
	Unknown    string
	ObservedAt time.Time
}

// estimateEntries reads the estimates of an Operation from status.estimates[]. An Operation the Provider's
// controller has not written to has none, and every estimate then reads pending, then unknown. An entry's
// shape (the hash that tells two submissions apart) is not read: a target has one shape, so the entries that
// cover it are told by the plan ID, the Provider and the target's UID.
func estimateEntries(op *v1alpha1.Operation) []estimateEntry {
	out := make([]estimateEntry, 0, len(op.Status.Estimates))
	for _, e := range op.Status.Estimates {
		entry := estimateEntry{Provider: e.Provider, PlanID: e.PlanID, Start: estimateStart{Unknown: e.Start.Unknown}}
		for _, uid := range e.Targets {
			entry.Targets = append(entry.Targets, types.UID(uid))
		}
		if e.Start.Time != nil {
			t := e.Start.Time.Time
			entry.Start.Time = &t
		}
		if e.Start.ObservedAt != nil {
			entry.Start.ObservedAt = e.Start.ObservedAt.Time
		}
		out = append(out, entry)
	}
	return out
}

type estimateKind string

const (
	estimateKnown   estimateKind = "known"
	estimatePending estimateKind = "pending"
	estimateUnknown estimateKind = "unknown"
)

// estimate is the derived state of one target's estimate.
type estimate struct {
	Kind       estimateKind
	Time       time.Time // known
	Unknown    string    // unknown: why
	ObservedAt time.Time // known, or a failed entry: when the entry was read
	Age        time.Duration
}

// deriveEstimate derives the estimate of the target t of the plan from the entries, at now. The caller
// reads the clock once and hands it in, so that a figure and its age are of one moment.
func deriveEstimate(plan *v1alpha1.OperationPlan, t v1alpha1.PlanTarget, entries []estimateEntry, now time.Time) estimate {
	var best *estimateEntry
	for i := range entries {
		e := &entries[i]
		if e.PlanID != plan.ID || e.Provider != t.Provider {
			continue // another plan's, or another Provider's
		}
		if len(e.Targets) > 0 && !slices.Contains(e.Targets, t.UID) {
			continue // covers other targets of the Provider
		}
		if best == nil || e.Start.ObservedAt.After(best.Start.ObservedAt) {
			best = e
		}
	}
	if best == nil {
		if now.Sub(plan.ObservedAt.Time) <= estimateWindow {
			return estimate{Kind: estimatePending}
		}
		return estimate{Kind: estimateUnknown, Unknown: noEstimateReason}
	}
	s := best.Start
	switch {
	case s.Unknown != "":
		return estimate{Kind: estimateUnknown, Unknown: s.Unknown, ObservedAt: s.ObservedAt}
	case s.Time == nil:
		return estimate{Kind: estimateUnknown, Unknown: "the entry carries no start time and no reason", ObservedAt: s.ObservedAt}
	}
	return estimate{Kind: estimateKnown, Time: *s.Time, ObservedAt: s.ObservedAt, Age: max(now.Sub(s.ObservedAt), 0)}
}

// hasEstimate says whether a target shows an estimate: it has a cost, and a Provider that could answer.
func hasEstimate(t v1alpha1.PlanTarget) bool { return t.Cost != nil && t.Provider != "" }
