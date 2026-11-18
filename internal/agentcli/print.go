package agentcli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// The text output. It is made from the same views as --json, so the two say the same.

// emitJSON writes v as indented JSON. HTML escaping is off: a message holds "<" and ">" and means them.
func emitJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// printOperation writes an Operation: its request, its phase, its conditions, its plan and its report.
func printOperation(w io.Writer, v operationView) {
	fmt.Fprintf(w, "operation %s (namespace %s): %s\n", v.Name, v.Namespace, requestText(v))
	fmt.Fprintf(w, "  owner: %s\n", v.Owner)
	if v.Purpose != "" {
		fmt.Fprintf(w, "  purpose: %s\n", v.Purpose)
	}
	fmt.Fprintf(w, "  phase: %s\n", v.Phase)
	if v.ConfirmedPlan != "" {
		fmt.Fprintf(w, "  confirmed plan: %s\n", v.ConfirmedPlan)
	} else {
		fmt.Fprintln(w, "  not confirmed")
	}
	if v.FinishedAt != "" {
		fmt.Fprintf(w, "  finished: %s\n", v.FinishedAt)
	}
	for _, c := range v.Conditions {
		fmt.Fprintf(w, "  condition %s=%s", c.Type, c.Status)
		if c.Reason != "" {
			fmt.Fprintf(w, " %s", c.Reason)
		}
		if c.Message != "" {
			fmt.Fprintf(w, ": %s", c.Message)
		}
		fmt.Fprintln(w)
	}
	if v.Plan != nil {
		printPlan(w, *v.Plan)
	}
	if len(v.Targets) > 0 {
		fmt.Fprintln(w, "report:")
		for _, t := range v.Targets {
			fmt.Fprintf(w, "  %s %s: %s", t.Kind, t.Name, t.State)
			if t.Reason != "" {
				fmt.Fprintf(w, ", %s", t.Reason)
			}
			if t.Message != "" {
				fmt.Fprintf(w, ": %s", t.Message)
			}
			fmt.Fprintln(w)
		}
	}
}

// requestText is what the Operation asks for, in a phrase.
func requestText(v operationView) string {
	switch {
	case v.Scale != nil:
		return fmt.Sprintf("%s ModelDeployment %s to %d Instances", v.Action, v.ModelDeployment, v.Scale.Instances)
	case v.End != nil && v.End.Delete:
		return fmt.Sprintf("%s ModelDeployment %s and delete it", v.Action, v.ModelDeployment)
	case v.Release != nil && v.Release.Selector != "":
		return fmt.Sprintf("%s the Allocations of ModelDeployment %s that match %q", v.Action, v.ModelDeployment, v.Release.Selector)
	case v.Release != nil:
		return fmt.Sprintf("%s %s of ModelDeployment %s", v.Action, strings.Join(v.Release.Allocations, ", "), v.ModelDeployment)
	}
	return fmt.Sprintf("%s ModelDeployment %s", v.Action, v.ModelDeployment)
}

// printPlan writes a plan: each target, what happens to it, whether it needs approval and under which
// policies, then its cost.
func printPlan(w io.Writer, p planView) {
	fmt.Fprintf(w, "plan %s for ModelDeployment %s, observed %s\n", p.ID, p.ModelDeployment.Name, p.ObservedAt)
	for _, t := range p.Targets {
		fmt.Fprintf(w, "  %s %s", t.Kind, t.Name)
		if t.Provider != "" {
			fmt.Fprintf(w, " (provider %s)", t.Provider)
		}
		if t.NeedsApproval {
			fmt.Fprintf(w, ": %s, needs approval", t.Effect)
		} else {
			fmt.Fprintf(w, ": %s, needs no approval", t.Effect)
		}
		if t.ApprovalRule != "" {
			fmt.Fprintf(w, " (%s)", t.ApprovalRule)
		}
		fmt.Fprintln(w)
		for _, pol := range t.Policies {
			fmt.Fprintf(w, "    policy %s: approvers %s\n", pol.Name, approversText(pol.Approvers))
		}
		printTargetCost(w, t)
	}
}

// printTargetCost writes one target's cost and estimate: every figure with its observation time, an
// unknown figure with its reason and never as a zero, the note, and "advisory" on an estimate. It is the
// one place the cost is printed.
func printTargetCost(w io.Writer, t targetView) {
	if t.Cost != nil {
		c := t.Cost
		if c.Note != "" {
			fmt.Fprintf(w, "    note: %s\n", c.Note)
		}
		if c.LastReady {
			fmt.Fprintln(w, "    last ready Instance: the model serves nothing on its own capacity until a new Allocation starts and its Instance is ready")
		}
		printFigure(w, "lease left", c.LeaseLeft)
		printFigure(w, "GPU-hours left", c.GPUHoursLeft)
		printFigure(w, "queue age", c.QueueAge)
		printFigure(w, "start estimate", c.StartEstimate)
		printQueueWaits(w, c.QueueWaits)
		printFigure(w, "start to ready", c.StartToReady)
	}
	if t.Estimate != nil {
		fmt.Fprintf(w, "    %s\n", estimateText(*t.Estimate))
	}
}

// printFigure writes one figure: label, value or "unknown: reason", then when it was observed.
func printFigure(w io.Writer, label string, f *figureView) {
	if f == nil {
		return
	}
	if f.Advisory {
		label += " (advisory)"
	}
	fmt.Fprintf(w, "    %s: %s%s\n", label, figureValue(f), observed(f.ObservedAt))
}

func figureValue(f *figureView) string {
	switch {
	case f.Unknown != "":
		return "unknown: " + f.Unknown
	case f.Duration != "":
		return f.Duration
	case f.Time != "":
		return f.Time
	}
	return f.GPUHours
}

func observed(at string) string {
	if at == "" {
		return ""
	}
	return " (observed " + at + ")"
}

func printQueueWaits(w io.Writer, q *queueWaitsView) {
	if q == nil {
		return
	}
	if q.Unknown != "" {
		fmt.Fprintf(w, "    provider queue waits: unknown: %s%s\n", q.Unknown, observed(q.ObservedAt))
		return
	}
	fmt.Fprintf(w, "    provider queue waits: median %s, p90 %s, max %s, of %d%s\n", q.Median, q.P90, q.Max, q.Count, observed(q.ObservedAt))
}

// estimateText is the line of an estimate: "estimate: <time> (advisory, read <age> ago)",
// "estimate: pending" or "estimate: unknown: <reason>".
func estimateText(e estimateView) string {
	switch e.State {
	case string(estimateKnown):
		return fmt.Sprintf("estimate: %s (advisory, read %s ago)", e.Time, e.Age)
	case string(estimatePending):
		return "estimate: pending"
	}
	return "estimate: unknown: " + e.Unknown
}

// printApprovers writes who must approve a plan that waits: each policy of a target that needs approval,
// once, with its approvers.
func printApprovers(w io.Writer, p *planView) {
	if p == nil {
		return
	}
	seen := map[string]bool{}
	for _, t := range p.Targets {
		if !t.NeedsApproval {
			continue
		}
		for _, pol := range t.Policies {
			if !seen[pol.Name] {
				seen[pol.Name] = true
				fmt.Fprintf(w, "  must approve, policy %s: %s\n", pol.Name, approversText(pol.Approvers))
			}
		}
	}
}
