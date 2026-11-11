package obscheck

// HBEP-8's events: the form every typed event must have and the check of an expected and a forbidden set against the
// events a run raised. The expected set is written by the caller from the Reasons table and its own steps, never from
// what a run emitted (issue #193, Verifications that prove nothing).

import (
	"fmt"
	"regexp"

	eventsv1 "k8s.io/api/events/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// WantEvent is one event the scenario must raise, or must not.
type WantEvent struct {
	Action, Reason        string
	Kind, Namespace, Name string // regarding
	Controller            string // reportingController; "" for any
	Related               string // the related object's name; "" for any
	Why                   string // the step and the HBEP line that raise it
}

func (w WantEvent) String() string {
	s := fmt.Sprintf("%s/%s regarding %s %s/%s", w.Action, w.Reason, w.Kind, w.Namespace, w.Name)
	if w.Controller != "" {
		s += " by " + w.Controller
	}
	if w.Related != "" {
		s += " related " + w.Related
	}
	return s
}

const (
	LifecycleController = v1alpha1.FieldManagerLifecycle
	FrontController     = "hpcburst-front"
)

var camelReason = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)

// hbep8Actions are HBEP-8's six types.
var hbep8Actions = map[string]bool{"started": true, "ready": true, "degraded": true, "failing": true, "ending-soon": true, "replaced": true}

// TypedEvent reports whether HBEP-8's form applies to an event: one the
// lifecycle controller or the front emitted, or any about an Instance or a
// ModelDeployment. The Provider controller's events are HBEP-4's untyped
// diagnostics in alpha (HBEP-8, open question 14), and are left out.
func TypedEvent(e eventsv1.Event) bool {
	switch e.ReportingController {
	case LifecycleController, FrontController:
		return true
	}
	return e.Regarding.Kind == "Instance" || e.Regarding.Kind == "ModelDeployment"
}

// formProblems holds one typed event to HBEP-8's form: an action that is one
// of the six types; a reason that is the CamelCase code alone; Warning exactly
// for degraded and failing; a regarding object; a note of at most 1,024
// bytes; and a reporting controller.
func formProblems(e eventsv1.Event) []string {
	var out []string
	id := fmt.Sprintf("event %s (%s/%s regarding %s %s)", e.Name, e.Action, e.Reason, e.Regarding.Kind, e.Regarding.Name)
	if !hbep8Actions[e.Action] {
		out = append(out, id+": action is not one of HBEP-8's six types")
	}
	if !camelReason.MatchString(e.Reason) {
		out = append(out, id+": reason is not a CamelCase code alone")
	}
	warn := e.Action == "degraded" || e.Action == "failing"
	if (e.Type == provider.EventKindWarning) != warn || (e.Type != provider.EventKindWarning && e.Type != provider.EventKindOrdinary) {
		out = append(out, fmt.Sprintf("%s: type %q for action %q", id, e.Type, e.Action))
	}
	if e.Regarding.Kind == "" || e.Regarding.Name == "" {
		out = append(out, id+": no regarding object")
	}
	if len(e.Note) > 1024 {
		out = append(out, fmt.Sprintf("%s: a note of %d bytes", id, len(e.Note)))
	}
	if e.ReportingController == "" {
		out = append(out, id+": no reporting controller")
	}
	return out
}

// Matches reports whether e is the event w names, in its form.
func (w WantEvent) Matches(e eventsv1.Event) bool {
	return e.Action == w.Action && e.Reason == w.Reason && e.Regarding.Kind == w.Kind && e.Regarding.Namespace == w.Namespace &&
		e.Regarding.Name == w.Name && (w.Controller == "" || e.ReportingController == w.Controller) &&
		(w.Related == "" || e.Related != nil && e.Related.Name == w.Related)
}

// EventProblems: each expected event appears at least once (the recorder
// merges isomorphic events, so never a count; HBEP-8, What the recorder
// merges and drops), with the type its action implies; no forbidden one
// appears; and every typed event has HBEP-8's form.
func EventProblems(evs []eventsv1.Event, want, forbid []WantEvent) (missing []WantEvent, problems []string) {
	for _, w := range want {
		found := false
		for _, e := range evs {
			if w.Matches(e) {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, w)
		}
	}
	for _, f := range forbid {
		for _, e := range evs {
			if f.Matches(e) {
				problems = append(problems, fmt.Sprintf("forbidden event %s (%s): %.200s", f, f.Why, e.Note))
			}
		}
	}
	for _, e := range evs {
		if e.Reason == v1alpha1.EventReasonTunnelOrphaned {
			problems = append(problems, fmt.Sprintf("forbidden event degraded/TunnelOrphaned regarding %s %s: %.200s", e.Regarding.Kind, e.Regarding.Name, e.Note))
		}
		if TypedEvent(e) {
			problems = append(problems, formProblems(e)...)
		}
	}
	return missing, problems
}
