package scenario_test

// Item 2 of issue #193, HBEP-8's events: every event of the Reasons table
// that the scenario's code paths raise, in the required form (action, reason,
// type, regarding), for the Instances and the ModelDeployments. The expected
// set is written here from the table and the scenario's steps, never from
// what a run emitted (issue #193, Verifications that prove nothing).

import (
	"fmt"
	"regexp"
	"strings"

	eventsv1 "k8s.io/api/events/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// wantEvent is one event the scenario must raise, or must not.
type wantEvent struct {
	action, reason string
	kind, ns, name string // regarding
	controller     string // reportingController; "" for any
	related        string // the related object's name; "" for any
	why            string // the step and the HBEP line that raise it
}

func (w wantEvent) String() string {
	s := fmt.Sprintf("%s/%s regarding %s %s/%s", w.action, w.reason, w.kind, w.ns, w.name)
	if w.controller != "" {
		s += " by " + w.controller
	}
	if w.related != "" {
		s += " related " + w.related
	}
	return s
}

const (
	lifecycleController = v1alpha1.FieldManagerLifecycle
	frontController     = "hpcburst-front"
)

// expectedEvents is the scenario's expected set, from HBEP-8's Reasons table
// (Design details, Events) and the steps that reach each row.
func (p3 *part3) expectedEvents() []wantEvent {
	var out []wantEvent
	inst := func(action, reason, name, why string) wantEvent {
		return wantEvent{action: action, reason: reason, kind: "Instance", ns: namespace, name: name, controller: lifecycleController, why: why}
	}
	for _, d := range p3.deps {
		for _, in := range []string{d.primary.Name, d.succ.Name} {
			out = append(out,
				inst("started", v1alpha1.ReasonAgentRegistered, in, "the agent registers (started; Instance start)"),
				inst("ready", "Ready", in, "verified, qualified and warmed up (ready; Instance start)"),
				inst("ending-soon", "NoReplacementReady", in,
					"ready with its drain start under an hour away (the 7 min lease) and no replacement ready: in no-overlap mode none can be (ending-soon)"))
		}
		out = append(out,
			wantEvent{action: "degraded", reason: v1alpha1.ReasonCapacityDropped, kind: "ModelDeployment", ns: namespace, name: d.name, controller: lifecycleController,
				related: d.primary.Name, why: "the drain start with no replacement ready (degraded, regarding the deployment, the Instance related; HBEP-7 No-overlap mode)"},
			wantEvent{action: "degraded", reason: v1alpha1.ReasonCapacityDropped, kind: "ModelDeployment", ns: namespace, name: d.name, controller: frontController,
				why: "the primary leaves routing at its drain start below the desired count (degraded by the front; HBEP-5 Drain)"})
	}
	out = append(out,
		inst("failing", v1alpha1.ReasonRequeued, p3.requeued.inst, "the requeue of the queued successor's job (failing; HBEP-3)"),
		wantEvent{action: "degraded", reason: "ObservationGap", kind: "Lease", ns: v1alpha1.ControllerNamespace, name: v1alpha1.LeaseLifecycle,
			controller: lifecycleController, why: "the controller restart of step (f) (degraded, the lifecycle Lease; Gaps after a restart)"},
		// A cluster-scoped object's event regards it with no namespace.
		wantEvent{action: "degraded", reason: v1alpha1.ReasonDrainWindowShort, kind: "PlatformProfile", name: profileName, controller: frontController,
			why: "the front's maxRequestDuration (the 2 h default; the rehearsal's front config sets none) plus the profile's drainMargin (30 s) is above its drainWindow (10 min) (HBEP-5, Drain-window check)"})
	return out
}

// forbiddenEvents must not appear: HBEP-7 sets no ReplacementLate in
// no-overlap mode; a primary whose replacement is never ready at its drain
// start is not handed over (HBEP-8 Test plan: HandedOver only when a
// replacement serves); and a real teardown leaves no orphaned tunnel.
func (p3 *part3) forbiddenEvents() []wantEvent {
	var out []wantEvent
	for _, d := range p3.deps {
		out = append(out,
			wantEvent{action: "degraded", reason: v1alpha1.ReasonReplacementLate, kind: "ModelDeployment", ns: namespace, name: d.name, why: "no-overlap mode plans the gap"},
			wantEvent{action: "replaced", reason: "HandedOver", kind: "Instance", ns: namespace, name: d.primary.Name, why: "no replacement is ready at the drain start"})
	}
	return out
}

// unreachableEvents are the rows of the Reasons table this scenario cannot
// raise, and why. They are reported, not asserted.
var unreachableEvents = []string{
	"failing/WorkerRestart, InstanceRestart, RestartLoop (Instance and ModelDeployment): no part of an Instance is killed here; #146's step (d) raises WorkerRestart",
	"failing/RegistrationTimeout, StartTimeout, TunnelNotConnected: every agent registers, and every Instance is ready well inside health.startTimeout",
	"failing/QualificationFailed, VerificationFailed and degraded/VerificationFailed: the engine double passes its checks",
	"failing/AllocationFailed, AllocationLost, AllocationRefused, SubmittedAsOther, AllocationEndedEarly: no job fails, vanishes, is refused, renamed or ends early",
	"degraded/IdentityUnresolved: one deployment ID per deployment, with no display-form clash",
	"ready/ShapesFit (ModelDeployment): raised when a deployment whose shapes failed verification is changed to fit (internal/lifecycle/verify.go, shape-lift); every shape fits here from the start",
	"degraded/ReplacementLate and replaced/HandedOver: forbidden here (no-overlap mode); #146's step (c) raises HandedOver",
	"degraded/TunnelOrphaned: forbidden here (a real teardown)",
	"degraded/CountsDrift (front): the front's counts never drift on a correct run",
	"degraded/PathProbeFailing (front): a ready, connected Instance's probe fails only on a race here (the requeue's kill against the controller's frps read)",
	"failing/HeartbeatMissed: no observer stops for longer than its staleness rule; the restart's Lease gap is shorter than a check can rely on",
	"failing/UsageWriteFailed (front): the usage directory accepts every write",
	"degraded/ObservationGap of the front: the front is not restarted",
	"the managed catalog controller's ready/degraded/failing events: they regard the Provider, and its conditions never fail or clear here",
}

var camelReason = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)

// hbep8Actions are HBEP-8's six types.
var hbep8Actions = map[string]bool{"started": true, "ready": true, "degraded": true, "failing": true, "ending-soon": true, "replaced": true}

// typedEvent reports whether HBEP-8's form applies to an event: one the
// lifecycle controller or the front emitted, or any about an Instance or a
// ModelDeployment. The Provider controller's events are HBEP-4's untyped
// diagnostics in alpha (HBEP-8, open question 14), and are left out.
func typedEvent(e eventsv1.Event) bool {
	switch e.ReportingController {
	case lifecycleController, frontController:
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

// matches reports whether e is the event w names, in its form.
func (w wantEvent) matches(e eventsv1.Event) bool {
	return e.Action == w.action && e.Reason == w.reason && e.Regarding.Kind == w.kind && e.Regarding.Namespace == w.ns &&
		e.Regarding.Name == w.name && (w.controller == "" || e.ReportingController == w.controller) &&
		(w.related == "" || e.Related != nil && e.Related.Name == w.related)
}

// eventProblems: each expected event appears at least once (the recorder
// merges isomorphic events, so never a count; HBEP-8, What the recorder
// merges and drops), with the type its action implies; no forbidden one
// appears; and every typed event has HBEP-8's form.
func eventProblems(evs []eventsv1.Event, want, forbid []wantEvent) (missing []wantEvent, problems []string) {
	for _, w := range want {
		found := false
		for _, e := range evs {
			if w.matches(e) {
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
			if f.matches(e) {
				problems = append(problems, fmt.Sprintf("forbidden event %s (%s): %.200s", f, f.why, e.Note))
			}
		}
	}
	for _, e := range evs {
		if e.Reason == v1alpha1.EventReasonTunnelOrphaned {
			problems = append(problems, fmt.Sprintf("forbidden event degraded/TunnelOrphaned regarding %s %s: %.200s", e.Regarding.Kind, e.Regarding.Name, e.Note))
		}
		if typedEvent(e) {
			problems = append(problems, formProblems(e)...)
		}
	}
	return missing, problems
}

// listAllEvents lists the events.k8s.io Events of every namespace: a
// cluster-scoped object's land in default, the Leases' in hpcburst-system.
func (p3 *part3) listAllEvents() []eventsv1.Event {
	var l eventsv1.EventList
	ctx, cancel := callCtx(p3.t)
	defer cancel()
	if err := p3.c.List(ctx, &l); err != nil {
		p3.t.Fatalf("list events.k8s.io events: %v", err)
	}
	return l.Items
}

// checkEvents waits until every expected event exists, then checks the
// forbidden ones and the form, and reports the table.
func (p3 *part3) checkEvents() {
	r := p3.rig
	r.t.Helper()
	want, forbid := p3.expectedEvents(), p3.forbiddenEvents()
	var missing []wantEvent
	var problems []string
	var evs []eventsv1.Event
	waitState(r.t, waitDeadline, "every expected event", func() (bool, string) {
		evs = p3.listAllEvents()
		missing, problems = eventProblems(evs, want, forbid)
		var s []string
		for _, m := range missing {
			s = append(s, m.String())
		}
		return len(missing) == 0, "missing: " + strings.Join(s, "; ")
	})
	for _, p := range problems {
		r.t.Error(p)
	}
	typed := 0
	for _, e := range evs {
		if typedEvent(e) {
			typed++
		}
	}
	r.t.Logf("EVENTS: %d events.k8s.io events, %d of them typed and held to HBEP-8's form", len(evs), typed)
	for _, w := range want {
		r.t.Logf("EVENT raised: %s: %s", w, w.why)
	}
	for _, f := range forbid {
		r.t.Logf("EVENT absent as required: %s: %s", f, f.why)
	}
	for _, u := range unreachableEvents {
		r.t.Logf("EVENT unreachable: %s", u)
	}
	// Seen but neither expected nor forbidden: reported, for the hand-back.
	seen := map[string]int{}
	for _, e := range evs {
		if !typedEvent(e) {
			continue
		}
		hit := false
		for _, w := range want {
			if w.matches(e) {
				hit = true
				break
			}
		}
		if !hit {
			seen[fmt.Sprintf("%s/%s regarding %s by %s", e.Action, e.Reason, e.Regarding.Kind, e.ReportingController)]++
		}
	}
	for k, n := range seen {
		r.t.Logf("EVENT also seen (%d): %s", n, k)
	}
}
