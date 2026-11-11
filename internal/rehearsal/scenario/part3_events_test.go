package scenario_test

// Item 2 of issue #193, HBEP-8's events: every event of the Reasons table
// that the scenario's code paths raise, in the required form (action, reason,
// type, regarding), for the Instances and the ModelDeployments. The expected
// set is written here from the table and the scenario's steps, never from
// what a run emitted (issue #193, Verifications that prove nothing).

import (
	"fmt"
	"strings"

	eventsv1 "k8s.io/api/events/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/obscheck"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// expectedEvents is the scenario's expected set, from HBEP-8's Reasons table
// (Design details, Events) and the steps that reach each row.
func (p3 *part3) expectedEvents() []obscheck.WantEvent {
	var out []obscheck.WantEvent
	inst := func(action, reason, name, why string) obscheck.WantEvent {
		return obscheck.WantEvent{Action: action, Reason: reason, Kind: "Instance", Namespace: stack.Namespace, Name: name, Controller: obscheck.LifecycleController, Why: why}
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
			obscheck.WantEvent{Action: "degraded", Reason: v1alpha1.ReasonCapacityDropped, Kind: "ModelDeployment", Namespace: stack.Namespace, Name: d.Name, Controller: obscheck.LifecycleController,
				Related: d.primary.Name, Why: "the drain start with no replacement ready (degraded, regarding the deployment, the Instance related; HBEP-7 No-overlap mode)"},
			obscheck.WantEvent{Action: "degraded", Reason: v1alpha1.ReasonCapacityDropped, Kind: "ModelDeployment", Namespace: stack.Namespace, Name: d.Name, Controller: obscheck.FrontController,
				Why: "the primary leaves routing at its drain start below the desired count (degraded by the front; HBEP-5 Drain)"})
	}
	out = append(out,
		inst("failing", v1alpha1.ReasonRequeued, p3.requeued.inst, "the requeue of the queued successor's job (failing; HBEP-3)"),
		obscheck.WantEvent{Action: "degraded", Reason: "ObservationGap", Kind: "Lease", Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseLifecycle,
			Controller: obscheck.LifecycleController, Why: "the controller restart of step (f) (degraded, the lifecycle Lease; Gaps after a restart)"},
		// A cluster-scoped object's event regards it with no namespace.
		obscheck.WantEvent{Action: "degraded", Reason: v1alpha1.ReasonDrainWindowShort, Kind: "PlatformProfile", Name: stack.ProfileName, Controller: obscheck.FrontController,
			Why: "the front's maxRequestDuration (the 2 h default; the rehearsal's front config sets none) plus the profile's drainMargin (30 s) is above its drainWindow (10 min) (HBEP-5, Drain-window check)"})
	return out
}

// forbiddenEvents must not appear: HBEP-7 sets no ReplacementLate in
// no-overlap mode; a primary whose replacement is never ready at its drain
// start is not handed over (HBEP-8 Test plan: HandedOver only when a
// replacement serves); and a real teardown leaves no orphaned tunnel. One
// found in the Events is a failure; their absence is proven on the
// raised-events counter (checkForbidden, assertNoOrphans), since a list of
// Events cannot show one the recorder has not yet delivered (#256).
func (p3 *part3) forbiddenEvents() []obscheck.WantEvent {
	var out []obscheck.WantEvent
	for _, d := range p3.deps {
		out = append(out,
			obscheck.WantEvent{Action: "degraded", Reason: v1alpha1.ReasonReplacementLate, Kind: "ModelDeployment", Namespace: stack.Namespace, Name: d.Name, Why: "no-overlap mode plans the gap"},
			obscheck.WantEvent{Action: "replaced", Reason: "HandedOver", Kind: "Instance", Namespace: stack.Namespace, Name: d.primary.Name, Why: "no replacement is ready at the drain start"})
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

// listAllEvents lists the events.k8s.io Events of every namespace: a
// cluster-scoped object's land in default, the Leases' in hpcburst-system.
func (p3 *part3) listAllEvents() []eventsv1.Event {
	var l eventsv1.EventList
	ctx, cancel := stack.CallCtx(p3.t)
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
	var missing []obscheck.WantEvent
	var problems []string
	var evs []eventsv1.Event
	stack.WaitState(r.t, stack.WaitDeadline, "every expected event", func() (bool, string) {
		evs = p3.listAllEvents()
		missing, problems = obscheck.EventProblems(evs, want, forbid)
		var s []string
		for _, m := range missing {
			s = append(s, m.String())
		}
		return len(missing) == 0, "missing: " + strings.Join(s, "; ")
	})
	for _, p := range problems {
		r.t.Error(p)
	}
	p3.checkForbidden()
	typed := 0
	for _, e := range evs {
		if obscheck.TypedEvent(e) {
			typed++
		}
	}
	r.t.Logf("EVENTS: %d events.k8s.io events, %d of them typed and held to HBEP-8's form", len(evs), typed)
	for _, w := range want {
		r.t.Logf("EVENT raised: %s: %s", w, w.Why)
	}
	for _, f := range forbid {
		r.t.Logf("EVENT absent as required: %s: %s", f, f.Why)
	}
	for _, u := range unreachableEvents {
		r.t.Logf("EVENT unreachable: %s", u)
	}
	// Seen but neither expected nor forbidden: reported, for the hand-back.
	seen := map[string]int{}
	for _, e := range evs {
		if !obscheck.TypedEvent(e) {
			continue
		}
		hit := false
		for _, w := range want {
			if w.Matches(e) {
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
