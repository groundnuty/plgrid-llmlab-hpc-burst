package scenario_test

// The real binaries' events in the durable log (HBEP-8, The durable log, Who writes; issue #710, the
// follow-up of #697, E2 of #625). #697's tests run each command's wiring on its own; this step runs the
// whole path under hpcburst up --event-log: the launcher's token file, the supervised hpcburst-controller
// and hpcburst-front, and HB_EVENTS, read back with a client of the step's own.
//
// An ObservationGap is no help here: each manager raises it only when its Lease already has a renewTime,
// that is, after a restart (internal/leaderlock/gap.go, GapStart: "a first start ... reports none";
// internal/front/lease.go, take). A scenario that restarts nothing sees none. The scenario's run raises
// one typed event from each of the three emitters, which the step expects instead.

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/obscheck"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// eventLogWait bounds the step's wait. Each event was raised before the step, as the Instance came up and
// served, so one read finds them; a minute covers a publisher that was still retrying.
const eventLogWait = time.Minute

// jobSubmittedReason is the Provider manager's reason for the submission of an Allocation's job
// (internal/providerctl/events.go, evJobSubmitted; action started). It is a literal here because the
// constant is not exported, and the drift guard keeps the code's reasons to the HBEP's table.
const jobSubmittedReason = "JobSubmitted"

// loggedEventsWanted are the events the log must hold after the Instance of step (a) has come up, one from
// each of the emitters that hpcburst up --event-log starts, written from HBEP-8's Reasons and Allocation
// events tables and the objects the API holds; a subject is hb.ev.<kind>.<UID of the regarding object>.
//
//   - hpcburst-controller's lifecycle controller: started/AgentRegistered, regarding the Instance, when
//     its agent registers (internal/lifecycle/instances.go).
//   - hpcburst-controller's Provider manager: started/JobSubmitted, regarding the Instance's Allocation,
//     when it submits the job (internal/providerctl/create.go).
//   - hpcburst-front: degraded/DrainWindowShort, regarding the PlatformProfile, because the profile's
//     10 min drain window is below the front's 2 h maxRequestDuration plus the drain margin, and the
//     rehearsal's front config sets none (internal/front/router.go, reportShortLocked).
func (r *rig) loggedEventsWanted(in *v1alpha1.Instance) []stack.WantLogged {
	r.t.Helper()
	var alloc *v1alpha1.Allocation
	allocs := r.allocations()
	for i := range allocs {
		if in.Status.JobID != "" && allocs[i].Status.AtProvider.JobID == in.Status.JobID {
			alloc = &allocs[i]
		}
	}
	if alloc == nil {
		r.t.Fatalf("no Allocation holds the Instance %s's job %q (Allocations:%s)", in.Name, in.Status.JobID, r.describe())
	}
	var profile v1alpha1.PlatformProfile
	if !r.get(&profile, stack.ProfileName, "") {
		r.t.Fatalf("the API has no PlatformProfile %s", stack.ProfileName)
	}
	for _, o := range []struct{ what, uid string }{{"the Instance", string(in.UID)}, {"the Allocation", string(alloc.UID)}, {"the PlatformProfile", string(profile.UID)}} {
		if o.uid == "" {
			r.t.Fatalf("%s has no UID, so its events have no subject", o.what)
		}
	}
	return []stack.WantLogged{
		{Controller: v1alpha1.FieldManagerLifecycle, Subject: "hb.ev.instance." + string(in.UID), Reason: v1alpha1.ReasonAgentRegistered, Action: "started"},
		{Controller: v1alpha1.FieldManagerProvider, Subject: "hb.ev.allocation." + string(alloc.UID), Reason: jobSubmittedReason, Action: "started"},
		{Controller: obscheck.FrontController, Subject: "hb.ev.platformprofile." + string(profile.UID), Reason: v1alpha1.ReasonDrainWindowShort, Action: "degraded"},
	}
}

// stepEventLog reads HB_EVENTS with a fresh client (the token in <state>/eventlog/token, the address at
// base+16), never through a publisher, and waits until the log holds each expected event, on its subject,
// with its reason, action and reporting controller. It fails naming each reporting controller whose event
// is missing, and what the log held. The scenario must have run with stack.EventLog.
func (r *rig) stepEventLog(in *v1alpha1.Instance) {
	r.t.Helper()
	defer r.stepTimer("event log")()
	want := r.loggedEventsWanted(in)
	var got []stack.LoggedEvent
	stack.WaitState(r.t, eventLogWait, "the durable log's HB_EVENTS to hold an event from each of the lifecycle controller, the Provider manager and the front",
		func() (bool, string) {
			ctx, cancel := stack.CallCtx(r.t)
			defer cancel()
			evs, err := r.h.ReadEvents(ctx)
			if err != nil {
				return false, "reading the log failed: " + err.Error() +
					" (hpcburst status says \"degraded: event log not started\" when the free space on the state's file system is under up's floor)"
			}
			got = evs
			missing := stack.MissingLogged(evs, want)
			return len(missing) == 0, stack.DescribeMissing(missing, evs)
		})
	var found []string
	for _, w := range want {
		found = append(found, w.Phrase())
	}
	r.t.Logf("the durable log holds %d events, including each of: %q", len(got), found)
}
