package lifecycle

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// raisedByLifecycle lists, for each of HBEP-8's types, the reasons the
// lifecycle controller raises: the rows of HBEP-8's Reasons table whose
// emitter it is, with HeartbeatMissed (it judges the Providers', the agents'
// and the front's heartbeats), ObservationGap (at its start) and
// TunnelOrphaned (cmd/hpcburst-controller's tunnel-state reader raises it
// through this controller's recorder). Each of its series is created at 0
// (HBEP-8, Raised events); the drift guard checks this list against the emit
// sites.
var raisedByLifecycle = map[string][]string{
	eventStarted: {v1alpha1.ReasonAgentRegistered},
	eventReady:   {v1alpha1.EventReasonReady, v1alpha1.EventReasonShapesFit},
	eventDegraded: {
		v1alpha1.ReasonReplacementLate, v1alpha1.ReasonRestartLoop, v1alpha1.ReasonIdentityUnresolved,
		v1alpha1.ReasonVerificationFailed, v1alpha1.ReasonCapacityDropped, v1alpha1.EventReasonTunnelOrphaned,
		reasonObservationGap,
	},
	eventFailing: {
		v1alpha1.ReasonWorkerRestart, v1alpha1.ReasonInstanceRestart, v1alpha1.ReasonRestartLoop,
		v1alpha1.ReasonRouterRestart,
		v1alpha1.ReasonCrashCorrelated,
		v1alpha1.ReasonRegistrationTimeout, v1alpha1.ReasonStartTimeout, v1alpha1.ReasonTunnelNotConnected,
		v1alpha1.ReasonQualificationFailed, v1alpha1.ReasonVerificationFailed, v1alpha1.ReasonAllocationFailed,
		v1alpha1.ReasonAllocationLost, v1alpha1.ReasonAllocationRefused, v1alpha1.ReasonRequeued,
		string(v1alpha1.ConditionSubmittedAsOther), string(v1alpha1.ConditionRelayEndedForGood),
		v1alpha1.ReasonAllocationEndedEarly, reasonHeartbeatMissed,
	},
	eventReplaced:   {v1alpha1.EventReasonHandedOver},
	eventEndingSoon: {v1alpha1.EventReasonNoReplacementReady},
}

// RaisedEvents returns the series the lifecycle controller can raise, each
// with the Kubernetes type KubeRecorder gives its action: Warning for degraded
// and failing, Normal for the others.
func RaisedEvents() []eventcount.Series {
	var out []eventcount.Series
	for _, action := range []string{eventStarted, eventReady, eventDegraded, eventFailing, eventReplaced, eventEndingSoon} {
		typ := provider.EventKindOrdinary
		if warningType(action) {
			typ = provider.EventKindWarning
		}
		for _, reason := range raisedByLifecycle[action] {
			out = append(out, eventcount.Series{Reason: reason, Action: action, Type: typ})
		}
	}
	return out
}
