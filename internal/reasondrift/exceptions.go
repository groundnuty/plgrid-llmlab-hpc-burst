package reasondrift

// Wrappers are the pass-through wrappers of the module: functions that call a
// recorder with the arguments they were called with. Their calls are no emit
// sites. The scan accepts one only if its recorder calls pass on what Forwards
// says, fails when a wrapper is not met (the name is stale) and when a call to
// Event or Eventf is neither a sink nor in a wrapper.
var Wrappers = []Wrapper{
	{
		Func: "internal/lifecycle.KubeRecorder.Event", Why: "forwards the lifecycle Recorder's arguments to client-go's events.k8s.io recorder; the callers of Recorder.Event are the emit sites",
		Forwards: []Forward{{Arg: 0, Param: 0}, {Arg: 3, Param: 3}, {Arg: 4, Param: 2}},
	},
	{
		Func: "internal/front.KubeRecorder.Event", Why: "forwards the front Recorder's arguments to client-go's events.k8s.io recorder; the callers of Recorder.Event are the emit sites",
		Forwards: []Forward{{Arg: 0, Param: 0}, {Arg: 3, Param: 3}, {Arg: 4, Param: 2}},
	},
	{
		Func: "internal/managed.Controller.record", Why: "writes a provider.ManagedEvent to client-go's recorder; the managed controller's reasons are not compared (see Scan)",
		Forwards: []Forward{{Arg: 0, Param: 0}, {Arg: 3, Param: 1, Field: "Reason"}, {Arg: 4, Param: 1, Field: "Action"}},
	},
	{
		Func: "pkg/provider/fake.ManagedController.raise", Why: "the fake managed controller replays the provider.ManagedEvents of the shared gate; it is a test double, not an emitter",
		Forwards: []Forward{{Arg: 0, Param: 0}, {Arg: 3, Of: "ManagedEvent", Field: "Reason"}, {Arg: 4, Of: "ManagedEvent", Field: "Action"}},
	},
	{
		Func: "internal/providerctl.createdFilter.Event", Why: "passes crossplane-runtime's own events on, minus one; the Provider controller's untyped events are not compared (see Scan)",
		Forwards: []Forward{{Arg: 0, Param: 0}, {Arg: 1, Param: 1}},
	},
	{
		Func: "internal/providerctl.external.event", Why: "hands an event the Provider controller built to crossplane-runtime's recorder; those events are not compared (see Scan)",
		Forwards: []Forward{{Arg: 0, Param: 0}, {Arg: 1, Param: 1}},
	},
}

// WrapperNames returns the Func of each wrapper.
func WrapperNames() []string {
	out := make([]string, len(Wrappers))
	for i, w := range Wrappers {
		out[i] = w.Func
	}
	return out
}

// Exceptions are the mismatches that are true to the design: what the table
// cannot list, or the scan cannot resolve, by HBEP-8's own text. Each entry
// states why. An entry that matches no mismatch is stale and fails the test.
//
// The Provider controller's events (untyped, HBEP-8 open question 14) and the
// managed catalog controller's (listed in the bullet under the table) were the
// exceptions of the first version; the scan no longer reads them at all, because
// both are built by a constructor (event.Normal, provider.NewManagedEvent), and
// a constructor's call is not an emit.
//
// Nothing that is a drift between an HBEP and the code goes here; that is
// knownDriftUntilHBEP8Erratum (guard/guard_test.go) until the erratum lands.
var Exceptions = []Entry{
	{
		Kind: OutsideTheScan, Package: "internal/agent/engine", Func: "sse.go",
		Why: "sse.go reads the Event field of an SSE event struct (sseEvent), which the scan's syntax-only look at packages it does not load takes for a recorder method. It records no Kubernetes event.",
	},
}
