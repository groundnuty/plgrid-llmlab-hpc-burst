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
		Func: "internal/managed.Controller.record", Why: "writes a provider.ManagedEvent, which provider.NewManagedEvent builds, to client-go's recorder; its callers' constructor calls are the emit sites",
		Forwards: []Forward{{Arg: 0, Param: 0}, {Arg: 3, Param: 1, Field: "Reason", Event: true}, {Arg: 4, Param: 1, Field: "Action", Event: true}},
	},
	{
		Func: "pkg/provider/fake.ManagedController.raise", Why: "the fake managed controller replays the provider.ManagedEvents of the shared gate; it is a test double, not an emitter",
		Forwards: []Forward{{Arg: 0, Param: 0}, {Arg: 3, Of: "ManagedEvent", Field: "Reason"}, {Arg: 4, Of: "ManagedEvent", Field: "Action"}},
	},
	{
		Func: "internal/providerctl.createdFilter.Event", Why: "passes crossplane-runtime's own events on, minus one; event.Normal and event.Warning are the emit sites",
		Forwards: []Forward{{Arg: 0, Param: 0}, {Arg: 1, Param: 1, Event: true}},
	},
	{
		Func: "internal/providerctl.external.event", Why: "hands an event that event.Normal or event.Warning built to crossplane-runtime's recorder; those calls are the emit sites",
		Forwards: []Forward{{Arg: 0, Param: 0}, {Arg: 1, Param: 1, Event: true}},
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
// states why. An entry that matches nothing is stale and fails the test.
//
// They are by reason, never by package or controller (`@hpc-burst-science-agent`,
// 2 Nov 2026), so that a new reason of the Provider controller or the managed
// catalog controller fails the guard until HBEP-8 lists it or an entry here
// does. Two entries name a function instead, since an event whose reason cannot
// be resolved has no reason to name.
//
// Nothing that is a drift between an HBEP and the code goes here.
var Exceptions = append(providerControllerEvents(), []Entry{
	{
		Kind: Unproven, Reason: "CatalogEntrySkipped",
		Why: "A managed catalog controller event, listed in the bullet under HBEP-8's table. pkg/provider builds it in RepeatGate.CatalogEvents and appends it to the slice that internal/managed's emitRead ranges over and hands to record; the scan does not follow a value through append, return and range, so it cannot show that it reaches the recorder.",
	},
	{
		Kind: Unproven, Reason: "CatalogTruncated",
		Why: "A managed catalog controller event, listed in the bullet under HBEP-8's table, built and handed on as CatalogEntrySkipped is.",
	},
	{
		Kind: EventNotConstructed, Package: "internal/managed", Func: "Controller.emitRead",
		Why: "emitRead hands the events of RepeatGate.CatalogEvents, which it ranges over, to record. They are built in that function, where the unproven entries above name their reasons, so a new reason there fails the guard; the scan does not follow the slice back to the constructor.",
	},
	{
		Kind: Unresolved, Package: "internal/managed", Func: "Controller.emitTransitions",
		Why: "The reason of these events is the reason of the Provider condition that changed, read from status at run time, and the type comes from a map over those reasons, so no reason exists to name. The scan checks what it can: every reason the managed controller can write into the conditions it reads (and the keys of its map of bad states) is a named constant, listed in HBEP-8's bullet on the managed controller's events with the type its event has, and the bullet lists nothing the code cannot write. That set is what the code can write, a superset of what it raises at a given moment.",
	},
}...)

// providerControllerEvents are the Provider controller's events, one entry per
// reason. They go through crossplane-runtime's recorder, which writes no action,
// so they are untyped in alpha and readers treat them as type other (HBEP-8,
// Events, open question 14); the table lists typed events only.
func providerControllerEvents() []Entry {
	var out []Entry
	for _, r := range []string{
		"DuplicateJob", "HeldDuplicateCancelled", "HoldLifted", "JobAdopted", "JobCancelled", "JobLost",
		"JobNameRewritten", "JobReappeared", "JobSubmitted", "SubmissionAbandoned", "SubmissionRejected",
		"SubmissionUnknown", "SubmissionUnresolved", "SubmittedAsOther",
	} {
		out = append(out, Entry{
			Kind: CodeOnly, Action: NoAction, Reason: r,
			Why: "An untyped event of the Provider controller (internal/providerctl), through crossplane-runtime's recorder, which writes no action (HBEP-8, Events, open question 14).",
		})
	}
	return out
}
