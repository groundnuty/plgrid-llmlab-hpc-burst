package reasondrift

// Wrappers are the pass-through wrappers of the module: functions that call a
// recorder with the arguments they were called with. Their calls are no emit
// sites. The scan fails when a wrapper is not met (the name is stale) and when
// a call to Event or Eventf is neither a sink nor in a wrapper.
var Wrappers = []Wrapper{
	{"internal/lifecycle.KubeRecorder.Event", "forwards the lifecycle Recorder's arguments to client-go's events.k8s.io recorder; the callers of Recorder.Event are the emit sites"},
	{"internal/front.KubeRecorder.Event", "forwards the front Recorder's arguments to client-go's events.k8s.io recorder; the callers of Recorder.Event are the emit sites"},
	{"internal/managed.Controller.record", "writes a provider.ManagedEvent, which provider.NewManagedEvent builds; its calls are the emit sites"},
	{"pkg/provider/fake.ManagedController.raise", "the fake managed controller replays a provider.ManagedEvent a test hands it; it is a test double, not an emitter"},
	{"internal/providerctl.createdFilter.Event", "passes crossplane-runtime's own events on, minus one; event.Normal and event.Warning are the emit sites"},
	{"internal/providerctl.external.event", "hands an event that event.Normal or event.Warning built to crossplane-runtime's recorder; those calls are the emit sites"},
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
// Nothing that is a drift between an HBEP and the code goes here; that is
// KnownDrift until the erratum lands.
var Exceptions = []Entry{
	{
		Kind: CodeOnly, Action: NoAction, Package: "internal/providerctl",
		Why: "The Provider controller's events go through crossplane-runtime's recorder, which writes no action, so they are untyped in alpha and readers treat them as type other (HBEP-8, Events, open question 14). The table lists typed events only.",
	},
	{
		Kind: CodeOnly, Action: "degraded", Reason: "CatalogEntrySkipped", Regarding: "Provider", Package: "pkg/provider",
		Why: "A managed catalog controller event. HBEP-8 lists these reasons in the bullet under the table (Events, The managed catalog controller's events), not in a row, since the controller raises them from condition transitions and read results.",
	},
	{
		Kind: CodeOnly, Action: "degraded", Reason: "CatalogTruncated", Regarding: "Provider", Package: "pkg/provider",
		Why: "A managed catalog controller event, listed in the bullet under the table (see CatalogEntrySkipped).",
	},
	{
		Kind: Unresolved, Package: "internal/managed", Func: "Controller.emitTransitions",
		Why: "The reason of these events is the reason of the Provider condition that changed, read from status at run time, and the type comes from a map over those reasons. HBEP-8 lists them in the bullet under the table: FileRefused, NoCredentialsFile, KeyRejected, Expired and ReadsFailing as failing, ExpiresWithinADay as degraded, and FileRead, KeyAccepted, NotExpiring and CatalogRead as ready.",
	},
}
