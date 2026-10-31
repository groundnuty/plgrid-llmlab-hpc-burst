package lifecycle

// reasonCatalogEntryListed is a managed source's Ready reason when it is true:
// an allowed managed Provider on the entry's platform lists it now (HBEP-7,
// Reconcile loop, step 1). conditions.go names only the false reasons; this
// one is proposed to the contracts owner.
const reasonCatalogEntryListed = "CatalogEntryListed"

// reasonAgentRegistered is the Provider's DialHomeUnreachable reason when it is
// cleared, False: an agent on the Provider registered through the dial-home
// path. conditions.go names only the True reason (RegistrationTimeout); this
// one is proposed to the contracts owner.
const reasonAgentRegistered = "AgentRegistered"

// HBEP-8's event types. A Kubernetes Event's reason is the CamelCase reason
// code alone, and its type is the events.k8s.io action, so a reader acts on
// the action and the reason, never on the note (HBEP-7, Events and conditions;
// HBEP-8). degraded and failing are Warning events, the others Normal.
const (
	eventStarted    = "started"
	eventReady      = "ready"
	eventDegraded   = "degraded"
	eventFailing    = "failing"
	eventReplaced   = "replaced"
	eventEndingSoon = "ending-soon"
)
