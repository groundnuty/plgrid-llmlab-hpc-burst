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

// HBEP-8's event types. A Kubernetes Event's reason is "<type>/<code>", so a
// reader acts on the type and the reason code, never on the message (HBEP-7,
// Events and conditions; HBEP-8).
const (
	eventStarted    = "started"
	eventReady      = "ready"
	eventDegraded   = "degraded"
	eventFailing    = "failing"
	eventReplaced   = "replaced"
	eventEndingSoon = "ending-soon"
)
