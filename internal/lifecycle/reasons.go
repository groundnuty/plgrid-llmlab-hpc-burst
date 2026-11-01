package lifecycle

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
