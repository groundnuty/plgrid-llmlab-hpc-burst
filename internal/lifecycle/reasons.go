package lifecycle

// Condition reasons that conditions.go does not hold yet. They are stand-ins,
// asked of the contracts owner (31 Oct 2026), who will add them to
// api/v1alpha1/conditions.go; this file then goes.
const (
	// Instance Verified, Qualified and WarmedUp.
	reasonPending         = "Pending"
	reasonPassed          = "Passed"
	reasonFailed          = "Failed"
	reasonWorkerRestart   = "WorkerRestart"
	reasonInstanceRestart = "InstanceRestart"

	// Instance Healthy, and the signals of a restart.
	reasonHealthy      = "Healthy"
	reasonProbeFailing = "ProbeFailing"
	reasonErrorsOnly   = "ErrorsOnly"
	reasonNoProgress   = "NoProgress"
	reasonPartExited   = "PartExited"

	// Instance AgentUnreachable.
	reasonReportsMissing = "ReportsMissing"
	reasonReporting      = "Reporting"

	// ModelDeployment Ready, Progressing and Degraded.
	reasonEnoughReady    = "EnoughReady"
	reasonNotEnoughReady = "NotEnoughReady"
	reasonProvisioning   = "Provisioning"
	reasonStable         = "Stable"
	reasonAsDesired      = "AsDesired"
	// reasonCatalogEntryListed: a managed source's Ready is true.
	reasonCatalogEntryListed = "CatalogEntryListed"
	// reasonIdentityUnresolved: a class has no current ID because the
	// checkpoint's file list could not be read, or the ID's display form
	// collides with another record (HBEP-3). The HBEP owner decides whether it
	// stays or folds into SpecUnsupported.
	reasonIdentityUnresolved = "IdentityUnresolved"

	// Instance end reasons beside those of conditions.go.
	endVerificationFailed = "VerificationFailed"
	endRestartLadder      = "RestartLadderExhausted"
	endScaledDown         = "ScaledDown"
	endShutdown           = "Shutdown"
)

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
