package reasondrift

// KnownDrift is the drift between HBEP-8 and HBEP-7 and the code on the day
// the guard was added (2 Nov 2026, #239). It is a temporary allowlist: the test
// passes with it, so the guard can merge before the erratum, and fails without it
// (REASONDRIFT_STRICT=1), which is the test failing on today's tree.
//
// Delete each entry when the erratum, or a code change, fixes what it names. An
// entry whose mismatch is gone is stale, and the test fails until the entry is
// removed, so the pull request that fixes the drift shows which lines it
// removes. When the list is empty, delete it and the switch in drift_test.go.
//
// Every entry says which side should change (the report is on #239).
var KnownDrift = []Entry{
	// The reasons whose constants are not in api/v1alpha1. HBEP-8 says the
	// reason strings are the contracts', plus those marked proposed. These are
	// implemented, as package-local constants or string literals, so the
	// proposed marks are stale. CODE should move them into api/v1alpha1: front
	// and lifecycle each spell HeartbeatMissed and ObservationGap on their own.
	{Kind: NoConstant, Reason: "CountsDrift", Why: "front-local constant (internal/front); HBEP-8 marks it proposed in PR #95"},
	{Kind: NoConstant, Reason: "HandedOver", Why: "string literal in internal/lifecycle; HBEP-8 marks it proposed in PR #88"},
	{Kind: NoConstant, Reason: "HeartbeatMissed", Why: "package-local constants in both internal/lifecycle and internal/front; HBEP-8 marks it proposed"},
	{Kind: NoConstant, Reason: "NoReplacementReady", Why: "string literal in internal/lifecycle; HBEP-8 marks it proposed in PR #88"},
	{Kind: NoConstant, Reason: "ObservationGap", Why: "package-local constants in both internal/lifecycle and internal/front; HBEP-8 marks it proposed"},
	{Kind: NoConstant, Reason: "PathProbeFailing", Why: "front-local constant (internal/front); HBEP-8 marks it proposed, HBEP-6"},
	{Kind: NoConstant, Reason: "Ready", Why: "string literal in internal/lifecycle; HBEP-8 marks it proposed in PR #88"},
	{Kind: NoConstant, Reason: "SubmittedAsOther", Why: "the code uses ConditionSubmittedAsOther, the condition's type constant, as the event's reason"},
	{Kind: NoConstant, Reason: "UsageWriteFailed", Why: "front-local constant (internal/front); HBEP-8 marks it proposed"},

	// A deployment's event regards the Instance once the ModelDeployment is
	// gone (internal/lifecycle, mdObject). HBEP-8's table says ModelDeployment
	// only. HBEP should say so.
	{Kind: CodeOnly, Action: "degraded", Reason: "CapacityDropped", Regarding: "Instance", Package: "internal/lifecycle", Why: "mdObject falls back to the Instance when the ModelDeployment is gone; the table lists ModelDeployment only"},
	{Kind: CodeOnly, Action: "degraded", Reason: "VerificationFailed", Regarding: "Instance", Package: "internal/lifecycle", Why: "mdObject falls back to the Instance when the ModelDeployment is gone; the table lists ModelDeployment only"},
}
