package v1alpha1

// DefaultHedgeWidth is spec.hedgeWidth's default: a hedge spans at most two
// Providers (HBEP-7, What the controller reads).
const DefaultHedgeWidth int32 = 2

// MaxStartOutcomes is how many start-up outcomes a class keeps, and the count
// at which its rate becomes known (HBEP-7, Hedging (beta), The start-up
// failure rate).
const MaxStartOutcomes = 20

// The results of a start-up outcome (StartOutcome.Result).
const (
	StartOutcomeReady  = "Ready"
	StartOutcomeFailed = "Failed"
)

// HedgeWidthCount is the deployment's hedge width: spec.hedgeWidth, absent
// being DefaultHedgeWidth. The API server defaults the field, but an object
// built in Go, or read through a client that does not default, may lack it.
func (s *ModelDeploymentSpec) HedgeWidthCount() int32 {
	if s.HedgeWidth == nil {
		return DefaultHedgeWidth
	}
	return *s.HedgeWidth
}
