package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Expected performance and over-allocation (beta; HBEP-4, Platform profile
// schema, Expected performance; HBEP-7, Over-allocation and qualification
// (beta)). The bounds are HBEP-4's proposals, measured against the API
// server's CEL cost budget.

// DefaultOverAllocation is spec.overAllocation's default: one extra
// Allocation per Provider (owner, 29 Oct 2026: "start with k = 1 per
// provider"; kept as the default on 15 Nov 2026).
const DefaultOverAllocation int32 = 1

// OverAllocationCount is the deployment's k: spec.overAllocation, absent being
// DefaultOverAllocation. The API server defaults the field, but an object
// built in Go, or read through a client that does not default, may lack it.
func (s *ModelDeploymentSpec) OverAllocationCount() int32 {
	if s.OverAllocation == nil {
		return DefaultOverAllocation
	}
	return *s.OverAllocation
}

// DefaultTolerancePercent is an expected-performance entry's default
// tolerance: a measurement passes up to twice the entry's value (HBEP-4,
// Expected performance; HBEP-7, The throughput gate, The limits).
const DefaultTolerancePercent int32 = 100

// TolerancePercentOf is the entry's tolerance, absent being
// DefaultTolerancePercent.
func (e *ExpectedPerformance) TolerancePercentOf() int32 {
	if e.TolerancePercent == nil {
		return DefaultTolerancePercent
	}
	return *e.TolerancePercent
}

// The throughput gate's verdicts (HBEP-7, The throughput gate; How k adjusts,
// Recorded), on a start-up outcome (StartOutcome.Gate) and on the Instance's
// gate record (InstanceGate.Verdict).
const (
	// GatePassed: the measurement was within the matching entry's limits.
	GatePassed = "Passed"
	// GateMissed: the measurement missed the matching entry's limits, on the
	// first qualification and its retry.
	GateMissed = "Missed"
	// GateSuspended: the matching entry is suspended for the deployment
	// (status.suspendedReferences), so the gate recorded only.
	GateSuspended = "Suspended"
	// GateRecordOnly: no entry matched, or the matching one is 90 days or more
	// past its date, so the gate recorded only.
	GateRecordOnly = "RecordOnly"
)

// ReferenceKey is what an expected-performance entry is matched on: one
// serving spec's deployment ID, its declared capacity flags, and the
// concurrent shape one worker ran (HBEP-4, Expected performance).
type ReferenceKey struct {
	// DeploymentID is the display form of the deployment ID measured, 12
	// lowercase hex characters (HBEP-3). It carries the class, the
	// checkpoint, the engine version, the image digest and every ID-input
	// flag, so an entry holds for one class and one engine build.
	//
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{12}$`
	DeploymentID string `json:"deploymentID"`

	// CapacityFlags are the serving spec's declared flags that HBEP-3's table
	// classes as capacity, with max-num-seqs always present, from the
	// Instance's launch.maxNumSeqs. They are no deployment-ID inputs but move
	// the figures, so they are part of the key, compared by value as
	// verification compares settings (HBEP-7, Instance start).
	//
	// +kubebuilder:validation:MinProperties=1
	// +kubebuilder:validation:MaxProperties=16
	// +kubebuilder:validation:XValidation:rule="'max-num-seqs' in self",message="capacityFlags always holds max-num-seqs (HBEP-4, Expected performance)"
	// +kubebuilder:validation:XValidation:rule="self.all(k, size(k) >= 1 && size(k) <= 64)",message="a capacity flag's name is 1 to 64 characters (HBEP-4, Expected performance)"
	CapacityFlags map[string]FlagValue `json:"capacityFlags"`

	// Sessions is the number of requests one worker ran at once: a worker's
	// share of the deployment's targetSessions (HBEP-7, Several workers).
	//
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1024
	Sessions int32 `json:"sessions"`

	// ContextTokens is each request's context: contextTokens.typical.
	//
	// +kubebuilder:validation:Minimum=1
	ContextTokens int32 `json:"contextTokens"`

	// OutputTokens is each request's output: outputTokens.typical. At least 2,
	// since time per output token is taken over the tokens after the first.
	//
	// +kubebuilder:validation:Minimum=2
	OutputTokens int32 `json:"outputTokens"`
}

// FlagValue is a capacity flag's value, as declared.
//
// +kubebuilder:validation:MaxLength=64
type FlagValue string

// ExpectedPerformance is one reference measurement of one serving spec on one
// class, which the lifecycle controller's throughput gate reads (beta; HBEP-4,
// Expected performance). It comes from a reference run apart from serving,
// written into the profile by a person: no value is copied from a serving
// deployment's Instances, and the controller never derives one.
type ExpectedPerformance struct {
	ReferenceKey `json:",inline"`

	// TimePerOutputToken is the mean time per output token over the settled
	// warm-up round's concurrent requests: (total − time to first token) ÷
	// (output tokens − 1) per request, as the agent's warm-up measures it.
	//
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="timePerOutputToken must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	TimePerOutputToken Duration `json:"timePerOutputToken"`

	// TimeToFirstToken is the mean time to the first token over the same
	// requests.
	//
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="timeToFirstToken must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	TimeToFirstToken Duration `json:"timeToFirstToken"`

	// TolerancePercent is the gate's allowance above each value: a
	// measurement passes up to the value times (100 + tolerancePercent) ÷ 100,
	// and time to first token also up to the value plus 100 ms (HBEP-7, The
	// throughput gate, The limits). Absent is DefaultTolerancePercent.
	//
	// +optional
	// +kubebuilder:default=100
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=400
	TolerancePercent *int32 `json:"tolerancePercent,omitempty"`

	// Method is how it was measured: the run, the launch, how many runs on
	// how many nodes (HBEP-4, open question 7, the envelope).
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	Method string `json:"method"`

	// Source is where its record is: an issue comment, or the reference
	// Instance's verification record.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	Source string `json:"source"`

	// Date is when it was measured, a calendar date (YYYY-MM-DD, UTC). An
	// entry gates for 90 days from its date, then records only (HBEP-4,
	// Ageing and refresh).
	//
	// +kubebuilder:validation:Pattern=`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`
	Date string `json:"date"`
}

// SuspendedReference is an expected-performance entry suspended for one
// deployment: the class's two most recent verdicts against it, from two
// Allocations, were both Missed (HBEP-7, The throughput gate, A suspect
// reference). The gate records only against an entry listed with the same
// date; the listing is removed when the profile's matching entry has another
// date or is gone.
type SuspendedReference struct {
	ReferenceKey `json:",inline"`

	// Date is the suspended entry's date.
	//
	// +kubebuilder:validation:Pattern=`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`
	Date string `json:"date"`

	// SuspendedTime is when the second miss suspended it.
	SuspendedTime metav1.Time `json:"suspendedTime"`
}

// InstanceGate is the throughput gate's record on one Instance (beta; HBEP-7,
// The throughput gate). A first miss of a worker records its measurement in
// Misses and restarts that worker at rung 1; the retry decides. With several
// workers each worker is judged on its own measurement.
type InstanceGate struct {
	// Verdict is the gate's final verdict: GatePassed, GateMissed,
	// GateSuspended or GateRecordOnly. It is absent while a retry is due.
	//
	// +optional
	// +kubebuilder:validation:Enum=Passed;Missed;Suspended;RecordOnly
	Verdict string `json:"verdict,omitempty"`

	// ReferenceDate is the matched expected-performance entry's date.
	//
	// +optional
	// +kubebuilder:validation:Pattern=`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`
	ReferenceDate string `json:"referenceDate,omitempty"`

	// Misses are the measurements that missed, oldest first, one per worker
	// and attempt: at most two a worker, the first and its retry's.
	//
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=16
	Misses []GateMiss `json:"misses,omitempty"`
}

// GateMiss is one measurement that missed the gate's limits.
type GateMiss struct {
	// Worker is the worker measured, worker-<i> (WorkerName); worker-0 for an
	// Instance of one worker.
	//
	// +kubebuilder:validation:Pattern=`^worker-[0-9]+$`
	Worker string `json:"worker"`

	// Throughput is the worker's measurement.
	Throughput Throughput `json:"throughput"`

	// Time is when the gate judged it.
	Time metav1.Time `json:"time"`
}
