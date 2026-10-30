// Package v1alpha1 holds the spike's Allocation managed resource.
//
// An Allocation is one Slurm job, submitted once and never edited (HBEP-3,
// HBEP-7). Its external name is a deterministic job tag derived from the
// object's UID; the Slurm job ID is observed state only.
package v1alpha1

import (
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// Group, version and kind of the spike's Allocation. The real API group is an
// owner decision (HBEP-11); this one is a placeholder for the spike.
const (
	Group   = "spike.hpc-burst.example"
	Version = "v1alpha1"
	Kind    = "Allocation"
)

var (
	// GroupVersion is the API group and version of Allocation.
	GroupVersion = schema.GroupVersion{Group: Group, Version: Version}
	// AllocationGroupVersionKind is the GVK of Allocation.
	AllocationGroupVersionKind = GroupVersion.WithKind(Kind)
	// SchemeBuilder registers the types with a scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	// AddToScheme adds the types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&Allocation{}, &AllocationList{})
}

// AllocationParameters are the submission parameters. They are immutable in
// practice: the management policies exclude Update, and Update refuses.
type AllocationParameters struct {
	Partition string `json:"partition,omitempty"`
	Account   string `json:"account,omitempty"`
	// TimeLimit in Slurm's format, e.g. "01:00:00".
	TimeLimit string `json:"timeLimit,omitempty"`
	// Wrap is the command passed to sbatch --wrap.
	Wrap string `json:"wrap,omitempty"`
}

// AllocationSpec is the desired state.
type AllocationSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              AllocationParameters `json:"forProvider"`
}

// Phases of an Allocation as the provider reports them.
const (
	PhasePending   = "Pending"
	PhaseRunning   = "Running"
	PhaseSucceeded = "Succeeded"
	PhaseFailed    = "Failed"
	// PhaseLost: the established job has been absent from both squeue and
	// sacct for longer than lost_grace. Never a reason to resubmit; a job
	// that reappears returns to its real phase.
	PhaseLost = "Lost"
	// PhaseSubmitting: a submission attempt is in flight.
	PhaseSubmitting = "Submitting"
	// PhaseUnknown: an sbatch outcome was ambiguous and no job with the tag
	// has been found yet.
	PhaseUnknown = "SubmissionUnknown"
	// PhaseRefused: the provider refuses to submit (non-canonical external
	// name); see the reason.
	PhaseRefused = "Refused"
)

// AllocationObservation is the observed state of the Slurm job.
type AllocationObservation struct {
	Tag          string       `json:"tag,omitempty"`
	JobID        int64        `json:"jobId,omitempty"`
	State        []string     `json:"state,omitempty"`
	Phase        string       `json:"phase,omitempty"`
	Reason       string       `json:"reason,omitempty"`
	Source       string       `json:"source,omitempty"`
	RestartCount int          `json:"restartCount,omitempty"`
	Requeued     bool         `json:"requeued,omitempty"`
	Held         bool         `json:"held,omitempty"`
	Duplicates   []int64      `json:"duplicates,omitempty"`
	Attempt      int          `json:"attempt,omitempty"`
	AttemptState string       `json:"attemptState,omitempty"`
	AbsentSince  *metav1.Time `json:"absentSince,omitempty"`
	SubmitTime   *metav1.Time `json:"submitTime,omitempty"`
	StartTime    *metav1.Time `json:"startTime,omitempty"`
	EndTime      *metav1.Time `json:"endTime,omitempty"`
}

// AllocationStatus is the observed state.
type AllocationStatus struct {
	xpv2.ManagedResourceStatus `json:",inline"`
	AtProvider                 AllocationObservation `json:"atProvider,omitempty"`
}

// Allocation is one Slurm job managed by the spike's provider.
type Allocation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AllocationSpec   `json:"spec"`
	Status AllocationStatus `json:"status,omitempty"`
}

// AllocationList is a list of Allocations.
type AllocationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Allocation `json:"items"`
}

// --- resource.ModernManaged ---

// GetCondition of this Allocation.
func (a *Allocation) GetCondition(ct xpv2.ConditionType) xpv2.Condition {
	return a.Status.GetCondition(ct)
}

// SetConditions of this Allocation.
func (a *Allocation) SetConditions(c ...xpv2.Condition) { a.Status.SetConditions(c...) }

// GetManagementPolicies of this Allocation.
func (a *Allocation) GetManagementPolicies() xpv2.ManagementPolicies {
	return a.Spec.ManagementPolicies
}

// SetManagementPolicies of this Allocation.
func (a *Allocation) SetManagementPolicies(p xpv2.ManagementPolicies) {
	a.Spec.ManagementPolicies = p
}

// GetProviderConfigReference of this Allocation.
func (a *Allocation) GetProviderConfigReference() *xpv2.ProviderConfigReference {
	return a.Spec.ProviderConfigReference
}

// SetProviderConfigReference of this Allocation.
func (a *Allocation) SetProviderConfigReference(p *xpv2.ProviderConfigReference) {
	a.Spec.ProviderConfigReference = p
}

// GetWriteConnectionSecretToReference of this Allocation.
func (a *Allocation) GetWriteConnectionSecretToReference() *xpv2.LocalSecretReference {
	return a.Spec.WriteConnectionSecretToReference
}

// SetWriteConnectionSecretToReference of this Allocation.
func (a *Allocation) SetWriteConnectionSecretToReference(r *xpv2.LocalSecretReference) {
	a.Spec.WriteConnectionSecretToReference = r
}

// GetObservedGeneration of this Allocation.
func (a *Allocation) GetObservedGeneration() int64 { return a.Status.GetObservedGeneration() }

// SetObservedGeneration of this Allocation.
func (a *Allocation) SetObservedGeneration(g int64) { a.Status.SetObservedGeneration(g) }

// --- runtime.Object (hand-written deep copies; the spike has no codegen) ---

// DeepCopyInto copies a into out.
func (in *Allocation) DeepCopyInto(out *Allocation) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.ManagedResourceSpec.DeepCopyInto(&out.Spec.ManagedResourceSpec)
	out.Spec.ForProvider = in.Spec.ForProvider
	in.Status.ManagedResourceStatus.DeepCopyInto(&out.Status.ManagedResourceStatus)
	in.Status.AtProvider.deepCopyInto(&out.Status.AtProvider)
}

func (in *AllocationObservation) deepCopyInto(out *AllocationObservation) {
	*out = *in
	if in.State != nil {
		out.State = append([]string(nil), in.State...)
	}
	if in.Duplicates != nil {
		out.Duplicates = append([]int64(nil), in.Duplicates...)
	}
	out.SubmitTime = in.SubmitTime.DeepCopy()
	out.StartTime = in.StartTime.DeepCopy()
	out.EndTime = in.EndTime.DeepCopy()
	out.AbsentSince = in.AbsentSince.DeepCopy()
}

// DeepCopy returns a deep copy.
func (in *Allocation) DeepCopy() *Allocation {
	if in == nil {
		return nil
	}
	out := new(Allocation)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject implements runtime.Object.
func (in *Allocation) DeepCopyObject() runtime.Object { return in.DeepCopy() }

// DeepCopyObject implements runtime.Object.
func (in *AllocationList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(AllocationList)
	*out = *in
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Allocation, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
	return out
}
