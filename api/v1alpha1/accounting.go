package v1alpha1

import (
	"encoding/json"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Provider accounting (HBEP-8, Provider accounting; HBEP-4, The Slurm dialect,
// Allocated time). Alpha counts allocated time: the GPUs or CPUs the scheduler
// allocated times the time they were allocated. Every number is "allocated, not
// billed"; the billing formula is open and blocks beta.

// AccountingBasisAllocated is the basis of every alpha accounting number.
const AccountingBasisAllocated = "allocated"

// AllocationAccounting is the allocated time of the Allocation's recorded job,
// as the Provider's controller last read it. A field the controller could not
// read is absent, never 0: absent means unknown. The whole block is absent
// while the job has not started, so a pending Allocation's hours are unknown.
// The numbers are allocated, not billed.
//
// The hours are derived: GPUHours and CPUHours.
type AllocationAccounting struct {
	// AllocatedCPUs is the number of CPUs the scheduler allocated: the cpu
	// entry of accounting's tres.allocated. Absent when accounting gave none.
	//
	// +optional
	// +kubebuilder:validation:Minimum=0
	AllocatedCPUs *int32 `json:"allocatedCPUs,omitempty"`

	// AllocatedGPUs is the number of GPUs the scheduler allocated: the gres
	// entry named gpu of accounting's tres.allocated; 0 for a job that asked
	// for no GPU and whose accounting lists none. Absent when a GPU was asked
	// for and accounting lists none, or has not been read.
	//
	// +optional
	// +kubebuilder:validation:Minimum=0
	AllocatedGPUs *int32 `json:"allocatedGPUs,omitempty"`

	// ElapsedSeconds is how long the job has been allocated: while it runs,
	// from the scheduler's start time to ReadTime; once it has ended, the queue
	// listing's end time minus its start time when the final state came from
	// the listing, and otherwise accounting's time.elapsed summed over every
	// execution. Absent when unknown, for example for a requeued job whose
	// final state came from the listing, whose earlier executions are unknown.
	//
	// +optional
	// +kubebuilder:validation:Minimum=0
	ElapsedSeconds *int64 `json:"elapsedSeconds,omitempty"`

	// ReadTime is when the controller made the read that these numbers rest
	// on. The Provider's clock; UTC.
	ReadTime metav1.Time `json:"readTime"`
}

// GPUSeconds returns the allocated GPU-seconds: AllocatedGPUs times
// ElapsedSeconds. ok is false when either is unknown.
func (a AllocationAccounting) GPUSeconds() (seconds int64, ok bool) {
	if a.AllocatedGPUs == nil || a.ElapsedSeconds == nil {
		return 0, false
	}
	return int64(*a.AllocatedGPUs) * *a.ElapsedSeconds, true
}

// CPUSeconds returns the allocated CPU-seconds: AllocatedCPUs times
// ElapsedSeconds. ok is false when either is unknown.
func (a AllocationAccounting) CPUSeconds() (seconds int64, ok bool) {
	if a.AllocatedCPUs == nil || a.ElapsedSeconds == nil {
		return 0, false
	}
	return int64(*a.AllocatedCPUs) * *a.ElapsedSeconds, true
}

// GPUHours returns the allocated GPU-hours, GPUSeconds over 3600; allocated,
// not billed. ok is false when the GPU count or the elapsed time is unknown.
func (a AllocationAccounting) GPUHours() (hours float64, ok bool) {
	s, ok := a.GPUSeconds()
	return float64(s) / 3600, ok
}

// CPUHours returns the allocated CPU-hours, CPUSeconds over 3600; allocated,
// not billed. ok is false when the CPU count or the elapsed time is unknown.
func (a AllocationAccounting) CPUHours() (hours float64, ok bool) {
	s, ok := a.CPUSeconds()
	return float64(s) / 3600, ok
}

// AccountingRecordName returns the name of an Allocation's accounting record:
// acct-<Allocation UID> (HBEP-8, Provider accounting; HBEP-11, Ownership,
// records and deletion). It returns an error for an empty UID, or one that does
// not make a valid object name. The API server gives every object a lowercase
// UUID, which always does.
func AccountingRecordName(uid types.UID) (string, error) {
	name := AccountingRecordPrefix + string(uid)
	if uid == "" {
		return "", fmt.Errorf("an accounting record needs the Allocation's UID")
	}
	if errs := validation.IsDNS1123Subdomain(name); len(errs) != 0 {
		return "", fmt.Errorf("%q is not a valid accounting record name: %v", name, errs)
	}
	return name, nil
}

// Names of the fields of AccountingRecordData that can be unknown, as the
// record's Unknown list spells them.
const (
	AccountingFieldAllocatedCPUs  = "allocatedCPUs"
	AccountingFieldAllocatedGPUs  = "allocatedGPUs"
	AccountingFieldElapsedSeconds = "elapsedSeconds"
	AccountingFieldStartTime      = "startTime"
	AccountingFieldEndTime        = "endTime"
)

// AccountingRecordData is the data of an accounting record: what the Provider's
// controller knew when the Allocation's last job had ended and its finalizer was
// about to go (HBEP-8, Provider accounting). A field that was not known is
// absent and named in Unknown, so a reader tells "unknown" from a field an
// older version did not write. Hours are derived as for AllocationAccounting:
// allocated, not billed.
type AccountingRecordData struct {
	// Provider is the Provider's name.
	Provider string `json:"provider"`

	// Partition is the partition the job ran on.
	Partition string `json:"partition"`

	// Tag is the Allocation's job tag, the job's name.
	Tag string `json:"tag"`

	// JobIDs are the IDs of every job that carried the tag, in numeric order;
	// the numbers below are the recorded job's.
	JobIDs []string `json:"jobIDs"`

	// AllocatedCPUs, AllocatedGPUs and ElapsedSeconds are as in
	// AllocationAccounting.
	AllocatedCPUs  *int32 `json:"allocatedCPUs,omitempty"`
	AllocatedGPUs  *int32 `json:"allocatedGPUs,omitempty"`
	ElapsedSeconds *int64 `json:"elapsedSeconds,omitempty"`

	// StartTime and EndTime are the recorded job's start and end, from the
	// scheduler (the latest execution's, for a requeued job).
	StartTime *metav1.Time `json:"startTime,omitempty"`
	EndTime   *metav1.Time `json:"endTime,omitempty"`

	// Basis is AccountingBasisAllocated.
	Basis string `json:"basis"`

	// Unknown names each field above that is absent because it was not known,
	// in the order AllocatedCPUs, AllocatedGPUs, ElapsedSeconds, StartTime,
	// EndTime. NewAccountingRecord fills it, so it always matches the fields.
	//
	// +optional
	Unknown []string `json:"unknown,omitempty"`
}

// unknownFields lists the fields of d that are absent, in a fixed order.
func (d AccountingRecordData) unknownFields() []string {
	var u []string
	if d.AllocatedCPUs == nil {
		u = append(u, AccountingFieldAllocatedCPUs)
	}
	if d.AllocatedGPUs == nil {
		u = append(u, AccountingFieldAllocatedGPUs)
	}
	if d.ElapsedSeconds == nil {
		u = append(u, AccountingFieldElapsedSeconds)
	}
	if d.StartTime == nil {
		u = append(u, AccountingFieldStartTime)
	}
	if d.EndTime == nil {
		u = append(u, AccountingFieldEndTime)
	}
	return u
}

// NewAccountingRecord returns the accounting record of an Allocation: a
// ControllerRevision named acct-<Allocation UID> in the Allocation's namespace,
// with no owner reference, so garbage collection never removes it. Its labels
// are record=accounting and provider=<the Provider's name>; modelDeployment,
// when the Allocation was assigned to one at deletion, adds the
// model-deployment label (LabelValue) and the annotation
// AnnotationModelDeployment with the full name. Data is d as JSON, with Basis
// set and Unknown filled. The caller creates the record if it is absent and
// never rewrites it; Revision is 1.
func NewAccountingRecord(namespace string, uid types.UID, modelDeployment string, d AccountingRecordData) (*appsv1.ControllerRevision, error) {
	name, err := AccountingRecordName(uid)
	if err != nil {
		return nil, err
	}
	if d.Provider == "" {
		return nil, fmt.Errorf("an accounting record needs the Provider's name")
	}
	if errs := validation.IsDNS1123Label(d.Provider); len(errs) != 0 {
		return nil, fmt.Errorf("Provider name %q is not a DNS-1123 label: %v", d.Provider, errs)
	}
	d.Basis = AccountingBasisAllocated
	d.Unknown = d.unknownFields()
	if d.JobIDs == nil {
		d.JobIDs = []string{}
	}
	data, err := json.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("accounting record data: %w", err)
	}
	labels := map[string]string{LabelRecord: RecordAccounting, LabelProvider: d.Provider}
	var annotations map[string]string
	if modelDeployment != "" {
		labels[LabelModelDeployment] = LabelValue(modelDeployment)
		annotations = map[string]string{AnnotationModelDeployment: modelDeployment}
	}
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels, Annotations: annotations},
		Data:       runtime.RawExtension{Raw: data},
		Revision:   1,
	}, nil
}
