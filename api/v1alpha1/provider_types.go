package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProviderSpec is our access to one platform: provider settings (HBEP-4) and a
// credentials reference. Organization, platform, partition and account are
// also labels on the Provider (HBEP-3), so queries select by label.
type ProviderSpec struct {
	// PlatformProfileRef names the PlatformProfile of the provider's platform.
	// Several providers can share one platform and one profile.
	PlatformProfileRef ObjectReference `json:"platformProfileRef"`

	// Account is the account (or grant account) jobs are charged to. Provider
	// names appear in client model names, so the account is a label and a
	// field here, never part of the name.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Account string `json:"account,omitempty"`

	// Grant names the grant the account belongs to.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Grant string `json:"grant,omitempty"`

	// AllowedPartitions lists the platform partitions this provider may use.
	//
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=253
	AllowedPartitions []string `json:"allowedPartitions,omitempty"`

	// QOS is the Slurm quality of service the provider's jobs are to be
	// submitted under. Which QOS a user may name belongs to the user's
	// association, like the account, so it is a provider setting, not a
	// profile field. HBEP-4 has the Slurm provider add --qos=<qos> to sbatch,
	// after checking it against the safe pattern of every value placed in a
	// command, and fail reading the profile, with this field named, when the
	// PlatformProfile's qos does not list it; both land with #151's provider
	// change, and until then the field is stored only. Absent, no --qos is
	// sent, the association's default QOS applies, and only the partition
	// QOS's typed limits are planned for (HBEP-4, Provider settings, The
	// Slurm dialect and QOS limits; `@hpc-burst-science-agent`, 1 Nov 2026).
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	QOS string `json:"qos,omitempty"`

	// CredentialsRef refers to the provider's credentials: a mode-600 file on
	// the control-plane host, or a Secret. The API never holds credentials.
	//
	// +optional
	CredentialsRef *CredentialsReference `json:"credentialsRef,omitempty"`
}

// QueueWait is one recorded job's wait in the queue: from its submission to its
// start, both from the job's accounting record (HBEP-4, Provider status).
type QueueWait struct {
	// JobID is the job's ID.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	JobID string `json:"jobID"`

	// SubmitTime is when the scheduler accepted the job.
	SubmitTime metav1.Time `json:"submitTime"`

	// StartTime is when the job started.
	StartTime metav1.Time `json:"startTime"`
}

// Wait returns the queue wait: StartTime minus SubmitTime.
func (w QueueWait) Wait() time.Duration { return w.StartTime.Sub(w.SubmitTime.Time) }

// MaxQueueWaits is how many queue waits a Provider's status keeps (HBEP-4).
const MaxQueueWaits = 20

// AddQueueWait returns waits with w added at the end, unless a wait with w's
// job ID is already there, in which case it returns waits unchanged. When the
// result would hold more than MaxQueueWaits entries, the oldest are dropped.
// It never changes the slice it is given.
func AddQueueWait(waits []QueueWait, w QueueWait) []QueueWait {
	for _, old := range waits {
		if old.JobID == w.JobID {
			return waits
		}
	}
	out := make([]QueueWait, 0, len(waits)+1)
	out = append(out, waits...)
	out = append(out, w)
	if len(out) > MaxQueueWaits {
		out = out[len(out)-MaxQueueWaits:]
	}
	return out
}

// ProviderStatus is the observed state of a Provider.
type ProviderStatus struct {
	ReconcileStatus `json:",inline"`

	// LastSuccessfulStatusRead is when the provider's status was last read
	// successfully; its age is the age of the control plane's view.
	//
	// +optional
	LastSuccessfulStatusRead *metav1.Time `json:"lastSuccessfulStatusRead,omitempty"`

	// QueueWaits are the last MaxQueueWaits observed queue waits, oldest first
	// (HBEP-4, Provider status). Each is one recorded job's wait from
	// submission to start, from the scheduler's own submit and start times in
	// the job record (provider.Job.SubmitTime and StartTime, as Observe read
	// them from the queue listing or accounting), never from when the
	// controller saw the change. HBEP-4 names the accounting record; taking
	// the same two times from either source is a contracts decision. The
	// Allocation's status.atProvider.queueWaitRecorded makes the write
	// retryable and once-only per job. Entries are keyed
	// by job ID, so a repeated observe adds nothing; a new entry beyond the
	// cap drops the oldest (AddQueueWait). The lifecycle controller plans
	// replacements from them (HBEP-7, Handover), and they live here so a
	// restarted controller plans from the same waits.
	//
	// +optional
	// +listType=map
	// +listMapKey=jobID
	// +kubebuilder:validation:MaxItems=20
	QueueWaits []QueueWait `json:"queueWaits,omitempty"`

	// Catalog is a managed Provider's catalog: one entry per model the
	// gateway has listed (HBEP-4, Managed provider in alpha). An entry that
	// leaves the listing stays, with listed false. The front routes to an
	// entry while it is listed, and a managed-source ModelDeployment's Ready
	// follows it (HBEP-5, Request path; HBEP-7, Reconcile loop, step 1).
	// Empty on a batch Provider.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=1024
	Catalog []CatalogEntryStatus `json:"catalog,omitempty"`

	// LastCatalogRead is when the managed Provider's controller last read the
	// catalog successfully; it reads at most once a minute (HBEP-4).
	//
	// +optional
	LastCatalogRead *metav1.Time `json:"lastCatalogRead,omitempty"`

	// Conditions of the provider, for example a credentials file that others
	// can read, or an unreachable platform. The Provider's controller writes
	// them all, under FieldManagerProvider, except DialHomeUnreachable, which
	// the lifecycle controller applies under FieldManagerLifecycle; neither
	// removes the other's (docs/development.md, Single writer;
	// DialHomeUnreachableApply).
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// Provider is our access to one platform: an account or grant, its credentials
// and its settings (HBEP-3). Provider names are DNS-1123 labels because they
// appear in client model names and never contain '@', '/' or '.'.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,categories=hpcburst
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Profile",type=string,JSONPath=`.spec.platformProfileRef.name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$') && size(self.metadata.name) <= 63",message="a Provider name must be a DNS-1123 label: lowercase letters, digits and '-', starting and ending with a letter or digit, at most 63 characters, so no '.', '@' or '/'"
type Provider struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProviderSpec   `json:"spec"`
	Status ProviderStatus `json:"status,omitempty"`
}

// ProviderList is a list of Providers.
//
// +kubebuilder:object:root=true
type ProviderList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Provider `json:"items"`
}

func init() { SchemeBuilder.Register(&Provider{}, &ProviderList{}) }

// CatalogEntryStatus is one model a managed gateway has listed (HBEP-4,
// Managed provider in alpha).
type CatalogEntryStatus struct {
	// Name is the model's name in the catalog, the catalogName a
	// managed-source ModelDeployment names.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Revision is the revision the gateway reports for the entry, absent when
	// it reports none.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Revision *string `json:"revision,omitempty"`

	// ManagedID is the entry's managed deployment ID, over the platform, the
	// catalog name and the revision (HBEP-3, Deployment ID; pkg/deploymentid).
	// The pinned name model@<provider>/<deployment-id> resolves on its display
	// form (HBEP-5).
	ManagedID FullDeploymentID `json:"managedID"`

	// Listed is whether the latest successful catalog read listed the entry.
	Listed bool `json:"listed"`

	// LastListedTime is when a catalog read last listed the entry.
	//
	// +optional
	LastListedTime *metav1.Time `json:"lastListedTime,omitempty"`
}

// ProviderPollInterval is the longest a batch Provider's controller goes
// without reading the platform: on a pass that observes no job it calls
// provider.Batch.Poll, so the Ready condition always rests on a real read
// (HBEP-4, Operational readiness 1).
const ProviderPollInterval = time.Minute

// ProviderReadyStaleness is the oldest a batch Provider's last successful
// status read may be while the Provider counts as ready (HBEP-4, Operational
// readiness 1): five poll intervals, so one or two failed reads do not flip
// it. The Provider's controller sets Ready False with ReasonReadsFailing once
// a pass fails its reads and the last success is older than this, or there
// was none; a reader also checks the age itself (ProviderReady), because a
// stopped controller sets nothing.
const ProviderReadyStaleness = 5 * ProviderPollInterval

// ProviderReady reports whether a batch Provider counts as ready at now: its
// Ready condition is True and its lastSuccessfulStatusRead is at most
// ProviderReadyStaleness old. The lifecycle controller skips a Provider for
// which it is false (HBEP-7, Reconcile loop, step 1). A nil Provider is not
// ready.
func ProviderReady(p *Provider, now time.Time) bool {
	if p == nil || p.Status.LastSuccessfulStatusRead == nil {
		return false
	}
	ready := false
	for _, c := range p.Status.Conditions {
		if c.Type == ConditionProviderReady {
			ready = c.Status == metav1.ConditionTrue
		}
	}
	return ready && now.Sub(p.Status.LastSuccessfulStatusRead.Time) <= ProviderReadyStaleness
}
