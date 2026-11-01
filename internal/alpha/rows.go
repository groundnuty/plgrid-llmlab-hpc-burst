package alpha

import "time"

// The deployment timing every stack row applies: the shortest that still shows
// a handover. An Allocation whose time limit is not longer than the lead an
// Instance needs hosts none (PartitionTooShort), and the lead is the Instance's
// drain window plus handoverMargin plus health.startTimeout
// (internal/lifecycle/timelimit.go:22). The drain window is
// routing.maxRequestDuration plus the profile's drainMargin when that is shorter
// than the profile's drainWindow (api/v1alpha1/writers.go, InstanceDrainWindow).
// Time limits are whole minutes and the check is "not longer than"
// (timelimit.go:42), so the shortest lease is the lead plus one minute.
//
// With a 5-minute routing.maxRequestDuration and the shipped profiles' values
// (drainMargin 10 min, handoverMargin 30 min, health.startTimeout 1 h) the lead
// is 1 h 45 min, the figure profiles/athena.yaml:39 gives for plgrid-now, and the
// shortest lease 1 h 46 min. A shorter maxRequestDuration would shorten it, but
// the streams the rows hold across a drain start must fit in it.
const (
	MaxRequestDuration = 5 * time.Minute
	DrainMargin        = 10 * time.Minute
	HandoverMargin     = 30 * time.Minute
	StartTimeout       = time.Hour
	// IdleLimit is the idle limit of the no-overlap row's ReleaseWhenIdle: the
	// predecessor's job is released this long after its Instance ended.
	IdleLimit = 2 * time.Minute
)

// Lead is the lead an Instance needs on a platform with these margins: the
// drain window (maxRequestDuration plus drainMargin; the profile's own
// drainWindow is longer here) plus handoverMargin plus health.startTimeout.
func Lead() time.Duration {
	return MaxRequestDuration + DrainMargin + HandoverMargin + StartTimeout
}

// MinLease is the shortest leaseLimit that is longer than Lead, in whole minutes.
func MinLease() time.Duration { return Lead() + time.Minute }

// Row is one row of hack/live-checks.sh's table as the test sees it. The
// script's cluster shapes are the same facts; hack/livechecks fails when the
// two drift.
type Row struct {
	// Name is the row's name in the script.
	Name string
	// Cluster is the ssh alias and the platform: helios or athena.
	Cluster string
	// Partition is the partition the run's jobs must have run on.
	Partition string
	// QOS is the QOS the run's jobs run under; empty for the partition's default.
	QOS string
	// GPUs is the number of GPUs of each job.
	GPUs int
	// Lease is the leaseLimit of the run's deployment, and so each job's time
	// limit. A stack row uses MinLease; the accounting rows' job is 5 minutes.
	Lease time.Duration
	// Budget is how long the run may take once its first job has started: the
	// second clock of the run. The first is the wait in the queue.
	Budget time.Duration
	// SlotHold is how long the run holds the one-job slot of QOS now, for the
	// rows that run there: the owner's shared GPU (#145). Zero for a row that
	// holds no slot.
	SlotHold time.Duration
	// MostJobs is the most jobs of the run in the queue at once.
	MostJobs int
	// PostsTo names where the run's result and its slot use are posted.
	PostsTo string
}

// ProviderRef is a Provider a deployment allows, by name and by the platform it
// is on.
type ProviderRef struct {
	Name    string `json:"name"`
	Cluster string `json:"cluster"`
}

// DeploymentSpec is what a run's ModelDeployment says: the Providers it allows
// and the timing it applies. Plan gives what a row requires; the scenario
// reports what the API held.
type DeploymentSpec struct {
	Providers          []ProviderRef `json:"providers"`
	LeaseLimit         time.Duration `json:"leaseLimit"`
	MaxRequestDuration time.Duration `json:"maxRequestDuration"`
	ReleaseWhenIdle    bool          `json:"releaseWhenIdle"`
	IdleLimit          time.Duration `json:"idleLimit"`
}

// Plan is the deployment the row's scenario must apply: the row's Provider alone,
// the shortest lease, the 5-minute request deadline, and for the no-overlap row
// ReleaseWhenIdle with a short idle limit (hbep/0007-lifecycle-controller/
// README.md, the alpha run on Helios).
func (r Row) Plan() DeploymentSpec {
	d := DeploymentSpec{Providers: []ProviderRef{{Name: r.Cluster, Cluster: r.Cluster}}, LeaseLimit: r.Lease, MaxRequestDuration: MaxRequestDuration}
	if r.Name == "alpha-helios-nooverlap" {
		d.ReleaseWhenIdle, d.IdleLimit = true, IdleLimit
	}
	return d
}

// rows are the live rows of the runbook that run the stack.
var rows = []Row{
	// Two Allocations together on plgrid-gpu-a100, which has no one-job slot.
	{Name: "alpha-athena-handover", Cluster: "athena", Partition: "plgrid-gpu-a100", GPUs: 1, Lease: MinLease(), Budget: 150 * time.Minute, MostJobs: 2, PostsTo: "#145 and #93"},
	// The predecessor, and the successor pending beside it in QOS now.
	{Name: "alpha-helios-nooverlap", Cluster: "helios", Partition: "plgrid-gpu-gh200", QOS: "now", GPUs: 1, Lease: MinLease(), Budget: 200 * time.Minute, SlotHold: MinLease() + 45*time.Minute, MostJobs: 2, PostsTo: "#145 and #94"},
	{Name: "tunnel-hour-helios", Cluster: "helios", Partition: "plgrid-gpu-gh200", QOS: "now", GPUs: 1, Lease: MinLease(), Budget: 130 * time.Minute, SlotHold: MinLease(), MostJobs: 1, PostsTo: "#145"},
	{Name: "tunnel-hour-athena", Cluster: "athena", Partition: "plgrid-gpu-a100", GPUs: 1, Lease: MinLease(), Budget: 130 * time.Minute, MostJobs: 1, PostsTo: "#145"},
}

// accountingRows are the rows of the accounting check, which runs in
// internal/providerctl/conformance (a GPU job through the Provider's controller,
// no stack and no dial-home route). Lease and Budget are the job's.
var accountingRows = []Row{
	{Name: "gpu-accounting-helios", Cluster: "helios", Partition: "plgrid-gpu-gh200", QOS: "now", GPUs: 1, Lease: 5 * time.Minute, Budget: 25 * time.Minute, SlotHold: 10 * time.Minute, MostJobs: 1, PostsTo: "#145 and #93"},
	{Name: "gpu-accounting-athena", Cluster: "athena", Partition: "plgrid-gpu-a100", GPUs: 1, Lease: 5 * time.Minute, Budget: 25 * time.Minute, MostJobs: 1, PostsTo: "#145 and #93"},
}

// Rows returns a copy of the stack rows: the ones this package runs.
func Rows() []Row { return append([]Row(nil), rows...) }

// AccountingRows returns a copy of the accounting rows.
func AccountingRows() []Row { return append([]Row(nil), accountingRows...) }

// RowByName finds a row, of either kind.
func RowByName(name string) (Row, bool) {
	for _, r := range append(append([]Row(nil), rows...), accountingRows...) {
		if r.Name == name {
			return r, true
		}
	}
	return Row{}, false
}
