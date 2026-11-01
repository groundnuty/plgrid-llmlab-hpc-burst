package alpha

import "time"

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
	// Lease is the lease the run plans for each Allocation. It is below the
	// shape's time limit and above the instance's drain window (HBEP-7,
	// Provisioning: PartitionTooShort).
	Lease time.Duration
	// Budget is how long the run may take once its first job has started: the
	// second clock of the run. The first is the wait in the queue.
	Budget time.Duration
	// PostsTo names where the run's result and its slot use are posted.
	PostsTo string
}

// rows are the live rows of the runbook that run the stack.
var rows = []Row{
	{Name: "alpha-athena-handover", Cluster: "athena", Partition: "plgrid-gpu-a100", GPUs: 1, Lease: 40 * time.Minute, Budget: 150 * time.Minute, PostsTo: "#145 and #93"},
	{Name: "alpha-helios-nooverlap", Cluster: "helios", Partition: "plgrid-gpu-gh200", QOS: "now", GPUs: 1, Lease: 40 * time.Minute, Budget: 200 * time.Minute, PostsTo: "#145 and #94"},
	{Name: "tunnel-hour-helios", Cluster: "helios", Partition: "plgrid-gpu-gh200", QOS: "now", GPUs: 1, Lease: 85 * time.Minute, Budget: 110 * time.Minute, PostsTo: "#145"},
	{Name: "tunnel-hour-athena", Cluster: "athena", Partition: "plgrid-gpu-a100", GPUs: 1, Lease: 85 * time.Minute, Budget: 110 * time.Minute, PostsTo: "#145"},
}

// accountingRows are the rows of the accounting check, which runs in
// internal/providerctl/conformance (a GPU job through the Provider's controller,
// no stack and no dial-home route). Lease and Budget are the job's.
var accountingRows = []Row{
	{Name: "gpu-accounting-helios", Cluster: "helios", Partition: "plgrid-gpu-gh200", QOS: "now", GPUs: 1, Lease: 5 * time.Minute, Budget: 25 * time.Minute, PostsTo: "#145 and #93"},
	{Name: "gpu-accounting-athena", Cluster: "athena", Partition: "plgrid-gpu-a100", GPUs: 1, Lease: 5 * time.Minute, Budget: 25 * time.Minute, PostsTo: "#145 and #93"},
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
