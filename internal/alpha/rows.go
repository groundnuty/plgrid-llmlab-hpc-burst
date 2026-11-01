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

// Rows are the live rows of the runbook that run the stack. The accounting
// rows are in internal/providerctl/conformance.
var rows = []Row{
	{Name: "alpha-athena-handover", Cluster: "athena", Partition: "plgrid-gpu-a100", GPUs: 1, Lease: 40 * time.Minute, Budget: 150 * time.Minute, PostsTo: "#145 and #93"},
	{Name: "alpha-helios-nooverlap", Cluster: "helios", Partition: "plgrid-gpu-gh200", QOS: "now", GPUs: 1, Lease: 40 * time.Minute, Budget: 200 * time.Minute, PostsTo: "#145 and #94"},
	{Name: "tunnel-hour-helios", Cluster: "helios", Partition: "plgrid-gpu-gh200", QOS: "now", GPUs: 1, Lease: 85 * time.Minute, Budget: 110 * time.Minute, PostsTo: "#145"},
	{Name: "tunnel-hour-athena", Cluster: "athena", Partition: "plgrid-gpu-a100", GPUs: 1, Lease: 85 * time.Minute, Budget: 110 * time.Minute, PostsTo: "#145"},
}

// Rows returns a copy of the stack rows.
func Rows() []Row { return append([]Row(nil), rows...) }

// RowByName finds a row.
func RowByName(name string) (Row, bool) {
	for _, r := range rows {
		if r.Name == name {
			return r, true
		}
	}
	return Row{}, false
}
