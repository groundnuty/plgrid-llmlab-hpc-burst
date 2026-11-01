// Package providerctl is the Slurm Provider's controller (HBEP-4, Two
// controllers): crossplane-runtime's managed reconciler over the Allocations
// that name one Provider, its external client implementing HBEP-7 open
// question 11's submission protocol, the Provider's status writer, and the
// supervisor that runs one controller-runtime manager per Provider.
//
// The external client maps onto HBEP-4's core operations:
//   - Connect reads and validates the Provider, its PlatformProfile and its
//     credentials (slurm.ProfileReader); a failure makes no call.
//   - Observe finds the tag, adopts a job when none is recorded, settles (in
//     hold mode: cancels every held job with the tag except the recorded one,
//     then lifts the recorded job's hold once), reads the recorded job back,
//     reports Lost after lostGrace counting only reads that succeeded, and a
//     requeue as Failed.
//   - Create checks the prerequisites and stages the files, claims an attempt
//     by compare-and-swap, submits, records the job ID by compare-and-swap and
//     settles in the same reconcile.
//   - Delete cancels every live job with the tag; the finalizer stays while one
//     is live or an attempt is unresolved; then the Allocation's files go.
//   - Update is never called: the management policies exclude it.
//
// Origin: ported from spike S4 (spikes/s4/internal/controller and
// spikes/s4/cmd/provider-slurm), onto the contracts in api/v1alpha1 and
// pkg/provider; the critical-annotation updater and the tag initializer are
// S4's. The Provider status writer and the supervisor are new.
package providerctl

import (
	"context"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Platform is what the controller calls: provider.Batch plus the reads that
// HBEP-4 needs and the core interface does not carry (the login-node paths for
// agent.json, the dial-home key check, a listing-only lookup for settle, and the
// poll-rate hint). internal/slurm.Batch
// implements it; tests wrap pkg/provider/fake.Batch. It stays internal to this
// controller: nothing another component consumes passes through it (queue
// waits reach the lifecycle controller through Provider.status.queueWaits).
type Platform interface {
	provider.Batch

	// ReadAccounting reads one job from accounting alone, past the queue
	// listing (sacct -j): the record from accounting with Job.Accounting set.
	// The controller calls it once for a recorded job after it is first seen
	// started, to learn the CPUs and GPUs the scheduler allocated (HBEP-4, Poll
	// rate; HBEP-8, Provider accounting). found is false when the read
	// succeeded and accounting does not show the job. internal/slurm.Batch and
	// pkg/provider/fake.Batch implement it.
	ReadAccounting(ctx context.Context, jobID string) (job provider.Job, found bool, err error)

	// ListByTag returns the jobs with the tag in the queue listing only (held,
	// pending, running): what settle needs, without an accounting read.
	ListByTag(ctx context.Context, tag string) ([]provider.Job, error)

	// Paths resolves the login node's HOME and stagingDir, once per
	// connection. An unresolved path wraps provider.ErrPathUnresolved.
	Paths(ctx context.Context) (slurm.Paths, error)

	// DialHomeKeysPresent reports whether both dial-home files exist.
	DialHomeKeysPresent(ctx context.Context) (bool, error)

	// SetBusy marks a tag as having an unresolved attempt or a held job, so
	// the queue listing is read at the short interval.
	SetBusy(tag string, busy bool)

	// Health reports the platform's state for the Provider's status.
	Health() slurm.Health
}

var _ Platform = (*slurm.Batch)(nil)

// Clock reads the time. pkg/provider/fake.Clock satisfies it.
type Clock interface{ Now() time.Time }
