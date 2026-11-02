package alpha

// The live Ledger (issue #245): the cluster's own record of the run's jobs,
// read by tag over the Provider's ssh through the plugin's batch client
// (internal/slurm), with the reads the Provider's controller makes itself: the
// queue listing (squeue) for the jobs it still shows, and accounting (sacct)
// for the rest (slurm.Batch.FindByTag). It never reads the controllers' account
// of the jobs: the verdicts judge the cluster's times. The same client ends the
// run's jobs by tag when the steps close (EndJobs).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// ErrNoLedger is what the live Ledger returns when the run never made its
// batch client: the run stopped before its Provider was read, so there is no
// record to judge.
var ErrNoLedger = errors.New("the cluster's record cannot be read: the run made no batch client (its Provider was never read)")

// AccountingSlack is how far before the run's start accounting is read: the
// cluster's clock is not this host's, and a job submitted at the run's first
// second must not fall outside the window.
const AccountingSlack = time.Hour

// TagFinder is the read of the plugin's batch client the Ledger makes (slurm.Batch).
type TagFinder interface {
	FindByTag(ctx context.Context, tag string, since time.Time) ([]provider.Job, error)
}

// BatchLedger is the Ledger over a batch client.
type BatchLedger struct {
	// Finder is the batch client; nil until the run has read its Provider.
	Finder TagFinder
	// Since is how far back accounting is read: the run's start less AccountingSlack.
	Since time.Time
}

// Jobs reads every job of each tag, in the order of the tags.
func (l *BatchLedger) Jobs(ctx context.Context, tags []string) ([]provider.Job, error) {
	if l == nil || l.Finder == nil {
		return nil, ErrNoLedger
	}
	var out []provider.Job
	for _, tg := range tags {
		js, err := l.Finder.FindByTag(ctx, tg, l.Since)
		if err != nil {
			return nil, fmt.Errorf("the jobs of a run's tag could not be read: %w", err)
		}
		out = append(out, js...)
	}
	return out, nil
}

// SSHBatch is the plugin's batch client for a Provider in the API, over the
// Provider's own profile and credentials, with the run's control directory and
// host lock: the transport the Provider's controller builds
// (internal/providerctl/factory.go), with one call in flight per host. Its
// master is the run's own, beside the controller's. The returned Closer stops
// that master.
func SSHBatch(ctx context.Context, r client.Reader, providerName, controlDir, hostLock string) (*slurm.Batch, io.Closer, error) {
	v, err := (&slurm.ProfileReader{Reader: r}).Read(ctx, providerName)
	if err != nil {
		return nil, nil, fmt.Errorf("the Provider's profile and credentials: %w", err)
	}
	tr, err := sshrun.New(sshrun.Options{Name: "alpha-" + v.Settings.Provider, Config: v.SSH, ControlDir: controlDir, HostLock: hostLock, MaxConcurrent: 1})
	if err != nil {
		return nil, nil, fmt.Errorf("the run's ssh transport: %w", err)
	}
	// Every read is fresh: the run reads only to judge and to end its jobs, and a
	// cached listing could miss a job submitted since.
	fresh := slurm.Options{PollInterval: time.Nanosecond, BusyInterval: time.Nanosecond}
	return slurm.NewBatch(tr, v.Profile, v.Settings, fresh), tr, nil
}

// JobEnder is what EndJobs needs of the batch client (slurm.Batch).
type JobEnder interface {
	ListByTag(ctx context.Context, tag string) ([]provider.Job, error)
	Cancel(ctx context.Context, jobIDs []string) []provider.CancelResult
}

// EndJobs ends the run's jobs: it reads the queue for each tag and cancels the
// jobs it still lists live, and repeats every interval until a pass finds none
// live, or ctx ends. A cancelled job leaves the live states only when the
// scheduler has acted, so the pass after a cancel is what shows it worked. The
// batch client must read the queue fresh on every pass (SSHBatch's does). Its
// errors name no job: a tag's position in the run stands for it.
func EndJobs(ctx context.Context, b JobEnder, tags []string, every time.Duration) error {
	for {
		live, errs := endPass(ctx, b, tags)
		if live == 0 && len(errs) == 0 {
			return nil
		}
		t := time.NewTimer(every)
		select {
		case <-ctx.Done():
			t.Stop()
			if live > 0 {
				errs = append(errs, fmt.Errorf("%d of the run's jobs were still live when the time to end them ran out", live))
			}
			return errors.Join(errs...)
		case <-t.C:
		}
	}
}

// endPass is one pass of EndJobs: how many jobs it found live, which it cancelled.
func endPass(ctx context.Context, b JobEnder, tags []string) (live int, errs []error) {
	for i, tg := range tags {
		js, err := b.ListByTag(ctx, tg)
		if err != nil {
			errs = append(errs, fmt.Errorf("the queue could not be read for the run's tag %d: %w", i+1, err))
			continue
		}
		var ids []string
		for _, j := range js {
			if liveJob(j) {
				ids = append(ids, j.ID)
			}
		}
		live += len(ids)
		if len(ids) == 0 {
			continue
		}
		for _, res := range b.Cancel(ctx, ids) {
			if res.Outcome == provider.CancelError {
				errs = append(errs, fmt.Errorf("a job of the run's tag %d could not be cancelled: %w", i+1, res.Err))
			}
		}
	}
	return live, errs
}

// liveJob reports whether a job can still hold a GPU or a queue place: held,
// pending or running. A cancelled job whose processes are still ending
// (COMPLETING) has the phase of its base state, Ended.
func liveJob(j provider.Job) bool {
	switch j.Phase {
	case v1alpha1.AllocationHeld, v1alpha1.AllocationPending, v1alpha1.AllocationRunning:
		return true
	}
	return false
}
