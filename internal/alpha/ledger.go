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
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

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
// master is the run's own, beside the controller's. The returned transport
// runs the run's own commands (the worker fault's srun) and its Close stops
// that master.
func SSHBatch(ctx context.Context, r client.Reader, providerName, controlDir, hostLock string) (*slurm.Batch, *sshrun.SSH, error) {
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

// JobEnder is what EndJobs needs of the batch client (slurm.Batch): the
// queue listing of every job of the user, read fresh, and the cancel.
type JobEnder interface {
	Queue(ctx context.Context) ([]provider.Job, error)
	Cancel(ctx context.Context, jobIDs []string) []provider.CancelResult
}

// Owned says which job names of the queue listing are the run's own.
type Owned func(name string) bool

// RunOwned is the run's ownership: a name with the run's own tag prefix
// (TagPrefix: every Allocation of the run's deployment is named after it, so
// every job the controller submits for the run carries it), or a tag the run
// wrote down. The prefix does not depend on the recorder: a job whose
// Allocation the watch never delivered, or whose tag could not be written, is
// still the run's (review of PR 255, B3). A name that is neither is never
// touched, whoever's it is.
func RunOwned(prefix string, rec *Recorder) Owned {
	return func(name string) bool {
		if prefix != "" && strings.HasPrefix(name, prefix) {
			return true
		}
		return rec != nil && rec.Tagged(name)
	}
}

// EndJobs ends the run's jobs: it reads the queue listing, cancels the run's
// jobs (owned) that the scheduler still runs or queues, and repeats every
// interval until a pass finds none of the run's jobs alive, or ctx ends. A
// cancelled job is alive until the scheduler has acted, and a job whose end
// state is set stays alive while it is COMPLETING, so the pass after a cancel
// is what shows it worked. The batch client must read the queue fresh on every
// pass (SSHBatch's does). Its errors name no job and no tag.
func EndJobs(ctx context.Context, b JobEnder, owned Owned, every time.Duration) error {
	for {
		live, errs := endPass(ctx, b, owned)
		if live == 0 && len(errs) == 0 {
			return nil
		}
		t := time.NewTimer(every)
		select {
		case <-ctx.Done():
			t.Stop()
			if live > 0 {
				errs = append(errs, fmt.Errorf("%d of the run's jobs were still alive when the time to end them ran out", live))
			}
			return errors.Join(errs...)
		case <-t.C:
		}
	}
}

// endPass is one pass of EndJobs: how many of the run's jobs it found alive.
// It cancels each that has not ended; one whose end state is set but that is
// still COMPLETING is counted, not cancelled again.
func endPass(ctx context.Context, b JobEnder, owned Owned) (live int, errs []error) {
	js, err := b.Queue(ctx)
	if err != nil {
		return 0, []error{fmt.Errorf("the queue could not be read: %w", err)}
	}
	var ids []string
	for _, j := range js {
		if !owned(j.Name) || !liveJob(j) {
			continue
		}
		live++
		if !endedBase(j) {
			ids = append(ids, j.ID)
		}
	}
	if len(ids) == 0 {
		return live, nil
	}
	for _, res := range b.Cancel(ctx, ids) {
		if res.Outcome == provider.CancelError {
			errs = append(errs, fmt.Errorf("a job of the run could not be cancelled: %w", res.Err))
		}
	}
	return live, errs
}

// endedStates are Slurm's base states of a job that is over (squeue(1), JOB
// STATE CODES): its end state is set. Any other base state, one this list does
// not know included, is a job the scheduler still holds, queues or runs.
var endedStates = map[string]bool{
	"BOOT_FAIL": true, "CANCELLED": true, "COMPLETED": true, "DEADLINE": true, "FAILED": true,
	"NODE_FAIL": true, "OUT_OF_MEMORY": true, "PREEMPTED": true, "TIMEOUT": true,
}

// endedBase reports whether the job's base state (State[0]) is an end state.
// A job with no state has not shown that it ended.
func endedBase(j provider.Job) bool {
	return len(j.State) > 0 && endedStates[strings.ToUpper(j.State[0])]
}

// liveJob reports whether the scheduler still counts a job as alive, by its
// own state, not by the plugin's phase: a job whose base state has not ended
// (held, pending, running, suspended, a requeued job queued or running again,
// or a state this does not know), and a job whose end state is set but that
// is still COMPLETING, whose processes or epilog may still run on its node.
// The phase would miss both: a requeued running job has the phase Failed, and
// a cancelled one still completing the phase Ended (review of PR 255, B4).
func liveJob(j provider.Job) bool {
	return j.Completing || !endedBase(j)
}
