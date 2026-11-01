// Package fake has the in-memory fakes of the provider side (HBEP-4, Test
// plan): Batch, a deterministic Slurm that implements provider.Batch, and
// Controller, a stand-in for one Provider's controller that drives Allocation
// and Provider status for the lifecycle controller's tests; and, for the
// managed kind, Managed, a scripted gateway that implements provider.Managed,
// and ManagedController, the stand-in that writes a managed Provider's catalog
// and conditions. The doc comment of Controller lists the semantics tests rely
// on, and that of ManagedController the same for the catalog loop.
//
// Batch also implements Poll (op "squeue-me"), the read a pass that observes no
// job makes so that Controller's Ready condition rests on a real read; and the
// test controls Requeue and RequeueHold, which send a job back to PENDING (the
// second held by its user). Controller models the readiness rule of
// docs/development.md, Provider readiness, the queue wait recorded at once and
// once per job (Allocation queueWaitRecorded), the read-back (a renamed job is
// Refused and cancelled, another partition, limit or GPU count sets
// SubmittedAsOther), the retention mirror and HBEP-7's adoption order.
//
// Nothing here starts a goroutine or sleeps. A test owns a Clock and moves it;
// every job state is computed from the clock at each call.
package fake

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

var _ provider.Batch = (*Batch)(nil)

// safeValue is the pattern every value placed in a command must match (HBEP-4,
// Transport): letters, digits and ._:=,/-.
var safeValue = regexp.MustCompile(`^[A-Za-z0-9._:=,/-]+$`)

// Partition is one partition of the fake platform.
type Partition struct {
	Name string
	// GPUModel is "none" for a CPU partition.
	GPUModel    string
	GPUsPerNode int32
	MaxTime     time.Duration
}

// Config configures a Batch.
type Config struct {
	Partitions []Partition
	// MinJobAge is how long a finished job stays in the queue listing; zero
	// means 10 s.
	MinJobAge time.Duration
	// DataParser is the data_parser version the replies carry; empty means
	// "v0.0.42".
	DataParser string
}

// Script says what the next Submit does. Enqueue queues one per future Submit.
type Script struct {
	// Outcome: "" means Accepted.
	Outcome provider.SubmitOutcome
	// Registered, for Unknown, says the scheduler registered the job anyway
	// (the lost-reply case). A Rejected submission never registers.
	Registered bool
	// Reason is the text of the error that Rejected and Unknown return. The
	// error's message is exactly this text.
	Reason string
	// QueueWait is the time from eligibility to start. A job is eligible at
	// its submit time, or at its hold-lift time when it was held.
	QueueWait time.Duration
	// StartEstimate, when above 0, makes a pending job's estimate
	// eligibility plus StartEstimate. Otherwise there is none.
	StartEstimate time.Duration
	// FailAfter, when set, makes the job fail that long after its start, in
	// state FailState (default "FAILED"; for example "BOOT_FAIL" or
	// "NODE_FAIL"). Zero is a failed start.
	FailAfter *time.Duration
	FailState string
	// AdminHold makes the job land held by an administrator (JobHeldAdmin),
	// whatever the request's Hold.
	AdminHold bool
	// Rename makes the scheduler store this name instead of the tag.
	Rename string
	// AsTimeLimit establishes the job with this limit instead of the
	// requested one.
	AsTimeLimit time.Duration
}

// DefaultScript applies when no Script is queued: Accepted, no queue wait.
var DefaultScript = Script{Outcome: provider.Accepted}

// Call is one entry of the call log.
type Call struct {
	Op   string
	Args []string
	Time time.Time
}

// Batch is an in-memory Slurm. It is safe for concurrent use.
//
// Vanish and FailReads hide or fail reads only. Cancel and LiftHold act on the
// scheduler's real jobs, vanished or not, as the scheduler would.
type Batch struct {
	clock *Clock
	cfg   Config

	mu      sync.Mutex
	nextID  int
	jobs    map[string]*jobRec
	scripts []Script
	files   map[string]provider.File
	calls   []Call
	readErr error
	// liftErrs are the errors the next LiftHold calls return, one each, in
	// order (FailNextLiftHold).
	liftErrs []error
}

// NewBatch returns a fake Slurm that reads clock.
func NewBatch(clock *Clock, cfg Config) *Batch {
	cfg.Partitions = append([]Partition(nil), cfg.Partitions...)
	if cfg.MinJobAge == 0 {
		cfg.MinJobAge = 10 * time.Second
	}
	if cfg.DataParser == "" {
		cfg.DataParser = "v0.0.42"
	}
	return &Batch{
		clock:  clock,
		cfg:    cfg,
		nextID: 998,
		jobs:   map[string]*jobRec{},
		files:  map[string]provider.File{},
	}
}

// Enqueue queues one Script per future Submit, consumed in order. A request
// refused before any call consumes none.
func (b *Batch) Enqueue(scripts ...Script) {
	for _, s := range scripts {
		switch s.Outcome {
		case "", provider.Accepted, provider.Rejected, provider.Unknown:
		default:
			panic(fmt.Sprintf("fake: script has unknown outcome %q", s.Outcome))
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.scripts = append(b.scripts, scripts...)
}

// log appends to the call log. The caller holds b.mu.
func (b *Batch) log(op string, args ...string) {
	b.calls = append(b.calls, Call{Op: op, Args: append([]string(nil), args...), Time: b.clock.Now()})
}

// Calls returns a copy of the call log. Content bytes are never in it.
func (b *Batch) Calls() []Call {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Call, len(b.calls))
	for i, c := range b.calls {
		c.Args = append([]string(nil), c.Args...)
		out[i] = c
	}
	return out
}

// Count returns how many calls the log holds for op. HBEP-4: submissions are
// counted from this log, never from return values.
func (b *Batch) Count(op string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, c := range b.calls {
		if c.Op == op {
			n++
		}
	}
	return n
}

// checkRequest refuses, before any call, what HBEP-4 says is refused.
func (b *Batch) checkRequest(req provider.SubmitRequest) error {
	refuse := func(format string, a ...any) error {
		return fmt.Errorf("%w: %s", provider.ErrInvalidRequest, fmt.Sprintf(format, a...))
	}
	if !safeValue.MatchString(req.Tag) {
		return refuse("tag %q is empty or unsafe", req.Tag)
	}
	if req.Account != "" && !safeValue.MatchString(req.Account) {
		return refuse("account %q is unsafe", req.Account)
	}
	var part *Partition
	for i := range b.cfg.Partitions {
		if b.cfg.Partitions[i].Name == req.Params.Partition {
			part = &b.cfg.Partitions[i]
			break
		}
	}
	if part == nil {
		return refuse("partition %q is not allowed", req.Params.Partition)
	}
	gpus := req.Params.GPUs
	switch {
	case gpus < 0:
		return refuse("gpus %d is negative", gpus)
	case gpus == 0 && part.GPUModel != "none":
		return refuse("partition %q is a GPU partition and the request has no GPU", part.Name)
	case gpus > part.GPUsPerNode:
		return refuse("gpus %d is above gpusPerNode %d of partition %q", gpus, part.GPUsPerNode, part.Name)
	}
	limit, err := req.Params.TimeLimit.AsDuration()
	switch {
	case err != nil:
		return refuse("time limit %q: %v", req.Params.TimeLimit, err)
	case limit <= 0 || limit%time.Minute != 0:
		return refuse("time limit %q is not a positive whole number of minutes", req.Params.TimeLimit)
	case limit > part.MaxTime:
		return refuse("time limit %q is above maxTime %s of partition %q", req.Params.TimeLimit, part.MaxTime, part.Name)
	}
	return nil
}

// Submit implements provider.Batch.
func (b *Batch) Submit(_ context.Context, req provider.SubmitRequest) (string, provider.SubmitOutcome, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkRequest(req); err != nil {
		return "", provider.Rejected, err
	}
	b.log("sbatch", req.Tag, req.Params.Partition, string(req.Params.TimeLimit), strconv.Itoa(int(req.Params.GPUs)))
	sc := DefaultScript
	if len(b.scripts) > 0 {
		sc, b.scripts = b.scripts[0], b.scripts[1:]
	}
	switch sc.Outcome {
	case provider.Rejected:
		return "", provider.Rejected, errors.New(reasonOr(sc.Reason, "Batch job submission failed: scripted rejection"))
	case provider.Unknown:
		if sc.Registered {
			b.register(req, sc)
		}
		return "", provider.Unknown, errors.New(reasonOr(sc.Reason, "scripted unknown outcome: the reply was lost"))
	default:
		return b.register(req, sc), provider.Accepted, nil
	}
}

func reasonOr(reason, def string) string {
	if reason == "" {
		return def
	}
	return reason
}

// register creates the job the scheduler would hold after an sbatch. The
// caller holds b.mu, and the request has passed checkRequest.
func (b *Batch) register(req provider.SubmitRequest, sc Script) string {
	now := b.clock.Now()
	limit, _ := req.Params.TimeLimit.AsDuration()
	if sc.AsTimeLimit > 0 {
		limit = sc.AsTimeLimit
	}
	name := req.Tag
	if sc.Rename != "" {
		name = sc.Rename
	}
	j := &jobRec{
		id:        strconv.Itoa(b.nextID),
		tag:       req.Tag,
		name:      name,
		partition: req.Params.Partition,
		limit:     limit,
		gpus:      req.Params.GPUs,
		cpus:      req.Params.CPUsPerTask,
		submit:    now,
		script:    sc,
		eligible:  now,
	}
	b.nextID++
	switch {
	case sc.AdminHold:
		j.hold = provider.HoldAdmin
	case req.Hold:
		j.hold = provider.HoldUser
	}
	b.jobs[j.id] = j
	return j.id
}

// byID sorts jobs by job ID, numerically (Slurm IDs), never in listing order.
func byID(jobs []provider.Job) {
	sort.Slice(jobs, func(i, k int) bool {
		a, errA := strconv.Atoi(jobs[i].ID)
		c, errC := strconv.Atoi(jobs[k].ID)
		if errA != nil || errC != nil {
			return jobs[i].ID < jobs[k].ID
		}
		return a < c
	})
}

// FindByTag implements provider.Batch. Accounting shows a job the queue
// listing no longer does, if it was submitted at or after since. A vanished
// job shows in neither.
func (b *Batch) FindByTag(_ context.Context, tag string, since time.Time) ([]provider.Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.log("squeue", tag)
	if b.readErr != nil {
		return nil, b.readErr
	}
	b.log("sacct", tag)
	now := b.clock.Now()
	var out []provider.Job
	for _, j := range b.jobs {
		if j.name != tag || j.vanished {
			continue
		}
		r := b.record(j, now)
		if r.Source == provider.SourceAccounting && r.SubmitTime.Before(since) {
			continue
		}
		out = append(out, r)
	}
	byID(out)
	return out, nil
}

// Poll implements provider.Batch: one read of the queue listing, logged as op
// "squeue-me". FailReads makes it fail.
func (b *Batch) Poll(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.log("squeue-me")
	return b.readErr
}

// Observe implements provider.Batch. It has no window: accounting finds a
// purged job by its ID.
func (b *Batch) Observe(_ context.Context, jobID string) (provider.Job, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.log("squeue", jobID)
	if b.readErr != nil {
		return provider.Job{}, false, b.readErr
	}
	j, ok := b.jobs[jobID]
	if !ok || j.vanished {
		b.log("sacct", jobID)
		return provider.Job{}, false, nil
	}
	r := b.record(j, b.clock.Now())
	if r.Source == provider.SourceAccounting {
		b.log("sacct", jobID)
	}
	return r, true, nil
}

// ReadAccounting reads one job from accounting alone, past the queue listing
// (sacct -j <id>), as the Slurm plugin does once after a job is first seen
// started (HBEP-4, Poll rate; HBEP-8, Provider accounting). The record is the
// job's as of now with Source accounting and Accounting set, whether or not the
// listing still shows the job. found is false when accounting does not have the
// job (Vanish). FailReads makes it fail. It is logged as op "sacct-job" with the
// job ID, so a test counts the reads. LagAccounting makes it find nothing while
// the queue listing still shows the job.
func (b *Batch) ReadAccounting(_ context.Context, jobID string) (provider.Job, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.log("sacct-job", jobID)
	if b.readErr != nil {
		return provider.Job{}, false, b.readErr
	}
	j, ok := b.jobs[jobID]
	if !ok || j.vanished || j.accountingLags {
		return provider.Job{}, false, nil
	}
	r := b.record(j, b.clock.Now())
	r.Source = provider.SourceAccounting
	r.Accounting = b.accountingOf(j, r, b.clock.Now())
	return r, true, nil
}

// Cancel implements provider.Batch.
func (b *Batch) Cancel(_ context.Context, jobIDs []string) []provider.CancelResult {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clock.Now()
	out := make([]provider.CancelResult, 0, len(jobIDs))
	for _, id := range jobIDs {
		b.log("scancel", id)
		j, ok := b.jobs[id]
		switch {
		case !ok:
			out = append(out, provider.CancelResult{JobID: id, Outcome: provider.CancelError, Err: fmt.Errorf("job %s: %w", id, provider.ErrNotFound)})
		case !live(b.record(j, now)):
			out = append(out, provider.CancelResult{JobID: id, Outcome: provider.AlreadyEnded})
		default:
			t := now
			j.cancelledAt = &t
			out = append(out, provider.CancelResult{JobID: id, Outcome: provider.Cancelled})
		}
	}
	return out
}
