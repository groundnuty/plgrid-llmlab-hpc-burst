package slurm

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Clock reads the time. pkg/provider/fake.Clock satisfies it.
type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// RealClock is the wall clock.
var RealClock Clock = realClock{}

// Poll intervals of the queue listing (HBEP-4, Transport, Poll rate): 30 s,
// and 5 s while any of the Provider's attempts is unresolved or a job is held.
const (
	DefaultPollInterval = 30 * time.Second
	DefaultBusyInterval = 5 * time.Second
)

// Options configure a Batch.
type Options struct {
	// Clock reads the time; nil means the wall clock.
	Clock Clock
	// PollInterval and BusyInterval are how old the cached queue listing may
	// get before a read refreshes it; zero means the defaults.
	PollInterval, BusyInterval time.Duration
}

// Paths are the login node's resolved paths (HBEP-4, Transport): absolute,
// matching the safe pattern, never a variable or "~".
type Paths struct {
	// Home is HOME on the login node.
	Home string
	// Staging is the profile's stagingDir with its variable resolved.
	Staging string
}

// DialHomeDir is the resolved dial-home directory, <home>/.hpcburst/dial-home.
func (p Paths) DialHomeDir() string { return p.Home + "/" + agentapi.DialHomeDir }

// Health is what the Provider's status reports about its platform (HBEP-4,
// Provider status and Operational readiness).
type Health struct {
	// LastListing is when the queue listing was last read successfully; zero
	// before the first.
	LastListing time.Time
	// ListingFailed is true when the latest attempt to read the listing
	// failed.
	ListingFailed bool
	// DialectMismatch is the data_parser version of the latest reply that
	// differed from the profile's, or "" when the latest listing matched.
	DialectMismatch string
	// PathUnresolved says why the login-node paths could not be resolved, or
	// is nil once they are.
	PathUnresolved error
	// DialHomeKeyMissing is the result of the latest dial-home key check:
	// nil before one, else whether a key file was missing.
	DialHomeKeyMissing *bool
	// ReadFailures counts the required status reads that failed since the
	// Batch was made: every failed queue listing and every failed accounting
	// read. It rises when sacct fails while the listing still succeeds, a
	// failure ListingFailed does not show.
	ReadFailures uint64
}

// listing is one read of the queue listing.
type listing struct {
	at      time.Time
	records []queueRecord
	parser  string
}

// Batch is provider.Batch for Slurm over a Runner (HBEP-4, Core operations),
// bound to one Provider. It keeps one cached queue listing for the whole
// Provider, read at most once per poll interval and refreshed after any
// write, and resolves the login-node paths once per connection. It is safe
// for concurrent use.
type Batch struct {
	runner Runner
	clock  Clock
	poll   time.Duration
	busyIv time.Duration

	mu       sync.Mutex
	profile  provider.Profile
	settings provider.Settings
	busy     map[string]bool
	health   Health
	paths    *Paths
	pathsGen uint64

	// lmu makes one listing read at a time; cache is the latest good one,
	// stale says a write has happened since.
	lmu   sync.Mutex
	cache *listing
	stale bool
}

var _ provider.Batch = (*Batch)(nil)

// NewBatch returns a Batch over runner for one Provider's validated profile
// and settings.
func NewBatch(runner Runner, profile provider.Profile, settings provider.Settings, o Options) *Batch {
	if o.Clock == nil {
		o.Clock = RealClock
	}
	if o.PollInterval <= 0 {
		o.PollInterval = DefaultPollInterval
	}
	if o.BusyInterval <= 0 {
		o.BusyInterval = DefaultBusyInterval
	}
	return &Batch{
		runner: runner, clock: o.Clock, poll: o.PollInterval, busyIv: o.BusyInterval,
		profile: profile, settings: settings, busy: map[string]bool{},
	}
}

// Configure replaces the profile and settings, for example after the
// PlatformProfile changed. The cached listing stays. The cached login-node
// paths stay only while the profile's stagingDir is the same: they were
// resolved from it, and the connection's generation would not change.
func (b *Batch) Configure(profile provider.Profile, settings provider.Settings) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if profile.Spec.StagingDir != b.profile.Spec.StagingDir {
		b.paths = nil
	}
	b.profile, b.settings = profile, settings
}

func (b *Batch) config() (provider.Profile, provider.Settings) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.profile, b.settings
}

// Health returns the platform's health as last seen.
func (b *Batch) Health() Health {
	b.mu.Lock()
	defer b.mu.Unlock()
	h := b.health
	if h.DialHomeKeyMissing != nil {
		v := *h.DialHomeKeyMissing
		h.DialHomeKeyMissing = &v
	}
	return h
}

// SetBusy marks tag as having an unresolved attempt or a held job, or clears
// the mark. While any tag is busy the listing is refreshed at the short
// interval.
func (b *Batch) SetBusy(tag string, busy bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if busy {
		b.busy[tag] = true
	} else {
		delete(b.busy, tag)
	}
}

func (b *Batch) maxAge() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.busy) > 0 {
		return b.busyIv
	}
	return b.poll
}

// markStale makes the next read refresh the listing: after any write.
func (b *Batch) markStale() {
	b.lmu.Lock()
	b.stale = true
	b.lmu.Unlock()
}

func (b *Batch) noteDialect(err error, got string) {
	if errors.Is(err, provider.ErrDialectMismatch) {
		b.mu.Lock()
		b.health.DialectMismatch = got
		b.mu.Unlock()
	}
}

// run runs one command with the default time-out.
func (b *Batch) run(ctx context.Context, line string, stdin []byte) (Result, error) {
	return b.runner.Run(ctx, Command{Line: line, Stdin: stdin, Timeout: provider.CommandTimeout})
}

// queue returns the cached listing, reading it again when it is older than
// the poll interval or a write has happened since. A failed read is an error;
// a stale listing is never served past its age.
func (b *Batch) queue(ctx context.Context) (*listing, error) {
	maxAge := b.maxAge()
	b.lmu.Lock()
	defer b.lmu.Unlock()
	now := b.clock.Now()
	if b.cache != nil && !b.stale && now.Sub(b.cache.at) < maxAge {
		return b.cache, nil
	}
	profile, _ := b.config()
	res, err := b.run(ctx, queueLine(), nil)
	if err == nil && res.ExitCode != 0 {
		err = fmt.Errorf("squeue: exit %d: %s", res.ExitCode, slurmMessage(string(res.Stderr)))
	}
	var recs []queueRecord
	var parser string
	if err == nil {
		recs, parser, err = parseQueue(res.Stdout, profile.Spec.JSONDataParser)
		b.noteDialect(err, parser)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil {
		b.health.ListingFailed = true
		b.health.ReadFailures++
		return nil, err
	}
	b.cache = &listing{at: now, records: recs, parser: parser}
	b.stale = false
	b.health.LastListing = now
	b.health.ListingFailed = false
	b.health.DialectMismatch = ""
	return b.cache, nil
}

// Poll reads the listing when it is due, so the Provider's listing is read
// once per poll interval even with no Allocation to observe.
func (b *Batch) Poll(ctx context.Context) error {
	_, err := b.queue(ctx)
	return err
}

// queueJobs returns the listing's jobs that keep says to keep.
func (b *Batch) queueJobs(l *listing, keep func(queueRecord) bool) ([]provider.Job, error) {
	var out []provider.Job
	var errs []error
	for _, r := range l.records {
		if !keep(r) {
			continue
		}
		j, err := r.job(l.parser)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		out = append(out, j)
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	sortJobs(out)
	return out, nil
}

// ListByTag returns the jobs with the tag in the queue listing only: held,
// pending, running and recently ended jobs, not accounting. Settle uses it.
func (b *Batch) ListByTag(ctx context.Context, tag string) ([]provider.Job, error) {
	if err := CheckValue("tag", tag); err != nil {
		return nil, fmt.Errorf("%w: %v", provider.ErrInvalidRequest, err)
	}
	l, err := b.queue(ctx)
	if err != nil {
		return nil, err
	}
	return b.queueJobs(l, func(r queueRecord) bool { return r.Name == tag })
}

// accounting runs one sacct line and merges its records. A failed read is
// counted in Health.ReadFailures: the listing can succeed while accounting
// fails, and then ListingFailed stays false.
func (b *Batch) accounting(ctx context.Context, line string) ([]provider.Job, error) {
	jobs, err := b.readAccounting(ctx, line)
	if err != nil {
		b.mu.Lock()
		b.health.ReadFailures++
		b.mu.Unlock()
	}
	return jobs, err
}

func (b *Batch) readAccounting(ctx context.Context, line string) ([]provider.Job, error) {
	profile, _ := b.config()
	res, err := b.run(ctx, line, nil)
	if err != nil {
		return nil, fmt.Errorf("sacct: %w", err)
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("sacct: exit %d: %s", res.ExitCode, slurmMessage(string(res.Stderr)))
	}
	recs, parser, err := parseAccounting(res.Stdout, profile.Spec.JSONDataParser)
	if err != nil {
		b.noteDialect(err, parser)
		return nil, err
	}
	return mergeAccounting(recs, parser)
}

// FindByTag implements provider.Batch: every job of the calling user whose
// name is exactly tag, from the queue listing, and from accounting since
// since for any job the listing does not show. Sorted by job ID.
func (b *Batch) FindByTag(ctx context.Context, tag string, since time.Time) ([]provider.Job, error) {
	listed, err := b.ListByTag(ctx, tag)
	if err != nil {
		return nil, err
	}
	seconds := int64(math.Ceil(b.clock.Now().Sub(since).Seconds()))
	acct, err := b.accounting(ctx, accountingTagLine(tag, seconds))
	if err != nil {
		return nil, err
	}
	out := append([]provider.Job(nil), listed...)
	seen := map[string]bool{}
	for _, j := range listed {
		seen[j.ID] = true
	}
	for _, j := range acct {
		if j.Name == tag && !seen[j.ID] {
			out = append(out, j)
		}
	}
	sortJobs(out)
	return out, nil
}

// Observe implements provider.Batch: the job from the queue listing, else from
// accounting. found is false only when both reads succeeded and neither shows
// the job.
func (b *Batch) Observe(ctx context.Context, jobID string) (provider.Job, bool, error) {
	if err := checkJobID(jobID); err != nil {
		return provider.Job{}, false, err
	}
	l, err := b.queue(ctx)
	if err != nil {
		return provider.Job{}, false, err
	}
	listed, err := b.queueJobs(l, func(r queueRecord) bool { return fmt.Sprint(r.JobID) == jobID })
	if err != nil {
		return provider.Job{}, false, err
	}
	if len(listed) > 0 {
		return listed[0], true, nil
	}
	return b.ReadAccounting(ctx, jobID)
}

// ReadAccounting reads one job from accounting alone, past the queue listing
// (sacct --json -X -D -j <id>): the job's record from accounting, with
// Accounting set from every execution -D returns (HBEP-8, Provider accounting).
// The controller makes it once for a recorded job after the job is first seen
// started, to learn what the scheduler allocated (HBEP-4, Poll rate); Observe
// makes the same read for a job the listing does not show. found is false when
// the read succeeded and accounting does not show the job; a failed read is an
// error and counts in Health.ReadFailures.
func (b *Batch) ReadAccounting(ctx context.Context, jobID string) (provider.Job, bool, error) {
	if err := checkJobID(jobID); err != nil {
		return provider.Job{}, false, err
	}
	acct, err := b.accounting(ctx, accountingJobLine(jobID))
	if err != nil {
		return provider.Job{}, false, err
	}
	for _, j := range acct {
		if j.ID == jobID {
			return j, true, nil
		}
	}
	return provider.Job{}, false, nil
}

// QueueJob reads one job from the queue with squeue --me --json --jobs=<id>,
// past the cache. listed is false for both of Slurm's "not listed" answers: an
// empty jobs list with exit 0, and exit 1 with "Invalid job id specified"
// (S7's capture). Any other failure is an error.
func (b *Batch) QueueJob(ctx context.Context, jobID string) (provider.Job, bool, error) {
	if err := checkJobID(jobID); err != nil {
		return provider.Job{}, false, err
	}
	profile, _ := b.config()
	res, err := b.run(ctx, queueJobLine(jobID), nil)
	if err != nil {
		return provider.Job{}, false, fmt.Errorf("squeue: %w", err)
	}
	if notListed(res) {
		return provider.Job{}, false, nil
	}
	if res.ExitCode != 0 {
		return provider.Job{}, false, fmt.Errorf("squeue: exit %d: %s", res.ExitCode, slurmMessage(string(res.Stderr)))
	}
	recs, parser, err := parseQueue(res.Stdout, profile.Spec.JSONDataParser)
	if err != nil {
		b.noteDialect(err, parser)
		return provider.Job{}, false, err
	}
	for _, r := range recs {
		if fmt.Sprint(r.JobID) == jobID {
			j, err := r.job(parser)
			return j, err == nil, err
		}
	}
	return provider.Job{}, false, nil
}

// refuse is a request refused before any call.
func refuse(format string, a ...any) error {
	return fmt.Errorf("%w: %s", provider.ErrInvalidRequest, fmt.Sprintf(format, a...))
}

var digestForm = regexp.MustCompile(`^[0-9a-f]{64}$`)

// CheckRequest refuses a request that HBEP-4 refuses (Submission), with an
// error that wraps provider.ErrInvalidRequest, and makes no call. Create calls
// it before staging files, so a bad request is refused before any platform
// call.
func CheckRequest(req provider.SubmitRequest, profile provider.Profile, settings provider.Settings) error {
	_, err := checkRequest(req, profile, settings)
	return err
}

// checkRequest refuses, before any call, what HBEP-4 refuses (Submission).
func checkRequest(req provider.SubmitRequest, profile provider.Profile, settings provider.Settings) (submission, error) {
	s := submission{tag: req.Tag, account: req.Account, hold: req.Hold}
	p := req.Params
	if err := CheckValue("tag", req.Tag); err != nil {
		return s, refuse("%v", err)
	}
	if len(req.Tag) > v1alpha1.JobTagLength {
		return s, refuse("tag is longer than %d characters", v1alpha1.JobTagLength)
	}
	if req.Account != "" {
		if err := CheckValue("account", req.Account); err != nil {
			return s, refuse("%v", err)
		}
	}
	if err := CheckValue("partition", p.Partition); err != nil {
		return s, refuse("%v", err)
	}
	if p.Partition != settings.Partition || p.Partition != profile.Partition.Name {
		return s, refuse("partition %q is not the one partition the Provider allows and the profile lists", p.Partition)
	}
	s.partition = p.Partition
	part := profile.Partition
	switch {
	case p.GPUs < 0:
		return s, refuse("gpus %d is negative", p.GPUs)
	case p.GPUs == 0 && part.GPUModel != "none":
		return s, refuse("partition %q is a GPU partition (%s) and the request has no GPU", part.Name, part.GPUModel)
	}
	// The QOS comes before the limits it sets: the flag is checked against the
	// safe pattern, and the limits are those that apply on the partition under
	// it (HBEP-4, QOS limits). One source feeds both, so a request cannot name
	// a QOS other than the one whose limits were checked.
	if req.QOS != settings.QOS {
		return s, refuse("qos %q is not the Provider's qos %q", req.QOS, settings.QOS)
	}
	if req.QOS != "" {
		if err := CheckValue("qos", req.QOS); err != nil {
			return s, refuse("%v", err)
		}
	}
	s.qos = req.QOS
	limits, err := v1alpha1.EffectiveQOSLimits(&profile.Spec, part, settings.QOS)
	if err != nil {
		return s, refuse("%v", err)
	}
	// A submit limit of 0 allows no job at all (`@hpc-burst-science-agent`,
	// 1 Nov 2026: an effective 0 means the Provider cannot host). Slurm would
	// refuse every submission with a reason that reads as a full slot, one that
	// never clears, so the request is refused here, before any call, as a GPU
	// cap of 0 is below (checkGPUs).
	if m := limits.MaxSubmitPerUser; m != nil && *m < 1 {
		return s, refuse("maxSubmitPerUser is %d under qos %q: no job can be submitted there", *m, limits.From[v1alpha1.LimitMaxSubmitPerUser])
	}
	if err := checkGPUs(p.GPUs, part, limits); err != nil {
		return s, err
	}
	s.gpus = p.GPUs
	limit, err := p.TimeLimit.AsDuration()
	switch {
	case err != nil:
		return s, refuse("time limit %q is not a duration", p.TimeLimit)
	case limit <= 0 || limit%time.Minute != 0:
		return s, refuse("time limit %q is not a positive whole number of minutes", p.TimeLimit)
	}
	if err := checkTimeLimit(limit, p.TimeLimit, part, limits); err != nil {
		return s, err
	}
	s.minutes = int64(limit / time.Minute)
	if p.CPUsPerTask < 1 {
		return s, refuse("cpusPerTask %d is below 1", p.CPUsPerTask)
	}
	if m := limits.MaxCPUsPerUser; m != nil && p.CPUsPerTask > *m {
		return s, refuse("cpusPerTask %d is above maxCpusPerUser %d of qos %q", p.CPUsPerTask, *m, limits.From[v1alpha1.LimitMaxCPUsPerUser])
	}
	s.cpusPerTask = p.CPUsPerTask
	// Memory 0 is refused although the CRD admits it: Slurm reads --mem=0 as
	// the whole node's memory (science agent's ruling, 31 Oct 2026).
	mem := p.Memory.Value()
	if mem <= 0 {
		return s, refuse("memory is not positive; --mem=0 would take the whole node")
	}
	s.memMiB = (mem + (1<<20 - 1)) >> 20
	if m := limits.MaxMemPerUser; m != nil {
		// Compared in the whole MiB that go to Slurm, rounded up, against the
		// cap in whole MiB, rounded down: a job never gets less than it asked
		// for, and never more than the cap.
		if capMiB := m.Value() >> 20; s.memMiB > capMiB {
			return s, refuse("memory %s (%d MiB) is above maxMemPerUser %s (%d MiB) of qos %q",
				p.Memory.String(), s.memMiB, m.String(), capMiB, limits.From[v1alpha1.LimitMaxMemPerUser])
		}
	}
	if p.Nodes > 1 {
		return s, refuse("nodes %d: alpha Allocations are single-node", p.Nodes)
	}
	if !digestForm.MatchString(p.AgentDigest) {
		return s, refuse("agentDigest is not 64 lowercase hex characters")
	}
	s.agentDigest = p.AgentDigest
	if err := checkTmpDir(part); err != nil {
		return s, refuse("partition %q: %v", part.Name, err)
	}
	s.tmpDir, s.tmpDirFeature = part.TmpDir, part.TmpDirFeature
	return s, nil
}

// tmpDirForm and featureForm are the CRD's patterns for a partition's tmpDir
// and tmpDirFeature (v1alpha1.PartitionProfile).
var (
	tmpDirForm  = regexp.MustCompile(`^(/|\$[A-Za-z_][A-Za-z0-9_]*)[A-Za-z0-9._:=,/-]*$`)
	featureForm = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)
)

// checkTmpDir holds a partition's tmpDir and tmpDirFeature to the CRD's rules
// again, because the bootstrap puts them into a shell line: tmpDir an absolute
// path or one that starts with a variable, of letters, digits and ._:=,/-,
// with no ".." segment; tmpDirFeature one feature name, and only with a tmpDir.
func checkTmpDir(p v1alpha1.PartitionProfile) error {
	if d := p.TmpDir; d != "" {
		if len(d) > 1024 || !tmpDirForm.MatchString(d) || slices.Contains(strings.Split(d, "/"), "..") {
			return fmt.Errorf("tmpDir %q is not an absolute path or $VARIABLE path of letters, digits and ._:=,/- without a '..' segment", d)
		}
	}
	if f := p.TmpDirFeature; f != "" {
		if !featureForm.MatchString(f) {
			return fmt.Errorf("tmpDirFeature %q is not one node feature name", f)
		}
		if p.TmpDir == "" {
			return fmt.Errorf("tmpDirFeature %q names the feature tmpDir needs, and no tmpDir is set", f)
		}
	}
	return nil
}

// Submit implements provider.Batch: one sbatch, held when req.Hold is set. A
// request refused before any call is Rejected with an error that wraps
// provider.ErrInvalidRequest; unresolved login-node paths are Rejected with
// provider.ErrPathUnresolved, and no sbatch runs.
func (b *Batch) Submit(ctx context.Context, req provider.SubmitRequest) (string, provider.SubmitOutcome, error) {
	profile, settings := b.config()
	s, err := checkRequest(req, profile, settings)
	if err != nil {
		return "", provider.Rejected, err
	}
	paths, err := b.Paths(ctx)
	if err != nil {
		return "", provider.Rejected, err
	}
	s.allocDir = paths.Staging + "/" + agentapi.AllocationDir(s.tag)
	s.agentPath = paths.Staging + "/" + agentapi.StagedAgentPath(s.agentDigest)
	line, err := submitLine(s)
	if err != nil {
		return "", provider.Rejected, refuse("%v", err)
	}
	res, runErr := b.runner.Run(ctx, Command{Line: line, Timeout: provider.SubmitTimeout})
	b.markStale()
	return classifySubmit(res, runErr)
}

// LiftHold implements provider.Batch with scontrol release.
func (b *Batch) LiftHold(ctx context.Context, jobID string) error {
	if err := checkJobID(jobID); err != nil {
		return err
	}
	res, err := b.run(ctx, releaseLine(jobID), nil)
	b.markStale()
	if err != nil {
		return fmt.Errorf("scontrol release %s: %w", jobID, err)
	}
	if res.ExitCode == 0 {
		return nil
	}
	// No real `scontrol release` error reply has been captured, so none of
	// these texts is checked against one. "Invalid job id specified" is real
	// Slurm text, but S7 captured it from squeue. "Access/permission denied"
	// and "already finished" are what fakeslurm and S4 emit ("Job has already
	// finished"), from S4's reading of Slurm's source. "admin", "not held" and
	// "Job is not pending" have no source: neither fake emits them. A text that
	// matches nothing falls through to the last return, an unknown error: the
	// reconciler then writes no hold-lifted marker and lifts again at the next
	// observe. The loose "admin" match is the one most likely to misread an
	// unrelated text.
	stderr := string(res.Stderr)
	switch {
	case strings.Contains(stderr, "Invalid job id specified"):
		return fmt.Errorf("job %s: %w", jobID, provider.ErrNotFound)
	case strings.Contains(stderr, "Access/permission denied"), strings.Contains(stderr, "admin"):
		return fmt.Errorf("job %s: %w", jobID, provider.ErrAdminHold)
	case strings.Contains(stderr, "already finished"), strings.Contains(stderr, "not held"),
		strings.Contains(stderr, "Job is not pending"):
		return fmt.Errorf("job %s: %w", jobID, provider.ErrNotHeld)
	}
	return fmt.Errorf("scontrol release %s: exit %d: %s", jobID, res.ExitCode, slurmMessage(stderr))
}

// Cancel implements provider.Batch: one result per job, in order. The outcome
// comes from the job's state, read just before the scancel, never from what
// scancel prints. Real Slurm 24.11's scancel prints nothing and exits 0 for a
// job that has ended and for a job it does not know (ESLURM_ALREADY_DONE and
// ESLURM_INVALID_JOB_ID, scancel.c in tag slurm-24-11-5-1), so its output
// cannot tell either from a cancel (#199). The state is read here, fresh and
// past the cache, and not taken from the caller's listing, which may be a poll
// interval old. It is read before the scancel, not after: the question is
// whether the job was live when it was cancelled, and a read after could only
// infer that from the final state, which a job cancelled by someone else shares.
//
// A job live at that read is cancelled and reports Cancelled. A job that has
// ended reports AlreadyEnded, and no scancel runs. A job neither the queue nor
// accounting shows reports an error that wraps provider.ErrNotFound. A job
// that ends in the instant between the read and the scancel still reports
// Cancelled on Slurm 24.11, which stays quiet about it. A read that fails is a
// CancelError: the job's state is unknown, and the caller retries.
func (b *Batch) Cancel(ctx context.Context, jobIDs []string) []provider.CancelResult {
	out := make([]provider.CancelResult, 0, len(jobIDs))
	defer b.markStale()
	for _, id := range jobIDs {
		out = append(out, b.cancelOne(ctx, id))
	}
	return out
}

func (b *Batch) cancelOne(ctx context.Context, id string) provider.CancelResult {
	failed := func(err error) provider.CancelResult {
		return provider.CancelResult{JobID: id, Outcome: provider.CancelError, Err: err}
	}
	if err := checkJobID(id); err != nil {
		return failed(err)
	}
	live, notFound, err := b.liveBeforeCancel(ctx, id)
	switch {
	case err != nil:
		return failed(fmt.Errorf("cancel job %s: read its state: %w", id, err))
	case notFound:
		return failed(fmt.Errorf("job %s: %w", id, provider.ErrNotFound))
	case !live:
		return provider.CancelResult{JobID: id, Outcome: provider.AlreadyEnded}
	}
	res, err := b.run(ctx, cancelLine(id), nil)
	stderr := string(res.Stderr)
	switch {
	case err != nil:
		return failed(fmt.Errorf("scancel %s: %w", id, err))
	// Real Slurm 24.11 says nothing about a job that ended after the read, but
	// an older or a noisier one (scancel -v) does. The job was live when it was
	// read, so these two texts only refine a job that ended in the window.
	case strings.Contains(stderr, "already completing or completed"):
		return provider.CancelResult{JobID: id, Outcome: provider.AlreadyEnded}
	case strings.Contains(stderr, "Invalid job id specified"):
		return failed(fmt.Errorf("job %s: %w", id, provider.ErrNotFound))
	case res.ExitCode != 0 || strings.Contains(stderr, "error"):
		return failed(fmt.Errorf("scancel %s: exit %d: %s", id, res.ExitCode, slurmMessage(stderr)))
	}
	return provider.CancelResult{JobID: id, Outcome: provider.Cancelled}
}

// liveBeforeCancel reads the job once, fresh: from the queue (squeue --me
// --jobs=<id>), else from accounting. live is true for a job with no end time;
// notFound is true when both reads succeeded and neither shows the job.
func (b *Batch) liveBeforeCancel(ctx context.Context, id string) (live, notFound bool, err error) {
	job, listed, err := b.QueueJob(ctx, id)
	if err != nil {
		return false, false, err
	}
	if listed {
		return job.EndTime == nil, false, nil
	}
	job, found, err := b.ReadAccounting(ctx, id)
	if err != nil {
		return false, false, err
	}
	return found && job.EndTime == nil, !found, nil
}

var varPrefix = regexp.MustCompile(`^\$([A-Za-z_][A-Za-z0-9_]*)`)

// absSafe reports whether p is an absolute path of safe characters with no
// "..".
func absSafe(p string) bool {
	return strings.HasPrefix(p, "/") && safeValue.MatchString(p) &&
		!strings.Contains(p, "/../") && !strings.HasSuffix(p, "/..")
}

// printenv reads one variable on the login node. It returns ErrPathUnresolved
// when it is unset, empty or unsafe.
func (b *Batch) printenv(ctx context.Context, name string) (string, error) {
	res, err := b.run(ctx, printenvLine(name), nil)
	if err != nil {
		return "", fmt.Errorf("printenv %s: %w", name, err)
	}
	v := strings.TrimRight(string(res.Stdout), "\n")
	switch {
	case res.ExitCode == 1 || (res.ExitCode == 0 && v == ""):
		return "", fmt.Errorf("%w: %s is unset or empty on the login node", provider.ErrPathUnresolved, name)
	case res.ExitCode != 0:
		return "", fmt.Errorf("printenv %s: exit %d", name, res.ExitCode)
	case !absSafe(v):
		return "", fmt.Errorf("%w: %s on the login node is not an absolute path of letters, digits and ._:=,/-", provider.ErrPathUnresolved, name)
	}
	return v, nil
}

// Paths resolves HOME and the profile's stagingDir on the login node, once
// per connection (HBEP-4, Transport). An unset or unsafe value is an error
// that wraps provider.ErrPathUnresolved, and Health says so.
func (b *Batch) Paths(ctx context.Context) (Paths, error) {
	// A runner that connects lazily is connected first, so a dropped and
	// replaced connection shows in Generation before the cached paths are
	// trusted.
	if c, ok := b.runner.(Connector); ok {
		if err := c.Connect(ctx); err != nil {
			return Paths{}, err
		}
	}
	gen := b.runner.Generation()
	b.mu.Lock()
	if b.paths != nil && b.pathsGen == gen && gen != 0 {
		p := *b.paths
		b.mu.Unlock()
		return p, nil
	}
	staging := b.profile.Spec.StagingDir
	b.mu.Unlock()

	p, err := b.resolve(ctx, staging)
	b.mu.Lock()
	defer b.mu.Unlock()
	if staging != b.profile.Spec.StagingDir {
		// Configure changed the stagingDir while this call resolved the old
		// one: its answer is good for this call only and is not cached.
		return p, err
	}
	if err != nil {
		if errors.Is(err, provider.ErrPathUnresolved) {
			b.health.PathUnresolved = err
		}
		return Paths{}, err
	}
	b.paths, b.pathsGen = &p, b.runner.Generation()
	b.health.PathUnresolved = nil
	return p, nil
}

func (b *Batch) resolve(ctx context.Context, staging string) (Paths, error) {
	home, err := b.printenv(ctx, "HOME")
	if err != nil {
		return Paths{}, err
	}
	switch {
	case staging == "":
		return Paths{}, fmt.Errorf("%w: the profile sets no stagingDir", provider.ErrPathUnresolved)
	case strings.HasPrefix(staging, "$"):
		m := varPrefix.FindStringSubmatch(staging)
		if m == nil {
			return Paths{}, fmt.Errorf("%w: stagingDir does not start with a variable name", provider.ErrPathUnresolved)
		}
		v, err := b.printenv(ctx, m[1])
		if err != nil {
			return Paths{}, err
		}
		staging = v + staging[len(m[0]):]
	}
	staging = strings.TrimRight(staging, "/")
	if !absSafe(staging) {
		return Paths{}, fmt.Errorf("%w: the resolved stagingDir is not an absolute path of letters, digits and ._:=,/-", provider.ErrPathUnresolved)
	}
	return Paths{Home: home, Staging: staging}, nil
}
