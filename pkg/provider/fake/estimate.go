package fake

// Estimate and its controls (beta; HBEP-4, Core operations, Estimate). New.

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// estimateControls are Estimate's controls, under Batch.mu. With none set the
// answer is unknown with provider.EstimateUndecided, as a real platform whose
// profile does not decide its estimate's reading answers.
type estimateControls struct {
	start   *time.Time
	unknown string
	err     error
	hold    *EstimateHold
	asked   []provider.SubmitRequest
}

// EstimateHold keeps every Estimate call from answering until Release, or until
// the call's context ends: a slow platform, with no sleep in the fake.
type EstimateHold struct {
	blocked, release       chan struct{}
	blockOnce, releaseOnce sync.Once
}

// Blocked is closed once an Estimate call waits at the hold: a test waits on it,
// never on a sleep.
func (h *EstimateHold) Blocked() <-chan struct{} { return h.blocked }

// Release lets the waiting calls answer, and every later call pass at once.
func (h *EstimateHold) Release() { h.releaseOnce.Do(func() { close(h.release) }) }

// SetEstimate makes every later Estimate answer with this start, in UTC.
func (b *Batch) SetEstimate(start time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	u := start.UTC()
	b.est.start, b.est.unknown, b.est.err = &u, "", nil
}

// SetEstimateUnknown makes every later Estimate answer unknown, with this
// reason (the plugin's own reasons are provider.EstimateUndecided and the others
// beside it). An empty reason panics: an unknown answer always says why.
func (b *Batch) SetEstimateUnknown(reason string) {
	if reason == "" {
		panic("fake: an unknown estimate needs a reason")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.est.start, b.est.unknown, b.est.err = nil, reason, nil
}

// FailEstimate makes every later Estimate return err, with no answer: the
// command failed or timed out. A nil err clears it.
func (b *Batch) FailEstimate(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.est.err = err
}

// HoldEstimates makes every later Estimate wait until the returned hold is
// released or its context ends, whichever comes first. A context that ends
// first fails the call with the context's error, as a command past its bound
// does.
func (b *Batch) HoldEstimates() *EstimateHold {
	h := &EstimateHold{blocked: make(chan struct{}), release: make(chan struct{})}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.est.hold = h
	return h
}

// EstimateRequests returns the requests Estimate was asked, oldest first,
// including those that failed or were held.
func (b *Batch) EstimateRequests() []provider.SubmitRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]provider.SubmitRequest, len(b.est.asked))
	for i, r := range b.est.asked {
		r.Params = *r.Params.DeepCopy()
		out[i] = r
	}
	return out
}

// Estimate implements provider.Batch. It is read-only: it registers no job,
// reads no Script, claims nothing and changes no job, and the call log has op
// "sbatch-test-only" (never "sbatch", which is a submission) with the tag, the
// partition, the time limit and the GPU count. A request refused before any call
// is not logged and wraps provider.ErrInvalidRequest.
func (b *Batch) Estimate(ctx context.Context, req provider.SubmitRequest) (provider.EstimateResult, error) {
	b.mu.Lock()
	if err := b.checkRequest(req); err != nil {
		b.mu.Unlock()
		return provider.EstimateResult{}, err
	}
	b.log("sbatch-test-only", req.Tag, req.Params.Partition, string(req.Params.TimeLimit), strconv.Itoa(int(req.Params.GPUs)))
	asked := req
	asked.Params = *req.Params.DeepCopy()
	b.est.asked = append(b.est.asked, asked)
	hold, ctl := b.est.hold, b.est
	b.mu.Unlock()
	if hold != nil {
		hold.blockOnce.Do(func() { close(hold.blocked) })
		select {
		case <-hold.release:
		case <-ctx.Done():
			return provider.EstimateResult{}, ctx.Err()
		}
	}
	if ctl.err != nil {
		return provider.EstimateResult{}, ctl.err
	}
	res := provider.EstimateResult{ReadAt: b.clock.Now()}
	switch {
	case ctl.start != nil:
		t := *ctl.start
		res.Start = &t
	case ctl.unknown != "":
		res.Unknown = ctl.unknown
	default:
		res.Unknown = provider.EstimateUndecided
	}
	return res, nil
}
