package fake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func TestEstimateAnswersWhatItWasSetTo(t *testing.T) {
	b, _ := newTestBatch(t)
	// A start worked out by hand: 1800000000 is 2027-01-15T08:00:00Z.
	want := time.Date(2027, 1, 15, 8, 0, 0, 0, time.UTC)
	b.SetEstimate(want.In(time.FixedZone("example", 3600))) // the zone is dropped: the answer is UTC
	res, err := b.Estimate(context.Background(), gpuReq("tag-a", true))
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}
	wantTime(t, "Start", res.Start, want)
	if res.Unknown != "" {
		t.Fatalf("Unknown = %q beside a start", res.Unknown)
	}
	if !res.ReadAt.Equal(at(12, 0, 0)) {
		t.Fatalf("ReadAt = %v, want the clock's 12:00:00", res.ReadAt)
	}
}

func TestEstimateUnsetIsUnknownAsAnUndecidedPlatform(t *testing.T) {
	b, _ := newTestBatch(t)
	res, err := b.Estimate(context.Background(), gpuReq("tag-a", false))
	if err != nil || res.Start != nil || res.Unknown != provider.EstimateUndecided {
		t.Fatalf("Estimate = (%+v, %v), want unknown with the undecided reason", res, err)
	}
	b.SetEstimateUnknown("a reason of the test's own")
	if res, _ := b.Estimate(context.Background(), gpuReq("tag-a", false)); res.Start != nil || res.Unknown != "a reason of the test's own" {
		t.Fatalf("Estimate = %+v, want that reason", res)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("an unknown estimate with no reason did not panic")
		}
	}()
	b.SetEstimateUnknown("")
}

func TestEstimateFailureIsAnErrorAndNeverATime(t *testing.T) {
	b, _ := newTestBatch(t)
	b.SetEstimate(time.Date(2027, 1, 15, 8, 0, 0, 0, time.UTC))
	boom := errors.New("sbatch: exit 1")
	b.FailEstimate(boom)
	res, err := b.Estimate(context.Background(), gpuReq("tag-a", false))
	if !errors.Is(err, boom) || res.Start != nil {
		t.Fatalf("Estimate = (%+v, %v), want the error and no start", res, err)
	}
	b.FailEstimate(nil)
	if res, err := b.Estimate(context.Background(), gpuReq("tag-a", false)); err != nil || res.Start == nil {
		t.Fatalf("after clearing the failure: (%+v, %v), want the answer back", res, err)
	}
}

func TestEstimateHoldEndsOnTheContextOrTheRelease(t *testing.T) {
	b, _ := newTestBatch(t)
	b.SetEstimate(time.Date(2027, 1, 15, 8, 0, 0, 0, time.UTC))
	hold := b.HoldEstimates()

	// A context that ends while the call waits fails it with that error.
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := b.Estimate(ctx, gpuReq("tag-a", false))
		errc <- err
	}()
	<-hold.Blocked()
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("held Estimate ended with %v, want context.Canceled", err)
	}

	// A release lets a waiting call answer.
	type out struct {
		res provider.EstimateResult
		err error
	}
	done := make(chan out, 1)
	go func() {
		res, err := b.Estimate(context.Background(), gpuReq("tag-b", false))
		done <- out{res, err}
	}()
	// The second call is logged before it waits; a release that comes before it waits lets it pass at once.
	for deadline := time.Now().Add(30 * time.Second); len(b.EstimateRequests()) < 2; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the second Estimate call never reached the fake")
		}
	}
	hold.Release()
	if o := <-done; o.err != nil || o.res.Start == nil {
		t.Fatalf("released Estimate = (%+v, %v), want the answer", o.res, o.err)
	}
}

// Estimate is read-only (HBEP-4): no job, no Script consumed, no submit in the
// call log, whatever it answered. The control: a Submit does all three, so the
// checks can fail.
func TestEstimateChangesNothing(t *testing.T) {
	b, _ := newTestBatch(t)
	b.Enqueue(Script{Outcome: provider.Rejected, Reason: "scripted for the next Submit"})
	b.SetEstimate(time.Date(2027, 1, 15, 8, 0, 0, 0, time.UTC))

	ctx := context.Background()
	for _, req := range []provider.SubmitRequest{gpuReq("tag-a", true), gpuReq("tag-b", false)} {
		if _, err := b.Estimate(ctx, req); err != nil {
			t.Fatalf("Estimate: %v", err)
		}
	}
	b.FailEstimate(errors.New("sbatch: exit 1"))
	_, _ = b.Estimate(ctx, gpuReq("tag-c", false))

	if n := b.Count("sbatch"); n != 0 {
		t.Fatalf("the call log holds %d submissions after three estimates, want 0", n)
	}
	if n := b.Count("sbatch-test-only"); n != 3 {
		t.Fatalf("the call log holds %d sbatch-test-only entries, want 3", n)
	}
	for _, op := range []string{"scancel", "scontrol-release", "ensure-file"} {
		if n := b.Count(op); n != 0 {
			t.Fatalf("the call log holds %d %s calls after estimates, want 0", n, op)
		}
	}
	jobs, err := b.FindByTag(ctx, "tag-a", time.Time{})
	if err != nil || len(jobs) != 0 {
		t.Fatalf("FindByTag after an estimate = (%v, %v), want no job", jobs, err)
	}
	// The Script is still queued: the first Submit meets it.
	if _, outcome, err := b.Submit(ctx, gpuReq("tag-d", false)); outcome != provider.Rejected || err == nil {
		t.Fatalf("Submit = (%s, %v), want the Script's rejection: an estimate consumed it", outcome, err)
	}
}

func TestEstimateRefusesAnInvalidRequestBeforeAnyCall(t *testing.T) {
	b, _ := newTestBatch(t)
	b.SetEstimate(time.Date(2027, 1, 15, 8, 0, 0, 0, time.UTC))
	req := gpuReq("tag-a", false)
	req.Params.Partition = "not-a-partition"
	res, err := b.Estimate(context.Background(), req)
	if !errors.Is(err, provider.ErrInvalidRequest) || res.Start != nil {
		t.Fatalf("Estimate = (%+v, %v), want a refusal wrapping ErrInvalidRequest", res, err)
	}
	if n := b.Count("sbatch-test-only"); n != 0 {
		t.Fatalf("a refused request is in the call log %d times, want 0", n)
	}
}

func TestEstimateRequestsKeepWhatWasAsked(t *testing.T) {
	b, _ := newTestBatch(t)
	req := gpuReq("tag-a", true)
	if _, err := b.Estimate(context.Background(), req); err != nil {
		t.Fatalf("Estimate: %v", err)
	}
	got := b.EstimateRequests()
	if len(got) != 1 || got[0].Tag != "tag-a" || got[0].Params.Partition != gpuPart || got[0].Params.GPUs != 1 {
		t.Fatalf("EstimateRequests = %+v, want the one request as asked", got)
	}
	got[0].Params.GPUs = 4 // a copy: the log's own is not changed by it
	if b.EstimateRequests()[0].Params.GPUs != 1 {
		t.Fatal("EstimateRequests returns the log's own request, not a copy")
	}
}
