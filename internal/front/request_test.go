package front

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
)

// TestResendReplacesTheLeaseTimer: a request resent to a second Instance
// is cut at that Instance's lease end, not at the first one's.
func TestResendReplacesTheLeaseTimer(t *testing.T) {
	clk := clock.NewFake(t0)
	x := newExchange(httptest.NewRequest("POST", "/v1/chat/completions", nil), clk, 10*time.Hour)
	defer x.stopTimers()
	x.armLease(func() (time.Time, bool) { return t0.Add(time.Minute), true })
	x.armLease(func() (time.Time, bool) { return t0.Add(time.Hour), true })
	clk.Advance(2 * time.Minute)
	if err := context.Cause(x.ctx); err != nil {
		t.Fatalf("cut at the first Instance's lease end: %v", err)
	}
	clk.Advance(time.Hour)
	if got := x.cutBy(); got != cutLeaseEnd {
		t.Errorf("cutBy = %q after the second Instance's lease end, want %q", got, cutLeaseEnd)
	}
}

// TestLeaseEndThatMovesLaterIsFollowed: a later hard end, once observed,
// moves the cut (HBEP-5, Draining).
func TestLeaseEndThatMovesLaterIsFollowed(t *testing.T) {
	clk := clock.NewFake(t0)
	x := newExchange(httptest.NewRequest("POST", "/v1/chat/completions", nil), clk, 10*time.Hour)
	defer x.stopTimers()
	end := t0.Add(time.Minute)
	x.armLease(func() (time.Time, bool) { return end, true })
	end = t0.Add(time.Hour) // the controller copied a later hard end
	clk.Advance(2 * time.Minute)
	if err := context.Cause(x.ctx); err != nil {
		t.Fatalf("cut at the old lease end: %v", err)
	}
	clk.Advance(time.Hour)
	if got := x.cutBy(); got != cutLeaseEnd {
		t.Errorf("cutBy = %q, want %q", got, cutLeaseEnd)
	}
}
