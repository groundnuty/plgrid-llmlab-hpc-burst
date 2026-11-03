package alpha

// The deployment ends that keep the stack rows to their job counts, worked out
// by hand.

import (
	"errors"
	"testing"
	"time"
)

func TestTheEndsOfARowOfOneJob(t *testing.T) {
	created := time.Date(2026, 11, 2, 12, 0, 0, 700_000_000, time.UTC)
	// 12 min lease, 1 min 45 s drain window: 12:00:00.7 + 10 min 15 s, to the second.
	if got, want := FirstEndOfOne(created, 12*time.Minute, 105*time.Second), time.Date(2026, 11, 2, 12, 10, 15, 0, time.UTC); !got.Equal(want) {
		t.Errorf("FirstEndOfOne = %s, want %s", got, want)
	}
	// The live tunnel lease, 2 h 17 min, and its 15 min drain window: 2 h 2 min.
	if got, want := FirstEndOfOne(created, 137*time.Minute, 15*time.Minute), time.Date(2026, 11, 2, 14, 2, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("FirstEndOfOne (live) = %s, want %s", got, want)
	}
	ds := time.Date(2026, 11, 2, 12, 20, 30, 400_000_000, time.UTC)
	if got, want := EndOfOne(ds), time.Date(2026, 11, 2, 12, 19, 30, 0, time.UTC); !got.Equal(want) {
		t.Errorf("EndOfOne = %s, want %s", got, want)
	}
}

func TestTheEndOfARowOfTwoJobs(t *testing.T) {
	pred := time.Date(2026, 11, 2, 12, 5, 0, 0, time.UTC)
	if got, err := EndOfTwo(pred, pred.Add(time.Second)); err != nil || !got.Equal(pred.Add(time.Second)) {
		t.Errorf("a successor a second later: %s %v, want its drain start", got, err)
	}
	if got, err := EndOfTwo(pred, pred.Add(37*time.Minute)); err != nil || !got.Equal(pred.Add(37*time.Minute)) {
		t.Errorf("a successor 37 min later: %s %v, want its drain start", got, err)
	}
	for name, succ := range map[string]time.Time{
		"the same second":            pred,
		"the same second, truncated": pred.Add(900 * time.Millisecond),
		"before the predecessor's":   pred.Add(-time.Minute),
	} {
		if got, err := EndOfTwo(pred, succ); err == nil {
			t.Errorf("%s: an end %s at or before the predecessor's drain start was accepted", name, got)
		}
	}
}

func TestCheckQueueWait(t *testing.T) {
	tunnel, _ := RowByName("tunnel-hour-athena")
	handover, _ := RowByName("alpha-athena-handover")
	dw := 15 * time.Minute // tunnel lease 2 h 17 min: the first end is 2 h 2 min after the deployment
	for _, c := range []struct {
		name string
		row  Row
		wait time.Duration
		ok   bool
	}{
		{"the script's default, 2 h", tunnel, 2 * time.Hour, true},
		{"a minute before the first end", tunnel, 2*time.Hour + time.Minute, true},
		{"30 s later", tunnel, 2*time.Hour + 90*time.Second, false},
		{"days", tunnel, 72 * time.Hour, false},
		{"a row of two jobs keeps no first end", handover, 72 * time.Hour, true},
	} {
		err := CheckQueueWait(c.row, c.wait, dw)
		if c.ok != (err == nil) || (err != nil && !errors.Is(err, ErrQueueWaitPastEnd)) {
			t.Errorf("%s: %v, want ok %v", c.name, err, c.ok)
		}
	}
}
