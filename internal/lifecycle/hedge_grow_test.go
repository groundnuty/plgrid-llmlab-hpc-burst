package lifecycle

// The hedge-time rule (HBEP-7, Hedging (beta), When a hedge grows), as a pure function of literal inputs. Every
// expected time is worked out by hand: a replacement hedge's deadline 09:20, a missing primary's created at 00:00 with
// a 30 min margin.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// day is 16 Nov 2026 at hh:mm.
func day(hh, mm int) time.Time { return time.Date(2026, 11, 16, hh, mm, 0, 0, time.UTC) }

// hedgeWaitsOf is n queue waits of w each.
func hedgeWaitsOf(n int, w time.Duration) []v1alpha1.QueueWait {
	var out []v1alpha1.QueueWait
	for i := 0; i < n; i++ {
		s := metav1.NewTime(day(0, 0).Add(-48*time.Hour + time.Duration(i)*time.Minute))
		out = append(out, v1alpha1.QueueWait{JobID: "w", SubmitTime: s, StartTime: metav1.NewTime(s.Add(w))})
	}
	return out
}

func TestTheHedgeTime(t *testing.T) {
	deadline, margin := day(9, 20), 30*time.Minute
	for _, c := range []struct {
		name        string
		now         time.Time
		submit      *time.Time
		waits       []v1alpha1.QueueWait
		replacement bool
		want        time.Time
	}{
		// A replacement submitted at 00:00.
		{"waits 10 h: expected 10:00, after 09:20: now", day(0, 0), ptrTo(day(0, 0)), hedgeWaitsOf(20, 10*time.Hour), true, day(0, 0)},
		{"waits 9 h 20 min: expected 09:20, not after: 09:20", day(0, 0), ptrTo(day(0, 0)), hedgeWaitsOf(20, 9*time.Hour+20*time.Minute), true, day(9, 20)},
		{"waits 9 h 21 min: expected 09:21, after: now", day(0, 0), ptrTo(day(0, 0)), hedgeWaitsOf(20, 9*time.Hour+21*time.Minute), true, day(0, 0)},
		// A replacement submitted at 06:50 with waits of 2 h: expected 08:50.
		{"waits 2 h, submitted 06:50: 08:50", day(7, 20), ptrTo(day(6, 50)), hedgeWaitsOf(20, 2*time.Hour), true, day(8, 50)},
		// No wait observed: the deadline minus the margin for a replacement.
		{"no wait, a replacement: 08:50", day(0, 0), ptrTo(day(0, 0)), nil, true, day(8, 50)},
		// Not submitted yet (Submitting): now plus the wait, moving with the clock.
		{"waits 2 h, not submitted, at 07:00: 09:00", day(7, 0), nil, hedgeWaitsOf(20, 2*time.Hour), true, day(9, 0)},
		{"waits 2 h, not submitted, at 07:30: 09:30, after: now", day(7, 30), nil, hedgeWaitsOf(20, 2*time.Hour), true, day(7, 30)},
		// Fewer than 20 waits: the largest.
		{"three waits, the largest 9 h 21 min: now", day(0, 0), ptrTo(day(0, 0)),
			append(hedgeWaitsOf(2, time.Hour), hedgeWaitsOf(1, 9*time.Hour+21*time.Minute)...), true, day(0, 0)},
	} {
		got := hedgeTimeOf(c.now, c.submit, c.waits, deadline, margin, c.replacement)
		if !got.Equal(c.want) {
			t.Errorf("%s: hedge time %s, want %s", c.name, got.Format(time.TimeOnly), c.want.Format(time.TimeOnly))
		}
	}
	// A missing primary created at 00:00: its deadline is 00:30.
	mdDeadline := day(0, 30)
	for _, c := range []struct {
		name  string
		now   time.Time
		waits []v1alpha1.QueueWait
		want  time.Time
	}{
		{"waits 10 h: now, its first pass", day(0, 0), hedgeWaitsOf(20, 10*time.Hour), day(0, 0)},
		{"waits 20 min: 00:20", day(0, 0), hedgeWaitsOf(20, 20*time.Minute), day(0, 20)},
		{"no wait: its deadline, 00:30, never its first pass", day(0, 0), nil, day(0, 30)},
	} {
		got := hedgeTimeOf(c.now, nil, c.waits, mdDeadline, margin, false)
		if !got.Equal(c.want) {
			t.Errorf("a missing primary, %s: hedge time %s, want %s", c.name, got.Format(time.TimeOnly), c.want.Format(time.TimeOnly))
		}
	}
}
