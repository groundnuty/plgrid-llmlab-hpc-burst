package alpha

// The deployment ends that keep the stack rows to their job counts, worked out
// by hand.

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
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

// The first clock: the operator's wait, and for a tunnel row no later than a
// minute before its deployment's first end, the time limit less the drain
// window. At the default tunnel lease, 2 h 17 min, and the live drain window,
// 15 min, the first end is 2 h 2 min after the deployment and the cap 2 h 1 min;
// at the longest lease, 4 h, the cap is 3 h 44 min. A row of two jobs has no cap.
func TestFirstClock(t *testing.T) {
	tunnel, _ := RowByName("tunnel-hour-athena")
	handover, _ := RowByName("alpha-athena-handover")
	dw := 15 * time.Minute
	for _, c := range []struct {
		name    string
		row     Row
		limit   time.Duration
		wait    time.Duration
		bound   time.Duration
		pastEnd bool
	}{
		{"the script's default, 2 h", tunnel, 137 * time.Minute, 2 * time.Hour, 2 * time.Hour, false},
		{"exactly the cap", tunnel, 137 * time.Minute, 2*time.Hour + time.Minute, 2*time.Hour + time.Minute, false},
		{"30 s past the cap", tunnel, 137 * time.Minute, 2*time.Hour + 90*time.Second, 2*time.Hour + time.Minute, true},
		{"days, at the default lease", tunnel, 137 * time.Minute, 72 * time.Hour, 2*time.Hour + time.Minute, true},
		{"days, at the 4 h lease", tunnel, 240 * time.Minute, 72 * time.Hour, 3*time.Hour + 44*time.Minute, true},
		{"30 min, the slot's", tunnel, 137 * time.Minute, 30 * time.Minute, 30 * time.Minute, false},
		{"a row of two jobs keeps no first end", handover, 106 * time.Minute, 72 * time.Hour, 72 * time.Hour, false},
	} {
		bound, pastEnd := FirstClock(c.row, c.wait, c.limit, dw)
		if bound != c.bound || pastEnd != c.pastEnd {
			t.Errorf("%s: FirstClock = %s %v, want %s %v", c.name, bound, pastEnd, c.bound, c.pastEnd)
		}
	}
}

// A first clock that ran out is ErrNotStarted, and ErrQueueWaitPastEnd too when
// the deployment's first end bounded it; NotRun then holds on every row, for
// either (the science agent's ruling of 8 Nov 2026, #272).
func TestANotStartedErrorIsNotRunWhereTheRulingsSay(t *testing.T) {
	pastEnd := &NotStartedError{Bound: 121 * time.Minute, PastEnd: true}
	pending := &NotStartedError{Bound: 2 * time.Hour}
	if !errors.Is(pastEnd, ErrQueueWaitPastEnd) || !errors.Is(pastEnd, ErrNotStarted) || errors.Is(pending, ErrQueueWaitPastEnd) || !errors.Is(pending, ErrNotStarted) {
		t.Fatal("NotStartedError's Is")
	}
	for _, c := range []struct {
		row  string
		err  error
		want bool
	}{
		{"tunnel-hour-athena", pastEnd, true},
		{"tunnel-hour-helios", pastEnd, true},
		{"tunnel-hour-athena", pending, true},
		{"tunnel-hour-helios", pending, true},
		{"alpha-helios-nooverlap", pending, true},
		{"alpha-athena-handover", pending, true},
		{"alpha-athena-handover", ErrQueueWaitPastEnd, true},
		{"tunnel-hour-athena", errors.New("another"), false},
		{"tunnel-hour-helios", errors.New("another"), false},
		{"alpha-helios-nooverlap", ErrQueueWaitPastEnd, true},
	} {
		if got := NotRun(rowOf(c.row), c.err); got != c.want {
			t.Errorf("NotRun(%s, %v) = %v, want %v", c.row, c.err, got, c.want)
		}
	}
	note := NotRunNote(rowOf("tunnel-hour-athena"), 3*time.Hour, fmt.Errorf("the run: %w", pastEnd))
	if want := "tunnel-hour-athena: a job was still pending after 2h1m0s, a minute before the deployment's first end;"; !strings.HasPrefix(note, want) {
		t.Errorf("the note %q, want it to begin %q", note, want)
	}
}

func TestParseClusterClock(t *testing.T) {
	if got, err := ParseClusterClock([]byte("1793606400\n")); err != nil || !got.Equal(time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)) {
		t.Errorf("ParseClusterClock = %s %v", got, err)
	}
	for _, bad := range []string{"", "-1", "0", "17936064OO", "1793606400 1", "1793606400\n1793606401", "+1793606400", "Mon Nov  2 08:00:00 UTC 2026"} {
		if _, err := ParseClusterClock([]byte(bad)); err == nil {
			t.Errorf("ParseClusterClock(%q) accepted", bad)
		}
	}
}

// Every job of a run has the run's prefix: the lifecycle controller names an
// Allocation after its Provider (internal/lifecycle/provision.go,
// randomName(pv.name): the name less trailing dashes and dots, a dash and 5
// characters), the run names its Provider RunName, and the tag is made from
// the Allocation's name (v1alpha1.JobTag). Another run's ID gives another
// prefix.
func TestEveryJobOfARunHasItsPrefix(t *testing.T) {
	id := "0a1b2c3d4e"
	if err := CheckRunID(id); err != nil {
		t.Fatal(err)
	}
	if got := RunName(id); got != "alpha-0a1b2c3d4e" {
		t.Errorf("RunName = %q", got)
	}
	alloc := RunName(id) + "-x7k2p"
	tag := v1alpha1.JobTag("default", alloc, types.UID("4f6c2c1e-0000-4000-8000-000000000001"))
	if p := TagPrefix(id); p != "hb-alpha-0a1b2c3d4e-" || !strings.HasPrefix(tag, p) || !tagRE.MatchString(tag) || len(tag) > 56 {
		t.Errorf("tag %q, prefix %q", tag, TagPrefix(id))
	}
	if strings.HasPrefix(tag, TagPrefix("0a1b2c3d4f")) {
		t.Error("another run's prefix matches this run's tag")
	}
	other, err := NewRunID()
	if err != nil || CheckRunID(other) != nil {
		t.Errorf("NewRunID = %q %v", other, err)
	}
}

// The two-job guard on a live no-overlap run, worked out by hand: a 1 h 46 min
// time limit and a 15 min drain window, 1 h 31 min from a start to its drain
// start. The end is the look's clock plus 1 h 31 min until the successor runs,
// then the successor's drain start. Every end it gives is no later than the
// drain start of a job that starts after that look, and once a successor may
// exist, after the predecessor's drain start.
func TestTheTwoJobGuardByHand(t *testing.T) {
	at := func(h, m, s int) time.Time { return time.Date(2026, 11, 2, h, m, s, 0, time.UTC) }
	limit := 106 * time.Minute
	job := func(id string, start *time.Time, state string) provider.Job {
		j := provider.Job{ID: id, Name: "hb-alpha-0a1b2c3d4e-" + id, State: []string{state}}
		if start != nil {
			s, he := *start, start.Add(limit)
			j.StartTime, j.HardEnd = &s, &he
		}
		return j
	}
	ptr := func(t time.Time) *time.Time { return &t }
	g := &TwoJobGuard{TimeLimit: limit, DrainWindow: 15 * time.Minute}
	steps := []struct {
		name  string
		look  Look
		end   time.Time
		final bool
	}{
		{"before the deployment", Look{Clock: at(8, 0, 0)}, at(9, 31, 0), false},
		{"the first job pends", Look{Clock: at(8, 30, 0), Jobs: []provider.Job{job("p", nil, "PENDING")}}, at(10, 1, 0), false},
		// It started at 09:00:00 (drain start 10:31:00), after the clock was read: the end stays at or before 10:31:00.
		{"it started after the clock", Look{Clock: at(8, 59, 59), Jobs: []provider.Job{job("p", ptr(at(9, 0, 0)), "RUNNING")}}, at(10, 30, 59), false},
		{"the next look", Look{Clock: at(9, 0, 10), Jobs: []provider.Job{job("p", ptr(at(9, 0, 0)), "RUNNING")}}, at(10, 31, 10), false},
		{"the successor pends", Look{Clock: at(9, 30, 0), Jobs: []provider.Job{job("p", ptr(at(9, 0, 0)), "RUNNING"), job("s", nil, "PENDING")}}, at(11, 1, 0), false},
		{"a clock read earlier never lowers it", Look{Clock: at(9, 20, 0), Jobs: []provider.Job{job("p", ptr(at(9, 0, 0)), "RUNNING"), job("s", nil, "PENDING")}}, at(11, 1, 0), false},
		{"the predecessor has left the listing", Look{Clock: at(10, 39, 0), Jobs: []provider.Job{job("s", nil, "PENDING")}}, at(12, 10, 0), false},
		// The successor started at 10:40:00: its drain start, 12:11:00, for good.
		{"the successor runs", Look{Clock: at(10, 40, 30), Jobs: []provider.Job{job("s", ptr(at(10, 40, 0)), "RUNNING")}}, at(12, 11, 0), true},
		{"later looks keep it", Look{Clock: at(11, 40, 0), Jobs: []provider.Job{job("s", ptr(at(10, 40, 0)), "RUNNING")}}, at(12, 11, 0), true},
	}
	for _, c := range steps {
		end, final, err := g.Next(c.look)
		if err != nil || !end.Equal(c.end) || final != c.final {
			t.Fatalf("%s: Next = %s %v %v, want %s %v", c.name, end.Format(time.TimeOnly), final, err, c.end.Format(time.TimeOnly), c.final)
		}
	}
	if _, _, err := g.Next(Look{Clock: at(11, 41, 0), Jobs: []provider.Job{job("s", ptr(at(10, 40, 0)), "RUNNING"), job("x", ptr(at(11, 40, 30)), "RUNNING")}}); err == nil {
		t.Error("a third job that started was not an error")
	}
}

func TestTheTwoJobGuardStopsTheRun(t *testing.T) {
	at := time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)
	limit := 106 * time.Minute
	run := func(id string, start time.Time, lim time.Duration) provider.Job {
		he := start.Add(lim)
		return provider.Job{ID: id, Name: "hb-alpha-0a1b2c3d4e-" + id, State: []string{"RUNNING"}, StartTime: &start, HardEnd: &he}
	}
	for name, jobs := range map[string][]provider.Job{
		"two start in the same second":     {run("p", at, limit), run("s", at.Add(500*time.Millisecond), limit)},
		"three start in one look":          {run("p", at, limit), run("s", at.Add(time.Minute), limit), run("x", at.Add(2*time.Minute), limit)},
		"a time limit not the planned one": {run("p", at, limit-time.Minute)},
		"a running job with no lease end":  {{ID: "p", Name: "hb-alpha-0a1b2c3d4e-p", State: []string{"RUNNING"}, StartTime: &at}},
	} {
		g := &TwoJobGuard{TimeLimit: limit, DrainWindow: 15 * time.Minute}
		if _, _, err := g.Next(Look{Clock: at.Add(10 * time.Minute), Jobs: jobs}); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	// A job cancelled while it pended has its start at its end: it never ran, and counts for nothing.
	g := &TwoJobGuard{TimeLimit: limit, DrainWindow: 15 * time.Minute}
	end := at
	never := provider.Job{ID: "c", Name: "hb-alpha-0a1b2c3d4e-c", State: []string{"CANCELLED"}, StartTime: &at, EndTime: &end}
	if e, final, err := g.Next(Look{Clock: at, Jobs: []provider.Job{never}}); err != nil || final || !e.Equal(at.Add(91*time.Minute)) {
		t.Errorf("a job cancelled while it pended: %s %v %v, want the clock plus 1 h 31 min", e, final, err)
	}
}
