package eventcount

import (
	"errors"
	"strings"
	"testing"
	"time"
)

var key = Key{ControllerLifecycle, "TunnelOrphaned", "degraded", "Warning"}

// t0 is a fixed instant; every time below is an offset from it.
var t0 = time.Date(2026, 11, 8, 6, 0, 0, 0, time.UTC)

func at(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }

func serving(sent, answered int, count float64) Read {
	return Read{Sent: at(sent), Answered: at(answered), ProcessStart: 1.7e9, HasProcessStart: true,
		Collectors: map[string]bool{"accounting": true, "instances": true}, Count: count, HasCount: true}
}

// good is a check that meets the four conditions: the first read at 1 s, the
// change at 10 s, the event due at 20 s, a pass that began at 30 s and was
// seen to end at 40 s, and the second read at 50 s.
func good() Absence {
	return Absence{
		Key: key, First: serving(0, 1, 3), Second: serving(50, 51, 3), Change: at(10), Collectors: LifecycleCollectors,
		Barriers: []Barrier{{What: "a tunnel-state pass that listed the witness", PassStart: at(30), Observed: at(40), Due: at(20)}},
		Quiet:    "TunnelOrphaned has one source, the tunnel-state reader",
	}
}

func TestAbsenceNotRaisedWhenTheFourConditionsHold(t *testing.T) {
	v, why := good().Judge()
	if v != NotRaised {
		t.Fatalf("verdict %s (%s), want not raised", v, why)
	}
	if err := good().Require(); err != nil {
		t.Fatal(err)
	}
}

func TestAbsenceRaisedWhenTheCountRose(t *testing.T) {
	a := good()
	a.Second.Count = 4
	if v, why := a.Judge(); v != Raised {
		t.Fatalf("verdict %s (%s), want raised", v, why)
	}
	if err := a.Require(); err == nil || errors.Is(err, ErrInconclusive) {
		t.Fatalf("Require = %v, want a plain error", err)
	}
}

// TestAbsenceIsInconclusiveWithoutACondition: each case breaks one condition,
// or reads a series that proves nothing; each is inconclusive, never a pass
// (HBEP-8, Test plan, Raised events: "None of these proves anything here").
func TestAbsenceIsInconclusiveWithoutACondition(t *testing.T) {
	cases := map[string]struct {
		mutate func(*Absence)
		want   string
	}{
		"a single read":                       {func(a *Absence) { a.Second = Read{} }, "a single read"},
		"a first read taken after the change": {func(a *Absence) { a.Change = at(0) }, "condition 1"},
		"no time of the change":               {func(a *Absence) { a.Change = time.Time{} }, "condition 1"},
		"a restart between the reads":         {func(a *Absence) { a.Second.ProcessStart = 1.7e9 + 90 }, "a restart or a hand-over"},
		"a hand-over to a copy that does not serve yet": {func(a *Absence) {
			a.Second.Collectors = map[string]bool{}
		}, "a copy that does not serve"},
		"a read with no process start": {func(a *Absence) { a.First.HasProcessStart = false }, "condition 2"},
		"no barrier":                   {func(a *Absence) { a.Barriers = nil }, "no barrier"},
		"a barrier seen before the first read": {func(a *Absence) {
			a.Barriers[0].PassStart, a.Barriers[0].Observed, a.Change, a.Barriers[0].Due = at(2), at(0), at(2), at(2)
		}, "not between the reads"},
		"a barrier seen after the second read": {func(a *Absence) { a.Barriers[0].Observed = at(55) }, "not between the reads"},
		"a pass that began before the event was due": {func(a *Absence) {
			a.Barriers[0].PassStart = at(15)
		}, "before the event was due"},
		"a pass that began before the change": {func(a *Absence) {
			a.Barriers[0].PassStart, a.Barriers[0].Due = at(5), time.Time{}
		}, "before the change"},
		"a barrier with no observed end": {func(a *Absence) { a.Barriers[0].Observed = time.Time{} }, "not observed"},
		"other sources not shown quiet":  {func(a *Absence) { a.Quiet = " " }, "condition 4"},
		"a series missing from a read":   {func(a *Absence) { a.First.HasCount = false }, "missing from a read"},
		"a count that fell":              {func(a *Absence) { a.Second.Count = 1 }, "reset"},
	}
	for name, c := range cases {
		a := good()
		a.Barriers = append([]Barrier(nil), a.Barriers...)
		c.mutate(&a)
		v, why := a.Judge()
		if v != Inconclusive || !strings.Contains(why, c.want) {
			t.Errorf("%s: verdict %s (%s), want inconclusive naming %q", name, v, why, c.want)
		}
		if err := a.Require(); !errors.Is(err, ErrInconclusive) {
			t.Errorf("%s: Require = %v, want ErrInconclusive", name, err)
		}
	}
}

// TestAbsenceOnTheFrontNeedsNoSuccessGauge: the front has no serving gate, so
// a check of its counter names no collectors; its process start still binds.
func TestAbsenceOnTheFrontNeedsNoSuccessGauge(t *testing.T) {
	a := good()
	a.Key.Controller = ControllerFront
	a.Collectors = nil
	a.First.Collectors, a.Second.Collectors = nil, nil
	if v, why := a.Judge(); v != NotRaised {
		t.Fatalf("verdict %s (%s), want not raised", v, why)
	}
	a.Second.ProcessStart++
	if v, _ := a.Judge(); v != Inconclusive {
		t.Fatalf("a restarted front: verdict %s, want inconclusive", v)
	}
}

const exposition = `# HELP hpcburst_collector_success x
# TYPE hpcburst_collector_success gauge
hpcburst_collector_success{collector="accounting"} 1
hpcburst_collector_success{collector="instances"} 0
# HELP hpcburst_events_raised_total events handed to the event recorder, not events delivered: the recorder may aggregate or drop them
# TYPE hpcburst_events_raised_total counter
hpcburst_events_raised_total{action="degraded",controller="lifecycle",reason="TunnelOrphaned",type="Warning"} 2
hpcburst_events_raised_total{action="degraded",controller="lifecycle",reason="ObservationGap",type="Warning"} 1
hpcburst_events_raised_total{action="degraded",controller="provider-x",reason="TunnelOrphaned",type="Warning"} 7
# HELP process_start_time_seconds Start time of the process since unix epoch in seconds.
# TYPE process_start_time_seconds gauge
process_start_time_seconds 1.76256e+09
`

func TestParseRead(t *testing.T) {
	r, err := ParseRead([]byte(exposition), key)
	if err != nil {
		t.Fatal(err)
	}
	if !r.HasCount || r.Count != 2 {
		t.Errorf("count %g (present %v), want 2: the lifecycle's series, not provider-x's", r.Count, r.HasCount)
	}
	if !r.HasProcessStart || r.ProcessStart != 1.76256e9 {
		t.Errorf("process start %g (present %v)", r.ProcessStart, r.HasProcessStart)
	}
	if !r.Collectors["accounting"] || !r.Collectors["instances"] || len(r.Collectors) != 2 {
		t.Errorf("collectors %v, want both, whatever their values", r.Collectors)
	}
	none, err := ParseRead([]byte(exposition), Key{ControllerLifecycle, "HandedOver", "replaced", "Normal"})
	if err != nil || none.HasCount {
		t.Errorf("a series the body lacks: %+v, %v; want it absent", none, err)
	}
	if _, err := ParseRead([]byte("not { a metric"), key); err == nil {
		t.Error("a body that does not parse: no error")
	}
}
