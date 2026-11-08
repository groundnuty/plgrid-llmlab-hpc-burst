package eventcount

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// HBEP-8's proof that an event was not raised (Metrics, Controller metrics,
// "Proving that an event was not raised"). It takes all four conditions;
// otherwise the result is inconclusive, never a pass:
//
//  1. the first read comes before the change that could raise the event;
//  2. both reads come from the same process while it serves:
//     process_start_time_seconds is unchanged, and on the lifecycle controller
//     both hpcburst_collector_success series are present;
//  3. between the reads, a pass of the emitting loop, for the same subject,
//     observed the change and finished emitting, and that pass started after
//     the time the event would have been due;
//  4. the other sources of the same series are quiet.
//
// A single read, a first read taken after the change, a count kept in the
// test's own client, and a barrier that shows only that the emitting read
// completed, or a pass of another subject or made before the event was due,
// prove nothing (HBEP-8, Test plan, Raised events).

// ProcessStartName is the process collector's start time, which tells two
// processes apart (condition 2).
const ProcessStartName = "process_start_time_seconds"

// CollectorSuccessName is the lifecycle controller's success gauge; a copy
// exports it only while it serves (HBEP-8, Unknown samples).
const CollectorSuccessName = "hpcburst_collector_success"

// LifecycleCollectors are the collector labels of the two
// hpcburst_collector_success series a serving lifecycle controller exports.
var LifecycleCollectors = []string{"accounting", "instances"}

// Key names one series of the counter.
type Key struct {
	Controller, Reason, Action, Type string
}

func (k Key) String() string {
	return fmt.Sprintf("%s{controller=%q,reason=%q,action=%q,type=%q}", Name, k.Controller, k.Reason, k.Action, k.Type)
}

// Read is one read of a /metrics endpoint, for one series of the counter.
// The caller sets Sent and Answered, the times the request was sent and its
// answer read; ParseRead fills the rest.
type Read struct {
	Sent, Answered time.Time
	// ProcessStart is process_start_time_seconds, when HasProcessStart.
	ProcessStart    float64
	HasProcessStart bool
	// Collectors holds the collector label of each hpcburst_collector_success
	// series the read has, whatever its value.
	Collectors map[string]bool
	// Count is the series' value, when HasCount.
	Count    float64
	HasCount bool
}

// ParseRead reads a /metrics body in Prometheus's text format for the
// series key, the process's start time and the success gauges.
func ParseRead(body []byte, key Key) (Read, error) {
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return Read{}, fmt.Errorf("parse the /metrics body: %w", err)
	}
	r := Read{Collectors: map[string]bool{}}
	if f := fams[ProcessStartName]; f != nil && len(f.GetMetric()) == 1 && f.GetMetric()[0].GetGauge() != nil {
		r.ProcessStart, r.HasProcessStart = f.GetMetric()[0].GetGauge().GetValue(), true
	}
	if f := fams[CollectorSuccessName]; f != nil {
		for _, m := range f.GetMetric() {
			if c, ok := label(m, "collector"); ok {
				r.Collectors[c] = true
			}
		}
	}
	if f := fams[Name]; f != nil {
		for _, m := range f.GetMetric() {
			if matches(m, key) && m.GetCounter() != nil {
				if r.HasCount {
					return Read{}, fmt.Errorf("two series %s in one read", key)
				}
				r.Count, r.HasCount = m.GetCounter().GetValue(), true
			}
		}
	}
	return r, nil
}

func label(m *dto.Metric, name string) (string, bool) {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue(), true
		}
	}
	return "", false
}

// matches reports whether m is exactly key's series: its four labels, and no
// other.
func matches(m *dto.Metric, k Key) bool {
	want := map[string]string{LabelController: k.Controller, LabelReason: k.Reason, LabelAction: k.Action, LabelType: k.Type}
	if len(m.GetLabel()) != len(want) {
		return false
	}
	for _, l := range m.GetLabel() {
		if v, ok := want[l.GetName()]; !ok || v != l.GetValue() {
			return false
		}
	}
	return true
}

// Barrier is the evidence of condition 3: a pass of the emitting loop, for the
// subject the event would regard, that finished emitting between the reads.
type Barrier struct {
	// What names the loop, the pass's marker and its subject, for the report.
	What string
	// PassStart is when the pass began: the loop's own record of it (a
	// ModelDeployment's status.lastReconcileTime), or a time the pass is shown
	// to have begun after (the creation of an object the pass listed).
	PassStart time.Time
	// Observed is when the test saw that the pass had finished emitting: the
	// write that ends it, or a test hook's signal after its recorder call.
	Observed time.Time
	// Due is when the event would have been due; zero when it would have been
	// due at the change itself.
	Due time.Time
}

// Absence is a check that the event of one series was not raised.
type Absence struct {
	Key Key
	// First and Second are the two reads, in order.
	First, Second Read
	// Change is when the change that could raise the event was made: the
	// first read must come before it.
	Change time.Time
	// Collectors are the hpcburst_collector_success series a serving copy
	// exports, which both reads must have: LifecycleCollectors on the
	// lifecycle controller, none on the front, which has no serving gate.
	Collectors []string
	// Barriers hold condition 3's evidence, one per subject that could raise
	// the event. There must be at least one.
	Barriers []Barrier
	// Quiet says why the series' other sources are quiet (condition 4); empty
	// means it was not shown.
	Quiet string
}

// Verdict is the outcome of an Absence check.
type Verdict int

const (
	// Inconclusive: a condition was not met. Never a pass.
	Inconclusive Verdict = iota
	// NotRaised: the four conditions hold and the count did not rise.
	NotRaised
	// Raised: the four conditions hold and the count rose.
	Raised
)

func (v Verdict) String() string {
	switch v {
	case NotRaised:
		return "not raised"
	case Raised:
		return "raised"
	default:
		return "inconclusive"
	}
}

// Judge applies the four conditions, then compares the counts. The string
// says why, for the log.
func (a Absence) Judge() (Verdict, string) {
	var unmet []string
	if a.First.Answered.IsZero() || a.Second.Sent.IsZero() || !a.Second.Sent.After(a.First.Answered) {
		unmet = append(unmet, "two reads in order are needed: a single read proves nothing")
	}
	if a.Change.IsZero() {
		unmet = append(unmet, "condition 1: no time of the change was given")
	} else if !a.First.Answered.Before(a.Change) {
		unmet = append(unmet, fmt.Sprintf("condition 1: the first read was answered at %s, not before the change at %s", stamp(a.First.Answered), stamp(a.Change)))
	}
	unmet = append(unmet, a.sameServingProcess()...)
	unmet = append(unmet, a.barriers()...)
	if strings.TrimSpace(a.Quiet) == "" {
		unmet = append(unmet, "condition 4: nothing shows that the series' other sources are quiet")
	}
	if !a.First.HasCount || !a.Second.HasCount {
		unmet = append(unmet, fmt.Sprintf("the series %s is missing from a read (first %v, second %v); it is created at 0, so a missing one is no 0", a.Key, a.First.HasCount, a.Second.HasCount))
	} else if a.Second.Count < a.First.Count {
		unmet = append(unmet, fmt.Sprintf("the count fell from %g to %g: the counter was reset between the reads", a.First.Count, a.Second.Count))
	}
	if len(unmet) > 0 {
		return Inconclusive, fmt.Sprintf("%s: inconclusive: %s", a.Key, strings.Join(unmet, "; "))
	}
	if d := a.Second.Count - a.First.Count; d > 0 {
		return Raised, fmt.Sprintf("%s: raised %g times between the reads (%g, then %g)", a.Key, d, a.First.Count, a.Second.Count)
	}
	return NotRaised, fmt.Sprintf("%s: not raised: %g at both reads, from one serving process; %d barrier(s): %s; quiet: %s",
		a.Key, a.First.Count, len(a.Barriers), a.barrierNames(), a.Quiet)
}

// sameServingProcess is condition 2.
func (a Absence) sameServingProcess() []string {
	var out []string
	switch {
	case !a.First.HasProcessStart || !a.Second.HasProcessStart:
		out = append(out, fmt.Sprintf("condition 2: %s is missing from a read (first %v, second %v)", ProcessStartName, a.First.HasProcessStart, a.Second.HasProcessStart))
	case a.First.ProcessStart != a.Second.ProcessStart:
		out = append(out, fmt.Sprintf("condition 2: %s changed from %g to %g: a restart or a hand-over between the reads", ProcessStartName, a.First.ProcessStart, a.Second.ProcessStart))
	}
	for _, c := range a.Collectors {
		if !a.First.Collectors[c] || !a.Second.Collectors[c] {
			out = append(out, fmt.Sprintf("condition 2: %s{collector=%q} is missing from a read (first %v, second %v): a copy that does not serve", CollectorSuccessName, c, a.First.Collectors[c], a.Second.Collectors[c]))
		}
	}
	return out
}

// barriers is condition 3.
func (a Absence) barriers() []string {
	if len(a.Barriers) == 0 {
		return []string{"condition 3: no barrier: nothing shows that a pass of the emitting loop finished between the reads"}
	}
	var out []string
	for _, b := range a.Barriers {
		if b.Observed.IsZero() || b.PassStart.IsZero() {
			out = append(out, fmt.Sprintf("condition 3: %s: the pass's start or end was not observed", b.What))
			continue
		}
		if b.Observed.Before(a.First.Answered) || a.Second.Sent.Before(b.Observed) {
			out = append(out, fmt.Sprintf("condition 3: %s: the pass was seen to end at %s, not between the reads (%s to %s)", b.What, stamp(b.Observed), stamp(a.First.Answered), stamp(a.Second.Sent)))
		}
		if b.PassStart.Before(a.Change) {
			out = append(out, fmt.Sprintf("condition 3: %s: the pass began at %s, before the change at %s, so it cannot have observed it", b.What, stamp(b.PassStart), stamp(a.Change)))
		}
		if b.PassStart.Before(b.Due) {
			out = append(out, fmt.Sprintf("condition 3: %s: the pass began at %s, before the event was due at %s", b.What, stamp(b.PassStart), stamp(b.Due)))
		}
	}
	return out
}

func (a Absence) barrierNames() string {
	names := make([]string, len(a.Barriers))
	for i, b := range a.Barriers {
		names[i] = b.What
	}
	return strings.Join(names, ", ")
}

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// ErrInconclusive is what Require returns for an inconclusive check.
var ErrInconclusive = errors.New("eventcount: the absence check is inconclusive")

// Require returns nil only when the check proves the event was not raised:
// an error wrapping ErrInconclusive when it is inconclusive, and a plain error
// when the event was raised.
func (a Absence) Require() error {
	v, why := a.Judge()
	switch v {
	case NotRaised:
		return nil
	case Raised:
		return errors.New(why)
	default:
		return fmt.Errorf("%w: %s", ErrInconclusive, why)
	}
}
