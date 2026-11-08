package slurm

// Slurm's NO_VAL and INFINITE markers in the fields the parser reads (issue
// #267). A job cancelled before it started has, in Slurm 24.11's sacct
// (data_parser v0.0.42), time.start 4294967294, the bare number: NO_VAL. It
// used to read as a start time in 2106, so a job that never started looked
// started. The source of the shapes is in parse.go (above noVal).
//
// Every expected value is written out by hand from the fixture's raw fields
// (testdata/v0.0.42/sacct-never-started.json: submission 1793351400, end
// 1793351462, limit 10 minutes) and from the markers' values in Slurm's
// slurm/slurm.h, never from this package's output. A fixture whose time.start
// is 0 or absent would prove nothing: both always read as no time.

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// markerForms are the ways a marker reaches a field: bare, as the data parser
// dumps a plain TIMESTAMP, and in the {set, infinite, number} triple, where
// Slurm itself writes NO_VAL as {set: false} and INFINITE as {infinite: true},
// and where a "set" triple that carries a marker's number is read the same.
var markerForms = []struct {
	name, raw string
	// infinite is whether the value is INFINITE, which noVal keeps apart from
	// NO_VAL: "unlimited" is not "unset".
	infinite bool
}{
	{"bare NO_VAL", `4294967294`, false},
	{"bare INFINITE", `4294967295`, true},
	// A native 64-bit marker is dumped as null by Slurm's UINT64 dumper (see
	// parse.go), so the four 64-bit rows are defensive.
	{"bare NO_VAL64", `18446744073709551614`, false},
	{"bare INFINITE64", `18446744073709551615`, true},
	{"triple, not set", `{"set": false, "infinite": false, "number": 0}`, false},
	{"triple, infinite", `{"set": false, "infinite": true, "number": 0}`, true},
	{"set triple with NO_VAL", `{"set": true, "infinite": false, "number": 4294967294}`, false},
	{"set triple with INFINITE", `{"set": true, "infinite": false, "number": 4294967295}`, true},
	{"set triple with NO_VAL64", `{"set": true, "infinite": false, "number": 18446744073709551614}`, false},
	{"set triple with INFINITE64", `{"set": true, "infinite": false, "number": 18446744073709551615}`, true},
}

// Each marker, in each form, is no value: not a number, not a time, not a
// duration. The control rows are values that stay values, among them the
// numbers next to the markers: only the markers themselves are special.
func TestNoValReadsSlurmMarkersAsNoValue(t *testing.T) {
	for _, f := range markerForms {
		t.Run(f.name, func(t *testing.T) {
			var n noVal
			if err := json.Unmarshal([]byte(f.raw), &n); err != nil {
				t.Fatal(err)
			}
			if v, ok := n.value(); ok {
				t.Errorf("value() = %v, true; want no value", v)
			}
			if n.Infinite != f.infinite {
				t.Errorf("Infinite = %v, want %v: INFINITE is unlimited, NO_VAL is unset", n.Infinite, f.infinite)
			}
			if e := n.epoch(); e != nil {
				t.Errorf("epoch() = %s, want none", e)
			}
			if m := n.minutes(); m != 0 {
				t.Errorf("minutes() = %s, want 0", m)
			}
		})
	}
	for _, c := range []struct {
		name, raw string
		want      float64
		epoch     *time.Time
		// noEpoch skips the epoch check: no int64 holds the number, and
		// nothing reads such a number as a time.
		noEpoch bool
	}{
		{"bare epoch", `1793351400`, 1793351400, ptr(at(2026, 10, 30, 9, 10, 0)), false},
		{"set triple", `{"set": true, "infinite": false, "number": 1793351400}`, 1793351400, ptr(at(2026, 10, 30, 9, 10, 0)), false},
		{"one below NO_VAL", `4294967293`, 4294967293, ptr(at(2106, 2, 7, 6, 28, 13)), false},
		// 0xfffffffffffffffd is NO_CONSUME_VAL64, a TRES marker and not one of
		// these; float64 holds it as 2**64, the same as the markers beside it.
		{"one below NO_VAL64", `18446744073709551613`, 1 << 64, nil, true},
		{"bare zero", `0`, 0, nil, false},
	} {
		t.Run("control: "+c.name, func(t *testing.T) {
			var n noVal
			if err := json.Unmarshal([]byte(c.raw), &n); err != nil {
				t.Fatal(err)
			}
			if v, ok := n.value(); !ok || v != c.want {
				t.Errorf("value() = %v, %v; want %v, true", v, ok, c.want)
			}
			if c.noEpoch {
				return
			}
			if e := n.epoch(); !reflect.DeepEqual(e, c.epoch) {
				t.Errorf("epoch() = %v, want %v", e, c.epoch)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

// A number in a triple that is not a number is still an error, as before the
// markers were read from the number's text.
func TestNoValTripleNumberOfTheWrongTypeIsAnError(t *testing.T) {
	for _, raw := range []string{
		`{"set": true, "number": "5"}`,
		`{"set": true, "number": true}`,
		`{"set": true, "number": {}}`,
		`"4294967294"`,
	} {
		var n noVal
		if err := json.Unmarshal([]byte(raw), &n); err == nil {
			t.Errorf("%s: no error, read as %+v", raw, n)
		}
	}
}

// ---- the real reply's shape ----

const neverStartedStart = `"start": 4294967294`

// checkNeverStarted: the job of testdata/v0.0.42/sacct-never-started.json, with
// whatever time.start it was given, did not start. The limit is 10 minutes, so
// a start time would give it a hard end: that there is none is the check.
func checkNeverStarted(t *testing.T, j provider.Job) {
	t.Helper()
	if j.TimeLimit != 10*time.Minute {
		t.Fatalf("precondition: TimeLimit %s, want the fixture's 10m0s", j.TimeLimit)
	}
	if j.StartTime != nil {
		t.Errorf("StartTime = %s, want none: the job never started", j.StartTime)
	}
	if j.HardEnd != nil {
		t.Errorf("HardEnd = %s, want none: it is the start plus the limit", j.HardEnd)
	}
	if want := at(2026, 10, 30, 9, 10, 0); !j.SubmitTime.Equal(want) {
		t.Errorf("SubmitTime = %s, want %s", j.SubmitTime, want)
	}
	if j.EndTime == nil || !j.EndTime.Equal(at(2026, 10, 30, 9, 11, 2)) {
		t.Errorf("EndTime = %v, want 2026-10-30T09:11:02Z (time.end 1793351462)", j.EndTime)
	}
	if j.Phase != v1alpha1.AllocationEnded || !reflect.DeepEqual(j.State, []string{"CANCELLED"}) {
		t.Errorf("Phase %s, State %v; want Ended, [CANCELLED]", j.Phase, j.State)
	}
	if j.Source != provider.SourceAccounting {
		t.Errorf("Source = %s, want accounting", j.Source)
	}
	a := j.Accounting
	if a == nil || a.AllocatedCPUs != nil || a.AllocatedGPUs != nil {
		t.Errorf("Accounting = %+v, want a record that lists no allocation", a)
	}
	if a != nil && (a.Elapsed == nil || *a.Elapsed != 0) {
		t.Errorf("Accounting.Elapsed = %v, want 0 (time.elapsed 0)", a.Elapsed)
	}
	// The block of status.atProvider.accounting: nil while the job has not
	// started, its hours unknown and never counted (HBEP-8).
	if got := provider.AccountingOf(j, 1, nil, at(2026, 10, 30, 9, 30, 0)); got != nil {
		t.Errorf("AccountingOf = %+v, want nil: a job that never started has no allocated hours", got)
	}
}

// The real reply's shape: a job cancelled before it started, whose time.start is
// the bare NO_VAL.
func TestAccountingNoValStartIsNeverStarted(t *testing.T) {
	doc := readTestdata(t, "v0.0.42", "sacct-never-started.json")
	if n := bytes.Count(doc, []byte(neverStartedStart)); n != 1 {
		t.Fatalf("precondition: the fixture holds %q %d times, want 1 (the real NO_VAL, bare)", neverStartedStart, n)
	}
	checkNeverStarted(t, accountingJobs(t, doc, "v0.0.42")["4311"])
}

// The other markers in time.start, each put in by text: a document that went
// through a map would write the 64-bit markers as floats.
func TestAccountingMarkerStartIsNeverStarted(t *testing.T) {
	base := readTestdata(t, "v0.0.42", "sacct-never-started.json")
	for _, f := range markerForms {
		t.Run(f.name, func(t *testing.T) {
			doc := bytes.Replace(base, []byte(neverStartedStart), []byte(`"start": `+f.raw), 1)
			if !bytes.Contains(doc, []byte(f.raw)) {
				t.Fatalf("precondition: the edited document lacks %s", f.raw)
			}
			checkNeverStarted(t, accountingJobs(t, doc, "v0.0.42")["4311"])
		})
	}
}

// Every time field the accounting reader takes is read the same way: the
// submission, the start, the end, the elapsed time and the limit.
func TestAccountingMarkersInEveryTimeField(t *testing.T) {
	base := readTestdata(t, "v0.0.42", "sacct-never-started.json")
	for _, f := range markerForms {
		t.Run(f.name, func(t *testing.T) {
			doc := onlyJob(t, base, 4311, func(j map[string]any) {
				tm := j["time"].(map[string]any)
				for _, k := range []string{"submission", "start", "end", "elapsed", "limit"} {
					tm[k] = json.RawMessage(f.raw)
				}
			})
			if !bytes.Contains(doc, []byte(strings.ReplaceAll(f.raw, " ", ""))) {
				t.Fatalf("precondition: the edited document lacks %s", f.raw)
			}
			j := accountingJobs(t, doc, "v0.0.42")["4311"]
			if !j.SubmitTime.IsZero() || j.StartTime != nil || j.EndTime != nil || j.HardEnd != nil || j.TimeLimit != 0 {
				t.Errorf("SubmitTime %s, StartTime %v, EndTime %v, HardEnd %v, TimeLimit %s; want none of them",
					j.SubmitTime, j.StartTime, j.EndTime, j.HardEnd, j.TimeLimit)
			}
			if a := j.Accounting; a == nil || a.Elapsed != nil {
				t.Errorf("Accounting = %+v, want a record whose elapsed time is unknown, never a marker's seconds", a)
			}
		})
	}
}

// The queue listing carries its times as triples (TIMESTAMP_NO_VAL); the reader
// takes a bare number too, so a marker is no value in either form, in every
// time field it reads.
func TestQueueMarkersInEveryTimeField(t *testing.T) {
	base := readTestdata(t, "v0.0.42", "squeue-derived.json")
	for _, f := range markerForms {
		t.Run(f.name, func(t *testing.T) {
			// An ended job: its start, end and submission are in the fields the
			// reader takes from the listing, and its limit is kept so that a
			// start time would give a hard end.
			doc := onlyJob(t, base, 4308, func(j map[string]any) {
				j["job_state"] = toAny("CANCELLED")
				for _, k := range []string{"submit_time", "start_time", "end_time"} {
					j[k] = json.RawMessage(f.raw)
				}
			})
			j := queueJobs(t, doc, "v0.0.42")["4308"]
			if j.TimeLimit != 10*time.Minute {
				t.Fatalf("precondition: TimeLimit %s, want the fixture's 10m0s", j.TimeLimit)
			}
			if !j.SubmitTime.IsZero() || j.StartTime != nil || j.EndTime != nil || j.HardEnd != nil {
				t.Errorf("SubmitTime %s, StartTime %v, EndTime %v, HardEnd %v; want none of them", j.SubmitTime, j.StartTime, j.EndTime, j.HardEnd)
			}
			// The limit is in minutes: a marker is not 4.29e9 of them.
			limited := onlyJob(t, base, 4308, func(j map[string]any) { j["time_limit"] = json.RawMessage(f.raw) })
			if got := queueJobs(t, limited, "v0.0.42")["4308"].TimeLimit; got != 0 {
				t.Errorf("TimeLimit = %s, want 0 (no limit read)", got)
			}
		})
	}
}

// A pending job's listed start is an estimate: a marker is no estimate.
func TestQueueMarkerStartIsNoEstimate(t *testing.T) {
	base := readTestdata(t, "v0.0.42", "squeue-derived.json")
	for _, f := range markerForms {
		t.Run(f.name, func(t *testing.T) {
			doc := onlyJob(t, base, 4303, func(j map[string]any) { j["start_time"] = json.RawMessage(f.raw) })
			j := queueJobs(t, doc, "v0.0.42")["4303"]
			if j.Phase != v1alpha1.AllocationPending {
				t.Fatalf("precondition: Phase %s, want Pending", j.Phase)
			}
			if j.StartEstimate != nil {
				t.Errorf("StartEstimate = %s, want none", j.StartEstimate)
			}
		})
	}
}

// The fixture keeps S7's structure: the sacct job object's 46 keys in S7's
// order, and the v0.0.42 data_parser; only its values are those of a job that
// never started. It holds the one real value the issue is about, the bare NO_VAL.
func TestNeverStartedFixtureKeepsS7Structure(t *testing.T) {
	doc := readTestdata(t, "v0.0.42", "sacct-never-started.json")
	if got := objectKeys(t, firstJob(t, doc)); !reflect.DeepEqual(got, s7SacctJobKeys) {
		t.Errorf("the job object has the keys %v, S7 lists %v", got, s7SacctJobKeys)
	}
	if !strings.Contains(string(doc), `"data_parser": "data_parser/v0.0.42"`) {
		t.Error("the fixture lacks meta.plugin.data_parser = data_parser/v0.0.42")
	}
	var top struct {
		Jobs []struct {
			Time struct {
				Start json.Number `json:"start"`
			} `json:"time"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(doc, &top); err != nil || len(top.Jobs) != 1 {
		t.Fatalf("jobs: %v, %d of them", err, len(top.Jobs))
	}
	if got := top.Jobs[0].Time.Start.String(); got != "4294967294" {
		t.Errorf("time.start is %s, want the bare 4294967294", got)
	}
}
