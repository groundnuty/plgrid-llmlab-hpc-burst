package agent

import (
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// rfc3339Strings collects, by path, every string of a decoded JSON document
// that parses as an RFC 3339 time. A path joins object keys with dots and
// writes [] for a list element, so the same key under two parents stays two
// paths.
func rfc3339Strings(v any, path string, out map[string][]string) {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			rfc3339Strings(e, path+"."+k, out)
		}
	case []any:
		for _, e := range x {
			rfc3339Strings(e, path+"[]", out)
		}
	case string:
		if _, err := time.Parse(time.RFC3339, x); err == nil {
			out[path] = append(out[path], x)
		}
	}
}

// Every time in a report is in UTC and written with Z, whatever zone the
// agent's clock, the process start and the health samples are in: a time with
// a node's local offset (+01:00) is valid RFC 3339, but a string comparison
// misorders it (issue #94, finding 7; `@hpc-burst-science-agent`, 1 Nov 2026,
// issue #125). The four times come from four sources, each given its own
// non-UTC zone, so a conversion missing from one source is seen on its own.
func TestSnapshotTimesAreUTC(t *testing.T) {
	var (
		cet = time.FixedZone("CET", 3600)
		jst = time.FixedZone("JST", 9*3600)
		est = time.FixedZone("EST", -5*3600)
		ist = time.FixedZone("IST", 5*3600+1800) // a half-hour offset
	)
	var (
		now     = time.Date(2026, 11, 1, 12, 0, 0, 0, cet)    // the agent's clock: Report.Time
		started = time.Date(2026, 11, 1, 10, 30, 0, 0, jst)   // the process: WorkerReport.StartTime
		read    = time.Date(2026, 10, 31, 20, 0, 0, 500, est) // the last metrics read: Health.MetricsReadAt
		sample  = time.Date(2026, 11, 1, 6, 30, 10, 0, ist)   // the latest sample: Health.Time
	)

	truePath, err := exec.LookPath("true")
	if err != nil {
		t.Fatal(err)
	}
	g, err := procgroup.Start(procgroup.Spec{Path: truePath}, started)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { <-g.Done() }) // no process is left behind

	// The worker is not worker-0, so the snapshot has no serving endpoint to
	// name and needs no forwarder.
	w := &worker{name: agentapi.WorkerName(1), phase: agentapi.PartRunning, proc: g}
	w.recordHealth(read, nil, agentapi.Health{}, nil)                                // a successful read, in EST
	w.recordHealth(sample, nil, agentapi.Health{}, errors.New("metrics unreadable")) // a later sample that keeps the read's time
	a := &Agent{o: Options{Now: func() time.Time { return now }}}
	a.inst = &instance{uid: "u", workers: []*worker{w}}

	r := a.snapshot()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	seen := map[string][]string{}
	rfc3339Strings(doc, "$", seen)

	// The walk cannot pass vacuously: each of the four fields must be in it.
	for _, path := range []string{
		"$.time",
		"$.instance.workers[].startTime",
		"$.instance.workers[].health.time",
		"$.instance.workers[].health.metricsReadAt",
	} {
		if len(seen[path]) != 1 {
			t.Errorf("the report holds %d times at %s; want 1 (report: %s)", len(seen[path]), path, b)
		}
	}
	for path, times := range seen {
		for _, s := range times {
			if !strings.HasSuffix(s, "Z") {
				t.Errorf("%s is %q; want a time in UTC that ends in Z", path, s)
			}
		}
	}

	// The conversion keeps each instant: the report still says when things
	// happened.
	var back agentapi.Report
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.Instance == nil || len(back.Instance.Workers) != 1 || back.Instance.Workers[0].Health == nil {
		t.Fatalf("the report lost the worker or its health: %s", b)
	}
	wr := back.Instance.Workers[0]
	if !back.Time.Equal(now) {
		t.Errorf("Report.Time is %v; want the instant %v", back.Time, now)
	}
	if wr.StartTime == nil || !wr.StartTime.Equal(started) {
		t.Errorf("StartTime is %v; want the instant %v", wr.StartTime, started)
	}
	if !wr.Health.Time.Equal(sample) {
		t.Errorf("Health.Time is %v; want the instant %v", wr.Health.Time, sample)
	}
	if wr.Health.MetricsReadAt == nil || !wr.Health.MetricsReadAt.Equal(read) {
		t.Errorf("MetricsReadAt is %v; want the instant %v", wr.Health.MetricsReadAt, read)
	}
}
