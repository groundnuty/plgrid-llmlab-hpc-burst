package agentapi

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// A sample with no gauges decodes to nil pointers, never to zeros: an absent
// gauge is unknown, and a zero would read as an idle engine
// (`@hpc-burst-science-agent`, 31 Oct 2026). The JSON is written by hand, as an
// agent that could not read /metrics would send it.
func TestHealthWithNoGaugesDecodesToUnknownNotZero(t *testing.T) {
	var h Health
	if err := json.Unmarshal([]byte(`{"time":"2026-10-31T12:00:00Z","probeOK":true}`), &h); err != nil {
		t.Fatal(err)
	}
	if h.RunningRequests != nil || h.WaitingRequests != nil || h.GeneratedTokens != nil || h.KVCacheUsage != nil {
		t.Errorf("gauges = %v %v %v %v; want all nil (unknown)", h.RunningRequests, h.WaitingRequests, h.GeneratedTokens, h.KVCacheUsage)
	}
	if h.MetricsReadAt != nil || h.ResponsesByStatus != nil {
		t.Errorf("MetricsReadAt = %v, ResponsesByStatus = %v; want both nil", h.MetricsReadAt, h.ResponsesByStatus)
	}
	if !h.ProbeOK {
		t.Error("ProbeOK = false; the probe is independent of the metrics")
	}
}

// The positive control of the test above: gauges that are present and zero
// decode to non-nil zeros, so the decoder tells "zero" from "absent". A gauge
// that is present next to absent ones keeps its place.
func TestHealthWithZeroGaugesDecodesToKnownZeros(t *testing.T) {
	var h Health
	const sample = `{"time":"2026-10-31T12:00:00Z","probeOK":true,"metricsReadAt":"2026-10-31T11:59:55Z",` +
		`"runningRequests":0,"waitingRequests":0,"generatedTokens":0,"kvCacheUsage":0}`
	if err := json.Unmarshal([]byte(sample), &h); err != nil {
		t.Fatal(err)
	}
	if h.RunningRequests == nil || *h.RunningRequests != 0 || h.WaitingRequests == nil || *h.WaitingRequests != 0 ||
		h.GeneratedTokens == nil || *h.GeneratedTokens != 0 || h.KVCacheUsage == nil || *h.KVCacheUsage != 0 {
		t.Errorf("gauges = %v %v %v %v; want four pointers to zero", h.RunningRequests, h.WaitingRequests, h.GeneratedTokens, h.KVCacheUsage)
	}
	if want := time.Date(2026, 10, 31, 11, 59, 55, 0, time.UTC); h.MetricsReadAt == nil || !h.MetricsReadAt.Equal(want) {
		t.Errorf("MetricsReadAt = %v, want %v", h.MetricsReadAt, want)
	}

	var partial Health
	if err := json.Unmarshal([]byte(`{"time":"2026-10-31T12:00:00Z","probeOK":true,"runningRequests":3}`), &partial); err != nil {
		t.Fatal(err)
	}
	if partial.RunningRequests == nil || *partial.RunningRequests != 3 || partial.WaitingRequests != nil || partial.KVCacheUsage != nil {
		t.Errorf("partial sample = %v %v %v; want running 3, waiting and KV cache nil", partial.RunningRequests, partial.WaitingRequests, partial.KVCacheUsage)
	}
}

// On the wire a nil gauge has no key and a zero gauge has its key: omitempty on
// a pointer drops only nil, so an unknown value is never sent as a zero.
func TestHealthKeepsAZeroGaugeOnTheWireAndDropsAnUnknownOne(t *testing.T) {
	b, err := json.Marshal(Health{Time: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC), ProbeOK: true, WaitingRequests: Int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.Contains(got, `"waitingRequests":0`) {
		t.Errorf("%s: a known zero gauge is missing", got)
	}
	for _, key := range []string{"runningRequests", "generatedTokens", "kvCacheUsage", "metricsReadAt"} {
		if strings.Contains(got, key) {
			t.Errorf("%s: carries %s, which was not set", got, key)
		}
	}
}

// MetricsFresh is the rule "no gauge-based action while MetricsReadAt is older
// than two probe intervals", judged at three ages (see MetricsFresh): the read
// when the sample was taken, the sample when its Report was made, and the Report
// since the controller received it. The boundary values are written out:
// exactly two intervals old is still fresh, one nanosecond more is stale.
func TestMetricsFresh(t *testing.T) {
	const interval = 10 * time.Second
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	ago := func(age time.Duration) *time.Time { r := at.Add(-age); return &r }
	// sample: taken at `at`, its metrics read readAge before; the Report made
	// reportAge after it, received at the Report's time plus 0 and judged
	// judgedAfter later on the controller's clock.
	judge := func(readAge, reportAge, judgedAfter time.Duration, probe time.Duration) bool {
		h := Health{Time: at, MetricsReadAt: ago(readAge)}
		reportTime := at.Add(reportAge)
		receivedAt := time.Date(2026, 10, 31, 12, 0, 7, 0, time.UTC) // the controller's clock, unrelated to the agent's
		return h.MetricsFresh(reportTime, receivedAt, receivedAt.Add(judgedAfter), probe)
	}
	for _, c := range []struct {
		name                            string
		readAge, reportAge, judgedAfter time.Duration
		probe                           time.Duration
		want                            bool
	}{
		{"everything just now", 0, 0, 0, interval, true},
		{"read one interval before the sample", interval, 0, 0, interval, true},
		{"read exactly two intervals before the sample", 2 * interval, 0, 0, interval, true},
		{"read two intervals and a nanosecond before the sample", 2*interval + 1, 0, 0, interval, false},
		{"read five minutes before the sample", 5 * time.Minute, 0, 0, interval, false},
		{"the report made exactly two intervals after the sample", 0, 2 * interval, 0, interval, true},
		{"the report made two intervals and a nanosecond after the sample", 0, 2*interval + 1, 0, interval, false},
		{"judged exactly two intervals after the report arrived", 0, 0, 2 * interval, interval, true},
		{"judged two intervals and a nanosecond after the report arrived", 0, 0, 2*interval + 1, interval, false},
		{"read after the sample's time", -time.Second, 0, 0, interval, true},
		{"a report dated before its sample", 0, -time.Second, 0, interval, true},
		{"a longer probe interval widens every bound", 25 * time.Second, 25 * time.Second, 25 * time.Second, 15 * time.Second, true},
		{"a zero probe interval", 0, 0, 0, 0, false},
	} {
		if got := judge(c.readAge, c.reportAge, c.judgedAfter, c.probe); got != c.want {
			t.Errorf("%s: MetricsFresh = %v, want %v", c.name, got, c.want)
		}
	}

	// Never read, or a missing time, is never fresh.
	rcv := at.Add(time.Second)
	for name, h := range map[string]Health{
		"never read":            {Time: at},
		"a sample with no time": {MetricsReadAt: ago(0)},
	} {
		if h.MetricsFresh(at, rcv, rcv, interval) {
			t.Errorf("%s: fresh", name)
		}
	}
	fresh := Health{Time: at, MetricsReadAt: ago(0)}
	if fresh.MetricsFresh(time.Time{}, rcv, rcv, interval) || fresh.MetricsFresh(at, time.Time{}, rcv, interval) || fresh.MetricsFresh(at, rcv, time.Time{}, interval) {
		t.Error("a zero report time, receipt time or now was fresh")
	}
	if MetricsStaleProbeIntervals != 2 {
		t.Errorf("MetricsStaleProbeIntervals = %d, want 2 (the science agent's ruling)", MetricsStaleProbeIntervals)
	}
}

// Astra's finding on PR 92: a sample that stays unchanged while time passes
// must turn stale. Time and MetricsReadAt are both T, the Report arrived at
// T+1 s on the controller's clock, and the probe interval is 10 s. As the
// controller's clock advances with no newer Report, the metrics stay fresh up
// to two intervals after receipt and are stale after.
func TestAnUnchangedSampleTurnsStaleAsTimeAdvances(t *testing.T) {
	const interval = 10 * time.Second
	sampleAt := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	h := Health{Time: sampleAt, MetricsReadAt: &sampleAt}
	received := time.Date(2026, 10, 31, 12, 0, 1, 0, time.UTC)
	for elapsed := time.Duration(0); elapsed <= time.Minute; elapsed += 5 * time.Second {
		want := elapsed <= 2*interval
		if got := h.MetricsFresh(sampleAt, received, received.Add(elapsed), interval); got != want {
			t.Errorf("%v after receipt: MetricsFresh = %v, want %v", elapsed, got, want)
		}
	}
}

// An agent that keeps re-sending one cached sample in new Reports: each Report
// is received just now, so the receipt age never grows, but the sample's age
// when the Report was made does.
func TestACachedSampleInNewReportsTurnsStale(t *testing.T) {
	const interval = 10 * time.Second
	sampleAt := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	h := Health{Time: sampleAt, MetricsReadAt: &sampleAt}
	controller := time.Date(2026, 10, 31, 12, 5, 0, 0, time.UTC) // a different clock
	for reportAfter := time.Duration(0); reportAfter <= time.Minute; reportAfter += 5 * time.Second {
		want := reportAfter <= 2*interval
		// The Report is made reportAfter after the sample and received just now.
		if got := h.MetricsFresh(sampleAt.Add(reportAfter), controller, controller, interval); got != want {
			t.Errorf("a Report made %v after the sample: MetricsFresh = %v, want %v", reportAfter, got, want)
		}
	}
}

// The scalar gauges keep an explicit zero and drop nil; ResponsesByStatus has
// three states and keeps all three on the wire: nil is unknown (null), an empty
// map is a read that found no responses ({}), and a populated map is itself.
// The JSON is written by hand.
func TestResponsesByStatusKeepsNilEmptyAndPopulatedOnTheWire(t *testing.T) {
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name string
		in   map[string]int64
		json string // the member as written on the wire
	}{
		{"nil", nil, `"responsesByStatus":null`},
		{"empty", map[string]int64{}, `"responsesByStatus":{}`},
		{"populated", map[string]int64{"2xx": 1200, "5xx": 3}, `"responsesByStatus":{"2xx":1200,"5xx":3}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, err := json.Marshal(Health{Time: at, ProbeOK: true, ResponsesByStatus: c.in})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(b), c.json) {
				t.Errorf("%s: want the member %s", b, c.json)
			}
			var back Health
			if err := json.Unmarshal(b, &back); err != nil {
				t.Fatal(err)
			}
			if (back.ResponsesByStatus == nil) != (c.in == nil) || len(back.ResponsesByStatus) != len(c.in) {
				t.Errorf("round trip: got %#v, want %#v (nil-ness and size)", back.ResponsesByStatus, c.in)
			}
			for k, v := range c.in {
				if back.ResponsesByStatus[k] != v {
					t.Errorf("round trip: %s = %d, want %d", k, back.ResponsesByStatus[k], v)
				}
			}
		})
	}

	// An agent that omits the member altogether, or sends null, is unknown; one
	// that sends {} read an empty count.
	for json_, wantNil := range map[string]bool{
		`{"time":"2026-10-31T12:00:00Z","probeOK":true}`:                               true,
		`{"time":"2026-10-31T12:00:00Z","probeOK":true,"responsesByStatus":null}`:      true,
		`{"time":"2026-10-31T12:00:00Z","probeOK":true,"responsesByStatus":{}}`:        false,
		`{"time":"2026-10-31T12:00:00Z","probeOK":true,"responsesByStatus":{"2xx":1}}`: false,
	} {
		var h Health
		if err := json.Unmarshal([]byte(json_), &h); err != nil {
			t.Fatal(err)
		}
		if (h.ResponsesByStatus == nil) != wantNil {
			t.Errorf("%s: nil = %v, want %v", json_, h.ResponsesByStatus == nil, wantNil)
		}
	}
}

func TestPointerHelpers(t *testing.T) {
	a, b := Int64(7), Int64(7)
	if *a != 7 || a == b {
		t.Errorf("Int64(7) = %d at %p and %p; want 7 at distinct addresses", *a, a, b)
	}
	if f := Float64(0.25); *f != 0.25 {
		t.Errorf("Float64(0.25) = %v", *f)
	}
}
