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
	for _, key := range []string{"runningRequests", "generatedTokens", "kvCacheUsage", "metricsReadAt", "responsesByStatus"} {
		if strings.Contains(got, key) {
			t.Errorf("%s: carries %s, which was not set", got, key)
		}
	}
}

// MetricsFresh is the rule "no gauge-based action while MetricsReadAt is older
// than two probe intervals", measured from the sample's own time. The boundary
// values are written out: exactly two intervals old is still fresh, one
// nanosecond more is stale.
func TestMetricsFresh(t *testing.T) {
	const interval = 10 * time.Second
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	readAgo := func(age time.Duration) *time.Time { r := at.Add(-age); return &r }
	for _, c := range []struct {
		name     string
		h        Health
		interval time.Duration
		want     bool
	}{
		{"read just now", Health{Time: at, MetricsReadAt: readAgo(0)}, interval, true},
		{"read one interval ago", Health{Time: at, MetricsReadAt: readAgo(interval)}, interval, true},
		{"read exactly two intervals ago", Health{Time: at, MetricsReadAt: readAgo(2 * interval)}, interval, true},
		{"read two intervals and a nanosecond ago", Health{Time: at, MetricsReadAt: readAgo(2*interval + 1)}, interval, false},
		{"read five minutes ago", Health{Time: at, MetricsReadAt: readAgo(5 * time.Minute)}, interval, false},
		{"never read", Health{Time: at}, interval, false},
		{"read after the sample's time", Health{Time: at, MetricsReadAt: readAgo(-time.Second)}, interval, true},
		{"a sample with no time", Health{MetricsReadAt: readAgo(0)}, interval, false},
		{"a zero probe interval", Health{Time: at, MetricsReadAt: readAgo(0)}, 0, false},
		{"a longer interval widens the bound", Health{Time: at, MetricsReadAt: readAgo(25 * time.Second)}, 15 * time.Second, true},
	} {
		if got := c.h.MetricsFresh(c.interval); got != c.want {
			t.Errorf("%s: MetricsFresh(%v) = %v, want %v", c.name, c.interval, got, c.want)
		}
	}
	if MetricsStaleProbeIntervals != 2 {
		t.Errorf("MetricsStaleProbeIntervals = %d, want 2 (the science agent's ruling)", MetricsStaleProbeIntervals)
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
