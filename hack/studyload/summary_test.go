package main

import (
	"bytes"
	"math"
	"testing"
)

func TestPercentileIsNearestRank(t *testing.T) {
	cases := []struct {
		vals   []float64
		p, out float64
	}{
		{[]float64{40, 10, 30, 20}, 50, 20},     // rank ceil(0.5*4) = 2
		{[]float64{40, 10, 30, 20}, 95, 40},     // rank ceil(0.95*4) = 4
		{[]float64{10, 20, 30, 40, 50}, 50, 30}, // rank ceil(2.5) = 3
		{[]float64{10, 20, 30, 40, 50}, 95, 50}, // rank ceil(4.75) = 5
		{[]float64{7}, 50, 7},                   // one value
		{[]float64{7}, 95, 7},
	}
	for _, c := range cases {
		if got := percentile(c.vals, c.p); got != c.out {
			t.Errorf("percentile(%v, %v) = %v, want %v", c.vals, c.p, got, c.out)
		}
	}
	if !math.IsNaN(percentile(nil, 50)) {
		t.Error("the percentile of nothing is not NaN")
	}
}

func rec(wl, session string, ttft float64, completion int, speed float64, p Attribution, errText string) Record {
	r := Record{Workload: wl, Session: session, Turn: 1, Status: 200, Attribution: p, Error: errText}
	if ttft > 0 {
		r.TTFTMs = ptrFloat(ttft)
	}
	if completion > 0 {
		r.CompletionTokens = ptrInt(completion)
	}
	if speed > 0 {
		r.DecodeTokensPerSec = ptrFloat(speed)
	}
	return r
}

func TestSummaryFiguresAreWhatTheRecordsHold(t *testing.T) {
	routeA := Attribution{Provider: "a", DeploymentID: "d1", Instance: "i1"}
	routeB := Attribution{Provider: "b", DeploymentID: "d2", Instance: "i2"}
	recs := []Record{
		// session s1 (route A): 10 and 20 tokens after the first, both at 100 tok/s: 30 tokens in 0.3 s, 100 tok/s.
		rec("w", "s1", 10, 11, 100, routeA, ""),
		rec("w", "s1", 20, 21, 100, routeA, ""),
		// session s2 (route B): 10 tokens after the first at 50 tok/s.
		rec("w", "s2", 30, 11, 50, routeB, ""),
		// session s3 (route B): 10 tokens after the first at 200 tok/s.
		rec("w", "s3", 40, 11, 200, routeB, ""),
		// a failed request on route B: counted as a request and an error, and it has no ttft or speed.
		rec("w", "s4", 0, 0, 0, routeB, "status 503"),
	}
	st := summarize(recs)
	if st.Sessions != 4 || st.SessionsErr != 1 || st.Requests != 5 || st.Errors != 1 {
		t.Errorf("sessions %d (%d with errors) requests %d errors %d, want 4 (1) 5 1", st.Sessions, st.SessionsErr, st.Requests, st.Errors)
	}
	// ttft of the four good requests: 10 20 30 40: p50 rank 2 = 20, p95 rank 4 = 40.
	if st.TTFTP50 != 20 || st.TTFTP95 != 40 {
		t.Errorf("ttft p50 %v p95 %v, want 20 and 40", st.TTFTP50, st.TTFTP95)
	}
	// speeds per session 100, 50, 200: sorted 50 100 200, p50 rank 2 = 100, min 50.
	if math.Abs(st.SpeedP50-100) > 1e-9 || math.Abs(st.SpeedMin-50) > 1e-9 {
		t.Errorf("speed p50 %v min %v, want 100 and 50", st.SpeedP50, st.SpeedMin)
	}

	var out bytes.Buffer
	writeSummary(&out, Config{Workloads: []Workload{{Name: "w"}}}, recs)
	want := "requests: 5\n" +
		"workload w: sessions 4 (1 with errors), requests 5, errors 1, ttft p50 20.0ms p95 40.0ms, speed per session p50 100.0 tok/s min 50.0 tok/s\n" +
		"route provider=a deployment=d1 instance=i1: sessions 1 (0 with errors), requests 2, errors 0, ttft p50 10.0ms p95 20.0ms, speed per session p50 100.0 tok/s min 100.0 tok/s\n" +
		"route provider=b deployment=d2 instance=i2: sessions 3 (1 with errors), requests 3, errors 1, ttft p50 30.0ms p95 40.0ms, speed per session p50 50.0 tok/s min 50.0 tok/s\n"
	if out.String() != want {
		t.Errorf("summary:\n%s\nwant:\n%s", out.String(), want)
	}
}

func TestSummaryOfNothingSaysNA(t *testing.T) {
	var out bytes.Buffer
	writeSummary(&out, Config{Workloads: []Workload{{Name: "w"}}}, nil)
	want := "requests: 0\nworkload w: sessions 0 (0 with errors), requests 0, errors 0, ttft p50 n/a p95 n/a, speed per session p50 n/a min n/a\n"
	if out.String() != want {
		t.Errorf("summary %q, want %q", out.String(), want)
	}
}
