package main

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
)

// stats summarises a set of records: the figures of the per-workload and per-route lines.
type stats struct {
	Sessions     int // distinct session IDs
	SessionsErr  int // sessions with at least one failed request
	Requests     int
	Errors       int
	TTFTP50      float64 // ms; NaN when no request has a time to first token
	TTFTP95      float64
	SpeedP50     float64 // tokens/s per session; NaN when no session has a speed
	SpeedMin     float64
	SessionSpeed map[string]float64
}

// summarize computes the stats of recs. A session's speed is the tokens after the first token of its requests over the time spent
// after their first tokens: sum of (completion_tokens - 1) over sum of (last delta - first delta). It is taken here from each record's
// decode speed and its token count, so a record without either contributes nothing.
func summarize(recs []Record) stats {
	st := stats{SessionSpeed: map[string]float64{}}
	sessions, failed := map[string]bool{}, map[string]bool{}
	var ttfts []float64
	tokens, secs := map[string]float64{}, map[string]float64{}
	for _, r := range recs {
		st.Requests++
		sessions[r.Session] = true
		if r.Error != "" {
			st.Errors++
			failed[r.Session] = true
			continue
		}
		if r.TTFTMs != nil {
			ttfts = append(ttfts, *r.TTFTMs)
		}
		if r.DecodeTokensPerSec != nil && r.CompletionTokens != nil {
			n := float64(*r.CompletionTokens - 1)
			tokens[r.Session] += n
			secs[r.Session] += n / *r.DecodeTokensPerSec
		}
	}
	st.Sessions, st.SessionsErr = len(sessions), len(failed)
	st.TTFTP50, st.TTFTP95 = percentile(ttfts, 50), percentile(ttfts, 95)
	var speeds []float64
	for s, t := range secs {
		v := tokens[s] / t
		st.SessionSpeed[s] = v
		speeds = append(speeds, v)
	}
	st.SpeedP50 = percentile(speeds, 50)
	st.SpeedMin = math.NaN()
	for _, v := range speeds {
		if math.IsNaN(st.SpeedMin) || v < st.SpeedMin {
			st.SpeedMin = v
		}
	}
	return st
}

// percentile is the nearest-rank percentile: the value at rank ceil(p/100 * n) of the sorted values. NaN for no values.
func percentile(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return math.NaN()
	}
	v := append([]float64(nil), vals...)
	sort.Float64s(v)
	rank := int(math.Ceil(p / 100 * float64(len(v))))
	return v[min(max(rank, 1), len(v))-1]
}

func fmtNum(v float64, unit string) string {
	if math.IsNaN(v) {
		return "n/a"
	}
	return fmt.Sprintf("%.1f%s", v, unit)
}

func (s stats) line() string {
	return fmt.Sprintf("sessions %d (%d with errors), requests %d, errors %d, ttft p50 %s p95 %s, speed per session p50 %s min %s",
		s.Sessions, s.SessionsErr, s.Requests, s.Errors,
		fmtNum(s.TTFTP50, "ms"), fmtNum(s.TTFTP95, "ms"), fmtNum(s.SpeedP50, " tok/s"), fmtNum(s.SpeedMin, " tok/s"))
}

// writeSummary prints one line per workload (in the config's order) and one per route (sorted). It prints numbers, names from the
// config and the attribution header values, and nothing else from a response.
func writeSummary(w io.Writer, cfg Config, recs []Record) {
	fmt.Fprintf(w, "requests: %d\n", len(recs))
	for _, wl := range cfg.Workloads {
		var sub []Record
		for _, r := range recs {
			if r.Workload == wl.Name {
				sub = append(sub, r)
			}
		}
		fmt.Fprintf(w, "workload %s: %s\n", wl.Name, summarize(sub).line())
	}
	byRoute := map[Route][]Record{}
	for _, r := range recs {
		byRoute[r.route()] = append(byRoute[r.route()], r)
	}
	routes := make([]Route, 0, len(byRoute))
	for r := range byRoute {
		routes = append(routes, r)
	}
	sort.Slice(routes, func(i, j int) bool { return routes[i].String() < routes[j].String() })
	for _, r := range routes {
		fmt.Fprintf(w, "route %s: %s\n", oneLine(r.String()), summarize(byRoute[r]).line())
	}
}

// oneLine keeps a header value from breaking the summary into lines.
func oneLine(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
}
