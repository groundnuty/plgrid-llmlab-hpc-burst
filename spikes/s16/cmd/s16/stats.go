package main

import (
	"math"
	"sort"
)

// dist summarises a sample: nearest-rank percentiles on a sorted copy, as
// spike S1c's s1cbench computes them (spikes/s1c/cmd/s1cbench/stats.go), copied from spikes/s14/cmd/s14/stats.go. An
// empty sample gives N=0 and zeros.
type dist struct {
	N    int     `json:"n"`
	Min  float64 `json:"min"`
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p999"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	rank = max(1, min(rank, len(sorted)))
	return sorted[rank-1]
}

func summarize(xs []float64) dist {
	if len(xs) == 0 {
		return dist{}
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	sum := 0.0
	for _, x := range s {
		sum += x
	}
	return dist{
		N: len(s), Min: round3(s[0]), P50: round3(percentile(s, 50)), P90: round3(percentile(s, 90)),
		P99: round3(percentile(s, 99)), P999: round3(percentile(s, 99.9)), Max: round3(s[len(s)-1]),
		Mean: round3(sum / float64(len(s))),
	}
}

// hist is a sample in 1 ms bins (key: the value's floor in ms), so that the
// repetitions of a configuration can be pooled at 1 ms resolution.
type hist map[int]int

func histOf(xs []float64) hist {
	h := hist{}
	for _, x := range xs {
		h[int(math.Floor(x))]++
	}
	return h
}

func round3(x float64) float64 { return math.Round(x*1000) / 1000 }
