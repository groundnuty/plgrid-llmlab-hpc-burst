package main

import (
	"encoding/json"
	"math"
	"os"
	"sort"
)

// dist summarises a sample: nearest-rank percentiles on a sorted copy, NaN-free (an empty sample
// gives N=0 and zeros).
type dist struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
	Min float64 `json:"min"`
	P5  float64 `json:"p5"`
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func summarize(xs []float64) dist {
	if len(xs) == 0 {
		return dist{}
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return dist{
		N: len(s), P50: percentile(s, 50), P95: percentile(s, 95), P99: percentile(s, 99),
		Max: s[len(s)-1], Min: s[0], P5: percentile(s, 5),
	}
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if path == "" || path == "-" {
		_, err = os.Stdout.Write(append(b, '\n'))
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func ms(d float64) float64 { return math.Round(d*1000) / 1000 }
