package rig

import (
	"fmt"
	"strings"
	"time"
)

// The sweep of #528, as the science agent accepted it (Q5): 5 requests a second for 2 minutes per cell;
// faults at 1% and 10% of requests, each on its own; the token bucket's burst {1, 3, 10} and refill
// {0.1, 1} a second, per deployment; F4 as one poison request at a time against a steady background,
// 20 times per arm, with and without the SDK's client retries (Q6). Streamed and non-streamed apart (Q1).
// F3x2, a tunnel that flaps twice under one request, measures arm (d)'s false stops (Q2) against (a)-(c).
const (
	SweepRPS      = 5
	SweepDuration = 2 * time.Minute
	SweepTrials   = 20
	SweepD1       = time.Second     // a death to out of routing (vLLM: about a probe interval)
	SweepD2       = 5 * time.Second // a death to restarted and ready (vLLM: minutes; see the README)
	SweepTokenGap = "5ms"
)

// Buckets are the swept token buckets: burst x refill.
var Buckets = [][2]float64{{1, 0.1}, {1, 1}, {3, 0.1}, {3, 1}, {10, 0.1}, {10, 1}}

// Sweep is every cell of the sweep, each in the group of hosted jobs it runs in.
func Sweep() []Cell {
	var out []Cell
	add := func(c Cell) {
		c.RPS, c.N, c.D1, c.D2, c.TokenDelay = SweepRPS, 3, SweepD1, SweepD2, SweepTokenGap
		c.Seed = int64(len(out) + 1)
		if c.Fault == "F4" {
			c.Trials = SweepTrials
		} else {
			c.Duration = SweepDuration
		}
		c.Name = cellName(c)
		out = append(out, c)
	}
	for _, stream := range []bool{false, true} {
		for _, fault := range []string{"F1", "F2", "F3"} {
			for _, rate := range []float64{0.01, 0.1} {
				add(Cell{Arm: "a", Fault: fault, Rate: rate, Stream: stream})
				add(Cell{Arm: "b", Fault: fault, Rate: rate, Stream: stream})
				for _, b := range Buckets {
					add(Cell{Arm: "c", Burst: b[0], Refill: b[1], Fault: fault, Rate: rate, Stream: stream})
				}
			}
		}
		for _, rate := range []float64{0.01, 0.1} {
			add(Cell{Arm: "a", Fault: "F3x2", Rate: rate, Stream: stream})
			add(Cell{Arm: "b", Fault: "F3x2", Rate: rate, Stream: stream})
			for _, b := range [][2]float64{{3, 1}, {10, 1}} {
				add(Cell{Arm: "c", Burst: b[0], Refill: b[1], Fault: "F3x2", Rate: rate, Stream: stream})
				add(Cell{Arm: "d", Burst: b[0], Refill: b[1], Fault: "F3x2", Rate: rate, Stream: stream})
			}
		}
		for _, cr := range []bool{false, true} {
			add(Cell{Arm: "a", Fault: "F4", Stream: stream, ClientRetries: cr})
			add(Cell{Arm: "b", Fault: "F4", Stream: stream, ClientRetries: cr})
			add(Cell{Arm: "c", Burst: 10, Refill: 1, Fault: "F4", Stream: stream, ClientRetries: cr})
			add(Cell{Arm: "c", Burst: 1, Refill: 0.1, Fault: "F4", Stream: stream, ClientRetries: cr})
			add(Cell{Arm: "d", Burst: 10, Refill: 1, Fault: "F4", Stream: stream, ClientRetries: cr})
		}
	}
	return out
}

func cellName(c Cell) string {
	n := fmt.Sprintf("%s-%s", c.Arm, c.Fault)
	if c.Arm == "c" || c.Arm == "d" {
		n += fmt.Sprintf("-b%g-r%g", c.Burst, c.Refill)
	}
	if c.Fault != "F4" {
		n += fmt.Sprintf("-p%g", c.Rate*100)
	} else if c.ClientRetries {
		n += "-sdk"
	}
	if c.Stream {
		n += "-stream"
	}
	return n
}

// cost is a cell's expected running time: its duration and its session probes, or for F4 about 45 s a
// trial (its restarts and the front's 30 s pause: the first hosted run took 8-10 min for 20 trials that
// killed one or two engines each).
func cost(c Cell) time.Duration {
	if c.Fault == "F4" {
		return time.Duration(c.Trials) * 45 * time.Second
	}
	return c.Duration + 20*time.Second
}

// Budget is a cell's context bound: well above its expected time. An F4 cell has no duration, only
// trials, so its bound grows with them (the first hosted run gave every F4 cell 10 min, and the 13
// whose trials killed three engines each ran out).
func Budget(c Cell) time.Duration {
	if c.Fault == "F4" {
		return time.Duration(c.Trials)*90*time.Second + 10*time.Minute
	}
	return c.Duration + 10*time.Minute
}

// Select is the cells of the sweep named in names, in the sweep's order; a name not in the sweep is an
// error.
func Select(cells []Cell, names []string) ([]Cell, error) {
	want := map[string]bool{}
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			want[n] = true
		}
	}
	var out []Cell
	for _, c := range cells {
		if want[c.Name] {
			out = append(out, c)
			delete(want, c.Name)
		}
	}
	for n := range want {
		return nil, fmt.Errorf("no cell %q in the sweep", n)
	}
	return out, nil
}

// Groups splits the cells into n groups of about equal running time, for n hosted jobs.
func Groups(cells []Cell, n int) [][]Cell {
	groups := make([][]Cell, n)
	load := make([]time.Duration, n)
	for _, c := range cells {
		k := 0
		for j := range load {
			if load[j] < load[k] {
				k = j
			}
		}
		groups[k] = append(groups[k], c)
		load[k] += cost(c)
	}
	return groups
}
