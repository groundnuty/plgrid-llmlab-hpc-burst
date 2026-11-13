package rig

import (
	"fmt"
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

// Groups splits the sweep into n groups of about equal running time, for n hosted jobs. A cell's
// time is its duration, or about 30 s per F4 trial (its restarts and the front's pause).
func Groups(cells []Cell, n int) [][]Cell {
	cost := func(c Cell) time.Duration {
		if c.Fault == "F4" {
			return time.Duration(c.Trials) * 30 * time.Second
		}
		return c.Duration + 20*time.Second
	}
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
