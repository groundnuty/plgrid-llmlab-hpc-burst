// Command s10sim runs spike S10's experiments: routing policies and
// conversation keys on a simulated agent workload, in steady state and
// around membership events, and writes Markdown and CSV tables.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10/sim"
)

const (
	winStart = 1200.0 // warm-up: the longest sessions take several minutes to build context
	winEnd   = 4800.0 // arrivals stop here
	evTime   = 3000.0 // membership events
)

type job struct {
	kv     int
	seed   uint64
	pol    sim.Policy
	events []sim.Event
	scen   string
}

type result struct {
	job
	m       *sim.Metrics
	elapsed time.Duration
}

func main() {
	mode := flag.String("mode", "all", "calibrate | steady | events | drainfix | all")
	seedsF := flag.String("seeds", "1,2,3", "workload seeds")
	kvF := flag.String("kv", "750000,1500000,3000000", "KV cache tokens per Instance (steady sweep)")
	evKV := flag.String("events-kv", "1500000,3000000", "KV cache tokens per Instance for the event runs")
	block := flag.Int("block", 64, "cache block size in tokens")
	out := flag.String("out", "results", "output directory")
	par := flag.Int("parallel", 2, "simulations run at once")
	flag.Parse()

	seeds := parseList(*seedsF)
	kvs := parseList(*kvF)
	var seedList []uint64
	for _, s := range seeds {
		seedList = append(seedList, uint64(s))
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	r := &runner{seeds: seedList, block: *block, par: *par}
	switch *mode {
	case "calibrate":
		r.calibrate(kvs)
	case "steady":
		r.steady(*out, kvs)
	case "events":
		r.eventRuns(*out, parseList(*evKV))
	case "drainfix":
		r.drainFix(*out, parseList(*evKV))
	case "all":
		r.steady(*out, kvs)
		r.eventRuns(*out, parseList(*evKV))
		r.drainFix(*out, parseList(*evKV))
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
}

func parseList(s string) []int {
	var out []int
	for _, f := range strings.Split(s, ",") {
		v, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil {
			log.Fatalf("bad list %q: %v", s, err)
		}
		out = append(out, v)
	}
	return out
}

type runner struct {
	seeds []uint64
	block int
	par   int
}

func (r *runner) run(jobs []job) []result {
	res := make([]result, len(jobs))
	sem := make(chan struct{}, r.par)
	var wg sync.WaitGroup
	for i, j := range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			cl := sim.DefaultCluster(j.kv)
			cl.BlockSize = r.block
			wl := sim.DefaultWorkload(j.seed)
			e := sim.NewEngine(cl, j.pol, wl, nil, j.events)
			e.M.WinStart, e.M.WinEnd = winStart, winEnd
			t0 := time.Now()
			if err := e.Run(); err != nil {
				log.Fatalf("%s %s kv=%d seed=%d %s: %v", j.pol.Name(), j.pol.Key, j.kv, j.seed, j.scen, err)
			}
			res[i] = result{job: j, m: e.M, elapsed: time.Since(t0)}
		}()
	}
	wg.Wait()
	return res
}

// policies returns every routing configuration compared in steady state.
func policies() []sim.Policy {
	ps := []sim.Policy{{Kind: sim.RoundRobin}, {Kind: sim.LeastLoad}}
	for _, k := range sim.AllKeys {
		ps = append(ps, sim.Policy{Kind: sim.Affinity, Key: k})
		for _, pct := range []int{125, 150, 200} {
			ps = append(ps, sim.Policy{Kind: sim.CHWBL, Key: k, Percent: pct})
		}
	}
	return ps
}

func (r *runner) calibrate(kvs []int) {
	var jobs []job
	for _, kv := range kvs {
		for _, seed := range r.seeds {
			for _, p := range []sim.Policy{{Kind: sim.RoundRobin}, {Kind: sim.LeastLoad}, {Kind: sim.Affinity, Key: sim.KeyHeaderOpening}, {Kind: sim.CHWBL, Key: sim.KeyHeaderOpening, Percent: 125}} {
				jobs = append(jobs, job{kv: kv, seed: seed, pol: p})
			}
		}
	}
	for _, x := range r.run(jobs) {
		c, tok, inf := x.m.WorkingSet()
		fmt.Printf("%s kv=%d seed=%d active=%.1f context=%.0f tokens (%.2fx cluster KV) inflight=%.1f peak=%d hit=%.3f mean/p95=%.1f/%.1fs requests=%d out=%d compactions=%d elapsed=%s\n",
			label(x.pol), x.kv, x.seed, c, tok, tok/float64(4*x.kv), inf, x.m.PeakLoad, x.m.HitRate(), x.m.MeanLatency(), x.m.Latency(0.95), x.m.Requests, x.m.OutputTokens, x.m.Compactions, x.elapsed.Round(time.Millisecond))
	}
}

func label(p sim.Policy) string {
	if p.Hashed() {
		return p.Name() + " / " + string(p.Key)
	}
	return p.Name()
}

func writeFile(dir, name, body string) {
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Println("wrote", p)
}
