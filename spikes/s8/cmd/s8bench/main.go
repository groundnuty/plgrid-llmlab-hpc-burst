// Command s8bench compares the publish throughput and latency of
// "sync_interval: always" with the default fsync policy, interleaving the runs
// (always, default, always, default, ...) and recording the VM load around and
// during each phase. It also times three ways of consuming a backlog.
//
//	go run ./cmd/s8bench -rounds 3 -dur 8s -workers 1,8,32
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/harness"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/jsnode"
	"github.com/nats-io/nats.go/jetstream"
)

type loadSample struct {
	Load1    float64
	CPUBusy  float64 // system-wide, percent, since the previous sample
	MemAvail float64 // GiB
}

type phaseResult struct {
	Round, Workers int
	Mode           string
	Acks           int
	Seconds        float64
	PerSecond      float64
	P50us, P90us   float64
	P99us, P999us  float64
	MaxUs          float64
	Load1Start     float64
	Load1End       float64
	CPUBusyMean    float64
	CPUBusyMax     float64
	MemAvailMinGiB float64
	DeviceFlushes  int64 // flush requests on the system disk during the measured window (all processes)
	Load1Min       float64
	Load1Max       float64
	Samples        []loadSample // one per sampling tick in the measured window, kept for the record
	// Errors counts publishes that failed. A phase with any error is not a measurement of the
	// publish path (the failing worker stops), so it is Invalid and must not be reported.
	Errors     int
	FirstError string
	Valid      bool
}

// probes are the readings a phase takes of the machine. Tests replace them.
type probes struct {
	flushes  func() int64
	load1    func() float64
	memAvail func() float64
	cpuTimes func() (busy, total float64)
}

func realProbes() probes {
	return probes{flushes: deviceFlushes, load1: harness.Load1, memAvail: harness.MemAvailGB, cpuTimes: cpuTimes}
}

func cpuTimes() (busy, total float64) {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0
	}
	f := strings.Fields(strings.SplitN(string(b), "\n", 2)[0])[1:]
	for i, v := range f {
		x, _ := strconv.ParseFloat(v, 64)
		total += x
		if i != 3 && i != 4 { // idle, iowait
			busy += x
		}
	}
	return
}

func deviceFlushes() int64 {
	b, err := os.ReadFile("/sys/block/vda/stat")
	if err != nil {
		return -1
	}
	f := strings.Fields(string(b))
	if len(f) < 16 {
		return -1
	}
	v, _ := strconv.ParseInt(f[15], 10, 64)
	return v
}

func pct(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)-1))
	return float64(sorted[i].Microseconds())
}

// sample records the machine load every `every` until stop is closed.
func sample(pr probes, every time.Duration, stop <-chan struct{}, out *[]loadSample, mu *sync.Mutex) {
	pb, pt := pr.cpuTimes()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			b, tt := pr.cpuTimes()
			busy := 0.0
			if tt > pt {
				busy = 100 * (b - pb) / (tt - pt)
			}
			pb, pt = b, tt
			mu.Lock()
			*out = append(*out, loadSample{Load1: pr.load1(), CPUBusy: busy, MemAvail: pr.memAvail()})
			mu.Unlock()
		}
	}
}

// phaseSpec describes one phase.
type phaseSpec struct {
	Round, Workers int
	Mode           string
	Dur            time.Duration
	Warmup         time.Duration // not recorded; every counter of the phase starts when it ends
	SampleEvery    time.Duration
	Publish        func(ctx context.Context, e cloudevents.Event) error
	Probes         probes
}

// runPhase drives Workers publishers for Dur. Everything it reports covers one window, the
// measured window from the end of the warm-up to the moment the last publish returned: the
// acknowledgements counted, the elapsed time, the device flush requests and the load samples.
func runPhase(sp phaseSpec, payload map[string]string) phaseResult {
	res := phaseResult{Round: sp.Round, Mode: sp.Mode, Workers: sp.Workers}
	var samples []loadSample
	var smu sync.Mutex
	stopS := make(chan struct{})
	var flush0 atomic.Int64
	var load0 atomic.Uint64
	windowOpen, samplerDone := make(chan struct{}), make(chan struct{})
	go func() { // opens the measured window: counters and sampler start together
		defer close(samplerDone)
		time.Sleep(sp.Warmup)
		flush0.Store(sp.Probes.flushes())
		load0.Store(uint64(sp.Probes.load1() * 1000))
		close(windowOpen)
		sample(sp.Probes, sp.SampleEvery, stopS, &samples, &smu)
	}()

	lat := make([][]time.Duration, sp.Workers)
	var errCount atomic.Int64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	start := time.Now()
	end := start.Add(sp.Dur)
	warm := start.Add(sp.Warmup)
	for w := 0; w < sp.Workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			fail := func(err error) {
				if errCount.Add(1) == 1 {
					firstErr.Store(err.Error())
				}
			}
			for i := 0; time.Now().Before(end); i++ {
				e, err := evlayer.NewEvent(fmt.Sprintf("b-%d-%d-%d-%d", sp.Round, sp.Workers, w, i), "allocation", fmt.Sprintf("r%d", w%16), "io.hpcburst.bench.v1", time.Now(), payload)
				if err != nil {
					fail(err)
					return
				}
				t0 := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err = sp.Publish(ctx, e)
				cancel()
				d := time.Since(t0)
				if err != nil {
					fail(err)
					return
				}
				if t0.After(warm) {
					lat[w] = append(lat[w], d)
				}
			}
		}(w)
	}
	wg.Wait()
	windowEnd := time.Now()
	<-windowOpen                  // Dur shorter than Warmup would leave no window: wait so the counters exist
	flush1 := sp.Probes.flushes() // the flush window ends where the acknowledgement window ends
	close(stopS)
	<-samplerDone
	load1End := sp.Probes.load1()
	el := windowEnd.Sub(warm)
	res.Load1Start = float64(load0.Load()) / 1000
	res.Load1End = load1End
	res.DeviceFlushes = flush1 - flush0.Load()
	res.Errors = int(errCount.Load())
	if v, ok := firstErr.Load().(string); ok {
		res.FirstError = v
	}
	res.Valid = res.Errors == 0

	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	res.Acks = len(all)
	if el > 0 { // a phase that failed during the warm-up has no measured window
		res.Seconds = el.Seconds()
		res.PerSecond = float64(len(all)) / el.Seconds()
	}
	res.P50us, res.P90us, res.P99us, res.P999us = pct(all, .5), pct(all, .9), pct(all, .99), pct(all, .999)
	if len(all) > 0 {
		res.MaxUs = float64(all[len(all)-1].Microseconds())
	}
	smu.Lock()
	defer smu.Unlock()
	res.Samples = append([]loadSample(nil), samples...)
	res.MemAvailMinGiB = 1e9
	res.Load1Min, res.Load1Max = res.Load1Start, res.Load1Start
	for _, s := range samples {
		res.CPUBusyMean += s.CPUBusy / float64(len(samples))
		if s.CPUBusy > res.CPUBusyMax {
			res.CPUBusyMax = s.CPUBusy
		}
		if s.MemAvail < res.MemAvailMinGiB {
			res.MemAvailMinGiB = s.MemAvail
		}
		res.Load1Min, res.Load1Max = min(res.Load1Min, s.Load1), max(res.Load1Max, s.Load1)
	}
	res.Load1Min, res.Load1Max = min(res.Load1Min, res.Load1End), max(res.Load1Max, res.Load1End)
	return res
}

func main() {
	rounds := flag.Int("rounds", 3, "number of always/default pairs (runs are interleaved A,B,A,B,...)")
	dur := flag.Duration("dur", 8*time.Second, "duration of each phase (the first second is a warm-up)")
	workersFlag := flag.String("workers", "1,8,32", "publisher concurrency levels, one phase each")
	consume := flag.Bool("consume", true, "also time the consumer variants on a 20000-event backlog")
	out := flag.String("out", "", "write the raw results as JSON to this file")
	flag.Parse()

	var levels []int
	for _, s := range strings.Split(*workersFlag, ",") {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			fmt.Fprintln(os.Stderr, "bad -workers")
			os.Exit(2)
		}
		levels = append(levels, n)
	}
	payload := map[string]string{"state": "ending-soon", "reason": "walltime", "note": strings.Repeat("x", 120)}
	var results []phaseResult
	invalid := 0
	for r := 1; r <= *rounds; r++ {
		for _, mode := range []jsnode.SyncMode{jsnode.SyncAlways, jsnode.SyncDefault} {
			if err := harness.WaitForCapacity(context.Background(), 4.0, 8); err != nil {
				panic(err)
			}
			dir, err := harness.NewDir("bench-" + string(mode))
			if err != nil {
				panic(err)
			}
			n, err := jsnode.Start(jsnode.Config{StoreDir: dir + "/js", Sync: mode})
			if err != nil {
				panic(err)
			}
			nc, js, err := evlayer.Connect(n.URL())
			if err != nil {
				panic(err)
			}
			if _, err := evlayer.EnsureStream(context.Background(), js, evlayer.StreamOptions{Name: "EV"}); err != nil {
				panic(err)
			}
			for _, w := range levels {
				res := runPhase(phaseSpec{
					Round: r, Workers: w, Mode: string(mode), Dur: *dur, Warmup: time.Second, SampleEvery: time.Second,
					Publish: func(ctx context.Context, e cloudevents.Event) error {
						_, err := evlayer.Publish(ctx, js, e)
						return err
					},
					Probes: realProbes(),
				}, payload)
				results = append(results, res)
				status := "ok"
				if !res.Valid {
					status = fmt.Sprintf("INVALID (%d publish errors, first: %s)", res.Errors, res.FirstError)
					invalid++
				}
				fmt.Printf("round=%d mode=%-7s workers=%-2d acks=%-6d rate=%8.0f/s p50=%7.0fus p99=%8.0fus max=%8.0fus load1 %.2f->%.2f (min %.2f max %.2f) cpu_busy mean=%.0f%% max=%.0f%% flushes=%d %s\n",
					res.Round, res.Mode, res.Workers, res.Acks, res.PerSecond, res.P50us, res.P99us, res.MaxUs, res.Load1Start, res.Load1End, res.Load1Min, res.Load1Max, res.CPUBusyMean, res.CPUBusyMax, res.DeviceFlushes, status)
			}
			nc.Close()
			n.Stop()
			_ = os.RemoveAll(dir)
		}
	}
	if *consume {
		consumeBench()
	}
	if *out != "" {
		b, err := json.MarshalIndent(results, "", " ")
		if err == nil {
			err = os.WriteFile(*out, b, 0o644)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "s8bench: writing results:", err)
			os.Exit(1)
		}
	}
	if invalid > 0 {
		fmt.Fprintf(os.Stderr, "s8bench: %d phase(s) had publish errors and are invalid; do not use these numbers\n", invalid)
		os.Exit(1)
	}
}

// consumeBench pre-publishes a backlog and times how fast each subscriber drains it
// with a no-op handler: a plain pull consumer, the durable layer (one direct get per
// message for re-sequencing) and the stored-cursor tail.
func consumeBench() {
	const total = 20000
	if err := harness.WaitForCapacity(context.Background(), 4.0, 8); err != nil {
		panic(err)
	}
	dir, _ := harness.NewDir("bench-consume")
	defer os.RemoveAll(dir)
	n, err := jsnode.Start(jsnode.Config{StoreDir: dir + "/js", Sync: jsnode.SyncDefault})
	if err != nil {
		panic(err)
	}
	defer n.Stop()
	nc, js, err := evlayer.Connect(n.URL())
	if err != nil {
		panic(err)
	}
	defer nc.Close()
	ctx := context.Background()
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: "EV"}); err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < total; i += 8 {
				e, _ := evlayer.NewEvent(fmt.Sprintf("c-%d", i), "allocation", fmt.Sprintf("r%d", i%16), "io.hpcburst.bench.v1", time.Now(), nil)
				if _, err := evlayer.Publish(ctx, js, e); err != nil {
					panic(err)
				}
			}
		}(w)
	}
	wg.Wait()
	noop := func(ctx context.Context, r evlayer.Record) error { return nil }

	// plain pull consumer
	cons, _ := js.CreateOrUpdateConsumer(ctx, "EV", jetstream.ConsumerConfig{Durable: "plain", AckPolicy: jetstream.AckExplicitPolicy, MaxAckPending: 1000})
	t0 := time.Now()
	got := 0
	for got < total {
		b, err := cons.Fetch(100, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			panic(err)
		}
		for m := range b.Messages() {
			_ = m.Ack()
			got++
		}
	}
	plain := time.Since(t0)

	// durable layer
	d, err := evlayer.NewDurable(ctx, js, evlayer.DurableConfig{Stream: "EV", Durable: "layer", AckWait: 30 * time.Second, MaxAckPending: 1000, Batch: 100})
	if err != nil {
		panic(err)
	}
	cnt := 0
	rctx, cancel := context.WithCancel(ctx)
	t0 = time.Now()
	done := make(chan error, 1)
	go func() {
		done <- d.Run(rctx, func(ctx context.Context, r evlayer.Record) error {
			cnt++
			if cnt == total {
				cancel()
			}
			return nil
		})
	}()
	<-done
	layer := time.Since(t0)

	// stored-cursor tail
	cnt = 0
	rctx2, cancel2 := context.WithCancel(ctx)
	t0 = time.Now()
	err = evlayer.Tail(rctx2, js, evlayer.TailConfig{Stream: "EV", Store: evlayer.FileCursor{Path: dir + "/cursor.json"}, SaveEvery: 100}, func(ctx context.Context, r evlayer.Record) error {
		cnt++
		if cnt == total {
			cancel2()
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	tail := time.Since(t0)
	_ = noop
	fmt.Printf("consume %d events: plain pull+ack %.0f/s, durable layer %.0f/s, stored-cursor tail %.0f/s (cursor saved every 100)\n",
		total, total/plain.Seconds(), total/layer.Seconds(), total/tail.Seconds())
}
