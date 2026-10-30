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
	"time"

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
	DeviceFlushes  int64 // flush requests on the system disk during the phase (all processes)
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

func sample(stop <-chan struct{}, out *[]loadSample, mu *sync.Mutex) {
	pb, pt := cpuTimes()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			b, tt := cpuTimes()
			busy := 0.0
			if tt > pt {
				busy = 100 * (b - pb) / (tt - pt)
			}
			pb, pt = b, tt
			mu.Lock()
			*out = append(*out, loadSample{Load1: harness.Load1(), CPUBusy: busy, MemAvail: harness.MemAvailGB()})
			mu.Unlock()
		}
	}
}

func runPhase(js jetstream.JetStream, round int, mode string, workers int, dur time.Duration, payload map[string]string) phaseResult {
	res := phaseResult{Round: round, Mode: mode, Workers: workers, Load1Start: harness.Load1()}
	flush0 := deviceFlushes()
	var samples []loadSample
	var smu sync.Mutex
	stopS := make(chan struct{})
	go sample(stopS, &samples, &smu)

	lat := make([][]time.Duration, workers)
	var wg sync.WaitGroup
	start := time.Now()
	end := start.Add(dur)
	warm := start.Add(time.Second) // first second is not recorded
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; time.Now().Before(end); i++ {
				e, _ := evlayer.NewEvent(fmt.Sprintf("b-%d-%d-%d-%d", round, workers, w, i), "allocation", fmt.Sprintf("r%d", w%16), "io.hpcburst.bench.v1", time.Now(), payload)
				t0 := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, err := evlayer.Publish(ctx, js, e)
				cancel()
				d := time.Since(t0)
				if err != nil {
					fmt.Fprintln(os.Stderr, "publish:", err)
					return
				}
				if t0.After(warm) {
					lat[w] = append(lat[w], d)
				}
			}
		}(w)
	}
	wg.Wait()
	close(stopS)
	el := time.Since(start) - time.Second
	res.Load1End = harness.Load1()
	res.DeviceFlushes = deviceFlushes() - flush0

	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	res.Acks = len(all)
	res.Seconds = el.Seconds()
	res.PerSecond = float64(len(all)) / el.Seconds()
	res.P50us, res.P90us, res.P99us, res.P999us = pct(all, .5), pct(all, .9), pct(all, .99), pct(all, .999)
	if len(all) > 0 {
		res.MaxUs = float64(all[len(all)-1].Microseconds())
	}
	smu.Lock()
	res.MemAvailMinGiB = 1e9
	for _, s := range samples {
		res.CPUBusyMean += s.CPUBusy / float64(len(samples))
		if s.CPUBusy > res.CPUBusyMax {
			res.CPUBusyMax = s.CPUBusy
		}
		if s.MemAvail < res.MemAvailMinGiB {
			res.MemAvailMinGiB = s.MemAvail
		}
	}
	smu.Unlock()
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
				res := runPhase(js, r, string(mode), w, *dur, payload)
				results = append(results, res)
				fmt.Printf("round=%d mode=%-7s workers=%-2d acks=%-6d rate=%8.0f/s p50=%7.0fus p99=%8.0fus max=%8.0fus load1 %.2f->%.2f cpu_busy mean=%.0f%% max=%.0f%% flushes=%d\n",
					res.Round, res.Mode, res.Workers, res.Acks, res.PerSecond, res.P50us, res.P99us, res.MaxUs, res.Load1Start, res.Load1End, res.CPUBusyMean, res.CPUBusyMax, res.DeviceFlushes)
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
		b, _ := json.MarshalIndent(results, "", " ")
		_ = os.WriteFile(*out, b, 0o644)
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
