package run

// A measured cell of S18 (#528), run when S18_CELL names its configuration (JSON, rig.Cell) and S18_OUT a
// directory: the stand under the cell's arm, a steady load at the cell's rate with the cell's fault on
// a share of requests (F1-F3), or one poison request at a time against that load (F4). It writes the
// client's attempts, the engines' records, the process ends, the layer's attempts and the summary.

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s18/rig"
)

func policyOf(c rig.Cell) *rig.Policy {
	switch c.Arm {
	case "b":
		return &rig.Policy{Name: "b", MaxRetries: 1}
	case "c":
		return &rig.Policy{Name: "c", MaxRetries: c.N - 1, Bucket: rig.NewBucket(c.Burst, c.Refill)}
	case "d":
		return &rig.Policy{Name: "d", MaxRetries: c.N - 1, Bucket: rig.NewBucket(c.Burst, c.Refill), StopAfterDeaths: 2}
	}
	return nil // a: the front as it is
}

func TestCell(t *testing.T) {
	spec, out := os.Getenv("S18_CELL"), os.Getenv("S18_OUT")
	if spec == "" || out == "" {
		t.Skip("a measured cell: set S18_CELL (a rig.Cell as JSON) and S18_OUT")
	}
	var cell rig.Cell
	if err := json.Unmarshal([]byte(spec), &cell); err != nil {
		t.Fatalf("S18_CELL: %v", err)
	}
	if cell.N == 0 {
		cell.N = 3
	}
	s := runCell(t, cell, filepath.Join(out, cell.Name))
	t.Logf("%s: success %.4f (marked %.4f, unmarked %.4f), fired %d of %d marked, duplicates %d, re-executions %d, deaths %d",
		cell.Name, s.Success, s.SuccessMarked, s.SuccessUnmarked, s.Fired, s.Marked, s.Duplicates, s.ReExecutions, s.Deaths)
}

// TestGroup runs one hosted job's group of the sweep (rig.Groups of rig.Sweep), S18_GROUP of S18_GROUPS,
// writing each cell under S18_OUT; S18_CELLS, when set, names the only cells to split (a rerun). Each cell is a subtest, so one cell's failure (its books unbalanced)
// is reported and the group goes on.
func TestGroup(t *testing.T) {
	g, n, out := os.Getenv("S18_GROUP"), os.Getenv("S18_GROUPS"), os.Getenv("S18_OUT")
	if g == "" || n == "" || out == "" {
		t.Skip("one hosted job's group of the sweep: set S18_GROUP, S18_GROUPS and S18_OUT")
	}
	var gi, gn int
	var err error
	if _, err = fmt.Sscan(g, &gi); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Sscan(n, &gn); err != nil {
		t.Fatal(err)
	}
	cells := rig.Sweep()
	if names := os.Getenv("S18_CELLS"); names != "" {
		if cells, err = rig.Select(cells, strings.Split(names, ",")); err != nil {
			t.Fatal(err)
		}
	}
	groups := rig.Groups(cells, gn)
	for _, cell := range groups[gi] {
		t.Run(cell.Name, func(t *testing.T) {
			start := time.Now()
			s := runCell(t, cell, filepath.Join(out, cell.Name))
			t.Logf("%s in %s: success %.4f, fired %.4f (%d of %d marked), duplicates %d, re-executions %d, deaths %d",
				cell.Name, time.Since(start).Round(time.Second), s.Success, s.SuccessFired, s.Fired, s.Marked, s.Duplicates, s.ReExecutions, s.Deaths)
		})
	}
}

func runCell(t *testing.T, cell rig.Cell, dir string) rig.Summary {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	r, err := rig.New(shortDir(t), rig.Config{N: cell.N, Fault: cell.Fault, Policy: policyOf(cell), D1: cell.D1, D2: cell.D2,
		Engine: engine(t), TokenDelay: cell.TokenDelay})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), rig.Budget(cell))
	defer cancel()
	c := rig.NewClient(r.Front.URL, r.Front.ClientKey, fmt.Sprintf("%x", time.Now().UnixNano()&0xffffff))
	rng := rand.New(rand.NewSource(cell.Seed))
	var mu sync.Mutex
	var results []rig.Result
	keep := func(rs ...rig.Result) {
		mu.Lock()
		results = append(results, rs...)
		mu.Unlock()
	}
	sessions := make([]string, cell.N)
	for i := range sessions {
		s, probes, err := r.SessionOn(ctx, c, i)
		if err != nil {
			t.Fatal(err)
		}
		keep(probes...) // ordinary requests of the run, accounted for like any other
		sessions[i] = s
	}
	start := time.Now()
	var wg sync.WaitGroup
	// load sends requests at cell.RPS until until, or until stop closes; with faulted, a share
	// cell.Rate of them carries the cell's fault.
	load := func(until time.Time, stop <-chan struct{}, faulted bool) {
		tick := time.NewTicker(time.Duration(float64(time.Second) / cell.RPS))
		defer tick.Stop()
		for time.Now().Before(until) {
			select {
			case <-tick.C:
			case <-stop:
				return
			}
			mu.Lock()
			marked, target, session := faulted && rng.Float64() < cell.Rate, rng.Intn(cell.N), fmt.Sprintf("bg-%d", rng.Int63())
			mu.Unlock()
			marker := ""
			if marked {
				switch cell.Fault {
				case "F1":
					marker, session = rig.MarkF1+fmt.Sprint(target), sessions[target]
				case "F2":
					marker, session = rig.MarkF2+fmt.Sprint(target), sessions[target]
				case "F3", "F3x2":
					marker = rig.MarkF3
				}
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				keep(c.Do(ctx, c.NextBase(), 0, session, cell.Stream, marker))
			}()
		}
	}
	var poison []rig.PoisonTrial
	var poisonRes [][]rig.Result
	if cell.Fault == "F4" {
		stop, done := make(chan struct{}), make(chan struct{})
		go func() { defer close(done); load(time.Now().Add(24*time.Hour), stop, false) }()
		backoff := []time.Duration(nil)
		if cell.ClientRetries {
			backoff = rig.SDKBackoff
		}
		for k := 0; k < cell.Trials; k++ {
			target := k % cell.N
			before := r.Deaths()
			rs := c.DoRetrying(ctx, c.NextBase(), sessions[target], cell.Stream, rig.MarkPoison, backoff)
			keep(rs...)
			poisonRes = append(poisonRes, rs)
			// Settled on state, not time: every engine the poison's own records show it died under has
			// been seen dead, and every restart is ready, before the next poison.
			begins, err := r.Begins()
			if err != nil {
				t.Fatal(err)
			}
			under := rig.PoisonCost(rs[0].Base, rs, begins, nil, nil).Deaths
			eventually(t, time.Minute, "the poison's deaths seen", func() bool { return r.Deaths() >= before+under })
			if err := r.AllReady(ctx); err != nil {
				t.Fatal(err)
			}
		}
		close(stop)
		<-done
		cell.Duration = time.Since(start)
	} else {
		load(start.Add(cell.Duration), nil, true)
	}
	wg.Wait()
	if err := r.AllReady(ctx); err != nil {
		t.Fatal(err)
	}
	begins, err := r.Begins()
	if err != nil {
		t.Fatal(err)
	}
	var attempts []rig.Attempt
	if r.Layer != nil {
		attempts = r.Layer.Attempts()
	}
	if probs := rig.Reconcile(results, begins); len(probs) > 0 {
		for _, p := range probs {
			t.Error(p)
		}
	}
	s := rig.Analyze(cell, results, begins, r.Doubles.Exits(), attempts, r.Restarts())
	for _, rs := range poisonRes {
		poison = append(poison, rig.PoisonCost(rs[0].Base, rs, begins, attempts, r.Doubles.Exits()))
	}
	s.Poison = poison
	writeJSONL(t, filepath.Join(dir, "attempts.jsonl"), results)
	writeJSONL(t, filepath.Join(dir, "layer.jsonl"), attempts)
	writeJSONL(t, filepath.Join(dir, "exits.jsonl"), r.Doubles.Exits())
	writeJSON(t, filepath.Join(dir, "engines.json"), begins)
	writeJSON(t, filepath.Join(dir, "summary.json"), s)
	return s
}

func writeJSONL[T any](t *testing.T, path string, xs []T) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, x := range xs {
		if err := enc.Encode(x); err != nil {
			t.Fatal(err)
		}
	}
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}
