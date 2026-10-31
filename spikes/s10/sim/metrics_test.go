package sim

import (
	"math"
	"testing"
)

// Throughput counts the output of requests that complete in the window,
// whenever they were dispatched. One Instance, prefill negligible, decode
// 1 s per output token, window [5, 20):
//
//	A: dispatched at 0,  7 tokens, completes at  7 -> completes in the window, dispatched before it
//	C: dispatched at 8,  3 tokens, completes at 11 -> dispatched and completes in the window
//	B: dispatched at 15, 11 tokens, completes at 26 -> dispatched in the window, completes after it
//
// Completed in the window: A + C = 10 tokens. Dispatched in it: C + B = 14.
func TestThroughputCountsCompletionsInTheWindow(t *testing.T) {
	cl := Cluster{Instances: 1, KVTokens: 1_000_000, BlockSize: 16, MaxRunning: 8, PrefillRate: 1e12, TPOT: 1}
	convs := []Conversation{
		conv(0, 0, 16, 16, 1, 7, 0, 0),
		conv(1, 8, 16, 16, 1, 3, 0, 0),
		conv(2, 15, 16, 16, 1, 11, 0, 0),
	}
	e := NewEngine(cl, Policy{Kind: RoundRobin}, noCompaction(), convs, nil)
	e.M.WinStart, e.M.WinEnd = 5, 20
	if err := e.Run(); err != nil {
		t.Fatal(err)
	}
	if e.M.DoneOutputTokens != 10 {
		t.Errorf("output completed in the window = %d, want 10 (A + C)", e.M.DoneOutputTokens)
	}
	if e.M.OutputTokens != 14 {
		t.Errorf("output of requests dispatched in the window = %d, want 14 (C + B)", e.M.OutputTokens)
	}
}

// Time above the engine limit is split at bucket and window boundaries.
// One Instance, engine limit (MaxRunning) 1, prefill negligible, decode 1 s
// per output token:
//
//	A: arrives at 50, 100 tokens: runs 50 -> 150
//	B: arrives at 55, 100 tokens: waits at the Instance, runs 150 -> 250
//
// From 55 to 150 two requests are in flight against a limit of 1, so the
// time above it is 95 s, in one stretch between two events. In 60 s buckets
// that is 5 s in bucket 0 (55-60), 60 s in bucket 1 (60-120) and 30 s in
// bucket 2 (120-150). Charging the stretch to the bucket of its start, 55,
// would put all 95 s in bucket 0.
//
// A window takes the part of the stretch inside it:
//
//	[100, 200): 100-150 = 50   (the stretch starts before the window)
//	[50, 100):  55-100  = 45   (the stretch runs past the window's end)
//	[70, 130):  70-130  = 60   (the stretch covers the window)
//	[0, inf):   55-150  = 95   (the default window)
func TestTimeAboveTheLimitIsSplitAtBucketAndWindowBoundaries(t *testing.T) {
	cl := Cluster{Instances: 1, KVTokens: 1_000_000, BlockSize: 16, MaxRunning: 1, PrefillRate: 1e12, TPOT: 1}
	run := func(winStart, winEnd float64) *Metrics {
		convs := []Conversation{conv(0, 50, 16, 16, 1, 100, 0, 0), conv(1, 55, 16, 16, 1, 100, 0, 0)}
		e := NewEngine(cl, Policy{Kind: RoundRobin}, noCompaction(), convs, nil)
		if winEnd > 0 {
			e.M.WinStart, e.M.WinEnd = winStart, winEnd
		}
		if err := e.Run(); err != nil {
			t.Fatal(err)
		}
		return e.M
	}
	m := run(0, 0)
	for _, b := range []struct{ from, to, want float64 }{{0, 60, 5}, {60, 120, 60}, {120, 180, 30}, {180, 300, 0}, {0, 300, 95}} {
		if got := m.AboveEngineBetween(b.from, b.to); math.Abs(got-b.want) > 1e-6 {
			t.Errorf("above the limit in buckets starting in [%.0f, %.0f) = %.4f s, want %.0f", b.from, b.to, got, b.want)
		}
	}
	if math.Abs(m.AboveEngine-95) > 1e-6 || math.Abs(m.AboveEngineInst-95) > 1e-6 {
		t.Errorf("whole run: %.4f s above the limit, %.4f Instance-seconds, want 95 and 95", m.AboveEngine, m.AboveEngineInst)
	}
	if math.Abs(m.AboveEngineWin-95) > 1e-6 {
		t.Errorf("default window: %.4f s above the limit, want 95", m.AboveEngineWin)
	}
	for _, w := range []struct{ start, end, want float64 }{{100, 200, 50}, {50, 100, 45}, {70, 130, 60}} {
		if got := run(w.start, w.end).AboveEngineWin; math.Abs(got-w.want) > 1e-6 {
			t.Errorf("window [%.0f, %.0f): %.4f s above the limit, want %.0f", w.start, w.end, got, w.want)
		}
	}
}
