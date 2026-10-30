package sim

import "testing"

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
