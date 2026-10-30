package sim

// Validation of the simulator itself. Every expected number below is worked
// out by hand in the comment above it, from the cache rules stated in
// cache.go (vLLM-style blocks, LRU, tail-first freeing, last prompt token
// always computed), never read back from a simulator run.

import (
	"testing"
)

// fastCluster is one Instance whose service time is negligible, so the
// order of requests is set only by arrival and think times.
func fastCluster(n, kvTokens, maxRunning int) Cluster {
	return Cluster{Instances: n, KVTokens: kvTokens, BlockSize: 16, MaxRunning: maxRunning, PrefillRate: 1e9, TPOT: 1e-6}
}

// conv builds a conversation: a unique system prompt of sys tokens, a first
// user message of user tokens, and turns with fixed output and append sizes.
func conv(id int, arrival float64, sys, user, turns, out, appendLen int, think float64) Conversation {
	c := Conversation{ID: id, Arrival: arrival,
		Sys:       []Seg{{ID: SegID(100, uint64(id)), Len: sys}},
		FirstUser: []Seg{{ID: SegID(101, uint64(id)), Len: user}}}
	for i := 0; i < turns; i++ {
		t := Turn{Out: out, Think: think}
		if i > 0 {
			t.Append = Seg{ID: SegID(102, uint64(id), uint64(i)), Len: appendLen}
		}
		c.Turns = append(c.Turns, t)
		c.Summaries = append(c.Summaries, Seg{ID: SegID(103, uint64(id), uint64(i)), Len: 100})
	}
	return c
}

func noCompaction() Workload { return Workload{CompactAt: 1 << 30} }

func runSim(t *testing.T, cl Cluster, pol Policy, convs []Conversation, events []Event) *Metrics {
	t.Helper()
	e := NewEngine(cl, pol, noCompaction(), convs, events)
	e.M.Trace = true
	if err := e.Run(); err != nil {
		t.Fatal(err)
	}
	return e.M
}

func hitsByTurn(m *Metrics, convID int) map[int][2]int {
	out := map[int][2]int{}
	for _, r := range m.Recs {
		if r.Conv == convID {
			out[r.Turn] = [2]int{r.Hit, r.Prompt}
		}
	}
	return out
}

// One Instance with a huge cache. System 1600, first user 160, output 160,
// append 160, block 16 (every size is a whole number of blocks).
//
//	turn 0: prompt 1760, nothing cached        -> hit 0
//	turn k: prompt 1760+320k; the previous prompt plus its output,
//	        1760+320(k-1)+160 = 1600+320k tokens, is cached -> hit 1600+320k
//
// So turn 1 hits 1920/2080, turn 2 2240/2400, turn 9 4480/4640 (96.6%).
func TestHugeCacheHitsAllButTheNewTokens(t *testing.T) {
	m := runSim(t, fastCluster(1, 10_000_000, 8), Policy{Kind: RoundRobin},
		[]Conversation{conv(1, 0, 1600, 160, 10, 160, 160, 1)}, nil)
	got := hitsByTurn(m, 1)
	if len(got) != 10 {
		t.Fatalf("completed %d turns, want 10", len(got))
	}
	if got[0] != [2]int{0, 1760} {
		t.Errorf("turn 0: hit/prompt = %v, want [0 1760]", got[0])
	}
	for k := 1; k < 10; k++ {
		want := [2]int{1600 + 320*k, 1760 + 320*k}
		if got[k] != want {
			t.Errorf("turn %d: hit/prompt = %v, want %v", k, got[k], want)
		}
	}
	if hr := float64(got[9][0]) / float64(got[9][1]); hr < 0.96 {
		t.Errorf("turn 9 hit rate %.3f, want above 0.96", hr)
	}
}

// Cache thrash. Capacity 64 blocks (1024 tokens). Three conversations A, B,
// C cycle strictly (one request at a time, arrivals 0, 1, 2, think 3 s).
// Each has a unique 480-token system prompt and a 16-token first message,
// 16 output and 16 append tokens: turn t's prompt is 496+32t tokens and
// reserves (496+32t+16)/16 = 32+2t blocks, at least half the cache.
// Between two turns of one conversation the two others reserve at least
// 64 blocks, the whole cache, and LRU evicts the oldest blocks first, which
// are that conversation's. So every request misses entirely: hit 0.
func TestThrashMisses(t *testing.T) {
	var convs []Conversation
	for i := 0; i < 3; i++ {
		convs = append(convs, conv(i, float64(i), 480, 16, 6, 16, 16, 3))
	}
	m := runSim(t, fastCluster(1, 64*16, 1), Policy{Kind: RoundRobin}, convs, nil)
	if len(m.Recs) != 18 {
		t.Fatalf("completed %d requests, want 18", len(m.Recs))
	}
	for _, r := range m.Recs {
		if r.Hit != 0 {
			t.Errorf("conv %d turn %d hit %d tokens, want 0 (thrash)", r.Conv, r.Turn, r.Hit)
		}
	}
}

// Control for the thrash case: the same workload with a cache large enough
// for all three. Turn t>0 hits everything but its 16 new tokens:
// hit = (496+32t) - 16.
func TestThrashControlHits(t *testing.T) {
	var convs []Conversation
	for i := 0; i < 3; i++ {
		convs = append(convs, conv(i, float64(i), 480, 16, 6, 16, 16, 3))
	}
	m := runSim(t, fastCluster(1, 1024*16, 1), Policy{Kind: RoundRobin}, convs, nil)
	for _, r := range m.Recs {
		want := 0
		if r.Turn > 0 {
			want = 496 + 32*r.Turn - 16
		}
		if r.Hit != want {
			t.Errorf("conv %d turn %d hit %d, want %d", r.Conv, r.Turn, r.Hit, want)
		}
	}
}

// Two conversations share a 1600-token system prompt (100 blocks). The
// second, arriving after the first finished its first turn, hits exactly
// those 1600 tokens on its first turn; its own first message is new.
func TestSharedSystemPromptHits(t *testing.T) {
	a := conv(1, 0, 1600, 160, 1, 160, 0, 1)
	b := conv(2, 5, 1600, 160, 1, 160, 0, 1)
	b.Sys = a.Sys
	m := runSim(t, fastCluster(1, 10_000_000, 8), Policy{Kind: RoundRobin}, []Conversation{a, b}, nil)
	if got := hitsByTurn(m, 2)[0]; got != [2]int{1600, 1760} {
		t.Errorf("second conversation turn 0 hit/prompt = %v, want [1600 1760]", got)
	}
}

// Round robin on two Instances with huge caches: one conversation alternates
// Instances, so turn k finds only the context it left there at turn k-2.
//
//	turn 1 (Instance 1, empty)      -> hit 0
//	turn k>=2: context of turn k-2 plus its output, 1600+320(k-1)-320 = 1280+320k
//
// Affinity keeps it on one Instance: turn k hits 1600+320k (as above).
func TestRoundRobinLosesContextAffinityKeeps(t *testing.T) {
	c := []Conversation{conv(1, 0, 1600, 160, 6, 160, 160, 1)}
	rr := hitsByTurn(runSim(t, fastCluster(2, 10_000_000, 8), Policy{Kind: RoundRobin}, c, nil), 1)
	if rr[1][0] != 0 {
		t.Errorf("round robin turn 1 hit %d, want 0", rr[1][0])
	}
	for k := 2; k < 6; k++ {
		if want := 1280 + 320*k; rr[k][0] != want {
			t.Errorf("round robin turn %d hit %d, want %d", k, rr[k][0], want)
		}
	}
	aff := hitsByTurn(runSim(t, fastCluster(2, 10_000_000, 8), Policy{Kind: Affinity, Key: KeyOpening}, c, nil), 1)
	for k := 1; k < 6; k++ {
		if want := 1600 + 320*k; aff[k][0] != want {
			t.Errorf("affinity turn %d hit %d, want %d", k, aff[k][0], want)
		}
	}
}

// Tail-first eviction. Capacity 8 blocks (128 tokens), block 16. A has a
// 64-token system prompt and a 16-token first message (prompt 80 = 5
// blocks) and 16 output tokens: it reserves 6 blocks, a1..a6 once full.
// Freed tail first, the LRU order is a6, a5, a4, a3, a2, a1. B (same shape,
// unique content) then needs 6 blocks: 2 are empty, so it evicts 4 from the
// LRU front, a6..a3, and a1, a2 survive. A's turn 2 (prompt 112) therefore
// hits exactly 2 blocks = 32 tokens. Freeing head first would have evicted
// a1..a4 and given 0.
func TestEvictionTakesTheTailFirst(t *testing.T) {
	a := conv(1, 0, 64, 16, 2, 16, 16, 2)
	b := conv(2, 1, 64, 16, 1, 16, 0, 1)
	m := runSim(t, fastCluster(1, 8*16, 1), Policy{Kind: RoundRobin}, []Conversation{a, b}, nil)
	if got := hitsByTurn(m, 1)[1]; got != [2]int{32, 112} {
		t.Errorf("A turn 1 hit/prompt = %v, want [32 112]", got)
	}
}
