package sim

// The generator must produce what the issue asks for: growing prefixes,
// distinct openings, a share of shared system prompts, short and long
// sessions, and a header sent by some clients only. Expected values come
// from DefaultWorkload's stated parameters.

import (
	"math"
	"reflect"
	"testing"
)

func genDefault(t *testing.T, seed uint64) []Conversation {
	t.Helper()
	convs, err := DefaultWorkload(seed).Generate()
	if err != nil {
		t.Fatal(err)
	}
	return convs
}

func TestGeneratorIsSeeded(t *testing.T) {
	a, b, c := genDefault(t, 1), genDefault(t, 1), genDefault(t, 2)
	if !reflect.DeepEqual(a, b) {
		t.Error("same seed gave different workloads")
	}
	if reflect.DeepEqual(a, c) {
		t.Error("different seeds gave the same workload")
	}
}

// Rate 0.45/s for 4800 s: about 2160 conversations (Poisson, +-5%).
func TestGeneratorShape(t *testing.T) {
	convs := genDefault(t, 1)
	if n := float64(len(convs)); math.Abs(n-2160)/2160 > 0.05 {
		t.Errorf("%d conversations, want about 2160", len(convs))
	}
	openings := map[string]bool{}
	byClient := make([]int, 4)
	var header, short int
	for _, c := range convs {
		openings[SegsKey(c.Sys, c.FirstUser)] = true
		byClient[c.Client]++
		if c.Header {
			header++
		}
		if len(c.Turns) <= 6 {
			short++
		}
		for i, tr := range c.Turns {
			if (i == 0) != (tr.Append.Len == 0) {
				t.Fatalf("conv %d turn %d: append %d (only turn 0 appends nothing)", c.ID, i, tr.Append.Len)
			}
		}
	}
	if len(openings) != len(convs) {
		t.Errorf("%d distinct openings for %d conversations; the hash key could not spread them", len(openings), len(convs))
	}
	n := float64(len(convs))
	// Shares 0.35, 0.25, 0.15, 0.25; shared system prompts = first three = 0.75.
	for i, want := range []float64{0.35, 0.25, 0.15, 0.25} {
		if got := float64(byClient[i]) / n; math.Abs(got-want) > 0.03 {
			t.Errorf("client %d share %.3f, want %.2f", i, got, want)
		}
	}
	if got := float64(header) / n; math.Abs(got-0.35) > 0.03 {
		t.Errorf("header share %.3f, want 0.35 (agent-a only)", got)
	}
	if got := float64(short) / n; math.Abs(got-0.6) > 0.03 {
		t.Errorf("short-session share %.3f, want 0.60", got)
	}
}

// KubeAI's 100-character key sees only the start of the first user message.
// agent-a and agent-b put a fixed preamble there (400 and 150 tokens, more
// than 25), so each type collapses to one key; agent-c and scripts start
// with a unique task. Distinct keys = 2 + count(agent-c) + count(script).
func TestKubeAIKeyCollapsesPreambleClients(t *testing.T) {
	convs := genDefault(t, 3)
	keys := map[string]bool{}
	unique := 0
	for _, c := range convs {
		keys[PrefixKey(c.FirstUser, KubeAI100cTokens)] = true
		if c.Client >= 2 {
			unique++
		}
	}
	if len(keys) != 2+unique {
		t.Errorf("%d distinct kubeai-100c keys, want %d", len(keys), 2+unique)
	}
}

// A prompt's blocks are a prefix of the next prompt's blocks: the context
// grows by appending, never by rewriting (outside compaction).
func TestContextGrowsByAppending(t *testing.T) {
	c := NewContext(16)
	c.Append(Seg{ID: 1, Len: 1000})
	before := append([]uint64(nil), c.Blocks...)
	c.Append(Seg{ID: 2, Len: 37})
	if !reflect.DeepEqual(c.Blocks[:len(before)], before) {
		t.Fatal("appending changed earlier blocks")
	}
	if c.Len != 1037 || len(c.Blocks) != 1037/16 {
		t.Fatalf("len %d blocks %d, want 1037 and %d", c.Len, len(c.Blocks), 1037/16)
	}
	d := NewContext(16)
	d.Append(Seg{ID: 1, Len: 1000})
	d.Append(Seg{ID: 3, Len: 37})
	// 1000 = 62 full blocks + 8 tokens; block 62 mixes segment 1 and the
	// appended segment, so the two contexts share exactly 62 blocks.
	if c.Blocks[61] != d.Blocks[61] || c.Blocks[62] == d.Blocks[62] {
		t.Fatal("shared prefix should end at block 62")
	}
}
