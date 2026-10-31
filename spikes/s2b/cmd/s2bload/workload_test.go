package main

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func testWorkload(run string) *Workload {
	return &Workload{Run: run, Seed: 7, Sessions: 50, Turns: 6, AgentWeights: []float64{0.5, 0.3, 0.2},
		SysWords: 300, TaskWords: 40, ToolMinWords: 20, ToolMaxWords: 60, ThinkMin: time.Second, ThinkMax: 2 * time.Second}
}

// The run label must change the very first line of every system prompt (so arms share no prefix)
// and nothing after it (so every arm gets the same workload).
func TestRunSaltsOnlyTheFirstLine(t *testing.T) {
	a, b := testWorkload("A1-rr"), testWorkload("A1-llmd")
	for k := 0; k < 3; k++ {
		pa, pb := a.SystemPrompt(k), b.SystemPrompt(k)
		if pa[:20] == pb[:20] {
			t.Fatalf("type %d: system prompts of two runs share their first 20 bytes", k)
		}
		ia, ib := strings.Index(pa, "\n"), strings.Index(pb, "\n")
		if pa[ia:] != pb[ib:] {
			t.Fatalf("type %d: system prompts differ after the first line", k)
		}
	}
}

// Sessions of one agent type share one system prompt; different types do not.
func TestSharedAndDistinctSystemPrompts(t *testing.T) {
	w := testWorkload("r")
	if w.SystemPrompt(0) != w.SystemPrompt(0) {
		t.Fatal("system prompt of a type is not stable")
	}
	if w.SystemPrompt(0) == w.SystemPrompt(1) || w.SystemPrompt(1) == w.SystemPrompt(2) {
		t.Fatal("two agent types share a system prompt")
	}
	if n := len(strings.Fields(w.SystemPrompt(0))); n < 300 {
		t.Fatalf("system prompt has %d words, want at least 300", n)
	}
}

func TestUserMessagesTaggedAndDeterministic(t *testing.T) {
	w, w2 := testWorkload("B1-x"), testWorkload("B1-x")
	for s := 0; s < 5; s++ {
		for tt := 0; tt < 6; tt++ {
			m := w.UserMessage(s, tt)
			want := "[s2b:B1-x:" + itoa(s) + ":" + itoa(tt) + "]"
			if !strings.HasSuffix(m, want) {
				t.Fatalf("message %d/%d does not end with %s", s, tt, want)
			}
			if m != w2.UserMessage(s, tt) {
				t.Fatal("same seed gave different messages")
			}
		}
	}
	if w.UserMessage(1, 1) == w.UserMessage(2, 1) {
		t.Fatal("two sessions got the same tool output")
	}
	n := len(strings.Fields(w.UserMessage(3, 2)))
	if n < 20+2 || n > 60+3 { // words plus "Tool result:" and the tag
		t.Fatalf("tool message has %d fields, outside the configured 20..60 words", n)
	}
}

func TestAgentPopularity(t *testing.T) {
	w := testWorkload("r")
	w.Sessions = 10000
	c := make([]int, 3)
	for s := 0; s < w.Sessions; s++ {
		c[w.AgentOf(s)]++
	}
	want := []float64{0.5, 0.3, 0.2}
	for k := range c {
		got := float64(c[k]) / 10000
		if got < want[k]-0.03 || got > want[k]+0.03 {
			t.Fatalf("agent %d share %.3f, want %.2f +- 0.03", k, got, want[k])
		}
	}
}

func TestThinkWithinBounds(t *testing.T) {
	w := testWorkload("r")
	for s := 0; s < 20; s++ {
		for tt := 0; tt < 5; tt++ {
			d := w.Think(s, tt)
			if d < time.Second || d >= 2*time.Second {
				t.Fatalf("think %v outside [1s, 2s)", d)
			}
		}
	}
}

func itoa(i int) string { return strconv.Itoa(i) }
