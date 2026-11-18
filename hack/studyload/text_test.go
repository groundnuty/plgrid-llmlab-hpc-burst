package main

import (
	"strings"
	"testing"
)

func TestVocabularyIsAFixedListOfDistinctSingleWords(t *testing.T) {
	seen := map[string]bool{}
	for _, w := range vocab {
		if w == "" || strings.Trim(w, "abcdefghijklmnopqrstuvwxyz") != "" || seen[w] {
			t.Fatalf("word %q is empty, not lowercase letters only, or repeated", w)
		}
		seen[w] = true
	}
	if len(seen) != 256 {
		t.Errorf("%d distinct words, want 256", len(seen))
	}
}

func TestWordsHaveExactlyTheCountAsked(t *testing.T) {
	for _, n := range []int{1, 2, 30, 400, 1000} {
		if got := len(strings.Fields(words(n, 7))); got != n {
			t.Errorf("words(%d) has %d words", n, got)
		}
	}
	if words(0, 7) != "" {
		t.Error("words(0) is not empty")
	}
}

func TestWordsFollowTheSeedAlone(t *testing.T) {
	if words(50, 1) != words(50, 1) {
		t.Error("the same seed gave two texts")
	}
	if words(50, 1) == words(50, 2) {
		t.Error("two seeds gave one text")
	}
	if seedOf("run-a-000", 1) == seedOf("run-a-001", 1) || seedOf("run-a-000", 1) == seedOf("run-a-000", 2) {
		t.Error("a session or a turn did not change the seed")
	}
}
