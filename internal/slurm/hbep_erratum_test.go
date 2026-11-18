package slurm

// HBEP-4 states the limit of editing qosByGpus under live jobs, in the words the science agent gave (review of #756, Astra 1):
// the controller recounts live Allocations under the new map, and Slurm still enforces each job's QOS at submission. The
// sentence and its history line are the contract this PR's code is built to, so a test holds them in the text.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHBEP4StatesWhatAQOSByGPUsEditDoesToLiveAllocations(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "hbep", "0004-provider-interface", "README.md"))
	if err != nil {
		t.Fatalf("read HBEP-4: %v", err) // a missing file fails; it never skips
	}
	text := string(raw)
	for _, want := range []string{
		"a profile edit of qosByGpus recounts live Allocations under the new map; Slurm still enforces each job's QOS at submission",
		"- 2026-11-18: erratum: a profile edit of `qosByGpus` recounts live Allocations under the new map",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("HBEP-4 lacks %q", want)
		}
	}
}
