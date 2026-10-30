package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestBadArgumentsExitWithStatus2(t *testing.T) {
	for _, args := range [][]string{
		{"-seeds", "one"},
		{"-no-such-flag"},
		{"-s10b-results", "/nonexistent-dir-for-test"},
	} {
		var out, errb bytes.Buffer
		if got := run(args, &out, &errb); got != 2 {
			t.Errorf("run(%v) = %d, want 2 (stderr: %s)", args, got, strings.TrimSpace(errb.String()))
		}
	}
}

func TestHelpExitsWithStatus0(t *testing.T) {
	var out, errb bytes.Buffer
	if got := run([]string{"-h"}, &out, &errb); got != 0 {
		t.Errorf("run(-h) = %d, want 0", got)
	}
}

func TestOptionRowsPicksOneOption(t *testing.T) {
	summary := "regime,key,option,x\ntight,k,a,1\ntight,k,b,2\ndrain,k,b,3\n"
	got, err := optionRows(summary, "b")
	if err != nil || got != "regime,key,option,x\ntight,k,b,2\ndrain,k,b,3\n" {
		t.Errorf("optionRows = %q, %v", got, err)
	}
	if _, err := optionRows(summary, "c"); err == nil {
		t.Error("no error for an option with no rows")
	}
}

// A repeated seed is refused before anything runs (review finding B1: it
// weighted that seed twice for option 5 only). The check is on parseArgs, so
// it cannot be mistaken for the later failure of a missing results directory,
// and run() is given real paths and an empty output directory to show that
// it exits 2 with the reason and writes nothing.
func TestRepeatedSeedIsRefusedBeforeAnythingRuns(t *testing.T) {
	if _, err := parseArgs([]string{"-seeds", "1,2,3,2"}, io.Discard); err == nil || !strings.Contains(err.Error(), "duplicate seed 2") {
		t.Errorf("parseArgs(-seeds 1,2,3,2) error %v, want one naming the duplicate seed 2", err)
	}
	if cfg, err := parseArgs([]string{"-seeds", "3, 1,2"}, io.Discard); err != nil || len(cfg.seeds) != 3 {
		t.Errorf("parseArgs(-seeds '3, 1,2') = %v, %v; want three seeds and no error", cfg.seeds, err)
	}
	dir := t.TempDir()
	var out, errb bytes.Buffer
	got := run([]string{"-seeds", "1,2,3,2", "-out", dir, "-s10b-results", "../../../s10b/results"}, &out, &errb)
	if got != 2 || !strings.Contains(errb.String(), "duplicate seed 2") || out.Len() != 0 {
		t.Errorf("run = %d, stderr %q, stdout %q; want 2, the duplicate seed named, nothing on stdout", got, errb.String(), out.String())
	}
	if files, _ := os.ReadDir(dir); len(files) != 0 {
		t.Errorf("%d files written to the output directory, want none", len(files))
	}
}

// The regression report ends in exactly one newline whether or not there are
// differences, which git diff --check needs (it flagged the committed file
// for a blank last line, the case with none).
func TestRegressionReportEndsInOneNewline(t *testing.T) {
	for name, found := range map[string][]string{
		"no differences": nil,
		"one difference": {"tight c=2.0+cap48 header>opening seed 1 differs:\n  ours:   a\n  S10b:   b"},
		"two":            {"x", "y"},
	} {
		got := regressionReport(8, 1, found)
		if !strings.HasSuffix(got, "\n") || strings.HasSuffix(got, "\n\n") {
			t.Errorf("%s: %q does not end in exactly one newline", name, got)
		}
		if lines := strings.Split(strings.TrimSuffix(got, "\n"), "\n"); !strings.HasPrefix(lines[0], "8 re-run cells") {
			t.Errorf("%s: first line %q", name, lines[0])
		}
	}
	if got := regressionReport(8, 1, nil); strings.Count(got, "\n") != 1 || !strings.Contains(got, "0 differences or missing rows") {
		t.Errorf("no differences: %q, want the summary line and one newline", got)
	}
	committed, err := os.ReadFile("../../results/regression-vs-s10b.txt")
	if err != nil {
		t.Fatal(err)
	}
	if s := string(committed); !strings.HasSuffix(s, "\n") || strings.HasSuffix(s, "\n\n") {
		t.Errorf("results/regression-vs-s10b.txt does not end in exactly one newline: %q", s[max(0, len(s)-40):])
	}
}
