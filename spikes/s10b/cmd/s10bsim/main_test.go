package main

// The command's exit status when the regression check fails. The fixture is
// a hand-written stand-in for S10's committed results (one file, two rows),
// and the executor returns made-up cells instead of running the simulator, so
// the test is fast and its expected values do not come from the code under
// test. The check itself runs against S10's real results in the study tests.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10b/study"
)

const fixtureRows = `scenario,kv_tokens,seed,policy,key,requests,prompt_tokens,hit_tokens,done_output_tokens
steady,1500000,1,round-robin,,90,1800,800,250
steady,1500000,1,chwbl-1.25,header>opening,100,2000,1500,300
`

func cell(option, key string, seed uint64, req int, prompt, hit, done int64) study.Cell {
	return study.Cell{Regime: "tight", Key: key, Option: option, Seed: seed, Requests: req, PromptTokens: prompt, HitTokens: hit, DoneOutput: done}
}

type outcome struct {
	status             int
	stdout, stderr     string
	regression, cellsF string
	planned            int
}

func runWith(t *testing.T, cells []study.Cell) outcome {
	t.Helper()
	s10, out := t.TempDir(), filepath.Join(t.TempDir(), "results")
	if err := os.WriteFile(filepath.Join(s10, "steady.csv"), []byte(fixtureRows), 0o644); err != nil {
		t.Fatal(err)
	}
	var o outcome
	var so, se bytes.Buffer
	exec := func(runs []study.Run, par int) ([]study.Cell, error) {
		o.planned = len(runs)
		return cells, nil
	}
	o.status = run([]string{"-seeds", "1", "-regimes", "tight", "-keys", "header>opening", "-out", out, "-s10-results", s10}, &so, &se, exec)
	o.stdout, o.stderr = so.String(), se.String()
	if b, err := os.ReadFile(filepath.Join(out, "regression-vs-s10.txt")); err == nil {
		o.regression = string(b)
	}
	if b, err := os.ReadFile(filepath.Join(out, "cells.csv")); err == nil {
		o.cellsF = string(b)
	}
	return o
}

func TestExitStatusFollowsTheRegressionCheck(t *testing.T) {
	const k = "header>opening"
	good := []study.Cell{cell("round-robin", "-", 1, 90, 1800, 800, 250), cell("c=1.25", k, 1, 100, 2000, 1500, 300)}
	t.Run("rows equal S10's: exit 0", func(t *testing.T) {
		o := runWith(t, good)
		if o.status != 0 {
			t.Errorf("exit status %d, want 0; stderr: %s", o.status, o.stderr)
		}
		if !strings.Contains(o.regression, "2 cells compared") || !strings.Contains(o.regression, "0 mismatches or missing rows") {
			t.Errorf("report %q", o.regression)
		}
		if o.planned != 5 {
			t.Errorf("%d runs planned for 1 regime, 1 seed, 1 key, want 5 (round robin and four options)", o.planned)
		}
	})
	t.Run("a hit-token mismatch: exit 1, report and tables kept", func(t *testing.T) {
		bad := []study.Cell{good[0], cell("c=1.25", k, 1, 100, 2000, 1499, 300)}
		o := runWith(t, bad)
		if o.status != 1 {
			t.Errorf("exit status %d, want 1; stderr: %s", o.status, o.stderr)
		}
		if want := "tight c=1.25 header>opening seed 1: hit_tokens is 1499, S10 committed 1500"; !strings.Contains(o.regression, want) {
			t.Errorf("report lacks the diagnostic %q: %q", want, o.regression)
		}
		if !strings.Contains(o.regression, "1 mismatches or missing rows") || !strings.Contains(o.stderr, "regression-vs-s10.txt") {
			t.Errorf("report %q, stderr %q", o.regression, o.stderr)
		}
		if !strings.Contains(o.cellsF, "c=1.25") {
			t.Errorf("cells.csv was not written before the exit: %q", o.cellsF)
		}
	})
	t.Run("a row S10 does not have: exit 1", func(t *testing.T) {
		missing := []study.Cell{good[0], cell("c=1.25", k, 7, 100, 2000, 1500, 300)}
		o := runWith(t, missing)
		if o.status != 1 {
			t.Errorf("exit status %d, want 1; stderr: %s", o.status, o.stderr)
		}
		if want := "tight c=1.25 header>opening seed 7: no row in steady.csv"; !strings.Contains(o.regression, want) {
			t.Errorf("report lacks the diagnostic %q: %q", want, o.regression)
		}
	})
}

func TestBadArgumentsExitWithStatus2(t *testing.T) {
	var so, se bytes.Buffer
	exec := func([]study.Run, int) ([]study.Cell, error) { t.Fatal("ran with bad arguments"); return nil, nil }
	for _, args := range [][]string{{"-regimes", "nosuch"}, {"-seeds", "x"}, {"-nosuchflag"}} {
		if st := run(append(args, "-out", t.TempDir()), &so, &se, exec); st != 2 {
			t.Errorf("%v: exit status %d, want 2", args, st)
		}
	}
}
