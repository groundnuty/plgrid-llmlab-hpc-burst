package main

import (
	"fmt"
	"sort"
)

// The expected test sets come from the spike's README table of the eleven e2e tests and from
// the deviation it records (a bare apiserver has no garbage collector). They are written out
// here, not read from the e2e source, so a test that is dropped or renamed there is noticed.
var (
	suiteTests = []string{
		"TestDiscovery", "TestValidation", "TestReconcileAndScale", "TestRestartConvergence",
		"TestSelectors", "TestConflict", "TestWatch", "TestCascadeBackground",
		"TestCascadeForeground", "TestFinalizerHoldsDeletion", "TestFinalizerThroughCascade",
	}
	// cascadeTests need a garbage collector; a bare apiserver (the negative control) fails them.
	cascadeTests = []string{"TestCascadeBackground", "TestCascadeForeground", "TestFinalizerThroughCascade"}
	// phaseTests are driven by the harness and skip in a plain run.
	phaseTests = []string{"TestPhaseLoad", "TestPhasePostUpgrade"}
)

// expectation is the exact outcome a run of the e2e binary must have.
type expectation struct{ Pass, Fail, Skip []string }

func without(all, drop []string) []string {
	skip := map[string]bool{}
	for _, d := range drop {
		skip[d] = true
	}
	var out []string
	for _, a := range all {
		if !skip[a] {
			out = append(out, a)
		}
	}
	return out
}

// expectSuite: a plain run of the whole binary.
func expectSuite(negativeControl bool) expectation {
	if negativeControl {
		return expectation{Pass: without(suiteTests, cascadeTests), Fail: cascadeTests, Skip: phaseTests}
	}
	return expectation{Pass: suiteTests, Skip: phaseTests}
}

// expectLoad: the harness-driven load phase passes on every host.
func expectLoad() expectation { return expectation{Pass: []string{"TestPhaseLoad"}} }

// expectPostUpgrade: scale one Fleet and cascade-delete another; it needs garbage collection.
func expectPostUpgrade(negativeControl bool) expectation {
	if negativeControl {
		return expectation{Fail: []string{"TestPhasePostUpgrade"}}
	}
	return expectation{Pass: []string{"TestPhasePostUpgrade"}}
}

func diffSets(kind string, want, got []string) []string {
	w, g := map[string]bool{}, map[string]bool{}
	for _, x := range want {
		w[x] = true
	}
	for _, x := range got {
		g[x] = true
	}
	var out []string
	for _, x := range want {
		if !g[x] {
			out = append(out, fmt.Sprintf("%s: %s expected but not seen", kind, x))
		}
	}
	for _, x := range got {
		if !w[x] {
			out = append(out, fmt.Sprintf("%s: %s seen but not expected", kind, x))
		}
	}
	sort.Strings(out)
	return out
}

// check returns every way the run differs from the expectation; nil means it matches exactly.
// A run in which no test passed never matches, whatever the exit status.
func (e expectation) check(what string, got e2eResult) []string {
	var out []string
	if len(got.Passed) == 0 && len(e.Pass) > 0 {
		out = append(out, "no test passed")
	}
	out = append(out, diffSets("passed", e.Pass, got.Passed)...)
	out = append(out, diffSets("failed", e.Fail, got.Failed)...)
	out = append(out, diffSets("skipped", e.Skip, got.Skipped)...)
	if len(e.Fail) == 0 && got.Exit != 0 {
		out = append(out, fmt.Sprintf("exit status %d, expected 0", got.Exit))
	}
	if len(e.Fail) > 0 && got.Exit == 0 {
		out = append(out, "exit status 0, but failures were expected")
	}
	for i := range out {
		out[i] = what + ": " + out[i]
	}
	return out
}
