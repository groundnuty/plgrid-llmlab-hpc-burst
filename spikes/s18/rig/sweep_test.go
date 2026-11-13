package rig

import (
	"testing"
	"time"
)

// The sweep holds what Q5 accepted, every cell named once, and the groups hold every cell once.
func TestTheSweepIsQ5sAndItsGroupsCoverIt(t *testing.T) {
	cells := Sweep()
	names := map[string]bool{}
	byFault := map[string]int{}
	for _, c := range cells {
		if names[c.Name] {
			t.Errorf("cell %s named twice", c.Name)
		}
		names[c.Name] = true
		byFault[c.Fault]++
		if c.RPS != 5 || c.N != 3 {
			t.Errorf("%s: %v rps on %d Instances, want 5 on 3", c.Name, c.RPS, c.N)
		}
		if c.Fault == "F4" && c.Trials != 20 {
			t.Errorf("%s: %d trials, want 20", c.Name, c.Trials)
		}
		if c.Fault != "F4" && (c.Duration != 2*time.Minute || (c.Rate != 0.01 && c.Rate != 0.1)) {
			t.Errorf("%s: %s at %v, want 2 min at 1%% or 10%%", c.Name, c.Duration, c.Rate)
		}
	}
	// Per stream mode: F1-F3 each 2 rates x (a, b, 6 buckets of c) = 16; F3x2 2 x (a, b, 2 c, 2 d) = 12;
	// F4 2 client modes x 5 arms = 10.
	for f, want := range map[string]int{"F1": 32, "F2": 32, "F3": 32, "F3x2": 24, "F4": 20} {
		if byFault[f] != want {
			t.Errorf("%s: %d cells, want %d", f, byFault[f], want)
		}
	}
	seen := map[string]int{}
	for _, g := range Groups(cells, 16) {
		for _, c := range g {
			seen[c.Name]++
		}
	}
	for n := range names {
		if seen[n] != 1 {
			t.Errorf("cell %s in %d groups, want 1", n, seen[n])
		}
	}
}

// A rerun selects cells by name, in the sweep's order, and refuses a name not in the sweep; an F4
// cell's bound grows with its trials.
func TestSelectAndBudget(t *testing.T) {
	got, err := Select(Sweep(), []string{"c-F4-b10-r1-sdk", " a-F1-p1 "})
	if err != nil || len(got) != 2 || got[0].Name != "a-F1-p1" || got[1].Name != "c-F4-b10-r1-sdk" {
		t.Errorf("Select: %v, %v", got, err)
	}
	if _, err := Select(Sweep(), []string{"z-F9"}); err == nil {
		t.Error("Select took a name not in the sweep")
	}
	f4 := Cell{Fault: "F4", Trials: 20}
	if b := Budget(f4); b < 20*45*time.Second+5*time.Minute {
		t.Errorf("an F4 cell of 20 trials gets %s, want well above 20 x 45 s", b)
	}
}
