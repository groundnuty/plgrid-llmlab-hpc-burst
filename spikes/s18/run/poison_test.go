package run

// A validity control of the F4 cells: every poison trial fires, under arm (a) with no client retry. A
// trial that starts while an Instance is still out (restarting, or paused by the front for its
// failures) goes elsewhere or is refused, and measures nothing; the cell settles each trial on the
// front's own pauses as well as the rig's restarts, so none is lost.

import (
	"path/filepath"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s18/rig"
)

func TestEveryPoisonTrialFires(t *testing.T) {
	if testing.Short() {
		t.Skip("about a minute and a half: three trials, each waiting out the front's 30 s pause")
	}
	cell := rig.Cell{Name: "poison-control", Arm: "a", Fault: "F4", Trials: 3, RPS: 5, N: 3,
		D1: rig.SweepD1, D2: rig.SweepD2, TokenDelay: rig.SweepTokenGap, Seed: 1}
	s := runCell(t, cell, filepath.Join(shortDir(t), cell.Name))
	if s.Marked != 3 || s.Fired != 3 || len(s.Poison) != 3 {
		t.Errorf("%d poison requests, %d fired, %d trials; want 3 of 3 fired", s.Marked, s.Fired, len(s.Poison))
	}
	for _, p := range s.Poison {
		if p.Deaths != 1 || p.Final != 502 {
			t.Errorf("poison %s: %d deaths, final %d; want one death and HBEP-5's 502 under alpha", p.Base, p.Deaths, p.Final)
		}
	}
}
