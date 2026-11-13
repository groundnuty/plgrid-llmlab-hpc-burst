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

// Four trials on three Instances: the poison goes to Instance k mod 3, so the 4th returns to the 1st's
// Instance, within the front's 30 s pause of its failures unless the trial waits it out.
func TestEveryPoisonTrialFires(t *testing.T) {
	if testing.Short() {
		t.Skip("about two minutes: four trials, each waiting out the front's 30 s pause")
	}
	cell := rig.Cell{Name: "poison-control", Arm: "a", Fault: "F4", Trials: 4, RPS: 5, N: 3,
		D1: rig.SweepD1, D2: rig.SweepD2, TokenDelay: rig.SweepTokenGap, Seed: 1}
	s := runCell(t, cell, filepath.Join(shortDir(t), cell.Name))
	if s.Marked != 4 || s.Fired != 4 || len(s.Poison) != 4 {
		t.Errorf("%d poison requests, %d fired, %d trials; want 4 of 4 fired", s.Marked, s.Fired, len(s.Poison))
	}
	for _, p := range s.Poison {
		if p.Deaths != 1 || p.Final != 502 {
			t.Errorf("poison %s: %d deaths, final %d; want one death and HBEP-5's 502 under alpha", p.Base, p.Deaths, p.Final)
		}
	}
}
