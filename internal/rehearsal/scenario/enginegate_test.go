package scenario_test

// The engine gate: a fixture that holds an engine's start until the test
// releases its job (issue #193, review of PR #247). The no-overlap phase
// holds each successor at its engine's start until the required requests
// have been answered in its gap, so the gap's coverage never rests on how
// fast this host starts an engine.
//
// The jobs' HPCBURST_APPTAINER is a wrapper script. While the gate is armed
// (<dir>/armed exists and <dir>/open does not), "apptainer exec", the
// engine's start, writes <dir>/held-<job ID> and waits until
// <dir>/release-<job ID> or <dir>/open exists, then runs the engine double's
// apptainer as before; every other command (pull, build) passes at once.
// The wrapper replaces itself with that apptainer (exec), so the engine's
// processes are what they were without it.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type engineGate struct{ dir string }

// installEngineGate makes the gate's directory and its wrapper, which runs
// the apptainer in fakes, and returns the gate and the wrapper's path.
func installEngineGate(t *testing.T, dir, fakes string) (*engineGate, string) {
	t.Helper()
	g := &engineGate{dir: filepath.Join(dir, "eg")}
	bin := filepath.Join(fakes, "gate")
	for _, d := range []string{g.dir, bin} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []string{g.dir, fakes} {
		if strings.ContainsAny(p, "'\n") {
			t.Fatalf("the engine gate's path %q cannot be quoted in its wrapper", p)
		}
	}
	script := fmt.Sprintf(`#!/bin/sh
# Issue #193's engine gate, a test fixture: while the gate is armed, an
# engine's start waits until the test releases its job.
g='%s'
if [ "$1" = exec ] && [ -e "$g/armed" ] && [ ! -e "$g/open" ]; then
	: > "$g/held-$SLURM_JOB_ID"
	while [ ! -e "$g/release-$SLURM_JOB_ID" ] && [ ! -e "$g/open" ]; do sleep 0.1; done
fi
exec '%s' "$@"
`, g.dir, filepath.Join(fakes, "apptainer"))
	wrapper := filepath.Join(bin, "apptainer")
	if err := os.WriteFile(wrapper, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	// A failing test never leaves an engine waiting at the gate.
	t.Cleanup(func() { _ = g.open() })
	return g, wrapper
}

func (g *engineGate) touch(name string) error {
	return os.WriteFile(filepath.Join(g.dir, name), nil, 0o644)
}

// arm makes every later engine start wait at the gate.
func (g *engineGate) arm() error { return g.touch("armed") }

// open lets every engine start pass, a waiting one included.
func (g *engineGate) open() error { return g.touch("open") }

// release lets the job's waiting engine start.
func (g *engineGate) release(job int) error { return g.touch("release-" + strconv.Itoa(job)) }

// held reports when the job's engine start reached the gate, from its marker.
func (g *engineGate) held(job int) (time.Time, bool) {
	fi, err := os.Stat(filepath.Join(g.dir, "held-"+strconv.Itoa(job)))
	if err != nil {
		return time.Time{}, false
	}
	return fi.ModTime(), true
}

// heldJobs lists the jobs whose engine start has reached the gate.
func (g *engineGate) heldJobs() ([]int, error) {
	es, err := os.ReadDir(g.dir)
	if err != nil {
		return nil, err
	}
	var out []int
	for _, e := range es {
		if v, ok := strings.CutPrefix(e.Name(), "held-"); ok {
			id, err := strconv.Atoi(v)
			if err != nil {
				return nil, errors.New("the engine gate holds a marker for no job ID: " + e.Name())
			}
			out = append(out, id)
		}
	}
	return out, nil
}
