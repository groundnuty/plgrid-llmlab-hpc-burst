package livechecks

// The keeper row on plgrid-now, beta-athena-now-keeper (issue #521), through the script. Its NOT RUNs are two: a job past the row's start wait, as on
// every row, and a deployment whose max-model-len does not hold a stream long enough to span a controller restart's outcome at the engine's measured
// rate. The second runs with the real row's test in dry mode (internal/alpha, dry_betakeeper_test.go), so what the script prints is what the test wrote:
// exit 3, NOT RUN with the rate and the context and nothing else, never PASS or FAIL, a cleanup that leaves nothing, and no text to post, since the run
// killed no controller. The row runs in host-dialed mode alone, which the script refuses to start it without. The lines are written out here from the
// issue's criteria, not read from the script.

import (
	"path/filepath"
	"strings"
	"testing"
)

// hostDialed puts the world in host-dialed mode, which has no address: the keeper row runs in no other.
func hostDialed(w *world) {
	w.alphaSettings(func(m map[string]string) {
		m["LIVE_DIALHOME_MODE"] = "host-dialed"
		delete(m, "LIVE_DIALHOME_ADDR")
	})
}

func TestTheKeeperRowsNotRunsThroughTheScript(t *testing.T) {
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_ALPHA_TEST", alphaPkg)
	realDir := filepath.Join(repoRoot(t), alphaPkg)
	const row = "beta-athena-now-keeper"
	for _, c := range []struct{ fault, line string }{
		{"pend-past-bound", row + ": NOT RUN (a job did not start within 10 min of the deployment's creation, the row's start wait, and was cancelled by its tag; not a failure)"},
		{"window-too-short", row + ": NOT RUN (the deployment's max-model-len does not hold a stream long enough to span a controller restart at the engine's measured rate: at 40 tokens/s with max-model-len 2912 the stream that fits is 1m0s, under the window of 2m40s; the controller was not killed, and the run's jobs were cancelled by tag; not a failure)"},
	} {
		t.Run(c.fault, func(t *testing.T) {
			t.Parallel()
			w := newAlphaWorld(t)
			w.installDry(dryWrapper, real, realDir)
			hostDialed(w)
			res := w.run([]string{"STUB_DRY_FAULT=" + c.fault}, "--run", row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, row+": tests: 0 passed, 0 failed, 1 skipped", c.line, "cleanup: left behind 0")
			if strings.Contains(res.out, row+": FAIL") || strings.Contains(res.out, row+": PASS") {
				t.Errorf("a NOT RUN run was also reported as a pass or a failure:\n%s", res.out)
			}
			if m, _ := filepath.Glob(filepath.Join(w.out(), row+"-*", "posts.md")); len(m) != 0 {
				t.Errorf("a run that killed nothing left a text to post: %v", m)
			}
		})
	}
	// The control: a window at exactly the floor is no NOT RUN and no failure, through the same script.
	t.Run("window-at-the-floor", func(t *testing.T) {
		t.Parallel()
		w := newAlphaWorld(t)
		w.installDry(dryWrapper, real, realDir)
		hostDialed(w)
		res := w.run([]string{"STUB_DRY_FAULT=window-at-the-floor"}, "--run", row, "--yes")
		if res.code != 0 {
			t.Fatalf("exit %d, want 0:\n%s", res.code, res.out)
		}
		if strings.Contains(res.out, row+": NOT RUN (") || !strings.Contains(res.out, row+": PASS") {
			t.Errorf("a window at the floor did not read as a pass:\n%s", res.out)
		}
	})
}

// The keeper marker on another row is no NOT RUN: only the keeper row has a window to span. And a marker whose parentheses hold more than the rate and
// the context prints no more than the fixed words, never what it also carried.
func TestTheKeeperMarkerOnAnotherRowIsAFailureAndCarriesOnlyTheNumbers(t *testing.T) {
	for _, row := range []string{"alpha-athena-now-serve", "beta-athena-now-router", "tunnel-hour-athena"} {
		t.Run(row, func(t *testing.T) {
			w := newAlphaWorld(t)
			res := w.run([]string{"STUB_ALPHA_MODE=not-run-keeper"}, "--run", row, "--yes")
			if res.code != 1 {
				t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
			}
			if strings.Contains(res.out, row+": NOT RUN") {
				t.Errorf("a keeper marker on %s read as NOT RUN:\n%s", row, res.out)
			}
		})
	}
	w := newAlphaWorld(t)
	hostDialed(w)
	res := w.run([]string{"STUB_ALPHA_MODE=not-run-keeper-words"}, "--run", "beta-athena-now-keeper", "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	if strings.Contains(res.out, "host-x.example.org") {
		t.Errorf("the script printed what the marker carried beside the numbers:\n%s", res.out)
	}
	w.contains(res, "the rate and the context could not be read from the marker")
}

// The keeper row is refused in another dial-home mode, before the estimate and before any build: it exits 3 and names the setting.
func TestTheKeeperRowIsRefusedOutsideHostDialedMode(t *testing.T) {
	for _, mode := range []string{"direct", "ssh"} {
		t.Run(mode, func(t *testing.T) {
			w := newAlphaWorld(t)
			w.exactMode = true // the refusal is what is under test: the harness must not give the row the mode it needs
			w.alphaSettings(func(m map[string]string) {
				m["LIVE_DIALHOME_MODE"] = mode
				if mode == "ssh" {
					m["LIVE_DIALHOME_KEY"] = filepath.Join(w.dir, "dialhome-key")
				}
			})
			res := w.run(nil, "--run", "beta-athena-now-keeper", "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, "beta-athena-now-keeper runs in host-dialed mode alone (LIVE_DIALHOME_MODE=host-dialed)")
			if strings.Contains(res.out, "building the test binary") {
				t.Errorf("the refused row started to build:\n%s", res.out)
			}
		})
	}
}
