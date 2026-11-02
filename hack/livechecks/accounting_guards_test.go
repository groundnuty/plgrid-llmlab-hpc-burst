package livechecks

// The two guards of gpu-accounting-helios (issue #212, the science agent's rule of 2 Nov 2026): it runs under QOS
// now on GH200, so, as conformance-helios-gh200 does, (1) it starts only when squeue --me shows no job in QOS now,
// and (2) its job must start within 10 minutes of its acceptance or be cancelled by its tag and reported NOT RUN.
// The script's half is here; the test's half (the bound, the cancel by tag, no record) is in
// internal/providerctl/conformance (TestTheAccountingRowIsNotRunWhenTheOwnersJobHoldsTheSlot), and both meet in the
// dry run through the script (alpha_dry_test.go, the slot-held variant). Also here: the cleanup that matches the
// owned tags on every poll (PR 210's fix) works with the tag files of the alpha and accounting rows.

import (
	"strings"
	"testing"
)

func TestTheAccountingRowOnHeliosStartsOnlyWithNoJobInQOSNow(t *testing.T) {
	const row = "gpu-accounting-helios"
	for _, tc := range []struct {
		name string
		rows []string
	}{
		{"a running job of the owner's", []string{"5000|someone-elses-job|now|plgrid-gpu-gh200|RUNNING"}},
		{"a pending job", []string{"5000|someone-elses-job|now|plgrid-gpu-gh200|PENDING"}},
		{"a pending job in another partition", []string{"5000|a-cpu-job|now|plgrid|PENDING"}},
		{"a pending job of an earlier run", []string{"5000|hb-leftover-aaaaaaaaaaaa|now|plgrid-gpu-gh200|PENDING"}},
		{"two jobs", []string{"5000|a|now|plgrid-gpu-gh200|RUNNING", "5001|b|now|plgrid-gpu-gh200|PENDING"}},
	} {
		for _, args := range [][]string{{"--run", row}, {"--run", row, "--yes"}} {
			t.Run(tc.name+" "+strings.Join(args[2:], ""), func(t *testing.T) {
				w := newAlphaWorld(t)
				w.seedQueue(tc.rows...)
				before := w.stateFile("queue")
				res := w.run(nil, args...)
				if res.code != 3 {
					t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
				}
				w.contains(res, row+": skipped: the now slot is in use", "the one-job slot (now) already holds")
				if strings.Contains(res.out, "queue estimate") || strings.Contains(res.out, "FAIL") {
					t.Errorf("an estimate or a failure was printed for a slot in use:\n%s", res.out)
				}
				// Nothing was touched.
				for name, got := range map[string]string{"sbatch": w.stateFile("sbatch-argv"), "scancel": w.stateFile("cancelled"), "build": w.stateFile("go-argv"), "test run": w.stateFile("test-env-TestGPUAccounting")} {
					if got != "" {
						t.Errorf("%s happened: %s", name, got)
					}
				}
				if after := w.stateFile("queue"); after != before {
					t.Errorf("the queue changed:\n%s\nbefore:\n%s", after, before)
				}
			})
		}
	}
	// The control: jobs outside QOS now, in any state, do not hold the slot.
	w := newAlphaWorld(t)
	w.seedQueue("4999|elsewhere|normal|plgrid|RUNNING", "5000|elsewhere-too|plgrid|plgrid-gpu-gh200|PENDING")
	if res := w.run(nil, "--run", row); res.code != 4 {
		t.Fatalf("with no job in now: exit %d, want 4 (shown, not run):\n%s", res.code, res.out)
	}
	// An unreadable slot is a busy one.
	w = newAlphaWorld(t)
	res := w.run([]string{"STUB_SQUEUE_FAIL=1"}, "--run", row, "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, "the slot could not be read, so it is taken as busy")
	// The Athena row has no slot and no guard: a busy queue does not stop it.
	w = newAlphaWorld(t)
	w.seedQueue("5000|someone-elses-job|now|plgrid-gpu-gh200|RUNNING")
	if res := w.run(nil, "--run", "gpu-accounting-athena"); res.code != 4 {
		t.Fatalf("athena: exit %d, want 4:\n%s", res.code, res.out)
	}
}

// The NOT RUN protocol for the Helios accounting row: its test skipped once at the top level, wrote the marker, and
// exited 0; the script reports NOT RUN apart from PASS and FAIL and exits 3, after the same cleanup. A row with no
// start wait (the Athena twin, the stack rows) does not take the protocol: the same skip is a failure there.
func TestTheAccountingRowOnHeliosCanBeNotRunAndNoOtherAccountingRowCan(t *testing.T) {
	t.Run("gpu-accounting-helios", func(t *testing.T) {
		w := newAlphaWorld(t)
		res := w.run([]string{"STUB_ALPHA_MODE=not-run"}, "--run", "gpu-accounting-helios", "--yes")
		if res.code != 3 {
			t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
		}
		w.contains(res, "gpu-accounting-helios: NOT RUN (a job did not start within 10 min of its acceptance and was cancelled by its tag; not a failure)", "cleanup: left behind 0")
		if strings.Contains(res.out, "gpu-accounting-helios: FAIL") || strings.Contains(res.out, "gpu-accounting-helios: PASS") {
			t.Errorf("a NOT RUN run was also a failure or a pass:\n%s", res.out)
		}
	})
	for _, row := range []string{"gpu-accounting-athena", "alpha-athena-handover", "alpha-helios-nooverlap", "tunnel-hour-athena", "tunnel-hour-helios"} {
		t.Run(row, func(t *testing.T) {
			w := newAlphaWorld(t)
			res := w.run([]string{"STUB_ALPHA_MODE=not-run"}, "--run", row, "--yes")
			if res.code != 1 {
				t.Fatalf("exit %d, want 1: a skip is a failure for a row with no start wait:\n%s", res.code, res.out)
			}
			w.contains(res, row+": FAIL")
			if strings.Contains(res.out, "NOT RUN") {
				t.Errorf("a row with no start wait was reported NOT RUN:\n%s", res.out)
			}
		})
	}
}

// PR 210's cleanup matches the run's owned tags on every poll. The alpha and accounting rows write their tags
// (job-tags.txt, one per line, before the submission) in the format it reads, so a job that is in the queue but
// not yet shown at the first snapshot is found by a later poll and cancelled, on every one of the six rows.
func TestCleanupCancelsALateJobOnEveryAlphaRow(t *testing.T) {
	for _, s := range alphaShapes {
		t.Run(s.row, func(t *testing.T) {
			w := newAlphaWorld(t)
			res := w.run(quick("STUB_ALPHA_MODE=late-hidden", "STUB_HIDE_READS=1"), "--run", s.row, "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d, want 0 (the test passed; the cleanup cancelled a job it owns):\n%s", res.code, res.out)
			}
			w.contains(res, "cleanup: left behind 0 (cancelled 1)")
			if q := w.stateFile("queue"); strings.Contains(q, "9003|") {
				t.Errorf("the late job is still in the queue after a cleanup that said it left 0 behind:\n%s", q)
			}
		})
	}
}
