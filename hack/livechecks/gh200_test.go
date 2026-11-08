package livechecks

// The conformance run on Helios plgrid-gpu-gh200 under QOS now
// (conformance-helios-gh200). Its normal QOS starts no job for weeks, so the
// row's qos has to reach the test, which puts it on the test's Provider
// (internal/providerctl/conformance, TestQOSReachesTheProviderAndEveryDirectSubmit
// and TestConformanceRowsPassTheRequestCheck cover that half). Here: the script's
// half. The expected strings are written out from the row's definition in the
// issue (a Helios GH200 row, qos now, the one-job slot now), not read from the
// script's table; a dry run against the stubs never reaches a cluster.

import (
	"strings"
	"testing"
)

func TestGH200ConformanceDryRunPrintsItsShapeAndRunsNothing(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run(nil, "--run", "conformance-helios-gh200")
	if res.code != 4 {
		t.Fatalf("exit %d, want 4:\n%s", res.code, res.out)
	}
	w.contains(res, "conformance-helios-gh200: shape: partition plgrid-gpu-gh200, qos now, 1 GPU, 1 CPU, 1Gi, time limit 00:05:00, one job, no requeue, each job to start within 10 min",
		"queue estimate: a start in about", "not run: pass --yes")
	w.noSubmit()
	if w.stateFile("go-argv") != "" {
		t.Errorf("the test was built before --yes: %s", w.stateFile("go-argv"))
	}
	// The estimate asks for the shape the test submits: under qos now, on the
	// GH200 partition, with the memfs feature the partition's tmpDir needs.
	argv := lines(w.stateFile("sbatch-argv"))
	if len(argv) != 1 {
		t.Fatalf("want exactly one sbatch call (the estimate), got %v", argv)
	}
	for _, f := range []string{"--test-only", "-p plgrid-gpu-gh200", "-q now", "-N 1", "-c 1", "--mem=1024M", "--gres=gpu:1", "-t 00:05:00", "-C memfs", "--no-requeue", "-A SEEDACCT-gh"} {
		if !strings.Contains(argv[0], f) {
			t.Errorf("the estimate lacks %q: %s", f, argv[0])
		}
	}
	// The one-job slot was read before the estimate.
	ev := w.events()
	if slot, est := index(ev, "squeue %q|%P", 1), index(ev, "sbatch test-only", 1); slot < 0 || est < 0 || slot > est {
		t.Errorf("the slot was not read before the estimate (slot read at %d, estimate at %d):\n%s", slot, est, strings.Join(ev, "\n"))
	}
}

// Precondition (the science agent, 2 Nov 2026): the row starts only when squeue
// --me shows no job in QOS now, running or pending, whoever's it is. Any such
// job, in any partition, refuses the row with exit 3 and "skipped: the now slot
// is in use", before anything is built, estimated, submitted or cancelled. The
// queue rows are id|name|qos|partition|state.
func TestGH200ConformanceStartsOnlyWithNoJobInQOSNow(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []string
	}{
		{"a running job of the owner's", []string{"5000|someone-elses-job|now|plgrid-gpu-gh200|RUNNING"}},
		{"a pending job", []string{"5000|someone-elses-job|now|plgrid-gpu-gh200|PENDING"}},
		{"a pending job in another partition", []string{"5000|a-cpu-job|now|plgrid|PENDING"}},
		{"a pending job of an earlier run of this row", []string{"5000|hb-leftover-aaaaaaaaaaaa|now|plgrid-gpu-gh200|PENDING"}},
		{"two jobs", []string{"5000|a|now|plgrid-gpu-gh200|RUNNING", "5001|b|now|plgrid-gpu-gh200|PENDING"}},
		{"among jobs that are not in now", []string{"4999|elsewhere|normal|plgrid|RUNNING", "5000|x|now|plgrid|PENDING"}},
	} {
		for _, args := range [][]string{{"--run", "conformance-helios-gh200"}, {"--run", "conformance-helios-gh200", "--yes"}} {
			t.Run(tc.name+" "+strings.Join(args[2:], ""), func(t *testing.T) {
				w := newWorld(t)
				w.useStubGo()
				w.seedQueue(tc.rows...)
				before := w.stateFile("queue")
				res := w.run(nil, args...)
				if res.code != 3 {
					t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
				}
				w.contains(res, "conformance-helios-gh200: skipped: the now slot is in use", "the one-job slot (now) already holds")
				if strings.Contains(res.out, "queue estimate") || strings.Contains(res.out, "FAIL") {
					t.Errorf("an estimate or a failure was printed for a slot in use:\n%s", res.out)
				}
				// Nothing was touched: no sbatch of any kind, no cancel, no build, no test run, the queue as it was.
				if argv := w.stateFile("sbatch-argv"); argv != "" {
					t.Errorf("sbatch ran: %s", argv)
				}
				if c := w.stateFile("cancelled"); c != "" {
					t.Errorf("a job was cancelled: %s", c)
				}
				if g := w.stateFile("go-argv"); g != "" {
					t.Errorf("a build ran: %s", g)
				}
				if w.stateFile("test-env-TestConformance") != "" {
					t.Errorf("the test binary ran")
				}
				if after := w.stateFile("queue"); after != before {
					t.Errorf("the queue changed:\n%s\nbefore:\n%s", after, before)
				}
			})
		}
	}
	// The control: jobs that are not in QOS now, in any state, do not hold its slot.
	w := newWorld(t)
	w.useStubGo()
	w.seedQueue("4999|elsewhere|normal|plgrid|RUNNING", "5000|elsewhere-too|plgrid|plgrid-gpu-gh200|PENDING")
	res := w.run(nil, "--run", "conformance-helios-gh200")
	if res.code != 4 {
		t.Fatalf("with no job in now: exit %d, want 4 (shown, not run):\n%s", res.code, res.out)
	}
	w.contains(res, "conformance-helios-gh200: shape:", "queue estimate: a start in about", "not run: pass --yes")
}

// A slot that cannot be read is taken as in use.
func TestGH200ConformanceTakesAnUnreadableSlotAsInUse(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_SQUEUE_FAIL=1"}, "--run", "conformance-helios-gh200", "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, "the slot could not be read, so it is taken as busy")
	w.noSubmit()
}

// The qos reaches the test as HPCBURST_CONF_QOS for the GH200 row, and for no
// other row, also when the caller's shell sets it: a stray value must not move
// the plgrid or Athena runs to another qos.
func TestGH200ConformanceGivesTheTestItsQOSAndNoOtherRowDoes(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run(nil, "--run", "conformance-helios-gh200", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	w.contains(res, "conformance-helios-gh200: PASS", "cleanup: left behind 0 (cancelled 0)")
	if m := slotUse.FindStringSubmatch(res.out); m == nil || m[1] != "now" {
		t.Errorf("no slot-use line for QOS now:\n%s", res.out)
	}
	env := w.stateFile("test-env-TestConformance")
	for _, want := range []string{"HPCBURST_CONF_QOS=now", "HPCBURST_CONFORMANCE=1", "HPCBURST_CONF_PROFILE=helios", "HPCBURST_CONF_PARTITION=plgrid-gpu-gh200",
		"HPCBURST_CONF_GPUS=1", "HPCBURST_CONF_CPUS=1", "HPCBURST_CONF_MEMORY=1Gi", "HPCBURST_CONF_ACCOUNT=SEEDACCT-gh", "HPCBURST_CONF_START_WAIT=10m"} {
		if !strings.Contains(env, want+"\n") {
			t.Errorf("the test did not get %s:\n%s", want, env)
		}
	}

	for _, row := range []string{"conformance-helios"} {
		t.Run(row, func(t *testing.T) {
			w := newWorld(t)
			w.useStubGo()
			res := w.run([]string{"HPCBURST_CONF_QOS=now", "HPCBURST_CONF_START_WAIT=10m"}, "--run", row, "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			env := w.stateFile("test-env-TestConformance")
			if !strings.Contains(env, "HPCBURST_CONF_PROFILE=") || strings.Contains(env, "HPCBURST_CONF_QOS") || strings.Contains(env, "HPCBURST_CONF_START_WAIT") {
				t.Errorf("the test got the caller's HPCBURST_CONF_QOS or HPCBURST_CONF_START_WAIT, or no environment was recorded:\n%s", env)
			}
		})
	}
}

// Start-wait bound (the science agent, 2 Nov 2026): a job that does not start
// within the bound is cancelled by its tag by the test, which skips and leaves
// a marker; the row is then NOT RUN, exit 3, not a failure, with the cleanup
// run and the slot's use reported. The stub test binary stands for the test in
// each mode; the test's own behaviour is tested in internal/providerctl/conformance.
func TestGH200ConformanceThatStopsOnItsStartWaitIsNotRunNotFailed(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_TEST_MODE=not-run"}, "--run", "conformance-helios-gh200", "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, "conformance-helios-gh200: NOT RUN (a job did not start within 10 min of its acceptance and was cancelled by its tag; not a failure)",
		"cleanup: left behind 0 (cancelled 0)")
	if strings.Contains(res.out, "FAIL") || strings.Contains(res.out, "PASS") {
		t.Errorf("a NOT RUN is neither a pass nor a failure:\n%s", res.out)
	}
	if m := slotUse.FindStringSubmatch(res.out); m == nil || m[1] != "now" {
		t.Errorf("no slot-use line for QOS now:\n%s", res.out)
	}
	if !strings.Contains(w.stateFile("test-env-TestConformance"), "HPCBURST_CONF_START_WAIT=10m\n") {
		t.Errorf("the test did not get the bound")
	}
}

// A job the test left behind is still cancelled by the script's own cleanup, and the row is still NOT RUN.
func TestNotRunStillCleansUpWhatTheTestLeftBehind(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_TEST_MODE=not-run-leave"}, "--run", "conformance-helios-gh200", "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, "NOT RUN", "cleanup: left behind 0 (cancelled 1)")
	if q := w.stateFile("queue"); strings.Contains(q, "9001|") {
		t.Errorf("the job the test left was not cancelled:\n%s", q)
	}
}

// What is not a NOT RUN stays a failure: a failure beside the marker, a skipped subtest, a skip with no marker,
// and a marker from a test whose row has no bound.
func TestOnlyAMarkedCleanSkipOfARowWithABoundIsNotRun(t *testing.T) {
	for _, tc := range []struct {
		name, row, mode string
	}{
		{"a failure beside the marker", "conformance-helios-gh200", "not-run-fail"},
		{"a skipped subtest beside the marker", "conformance-helios-gh200", "not-run-sub"},
		{"a marker that is a link", "conformance-helios-gh200", "not-run-link"},
		{"a nonzero exit beside the marker", "conformance-helios-gh200", "not-run-rc"},
		{"a failed subtest beside the marker", "conformance-helios-gh200", "not-run-subfail"},
		{"a skip with no marker", "conformance-helios-gh200", "skip"},
		{"a marker from a row with no bound", "conformance-helios", "not-run"},
		{"a marker from another row with no bound", "gpu-accounting-athena", "not-run"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			w.useStubGo()
			res := w.run([]string{"STUB_TEST_MODE=" + tc.mode}, "--run", tc.row, "--yes")
			if res.code != 1 {
				t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
			}
			if strings.Contains(res.out, "NOT RUN") || !strings.Contains(res.out, tc.row+": FAIL") {
				t.Errorf("want a FAIL and no NOT RUN:\n%s", res.out)
			}
		})
	}
}
