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
	w.contains(res, "conformance-helios-gh200: shape: partition plgrid-gpu-gh200, qos now, 1 GPU, 1 CPU, 1Gi, time limit 00:05:00, one job, no requeue",
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

// A job of the user's in QOS now holds the slot, whatever partition it is in.
func TestGH200ConformanceDoesNotQueueBehindAJobInQOSNow(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	w.seedQueue("5000|someone-elses-job|plgrid-gpu-gh200|now|RUNNING")
	res := w.run(nil, "--run", "conformance-helios-gh200", "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, "the one-job slot (now) already holds 1 job(s) of yours; not queuing behind it")
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
		"HPCBURST_CONF_GPUS=1", "HPCBURST_CONF_CPUS=1", "HPCBURST_CONF_MEMORY=1Gi", "HPCBURST_CONF_ACCOUNT=SEEDACCT-gh"} {
		if !strings.Contains(env, want+"\n") {
			t.Errorf("the test did not get %s:\n%s", want, env)
		}
	}

	for _, row := range []string{"conformance-helios", "conformance-athena"} {
		t.Run(row, func(t *testing.T) {
			w := newWorld(t)
			w.useStubGo()
			res := w.run([]string{"HPCBURST_CONF_QOS=now"}, "--run", row, "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			if env := w.stateFile("test-env-TestConformance"); !strings.Contains(env, "HPCBURST_CONF_PROFILE=") || strings.Contains(env, "HPCBURST_CONF_QOS") {
				t.Errorf("the test got the caller's HPCBURST_CONF_QOS, or no environment was recorded:\n%s", env)
			}
		})
	}
}
