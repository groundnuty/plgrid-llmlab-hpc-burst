package livechecks

// The reach rows of hack/live-checks.sh (issue #225): reach-helios and reach-athena, with the alias reach. Each runs
// one minimal job on a compute node that tries a TCP connect to the gateway's forwarded port. The script's side is
// tested against the stub cluster, as the other rows are (this file); the row's own test is run for real in dry mode,
// against sockets (reach_dry_test.go).
//
// The gateway's host and port are private settings: they must reach the test and nothing else. The stub test binary
// prints them in its log, as the stub cluster prints its seeds, so that a script that echoed a log would be caught.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

const (
	reachSeedHost  = "reach-seed.example"
	reachSeedPort  = "40321"
	reachSeedLabel = "example-gateway"
)

func init() {
	leakSeeds = append(leakSeeds, reachSeedHost, reachSeedPort)
}

// reachShape is what a reach row asks of the scheduler, worked out from the issue (#225: the smallest shape the
// request check accepts, 2 minutes, a start wait of 10) and the platforms (the partitions and the QOS now of
// Helios's GH200 nodes and Athena's one-job partition plgrid-now); not read from the script's table.
type reachShape struct {
	row, host, shape, slot, account string
}

var reachShapes = []reachShape{
	{"reach-helios", "helios", "partition plgrid-gpu-gh200, qos now, 1 GPU, 1 CPU, 1Gi, time limit 00:02:00, one job, no requeue, each job to start within 10 min", "now", "SEEDACCT-gh"},
	{"reach-athena", "athena", "partition plgrid-now, 1 GPU, 1 CPU, 1Gi, time limit 00:02:00, one job, no requeue, each job to start within 10 min", "plgrid-now", "SEEDACCT-a100"},
}

// reachSettings adds the gateway's forwarded port to the world's settings file.
func (w *world) reachSettings(mutate func(m map[string]string)) {
	w.t.Helper()
	w.alphaSettings(func(m map[string]string) {
		m["LIVE_PROBE_HOST"] = reachSeedHost
		m["LIVE_PROBE_PORT"] = reachSeedPort
		m["LIVE_PROBE_LABEL"] = reachSeedLabel
		if mutate != nil {
			mutate(m)
		}
	})
}

func newReachWorld(t *testing.T) *world {
	t.Helper()
	w := newAlphaWorld(t)
	w.reachSettings(nil)
	return w
}

func TestListNamesTheReachRowsAndTheAlias(t *testing.T) {
	w := newWorld(t)
	res := w.run(nil, "--list")
	if res.code != 0 {
		t.Fatalf("exit %d", res.code)
	}
	for _, s := range reachShapes {
		w.contains(res, s.row+"  cluster  ready  gate HPCBURST_CONFORMANCE")
	}
	w.contains(res, "reach  alias  runs reach-athena in turn")
}

// The script's shapes and the package's reach rows are one fact each; the expected values here are #225's.
func TestTheScriptsReachShapesAreThePackagesRows(t *testing.T) {
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	shapes := map[string][]string{}
	for _, l := range block(t, string(b), "CLUSTER SHAPES") {
		f := strings.Split(l, "|")
		shapes[f[0]] = f
	}
	rows := alpha.ReachRows()
	if len(rows) != len(reachShapes) {
		t.Fatalf("the package has %d reach rows, the test table %d", len(rows), len(reachShapes))
	}
	for _, r := range rows {
		f, ok := shapes[r.Name]
		if !ok {
			t.Errorf("%s: no cluster shape in the script", r.Name)
			continue
		}
		qos := r.QOS
		if qos == "" {
			qos = "-"
		}
		// name | host | profile | partition | qos | slot | account key | GPUs | CPUs | memory | time limit | timeout | extra | start wait
		if f[1] != r.Cluster || f[2] != r.Cluster || f[3] != r.Partition || f[4] != qos || f[7] != "1" || f[8] != "1" || f[9] != "1Gi" || f[10] != "00:02:00" || len(f) != 14 || f[13] != "10" {
			t.Errorf("%s: shape %v differs from #225's (the partition and QOS of the package's row %+v; 1 GPU, 1 CPU, 1Gi, 2 min, a start wait of 10)", r.Name, f, r)
		}
		if r.Lease != 2*time.Minute || r.StartWait != 10*time.Minute || r.MostJobs != 1 {
			t.Errorf("%s: the package's row %+v is not one job of 2 minutes with a start wait of 10", r.Name, r)
		}
		if f[11] != fmt.Sprint(int(r.Budget/time.Minute)) {
			t.Errorf("%s: the shape's timeout is %s minutes, the package's budget %s", r.Name, f[11], r.Budget)
		}
	}
	// The Helios row asks for the constraint the Helios profile asks of every job; the Athena row has none.
	if shapes["reach-helios"][12] != "-C memfs" || shapes["reach-athena"][12] != "-" {
		t.Errorf("extra options: Helios %q, Athena %q, want -C memfs and -", shapes["reach-helios"][12], shapes["reach-athena"][12])
	}
}

func TestEveryReachRowRunsOnlyWithYesAndPrintsItsShapeAndEstimate(t *testing.T) {
	for _, s := range reachShapes {
		t.Run(s.row, func(t *testing.T) {
			w := newReachWorld(t)
			res := w.run(nil, "--run", s.row)
			if res.code != 4 {
				t.Fatalf("exit %d, want 4:\n%s", res.code, res.out)
			}
			w.contains(res, s.row+": shape: "+s.shape, "queue estimate: a start in about", "not run: pass --yes")
			if g := w.stateFile("go-argv"); g != "" {
				t.Errorf("something was built before --yes: %s", g)
			}
			argv := lines(w.stateFile("sbatch-argv"))
			if len(argv) != 1 || !strings.Contains(argv[0], "--test-only") || !strings.Contains(argv[0], "-A "+s.account) {
				t.Errorf("want exactly the estimate, for the row's account; got %v", argv)
			}
			if !strings.Contains(argv[0], "--no-requeue") || !strings.Contains(argv[0], "-t 00:02:00") {
				t.Errorf("the estimate is not for the row's job (no requeue, 2 minutes): %s", argv[0])
			}
			if _, err := os.Stat(filepath.Join(w.state, "test-env-TestReach")); err == nil {
				t.Error("the test binary ran without --yes")
			}
		})
	}
}

// The estimate asks for the row's own shape: the memfs constraint on Helios, and nothing of the other row's.
func TestTheReachEstimatesAskForTheirOwnShapes(t *testing.T) {
	for _, c := range []struct{ row, want, notWant string }{
		{"reach-helios", "-p plgrid-gpu-gh200 -q now -N 1 --ntasks=1 -c 1 --mem=1024M --gres=gpu:1 -t 00:02:00 -C memfs --no-requeue", "plgrid-now"},
		{"reach-athena", "-p plgrid-now -N 1 --ntasks=1 -c 1 --mem=1024M --gres=gpu:1 -t 00:02:00 --no-requeue", "-q "},
	} {
		t.Run(c.row, func(t *testing.T) {
			w := newReachWorld(t)
			w.run(nil, "--run", c.row)
			argv := w.stateFile("sbatch-argv")
			if !strings.Contains(argv, c.want) || strings.Contains(argv, c.notWant) {
				t.Errorf("the estimate's options are\n%s\nwant them to hold %q and not %q", argv, c.want, c.notWant)
			}
		})
	}
}

// A reach row refuses to run, with exit 3 and the setting's name, when a setting is missing or unusable. The
// message never holds a value, a refused run builds and calls nothing, and every problem is reported at once.
func TestReachRowsRefuseWithoutTheirSettings(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]string)
		want   []string
	}{
		{"no host", func(m map[string]string) { delete(m, "LIVE_PROBE_HOST") }, []string{"setting LIVE_PROBE_HOST is missing"}},
		{"no port", func(m map[string]string) { delete(m, "LIVE_PROBE_PORT") }, []string{"setting LIVE_PROBE_PORT is missing"}},
		{"no label", func(m map[string]string) { delete(m, "LIVE_PROBE_LABEL") }, []string{"setting LIVE_PROBE_LABEL is missing"}},
		{"none of the three", func(m map[string]string) {
			delete(m, "LIVE_PROBE_HOST")
			delete(m, "LIVE_PROBE_PORT")
			delete(m, "LIVE_PROBE_LABEL")
		}, []string{"setting LIVE_PROBE_HOST is missing", "setting LIVE_PROBE_PORT is missing", "setting LIVE_PROBE_LABEL is missing"}},
		{"a host with a quote", func(m map[string]string) { m["LIVE_PROBE_HOST"] = "x';touch${IFS}y;'" }, []string{"LIVE_PROBE_HOST must be a host name or an address"}},
		{"a host with a space", func(m map[string]string) { m["LIVE_PROBE_HOST"] = "reach seed" }, []string{"LIVE_PROBE_HOST must be a host name or an address"}},
		{"a host that starts with a dash", func(m map[string]string) { m["LIVE_PROBE_HOST"] = "-reach-seed.example" }, []string{"LIVE_PROBE_HOST must be a host name or an address"}},
		{"port zero", func(m map[string]string) { m["LIVE_PROBE_PORT"] = "0" }, []string{"LIVE_PROBE_PORT must be a port number from 1 to 65535"}},
		{"a port over 65535", func(m map[string]string) { m["LIVE_PROBE_PORT"] = "65536" }, []string{"LIVE_PROBE_PORT must be a port number from 1 to 65535"}},
		{"a port that is not a number", func(m map[string]string) { m["LIVE_PROBE_PORT"] = "ssh" }, []string{"LIVE_PROBE_PORT must be a port number from 1 to 65535"}},
		{"a label with a space", func(m map[string]string) { m["LIVE_PROBE_LABEL"] = "my gateway" }, []string{"LIVE_PROBE_LABEL must be 1 to 40 letters"}},
		{"a label with a quote", func(m map[string]string) { m["LIVE_PROBE_LABEL"] = "gw';id;'" }, []string{"LIVE_PROBE_LABEL must be 1 to 40 letters"}},
		{"a label over 40 characters", func(m map[string]string) { m["LIVE_PROBE_LABEL"] = strings.Repeat("g", 41) }, []string{"LIVE_PROBE_LABEL must be 1 to 40 letters"}},
		{"a label that holds the host", func(m map[string]string) { m["LIVE_PROBE_LABEL"] = "via-" + reachSeedHost }, []string{"LIVE_PROBE_LABEL holds the host"}},
		{"a label that holds the port", func(m map[string]string) { m["LIVE_PROBE_LABEL"] = "gateway-" + reachSeedPort }, []string{"LIVE_PROBE_LABEL holds the port"}},
		// The same name in other spellings is still the name (the review of PR 235, B1).
		{"a label that is the host in capitals", func(m map[string]string) { m["LIVE_PROBE_LABEL"] = strings.ToUpper(reachSeedHost) }, []string{"LIVE_PROBE_LABEL holds the host"}},
		{"a label that holds the host in capitals among other characters", func(m map[string]string) { m["LIVE_PROBE_LABEL"] = "via-" + strings.ToUpper(reachSeedHost) + "-1" }, []string{"LIVE_PROBE_LABEL holds the host"}},
		{"a host with the root dot, a label without it", func(m map[string]string) {
			m["LIVE_PROBE_HOST"] = reachSeedHost + "."
			m["LIVE_PROBE_LABEL"] = reachSeedHost
		}, []string{"LIVE_PROBE_LABEL holds the host"}},
		{"a label that spells the host with other separators", func(m map[string]string) { m["LIVE_PROBE_LABEL"] = "reach_seed-example" }, []string{"LIVE_PROBE_LABEL holds the host"}},
		{"a label that runs the host together", func(m map[string]string) { m["LIVE_PROBE_LABEL"] = "reachseedexample" }, []string{"LIVE_PROBE_LABEL holds the host"}},
		{"a label that holds the port between separators", func(m map[string]string) { m["LIVE_PROBE_LABEL"] = "gateway-403-21" }, []string{"LIVE_PROBE_LABEL holds the port"}},
	}
	for _, s := range reachShapes {
		for _, c := range cases {
			t.Run(s.row+"/"+c.name, func(t *testing.T) {
				w := newAlphaWorld(t)
				w.reachSettings(c.mutate)
				res := w.run(nil, "--run", s.row, "--yes")
				if res.code != 3 {
					t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
				}
				w.contains(res, c.want...)
				if g := w.stateFile("go-argv"); g != "" || w.stateFile("sbatch-argv") != "" || w.stateFile("ssh-argv") != "" {
					t.Errorf("a refused run built or called the cluster: %s %s %s", g, w.stateFile("sbatch-argv"), w.stateFile("ssh-argv"))
				}
			})
		}
	}
}

// The probe settings are asked of the reach rows only: every other row runs without them.
func TestTheProbeSettingsAreAskedOnlyOfTheReachRows(t *testing.T) {
	w := newAlphaWorld(t)
	w.reachSettings(func(m map[string]string) {
		delete(m, "LIVE_PROBE_HOST")
		delete(m, "LIVE_PROBE_PORT")
		delete(m, "LIVE_PROBE_LABEL")
	})
	for _, row := range []string{"gpu-accounting-helios", "gpu-accounting-athena", "conformance-athena", "alpha-athena-handover"} {
		if res := w.run(nil, "--run", row); res.code != 4 {
			t.Errorf("%s with no probe settings: exit %d, want 4:\n%s", row, res.code, res.out)
		}
	}
}

// The one-job slot is guarded on both rows: a job of the user's in it (QOS now on Helios, the partition plgrid-now on
// Athena) refuses the run before anything is built or submitted, and a job in another slot does not.
func TestTheReachRowsDoNotQueueBehindTheSlot(t *testing.T) {
	for _, c := range []struct {
		row, busy, idle string
	}{
		{"reach-helios", "5000|someone-elses-job|now|plgrid-gpu-gh200|RUNNING", "5000|someone-elses-job|plgrid|plgrid-gpu-gh200|RUNNING"},
		{"reach-athena", "5000|someone-elses-job|plgrid-now|plgrid-now|RUNNING", "5000|someone-elses-job|plgrid|plgrid-gpu-a100|RUNNING"},
	} {
		t.Run(c.row, func(t *testing.T) {
			w := newReachWorld(t)
			w.seedQueue(c.busy)
			res := w.run(nil, "--run", c.row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, "already holds 1 job(s) of yours; not queuing behind it")
			if w.stateFile("go-argv") != "" || w.stateFile("cancelled") != "" || strings.Contains(w.stateFile("sbatch-argv"), "7001") {
				t.Errorf("a refused run built, cancelled or submitted: %s %s", w.stateFile("go-argv"), w.stateFile("cancelled"))
			}
			if _, err := os.Stat(filepath.Join(w.state, "test-env-TestReach")); err == nil {
				t.Error("the test binary ran behind a busy slot")
			}
			// A job in another slot is not the row's concern.
			w2 := newReachWorld(t)
			w2.seedQueue(c.idle)
			if res := w2.run(nil, "--run", c.row, "--yes"); res.code != 0 {
				t.Errorf("a job in another slot stopped the row, exit %d:\n%s", res.code, res.out)
			}
		})
	}
}

// A job that has not started within the bound is cancelled by its tag and the row ends as NOT RUN: exit 3, not a
// failure, with the line and nothing left behind. The stub test binary follows the protocol; the real test's own
// protocol is exercised in dry mode (reach_dry_test.go).
func TestAReachRowThatWaitedTooLongIsNotRun(t *testing.T) {
	for _, s := range reachShapes {
		t.Run(s.row, func(t *testing.T) {
			w := newReachWorld(t)
			res := w.run([]string{"STUB_ALPHA_MODE=not-run"}, "--run", s.row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, s.row+": tests: 0 passed, 0 failed, 1 skipped", s.row+": NOT RUN (a job did not start within 10 min of its acceptance and was cancelled by its tag; not a failure)", "cleanup: left behind 0")
			if strings.Contains(res.out, s.row+": FAIL") || strings.Contains(res.out, s.row+": PASS") {
				t.Errorf("a NOT RUN run was also reported as a pass or a failure:\n%s", res.out)
			}
		})
	}
}

func TestASkippedOrFailedReachTestIsAFailure(t *testing.T) {
	for _, mode := range []string{"skip", "fail"} {
		for _, s := range reachShapes {
			t.Run(mode+"/"+s.row, func(t *testing.T) {
				w := newReachWorld(t)
				res := w.run([]string{"STUB_ALPHA_MODE=" + mode}, "--run", s.row, "--yes")
				if res.code != 1 {
					t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
				}
				w.contains(res, s.row+": FAIL")
				if mode == "skip" {
					w.contains(res, "skipped: its gate was not met")
				}
			})
		}
	}
}

// What a reach row's test is given: its place in the queue (the QOS and the start wait as its one clock), the
// gateway's three settings, and no dry switch, whoever set one. What reaches the screen is a status and the slot's
// use, and the seeds of the host and port never do.
func TestAReachRunGivesTheTestItsEnvironmentAndKeepsTheScreenClean(t *testing.T) {
	for _, s := range reachShapes {
		t.Run(s.row, func(t *testing.T) {
			w := newReachWorld(t)
			res := w.run([]string{"HPCBURST_ALPHA_DRY=1", "HPCBURST_ALPHA_DRY_FAULT=cut", "HPCBURST_ALPHA_ROW=" + s.row, "HPCBURST_ALPHA_OUT=/caller/out", "HPCBURST_ALPHA_PROBE_HOST=caller-host"}, "--run", s.row, "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			w.contains(res, s.row+": tests: 1 passed, 0 failed, 0 skipped", s.row+": PASS", "cleanup: left behind 0 (cancelled 0)")
			env := w.stateFile("test-env-TestReach")
			for _, bad := range []string{"HPCBURST_ALPHA_"} {
				if strings.Contains(env, bad) {
					t.Errorf("%s reached a live reach run:\n%s", bad, env)
				}
			}
			want := []string{"HPCBURST_CONFORMANCE=1", "HPCBURST_CONF_PROFILE=" + s.host, "HPCBURST_CONF_START_WAIT=10m", "HPCBURST_CONF_GPUS=1", "HPCBURST_CONF_CPUS=1", "HPCBURST_CONF_MEMORY=1Gi",
				"HPCBURST_CONF_ACCOUNT=" + s.account, "HPCBURST_CONF_PROBE_HOST=" + reachSeedHost, "HPCBURST_CONF_PROBE_PORT=" + reachSeedPort, "HPCBURST_CONF_PROBE_LABEL=" + reachSeedLabel}
			if s.slot == "now" {
				want = append(want, "HPCBURST_CONF_QOS=now", "HPCBURST_CONF_PARTITION=plgrid-gpu-gh200")
			} else {
				want = append(want, "HPCBURST_CONF_PARTITION=plgrid-now")
			}
			for _, x := range want {
				if !strings.Contains(env, x) {
					t.Errorf("the test did not get %s:\n%s", x, env)
				}
			}
			if s.slot != "now" && strings.Contains(env, "HPCBURST_CONF_QOS") {
				t.Errorf("a QOS reached the row that sends none (the partition's own applies):\n%s", env)
			}
			// The bound is the one clock: the operator's wait in the queue is not passed on, and the timeout is the row's 20
			// minutes, with the test's own one more.
			if strings.Contains(env, "PENDING_MAX_SECONDS") {
				t.Errorf("a row with a start wait got a second clock:\n%s", env)
			}
			if args := w.stateFile("test-args-TestReach"); !strings.Contains(args, "-test.timeout 21m") || !strings.Contains(args, "-test.run ^TestReach$") {
				t.Errorf("the test binary ran with %q, want -test.run ^TestReach$ and -test.timeout 21m", args)
			}
			if m := slotUse.FindStringSubmatch(res.out); m == nil || m[1] != s.slot {
				t.Errorf("no slot-use line for %s:\n%s", s.slot, res.out)
			}
			// The settings never went into the ssh arguments, where ps shows them.
			if a := w.stateFile("ssh-argv"); strings.Contains(a, reachSeedHost) || strings.Contains(a, reachSeedPort) {
				t.Errorf("a probe setting went into the ssh arguments: %s", a)
			}
			// The test's log holds the seeds (the stub prints them), and stays in the private directory.
			log, _ := os.ReadFile(filepath.Join(w.out(), s.row+".log"))
			if !strings.Contains(string(log), reachSeedHost) {
				t.Errorf("the stub test's log does not carry the seed, so the screen check proves nothing:\n%s", log)
			}
		})
	}
}

// The gateway's settings reach the reach rows and no other: a caller's own variable of the same name is removed from
// every other row's environment.
func TestTheProbeSettingsReachNoOtherRow(t *testing.T) {
	w := newReachWorld(t)
	res := w.run([]string{"HPCBURST_CONF_PROBE_HOST=caller-host", "HPCBURST_CONF_PROBE_PORT=1", "HPCBURST_CONF_PROBE_LABEL=caller-label"}, "--run", "gpu-accounting-athena", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	if env := w.stateFile("test-env-TestGPUAccounting"); strings.Contains(env, "PROBE") {
		t.Errorf("a probe setting reached another row's test:\n%s", env)
	}
}

// The alias stands for reach-athena alone (issue #386: the Helios row is beta's and runs by its own name).
func TestTheReachAliasRunsItsAthenaRowAlone(t *testing.T) {
	w := newReachWorld(t)
	res := w.run(nil, "--run", "reach", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	if !strings.Contains(res.out, "reach-athena: PASS") || strings.Contains(res.out, "reach-helios") {
		t.Fatalf("want reach-athena: PASS and no reach-helios:\n%s", res.out)
	}
	if n := strings.Count(w.stateFile("events"), "testbin "); n != 1 {
		t.Errorf("%d test runs, want 1", n)
	}
	if !strings.Contains(res.out, "cleanup: left behind 0") {
		t.Errorf("no cleanup line:\n%s", res.out)
	}
}

// A job a reach test queued and lost is cancelled by its tag, as for any cluster row, and a stopped test is waited for.
func TestAJobAReachTestLeftBehindIsCancelledByItsTag(t *testing.T) {
	w := newReachWorld(t)
	res := w.run([]string{"STUB_ALPHA_MODE=leave-late", "STUB_FOREIGN=1"}, "--run", "reach-helios", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d, want 0 (the job it left was cancelled):\n%s", res.code, res.out)
	}
	w.contains(res, "cleanup: left behind 0 (cancelled 1)")
	if got := strings.TrimSpace(w.stateFile("cancelled")); got != "9001" {
		t.Errorf("cancelled %q, want exactly the tagged job 9001 and not the controller's 9002", got)
	}
}

// A conformance change selects the reach rows with the other rows of that package, and so does a change to the
// probe script, which the package embeds in its tests. The run sets are written out from the fixture's wiring.
func TestPlanNamesTheReachRowsWithTheirPackage(t *testing.T) {
	conformanceRows := []string{"conformance-athena", "conformance-helios", "conformance-helios-gh200", "gpu-accounting-athena", "gpu-accounting-helios", "reach-athena", "reach-helios",
		"reach-host-dialed-athena", "reach-host-dialed-helios"}
	for _, c := range []struct{ name, path, content string }{
		{"the probe script", "internal/providerctl/conformance/testdata/reach-probe.sh", "#!/bin/bash\n# changed\n"},
		{"a test of the package", "internal/providerctl/conformance/c_test.go", "package conformance\n\nimport (\n\t\"testing\"\n\n\t_ \"example.test/fx/internal/providerctl\"\n)\n\nfunc TestConformance(t *testing.T) { _ = 1 }\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := newWorld(t)
			res := w.run(nil, "--plan", w.commit(c.path, c.content))
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			got := runSet(t, res.out)
			if strings.Join(got, ",") != strings.Join(conformanceRows, ",") {
				t.Errorf("run set %v, want %v\n%s", got, conformanceRows, res.out)
			}
			w.contains(res, "covered: reach-helios (cluster; gate HPCBURST_CONFORMANCE)", "covered: reach-athena (cluster; gate HPCBURST_CONFORMANCE)")
		})
	}
}
