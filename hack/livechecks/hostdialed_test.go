package livechecks

// The host-dialed relay rows of hack/live-checks.sh (issue #305): reach-host-dialed-helios and reach-host-dialed-athena,
// with the alias reach-host-dialed. Each runs the real agent in host-dialed mode with no Instance in one short job, and the
// relays the Provider's controller starts to it. The script's side is tested against the stub cluster here; the row's own
// test is run for real in dry mode in hostdialed_dry_test.go. And the reach rows in host-dialed mode: they have no path to
// probe there, so they are NOT RUN, exit 3, before anything is built or asked of a cluster.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

// hostDialedShapes are the rows' shapes, written out from the issue (#305: at most 10 minutes, under the slot guard, with a
// start wait) and the reach rows' platforms: the smallest job the request check accepts, of 5 minutes.
var hostDialedShapes = []reachShape{
	{"reach-host-dialed-helios", "helios", "partition plgrid-gpu-gh200, qos now, 1 GPU, 1 CPU, 1Gi, time limit 00:05:00, one job, no requeue, each job to start within 10 min", "now", "SEEDACCT-gh"},
	{"reach-host-dialed-athena", "athena", "partition plgrid-now, 1 GPU, 1 CPU, 1Gi, time limit 00:05:00, one job, no requeue, each job to start within 10 min", "plgrid-now", "SEEDACCT-a100"},
}

// hostDialedWorld is a world with every cluster setting and none of the reach rows' or the dial-home route's: the host-dialed
// rows need neither.
func hostDialedWorld(t *testing.T) *world {
	t.Helper()
	w := newAlphaWorld(t)
	w.alphaSettings(func(m map[string]string) {
		for _, k := range []string{"LIVE_DIALHOME_MODE", "LIVE_DIALHOME_ADDR", "LIVE_PROBE_HOST", "LIVE_PROBE_PORT", "LIVE_PROBE_LABEL"} {
			delete(m, k)
		}
	})
	return w
}

func TestListNamesTheReachHostDialedRowsAndTheAlias(t *testing.T) {
	w := newWorld(t)
	res := w.run(nil, "--list")
	if res.code != 0 {
		t.Fatalf("exit %d", res.code)
	}
	for _, s := range hostDialedShapes {
		w.contains(res, s.row+"  cluster  ready  gate HPCBURST_CONFORMANCE")
	}
	w.contains(res, "reach-host-dialed  alias  runs reach-host-dialed-athena in turn")
}

// The script's tables and the package's host-dialed rows are one fact each; the expected values are #305's.
func TestTheScriptsReachHostDialedRowsAreThePackagesRows(t *testing.T) {
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	table := func(name string) map[string][]string {
		m := map[string][]string{}
		for _, l := range block(t, string(b), name) {
			f := strings.Split(l, "|")
			m[f[0]] = f
		}
		return m
	}
	shapes, live, most, stack := table("CLUSTER SHAPES"), table("LIVE TESTS"), table("MOST JOBS"), table("PLAN STACK")
	rows := alpha.HostDialedRows()
	if len(rows) != len(hostDialedShapes) {
		t.Fatalf("the package has %d host-dialed rows, the test table %d", len(rows), len(hostDialedShapes))
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
		if f[1] != r.Cluster || f[2] != r.Cluster || f[3] != r.Partition || f[4] != qos || f[5] == "-" || f[7] != "1" || f[8] != "1" || f[9] != "1Gi" || f[10] != "00:05:00" || len(f) != 14 || f[13] != "10" {
			t.Errorf("%s: shape %v differs from #305's (the partition and QOS of %+v; a slot; 1 GPU, 1 CPU, 1Gi, 5 min, a start wait of 10)", r.Name, f, r)
		}
		if r.Lease != 5*time.Minute || r.StartWait != 10*time.Minute || r.MostJobs != 1 || r.PostsTo != "#145" || r.Origin != "#305" {
			t.Errorf("%s: the package's row %+v is not one job of 5 minutes with a start wait of 10, posted to #145 with the origin issue #305", r.Name, r)
		}
		if f[11] != fmt.Sprint(int(r.Budget/time.Minute)) {
			t.Errorf("%s: the shape's timeout is %s minutes, the package's budget %s", r.Name, f[11], r.Budget)
		}
		if l := live[r.Name]; len(l) != 8 || l[2] != "internal/providerctl/conformance" || l[3] != "^TestReachHostDialed$" || l[5] != "profiles/"+r.Cluster+".yaml" {
			t.Errorf("%s: live test row %v, want TestReachHostDialed of the conformance package with its profile", r.Name, l)
		}
		if m := most[r.Name]; len(m) != 2 || m[1] != "1" {
			t.Errorf("%s: most jobs %v, want 1", r.Name, m)
		}
		// The job runs the agent, which the test package does not import: --plan names the row for an agent change.
		if s := stack[r.Name]; len(s) != 2 || s[1] != "cmd/hb-agent" {
			t.Errorf("%s: plan stack %v, want cmd/hb-agent", r.Name, s)
		}
	}
	// GH200 is aarch64: the Helios row's closure is the arm64 build's too, and it asks for the memfs constraint.
	if live["reach-host-dialed-helios"][6] != "host,linux-arm64" || live["reach-host-dialed-athena"][6] != "host" {
		t.Errorf("build contexts: Helios %q, Athena %q, want host,linux-arm64 and host", live["reach-host-dialed-helios"][6], live["reach-host-dialed-athena"][6])
	}
	if shapes["reach-host-dialed-helios"][12] != "-C memfs" || shapes["reach-host-dialed-athena"][12] != "-" {
		t.Errorf("extra options: Helios %q, Athena %q, want -C memfs and -", shapes["reach-host-dialed-helios"][12], shapes["reach-host-dialed-athena"][12])
	}
}

func TestEveryReachHostDialedRowRunsOnlyWithYesAndPrintsItsShapeAndEstimate(t *testing.T) {
	for _, s := range hostDialedShapes {
		t.Run(s.row, func(t *testing.T) {
			w := hostDialedWorld(t)
			res := w.run(nil, "--run", s.row)
			if res.code != 4 {
				t.Fatalf("exit %d, want 4:\n%s", res.code, res.out)
			}
			w.contains(res, s.row+": shape: "+s.shape, "queue estimate: a start in about", "not run: pass --yes")
			if g := w.stateFile("go-argv"); g != "" || strings.Contains(w.stateFile("events"), "build-agents") {
				t.Errorf("something was built before --yes: %s", g)
			}
			argv := lines(w.stateFile("sbatch-argv"))
			if len(argv) != 1 || !strings.Contains(argv[0], "--test-only") || !strings.Contains(argv[0], "-A "+s.account) || !strings.Contains(argv[0], "-t 00:05:00") {
				t.Errorf("want exactly the estimate of the row's job, for its account; got %v", argv)
			}
		})
	}
}

// What a host-dialed row's test is given: the agent builds the script made before the slot, its place in the queue (the
// QOS and the start wait as its one clock), and no probe setting or dry switch, whoever set one.
func TestAReachHostDialedRunBuildsTheAgentAndGivesTheTestItsEnvironment(t *testing.T) {
	for _, s := range hostDialedShapes {
		t.Run(s.row, func(t *testing.T) {
			w := hostDialedWorld(t)
			// The operator's wait in the queue is set: a row with a start wait takes no second clock and no pending override.
			res := w.run([]string{"HPCBURST_ALPHA_DRY=1", "HPCBURST_ALPHA_AGENT_BIN=/caller/agent", "HPCBURST_CONF_PROBE_HOST=caller-host", "HPCBURST_CONF_AGENT_BUILDS=/caller/builds", "LIVE_PENDING_MAX_SECONDS=7200"}, "--run", s.row, "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			w.contains(res, s.row+": tests: 1 passed, 0 failed, 0 skipped", s.row+": PASS", "cleanup: left behind 0 (cancelled 0)")
			ev := w.stateFile("events")
			build, run := strings.Index(ev, "build-agents "), strings.Index(ev, "testbin TestReachHostDialed")
			if build < 0 || run < build {
				t.Errorf("the agent builds were not made before the test ran:\n%s", ev)
			}
			env := w.stateFile("test-env-TestReachHostDialed")
			var builds string
			for _, l := range strings.Split(env, "\n") {
				if v, ok := strings.CutPrefix(l, "HPCBURST_CONF_AGENT_BUILDS="); ok {
					builds = v
				}
			}
			if !strings.HasPrefix(builds, w.out()+"/") || filepath.Base(builds) != "agent-builds" {
				t.Errorf("HPCBURST_CONF_AGENT_BUILDS is %q, want the run's own agent-builds directory in the output directory", builds)
			} else if _, err := os.Stat(filepath.Join(builds, "hb-agent-x86_64")); err != nil {
				t.Errorf("the agent builds directory holds no build: %v", err)
			}
			for _, bad := range []string{"HPCBURST_ALPHA_", "PROBE", "PENDING_MAX_SECONDS", "/caller/"} {
				if strings.Contains(env, bad) {
					t.Errorf("%s reached a live host-dialed run:\n%s", bad, env)
				}
			}
			want := []string{"HPCBURST_CONFORMANCE=1", "HPCBURST_CONF_PROFILE=" + s.host, "HPCBURST_CONF_START_WAIT=10m", "HPCBURST_CONF_ACCOUNT=" + s.account, "HPCBURST_CONF_CPUS=1", "HPCBURST_CONF_MEMORY=1Gi"}
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
			// The timeout is the row's 25 minutes (the start wait is its one clock), with the test's own one more.
			if args := w.stateFile("test-args-TestReachHostDialed"); !strings.Contains(args, "-test.timeout 26m") || !strings.Contains(args, "-test.run ^TestReachHostDialed$") {
				t.Errorf("the test binary ran with %q, want -test.run ^TestReachHostDialed$ and -test.timeout 26m", args)
			}
			if m := slotUse.FindStringSubmatch(res.out); m == nil || m[1] != s.slot {
				t.Errorf("no slot-use line for %s:\n%s", s.slot, res.out)
			}
		})
	}
}

// The one-job slot is guarded: a job of the user's in it refuses the run before anything is built or submitted.
func TestTheReachHostDialedRowsDoNotQueueBehindTheSlot(t *testing.T) {
	for _, c := range []struct{ row, busy string }{
		{"reach-host-dialed-helios", "5000|someone-elses-job|now|plgrid-gpu-gh200|RUNNING"},
		{"reach-host-dialed-athena", "5000|someone-elses-job|plgrid-now|plgrid-now|RUNNING"},
	} {
		t.Run(c.row, func(t *testing.T) {
			w := hostDialedWorld(t)
			w.seedQueue(c.busy)
			res := w.run(nil, "--run", c.row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, "already holds 1 job(s) of yours; not queuing behind it")
			if w.stateFile("go-argv") != "" || strings.Contains(w.stateFile("events"), "build-agents") || strings.Contains(w.stateFile("sbatch-argv"), "7001") {
				t.Errorf("a refused run built or submitted: %s", w.stateFile("events"))
			}
		})
	}
}

// A job that never started within the start wait: NOT RUN, exit 3, the start wait's line.
func TestAReachHostDialedRowThatWaitedTooLongIsNotRun(t *testing.T) {
	w := hostDialedWorld(t)
	res := w.run([]string{"STUB_ALPHA_MODE=not-run"}, "--run", "reach-host-dialed-athena", "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, "reach-host-dialed-athena: NOT RUN (a job did not start within 10 min of its acceptance and was cancelled by its tag; not a failure)")
}

// The alias stands for reach-host-dialed-athena alone (issue #386: the Helios row is beta's and runs by its own name).
func TestTheReachHostDialedAliasRunsItsAthenaRowAlone(t *testing.T) {
	w := hostDialedWorld(t)
	res := w.run(nil, "--run", "reach-host-dialed", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	if !strings.Contains(res.out, "reach-host-dialed-athena: PASS") || strings.Contains(res.out, "reach-host-dialed-helios") {
		t.Fatalf("want reach-host-dialed-athena: PASS and no Helios row:\n%s", res.out)
	}
	if n := strings.Count(w.stateFile("events"), "testbin "); n != 1 {
		t.Errorf("%d test runs, want 1", n)
	}
}

// In host-dialed mode the reach rows have no path to probe: each is NOT RUN, exit 3, with the reason, before anything is
// built, estimated or run, with --yes or without, and whether or not their probe settings are there. The host-dialed rows
// run on in the same mode.
func TestTheReachRowsAreNotRunInHostDialedMode(t *testing.T) {
	for _, c := range []struct {
		name  string
		probe bool
		args  []string
	}{
		{"with --yes and no probe settings", false, []string{"--run", "reach", "--yes"}},
		{"with the probe settings", true, []string{"--run", "reach", "--yes"}},
		{"without --yes", false, []string{"--run", "reach"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := newAlphaWorld(t)
			w.alphaSettings(func(m map[string]string) {
				m["LIVE_DIALHOME_MODE"] = "host-dialed"
				delete(m, "LIVE_DIALHOME_ADDR")
				if c.probe {
					m["LIVE_PROBE_HOST"], m["LIVE_PROBE_PORT"], m["LIVE_PROBE_LABEL"] = reachSeedHost, reachSeedPort, reachSeedLabel
				}
			})
			res := w.run(nil, c.args...)
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			// The alias stands for reach-athena alone (issue #386); reach-helios is refused the same way by its own name
			// (below).
			for _, row := range []string{"reach-athena"} {
				w.contains(res, row+": NOT RUN (the reach check applies only to direct and ssh modes; LIVE_DIALHOME_MODE is host-dialed")
				if strings.Contains(res.out, row+": FAIL") || strings.Contains(res.out, row+": PASS") || strings.Contains(res.out, row+": shape") {
					t.Errorf("%s was refused and also went on:\n%s", row, res.out)
				}
			}
			if strings.Contains(res.out, "LIVE_PROBE") {
				t.Errorf("a refused reach row asked for its probe settings:\n%s", res.out)
			}
			if w.stateFile("go-argv") != "" || w.stateFile("sbatch-argv") != "" || w.stateFile("ssh-argv") != "" || strings.Contains(w.stateFile("events"), "testbin") {
				t.Errorf("a refused run built, estimated or ran: %s", w.stateFile("events"))
			}
		})
	}
	t.Run("reach-helios, by its own name, is refused the same way", func(t *testing.T) {
		w := newAlphaWorld(t)
		w.alphaSettings(func(m map[string]string) {
			m["LIVE_DIALHOME_MODE"] = "host-dialed"
			delete(m, "LIVE_DIALHOME_ADDR")
		})
		res := w.run(nil, "--run", "reach-helios", "--yes")
		if res.code != 3 {
			t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
		}
		w.contains(res, "reach-helios: NOT RUN (the reach check applies only to direct and ssh modes; LIVE_DIALHOME_MODE is host-dialed")
	})
	t.Run("the host-dialed rows run on", func(t *testing.T) {
		w := newAlphaWorld(t)
		w.alphaSettings(func(m map[string]string) {
			m["LIVE_DIALHOME_MODE"] = "host-dialed"
			delete(m, "LIVE_DIALHOME_ADDR")
		})
		if res := w.run(nil, "--run", "reach-host-dialed-athena", "--yes"); res.code != 0 || !strings.Contains(res.out, "reach-host-dialed-athena: PASS") {
			t.Errorf("exit %d, want a pass:\n%s", res.code, res.out)
		}
	})
	t.Run("the reach rows run on in direct mode", func(t *testing.T) {
		w := newReachWorld(t)
		if res := w.run(nil, "--run", "reach-athena", "--yes"); res.code != 0 || strings.Contains(res.out, "NOT RUN") {
			t.Errorf("exit %d, want a pass:\n%s", res.code, res.out)
		}
	})
}

// --plan names the host-dialed rows for a change in the agent their job runs, which their test does not import, and the
// Helios row alone for a change only the arm64 build has.
func TestPlanNamesTheReachHostDialedRowsForAnAgentChange(t *testing.T) {
	for _, c := range []struct {
		name, path, content string
		want, not           []string
	}{
		{"the agent", "internal/agent/agent2.go", "package agent\n", []string{"reach-host-dialed-athena", "reach-host-dialed-helios"}, nil},
		{"the arm64 build only", "internal/armonly/armonly.go", "package armonly\n\nconst x = 1\n", []string{"reach-host-dialed-helios"}, []string{"reach-host-dialed-athena"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := newWorld(t)
			res := w.run(nil, "--plan", w.commit(c.path, c.content))
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			got := strings.Join(runSet(t, res.out), ",")
			for _, row := range c.want {
				if !strings.Contains(got, row) {
					t.Errorf("run set %s lacks %s\n%s", got, row, res.out)
				}
			}
			for _, row := range c.not {
				if strings.Contains(got, row) {
					t.Errorf("run set %s holds %s\n%s", got, row, res.out)
				}
			}
		})
	}
}
