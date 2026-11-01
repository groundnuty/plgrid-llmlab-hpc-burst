package livechecks

// The alpha rows of hack/live-checks.sh (issue #194): alpha-athena-handover,
// alpha-helios-nooverlap, tunnel-hour-helios/-athena and gpu-accounting-helios/
// -athena, with the aliases tunnel-hour and gpu-accounting. The script's side is
// tested against the stub cluster, as the other rows are; the rows' own tests are
// run for real in dry mode (alpha_dry_test.go).

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

func init() {
	// The dial-home address and key are settings that must never reach the screen.
	leakSeeds = append(leakSeeds, "gateway-seed.example", "SEEDDIALKEY")
}

// alphaShape is what a row asks of the scheduler, worked out from the issue (#194),
// HBEP-7's graduation criteria (the partitions and the QOS now), the profiles' per-GPU
// sizes (Athena A100: 16 CPUs and 125Gi; Helios GH200: 72 CPUs, 120000M as the real-engine run
// uses) and S1's run limit of 1 h 25 min for the hour; not read from the script's table.
type alphaShape struct {
	row, test, host, shape string
	slot                   string // the one-job slot's token, "" for none
	account                string
	needsStack             bool
}

var alphaShapes = []alphaShape{
	{"alpha-athena-handover", "TestAlphaAthenaHandover", "athena", "partition plgrid-gpu-a100, 1 GPU, 16 CPU, 125Gi, time limit 01:00:00, one job, no requeue", "", "SEEDACCT-a100", true},
	{"alpha-helios-nooverlap", "TestAlphaHeliosNoOverlap", "helios", "partition plgrid-gpu-gh200, qos now, 1 GPU, 72 CPU, 120000M, time limit 01:00:00, one job, no requeue", "now", "SEEDACCT-gh", true},
	{"tunnel-hour-helios", "TestTunnelHour", "helios", "partition plgrid-gpu-gh200, qos now, 1 GPU, 72 CPU, 120000M, time limit 01:25:00, one job, no requeue", "now", "SEEDACCT-gh", true},
	{"tunnel-hour-athena", "TestTunnelHour", "athena", "partition plgrid-gpu-a100, 1 GPU, 16 CPU, 125Gi, time limit 01:25:00, one job, no requeue", "", "SEEDACCT-a100", true},
	{"gpu-accounting-helios", "TestGPUAccounting", "helios", "partition plgrid-gpu-gh200, qos now, 1 GPU, 72 CPU, 117Gi, time limit 00:05:00, one job, no requeue", "now", "SEEDACCT-gh", false},
	{"gpu-accounting-athena", "TestGPUAccounting", "athena", "partition plgrid-gpu-a100, 1 GPU, 16 CPU, 125Gi, time limit 00:05:00, one job, no requeue", "", "SEEDACCT-a100", false},
}

// alphaStubGo stands in for go in an alpha row's prepare step: build into a directory, and test -c.
const alphaStubGo = `#!/bin/bash
printf '%s\n' "$*" >> "$STUB_STATE/go-argv"
echo "go $1" >> "$STUB_STATE/events"
out=""; prev=""
for a in "$@"; do [ "$prev" = -o ] && out=$a; prev=$a; done
case "$1" in
  build) mkdir -p "$out" && for b in hpcburst hpcburst-controller hpcburst-front; do echo binary > "$out/$b"; done ;;
  test) mkdir -p "$(dirname "$out")"; cp "$STUB_STATE/testbin.sh" "$out"; chmod +x "$out" ;;
esac
`

// alphaStubTest stands for the compiled test of an alpha row: it records its environment, skips unless the
// row named in HPCBURST_ALPHA_ROW is one of its own (as the real tests do), and ends as STUB_ALPHA_MODE says:
// pass; fail; skip; leave-late (writes its job tag only after the job is queued, as a scenario that records
// a tag late would); sleep (the same, then waits to be signalled).
const alphaStubTest = `#!/bin/bash
run=""; prev=""
for a in "$@"; do [ "$prev" = -test.run ] && run=$a; prev=$a; done
t=${run#^}; t=${t%\$}
echo "testbin $t" >> "$STUB_STATE/events"
printf '%s\n' "$*" > "$STUB_STATE/test-args-$t"
env | grep -E '^(HPCBURST|HB_)' | sort > "$STUB_STATE/test-env-$t.$$"
cp "$STUB_STATE/test-env-$t.$$" "$STUB_STATE/test-env-$t"
echo "log line: host seedhost01 job 424242 key SEEDKEY dialhome gateway-seed.example"
mode=${STUB_ALPHA_MODE:-pass}
[ -n "${HPCBURST_ALPHA_ROW:-}${HPCBURST_CONFORMANCE:-}" ] || mode=skip
leave() {
  tag="hb-alphaleft-aaaaaaaaaaaa"
  echo "9001|$tag|${HPCBURST_ALPHA_QOS:-normal}|${HPCBURST_ALPHA_PARTITION:-plgrid}|PENDING" >> "$STUB_STATE/queue"
  echo "$tag" >> "${HPCBURST_ALPHA_OUT:-${HPCBURST_CONF_OUT:-/dev/null}}/job-tags.txt"
  [ -z "${STUB_FOREIGN:-}" ] || echo "9002|hb-controller-xxxxxxxxxxxx|${HPCBURST_ALPHA_QOS:-normal}|${HPCBURST_ALPHA_PARTITION:-plgrid}|RUNNING" >> "$STUB_STATE/queue"
}
case $mode in
  pass) echo "=== RUN   $t"; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  skip) echo "--- SKIP: $t (0.00s)"; echo PASS ;;
  fail) echo "--- FAIL: $t (0.01s)"; echo FAIL; exit 1 ;;
  leave-late) leave; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  sleep) leave; touch "$STUB_STATE/started"; exec sleep 3600 ;;
esac
`

// useAlphaStubs installs the stubs of an alpha row, over the ones newWorld and useStubGo made.
func (w *world) useAlphaStubs(rows ...string) {
	w.t.Helper()
	w.useStubGo()
	write(w.t, filepath.Join(w.bin, "go"), alphaStubGo, 0o755)
	write(w.t, filepath.Join(w.state, "testbin.sh"), alphaStubTest, 0o755)
	_ = rows
}

// alphaSettings adds what the alpha rows need to the world's settings file.
func (w *world) alphaSettings(mutate func(m map[string]string)) {
	w.t.Helper()
	bin := filepath.Join(w.dir, "bindir")
	key := filepath.Join(w.dir, "dialhome-key")
	for _, d := range []string{bin} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			w.t.Fatal(err)
		}
	}
	write(w.t, key, "SEEDDIALKEY\n", 0o600)
	w.writeSettings(func(m map[string]string) {
		m["LIVE_DIALHOME_MODE"] = "direct"
		m["LIVE_DIALHOME_ADDR"] = "gateway-seed.example:29999"
		m["LIVE_BIN_DIR"] = bin
		m["LIVE_ISSUE_174_LANDED"] = "yes"
		if mutate != nil {
			mutate(m)
		}
	})
}

// newAlphaWorld is a world with the alpha settings, the alpha stubs and the stub build scripts executable.
func newAlphaWorld(t *testing.T) *world {
	t.Helper()
	w := newWorld(t)
	w.alphaSettings(nil)
	w.useAlphaStubs()
	for _, s := range []string{"hack/build-agents.sh", "hack/fetch-frp.sh"} {
		if err := os.Chmod(filepath.Join(w.repo, s), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return w
}

func TestListNamesTheAlphaRowsAndTheAliases(t *testing.T) {
	w := newWorld(t)
	res := w.run(nil, "--list")
	if res.code != 0 {
		t.Fatalf("exit %d", res.code)
	}
	for _, s := range alphaShapes {
		w.contains(res, s.row+"  cluster  ready")
	}
	w.contains(res, "tunnel-hour  alias  runs tunnel-hour-helios, tunnel-hour-athena in turn", "gpu-accounting  alias  runs gpu-accounting-helios, gpu-accounting-athena in turn")
}

// The script's cluster shapes and the package's rows are one fact each: a partition, a QOS and a GPU count
// that differ would run a job the test then judges against another.
func TestTheScriptsShapesAreThePackagesRows(t *testing.T) {
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	shapes := map[string][]string{}
	for _, l := range block(t, string(b), "CLUSTER SHAPES") {
		f := strings.Split(l, "|")
		shapes[f[0]] = f
	}
	all := append(alpha.Rows(), alpha.AccountingRows()...)
	if len(all) != len(alphaShapes) {
		t.Fatalf("the package has %d alpha rows, the test table %d", len(all), len(alphaShapes))
	}
	for _, r := range all {
		f, ok := shapes[r.Name]
		if !ok {
			t.Errorf("%s: no cluster shape in the script", r.Name)
			continue
		}
		qos := r.QOS
		if qos == "" {
			qos = "-"
		}
		// name | host | profile | partition | qos | slot | account key | GPUs | CPUs | memory | time limit | timeout | extra
		if f[1] != r.Cluster || f[2] != r.Cluster || f[3] != r.Partition || f[4] != qos || f[7] != "1" {
			t.Errorf("%s: shape %v differs from the package's row %+v", r.Name, f, r)
		}
		var h, m, s int
		if _, err := fmt.Sscanf(f[10], "%d:%d:%d", &h, &m, &s); err != nil {
			t.Errorf("%s: time limit %q: %v", r.Name, f[10], err)
			continue
		}
		if tl := time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second; tl < r.Lease {
			t.Errorf("%s: the shape's time limit %s is under the lease %s the run plans", r.Name, tl, r.Lease)
		}
	}
}

func TestEveryAlphaRowRunsOnlyWithYesAndPrintsItsShapeAndEstimate(t *testing.T) {
	for _, s := range alphaShapes {
		t.Run(s.row, func(t *testing.T) {
			w := newAlphaWorld(t)
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
			if _, err := os.Stat(filepath.Join(w.state, "test-env-"+s.test)); err == nil {
				t.Error("the test binary ran without --yes")
			}
		})
	}
}

func TestAlphaRowsRefuseWithoutTheirSettings(t *testing.T) {
	cases := []struct {
		name   string
		row    string
		mutate func(map[string]string)
		want   []string
	}{
		{"no dial-home route at all", "alpha-athena-handover", func(m map[string]string) {
			delete(m, "LIVE_DIALHOME_MODE")
			delete(m, "LIVE_DIALHOME_ADDR")
			delete(m, "LIVE_BIN_DIR")
		}, []string{"setting LIVE_DIALHOME_MODE is missing", "setting LIVE_DIALHOME_ADDR is missing", "setting LIVE_BIN_DIR is missing"}},
		{"ssh mode without a key", "tunnel-hour-athena", func(m map[string]string) { m["LIVE_DIALHOME_MODE"] = "ssh" }, []string{"setting LIVE_DIALHOME_KEY is missing"}},
		{"a key that others can read", "tunnel-hour-helios", func(m map[string]string) {
			m["LIVE_DIALHOME_MODE"] = "ssh"
			m["LIVE_DIALHOME_KEY"] = "/etc/hostname"
		}, []string{"LIVE_DIALHOME_KEY names a file that is not mode 600"}},
		{"a mode that is neither", "alpha-athena-handover", func(m map[string]string) { m["LIVE_DIALHOME_MODE"] = "tcp" }, []string{"LIVE_DIALHOME_MODE must be direct or ssh"}},
		{"an address with no port", "alpha-athena-handover", func(m map[string]string) { m["LIVE_DIALHOME_ADDR"] = "gateway-seed.example" }, []string{"LIVE_DIALHOME_ADDR must be host:port"}},
		{"a binary directory that is not one", "tunnel-hour-athena", func(m map[string]string) { m["LIVE_BIN_DIR"] = "/no/such/dir" }, []string{"LIVE_BIN_DIR is not a directory"}},
		{"a relative binary directory", "tunnel-hour-athena", func(m map[string]string) { m["LIVE_BIN_DIR"] = "bin" }, []string{"LIVE_BIN_DIR must be an absolute path"}},
		{"no-overlap without #174", "alpha-helios-nooverlap", func(m map[string]string) { delete(m, "LIVE_ISSUE_174_LANDED") }, []string{"LIVE_ISSUE_174_LANDED must be yes", "needs #174"}},
		{"no-overlap with #174 marked not landed", "alpha-helios-nooverlap", func(m map[string]string) { m["LIVE_ISSUE_174_LANDED"] = "no" }, []string{"LIVE_ISSUE_174_LANDED must be yes"}},
		{"an account the shape needs", "alpha-athena-handover", func(m map[string]string) { delete(m, "ACCOUNT_ATHENA_A100") }, []string{"setting ACCOUNT_ATHENA_A100 is missing"}},
		{"the provider's credentials", "alpha-helios-nooverlap", func(m map[string]string) { delete(m, "LIVE_CONF_CREDENTIALS_HELIOS") }, []string{"setting LIVE_CONF_CREDENTIALS_HELIOS is missing"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newAlphaWorld(t)
			w.alphaSettings(c.mutate)
			res := w.run(nil, "--run", c.row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, c.want...)
			if g := w.stateFile("go-argv"); g != "" || w.stateFile("sbatch-argv") != "" {
				t.Errorf("a refused run built or called the cluster: %s %s", g, w.stateFile("sbatch-argv"))
			}
		})
	}
}

// What an alpha row needs is its own: #174 only for the no-overlap row, the dial-home route for none of the accounting rows.
func TestSettingsAreAskedOnlyOfTheRowsThatNeedThem(t *testing.T) {
	w := newAlphaWorld(t)
	w.alphaSettings(func(m map[string]string) {
		delete(m, "LIVE_ISSUE_174_LANDED")
		delete(m, "LIVE_DIALHOME_MODE")
		delete(m, "LIVE_DIALHOME_ADDR")
		delete(m, "LIVE_BIN_DIR")
	})
	for _, row := range []string{"gpu-accounting-helios", "gpu-accounting-athena"} {
		if res := w.run(nil, "--run", row); res.code != 4 {
			t.Errorf("%s with no dial-home route: exit %d, want 4:\n%s", row, res.code, res.out)
		}
	}
	// #174 is the no-overlap row's alone.
	w.alphaSettings(func(m map[string]string) { delete(m, "LIVE_ISSUE_174_LANDED") })
	for _, row := range []string{"alpha-athena-handover", "tunnel-hour-helios", "tunnel-hour-athena"} {
		if res := w.run(nil, "--run", row); res.code != 4 {
			t.Errorf("%s without #174: exit %d, want 4:\n%s", row, res.code, res.out)
		}
	}
}

func TestTheNoOverlapRowDoesNotQueueBehindTheSlot(t *testing.T) {
	w := newAlphaWorld(t)
	w.seedQueue("5000|someone-elses-job|now|plgrid-gpu-gh200|RUNNING")
	res := w.run(nil, "--run", "alpha-helios-nooverlap", "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, "already holds 1 job(s) of yours; not queuing behind it")
	if w.stateFile("go-argv") != "" || w.stateFile("cancelled") != "" {
		t.Errorf("a run that was refused built something or cancelled a job: %s %s", w.stateFile("go-argv"), w.stateFile("cancelled"))
	}
	// The same busy slot does not stop a row that has none.
	w2 := newAlphaWorld(t)
	w2.seedQueue("5000|someone-elses-job|now|plgrid-gpu-gh200|RUNNING")
	if res := w2.run(nil, "--run", "alpha-athena-handover", "--yes"); res.code != 0 {
		t.Errorf("a row with no slot, exit %d:\n%s", res.code, res.out)
	}
}

func TestAnAlphaRunBuildsBeforeTheSlotAndGivesTheTestItsEnvironment(t *testing.T) {
	for _, s := range alphaShapes {
		if !s.needsStack {
			continue
		}
		t.Run(s.row, func(t *testing.T) {
			w := newAlphaWorld(t)
			// A caller's dry switch must not reach a live run, whoever set it.
			res := w.run([]string{"HPCBURST_ALPHA_DRY=1", "HPCBURST_ALPHA_DRY_FAULT=cut"}, "--run", s.row, "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			w.contains(res, s.row+": tests: 1 passed, 0 failed, 0 skipped", s.row+": PASS", "cleanup: left behind 0 (cancelled 0)")
			// Every build step comes before the test binary runs, which is when the slot is in use.
			ev := lines(w.stateFile("events"))
			first := -1
			for i, e := range ev {
				if strings.HasPrefix(e, "testbin") {
					first = i
					break
				}
			}
			if first < 0 {
				t.Fatalf("the test never ran:\n%v", ev)
			}
			joined := strings.Join(ev[:first], "\n")
			for _, want := range []string{"go test", "go build", "build-agents ", "fetch-frp --agent-builds "} {
				if !strings.Contains(joined, want) {
					t.Errorf("%q did not run before the test binary:\n%s", want, joined)
				}
			}
			env := w.stateFile("test-env-" + s.test)
			for _, bad := range []string{"HPCBURST_ALPHA_DRY"} {
				if strings.Contains(env, bad) {
					t.Errorf("%s reached a live run's test:\n%s", bad, env)
				}
			}
			want := []string{"HPCBURST_ALPHA_ROW=" + s.row, "HPCBURST_ALPHA_GPUS=1", "HPCBURST_ALPHA_ACCOUNT=" + s.account,
				"HPCBURST_ALPHA_DIALHOME_MODE=direct", "HPCBURST_ALPHA_DIALHOME_ADDR=gateway-seed.example:29999",
				"HPCBURST_ALPHA_PENDING_MAX_SECONDS=7200", "HPCBURST_ALPHA_HOST_LOCK=" + filepath.Join(w.dir, "locks", s.host+".lock"),
				"HPCBURST_ALPHA_CREDENTIALS=" + filepath.Join(w.dir, s.host+".ssh"), "HPCBURST_ALPHA_BIN_DIR=" + filepath.Join(w.dir, "bindir")}
			if s.slot != "" {
				want = append(want, "HPCBURST_ALPHA_QOS="+s.slot)
			}
			for _, x := range want {
				if !strings.Contains(env, x) {
					t.Errorf("the test did not get %s:\n%s", x, env)
				}
			}
			if !s.hasQOS() && strings.Contains(env, "HPCBURST_ALPHA_QOS") {
				t.Errorf("a QOS reached a row that has none:\n%s", env)
			}
			if s.slot != "" {
				if m := slotUse.FindStringSubmatch(res.out); m == nil || m[1] != s.slot {
					t.Errorf("no slot-use line for %s:\n%s", s.slot, res.out)
				}
			} else {
				w.contains(res, "cluster run (no one-job slot): start ")
			}
			// The build directory the test is told of exists and holds what prepare made.
			for _, line := range strings.Split(env, "\n") {
				if d, ok := strings.CutPrefix(line, "HPCBURST_ALPHA_BUILD_DIR="); ok {
					for _, f := range []string{"bin/hpcburst", "agent-builds/hb-agent-x86_64", "agent-builds/frpc-aarch64"} {
						if _, err := os.Stat(filepath.Join(d, f)); err != nil {
							t.Errorf("the build directory lacks %s: %v", f, err)
						}
					}
				}
			}
			if strings.Contains(w.stateFile("ssh-argv"), "SEEDACCT") || strings.Contains(w.stateFile("ssh-argv"), "gateway-seed") {
				t.Errorf("a setting went into the ssh arguments, where ps shows it")
			}
		})
	}
}

func (s alphaShape) hasQOS() bool { return s.slot != "" }

func TestTheAliasesRunTheirRowsOneAfterTheOther(t *testing.T) {
	for alias, rows := range map[string][]string{
		"tunnel-hour":    {"tunnel-hour-helios", "tunnel-hour-athena"},
		"gpu-accounting": {"gpu-accounting-helios", "gpu-accounting-athena"},
	} {
		t.Run(alias, func(t *testing.T) {
			w := newAlphaWorld(t)
			res := w.run(nil, "--run", alias, "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			at := -1
			for _, r := range rows {
				i := strings.Index(res.out, r+": PASS")
				if i < 0 || i < at {
					t.Fatalf("%s: PASS is missing or out of order:\n%s", r, res.out)
				}
				at = i
			}
			// One test binary run per row, and only one job in flight at a time: the first row's cleanup is
			// reported before the second row's shape.
			if n := strings.Count(w.stateFile("events"), "testbin "); n != 2 {
				t.Errorf("%d test runs, want 2", n)
			}
			if a, b := strings.Index(res.out, "cleanup: left behind 0"), strings.Index(res.out, rows[1]+": shape"); a < 0 || b < 0 || a > b {
				t.Errorf("the first row's cleanup does not come before the second row starts:\n%s", res.out)
			}
		})
	}
	// A bare name that is no row or alias is still an error.
	w := newAlphaWorld(t)
	if res := w.run(nil, "--run", "tunnel", "--yes"); res.code != 2 {
		t.Errorf("an unknown name: exit %d, want 2:\n%s", res.code, res.out)
	}
}

func TestASkippedOrFailedAlphaTestIsAFailure(t *testing.T) {
	for _, mode := range []string{"skip", "fail"} {
		for _, row := range []string{"alpha-athena-handover", "gpu-accounting-athena"} {
			t.Run(mode+"/"+row, func(t *testing.T) {
				w := newAlphaWorld(t)
				res := w.run([]string{"STUB_ALPHA_MODE=" + mode}, "--run", row, "--yes")
				if res.code != 1 {
					t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
				}
				w.contains(res, row+": FAIL")
				if mode == "skip" {
					w.contains(res, "skipped: its gate was not met")
				}
			})
		}
	}
}

// A tag the test writes down only after its job is queued is still the run's: the cleanup reads the file when
// the run ends, and cancels that job and only that.
func TestAJobWhoseTagWasWrittenLateIsStillCancelled(t *testing.T) {
	for _, row := range []string{"alpha-helios-nooverlap", "alpha-athena-handover", "gpu-accounting-helios"} {
		t.Run(row, func(t *testing.T) {
			w := newAlphaWorld(t)
			res := w.run([]string{"STUB_ALPHA_MODE=leave-late", "STUB_FOREIGN=1"}, "--run", row, "--yes")
			// As for the conformance rows: the leftover is cancelled and gone, so the pass stands.
			if res.code != 0 {
				t.Fatalf("exit %d, want 0 (the job it left was cancelled):\n%s", res.code, res.out)
			}
			w.contains(res, "cleanup: left behind 0 (cancelled 1)")
			if got := strings.TrimSpace(w.stateFile("cancelled")); got != "9001" {
				t.Errorf("cancelled %q, want exactly the tagged job 9001 and not the controller's 9002", got)
			}
			if !strings.Contains(w.stateFile("queue"), "9002|hb-controller") {
				t.Errorf("the foreign job left the queue: %s", w.stateFile("queue"))
			}
		})
	}
}

func TestASignalDuringAnAlphaRunRunsTheCleanup(t *testing.T) {
	w := newAlphaWorld(t)
	cmd, out := w.start([]string{"STUB_ALPHA_MODE=sleep"}, "--run", "alpha-helios-nooverlap", "--yes")
	w.waitFor("started")
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatalf("the script did not stop after SIGTERM:\n%s", out.String())
	}
	w.noLeftoverProcess(cmd)
	if !strings.Contains(out.String(), "cleanup: left behind 0 (cancelled 1)") || strings.TrimSpace(w.stateFile("cancelled")) != "9001" {
		t.Errorf("the signalled run did not cancel its tagged job (cancelled %q):\n%s", w.stateFile("cancelled"), out.String())
	}
}

func TestPlanNamesTheAlphaRowsWhenTheStackChanges(t *testing.T) {
	alphaRows := []string{"alpha-athena-handover", "alpha-helios-nooverlap", "tunnel-hour-athena", "tunnel-hour-helios"}
	cases := []struct {
		name, path, content string
		want                []string
	}{
		{"a package only the stack imports", "internal/stack/stack.go", "package stack\n\nvar X = 1\n", alphaRows},
		{"the rows' own package", "internal/alpha/alpha.go", "package alpha\n\nvar X = 1\n", alphaRows},
		{"a stack binary", "cmd/hpcburst-front/main.go", "package main\n\nimport _ \"example.test/fx/internal/stack\"\n\nfunc main() { _ = 1 }\n", alphaRows},
		{"the example the rows apply", "examples/provider-athena-a100.yaml", "kind: Provider\nx: 1\n", []string{"alpha-athena-handover"}},
		{"a package the accounting rows and conformance run, not the stack", "internal/providerctl/providerctl.go", "package providerctl\n\nimport _ \"example.test/fx/internal/slurm\"\n\nvar X = 1\n",
			[]string{"conformance-athena", "conformance-helios", "gpu-accounting-athena", "gpu-accounting-helios"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWorld(t)
			res := w.run(nil, "--plan", w.commit(c.path, c.content))
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			got := runSet(t, res.out)
			sort.Strings(c.want)
			if strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Errorf("run set %v, want %v\n%s", got, c.want, res.out)
			}
			// The plan names what changed for each row it covers: that is its map from a row to code.
			for _, r := range c.want {
				w.contains(res, "covered: "+r+" (cluster; gate ")
			}
		})
	}
}

// The script's timeout for a queued row is the row's run time plus the wait in the queue the operator allows,
// so a long wait does not eat the run's time (the real-engine row has the same two clocks). The expected
// values are the shape's timeout column, 150 minutes for the handover and 25 for the accounting rows, plus the wait.
func TestTheTimeoutIsTheRunTimePlusTheWaitInTheQueue(t *testing.T) {
	for _, c := range []struct{ row, test, want string }{
		{"alpha-athena-handover", "TestAlphaAthenaHandover", "-test.timeout 161m"}, // 150 + 600/60 + 1
		{"gpu-accounting-athena", "TestGPUAccounting", "-test.timeout 36m"},        // 25 + 600/60 + 1
	} {
		t.Run(c.row, func(t *testing.T) {
			w := newAlphaWorld(t)
			if res := w.run([]string{"LIVE_PENDING_MAX_SECONDS=600"}, "--run", c.row, "--yes"); res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			if args := w.stateFile("test-args-" + c.test); !strings.Contains(args, c.want) {
				t.Errorf("the test binary ran with %q, want %q", args, c.want)
			}
			if env := w.stateFile("test-env-" + c.test); !strings.Contains(env, "PENDING_MAX_SECONDS=600") {
				t.Errorf("the test did not get the wait as its first clock:\n%s", env)
			}
		})
	}
}

// Every environment that reaches a gpu-accounting row is the script's own: the caller's dry switch and record
// directory are removed, and the row's QOS and its first clock are set.
func TestTheAccountingRowsGetTheirQOSAndNoDryEnvironment(t *testing.T) {
	w := newAlphaWorld(t)
	res := w.run([]string{"HPCBURST_ALPHA_DRY=1", "HPCBURST_ALPHA_DRY_FAULT=node-two", "HPCBURST_ALPHA_ROW=gpu-accounting-helios", "HPCBURST_ALPHA_OUT=/caller/out"},
		"--run", "gpu-accounting-helios", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	env := w.stateFile("test-env-TestGPUAccounting")
	for _, bad := range []string{"HPCBURST_ALPHA_DRY", "HPCBURST_ALPHA_ROW", "HPCBURST_ALPHA_OUT"} {
		if strings.Contains(env, bad) {
			t.Errorf("%s reached a live accounting run:\n%s", bad, env)
		}
	}
	for _, want := range []string{"HPCBURST_CONF_QOS=now", "HPCBURST_CONF_PENDING_MAX_SECONDS=7200", "HPCBURST_CONF_PARTITION=plgrid-gpu-gh200", "HPCBURST_CONF_GPUS=1"} {
		if !strings.Contains(env, want) {
			t.Errorf("the test did not get %s:\n%s", want, env)
		}
	}
	// A row with no QOS gets none.
	w2 := newAlphaWorld(t)
	if res := w2.run(nil, "--run", "gpu-accounting-athena", "--yes"); res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	if env := w2.stateFile("test-env-TestGPUAccounting"); strings.Contains(env, "HPCBURST_CONF_QOS") {
		t.Errorf("a QOS reached a row that has none:\n%s", env)
	}
}

// In ssh mode the dial-home key's path reaches the test; in direct mode no key does.
func TestTheDialHomeKeyReachesTheTestInSSHModeOnly(t *testing.T) {
	w := newAlphaWorld(t)
	key := filepath.Join(w.dir, "dialhome-key")
	w.alphaSettings(func(m map[string]string) {
		m["LIVE_DIALHOME_MODE"] = "ssh"
		m["LIVE_DIALHOME_KEY"] = key
	})
	if res := w.run(nil, "--run", "tunnel-hour-athena", "--yes"); res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	env := w.stateFile("test-env-TestTunnelHour")
	for _, want := range []string{"HPCBURST_ALPHA_DIALHOME_MODE=ssh", "HPCBURST_ALPHA_DIALHOME_KEY=" + key} {
		if !strings.Contains(env, want) {
			t.Errorf("the test did not get %s:\n%s", want, env)
		}
	}
	w2 := newAlphaWorld(t)
	if res := w2.run(nil, "--run", "tunnel-hour-athena", "--yes"); res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	if env := w2.stateFile("test-env-TestTunnelHour"); strings.Contains(env, "DIALHOME_KEY") {
		t.Errorf("a key reached a direct-mode run:\n%s", env)
	}
}
