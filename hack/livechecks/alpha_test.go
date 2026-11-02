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
	"regexp"
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
// uses) and the lease: 1 h 46 min for the handover rows, the shortest longer than the lead (1 h 45 min with a
// 5-minute request deadline, profiles/athena.yaml:39), and 2 h 17 min for the tunnel rows (a start of up to an
// hour, the hour, the 15-minute drain window and a minute, by default); not read from the script's table.
type alphaShape struct {
	row, test, host, shape string
	slot                   string // the one-job slot's token, "" for none
	account                string
	needsStack             bool
}

var alphaShapes = []alphaShape{
	{"alpha-athena-handover", "TestAlphaAthenaHandover", "athena", "partition plgrid-gpu-a100, 1 GPU, 16 CPU, 125Gi, time limit 01:46:00, at most 2 jobs at once, each of this shape, no requeue", "", "SEEDACCT-a100", true},
	{"alpha-helios-nooverlap", "TestAlphaHeliosNoOverlap", "helios", "partition plgrid-gpu-gh200, qos now, 1 GPU, 72 CPU, 120000M, time limit 01:46:00, at most 2 jobs at once, each of this shape, no requeue", "now", "SEEDACCT-gh", true},
	{"tunnel-hour-helios", "TestTunnelHour", "helios", "partition plgrid-gpu-gh200, qos now, 1 GPU, 72 CPU, 120000M, time limit 02:17:00, one job, no requeue", "now", "SEEDACCT-gh", true},
	{"tunnel-hour-athena", "TestTunnelHour", "athena", "partition plgrid-gpu-a100, 1 GPU, 16 CPU, 125Gi, time limit 02:17:00, one job, no requeue", "", "SEEDACCT-a100", true},
	{"gpu-accounting-helios", "TestGPUAccounting", "helios", "partition plgrid-gpu-gh200, qos now, 1 GPU, 72 CPU, 117Gi, time limit 00:05:00, one job, no requeue, each job to start within 10 min", "now", "SEEDACCT-gh", false},
	{"gpu-accounting-athena", "TestGPUAccounting", "athena", "partition plgrid-gpu-a100, 1 GPU, 16 CPU, 125Gi, time limit 00:05:00, one job, no requeue", "", "SEEDACCT-a100", false},
}

// alphaStubGo stands in for go in an alpha row's prepare step: build into a directory, and test -c.
const alphaStubGo = `#!/bin/bash
printf '%s\n' "$*" >> "$STUB_STATE/go-argv"
echo "go $1" >> "$STUB_STATE/events"
out=""; prev=""
for a in "$@"; do [ "$prev" = -o ] && out=$a; prev=$a; done
case "$1" in
  build) mkdir -p "$out" && for b in hpcburst hpcburst-controller hpcburst-front; do echo binary > "$out/$b"; done
         [ -z "${STUB_ROLL_AT_BUILD:-}" ] || echo "$STUB_NEXT_DATE" > "$LIVE_FAKE_DATE_FILE" ;;
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
  midnight) echo "--- PASS: $t (0.01s)"; echo PASS; echo "$STUB_NEXT_DATE" > "$LIVE_FAKE_DATE_FILE" ;;
  late-hidden) echo "hb-late-bbbbbbbbbbbb" >> "${HPCBURST_ALPHA_OUT:-${HPCBURST_CONF_OUT:-/dev/null}}/job-tags.txt"; echo "9003|hb-late-bbbbbbbbbbbb|normal|plgrid|PENDING" >> "$STUB_STATE/queue"; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  not-run) echo "step: a job did not start within the start-wait bound of 10m0s; this run's jobs were cancelled by tag" > "${HPCBURST_ALPHA_OUT:-${HPCBURST_CONF_OUT:-/dev/null}}/not-run.txt"; echo "--- SKIP: $t (0.00s)"; echo PASS ;;
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
		m["LIVE_OWNER_GO_HELIOS_NOW"] = time.Now().UTC().Format("2006-01-02")
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
		if tl := time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second; tl != r.Lease {
			t.Errorf("%s: the shape's time limit %s is not the lease %s the run plans (the controller's Allocation time limit is the leaseLimit)", r.Name, tl, r.Lease)
		}
		// The optional 14th column is the start wait in minutes: the package's StartWait, and none where it has none.
		wantWait := ""
		if r.StartWait > 0 {
			wantWait = fmt.Sprint(int(r.StartWait / time.Minute))
		}
		if gotWait := func() string {
			if len(f) > 13 && f[13] != "-" {
				return f[13]
			}
			return ""
		}(); gotWait != wantWait {
			t.Errorf("%s: the shape's start wait is %q minutes, the package's row has %q", r.Name, gotWait, wantWait)
		}
		if tm := fmt.Sprint(int(r.Budget / time.Minute)); r.StartWait > 0 && f[11] != tm {
			t.Errorf("%s: the shape's timeout is %s minutes, the package's budget %s", r.Name, f[11], tm)
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

// What an alpha row needs is its own: the dial-home route for none of the accounting rows.
func TestSettingsAreAskedOnlyOfTheRowsThatNeedThem(t *testing.T) {
	w := newAlphaWorld(t)
	w.alphaSettings(func(m map[string]string) {
		delete(m, "LIVE_DIALHOME_MODE")
		delete(m, "LIVE_DIALHOME_ADDR")
		delete(m, "LIVE_BIN_DIR")
	})
	for _, row := range []string{"gpu-accounting-helios", "gpu-accounting-athena"} {
		if res := w.run(nil, "--run", row); res.code != 4 {
			t.Errorf("%s with no dial-home route: exit %d, want 4:\n%s", row, res.code, res.out)
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
	// The rehearsal row (issue #193) builds and runs the same stack, on this host: it is a local row.
	stackRows := append(append([]string{}, alphaRows...), "rehearsal-part3")
	cases := []struct {
		name, path, content string
		want                []string
	}{
		{"a package only the stack imports", "internal/stack/stack.go", "package stack\n\nvar X = 1\n", stackRows},
		{"the rows' own package", "internal/alpha/alpha.go", "package alpha\n\nvar X = 1\n", alphaRows},
		{"a stack binary", "cmd/hpcburst-front/main.go", "package main\n\nimport _ \"example.test/fx/internal/stack\"\n\nfunc main() { _ = 1 }\n", stackRows},
		{"the example the rows apply", "examples/provider-athena-a100.yaml", "kind: Provider\nx: 1\n", []string{"alpha-athena-handover"}},
		{"a package the accounting rows and conformance run, not the stack", "internal/providerctl/providerctl.go", "package providerctl\n\nimport _ \"example.test/fx/internal/slurm\"\n\nvar X = 1\n",
			[]string{"conformance-athena", "conformance-helios", "conformance-helios-gh200", "gpu-accounting-athena", "gpu-accounting-helios"}},
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
				kind := "cluster"
				if r == "rehearsal-part3" {
					kind = "local"
				}
				w.contains(res, "covered: "+r+" ("+kind+"; gate ")
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
		// Under a start-wait bound the bound is the first clock: 30 + 0 + 1, whatever the wait the operator allows.
		{"gpu-accounting-helios", "TestGPUAccounting", "-test.timeout 31m"},
	} {
		t.Run(c.row, func(t *testing.T) {
			w := newAlphaWorld(t)
			if res := w.run([]string{"LIVE_PENDING_MAX_SECONDS=600"}, "--run", c.row, "--yes"); res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			if args := w.stateFile("test-args-" + c.test); !strings.Contains(args, c.want) {
				t.Errorf("the test binary ran with %q, want %q", args, c.want)
			}
			env := w.stateFile("test-env-" + c.test)
			if c.row == "gpu-accounting-helios" {
				// The bound is its first clock; the operator's wait is not passed on, and does not lengthen the timeout.
				if !strings.Contains(env, "HPCBURST_CONF_START_WAIT=10m") || strings.Contains(env, "PENDING_MAX_SECONDS") {
					t.Errorf("the test did not get the start-wait bound alone as its first clock:\n%s", env)
				}
			} else if !strings.Contains(env, "PENDING_MAX_SECONDS=600") {
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
	for _, want := range []string{"HPCBURST_CONF_QOS=now", "HPCBURST_CONF_START_WAIT=10m", "HPCBURST_CONF_PARTITION=plgrid-gpu-gh200", "HPCBURST_CONF_GPUS=1"} {
		if !strings.Contains(env, want) {
			t.Errorf("the test did not get %s:\n%s", want, env)
		}
	}
	// The bound is its one clock: the wait in the queue the operator allows is the Athena row's.
	if strings.Contains(env, "HPCBURST_CONF_PENDING_MAX_SECONDS") {
		t.Errorf("a row with a start wait got a second clock:\n%s", env)
	}
	// A row with no QOS gets none.
	w2 := newAlphaWorld(t)
	if res := w2.run(nil, "--run", "gpu-accounting-athena", "--yes"); res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	env2 := w2.stateFile("test-env-TestGPUAccounting")
	if strings.Contains(env2, "HPCBURST_CONF_QOS") || strings.Contains(env2, "HPCBURST_CONF_START_WAIT") {
		t.Errorf("a QOS or a start wait reached a row that has none:\n%s", env2)
	}
	if !strings.Contains(env2, "HPCBURST_CONF_PENDING_MAX_SECONDS=7200") {
		t.Errorf("the row with no start wait did not get the wait in the queue:\n%s", env2)
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

// The rows that hold QOS now's GPU, the owner's shared slot, for hours need the owner's go on top of --yes
// (the development rule on #145). The expected hold is worked out by hand: the shortest lease, 1 h 46 min, and
// for the no-overlap row 45 minutes more for the successor to start and become ready, 2 h 31 min, which the
// script rounds up to ten minutes, 2 h 40 min; the hour's row holds its lease, by default 2 h 17 min (the GH200 measurement of 23 min 39 s is over the 20 minutes
// under which the short lease would be kept), rounded to 2 h 20 min.
func TestTheRowsThatHoldTheOwnersSlotForHoursNeedTheOwnersGo(t *testing.T) {
	today := time.Now().UTC().Format("2006-01-02")
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
	rows := []struct{ row, hold string }{
		{"alpha-helios-nooverlap", "holds QOS now's GPU for about 2 h 40 min; the owner's go is needed (the development rule on #145)"},
		{"tunnel-hour-helios", "holds QOS now's GPU for about 2 h 20 min; the owner's go is needed (the development rule on #145)"},
	}
	for _, c := range rows {
		for _, stale := range []struct{ name, value string }{{"missing", ""}, {"yesterday's", yesterday}, {"1", "1"}, {"yes", "yes"}} {
			t.Run(c.row+"/"+stale.name, func(t *testing.T) {
				w := newAlphaWorld(t)
				w.alphaSettings(func(m map[string]string) {
					if stale.value == "" {
						delete(m, "LIVE_OWNER_GO_HELIOS_NOW")
					} else {
						m["LIVE_OWNER_GO_HELIOS_NOW"] = stale.value
					}
				})
				res := w.run(nil, "--run", c.row, "--yes")
				if res.code != 3 {
					t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
				}
				w.contains(res, c.row+": "+c.hold, "setting LIVE_OWNER_GO_HELIOS_NOW must hold today's UTC date ("+today+")")
				if w.stateFile("go-argv") != "" || w.stateFile("sbatch-argv") != "" || w.stateFile("cancelled") != "" {
					t.Errorf("a refused run built, estimated or cancelled something: %s %s", w.stateFile("go-argv"), w.stateFile("sbatch-argv"))
				}
			})
		}
		t.Run(c.row+"/today's date", func(t *testing.T) {
			w := newAlphaWorld(t)
			// Without --yes it prints the hold before the estimate, and runs nothing.
			res := w.run(nil, "--run", c.row)
			if res.code != 4 {
				t.Fatalf("exit %d, want 4:\n%s", res.code, res.out)
			}
			w.contains(res, c.row+": "+c.hold, "queue estimate")
			if strings.Index(res.out, "the owner's go is needed") > strings.Index(res.out, "queue estimate") {
				t.Errorf("the hold is stated after the estimate:\n%s", res.out)
			}
			w.noSubmit()
		})
	}
	// The other rows hold no slot for hours: no go, and no note.
	for _, row := range []string{"alpha-athena-handover", "tunnel-hour-athena", "gpu-accounting-helios", "gpu-accounting-athena"} {
		t.Run(row+"/needs none", func(t *testing.T) {
			w := newAlphaWorld(t)
			w.alphaSettings(func(m map[string]string) { delete(m, "LIVE_OWNER_GO_HELIOS_NOW") })
			res := w.run(nil, "--run", row)
			if res.code != 4 {
				t.Fatalf("exit %d, want 4:\n%s", res.code, res.out)
			}
			if strings.Contains(res.out, "owner's go") {
				t.Errorf("a note about the owner's go on a row that holds no slot for hours:\n%s", res.out)
			}
		})
	}
}

// The script's hold and the package's are one fact each.
func TestTheScriptsSlotHoldIsThePackagesRoundedUp(t *testing.T) {
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, l := range block(t, string(b), "OWNER GO") {
		f := strings.Split(l, "|")
		if len(f) != 3 {
			t.Fatalf("owner-go row %q has %d fields, want 3", l, len(f))
		}
		got[f[0]] = f[2]
	}
	for _, r := range alpha.Rows() {
		min, ok := got[r.Name]
		wantHours := r.SlotHold > time.Hour // the rows that hold the slot for hours need the go
		if wantHours != ok {
			t.Errorf("%s: slot hold %s, in the owner-go table: %t", r.Name, r.SlotHold, ok)
			continue
		}
		if ok {
			rounded := int((r.SlotHold + 10*time.Minute - time.Nanosecond) / (10 * time.Minute) * 10)
			if min != fmt.Sprint(rounded) {
				t.Errorf("%s: the script holds %s minutes, the package's %s rounds up to %d", r.Name, min, r.SlotHold, rounded)
			}
		}
	}
}

// The owner's go is read again just before each protected row, not once at the start: an earlier row that runs
// past UTC midnight, or a build that does, leaves the go of another day. The date comes from a file the test
// changes (LIVE_FAKE_DATE_FILE, read each time), so the run crosses midnight between two rows.
func TestTheOwnersGoIsReadAgainBeforeEachProtectedRow(t *testing.T) {
	today := time.Now().UTC().Format("2006-01-02")
	tomorrow := time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02")
	withDate := func(t *testing.T, extra ...string) (*world, []string) {
		w := newAlphaWorld(t)
		f := filepath.Join(w.dir, "today")
		write(t, f, today+"\n", 0o644)
		return w, append([]string{"LIVE_FAKE_DATE_FILE=" + f, "STUB_NEXT_DATE=" + tomorrow}, extra...)
	}
	t.Run("the control: the date stays", func(t *testing.T) {
		w, env := withDate(t)
		res := w.run(env, "--run", "alpha-athena-handover", "tunnel-hour-helios", "--yes")
		if res.code != 0 || strings.Count(w.stateFile("events"), "testbin ") != 2 {
			t.Fatalf("exit %d, test runs %d, want 0 and 2:\n%s", res.code, strings.Count(w.stateFile("events"), "testbin "), res.out)
		}
	})
	t.Run("between two rows", func(t *testing.T) {
		w, env := withDate(t, "STUB_ALPHA_MODE=midnight")
		res := w.run(env, "--run", "alpha-athena-handover", "tunnel-hour-helios", "--yes")
		if res.code != 3 {
			t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
		}
		w.contains(res, "alpha-athena-handover: PASS", "setting LIVE_OWNER_GO_HELIOS_NOW must hold today's UTC date ("+tomorrow+")")
		if n := strings.Count(w.stateFile("events"), "testbin "); n != 1 {
			t.Errorf("%d test runs, want only the first row's", n)
		}
		// The refused row built nothing: only the first row's two go calls.
		if g := lines(w.stateFile("go-argv")); len(g) != 2 {
			t.Errorf("go was called %d times, want the first row's 2: %v", len(g), g)
		}
	})
	t.Run("during the row's own builds", func(t *testing.T) {
		w, env := withDate(t, "STUB_ROLL_AT_BUILD=1")
		res := w.run(env, "--run", "alpha-helios-nooverlap", "--yes")
		if res.code != 3 {
			t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
		}
		w.contains(res, "setting LIVE_OWNER_GO_HELIOS_NOW must hold today's UTC date ("+tomorrow+")")
		if strings.Contains(w.stateFile("events"), "testbin ") {
			t.Errorf("the test binary ran with yesterday's go")
		}
		// Built, estimated, but nothing submitted and nothing cancelled.
		if argv := lines(w.stateFile("sbatch-argv")); len(argv) != 1 || !strings.Contains(argv[0], "--test-only") {
			t.Errorf("sbatch calls %v, want only the estimate", argv)
		}
	})
}

// The script's most jobs and the package's are one fact each.
func TestTheScriptsMostJobsAreThePackagesRows(t *testing.T) {
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, l := range block(t, string(b), "MOST JOBS") {
		f := strings.Split(l, "|")
		if len(f) != 2 {
			t.Fatalf("most-jobs row %q has %d fields, want 2", l, len(f))
		}
		got[f[0]] = f[1]
	}
	for _, r := range append(alpha.Rows(), alpha.AccountingRows()...) {
		if got[r.Name] != fmt.Sprint(r.MostJobs) {
			t.Errorf("%s: the script allows %q jobs at once, the package's row %d", r.Name, got[r.Name], r.MostJobs)
		}
	}
}

// The tunnel rows' lease is the owner's choice, given with the go: LIVE_TUNNEL_LEASE_MINUTES, 106 (the lead an
// Instance needs plus a minute) to 240. The shape, the hold note, the timeout and the test's environment follow
// it. The expected values are by hand: 106 minutes is 01:46:00, a hold of 1 h 50 min once rounded up to ten
// minutes, and a timeout of 106 + 15 + the 120 minutes of the queue wait + 1 = 242; the default 137 minutes is 02:17:00,
// 2 h 20 min and 137 + 15 + 120 + 1 = 273.
func TestTheTunnelLeaseIsTheOwnersChoice(t *testing.T) {
	for _, c := range []struct {
		name, setting, limit, hold, timeout, env string
	}{
		{"the default", "", "02:17:00", "2 h 20 min", "273m", "HPCBURST_ALPHA_LEASE_MINUTES=137"},
		{"the short lease", "106", "01:46:00", "1 h 50 min", "242m", "HPCBURST_ALPHA_LEASE_MINUTES=106"},
		{"the longest", "240", "04:00:00", "4 h 00 min", "376m", "HPCBURST_ALPHA_LEASE_MINUTES=240"},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := newAlphaWorld(t)
			w.alphaSettings(func(m map[string]string) {
				if c.setting != "" {
					m["LIVE_TUNNEL_LEASE_MINUTES"] = c.setting
				}
			})
			res := w.run(nil, "--run", "tunnel-hour-helios")
			if res.code != 4 {
				t.Fatalf("exit %d, want 4:\n%s", res.code, res.out)
			}
			w.contains(res, "time limit "+c.limit, "holds QOS now's GPU for about "+c.hold+"; the owner's go is needed (the development rule on #145)")
			res = w.run(nil, "--run", "tunnel-hour-helios", "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			if env := w.stateFile("test-env-TestTunnelHour"); !strings.Contains(env, c.env) {
				t.Errorf("the test did not get %s:\n%s", c.env, env)
			}
			if args := w.stateFile("test-args-TestTunnelHour"); !strings.Contains(args, "-test.timeout "+strings.TrimSuffix(c.timeout, "m")+"m") {
				t.Errorf("the test ran with %q, want -test.timeout %s", args, c.timeout)
			}
		})
	}
	t.Run("a refusal quotes the hold the lease gives", func(t *testing.T) {
		w := newAlphaWorld(t)
		w.alphaSettings(func(m map[string]string) {
			m["LIVE_TUNNEL_LEASE_MINUTES"] = "106"
			delete(m, "LIVE_OWNER_GO_HELIOS_NOW")
		})
		res := w.run(nil, "--run", "tunnel-hour-helios", "--yes")
		if res.code != 3 {
			t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
		}
		w.contains(res, "holds QOS now's GPU for about 1 h 50 min")
	})
	for _, v := range []string{"105", "241", "abc", "1h46", "106.5", "-106"} {
		t.Run("refused: "+v, func(t *testing.T) {
			w := newAlphaWorld(t)
			w.alphaSettings(func(m map[string]string) { m["LIVE_TUNNEL_LEASE_MINUTES"] = v })
			res := w.run(nil, "--run", "tunnel-hour-athena", "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, "LIVE_TUNNEL_LEASE_MINUTES must be whole minutes from 106 to 240")
			if w.stateFile("go-argv") != "" || w.stateFile("sbatch-argv") != "" {
				t.Errorf("a refused run built or called the cluster")
			}
		})
	}
	t.Run("the other rows ignore it", func(t *testing.T) {
		w := newAlphaWorld(t)
		w.alphaSettings(func(m map[string]string) { m["LIVE_TUNNEL_LEASE_MINUTES"] = "abc" })
		res := w.run([]string{"HPCBURST_ALPHA_LEASE_MINUTES=999"}, "--run", "alpha-athena-handover", "--yes")
		if res.code != 0 {
			t.Fatalf("exit %d:\n%s", res.code, res.out)
		}
		if env := w.stateFile("test-env-TestAlphaAthenaHandover"); strings.Contains(env, "LEASE_MINUTES") {
			t.Errorf("a lease reached a row that takes none:\n%s", env)
		}
	})
}

// The script's bounds for the lease are the package's.
func TestTheScriptsLeaseBoundsArethePackagesBounds(t *testing.T) {
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]time.Duration{"TUNNEL_LEASE_MIN": alpha.MinLease(), "TUNNEL_LEASE_MAX": alpha.MaxTunnelLease} {
		m := regexp.MustCompile(`(?m)^` + name + `=(\d+)$`).FindSubmatch(b)
		if m == nil {
			t.Fatalf("the script has no %s", name)
		}
		if got := string(m[1]); got != fmt.Sprint(int(want/time.Minute)) {
			t.Errorf("%s = %s minutes, the package's bound is %d", name, got, int(want/time.Minute))
		}
	}
}
