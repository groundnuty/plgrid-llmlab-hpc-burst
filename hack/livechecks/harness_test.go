package livechecks

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The leak seeds. The stub cluster and the stub test binary print these in
// every reply they give, so an output rule that holds is one that would catch
// them; with nothing seeded, "the output is clean" would be an absence check
// that proves nothing.
var leakSeeds = []string{"SEEDACCT", "424242", "seedhost01", "seed.example", "SEEDTOKEN", "SEEDKEY"}

const fixtureGoMod = "module example.test/fx\n\ngo 1.21\n"

// fixtureFiles is a miniature of the module: the five packages the script's
// table names, a dependency of each kind, and files that belong to none.
func fixtureFiles() map[string]string {
	m := map[string]string{
		"go.mod":                                     fixtureGoMod,
		"internal/front/front.go":                    "package front\n\nimport (\n\t_ \"embed\"\n\n\t_ \"example.test/fx/pkg/provider\"\n)\n\n//go:embed embedded.md\nvar doc string\n",
		"internal/front/embedded.md":                 "embedded prose\n",
		"internal/front/front_test.go":               "package front\n\nimport \"testing\"\n\nfunc TestLLMLabThroughTheFront(t *testing.T) {}\n",
		"internal/front/README.md":                   "front\n",
		"pkg/provider/provider.go":                   "package provider\n",
		"pkg/hubfiles/hubfiles.go":                   "package hubfiles\n",
		"pkg/hubfiles/golden_test.go":                "package hubfiles\n\nimport \"testing\"\n\nfunc TestGoldenHub(t *testing.T) {}\n",
		"internal/providerctl/providerctl.go":        "package providerctl\n\nimport _ \"example.test/fx/internal/slurm\"\n",
		"internal/slurm/slurm.go":                    "package slurm\n",
		"internal/slurm/slurm_test.go":               "package slurm\n\nimport \"testing\"\n\nfunc TestSlurm(t *testing.T) {}\n",
		"internal/providerctl/conformance/c_test.go": "package conformance\n\nimport (\n\t\"testing\"\n\n\t_ \"example.test/fx/internal/providerctl\"\n)\n\nfunc TestConformance(t *testing.T) {}\n",
		"cmd/hb-agent/main.go":                       "package main\n\nimport _ \"example.test/fx/internal/agent\"\n\nfunc main() {}\n",
		"cmd/hb-agent/realengine_test.go":            "package main\n\nimport \"testing\"\n\nfunc TestRealEngine(t *testing.T) {}\n",
		"internal/agent/agent.go":                    "package agent\n\nimport \"embed\"\n\n//go:embed data.txt\nvar data string\n\n//go:embed assets/*.txt\nvar assets embed.FS\n",
		"internal/agent/assets/a.txt":                "asset a\n",
		"internal/agent/assets/b.txt":                "asset b\n",
		"internal/agent/arm64.go":                    "//go:build linux && arm64\n\npackage agent\n\nimport _ \"example.test/fx/internal/armonly\"\n",
		"internal/armonly/armonly.go":                "package armonly\n",
		"spikes/s7/checkpoints.env":                  "REV=1\n",
		"spikes/s7/results/run1/hub/a.json":          "{}\n",
		"spikes/s7/results/run1/hash-a.jsonl":        "{}\n",
		"spikes/s7/results/run1/other.txt":           "not an input\n",
		"spikes/s7/README.md":                        "readme\n",
		"internal/agent/data.txt":                    "embedded\n",
		"internal/agent/notes.txt":                   "not embedded\n",
		"unrelated/unrelated.go":                     "package unrelated\n",
		"docs/notes.md":                              "notes\n",
		"profiles/helios.yaml":                       "name: helios\n",
		"profiles/athena.yaml":                       "name: athena\n",
		"spikes/s94/job.sh":                          "#!/bin/bash\n",
		"spikes/s94/README.md":                       "readme\n",
	}
	for p, c := range alphaFixtureFiles() {
		m[p] = c
	}
	for p, c := range rehearsalFixtureFiles() {
		m[p] = c
	}
	return m
}

type world struct {
	t                     *testing.T
	repo, dir, state, bin string
	settings, base, tmp   string
	// day is the one UTC day of this world, read from its clock once, when the world is made. The
	// owner's-go settings of a fixture hold it, and the script's idea of today is it too: w.env gives the
	// script LIVE_FAKE_DATE_FILE, a file with it. Two readings of the real clock, one by the fixture
	// and one by the script's `date -u`, are two clocks, and a test that spans UTC midnight between them
	// fails (issue #422).
	day      string
	reports  map[*exec.Cmd]string // the reaper's report of each command built by command
	leakSink errorfer             // tests only: where noLeak reports; nil means t
}

// errorfer is the part of *testing.T that noLeak reports through.
type errorfer interface {
	Helper()
	Errorf(format string, args ...any)
}

func (w *world) out() string { return filepath.Join(w.dir, "out") }

func write(t *testing.T, p, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@example.test", "-c", "init.defaultBranch=main"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// newWorld builds the fixture repository with the real script in it, the stub
// commands, a stub cluster state and a settings file that is complete.
func newWorld(t *testing.T) *world { return newWorldAt(t, time.Now) }

// newWorldAt is newWorld with the clock that gives the world its day. A test that pins a boundary passes
// a clock of its own; every other test takes the real one.
func newWorldAt(t *testing.T, clock func() time.Time) *world {
	t.Helper()
	for _, tool := range []string{"git", "go", "flock", "timeout", "tar", "bash", "python3"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatalf("the script tests need %s on PATH: %v", tool, err)
		}
	}
	script, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	w := &world{t: t, dir: t.TempDir(), reports: map[*exec.Cmd]string{}, day: clock().UTC().Format("2006-01-02")}
	w.repo, w.state, w.bin, w.tmp = filepath.Join(w.dir, "repo"), filepath.Join(w.dir, "state"), filepath.Join(w.dir, "bin"), filepath.Join(w.dir, "tmp")
	for _, d := range []string{w.repo, w.state, w.bin, w.tmp, filepath.Join(w.dir, "locks"), filepath.Join(w.dir, "reports")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for p, c := range fixtureFiles() {
		write(t, filepath.Join(w.repo, p), c, 0o644)
	}
	write(t, filepath.Join(w.repo, "hack/live-checks.sh"), string(script), 0o755)
	git(t, w.repo, "init", "-q")
	git(t, w.repo, "add", "-A")
	git(t, w.repo, "commit", "-q", "-m", "base")
	w.base = git(t, w.repo, "rev-parse", "HEAD")
	w.installStubs()
	w.writeSettings(nil)
	write(t, w.dayFile(), w.day+"\n", 0o644)
	t.Cleanup(func() { _ = os.RemoveAll(w.tmp) })
	return w
}

// dayFile is the file the script reads its today from (LIVE_FAKE_DATE_FILE, a seam of its own).
func (w *world) dayFile() string { return filepath.Join(w.dir, "fixture-today") }

// dayPlus is the world's day moved by n days: yesterday is -1, tomorrow 1.
func (w *world) dayPlus(n int) string {
	w.t.Helper()
	d, err := time.Parse("2006-01-02", w.day)
	if err != nil {
		w.t.Fatal(err)
	}
	return d.AddDate(0, 0, n).Format("2006-01-02")
}

// commitFile changes (or, with content "", deletes) one file in a new commit and returns the commit.
func (w *world) commitFile(path, content string) string {
	w.t.Helper()
	if content == "" {
		git(w.t, w.repo, "rm", "-q", path)
	} else {
		write(w.t, filepath.Join(w.repo, path), content, 0o644)
		git(w.t, w.repo, "add", "-A")
	}
	git(w.t, w.repo, "commit", "-q", "-m", "change "+path)
	return git(w.t, w.repo, "rev-parse", "HEAD")
}

// commit changes one file and returns the range from the base to the new commit.
func (w *world) commit(path, content string) string {
	w.t.Helper()
	write(w.t, filepath.Join(w.repo, path), content, 0o644)
	git(w.t, w.repo, "add", "-A")
	git(w.t, w.repo, "commit", "-q", "-m", "change "+path)
	return w.base + ".." + git(w.t, w.repo, "rev-parse", "HEAD")
}

func (w *world) writeSettings(mutate func(m map[string]string)) {
	w.t.Helper()
	llmlab := filepath.Join(w.dir, "llmlab.json")
	m := map[string]string{
		"LIVE_OUT_DIR":                 filepath.Join(w.dir, "out"),
		"LIVE_LOCK_DIR":                filepath.Join(w.dir, "locks"),
		"LIVE_LLMLAB_CREDENTIALS":      llmlab,
		"LIVE_CONF_CREDENTIALS_HELIOS": filepath.Join(w.dir, "helios.ssh"),
		"LIVE_CONF_CREDENTIALS_ATHENA": filepath.Join(w.dir, "athena.ssh"),
		"ACCOUNT_HELIOS_CPU":           "SEEDACCT-cpu",
		"ACCOUNT_HELIOS_GH200":         "SEEDACCT-gh",
		"ACCOUNT_ATHENA_A100":          "SEEDACCT-a100",
	}
	for _, p := range []string{llmlab, m["LIVE_CONF_CREDENTIALS_HELIOS"], m["LIVE_CONF_CREDENTIALS_ATHENA"]} {
		write(w.t, p, "key SEEDKEY\n", 0o600)
	}
	if mutate != nil {
		mutate(m)
	}
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("# a settings file\n")
	for _, k := range keys {
		if m[k] != "" {
			b.WriteString(k + "=" + m[k] + "\n")
		}
	}
	w.settings = filepath.Join(w.dir, "settings.env")
	write(w.t, w.settings, b.String(), 0o600)
	if err := os.Chmod(w.settings, 0o600); err != nil {
		w.t.Fatal(err)
	}
}

type result struct {
	out  string
	code int
}

func (w *world) env(extra []string) []string {
	var env []string
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "HPCBURST_") || strings.HasPrefix(e, "HB_") || strings.HasPrefix(e, "LIVE_") || strings.HasPrefix(e, "STUB_") || strings.HasPrefix(e, "REHEARSAL_") {
			continue
		}
		env = append(env, e)
	}
	env = append(env, "PATH="+w.bin+":"+os.Getenv("PATH"), "HPCBURST_LIVE_SETTINGS="+w.settings, "STUB_STATE="+w.state,
		"LIVE_POLL_SECONDS=0", "LIVE_CLEAN_POLL_SECONDS=0", "TMPDIR="+w.tmp, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
		// The script's today is the world's day, not the real clock's (see world.day). A test's own entry
		// below wins: the last of two for one name is the one the script gets.
		"LIVE_FAKE_DATE_FILE="+w.dayFile())
	return append(env, extra...)
}

// command builds the command that runs the script under the reaper (reaper_test.go). Its process
// is the reaper: a signal sent to it is forwarded to the script, and its exit status is the
// script's. noLeftoverProcess reads the reaper's report of this command.
func (w *world) command(extra []string, args ...string) *exec.Cmd {
	report := filepath.Join(w.dir, "reports", strconv.Itoa(len(w.reports)+1))
	cmd := reaperCommand(w.t, report, reaperOpts{grace: leftoverDeadline}, w.repo, w.env(extra), append([]string{filepath.Join(w.repo, "hack/live-checks.sh")}, args...)...)
	w.reports[cmd] = report
	return cmd
}

func exitCode(err error) int {
	var ee *exec.ExitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &ee):
		return ee.ExitCode()
	}
	return -1
}

// run runs the script to its end.
func (w *world) run(extra []string, args ...string) result {
	w.t.Helper()
	cmd := w.command(extra, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	w.noLeak(out.String())
	w.noLeftoverProcess(cmd)
	return result{out.String(), exitCode(err)}
}

// start runs the script in the background, for a test that signals it.
func (w *world) start(extra []string, args ...string) (*exec.Cmd, *bytes.Buffer) {
	w.t.Helper()
	cmd := w.command(extra, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		w.t.Fatal(err)
	}
	// No cleanup here: reaperCommand's closes the pipe to the reaper, which kills and reaps what
	// is left of the run, descendants in other sessions included.
	return cmd, &out
}

// stopBound bounds a test's wait for a script it has stopped (issue #494). A run that stops normally takes seconds, a loaded host
// some more; past it the script is not stopping, and the test says what it was doing instead of waiting for the package's timeout.
const stopBound = 60 * time.Second

// rehearsalStopBound bounds the wait for a stopped rehearsal row. Its script waits up to REHEARSAL_STOP_STEPS x 0.2 s (90 s) between TERM
// and KILL for rehearsal.sh's teardown (hack/live-checks.sh), then the reaper gives what is left a grace and kills it (up to 2 x
// leftoverDeadline): a correct stop can take about 100 s, and stopBound would give up on it. This is that, and 20 s more.
// TestTheRehearsalStopBoundExceedsTheScriptsGrace reads the script's steps, so the bound cannot drift below them.
const rehearsalStopBound = 2 * time.Minute

// waitBounded waits for cmd for at most bound, and says whether it did not exit in time (then err is nil).
func waitBounded(cmd *exec.Cmd, bound time.Duration) (err error, timedOut bool) {
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err, false
	case <-time.After(bound):
		return nil, true
	}
}

// waitStopped waits for cmd, the reaper over a script that the test has signalled, for at most stopBound. When it does not exit in
// time, the test fails with the output so far and a dump of the process tree (hangDump). The reaper is stopped by the test's cleanup
// (reaperCommand), which closes its pipe so that it kills what is left. It returns what cmd.Wait returned.
func (w *world) waitStopped(cmd *exec.Cmd, out *bytes.Buffer) error {
	w.t.Helper()
	err, timedOut := waitBounded(cmd, stopBound)
	if timedOut {
		w.t.Fatalf("the script did not stop within %s of its stop signal; its output so far:\n%s\nthe process tree, as it is now:\n%s", stopBound, out.String(), hangDump(cmd.Process.Pid))
	}
	return err
}

// signal sends sig to the reaper, which forwards it to the script, and returns once the reaper
// has acknowledged the forward. A test that goes on to release what the script waits on needs
// this: forwarding is asynchronous, and without the acknowledgement the script could see the
// release before the signal.
func (w *world) signal(cmd *exec.Cmd, sig syscall.Signal) {
	w.t.Helper()
	report, ok := w.reports[cmd]
	if !ok {
		w.t.Fatal("the command was not built by world.command, so no reaper forwards its signals")
	}
	if err := cmd.Process.Signal(sig); err != nil {
		w.t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(ackPath(report)); err == nil {
			return
		}
		if time.Now().After(deadline) {
			w.t.Fatalf("the reaper never acknowledged forwarding %v", sig)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// leftoverDeadline bounds how long a process the script started may outlive it. The script's
// last children (a subshell, a command substitution, a timeout and what it killed) can still be
// exiting when the script's own exit is seen, and that takes milliseconds, a little longer on a
// loaded host. The check catches a process that is still alive when the deadline passes; one that
// exits within the grace passes. Five seconds is generous for ordinary teardown, and no guarantee
// against a host that starves the run for longer.
const leftoverDeadline = 5 * time.Second

// noLeftoverProcess fails when the reaper that ran cmd found a process of the run still alive
// leftoverDeadline after the script exited (in any process group or session; the reaper killed
// it), and when the reaper left no report: a missing report is never read as "clean".
func (w *world) noLeftoverProcess(cmd *exec.Cmd) {
	w.t.Helper()
	report, ok := w.reports[cmd]
	if !ok {
		w.t.Fatal("the command was not built by world.command, so no reaper watched it")
	}
	kind, details, err := readReport(report)
	switch {
	case err != nil:
		w.t.Errorf("%v", err)
	case kind == "clean":
	case kind == "leftover":
		w.t.Errorf("a process of the run was still alive %s after the script exited: %s", leftoverDeadline, strings.Join(details, ", "))
	default:
		w.t.Errorf("the reaper failed (%s): %s", kind, strings.Join(details, "; "))
	}
}

// noLeak fails when the screen output carries a seeded secret or a private path.
func (w *world) noLeak(out string) {
	w.t.Helper()
	var sink errorfer = w.t
	if w.leakSink != nil {
		sink = w.leakSink
	}
	for _, s := range leaks(out, append(append([]string{}, leakSeeds...), w.dir, os.TempDir()+"/")) {
		sink.Errorf("the output carries %q:\n%s", s, out)
	}
}

// hashRunMin is the shortest run of hex digits that counts as a hash: the
// output holds the plan's range, two 40-digit commit hashes, and a run's name
// with a 9-digit abbreviated one (hack/live-checks.sh, rev-parse --short=9).
// Git's shortest abbreviation is 7, but a 7- or 8-digit run is no hash this
// output prints, and one is how a seed hides: "job424242" has the run
// "b424242".
const hashRunMin = 9

// leaks returns the seeds the output carries. A seed made only of hex digits
// (a port, a job id) is not counted where it lies inside a hash: inside a
// maximal run of hex digits, bounded by other characters or the ends of the
// output, that is hashRunMin or more long and longer than the seed itself. Such runs are new on every run, and
// a hex-only seed turns up inside one by chance (#366: "40321" inside
// c0 40321 99613...). Anywhere else the seed is a leak, whatever its
// neighbours: ":40321", "probe40321", "jobs/job424242.out", "40321bytes". Every
// other seed is a plain substring: it holds a character no hash does.
func leaks(out string, seeds []string) []string {
	var found []string
	for _, s := range seeds {
		if containsSeed(out, s) {
			found = append(found, s)
		}
	}
	return found
}

func containsSeed(out, seed string) bool {
	if seed == "" {
		return false
	}
	if !isHex(seed) {
		return strings.Contains(out, seed)
	}
	for from := 0; ; {
		i := strings.Index(out[from:], seed)
		if i < 0 {
			return false
		}
		start := from + i
		lo, hi := start, start+len(seed)
		for lo > 0 && isHexDigit(out[lo-1]) {
			lo--
		}
		for hi < len(out) && isHexDigit(out[hi]) {
			hi++
		}
		// Hidden only if the run is a hash's length AND longer than the seed
		// itself: a run that IS the seed is a value, not a hash (a standalone
		// job id of 9 digits is a leak).
		if hi-lo < hashRunMin || hi-lo == len(seed) {
			return true
		}
		from = start + 1
	}
}

func isHexDigit(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		if !isHexDigit(s[i]) {
			return false
		}
	}
	return true
}

func (w *world) stateFile(name string) string {
	b, _ := os.ReadFile(filepath.Join(w.state, name))
	return string(b)
}

func (w *world) waitFor(name string) {
	w.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(w.state, name)); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	w.t.Fatalf("the stub never wrote %s", name)
}

func (w *world) installStubs() {
	for name, body := range map[string]string{
		"ssh":          stubSSH,
		"slurm/squeue": stubSqueue, "slurm/sbatch": stubSbatch, "slurm/sacct": stubSacct, "slurm/scancel": stubScancel,
	} {
		write(w.t, filepath.Join(w.bin, name), body, 0o755)
	}
	write(w.t, filepath.Join(w.state, "testbin.sh"), stubTestBinary, 0o755)
	write(w.t, filepath.Join(w.state, "queue"), "", 0o644)
	// The history of a job that never started, as sacct --json really answers: the real GH200 reply (issue #267, PR #269), read from
	// the tree each time, never copied here, so that the dry runs cannot drift from the form the parser's own tests use.
	real, err := os.ReadFile(filepath.Join(repoRoot(w.t), neverStartedReplyPath))
	if err != nil {
		w.t.Fatal(err)
	}
	write(w.t, filepath.Join(w.state, "sacct-never-started.json"), string(real), 0o644)
}

// useStubGo puts the stub go command first on PATH, for the tests of --run. --plan needs the real one.
func (w *world) useStubGo() { write(w.t, filepath.Join(w.bin, "go"), stubGo, 0o755) }

func lines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}
