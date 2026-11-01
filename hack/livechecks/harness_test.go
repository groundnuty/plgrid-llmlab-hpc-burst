package livechecks

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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
	return map[string]string{
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
}

type world struct {
	t                     *testing.T
	repo, dir, state, bin string
	settings, base, tmp   string
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
func newWorld(t *testing.T) *world {
	t.Helper()
	for _, tool := range []string{"git", "go", "flock", "timeout", "tar", "bash"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatalf("the script tests need %s on PATH: %v", tool, err)
		}
	}
	script, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	w := &world{t: t, dir: t.TempDir()}
	w.repo, w.state, w.bin, w.tmp = filepath.Join(w.dir, "repo"), filepath.Join(w.dir, "state"), filepath.Join(w.dir, "bin"), filepath.Join(w.dir, "tmp")
	for _, d := range []string{w.repo, w.state, w.bin, w.tmp, filepath.Join(w.dir, "locks")} {
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
	t.Cleanup(func() { _ = os.RemoveAll(w.tmp) })
	return w
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
		if strings.HasPrefix(e, "HPCBURST_") || strings.HasPrefix(e, "HB_") || strings.HasPrefix(e, "LIVE_") || strings.HasPrefix(e, "STUB_") {
			continue
		}
		env = append(env, e)
	}
	env = append(env, "PATH="+w.bin+":"+os.Getenv("PATH"), "HPCBURST_LIVE_SETTINGS="+w.settings, "STUB_STATE="+w.state,
		"LIVE_POLL_SECONDS=0", "LIVE_CLEAN_POLL_SECONDS=0", "TMPDIR="+w.tmp, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	return append(env, extra...)
}

func (w *world) command(extra []string, args ...string) *exec.Cmd {
	cmd := exec.Command("bash", append([]string{filepath.Join(w.repo, "hack/live-checks.sh")}, args...)...)
	cmd.Dir = w.repo
	cmd.Env = w.env(extra)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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
	w.t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
	return cmd, &out
}

// leftoverDeadline bounds how long a member of the run's process group may outlive the script.
// The script's last children (a subshell, a command substitution, a timeout and what it killed)
// can still be exiting, or be waiting for init to reap them, when the script's own exit is
// seen, and that takes milliseconds, a little longer on a loaded host. A run that leaves a
// process behind leaves it for good, so it is still there when the deadline passes.
const leftoverDeadline = 5 * time.Second

// groupExists reports whether process group pgid has a member, a zombie included. Only ESRCH
// means the group is gone: EPERM, or any other error, means something is there.
func groupExists(pgid int) bool {
	return !errors.Is(syscall.Kill(-pgid, 0), syscall.ESRCH)
}

// groupOutlives polls process group pgid until it is empty and returns false, or returns true
// when a member is still there at the deadline. It waits on the group's state, never on a
// fixed window: an empty group ends the wait at once.
func groupOutlives(pgid int, deadline time.Duration) bool {
	end := time.Now().Add(deadline)
	for groupExists(pgid) {
		if time.Now().After(end) {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// noLeftoverProcess fails when a process of the run's group is still alive leftoverDeadline
// after the script exited, and kills what is left.
func (w *world) noLeftoverProcess(cmd *exec.Cmd) {
	w.t.Helper()
	pgid := cmd.Process.Pid
	if groupOutlives(pgid, leftoverDeadline) {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		w.t.Errorf("a process of the run was still alive %s after the script exited", leftoverDeadline)
	}
}

// noLeak fails when the screen output carries a seeded secret or a private path.
func (w *world) noLeak(out string) {
	w.t.Helper()
	for _, s := range append(append([]string{}, leakSeeds...), w.dir, os.TempDir()+"/") {
		if strings.Contains(out, s) {
			w.t.Errorf("the output carries %q:\n%s", s, out)
		}
	}
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
