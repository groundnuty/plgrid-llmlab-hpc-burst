package quickstart

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
)

// The commands the quickstart tells a user to run are checked against the real thing: the
// hpcburst binary, built from cmd/hpcburst and asked with -h, and the hack/ scripts, run with an
// environment that stops them right after they have read their arguments. Nothing here reads a
// flag definition out of the source text (PR #170 review, nit).

var (
	buildOnce sync.Once
	builtDir  string
	builtBin  string
	buildErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if builtDir != "" {
		os.RemoveAll(builtDir)
	}
	os.Exit(code)
}

// hpcburst builds cmd/hpcburst once and returns the binary's path. The binary sits alone in its
// directory, so `up` finds no hpcburst-controller beside it and starts no controllers.
func hpcburst(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		builtDir, buildErr = os.MkdirTemp("", "qsbin")
		if buildErr != nil {
			return
		}
		builtBin = filepath.Join(builtDir, "hpcburst")
		cmd := exec.Command("go", "build", "-buildvcs=false", "-o", builtBin, "./cmd/hpcburst")
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build ./cmd/hpcburst: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return builtBin
}

// run executes a program with exactly the given environment, in an empty directory of its own,
// and returns its combined output and its exit code (-1 when it did not start). A program that
// writes a relative path writes it there and not into the repository.
func run(t *testing.T, env []string, prog string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(prog, args...)
	cmd.Dir = t.TempDir()
	cmd.Env = env
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return out.String(), 0
	case errors.As(err, &ee):
		return out.String(), ee.ExitCode()
	}
	t.Fatalf("run %s: %v", prog, err)
	return "", -1
}

func baseEnv(home string) []string {
	return []string{"HOME=" + home, "PATH=" + os.Getenv("PATH")}
}

// dirs are the values the quickstart's STATE and BIN take while its commands are parsed. They are
// not the launcher's defaults, so a command that leaves a directory to the launcher's default
// does not agree with them.
type dirs struct{ state, bin string }

// command is one line of a shell block that starts a program of the repository (bin/... or
// hack/...), with its words as the shell sees them.
type command struct {
	line  string
	words []string
}

// shellWords expands a command line the way a shell does, with STATE and BIN set, and returns its
// words. The quickstart's placeholders get values first. noglob keeps a pattern a word.
func shellWords(t *testing.T, line string, d dirs, home string) []string {
	t.Helper()
	script := `set -f; STATE=$1; BIN=$2; eval "set -- $LINE"; printf '%s\0' "$@"`
	cmd := exec.Command("bash", "-c", script, "bash", d.state, d.bin)
	cmd.Env = append(baseEnv(home), "LINE="+fill(line))
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("the shell cannot read %q: %v", line, err)
	}
	return strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00")
}

// joinContinuations joins the lines that end in a backslash with the line after them.
func joinContinuations(text string) []string {
	var out []string
	cur := ""
	for _, l := range strings.Split(text, "\n") {
		if strings.HasSuffix(l, "\\") {
			cur += strings.TrimSuffix(l, "\\") + " "
			continue
		}
		out = append(out, cur+l)
		cur = ""
	}
	if strings.TrimSpace(cur) != "" {
		out = append(out, cur)
	}
	return out
}

// repoCommands returns the command lines of the quickstart's shell blocks that start with a
// program of the repository.
func repoCommands(t *testing.T, d dirs, home string) []command {
	t.Helper()
	var out []command
	for _, b := range blocks(t) {
		if b.lang != "" {
			continue
		}
		for _, l := range joinContinuations(b.text) {
			l = strings.TrimSpace(l)
			if prog := strings.Fields(l + " x")[0]; strings.HasPrefix(prog, "bin/") || strings.HasPrefix(prog, "hack/") {
				out = append(out, command{line: l, words: shellWords(t, l, d, home)})
			}
		}
	}
	if len(out) == 0 {
		t.Fatal("found no bin/ or hack/ command in the quickstart's shell blocks")
	}
	return out
}

func testDirs(t *testing.T) (dirs, string) {
	t.Helper()
	home, err := os.MkdirTemp("", "qsh")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(home) })
	return dirs{state: filepath.Join(home, "st"), bin: filepath.Join(home, "bn")}, home
}

// helpOf runs `hpcburst <sub> -h` and returns its output, its exit code and the flags it lists.
// -h runs nothing: the subcommand parses its flags, sees the request for help and returns. The
// flags of the quickstart are never passed to the binary, since a flag that takes a value would
// swallow -h and start the command.
func helpOf(t *testing.T, home, sub string) (string, int, map[string]bool) {
	t.Helper()
	out, code := run(t, baseEnv(home), hpcburst(t), sub, "-h")
	set := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^\s+-([a-z][a-z0-9-]*)\b`).FindAllStringSubmatch(out, -1) {
		set[m[1]] = true
	}
	return out, code, set
}

// helpFlags returns the flags a subcommand lists in its -h output, and fails when the binary does
// not know the subcommand.
func helpFlags(t *testing.T, home, sub string) map[string]bool {
	t.Helper()
	out, code, set := helpOf(t, home, sub)
	if code != 0 || len(set) == 0 {
		t.Fatalf("hpcburst %s -h exits %d and lists %d flags:\n%s", sub, code, len(set), out)
	}
	return set
}

// checkHpcburst asks the real binary whether a subcommand and its flags exist: the subcommand
// must answer -h, and each flag in args must be one its -h output lists.
func checkHpcburst(t *testing.T, home string, args []string) {
	t.Helper()
	if len(args) == 0 {
		t.Errorf("an hpcburst command names no subcommand")
		return
	}
	out, code, set := helpOf(t, home, args[0])
	if code != 0 || len(set) == 0 {
		t.Errorf("hpcburst %s: the binary has no such subcommand (-h exits %d):\n%s", args[0], code, out)
		return
	}
	for _, a := range args[1:] {
		if !strings.HasPrefix(a, "-") {
			continue
		}
		name, _, _ := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if name == "h" || name == "help" { // the flag package's own request for help
			continue
		}
		if !set[name] {
			t.Errorf("hpcburst %s: the binary lists no flag %s", args[0], a)
		}
	}
}

// Every hpcburst command in the quickstart's blocks, and every `hpcburst ...` or bare-flag span
// in its prose, is accepted by the real binary: a misspelt subcommand or flag fails. The words are
// compared with the binary's own -h output.
func TestHpcburstCommandsAreAcceptedByTheBinary(t *testing.T) {
	d, home := testDirs(t)
	n := 0
	for _, c := range repoCommands(t, d, home) {
		if !strings.HasPrefix(c.words[0], "bin/") {
			continue
		}
		name := strings.TrimPrefix(c.words[0], "bin/")
		if fi, err := os.Stat(filepath.Join(root, "cmd", name)); err != nil || !fi.IsDir() {
			t.Errorf("%q runs %s, but cmd/%s is not a program of this repository", c.line, c.words[0], name)
			continue
		}
		if name != "hpcburst" {
			continue
		}
		if len(c.words) < 2 {
			t.Errorf("%q names no subcommand", c.line)
			continue
		}
		n++
		checkHpcburst(t, home, c.words[1:])
	}
	if n == 0 {
		t.Fatal("found no bin/hpcburst command in the quickstart's shell blocks")
	}

	// The prose: spans outside the fenced blocks.
	prose := fence.ReplaceAllString(readRepoFile(t, "docs/quickstart.md"), "")
	spans := 0
	for _, m := range regexp.MustCompile("`([^`\n]+)`").FindAllStringSubmatch(prose, -1) {
		f := strings.Fields(m[1])
		switch {
		case f[0] == "hpcburst" || f[0] == "bin/hpcburst":
			spans++
			checkHpcburst(t, home, f[1:])
		case strings.HasPrefix(f[0], "--"):
			// A bare flag belongs to the command of the step it sits in. In this document that is
			// `hpcburst up`, which takes the flags that start a host.
			spans++
			checkHpcburst(t, home, append([]string{"up"}, f...))
		default:
			for _, w := range f[1:] {
				if strings.HasPrefix(w, "--") {
					t.Errorf("the span %q holds the flag %s, and the test cannot tell which command it belongs to: start the span with the command, or with the flag", m[1], w)
				}
			}
		}
	}
	if spans == 0 {
		t.Fatal("found no hpcburst command or flag span in the quickstart's prose")
	}
}

// What each hack/ script prints when it gets past reading its arguments and stops on the
// environment given here, instead of downloading or building anything. A usage error prints
// "usage:" and none of these.
var scriptStops = map[string]struct {
	env     func(t *testing.T) []string // added to the base environment
	reached []string                    // one of these in the output shows the arguments were accepted
}{
	"hack/fetch-frp.sh": {
		env: func(t *testing.T) []string {
			return []string{"FRP_PINS_FILE=/nonexistent/frp-pins", "FRP_TARBALL=/nonexistent/frp.tar.gz"}
		},
		reached: []string{"is not a file", "only linux_amd64 is pinned"},
	},
	"hack/fetch-k3s.sh": {
		env:     func(t *testing.T) []string { return []string{"K3S_PINS_FILE=/nonexistent/k3s-pins"} },
		reached: []string{"cannot read", "no pinned k3s binary"},
	},
	"hack/build-agents.sh": {
		// A go that fails: the script has read its arguments and checked the architectures, and
		// stops at the toolchain check, before it creates anything.
		env: func(t *testing.T) []string {
			stub := t.TempDir()
			if err := os.WriteFile(filepath.Join(stub, "go"), []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			return []string{"PATH=" + stub + string(os.PathListSeparator) + os.Getenv("PATH")}
		},
		reached: []string{"nothing was built"},
	},
}

// Every hack/ script command in the quickstart's blocks is accepted by the script itself, with
// the arguments the quickstart gives it.
func TestScriptCommandsAreAcceptedByTheScripts(t *testing.T) {
	d, home := testDirs(t)
	seen := map[string]bool{}
	for _, c := range repoCommands(t, d, home) {
		if !strings.HasPrefix(c.words[0], "hack/") {
			continue
		}
		stop, ok := scriptStops[c.words[0]]
		if !ok {
			t.Errorf("%q: the test has no way to run %s without side effects; add it to scriptStops", c.line, c.words[0])
			continue
		}
		seen[c.words[0]] = true
		t.Run(c.line, func(t *testing.T) {
			if _, err := os.Stat(filepath.Join(root, c.words[0])); err != nil {
				t.Fatal(err)
			}
			env := append(baseEnv(home), stop.env(t)...)
			script, err := filepath.Abs(filepath.Join(root, c.words[0]))
			if err != nil {
				t.Fatal(err)
			}
			out, code := run(t, env, script, c.words[1:]...)
			if strings.Contains(out, "usage:") {
				t.Errorf("the script refuses the arguments:\n%s", out)
			}
			if code == 0 {
				t.Errorf("the script ran to the end (exit 0) under an environment built to stop it:\n%s", out)
			}
			found := false
			for _, r := range stop.reached {
				found = found || strings.Contains(out, r)
			}
			if !found {
				t.Errorf("the script stopped for another reason than the one the test provoked (want one of %q):\n%s", stop.reached, out)
			}
			if entries, _ := filepath.Glob(filepath.Join(c.words[len(c.words)-1], ".build.*")); len(entries) > 0 {
				t.Errorf("the script left its build stage behind: %v", entries)
			}
		})
	}
	for _, s := range []string{"hack/fetch-k3s.sh", "hack/fetch-frp.sh", "hack/build-agents.sh"} {
		if !seen[s] {
			t.Errorf("the quickstart no longer runs %s", s)
		}
	}
}
