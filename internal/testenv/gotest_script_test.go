package testenv

// hack/go-test.sh is what hack/check.sh and CI's go job run (issue #282). It runs one test on a
// timeout of its own and leaves that test out of the packages' run, so a mistake in it could drop the
// test without a word: go test exits 0 when -run matches nothing. These tests run the script with a
// double in front of the real go on PATH, which records its arguments and answers as a test case says.
// They start nothing else and run offline. The expected flags and the test's name are literals here,
// not read from the script.

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	ownTestName = "TestLifecycleManagerReleasesItsLeaseOnEveryStop"
	ownPassLine = "--- PASS: " + ownTestName + " (46.70s)\nPASS\nok  \texample/internal/lifecycle\t46.9s\n"
	wantPackage = "test -count=1 -v -skip ^" + ownTestName + "$ ./..."
	wantOwn     = "test -count=1 -v -timeout 150s -run ^" + ownTestName + "$ ./internal/lifecycle"
)

// goDouble is what the double answers: for the packages' run (told apart by its -skip) and for the
// test's own run.
type goDouble struct {
	packagesCode string
	ownOutput    string
	ownCode      string
}

// runGoTestScript runs the script with the double as go, and returns its output, its exit code and the
// double's calls, one line each.
func runGoTestScript(t *testing.T, d goDouble, args ...string) (string, int, []string) {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	ownOutput := filepath.Join(dir, "own-output")
	if err := os.WriteFile(ownOutput, []byte(d.ownOutput), 0o600); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$GO_DOUBLE_CALLS"
case "$*" in
  *-skip*) exit "$GO_DOUBLE_PACKAGES_CODE" ;;
  *) cat "$GO_DOUBLE_OWN_OUTPUT"; exit "$GO_DOUBLE_OWN_CODE" ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", append([]string{filepath.Join(root, "hack", "go-test.sh")}, args...)...)
	cmd.Env = append(os.Environ(),
		"PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"GO_DOUBLE_CALLS="+calls, "GO_DOUBLE_OWN_OUTPUT="+ownOutput,
		"GO_DOUBLE_PACKAGES_CODE="+d.packagesCode, "GO_DOUBLE_OWN_CODE="+d.ownCode)
	out, err := cmd.CombinedOutput()
	code := 0
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		code = exit.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(calls)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	var lines []string
	if s := strings.TrimSpace(string(raw)); s != "" {
		lines = strings.Split(s, "\n")
	}
	return string(out), code, lines
}

// The packages' run leaves out that one test by its exact, anchored name and keeps go test's default
// timeout; the test's own run has its own timeout, in its own package.
func TestGoTestScriptPassesTheExactFlags(t *testing.T) {
	for _, c := range []struct {
		mode string
		want string
	}{{"packages", wantPackage}, {"own", wantOwn}} {
		out, code, calls := runGoTestScript(t, goDouble{packagesCode: "0", ownOutput: ownPassLine, ownCode: "0"}, c.mode)
		if code != 0 || len(calls) != 1 || calls[0] != c.want {
			t.Errorf("%s: exit %d, go called %q, want once with %q; output:\n%s", c.mode, code, calls, c.want, out)
		}
	}
}

// The test's own run passes only when the test itself has passed, whatever go test exits with.
func TestGoTestScriptOwnNeedsTheTestsPassLine(t *testing.T) {
	const guard = "FAIL hack/go-test.sh: " + ownTestName + " did not run and pass"
	for _, c := range []struct {
		name, output, code string
		wantCode           int
		wantGuard          bool
	}{
		{"it passed", ownPassLine, "0", 0, false},
		{"it was renamed: -run matches nothing", "testing: warning: no tests to run\nPASS\nok  \texample\t0.1s [no tests to run]\n", "0", 1, true},
		{"it skipped", "--- SKIP: " + ownTestName + " (0.00s)\nPASS\nok  \texample\t0.1s\n", "0", 1, true},
		{"only a longer name passed", "--- PASS: " + ownTestName + "Again (1.00s)\nPASS\n", "0", 1, true},
		{"only a subtest passed", "    --- PASS: " + ownTestName + " (1.00s)\nPASS\n", "0", 1, true},
		{"it failed: go test's own exit is kept", "--- FAIL: " + ownTestName + " (1.00s)\nFAIL\n", "3", 3, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, code, _ := runGoTestScript(t, goDouble{packagesCode: "0", ownOutput: c.output, ownCode: c.code}, "own")
			if code != c.wantCode || strings.Contains(out, guard) != c.wantGuard {
				t.Errorf("exit %d, want %d; the guard's line shown: %v, want %v; output:\n%s", code, c.wantCode, strings.Contains(out, guard), c.wantGuard, out)
			}
			if !strings.Contains(out, c.output) {
				t.Errorf("go test's own output was not passed on, so CI's log would lose it:\n%s", out)
			}
		})
	}
}

// "all" runs both in order, the second even when the first fails, and fails if either does.
func TestGoTestScriptAllRunsBoth(t *testing.T) {
	const noMatch = "testing: warning: no tests to run\nPASS\n"
	for _, c := range []struct {
		name     string
		d        goDouble
		wantCode int
	}{
		{"both pass", goDouble{"0", ownPassLine, "0"}, 0},
		{"the packages fail", goDouble{"1", ownPassLine, "0"}, 1},
		{"the test fails", goDouble{"0", "--- FAIL: " + ownTestName + " (1.00s)\n", "1"}, 1},
		{"the test matched nothing", goDouble{"0", noMatch, "0"}, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, code, calls := runGoTestScript(t, c.d, "all")
			if code != c.wantCode || len(calls) != 2 || calls[0] != wantPackage || calls[1] != wantOwn {
				t.Errorf("exit %d, want %d; go called %q, want %q then %q; output:\n%s", code, c.wantCode, calls, wantPackage, wantOwn, out)
			}
		})
	}
}

func TestGoTestScriptRefusesAnUnknownMode(t *testing.T) {
	for _, args := range [][]string{nil, {"everything"}} {
		_, code, calls := runGoTestScript(t, goDouble{packagesCode: "0", ownOutput: ownPassLine, ownCode: "0"}, args...)
		if code != 2 || len(calls) != 0 {
			t.Errorf("%q: exit %d, go called %q; want exit 2 and no call, so a typo in a caller does not pass with no test run", args, code, calls)
		}
	}
}

// The name the script's -skip leaves out belongs to exactly one test in the root module, the one its
// own run runs, so that the -skip hides nothing else, in any package.
func TestGoTestScriptNamesOneRealTest(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	decl := regexp.MustCompile(`(?m)^func ` + ownTestName + `\(`)
	var found []string
	err = filepath.WalkDir(root, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			switch e.Name() {
			case ".git", "spikes", "third_party": // not the root module
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if decl.Match(src) {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			found = append(found, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || filepath.Dir(found[0]) != filepath.Join("internal", "lifecycle") {
		t.Fatalf("%s is declared in %q, want once, in internal/lifecycle", ownTestName, found)
	}
}

// CI's go job and hack/check.sh both go through the script, so the two cannot drift, and neither runs
// the packages by hand: that would run the test twice, or lose its own timeout.
func TestGoTestScriptIsWhatCIAndCheckRun(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	const byHand = "go test -count=1 -v ./..."
	for _, c := range []struct {
		file string
		want []string
	}{
		{filepath.Join(".github", "workflows", "ci.yml"), []string{"bash hack/go-test.sh packages", "bash hack/go-test.sh own"}},
		{filepath.Join("hack", "check.sh"), []string{"bash hack/go-test.sh all"}},
	} {
		raw, err := os.ReadFile(filepath.Join(root, c.file))
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range c.want {
			if !strings.Contains(string(raw), w) {
				t.Errorf("%s does not run %q", c.file, w)
			}
		}
		if strings.Contains(string(raw), byHand) {
			t.Errorf("%s still runs %q by hand", c.file, byHand)
		}
	}
}
