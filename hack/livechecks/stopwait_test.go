package livechecks

// Every wait for a process that a test has stopped is bounded (issue #494). A bare cmd.Wait() after a signal is the shape that hung the
// package for 8 min 41 s in CI: if the script does not stop, the test waits for the package's timeout. The stop tests wait through
// waitBounded (waitStopped, rehearsalStopBound), which fails with the script's output and the process tree. This test reads the package's
// test files and fails on any other Wait on a process, unless the line is one of the few below, each with the reason it cannot hang: a
// new bare Wait must be added here on purpose, with its reason, or bounded.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// allowedWaits are the lines of the package's tests that wait on a process, each bounded some other way.
var allowedWaits = []struct{ file, line, why string }{
	{"harness_test.go", "go func() { done <- cmd.Wait() }()", "waitBounded itself: the goroutine whose result its select bounds"},
	{"alpha_test.go", "go func() { done <- cmd.Wait() }()", "a goroutine whose result the test's select bounds at 60 s"},
	{"mutate_broken_test.go", "if err := cmd.Wait(); ctx.Err() != nil {", "the command runs under a context with a deadline: it is killed at the bound"},
	{"leftover_test.go", "_ = runner.Wait()", "after runner.Process.Kill() (SIGKILL), which a process cannot ignore"},
	{"leftover_test.go", "_ = cmd.Process.Kill(); _ = cmd.Wait()", "a cleanup that kills the process (SIGKILL) first"},
	{"leftover_test.go", "_ = holder.Process.Kill(); _ = holder.Wait()", "a cleanup that kills the process (SIGKILL) first"},
	{"leftover_test.go", "_ = waiter.Process.Kill(); _ = waiter.Wait()", "a cleanup that kills the process (SIGKILL) first"},
}

func TestNoStopTestWaitsOnAProcessWithoutABound(t *testing.T) {
	files, err := filepath.Glob("*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no test files found (%v)", err)
	}
	wait := regexp.MustCompile(`\.Wait\(\)`)
	used := map[int]bool{}
	for _, f := range files {
		switch f {
		case "reaper_test.go":
			continue // the reaper's own implementation: it waits on the script it runs, and every test waits on the reaper through a bound
		case "stopwait_test.go":
			continue // this file
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for n, l := range strings.Split(string(b), "\n") {
			if !wait.MatchString(l) || strings.HasPrefix(strings.TrimSpace(l), "//") {
				continue
			}
			ok := false
			for i, a := range allowedWaits {
				if a.file == f && strings.Contains(l, a.line) {
					ok, used[i] = true, true
				}
			}
			if !ok {
				t.Errorf("%s:%d waits on a process without a bound: %s\n\twait through waitBounded (waitStopped, rehearsalStopBound), or add the line to allowedWaits with the reason it cannot hang", f, n+1, strings.TrimSpace(l))
			}
		}
	}
	for i, a := range allowedWaits {
		if !used[i] {
			t.Errorf("allowedWaits holds a line that no test file has any more (%s: %s): drop it", a.file, a.line)
		}
	}
}
