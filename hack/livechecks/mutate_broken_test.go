package livechecks

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// A mutant that does not compile is not a mutant that a test killed (PR 244, B3): the build of the tests fails, and no test
// ran. hack/mutate-alpha.sh compiles the mutated package first and reports BROKEN for a failure, and fails the run for a
// broken mutant as for a survivor. It runs here on a throwaway module (MUTATE_ALPHA_ROOT, with its own mutants in
// MUTATE_ALPHA_MUTANTS), so each ending is shown on a mutant written for it: one that a test kills, one that does not
// compile because it names nothing, one that does not compile because the `if false` it was given removed the declaration
// its body uses (the two mutants the review found), the same mutant kept compiling with `false &&` (killed, not broken),
// and one that no test catches (survived).
const mutateFixture = `package alpha

import "errors"

func Clamp(v int) int {
	if v < 0 {
		return 0
	}
	return v
}

var errEmpty = errors.New("empty")

func Check(s string) error {
	if err := validate(s); err != nil {
		return err
	}
	return nil
}

func validate(s string) error {
	if s == "" {
		return errEmpty
	}
	return nil
}

type note string

func (n note) Text() string { return string(n) }
`

const mutateFixtureTest = `package alpha

import "testing"

func TestClamp(t *testing.T) {
	if Clamp(-1) != 0 || Clamp(5) != 5 {
		t.Fatal("clamp")
	}
}

func TestCheck(t *testing.T) {
	if Check("") == nil {
		t.Fatal("an empty string was accepted")
	}
	if Check("x") != nil {
		t.Fatal("x was refused")
	}
}
`

// runMutateFixture runs the harness over a throwaway module of these files, and returns its exit status and output.
func runMutateFixture(t *testing.T, files map[string]string) (int, string) {
	t.Helper()
	dir := t.TempDir()
	write(t, filepath.Join(dir, "go.mod"), "module example.test/fx\n\ngo 1.26\n", 0o644)
	for name, body := range files {
		write(t, filepath.Join(dir, name), body, 0o644)
	}
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "hack", "mutate-alpha.sh"))
	cmd.Env = append(os.Environ(), "MUTATE_ALPHA_ROOT="+dir, "MUTATE_ALPHA_MUTANTS="+filepath.Join(dir, "mutants.txt"))
	out, err := cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode(), string(out)
	} else if err != nil {
		t.Fatal(err)
	}
	return 0, string(out)
}

// Tests that already fail would count every mutant as killed, and "0 survived" would say nothing (#285). So before the
// first mutant of each kind the harness runs that kind's tests under that kind's own command, and stops with exit 2 and
// no mutant tried when they fail. Each kind is here twice: a failing test (exit 2) and the same module with that test
// passing (the run goes on to its mutant), so the refusal is the failing test's and not the fixture's. The conformance
// kind runs two packages, and each can be the failing one.
func TestMutateAlphaRefusesEachKindsTestsThatFailWithNoMutant(t *testing.T) {
	pkgTest := func(pkg, name, body string) string {
		return "package " + pkg + "\n\nimport \"testing\"\n\nfunc " + name + "(t *testing.T) { " + body + " }\n"
	}
	const fails, passes = `t.Fatal("this fails with no mutant")`, ""
	alphaFiles := func(alphaBody string) map[string]string {
		return map[string]string{"internal/alpha/fx.go": "package alpha\n\nconst V = 1\n", "internal/alpha/k_test.go": pkgTest("alpha", "TestKind", alphaBody)}
	}
	with := func(base map[string]string, more map[string]string) map[string]string {
		for k, v := range more {
			base[k] = v
		}
		return base
	}
	confFiles := func(alphaBody, confBody string) map[string]string {
		return with(alphaFiles(alphaBody), map[string]string{"internal/providerctl/conformance/c.go": "package conformance\n\nconst V = 1\n",
			"internal/providerctl/conformance/c_test.go": pkgTest("conformance", "TestKind", confBody), "mutants.txt": "internal/providerctl/conformance/c.go@@V = 1@@V = 2\n"})
	}
	// The dry kind's and the script's tests are in hack/livechecks, picked by name: each fixture's test has one its kind picks.
	dryFiles := func(body string) map[string]string {
		return with(alphaFiles(passes), map[string]string{"internal/alpha/alpha_live_test.go": "package alpha\n\nconst dry = 1\n",
			"hack/livechecks/k_test.go": pkgTest("livechecks", "TestPendingPastTheBoundKind", body), "mutants.txt": "alpha_live_test.go@@dry = 1@@dry = 2\n"})
	}
	scriptFiles := func(body string) map[string]string {
		return map[string]string{"hack/live-checks.sh": "#!/bin/bash\nA=1\n", "hack/livechecks/k_test.go": pkgTest("livechecks", "TestNeverStartedKind", body),
			"mutants.txt": "hack/live-checks.sh@@A=1@@A=2\n"}
	}
	pkgFiles := func(body string) map[string]string {
		return with(alphaFiles(body), map[string]string{"mutants.txt": "fx.go@@V = 1@@V = 2\n"})
	}
	for _, c := range []struct {
		name, want string
		failing    map[string]string
		passing    map[string]string
	}{
		{"the package's", "the package's tests fail with no mutant, so every mutant would count as killed: TestKind", pkgFiles(fails), pkgFiles(passes)},
		{"the conformance package's own half", "the conformance package's tests fail with no mutant, so every mutant would count as killed: TestKind", confFiles(passes, fails), confFiles(passes, passes)},
		{"the conformance kind's other package", "the conformance package's tests fail with no mutant, so every mutant would count as killed: TestKind", confFiles(fails, passes), confFiles(passes, passes)},
		{"the dry runs'", "the dry runs' tests fail with no mutant, so every mutant would count as killed: TestPendingPastTheBoundKind", dryFiles(fails), dryFiles(passes)},
		{"the script's", "the script's tests fail with no mutant, so every mutant would count as killed: TestNeverStartedKind", scriptFiles(fails), scriptFiles(passes)},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			code, out := runMutateFixture(t, c.failing)
			if code != 2 || !strings.Contains(out, c.want) {
				t.Errorf("exit %d, want 2 and %q:\n%s", code, c.want, out)
			}
			if strings.Contains(out, "mutants:") || strings.Contains(out, "KILLED") || strings.Contains(out, "SURVIVED") {
				t.Errorf("a mutant was tried on tests that fail with no mutant:\n%s", out)
			}
			// The control: nothing tests the mutated value, so it survives, which is the run going on past its baseline.
			code, out = runMutateFixture(t, c.passing)
			if code != 1 || !strings.Contains(out, "mutants: 1, killed 0, survived 1, broken 0") {
				t.Errorf("with the test passing: exit %d, want 1 and the mutant tried:\n%s", code, out)
			}
		})
	}
}

// The package's tests stop too when they do not compile with no mutant. And the deadline the harness gives them leaves a
// wait time to run: at -test.timeout 120s the stack's waits, which give up two minutes before the deadline
// (internal/rehearsal/stack, CleanupMargin), failed at once, which is how every mutant came to be killed (#285). A test
// that reads the deadline the same way stands for them here.
func TestMutateAlphaGivesTheTestsTimeBeforeTheirDeadline(t *testing.T) {
	const deadline = `package alpha

import (
	"testing"
	"time"
)

func TestTheDeadlineLeavesAWaitTime(t *testing.T) {
	if d, ok := t.Deadline(); ok && time.Until(d) < 2*time.Minute+30*time.Second {
		t.Fatalf("the deadline is %s away: a wait that gives up two minutes before it has no time to run", time.Until(d))
	}
}
`
	code, out := runMutateFixture(t, map[string]string{"internal/alpha/fx.go": mutateFixture, "internal/alpha/fx_test.go": mutateFixtureTest, "internal/alpha/d_test.go": deadline, "mutants.txt": "fx.go@@return v@@return v + 1\n"})
	if code != 0 || !strings.Contains(out, "mutants: 1, killed 1, survived 0, broken 0") {
		t.Errorf("exit %d, want 0 and one mutant killed:\n%s", code, out)
	}
	code, out = runMutateFixture(t, map[string]string{"internal/alpha/fx.go": "package alpha\n\nvar x = nothing\n", "mutants.txt": "fx.go@@x = @@y = \n"})
	if code != 2 || !strings.Contains(out, "the package's tests do not compile with no mutant") || strings.Contains(out, "mutants:") {
		t.Errorf("a package that does not compile: exit %d, want 2 and no mutant tried:\n%s", code, out)
	}
}

// A kill with no failing test says what it was: a panic, a timeout, an exit status (#285). A compile error in the Go a mutant
// changes reads BROKEN (TestMutateAlphaReportsAMutantThatDoesNotCompileAsBrokenNotKilled).
func TestMutateAlphaSaysWhatAKillWithNoFailingTestWas(t *testing.T) {
	const noTestFailed = `package alpha

import "os"

// Each is on when its mutant says so: a run that ends with no test named as failed.
const panics, timesOut, exits = false, false, false

func init() {
	if panics {
		panic("the package could not start")
	}
	if timesOut {
		panic("test timed out after 300s") // what the testing package prints, with no wait for it
	}
	if exits {
		os.Exit(7)
	}
}
`
	const passes = "package alpha\n\nimport \"testing\"\n\nfunc TestPasses(t *testing.T) {}\n"
	code, out := runMutateFixture(t, map[string]string{"internal/alpha/fx.go": noTestFailed, "internal/alpha/fx_test.go": passes,
		"mutants.txt": strings.Join([]string{"fx.go@@const panics, timesOut, exits = false, false, false@@const panics, timesOut, exits = true, false, false",
			"fx.go@@const panics, timesOut, exits = false, false, false@@const panics, timesOut, exits = false, true, false",
			"fx.go@@const panics, timesOut, exits = false, false, false@@const panics, timesOut, exits = false, false, true"}, "\n") + "\n"})
	if code != 0 {
		t.Errorf("exit %d, want 0:\n%s", code, out)
	}
	for _, want := range []string{
		"= true, false, false: by a panic, no test named as failed",
		"= false, true, false: by a timeout, no test named as failed",
		"= false, false, true: by exit 7, no test named as failed",
		"mutants: 3, killed 3, survived 0, broken 0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the output lacks %q:\n%s", want, out)
		}
	}
}

// A test that goes red after the baseline (a flake, a date, a port) would kill every mutant it meets. The science agent's rule on
// PR 284: such a kill reads BROKEN, with the test named, so a red test can never read as a perfect score. It stands for one
// here by counting its runs: green at the baseline, red from then on. The mutant changes what no test reads, so the
// test is all that fails. Beside it, a mutant a real test catches is a kill, even with a red test in the run.
func TestMutateAlphaReadsAKillTheUnmutatedTestsAlsoMakeAsBroken(t *testing.T) {
	const goesRed = `package alpha

import (
	"os"
	"testing"
)

func TestGoesRedAfterTheBaseline(t *testing.T) {
	_, err := os.Stat("ran-once")
	if err == nil {
		t.Fatal("red, with or without a mutant")
	}
	if err := os.WriteFile("ran-once", nil, 0o644); err != nil {
		t.Fatal(err)
	}
}
`
	files := func(mutants string) map[string]string {
		return map[string]string{"internal/alpha/fx.go": mutateFixture, "internal/alpha/a_test.go": goesRed, "mutants.txt": mutants + "\n"}
	}
	code, out := runMutateFixture(t, files("fx.go@@return string(n)@@return string(n) + \".\""))
	if code != 1 {
		t.Errorf("exit %d, want 1:\n%s", code, out)
	}
	for _, want := range []string{"BROKEN   fx.go: return string(n)", "killed only by TestGoesRedAfterTheBaseline, which also fails with no mutant", "mutants: 1, killed 0, survived 0, broken 1"} {
		if !strings.Contains(out, want) {
			t.Errorf("the output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "KILLED") || strings.Contains(out, "kills by what failed") {
		t.Errorf("a test that is red with no mutant was counted as a kill:\n%s", out)
	}
}

// A test seen green with no mutant is not run again for ten minutes: two kills by one test cost one look, so the script's slow
// tests are not run once per mutant. Its runs are counted: the baseline, the first mutant, the look, the second mutant.
func TestMutateAlphaLooksAgainAtAKillingTestOnce(t *testing.T) {
	const counted = `package alpha

import (
	"os"
	"testing"
)

func TestCounted(t *testing.T) {
	f, err := os.OpenFile("runs", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString("run\n"); err != nil {
		t.Fatal(err)
	}
	if Clamp(5) != 5 {
		t.Fatal("clamp")
	}
}
`
	dir := t.TempDir()
	write(t, filepath.Join(dir, "go.mod"), "module example.test/fx\n\ngo 1.26\n", 0o644)
	write(t, filepath.Join(dir, "internal/alpha/fx.go"), mutateFixture, 0o644)
	write(t, filepath.Join(dir, "internal/alpha/fx_test.go"), counted, 0o644)
	write(t, filepath.Join(dir, "mutants.txt"), "fx.go@@return v@@return v + 1\nfx.go@@return v@@return v + 2\n", 0o644)
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "hack", "mutate-alpha.sh"))
	cmd.Env = append(os.Environ(), "MUTATE_ALPHA_ROOT="+dir, "MUTATE_ALPHA_MUTANTS="+filepath.Join(dir, "mutants.txt"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v:\n%s", err, out)
	}
	if !strings.Contains(string(out), "mutants: 2, killed 2, survived 0, broken 0") {
		t.Errorf("want both killed:\n%s", out)
	}
	runs, err := os.ReadFile(filepath.Join(dir, "internal/alpha/runs"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(runs), "run\n"); n != 4 {
		t.Errorf("the test ran %d times, want 4: the baseline, a mutant, one look with no mutant, the other mutant", n)
	}
}

// Each kill names the test that failed, and the run ends with how many each made, so that one test failing for every
// mutant shows (#285). A kill count alone is the verification that proves nothing.
func TestMutateAlphaNamesTheTestThatKilledEachMutant(t *testing.T) {
	code, out := runMutateFixture(t, map[string]string{"internal/alpha/fx.go": mutateFixture, "internal/alpha/fx_test.go": mutateFixtureTest,
		"mutants.txt": strings.Join([]string{"fx.go@@return v@@return v + 1", "fx.go@@return 0@@return 1", `fx.go@@return errEmpty@@return nil`}, "\n") + "\n"})
	if code != 0 {
		t.Errorf("exit %d, want 0:\n%s", code, out)
	}
	for _, want := range []string{
		"KILLED   fx.go: return v  ->  return v + 1: by TestClamp",
		"KILLED   fx.go: return 0  ->  return 1: by TestClamp",
		"KILLED   fx.go: return errEmpty  ->  return nil: by TestCheck",
		"kills by what failed:",
		"2 TestClamp",
		"1 TestCheck",
		"mutants: 3, killed 3, survived 0, broken 0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the output lacks %q:\n%s", want, out)
		}
	}
}

func TestMutateAlphaReportsAMutantThatDoesNotCompileAsBrokenNotKilled(t *testing.T) {
	for _, c := range []struct {
		name, mutant string
		code         int
		want         []string // lines of the output
		wantNot      []string
	}{
		{"a mutant a test kills", "fx.go@@return v@@return v + 1", 0,
			[]string{"mutants: 1, killed 1, survived 0, broken 0"}, []string{"BROKEN", "SURVIVED"}},
		{"a mutant that names nothing", "fx.go@@return 0@@return zero", 1,
			[]string{"BROKEN   fx.go: ", ": the mutant does not compile: ", "mutants: 1, killed 0, survived 0, broken 1"}, []string{"SURVIVED"}},
		{"an if false that removes the declaration its body uses", "fx.go@@if err := validate(s); err != nil {@@if false {", 1,
			[]string{"BROKEN   fx.go: ", ": the mutant does not compile: ", "mutants: 1, killed 0, survived 0, broken 1"}, []string{"SURVIVED"}},
		{"the same condition kept compiling", "fx.go@@if err := validate(s); err != nil {@@if err := validate(s); false && err != nil {", 0,
			[]string{"mutants: 1, killed 1, survived 0, broken 0"}, []string{"BROKEN", "SURVIVED"}},
		{"a mutant no test catches", "fx.go@@return string(n)@@return string(n) + \".\"", 1,
			[]string{"SURVIVED fx.go", "mutants: 1, killed 0, survived 1, broken 0"}, []string{"BROKEN"}},
		{"a text that does not occur", "fx.go@@nothing like this@@x", 1,
			[]string{"BROKEN   fx.go: the text 'nothing like this' does not occur exactly once", "mutants: 1, killed 0, survived 0, broken 1"}, []string{"SURVIVED"}},
		{"one of each: the run counts them all and fails", strings.Join([]string{
			"fx.go@@return v@@return v + 1", "fx.go@@return 0@@return zero",
			"fx.go@@return string(n)@@return string(n) + \".\""}, "\n"), 1,
			[]string{"mutants: 3, killed 1, survived 1, broken 1"}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, body := range map[string]string{
				"go.mod":                    "module example.test/fx\n\ngo 1.26\n",
				"internal/alpha/fx.go":      mutateFixture,
				"internal/alpha/fx_test.go": mutateFixtureTest,
				"mutants.txt":               c.mutant + "\n",
			} {
				write(t, filepath.Join(dir, name), body, 0o644)
			}
			cmd := exec.Command("bash", filepath.Join(repoRoot(t), "hack", "mutate-alpha.sh"))
			cmd.Env = append(os.Environ(), "MUTATE_ALPHA_ROOT="+dir, "MUTATE_ALPHA_MUTANTS="+filepath.Join(dir, "mutants.txt"))
			out, err := cmd.CombinedOutput()
			code := 0
			if ee, ok := err.(*exec.ExitError); ok {
				code = ee.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			if code != c.code {
				t.Errorf("exit %d, want %d:\n%s", code, c.code, out)
			}
			for _, w := range c.want {
				if !strings.Contains(string(out), w) {
					t.Errorf("the output lacks %q:\n%s", w, out)
				}
			}
			for _, w := range c.wantNot {
				if strings.Contains(string(out), w) {
					t.Errorf("the output has %q:\n%s", w, out)
				}
			}
			// The mutated file is back as it was.
			if got, _ := os.ReadFile(filepath.Join(dir, "internal/alpha/fx.go")); string(got) != mutateFixture {
				t.Errorf("the run left the mutated file behind:\n%s", got)
			}
		})
	}
}
