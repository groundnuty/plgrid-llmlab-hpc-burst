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
			[]string{"BROKEN   fx.go: the mutant does not compile", "mutants: 1, killed 0, survived 0, broken 1"}, []string{"SURVIVED"}},
		{"an if false that removes the declaration its body uses", "fx.go@@if err := validate(s); err != nil {@@if false {", 1,
			[]string{"BROKEN   fx.go: the mutant does not compile", "mutants: 1, killed 0, survived 0, broken 1"}, []string{"SURVIVED"}},
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
