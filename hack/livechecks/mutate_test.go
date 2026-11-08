package livechecks

import (
	"os/exec"
	"strings"
	"testing"
)

// hack/mutate-alpha.sh --only picks the mutants to try. An expression that cannot be read, and one that matches no
// mutant, are both a run that checked nothing, so neither may end as a success (PR 230, N1): exit 2, a message that
// names the cause, and no mutant run (a mutant run edits the tree and takes minutes; these two end before the first).
func TestMutateAlphaOnlyRefusesAnExpressionThatSelectsNothing(t *testing.T) {
	for _, c := range []struct{ name, pattern, want string }{
		{"an expression that is not valid", "[", "not a valid regular expression"},
		{"a misspelt pattern", "no-such-mutant-text-zzz", "selected no mutant"},
		{"an expression that is valid and matches nothing", "^$", "selected no mutant"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cmd := exec.Command("bash", "hack/mutate-alpha.sh", "--only", c.pattern)
			cmd.Dir = repoRoot(t)
			out, err := cmd.CombinedOutput()
			ee, ok := err.(*exec.ExitError)
			if !ok || ee.ExitCode() != 2 {
				t.Fatalf("--only %q: %v, want exit 2:\n%s", c.pattern, err, out)
			}
			if !strings.Contains(string(out), c.want) || strings.Contains(string(out), "mutants:") {
				t.Errorf("--only %q: the output does not say %q, or the run went on:\n%s", c.pattern, c.want, out)
			}
		})
	}
}
