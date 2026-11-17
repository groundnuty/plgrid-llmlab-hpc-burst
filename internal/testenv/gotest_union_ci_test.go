package testenv

// The CI half of the union count (gotest_union_test.go): CI's go job runs the packages, its go own-timeout
// job (id go-own, a required check since issue #295) runs every group in ONE step, "own", whose lanes run
// the groups of different packages at the same time, and the crd job runs hack/check.sh, whose "all" runs
// every part. A step or a job that can be skipped, or whose failure does not fail the job, would leave a
// part's tests out of a required check while the check stays green, so the guard reads each step's and each
// job's if: and continue-on-error as well as its run script.

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// ciStep and ciJob are what the guard reads of ci.yml. if: and continue-on-error are kept as YAML gave
// them: "if: false" is a bool, and continue-on-error can be an expression.
type ciStep struct {
	Run             string `json:"run"`
	If              any    `json:"if"`
	ContinueOnError any    `json:"continue-on-error"`
}

type ciJob struct {
	If              any      `json:"if"`
	ContinueOnError any      `json:"continue-on-error"`
	Steps           []ciStep `json:"steps"`
}

// verifiedGate is the one condition a part's step may carry: it runs after an earlier failure, but only
// on the verified Go (TestCIEveryStepThatRunsAfterAFailureRequiresTheVerifiedToolchain). With no if: at
// all, a step gets GitHub's implicit success(), which is also allowed.
const verifiedGate = "${{ !cancelled() && steps.gocheck.outcome == 'success' }}"

func condition(v any) string {
	switch c := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(c)
	default:
		return fmt.Sprint(c)
	}
}

// mayContinue reports whether continue-on-error can let the step or job pass when it fails: anything but
// absent or false, since an expression can be true at run time.
func mayContinue(v any) bool {
	b, ok := v.(bool)
	return v != nil && (!ok || b)
}

// ciJobs returns ci.yml's jobs by id.
func ciJobs(t *testing.T, root string) map[string]ciJob {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Jobs map[string]ciJob `json:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("ci.yml: %v", err)
	}
	return doc.Jobs
}

// ownCmd is the script's "own" with no group: every group, in lanes.
const ownCmd = "bash hack/go-test.sh own"

// groupWord is what a group's name looks like on a command line, as the script's table spells them.
var groupWord = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// runsOwnAlone reports whether a line of a run script is "own" with no group: the command, then nothing,
// or a redirect or pipe, and not a group's name ("own lease", which stepsRunning(ownCmd) would also match).
func runsOwnAlone(line string) bool {
	rest, ok := strings.CutPrefix(strings.TrimSpace(line), ownCmd)
	if !ok || (rest != "" && rest[0] != ' ' && rest[0] != '\t') {
		return false
	}
	f := strings.Fields(rest)
	return len(f) == 0 || !groupWord.MatchString(f[0])
}

// stepsRunningOwnAlone returns the steps with a line that runs "own" with no group.
func stepsRunningOwnAlone(steps []ciStep) []ciStep {
	var found []ciStep
	for _, s := range steps {
		for _, line := range strings.Split(s.Run, "\n") {
			if runsOwnAlone(line) {
				found = append(found, s)
				break
			}
		}
	}
	return found
}

// ciProblems returns one line for each part that CI does not run in exactly one step that always counts:
// the packages' run in the go job, every group in the go-own job (go own-timeout) in the one step "own",
// and the crd job's hack/check.sh, whose "all" runs every part (TestGoTestScriptIsWhatCIAndCheckRun
// checks that it does). A part in the wrong job is a problem too: "own" in the go job would run the
// groups there as well, a step for one group in go-own would run that group twice (the lanes of "own"
// run it too), and the packages' run in the go-own job likewise.
func ciProblems(jobs map[string]ciJob, groups []string) []string {
	var problems []string
	add := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	for _, id := range []string{"go", "go-own", "crd"} {
		j := jobs[id]
		// A job skipped by its if: reports success, so a required check would pass with nothing run.
		if c := condition(j.If); c != "" {
			add("CI's %s job has if: %s, which can skip it, and a skipped job passes its required check", id, c)
		}
		if mayContinue(j.ContinueOnError) {
			add("CI's %s job has continue-on-error: %v, which passes it when a part fails", id, j.ContinueOnError)
		}
	}
	// "all" in either job would run the groups or the packages a second time, and "own" in the go job
	// would run the groups there.
	for _, id := range []string{"go", "go-own"} {
		extras := []string{"bash hack/go-test.sh all"}
		if id == "go" {
			extras = append(extras, ownCmd)
		}
		for _, extra := range extras {
			for _, s := range jobs[id].Steps {
				for _, line := range strings.Split(s.Run, "\n") {
					if strings.TrimSpace(line) == extra {
						add("CI's %s job runs %q, which runs the groups again", id, extra)
					}
				}
			}
		}
	}
	want := []struct {
		job, cmd string
		find     func([]ciStep) []ciStep
	}{
		{"go", "bash hack/go-test.sh packages", func(s []ciStep) []ciStep { return stepsRunning(s, "bash hack/go-test.sh packages") }},
		{"go-own", ownCmd, stepsRunningOwnAlone},
		{"crd", "bash hack/check.sh", func(s []ciStep) []ciStep { return stepsRunning(s, "bash hack/check.sh") }},
	}
	for _, w := range want {
		steps := w.find(jobs[w.job].Steps)
		if len(steps) != 1 {
			add("CI's %s job runs %q in %d steps, want 1", w.job, w.cmd, len(steps))
			continue
		}
		s := steps[0]
		if c := condition(s.If); c != "" && c != verifiedGate {
			add("CI's %s job runs %q under if: %s, which can skip it; want no if: or %s", w.job, w.cmd, c, verifiedGate)
		}
		if mayContinue(s.ContinueOnError) {
			add("CI's %s job runs %q with continue-on-error: %v, which passes the job when it fails", w.job, w.cmd, s.ContinueOnError)
		}
	}
	// The other direction: no part in the other job, and no group in a step of its own beside the lanes.
	for _, g := range groups {
		if n := len(stepsRunning(jobs["go"].Steps, ownCmd+" "+g)); n > 0 {
			add("CI's go job runs %q in %d steps, which belong to the go-own job: it would run twice", ownCmd+" "+g, n)
		}
		if n := len(stepsRunning(jobs["go-own"].Steps, ownCmd+" "+g)); n > 0 {
			add("CI's go-own job runs %q in %d steps, but its step %q already runs every group in lanes: it would run twice", ownCmd+" "+g, n, ownCmd)
		}
	}
	if n := len(stepsRunning(jobs["go-own"].Steps, "bash hack/go-test.sh packages")); n > 0 {
		add("CI's go-own job runs %q in %d steps, which belongs to the go job: it would run twice", "bash hack/go-test.sh packages", n)
	}
	return problems
}

// stepsRunning returns the steps whose run script has a line that is exactly cmd, or that runs cmd and
// pipes or redirects it (bash hack/go-test.sh packages 2>&1 | tee ...).
func stepsRunning(steps []ciStep, cmd string) []ciStep {
	var found []ciStep
	for _, s := range steps {
		for _, line := range strings.Split(s.Run, "\n") {
			line = strings.TrimSpace(line)
			if line == cmd || strings.HasPrefix(line, cmd+" ") {
				found = append(found, s)
				break
			}
		}
	}
	return found
}
