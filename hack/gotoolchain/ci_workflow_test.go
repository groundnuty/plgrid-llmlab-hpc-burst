package gotoolchain

// The workflow runs the same five Go steps in each of its jobs (issue #294), and a copy that drifted
// from the others would run an unpinned Go, or cache it under another key, in one job only. These
// tests read .github/workflows/ci.yml as YAML. The expected steps are written out here, not taken from
// the workflow, so a change to the workflow and none to this file fails.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

type step map[string]any

func (s step) str(key string) string {
	v, _ := s[key].(string)
	return v
}

func (s step) with(key string) (string, bool) {
	w, _ := s["with"].(map[string]any)
	v, ok := w[key].(string)
	return v, ok
}

type job struct {
	id    string
	name  string
	steps []step
}

func loadJobs(t *testing.T) []job {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Jobs map[string]struct {
			Name  string `json:"name"`
			Steps []step `json:"steps"`
		} `json:"jobs"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatalf("ci.yml is not valid YAML: %v", err)
	}
	if len(doc.Jobs) == 0 {
		t.Fatal("ci.yml has no jobs")
	}
	var jobs []job
	for id, j := range doc.Jobs {
		jobs = append(jobs, job{id: id, name: j.Name, steps: j.Steps})
	}
	return jobs
}

// goSteps are the five steps of every job, by name and in order.
var goSteps = []string{
	"Read the Go version from go.mod",
	"Restore the cached Go archive",
	"Install the pinned Go toolchain",
	"Save the Go archive to the cache",
	"Check the Go on PATH is the pinned toolchain",
}

// goBlock returns the job's five steps, which must follow one another in this order.
func goBlock(t *testing.T, j job) []step {
	t.Helper()
	for i, s := range j.steps {
		if s.str("name") != goSteps[0] {
			continue
		}
		if i+len(goSteps) > len(j.steps) {
			break
		}
		block := j.steps[i : i+len(goSteps)]
		for k, want := range goSteps {
			if block[k].str("name") != want {
				t.Fatalf("job %s: step %d after %q is %q, want %q", j.id, k, goSteps[0], block[k].str("name"), want)
			}
		}
		return block
	}
	t.Fatalf("job %s has no %q step followed by the other four", j.id, goSteps[0])
	return nil
}

const (
	wantRestoreKey = "go-toolchain-${{ steps.goversion.outputs.version }}-${{ runner.os }}-${{ runner.arch }}-${{ hashFiles('hack/fetch-go.sh', 'hack/go-pins.txt') }}"
	wantArchive    = "${{ runner.temp }}/go-download"
	wantSaveIf     = "steps.goarchive.outputs.cache-hit != 'true' && github.event_name != 'pull_request'"
	checkID        = "gocheck"
	wantVerified   = "steps.gocheck.outcome == 'success'"
)

// Every job installs Go through the script, and no job still uses actions/setup-go.
func TestCIEveryJobRunsTheSameFivePinnedGoSteps(t *testing.T) {
	jobs := loadJobs(t)
	names := map[string]bool{}
	var first []step
	for _, j := range jobs {
		names[j.name] = true
		block := goBlock(t, j)
		if first == nil {
			first = block
		} else if !reflect.DeepEqual(first, block) {
			a, _ := json.Marshal(first)
			b, _ := json.Marshal(block)
			t.Errorf("job %s's Go steps differ from the others':\n%s\n%s", j.id, b, a)
		}
		for _, s := range j.steps {
			if strings.HasPrefix(s.str("uses"), "actions/setup-go") {
				t.Errorf("job %s still uses %s", j.id, s.str("uses"))
			}
		}

		v, restore, install, save, check := block[0], block[1], block[2], block[3], block[4]
		if v.str("id") != "goversion" || !strings.Contains(v.str("run"), `version=$(bash hack/fetch-go.sh --version)`) || !strings.Contains(v.str("run"), `echo "version=$version" >> "$GITHUB_OUTPUT"`) {
			t.Errorf("job %s: the version step is %v", j.id, v)
		}
		if restore.str("id") != "goarchive" || !strings.HasPrefix(restore.str("uses"), "actions/cache/restore@") {
			t.Errorf("job %s: the restore step is %v", j.id, restore)
		}
		if p, _ := restore.with("path"); p != wantArchive {
			t.Errorf("job %s: the restore caches %q, want only %q", j.id, p, wantArchive)
		}
		if k, _ := restore.with("key"); k != wantRestoreKey {
			t.Errorf("job %s: the restore key is %q, want %q", j.id, k, wantRestoreKey)
		}
		if _, ok := restore.with("restore-keys"); ok {
			t.Errorf("job %s: the restore has restore-keys, so an old entry could serve a new pin", j.id)
		}
		run := install.str("run")
		for _, want := range []string{
			`bash hack/fetch-go.sh "$RUNNER_TEMP/go-download" "$RUNNER_TEMP/go-toolchain"`,
			`echo "$RUNNER_TEMP/go-toolchain/go/bin" >> "$GITHUB_PATH"`,
			`echo "GOROOT=$RUNNER_TEMP/go-toolchain/go" >> "$GITHUB_ENV"`,
			`.github/go-problem-matcher.json`,
		} {
			if !strings.Contains(run, want) {
				t.Errorf("job %s: the install step does not contain %q:\n%s", j.id, want, run)
			}
		}
		if !strings.HasPrefix(save.str("uses"), "actions/cache/save@") {
			t.Errorf("job %s: the save step is %v", j.id, save)
		}
		if save.str("if") != wantSaveIf {
			t.Errorf("job %s: the save runs if %q, want %q (on a miss, and never on a pull request)", j.id, save.str("if"), wantSaveIf)
		}
		if p, _ := save.with("path"); p != wantArchive {
			t.Errorf("job %s: the save caches %q, want the restore's %q", j.id, p, wantArchive)
		}
		if k, _ := save.with("key"); k != "${{ steps.goarchive.outputs.cache-primary-key }}" {
			t.Errorf("job %s: the save key is %q, want the restore's primary key", j.id, k)
		}
		if check.str("id") != checkID {
			t.Errorf("job %s: the check step's id is %q, want %q (later steps name it)", j.id, check.str("id"), checkID)
		}
		if check.str("run") != `bash hack/fetch-go.sh --check "$RUNNER_TEMP/go-toolchain"` {
			t.Errorf("job %s: the check step runs %q", j.id, check.str("run"))
		}
	}
	// Branch protection names these four jobs, so a rename blocks every merge.
	for _, name := range []string{"go", "tidy", "crd and envtest", "launcher-k3s"} {
		if !names[name] {
			t.Errorf("ci.yml has no job named %q: branch protection requires it", name)
		}
	}
}

// Every action is pinned by a full commit SHA, so a moved tag changes nothing.
func TestCIEveryActionIsPinnedByCommitSHA(t *testing.T) {
	pinned := regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[0-9a-f]{40}$`)
	n := 0
	for _, j := range loadJobs(t) {
		for _, s := range j.steps {
			if u := s.str("uses"); u != "" {
				n++
				if !pinned.MatchString(u) {
					t.Errorf("job %s step %q uses %q, which is not pinned by a 40-character commit SHA", j.id, s.str("name"), u)
				}
			}
		}
	}
	if n == 0 {
		t.Error("found no step that uses an action")
	}
}

// tidy still restores no module cache: the Go archive is the only thing it restores or saves, and the
// step that asserts an empty module cache is still there.
func TestCITidyRestoresOnlyTheGoArchiveAndStillAssertsAnEmptyModuleCache(t *testing.T) {
	for _, j := range loadJobs(t) {
		if j.name != "tidy" {
			continue
		}
		asserted := false
		for _, s := range j.steps {
			if strings.HasPrefix(s.str("uses"), "actions/cache") {
				if p, _ := s.with("path"); p != wantArchive {
					t.Errorf("tidy's step %q caches %q, want only the Go archive", s.str("name"), p)
				}
			}
			if s.str("name") == "Assert that the module cache is empty" && strings.Contains(s.str("run"), "go env GOMODCACHE") {
				asserted = true
			}
		}
		if !asserted {
			t.Error("tidy no longer asserts that the module cache is empty")
		}
		return
	}
	t.Fatal("no job named tidy")
}

// The matcher that setup-go registered for Go's errors, kept because setup-go is gone.
func TestCIProblemMatcherIsAValidGoMatcher(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "go-problem-matcher.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		ProblemMatcher []struct {
			Owner   string `json:"owner"`
			Pattern []struct {
				Regexp string `json:"regexp"`
				File   int    `json:"file"`
				Line   int    `json:"line"`
			} `json:"pattern"`
		} `json:"problemMatcher"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("the matcher is not JSON: %v", err)
	}
	if len(m.ProblemMatcher) != 1 || m.ProblemMatcher[0].Owner != "go" || len(m.ProblemMatcher[0].Pattern) != 1 {
		t.Fatalf("unexpected matcher: %+v", m)
	}
	re, err := regexp.Compile(m.ProblemMatcher[0].Pattern[0].Regexp)
	if err != nil {
		t.Fatalf("the matcher's regexp does not compile: %v", err)
	}
	sub := re.FindStringSubmatch("internal/foo/bar.go:12:5: undefined: x")
	if sub == nil || sub[m.ProblemMatcher[0].Pattern[0].File] != "internal/foo/bar.go" || sub[m.ProblemMatcher[0].Pattern[0].Line] != "12" {
		t.Errorf("the matcher does not read a compiler error: %v", sub)
	}
}

// A step that runs even when an earlier one failed (!cancelled(), always(), failure()) would, after a
// failed or skipped toolchain check, run on whatever go the runner image has: the install exits before
// it puts the pinned Go on PATH. So every such step after the check must also require the check's
// success. (A step with no status function, or one that has only an unrelated condition, gets
// GitHub's implicit success() and is skipped after a failure, so it needs nothing.) The steps are
// read from the YAML, so a new one is covered too.
func TestCIEveryStepThatRunsAfterAFailureRequiresTheVerifiedToolchain(t *testing.T) {
	status := regexp.MustCompile(`\b(always|cancelled|failure)\(\)`)
	guarded := 0
	for _, j := range loadJobs(t) {
		check := -1
		for i, s := range j.steps {
			if s.str("name") == goSteps[4] {
				check = i
			}
		}
		if check < 0 {
			t.Fatalf("job %s has no %q step", j.id, goSteps[4])
		}
		for _, s := range j.steps[check+1:] {
			cond := s.str("if")
			if !status.MatchString(cond) {
				continue
			}
			guarded++
			if !strings.Contains(cond, wantVerified) {
				t.Errorf("job %s: step %q runs after a failure (if: %q) without requiring %q, so it can run on an unverified Go", j.id, s.str("name"), cond, wantVerified)
			}
		}
		// The steps before the check have no pinned Go on PATH yet, so none of them may run go.
		for _, s := range j.steps[:check] {
			if cond := s.str("if"); status.MatchString(cond) {
				t.Errorf("job %s: step %q before the Go check runs after a failure (if: %q)", j.id, s.str("name"), cond)
			}
		}
	}
	if guarded == 0 {
		t.Error("found no step that runs after a failure; this test would check nothing (the go job has three)")
	}
}
