package testenv

// CI's go, go own-timeout (go-own) and crd jobs run every test that runs Envoy, so each must fetch the
// pinned binary and set HPCBURST_REQUIRE_ENVOY=1 before its tests, or those tests skip and the
// required check passes with them unrun. The binary comes from its own cache entry, keyed by the
// script and the pins file it reads, with no restore-keys, saved only on a miss. The expected step
// text is written here, not read from ci.yml.

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

type envoyCIStep struct {
	Name string            `json:"name"`
	ID   string            `json:"id"`
	Uses string            `json:"uses"`
	Run  string            `json:"run"`
	If   any               `json:"if"`
	COE  any               `json:"continue-on-error"`
	With map[string]string `json:"with"`
}

func envoyCIJobs(t *testing.T) map[string][]envoyCIStep {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Jobs map[string]struct {
			Steps []envoyCIStep `json:"steps"`
		} `json:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("ci.yml: %v", err)
	}
	out := map[string][]envoyCIStep{}
	for id, j := range doc.Jobs {
		out[id] = j.Steps
	}
	return out
}

// envoyCIProblems lists what is wrong with the Envoy steps of each job that runs tests.
func envoyCIProblems(jobs map[string][]envoyCIStep) []string {
	const (
		wantPath = "${{ runner.temp }}/envoy"
		wantKey  = "envoy-${{ runner.os }}-${{ runner.arch }}-${{ hashFiles('hack/fetch-envoy.sh', 'internal/agent/envoypin/envoy-pins.txt') }}"
		fetch    = `bash hack/fetch-envoy.sh "$RUNNER_TEMP/envoy"`
	)
	tests := map[string]string{"go": "bash hack/go-test.sh packages", "go-own": "bash hack/go-test.sh own", "crd": "bash hack/check.sh"}
	var problems []string
	for _, id := range []string{"go", "go-own", "crd"} {
		steps, ok := jobs[id]
		if !ok {
			problems = append(problems, "ci.yml has no job "+id)
			continue
		}
		restore, fetchAt, save, test := -1, -1, -1, -1
		for i, s := range steps {
			switch {
			case s.ID == "envoy" && strings.HasPrefix(s.Uses, "actions/cache/restore@"):
				restore = i
			case strings.Contains(s.Run, fetch):
				if fetchAt >= 0 {
					problems = append(problems, id+": Envoy is fetched by two steps")
				}
				fetchAt = i
			case strings.HasPrefix(s.Uses, "actions/cache/save@") && s.With["key"] == "${{ steps.envoy.outputs.cache-primary-key }}":
				save = i
			}
			if test < 0 {
				for _, line := range strings.Split(s.Run, "\n") {
					if l := strings.TrimSpace(line); l == tests[id] || strings.HasPrefix(l, tests[id]+" ") {
						test = i
					}
				}
			}
		}
		if restore < 0 || fetchAt < 0 || save < 0 || test < 0 {
			problems = append(problems, id+": want a restore (id envoy), a fetch, a save and the tests; found the steps at "+
				strings.Join([]string{itoa(restore), itoa(fetchAt), itoa(save), itoa(test)}, ", "))
			continue
		}
		if !(restore < fetchAt && fetchAt < save && fetchAt < test) {
			problems = append(problems, id+": the order must be restore, fetch, then the save and the tests; it is "+
				strings.Join([]string{itoa(restore), itoa(fetchAt), itoa(save), itoa(test)}, ", "))
		}
		r, f, s := steps[restore], steps[fetchAt], steps[save]
		if r.With["path"] != wantPath || r.With["key"] != wantKey {
			problems = append(problems, id+": the restore caches "+r.With["path"]+" under "+r.With["key"]+", want "+wantPath+" under "+wantKey)
		}
		if _, ok := r.With["restore-keys"]; ok {
			problems = append(problems, id+": the restore has restore-keys, so an old entry could serve a new pin")
		}
		for _, want := range []string{`echo "HPCBURST_ENVOY_DIR=$RUNNER_TEMP/envoy"`, `echo "HPCBURST_REQUIRE_ENVOY=1"`, `>> "$GITHUB_ENV"`} {
			if !strings.Contains(f.Run, want) {
				problems = append(problems, id+": the fetch step does not run "+want)
			}
		}
		if f.If != nil || f.COE != nil {
			problems = append(problems, id+": the fetch step has an if: or continue-on-error, so a failed fetch could leave the tests to skip")
		}
		if s.With["path"] != wantPath || s.If != "steps.envoy.outputs.cache-hit != 'true'" {
			problems = append(problems, id+": the save step must cache "+wantPath+" on a miss only")
		}
	}
	return problems
}

// itoa is a step's index, or "none" for a step not found.
func itoa(i int) string {
	if i < 0 {
		return "none"
	}
	return strconv.Itoa(i)
}

func TestCIFetchesEnvoyBeforeEveryJobsTests(t *testing.T) {
	for _, p := range envoyCIProblems(envoyCIJobs(t)) {
		t.Error(p)
	}
}

// The guard can fail: each fault, made to a copy of ci.yml's own steps, is reported.
func TestCIEnvoyGuardSeesEachFault(t *testing.T) {
	for _, c := range []struct {
		name string
		edit func(steps []envoyCIStep) []envoyCIStep
	}{
		{"no require", func(s []envoyCIStep) []envoyCIStep {
			for i := range s {
				s[i].Run = strings.ReplaceAll(s[i].Run, `echo "HPCBURST_REQUIRE_ENVOY=1"`, "")
			}
			return s
		}},
		{"a key without the pins file", func(s []envoyCIStep) []envoyCIStep {
			for i := range s {
				if s[i].ID == "envoy" {
					s[i].With = map[string]string{"path": s[i].With["path"], "key": strings.ReplaceAll(s[i].With["key"], ", 'internal/agent/envoypin/envoy-pins.txt'", "")}
				}
			}
			return s
		}},
		{"restore-keys", func(s []envoyCIStep) []envoyCIStep {
			for i := range s {
				if s[i].ID == "envoy" {
					w := map[string]string{"restore-keys": "envoy-"}
					for k, v := range s[i].With {
						w[k] = v
					}
					s[i].With = w
				}
			}
			return s
		}},
		{"the fetch after the tests", func(s []envoyCIStep) []envoyCIStep {
			var fetch envoyCIStep
			var rest []envoyCIStep
			for _, st := range s {
				if strings.Contains(st.Run, "hack/fetch-envoy.sh") {
					fetch = st
				} else {
					rest = append(rest, st)
				}
			}
			return append(rest, fetch)
		}},
		{"a fetch that may fail", func(s []envoyCIStep) []envoyCIStep {
			for i := range s {
				if strings.Contains(s[i].Run, "hack/fetch-envoy.sh") {
					s[i].COE = true
				}
			}
			return s
		}},
		{"no fetch", func(s []envoyCIStep) []envoyCIStep {
			var rest []envoyCIStep
			for _, st := range s {
				if !strings.Contains(st.Run, "hack/fetch-envoy.sh") {
					rest = append(rest, st)
				}
			}
			return rest
		}},
	} {
		for _, job := range []string{"go", "go-own", "crd"} {
			t.Run(c.name+"/"+job, func(t *testing.T) {
				jobs := envoyCIJobs(t)
				jobs[job] = c.edit(append([]envoyCIStep(nil), jobs[job]...))
				found := false
				for _, p := range envoyCIProblems(jobs) {
					found = found || strings.HasPrefix(p, job+": ")
				}
				if !found {
					t.Errorf("the guard missed %q in %s", c.name, job)
				}
			})
		}
	}
}
