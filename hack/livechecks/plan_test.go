package livechecks

import (
	"os"
	"sort"
	"strings"
	"testing"
)

// runSet reads the "run:" line of a plan.
func runSet(t *testing.T, out string) []string {
	t.Helper()
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "run:") {
			f := strings.Fields(strings.TrimPrefix(l, "run:"))
			if len(f) > 0 && f[0] == "(none)" {
				return nil
			}
			sort.Strings(f)
			return f
		}
	}
	t.Fatalf("the plan has no run: line:\n%s", out)
	return nil
}

// Each case changes one file of the fixture module. The expected tests follow
// from how the fixture is wired (front imports pkg/provider; the conformance
// package imports providerctl, which imports slurm; hb-agent imports agent,
// which embeds data.txt), not from the script's table or its output.
func TestPlanMapsChangedFilesToTheLiveTestsThatRunThem(t *testing.T) {
	cases := []struct {
		name, path, content string
		want                []string
	}{
		{"docs only", "docs/notes.md", "changed\n", nil},
		{"a readme inside a live package", "internal/front/README.md", "changed\n", nil},
		{"a package that nothing live imports", "unrelated/unrelated.go", "package unrelated\n\nvar X = 1\n", nil},
		{"a dependency only: slurm", "internal/slurm/slurm.go", "package slurm\n\nvar X = 1\n", []string{"conformance-athena", "conformance-helios"}},
		{"a dependency of one test: pkg/provider", "pkg/provider/provider.go", "package provider\n\nvar X = 1\n", []string{"llmlab-smoke"}},
		{"a holding package", "internal/front/front.go", "package front\n\nimport _ \"example.test/fx/pkg/provider\"\n\nvar X = 1\n", []string{"llmlab-smoke"}},
		{"a test file of the holding package", "internal/front/front_test.go", "package front\n\nimport \"testing\"\n\nfunc TestLLMLabThroughTheFront(t *testing.T) { _ = 1 }\n", []string{"llmlab-smoke"}},
		{"a test file of a dependency", "internal/slurm/slurm_test.go", "package slurm\n\nimport \"testing\"\n\nfunc TestSlurm(t *testing.T) { _ = 1 }\n", nil},
		{"go.mod", "go.mod", fixtureGoMod + "\n// a comment\n", []string{"conformance-athena", "conformance-helios", "hub-golden", "llmlab-smoke", "real-engine"}},
		{"a profile read whole by one run", "profiles/helios.yaml", "name: helios\nx: 1\n", []string{"conformance-helios"}},
		{"the real-engine job script", "spikes/s94/job.sh", "#!/bin/bash\n# changed\n", []string{"real-engine"}},
		{"a file the agent embeds", "internal/agent/data.txt", "changed\n", []string{"real-engine"}},
		{"a file the agent does not embed", "internal/agent/notes.txt", "changed\n", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWorld(t)
			rng := w.commit(c.path, c.content)
			res := w.run(nil, "--plan", rng)
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			got := runSet(t, res.out)
			if strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Errorf("run set %v, want %v\n%s", got, c.want, res.out)
			}
			if !strings.Contains(res.out, "pending: rehearsal") {
				t.Errorf("the pending rehearsal is not listed:\n%s", res.out)
			}
			// It runs nothing: no ssh, no sbatch, no go build or test.
			for _, f := range []string{"ssh-argv", "sbatch-argv", "go-argv"} {
				if got := w.stateFile(f); got != "" {
					t.Errorf("--plan called %s: %s", f, got)
				}
			}
			if _, err := os.Stat(w.out()); err == nil {
				t.Errorf("--plan made the output directory")
			}
		})
	}
}

func TestPlanNamesWhatChangedAndNothingPrivate(t *testing.T) {
	w := newWorld(t)
	res := w.run(nil, "--plan", w.commit("internal/slurm/slurm.go", "package slurm\n\nvar X = 1\n"))
	for _, want := range []string{"1 file(s) changed", "covered: conformance-helios", "changed internal/slurm", "not covered: llmlab-smoke hub-golden real-engine"} {
		if !strings.Contains(res.out, want) {
			t.Errorf("the plan lacks %q:\n%s", want, res.out)
		}
	}
}

func TestPlanRefusesARangeItCannotRead(t *testing.T) {
	w := newWorld(t)
	for _, rng := range []string{"nonsense", "0000000000000000000000000000000000000000..1111111111111111111111111111111111111111"} {
		res := w.run(nil, "--plan", rng)
		if res.code != 2 {
			t.Errorf("--plan %q: exit %d, want 2:\n%s", rng, res.code, res.out)
		}
		if strings.Contains(res.out, "run:") {
			t.Errorf("--plan %q printed a run line: an unreadable range must not read as 'no live test covers it':\n%s", rng, res.out)
		}
	}
}

func TestListNamesEveryRowAndRunsNothing(t *testing.T) {
	w := newWorld(t)
	res := w.run(nil, "--list")
	if res.code != 0 {
		t.Fatalf("exit %d", res.code)
	}
	for _, r := range readRows(t) {
		if !strings.Contains(res.out, r.name) {
			t.Errorf("--list lacks %s", r.name)
		}
	}
}
