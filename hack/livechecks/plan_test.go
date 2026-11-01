package livechecks

import (
	"os"
	"path/filepath"
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
		{"an embedded asset in a directory that is not a package", "internal/agent/assets/a.txt", "changed\n", []string{"real-engine"}},
		{"an embedded markdown file of the holding package", "internal/front/embedded.md", "changed\n", []string{"llmlab-smoke"}},
		{"a dependency that only the arm64 build imports", "internal/armonly/armonly.go", "package armonly\n\nvar X = 1\n", []string{"real-engine"}},
		{"the Hub golden test's pinned checkpoints", "spikes/s7/checkpoints.env", "REV=2\n", []string{"hub-golden"}},
		{"a Hub reference listing", "spikes/s7/results/run1/hub/a.json", "{\"x\":1}\n", []string{"hub-golden"}},
		{"a hash reference of the Hub golden test", "spikes/s7/results/run1/hash-a.jsonl", "{\"x\":1}\n", []string{"hub-golden"}},
		{"a file of the spike that the Hub golden test does not read", "spikes/s7/results/run1/other.txt", "changed\n", nil},
		{"the spike's readme", "spikes/s7/README.md", "changed\n", nil},
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

// A file the range deletes was an embedded asset at its base; the head no longer lists it.
func TestPlanCoversADeletedEmbeddedAsset(t *testing.T) {
	w := newWorld(t)
	head := w.commitFile("internal/agent/assets/b.txt", "")
	res := w.run(nil, "--plan", w.base+".."+head)
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	if got := runSet(t, res.out); strings.Join(got, ",") != "real-engine" {
		t.Errorf("run set %v, want [real-engine]\n%s", got, res.out)
	}
}

// Discovery reads the range's own head, not the checkout the user happens to be on. The range adds an
// import edge (provider imports a new package) and the new package in one commit, with the checkout left
// on the base, which has neither: only a closure taken at the head lists the new package, and the plan
// says so by naming it.
func TestPlanReadsTheHeadOfTheRangeNotTheCheckout(t *testing.T) {
	w := newWorld(t)
	write(t, filepath.Join(w.repo, "pkg/newdep/newdep.go"), "package newdep\n", 0o644)
	head := w.commitFile("pkg/provider/provider.go", "package provider\n\nimport _ \"example.test/fx/pkg/newdep\"\n")
	git(t, w.repo, "checkout", "-q", w.base) // the checkout predates the dependency
	res := w.run(nil, "--plan", w.base+".."+head)
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	var covered string
	for _, l := range strings.Split(res.out, "\n") {
		if strings.HasPrefix(l, "covered: llmlab-smoke") {
			covered = l
		}
	}
	if !strings.Contains(covered, "pkg/newdep") {
		t.Errorf("pkg/newdep is a dependency at the range's head and must be named among what covers llmlab-smoke:\n%s", res.out)
	}
}

// A discovery that fails, or lists only part of the closure, must not read as "no live test covers this".
func TestPlanFailsWhenDependencyDiscoveryFails(t *testing.T) {
	w := newWorld(t)
	head := w.commitFile("internal/slurm/broken.go", "package slurm\n\nimport _ \"example.test/fx/nonexistent\"\n")
	res := w.run(nil, "--plan", w.base+".."+head)
	if res.code != 2 {
		t.Fatalf("exit %d, want 2:\n%s", res.code, res.out)
	}
	if strings.Contains(res.out, "run:") {
		t.Errorf("a failed discovery printed a run line:\n%s", res.out)
	}
	if !strings.Contains(res.out, "dependency discovery failed for package internal/providerctl/conformance (host) at the head of the range") {
		t.Errorf("no fixed failure message:\n%s", res.out)
	}
}
