package livechecks

import (
	"fmt"
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
// package, which the accounting rows share, imports providerctl, which imports
// slurm; hb-agent imports agent, which embeds data.txt; the alpha rows and the
// rehearsal run the stack, so they take the closure of the three host binaries and hb-agent too),
// not from the script's table or its output.
func TestPlanMapsChangedFilesToTheLiveTestsThatRunThem(t *testing.T) {
	cases := []struct {
		name, path, content string
		want                []string
	}{
		{"docs only", "docs/notes.md", "changed\n", nil},
		{"a readme inside a live package", "internal/front/README.md", "changed\n", nil},
		{"a package that nothing live imports", "unrelated/unrelated.go", "package unrelated\n\nvar X = 1\n", nil},
		{"a dependency only: slurm", "internal/slurm/slurm.go", "package slurm\n\nvar X = 1\n", []string{"alpha-athena-now-stage", "conformance-athena", "conformance-helios", "conformance-helios-gh200", "gpu-accounting-athena", "gpu-accounting-helios", "reach-athena", "reach-helios", "reach-host-dialed-athena", "reach-host-dialed-helios"}},
		{"a dependency of one test: pkg/provider", "pkg/provider/provider.go", "package provider\n\nvar X = 1\n", []string{"llmlab-smoke"}},
		{"a holding package", "internal/front/front.go", "package front\n\nimport _ \"example.test/fx/pkg/provider\"\n\nvar X = 1\n", []string{"llmlab-smoke"}},
		{"a test file of the holding package", "internal/front/front_test.go", "package front\n\nimport \"testing\"\n\nfunc TestLLMLabThroughTheFront(t *testing.T) { _ = 1 }\n", []string{"llmlab-smoke"}},
		{"a test file of a dependency", "internal/slurm/slurm_test.go", "package slurm\n\nimport \"testing\"\n\nfunc TestSlurm(t *testing.T) { _ = 1 }\n", nil},
		{"go.mod", "go.mod", fixtureGoMod + "\n// a comment\n", []string{"alpha-athena-handover", "alpha-athena-now-serve", "alpha-athena-now-stage", "alpha-helios-nooverlap", "conformance-athena", "conformance-helios", "conformance-helios-gh200", "gpu-accounting-athena", "gpu-accounting-helios", "hub-golden", "llmlab-smoke", "reach-athena", "reach-helios", "reach-host-dialed-athena", "reach-host-dialed-helios", "real-engine", "rehearsal-part3", "tunnel-hour-athena", "tunnel-hour-helios"}},
		{"a profile read whole by the runs on that platform", "profiles/helios.yaml", "name: helios\nx: 1\n", []string{"alpha-helios-nooverlap", "conformance-helios", "conformance-helios-gh200", "gpu-accounting-helios", "reach-helios", "reach-host-dialed-helios", "tunnel-hour-helios"}},
		{"the real-engine job script", "spikes/s94/job.sh", "#!/bin/bash\n# changed\n", []string{"real-engine"}},
		{"a file the agent embeds", "internal/agent/data.txt", "changed\n", agentEmbedders},
		{"a file the agent does not embed", "internal/agent/notes.txt", "changed\n", nil},
		{"an embedded asset in a directory that is not a package", "internal/agent/assets/a.txt", "changed\n", agentEmbedders},
		{"an embedded markdown file of the holding package", "internal/front/embedded.md", "changed\n", []string{"llmlab-smoke"}},
		{"a dependency that only the arm64 build imports", "internal/armonly/armonly.go", "package armonly\n\nvar X = 1\n", []string{"alpha-helios-nooverlap", "reach-host-dialed-helios", "real-engine", "tunnel-hour-helios"}},
		{"the rehearsal's script, which runs the row", "hack/rehearsal.sh", "#!/bin/bash\n# changed\n", []string{"rehearsal-part3"}},
		{"a CRD manifest the rehearsal installs", "config/crd/a.yaml", "kind: CustomResourceDefinition\n", []string{"rehearsal-part3"}},
		{"a test file of the rehearsal's package", "internal/rehearsal/scenario/part3_test.go", "package scenario_test\n\nimport \"testing\"\n\nfunc TestRehearsalPart3(t *testing.T) { _ = 1 }\n", []string{"rehearsal-part3"}},
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

// agentEmbedders are the rows whose build contains hb-agent, and so what it embeds: the real-engine run,
// the five rows that run the stack, the rehearsal among them, and the two host-dialed relay rows, whose job is
// the agent (sorted, as runSet gives them).
var agentEmbedders = []string{"alpha-athena-handover", "alpha-athena-now-serve", "alpha-helios-nooverlap", "reach-host-dialed-athena", "reach-host-dialed-helios", "real-engine", "rehearsal-part3", "tunnel-hour-athena", "tunnel-hour-helios"}

func TestPlanNamesWhatChangedAndNothingPrivate(t *testing.T) {
	w := newWorld(t)
	res := w.run(nil, "--plan", w.commit("internal/slurm/slurm.go", "package slurm\n\nvar X = 1\n"))
	for _, want := range []string{"1 file(s) changed", "covered: conformance-helios", "changed internal/slurm", "not covered: llmlab-smoke hub-golden rehearsal-part3 real-engine alpha-athena-handover alpha-helios-nooverlap tunnel-hour-helios tunnel-hour-athena"} {
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
	if got := runSet(t, res.out); strings.Join(got, ",") != strings.Join(agentEmbedders, ",") {
		t.Errorf("run set %v, want %v\n%s", got, agentEmbedders, res.out)
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

// The plan's lookups in a closure listing must not depend on how a pipe's reader and writer are scheduled
// (#213). It once tested membership with printf ... | grep -q under pipefail: bash's stdout is line-buffered,
// so printf writes the listing a line at a time, grep -q exits at the first match, and a printf still
// writing dies of SIGPIPE, which fails the pipeline and reads as "not covered" with exit 0. Under load that
// lost a row now and then. A listing larger than a pipe's buffer (64 KiB on Linux) makes the race certain:
// printf cannot have written it all before grep exits. The assets here are many long-named files the agent
// embeds, so the listing of every row that builds hb-agent is that large, and each case changes a file
// whose line sorts early in it. The expected rows follow from the fixture's wiring (see agentEmbedders).
func TestPlanKeepsEveryRowWhenTheClosureListingIsLarge(t *testing.T) {
	const assets, pipeBuffer = 1500, 64 << 10
	name := func(i int) string {
		return fmt.Sprintf("internal/agent/assets/%s-%04d.txt", strings.Repeat("x", 120), i)
	}
	// The seam is only a seam while the listing is well over the buffer: printf may fill the pipe once
	// while grep reads it, so more than twice the buffer leaves printf certainly still writing.
	listing := 0
	for i := 0; i < assets; i++ {
		listing += len("embed:"+name(i)) + 1
	}
	if listing <= 2*pipeBuffer {
		t.Fatalf("the embedded assets list %d bytes, not more than twice a pipe's buffer: the test would prove nothing", listing)
	}
	cases := []struct{ name, path, content string }{
		// a.txt sorts before the long names, so its embed: line is near the top of the listing.
		{"an embedded asset", "internal/agent/assets/a.txt", "changed\n"},
		// cmd/hb-agent sorts before every embed: line; for the stack rows it is a dependency, not their package.
		{"the agent binary's package", "cmd/hb-agent/main.go", "package main\n\nimport _ \"example.test/fx/internal/agent\"\n\nfunc main() { _ = 1 }\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWorld(t)
			for i := 0; i < assets; i++ {
				write(t, filepath.Join(w.repo, name(i)), "asset\n", 0o644)
			}
			git(t, w.repo, "add", "-A")
			git(t, w.repo, "commit", "-q", "-m", "many embedded assets")
			base := git(t, w.repo, "rev-parse", "HEAD")
			head := w.commitFile(c.path, c.content)
			res := w.run(nil, "--plan", base+".."+head)
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			if got := runSet(t, res.out); strings.Join(got, ",") != strings.Join(agentEmbedders, ",") {
				t.Errorf("run set %v, want %v\n%s", got, agentEmbedders, res.out)
			}
		})
	}
}
