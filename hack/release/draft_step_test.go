package release

// Issue #547 (HBEP-15, Releases): a tag with a pre-release part (-alpha.N, -beta.N, -rc.N) is drafted
// with gh release create --prerelease, so that publishing it never makes it the latest release, and a
// plain vMAJOR.MINOR.PATCH is not. The decision is shell inside release.yml's draft step, since no code
// of the repository may run beside the write token (see workflows_test.go).
//
// This test runs that step's own shell, extracted from release.yml, as GitHub runs a step of a workflow
// whose defaults say shell: bash (bash --noprofile --norc -eo pipefail), once for each tag below. A stub
// gh comes first on PATH and records its arguments. A test that only looked for the string --prerelease
// in the file would pass whether or not the condition is right, so none does. Expected values are the
// literals of the table, not anything the step computes.

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// ghStub stands in for gh: it appends a marker line and its arguments, one per line (none of ours holds a
// newline), to the file named by GH_STUB_LOG, and succeeds.
const ghStub = `#!/bin/sh
{ echo '[call]'; printf '%s\n' "$@"; } >> "$GH_STUB_LOG"
`

// The tags of the table are valid ones for hack/release-check.sh, which the workflow's check job has
// already run on the tag when the draft step runs: v1.0.0-0 has a numeric pre-release identifier,
// v1.0.0-x-y a hyphen inside one, and v10.20.30 a plain version of several digits.
var draftTags = []struct {
	tag        string
	preRelease bool
}{
	{"v0.1.0-alpha.1", true},
	{"v1.2.0-beta.3", true},
	{"v2.0.0-rc.1", true},
	{"v0.2.0-beta.1", true},
	{"v1.0.0-0", true},
	{"v1.0.0-x-y", true},
	{"v0.1.0", false},
	{"v1.10.0", false},
	{"v10.20.30", false},
}

// draftStepOf returns the draft step's run text and its env, from release.yml.
func draftStepOf(t *testing.T) (string, map[string]any) {
	t.Helper()
	d := readWorkflow(t, "release.yml")
	if shell := str(asMap(asMap(d["defaults"])["run"])["shell"]); shell != "bash" {
		t.Fatalf("release.yml's default shell is %q: this test runs the step as GitHub runs shell: bash, and must be changed with it", shell)
	}
	step := stepNamed(t, jobsOf(t, d)["draft"], "Create the draft release")
	if _, ok := step["shell"]; ok {
		t.Fatal("the draft step names a shell of its own: this test runs it as the default shell: bash")
	}
	run := str(step["run"])
	if strings.TrimSpace(run) == "" {
		t.Fatal("the draft step has no run text")
	}
	if strings.Contains(run, "${{") {
		t.Errorf("the draft step's script has an expression in it: a tag name must reach it through env: %q", run)
	}
	return run, asMap(step["env"])
}

// stepEnv gives the step's env the values its expressions have in a push of the tag. An expression this
// test does not know fails it, so that a new input of the step is not silently left unset.
func stepEnv(t *testing.T, env map[string]any, tag string) []string {
	t.Helper()
	values := map[string]string{
		"${{ github.ref_name }}": tag,
		"${{ github.token }}":    "stub-token",
	}
	var out []string
	for name, v := range env {
		val, ok := values[str(v)]
		if !ok {
			t.Fatalf("the draft step's env %s is %q, an expression this test does not evaluate", name, v)
		}
		out = append(out, name+"="+val)
	}
	if _, ok := env["TAG"]; !ok {
		t.Fatal("the draft step's env has no TAG: the tag must reach the script through env")
	}
	return out
}

// runDraftStep runs the step's shell for the tag, in a fake checkout that holds its notes file, with the
// archive and checksums where the previous steps leave them, and returns what the stub gh was called with
// and the step's RUNNER_TEMP.
func runDraftStep(t *testing.T, tag string) (argv []string, runnerTemp string) {
	t.Helper()
	run, env := draftStepOf(t)
	base := t.TempDir()
	checkout, bin, log := filepath.Join(base, "checkout"), filepath.Join(base, "bin"), filepath.Join(base, "gh.log")
	runnerTemp = filepath.Join(base, "runner-temp")
	write(t, filepath.Join(checkout, "docs", "releases", tag+".md"), "# "+tag+"\n", 0o644)
	write(t, filepath.Join(runnerTemp, "release", "hpcburst-"+tag+"-linux-amd64.tar.gz"), "archive\n", 0o644)
	write(t, filepath.Join(runnerTemp, "release", "SHA256SUMS"), "sums\n", 0o644)
	write(t, filepath.Join(bin, "gh"), ghStub, 0o755)
	script := filepath.Join(base, "step.sh")
	write(t, script, run, 0o644)

	cmd := exec.Command("bash", "--noprofile", "--norc", "-eo", "pipefail", script)
	cmd.Dir = checkout
	// An environment of its own: the machine's own GH_TOKEN or TAG must not reach the step, and the
	// stub, first on PATH, is the only gh it can find.
	cmd.Env = append(stepEnv(t, env, tag), "PATH="+bin+":/usr/bin:/bin", "RUNNER_TEMP="+runnerTemp, "GH_STUB_LOG="+log, "HOME="+base)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the draft step failed for %s: %v\n%s", tag, err, out)
	}
	raw, err := os.ReadFile(log)
	if err != nil {
		t.Fatalf("the draft step ran for %s and never called gh: %v\n%s", tag, err, out)
	}
	calls := strings.Split(string(raw), "[call]\n")[1:]
	if len(calls) != 1 {
		t.Fatalf("the draft step called gh %d times for %s, want once: %q", len(calls), tag, raw)
	}
	return strings.Split(strings.TrimSuffix(calls[0], "\n"), "\n"), runnerTemp
}

func count(list []string, want string) int {
	n := 0
	for _, s := range list {
		if s == want {
			n++
		}
	}
	return n
}

// The draft step gives gh --prerelease for a tag with a pre-release part and for no other, and in every
// case still makes a draft of the same release, with the same files and notes.
func TestReleaseDraftStepIsAPreReleaseExactlyWhenTheTagHasAPreReleasePart(t *testing.T) {
	for _, c := range draftTags {
		t.Run(c.tag, func(t *testing.T) {
			argv, runnerTemp := runDraftStep(t, c.tag)

			if len(argv) < 5 || argv[0] != "release" || argv[1] != "create" || argv[2] != c.tag {
				t.Fatalf("gh was called with %q, want it to start release create %s", argv, c.tag)
			}
			got, want := count(argv, "--prerelease"), 0
			if c.preRelease {
				want = 1
			}
			switch {
			case c.preRelease && got == 0:
				t.Errorf("%s has a pre-release part and the draft step gave gh no --prerelease, so publishing it would make it the latest release: gh got %q", c.tag, argv)
			case !c.preRelease && got > 0:
				t.Errorf("%s has no pre-release part and the draft step gave gh --prerelease: gh got %q", c.tag, argv)
			case got != want:
				t.Errorf("the draft step gave gh --prerelease %d times for %s, want %d: gh got %q", got, c.tag, want, argv)
			}
			if got := count(argv, "--draft"); got != 1 {
				t.Errorf("the draft step gave gh --draft %d times for %s, want once, so that nothing is published: gh got %q", got, c.tag, argv)
			}

			// The rest of the call is what it was before #547: the two files last, the notes file
			// of the tag, and no flag but --verify-tag and --generate-notes beside the two above.
			archive := filepath.Join(runnerTemp, "release", "hpcburst-"+c.tag+"-linux-amd64.tar.gz")
			sums := filepath.Join(runnerTemp, "release", "SHA256SUMS")
			if n := len(argv); argv[n-2] != archive || argv[n-1] != sums {
				t.Errorf("gh ended with %q, want the archive %s and then %s", argv[len(argv)-2:], archive, sums)
			}
			var flags []string
			notes := ""
			for i := 3; i < len(argv)-2; i++ {
				switch argv[i] {
				case "--prerelease", "--draft":
				case "--notes-file":
					if i++; i < len(argv)-2 {
						notes = argv[i]
					}
				default:
					flags = append(flags, argv[i])
				}
			}
			sort.Strings(flags)
			if got, want := strings.Join(flags, " "), "--generate-notes --verify-tag"; got != want {
				t.Errorf("gh got the other arguments %q for %s, want %q: gh got %q", got, c.tag, want, argv)
			}
			if want := "docs/releases/" + c.tag + ".md"; notes != want {
				t.Errorf("gh got --notes-file %q for %s, want %q", notes, c.tag, want)
			}
		})
	}
}
