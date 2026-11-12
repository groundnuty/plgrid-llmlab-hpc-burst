package release

// The archive's agents report the release's version (issue #469): hack/release-archive.sh builds them
// with hack/build-agents.sh --version and the version its host binaries report, and asks the x86_64
// agent what it reports, since the linker's -X ignores a variable that is not there. The fixture's
// hb-agent is agentStandIn, which prints its -version line as cmd/hb-agent does.

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// agentStandIn is the fixture's cmd/hb-agent: a version variable the linker sets, and the -version line
// of the real agent (cmd/hb-agent/main.go), on stderr.
const agentStandIn = `package main

import (
	"fmt"
	"os"
)

// version is set with -ldflags "-X main.version=<version>", as cmd/hb-agent's is.
var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-version" {
		fmt.Fprintf(os.Stderr, "hb-agent %s, protocol v1\n", version)
	}
}
`

// agentWithoutTheVariable is a cmd/hb-agent whose version is not the variable the linker sets: the
// build succeeds, and the agent says dev.
const agentWithoutTheVariable = `package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-version" {
		fmt.Fprintf(os.Stderr, "hb-agent %s, protocol v1\n", "dev")
	}
}
`

// requireX86 skips where the x86_64 agent cannot run, which is where the archive script does not ask it.
func requireX86(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skipf("the x86_64 agent runs only on linux amd64, not %s %s", runtime.GOOS, runtime.GOARCH)
	}
}

// agentSays unpacks archive and returns what its x86_64 agent's -version prints.
func agentSays(t *testing.T, archive string) string {
	t.Helper()
	dir := t.TempDir()
	if out, code := run(t, dir, nil, "tar", "-xzf", archive); code != 0 {
		t.Fatal(out)
	}
	out, code := run(t, dir, nil, filepath.Join(dir, "agent-builds", "hb-agent-x86_64"), "-version")
	if code != 0 {
		t.Fatalf("hb-agent-x86_64 -version: exit %d: %s", code, out)
	}
	return out
}

// A tag's archive holds agents that report the tag, and the script says what the x86_64 agent reported.
func TestArchiveAgentsReportTheTag(t *testing.T) {
	requireX86(t)
	_, archive := shared(t)
	if got, want := agentSays(t, archive), "hb-agent "+fixtureTag+", protocol v1\n"; got != want {
		t.Errorf("the archive's hb-agent-x86_64 -version prints %q, want %q", got, want)
	}
	if want := "agent    hb-agent-x86_64 reports: hb-agent " + fixtureTag + ", protocol v1"; !strings.Contains(sharedOutput, want) {
		t.Errorf("the build's output lacks %q:\n%s", want, sharedOutput)
	}
}

// A dry run builds the agents as a tag does, with the pseudo-version of the commit: the version the
// archive is named by and its host binaries report.
func TestArchiveAgentsOfADryRunReportThePseudoVersion(t *testing.T) {
	requireX86(t)
	f := newFixture(t)
	write(t, filepath.Join(f.dir, "later.txt"), "a commit after the tag\n", 0o644)
	git(t, f.dir, "add", "-A")
	git(t, f.dir, "commit", "-q", "-m", "after the tag")
	head := git(t, f.dir, "rev-parse", "HEAD")
	out, code := f.build(t)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, out)
	}
	entries, err := os.ReadDir(f.out)
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`^hpcburst-(v[0-9][0-9A-Za-z.-]*[-.][0-9]{14}-` + head[:12] + `)-linux-amd64\.tar\.gz$`)
	var version string
	for _, e := range entries {
		if m := re.FindStringSubmatch(e.Name()); m != nil {
			version = m[1]
		}
	}
	if version == "" {
		t.Fatalf("no archive named by a pseudo-version of %s in %v", head[:12], entries)
	}
	if got, want := agentSays(t, filepath.Join(f.out, "hpcburst-"+version+"-linux-amd64.tar.gz")), "hb-agent "+version+", protocol v1\n"; got != want {
		t.Errorf("the dry run's hb-agent-x86_64 -version prints %q, want %q", got, want)
	}
}

// An agent whose version the linker could not set is refused before the archive is written: the build
// succeeds, since -X ignores a variable that is not there, so only the agent's own line shows it.
func TestArchiveRefusesAnAgentThatDoesNotReportTheVersion(t *testing.T) {
	requireX86(t)
	f := newFixture(t)
	write(t, filepath.Join(f.dir, "cmd", "hb-agent", "main.go"), agentWithoutTheVariable, 0o644)
	write(t, filepath.Join(f.dir, "docs", "releases", "v0.1.0-alpha.2.md"), "The summary of v0.1.0-alpha.2.\n", 0o644)
	git(t, f.dir, "add", "-A")
	git(t, f.dir, "commit", "-q", "-m", "an agent without the variable")
	git(t, f.dir, "tag", "v0.1.0-alpha.2")
	out, code := f.build(t, "--tag", "v0.1.0-alpha.2")
	want := "hb-agent-x86_64 reports 'hb-agent dev, protocol v1', and the host binaries report v0.1.0-alpha.2"
	if code == 0 || !strings.Contains(out, want) {
		t.Fatalf("exit %d, want a refusal saying %q:\n%s", code, want, out)
	}
	f.emptyOut(t)
}
