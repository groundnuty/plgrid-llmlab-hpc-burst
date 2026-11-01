package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// What up prints when a build is missing or mismatched names the cpuArch, the digest found and the
// digest expected, and the command that fixes it; every line starts with ERROR so it cannot be missed.
func TestPrintBuildsErrorNamesTheCPUArchTheDigestsAndTheFix(t *testing.T) {
	const found, want = "1111111111111111111111111111111111111111111111111111111111111111", "2222222222222222222222222222222222222222222222222222222222222222"
	ab := &launcher.AgentBuilds{Dir: "/state/agent-builds", Checked: true, Profiles: 1,
		Archs: []launcher.ArchBuilds{{
			CPUArch: "aarch64", Partitions: []string{"example-batch/example-gpu"},
			Agent: launcher.BuildFile{Name: "hb-agent-aarch64"},
			Frpc:  launcher.BuildFile{Name: "frpc-aarch64", Present: true, SHA256: found, Expected: want},
		}},
		Problems: []launcher.BuildProblem{
			{CPUArch: "aarch64", Partitions: []string{"example-batch/example-gpu"}, What: "hb-agent-aarch64 is missing from /state/agent-builds", Fix: "hack/build-agents.sh /state/agent-builds aarch64"},
			{CPUArch: "aarch64", Partitions: []string{"example-batch/example-gpu"}, What: "frpc-aarch64 in /state/agent-builds has sha256 " + found + ", but the pin (frp-pins.txt) says " + want, Fix: "hack/fetch-frp.sh --agent-builds /state/agent-builds"},
		}}
	var b bytes.Buffer
	printBuildsError(&b, &launcher.AgentBuildsError{Builds: ab}, "/state")
	out := b.String()
	for _, w := range []string{
		"cpuArch aarch64", "hb-agent-aarch64 is missing from /state/agent-builds", "fix: hack/build-agents.sh /state/agent-builds aarch64",
		"found " + found + ", expected " + want, "fix: hack/fetch-frp.sh --agent-builds /state/agent-builds",
		"left running", "hpcburst status --state-dir /state",
	} {
		if !strings.Contains(out, w) {
			t.Errorf("the error lacks %q:\n%s", w, out)
		}
	}
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if !strings.HasPrefix(line, "ERROR:") {
			t.Errorf("a line without ERROR: %q", line)
		}
	}
}

// A check that could not run is said so, and is not mistaken for a list of missing builds.
func TestPrintBuildsErrorWhenTheCheckCouldNotRun(t *testing.T) {
	var b bytes.Buffer
	printBuildsError(&b, &launcher.AgentBuildsError{Builds: &launcher.AgentBuilds{Dir: "/d", Detail: "read the PlatformProfiles: boom"}}, "/state")
	if out := b.String(); !strings.Contains(out, "could not be checked: read the PlatformProfiles: boom") || strings.Contains(out, "missing or do not match") {
		t.Errorf("%s", out)
	}
}

func TestPrintAgentBuildsLines(t *testing.T) {
	var b bytes.Buffer
	printAgentBuilds(&b, &launcher.AgentBuilds{Dir: "/d", Detail: "--no-api: not asked"})
	printAgentBuilds(&b, &launcher.AgentBuilds{Dir: "/d", Checked: true, Profiles: 0})
	printAgentBuilds(&b, nil)
	out := b.String()
	if !strings.Contains(out, "agent builds (/d): not checked: --no-api: not asked") || !strings.Contains(out, "agent builds (/d): none needed (0 PlatformProfiles, no partition)") {
		t.Errorf("%s", out)
	}
}
