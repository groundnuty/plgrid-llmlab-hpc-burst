package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// printAgentBuilds writes the agent-builds section of the text status: one line per cpuArch the
// PlatformProfiles' partitions have, with each build's state, and why the check was skipped when
// the API server was not asked.
func printAgentBuilds(w io.Writer, ab *launcher.AgentBuilds) {
	if ab == nil {
		return
	}
	if !ab.Checked {
		fmt.Fprintf(w, "agent builds (%s): not checked: %s\n", ab.Dir, ab.Detail)
		return
	}
	if len(ab.Archs) == 0 && len(ab.Problems) == 0 {
		fmt.Fprintf(w, "agent builds (%s): none needed (%d PlatformProfiles, no partition)\n", ab.Dir, ab.Profiles)
		return
	}
	fmt.Fprintf(w, "agent builds (%s):\n", ab.Dir)
	for _, a := range ab.Archs {
		fmt.Fprintf(w, "  %s: %s, %s (partitions %s)\n", a.CPUArch, buildState(a.Agent), buildState(a.Frpc), strings.Join(a.Partitions, ", "))
	}
	for _, p := range ab.Problems {
		if !hasArch(ab, p.CPUArch) {
			fmt.Fprintf(w, "  %s: unknown cpuArch (partitions %s)\n", p.CPUArch, strings.Join(p.Partitions, ", "))
		}
	}
}

func hasArch(ab *launcher.AgentBuilds, cpuArch string) bool {
	for _, a := range ab.Archs {
		if a.CPUArch == cpuArch {
			return true
		}
	}
	return false
}

// buildState is one build's state in a line of the status: its name, and ok with the digest found
// (and the ELF machine of an agent build), missing, wrong for its architecture, or the digest found
// against the one expected.
func buildState(f launcher.BuildFile) string {
	switch {
	case f.OK && f.Machine != "":
		return fmt.Sprintf("%s ok (%s, sha256 %s)", f.Name, f.Machine, f.SHA256)
	case f.OK:
		return fmt.Sprintf("%s ok (sha256 %s)", f.Name, f.SHA256)
	case !f.Present:
		return f.Name + " MISSING"
	case f.SHA256 == "":
		return f.Name + " UNREADABLE"
	case f.Detail != "":
		return fmt.Sprintf("%s WRONG (%s, sha256 %s)", f.Name, f.Detail, f.SHA256)
	default:
		return fmt.Sprintf("%s MISMATCH (found %s, expected %s)", f.Name, f.SHA256, f.Expected)
	}
}

// printBuildsError is what up prints when the host is running but its builds are not in place: a
// loud ERROR that names each cpuArch, the digests and the command that fixes it, and says the host
// was left running.
func printBuildsError(w io.Writer, e *launcher.AgentBuildsError, stateDir string) {
	ab := e.Builds
	if !ab.Checked {
		fmt.Fprintf(w, "ERROR: the host is up, but its agent builds could not be checked: %s\n", ab.Detail)
	} else {
		fmt.Fprintf(w, "ERROR: the host is up, but its agent builds in %s are missing or do not match, so the first submission would be refused:\n", ab.Dir)
		for _, a := range ab.Archs {
			for _, f := range []launcher.BuildFile{a.Agent, a.Frpc} {
				if !f.OK {
					fmt.Fprintf(w, "ERROR:   cpuArch %s: %s\n", a.CPUArch, buildState(f))
				}
			}
		}
		for _, p := range ab.Problems {
			fmt.Fprintf(w, "ERROR:   cpuArch %s (partitions %s): %s\nERROR:     fix: %s\n", p.CPUArch, strings.Join(p.Partitions, ", "), p.What, p.Fix)
		}
	}
	fmt.Fprintf(w, "ERROR: the host was left running; fix the builds, then run: hpcburst status --state-dir %s\n", launcher.ShellQuote(stateDir))
}
