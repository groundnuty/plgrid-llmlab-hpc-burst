package quickstart

// The quickstart's beta router section (#454) stands on its own, so a rewrite of step 4 cannot drop it
// unseen: it holds exactly one block, whose commands are, in order, hack/fetch-envoy.sh --agent-builds
// into the state directory's agent builds and hpcburst up with --instance-router envoy. Both are
// checked against the real script and binary, as the rest of the quickstart's commands are.

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

// routerSection is the text of the quickstart's "## Beta: Instances behind a router" section.
func routerSection(t *testing.T) string {
	t.Helper()
	text := readRepoFile(t, "docs/quickstart.md")
	const head = "## Beta: Instances behind a router\n"
	i := strings.Index(text, head)
	if i < 0 {
		t.Fatalf("docs/quickstart.md has no %q section", strings.TrimSpace(head))
	}
	rest := text[i+len(head):]
	if j := strings.Index(rest, "\n## "); j >= 0 {
		rest = rest[:j]
	}
	return rest
}

func TestTheRouterSectionFetchesEnvoyThenStartsWithTheRouter(t *testing.T) {
	sec := routerSection(t)
	bl := fence.FindAllStringSubmatch(sec, -1)
	if len(bl) != 1 || bl[0][1] != "" {
		t.Fatalf("the router section holds %d blocks, want one shell block", len(bl))
	}
	d, home := testDirs(t)
	var cmds []command
	for _, l := range joinContinuations(bl[0][2]) {
		if l = strings.TrimSpace(l); l != "" {
			cmds = append(cmds, command{line: l, words: shellWords(t, l, d, home)})
		}
	}
	if len(cmds) != 2 {
		t.Fatalf("the router section's block has %d commands, want the fetch and up: %q", len(cmds), cmds)
	}
	fetch, up := cmds[0].words, cmds[1].words
	if !slices.Equal(fetch, []string{"hack/fetch-envoy.sh", "--agent-builds", d.state + "/agent-builds"}) {
		t.Errorf("the first command is %q, want hack/fetch-envoy.sh --agent-builds into the state directory's agent builds", fetch)
	}
	runScript(t, home, cmds[0])
	if len(up) < 2 || up[0] != "bin/hpcburst" || up[1] != "up" {
		t.Fatalf("the second command is %q, want bin/hpcburst up", up)
	}
	if i := slices.Index(up, "--instance-router"); i < 0 || i+1 >= len(up) || up[i+1] != "envoy" {
		t.Errorf("the up command %q has no --instance-router envoy", up)
	}
	if i := slices.Index(up, "--state-dir"); i < 0 || i+1 >= len(up) || up[i+1] != d.state {
		t.Errorf("the up command %q does not start the state directory the fetch filled", up)
	}
	checkHpcburst(t, home, up[1:])
	// The prose names the glibc the agent checks, as HBEP-5 records it.
	if !regexp.MustCompile(`glibc 2\.30 or later`).MatchString(sec) {
		t.Error("the router section does not say Envoy needs glibc 2.30 or later")
	}
}
