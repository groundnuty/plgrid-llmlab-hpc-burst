package main

// "hpcburst version" (HBEP-15, Releases; issue #401): the release from the binary's build information.
// The commands run in this process through run, with a build information the test gives (the
// internal/version seam), and the expected lines are literals. --version keeps its k3s meaning: it
// is a flag of up, and hpcburst has no --version of its own.

import (
	"bytes"
	"context"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/version"
)

// withBuild makes the binary's build information the one given, until the test ends.
func withBuild(t *testing.T, moduleVersion, modified string) {
	t.Helper()
	saved := version.ReadBuildInfo
	t.Cleanup(func() { version.ReadBuildInfo = saved })
	settings := []debug.BuildSetting{
		{Key: "vcs.revision", Value: "05cbafcb8ea4755bf2eca64f958d38ba348ff16b"},
		{Key: "vcs.time", Value: "2026-11-11T10:00:00Z"},
		{Key: "vcs.modified", Value: modified},
	}
	version.ReadBuildInfo = func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{Main: debug.Module{Version: moduleVersion}, Settings: settings}, true
	}
}

func runInProcess(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errb bytes.Buffer
	code = run(context.Background(), args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestVersionSubcommandPrintsTheReleaseFromTheBuildInformation(t *testing.T) {
	for _, tc := range []struct{ name, module, modified, want string }{
		{"a tagged build", "v0.1.0-alpha.1", "false", "hpcburst v0.1.0-alpha.1 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z\n"},
		{"a devel build", "(devel)", "false", "hpcburst (devel) 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z\n"},
		{"a dirty build", "v0.1.0-alpha.1+dirty", "true", "hpcburst v0.1.0-alpha.1 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z dirty\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withBuild(t, tc.module, tc.modified)
			code, out, errs := runInProcess(t, "version")
			if code != exitOK || out != tc.want || errs != "" {
				t.Errorf("exit %d, stdout %q, stderr %q; want exit 0, stdout %q, no stderr", code, out, errs, tc.want)
			}
		})
	}
}

// The command takes no arguments: --version after it, which would be the k3s flag, is a usage
// error and prints no version.
func TestVersionSubcommandTakesNoArguments(t *testing.T) {
	withBuild(t, "v0.1.0-alpha.1", "false")
	for _, args := range [][]string{{"version", "--version", "1.99.0"}, {"version", "--json"}, {"version", "extra"}} {
		code, out, errs := runInProcess(t, args...)
		if code != exitUsage || out != "" || errs != "hpcburst version: takes no arguments (got \""+args[1]+"\")\n" {
			t.Errorf("%v: exit %d, stdout %q, stderr %q; want exit 2, no stdout and the usage line", args, code, out, errs)
		}
	}
}

// "version -h" prints the command's usage on stdout and exits 0, as "hpcburst help" does.
func TestVersionHelp(t *testing.T) {
	code, out, errs := runInProcess(t, "version", "-h")
	if code != exitOK || errs != "" || out != "usage: hpcburst version  (prints one line: hpcburst <tag> <commit> <time>[ dirty])\n" {
		t.Errorf("version -h: exit %d, stdout %q, stderr %q", code, out, errs)
	}
}

// --version keeps its meaning. hpcburst has no top-level --version (it is an unknown command, exit
// 2, printing no version), up's --version is still the k3s release to start (its help says so, and
// an unpinned release is refused as one, before anything starts), and the usage names the command.
func TestVersionFlagKeepsItsK3sMeaning(t *testing.T) {
	withBuild(t, "v0.1.0-alpha.1", "false")
	for _, arg := range []string{"--version", "-version", "-v"} {
		code, out, errs := runInProcess(t, arg)
		if code != exitUsage || out != "" || !strings.Contains(errs, "unknown command \""+arg+"\"") {
			t.Errorf("%s: exit %d, stdout %q, stderr %q; want exit 2, no stdout, an unknown command", arg, code, out, errs)
		}
	}

	code, out, errs := runInProcess(t, "up", "-h")
	if code != exitOK || out != "" || !strings.Contains(errs, "-version string") || !strings.Contains(errs, "k3s release to start (must be pinned)") {
		t.Errorf("up -h: exit %d, stdout %q; its --version is not the k3s release flag:\n%s", code, out, errs)
	}

	state := shortTmp(t, false) + "/st"
	r := cli(t, "up", "--state-dir", state, "--bin-dir", t.TempDir(), "--version", "1.99.0")
	if r.code != exitFail || !strings.Contains(r.err, "not pinned") {
		t.Errorf("up --version 1.99.0: exit %d, stderr %q; want the k3s release refused as not pinned", r.code, r.err)
	}
	if strings.Contains(r.out+r.err, "hpcburst v") || strings.Contains(r.out+r.err, "05cbafcb8ea4") {
		t.Errorf("up --version printed a release line: %q %q", r.out, r.err)
	}

	code, _, errs = runInProcess(t)
	if code != exitUsage || !strings.Contains(errs, "usage: hpcburst <up|status|upgrade|restore|down|guard|version> [flags]") {
		t.Errorf("no command: exit %d, stderr %q; want the usage naming version", code, errs)
	}
}
