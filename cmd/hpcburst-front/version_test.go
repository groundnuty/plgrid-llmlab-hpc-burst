package main

// "hpcburst-front version" (HBEP-15, Releases; issue #401): a subcommand, handled before the
// front's flags are parsed. The front's flags all begin with "--", so the first argument "version"
// cannot be one of them.

import (
	"bytes"
	"runtime/debug"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/version"
)

func TestVersionSubcommandPrintsTheReleaseFromTheBuildInformation(t *testing.T) {
	saved := version.ReadBuildInfo
	t.Cleanup(func() { version.ReadBuildInfo = saved })
	version.ReadBuildInfo = func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}, Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "05cbafcb8ea4755bf2eca64f958d38ba348ff16b"}, {Key: "vcs.time", Value: "2026-11-11T10:00:00Z"}, {Key: "vcs.modified", Value: "false"}}}, true
	}
	var out, errb bytes.Buffer
	code, ok := versionCommand([]string{"version"}, &out, &errb)
	if !ok || code != 0 || errb.Len() != 0 ||
		out.String() != "hpcburst-front (devel) 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z\n" {
		t.Errorf("ok %v, exit %d, stdout %q, stderr %q", ok, code, out.String(), errb.String())
	}
	out.Reset()
	if code, ok := versionCommand([]string{"version", "--config", "/etc/front.json"}, &out, &errb); !ok || code != 2 || out.Len() != 0 ||
		errb.String() != "hpcburst-front version: takes no arguments (got \"--config\")\n" {
		t.Errorf("version with a flag: ok %v, exit %d, stdout %q, stderr %q; want the usage error", ok, code, out.String(), errb.String())
	}
}

// The front's own command lines are not the version command's: they are left to run, with nothing
// printed, and none of its flags is "version".
func TestTheFrontsOwnCommandLinesAreNotTheVersionCommand(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--config", "/etc/front.json", "--state-dir", "/state", "--port-base", "29440"},
		{"--version"},
		{"--mode", "version"},
	} {
		var out, errb bytes.Buffer
		if code, ok := versionCommand(args, &out, &errb); ok || code != 0 || out.Len() != 0 || errb.Len() != 0 {
			t.Errorf("%v: ok %v, exit %d, output %q %q; want it left to run", args, ok, code, out.String(), errb.String())
		}
	}
}
