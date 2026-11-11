package main

// "hpcburst-controller version" (HBEP-15, Releases; issue #401): a first argument, handled before
// the controller's flags are parsed, so no flag of the controller can be taken for it.

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
		return &debug.BuildInfo{Main: debug.Module{Version: "v0.1.0-alpha.1"}, Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "05cbafcb8ea4755bf2eca64f958d38ba348ff16b"}, {Key: "vcs.time", Value: "2026-11-11T10:00:00Z"}, {Key: "vcs.modified", Value: "true"}}}, true
	}
	var out, errb bytes.Buffer
	code, ok := versionCommand([]string{"version"}, &out, &errb)
	if !ok || code != 0 || errb.Len() != 0 ||
		out.String() != "hpcburst-controller v0.1.0-alpha.1 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z dirty\n" {
		t.Errorf("ok %v, exit %d, stdout %q, stderr %q", ok, code, out.String(), errb.String())
	}
	out.Reset()
	if code, ok := versionCommand([]string{"version", "--state-dir", "/state"}, &out, &errb); !ok || code != 2 || out.Len() != 0 ||
		errb.String() != "hpcburst-controller version: takes no arguments (got \"--state-dir\")\n" {
		t.Errorf("version with a flag: ok %v, exit %d, stdout %q, stderr %q; want the usage error", ok, code, out.String(), errb.String())
	}
}

// The controller's own command lines and the relay keeper's are not the version command's: they
// are left to main, with nothing printed.
func TestTheControllersOwnCommandLinesAreNotTheVersionCommand(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--state-dir", "/state", "--port-base", "30010", "--health-addr", "127.0.0.1:30016"},
		{"--version"},
		{"relaykeeper", "--state-dir", "/state", "--port-base", "30010"},
		{"relaykeeper", "version"},
		{"--state-dir", "version"},
	} {
		var out, errb bytes.Buffer
		if code, ok := versionCommand(args, &out, &errb); ok || code != 0 || out.Len() != 0 || errb.Len() != 0 {
			t.Errorf("%v: ok %v, exit %d, output %q %q; want it left to main", args, ok, code, out.String(), errb.String())
		}
	}
}
