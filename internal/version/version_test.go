package version

// The version line from a build's information, with the expected lines written out as literals
// (HBEP-15, Releases; issue #401): none is built by the formatter under test, so a change to the
// format shows here, in the one place the documented format is pinned.

import (
	"bytes"
	"runtime/debug"
	"testing"
)

const (
	rev  = "05cbafcb8ea4755bf2eca64f958d38ba348ff16b"
	when = "2026-11-11T10:00:00Z"
)

func vcs(modified string) []debug.BuildSetting {
	s := []debug.BuildSetting{{Key: "-buildmode", Value: "exe"}, {Key: "vcs", Value: "git"}, {Key: "vcs.revision", Value: rev}, {Key: "vcs.time", Value: when}}
	if modified != "" {
		s = append(s, debug.BuildSetting{Key: "vcs.modified", Value: modified})
	}
	return append(s, debug.BuildSetting{Key: "GOOS", Value: "linux"})
}

func build(version string, settings []debug.BuildSetting) *debug.BuildInfo {
	return &debug.BuildInfo{GoVersion: "go1.26.0", Main: debug.Module{Path: "github.com/groundnuty/plgrid-llmlab-hpc-burst", Version: version}, Settings: settings}
}

func TestTheVersionLineOfEachBuild(t *testing.T) {
	for _, tc := range []struct {
		name string
		bi   *debug.BuildInfo
		ok   bool
		want string
	}{
		{"a tagged, clean build", build("v0.1.0-alpha.1", vcs("false")), true,
			"hpcburst v0.1.0-alpha.1 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z"},
		{"a tagged build with no vcs.modified setting", build("v0.1.0-alpha.1", vcs("")), true,
			"hpcburst v0.1.0-alpha.1 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z"},
		{"a build between tags (a pseudo-version)", build("v0.0.0-20261111100000-05cbafcb8ea4", vcs("false")), true,
			"hpcburst v0.0.0-20261111100000-05cbafcb8ea4 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z"},
		{"a build after a pre-release tag (Go's .0. pseudo-version)", build("v0.1.0-alpha.1.0.20261112090000-1a2b3c4d5e6f", vcs("false")), true,
			"hpcburst v0.1.0-alpha.1.0.20261112090000-1a2b3c4d5e6f 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z"},
		{"a build after a release tag (Go's -0. pseudo-version), dirty", build("v0.1.1-0.20261112090000-1a2b3c4d5e6f+dirty", vcs("true")), true,
			"hpcburst v0.1.1-0.20261112090000-1a2b3c4d5e6f 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z dirty"},
		{"a devel build with its commit", build("(devel)", vcs("false")), true,
			"hpcburst (devel) 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z"},
		{"a build whose module has no version (a test binary's shape)", build("", vcs("false")), true,
			"hpcburst (devel) 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z"},
		{"a dirty devel build", build("(devel)", vcs("true")), true,
			"hpcburst (devel) 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z dirty"},
		{"a dirty tagged build, as the Go tool stamps it (+dirty on the version and vcs.modified)", build("v0.1.0-alpha.1+dirty", vcs("true")), true,
			"hpcburst v0.1.0-alpha.1 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z dirty"},
		{"a dirty version suffix with no vcs.modified setting", build("v0.1.0-alpha.1+dirty", vcs("")), true,
			"hpcburst v0.1.0-alpha.1 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z dirty"},
		{"a build with no VCS information (-buildvcs=false)", build("(devel)", []debug.BuildSetting{{Key: "-buildmode", Value: "exe"}, {Key: "GOOS", Value: "linux"}}), true,
			"hpcburst (devel) unknown unknown"},
		{"a tagged module with no VCS information", build("v0.1.0-alpha.1", nil), true,
			"hpcburst v0.1.0-alpha.1 unknown unknown"},
		{"VCS settings with empty values", build("(devel)", []debug.BuildSetting{{Key: "vcs.revision", Value: ""}, {Key: "vcs.time", Value: ""}, {Key: "vcs.modified", Value: ""}}), true,
			"hpcburst (devel) unknown unknown"},
		{"no build information", nil, false,
			"hpcburst (unknown) unknown unknown"},
		{"information with ok false (never given by the runtime, but ok is the contract)", build("v0.1.0-alpha.1", vcs("false")), false,
			"hpcburst (unknown) unknown unknown"},
		{"an ok with no information", nil, true,
			"hpcburst (unknown) unknown unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := FromBuildInfo(tc.bi, tc.ok).Line("hpcburst"); got != tc.want {
				t.Errorf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}

// The binary's name is the line's first field, for each of the three binaries.
func TestTheLineNamesTheBinary(t *testing.T) {
	for binary, want := range map[string]string{
		"hpcburst":            "hpcburst v0.1.0-alpha.1 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z",
		"hpcburst-controller": "hpcburst-controller v0.1.0-alpha.1 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z",
		"hpcburst-front":      "hpcburst-front v0.1.0-alpha.1 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z",
	} {
		if got := FromBuildInfo(build("v0.1.0-alpha.1", vcs("false")), true).Line(binary); got != want {
			t.Errorf("%s: got %q, want %q", binary, got, want)
		}
	}
}

// Current reads the package's seam, which a test replaces: the command reports the build it is
// given, and the real one by default.
func TestCurrentReadsTheBuildInformationSeam(t *testing.T) {
	saved := ReadBuildInfo
	t.Cleanup(func() { ReadBuildInfo = saved })
	ReadBuildInfo = func() (*debug.BuildInfo, bool) { return build("v9.8.7", vcs("false")), true }
	if got, want := Current().Line("hpcburst"), "hpcburst v9.8.7 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	ReadBuildInfo = saved
	if bi, ok := saved(); !ok || bi == nil {
		t.Fatal("the running test binary has no build information")
	}
	// The real information is read without panicking, and yields a line of four fields at least.
	if got := Current().Line("hpcburst"); len(got) < len("hpcburst (devel) unknown unknown") {
		t.Errorf("the real build's line is %q", got)
	}
}

// The command prints the line and a newline on stdout, nothing on stderr, and returns 0; any
// argument is a usage error, 2, with its own message and nothing on stdout.
func TestTheVersionCommand(t *testing.T) {
	saved := ReadBuildInfo
	t.Cleanup(func() { ReadBuildInfo = saved })
	ReadBuildInfo = func() (*debug.BuildInfo, bool) { return build("v0.1.0-alpha.1", vcs("true")), true }

	var out, errb bytes.Buffer
	if code := Command("hpcburst-front", nil, &out, &errb); code != 0 {
		t.Errorf("exit %d, want 0", code)
	}
	if got, want := out.String(), "hpcburst-front v0.1.0-alpha.1 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z dirty\n"; got != want {
		t.Errorf("stdout %q, want %q", got, want)
	}
	if errb.Len() != 0 {
		t.Errorf("stderr %q, want none", errb.String())
	}

	for _, h := range []string{"-h", "-help", "--help"} {
		out.Reset()
		errb.Reset()
		if code := Command("hpcburst-front", []string{h}, &out, &errb); code != 0 || errb.Len() != 0 ||
			out.String() != "usage: hpcburst-front version  (prints one line: hpcburst-front <tag> <commit> <time>[ dirty])\n" {
			t.Errorf("%s: exit %d, stdout %q, stderr %q; want the usage on stdout and exit 0", h, code, out.String(), errb.String())
		}
	}
	for _, args := range [][]string{{"--version"}, {"--json"}, {"extra", "more"}, {"-h", "extra"}} {
		out.Reset()
		errb.Reset()
		if code := Command("hpcburst", args, &out, &errb); code != 2 {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
		if out.Len() != 0 {
			t.Errorf("%v: stdout %q, want none", args, out.String())
		}
		if got, want := errb.String(), "hpcburst version: takes no arguments (got \""+args[0]+"\")\n"; got != want {
			t.Errorf("%v: stderr %q, want %q", args, got, want)
		}
	}
}

// Subcommand claims "version" and nothing else: not --version (hpcburst's own flag means a k3s
// release), not a prefix or another case, and an empty command line is the binary's own.
func TestSubcommandClaimsOnlyTheWordVersion(t *testing.T) {
	saved := ReadBuildInfo
	t.Cleanup(func() { ReadBuildInfo = saved })
	ReadBuildInfo = func() (*debug.BuildInfo, bool) { return build("v0.1.0-alpha.1", vcs("false")), true }

	var out, errb bytes.Buffer
	code, ok := Subcommand("hpcburst-controller", []string{"version"}, &out, &errb)
	if !ok || code != 0 || out.String() != "hpcburst-controller v0.1.0-alpha.1 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z\n" {
		t.Errorf("version: ok %v, exit %d, stdout %q", ok, code, out.String())
	}
	out.Reset()
	if code, ok := Subcommand("hpcburst-controller", []string{"version", "x"}, &out, &errb); !ok || code != 2 {
		t.Errorf("version x: ok %v, exit %d; want the subcommand's usage error, 2", ok, code)
	}
	for _, args := range [][]string{nil, {}, {"--version"}, {"-version"}, {"versions"}, {"Version"}, {"--state-dir", "version"}, {"relaykeeper", "version"}} {
		out.Reset()
		errb.Reset()
		if code, ok := Subcommand("hpcburst-controller", args, &out, &errb); ok || code != 0 || out.Len() != 0 || errb.Len() != 0 {
			t.Errorf("%v: ok %v, exit %d, output %q %q; want it left to the binary, with nothing printed", args, ok, code, out.String(), errb.String())
		}
	}
}
