// Package version reports a binary's release identity from Go's build information, for the
// version subcommand of the three host binaries (hpcburst, hpcburst-controller and
// hpcburst-front; HBEP-15, Releases; issue #401). Nothing is injected at link time: the tag, the
// commit, its time and whether the tree was modified are what the Go tool stamped
// (runtime/debug.ReadBuildInfo), with VCS stamping on, which is go build's default inside a
// repository.
//
// # The output
//
// "<binary> version" prints one line, newline-terminated, with its fields separated by one space,
// and the format is stable: operators and the release tooling around them read it.
//
//	<binary> <tag> <commit> <time>[ dirty]
//
// The fields:
//
//   - binary: the program's name: hpcburst, hpcburst-controller or hpcburst-front.
//   - tag: the main module's version as the Go tool stamped it. With VCS stamping (go build
//     inside a repository) that is the release tag when the binary is built from the tagged
//     commit (v0.1.0-alpha.1), and a pseudo-version, which carries the commit's time and its
//     first 12 hex digits, on any other commit: v0.0.0-20261111100000-05cbafcb8ea4 before the
//     first tag, v0.1.0-alpha.1.0.20261112090000-1a2b3c4d5e6f after a pre-release tag,
//     v0.1.1-0.20261112090000-1a2b3c4d5e6f after a release tag. "(devel)" is what the tool
//     stamps when it has no version control to read (-buildvcs=false, or no repository), and the
//     line then says "unknown" for the commit and the time; "(unknown)" means the binary carries
//     no build information at all. A "+dirty" suffix the tool adds to the version of a modified
//     tree is removed: dirtiness is its own field.
//   - commit: vcs.revision, the full 40-hex commit, or "unknown" when the binary was built outside
//     a repository or with -buildvcs=false.
//   - time: vcs.time, the commit's time in UTC as RFC 3339 (2026-11-11T10:00:00Z), or "unknown".
//   - "dirty": present, as a last field, when vcs.modified is true (or the tool put "+dirty" on
//     the version): the tree had uncommitted changes when the binary was built. Absent otherwise.
//
// Examples:
//
//	hpcburst v0.1.0-alpha.1 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z
//	hpcburst-controller v0.0.0-20261111100000-05cbafcb8ea4 05cbafcb8ea4755bf2eca64f958d38ba348ff16b 2026-11-11T10:00:00Z dirty
//	hpcburst-front (devel) unknown unknown
//
// The release workflow itself checks "go version -m" on the binaries (HBEP-15), not this line.
//
// Provenance: new package; the standard library's runtime/debug only.
package version

import (
	"fmt"
	"io"
	"runtime/debug"
	"strings"
)

// ReadBuildInfo is the build information the package reads: runtime/debug's. Only a test replaces
// it, to give a binary's version command a build it did not have.
var ReadBuildInfo = debug.ReadBuildInfo

// The words of the line that stand for what the build information lacks.
const (
	// Unknown is the commit or the time when the binary has no VCS information.
	Unknown = "unknown"
	// Devel is the tag when the Go tool stamped no version on the main module.
	Devel = "(devel)"
	// NoBuildInfo is the tag when the binary has no build information at all.
	NoBuildInfo = "(unknown)"
	// DirtyWord is the last field of the line of a binary built from a modified tree.
	DirtyWord = "dirty"
)

// dirtySuffix is what the Go tool adds to the version of a binary built from a modified tree.
const dirtySuffix = "+dirty"

// Info is what the build information says of a binary's release.
type Info struct {
	// Tag is the main module's version, Devel or NoBuildInfo; without a "+dirty" suffix.
	Tag string
	// Commit is vcs.revision, or Unknown.
	Commit string
	// Time is vcs.time, or Unknown.
	Time string
	// Dirty is vcs.modified.
	Dirty bool
}

// FromBuildInfo reads Info from a build as runtime/debug.ReadBuildInfo gave it: ok false is a
// binary with no build information.
func FromBuildInfo(bi *debug.BuildInfo, ok bool) Info {
	if !ok || bi == nil {
		return Info{Tag: NoBuildInfo, Commit: Unknown, Time: Unknown}
	}
	i := Info{Tag: bi.Main.Version, Commit: Unknown, Time: Unknown}
	if strings.HasSuffix(i.Tag, dirtySuffix) {
		i.Tag, i.Dirty = strings.TrimSuffix(i.Tag, dirtySuffix), true
	}
	if i.Tag == "" {
		i.Tag = Devel
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			if s.Value != "" {
				i.Commit = s.Value
			}
		case "vcs.time":
			if s.Value != "" {
				i.Time = s.Value
			}
		case "vcs.modified":
			if s.Value == "true" {
				i.Dirty = true
			}
		}
	}
	return i
}

// Current is the running binary's Info.
func Current() Info {
	bi, ok := ReadBuildInfo()
	return FromBuildInfo(bi, ok)
}

// Line is the version command's output for the named binary, without its newline (see the
// package comment for the format).
func (i Info) Line(binary string) string {
	line := binary + " " + i.Tag + " " + i.Commit + " " + i.Time
	if i.Dirty {
		line += " " + DirtyWord
	}
	return line
}

// Command is the version subcommand of a binary: it prints the line of the running binary and
// returns 0. args are the arguments after "version"; the command takes none, except -h, -help and
// --help, which print its usage and return 0. Any other argument is a usage error, 2, so that a
// flag meant for something else (hpcburst's --version is the k3s release) is never taken for the
// version command's.
func Command(binary string, args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "-help" || args[0] == "--help") {
		fmt.Fprintf(stdout, "usage: %s version  (prints one line: %s <tag> <commit> <time>[ dirty])\n", binary, binary)
		return 0
	}
	if len(args) > 0 {
		fmt.Fprintf(stderr, "%s version: takes no arguments (got %q)\n", binary, args[0])
		return 2
	}
	fmt.Fprintln(stdout, Current().Line(binary))
	return 0
}

// Subcommand runs the version subcommand if args (os.Args[1:], which begin with the subcommand's
// name) name it, and returns its exit code; ok is false when they are the binary's own.
func Subcommand(binary string, args []string, stdout, stderr io.Writer) (code int, ok bool) {
	if len(args) > 0 && args[0] == "version" {
		return Command(binary, args[1:], stdout, stderr), true
	}
	return 0, false
}
