package main

import (
	"io"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/version"
)

// versionCommand runs the version subcommand if args (os.Args[1:]) name it: it prints
// "hpcburst-front <tag> <commit> <time>[ dirty]" from the binary's build information
// (internal/version), and ok is false when args are the front's own.
func versionCommand(args []string, stdout, stderr io.Writer) (code int, ok bool) {
	return version.Subcommand("hpcburst-front", args, stdout, stderr)
}
