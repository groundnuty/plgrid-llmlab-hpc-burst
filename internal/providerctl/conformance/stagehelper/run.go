package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

// resultName is the file the helper leaves in the Allocation's directory.
const resultName = "stage-result"

// run is the helper: args are the arguments after the program name, getenv is
// os.Getenv in production. It returns 0 once the result line is written,
// whatever that line says, 2 on a usage error, and 1 when the line cannot be
// written, the only case the job's exit code tells apart.
func run(args []string, getenv func(string) string, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return runInputs(ctx, compiled(), args, getenv, stderr)
}

// runInputs is run over the inputs and the context it is given.
func runInputs(ctx context.Context, in inputs, args []string, getenv func(string) string, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: stagehelper <allocation directory>")
		return 2
	}
	dir := args[0]
	res := stageAll(ctx, dir, in, getenv, stderr)
	// A step that failed once the signal had cancelled the staging failed because of it, whatever error the step then
	// returned (a killed Apptainer's exit status, a cut download): the signalled line, never the step's error line. It is
	// judged here, once, and not from each step's error, which need not wrap the context's.
	if res.step != "" && ctx.Err() != nil {
		res.signalled = true
	}
	if err := writeResult(dir, res.line()); err != nil {
		fmt.Fprintln(stderr, "stagehelper: cannot write the result:", err)
		return 1
	}
	return 0
}

// writeResult leaves line, and a newline, as the Allocation directory's result
// file: written under a temporary name beside it and renamed, so a reader never
// sees half of it (testdata/reach-probe.sh leaves its line the same way).
func writeResult(dir, line string) error {
	final := filepath.Join(dir, resultName)
	part := final + ".part"
	if err := os.WriteFile(part, []byte(line+"\n"), 0o640); err != nil {
		return err
	}
	if err := os.Rename(part, final); err != nil {
		os.Remove(part)
		return err
	}
	return nil
}
