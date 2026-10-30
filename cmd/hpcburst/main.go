// Command hpcburst starts, checks, upgrades and stops the single-user control-plane host of
// HBEP-11: a pinned k3s running as the invoking user, in its own state directory and on its
// own loopback ports.
//
//	hpcburst up      [flags]   start the host and wait until it is ready
//	hpcburst status  [flags]   report its health from the processes the launcher started
//	hpcburst upgrade --to V    move it one minor version up and verify every object is kept
//	hpcburst down    [flags]   stop the recorded processes and confirm nothing listens
//
// Exit codes: 0 success, 1 failure (status: unhealthy), 2 usage error, 3 (status only) stopped.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

const (
	exitOK        = 0
	exitFail      = 1
	exitUsage     = 2
	exitStopped   = 3
	envBinDir     = "HPCBURST_BIN_DIR"
	usageSummary  = "usage: hpcburst <up|status|upgrade|down> [flags]  (hpcburst <command> -h lists a command's flags)"
	stateDirUsage = "state directory (default $XDG_STATE_HOME/hpcburst, or ~/.local/state/hpcburst)"
	binDirUsage   = "directory with the pinned k3s binaries, as hack/fetch-k3s.sh lays them out (default $" + envBinDir + ", or $XDG_CACHE_HOME/hpcburst/k3s, or ~/.cache/hpcburst/k3s)"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// run executes one command line and returns the exit code.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usageSummary)
		return exitUsage
	}
	switch args[0] {
	case "up":
		return cmdUp(ctx, args[1:], stdout, stderr)
	case "status":
		return cmdStatus(ctx, args[1:], stdout, stderr)
	case "upgrade":
		return cmdUpgrade(ctx, args[1:], stdout, stderr)
	case "down":
		return cmdDown(ctx, args[1:], stdout, stderr)
	case "-h", "-help", "--help", "help":
		fmt.Fprintln(stdout, usageSummary)
		return exitOK
	}
	fmt.Fprintf(stderr, "hpcburst: unknown command %q\n%s\n", args[0], usageSummary)
	return exitUsage
}

// flags makes a flag set that reports errors on stderr and does not exit.
func flags(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("hpcburst "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func parse(fs *flag.FlagSet, args []string) (code int, done bool) {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK, true
		}
		return exitUsage, true
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(fs.Output(), "%s: unexpected argument %q\n", fs.Name(), fs.Arg(0))
		return exitUsage, true
	}
	return 0, false
}

func stateDir(given string) (string, error) {
	if given != "" {
		return given, nil
	}
	return launcher.DefaultStateDir()
}

func binDir(given string) (string, error) {
	if given != "" {
		return given, nil
	}
	if e := os.Getenv(envBinDir); e != "" {
		return e, nil
	}
	return launcher.DefaultBinDir()
}

func emitJSON(w io.Writer, v any) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func fail(stderr io.Writer, err error) int {
	fmt.Fprintf(stderr, "hpcburst: %v\n", err)
	return exitFail
}

func cmdUp(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flags("up", stderr)
	var o launcher.UpOptions
	var sd, bd string
	var asJSON bool
	fs.StringVar(&sd, "state-dir", "", stateDirUsage)
	fs.StringVar(&bd, "bin-dir", "", binDirUsage)
	fs.StringVar(&o.Version, "version", launcher.DefaultVersion, "k3s release to start (must be pinned in the launcher)")
	fs.IntVar(&o.PortBase, "port-base", launcher.DefaultPortBase, "first of a block of six loopback ports: base (apiserver), +1 (https), +2 (controller manager), +5 (reserved)")
	fs.StringVar(&o.Front.Mode, "front-mode", "", "how compute nodes reach the front: direct, or ssh (port 22, restricted key)")
	fs.StringVar(&o.Front.Addr, "front-addr", "", "the front's address, host:port (ssh: host or host:22)")
	fs.StringVar(&o.Front.KeyPath, "front-key", "", "ssh mode: path of the restricted key (recorded, never read)")
	fs.IntVar(&o.Nice, "nice", 0, "run k3s under nice(1) with this value (0: leave the priority alone)")
	fs.StringVar(&o.ManifestsDir, "manifests", "", "directory of YAML manifests to apply once the host is ready, CRDs first")
	fs.DurationVar(&o.ReadyTimeout, "ready-timeout", launcher.DefaultReadyTimeout, "how long to wait for readiness")
	fs.BoolVar(&asJSON, "json", false, "print the result as JSON")
	if code, done := parse(fs, args); done {
		return code
	}
	var err error
	if o.StateDir, err = stateDir(sd); err != nil {
		return fail(stderr, err)
	}
	if o.BinDir, err = binDir(bd); err != nil {
		return fail(stderr, err)
	}
	o.Log = stderr
	rep, err := launcher.Up(ctx, o)
	if err != nil {
		return fail(stderr, err)
	}
	if asJSON {
		emitJSON(stdout, rep)
		return exitOK
	}
	fmt.Fprintf(stdout, "k3s %s is ready (pid %d): readyz after %d ms, controller manager after %d ms, RSS %.0f MiB\n",
		rep.Version, rep.PID, rep.ReadyzMS, rep.ReadyMS, rep.RSSMiB)
	fmt.Fprintf(stdout, "kubeconfig: %s\n", rep.Kubeconfig)
	fmt.Fprintf(stdout, "ports: apiserver %d, https %d, controller manager %d (loopback)\n", rep.Ports.API, rep.Ports.HTTPS, rep.Ports.KCM)
	if rep.Front.Set() {
		fmt.Fprintf(stdout, "front: %s %s\n", rep.Front.Mode, rep.Front.Addr)
	}
	if o.ManifestsDir != "" {
		fmt.Fprintf(stdout, "applied %d manifests from %s\n", rep.Applied, o.ManifestsDir)
	}
	return exitOK
}

func cmdStatus(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flags("status", stderr)
	var o launcher.StatusOptions
	var sd string
	var asJSON bool
	fs.StringVar(&sd, "state-dir", "", stateDirUsage)
	fs.BoolVar(&o.NoAPI, "no-api", false, "check only the processes and the sockets they hold; do not ask the API (no /readyz, no write probe)")
	fs.BoolVar(&asJSON, "json", false, "print the report as JSON")
	if code, done := parse(fs, args); done {
		return code
	}
	var err error
	if o.StateDir, err = stateDir(sd); err != nil {
		return fail(stderr, err)
	}
	rep, err := launcher.Status(ctx, o)
	if err != nil {
		return fail(stderr, err)
	}
	if asJSON {
		emitJSON(stdout, rep)
	} else {
		printStatus(stdout, rep)
	}
	switch rep.Health {
	case launcher.Healthy:
		return exitOK
	case launcher.Stopped:
		return exitStopped
	}
	return exitFail
}

func printStatus(w io.Writer, r *launcher.StatusReport) {
	fmt.Fprintf(w, "%s\n", r.Health)
	for _, s := range r.Reasons {
		fmt.Fprintf(w, "  - %s\n", s)
	}
	if r.Version != "" {
		fmt.Fprintf(w, "k3s %s in %s\n", r.Version, r.StateDir)
	}
	for _, p := range r.Processes {
		fmt.Fprintf(w, "process %s pid %d: %s\n", p.Name, p.PID, p.Verdict)
	}
	if len(r.Listeners) > 0 {
		fmt.Fprintf(w, "listeners held by the process: %v\n", r.Listeners)
	}
	if r.RSSMiB > 0 {
		fmt.Fprintf(w, "RSS %.0f MiB\n", r.RSSMiB)
	}
	for _, c := range []struct {
		name string
		v    *bool
	}{{"apiserver /readyz", r.Readyz}, {"controller manager /healthz", r.KCM}, {"write probe", r.Writes}} {
		if c.v != nil {
			fmt.Fprintf(w, "%s: %v\n", c.name, *c.v)
		}
	}
	if r.Front != nil && r.Front.Set() {
		fmt.Fprintf(w, "front: %s %s\n", r.Front.Mode, r.Front.Addr)
	} else if r.Version != "" {
		fmt.Fprintln(w, "front: not recorded")
	}
}

func cmdDown(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flags("down", stderr)
	var o launcher.DownOptions
	var sd string
	var asJSON bool
	fs.StringVar(&sd, "state-dir", "", stateDirUsage)
	fs.DurationVar(&o.Grace, "grace", launcher.DefaultGrace, "how long to wait after SIGTERM before SIGKILL")
	fs.BoolVar(&asJSON, "json", false, "print the result as JSON")
	if code, done := parse(fs, args); done {
		return code
	}
	var err error
	if o.StateDir, err = stateDir(sd); err != nil {
		return fail(stderr, err)
	}
	o.Log = stderr
	rep, err := launcher.Down(ctx, o)
	if err != nil {
		return fail(stderr, err)
	}
	if asJSON {
		emitJSON(stdout, rep)
		return exitOK
	}
	if rep == nil {
		fmt.Fprintln(stdout, "nothing to stop")
		return exitOK
	}
	for _, p := range rep.Procs {
		line := fmt.Sprintf("%s pid %d: %s", p.Name, p.PID, p.Before)
		if p.Signals != "" {
			line += fmt.Sprintf(", signalled %s, gone after %d ms", p.Signals, p.TookMS)
		}
		if p.Detail != "" {
			line += " (" + p.Detail + ")"
		}
		fmt.Fprintln(stdout, line)
	}
	ours := 0
	for _, p := range rep.Procs {
		if p.Before == "ours" {
			ours++
		}
	}
	if ours == 0 {
		fmt.Fprintln(stdout, "nothing of ours was running; the record is cleared")
	} else {
		fmt.Fprintf(stdout, "stopped; %d listening sockets released\n", rep.GoneListeners)
	}
	return exitOK
}

func cmdUpgrade(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flags("upgrade", stderr)
	var o launcher.UpgradeOptions
	var sd, bd string
	var asJSON bool
	fs.StringVar(&sd, "state-dir", "", stateDirUsage)
	fs.StringVar(&bd, "bin-dir", "", binDirUsage)
	fs.StringVar(&o.To, "to", "", "k3s release to upgrade to: exactly one minor above the running one, and pinned (required)")
	fs.DurationVar(&o.Grace, "grace", launcher.DefaultGrace, "how long to wait after SIGTERM before SIGKILL")
	fs.DurationVar(&o.ReadyTimeout, "ready-timeout", launcher.DefaultReadyTimeout, "how long to wait for readiness of the new host")
	fs.BoolVar(&asJSON, "json", false, "print the result as JSON")
	if code, done := parse(fs, args); done {
		return code
	}
	if o.To == "" {
		fmt.Fprintln(stderr, "hpcburst upgrade: --to is required")
		return exitUsage
	}
	var err error
	if o.StateDir, err = stateDir(sd); err != nil {
		return fail(stderr, err)
	}
	if o.BinDir, err = binDir(bd); err != nil {
		return fail(stderr, err)
	}
	o.Log = stderr
	rep, err := launcher.Upgrade(ctx, o)
	if rep != nil {
		if asJSON {
			emitJSON(stdout, rep)
		} else {
			v := rep.Verification
			fmt.Fprintf(stdout, "upgraded %s to %s: kept %d of %d objects (%d added), stop to ready %d ms, RSS %.0f MiB\nbackup: %s\n",
				rep.From, rep.To, v.Kept, v.Before, v.Added, rep.StopToReady, rep.RSSMiB, rep.Backup)
		}
	}
	if err != nil {
		return fail(stderr, err)
	}
	return exitOK
}
