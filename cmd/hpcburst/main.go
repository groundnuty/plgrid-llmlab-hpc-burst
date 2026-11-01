// Command hpcburst starts, checks, upgrades and stops the single-user control-plane host of
// HBEP-11: a pinned k3s running as the invoking user, in its own state directory and on its
// own loopback ports.
//
//	hpcburst up      [flags]   start the host and wait until it is ready
//	hpcburst status  [flags]   report its health from the processes the launcher started
//	hpcburst upgrade --to V    move it one step up (next minor, or a newer patch) and verify every object is kept
//	hpcburst restore --backup DIR  put a backup's store back (the host stopped): the only way down
//	hpcburst down    [flags]   stop the recorded processes and confirm nothing listens
//
// up also starts the controllers' supervisor, a hidden "hpcburst supervise" process that keeps
// hpcburst-controller running and restarts it when it exits.
//
// Exit codes: 0 success, 1 failure (status: unhealthy), 2 usage error, 3 (status only) stopped,
// 4 (up only) the host is up and left running, but an agent build or frpc build that a
// PlatformProfile partition needs is missing or does not match its pin (or the check could not
// read the profiles): fix the builds (hack/build-agents.sh, hack/fetch-frp.sh --agent-builds) and
// run status.
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
	exitBuilds    = 4
	envBinDir     = "HPCBURST_BIN_DIR"
	usageSummary  = "usage: hpcburst <up|status|upgrade|restore|down> [flags]  (hpcburst <command> -h lists a command's flags)"
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
	case "restore":
		return cmdRestore(args[1:], stdout, stderr)
	case "supervise": // hidden: started by up
		return cmdSupervise(ctx, args[1:], stdout, stderr)
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
	if errors.Is(err, launcher.ErrGraceTooShort) {
		return exitUsage // a flag the host cannot honour: nothing was stopped
	}
	return exitFail
}

func cmdUp(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flags("up", stderr)
	var o launcher.UpOptions
	var sd, bd string
	var asJSON bool
	fs.StringVar(&sd, "state-dir", "", stateDirUsage)
	fs.StringVar(&bd, "bin-dir", "", binDirUsage)
	fs.StringVar(&o.Version, "version", "", "k3s release to start (must be pinned): default the release that last wrote the store, or "+launcher.DefaultVersion+" for a new one; another release than the store's is refused (use upgrade or restore)")
	fs.IntVar(&o.PortBase, "port-base", launcher.DefaultPortBase, "first of a block of 13 loopback ports, base to base+12, shared with the controller, the front and frps, all of which must be free before a start: base (apiserver), +1 (https), +2 (controller manager), +3 (the agent endpoint, the controller's server), +4 (the front's /healthz), +5 (reserved, never bound), +6 (the lifecycle controller's /healthz), +7 (the front's ssh-mode dial-home listener), +8 (the frps server plugin, the controller's), +9 to +11 (frps's bind, tcpmux and web ports), +12 (the lifecycle controller's /metrics); the restricted VM keys forward to +3 and +7, so another base needs new keys")
	fs.StringVar(&o.Front.Mode, "front-mode", "", "how compute nodes reach the front: direct, or ssh (restricted key)")
	fs.StringVar(&o.Front.Addr, "front-addr", "", "the front's address, host:port (ssh: host or host:port; the port defaults to 22)")
	fs.StringVar(&o.Front.KeyPath, "front-key", "", "ssh mode: path of the restricted key (recorded, never read)")
	fs.IntVar(&o.Nice, "nice", 0, "run k3s under nice(1) with this value (0: leave the priority alone)")
	fs.StringVar(&o.ManifestsDir, "manifests", "", "directory of YAML manifests to apply once the host is ready, CRDs first")
	fs.DurationVar(&o.ReadyTimeout, "ready-timeout", launcher.DefaultReadyTimeout, "how long to wait for readiness")
	fs.StringVar(&o.ControllerBin, "controller-bin", "", "the controllers' binary (default "+launcher.ControllerBinName+" in the directory of this executable; when that is absent, up starts the host with a warning and no controller runs)")
	fs.StringVar(&o.FrontConfig, "front-config", "", "the front's configuration file (HBEP-5); when given, the supervisor runs the front beside the controllers")
	fs.StringVar(&o.FrontBin, "front-bin", "", "the front's binary (default "+launcher.FrontBinName+" in the directory of this executable)")
	fs.StringVar(&o.FrpsBin, "frps-bin", "", "frps, the dial-home tunnel's server (HBEP-6), checked against the pinned frp "+launcher.DefaultFrpVersion+" (default frp-"+launcher.DefaultFrpVersion+"/frps in --bin-dir; when that is absent, no frps runs and up warns); it runs under the controllers' supervisor")
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
	// An AgentBuildsError comes with the report: the host is up, and the builds are the operator's
	// to fix. The report is printed as usual, and the ERROR and the exit code come last.
	var buildsErr *launcher.AgentBuildsError
	if err != nil && !(errors.As(err, &buildsErr) && rep != nil) {
		return fail(stderr, err)
	}
	done := func() int {
		if buildsErr == nil {
			return exitOK
		}
		printBuildsError(stderr, buildsErr, o.StateDir)
		return exitBuilds
	}
	if asJSON {
		emitJSON(stdout, rep)
		return done()
	}
	fmt.Fprintf(stdout, "k3s %s is ready (pid %d): readyz after %d ms, controller manager after %d ms, RSS %.0f MiB\n",
		rep.Version, rep.PID, rep.ReadyzMS, rep.ReadyMS, rep.RSSMiB)
	fmt.Fprintf(stdout, "kubeconfig: %s\n", rep.Kubeconfig)
	fmt.Fprintf(stdout, "ports: apiserver %d, https %d, controller manager %d (loopback)\n", rep.Ports.API, rep.Ports.HTTPS, rep.Ports.KCM)
	if rep.Front.Set() {
		fmt.Fprintf(stdout, "front: %s %s\n", rep.Front.Mode, rep.Front.Addr)
	}
	if rep.SupervisorPID != 0 {
		fmt.Fprintf(stdout, "controllers: supervisor pid %d runs %s, health on %s\n", rep.SupervisorPID, rep.ControllerBin, rep.ControllerHealth)
	}
	if rep.FrontHealth != "" {
		fmt.Fprintf(stdout, "front: supervised, health on %s\n", rep.FrontHealth)
	}
	if len(rep.FrpsPorts) > 0 {
		fmt.Fprintf(stdout, "frps: supervised, loopback ports %v\n", rep.FrpsPorts)
	}
	if o.ManifestsDir != "" {
		fmt.Fprintf(stdout, "applied %d manifests from %s\n", rep.Applied, o.ManifestsDir)
	}
	printAgentBuilds(stdout, rep.AgentBuilds)
	return done()
}

func cmdStatus(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flags("status", stderr)
	var o launcher.StatusOptions
	var sd string
	var asJSON bool
	fs.StringVar(&sd, "state-dir", "", stateDirUsage)
	fs.BoolVar(&o.NoAPI, "no-api", false, "check only the processes and the sockets they hold; do not ask the API (no /readyz, no write or garbage-collector probe)")
	fs.BoolVar(&o.NoGCProbe, "no-gc-probe", false, "skip the garbage-collector probe (an owner and a dependent ConfigMap; the dependent must go within a minute)")
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
	if r.Executable != "" {
		fmt.Fprintf(w, "executable: %s\n", r.Executable)
	}
	switch {
	case r.EventTTL != "" && r.EventTTLDefault:
		fmt.Fprintf(w, "event TTL: %s (default; restart to apply %s)\n", r.EventTTL, launcher.DefaultEventTTL)
	case r.EventTTL != "":
		fmt.Fprintf(w, "event TTL: %s\n", r.EventTTL)
	}
	if r.RSSMiB > 0 {
		fmt.Fprintf(w, "RSS %.0f MiB\n", r.RSSMiB)
	}
	for _, c := range []struct {
		name string
		v    *bool
	}{{"apiserver /readyz", r.Readyz}, {"controller manager /healthz", r.KCM}, {"write probe", r.Writes}, {"garbage-collector probe", r.GC}} {
		if c.v != nil {
			fmt.Fprintf(w, "%s: %v\n", c.name, *c.v)
		}
	}
	printSupervision(w, r)
	printAgentBuilds(w, r.AgentBuilds)
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
	fs.DurationVar(&o.Grace, "grace", launcher.DefaultGrace, "how long to wait after SIGTERM before SIGKILL; when the supervisor runs the front, at least "+launcher.SupervisorGraceWithFront.String()+" (the front needs up to "+launcher.FrontTermGrace.String()+" to flush its usage records), and a shorter grace is refused")
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
	fs.StringVar(&o.To, "to", "", "k3s release to upgrade to (required, pinned): the next minor, or a newer patch of the running minor")
	fs.DurationVar(&o.Grace, "grace", launcher.DefaultGrace, "how long to wait after SIGTERM before SIGKILL; when the supervisor runs the front, at least "+launcher.SupervisorGraceWithFront.String()+" (the front needs up to "+launcher.FrontTermGrace.String()+" to flush its usage records), and a shorter grace is refused")
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

func cmdRestore(args []string, stdout, stderr io.Writer) int {
	fs := flags("restore", stderr)
	var o launcher.RestoreOptions
	var sd string
	var asJSON bool
	fs.StringVar(&sd, "state-dir", "", stateDirUsage)
	fs.StringVar(&o.Backup, "backup", "", "a backup directory under <state-dir>/backups, as upgrade made it (required)")
	fs.BoolVar(&asJSON, "json", false, "print the result as JSON")
	if code, done := parse(fs, args); done {
		return code
	}
	if o.Backup == "" {
		fmt.Fprintln(stderr, "hpcburst restore: --backup is required")
		return exitUsage
	}
	var err error
	if o.StateDir, err = stateDir(sd); err != nil {
		return fail(stderr, err)
	}
	o.Log = stderr
	rep, err := launcher.Restore(o)
	if err != nil {
		return fail(stderr, err)
	}
	if asJSON {
		emitJSON(stdout, rep)
		return exitOK
	}
	fmt.Fprintf(stdout, "restored the store of k3s %s from %s; hpcburst up starts that release\n", rep.Version, rep.Backup)
	if rep.Replaced != "" {
		fmt.Fprintf(stdout, "the replaced store is in %s\n", rep.Replaced)
	}
	return exitOK
}
