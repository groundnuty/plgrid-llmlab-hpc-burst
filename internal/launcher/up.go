package launcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// DefaultReadyTimeout bounds the wait for readiness. A cold start of k3s unpacks its embedded
// binaries first (about 10 s on a loaded VM in S3).
const DefaultReadyTimeout = 120 * time.Second

// UpOptions are the inputs of Up.
type UpOptions struct {
	StateDir     string
	BinDir       string
	Version      string // default: the release that last wrote the store, or DefaultVersion for a new one
	PortBase     int    // default DefaultPortBase
	Front        Front
	Nice         int    // nice(1) value for k3s; 0 leaves it alone
	ManifestsDir string // applied after readiness, CRDs first
	ReadyTimeout time.Duration
	Log          io.Writer // progress and warnings
	// ControllerBin is the controllers' binary. Empty means ControllerBinName beside the running
	// executable; if that is absent too, the host starts with a warning and no controller runs.
	// A path that is given and missing is an error, raised before anything starts.
	ControllerBin string
	// FrontConfig is the front's configuration file; empty means no front runs. FrontBin is its
	// binary, default FrontBinName beside the running executable (issue #64). The front runs under
	// the controllers' supervisor, so it needs a controller binary too.
	FrontConfig, FrontBin string
	// FrpsBin is frps (HBEP-6), checked against the pin; empty means FrpDirName/frps under BinDir,
	// and a missing default starts no frps, with a warning. frps runs under the controllers'
	// supervisor, so a given FrpsBin needs a controller binary too.
	FrpsBin string
	// RelayKeeper is up --relay-keeper (beta): the supervisor runs the relay keeper, and the
	// controller hands it its host-dialed relays. Valid only with Front.Mode host-dialed and a
	// controller binary. Without it, up refuses while the keeper's state file shows it ran and
	// Allocations are running (relaykeeper.go).
	RelayKeeper bool
	// BulkPath is up --bulk-path (beta; HBEP-6, The bulk path): the supervisor runs a second frps
	// for the bulk path, frps-bulk, at base+13 to base+15, and up keeps a second certificate for the
	// agent endpoint's key, naming the bulk server only. It needs a controller binary and frps.
	BulkPath bool

	// supervisorArgv, set by tests only, replaces the supervisor's command line.
	supervisorArgv []string
	// limitReader, set by tests only, replaces the read of a tunnel process's open-files limits
	// (openFilesWarnings); nil reads /proc/<pid>/limits as status does.
	limitReader func(h *procHandle) (soft, hard Limit, err error)
	// eventTTL and omitEventTTL, set by tests only, are launchSpec's fields of the same names.
	eventTTL     string
	omitEventTTL bool
}

// UpReport is the outcome of a start.
type UpReport struct {
	Version    string  `json:"version"`
	StateDir   string  `json:"state_dir"`
	Kubeconfig string  `json:"kubeconfig"`
	Ports      Ports   `json:"ports"`
	PID        int     `json:"pid"`
	ReadyzMS   int64   `json:"readyz_ms"`
	ReadyMS    int64   `json:"ready_ms"`
	RSSMiB     float64 `json:"rss_mib"`
	Applied    int     `json:"manifests_applied"`
	// CRDGuard is true when up installed the CRD deletion guard (guard.go); it does so on every start.
	CRDGuard bool     `json:"crd_guard"`
	Front    Front    `json:"front"`
	Warnings []string `json:"warnings,omitempty"`
	// SupervisorPID and ControllerBin are set when controllers run; ControllerHealth is the
	// address of the lifecycle controller's /healthz.
	SupervisorPID    int    `json:"supervisor_pid,omitempty"`
	ControllerBin    string `json:"controller_bin,omitempty"`
	ControllerHealth string `json:"controller_health,omitempty"`
	// FrontHealth is the address of the front's /healthz when a front runs.
	FrontHealth string `json:"front_health,omitempty"`
	// FrpsPorts are frps's bind, tcpmux and web ports when frps runs.
	FrpsPorts []int `json:"frps_ports,omitempty"`
	// FrpsBulkPorts are the second frps's bind, tcpmux and web ports when up ran it (--bulk-path).
	FrpsBulkPorts []int `json:"frps_bulk_ports,omitempty"`
	// RelayKeeperSocket is the relay keeper's socket when up ran it (--relay-keeper).
	RelayKeeperSocket string `json:"relay_keeper_socket,omitempty"`
	// AgentBuilds is the check of the builds the PlatformProfiles' partitions need (#158), made
	// once the controllers run. When it finds a problem, Up returns this report together with an
	// *AgentBuildsError, and the host keeps running.
	AgentBuilds *AgentBuilds `json:"agent_builds,omitempty"`
}

// logTail returns the last few lines of the k3s log, for error messages.
func logTail(path string, n int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	lines := bytes.Split(bytes.TrimRight(b, "\n"), []byte("\n"))
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return "\n--- last lines of " + path + " ---\n" + string(bytes.Join(lines, []byte("\n")))
}

// newRecord is the first durable record of a start: k3s as it was launched, with what the launcher
// chose for it, the event TTL among that.
func newRecord(spec launchSpec, rec ProcRec, front Front, startedAt time.Time) *State {
	return &State{
		Version: spec.bin.Version.String(), Binary: spec.bin.Path, BinarySHA256: spec.bin.SHA256,
		Nice: spec.nice, Ports: spec.ports, Procs: []ProcRec{rec}, StartedAt: startedAt, Front: front,
		Controllers: spec.controllers, FrontRun: spec.frontRun, Frps: spec.frps, EventTTL: spec.eventTTLValue(),
	}
}

// startAndWait launches k3s from spec, records it at once, waits for readiness, and writes the
// kubeconfig (mode 0600) only after the host is ready. If anything fails it stops what it
// started, so a failed start never leaves a process behind.
func startAndWait(ctx context.Context, spec launchSpec, front Front, timeout time.Duration, log io.Writer) (*State, time.Duration, time.Duration, error) {
	t0 := time.Now()
	h, err := launch(spec)
	if err != nil {
		return nil, 0, 0, err
	}
	defer h.Close()
	rec := h.rec
	st := newRecord(spec, rec, front, t0)
	st.RelayKeeper = spec.relayKeeper
	st.BulkPath = spec.bulkPath
	// Record before waiting: an interrupted or crashed start is then still cleaned up by down.
	if err := spec.l.saveState(st); err != nil {
		_, _ = stopRecorded(context.Background(), st, 10*time.Second, log)
		return nil, 0, 0, err
	}
	logf(log, "started k3s %s as pid %d; waiting for it to be ready", st.Version, rec.PID)
	fail := func(cause error) (*State, time.Duration, time.Duration, error) {
		_, serr := stopRecorded(context.Background(), st, 10*time.Second, log)
		if serr != nil {
			return nil, 0, 0, fmt.Errorf("%w (and stopping it failed: %v; run hpcburst down)", cause, serr)
		}
		_ = spec.l.clearRecord()
		return nil, 0, 0, fmt.Errorf("%w%s", cause, logTail(spec.l.logFile(), 15))
	}
	readyzAt, readyAt, err := waitReady(ctx, spec.l, st, h, timeout)
	if err != nil {
		return fail(err)
	}
	if err := writeKubeconfig(spec.l); err != nil {
		return fail(err)
	}
	return st, readyzAt, readyAt, nil
}

// writeKubeconfig publishes k3s's admin kubeconfig as the launcher's kubeconfig, mode 0600,
// and checks the mode it wrote.
func writeKubeconfig(l layout) error {
	b, err := os.ReadFile(l.adminKubeconfig())
	if err != nil {
		return fmt.Errorf("read k3s's kubeconfig: %w", err)
	}
	if err := writeFileAtomic(l.kubeconfig(), b, 0o600); err != nil {
		return err
	}
	fi, err := os.Stat(l.kubeconfig())
	if err != nil {
		return err
	}
	if fi.Mode().Perm() != 0o600 {
		return fmt.Errorf("kubeconfig %s has mode %v, want 0600", l.kubeconfig(), fi.Mode().Perm())
	}
	return nil
}

// Up starts the pinned k3s as the invoking user and returns once it is ready. It refuses to
// start when the state directory already records a live host (or one whose identity cannot be
// checked), when the store was written by another release than the one asked for, or when one of
// its ports is taken.
func Up(ctx context.Context, o UpOptions) (*UpReport, error) {
	l, err := newLayout(o.StateDir)
	if err != nil {
		return nil, err
	}
	front, err := o.Front.Normalize()
	if err != nil {
		return nil, err
	}
	if o.PortBase == 0 {
		o.PortBase = DefaultPortBase
	}
	ports, err := PortsFromBase(o.PortBase)
	if err != nil {
		return nil, err
	}
	if o.Version != "" {
		if _, err := ParseVersion(o.Version); err != nil {
			return nil, err
		}
	}
	if o.ReadyTimeout <= 0 {
		o.ReadyTimeout = DefaultReadyTimeout
	}
	ctrlBin, err := resolveControllerBin(o.ControllerBin)
	if err != nil {
		return nil, err
	}
	frontRun, err := resolveFrontRun(o.FrontConfig, o.FrontBin)
	if err != nil {
		return nil, err
	}
	if o.RelayKeeper && front.Mode != string(agentapi.DialHomeHostDialed) {
		return nil, errRelayKeeperMode
	}
	if o.BulkPath {
		if err := checkBulkPathFront(front, frontRun); err != nil {
			return nil, err
		}
		if ports, err = ports.WithBulkPath(); err != nil {
			return nil, err
		}
	}
	if o.RelayKeeper && ctrlBin == "" {
		return nil, errors.New("the relay keeper was asked for (--relay-keeper), but no controller binary was found: the keeper is the controller binary's relaykeeper subcommand")
	}
	if o.RelayKeeper {
		if err := pidfdProbe(); err != nil {
			return nil, fmt.Errorf("the relay keeper (--relay-keeper) needs pidfds (Linux 5.3 or later), and pidfd_open failed: %w", err)
		}
	}
	if frontRun != nil && ctrlBin == "" {
		return nil, errors.New("a front was asked for (--front-config), but no controller binary was found: the front runs under the controllers' supervisor")
	}
	var frpsRun *FrpsRunConfig
	if ctrlBin != "" {
		if frpsRun, err = resolveFrpsRun(o.BinDir, o.FrpsBin, DefaultFrpVersion); err != nil {
			return nil, err
		}
	} else if o.FrpsBin != "" {
		return nil, errors.New("frps was asked for (--frps-bin), but no controller binary was found: frps runs under the controllers' supervisor")
	}
	if o.BulkPath && frpsRun == nil {
		return nil, errBulkPathNoFrps
	}
	unlock, err := l.lockState()
	if err != nil {
		return nil, err
	}
	defer unlock()

	prior, err := l.loadState()
	if err != nil {
		return nil, err
	}
	if prior != nil {
		for _, rec := range prior.Procs {
			switch v, d := identify(rec); v {
			case Ours:
				return nil, fmt.Errorf("a host is already running in %s (%s pid %d); run hpcburst down first", l.dir, rec.Name, rec.PID)
			case Unknown:
				return nil, fmt.Errorf("cannot tell whether the recorded %s still runs (%s); nothing was started, and the record in %s is kept", rec.Name, d, l.dir)
			}
		}
		logf(o.Log, "the record in %s names processes that have exited or whose PIDs other processes now use; replacing it", l.dir)
		if err := l.clearRecord(); err != nil {
			return nil, err
		}
	}
	// The same version rule holds after a stale record as after a clean stop.
	version, err := startVersion(l, o.Version)
	if err != nil {
		return nil, fmt.Errorf("refusing to start: %w", err)
	}
	// With controllers, the agent endpoint's certificate and key are made or checked before anything
	// starts, so a broken pair costs nothing; startSupervisor checks them again.
	if ctrlBin != "" {
		if err := ensureAgentEndpointPair(l.host(ports), o.Log); err != nil {
			return nil, err
		}
	}
	// The bulk path's second certificate, for the same key, before anything starts too: the first
	// is only read (HBEP-6, The bulk path).
	if o.BulkPath {
		if err := ensureBulkCert(l.host(ports), o.Log); err != nil {
			return nil, err
		}
	}
	bin, err := ResolveBinary(o.BinDir, version)
	if err != nil {
		return nil, err
	}
	defer bin.Close()
	fresh, err := l.storeExists()
	if err != nil {
		return nil, err
	}
	fresh = !fresh
	if fresh {
		// Recorded before the start: if k3s creates the store and the start then fails, the store
		// still has the record of the release that created it.
		if err := l.saveStoreVersion(bin.Version.String(), "up"); err != nil {
			return nil, err
		}
	}
	// Checked last, as close to the start as possible: another process may take a port while the
	// binary is being hashed.
	if err := checkPortsFree(ports.All()); err != nil {
		return nil, fmt.Errorf("refusing to start: %w", err)
	}

	spec := launchSpec{l: l, bin: bin, ports: ports, nice: o.Nice, eventTTL: o.eventTTL, omitEventTTL: o.omitEventTTL}
	if ctrlBin != "" {
		spec.controllers, spec.frontRun, spec.frps = &ControllerConfig{Binary: ctrlBin}, frontRun, frpsRun
		spec.relayKeeper = o.RelayKeeper
		spec.bulkPath = o.BulkPath
	}
	st, readyzAt, readyAt, err := startAndWait(ctx, spec, front, o.ReadyTimeout, o.Log)
	if err != nil {
		return nil, err
	}
	if fresh {
		if err := l.saveStoreVersion(st.Version, "up"); err != nil {
			return nil, err
		}
	}
	rep := &UpReport{
		Version: st.Version, StateDir: l.dir, Kubeconfig: l.kubeconfig(), Ports: ports, PID: st.Procs[0].PID,
		ReadyzMS: readyzAt.Milliseconds(), ReadyMS: readyAt.Milliseconds(), Front: front,
	}
	if rss, err := rssMiB([]int{rep.PID}); err == nil {
		rep.RSSMiB = rss
	}
	// Either fully up or not up: a failure from here on stops what was started, so a retry
	// starts from a known state.
	abort := func(what string, cause error) (*UpReport, error) {
		_, serr := stopRecorded(context.Background(), st, 15*time.Second, o.Log)
		if serr != nil {
			return nil, fmt.Errorf("%s: %w (and stopping the host failed: %v; run hpcburst down)", what, cause, serr)
		}
		_ = l.clearRecord()
		return nil, fmt.Errorf("%s: %w (the host was stopped again)", what, cause)
	}
	if o.ManifestsDir != "" {
		cfg, err := restConfig(l.kubeconfig(), ports, 30*time.Second)
		if err == nil {
			rep.Applied, err = ApplyManifests(ctx, cfg, o.ManifestsDir, o.Log)
		}
		if err != nil {
			return abort("manifests", err)
		}
	}
	// The CRD deletion guard goes in on every start, whatever --manifests named (HBEP-11; issue
	// #242), and after the manifests, which may have applied the CRDs it protects.
	// up returns only once the API server refuses a delete of a framework CRD, if there is one: admission
	// learns of the guard after the apply (WaitGuardEnforced).
	if cfg, err := restConfig(l.kubeconfig(), ports, 30*time.Second); err != nil {
		return abort("CRD deletion guard", err)
	} else if err := InstallGuard(ctx, cfg, o.Log); err != nil {
		return abort("CRD deletion guard", err)
	} else if err := WaitGuardEnforced(ctx, cfg, o.Log); err != nil {
		return abort("CRD deletion guard", err)
	}
	rep.CRDGuard = true
	// The relay keeper's switch back, with k3s ready and before the controllers start (HBEP-6,
	// Compatibility and upgrades).
	if cfg, err := restConfig(l.kubeconfig(), ports, 30*time.Second); err != nil {
		return abort("relay keeper", err)
	} else if err := checkKeeperSwitchBack(ctx, cfg, l, o.RelayKeeper, o.Log); err != nil {
		return abort("relay keeper", err)
	}
	if ctrlBin == "" {
		rep.Warnings = append(rep.Warnings, noControllerWarning)
		logf(o.Log, "%s", noControllerWarning)
	} else {
		// In this order: the namespace the controllers' Leases live in, then the controllers.
		cfg, err := restConfig(l.kubeconfig(), ports, 30*time.Second)
		if err == nil {
			err = ensureControllerNamespace(ctx, cfg)
		}
		if err != nil {
			return abort("controllers", err)
		}
		if what, err := startControllers(ctx, l, ports, o, st, ctrlBin, frontRun, frpsRun, rep); err != nil {
			return abort(what, err)
		}
	}
	if !front.Set() {
		rep.Warnings = append(rep.Warnings, frontWarning)
		logf(o.Log, "%s", frontWarning)
	}
	if ctrlBin != "" {
		// Last, with the host and its controllers up: a problem with the builds is not a failed
		// start. The host keeps running, so the operator can place the builds and run status.
		return rep, checkBuildsAtUp(ctx, l, ports, rep)
	}
	return rep, nil
}

// startControllers is the step of up that starts the controllers, once the namespace their Leases
// live in exists: it records them in st, writes frps's files, starts the supervisor, which starts
// the controller, the front and frps, and fills in the report, with the warnings among it. On an
// error it returns the step's name for up's abort.
func startControllers(ctx context.Context, l layout, ports Ports, o UpOptions, st *State, ctrlBin string, frontRun *FrontRunConfig, frpsRun *FrpsRunConfig, rep *UpReport) (string, error) {
	argv := o.supervisorArgv
	if argv == nil {
		var err error
		if argv, err = supervisorArgv(l); err != nil {
			return "controllers", err
		}
	}
	st.Controllers = &ControllerConfig{Binary: ctrlBin}
	st.FrontRun, st.Frps = frontRun, frpsRun
	if frpsRun != nil {
		// frps's web password and configuration, both mode 600 (HBEP-6), before the supervisor
		// that starts frps; a password that exists is kept, so the controller's copy stays valid.
		// With a front, frps posts each admitted registration to the front's plugin as well, on
		// the front's health address (HBEP-6, Admission; #371).
		frontAddr := ""
		if frontRun != nil {
			frontAddr = ports.FrontHealthAddr()
		}
		if _, err := writeFrpsFiles(l.host(ports), frontAddr); err != nil {
			return "frps", err
		}
		// The bulk path's second frps: its own password and configuration, posting to the
		// plugins' bulk paths (HBEP-6, The bulk path).
		if st.BulkPath {
			if _, err := writeFrpsBulkFiles(l.host(ports), frontAddr); err != nil {
				return "frps-bulk", err
			}
		}
	} else {
		rep.Warnings = append(rep.Warnings, noFrpsWarning)
		logf(o.Log, "%s", noFrpsWarning)
	}
	if err := startSupervisor(ctx, l, st, argv, superviseStartTimeout, o.Log); err != nil {
		return "controllers", err
	}
	rep.SupervisorPID, rep.ControllerBin, rep.ControllerHealth = st.Procs[len(st.Procs)-1].PID, ctrlBin, ports.ControllerAddr()
	if frontRun != nil {
		rep.FrontHealth = ports.FrontHealthAddr()
	}
	if frpsRun != nil {
		h := l.host(ports)
		rep.FrpsPorts = []int{h.FrpsBindPort(), h.FrpsTCPMuxPort(), h.FrpsWebPort()}
		if st.BulkPath {
			rep.FrpsBulkPorts = []int{h.FrpsBulkBindPort(), h.FrpsBulkTCPMuxPort(), h.FrpsBulkWebPort()}
		}
	}
	if o.RelayKeeper {
		rep.RelayKeeperSocket = l.host(ports).RelayKeeperSocket()
	}
	// A tunnel process whose hard limit on open files is below HBEP-6's figure is warned about,
	// never refused: the limit is the host's to set (HBEP-11, Single-user host; #431).
	for _, w := range openFilesWarnings(l, o.limitReader) {
		rep.Warnings = append(rep.Warnings, w)
		logf(o.Log, "%s", w)
	}
	return "", nil
}

// checkBuildsAtUp sets rep.AgentBuilds. It returns an *AgentBuildsError when a build is missing or
// does not match, or when the PlatformProfiles could not be read (the check did not run).
func checkBuildsAtUp(ctx context.Context, l layout, ports Ports, rep *UpReport) error {
	dir := l.host(ports).AgentBuildsDir()
	cfg, err := restConfig(l.kubeconfig(), ports, 30*time.Second)
	var ab *AgentBuilds
	if err == nil {
		ab, err = CheckAgentBuilds(ctx, cfg, dir)
	}
	if err != nil {
		rep.AgentBuilds = &AgentBuilds{Dir: dir, Detail: err.Error()}
		return &AgentBuildsError{rep.AgentBuilds}
	}
	rep.AgentBuilds = ab
	if len(ab.Problems) > 0 {
		return &AgentBuildsError{ab}
	}
	return nil
}

var errNotRunning = errors.New("no host is recorded in the state directory")
