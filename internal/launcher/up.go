package launcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// DefaultReadyTimeout bounds the wait for readiness. A cold start of k3s unpacks its embedded
// binaries first (about 10 s on a loaded VM in S3).
const DefaultReadyTimeout = 120 * time.Second

// UpOptions are the inputs of Up.
type UpOptions struct {
	StateDir     string
	BinDir       string
	Version      string // default DefaultVersion
	PortBase     int    // default DefaultPortBase
	Front        Front
	Nice         int    // nice(1) value for k3s; 0 leaves it alone
	ManifestsDir string // applied after readiness, CRDs first
	ReadyTimeout time.Duration
	Log          io.Writer // progress and warnings
}

// UpReport is the outcome of a start.
type UpReport struct {
	Version    string   `json:"version"`
	StateDir   string   `json:"state_dir"`
	Kubeconfig string   `json:"kubeconfig"`
	Ports      Ports    `json:"ports"`
	PID        int      `json:"pid"`
	ReadyzMS   int64    `json:"readyz_ms"`
	ReadyMS    int64    `json:"ready_ms"`
	RSSMiB     float64  `json:"rss_mib"`
	Applied    int      `json:"manifests_applied"`
	Front      Front    `json:"front"`
	Warnings   []string `json:"warnings,omitempty"`
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

// startAndWait launches k3s from spec, records it at once, waits for readiness, and writes the
// kubeconfig (mode 0600) only after the host is ready. If anything fails it stops what it
// started, so a failed start never leaves a process behind.
func startAndWait(ctx context.Context, spec launchSpec, front Front, timeout time.Duration, log io.Writer) (*State, time.Duration, time.Duration, error) {
	t0 := time.Now()
	rec, err := launch(spec)
	if err != nil {
		return nil, 0, 0, err
	}
	st := &State{
		Version: spec.bin.Version.String(), Binary: spec.bin.Path, BinarySHA256: spec.bin.SHA256,
		Nice: spec.nice, Ports: spec.ports, Procs: []ProcRec{rec}, StartedAt: t0, Front: front,
	}
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
	readyzAt, readyAt, err := waitReady(ctx, spec.l, st, timeout)
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
// start when the state directory already records a live host, or when one of its ports is taken.
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
	if o.Version == "" {
		o.Version = DefaultVersion
	}
	if o.ReadyTimeout <= 0 {
		o.ReadyTimeout = DefaultReadyTimeout
	}
	bin, err := ResolveBinary(o.BinDir, o.Version)
	if err != nil {
		return nil, err
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
			if v, _ := identify(rec); v == Ours {
				return nil, fmt.Errorf("a host is already running in %s (%s pid %d); run hpcburst down first", l.dir, rec.Name, rec.PID)
			}
		}
		logf(o.Log, "the record in %s names processes that are gone; replacing it", l.dir)
		if err := l.clearRecord(); err != nil {
			return nil, err
		}
	}
	// Checked last, as close to the start as possible: another process may take a port while the
	// binary is being hashed.
	if err := checkPortsFree(ports.All()); err != nil {
		return nil, fmt.Errorf("refusing to start: %w", err)
	}

	st, readyzAt, readyAt, err := startAndWait(ctx, launchSpec{l: l, bin: bin, ports: ports, nice: o.Nice}, front, o.ReadyTimeout, o.Log)
	if err != nil {
		return nil, err
	}
	rep := &UpReport{
		Version: st.Version, StateDir: l.dir, Kubeconfig: l.kubeconfig(), Ports: ports, PID: st.Procs[0].PID,
		ReadyzMS: readyzAt.Milliseconds(), ReadyMS: readyAt.Milliseconds(), Front: front,
	}
	if rss, err := rssMiB([]int{rep.PID}); err == nil {
		rep.RSSMiB = rss
	}
	if o.ManifestsDir != "" {
		cfg, err := restConfig(l.kubeconfig(), ports, 30*time.Second)
		if err == nil {
			rep.Applied, err = ApplyManifests(ctx, cfg, o.ManifestsDir, o.Log)
		}
		if err != nil {
			// Either fully up or not up: stop the host, so a retry starts from a known state.
			_, serr := stopRecorded(context.Background(), st, 10*time.Second, o.Log)
			if serr != nil {
				return nil, fmt.Errorf("manifests: %w (and stopping the host failed: %v; run hpcburst down)", err, serr)
			}
			_ = l.clearRecord()
			return nil, fmt.Errorf("manifests: %w (the host was stopped again)", err)
		}
	}
	if !front.Set() {
		rep.Warnings = append(rep.Warnings, frontWarning)
		logf(o.Log, "%s", frontWarning)
	}
	return rep, nil
}

var errNotRunning = errors.New("no host is recorded in the state directory")
