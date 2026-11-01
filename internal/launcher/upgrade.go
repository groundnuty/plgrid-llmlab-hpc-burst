package launcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// UpgradeOptions are the inputs of Upgrade.
type UpgradeOptions struct {
	StateDir     string
	BinDir       string
	To           string // target version
	Grace        time.Duration
	ReadyTimeout time.Duration
	Log          io.Writer

	// supervisorArgv, set by tests only, replaces the supervisor's command line.
	supervisorArgv []string
}

// noFrpsUpgradeWarning is printed when upgrade keeps a host whose record runs the controllers
// without frps (one that an earlier release started, or one up found no frps for): upgrade runs
// what the record names and resolves no binary.
const noFrpsUpgradeWarning = "warning: the host runs the controllers without frps, and upgrade keeps what it runs: Instances cannot open their dial-home tunnels (HBEP-6); to run frps, hpcburst down, then up with --frps-bin or frp-" + DefaultFrpVersion + "/frps in --bin-dir"

// frpsMissing reports whether the record runs the controllers but no frps.
func (s *State) frpsMissing() bool { return s.Controllers != nil && s.Frps == nil }

// UpgradeReport is the outcome of an upgrade.
type UpgradeReport struct {
	From         string       `json:"from"`
	To           string       `json:"to"`
	Backup       string       `json:"backup"`
	Verification Verification `json:"verification"`
	StopMS       int64        `json:"stop_ms"`
	ReadyMS      int64        `json:"ready_ms"`
	StopToReady  int64        `json:"stop_to_ready_ms"`
	RSSMiB       float64      `json:"rss_mib"`
	Warnings     []string     `json:"warnings,omitempty"`
}

// Upgrade moves a running host one step up (the next minor, or a newer patch of the same minor):
// it snapshots every object, stops the host, backs the store up, starts the new binary on the same
// data directory, and compares the objects. It reports the comparison; a host that lost or changed
// an object fails the upgrade, though it is left running so the damage can be inspected.
func Upgrade(ctx context.Context, o UpgradeOptions) (*UpgradeReport, error) {
	l, err := newLayout(o.StateDir)
	if err != nil {
		return nil, err
	}
	if o.Grace <= 0 {
		o.Grace = DefaultGrace
	}
	if o.ReadyTimeout <= 0 {
		o.ReadyTimeout = DefaultReadyTimeout
	}
	unlock, err := l.lockState()
	if err != nil {
		return nil, err
	}
	defer unlock()
	st, err := l.loadState()
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, fmt.Errorf("%w (%s): start one with hpcburst up", errNotRunning, l.dir)
	}
	sv, err := l.loadStoreVersion()
	if err != nil {
		return nil, err
	}
	if sv == nil {
		return nil, fmt.Errorf("%s is missing: there is no record of the k3s release that wrote the store, so no upgrade step can be checked", l.storeVersionFile())
	}
	if sv.Version != st.Version {
		return nil, fmt.Errorf("the host runs k3s %s, but the store is recorded as written by %s; refusing to upgrade a host in that state", st.Version, sv.Version)
	}
	from, err := ParseVersion(sv.Version)
	if err != nil {
		return nil, err
	}
	bin, err := ResolveBinary(o.BinDir, o.To)
	if err != nil {
		return nil, err
	}
	defer bin.Close()
	if err := checkUpgradePath(from, bin.Version); err != nil {
		return nil, err
	}
	if err := checkGrace(st, o.Grace); err != nil {
		return nil, err
	}
	// Only a healthy host is upgraded: the snapshot below needs the API, and a host that is
	// already sick would make the comparison meaningless.
	if sr, err := Status(ctx, StatusOptions{StateDir: l.dir}); err != nil {
		return nil, err
	} else if sr.Health != Healthy {
		return nil, fmt.Errorf("the host is %s (%v); fix it or stop it before upgrading", sr.Health, sr.Reasons)
	}
	// The controllers' certificate pair is kept, and checked before the old host is stopped: a pair
	// that has since been broken is found while nothing has changed.
	if st.Controllers != nil {
		if err := ensureAgentEndpointPair(l.host(st.Ports), o.Log); err != nil {
			return nil, err
		}
	}
	cfg, err := restConfig(l.kubeconfig(), st.Ports, 60*time.Second)
	if err != nil {
		return nil, err
	}
	before, err := TakeSnapshot(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("snapshot of the objects before the upgrade: %w", err)
	}
	logf(o.Log, "upgrading %s to %s: %d objects recorded", st.Version, bin.Version, len(before))

	nst, backup, stopDur, readyAt, err := replaceHost(ctx, l, st, bin, realHost{}, o.Grace, o.ReadyTimeout, o.Log)
	if err != nil {
		return nil, err
	}
	// replaceHost stopped the supervisor with the old host. The new record keeps what it ran, the
	// controllers and the front (issue #64), from its first write on (replaceHost), so a status
	// taken from here on says they are missing until they are started again, and the new
	// supervisor runs the same set.
	rep := &UpgradeReport{From: st.Version, To: nst.Version, Backup: backup, StopMS: stopDur.Milliseconds(), ReadyMS: readyAt.Milliseconds()}
	if nst.frpsMissing() {
		rep.Warnings = append(rep.Warnings, noFrpsUpgradeWarning) // replaceHost has logged it
	}
	rep.StopToReady = stopDur.Milliseconds() + readyAt.Milliseconds()
	if rss, err := rssMiB([]int{nst.Procs[0].PID}); err == nil {
		rep.RSSMiB = rss
	}
	cfg2, err := restConfig(l.kubeconfig(), nst.Ports, 60*time.Second)
	if err != nil {
		return rep, err
	}
	after, err := TakeSnapshot(ctx, cfg2)
	if err != nil {
		return rep, fmt.Errorf("snapshot of the objects after the upgrade: %w", err)
	}
	rep.Verification = compareSnapshots(before, after)
	if !rep.Verification.OK() {
		return rep, fmt.Errorf("the upgrade lost or changed objects: kept %d of %d, missing %v, changed %v (the new host is running; the backup is %s)",
			rep.Verification.Kept, rep.Verification.Before, rep.Verification.Missing, rep.Verification.Changed, backup)
	}
	// The controllers start again only after the comparison: they write to the store, and the
	// comparison is of what the upgrade itself kept.
	if nst.Controllers != nil {
		argv := o.supervisorArgv
		if argv == nil {
			if argv, err = supervisorArgv(l); err != nil {
				return rep, err
			}
		}
		if err := startSupervisor(ctx, l, nst, argv, superviseStartTimeout, o.Log); err != nil {
			return rep, fmt.Errorf("the new host is running and kept every object, but the controllers did not start again (hpcburst down, then up): %w", err)
		}
	}
	return rep, nil
}

// hostOps stops and starts a host. Tests replace it to check the order of an upgrade's steps.
type hostOps interface {
	stop(ctx context.Context, st *State, grace time.Duration, log io.Writer) error
	start(ctx context.Context, spec launchSpec, front Front, timeout time.Duration, log io.Writer) (*State, time.Duration, error)
}

type realHost struct{}

func (realHost) stop(ctx context.Context, st *State, grace time.Duration, log io.Writer) error {
	_, err := stopRecorded(ctx, st, grace, log)
	return err
}

func (realHost) start(ctx context.Context, spec launchSpec, front Front, timeout time.Duration, log io.Writer) (*State, time.Duration, error) {
	st, _, readyAt, err := startAndWait(ctx, spec, front, timeout, log)
	return st, readyAt, err
}

// replaceHost is the part of an upgrade that touches the store, in this order: stop the old host,
// back the store up, record the new release as the store's writer, start the new host. So the
// backup holds the store exactly as the old release left it, and nothing the new release wrote;
// and once the new release may have written the store, the record says so, which leaves a restore
// as the only way back.
func replaceHost(ctx context.Context, l layout, st *State, bin Binary, ops hostOps, grace, timeout time.Duration, log io.Writer) (nst *State, backup string, stopDur, readyAt time.Duration, err error) {
	t0 := time.Now()
	if err := ops.stop(ctx, st, grace, log); err != nil {
		return nil, "", 0, 0, fmt.Errorf("stopping the old host: %w", err)
	}
	stopDur = time.Since(t0)
	backup, err = backupStore(l, st.Version)
	if err != nil {
		return nil, "", stopDur, 0, fmt.Errorf("the old host is stopped, but the store could not be backed up (nothing was started; hpcburst up starts %s again): %w", st.Version, err)
	}
	logf(log, "backed up the store to %s", backup)
	if err := l.saveStoreVersion(bin.Version.String(), "upgrade"); err != nil {
		return nil, backup, stopDur, 0, fmt.Errorf("the old host is stopped and backed up to %s, but the new release could not be recorded: %w", backup, err)
	}
	// The replacement's first record carries the old host's supervision, so an upgrade interrupted
	// while the new k3s starts leaves a record that still names the controllers and the front.
	spec := launchSpec{l: l, bin: bin, ports: st.Ports, nice: st.Nice, controllers: st.Controllers, frontRun: st.FrontRun, frps: st.Frps}
	if st.frpsMissing() {
		logf(log, "%s", noFrpsUpgradeWarning)
	}
	nst, readyAt, err = ops.start(ctx, spec, st.Front, timeout, log)
	if err != nil {
		return nil, backup, stopDur, 0, fmt.Errorf("the new host did not come up (the host is stopped; the store in %s is recorded as written by %s; to go back to %s, run hpcburst restore --backup %s): %w",
			l.dataDir(), bin.Version, st.Version, backup, err)
	}
	return nst, backup, stopDur, readyAt, nil
}

// backupInfo is backup.json, inside every backup.
type backupInfo struct {
	Version string    `json:"version"` // the k3s release that wrote the store
	TakenAt time.Time `json:"taken_at"`
	Files   []string  `json:"files"`
}

const backupInfoFile = "backup.json"

// backupStore copies the store as k3s left it to a new directory under backups/: every file of
// data/server/db, and the server token, without which k3s cannot read a restored store (k3s's
// backup and restore documentation). Each file is streamed and synced to disk, and backup.json
// names the release that wrote the store. The directory gets its final name only once it is
// complete. The store must not be in use.
func backupStore(l layout, version string) (string, error) {
	dst := filepath.Join(l.backupsDir(), fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102T150405Z"), version))
	tmp := dst + ".partial"
	if err := os.MkdirAll(filepath.Join(tmp, "db"), 0o700); err != nil {
		return "", err
	}
	ents, err := os.ReadDir(l.sqliteDir())
	if err != nil {
		return "", err
	}
	info := backupInfo{Version: version, TakenAt: time.Now().UTC()}
	for _, e := range ents {
		if !e.Type().IsRegular() {
			continue
		}
		if err := copyFileSync(filepath.Join(l.sqliteDir(), e.Name()), filepath.Join(tmp, "db", e.Name())); err != nil {
			return "", err
		}
		info.Files = append(info.Files, "db/"+e.Name())
	}
	if len(info.Files) == 0 {
		return "", fmt.Errorf("no store files found in %s", l.sqliteDir())
	}
	if err := copyFileSync(l.tokenFile(), filepath.Join(tmp, "token")); err != nil {
		return "", fmt.Errorf("the server token: %w", err)
	}
	info.Files = append(info.Files, "token")
	b, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return "", err
	}
	if err := writeFileAtomic(filepath.Join(tmp, backupInfoFile), append(b, '\n'), 0o600); err != nil {
		return "", err
	}
	for _, d := range []string{filepath.Join(tmp, "db"), tmp} {
		if err := syncDir(d); err != nil {
			return "", err
		}
	}
	if err := os.Rename(tmp, dst); err != nil {
		return "", err
	}
	return dst, syncDir(l.backupsDir())
}

// copyFileSync streams src to a new file dst (mode 0600) and syncs it to disk.
func copyFileSync(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return fmt.Errorf("copy %s: %w", src, err)
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
