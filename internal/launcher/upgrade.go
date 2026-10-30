package launcher

import (
	"context"
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
}

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
}

// Upgrade moves a running host one minor version up: it snapshots every object, stops the host,
// copies the SQLite store to a backup, starts the new binary on the same data directory, and
// compares the objects. It reports the comparison; a host that lost or changed an object fails
// the upgrade, though it is left running so the damage can be inspected.
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
	from, err := ParseVersion(st.Version)
	if err != nil {
		return nil, err
	}
	bin, err := ResolveBinary(o.BinDir, o.To)
	if err != nil {
		return nil, err
	}
	if err := checkUpgradePath(from, bin.Version); err != nil {
		return nil, err
	}
	// Only a healthy host is upgraded: the snapshot below needs the API, and a host that is
	// already sick would make the comparison meaningless.
	if sr, err := Status(ctx, StatusOptions{StateDir: l.dir}); err != nil {
		return nil, err
	} else if sr.Health != Healthy {
		return nil, fmt.Errorf("the host is %s (%v); fix it or stop it before upgrading", sr.Health, sr.Reasons)
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

	t0 := time.Now()
	if _, err := stopRecorded(ctx, st, o.Grace, o.Log); err != nil {
		return nil, fmt.Errorf("stopping the old host: %w", err)
	}
	stopDur := time.Since(t0)
	backup, err := backupStore(l, st.Version)
	if err != nil {
		return nil, fmt.Errorf("the old host is stopped, but the store could not be backed up: %w", err)
	}
	logf(o.Log, "backed up the SQLite store to %s", backup)

	spec := launchSpec{l: l, bin: bin, ports: st.Ports, nice: st.Nice}
	nst, _, readyAt, err := startAndWait(ctx, spec, st.Front, o.ReadyTimeout, o.Log)
	if err != nil {
		return nil, fmt.Errorf("the new host did not come up (the host is stopped; the store is in %s and its backup in %s): %w", l.dataDir(), backup, err)
	}
	rep := &UpgradeReport{From: st.Version, To: nst.Version, Backup: backup, StopMS: stopDur.Milliseconds(), ReadyMS: readyAt.Milliseconds()}
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
	return rep, nil
}

// backupStore copies the files of the SQLite store, which must not be in use, to a new
// directory under backups/. It returns the directory.
func backupStore(l layout, fromVersion string) (string, error) {
	dst := filepath.Join(l.backupsDir(), fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102T150405Z"), fromVersion))
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return "", err
	}
	ents, err := os.ReadDir(l.sqliteDir())
	if err != nil {
		return "", err
	}
	n := 0
	for _, e := range ents {
		if !e.Type().IsRegular() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(l.sqliteDir(), e.Name()))
		if err != nil {
			return "", err
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0o600); err != nil {
			return "", err
		}
		n++
	}
	if n == 0 {
		return "", fmt.Errorf("no store files found in %s", l.sqliteDir())
	}
	return dst, nil
}
