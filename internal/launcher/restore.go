package launcher

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// RestoreOptions are the inputs of Restore.
type RestoreOptions struct {
	StateDir string
	Backup   string // a directory under backups/, as upgrade made it
	Log      io.Writer
}

// RestoreReport is the outcome of a restore.
type RestoreReport struct {
	Version  string `json:"version"`  // the release that wrote the restored store: what up starts next
	Backup   string `json:"backup"`   // what was restored
	Replaced string `json:"replaced"` // where the store it replaced was moved; empty when there was none
}

// Restore puts a backup's store and server token back in place of the current ones, with the host
// stopped, and records the backup's release as the store's writer. It is HBEP-11's way down: the
// only way to start an earlier release on a store a later one has written. Changes made after
// the backup are lost from the store, but the replaced store is moved aside, not deleted.
func Restore(o RestoreOptions) (*RestoreReport, error) {
	l, err := newLayout(o.StateDir)
	if err != nil {
		return nil, err
	}
	if o.Backup == "" {
		return nil, errors.New("no backup given")
	}
	src, err := filepath.Abs(o.Backup)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(l.dir); err != nil {
		return nil, fmt.Errorf("state directory %s: %w", l.dir, err)
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
	if st != nil {
		for _, rec := range st.Procs {
			switch v, d := identify(rec); v {
			case Ours:
				return nil, fmt.Errorf("a host is running in %s (%s pid %d); stop it first with hpcburst down", l.dir, rec.Name, rec.PID)
			case Unknown:
				return nil, fmt.Errorf("cannot tell whether the recorded %s still runs (%s); nothing was restored", rec.Name, d)
			}
		}
		if err := l.clearRecord(); err != nil {
			return nil, err
		}
	}
	info, err := readBackupInfo(src)
	if err != nil {
		return nil, err
	}

	// Move the current store aside rather than copy over it: a write-ahead log the later release
	// left next to state.db would otherwise be replayed onto the restored file.
	rep := &RestoreReport{Version: info.Version, Backup: src}
	server := filepath.Dir(l.sqliteDir())
	if err := os.MkdirAll(server, 0o700); err != nil {
		return nil, err
	}
	aside := filepath.Join(server, "replaced-"+time.Now().UTC().Format("20060102T150405Z"))
	for _, p := range []string{l.sqliteDir(), l.tokenFile()} {
		if _, err := os.Lstat(p); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		if rep.Replaced == "" {
			if err := os.Mkdir(aside, 0o700); err != nil {
				return nil, err
			}
			rep.Replaced = aside
		}
		if err := os.Rename(p, filepath.Join(aside, filepath.Base(p))); err != nil {
			return nil, fmt.Errorf("move the current %s aside: %w", p, err)
		}
	}
	if err := os.Mkdir(l.sqliteDir(), 0o700); err != nil {
		return nil, err
	}
	for _, f := range info.Files {
		if err := copyFileSync(filepath.Join(src, f), filepath.Join(server, f)); err != nil {
			return nil, fmt.Errorf("restore %s: %w", f, err)
		}
	}
	for _, d := range []string{l.sqliteDir(), server} {
		if err := syncDir(d); err != nil {
			return nil, err
		}
	}
	if err := l.saveStoreVersion(info.Version, "restore"); err != nil {
		return nil, err
	}
	logf(o.Log, "restored the store of k3s %s from %s", info.Version, src)
	if rep.Replaced != "" {
		logf(o.Log, "the store it replaced is in %s", rep.Replaced)
	}
	return rep, nil
}

// readBackupInfo reads and checks backup.json: a known release, and the store and token present.
func readBackupInfo(dir string) (backupInfo, error) {
	var info backupInfo
	b, err := os.ReadFile(filepath.Join(dir, backupInfoFile))
	if err != nil {
		return info, fmt.Errorf("%s is not a backup made by hpcburst upgrade: %w", dir, err)
	}
	if err := json.Unmarshal(b, &info); err != nil {
		return info, fmt.Errorf("%s: %w", filepath.Join(dir, backupInfoFile), err)
	}
	if _, err := ParseVersion(info.Version); err != nil {
		return info, fmt.Errorf("%s: %w", filepath.Join(dir, backupInfoFile), err)
	}
	have := map[string]bool{}
	for _, f := range info.Files {
		if !filepath.IsLocal(f) || (f != "token" && filepath.Dir(f) != "db") {
			return info, fmt.Errorf("%s lists %q, which is not a store file", filepath.Join(dir, backupInfoFile), f)
		}
		if fi, err := os.Stat(filepath.Join(dir, f)); err != nil || !fi.Mode().IsRegular() {
			return info, fmt.Errorf("the backup %s lacks %s: %v", dir, f, err)
		}
		have[f] = true
	}
	if !have["db/state.db"] || !have["token"] {
		return info, fmt.Errorf("the backup %s must hold db/state.db and token, and holds %v", dir, info.Files)
	}
	return info, nil
}
