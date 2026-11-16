package launcher

// The durable log in upgrade's backup and in restore (HBEP-8, The durable log, Backup and restore;
// HBEP-11, Single-user host; issue #625, R5). Before each upgrade, once the log has stopped (it stops
// last), the launcher replaces the previous upgrade's copy of <state>/eventlog/ with a new one, beside
// its copy of the SQLite file: it removes the old copy, then copies the store, so copies never pile
// up. There is one copy, in <state>/backups/eventlog/, and many SQLite backups, so the copy records
// which backup it sits beside (copy.json), and restore puts it back only with that backup.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

// eventLogCopyInfoFile is the copy's record of the SQLite backup it sits beside.
const eventLogCopyInfoFile = "copy.json"

// eventLogStoreDir is the directory of the log's store inside <state>/eventlog/: nats-server's
// jetstream directory. The rest of <state>/eventlog/ (the configuration, the token and the caps) is
// written again at each start.
const eventLogStoreDir = "jetstream"

// eventLogCopyInfo is copy.json: which SQLite backup (the name of its directory under backups/) the
// copy sits beside, the k3s release that wrote that backup's store, and when the copy was made.
type eventLogCopyInfo struct {
	Backup  string    `json:"backup"`
	Version string    `json:"version"`
	TakenAt time.Time `json:"taken_at"`
}

// backupEventLog replaces the previous copy of the log's store with a copy of the current one, beside
// the SQLite backup in backupDir. The log must have stopped. The old copy is removed first, and the new one
// is built in a directory of its own that gets its final name only once it is complete and synced,
// each file synced as copyFileSync does. A copy that fails leaves no partial one, and the live store
// is only read.
func backupEventLog(l layout, backupDir, version string) error {
	dst := l.eventLogCopyDir()
	tmp := dst + ".partial"
	for _, p := range []string{tmp, dst} {
		if err := os.RemoveAll(p); err != nil {
			return fmt.Errorf("remove the previous copy %s: %w", p, err)
		}
	}
	fail := func(err error) error {
		_ = os.RemoveAll(tmp)
		return err
	}
	if err := os.MkdirAll(l.backupsDir(), 0o700); err != nil {
		return err
	}
	if _, err := copyTreeSync(l.eventLogDir(), tmp); err != nil {
		return fail(fmt.Errorf("copy %s: %w", l.eventLogDir(), err))
	}
	info := eventLogCopyInfo{Backup: filepath.Base(backupDir), Version: version, TakenAt: time.Now().UTC()}
	b, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fail(err)
	}
	if err := eventlog.WriteFileAtomic(filepath.Join(tmp, eventLogCopyInfoFile), append(b, '\n'), 0o600); err != nil {
		return fail(err)
	}
	if err := syncDir(tmp); err != nil {
		return fail(err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fail(err)
	}
	return syncDir(l.backupsDir())
}

// copyTreeSync copies the tree at src to a new directory dst, file by file, each file streamed and
// synced (copyFileSync) and each directory synced after its files. It copies directories and regular
// files only, and refuses anything else (a link, a socket), which a JetStream store does not hold.
// It returns the bytes of file data copied.
func copyTreeSync(src, dst string) (int64, error) {
	var total int64
	var dirs []string
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		switch {
		case d.IsDir():
			if err := os.MkdirAll(target, 0o700); err != nil {
				return err
			}
			dirs = append(dirs, target)
		case d.Type().IsRegular():
			if err := copyFileSync(p, target); err != nil {
				return err
			}
			fi, err := os.Stat(target)
			if err != nil {
				return err
			}
			total += fi.Size()
		default:
			return fmt.Errorf("%s is neither a directory nor a regular file (%v)", p, d.Type())
		}
		return nil
	})
	if err != nil {
		return total, err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dirs))) // each directory after what is inside it
	for _, d := range dirs {
		if err := syncDir(d); err != nil {
			return total, err
		}
	}
	return total, nil
}

// restoreEventLog puts the log's copy back, only when it sits beside the backup being restored (R5).
// It returns what it did, in words, for the report and the log: "" when there is no copy at all.
// With a copy that sits beside another backup (or whose record cannot be read) it leaves the live
// store alone and says so: the log is then ahead of the API store, which is rewritten history for
// HBEP-10's cursors (S8, Finding 7). When it restores, the live store is moved into aside, not
// deleted, as the SQLite store is.
func restoreEventLog(l layout, backup, aside string) (string, error) {
	copyDir := l.eventLogCopyDir()
	if _, err := os.Stat(copyDir); errors.Is(err, fs.ErrNotExist) {
		return "", nil
	} else if err != nil {
		return "", err
	}
	b, err := os.ReadFile(filepath.Join(copyDir, eventLogCopyInfoFile))
	var info eventLogCopyInfo
	if err == nil {
		err = json.Unmarshal(b, &info)
	}
	if err != nil || info.Backup == "" {
		return fmt.Sprintf("left the live event log store alone: the copy in %s has no readable %s naming its backup (%v), so it cannot be shown to belong to this one", copyDir, eventLogCopyInfoFile, err), nil
	}
	if info.Backup != filepath.Base(backup) {
		return fmt.Sprintf("left the live event log store alone: the copy in %s sits beside the backup %s, not %s; the log is now ahead of the restored API store, so HBEP-10's cursors see rewritten history", copyDir, info.Backup, filepath.Base(backup)), nil
	}
	live := filepath.Join(l.eventLogDir(), eventLogStoreDir)
	if err := os.MkdirAll(l.eventLogDir(), 0o700); err != nil {
		return "", err
	}
	if _, err := os.Lstat(live); err == nil {
		if err := os.MkdirAll(aside, 0o700); err != nil {
			return "", err
		}
		if err := os.Rename(live, filepath.Join(aside, "eventlog-"+eventLogStoreDir)); err != nil {
			return "", fmt.Errorf("move the current event log store aside: %w", err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	saved := filepath.Join(copyDir, eventLogStoreDir)
	if _, err := os.Stat(saved); errors.Is(err, fs.ErrNotExist) {
		// The log had never started when the copy was made (it was refused for space, say): the
		// restored state is no store, and the log creates one at its next start.
		return fmt.Sprintf("restored the event log's state from %s, which sits beside this backup: it holds no store, since the log had not started then, so the live store is moved aside and the log starts empty", copyDir), nil
	}
	if _, err := copyTreeSync(saved, live); err != nil {
		return "", fmt.Errorf("restore the event log's store from %s: %w", copyDir, err)
	}
	if err := syncDir(l.eventLogDir()); err != nil {
		return "", err
	}
	return fmt.Sprintf("restored the event log's store from %s, which sits beside this backup: the log ends at the backup's last message, and the events raised since are lost from it, as from the restored API store (usage loses nothing: the forwarder re-publishes from the files)", copyDir), nil
}
