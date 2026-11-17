package launcher

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A store as nats-server leaves one: nested directories and files, and the launcher's own files.
func writeLogStore(t *testing.T, l layout, content string) map[string]string {
	t.Helper()
	files := map[string]string{
		"nats.conf": "conf " + content,
		"token":     "tok " + content,
		"caps.json": "caps " + content,
		"jetstream/$G/streams/HB_EVENTS/meta.inf":   "meta " + content,
		"jetstream/$G/streams/HB_EVENTS/msgs/1.blk": strings.Repeat(content, 1000),
		"jetstream/$G/streams/HB_USAGE/msgs/1.blk":  strings.Repeat(content, 10),
	}
	for name, data := range files {
		p := filepath.Join(l.eventLogDir(), name)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return files
}

func eventLogBackupLayout(t *testing.T) layout {
	t.Helper()
	l := layout{dir: filepath.Join(t.TempDir(), "st")}
	for _, d := range []string{l.dir, l.backupsDir()} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return l
}

func readTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		b, err := os.ReadFile(p)
		out[rel] = string(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// The upgrade's copy (HBEP-8, Backup and restore): one copy, in backups/eventlog, replaced at each
// upgrade, with copy.json naming the SQLite backup it sits beside. After two upgrades one copy remains,
// and it is the second's: the first's files are gone, and no partial directory is left.
func TestTheUpgradeCopyIsReplacedAndNeverPilesUp(t *testing.T) {
	l := eventLogBackupLayout(t)
	first := writeLogStore(t, l, "first")
	b1 := filepath.Join(l.backupsDir(), "20261116T010000Z-v1.35.9+k3s1")
	if err := backupEventLog(l, b1, "v1.35.9+k3s1"); err != nil {
		t.Fatal(err)
	}
	copied := readTree(t, l.eventLogCopyDir())
	for name, want := range first {
		if copied[name] != want {
			t.Errorf("the first copy's %s is %q, want %q", name, copied[name], want)
		}
	}
	var info eventLogCopyInfo
	mustJSON(t, filepath.Join(l.eventLogCopyDir(), "copy.json"), &info)
	if info.Backup != "20261116T010000Z-v1.35.9+k3s1" || info.Version != "v1.35.9+k3s1" || time.Since(info.TakenAt) > time.Minute {
		t.Errorf("copy.json %+v", info)
	}
	// Modes: directories 700, files 600, as everything of the log's.
	_ = filepath.WalkDir(l.eventLogCopyDir(), func(p string, d os.DirEntry, err error) error {
		fi, _ := d.Info()
		if want := map[bool]os.FileMode{true: 0o700, false: 0o600}[d.IsDir()]; fi.Mode().Perm() != want {
			t.Errorf("%s has mode %v, want %v", p, fi.Mode().Perm(), want)
		}
		return nil
	})

	// A second upgrade, after the store changed: the copy is replaced.
	if err := os.RemoveAll(filepath.Join(l.eventLogDir(), "jetstream", "$G", "streams", "HB_USAGE")); err != nil {
		t.Fatal(err)
	}
	second := writeLogStore(t, l, "second")
	// The second store has no USAGE stream: the file writeLogStore wrote for it goes again.
	if err := os.RemoveAll(filepath.Join(l.eventLogDir(), "jetstream", "$G", "streams", "HB_USAGE")); err != nil {
		t.Fatal(err)
	}
	delete(second, "jetstream/$G/streams/HB_USAGE/msgs/1.blk")
	b2 := filepath.Join(l.backupsDir(), "20261117T010000Z-v1.36.5+k3s1")
	if err := backupEventLog(l, b2, "v1.36.5+k3s1"); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(l.backupsDir())
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != 1 || names[0] != "eventlog" {
		t.Fatalf("backups/ holds %v after two upgrades, want exactly one copy, eventlog", names)
	}
	if got := readTree(t, l.eventLogCopyDir()); got["jetstream/$G/streams/HB_EVENTS/msgs/1.blk"] != second["jetstream/$G/streams/HB_EVENTS/msgs/1.blk"] {
		t.Error("the copy is not the second upgrade's")
	} else if _, ok := got["jetstream/$G/streams/HB_USAGE/msgs/1.blk"]; ok {
		t.Error("the first copy's USAGE block is still there: the old copy was not removed first")
	}
	mustJSON(t, filepath.Join(l.eventLogCopyDir(), "copy.json"), &info)
	if info.Backup != "20261117T010000Z-v1.36.5+k3s1" {
		t.Errorf("copy.json names %q, want the second backup", info.Backup)
	}
}

func mustJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

// A failed copy leaves no partial copy and does not touch the live store, and the old copy is gone
// (HBEP-8: it removes the old copy, then copies the store; the live store is what is kept).
func TestAFailedCopyLeavesNoPartialCopyAndTheLiveStoreAlone(t *testing.T) {
	l := eventLogBackupLayout(t)
	live := writeLogStore(t, l, "live")
	// A link in the store is neither a directory nor a regular file: the copy refuses it.
	if err := os.Symlink("/etc/hostname", filepath.Join(l.eventLogDir(), "jetstream", "link")); err != nil {
		t.Fatal(err)
	}
	// A previous copy exists.
	if err := os.MkdirAll(l.eventLogCopyDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	err := backupEventLog(l, filepath.Join(l.backupsDir(), "20261116T010000Z-v1.36.5+k3s1"), "v1.36.5+k3s1")
	if err == nil || !strings.Contains(err.Error(), "link") {
		t.Fatalf("the copy of a store with a link: %v, want an error naming it", err)
	}
	if _, err := os.Stat(l.eventLogCopyDir() + ".partial"); err == nil {
		t.Error("a partial copy was left")
	}
	if _, err := os.Stat(l.eventLogCopyDir()); err == nil {
		t.Error("a copy exists after a failed one")
	}
	_ = os.Remove(filepath.Join(l.eventLogDir(), "jetstream", "link"))
	for name, want := range live {
		if b, err := os.ReadFile(filepath.Join(l.eventLogDir(), name)); err != nil || string(b) != want {
			t.Errorf("the live store's %s changed: %q, %v", name, b, err)
		}
	}
}

// restore puts the log's copy back only with the backup it sits beside (R5). With the paired backup
// the live store is moved aside and the copy's store takes its place; with an older backup the live
// store is left alone and the report says so; with no copy there is nothing to say.
func TestRestoreAnswersForTheLogByItsPairedBackup(t *testing.T) {
	l := eventLogBackupLayout(t)
	writeLogStore(t, l, "at the backup")
	b2 := filepath.Join(l.backupsDir(), "20261117T010000Z-v1.36.5+k3s1")
	b1 := filepath.Join(l.backupsDir(), "20261116T010000Z-v1.35.9+k3s1")
	if err := backupEventLog(l, b2, "v1.36.5+k3s1"); err != nil {
		t.Fatal(err)
	}
	aside := filepath.Join(l.dir, "data", "server", "replaced-test")
	// The store moves on after the upgrade.
	blk := filepath.Join(l.eventLogDir(), "jetstream", "$G", "streams", "HB_EVENTS", "msgs", "1.blk")
	if err := os.WriteFile(blk, []byte("after the backup"), 0o600); err != nil {
		t.Fatal(err)
	}

	// An older backup: left alone, and it says why.
	msg, err := restoreEventLog(l, b1, aside)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"left the live event log store alone", "20261117T010000Z-v1.36.5+k3s1", "20261116T010000Z-v1.35.9+k3s1", "rewritten history"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the report %q does not contain %q", msg, want)
		}
	}
	if b, _ := os.ReadFile(blk); string(b) != "after the backup" {
		t.Errorf("the live store changed with an unpaired backup: %q", b)
	}
	if _, err := os.Stat(aside); err == nil {
		t.Error("something was moved aside for an unpaired backup")
	}

	// The paired backup: the store is the copy's, and the live one is in aside, not deleted.
	msg, err = restoreEventLog(l, b2, aside)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(msg, "restored the event log's store") {
		t.Errorf("the report %q", msg)
	}
	if b, _ := os.ReadFile(blk); string(b) != strings.Repeat("at the backup", 1000) {
		t.Errorf("the restored block is %q", b[:min(len(b), 40)])
	}
	if b, err := os.ReadFile(filepath.Join(aside, "eventlog-jetstream", "$G", "streams", "HB_EVENTS", "msgs", "1.blk")); err != nil || string(b) != "after the backup" {
		t.Errorf("the replaced store in aside: %q, %v", b, err)
	}
	// The configuration, the token and the caps are not the copy's to put back: up writes them.
	if b, _ := os.ReadFile(filepath.Join(l.eventLogDir(), "token")); string(b) != "tok at the backup" {
		t.Errorf("the token file was touched: %q", b)
	}

	// No copy at all: nothing to say.
	l2 := eventLogBackupLayout(t)
	if msg, err := restoreEventLog(l2, b2, aside); msg != "" || err != nil {
		t.Errorf("with no copy: %q, %v", msg, err)
	}
	// A copy whose copy.json cannot be read leaves the live store alone, and says so.
	if err := os.Remove(filepath.Join(l.eventLogCopyDir(), "copy.json")); err != nil {
		t.Fatal(err)
	}
	if msg, err := restoreEventLog(l, b2, aside); err != nil || !strings.Contains(msg, "left the live event log store alone") {
		t.Errorf("a copy with no record: %q, %v", msg, err)
	}
}
