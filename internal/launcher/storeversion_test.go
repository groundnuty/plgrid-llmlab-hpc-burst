package launcher

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func storeLayout(t *testing.T) layout {
	t.Helper()
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(l.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return l
}

// putStore writes a stand-in store and server token, as k3s lays them out.
func putStore(t *testing.T, l layout, db, token string, wal string) {
	t.Helper()
	if err := os.MkdirAll(l.sqliteDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{l.storeDB(): db, l.tokenFile(): token}
	if wal != "" {
		files[l.storeDB()+"-wal"] = wal
	}
	for p, c := range files {
		if err := os.WriteFile(p, []byte(c), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func fileText(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestStartVersion: which release up may start on a state directory. The rule is HBEP-11's:
// every version change goes through an upgrade (backup first, one step) or a restore.
func TestStartVersion(t *testing.T) {
	t.Run("no store: the requested release, or the default", func(t *testing.T) {
		l := storeLayout(t)
		if v, err := startVersion(l, ""); err != nil || v != DefaultVersion {
			t.Errorf("%q, %v", v, err)
		}
		if v, err := startVersion(l, "1.35.9"); err != nil || v != "1.35.9" {
			t.Errorf("%q, %v", v, err)
		}
	})
	t.Run("a store without a record is refused", func(t *testing.T) {
		l := storeLayout(t)
		putStore(t, l, "db", "tok", "")
		if _, err := startVersion(l, ""); err == nil || !strings.Contains(err.Error(), "no record") {
			t.Errorf("%v", err)
		}
	})
	l := storeLayout(t)
	putStore(t, l, "db", "tok", "")
	if err := l.saveStoreVersion("v1.35.9+k3s1", "up"); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		req, want, refusal string
	}{
		{"", "v1.35.9+k3s1", ""}, // a plain up starts the store's release, not DefaultVersion
		{"1.35.9", "v1.35.9+k3s1", ""},
		{"v1.35.9+k3s1", "v1.35.9+k3s1", ""},
		{"1.36.5", "", "hpcburst upgrade"},       // newer: an upgrade, with its backup
		{"v1.35.9+k3s2", "", "hpcburst upgrade"}, // a newer revision too
		{"1.34.1", "", "hpcburst restore"},       // older: only by restore
		{"1.35.8", "", "hpcburst restore"},
	} {
		v, err := startVersion(l, c.req)
		if c.refusal == "" {
			if err != nil || v != c.want {
				t.Errorf("--version %q: %q, %v; want %q", c.req, v, err, c.want)
			}
		} else if err == nil || !strings.Contains(err.Error(), c.refusal) {
			t.Errorf("--version %q: %q, %v; want a refusal pointing to %s", c.req, v, err, c.refusal)
		}
	}
}

// fakeHostOps stands in for stopping and starting k3s. stop writes the old release's last bytes
// to the store, as a host flushing on shutdown does; start writes the new release's bytes and a
// write-ahead log, and notes which release the record named when it was called.
type fakeHostOps struct {
	t          *testing.T
	l          layout
	steps      []string
	recordSeen string
	stopErr    error
}

func (f *fakeHostOps) stop(ctx context.Context, st *State, grace time.Duration, log io.Writer) error {
	f.steps = append(f.steps, "stop")
	if f.stopErr != nil {
		return f.stopErr
	}
	return os.WriteFile(f.l.storeDB(), []byte("v1-final"), 0o600)
}

func (f *fakeHostOps) start(ctx context.Context, spec launchSpec, front Front, timeout time.Duration, log io.Writer) (*State, time.Duration, error) {
	f.steps = append(f.steps, "start")
	sv, err := f.l.loadStoreVersion()
	if err != nil || sv == nil {
		f.t.Fatalf("no store record at start: %v", err)
	}
	f.recordSeen = sv.Version
	putStore(f.t, f.l, "v2", "tok", "v2-wal")
	return &State{Version: spec.bin.Version.String()}, time.Millisecond, nil
}

// TestReplaceHostOrder pins the order of an upgrade's store steps: stop, back up, record the new
// release, start. A backup taken before the stop would miss the old host's last writes; one taken
// after the start would hold the new release's.
func TestReplaceHostOrder(t *testing.T) {
	newBin := Binary{Version: mustVersion(t, "v1.36.5+k3s1")}
	st := &State{Version: "v1.35.9+k3s1", Ports: Ports{1, 2, 3, 4}}
	t.Run("in order", func(t *testing.T) {
		l := storeLayout(t)
		putStore(t, l, "v1", "tok", "")
		if err := l.saveStoreVersion("v1.35.9+k3s1", "up"); err != nil {
			t.Fatal(err)
		}
		ops := &fakeHostOps{t: t, l: l}
		_, backup, _, _, err := replaceHost(context.Background(), l, st, newBin, ops, time.Second, time.Second, nil)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(ops.steps, ",") != "stop,start" {
			t.Errorf("steps %v", ops.steps)
		}
		if got := fileText(t, filepath.Join(backup, "db", "state.db")); got != "v1-final" {
			t.Errorf("the backup holds %q, want the store as the old host left it at its stop", got)
		}
		if _, err := os.Stat(filepath.Join(backup, "db", "state.db-wal")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the backup holds the new release's write-ahead log: %v", err)
		}
		if ops.recordSeen != "v1.36.5+k3s1" {
			t.Errorf("when the new release started, the record named %q; it must name the new release", ops.recordSeen)
		}
		// The backup: the store and the token, mode 0600, and backup.json naming the old release.
		for _, f := range []string{"db/state.db", "token", "backup.json"} {
			fi, err := os.Stat(filepath.Join(backup, f))
			if err != nil || fi.Mode().Perm() != 0o600 {
				t.Errorf("%s: %v, %v", f, fi, err)
			}
		}
		if fileText(t, filepath.Join(backup, "token")) != "tok" {
			t.Error("the token in the backup differs")
		}
		var info backupInfo
		if err := json.Unmarshal([]byte(fileText(t, filepath.Join(backup, "backup.json"))), &info); err != nil || info.Version != "v1.35.9+k3s1" {
			t.Errorf("backup.json: %+v, %v", info, err)
		}
	})
	t.Run("a failed stop starts nothing and backs up nothing", func(t *testing.T) {
		l := storeLayout(t)
		putStore(t, l, "v1", "tok", "")
		_ = l.saveStoreVersion("v1.35.9+k3s1", "up")
		ops := &fakeHostOps{t: t, l: l, stopErr: errors.New("boom")}
		if _, _, _, _, err := replaceHost(context.Background(), l, st, newBin, ops, time.Second, time.Second, nil); err == nil {
			t.Fatal("no error")
		}
		if strings.Join(ops.steps, ",") != "stop" {
			t.Errorf("steps %v", ops.steps)
		}
		if _, err := os.Stat(l.backupsDir()); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("a backup was made: %v", err)
		}
		if sv, _ := l.loadStoreVersion(); sv.Version != "v1.35.9+k3s1" {
			t.Errorf("the record moved to %s", sv.Version)
		}
	})
	t.Run("a failed backup starts nothing and keeps the record", func(t *testing.T) {
		l := storeLayout(t)
		putStore(t, l, "v1", "tok", "")
		_ = l.saveStoreVersion("v1.35.9+k3s1", "up")
		if err := os.Remove(l.tokenFile()); err != nil {
			t.Fatal(err)
		}
		ops := &fakeHostOps{t: t, l: l}
		if _, _, _, _, err := replaceHost(context.Background(), l, st, newBin, ops, time.Second, time.Second, nil); err == nil || !strings.Contains(err.Error(), "token") {
			t.Fatalf("err %v", err)
		}
		if strings.Join(ops.steps, ",") != "stop" {
			t.Errorf("steps %v", ops.steps)
		}
		if sv, _ := l.loadStoreVersion(); sv.Version != "v1.35.9+k3s1" {
			t.Errorf("the record moved to %s", sv.Version)
		}
	})
}

// TestRestore: the store and token come back from the backup, the replaced ones are moved aside
// (never copied over: the later release's write-ahead log must not stay next to the restored
// file), and the record names the backup's release.
func TestRestore(t *testing.T) {
	l := storeLayout(t)
	putStore(t, l, "v1", "tok1", "")
	backup, err := backupStore(l, "v1.35.9+k3s1")
	if err != nil {
		t.Fatal(err)
	}
	putStore(t, l, "v2", "tok2", "v2-wal")
	if err := l.saveStoreVersion("v1.36.5+k3s1", "upgrade"); err != nil {
		t.Fatal(err)
	}

	t.Run("refused while a host runs", func(t *testing.T) {
		cmd := startBystander(t, "sleep", "300")
		rec, err := newRec("k3s", cmd.Process.Pid)
		if err != nil {
			t.Fatal(err)
		}
		if err := l.saveState(&State{Version: "v1.36.5+k3s1", Procs: []ProcRec{rec}}); err != nil {
			t.Fatal(err)
		}
		defer l.clearRecord()
		if _, err := Restore(RestoreOptions{StateDir: l.dir, Backup: backup}); err == nil || !strings.Contains(err.Error(), "stop it first") {
			t.Fatalf("restore over a running host: %v", err)
		}
		if fileText(t, l.storeDB()) != "v2" {
			t.Fatal("the store changed")
		}
	})
	t.Run("refused for a directory that is not a backup", func(t *testing.T) {
		if _, err := Restore(RestoreOptions{StateDir: l.dir, Backup: shortDir(t)}); err == nil {
			t.Fatal("accepted")
		}
	})

	rep, err := Restore(RestoreOptions{StateDir: l.dir, Backup: backup})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Version != "v1.35.9+k3s1" {
		t.Errorf("version %s", rep.Version)
	}
	if fileText(t, l.storeDB()) != "v1" || fileText(t, l.tokenFile()) != "tok1" {
		t.Error("the store or the token was not restored")
	}
	if _, err := os.Stat(l.storeDB() + "-wal"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the later release's write-ahead log is still next to the restored store: %v", err)
	}
	for _, p := range []string{l.storeDB(), l.tokenFile()} {
		if fi, _ := os.Stat(p); fi.Mode().Perm() != 0o600 {
			t.Errorf("%s mode %v", p, fi.Mode().Perm())
		}
	}
	if fileText(t, filepath.Join(rep.Replaced, "db", "state.db")) != "v2" || fileText(t, filepath.Join(rep.Replaced, "db", "state.db-wal")) != "v2-wal" ||
		fileText(t, filepath.Join(rep.Replaced, "token")) != "tok2" {
		t.Error("the replaced store was not kept aside")
	}
	sv, err := l.loadStoreVersion()
	if err != nil || sv.Version != "v1.35.9+k3s1" || sv.By != "restore" {
		t.Errorf("record %+v, %v", sv, err)
	}
	// And up now starts the restored release, and refuses the later one.
	if v, err := startVersion(l, ""); err != nil || v != "v1.35.9+k3s1" {
		t.Errorf("after the restore up would start %q, %v", v, err)
	}
	if _, err := startVersion(l, "1.36.5"); err == nil {
		t.Error("after the restore up would start 1.36.5 on the restored store")
	}
}
