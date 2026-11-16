package launcher

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

// fileDisk stands in for the disk of a supervisor that runs as a process of its own (the
// controllers_test.go helper reads HBTEST_DISK_FILE): the file holds {"free": {path: bytes},
// "size": {path: bytes}}, and is read at each ask, so rewriting it changes what the gate sees. A
// path it does not name is an error, as in fakeDisk, and every ask is appended to <file>.asked.
type fileDisk struct{ path string }

type diskFile struct {
	Free map[string]int64 `json:"free"`
	Size map[string]int64 `json:"size"`
}

func (d fileDisk) read(kind, p string) (int64, error) {
	if f, err := os.OpenFile(d.path+".asked", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
		_, _ = io.WriteString(f, kind+" "+p+"\n")
		f.Close()
	}
	b, err := os.ReadFile(d.path)
	if err != nil {
		return 0, err
	}
	var df diskFile
	if err := json.Unmarshal(b, &df); err != nil {
		return 0, err
	}
	m := df.Free
	if kind == "Size" {
		m = df.Size
	}
	v, ok := m[p]
	if !ok {
		return 0, errors.New("fileDisk: " + kind + " was asked about a path that is not the store's: " + p)
	}
	return v, nil
}

func (d fileDisk) Free(p string) (int64, error) { return d.read("Free", p) }
func (d fileDisk) Size(p string) (int64, error) { return d.read("Size", p) }

var diskFileMu sync.Mutex

func (f *ctlFixture) writeDiskFile(free, store, prevCopy int64) string {
	f.t.Helper()
	diskFileMu.Lock()
	defer diskFileMu.Unlock()
	path := filepath.Join(f.ctl, "disk.json")
	b, _ := json.Marshal(diskFile{
		Free: map[string]int64{f.l.eventLogDir(): free},
		Size: map[string]int64{f.l.eventLogDir(): store, f.l.eventLogCopyDir(): prevCopy},
	})
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		f.t.Fatal(err)
	}
	return path
}

// The same row through the real start path: startSupervisor starts the supervisor as a process of its
// own (the test binary, as "hpcburst supervise") on a record with the log and a disk that is 23.5 GB
// free. The start succeeds, the controller runs, the log does not, and the record says why where up
// reads it; the files the clients need are written anyway, the token among them; and once the space
// reads 23.6 GB the supervisor starts the log with no new up.
func TestStartSupervisorWithAGatedLogRunsTheControllersAndSaysWhy(t *testing.T) {
	cfg := EventLogConfig{Caps: eventlog.Caps{Events: 2 * gib, Usage: 2 * gib}, Floor: floor15gb}
	f := eventLogFixture(t, cfg)
	diskPath := f.writeDiskFile(23_500_000_000, 0, 0)
	// k3s's stand-in is a sleep process, recorded first as k3s is, never this test process: the
	// clean-up stops whatever the record names.
	k3s := startBystander(t, "sleep", "300")
	k3sRec, err := newRec("k3s", k3s.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	f.st.Procs = []ProcRec{k3sRec}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(superviseEnv, f.l.dir)
	t.Setenv("HBTEST_DISK_FILE", diskPath)
	stopOnExit(t, f)
	// The record of a started host: the supervisor will be recorded after k3s's stand-in.
	var log strings.Builder
	if err := startSupervisor(context.Background(), f.l, f.st, []string{self}, 30*time.Second, &log); err != nil {
		t.Fatalf("startSupervisor with a gated log: %v\n%s", err, log.String())
	}
	why := eventLogNotStarted(f.l)
	for _, want := range []string{"23500 MB free on the store's file system", "caps 2 GiB+2 GiB", "floor 15 GB"} {
		if !strings.Contains(why, want) {
			t.Errorf("the reason up reads from the record, %q, lacks %q", why, want)
		}
	}
	ctl := f.record().Entries[0]
	if ctl.Name != controllerEntryName || ctl.State != entryRunning || ctl.Proc == nil {
		t.Errorf("the controller's entry %+v: it must run although the log does not", ctl)
	}
	for _, p := range []string{f.l.host(f.ports).EventLogTokenFile(), filepath.Join(f.l.eventLogDir(), "nats.conf"), filepath.Join(f.l.eventLogDir(), "caps.json")} {
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != 0o600 {
			t.Errorf("%s: %v, %v; want it written, mode 600, although the log is not started", p, fi, err)
		}
	}
	// Every ask of the seam was about the store's own paths.
	asked := fileFor(t, diskPath+".asked")
	for _, line := range strings.Split(strings.TrimSpace(asked), "\n") {
		if line != "Free "+f.l.eventLogDir() && line != "Size "+f.l.eventLogDir() && line != "Size "+f.l.eventLogCopyDir() {
			t.Errorf("the supervisor asked the disk %q", line)
		}
	}
	// The space reads 23.6 GB: a later pass starts the log.
	f.writeDiskFile(23_600_000_000, 0, 0)
	e := f.awaitEntry(eventLogEntryName, "the log to run", func(e EntryRecord) bool { return e.State == entryRunning && e.Proc != nil })
	f.logReady()
	if e.Gate != "" || e.Restarts != 0 {
		t.Errorf("the log's entry %+v", e)
	}
	if code, err := healthzCode(f.ports.EventLogMonitorAddr()); err != nil || code != 200 {
		t.Errorf("GET /healthz on the log: %d, %v", code, err)
	}
	if why := eventLogNotStarted(f.l); why != "" {
		t.Errorf("up would still warn: %q", why)
	}
}

func fileFor(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// replaceHost with the log (HBEP-8, Backup and restore; HBEP-11): once the old host has stopped (the
// log last), the log's store is copied beside the SQLite backup, before the new release is recorded,
// and the choice rides to the new host's launch spec. The copy holds what the log wrote at its stop, which
// only a copy made after the stop can.
type logOps struct {
	fakeHostOps
	recordAtStop string
}

func (o *logOps) stop(ctx context.Context, st *State, grace time.Duration, log io.Writer) error {
	if err := o.fakeHostOps.stop(ctx, st, grace, log); err != nil {
		return err
	}
	// The log's last write, at its stop.
	return os.WriteFile(filepath.Join(o.l.eventLogDir(), "jetstream", "last.blk"), []byte("written at the stop"), 0o600)
}

func TestReplaceHostCopiesTheLogStoreAfterTheStopAndBeforeTheNewReleaseIsRecorded(t *testing.T) {
	l := storeLayout(t)
	putStore(t, l, "v1", "tok", "")
	if err := l.saveStoreVersion("v1.35.9+k3s1", "up"); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(l.eventLogDir(), "jetstream"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := &EventLogConfig{Caps: eventlog.Caps{Events: 1 << 20, Usage: 1 << 20}, Floor: 1_000_000_000}
	st := &State{Version: "v1.35.9+k3s1", Ports: Ports{API: 1, HTTPS: 2, KCM: 3, LB: 4}, EventLog: cfg}
	ops := &logOps{fakeHostOps: fakeHostOps{t: t, l: l}}
	_, backup, _, _, err := replaceHost(context.Background(), l, st, Binary{Version: mustVersion(t, "v1.36.5+k3s1")}, ops, time.Second, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := fileText(t, filepath.Join(l.eventLogCopyDir(), "jetstream", "last.blk")); got != "written at the stop" {
		t.Errorf("the copy holds %q: it was made before the stop", got)
	}
	var info eventLogCopyInfo
	mustJSON(t, filepath.Join(l.eventLogCopyDir(), "copy.json"), &info)
	if info.Backup != filepath.Base(backup) || info.Version != "v1.35.9+k3s1" {
		t.Errorf("copy.json %+v: want the backup %s and the old release", info, filepath.Base(backup))
	}
	if ops.recordSeen != "v1.36.5+k3s1" {
		t.Errorf("when the new release started, the record named %q", ops.recordSeen)
	}
	if ops.spec.eventLog == nil || *ops.spec.eventLog != *cfg {
		t.Errorf("the new host's launch spec carries %+v, want the record's %+v: upgrade keeps the choice", ops.spec.eventLog, cfg)
	}
	// No log, no copy: an alpha host has none.
	l2 := storeLayout(t)
	putStore(t, l2, "v1", "tok", "")
	_ = l2.saveStoreVersion("v1.35.9+k3s1", "up")
	if _, _, _, _, err := replaceHost(context.Background(), l2, &State{Version: "v1.35.9+k3s1", Ports: st.Ports}, Binary{Version: mustVersion(t, "v1.36.5+k3s1")}, &fakeHostOps{t: t, l: l2}, time.Second, time.Second, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(l2.eventLogCopyDir()); err == nil {
		t.Error("a host without the log got a copy")
	}
}

// A failed copy stops the upgrade before the new release is recorded: the record still names the old
// release, nothing is started, and the error says how to start the old release again.
func TestAFailedLogCopyStopsTheUpgradeBeforeTheNewReleaseIsRecorded(t *testing.T) {
	l := storeLayout(t)
	putStore(t, l, "v1", "tok", "")
	if err := l.saveStoreVersion("v1.35.9+k3s1", "up"); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(l.eventLogDir(), "jetstream"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc/hostname", filepath.Join(l.eventLogDir(), "jetstream", "link")); err != nil {
		t.Fatal(err)
	}
	st := &State{Version: "v1.35.9+k3s1", Ports: Ports{API: 1, HTTPS: 2, KCM: 3, LB: 4}, EventLog: &EventLogConfig{Caps: eventlog.Caps{Events: 1, Usage: 1}, Floor: 1}}
	ops := &fakeHostOps{t: t, l: l}
	_, backup, _, _, err := replaceHost(context.Background(), l, st, Binary{Version: mustVersion(t, "v1.36.5+k3s1")}, ops, time.Second, time.Second, nil)
	if err == nil {
		t.Fatal("a failed copy did not stop the upgrade")
	}
	for _, want := range []string{"event log's store could not be copied", "untouched", "hpcburst up --event-log", "v1.35.9+k3s1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error %q does not say %q", err, want)
		}
	}
	if strings.Join(ops.steps, ",") != "stop" {
		t.Errorf("steps %v, want the stop only: nothing starts after a failed copy", ops.steps)
	}
	if sv, _ := l.loadStoreVersion(); sv.Version != "v1.35.9+k3s1" {
		t.Errorf("the store's writer is recorded as %s: the new release was recorded before the copy failed", sv.Version)
	}
	if backup == "" {
		t.Error("the SQLite backup is not named: it was made, and the error must say where")
	}
	if _, err := os.Stat(l.eventLogCopyDir()); err == nil {
		t.Error("a copy exists")
	}
}
