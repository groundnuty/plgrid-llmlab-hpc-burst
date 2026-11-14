package scenario_test

// The upgrade test's checks on their own (issue #552), with no stack: each check tells a good state from
// the bad one it exists to catch.

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/keeperprobe"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// startCopy starts a copy of sleep from path, ended at the test's end.
func startCopy(t *testing.T, path string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(path, "300")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	return cmd
}

// A process keeps running the file it started from after another file is renamed over its path: its
// exe's identity is the old file's, and only a process started after the replacement runs the new one.
// This is what tells an upgraded controller from one that kept the old build, though the builds are
// byte-identical.
func TestExeIDFollowsTheRunningFileAcrossAReplacement(t *testing.T) {
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("no sleep on PATH")
	}
	bin := filepath.Join(t.TempDir(), "worker")
	if err := copyExecutable(sleep, bin); err != nil {
		t.Fatal(err)
	}
	first, err := fileIDOf(bin)
	if err != nil {
		t.Fatal(err)
	}
	old := startCopy(t, bin)
	if got, err := exeIDOf(old.Process.Pid); err != nil || got != first {
		t.Fatalf("the process runs %v (%v), want the file it started from, %v", got, err, first)
	}
	second, err := replaceAtomically(sleep, bin)
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatalf("the replacement kept the identity %v: it was no new file", first)
	}
	if got, _ := exeIDOf(old.Process.Pid); got != first || got == second {
		t.Errorf("after the replacement the old process runs %v, want still %v (not the new %v)", got, first, second)
	}
	fresh := startCopy(t, bin)
	if got, _ := exeIDOf(fresh.Process.Pid); got != second {
		t.Errorf("a process started after the replacement runs %v, want the new file %v", got, second)
	}
}

// pinnedSleep starts sleep and pins it by a pidfd.
func pinnedSleep(t *testing.T, sleep string) (*exec.Cmd, *stackPin) {
	t.Helper()
	cmd := startCopy(t, sleep)
	p, err := keeperprobe.OpenPin(t, "sleep", cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	return cmd, p
}

func TestRestartProblems(t *testing.T) {
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("no sleep on PATH")
	}
	gone, gonePin := pinnedSleep(t, sleep)
	_ = gone.Process.Kill()
	_ = gone.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for !gonePin.Exited() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	_, alivePin := pinnedSleep(t, sleep)
	_, newPin := pinnedSleep(t, sleep)
	for _, c := range []struct {
		name       string
		old, fresh hostPins
		want       string
	}{
		{"exited and replaced", hostPins{"front": gonePin}, hostPins{"front": newPin}, ""},
		{"still running", hostPins{"front": alivePin}, hostPins{"front": newPin}, "still running"},
		{"no new process", hostPins{"front": gonePin}, hostPins{}, "no new process"},
		{"the same process after", hostPins{"front": gonePin}, hostPins{"front": gonePin}, "the same process"},
	} {
		got := strings.Join(restartProblems(c.old, c.fresh), "; ")
		if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
			t.Errorf("%s: problems %q, want %q", c.name, got, c.want)
		}
	}
}

func TestUIDProblems(t *testing.T) {
	md := objKey{"ModelDeployment", "default", "md"}
	in := objKey{"Instance", "default", "md-1"}
	before := map[objKey]types.UID{md: "u-md", in: "u-in"}
	for _, c := range []struct {
		name  string
		after map[objKey]types.UID
		want  string
	}{
		{"kept, and one added", map[objKey]types.UID{md: "u-md", in: "u-in", {"Lease", "x", "new"}: "u-l"}, ""},
		{"one gone", map[objKey]types.UID{md: "u-md"}, "Instance default/md-1 is gone"},
		{"one recreated", map[objKey]types.UID{md: "u-md", in: "u-other"}, "Instance default/md-1 has UID u-other, was u-in"},
	} {
		got := strings.Join(uidProblems(before, c.after), "; ")
		if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
			t.Errorf("%s: problems %q, want %q", c.name, got, c.want)
		}
	}
}

// writeBackup makes a backup directory as the launcher's upgrade writes it, changed by edit.
func writeBackup(t *testing.T, edit func(dir string)) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "20261114T030236Z-v1.35.9+k3s1")
	if err := os.MkdirAll(filepath.Join(dir, "db"), 0o700); err != nil {
		t.Fatal(err)
	}
	for f, c := range map[string]string{
		"db/state.db": "SQLite format 3 ... uid-of-the-md ... uid-of-the-instance ...",
		"token":       "K10example::server:token",
		"backup.json": `{"version":"v1.35.9+k3s1","taken_at":"2026-11-14T03:02:36Z","files":["db/state.db","token"]}`,
	} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(c), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if edit != nil {
		edit(dir)
	}
	return dir
}

func TestBackupProblems(t *testing.T) {
	holds := []string{"uid-of-the-md", "uid-of-the-instance"}
	for _, c := range []struct {
		name string
		edit func(dir string)
		want string
	}{
		{"as the upgrade writes it", nil, ""},
		{"no backup.json", func(d string) { _ = os.Remove(filepath.Join(d, "backup.json")) }, "backup.json"},
		{"another release named", func(d string) {
			_ = os.WriteFile(filepath.Join(d, "backup.json"), []byte(`{"version":"v1.36.5+k3s1","files":["db/state.db","token"]}`), 0o600)
		}, `names "v1.36.5+k3s1"`},
		{"a listed file missing", func(d string) { _ = os.Remove(filepath.Join(d, "token")) }, "token"},
		{"a file readable by others", func(d string) { _ = os.Chmod(filepath.Join(d, "token"), 0o644) }, "mode 644"},
		{"no store file", func(d string) {
			_ = os.WriteFile(filepath.Join(d, "backup.json"), []byte(`{"version":"v1.35.9+k3s1","files":["token"]}`), 0o600)
		}, "no store file"},
		{"a store without the objects", func(d string) {
			_ = os.WriteFile(filepath.Join(d, "db", "state.db"), []byte("SQLite format 3 empty"), 0o600)
		}, "holds no uid-of-the-md"},
	} {
		dir := writeBackup(t, c.edit)
		got := strings.Join(backupProblems(dir, "v1.35.9+k3s1", holds), "; ")
		if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
			t.Errorf("%s: problems %q, want one with %q", c.name, got, c.want)
		}
	}
	if got := backupProblems(writeBackup(t, nil)+".partial", "v1.35.9+k3s1", nil); len(got) == 0 || !strings.Contains(got[0], "unfinished") {
		t.Errorf("a .partial directory: problems %v, want it named unfinished", got)
	}
}

func TestNewBackup(t *testing.T) {
	state := t.TempDir()
	for _, n := range []string{"20261113T000000Z-v1.34.9+k3s1", "20261114T030236Z-v1.35.9+k3s1"} {
		if err := os.MkdirAll(filepath.Join(state, "backups", n), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	before := map[string]bool{"20261113T000000Z-v1.34.9+k3s1": true}
	if got, err := newBackup(state, before); err != nil || filepath.Base(got) != "20261114T030236Z-v1.35.9+k3s1" {
		t.Errorf("newBackup: %q, %v; want the one directory before did not list", got, err)
	}
	if _, err := newBackup(state, backupNames(state)); err == nil {
		t.Error("newBackup found a new backup where none was made")
	}
}

// portsAnswered finds a port of the block that accepts a connection, and none in a block where nothing
// listens.
func TestPortsAnswered(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port
	if port < 17 {
		t.Skip("the kernel gave a port too low for a block before it")
	}
	got := portsAnswered(stack.Ports{Base: port - 4})
	if len(got) != 1 || got[0] != port {
		t.Errorf("portsAnswered = %v, want [%d], the one port that listens", got, port)
	}
}
