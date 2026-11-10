package launcher

// The rest of the launcher's part of the relay keeper (relaykeeper.go; #343, PR 5): the fake
// controller's --relay-keeper-socket under the real controller's rule, up's refusals before
// anything starts, and restore's clear of the keeper's accepted Lease epochs.

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The fake controller takes --relay-keeper-socket as the real one does (#343, PR 4): with
// --front-mode host-dialed only, an absolute path of at most agentapi.MaxRelaySocketPath bytes;
// any other use is a bad command line, exit 2. The accepted form is TestTheSupervisorRunsTheKeeper-
// BesideTheController's, whose controller would exit 2 on it if it were refused.
func TestTheFakeControllerTakesTheKeepersSocketAsTheRealOneDoes(t *testing.T) {
	bin := fakeControllerBin(t)
	base := []string{"--state-dir", t.TempDir(), "--port-base", "30034", "--health-addr", "127.0.0.1:30040"}
	for name, extra := range map[string][]string{
		"without a front mode":      {"--relay-keeper-socket", "/state/example/relaykeeper.sock"},
		"in direct mode":            {"--front-mode", "direct", "--front-addr", "front.example.test:8443", "--relay-keeper-socket", "/state/example/relaykeeper.sock"},
		"a relative path":           {"--front-mode", "host-dialed", "--relay-keeper-socket", "relaykeeper.sock"},
		"a path over the agent API": {"--front-mode", "host-dialed", "--relay-keeper-socket", "/" + strings.Repeat("s", agentapi.MaxRelaySocketPath)},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bin, append(append([]string{}, base...), extra...)...)
			cmd.Env = []string{"FAKECTRL_DIR=" + t.TempDir()}
			err := cmd.Run()
			var ee *exec.ExitError
			if !errors.As(err, &ee) || ee.ExitCode() != 2 {
				t.Fatalf("the fake controller with %q: got %v, want exit status 2", extra, err)
			}
		})
	}
}

// up refuses --relay-keeper outside host-dialed mode, and with no controller binary, before
// anything starts: no record, no process (HBEP-6, Test plan: "--relay-keeper without --front-mode
// host-dialed is refused").
func TestUpRefusesTheKeeperOutsideHostDialed(t *testing.T) {
	for _, c := range []struct {
		name  string
		o     UpOptions
		want  string
		isErr error
	}{
		{"no front mode", UpOptions{RelayKeeper: true, ControllerBin: fakeControllerBin(t)}, "host-dialed only", errRelayKeeperMode},
		{"direct mode", UpOptions{RelayKeeper: true, ControllerBin: fakeControllerBin(t), Front: Front{Mode: "direct", Addr: "front.example.test:8443"}}, "host-dialed only", errRelayKeeperMode},
		{"no controller binary", UpOptions{RelayKeeper: true, ControllerBin: "", Front: Front{Mode: string(agentapi.DialHomeHostDialed)}}, "no controller binary", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.o.ControllerBin == "" {
				exe, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(filepath.Join(filepath.Dir(exe), ControllerBinName)); err == nil {
					// hack/test-launcher.sh fails a run with any skipped test: a precondition, not a skip.
					t.Fatal("a controller binary sits beside the test binary, so up would find one")
				}
			}
			l, err := newLayout(filepath.Join(shortDir(t), "st"))
			if err != nil {
				t.Fatal(err)
			}
			c.o.StateDir, c.o.PortBase = l.dir, 30020
			_, err = Up(context.Background(), c.o)
			if err == nil || !strings.Contains(err.Error(), c.want) || (c.isErr != nil && !errors.Is(err, c.isErr)) {
				t.Fatalf("up: %v; want refused, naming %q", err, c.want)
			}
			if _, err := os.Stat(l.stateFile()); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("a record after the refusal: %v", err)
			}
		})
	}
}

// restore clears the keeper's accepted Lease epochs and keeps the rest of its state file, and with
// a keeper running on the state directory (one started by hand: it holds the keeper's process
// lock) it refuses before the store changes.
func TestRestoreClearsTheKeepersFences(t *testing.T) {
	l := storeLayout(t)
	putStore(t, l, "v1", "tok1", "")
	backup, err := backupStore(l, "v1.35.9+k3s1")
	if err != nil {
		t.Fatal(err)
	}
	putStore(t, l, "v2", "tok2", "")
	if err := l.saveStoreVersion("v1.36.5+k3s1", "upgrade"); err != nil {
		t.Fatal(err)
	}
	path := relaykeeper.StatePath(l.dir)
	ks, err := relaykeeper.OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	ks.Providers = map[string]relaykeeper.ProviderFence{"example-a": {HighestEpoch: 7}}
	if err := relaykeeper.SaveState(path, ks); err != nil {
		t.Fatal(err)
	}

	t.Run("refused while a keeper holds its lock", func(t *testing.T) {
		f, err := os.OpenFile(relaykeeper.LockPath(l.dir), os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := unix.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			t.Fatal(err)
		}
		_, err = Restore(RestoreOptions{StateDir: l.dir, Backup: backup})
		if err == nil || !strings.Contains(err.Error(), "another relay keeper holds") {
			t.Fatalf("restore with the keeper's lock held: %v; want refused, naming the lock", err)
		}
		if fileText(t, l.storeDB()) != "v2" {
			t.Error("the store changed though the restore was refused")
		}
		if got, _ := relaykeeper.LoadState(path); got == nil || got.Providers["example-a"].HighestEpoch != 7 {
			t.Errorf("the keeper's fence after the refusal: %+v, want it kept", got)
		}
	})

	rep, err := Restore(RestoreOptions{StateDir: l.dir, Backup: backup})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.KeeperFencesCleared || fileText(t, l.storeDB()) != "v1" {
		t.Errorf("restore: %+v; want the store restored and the keeper's fences cleared", rep)
	}
	got, err := relaykeeper.LoadState(path)
	if err != nil || len(got.Providers) != 0 || got.Starts != 1 {
		t.Errorf("the keeper's state file after restore: %+v, %v; want no fence, its one start kept", got, err)
	}
}

// restore on a host that never ran a keeper has no keeper file to clear, and makes none.
func TestRestoreWithoutAKeeperMakesNoKeeperFile(t *testing.T) {
	l := storeLayout(t)
	putStore(t, l, "v1", "tok1", "")
	backup, err := backupStore(l, "v1.35.9+k3s1")
	if err != nil {
		t.Fatal(err)
	}
	rep, err := Restore(RestoreOptions{StateDir: l.dir, Backup: backup})
	if err != nil {
		t.Fatal(err)
	}
	if rep.KeeperFencesCleared {
		t.Error("restore says it cleared a keeper's fences on a host with no keeper")
	}
	if _, err := os.Stat(agentapi.Host{StateDir: l.dir}.RelayKeeperDir()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the keeper's directory after restore: %v, want none made", err)
	}
}

// down's removal of the launcher's own record (clearRecord) keeps the keeper's directory, its
// state file and its lock: the switch-back guard reads that file after down (HBEP-6, Compatibility
// and upgrades: "The launcher's own record doesn't survive down, but that file does").
func TestDownsRecordRemovalKeepsTheKeepersStateFile(t *testing.T) {
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(l.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := l.saveState(&State{Version: "v1.36.5+k3s1", RelayKeeper: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := relaykeeper.OpenState(relaykeeper.StatePath(l.dir)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(relaykeeper.LockPath(l.dir), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(relaykeeper.StatePath(l.dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.clearRecord(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(l.stateFile()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the launcher's record after clearRecord: %v, want it removed", err)
	}
	if after, err := os.ReadFile(relaykeeper.StatePath(l.dir)); err != nil || string(after) != string(before) {
		t.Errorf("the keeper's state file after clearRecord: %v, %q; want it kept as it was", err, after)
	}
	if _, err := os.Stat(relaykeeper.LockPath(l.dir)); err != nil {
		t.Errorf("the keeper's lock file after clearRecord: %v, want it kept", err)
	}
	if err := writeRetiredMarker(l, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := l.clearRecord(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(retiredMarker(l)); err != nil {
		t.Errorf("the keeper retired marker after clearRecord: %v, want it kept", err)
	}
}
