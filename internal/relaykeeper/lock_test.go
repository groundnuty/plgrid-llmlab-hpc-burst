package relaykeeper

// New for the review of PR 356: the keeper's process lock.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A second Run on the state directory of a running one fails before it
// reads or writes the state file, naming the lock: the file is byte for
// byte the first's. The lock is an flock on an open file of its own, so the
// two Runs conflict within one process as two processes would. (The
// subcommand's test shows it with two keeper processes.)
func TestASecondRunOnALiveKeepersStateDirChangesNothing(t *testing.T) {
	dir, err := os.MkdirTemp("", "rkl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	ports := map[agentapi.RelayChannel]int{agentapi.RelayControl: 30002, agentapi.RelayServing: 30003}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan int64, 1)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, ProcessConfig{StateDir: dir, Ports: ports, Ready: func(e int64, _ string) { ready <- e }})
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("the first Run ended: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the first Run was not ready within 30 s")
	}
	before, err := os.ReadFile(StatePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	err = Run(context.Background(), ProcessConfig{StateDir: dir, Ports: ports})
	if err == nil || !strings.Contains(err.Error(), "another relay keeper holds") {
		t.Errorf("the second Run: %v, want refused, naming the process lock", err)
	}
	if after, err := os.ReadFile(StatePath(dir)); err != nil || !bytes.Equal(after, before) {
		t.Errorf("the state file after the second Run: %v, %q; want it unchanged, %q", err, after, before)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("the first Run: %v", err)
	}
}

// cleaner is what hangingSSHG needs of a test: a *testing.T, or the hand-run
// double of lock_leak_test.go.
type cleaner interface {
	Helper()
	TempDir() string
	Cleanup(func())
	Fatalf(format string, args ...any)
}

// hangingSSHG makes a test double ssh whose ssh -G waits until the file
// release appears in its directory, and answers then; any other call of it is
// a sleep. It writes "running" in the directory when its ssh -G starts to wait.
//
// The double must never outlive its test (issue 402: one whose test had failed
// ran for 22 h 40 min, since the test wrote release only on its success path).
// Two things end it. A cleanup registered right after TempDir writes release;
// cleanups run last in, first out, so it runs before TempDir's removal of the
// directory, on every exit of the test. And its loop also ends when the
// directory is gone, so a test that never reaches the cleanup, or a cleanup
// that fails to write, still frees it.
func hangingSSHG(t cleaner) (dir, ssh, release string) {
	t.Helper()
	dir = t.TempDir()
	if strings.Contains(dir, "'") {
		t.Fatalf("the test's directory holds a quote: %q", dir)
	}
	release = filepath.Join(dir, "release")
	t.Cleanup(func() {
		// Best effort: the double's loop also ends when the directory is gone.
		_ = os.WriteFile(release, nil, 0o600)
	})
	ssh = filepath.Join(dir, "ssh")
	script := "#!/bin/sh\ncase \"$1\" in -G) : > '" + dir + "/running'; while [ ! -e '" + release + "' ] && [ -d '" + dir + "' ]; do sleep 0.05; done; echo hostname login.example; exit 0;; esac\nexec sleep 300 </dev/null\n"
	if err := os.WriteFile(ssh, []byte(script), 0o700); err != nil {
		t.Fatalf("the double ssh: %v", err)
	}
	return dir, ssh, release
}

// A Provider's first set resolves its login host with ssh -G outside the
// keeper's lock, so a slow ssh -G stalls only that set: while it runs, ping
// and status, which take the lock, still answer. Here the double's ssh -G
// waits until the test lets it go (the review of PR 356).
func TestSshGRunsOutsideTheKeepersLock(t *testing.T) {
	dir, ssh, release := hangingSSHG(t)
	r := newRig(t)
	var runner *OwnerRunner
	k, _ := r.open(func(o *Options) {
		o.Runner = func(e int64) (Runner, error) {
			var err error
			runner, err = NewOwnerRunner(context.Background(), OwnerRunnerOptions{Epoch: e,
				Ports:  map[agentapi.RelayChannel]int{agentapi.RelayControl: 30002, agentapi.RelayServing: 30003},
				SSHDir: filepath.Join(dir, "d"), SSHBinary: ssh})
			return runner, err
		}
	})
	t.Cleanup(func() { runner.Stop() })
	setDone := make(chan wire.Kind, 1)
	go func() {
		kind, _ := send(t, k, wire.KindSet, set("example-a", 1, true, "login.example", entry(1)))
		setDone <- kind
	}()
	eventuallyQ(t, 30*time.Second, "the set's ssh -G running", func() bool {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		return runner.provs["example-a"] != nil
	})
	for _, kind := range []wire.Kind{wire.KindPing, wire.KindStatus} {
		answered := make(chan wire.Kind, 1)
		go func() { got, _ := send(t, k, kind, struct{}{}); answered <- got }()
		select {
		case got := <-answered:
			if got == wire.KindRefused {
				t.Errorf("the %s while ssh -G runs was refused", kind)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("the %s did not answer within 10 s while a set's ssh -G runs: the keeper's lock is held across it", kind)
		}
	}
	select {
	case kind := <-setDone:
		t.Fatalf("the set was answered %s before its ssh -G returned", kind)
	default:
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case kind := <-setDone:
		if kind != wire.KindReport {
			t.Errorf("the set after its ssh -G: %s, want a report", kind)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the set did not finish within 30 s of its ssh -G returning")
	}
}

// The keeper's places are pkg/agentapi's, unchanged in bytes from PR 3: the
// launcher and the controller name the same files (#343, PR 5).
func TestTheKeepersPlacesAreAgentAPIs(t *testing.T) {
	const dir = "/state/example"
	h := agentapi.Host{StateDir: dir}
	for _, c := range []struct{ got, want string }{
		{SocketPath(dir), "/state/example/relaykeeper.sock"},
		{SocketPath(dir), h.RelayKeeperSocket()},
		{StatePath(dir), "/state/example/relaykeeper/state.json"},
		{SSHDir(dir), "/state/example/relaykeeper/ssh"},
		{LockPath(dir), "/state/example/relaykeeper/lock"},
		{h.RelayKeeperDir(), "/state/example/relaykeeper"},
	} {
		if c.got != c.want {
			t.Errorf("%q, want %q", c.got, c.want)
		}
	}
}

// ClearLeaseEpochs, the launcher's restore's, takes the keeper's process
// lock first: with a keeper running on the state directory it refuses,
// naming the lock, and the state file is unchanged; once the keeper has
// stopped it clears the fences and keeps the start count (#343, PR 5).
func TestClearLeaseEpochsRefusesWhileAKeeperRuns(t *testing.T) {
	dir, err := os.MkdirTemp("", "rkc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	ports := map[agentapi.RelayChannel]int{agentapi.RelayControl: 30002, agentapi.RelayServing: 30003}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan int64, 1)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, ProcessConfig{StateDir: dir, Ports: ports, Ready: func(e int64, _ string) { ready <- e }})
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("the keeper ended: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the keeper was not ready within 30 s")
	}
	// A fence on disk, as a set would leave it, written with the keeper's
	// lock held by the keeper: SaveState itself takes no lock.
	s, err := LoadState(StatePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	s.Providers = map[string]ProviderFence{"example-a": {HighestEpoch: 7}}
	if err := SaveState(StatePath(dir), s); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(StatePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := ClearLeaseEpochs(StatePath(dir)); err == nil || !strings.Contains(err.Error(), "another relay keeper holds") {
		t.Errorf("ClearLeaseEpochs with a keeper running: %v, want refused, naming the process lock", err)
	}
	if after, _ := os.ReadFile(StatePath(dir)); !bytes.Equal(after, before) {
		t.Errorf("the state file after the refused clear: %q, want it unchanged", after)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("the keeper: %v", err)
	}
	if err := ClearLeaseEpochs(StatePath(dir)); err != nil {
		t.Fatalf("ClearLeaseEpochs with no keeper: %v", err)
	}
	got, err := LoadState(StatePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Providers) != 0 || got.Starts != 1 {
		t.Errorf("after the clear: providers %+v, starts %d; want no fence and the one start kept", got.Providers, got.Starts)
	}
}
