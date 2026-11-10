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

// A Provider's first set resolves its login host with ssh -G outside the
// keeper's lock, so a slow ssh -G stalls only that set: while it runs, ping
// and status, which take the lock, still answer. Here the double's ssh -G
// waits until the test lets it go (the review of PR 356).
func TestSshGRunsOutsideTheKeepersLock(t *testing.T) {
	dir := t.TempDir()
	release := filepath.Join(dir, "release")
	ssh := filepath.Join(dir, "ssh")
	script := "#!/bin/sh\ncase \"$1\" in -G) while [ ! -e " + release + " ]; do sleep 0.05; done; echo hostname login.example; exit 0;; esac\nexec sleep 300 </dev/null\n"
	if err := os.WriteFile(ssh, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
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
