package relaykeeper

// New for PR 3, from #359's mutant run: the real Runner keeps one instance
// per running relay. A Want of the same identity, which every set repeats,
// changes nothing: hostdial's own Want ignores an equal target, so the
// relays run on, and the Runner must keep listing them once, not file them
// under a new instance and keep the old as stopping, which would list each
// twice and make the gate's 4-relay count wrong. The positive control is a
// Want of a new generation, which replaces the relays.

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// runningJob is a listing that shows e's job running under its tag.
type runningJob struct{ e wire.Entry }

func (j runningJob) ListByTag(_ context.Context, tag string) ([]provider.Job, error) {
	if tag != j.e.Tag {
		return nil, nil
	}
	return []provider.Job{{ID: j.e.JobID, Name: tag, Phase: v1alpha1.AllocationRunning}}, nil
}

// identityRunner is a real Runner whose relays' "ssh" is a shell double that
// answers ssh -G (sshrun.Relay.Login) with the login host, and as a relay
// keeps its standard output open and sends nothing, so a started relay stays
// up until hostdial's preamble limit (30 s), far longer than the tests'
// sub-second sequences: a run slower than that fails, never passes falsely.
// The relay's stop ends it through its pidfd. ops counts the worker's Wants
// and Drops.
func identityRunner(t *testing.T, ops *atomic.Int64) *OwnerRunner {
	t.Helper()
	dir := t.TempDir()
	ssh := filepath.Join(dir, "ssh")
	if err := os.WriteFile(ssh, []byte("#!/bin/sh\ncase \"$1\" in -G) echo hostname login.example; exit 0;; esac\nexec sleep 300 </dev/null\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	r, err := NewOwnerRunner(context.Background(), OwnerRunnerOptions{Epoch: EpochBase + 1,
		Ports:  map[agentapi.RelayChannel]int{agentapi.RelayControl: 30002, agentapi.RelayServing: 30003},
		SSHDir: filepath.Join(dir, "d"), SSHBinary: ssh,
		MinBackoff: 50 * time.Millisecond, MaxBackoff: 200 * time.Millisecond, StopGrace: 2 * time.Second,
		beforeOp: func(string, string) { ops.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Stop)
	return r
}

// identityWant is a Want of e whose gate allows every start.
func identityWant(e wire.Entry) Want {
	return Want{Entry: e, ProviderUID: "0a1b2c3d-0000-4000-8000-000000000001", Jobs: runningJob{e},
		Finals:      func() map[agentapi.RelayChannel]string { return nil },
		BeforeStart: func(agentapi.RelayChannel, int64, int64) bool { return true },
		OnStart:     func(agentapi.RelayChannel, int64, int64) {}, StartFailed: func(agentapi.RelayChannel) {},
		OnFinal: nil}
}

// upRelays is the Runner's relays of the Provider that are up, by channel,
// and how many it lists in all.
func upRelays(r *OwnerRunner) (map[agentapi.RelayChannel]wire.RelayState, int) {
	all := r.Relays("example-a")
	up := map[agentapi.RelayChannel]wire.RelayState{}
	for _, s := range all {
		if s.State == wire.StateUp {
			up[s.Channel] = s
		}
	}
	return up, len(all)
}

func TestAWantOfTheSameIdentityKeepsEachRelayListedOnce(t *testing.T) {
	var ops atomic.Int64
	r := identityRunner(t, &ops)
	ssh := wire.SSH{HostName: "login.example", User: "example-user", IdentityFile: "/keys/example"}
	e := entry(1)
	r.Want("example-a", ssh, identityWant(e))
	var first map[agentapi.RelayChannel]wire.RelayState
	eventuallyQ(t, 30*time.Second, "both relays up", func() bool {
		up, _ := upRelays(r)
		first = up
		return len(up) == 2
	})
	for i := range 5 {
		r.Want("example-a", ssh, identityWant(e))
		n := int64(2 + i)
		eventuallyQ(t, 30*time.Second, "the worker's Want", func() bool { return ops.Load() >= n })
	}
	eventuallyQ(t, 30*time.Second, "the worker idle", func() bool { return r.prov("example-a").queue.pending() == 0 })
	up, n := upRelays(r)
	if n != 2 || len(up) != 2 {
		t.Fatalf("after five Wants of the same identity the Runner lists %d relays, %d up: want each of the two once", n, len(up))
	}
	for ch, s := range up {
		if s.PID != first[ch].PID {
			t.Errorf("the %s relay's process changed, %d to %d: a Want of the same identity restarted it", ch, first[ch].PID, s.PID)
		}
	}
	p := r.prov("example-a")
	p.mu.Lock()
	stopping := len(p.stopping)
	p.mu.Unlock()
	if stopping != 0 {
		t.Errorf("%d instances kept as stopping after Wants of the same identity, want none", stopping)
	}

	// The positive control: a new generation replaces the relays. The old
	// ones are listed as stopping until they are gone, then the new ones
	// alone, each once.
	e2 := e
	e2.Gen = "2b3c4d5e6f7a8b9c"
	r.Want("example-a", ssh, identityWant(e2))
	eventuallyQ(t, 30*time.Second, "the new generation's relays up, the old ones gone", func() bool {
		up, n := upRelays(r)
		if n != 2 || len(up) != 2 {
			return false
		}
		for ch, s := range up {
			if s.PID == first[ch].PID {
				return false
			}
		}
		return true
	})
}
