package providerctl

// The keeper-mode harness (#343, PR 4): a socket-level FAKE relay keeper,
// served by the wire package's own server on a Unix socket the test makes,
// and an env option that points the controller at it. The fake records every
// request it decoded, checks each one with the wire package's own validation,
// and answers what the test scripts. These are tests of the controller's
// logic; the real keeper process and real controller processes are Stage B's.

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// exampleProviderUID is the Provider's UID in keeper mode's tests: the fake
// client gives objects none of their own.
const exampleProviderUID = "uid-example-provider"

// fakeKeeper is a relay keeper's socket, answered by the test.
type fakeKeeper struct {
	t    *testing.T
	path string

	mu   sync.Mutex
	sets []wire.DesiredSet
	acks []wire.Ack
	// onSet and onAck answer a request; nil answers a set with an accepted
	// Report naming loginHost, and an ack with Acked{Decided: true}.
	onSet func(wire.DesiredSet) (wire.Kind, any)
	onAck func(wire.Ack) (wire.Kind, any)
	// ends are reported in every accepted set's Report until an ack names
	// them, as the keeper does; keepAcked keeps reporting them anyway, as a
	// keeper whose acknowledgement was lost would.
	ends      []wire.FinalEnd
	keepAcked bool
	stopServe func()
}

// fakeLoginHost is the login host the fake names.
const fakeLoginHost = "login.example.org"

func startFakeKeeper(t *testing.T) *fakeKeeper {
	t.Helper()
	k := newFakeKeeper(t)
	k.start()
	return k
}

// newFakeKeeper is a fake keeper whose socket's path is chosen and not
// listened on yet: a keeper that is not there.
func newFakeKeeper(t *testing.T) *fakeKeeper {
	t.Helper()
	dir, err := os.MkdirTemp("", "pk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return &fakeKeeper{t: t, path: filepath.Join(dir, "k.sock")}
}

// start listens on the fake's socket and serves it until stop or the test's
// end.
func (k *fakeKeeper) start() {
	k.t.Helper()
	ln, err := wire.Listen(k.path)
	if err != nil {
		k.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = wire.Serve(ctx, ln, k.handle, nil); close(done) }()
	var once sync.Once
	stop := func() { once.Do(func() { cancel(); <-done }) }
	k.mu.Lock()
	k.stopServe = stop
	k.mu.Unlock()
	k.t.Cleanup(stop)
}

// stop closes the fake's socket, which removes it: the keeper is gone.
func (k *fakeKeeper) stop() {
	k.mu.Lock()
	stop := k.stopServe
	k.mu.Unlock()
	if stop != nil {
		stop()
	}
	if _, err := os.Stat(k.path); err == nil {
		k.t.Fatalf("the fake keeper's socket is still at its path after the stop")
	}
}

func (k *fakeKeeper) handle(_ context.Context, req wire.Envelope) (wire.Kind, any) {
	switch req.Kind {
	case wire.KindSet:
		var s wire.DesiredSet
		if err := req.Decode(&s); err != nil {
			k.t.Errorf("the fake keeper could not decode a set: %v", err)
			return wire.KindRefused, wire.Refused{Reason: wire.RefusedMalformed, Message: err.Error()}
		}
		if err := s.Validate(); err != nil {
			k.t.Errorf("the controller sent a set the keeper refuses as malformed: %v", err)
		}
		k.mu.Lock()
		k.sets = append(k.sets, s)
		on, ends := k.onSet, append([]wire.FinalEnd(nil), k.ends...)
		k.mu.Unlock()
		if on != nil {
			kind, body := on(s)
			if rep, ok := body.(wire.Report); ok && kind == wire.KindReport && rep.Ends == nil {
				rep.Ends = ends
				body = rep
			}
			return kind, body
		}
		return wire.KindReport, wire.Report{Provider: s.Provider, KeeperEpoch: 1<<31 + 1, LoginHost: fakeLoginHost, Ends: ends}
	case wire.KindAck:
		var a wire.Ack
		if err := req.Decode(&a); err != nil {
			k.t.Errorf("the fake keeper could not decode an ack: %v", err)
			return wire.KindRefused, wire.Refused{Reason: wire.RefusedMalformed, Message: err.Error()}
		}
		if err := a.Validate(); err != nil {
			k.t.Errorf("the controller sent an ack the keeper refuses as malformed: %v", err)
		}
		k.mu.Lock()
		k.acks = append(k.acks, a)
		on := k.onAck
		if !k.keepAcked {
			kept := k.ends[:0:0]
			for _, e := range k.ends {
				if e.UID != a.UID || e.JobID != a.JobID || e.Channel != a.Channel || e.Epoch != a.Epoch || e.Seq != a.Seq {
					kept = append(kept, e)
				}
			}
			k.ends = kept
		}
		k.mu.Unlock()
		if on != nil {
			return on(a)
		}
		return wire.KindAcked, wire.Acked{Provider: a.Provider, Decided: true}
	}
	return wire.KindRefused, wire.Refused{Reason: wire.RefusedMalformed, Message: "not served by the fake"}
}

func (k *fakeKeeper) script(f func(k *fakeKeeper)) {
	k.mu.Lock()
	defer k.mu.Unlock()
	f(k)
}

func (k *fakeKeeper) setCount() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.sets)
}

func (k *fakeKeeper) requests() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.sets) + len(k.acks)
}

func (k *fakeKeeper) ackList() []wire.Ack {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]wire.Ack(nil), k.acks...)
}

// waitSet waits for a set received after the first after sets that pred
// accepts, and returns it.
func (k *fakeKeeper) waitSet(t *testing.T, what string, after int, pred func(wire.DesiredSet) bool) wire.DesiredSet {
	t.Helper()
	end := time.Now().Add(30 * time.Second)
	for {
		k.mu.Lock()
		for i := after; i < len(k.sets); i++ {
			if pred(k.sets[i]) {
				s := k.sets[i]
				k.mu.Unlock()
				return s
			}
		}
		k.mu.Unlock()
		if time.Now().After(end) {
			t.Fatalf("%s: no such set within 30 s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitAcks waits until pred accepts the acks received.
func (k *fakeKeeper) waitAcks(t *testing.T, what string, pred func([]wire.Ack) bool) []wire.Ack {
	t.Helper()
	end := time.Now().Add(30 * time.Second)
	for {
		acks := k.ackList()
		if pred(acks) {
			return acks
		}
		if time.Now().After(end) {
			t.Fatalf("%s: not within 30 s; the acks were %+v", what, acks)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// keeperMode makes the env's controller host-dialed in keeper mode, against
// the fake k, with the Lease epoch epoch (a test that checks the epoch sets
// Epoch nil and wires a real fence instead).
func keeperMode(k *fakeKeeper, epoch int64) envOption {
	return func(e *env) {
		front := exampleFront(e.t, e.buildDir)
		front.Mode, front.Endpoint = agentapi.DialHomeHostDialed, ""
		e.opts.Front = front
		e.opts.Relays = &RelaySettings{
			Ports:        map[agentapi.RelayChannel]int{agentapi.RelayControl: 1, agentapi.RelayServing: 2},
			KeeperSocket: k.path, KeeperSendInterval: 20 * time.Millisecond,
			Epoch: func(context.Context) (int64, error) { return epoch, nil },
		}
		var prov v1alpha1.Provider
		if err := e.c.Get(e.ctx, client.ObjectKey{Name: testProvider}, &prov); err != nil {
			e.t.Fatal(err)
		}
		prov.UID = types.UID(exampleProviderUID)
		if err := e.c.Update(e.ctx, &prov); err != nil {
			e.t.Fatal(err)
		}
	}
}

// runKeeperClient runs the controller's relay runnable, in keeper mode its
// sender and acknowledger, until the test ends, and returns its stop.
func runKeeperClient(e *env) (stop func()) {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = e.ctl.RunRelays(ctx); close(done) }()
	// Observations count for a set's completeness only under the lead, so
	// the test's next observation must come after the lead began.
	k := e.ctl.shared.relays.keeper
	waitRelays(e.t, "the keeper client's lead began", func() bool {
		k.mu.Lock()
		defer k.mu.Unlock()
		return k.done != nil
	})
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				e.t.Error("the keeper client did not stop")
			}
		})
	}
	e.t.Cleanup(stop)
	return stop
}

// keeperUp reads the Provider's status once, so its platform and listing
// exist, starts e's keeper client and waits until an exchange with the keeper
// has succeeded under its lead, so the Provider admits Allocations again
// (RelayKeeperUnavailable).
func keeperUp(e *env) (stop func()) {
	e.t.Helper()
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		e.t.Fatal(err)
	}
	stop = runKeeperClient(e)
	k := e.ctl.shared.relays.keeper
	waitRelays(e.t, "an exchange with the keeper succeeded under the lead", func() bool { return k.unavailable(e.clock.Now()) == "" })
	return stop
}

// running runs a new Allocation to Running and returns it.
func (e *env) running(name string) *v1alpha1.Allocation {
	e.t.Helper()
	e.newAlloc(name)
	e.run(name, 4)
	a := e.get(name)
	if a.Status.AtProvider.Phase != v1alpha1.AllocationRunning || a.Status.AtProvider.JobID == "" {
		e.t.Fatalf("%s is %s with job %q, want Running", name, a.Status.AtProvider.Phase, a.Status.AtProvider.JobID)
	}
	return a
}

// entryOf is the set's entry for tag.
func entryOf(s wire.DesiredSet, tag string) (wire.Entry, bool) {
	for _, en := range s.Allocations {
		if en.Tag == tag {
			return en, true
		}
	}
	return wire.Entry{}, false
}
