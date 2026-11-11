package providerctl

// The delta review of PR 360's round: the races it found, each with a test
// against the socket-level fake keeper (keeper_harness_test.go).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// gatedPlatform is a Platform whose file staging waits, once gated, until the
// test opens the gate, and whose poll interval in force is maxAge.
type gatedPlatform struct {
	*fakePlatform
	maxAge  time.Duration
	gated   *atomic.Bool
	entered chan struct{}
	open    chan struct{}
}

func (p gatedPlatform) ListingMaxAge() time.Duration { return p.maxAge }

func (p gatedPlatform) EnsureFile(ctx context.Context, f provider.File) error {
	if p.gated.Load() {
		wake(p.entered)
		<-p.open
	}
	return p.fakePlatform.EnsureFile(ctx, f)
}

// keeperGone stops the fake keeper and waits until a send has failed since,
// so a set in flight at the stop has recorded its success: the controller's
// clock may then be moved past the validity.
func keeperGone(t *testing.T, e *env, k *fakeKeeper) {
	t.Helper()
	k.stop()
	keeper := e.ctl.shared.relays.keeper
	waitRelays(t, "a send failed after the keeper went", func() bool {
		keeper.mu.Lock()
		defer keeper.mu.Unlock()
		return keeper.lastErr != ""
	})
}

// Admission is read again, as one state, right before the submission: an
// Allocation whose Create passed the first checks while the keeper answered,
// and whose staging then took past the validity while the keeper was gone,
// submits nothing; its attempt is recorded as refused.
func TestKeeperAdmissionIsReadAgainRightBeforeSubmit(t *testing.T) {
	k := startFakeKeeper(t)
	var gated atomic.Bool
	entered, open := make(chan struct{}, 1), make(chan struct{})
	e := newEnv(t, keeperMode(k, 5), func(e *env) {
		p := e.platform
		e.opts.NewPlatform = func(slurm.Validated) (Platform, io.Closer, error) {
			return gatedPlatform{fakePlatform: p, maxAge: 30 * time.Second, gated: &gated, entered: entered, open: open}, io.NopCloser(nil), nil
		}
	})
	keeperUp(e)
	submits := e.count("sbatch")
	gated.Store(true)
	e.newAlloc("alloc-a")
	done := make(chan struct{})
	go func() { e.run("alloc-a", 1); close(done) }()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("Create did not reach the staging")
	}
	keeperGone(t, e, k)
	e.clock.Advance(41 * time.Second) // past the 40 s validity
	if why := e.ctl.shared.relays.keeper.admission(e.clock.Now()); why == "" {
		t.Fatal("the keeper still admits past the validity; the test needs it closed")
	}
	close(open)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the reconcile did not return once the staging went on")
	}
	if got := e.count("sbatch"); got != submits {
		t.Errorf("%d jobs were submitted after the keeper went during the staging", got-submits)
	}
}

// statusRecorder records every Ready condition the Provider's status writes
// carry, in order.
type statusRecorder struct {
	client.Client
	mu    sync.Mutex
	ready []string
}

func (c *statusRecorder) Status() client.SubResourceWriter {
	return recordingWriter{SubResourceWriter: c.Client.Status(), c: c}
}

type recordingWriter struct {
	client.SubResourceWriter
	c *statusRecorder
}

func (w recordingWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	err := w.SubResourceWriter.Update(ctx, obj, opts...)
	if prov, ok := obj.(*v1alpha1.Provider); ok && err == nil {
		for _, c := range prov.Status.Conditions {
			if c.Type == v1alpha1.ConditionProviderReady {
				w.c.mu.Lock()
				w.c.ready = append(w.c.ready, string(c.Status)+"/"+c.Reason)
				w.c.mu.Unlock()
			}
		}
	}
	return err
}

func (c *statusRecorder) writes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.ready...)
}

// A keeper that comes back after an outage with a refusal for the login host:
// its reachability and the claim's decision change together, so from the
// outage on no Ready True is written and no Allocation is admitted, until and
// after Ready is False with LoginHostInUse.
func TestKeeperRecoveryWithALoginHostRefusalNeverAdmits(t *testing.T) {
	k := startFakeKeeper(t)
	var refusing atomic.Bool
	k.script(func(k *fakeKeeper) {
		k.onSet = func(s wire.DesiredSet) (wire.Kind, any) {
			if refusing.Load() {
				return wire.KindRefused, wire.Refused{Reason: wire.RefusedLoginHost, LoginHost: fakeLoginHost, Holder: "older-provider", Message: "held"}
			}
			return wire.KindReport, wire.Report{Provider: s.Provider, KeeperEpoch: 1<<31 + 1, LoginHost: fakeLoginHost}
		}
	})
	var rec *statusRecorder
	e := newEnv(t, keeperMode(k, 5), func(e *env) {
		rec = &statusRecorder{Client: e.c}
		e.c = rec
		e.opts.PollInterval = 10 * time.Minute
		p := e.platform
		e.opts.NewPlatform = func(slurm.Validated) (Platform, io.Closer, error) {
			return maxAgePlatform{fakePlatform: p, maxAge: 30 * time.Second}, io.NopCloser(nil), nil
		}
	})
	keeperUp(e)
	statusCtx, stopStatus := context.WithCancel(context.Background())
	statusDone := make(chan struct{})
	go func() { _ = e.ctl.status.Start(statusCtx); close(statusDone) }()
	t.Cleanup(func() { stopStatus(); <-statusDone })
	waitRelays(t, "Ready True while the keeper answers", func() bool { return providerReady(t, e).Status == "True" })

	refusing.Store(true)
	keeperGone(t, e, k)
	e.clock.Advance(41 * time.Second)
	waitRelays(t, "Ready False once the keeper is gone past the validity", func() bool {
		r := providerReady(t, e)
		return r.Status == "False" && r.Reason == v1alpha1.ReasonRelayKeeperUnavailable
	})
	from := len(rec.writes())
	submits := e.count("sbatch")
	e.newAlloc("alloc-b")
	stopCreating := make(chan struct{})
	creating := make(chan struct{})
	go func() {
		defer close(creating)
		for {
			select {
			case <-stopCreating:
				return
			default:
				e.run("alloc-b", 1)
			}
		}
	}()
	k.start()
	keeper := e.ctl.shared.relays.keeper
	end := time.Now().Add(30 * time.Second)
	for {
		r := providerReady(t, e)
		if r.Status == "False" && r.Reason == v1alpha1.ReasonLoginHostInUse {
			break
		}
		if time.Now().After(end) {
			keeper.mu.Lock()
			state := fmt.Sprintf("unavailable %q, answered %v, inUse %q, okAt %v, now %v, seen %q",
				keeper.unavailableLocked(keeper.now()), keeper.answered, keeper.inUse, keeper.okAt, keeper.now(), keeper.seen)
			keeper.mu.Unlock()
			t.Fatalf("Ready False with LoginHostInUse once the keeper is back and refuses: not within 30 s; Ready %+v; keeper %s; Ready writes %v", r, state, rec.writes()[from:])
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(stopCreating)
	<-creating
	for _, w := range rec.writes()[from:] {
		if w[:4] == "True" {
			t.Errorf("Ready was written True between the outage and the refusal: %v", rec.writes()[from:])
			break
		}
	}
	if got := e.count("sbatch"); got != submits {
		t.Errorf("%d jobs were submitted while the keeper came back refusing the login host", got-submits)
	}
}

// A Provider found replaced is fenced at once: right after the read that
// finds the new UID, while this copy's lead is still live, an exchange sends
// nothing. The positive control: before the re-creation the same exchange is
// sent.
func TestKeeperAReplacedProviderFencesItsExchangesAtOnce(t *testing.T) {
	k := startFakeKeeper(t)
	e := newEnv(t, keeperMode(k, 5))
	r := e.ctl.shared.relays
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r.startLead(ctx)
	lead := r.leadContext()
	if _, _, err := e.ctl.shared.keeperProvider(ctx); err != nil {
		t.Fatal(err)
	}
	ack := wire.Ack{Provider: testProvider, LeaseEpoch: 5, UID: "uid-gone", JobID: "1", Channel: agentapi.RelayControl,
		Seq: 1, Outcome: wire.OutcomeRefused, Reason: wire.AckDeleted}
	var acked wire.Acked
	if err := r.keeper.exchange(lead, wire.KindAck, ack, &acked); err != nil || len(k.ackList()) != 1 {
		t.Fatalf("the positive control: an exchange before the re-creation returned %v and sent %d acks, want 1", err, len(k.ackList()))
	}

	var prov v1alpha1.Provider
	if err := e.c.Get(e.ctx, client.ObjectKey{Name: testProvider}, &prov); err != nil {
		t.Fatal(err)
	}
	if err := e.c.Delete(e.ctx, &prov); err != nil {
		t.Fatal(err)
	}
	if err := e.c.Create(e.ctx, &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: testProvider, UID: "uid-example-provider-again",
		CreationTimestamp: metav1.NewTime(at(11, 30, 0))}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.ctl.shared.keeperProvider(ctx); !errors.Is(err, errProviderReplaced) {
		t.Fatalf("the read after the re-creation returned %v; want errProviderReplaced", err)
	}
	if lead.Err() != nil {
		t.Fatal("the lead ended; the test needs it live")
	}
	if err := r.keeper.exchange(lead, wire.KindAck, ack, &acked); !errors.Is(err, errLeadEnded) {
		t.Errorf("an exchange right after the replacement was found returned %v; want errLeadEnded", err)
	}
	if got := len(k.ackList()); got != 1 {
		t.Errorf("%d acks were sent after the replacement was found", got-1)
	}
}

// The validity's deadline wakes the status loop by itself (the delta review of
// PR 360): with a keeper that went quiet and no send to fail (the send
// interval and the poll are both 10 min here), Ready turns False with
// RelayKeeperUnavailable once the validity has passed, by the deadline's
// timer alone. The validity is 300 ms (100 ms in force plus a 200 ms renew
// deadline), and the controller's clock is moved past it.
func TestKeeperReadyFallsAtTheValidityDeadlineWithNoExchange(t *testing.T) {
	k := startFakeKeeper(t)
	e := newEnv(t, keeperMode(k, 5), func(e *env) {
		e.opts.PollInterval = 10 * time.Minute
		e.opts.Relays.KeeperSendInterval = 10 * time.Minute
		e.opts.Relays.RenewDeadline = 200 * time.Millisecond
		p := e.platform
		e.opts.NewPlatform = func(slurm.Validated) (Platform, io.Closer, error) {
			return maxAgePlatform{fakePlatform: p, maxAge: 100 * time.Millisecond}, io.NopCloser(nil), nil
		}
	})
	keeperUp(e)
	statusCtx, stopStatus := context.WithCancel(context.Background())
	statusDone := make(chan struct{})
	go func() { _ = e.ctl.status.Start(statusCtx); close(statusDone) }()
	t.Cleanup(func() { stopStatus(); <-statusDone })
	waitRelays(t, "Ready True while the keeper answers", func() bool { return providerReady(t, e).Status == "True" })
	k.stop()
	e.clock.Advance(time.Second) // past the 300 ms validity
	waitRelays(t, "Ready False at the validity's deadline", func() bool {
		r := providerReady(t, e)
		return r.Status == "False" && r.Reason == v1alpha1.ReasonRelayKeeperUnavailable
	})
}
