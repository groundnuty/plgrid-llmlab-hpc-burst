package lifecycle

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock/leasefault"
)

// TestLifecycleLeaseReleaseUnderLatency covers the lifecycle Lease's whole
// two-phase release against an API server that answers the Lease's lock only
// after a latency, with the elector's first release write answered by a
// conflict, as a late renew makes it conflict (issue #131), under one shared
// ReleaseBudget (`@hpc-burst-science-agent`, 2 Nov 2026, review of PR #215:
// B2).
//   - At 600 ms the retry's read and write land 1.3 s after the conflict,
//     within the budget: the Lease is free when Start returns, and no release
//     follows Start.
//   - At 1 s the retry's write would end 2.1 s after the conflict: the budget
//     cuts it before it is sent, nothing is released after the budget, the
//     release after Start finds no budget left and is skipped, and the Lease
//     expires: the next manager takes it after its lease duration.
//
// In both, the release, from the elector's release read to Start's return,
// ends inside the supervisor's grace even after a whole shutdown grace, and
// so inside down's 40 s (launcher.SupervisorGrace; TestControllerStopBudget).
// At 1 s it can pass the controller's own kill grace (launcher.
// ControllerTermGrace, 34 s): its first attempt, three requests, is not in
// the budget, and the supervisor's SIGKILL then ends the stop, as the kill
// grace's comment says.
func TestLifecycleLeaseReleaseUnderLatency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cfg, cl := leaseControlPlane(t, ctx)
	creds, builds := t.TempDir(), t.TempDir()
	for _, tc := range []struct {
		name     string
		latency  time.Duration
		released bool
	}{
		{"600ms: the retry lands within the budget", 600 * time.Millisecond, true},
		{"1s: the budget cuts the retry and the Lease expires", time.Second, false},
	} {
		if !t.Run(tc.name, func(t *testing.T) {
			releaseUnderLatency(t, ctx, cl, cfg, Options{
				CredentialsDir: creds, AgentBuildsDir: builds, LeaderElection: true,
				Checkpoints: &stubSource{}, Registry: prometheus.NewRegistry(),
			}, tc.latency, tc.released)
		}) {
			return
		}
	}
}

// releaseUnderLatency runs one case of TestLifecycleLeaseReleaseUnderLatency.
func releaseUnderLatency(t *testing.T, ctx context.Context, cl client.Client, cfg *rest.Config, o Options, latency time.Duration, wantReleased bool) {
	t.Helper()
	f := leasefault.New(v1alpha1.ControllerNamespace, v1alpha1.LeaseLifecycle)
	o.Config = f.Config(cfg)
	holder, stop, done := startLifecycleLeader(t, ctx, cl, o)
	f.SetDelay(latency)
	f.ConflictNextReleaseWrite()
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the manager returned %v", err)
		}
	case <-time.After(time.Minute):
		t.Fatalf("the manager did not return within a minute of its cancel; goroutines:\n%s", goroutines())
	}
	returned := time.Now()
	reqs := f.Requests()
	read, conflict := -1, -1
	for i, r := range reqs {
		switch {
		case read < 0 && r.Method == http.MethodGet && !r.Marked:
			read = i
		case r.Conflict && conflict >= 0:
			t.Fatalf("two writes were answered with the injected conflict (requests %+v)", reqs)
		case r.Conflict:
			conflict = i
		}
	}
	if read < 0 || conflict < read {
		t.Fatalf("want the elector's release read, then its write answered with the injected conflict (requests %+v)", reqs)
	}
	if n := f.MarkedAfterReleaseRead(); n != 0 {
		t.Fatalf("%d requests came after the elector's release read from a release after Start; want none: the elector's release landed, or left no budget (requests %+v)", n, reqs)
	}
	budgetEnd := reqs[conflict].Done.Add(leaderlock.ReleaseBudget)
	took := returned.Sub(reqs[read].At)
	got, err := leaseHolderOf(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if wantReleased {
		if got != "" {
			t.Fatalf("at %s the Lease is still held by %q when Start has returned; want the retry to release it within the budget", latency, got)
		}
		landed := false
		for _, r := range reqs[conflict+1:] {
			if r.Method == http.MethodPut && r.Sent && r.Status == http.StatusOK && !r.Done.After(budgetEnd) {
				landed = true
			}
		}
		if !landed {
			t.Fatalf("no release write landed within %s of the conflict (requests %+v)", leaderlock.ReleaseBudget, reqs)
		}
	} else {
		if got != holder {
			t.Fatalf("at %s the Lease's holder is %q when Start has returned; want the stopped manager, %q, until it expires", latency, got, holder)
		}
		cut := false
		for _, r := range reqs[conflict+1:] {
			if r.Sent && r.At.After(budgetEnd) {
				t.Fatalf("a %s was sent %s after the shared budget ended (requests %+v)", r.Method, r.At.Sub(budgetEnd), reqs)
			}
			cut = cut || (!r.Sent && !r.Conflict)
		}
		if !cut {
			t.Fatalf("no request after the conflict was cut before it was sent; this case needs the budget to cut the retry (requests %+v)", reqs)
		}
	}
	if stopAt := afterAWholeGrace(took); stopAt >= launcher.SupervisorGrace {
		t.Fatalf("the release took %s: after a whole shutdown grace the stop would end at %s, past the supervisor's grace %s and down's", took, stopAt, launcher.SupervisorGrace)
	}
	if !wantReleased {
		f.SetDelay(0)
		next, nextStop, nextDone := startLifecycleLeader(t, ctx, cl, Options{
			Config: cfg, CredentialsDir: o.CredentialsDir, AgentBuildsDir: o.AgentBuildsDir, LeaderElection: true,
			Checkpoints: &stubSource{}, Registry: prometheus.NewRegistry(),
		})
		if next == holder {
			t.Fatalf("the next manager has the stopped one's identity %q", next)
		}
		nextStop()
		if err := <-nextDone; err != nil {
			t.Fatalf("the next manager returned %v", err)
		}
	}
}

// TestStartLifecycleLeaderOfAManagerThatLeadsBeforeTheLeaseIsRead (issue #252):
// a manager that takes a free Lease at once must pass startLifecycleLeader's
// wait, however late the helper first reads the Lease. The helper reads the
// Lease through a client whose reads are held until the manager holds it, as
// they land when a fast election wins the race against the helper's first
// read. A helper that took "the holder before the manager started" from such a
// read would take the manager's own identity and wait for the Lease to change
// to the end of its deadline; this one waits for the Lease to name the manager.
func TestStartLifecycleLeaderOfAManagerThatLeadsBeforeTheLeaseIsRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cfg, cl := leaseControlPlane(t, ctx)
	if h, err := leaseHolderOf(ctx, cl); err == nil && h != "" {
		t.Fatalf("the Lease is held by %q before the manager starts; the test needs a free one", h)
	}
	released := make(chan struct{})
	go func() {
		// The release is a watch on the Lease through the client that is not
		// held: the manager has led once the Lease names a holder.
		for ctx.Err() == nil {
			if h, err := leaseHolderOf(ctx, cl); err == nil && h != "" {
				close(released)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	held := &heldLeaseReads{Client: cl, until: released}
	holder, stop, done := startLifecycleLeader(t, ctx, held, Options{
		Config: cfg, CredentialsDir: t.TempDir(), AgentBuildsDir: t.TempDir(), LeaderElection: true,
		Checkpoints: &stubSource{}, Registry: prometheus.NewRegistry(),
	})
	select {
	case <-released:
	default:
		t.Fatal("the helper returned before the manager held the Lease")
	}
	if got, err := leaseHolderOf(ctx, cl); err != nil || got != holder {
		t.Fatalf("the helper returned %q as the holder; the Lease names %q (error %v)", holder, got, err)
	}
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the manager returned %v", err)
		}
	case <-time.After(time.Minute):
		t.Fatalf("the manager did not return within a minute of its cancel; goroutines:\n%s", goroutines())
	}
}

// heldLeaseReads is a client whose reads wait until until is closed (or their
// own context ends), and then go through.
type heldLeaseReads struct {
	client.Client
	until <-chan struct{}
}

func (h *heldLeaseReads) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	select {
	case <-h.until:
	case <-ctx.Done():
		return ctx.Err()
	}
	return h.Client.Get(ctx, key, obj, opts...)
}

// afterAWholeGrace is when a stop whose runnables used the lifecycle
// manager's whole shutdown grace would end, with the release taking took.
func afterAWholeGrace(took time.Duration) time.Duration { return ShutdownGrace + took }

// startLifecycleLeader starts a lifecycle manager built from o on a marked
// context and waits, with a generous deadline, until it leads and the
// lifecycle Lease names it: the Lease's holder is the identity of the lock the
// manager was built with, so the wait asks whose Lease it is, never whether the
// Lease has changed since some earlier read (issue #252: a read taken after the
// manager started could already name the new manager, and the wait then never
// ended). A manager whose Lease another holder still holds waits until that
// expires. It returns the holder (the manager's identity), the manager's cancel
// and the channel of Start's result, and stops the manager when the test ends
// if the test has not.
func startLifecycleLeader(t *testing.T, ctx context.Context, cl client.Client, o Options) (string, func(), <-chan error) {
	t.Helper()
	mgr, identity, err := newManagerWithIdentity(o)
	if err != nil {
		t.Fatal(err)
	}
	mctx, stop := context.WithCancel(leasefault.Mark(ctx))
	done := make(chan error, 1)
	go func() { done <- mgr.Start(mctx) }()
	t.Cleanup(stop)
	elected := mgr.Elected()
	eventually(t, 2*time.Minute, "the manager leads and holds the lifecycle Lease", func() (bool, string) {
		select {
		case <-elected:
		default:
			return false, "not elected"
		}
		h, err := leaseHolderOf(ctx, cl)
		if err != nil {
			return false, err.Error()
		}
		return h == identity, "the Lease names " + h + ", want " + identity
	})
	return identity, stop, done
}

// newManagerWithIdentity builds the lifecycle manager from o, which must have
// leader election on, and returns the identity of the Lease lock it was built
// with: the holder it writes into the Lease. It wraps newLeaseLock only while
// NewManager builds the lock.
func newManagerWithIdentity(o Options) (manager.Manager, string, error) {
	var identity string
	realLock := newLeaseLock
	defer func() { newLeaseLock = realLock }()
	newLeaseLock = func(cfg *rest.Config, ns, name string, renew time.Duration, log logr.Logger) (resourcelock.Interface, error) {
		l, err := realLock(cfg, ns, name, renew, log)
		if err == nil {
			identity = l.Identity()
		}
		return l, err
	}
	mgr, _, err := NewManager(o)
	if err == nil && identity == "" {
		err = errors.New("NewManager built no lifecycle Lease lock: the options need LeaderElection")
	}
	return mgr, identity, err
}
