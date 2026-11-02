package lifecycle

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

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

// afterAWholeGrace is when a stop whose runnables used the lifecycle
// manager's whole shutdown grace would end, with the release taking took.
func afterAWholeGrace(took time.Duration) time.Duration { return ShutdownGrace + took }

// startLifecycleLeader starts a lifecycle manager built from o on a marked
// context and waits, with a generous deadline, until it leads and the
// lifecycle Lease names it; a manager whose Lease another holder still holds
// waits until that expires. It returns the holder, the manager's cancel and
// the channel of Start's result, and stops the manager when the test ends if
// the test has not.
func startLifecycleLeader(t *testing.T, ctx context.Context, cl client.Client, o Options) (string, func(), <-chan error) {
	t.Helper()
	mgr, _, err := NewManager(o)
	if err != nil {
		t.Fatal(err)
	}
	mctx, stop := context.WithCancel(leasefault.Mark(ctx))
	done := make(chan error, 1)
	go func() { done <- mgr.Start(mctx) }()
	t.Cleanup(stop)
	before, err := leaseHolderOf(ctx, cl)
	if err != nil {
		before = ""
	}
	elected := mgr.Elected()
	var holder string
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
		holder = h
		return h != "" && h != before, "the Lease names " + h
	})
	return holder, stop, done
}
