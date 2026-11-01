package providerctl

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
)

// countingLock counts the reads the elector makes through a lock.
type countingLock struct {
	resourcelock.Interface
	gets atomic.Int64
}

func (c *countingLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	c.gets.Add(1)
	return c.Interface.Get(ctx)
}

// A Provider manager leads through leaderlock's lock on its own Lease, so a
// release that a late renew made fail is retried (issue #131). The manager
// that NewProviderManager builds is started against an API server that
// cannot be reached: its elector tries to take the Lease at once, and every
// try reads it through the manager's lock. The test sees those reads in the
// lock that newLeaseLock returned, so a manager that led through
// controller-runtime's own lock instead (no LeaderElectionResourceLockInterface)
// fails it (PR #159 review).
func TestProviderManagerLeadsThroughLeaderlocksLock(t *testing.T) {
	var built *countingLock
	var builtName string
	var builtRenew time.Duration
	real := newLeaseLock
	t.Cleanup(func() { newLeaseLock = real })
	newLeaseLock = func(cfg *rest.Config, ns, name string, renew time.Duration, log logr.Logger) (resourcelock.Interface, error) {
		inner, err := real(cfg, ns, name, renew, log)
		if err != nil {
			return nil, err
		}
		if !leaderlock.RetriesRelease(inner) {
			t.Errorf("leaderlock.New returned a %T, not its retrying lock", inner)
		}
		built, builtName, builtRenew = &countingLock{Interface: inner}, ns+"/"+name, renew
		return built, nil
	}
	mgr, err := NewProviderManager(&rest.Config{Host: "https://127.0.0.1:1"}, runtime.NewScheme(), "helios-a",
		ManagerOptions{RenewDeadline: 3 * time.Second, RetryPeriod: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if want := v1alpha1.ControllerNamespace + "/" + v1alpha1.ProviderLeaseName("helios-a"); builtName != want || builtRenew != 3*time.Second {
		t.Fatalf("the lock was built for %s with a renew deadline of %v; want %s and 3s", builtName, builtRenew, want)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("the manager did not stop within 30 s of its cancel")
		}
	}()
	deadline := time.Now().Add(30 * time.Second)
	for built.gets.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the manager's elector made no read through the lock newLeaseLock built within 30 s: it leads through another lock")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
