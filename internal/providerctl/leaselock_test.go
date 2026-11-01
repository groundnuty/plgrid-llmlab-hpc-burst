package providerctl

import (
	"testing"

	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
)

// A Provider manager leads through leaderlock's lock on its own Lease, so a
// release that a late renew made fail is retried (issue #131).
func TestProviderLeaseLockRetriesItsRelease(t *testing.T) {
	lock, err := providerLeaseLock(&rest.Config{Host: "https://127.0.0.1:1"}, "helios-a", ManagerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !leaderlock.RetriesRelease(lock) {
		t.Fatalf("the Provider's Lease lock is a %T, not leaderlock's", lock)
	}
	if got, want := lock.Describe(), v1alpha1.ControllerNamespace+"/"+v1alpha1.ProviderLeaseName("helios-a"); got != want {
		t.Fatalf("the lock is for %s, want %s", got, want)
	}
}
