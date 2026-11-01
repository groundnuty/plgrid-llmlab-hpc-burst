package lifecycle

import (
	"testing"
	"time"

	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
)

// The lifecycle Lease's gapLock wraps leaderlock's lock, so a release that a
// late renew made fail is retried (issue #131).
func TestGapLockRetriesItsRelease(t *testing.T) {
	l, err := newGapLock(&rest.Config{Host: "https://127.0.0.1:1"}, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !leaderlock.RetriesRelease(l.Interface) {
		t.Fatalf("the lifecycle Lease's lock wraps a %T, not leaderlock's", l.Interface)
	}
	if got, want := l.Describe(), v1alpha1.ControllerNamespace+"/"+v1alpha1.LeaseLifecycle; got != want {
		t.Fatalf("the lock is for %s, want %s", got, want)
	}
}
