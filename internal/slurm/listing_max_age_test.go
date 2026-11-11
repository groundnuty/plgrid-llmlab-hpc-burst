package slurm

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The poll interval in force, which the relay keeper's sets carry in their
// validity (#343's Q5): the poll interval, and the busy interval while any
// tag is busy (HBEP-4, Transport, Poll rate).
func TestListingMaxAgeIsThePollIntervalInForce(t *testing.T) {
	b := NewBatch(nil, provider.Profile{}, provider.Settings{}, Options{PollInterval: 30 * time.Second, BusyInterval: 5 * time.Second})
	if got := b.ListingMaxAge(); got != 30*time.Second {
		t.Fatalf("with no busy tag: %v, want 30s", got)
	}
	b.SetBusy("hb-example-000000000001", true)
	if got := b.ListingMaxAge(); got != 5*time.Second {
		t.Errorf("with a busy tag: %v, want 5s", got)
	}
	b.SetBusy("hb-example-000000000001", false)
	if got := b.ListingMaxAge(); got != 30*time.Second {
		t.Errorf("once no tag is busy again: %v, want 30s", got)
	}
}
