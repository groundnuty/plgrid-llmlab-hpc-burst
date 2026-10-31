package fake

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

var _ provider.Managed = (*Managed)(nil)

// Managed is a managed gateway's catalog, scripted by a test, that implements
// provider.Managed. It is safe for concurrent use. A read never changes it: the
// catalog is what the test last set, whatever the clock says. Every
// ListCatalog call is in the call log, op "list-catalog", with no arguments.
//
// A read has one of three outcomes, in this order: FailReads makes it fail with
// an unknown error (a read that fails never reaches the gateway, so a rejected
// key does not show); RejectKey makes it fail with an error that wraps
// provider.ErrKeyRejected, as a 401 or 403 does; otherwise it returns a copy
// of the catalog.
type Managed struct {
	clock *Clock

	mu          sync.Mutex
	catalog     []provider.CatalogEntry
	keyRejected bool
	readErr     error
	keyExpiry   time.Time
	calls       []Call
}

// NewManaged returns a managed gateway with an empty catalog that reads clock.
func NewManaged(clock *Clock) *Managed { return &Managed{clock: clock} }

// SetCatalog replaces the catalog the gateway lists.
func (m *Managed) SetCatalog(entries ...provider.CatalogEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.catalog = copyEntries(entries)
}

// RejectKey makes the gateway answer the catalog read as it does for a key it
// refuses, or, with false, accept the key again.
func (m *Managed) RejectKey(on bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keyRejected = on
}

// FailReads makes ListCatalog fail with err, an unknown error (it wraps no
// error kind of package provider); nil clears it.
func (m *Managed) FailReads(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readErr = err
}

// SetKeyExpiry sets when the key expires, as the Provider's controller knows
// it from the key's metadata. The zero time says it has no expiry. ListCatalog
// does not read it; ManagedController does.
func (m *Managed) SetKeyExpiry(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keyExpiry = t.UTC()
}

// KeyExpiry returns the key's expiry; the zero time when there is none.
func (m *Managed) KeyExpiry() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.keyExpiry
}

// ListCatalog implements provider.Managed.
func (m *Managed) ListCatalog(_ context.Context) ([]provider.CatalogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, Call{Op: "list-catalog", Time: m.clock.Now()})
	switch {
	case m.readErr != nil:
		return nil, m.readErr
	case m.keyRejected:
		return nil, fmt.Errorf("catalog read answered 401: %w", provider.ErrKeyRejected)
	}
	return copyEntries(m.catalog), nil
}

// Calls returns a copy of the call log.
func (m *Managed) Calls() []Call {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Call(nil), m.calls...)
}

// Count returns how many calls the log holds for op ("list-catalog").
func (m *Managed) Count(op string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, c := range m.calls {
		if c.Op == op {
			n++
		}
	}
	return n
}

// copyEntries returns entries with every revision copied, so a caller and the
// gateway never share a pointer.
func copyEntries(entries []provider.CatalogEntry) []provider.CatalogEntry {
	out := make([]provider.CatalogEntry, len(entries))
	for i, e := range entries {
		out[i] = provider.CatalogEntry{Name: e.Name}
		if e.Revision != nil {
			r := *e.Revision
			out[i].Revision = &r
		}
	}
	return out
}
