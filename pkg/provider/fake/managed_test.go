package fake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func rev(s string) *string { return &s }

func entry(name string) provider.CatalogEntry { return provider.CatalogEntry{Name: name} }

func TestManagedListsTheScriptedCatalogAsACopy(t *testing.T) {
	m := NewManaged(NewClock(at(12, 0, 0)))
	ctx := context.Background()
	got, err := m.ListCatalog(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("ListCatalog of an empty gateway = %v, %v, want nothing", got, err)
	}

	in := []provider.CatalogEntry{{Name: "example-a", Revision: rev("r1")}, entry("example-b")}
	m.SetCatalog(in...)
	*in[0].Revision = "changed after SetCatalog" // the gateway holds its own copy
	got, err = m.ListCatalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "example-a" || got[0].Revision == nil || *got[0].Revision != "r1" || got[1].Name != "example-b" || got[1].Revision != nil {
		t.Fatalf("ListCatalog = %+v, want example-a at r1 and example-b with no revision", got)
	}
	*got[0].Revision = "changed after ListCatalog" // and hands out copies
	again, _ := m.ListCatalog(ctx)
	if *again[0].Revision != "r1" {
		t.Fatalf("a caller changed the gateway's catalog: %q", *again[0].Revision)
	}

	m.SetCatalog() // the listing can shrink to nothing
	if got, _ = m.ListCatalog(ctx); len(got) != 0 {
		t.Fatalf("ListCatalog = %+v, want nothing", got)
	}
}

func TestManagedRejectKeyWrapsErrKeyRejected(t *testing.T) {
	m := NewManaged(NewClock(at(12, 0, 0)))
	ctx := context.Background()
	m.SetCatalog(entry("example-a"))
	m.RejectKey(true)
	got, err := m.ListCatalog(ctx)
	if !errors.Is(err, provider.ErrKeyRejected) || got != nil {
		t.Fatalf("ListCatalog = %v, %v, want no catalog and ErrKeyRejected", got, err)
	}
	m.RejectKey(false)
	if got, err = m.ListCatalog(ctx); err != nil || len(got) != 1 {
		t.Fatalf("ListCatalog after the key is accepted = %v, %v, want the catalog", got, err)
	}
}

func TestManagedFailReadsIsAnUnknownErrorThatWinsOverARejectedKey(t *testing.T) {
	m := NewManaged(NewClock(at(12, 0, 0)))
	ctx := context.Background()
	m.SetCatalog(entry("example-a"))
	boom := errors.New("example transport failure")
	m.FailReads(boom)
	if _, err := m.ListCatalog(ctx); !errors.Is(err, boom) || errors.Is(err, provider.ErrKeyRejected) {
		t.Fatalf("ListCatalog = %v, want the unknown error, not a key rejection", err)
	}
	m.RejectKey(true) // a read that fails never reaches the gateway
	if _, err := m.ListCatalog(ctx); !errors.Is(err, boom) || errors.Is(err, provider.ErrKeyRejected) {
		t.Fatalf("ListCatalog with both set = %v, want the unknown error", err)
	}
	m.FailReads(nil)
	if _, err := m.ListCatalog(ctx); !errors.Is(err, provider.ErrKeyRejected) {
		t.Fatalf("ListCatalog after FailReads(nil) = %v, want ErrKeyRejected", err)
	}
}

func TestManagedCallLogCountsEveryReadWhateverItsOutcome(t *testing.T) {
	clock := NewClock(at(12, 0, 0))
	m := NewManaged(clock)
	ctx := context.Background()
	_, _ = m.ListCatalog(ctx)
	clock.Set(at(12, 1, 0))
	m.RejectKey(true)
	_, _ = m.ListCatalog(ctx)
	m.RejectKey(false)
	m.FailReads(errors.New("example failure"))
	_, _ = m.ListCatalog(ctx)
	if n := m.Count("list-catalog"); n != 3 {
		t.Fatalf("list-catalog logged %d times, want 3", n)
	}
	if n := m.Count("other"); n != 0 {
		t.Fatalf("an op that never ran is counted %d times", n)
	}
	calls := m.Calls()
	if len(calls) != 3 || !calls[1].Time.Equal(at(12, 1, 0)) {
		t.Fatalf("calls = %+v, want three, the second at 12:01:00", calls)
	}
	calls[0].Op = "changed" // Calls returns a copy
	if m.Count("list-catalog") != 3 {
		t.Fatal("a caller changed the call log")
	}
}

func TestManagedKeyExpiryIsKeptInUTC(t *testing.T) {
	m := NewManaged(NewClock(at(12, 0, 0)))
	if !m.KeyExpiry().IsZero() {
		t.Fatalf("KeyExpiry = %v, want none", m.KeyExpiry())
	}
	want := time.Date(2026, 11, 2, 12, 0, 0, 0, time.FixedZone("example", 3600))
	m.SetKeyExpiry(want)
	if got := m.KeyExpiry(); !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("KeyExpiry = %v, want %v in UTC", got, want)
	}
}
