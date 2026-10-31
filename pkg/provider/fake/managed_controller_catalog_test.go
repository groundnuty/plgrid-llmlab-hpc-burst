package fake

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The tests of the managed fake's bounds on status.catalog and the events a
// read raises (HBEP-4, Managed provider in alpha; @hpc-burst-science-agent's
// ruling of 31 Oct 2026). Every expected name, count, time and event line is
// written out by hand, never produced by provider.MergeCatalog or the note
// functions of package provider, which the fake calls.

// withRecorder gives the environment's controller an event recorder, and
// returns it.
func (e *menv) withRecorder() *events.FakeRecorder {
	rec := events.NewFakeRecorder(64)
	e.ctl.Recorder = rec
	return rec
}

// drainEvents returns the events recorded since the last call, as the fake
// recorder formats them: "<type> <reason> <note>".
func drainEvents(rec *events.FakeRecorder) []string {
	var out []string
	for {
		select {
		case ev := <-rec.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func TestManagedSkipsAnEntryTheStatusCannotHold(t *testing.T) {
	long := strings.Repeat("n", 254)
	for _, tc := range []struct {
		name  string
		bad   provider.CatalogEntry
		event string
	}{
		{"an entry with no name", entry(""),
			`Warning CatalogEntrySkipped catalog entry "" skipped: an empty name`},
		{"a name above 253 characters", entry(long),
			`Warning CatalogEntrySkipped catalog entry "` + strings.Repeat("n", 64) + `..." skipped: a name of 254 characters, above 253`},
		{"a revision above 256 characters", provider.CatalogEntry{Name: "example-a", Revision: rev(strings.Repeat("r", 257))},
			`Warning CatalogEntrySkipped catalog entry "example-a" skipped: a revision of 257 characters, above 256`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newManagedEnv(t)
			rec := e.withRecorder()
			e.gw.SetCatalog(entry("example-ok"))
			e.syncAt(at(12, 0, 0))
			if ev := drainEvents(rec); len(ev) != 0 {
				t.Fatalf("events after a clean read = %q, want none", ev)
			}

			e.gw.SetCatalog(entry("example-fine"), tc.bad)
			e.syncAt(at(12, 1, 0))
			// The rest of the listing is recorded, and the read counts.
			if got := e.names(); !slices.Equal(got, []string{"example-fine", "example-ok"}) {
				t.Fatalf("catalog = %v, want [example-fine example-ok]: the entry that cannot be held is skipped, the rest recorded", got)
			}
			wantEntry(t, e.entry("example-fine"), true, "", at(12, 1, 0))
			wantEntry(t, e.entry("example-ok"), false, "", at(12, 0, 0))
			prov := e.provider()
			wantMT(t, "lastCatalogRead", prov.Status.LastCatalogRead, at(12, 1, 0))
			wantMT(t, "lastSuccessfulStatusRead", prov.Status.LastSuccessfulStatusRead, at(12, 1, 0))
			e.wantReady(true, v1alpha1.ReasonCatalogRead, "")
			e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionFalse, v1alpha1.ReasonKeyAccepted)
			// One event, naming the entry.
			if ev := drainEvents(rec); !slices.Equal(ev, []string{tc.event}) {
				t.Fatalf("events = %q, want exactly [%q]", ev, tc.event)
			}
		})
	}

	t.Run("no recorder is no events and no panic", func(t *testing.T) {
		e := newManagedEnv(t)
		e.gw.SetCatalog(entry("example-fine"), entry(""))
		e.syncAt(at(12, 0, 0))
		if got := e.names(); !slices.Equal(got, []string{"example-fine"}) {
			t.Fatalf("catalog = %v, want [example-fine]", got)
		}
	})

	t.Run("each skipped entry has its own event", func(t *testing.T) {
		e := newManagedEnv(t)
		rec := e.withRecorder()
		e.gw.SetCatalog(entry(""), entry("example-fine"), entry(long), provider.CatalogEntry{Name: "example-a", Revision: rev(strings.Repeat("r", 257))})
		e.syncAt(at(12, 0, 0))
		want := []string{
			`Warning CatalogEntrySkipped catalog entry "" skipped: an empty name`,
			`Warning CatalogEntrySkipped catalog entry "` + strings.Repeat("n", 64) + `..." skipped: a name of 254 characters, above 253`,
			`Warning CatalogEntrySkipped catalog entry "example-a" skipped: a revision of 257 characters, above 256`,
		}
		if ev := drainEvents(rec); !slices.Equal(ev, want) {
			t.Fatalf("events = %q, want %q", ev, want)
		}
		// The same listing raises the same events on the next read.
		e.syncAt(at(12, 1, 0))
		if ev := drainEvents(rec); !slices.Equal(ev, want) {
			t.Fatalf("events on the next read = %q, want %q", ev, want)
		}
	})
}

func TestManagedCatalogHoldsAtMost1024Entries(t *testing.T) {
	// base is 1022 names always listed; the other entries come and go.
	var base []provider.CatalogEntry
	for i := range 1022 {
		base = append(base, entry(fmt.Sprintf("example-%04d", i)))
	}
	with := func(extra ...string) []provider.CatalogEntry {
		out := slices.Clone(base)
		for _, n := range extra {
			out = append(out, entry(n))
		}
		return out
	}
	// listed counts the catalog entries with listed true.
	listed := func(e *menv) int {
		n := 0
		for _, ce := range e.provider().Status.Catalog {
			if ce.Listed {
				n++
			}
		}
		return n
	}

	e := newManagedEnv(t)
	rec := e.withRecorder()
	// Two entries that will leave the listing. "example-zz-oldest" is last
	// listed at 12:00 and "example-aa-newer" at 12:01, so that pruning by time
	// and pruning by name disagree.
	e.gw.SetCatalog(with("example-zz-oldest")...) // 1023 entries
	e.syncAt(at(12, 0, 0))
	e.gw.SetCatalog(with("example-aa-newer")...) // 1024 entries: zz-oldest unlisted
	e.syncAt(at(12, 1, 0))
	e.gw.SetCatalog(base...) // 1024 entries: both unlisted, 1022 listed
	e.syncAt(at(12, 2, 0))
	if n := len(e.provider().Status.Catalog); n != 1024 {
		t.Fatalf("%d entries, want 1024: they fit", n)
	}
	wantEntry(t, e.entry("example-zz-oldest"), false, "", at(12, 0, 0))
	wantEntry(t, e.entry("example-aa-newer"), false, "", at(12, 1, 0))
	if n := listed(e); n != 1022 {
		t.Fatalf("%d listed entries, want 1022", n)
	}

	// A 1025th name: the unlisted entry with the oldest lastListedTime goes,
	// not the one that sorts first by name, and never a listed one.
	e.gw.SetCatalog(with("example-new-1")...)
	e.syncAt(at(12, 3, 0))
	got := e.names()
	if len(got) != 1024 {
		t.Fatalf("%d entries, want 1024", len(got))
	}
	if slices.Contains(got, "example-zz-oldest") || !slices.Contains(got, "example-aa-newer") || !slices.Contains(got, "example-new-1") {
		t.Fatalf("catalog has zz-oldest %v, aa-newer %v, new-1 %v, want only the oldest unlisted entry pruned",
			slices.Contains(got, "example-zz-oldest"), slices.Contains(got, "example-aa-newer"), slices.Contains(got, "example-new-1"))
	}
	if n := listed(e); n != 1023 {
		t.Fatalf("%d listed entries, want 1023: all of the listing", n)
	}
	wantEntry(t, e.entry("example-aa-newer"), false, "", at(12, 1, 0)) // as it was
	wantEntry(t, e.entry("example-new-1"), true, "", at(12, 3, 0))
	wantEntry(t, e.entry("example-0000"), true, "", at(12, 3, 0))
	wantEntry(t, e.entry("example-1021"), true, "", at(12, 3, 0))
	e.wantReady(true, v1alpha1.ReasonCatalogRead, "")
	if ev := drainEvents(rec); len(ev) != 0 {
		t.Fatalf("events = %q, want none: pruning an unlisted entry raises none", ev)
	}

	// A second new name takes the last unlisted entry's room.
	e.gw.SetCatalog(with("example-new-1", "example-new-2")...) // 1024 listed
	e.syncAt(at(12, 4, 0))
	got = e.names()
	if len(got) != 1024 || slices.Contains(got, "example-aa-newer") || listed(e) != 1024 {
		t.Fatalf("%d entries, %d listed, aa-newer kept %v, want 1024 all listed and aa-newer gone",
			len(got), listed(e), slices.Contains(got, "example-aa-newer"))
	}
	e.wantReady(true, v1alpha1.ReasonCatalogRead, "")
}

func TestManagedListingAbove1024KeepsTheFirst1024AndRaisesTruncated(t *testing.T) {
	e := newManagedEnv(t)
	rec := e.withRecorder()
	var listing []provider.CatalogEntry
	for i := 1029; i >= 0; i-- { // 1030 names, listed in descending order
		listing = append(listing, entry(fmt.Sprintf("example-%04d", i)))
	}
	e.gw.SetCatalog(listing...)
	want := "Warning CatalogTruncated the gateway listed 6 entries more than status.catalog holds (1024); the first 1024 by name are kept"

	for i, when := range []struct{ h, m, s int }{{12, 0, 0}, {12, 1, 0}} {
		e.syncAt(at(when.h, when.m, when.s))
		got := e.names()
		if len(got) != 1024 || got[0] != "example-0000" || got[1023] != "example-1023" {
			t.Fatalf("read %d: %d entries from %s to %s, want 1024 from example-0000 to example-1023", i+1, len(got), got[0], got[len(got)-1])
		}
		wantEntry(t, e.entry("example-1023"), true, "", at(when.h, when.m, when.s))
		// Ready stays True, and the read counts.
		e.wantReady(true, v1alpha1.ReasonCatalogRead, "")
		wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, at(when.h, when.m, when.s))
		if ev := drainEvents(rec); !slices.Equal(ev, []string{want}) {
			t.Fatalf("read %d: events = %q, want exactly [%q]", i+1, ev, want)
		}
	}

	// The listing shrinks to fit: no more events.
	e.gw.SetCatalog(listing[6:]...) // 1024 names, example-0000 to example-1023
	e.syncAt(at(12, 2, 0))
	if ev := drainEvents(rec); len(ev) != 0 {
		t.Fatalf("events = %q, want none once the listing fits", ev)
	}
}
