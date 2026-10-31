package fake

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The tests of the managed fake's events: their form, and the rate rule for
// the ones that repeat (HBEP-8, Events: Form and Repeating conditions). Every
// expected type, action, reason, count and time is written out by hand, never
// taken from package provider's constants or functions, which the fake calls.
// The real controller (internal/managed) has the same tests.

// sentEvent is one Eventf call with every argument kept. client-go's
// FakeRecorder drops the action, which is what these tests look at.
type sentEvent struct {
	Type, Reason, Action, Note string
}

type captureRecorder struct {
	mu  sync.Mutex
	evs []sentEvent
}

func (r *captureRecorder) Eventf(_, _ runtime.Object, eventtype, reason, action, note string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, sentEvent{eventtype, reason, action, fmt.Sprintf(note, args...)})
}

// take returns the events sent since the last call.
func (r *captureRecorder) take() []sentEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.evs
	r.evs = nil
	return out
}

func (e *menv) withCapture() *captureRecorder {
	rec := &captureRecorder{}
	e.ctl.Recorder = rec
	return rec
}

// truncatedListing is 1030 names, 6 more than status.catalog holds.
func truncatedListing() []provider.CatalogEntry {
	var l []provider.CatalogEntry
	for i := range 1030 {
		l = append(l, entry(fmt.Sprintf("example-%04d", i)))
	}
	return l
}

const wantTruncatedNote = "the gateway listed 6 entries more than status.catalog holds (1024); the first 1024 by name are kept"

func TestManagedEventsHaveTheirTypeInActionTheCodeAsReasonAndAShortNote(t *testing.T) {
	e := newManagedEnv(t)
	rec := e.withCapture()
	e.gw.SetCatalog(append(truncatedListing(), entry(""), provider.CatalogEntry{Name: "example-rev", Revision: rev(strings.Repeat("r", 257))})...)
	e.syncAt(at(12, 0, 0))
	got := rec.take()
	want := []sentEvent{
		{"Warning", "CatalogEntrySkipped", "degraded", `catalog entry "" skipped: an empty name`},
		{"Warning", "CatalogEntrySkipped", "degraded", `catalog entry "example-rev" skipped: a revision of 257 characters, above 256`},
		{"Warning", "CatalogTruncated", "degraded", wantTruncatedNote},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("events = %+v, want %+v", got, want)
	}
	for _, ev := range got {
		if len(ev.Note) > 1024 {
			t.Fatalf("event %s has a note of %d bytes, want at most 1024", ev.Reason, len(ev.Note))
		}
		if ev.Action == "ReadCatalog" || strings.Contains(ev.Reason, "/") {
			t.Fatalf("event %+v keeps the old action or a type/code reason", ev)
		}
	}
}

func TestManagedTruncationOfThreeHoursGivesThreeEvents(t *testing.T) {
	e := newManagedEnv(t)
	rec := e.withCapture()
	e.gw.SetCatalog(truncatedListing()...)
	want := sentEvent{"Warning", "CatalogTruncated", "degraded", wantTruncatedNote}

	// One read every 15 minutes from 12:00 to 14:45. (Reads of a 1030-entry
	// listing are slow under -race; the minute-by-minute run of the same rule
	// is package provider's TestRepeatGateTruncatedFiresOnStartThenHourly.)
	var firedAt []string
	for m := 0; m < 180; m += 15 {
		when := at(12, 0, 0).Add(time.Duration(m) * time.Minute)
		e.syncAt(when)
		if evs := rec.take(); len(evs) > 0 {
			if len(evs) != 1 || evs[0] != want {
				t.Fatalf("%s: events = %+v, want [%+v]", when.Format("15:04"), evs, want)
			}
			firedAt = append(firedAt, when.Format("15:04"))
		}
	}
	if !slices.Equal(firedAt, []string{"12:00", "13:00", "14:00"}) {
		t.Fatalf("CatalogTruncated fired at %v, want [12:00 13:00 14:00]", firedAt)
	}

	// It fits at 15:00, truncates again at 15:10: a new start fires at once.
	e.gw.SetCatalog(truncatedListing()[:1024]...)
	e.syncAt(at(15, 0, 0))
	if evs := rec.take(); len(evs) != 0 {
		t.Fatalf("15:00, the listing fits: events = %+v, want none", evs)
	}
	e.gw.SetCatalog(truncatedListing()...)
	e.syncAt(at(15, 10, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{want}) {
		t.Fatalf("15:10, a new truncation: events = %+v, want [%+v] at once", evs, want)
	}
	for _, when := range []time.Time{at(15, 11, 0), at(15, 30, 0), at(16, 9, 59)} { // inside the hour from 15:10
		e.syncAt(when)
		if evs := rec.take(); len(evs) != 0 {
			t.Fatalf("%s: events = %+v, want none", when.Format("15:04:05"), evs)
		}
	}
	e.syncAt(at(16, 10, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{want}) {
		t.Fatalf("16:10: events = %+v, want one, an hour after 15:10", evs)
	}
}

func TestManagedSkippedEntryFiresOnStartThenHourly(t *testing.T) {
	e := newManagedEnv(t)
	rec := e.withCapture()
	e.gw.SetCatalog(entry("example-ok"), entry(""))
	want := sentEvent{"Warning", "CatalogEntrySkipped", "degraded", `catalog entry "" skipped: an empty name`}

	e.syncAt(at(12, 0, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{want}) {
		t.Fatalf("12:00: events = %+v, want [%+v]", evs, want)
	}
	e.syncAt(at(12, 1, 0))
	e.syncAt(at(12, 59, 0))
	if evs := rec.take(); len(evs) != 0 {
		t.Fatalf("12:01 and 12:59: events = %+v, want none", evs)
	}
	e.syncAt(at(13, 0, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{want}) {
		t.Fatalf("13:00: events = %+v, want [%+v]", evs, want)
	}

	// The entry is fixed at 13:10 and skipped again at 13:20: it fires at once.
	e.gw.SetCatalog(entry("example-ok"))
	e.syncAt(at(13, 10, 0))
	e.gw.SetCatalog(entry("example-ok"), entry(""))
	e.syncAt(at(13, 20, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{want}) {
		t.Fatalf("13:20: events = %+v, want [%+v] at once", evs, want)
	}
}

// A read that fails says nothing about the listing, so it leaves the rate
// state alone: a truncation interrupted by a failed read is the same
// condition, not a new start.
func TestManagedFailedReadLeavesTheRepeatStateAlone(t *testing.T) {
	e := newManagedEnv(t)
	rec := e.withCapture()
	e.gw.SetCatalog(truncatedListing()...)
	e.syncAt(at(12, 0, 0))
	if len(rec.take()) != 1 {
		t.Fatal("12:00: the start did not fire")
	}
	e.gw.FailReads(errors.New("example: the gateway is down"))
	e.syncAt(at(12, 10, 0))
	e.gw.FailReads(nil)
	e.gw.RejectKey(true)
	e.syncAt(at(12, 20, 0))
	e.gw.RejectKey(false)
	if evs := rec.take(); len(evs) != 0 {
		t.Fatalf("failed reads: events = %+v, want none: this fake raises none for them", evs)
	}
	e.syncAt(at(12, 30, 0)) // truncated again, 30 minutes after the event
	if evs := rec.take(); len(evs) != 0 {
		t.Fatalf("12:30: events = %+v, want none: the failed reads did not end the condition", evs)
	}
	e.syncAt(at(13, 0, 0))
	if evs := rec.take(); len(evs) != 1 {
		t.Fatalf("13:00: events = %+v, want one", evs)
	}
}
