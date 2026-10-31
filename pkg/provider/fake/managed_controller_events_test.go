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

	// It fits at 14:50 and truncates again at 14:55, 55 minutes after the last
	// event at 14:00: a new start fires at once, which a gate that never
	// forgot the recovered condition would not do inside the hour.
	e.gw.SetCatalog(truncatedListing()[:1024]...)
	e.syncAt(at(14, 50, 0))
	if evs := rec.take(); len(evs) != 0 {
		t.Fatalf("14:50, the listing fits: events = %+v, want none", evs)
	}
	e.gw.SetCatalog(truncatedListing()...)
	e.syncAt(at(14, 55, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{want}) {
		t.Fatalf("14:55, a new truncation: events = %+v, want [%+v] at once", evs, want)
	}
	for _, when := range []time.Time{at(14, 56, 0), at(15, 30, 0), at(15, 54, 59)} { // inside the hour from 14:55
		e.syncAt(when)
		if evs := rec.take(); len(evs) != 0 {
			t.Fatalf("%s: events = %+v, want none", when.Format("15:04:05"), evs)
		}
	}
	e.syncAt(at(15, 55, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{want}) {
		t.Fatalf("15:55: events = %+v, want one, an hour after 14:55", evs)
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

// The key of a skipped entry is its name and the kind of its failure, not the
// measured value in the note: a revision that changes length within the hour
// is the same condition (review of PR 116, Astra B1).
func TestManagedSkippedRevisionChangingLengthGivesOneEventAnHour(t *testing.T) {
	e := newManagedEnv(t)
	rec := e.withCapture()
	longRev := func(n int) provider.CatalogEntry {
		return provider.CatalogEntry{Name: "example-r", Revision: rev(strings.Repeat("r", n))}
	}
	note := func(n int) sentEvent {
		return sentEvent{"Warning", "CatalogEntrySkipped", "degraded",
			fmt.Sprintf(`catalog entry "example-r" skipped: a revision of %d characters, above 256`, n)}
	}
	e.gw.SetCatalog(entry("example-ok"), longRev(257))
	e.syncAt(at(12, 0, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{note(257)}) {
		t.Fatalf("12:00: events = %+v, want [%+v]", evs, note(257))
	}
	for i, n := range []int{258, 257, 300, 258, 257} { // 12:10, 12:20 ... 12:50
		e.gw.SetCatalog(entry("example-ok"), longRev(n))
		e.syncAt(at(12, 10*(i+1), 0))
		if evs := rec.take(); len(evs) != 0 {
			t.Fatalf("12:%02d, a revision of %d characters: events = %+v, want none", 10*(i+1), n, evs)
		}
	}
	e.gw.SetCatalog(entry("example-ok"), longRev(300))
	e.syncAt(at(13, 0, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{note(300)}) {
		t.Fatalf("13:00: events = %+v, want [%+v]", evs, note(300))
	}
}

// wantSkipped25 is the hand-written event of the i-th of 25 names that are
// each 257 characters long, above the 253 the status holds.
func wantSkipped25(i int) sentEvent {
	shown := fmt.Sprintf("n%02d-", i) + strings.Repeat("n", 60) // the first 64 characters
	return sentEvent{"Warning", "CatalogEntrySkipped", "degraded",
		`catalog entry "` + shown + `..." skipped: a name of 257 characters, above 253`}
}

// wantSummary names the remaining 15 as far as 1,024 bytes allow: "n10-" to
// "n22-", 13 names of 69 bytes each, after an 83-byte head, then an ellipsis
// (83 + 13*69 + 12*2 + 5 = 1,009; a 14th would make 1,080).
var wantSummary = func() sentEvent {
	var named []string
	for i := 10; i < 23; i++ {
		named = append(named, fmt.Sprintf(`"n%02d-`, i)+strings.Repeat("n", 60)+`..."`)
	}
	return sentEvent{"Warning", "CatalogEntrySkipped", "degraded",
		"15 more catalog entries skipped (at most 10 skipped entries are named in an hour): " + strings.Join(named, ", ") + ", …"}
}()

// At most 10 skip events for entries an hour, then one summary with the count
// (@hpc-burst-science-agent, 31 Oct 2026): 25 bad names give 10 and 1.
func TestManagedSkippedEntriesAreCappedAtTenAnHourWithASummary(t *testing.T) {
	e := newManagedEnv(t)
	rec := e.withCapture()
	catalog := []provider.CatalogEntry{entry("example-ok")}
	for i := range 25 {
		catalog = append(catalog, entry(fmt.Sprintf("n%02d-", i)+strings.Repeat("n", 253)))
	}
	e.gw.SetCatalog(catalog...)

	var want []sentEvent
	for i := range 10 {
		want = append(want, wantSkipped25(i))
	}
	want = append(want, wantSummary)
	var got []sentEvent
	for _, min := range []int{0, 1, 2, 30, 58, 59} { // 12:00 to 12:59
		e.syncAt(at(12, min, 0))
		got = append(got, rec.take()...)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("in the first hour: events = %+v, want the 10 first entries and the summary %+v", got, want)
	}
	// An hour on, the same again.
	e.syncAt(at(13, 0, 0))
	if got := rec.take(); !slices.Equal(got, want) {
		t.Fatalf("13:00: events = %+v, want %+v", got, want)
	}
}
