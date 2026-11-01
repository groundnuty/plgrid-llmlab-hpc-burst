package managed

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// The events of the managed controller: their form, and the rate rule for the
// ones that repeat (HBEP-8, Events: Form and Repeating conditions;
// @hpc-burst-science-agent, 31 Oct 2026). Every expected type, action, reason,
// count and time is written out by hand, never taken from package provider's
// constants or functions, or from this package's tables. The stand-in
// (pkg/provider/fake.ManagedController) has the same tests for the events it
// raises.

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

// kinds drops the notes: "Type Reason action" of each event.
func kinds(evs []sentEvent) []string {
	var out []string
	for _, ev := range evs {
		out = append(out, ev.Type+" "+ev.Reason+" "+ev.Action)
	}
	return out
}

// only keeps the events of one reason.
func only(evs []sentEvent, reason string) []sentEvent {
	var out []sentEvent
	for _, ev := range evs {
		if ev.Reason == reason {
			out = append(out, ev)
		}
	}
	return out
}

// manyModels is n models, example-0000 to example-<n-1>.
func manyModels(n int) []llmlabtest.Model {
	var ms []llmlabtest.Model
	for i := range n {
		ms = append(ms, model(fmt.Sprintf("example-%04d", i)))
	}
	return ms
}

const truncatedNote = "the gateway listed 6 entries more than status.catalog holds (1024); the first 1024 by name are kept"

// Every event the controller emits has HBEP-8's type in action, the reason
// code alone, the Kubernetes type Warning exactly for degraded and failing,
// and a note of at most 1,024 bytes: one pass per event, in a fixed story of
// a Provider that breaks and recovers.
func TestEventsHaveTheirTypeInActionTheCodeAsReasonAndAShortNote(t *testing.T) {
	rec := &captureRecorder{}
	e := newEnv(t, withModels(model("example-a")), withRecorder(rec))
	ref := e.provider().Spec.CredentialsRef

	// want is the Type, Reason and action of the events a step raises, in
	// order, and the start of the first note.
	steps := []struct {
		what   string
		at     time.Time
		before func()
		want   []string
		note   string
	}{
		{"a healthy first read raises none", at(12, 0, 0), func() {}, nil, ""},
		{"a refused file", at(12, 2, 0), func() {
			writeFile(t, e.path, fmt.Sprintf(`{"key":%q,"baseURL":%q,"extra":true}`, e.key, e.gw.BaseURL()))
		}, []string{"Warning FileRefused failing"}, "CredentialsInvalid: "},
		{"the file is fixed", at(12, 4, 0), func() { e.pointAt(e.gw.BaseURL(), "") },
			[]string{"Normal FileRead ready"}, "CredentialsInvalid recovered"},
		{"no file is named", at(12, 6, 0), func() { e.setCredentialsRef(nil) },
			[]string{"Warning NoCredentialsFile failing"}, "CredentialsInvalid: "},
		{"a file is named again", at(12, 8, 0), func() { e.setCredentialsRef(ref) },
			[]string{"Normal FileRead ready"}, "CredentialsInvalid recovered"},
		{"the gateway rejects the key", at(12, 10, 0), func() { e.gw.RejectKey(e.key, 401) },
			[]string{"Warning KeyRejected failing"}, "CredentialsRejected: "},
		{"still rejected, same reason: no new event", at(12, 12, 0), func() {}, nil, ""},
		{"rejected for more than 5 minutes since the last read", at(12, 14, 0), func() {},
			[]string{"Warning ReadsFailing failing"}, "Ready: "},
		{"the gateway accepts the key again", at(12, 16, 0), func() { e.gw.RejectKey(e.key, 0) },
			[]string{"Normal KeyAccepted ready", "Normal CatalogRead ready"}, "CredentialsRejected recovered"},
		{"the key expires within a day", at(12, 18, 0), func() { e.pointAt(e.gw.BaseURL(), "2026-11-01T12:18:00Z") },
			[]string{"Warning ExpiresWithinADay degraded"}, "CredentialsExpiring: "},
		{"the key has expired", at(12, 20, 0), func() { e.pointAt(e.gw.BaseURL(), "2026-10-31T12:20:00Z") },
			[]string{"Warning Expired failing"}, "CredentialsExpiring: "},
		{"the key is rotated to one without an expiry", at(12, 22, 0), func() { e.pointAt(e.gw.BaseURL(), "") },
			[]string{"Normal NotExpiring ready"}, "CredentialsExpiring recovered"},
		{"a skipped entry and a truncated listing", at(12, 24, 0), func() {
			e.gw.SetModels(append(manyModels(1030), model(""))...)
		}, []string{"Warning CatalogEntrySkipped degraded", "Warning CatalogTruncated degraded"}, `catalog entry "" skipped: an empty name`},
	}
	for _, st := range steps {
		st.before()
		e.syncAt(st.at)
		got := rec.take()
		if !slices.Equal(kinds(got), st.want) {
			t.Fatalf("%s (%s): events = %q, want %q", st.what, st.at.Format("15:04"), kinds(got), st.want)
		}
		for _, ev := range got {
			if len(ev.Note) > 1024 || !utf8.ValidString(ev.Note) {
				t.Fatalf("%s: event %s has a note of %d bytes, valid UTF-8 %v; want at most 1024", st.what, ev.Reason, len(ev.Note), utf8.ValidString(ev.Note))
			}
			if ev.Action == "ReadCatalog" || strings.Contains(ev.Reason, "/") || ev.Reason == "" {
				t.Fatalf("%s: event %+v has the old action or a type/code reason", st.what, ev)
			}
		}
		if len(got) > 0 && !strings.HasPrefix(got[0].Note, st.note) {
			t.Fatalf("%s: the note is %q, want it to start with %q", st.what, got[0].Note, st.note)
		}
	}
}

// A message over 1,024 bytes is cut: a refused file whose path is 1,600
// characters long makes the condition message longer than the API accepts.
func TestEventNoteIsCutTo1024Bytes(t *testing.T) {
	rec := &captureRecorder{}
	e := newEnv(t, withModels(model("example-a")), withRecorder(rec))
	long := "/" + strings.Repeat(strings.Repeat("d", 200)+"/", 8) + "credentials.json" // 1,617 bytes, absent
	e.setCredentialsRef(&v1alpha1.CredentialsReference{File: long})
	e.syncAt(at(12, 0, 0))
	c := e.wantCondition(v1alpha1.ConditionCredentialsInvalid, "True", "FileRefused")
	if len(c.Message) <= 1024 {
		t.Fatalf("control: the condition message is %d bytes, want over 1024 for this test to cut anything", len(c.Message))
	}
	// The first pass with no successful read yet also turns Ready False.
	evs := rec.take()
	if want := []string{"Warning FileRefused failing", "Warning ReadsFailing failing"}; !slices.Equal(kinds(evs), want) {
		t.Fatalf("events = %q, want %q", kinds(evs), want)
	}
	if n := len(evs[0].Note); n != 1024 {
		t.Fatalf("the FileRefused note is %d bytes, want it cut to 1024", n)
	}
}

// A truncation that lasts three hours gives three events, one when it starts
// and then one an hour on, not one per read. The listing is read every 15
// minutes: reads of a 1030-entry listing are slow under -race, and the
// minute-by-minute run of the same rule is package provider's
// TestRepeatGateTruncatedFiresOnStartThenHourly.
func TestTruncationOfThreeHoursGivesThreeEvents(t *testing.T) {
	rec := &captureRecorder{}
	e := newEnv(t, withModels(manyModels(1030)...), withRecorder(rec))
	want := sentEvent{"Warning", "CatalogTruncated", "degraded", truncatedNote}

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
	e.gw.SetModels(manyModels(1024)...)
	e.syncAt(at(14, 50, 0))
	if evs := rec.take(); len(evs) != 0 {
		t.Fatalf("14:50, the listing fits: events = %+v, want none", evs)
	}
	e.gw.SetModels(manyModels(1030)...)
	e.syncAt(at(14, 55, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{want}) {
		t.Fatalf("14:55, a new truncation: events = %+v, want [%+v] at once", evs, want)
	}
	for _, when := range []time.Time{at(14, 56, 0), at(15, 30, 0), at(15, 54, 0)} { // inside the hour from 14:55
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

// A skipped entry is a condition of its own: it fires when it starts, then at
// most once an hour, and a new one fires at once.
func TestSkippedEntryFiresOnStartThenHourly(t *testing.T) {
	rec := &captureRecorder{}
	e := newEnv(t, withModels(model("example-ok"), model("")), withRecorder(rec))
	wantEmpty := sentEvent{"Warning", "CatalogEntrySkipped", "degraded", `catalog entry "" skipped: an empty name`}
	wantRev := sentEvent{"Warning", "CatalogEntrySkipped", "degraded", `catalog entry "example-r" skipped: a revision of 257 characters, above 256`}

	e.syncAt(at(12, 0, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{wantEmpty}) {
		t.Fatalf("12:00: events = %+v, want [%+v]", evs, wantEmpty)
	}
	e.syncAt(at(12, 1, 0))
	if evs := rec.take(); len(evs) != 0 {
		t.Fatalf("12:01: events = %+v, want none", evs)
	}
	// A second entry starts to be skipped at 12:30: only it fires.
	e.gw.SetModels(model("example-ok"), model(""), modelRev("example-r", strings.Repeat("r", 257)))
	e.syncAt(at(12, 30, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{wantRev}) {
		t.Fatalf("12:30: events = %+v, want only [%+v]", evs, wantRev)
	}
	e.syncAt(at(12, 59, 0))
	if evs := rec.take(); len(evs) != 0 {
		t.Fatalf("12:59: events = %+v, want none", evs)
	}
	e.syncAt(at(13, 0, 0)) // the first is an hour old, the second 30 minutes
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{wantEmpty}) {
		t.Fatalf("13:00: events = %+v, want only [%+v]", evs, wantEmpty)
	}
	// The entry is fixed at 13:10 and skipped again at 13:20: it fires at once.
	e.gw.SetModels(model("example-ok"), model("example-r"))
	e.syncAt(at(13, 10, 0))
	e.gw.SetModels(model("example-ok"), model("example-r"), model(""))
	e.syncAt(at(13, 20, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{wantEmpty}) {
		t.Fatalf("13:20: events = %+v, want [%+v] at once", evs, wantEmpty)
	}
}

// A pass that did not read the catalog says nothing about the listing, so it
// leaves the rate state alone: a truncation interrupted by a rejected key is
// the same condition afterwards, not a new start.
func TestRejectedKeyLeavesTheRepeatStateAlone(t *testing.T) {
	rec := &captureRecorder{}
	e := newEnv(t, withModels(manyModels(1030)...), withRecorder(rec))
	e.syncAt(at(12, 0, 0))
	if n := len(only(rec.take(), "CatalogTruncated")); n != 1 {
		t.Fatalf("12:00: %d CatalogTruncated events, want 1", n)
	}
	e.gw.RejectKey(e.key, 401)
	e.syncAt(at(12, 10, 0))
	e.gw.RejectKey(e.key, 0)
	e.syncAt(at(12, 20, 0)) // read again, truncated, 20 minutes after the event
	evs := rec.take()
	if n := len(only(evs, "CatalogTruncated")); n != 0 {
		t.Fatalf("12:20: %d CatalogTruncated events, want none: the rejected read did not end the condition (events %q)", n, kinds(evs))
	}
	// The rejection (10 minutes after the last read, so Ready turned False
	// too) and the recovery raised their own events meanwhile.
	want := []string{"Warning KeyRejected failing", "Warning ReadsFailing failing", "Normal KeyAccepted ready", "Normal CatalogRead ready"}
	if !slices.Equal(kinds(evs), want) {
		t.Fatalf("events = %q, want %q", kinds(evs), want)
	}
	e.syncAt(at(13, 0, 0))
	if n := len(only(rec.take(), "CatalogTruncated")); n != 1 {
		t.Fatalf("13:00: %d CatalogTruncated events, want 1, an hour after 12:00", n)
	}
}

// The key of a skipped entry is its name and the kind of its failure, not the
// measured value in the note: a revision that changes length within the hour
// is the same condition (review of PR 116, Astra B1).
func TestSkippedRevisionChangingLengthGivesOneEventAnHour(t *testing.T) {
	rec := &captureRecorder{}
	r := func(n int) llmlabtest.Model { return modelRev("example-r", strings.Repeat("r", n)) }
	e := newEnv(t, withModels(model("example-ok"), r(257)), withRecorder(rec))
	note := func(n int) sentEvent {
		return sentEvent{"Warning", "CatalogEntrySkipped", "degraded",
			fmt.Sprintf(`catalog entry "example-r" skipped: a revision of %d characters, above 256`, n)}
	}
	e.syncAt(at(12, 0, 0))
	if evs := rec.take(); !slices.Equal(evs, []sentEvent{note(257)}) {
		t.Fatalf("12:00: events = %+v, want [%+v]", evs, note(257))
	}
	for i, n := range []int{258, 257, 300, 258, 257} { // 12:10, 12:20 ... 12:50
		e.gw.SetModels(model("example-ok"), r(n))
		e.syncAt(at(12, 10*(i+1), 0))
		if evs := rec.take(); len(evs) != 0 {
			t.Fatalf("12:%02d, a revision of %d characters: events = %+v, want none", 10*(i+1), n, evs)
		}
	}
	e.gw.SetModels(model("example-ok"), r(300))
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
func TestSkippedEntriesAreCappedAtTenAnHourWithASummary(t *testing.T) {
	rec := &captureRecorder{}
	models := []llmlabtest.Model{model("example-ok")}
	for i := range 25 {
		models = append(models, model(fmt.Sprintf("n%02d-", i)+strings.Repeat("n", 253)))
	}
	e := newEnv(t, withModels(models...), withRecorder(rec))

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
	// The summary names some of the remaining 15 and not all (13 fit), ends
	// with an ellipsis, and is within the 1,024 bytes of a note.
	sum := got[len(got)-1].Note
	if len(sum) > 1024 || !strings.HasSuffix(sum, "\u2026") || !strings.Contains(sum, `"n22-`) || strings.Contains(sum, `"n23-`) {
		t.Fatalf("summary note of %d bytes: want at most 1024, an ellipsis, n22- named and n23- not: %q", len(sum), sum)
	}
	// An hour on, the same again.
	e.syncAt(at(13, 0, 0))
	if got := rec.take(); !slices.Equal(got, want) {
		t.Fatalf("13:00: events = %+v, want %+v", got, want)
	}
}
