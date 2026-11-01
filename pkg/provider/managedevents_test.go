package provider

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// The form and the rate rule of a managed Provider's events (HBEP-8, Events:
// Form and Repeating conditions). Every expected type, reason, length and time
// is written out by hand, never produced by the functions under test.

func TestCutNote(t *testing.T) {
	a := func(n int) string { return strings.Repeat("a", n) }
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{"short", "hello", "hello"},
		{"1024 ASCII bytes are kept", a(1024), a(1024)},
		{"1025 ASCII bytes are cut to 1024", a(1025), a(1024)},
		{"a 2-byte character that ends at 1024 is kept", a(1022) + "é", a(1022) + "é"},
		{"a 2-byte character across 1024 is dropped whole", a(1023) + "é", a(1023)},
		{"a 3-byte character that ends at 1024 is kept", a(1021) + "世" + "x", a(1021) + "世"},
		{"a 3-byte character across 1024, one byte in", a(1023) + "世", a(1023)},
		{"a 3-byte character across 1024, two bytes in", a(1022) + "世", a(1022)},
		{"a 4-byte character across 1024", a(1021) + "\U0001f600", a(1021)},
		{"2048 bytes of 2-byte characters", strings.Repeat("é", 1024), strings.Repeat("é", 512)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := CutNote(tc.in)
			if got != tc.want {
				t.Fatalf("CutNote = %d bytes %q, want %d bytes %q", len(got), got, len(tc.want), tc.want)
			}
			if !utf8.ValidString(got) {
				t.Fatalf("CutNote = %q, not valid UTF-8", got)
			}
		})
	}
}

func TestNewManagedEventSetsTypeAndCutsTheNote(t *testing.T) {
	for _, tc := range []struct {
		action, wantType string
	}{
		{"degraded", "Warning"},
		{"failing", "Warning"},
		{"ready", "Normal"},
	} {
		ev := NewManagedEvent(tc.action, "SomeCode", strings.Repeat("n", 5000))
		if ev.Action != tc.action || ev.Reason != "SomeCode" || ev.Type != tc.wantType || len(ev.Note) != 1024 {
			t.Fatalf("NewManagedEvent(%q) = {Type %q, Reason %q, Action %q, Note of %d bytes}, want Type %q, Reason SomeCode, Action %q and a note of 1024 bytes",
				tc.action, ev.Type, ev.Reason, ev.Action, len(ev.Note), tc.wantType, tc.action)
		}
	}
	// The constants are the strings of HBEP-8's Events table.
	if EventActionReady != "ready" || EventActionDegraded != "degraded" || EventActionFailing != "failing" {
		t.Fatalf("event actions = %q %q %q, want ready degraded failing", EventActionReady, EventActionDegraded, EventActionFailing)
	}
}

// truncatedMerge is a read whose listing was 6 entries too long.
var truncatedMerge = CatalogMerge{Truncated: 6}

const truncatedNote = "the gateway listed 6 entries more than status.catalog holds (1024); the first 1024 by name are kept"

func reasons(evs []ManagedEvent) []string {
	var out []string
	for _, ev := range evs {
		out = append(out, ev.Reason)
	}
	return out
}

func TestRepeatGateTruncatedFiresOnStartThenHourly(t *testing.T) {
	var g RepeatGate
	evs := g.CatalogEvents(t0, truncatedMerge)
	want := ManagedEvent{Type: "Warning", Reason: "CatalogTruncated", Action: "degraded", Note: truncatedNote}
	if len(evs) != 1 || evs[0] != want {
		t.Fatalf("first read: events = %+v, want exactly [%+v]", evs, want)
	}
	// Reads every minute for 3 hours: events at 0, 60 and 120 minutes only.
	var firedAt []int
	for m := 1; m < 180; m++ {
		if evs := g.CatalogEvents(t0.Add(time.Duration(m)*time.Minute), truncatedMerge); len(evs) > 0 {
			if len(evs) != 1 || evs[0] != want {
				t.Fatalf("minute %d: events = %+v, want [%+v]", m, evs, want)
			}
			firedAt = append(firedAt, m)
		}
	}
	if !slices.Equal(firedAt, []int{60, 120}) {
		t.Fatalf("fired again at minutes %v, want [60 120]", firedAt)
	}
	// The boundary: 59m59s is within the hour, an hour exactly is not.
	var h RepeatGate
	h.CatalogEvents(t0, truncatedMerge)
	if evs := h.CatalogEvents(t0.Add(time.Hour-time.Second), truncatedMerge); len(evs) != 0 {
		t.Fatalf("at 59m59s: events = %+v, want none", evs)
	}
	if evs := h.CatalogEvents(t0.Add(time.Hour), truncatedMerge); len(evs) != 1 {
		t.Fatalf("at 60m: events = %+v, want one", evs)
	}
}

func TestRepeatGateRecoveryThenNewTruncationFiresAtOnce(t *testing.T) {
	var g RepeatGate
	if len(g.CatalogEvents(t0, truncatedMerge)) != 1 {
		t.Fatal("the start did not fire")
	}
	// The listing fits at 12:10, and truncates again at 12:20, 20 minutes
	// after the first event.
	if evs := g.CatalogEvents(t0.Add(10*time.Minute), CatalogMerge{}); len(evs) != 0 {
		t.Fatalf("a read that fits: events = %+v, want none", evs)
	}
	evs := g.CatalogEvents(t0.Add(20*time.Minute), truncatedMerge)
	if got := reasons(evs); !slices.Equal(got, []string{"CatalogTruncated"}) {
		t.Fatalf("a new truncation: events = %v, want one CatalogTruncated at once", got)
	}
	// Its hour counts from 12:20.
	if evs := g.CatalogEvents(t0.Add(79*time.Minute), truncatedMerge); len(evs) != 0 {
		t.Fatalf("at 13:19: events = %+v, want none", evs)
	}
	if evs := g.CatalogEvents(t0.Add(80*time.Minute), truncatedMerge); len(evs) != 1 {
		t.Fatalf("at 13:20: events = %+v, want one", evs)
	}
	// A change in the count is not a new start.
	if evs := g.CatalogEvents(t0.Add(81*time.Minute), CatalogMerge{Truncated: 7}); len(evs) != 0 {
		t.Fatalf("a count of 7 at 13:21: events = %+v, want none", evs)
	}
}

func TestRepeatGateSkippedEntriesAreConditionsOfTheirOwn(t *testing.T) {
	var g RepeatGate
	a := SkippedEntry{Name: "example-a", Category: "EmptyName", Why: "an empty name"}
	b := SkippedEntry{Name: "example-b", Category: "RevisionTooLong", Why: "a revision of 257 characters, above 256"}
	notes := func(evs []ManagedEvent) []string {
		var out []string
		for _, ev := range evs {
			if ev.Reason != "CatalogEntrySkipped" || ev.Action != "degraded" || ev.Type != "Warning" {
				t.Fatalf("event %+v, want a Warning CatalogEntrySkipped of type degraded", ev)
			}
			out = append(out, ev.Note)
		}
		return out
	}
	noteA := `catalog entry "example-a" skipped: an empty name`
	noteB := `catalog entry "example-b" skipped: a revision of 257 characters, above 256`

	if got := notes(g.CatalogEvents(t0, CatalogMerge{Skipped: []SkippedEntry{a}})); !slices.Equal(got, []string{noteA}) {
		t.Fatalf("12:00: notes = %q, want [%q]", got, noteA)
	}
	// 12:30: A still skipped, B starts: only B fires.
	if got := notes(g.CatalogEvents(t0.Add(30*time.Minute), CatalogMerge{Skipped: []SkippedEntry{a, b}})); !slices.Equal(got, []string{noteB}) {
		t.Fatalf("12:30: notes = %q, want only [%q]", got, noteB)
	}
	// 13:00: A is an hour old, B is 30 minutes old.
	if got := notes(g.CatalogEvents(t0.Add(time.Hour), CatalogMerge{Skipped: []SkippedEntry{a, b}})); !slices.Equal(got, []string{noteA}) {
		t.Fatalf("13:00: notes = %q, want only [%q]", got, noteA)
	}
	// The same entry for another reason is another condition.
	a2 := SkippedEntry{Name: "example-a", Category: "AmbiguousRevision", Why: "listed twice with different revisions"}
	got := notes(g.CatalogEvents(t0.Add(61*time.Minute), CatalogMerge{Skipped: []SkippedEntry{a, a2}}))
	if want := []string{`catalog entry "example-a" skipped: listed twice with different revisions`}; !slices.Equal(got, want) {
		t.Fatalf("13:01: notes = %q, want %q", got, want)
	}
	// The same condition twice in one read is one event.
	var h RepeatGate
	if got := h.CatalogEvents(t0, CatalogMerge{Skipped: []SkippedEntry{a, a}}); len(got) != 1 {
		t.Fatalf("a duplicate skipped entry: %d events, want 1", len(got))
	}
}

func TestRepeatGateTruncationAndSkipsDoNotShareARate(t *testing.T) {
	var g RepeatGate
	a := SkippedEntry{Name: "example-a", Category: "EmptyName", Why: "an empty name"}
	both := CatalogMerge{Skipped: []SkippedEntry{a}, Truncated: 6}
	if got := reasons(g.CatalogEvents(t0, both)); !slices.Equal(got, []string{"CatalogEntrySkipped", "CatalogTruncated"}) {
		t.Fatalf("12:00: %v, want a skip and a truncation", got)
	}
	// Truncation ends and returns while the skip stays: only truncation fires.
	g.CatalogEvents(t0.Add(10*time.Minute), CatalogMerge{Skipped: []SkippedEntry{a}})
	if got := reasons(g.CatalogEvents(t0.Add(20*time.Minute), both)); !slices.Equal(got, []string{"CatalogTruncated"}) {
		t.Fatalf("12:20: %v, want only the returned truncation", got)
	}
}

func TestRepeatGateClockSteppedBackFiresOnce(t *testing.T) {
	var g RepeatGate
	g.CatalogEvents(t0, truncatedMerge)
	back := t0.Add(-3 * time.Hour)
	if len(g.CatalogEvents(back, truncatedMerge)) != 1 {
		t.Fatal("a read 3 hours before the last event did not fire")
	}
	if len(g.CatalogEvents(back.Add(time.Minute), truncatedMerge)) != 0 {
		t.Fatal("the read after it fired again")
	}
}

// The key of a skipped entry is its name and the category of its failure, not
// the words: a revision that gets one character longer is the same condition
// (review of PR 116, Astra B1).
func TestRepeatGateSkippedKeyIgnoresTheMeasuredValue(t *testing.T) {
	var g RepeatGate
	skip := func(n int) CatalogMerge {
		return CatalogMerge{Skipped: []SkippedEntry{{Name: "example-r", Category: "RevisionTooLong",
			Why: fmt.Sprintf("a revision of %d characters, above 256", n)}}}
	}
	wantFirst := `catalog entry "example-r" skipped: a revision of 257 characters, above 256`
	evs := g.CatalogEvents(t0, skip(257))
	if len(evs) != 1 || evs[0].Note != wantFirst {
		t.Fatalf("12:00: events = %+v, want one with the note %q", evs, wantFirst)
	}
	// 257, 258, 257, 300 ... every minute of the hour: no event.
	for m := 1; m < 60; m++ {
		n := []int{258, 257, 300, 258}[m%4]
		if evs := g.CatalogEvents(t0.Add(time.Duration(m)*time.Minute), skip(n)); len(evs) != 0 {
			t.Fatalf("minute %d, a revision of %d characters: events = %+v, want none: the same entry and failure", m, n, evs)
		}
	}
	// An hour on it fires once, with the words of that read.
	evs = g.CatalogEvents(t0.Add(time.Hour), skip(300))
	if want := `catalog entry "example-r" skipped: a revision of 300 characters, above 256`; len(evs) != 1 || evs[0].Note != want {
		t.Fatalf("13:00: events = %+v, want one with the note %q", evs, want)
	}
	// Another failure of the same entry is another condition.
	other := CatalogMerge{Skipped: []SkippedEntry{{Name: "example-r", Category: "AmbiguousRevision", Why: "listed twice with different revisions"}}}
	if evs := g.CatalogEvents(t0.Add(61*time.Minute), other); len(evs) != 1 {
		t.Fatalf("13:01, another category: events = %+v, want one", evs)
	}
}

// names25 is 25 distinct names that differ within the first 64 characters, so
// their notes differ too, each too long for the status.
func names25() CatalogMerge {
	var m CatalogMerge
	for i := range 25 {
		m.Skipped = append(m.Skipped, SkippedEntry{Name: fmt.Sprintf("n%02d-", i) + strings.Repeat("n", 253), Category: "NameTooLong",
			Why: "a name of 257 characters, above 253"})
	}
	return m
}

// wantSummary15 is the hand-written summary of names25 beyond its first ten:
// "n10-" to "n24-". Each is shown as its first 64 characters and "...", quoted,
// so 69 bytes, with ", " between them, after the 83 bytes of the head. 13 fit
// in 1,024 bytes with the ellipsis (83 + 13*69 + 12*2 + 5 = 1,009; a 14th would
// make 1,080), so "n10-" to "n22-" are named, and "n23-" and "n24-" are not.
var wantSummary15 = func() string {
	var named []string
	for i := 10; i < 23; i++ {
		named = append(named, fmt.Sprintf(`"n%02d-`, i)+strings.Repeat("n", 60)+`..."`)
	}
	return "15 more catalog entries skipped (at most 10 skipped entries are named in an hour): " + strings.Join(named, ", ") + ", …"
}()

// At most 10 skip events for individual entries an hour, then one summary an
// hour with the count (@hpc-burst-science-agent, 31 Oct 2026).
func TestRepeatGateCapsSkippedEntriesAtTenAnHourWithASummary(t *testing.T) {
	var g RepeatGate
	m := names25()
	var individual, summaries []string
	collect := func(evs []ManagedEvent) {
		for _, ev := range evs {
			if ev.Reason != "CatalogEntrySkipped" || ev.Action != "degraded" || ev.Type != "Warning" {
				t.Fatalf("event %+v, want a Warning CatalogEntrySkipped of type degraded", ev)
			}
			if strings.HasPrefix(ev.Note, "15 more") {
				if len(ev.Note) > MaxEventNote || !strings.HasSuffix(ev.Note, "\u2026") {
					t.Fatalf("summary note of %d bytes %q, want at most %d bytes, ending with an ellipsis", len(ev.Note), ev.Note, MaxEventNote)
				}
				summaries = append(summaries, ev.Note)
			} else {
				individual = append(individual, ev.Note)
			}
		}
	}
	for min := 0; min < 60; min++ { // 12:00 to 12:59, a read a minute
		collect(g.CatalogEvents(t0.Add(time.Duration(min)*time.Minute), m))
	}
	if len(individual) != 10 || !slices.Equal(summaries, []string{wantSummary15}) {
		t.Fatalf("in the first hour: %d individual events and summaries %q, want 10 and exactly [%q]", len(individual), summaries, wantSummary15)
	}
	// The first ten in listing order, each naming its own entry.
	for i, n := range individual {
		if want := fmt.Sprintf(`catalog entry "n%02d-`, i); !strings.HasPrefix(n, want) {
			t.Fatalf("individual event %d = %q, want it to start with %q", i, n, want)
		}
	}
	// The next hour is the same again: 10 and one summary.
	individual, summaries = nil, nil
	for min := 60; min < 120; min++ {
		collect(g.CatalogEvents(t0.Add(time.Duration(min)*time.Minute), m))
	}
	if len(individual) != 10 || !slices.Equal(summaries, []string{wantSummary15}) {
		t.Fatalf("in the second hour: %d individual events and summaries %q, want 10 and exactly [%q]", len(individual), summaries, wantSummary15)
	}
}

// The limit counts every skip event of the last hour, entries that recovered
// included, and the summary is once an hour whatever starts and stops.
func TestRepeatGateSkipLimitHoldsAcrossRecoveries(t *testing.T) {
	var g RepeatGate
	if evs := g.CatalogEvents(t0, names25()); len(evs) != 11 {
		t.Fatalf("12:00: %d events, want 10 and a summary", len(evs))
	}
	// All fixed at 12:10, then 25 new bad names at 12:20: the budget of the
	// hour is spent, so no individual event, and the summary has fired.
	if evs := g.CatalogEvents(t0.Add(10*time.Minute), CatalogMerge{}); len(evs) != 0 {
		t.Fatalf("12:10: events = %+v, want none", evs)
	}
	if evs := g.CatalogEvents(t0.Add(20*time.Minute), names25()); len(evs) != 0 {
		t.Fatalf("12:20: %d events (%+v), want none: 10 skip events and the summary already fired in the hour", len(evs), reasons(evs))
	}
	// At 13:00 the budget is free again, and the summary too.
	evs := g.CatalogEvents(t0.Add(time.Hour), names25())
	if len(evs) != 11 || evs[10].Note != wantSummary15 {
		t.Fatalf("13:00: %d events, want 10 and the summary %q", len(evs), wantSummary15)
	}
	// Fewer than 10 skipped entries have no summary; one over has "1 more".
	var h RepeatGate
	m := names25()
	m.Skipped = m.Skipped[:10]
	if evs := h.CatalogEvents(t0, m); len(evs) != 10 {
		t.Fatalf("10 skipped entries: %d events, want 10 and no summary", len(evs))
	}
	m.Skipped = names25().Skipped[:11]
	evs = h.CatalogEvents(t0.Add(time.Minute), m)
	if want := `1 more catalog entry skipped (at most 10 skipped entries are named in an hour): "n10-` + strings.Repeat("n", 60) + `..."`; len(evs) != 1 || evs[0].Note != want {
		t.Fatalf("an 11th entry: events = %+v, want one summary %q", evs, want)
	}
}
