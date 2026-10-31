package provider

import (
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
	a := SkippedEntry{Name: "example-a", Why: "an empty name"}
	b := SkippedEntry{Name: "example-b", Why: "a revision of 257 characters, above 256"}
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
	a2 := SkippedEntry{Name: "example-a", Why: "listed twice with different revisions"}
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
	a := SkippedEntry{Name: "example-a", Why: "an empty name"}
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
