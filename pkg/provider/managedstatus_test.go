package provider

import (
	"fmt"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestExpiringCondition(t *testing.T) {
	exp := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		expiry *time.Time
		now    time.Time
		status metav1.ConditionStatus
		reason string
		msg    string
	}{
		{nil, t0, metav1.ConditionFalse, "NotExpiring", "the credentials file names no expiry"},
		{&exp, time.Date(2026, 10, 31, 11, 59, 59, 0, time.UTC), metav1.ConditionFalse, "NotExpiring", "the key expires at 2026-11-01T12:00:00Z"},
		{&exp, time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC), metav1.ConditionTrue, "ExpiresWithinADay", "the key expires at 2026-11-01T12:00:00Z"},
		{&exp, time.Date(2026, 11, 1, 11, 59, 59, 0, time.UTC), metav1.ConditionTrue, "ExpiresWithinADay", "the key expires at 2026-11-01T12:00:00Z"},
		{&exp, exp, metav1.ConditionTrue, "Expired", "the key expired at 2026-11-01T12:00:00Z"},
	} {
		s, r, m := ExpiringCondition(tc.expiry, tc.now)
		if s != tc.status || r != tc.reason || m != tc.msg {
			t.Fatalf("at %s: %s %s %q, want %s %s %q", tc.now, s, r, m, tc.status, tc.reason, tc.msg)
		}
	}
}

func TestSetManagedReadinessIsTheBatchStalenessRule(t *testing.T) {
	var st v1alpha1.ProviderStatus
	ready := func() *metav1.Condition { return apimeta.FindStatusCondition(st.Conditions, "Ready") }

	SetManagedReadiness(&st, 1, false, t0) // no read ever
	if r := ready(); r == nil || r.Status != metav1.ConditionFalse || r.Reason != "ReadsFailing" || !strings.Contains(r.Message, "no successful read yet") {
		t.Fatalf("Ready = %+v, want False ReadsFailing, no successful read yet", r)
	}
	SetManagedReadiness(&st, 1, true, t0)
	if r := ready(); r.Status != metav1.ConditionTrue || r.Reason != "CatalogRead" || st.LastSuccessfulStatusRead == nil || !st.LastSuccessfulStatusRead.Time.Equal(t0) {
		t.Fatalf("Ready = %+v, lastSuccessfulStatusRead %v, want True CatalogRead at t0", r, st.LastSuccessfulStatusRead)
	}
	if !v1alpha1.ProviderReady(&v1alpha1.Provider{Status: st}, t0.Add(5*time.Minute)) {
		t.Fatal("ProviderReady = false 5 min after a successful read")
	}
	// Failed passes keep Ready True for five minutes, then turn it False.
	for m := 1; m <= 5; m++ {
		SetManagedReadiness(&st, 1, false, t0.Add(time.Duration(m)*time.Minute))
		if r := ready(); r.Status != metav1.ConditionTrue {
			t.Fatalf("Ready turned %s after %d min of failed reads, want True up to 5 min", r.Status, m)
		}
	}
	SetManagedReadiness(&st, 1, false, t0.Add(6*time.Minute))
	r := ready()
	if r.Status != metav1.ConditionFalse || r.Reason != "ReadsFailing" || r.Message != "reads are failing: the last successful read was at 2026-10-31T12:00:00Z" {
		t.Fatalf("Ready = %+v after 6 min of failed reads, want False ReadsFailing naming 12:00:00", r)
	}
	if v1alpha1.ProviderReady(&v1alpha1.Provider{Status: st}, t0.Add(6*time.Minute)) {
		t.Fatal("ProviderReady = true after 6 min without a successful read")
	}
	if !st.LastSuccessfulStatusRead.Time.Equal(t0) {
		t.Fatal("a failed pass moved lastSuccessfulStatusRead")
	}
}

func TestEventNotes(t *testing.T) {
	long := strings.Repeat("n", 300)
	if n := SkippedNote(SkippedEntry{Name: long, Why: "a name of 300 characters, above 253"}); strings.Contains(n, long) || !strings.Contains(n, strings.Repeat("n", 64)+"...") {
		t.Fatalf("note %q: want the name shortened to 64 characters", n)
	}
	if n := TruncatedNote(6); n != "the gateway listed 6 entries more than status.catalog holds (1024); the first 1024 by name are kept" {
		t.Fatalf("note = %q", n)
	}
}

// restOf returns skipped entries whose names are the given lengths, distinct
// and below the 64 characters a note shows whole.
func restOf(lengths ...int) []SkippedEntry {
	var out []SkippedEntry
	for i, l := range lengths {
		out = append(out, SkippedEntry{Name: fmt.Sprintf("%02d", i) + strings.Repeat("x", l-2), Category: "NameTooLong"})
	}
	return out
}

// refSummary states the rule differently from SkippedSummaryNote, to check it
// against: try the most names first, and take the longest list whose note,
// with the ellipsis when names are left out, fits in MaxEventNote bytes.
func refSummary(rest []SkippedEntry) string {
	head := fmt.Sprintf("%d more catalog entries skipped (at most 10 skipped entries are named in an hour): ", len(rest))
	if len(rest) == 1 {
		head = "1 more catalog entry skipped (at most 10 skipped entries are named in an hour): "
	}
	var quoted []string
	for _, s := range rest {
		quoted = append(quoted, `"`+s.Name+`"`) // restOf names have no character that %q escapes
	}
	for k := len(rest); k >= 0; k-- {
		note := head + strings.Join(quoted[:k], ", ")
		if k < len(rest) {
			if k > 0 {
				note += ", "
			}
			note += "\u2026"
		}
		if len(note) <= MaxEventNote {
			return note
		}
	}
	return head + "\u2026"
}

// The summary names as many of the remaining skipped entries as fit in 1,024
// bytes, whole and in order, and ends with an ellipsis when it leaves some out
// (@hpc-burst-science-agent, 31 Oct 2026, for #114).
func TestSkippedSummaryNoteNamesAsManyAsFit(t *testing.T) {
	// Fifteen names of 59, 59, ... 58 characters fill the note exactly: 83 bytes
	// of head, 15*2 quotes, 14*2 separators and 883 characters make 1,024.
	exact := restOf(59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 58, 58)
	over := restOf(59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 59, 58, 59) // one byte more
	for _, tc := range []struct {
		name     string
		rest     []SkippedEntry
		want     string // the whole note, when it is hand-written
		wantLen  int    // otherwise its length
		ellipsis bool
	}{
		{"one", restOf(5), `1 more catalog entry skipped (at most 10 skipped entries are named in an hour): "00xxx"`, 0, false},
		{"two", restOf(4, 6), `2 more catalog entries skipped (at most 10 skipped entries are named in an hour): "00xx", "01xxxx"`, 0, false},
		{"all fit, to the byte", exact, "", 1024, false},
		{"one byte over", over, "", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := SkippedSummaryNote(tc.rest)
			if tc.want != "" && got != tc.want {
				t.Errorf("note = %q, want %q", got, tc.want)
			}
			if tc.wantLen != 0 && len(got) != tc.wantLen {
				t.Errorf("note has %d bytes, want %d", len(got), tc.wantLen)
			}
			if len(got) > MaxEventNote {
				t.Errorf("note has %d bytes, above %d", len(got), MaxEventNote)
			}
			if has := strings.HasSuffix(got, "\u2026"); has != tc.ellipsis {
				t.Errorf("ends with an ellipsis %v, want %v: %q", has, tc.ellipsis, got[max(0, len(got)-40):])
			}
			if want := refSummary(tc.rest); got != want {
				t.Errorf("note differs from the longest list that fits:\n got %q\nwant %q", got, want)
			}
		})
	}
	// A name that is not the last must leave room for the ellipsis after it.
	// Sixteen names: the first fourteen of 64 characters or fewer are shaped to
	// end the note, with their quotes and separators, margin bytes short of the
	// 1,024 (16 names make an 83-byte head, 14*2 quotes, 13*2 separators: 137
	// bytes and the characters). With a margin under 5 bytes the ellipsis
	// (", " and 3 bytes) cannot follow the 14th name, so 13 names are listed; at
	// 5 it fits, and 14 are.
	for margin := 0; margin <= 8; margin++ {
		lengths := make([]int, 16)
		for i := range lengths {
			lengths[i] = 64
		}
		cut := 14*64 - (1024 - margin - 137) // characters to take off the first fourteen
		for i := 0; cut > 0; i, cut = i+1, cut-1 {
			lengths[i%14]--
		}
		got := SkippedSummaryNote(restOf(lengths...))
		wantNames := 13
		if margin >= 5 {
			wantNames = 14
		}
		if n := strings.Count(got, `"`) / 2; n != wantNames || len(got) > MaxEventNote || !strings.HasSuffix(got, ", \u2026") {
			t.Errorf("margin %d: %d names in a note of %d bytes ending %q, want %d names, at most %d bytes and an ellipsis",
				margin, n, len(got), got[len(got)-12:], wantNames, MaxEventNote)
		}
		if want := refSummary(restOf(lengths...)); got != want {
			t.Errorf("margin %d: note differs from the longest list that fits", margin)
		}
	}
}

// A long name is shortened in the summary as in its own event, and a name with
// an escaped character is counted by the bytes it takes quoted.
func TestSkippedSummaryNoteShortensAndCountsQuotedBytes(t *testing.T) {
	rest := []SkippedEntry{{Name: strings.Repeat("n", 300)}, {Name: "a\"b"}}
	want := `2 more catalog entries skipped (at most 10 skipped entries are named in an hour): "` +
		strings.Repeat("n", 64) + `...", "a\"b"`
	if got := SkippedSummaryNote(rest); got != want {
		t.Fatalf("note = %q, want %q", got, want)
	}
	// Forty names whose characters each take 4 bytes quoted (\x01) cannot all
	// fit: the note stays within 1,024 bytes and keeps whole names.
	hard := strings.Repeat("\x01", 64)
	var many []SkippedEntry
	for i := range 40 {
		many = append(many, SkippedEntry{Name: fmt.Sprintf("%02d", i) + hard})
	}
	got := SkippedSummaryNote(many)
	if len(got) > MaxEventNote || !strings.HasSuffix(got, ", \u2026") {
		t.Fatalf("note of %d bytes ends %q, want at most %d bytes and an ellipsis", len(got), got[len(got)-30:], MaxEventNote)
	}
	if !strings.Contains(got, `"00`) || strings.Count(got, `"`)%2 != 0 {
		t.Fatalf("note %q: want the first name, and no name cut in the middle", got)
	}
}
