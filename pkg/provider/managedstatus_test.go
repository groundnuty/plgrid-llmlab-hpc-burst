package provider

import (
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
