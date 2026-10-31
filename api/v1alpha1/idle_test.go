package v1alpha1

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIdleSinceRoundTripsInUTCSeconds(t *testing.T) {
	a := &Allocation{}
	if _, ok, err := IdleSince(a); ok || err != nil {
		t.Fatalf("no annotation: ok %v err %v, want not idle", ok, err)
	}
	SetIdleSince(a, time.Date(2026, 10, 31, 13, 4, 5, 999_000_000, time.FixedZone("CET", 3600)))
	if got := a.Annotations["hpcburst.groundnuty.github.io/idle-since"]; got != "2026-10-31T12:04:05Z" {
		t.Fatalf("annotation = %q, want 2026-10-31T12:04:05Z (UTC, whole seconds)", got)
	}
	got, ok, err := IdleSince(a)
	if !ok || err != nil || !got.Equal(time.Date(2026, 10, 31, 12, 4, 5, 0, time.UTC)) || got.Location() != time.UTC {
		t.Fatalf("IdleSince = %v, %v, %v", got, ok, err)
	}
	ClearIdleSince(a)
	if _, ok, _ := IdleSince(a); ok {
		t.Fatal("the annotation is still there after ClearIdleSince")
	}
}

func TestIdleSinceRefusesAnUnreadableValue(t *testing.T) {
	for _, v := range []string{"", "yesterday", "2026-10-31", "1761912245"} {
		a := &Allocation{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{AnnotationIdleSince: v}}}
		if got, ok, err := IdleSince(a); err == nil || !ok || !got.IsZero() {
			t.Errorf("value %q: IdleSince = %v, %v, %v; want present, an error and no time", v, got, ok, err)
		}
	}
}

func TestProviderReadyNeedsTrueAndAFreshRead(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	prov := func(status metav1.ConditionStatus, read *time.Time) *Provider {
		p := &Provider{}
		if status != "" {
			p.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: status, Reason: "Polled"}}
		}
		if read != nil {
			m := metav1.NewTime(*read)
			p.Status.LastSuccessfulStatusRead = &m
		}
		return p
	}
	at := func(d time.Duration) *time.Time { t := now.Add(-d); return &t }
	for _, tc := range []struct {
		name string
		p    *Provider
		want bool
	}{
		{"True and read now", prov(metav1.ConditionTrue, at(0)), true},
		{"True and read exactly five minutes ago", prov(metav1.ConditionTrue, at(5*time.Minute)), true},
		{"True but read five minutes and a second ago", prov(metav1.ConditionTrue, at(5*time.Minute+time.Second)), false},
		{"True with no read recorded", prov(metav1.ConditionTrue, nil), false},
		{"False and read now", prov(metav1.ConditionFalse, at(0)), false},
		{"Unknown and read now", prov(metav1.ConditionUnknown, at(0)), false},
		{"no Ready condition", prov("", at(0)), false},
		{"nil", nil, false},
	} {
		if got := ProviderReady(tc.p, now); got != tc.want {
			t.Errorf("%s: ProviderReady = %v, want %v", tc.name, got, tc.want)
		}
	}
}
