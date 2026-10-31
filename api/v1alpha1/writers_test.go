package v1alpha1

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func dur(s string) *Duration { d := Duration(s); return &d }

// HBEP-7's Test plan, Drain window, worked out by hand: on a profile with
// drainWindow 2 h 10 min and drainMargin 10 min, a deployment with
// routing.maxRequestDuration 30 min drains 40 min before its hard end, and one
// with 3 h drains 2 h 10 min before it (a deployment never lengthens the
// window).
func TestInstanceDrainWindow(t *testing.T) {
	profile := PlatformProfileSpec{DrainWindow: dur("2h10m"), DrainMargin: dur("10m")}
	cases := []struct {
		name    string
		profile PlatformProfileSpec
		routing *Routing
		want    time.Duration
	}{
		{"HBEP-7: a 30 min deadline", profile, &Routing{MaxRequestDuration: dur("30m")}, 40 * time.Minute},
		{"HBEP-7: a 3 h deadline never lengthens", profile, &Routing{MaxRequestDuration: dur("3h")}, 130 * time.Minute},
		{"no routing", profile, nil, 130 * time.Minute},
		{"routing without a deadline", profile, &Routing{InstanceCap: new(int32)}, 130 * time.Minute},
		{"a deadline whose window equals drainWindow", profile, &Routing{MaxRequestDuration: dur("2h")}, 130 * time.Minute},
		{"an absent profile window and margin take HBEP-4's defaults", PlatformProfileSpec{}, &Routing{MaxRequestDuration: dur("1h")}, 70 * time.Minute},
		{"a profile window of 1 h with a 2 h deadline", PlatformProfileSpec{DrainWindow: dur("1h"), DrainMargin: dur("5m")}, &Routing{MaxRequestDuration: dur("2h")}, time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := InstanceDrainWindow(tc.profile, tc.routing)
			if err != nil || got != tc.want {
				t.Fatalf("InstanceDrainWindow = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
	if DefaultDrainWindow != 130*time.Minute || DefaultDrainMargin != 10*time.Minute {
		t.Errorf("defaults = %v and %v, want HBEP-4's 2h10m and 10m", DefaultDrainWindow, DefaultDrainMargin)
	}

	// DrainStart: a lease ending at 12:00 UTC drains from 11:20 with a 30 min
	// deadline.
	end := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	got, err := DrainStart(end, profile, &Routing{MaxRequestDuration: dur("30m")})
	if want := time.Date(2026, 11, 2, 11, 20, 0, 0, time.UTC); err != nil || !got.Equal(want) {
		t.Errorf("DrainStart = %v, %v; want %v", got, err, want)
	}

	for name, tc := range map[string]struct {
		p PlatformProfileSpec
		r *Routing
	}{
		"a drain window that is not a duration": {PlatformProfileSpec{DrainWindow: dur("two hours")}, nil},
		"a margin that is not a duration":       {PlatformProfileSpec{DrainMargin: dur("ten")}, &Routing{MaxRequestDuration: dur("1h")}},
		"a deadline that is not a duration":     {profile, &Routing{MaxRequestDuration: dur("1 h")}},
	} {
		if _, err := InstanceDrainWindow(tc.p, tc.r); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestImageFormatOf(t *testing.T) {
	if got := ImageFormatOf(PartitionProfile{}); got != "sif" {
		t.Errorf("an unstated format = %q, want sif", got)
	}
	if got := ImageFormatOf(PartitionProfile{ImageFormat: ImageFormatSandbox}); got != "sandbox" {
		t.Errorf("a sandbox partition = %q, want sandbox", got)
	}
}

// The apply object holds the Provider's name and the one condition, and
// nothing else, so the lifecycle controller's apply owns that entry only.
func TestDialHomeUnreachableApply(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC))
	cond := metav1.Condition{Type: "Ignored", Status: metav1.ConditionTrue, Reason: "NoRegistration", Message: "m", LastTransitionTime: t0, ObservedGeneration: 3}
	u, err := DialHomeUnreachableApply("helios-a", cond)
	if err != nil {
		t.Fatal(err)
	}
	if u.GetAPIVersion() != "hpcburst.groundnuty.github.io/v1alpha1" || u.GetKind() != "Provider" || u.GetName() != "helios-a" {
		t.Errorf("object = %s %s %s", u.GetAPIVersion(), u.GetKind(), u.GetName())
	}
	if len(u.Object) != 4 || len(u.Object["metadata"].(map[string]any)) != 1 || len(u.Object["status"].(map[string]any)) != 1 {
		t.Errorf("the apply carries more than the name and the condition: %v", u.Object)
	}
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	want := map[string]any{"type": "DialHomeUnreachable", "status": "True", "reason": "NoRegistration", "message": "m",
		"lastTransitionTime": "2026-10-31T09:00:00Z", "observedGeneration": int64(3)}
	if len(conds) != 1 {
		t.Fatalf("conditions = %v", conds)
	}
	got := conds[0].(map[string]any)
	for k, w := range want {
		if got[k] != w {
			t.Errorf("condition %s = %v (%T), want %v", k, got[k], got[k], w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("condition has extra keys: %v", got)
	}

	for name, c := range map[string]metav1.Condition{
		"no reason":          {Status: metav1.ConditionTrue, LastTransitionTime: t0},
		"no transition time": {Status: metav1.ConditionTrue, Reason: "R"},
		"a bad status":       {Status: "Maybe", Reason: "R", LastTransitionTime: t0},
	} {
		if _, err := DialHomeUnreachableApply("helios-a", c); err == nil || !strings.Contains(err.Error(), "dial-home") {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if _, err := DialHomeUnreachableApply("", cond); err == nil {
		t.Error("no Provider name: no error")
	}
}
