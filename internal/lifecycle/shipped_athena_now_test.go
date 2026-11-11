package lifecycle

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// profiles/athena-now.yaml (issue #376; the owner's decision 2 of 11 Nov 2026, and the science
// agent's rulings of that day): a second shipped Athena profile, for short TEST deployments on
// plgrid-now, whose margins are scaled to the partition's hour. athena.yaml is unchanged by it
// and still refuses plgrid-now (TestShippedAthenaNowIsTooShort, which is not touched).
//
// The expected values are worked out by hand from the ruled margins, never read back through
// the code under test:
//   - the lead: drain window min(drainWindow 7m, routing.maxRequestDuration + drainMargin 2m)
//     + handoverMargin 1m + health.startTimeout 12m. With a 5 min deadline, or none, the drain
//     window is 7m and the lead 20m; a time limit must be longer than it, in whole minutes, so
//     the shortest lease is 21m;
//   - a shorter deadline shortens the window (2m + 2m = 4m, lead 17m, shortest lease 18m), and a
//     longer one never lengthens it (30m + 2m is above 7m, so 7m);
//   - the hour of the one-slot handover (the plan of issue #376, section 3): a start wait of 10m,
//     the predecessor's 21m lease, a second start wait of 10m, the successor ready within
//     startTimeout 12m, and 2m of cleanup: 10 + 21 + 10 + 12 + 2 = 55m, inside 1 h.

// shippedProfileFile reads a shipped manifest strictly; a missing file fails, it never skips.
func shippedProfileFile(t *testing.T, file string) (v1alpha1.PlatformProfile, map[string]any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", file))
	if err != nil {
		t.Fatalf("read the shipped profile: %v", err)
	}
	var prof v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(raw, &prof); err != nil {
		t.Fatalf("%s: %v", file, err)
	}
	var loose map[string]any
	if err := yaml.Unmarshal(raw, &loose); err != nil {
		t.Fatalf("%s: %v", file, err)
	}
	return prof, loose
}

func partitionOf(t *testing.T, prof v1alpha1.PlatformProfile, name string) v1alpha1.PartitionProfile {
	t.Helper()
	for _, p := range prof.Spec.Partitions {
		if p.Name == name {
			return p
		}
	}
	t.Fatalf("profile %s does not list %s", prof.Name, name)
	return v1alpha1.PartitionProfile{}
}

// athena-now holds plgrid-now only, with its QOS only, and sends the agent a sandbox there, as
// athena.yaml's plgrid-now now does (S1 probe 2: a SIF fails on these A100 nodes and a sandbox
// runs; plgrid-now is the same node type, and its first run is the measurement).
func TestShippedAthenaNowHoldsPlgridNowOnlyAndSendsASandbox(t *testing.T) {
	now, _ := shippedProfileFile(t, "athena-now.yaml")
	if now.Name != "athena-now" || now.Spec.ProviderKind != v1alpha1.PlatformBatch {
		t.Errorf("profile %q of kind %q; want the batch profile athena-now", now.Name, now.Spec.ProviderKind)
	}
	if len(now.Spec.Partitions) != 1 || now.Spec.Partitions[0].Name != "plgrid-now" {
		t.Errorf("partitions %+v; want plgrid-now only", now.Spec.Partitions)
	}
	if len(now.Spec.QOS) != 1 || now.Spec.QOS[0].Name != "plgrid-now" {
		t.Errorf("qos %+v; want plgrid-now only", now.Spec.QOS)
	}
	athena, _ := shippedProfileFile(t, "athena.yaml")
	for name, prof := range map[string]v1alpha1.PlatformProfile{"athena-now": now, "athena": athena} {
		if got := v1alpha1.ImageFormatOf(partitionOf(t, prof, "plgrid-now")); got != v1alpha1.ImageFormatSandbox {
			t.Errorf("%s: the agent is sent a %q on plgrid-now, want a sandbox", name, got)
		}
	}
	if got := v1alpha1.ImageFormatOf(partitionOf(t, athena, "plgrid-gpu-a100")); got != v1alpha1.ImageFormatSandbox {
		t.Errorf("athena: plgrid-gpu-a100 sends a %q, want the sandbox it already had", got)
	}
	// The same node type, so the same sizing and the same caps.
	a, n := partitionOf(t, athena, "plgrid-now"), partitionOf(t, now, "plgrid-now")
	if !reflect.DeepEqual(a, n) {
		t.Errorf("plgrid-now differs between the two profiles:\n athena     %+v\n athena-now %+v", a, n)
	}
	if !reflect.DeepEqual(athena.Spec.QOS[1], now.Spec.QOS[0]) || athena.Spec.QOS[1].Name != "plgrid-now" {
		t.Errorf("the plgrid-now QOS differs between the two profiles:\n athena     %+v\n athena-now %+v", athena.Spec.QOS, now.Spec.QOS)
	}
}

// The facts are the platform's: athena-now carries athena.yaml's untyped block whole, so a fact
// path in either file leads to the same fact. A change to one that is not made to the other shows.
func TestShippedAthenaNowKeepsAthenasFacts(t *testing.T) {
	_, athena := shippedProfileFile(t, "athena.yaml")
	_, now := shippedProfileFile(t, "athena-now.yaml")
	a := athena["spec"].(map[string]any)["profile"]
	n := now["spec"].(map[string]any)["profile"]
	if a == nil || !reflect.DeepEqual(a, n) {
		t.Error("spec.profile of athena-now.yaml is not athena.yaml's")
	}
}

// athena.yaml keeps the margins that make plgrid-now too short (TestShippedAthenaNowIsTooShort
// shows the refusal): adding athena-now must not have moved them.
func TestShippedAthenaKeepsItsMargins(t *testing.T) {
	athena, _ := shippedProfileFile(t, "athena.yaml")
	s := athena.Spec
	for _, c := range []struct {
		name string
		got  *v1alpha1.Duration
		want v1alpha1.Duration
	}{
		{"drainWindow", s.DrainWindow, "2h10m"},
		{"drainMargin", s.DrainMargin, "10m"},
		{"handoverMargin", s.HandoverMargin, "30m"},
		{"health.startTimeout", s.Health.StartTimeout, "1h"},
	} {
		if c.got == nil || *c.got != c.want {
			t.Errorf("athena.yaml %s is %v, want %s", c.name, c.got, c.want)
		}
	}
}

// athena-now fits the partition's hour: the lead, from the profile's own values, is 20m, and an
// Allocation of 21m (the shortest in whole minutes), of 48m or of the partition's 1 h is placed;
// one of 20m is not. The shipped values go through the lifecycle controller's own check, with
// the partition's limit, as TestShippedAthenaNowIsTooShort does for athena.yaml.
func TestShippedAthenaNowFitsTheHour(t *testing.T) {
	prof, _ := shippedProfileFile(t, "athena-now.yaml")
	part := partitionOf(t, prof, "plgrid-now")
	maxTime, err := part.MaxTime.AsDuration()
	if err != nil {
		t.Fatal(err)
	}
	if maxTime != time.Hour {
		t.Fatalf("plgrid-now's maxTime is %s, want 1h", maxTime)
	}
	shipped := func() []rigOption {
		return []rigOption{
			withMaxTime(maxTime),
			withProfile(func(s *v1alpha1.PlatformProfileSpec) {
				s.DrainWindow, s.DrainMargin, s.HandoverMargin = prof.Spec.DrainWindow, prof.Spec.DrainMargin, prof.Spec.HandoverMargin
				s.Health, s.QOS = prof.Spec.Health, prof.Spec.QOS
			}),
			withPartition(func(p *v1alpha1.PartitionProfile) {
				p.MaxTime, p.GPUsPerNode, p.CPUsPerGPU, p.MemPerGPU, p.QOS = part.MaxTime, part.GPUsPerNode, part.CPUsPerGPU, part.MemPerGPU, part.QOS
			}),
		}
	}
	min := func(n int) *v1alpha1.Duration { return dur(time.Duration(n) * time.Minute) }
	for _, tc := range []struct {
		name    string
		maxReq  *v1alpha1.Duration // routing.maxRequestDuration; nil: the deployment sets none
		lease   *v1alpha1.Duration // leaseLimit; nil: the partition's own limit
		limit   string             // the time limit of the Allocation, when it is placed
		tooShor string             // when set, the message names this lead and no Allocation is made
	}{
		{"the partition's own limit, no deadline set", nil, nil, "1h0m0s", ""},
		{"the partition's own limit, a 5 min deadline", min(5), nil, "1h0m0s", ""},
		{"row A's 48 min lease, a 5 min deadline", min(5), min(48), "48m0s", ""},
		{"row B's 21 min lease, a 5 min deadline: the shortest placed", min(5), min(21), "21m0s", ""},
		{"a 21 min lease with no deadline set: the profile's window is the same", nil, min(21), "21m0s", ""},
		{"a 20 min lease equals the lead: too short", min(5), min(20), "", "20m0s"},
		{"a 30 min deadline never lengthens the window", min(30), min(21), "21m0s", ""},
		{"a 2 min deadline shortens the window: lead 17m, so 18m is placed", min(2), min(18), "18m0s", ""},
		{"a 2 min deadline: a 17 min lease equals the lead of 17m: too short", min(2), min(17), "", "17m0s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, shipped()...)
			md := mdSpec("md")
			if tc.maxReq != nil {
				md.Spec.Routing = &v1alpha1.Routing{MaxRequestDuration: tc.maxReq}
			}
			md.Spec.LeaseLimit = tc.lease
			shortStep(rg, md)
			if tc.tooShor != "" {
				wantTooShort(t, rg, "md", "partition "+rigPartition, "set by leaseLimit", "lead "+tc.tooShor)
				return
			}
			wantAllocated(t, rg, "md", v1alpha1.Duration(tc.limit))
		})
	}
}

// The profile's own text states the lead and the hour it is built for, and the arithmetic holds
// with the values it carries: the lead, from drainWindow, handoverMargin and startTimeout, is
// 20m; the shortest lease, 21m; and the one-slot handover's hour (a 10 min start wait, that
// lease, a second start wait, a start within startTimeout, 2 min of cleanup) is inside the
// partition's 1 h. A startTimeout of 15m, say, would make the hour 61m and fail here.
func TestShippedAthenaNowStatesItsLeadAndTheHourFits(t *testing.T) {
	prof, _ := shippedProfileFile(t, "athena-now.yaml")
	d := func(v *v1alpha1.Duration) time.Duration {
		if v == nil {
			t.Fatal("a margin is missing")
		}
		got, err := v.AsDuration()
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	drain, handover, start := d(prof.Spec.DrainWindow), d(prof.Spec.HandoverMargin), d(prof.Spec.Health.StartTimeout)
	if drain != 7*time.Minute || handover != time.Minute || start != 12*time.Minute {
		t.Fatalf("drainWindow %s, handoverMargin %s, startTimeout %s; want the ruled 7m, 1m and 12m", drain, handover, start)
	}
	lead := drain + handover + start
	if lead != 20*time.Minute {
		t.Errorf("the lead is %s, want 20m", lead)
	}
	lease := lead + time.Minute // whole minutes, and a time limit must be longer than the lead
	hold := 10*time.Minute + lease + 10*time.Minute + start + 2*time.Minute
	if lease != 21*time.Minute || hold != 55*time.Minute || hold > time.Hour {
		t.Errorf("shortest lease %s, one-slot hour %s; want 21m and 55m, inside 1h", lease, hold)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", "athena-now.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"= 20m", "21 minutes or more", "10 + 21 + 10 + 12 + 2 = 55 minutes", "NOT for real serving", "TEST deployments"} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("athena-now.yaml's header does not say %q", want)
		}
	}
}
