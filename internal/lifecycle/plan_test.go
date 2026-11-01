package lifecycle

import (
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The tests in this file need no rig. Every expected value is a literal worked
// out by hand from HBEP-7 (the arithmetic is in the comment above it); none is
// computed by the function under test or a sibling.

var planBase = time.Date(2026, 11, 2, 6, 0, 0, 0, time.UTC)

// planWaits builds queue waits from minute counts, in the order given. Each
// wait is submitted at planBase and starts that many minutes later.
func planWaits(minutes ...int) []v1alpha1.QueueWait {
	out := make([]v1alpha1.QueueWait, len(minutes))
	for i, m := range minutes {
		out[i] = v1alpha1.QueueWait{
			JobID:      "job-" + string(rune('a'+i)),
			SubmitTime: metav1.NewTime(planBase),
			StartTime:  metav1.NewTime(planBase.Add(time.Duration(m) * time.Minute)),
		}
	}
	return out
}

// TestP90Wait covers HBEP-7, Handover ("The p90 is taken over the last 20
// waits ... With fewer than 20, the largest observed wait is used") and open
// question 2 (nearest rank, ceil(0.9 n)-th smallest). The inputs are shuffled,
// so a missing sort shows.
func TestP90Wait(t *testing.T) {
	// 20 waits: 1..17 min, then 18, 50 and 60 min, shuffled. Sorted, the
	// 18th is 18 min, the 19th 50 min, the 20th 60 min. ceil(0.9 * 20) = 18,
	// so nearest rank gives 18 min. A 19th-value method would give 50 min and
	// linear interpolation (index 0.9 * 19 = 17.1 from zero) 18 + 0.1 * 32 =
	// 21.2 min, so the three methods are told apart.
	twenty := planWaits(60, 3, 17, 9, 1, 50, 12, 5, 16, 2, 18, 8, 14, 4, 11, 7, 15, 6, 13, 10)
	// 19 waits: 1..18 min, and 90 min in the middle of the input. Fewer than
	// 20, so the largest is used: 90 min (nearest rank would give the 18th
	// smallest, 18 min).
	nineteen := planWaits(9, 3, 17, 5, 1, 12, 7, 14, 90, 2, 18, 8, 15, 4, 11, 16, 6, 13, 10)
	// 25 waits (more than the status keeps, but the rule is defined for any
	// n >= 20): 1..22 min, then 30, 70 and 100 min, shuffled. ceil(0.9 * 25) =
	// ceil(22.5) = 23, the 23rd smallest is 30 min (the 22nd is 22, the 24th
	// is 70).
	twentyFive := planWaits(100, 5, 22, 9, 1, 70, 17, 12, 3, 21, 8, 15, 30, 2, 19, 11, 6, 14, 4, 20, 10, 16, 7, 13, 18)

	tests := []struct {
		name   string
		waits  []v1alpha1.QueueWait
		want   time.Duration
		wantOK bool
	}{
		{"none", nil, 0, false},
		{"one", planWaits(7), 7 * time.Minute, true},
		{"two take the larger", planWaits(20, 10), 20 * time.Minute, true},
		{"19 take the largest", nineteen, 90 * time.Minute, true},
		{"20 take the 18th smallest", twenty, 18 * time.Minute, true},
		{"25 take the 23rd smallest", twentyFive, 30 * time.Minute, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := p90Wait(tc.waits)
			if ok != tc.wantOK || got != tc.want {
				t.Fatalf("p90Wait = %s, %v; want %s, %v", got, ok, tc.want, tc.wantOK)
			}
		})
	}
	if got := len(twenty); got != 20 {
		t.Fatalf("fixture has %d waits, want 20", got)
	}
	if got := len(nineteen); got != 19 {
		t.Fatalf("fixture has %d waits, want 19", got)
	}
	if got := len(twentyFive); got != 25 {
		t.Fatalf("fixture has %d waits, want 25", got)
	}
}

// TestP90WaitDoesNotReorderItsInput checks that the planner sorts a copy: the
// Provider's status keeps its waits oldest first (HBEP-4).
func TestP90WaitDoesNotReorderItsInput(t *testing.T) {
	waits := planWaits(30, 10, 20)
	p90Wait(waits)
	for i, want := range []time.Duration{30 * time.Minute, 10 * time.Minute, 20 * time.Minute} {
		if got := waits[i].Wait(); got != want {
			t.Fatalf("waits[%d] = %s after p90Wait, want %s", i, got, want)
		}
	}
}

// TestReplacementTime covers HBEP-7, Handover, "When the replacement is
// submitted": the drain start minus (the p90 wait + the class's start-to-ready
// time + handoverMargin), never before the primary's start, and the primary's
// start with no wait observed.
func TestReplacementTime(t *testing.T) {
	drain := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	primary := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	const s2r, margin = 15 * time.Minute, 30 * time.Minute

	// 20 waits as in TestP90Wait: p90 = 18 min. 12:00 - (18 + 30 + 30) min =
	// 12:00 - 78 min = 10:42. Their drain start is 12:00 here.
	drain12 := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	twenty := planWaits(60, 3, 17, 9, 1, 50, 12, 5, 16, 2, 18, 8, 14, 4, 11, 7, 15, 6, 13, 10)

	tests := []struct {
		name  string
		drain time.Time
		waits []v1alpha1.QueueWait
		s2r   time.Duration
		want  time.Time
	}{
		// No wait observed: the primary's start (08:00).
		{"no waits", drain, nil, s2r, time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)},
		// Two waits, fewer than 20: the largest, 20 min. 10:00 - (20 + 15 + 30)
		// min = 10:00 - 65 min = 08:55.
		{"fewer than 20 use the largest", drain, planWaits(10, 20), s2r, time.Date(2026, 11, 2, 8, 55, 0, 0, time.UTC)},
		// 10:00 - (180 + 15 + 30) min = 10:00 - 225 min = 06:15, before the
		// primary's start 08:00: clamped to 08:00.
		{"clamped to the primary's start", drain, planWaits(180), s2r, time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)},
		// A plan landing exactly on the primary's start: 10:00 - (75 + 15 + 30)
		// min = 10:00 - 120 min = 08:00.
		{"lands exactly on the primary's start", drain, planWaits(75), s2r, time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)},
		// This row's start-to-ready time is 30 min: 12:00 - (18 + 30 + 30) min.
		{"20 waits use the 18th smallest", drain12, twenty, 30 * time.Minute, time.Date(2026, 11, 2, 10, 42, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := replacementTime(tc.drain, primary, tc.waits, tc.s2r, margin)
			if !got.Equal(tc.want) {
				t.Fatalf("replacementTime = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestLateAt covers HBEP-7, Handover, "Lateness": the predecessor's drain start
// minus the class's start-to-ready time. 10:00 - 30 min = 09:30.
func TestLateAt(t *testing.T) {
	drain := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	want := time.Date(2026, 11, 2, 9, 30, 0, 0, time.UTC)
	if got := lateAt(drain, 30*time.Minute); !got.Equal(want) {
		t.Fatalf("lateAt = %s, want %s", got, want)
	}
}

// TestTimeLimit covers HBEP-7, Provisioning: "timeLimit: the partition's
// maxTime, or leaseLimit when shorter", whole minutes, and (docs/development.md,
// Time limits) a limit below a minute is unusable.
func TestTimeLimit(t *testing.T) {
	d := func(v time.Duration) *time.Duration { return &v }
	tests := []struct {
		name          string
		maxT, lease   *time.Duration
		want          time.Duration
		wantOK        bool
		wantFormatted string
	}{
		{"leaseLimit shorter than maxTime", d(48 * time.Hour), d(4 * time.Hour), 4 * time.Hour, true, "4h0m0s"},
		{"maxTime shorter than leaseLimit", d(4 * time.Hour), d(48 * time.Hour), 4 * time.Hour, true, "4h0m0s"},
		{"maxTime only", d(48 * time.Hour), nil, 48 * time.Hour, true, "48h0m0s"},
		{"leaseLimit only", nil, d(4 * time.Hour), 4 * time.Hour, true, "4h0m0s"},
		// 4h0m59s rounds down to 4h0m0s.
		{"seconds are dropped", d(4*time.Hour + 59*time.Second), nil, 4 * time.Hour, true, "4h0m0s"},
		// min(48h, 1h30m45s) = 1h30m45s, down to 1h30m.
		{"min then round down", d(48 * time.Hour), d(time.Hour + 30*time.Minute + 45*time.Second), 90 * time.Minute, true, "1h30m0s"},
		{"exactly one minute", d(60 * time.Second), nil, time.Minute, true, "1m0s"},
		{"59 s is below a minute", d(59 * time.Second), nil, 0, false, ""},
		{"zero", d(0), nil, 0, false, ""},
		{"a lease below a minute wins the min", d(48 * time.Hour), d(30 * time.Second), 0, false, ""},
		{"neither", nil, nil, 0, false, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := timeLimit(tc.maxT, tc.lease)
			if ok != tc.wantOK {
				t.Fatalf("timeLimit ok = %v, want %v (got %s)", ok, tc.wantOK, got)
			}
			if !ok {
				return
			}
			if got != tc.want {
				t.Fatalf("timeLimit = %s, want %s", got, tc.want)
			}
			if f := formatMinutes(got); string(f) != tc.wantFormatted {
				t.Fatalf("formatMinutes(%s) = %q, want %q", got, f, tc.wantFormatted)
			}
		})
	}
}

// TestFormatMinutes: a whole-minute duration is written the way the API keeps
// it, and a stray second never reaches the Allocation's time limit.
func TestFormatMinutes(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want string
	}{
		{4 * time.Hour, "4h0m0s"},
		{90 * time.Minute, "1h30m0s"},
		{48 * time.Hour, "48h0m0s"},
		{time.Minute, "1m0s"},
		{4*time.Hour + 59*time.Second, "4h0m0s"},
	}
	for _, tc := range tests {
		if got := formatMinutes(tc.in); string(got) != tc.want {
			t.Errorf("formatMinutes(%s) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestDrainStartWindow covers HBEP-7, Test plan, "Drain window": on a profile
// with drainWindow 2h10m and drainMargin 10m, an Instance of a deployment with
// routing.maxRequestDuration 30 min drains 40 min before its hard end; one with
// 3 h drains 2 h 10 min before it. Hard end 2026-11-02T12:00:00Z.
func TestDrainStartWindow(t *testing.T) {
	end := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	profile := v1alpha1.PlatformProfileSpec{DrainWindow: dur(2*time.Hour + 10*time.Minute), DrainMargin: dur(10 * time.Minute)}
	routing := func(d time.Duration) *v1alpha1.Routing { return &v1alpha1.Routing{MaxRequestDuration: dur(d)} }

	tests := []struct {
		name    string
		profile v1alpha1.PlatformProfileSpec
		routing *v1alpha1.Routing
		want    time.Time
	}{
		// 30 min + 10 min margin = 40 min < 2h10m: window 40 min, 12:00 - 40
		// min = 11:20.
		{"30 min requests", profile, routing(30 * time.Minute), time.Date(2026, 11, 2, 11, 20, 0, 0, time.UTC)},
		// 3 h + 10 min = 3h10m, not shorter than 2h10m: window 2h10m, 12:00 -
		// 2h10m = 09:50.
		{"3 h requests", profile, routing(3 * time.Hour), time.Date(2026, 11, 2, 9, 50, 0, 0, time.UTC)},
		// No routing: the profile's window, 09:50.
		{"no routing", profile, nil, time.Date(2026, 11, 2, 9, 50, 0, 0, time.UTC)},
		// The schema defaults are 2h10m and 10m, so an empty profile gives the
		// same: 30 min -> 11:20.
		{"empty profile, 30 min requests", v1alpha1.PlatformProfileSpec{}, routing(30 * time.Minute), time.Date(2026, 11, 2, 11, 20, 0, 0, time.UTC)},
		// 2 h exactly + 10 min = 2h10m, equal to the window, not shorter:
		// window 2h10m, 09:50.
		{"requests as long as the window less the margin", profile, routing(2 * time.Hour), time.Date(2026, 11, 2, 9, 50, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := v1alpha1.DrainStart(end, tc.profile, tc.routing)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(tc.want) {
				t.Fatalf("DrainStart = %s, want %s", got, tc.want)
			}
		})
	}
}

// planProvider is a Provider view on the rig-like partition: 16 CPUs and 120Gi
// per GPU, 4 GPUs per node, maxTime 48h.
func planProvider() *providerView {
	return &providerView{
		name:    "helios-gpu",
		profile: &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: "helios"}},
		partition: v1alpha1.PartitionProfile{
			Name:             "gpu-part",
			AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "GH200", CPUArch: "aarch64"},
			MaxTime:          dur(48 * time.Hour), GPUsPerNode: ptrTo(int32(4)), CPUsPerGPU: ptrTo(int32(16)),
			MemPerGPU: ptrTo(resource.MustParse("120Gi")),
		},
		agentDigest: strings.Repeat("a", 64),
	}
}

// TestPartitionSizing covers HBEP-7, Provisioning and its Test plan item
// "Provisioning": the submission parameters follow the serving spec and the
// profile, with the time limit capped by leaseLimit.
func TestPartitionSizing(t *testing.T) {
	tests := []struct {
		name      string
		gpus      int32
		lease     *v1alpha1.Duration
		wantCPUs  int32
		wantMem   string
		wantLimit string
	}{
		// 2 GPUs: cpusPerTask 2 * 16 = 32, memory 2 * 120Gi = 240Gi; min(48h,
		// 4h) = 4h.
		{"two GPUs, 4 h lease", 2, dur(4 * time.Hour), 32, "240Gi", "4h0m0s"},
		// 1 GPU, no leaseLimit: 16 CPUs, 120Gi, the partition's 48h.
		{"one GPU, no lease limit", 1, nil, 16, "120Gi", "48h0m0s"},
		// 4 GPUs is the node's count, allowed: 64 CPUs, 480Gi.
		{"a full node", 4, dur(4 * time.Hour), 64, "480Gi", "4h0m0s"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := partitionSizing(planProvider(), tc.gpus, tc.lease)
			if err != nil {
				t.Fatal(err)
			}
			if got.Partition != "gpu-part" || got.GPUs != tc.gpus || got.Nodes != 1 {
				t.Errorf("partition/GPUs/nodes = %q/%d/%d, want gpu-part/%d/1", got.Partition, got.GPUs, got.Nodes, tc.gpus)
			}
			if got.CPUsPerTask != tc.wantCPUs {
				t.Errorf("cpusPerTask = %d, want %d", got.CPUsPerTask, tc.wantCPUs)
			}
			if want := resource.MustParse(tc.wantMem); got.Memory.Cmp(want) != 0 {
				t.Errorf("memory = %s, want %s", got.Memory.String(), tc.wantMem)
			}
			if string(got.TimeLimit) != tc.wantLimit {
				t.Errorf("timeLimit = %q, want %q", got.TimeLimit, tc.wantLimit)
			}
			if got.AgentDigest != strings.Repeat("a", 64) {
				t.Errorf("agentDigest = %q, want the Provider's build", got.AgentDigest)
			}
		})
	}
}

// TestPartitionSizingRefusesMoreGPUsThanANode: HBEP-7, Provisioning, "It must
// not exceed gpusPerNode, since alpha Instances are single-node". 5 GPUs on a
// 4-GPU node is refused as an unsupported spec, with no parameters.
func TestPartitionSizingRefusesMoreGPUsThanANode(t *testing.T) {
	got, err := partitionSizing(planProvider(), 5, dur(4*time.Hour))
	var u *unsupported
	if !errors.As(err, &u) {
		t.Fatalf("error = %v (%T), want *unsupported", err, err)
	}
	if got.Partition != "" || got.GPUs != 0 {
		t.Fatalf("parameters returned with the refusal: %+v", got)
	}
}

// TestPartitionSizingNeedsThePerGPUValues: a profile without the per-GPU values
// cannot size an Allocation. It is a profile problem, not an unsupported spec.
func TestPartitionSizingNeedsThePerGPUValues(t *testing.T) {
	pv := planProvider()
	pv.partition.CPUsPerGPU = nil
	_, err := partitionSizing(pv, 1, nil)
	if err == nil {
		t.Fatal("no error for a partition without cpusPerGpu")
	}
	var u *unsupported
	if errors.As(err, &u) {
		t.Fatalf("a profile problem is reported as an unsupported spec: %v", err)
	}
}
