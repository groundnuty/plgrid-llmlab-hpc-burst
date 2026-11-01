package v1alpha1

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

// The expected limits below are worked out by hand from HBEP-4's QOS limits:
// for each limit, the partition QOS's value if set, unless the job QOS has
// overPartQos, else the job QOS's; a limit neither sets is none. None is read
// back through EffectiveQOSLimits itself.

func qosSpec(qs ...QOSProfile) *PlatformProfileSpec {
	return &PlatformProfileSpec{ProviderKind: PlatformBatch, QOS: qs}
}

func qty(s string) *resource.Quantity { q := resource.MustParse(s); return &q }

// limitsWant is a hand-written expectation: nil means no limit.
type limitsWant struct {
	wall               time.Duration // 0: no limit
	submit, gpus, cpus *int32
	mem                string // "": no limit
	from               map[string]string
}

func checkLimits(t *testing.T, got QOSLimits, want limitsWant) {
	t.Helper()
	switch {
	case want.wall == 0 && got.MaxWall != nil:
		t.Errorf("maxWall = %v, want none", *got.MaxWall)
	case want.wall != 0 && (got.MaxWall == nil || *got.MaxWall != want.wall):
		t.Errorf("maxWall = %v, want %v", got.MaxWall, want.wall)
	}
	for _, c := range []struct {
		name      string
		got, want *int32
	}{{"maxSubmitPerUser", got.MaxSubmitPerUser, want.submit}, {"maxGpusPerUser", got.MaxGPUsPerUser, want.gpus}, {"maxCpusPerUser", got.MaxCPUsPerUser, want.cpus}} {
		switch {
		case c.want == nil && c.got != nil:
			t.Errorf("%s = %d, want none", c.name, *c.got)
		case c.want != nil && (c.got == nil || *c.got != *c.want):
			t.Errorf("%s = %v, want %d", c.name, c.got, *c.want)
		}
	}
	switch {
	case want.mem == "" && got.MaxMemPerUser != nil:
		t.Errorf("maxMemPerUser = %s, want none", got.MaxMemPerUser.String())
	case want.mem != "" && (got.MaxMemPerUser == nil || got.MaxMemPerUser.Cmp(resource.MustParse(want.mem)) != 0):
		t.Errorf("maxMemPerUser = %v, want %s", got.MaxMemPerUser, want.mem)
	}
	if len(got.From) != len(want.from) {
		t.Errorf("From = %v, want %v", got.From, want.from)
	}
	for k, v := range want.from {
		if got.From[k] != v {
			t.Errorf("From[%s] = %q, want %q", k, got.From[k], v)
		}
	}
}

func TestEffectiveQOSLimitsPrecedence(t *testing.T) {
	// Helios-shaped fixtures with example values: a partition QOS that sets a
	// submit limit only, and a job QOS that sets every limit.
	partQOS := QOSProfile{Name: "part-qos", MaxSubmitPerUser: ptr.To(int32(2000))}
	jobQOS := QOSProfile{Name: "fast", MaxWall: dur("12h"), MaxSubmitPerUser: ptr.To(int32(1)),
		MaxGPUsPerUser: ptr.To(int32(1)), MaxCPUsPerUser: ptr.To(int32(72)), MaxMemPerUser: qty("122400Mi")}
	jobOver := jobQOS
	jobOver.Name, jobOver.OverPartQOS = "fast-over", true
	partOver := partQOS
	partOver.Name, partOver.OverPartQOS = "part-over", true
	wallPart := QOSProfile{Name: "short-part", MaxWall: dur("1h")}
	plain := QOSProfile{Name: "plain"}
	capsPart := QOSProfile{Name: "caps-part", MaxGPUsPerUser: ptr.To(int32(4)), MaxMemPerUser: qty("200Gi")}
	capsJob := QOSProfile{Name: "caps-job", MaxWall: dur("6h"), MaxSubmitPerUser: ptr.To(int32(5)), MaxGPUsPerUser: ptr.To(int32(2)),
		MaxCPUsPerUser: ptr.To(int32(32)), MaxMemPerUser: qty("100Gi")}
	capsJobOver := capsJob
	capsJobOver.Name, capsJobOver.OverPartQOS = "caps-job-over", true
	gpuOnlyOver := QOSProfile{Name: "gpu-only-over", MaxGPUsPerUser: ptr.To(int32(1)), OverPartQOS: true}
	spec := qosSpec(partQOS, jobQOS, jobOver, partOver, wallPart, plain, capsPart, capsJob, capsJobOver, gpuOnlyOver)
	onPart := func(q string) PartitionProfile { return PartitionProfile{Name: "example-gpu", QOS: q} }

	for _, tc := range []struct {
		name      string
		partition PartitionProfile
		job       string
		want      limitsWant
	}{
		{
			// The GH200 case: the partition QOS's 2,000 jobs come first;
			// the wall limit and the per-user caps, which it does not set,
			// come from the job QOS.
			name: "partition QOS first, the job QOS fills what it does not set", partition: onPart("part-qos"), job: "fast",
			want: limitsWant{wall: 12 * time.Hour, submit: ptr.To(int32(2000)), gpus: ptr.To(int32(1)), cpus: ptr.To(int32(72)), mem: "122400Mi",
				from: map[string]string{"maxWall": "fast", "maxSubmitPerUser": "part-qos", "maxGpusPerUser": "fast", "maxCpusPerUser": "fast", "maxMemPerUser": "fast"}},
		},
		{
			// The other order: overPartQos on the job QOS puts its 1 first.
			name: "overPartQos on the job QOS reverses the order", partition: onPart("part-qos"), job: "fast-over",
			want: limitsWant{wall: 12 * time.Hour, submit: ptr.To(int32(1)), gpus: ptr.To(int32(1)), cpus: ptr.To(int32(72)), mem: "122400Mi",
				from: map[string]string{"maxWall": "fast-over", "maxSubmitPerUser": "fast-over", "maxGpusPerUser": "fast-over", "maxCpusPerUser": "fast-over", "maxMemPerUser": "fast-over"}},
		},
		{
			// Slurm reads the flag on the job's QOS only.
			name: "overPartQos on the partition QOS changes nothing", partition: onPart("part-over"), job: "fast",
			want: limitsWant{wall: 12 * time.Hour, submit: ptr.To(int32(2000)), gpus: ptr.To(int32(1)), cpus: ptr.To(int32(72)), mem: "122400Mi",
				from: map[string]string{"maxWall": "fast", "maxSubmitPerUser": "part-over", "maxGpusPerUser": "fast", "maxCpusPerUser": "fast", "maxMemPerUser": "fast"}},
		},
		{
			name: "no partition QOS: the job QOS's limits", partition: onPart(""), job: "fast",
			want: limitsWant{wall: 12 * time.Hour, submit: ptr.To(int32(1)), gpus: ptr.To(int32(1)), cpus: ptr.To(int32(72)), mem: "122400Mi",
				from: map[string]string{"maxWall": "fast", "maxSubmitPerUser": "fast", "maxGpusPerUser": "fast", "maxCpusPerUser": "fast", "maxMemPerUser": "fast"}},
		},
		{
			// No Provider qos: the association's default applies, which the
			// profile cannot know, so only the partition QOS's limits count.
			name: "no job QOS: the partition QOS's limits only", partition: onPart("part-qos"), job: "",
			want: limitsWant{submit: ptr.To(int32(2000)), from: map[string]string{"maxSubmitPerUser": "part-qos"}},
		},
		{
			name: "neither QOS: no limits", partition: onPart(""), job: "",
			want: limitsWant{from: map[string]string{}},
		},
		{
			// A partition QOS's wall limit applies whatever the job QOS sets.
			name: "a partition QOS's maxWall comes before the job QOS's", partition: onPart("short-part"), job: "fast",
			want: limitsWant{wall: time.Hour, submit: ptr.To(int32(1)), gpus: ptr.To(int32(1)), cpus: ptr.To(int32(72)), mem: "122400Mi",
				from: map[string]string{"maxWall": "short-part", "maxSubmitPerUser": "fast", "maxGpusPerUser": "fast", "maxCpusPerUser": "fast", "maxMemPerUser": "fast"}},
		},
		{
			name: "with overPartQos the job QOS's maxWall comes first", partition: onPart("short-part"), job: "fast-over",
			want: limitsWant{wall: 12 * time.Hour, submit: ptr.To(int32(1)), gpus: ptr.To(int32(1)), cpus: ptr.To(int32(72)), mem: "122400Mi",
				from: map[string]string{"maxWall": "fast-over", "maxSubmitPerUser": "fast-over", "maxGpusPerUser": "fast-over", "maxCpusPerUser": "fast-over", "maxMemPerUser": "fast-over"}},
		},
		{
			name: "a QOS listed with no limits sets none", partition: onPart("plain"), job: "plain",
			want: limitsWant{from: map[string]string{}},
		},
		{
			// Both QOSes set the GPU and memory caps, with different values:
			// the partition QOS's win, and the job QOS fills the rest.
			name: "conflicting GPU and memory caps: the partition QOS's", partition: onPart("caps-part"), job: "caps-job",
			want: limitsWant{wall: 6 * time.Hour, submit: ptr.To(int32(5)), gpus: ptr.To(int32(4)), cpus: ptr.To(int32(32)), mem: "200Gi",
				from: map[string]string{"maxWall": "caps-job", "maxSubmitPerUser": "caps-job", "maxGpusPerUser": "caps-part", "maxCpusPerUser": "caps-job", "maxMemPerUser": "caps-part"}},
		},
		{
			name: "conflicting caps with overPartQos: the job QOS's", partition: onPart("caps-part"), job: "caps-job-over",
			want: limitsWant{wall: 6 * time.Hour, submit: ptr.To(int32(5)), gpus: ptr.To(int32(2)), cpus: ptr.To(int32(32)), mem: "100Gi",
				from: map[string]string{"maxWall": "caps-job-over", "maxSubmitPerUser": "caps-job-over", "maxGpusPerUser": "caps-job-over", "maxCpusPerUser": "caps-job-over", "maxMemPerUser": "caps-job-over"}},
		},
		{
			// overPartQos puts the job QOS first, but a limit it does not set
			// still comes from the partition QOS.
			name: "overPartQos: a limit the job QOS lacks falls back to the partition QOS", partition: onPart("part-qos"), job: "gpu-only-over",
			want: limitsWant{submit: ptr.To(int32(2000)), gpus: ptr.To(int32(1)),
				from: map[string]string{"maxSubmitPerUser": "part-qos", "maxGpusPerUser": "gpu-only-over"}},
		},
		{
			name: "the same QOS on the partition and the job", partition: onPart("fast"), job: "fast",
			want: limitsWant{wall: 12 * time.Hour, submit: ptr.To(int32(1)), gpus: ptr.To(int32(1)), cpus: ptr.To(int32(72)), mem: "122400Mi",
				from: map[string]string{"maxWall": "fast", "maxSubmitPerUser": "fast", "maxGpusPerUser": "fast", "maxCpusPerUser": "fast", "maxMemPerUser": "fast"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := EffectiveQOSLimits(spec, tc.partition, tc.job)
			if err != nil {
				t.Fatalf("EffectiveQOSLimits: %v", err)
			}
			checkLimits(t, got, tc.want)
		})
	}
}

// A limit that only the job QOS defines applies even though a partition QOS
// comes first in the order (HBEP-7, Test plan, Which limit applies: "The GPU
// cap comes from the Provider's QOS when the partition QOS sets none").
func TestEffectiveQOSLimitsTakesALimitOnlyTheJobQOSDefines(t *testing.T) {
	spec := qosSpec(
		QOSProfile{Name: "part-qos", MaxSubmitPerUser: ptr.To(int32(2000)), MaxCPUsPerUser: ptr.To(int32(500))},
		QOSProfile{Name: "job-qos", MaxGPUsPerUser: ptr.To(int32(1)), MaxCPUsPerUser: ptr.To(int32(16))},
	)
	got, err := EffectiveQOSLimits(spec, PartitionProfile{Name: "p", QOS: "part-qos"}, "job-qos")
	if err != nil {
		t.Fatal(err)
	}
	checkLimits(t, got, limitsWant{submit: ptr.To(int32(2000)), gpus: ptr.To(int32(1)), cpus: ptr.To(int32(500)),
		from: map[string]string{"maxSubmitPerUser": "part-qos", "maxGpusPerUser": "job-qos", "maxCpusPerUser": "part-qos"}})
}

func TestEffectiveQOSLimitsRefusesAnUnlistedQOS(t *testing.T) {
	spec := qosSpec(QOSProfile{Name: "listed"})
	if _, err := EffectiveQOSLimits(spec, PartitionProfile{Name: "p", QOS: "missing"}, "listed"); err == nil || !strings.Contains(err.Error(), "partitions[p].qos") {
		t.Errorf("an unlisted partition QOS: err = %v, want one naming partitions[p].qos", err)
	}
	if _, err := EffectiveQOSLimits(spec, PartitionProfile{Name: "p"}, "missing"); err == nil || !strings.Contains(err.Error(), "the Provider's qos") {
		t.Errorf("an unlisted job QOS: err = %v, want one naming the Provider's qos", err)
	}
}

// The result shares no memory with the profile: changing it leaves the profile
// as it was.
func TestEffectiveQOSLimitsCopies(t *testing.T) {
	spec := qosSpec(QOSProfile{Name: "q", MaxSubmitPerUser: ptr.To(int32(3)), MaxMemPerUser: qty("1Gi")})
	got, err := EffectiveQOSLimits(spec, PartitionProfile{Name: "p"}, "q")
	if err != nil {
		t.Fatal(err)
	}
	*got.MaxSubmitPerUser = 99
	got.MaxMemPerUser.Add(resource.MustParse("1Gi"))
	if *spec.QOS[0].MaxSubmitPerUser != 3 || spec.QOS[0].MaxMemPerUser.Cmp(resource.MustParse("1Gi")) != 0 {
		t.Errorf("the profile changed: submit %d, mem %s", *spec.QOS[0].MaxSubmitPerUser, spec.QOS[0].MaxMemPerUser.String())
	}
}

func TestAllocationTimeLimit(t *testing.T) {
	h := func(n float64) *time.Duration { d := time.Duration(n * float64(time.Hour)); return &d }
	for _, tc := range []struct {
		name                    string
		maxTime, maxWall, lease *time.Duration
		want                    time.Duration
		bound                   TimeLimitBound
		ok                      bool
	}{
		// HBEP-7's Test plan, QOS time limit, worked out by hand.
		{"48 h partition under a 12 h QOS", h(48), h(12), nil, 12 * time.Hour, TimeLimitMaxWall, true},
		{"48 h partition under a 72 h QOS", h(48), h(72), nil, 48 * time.Hour, TimeLimitMaxTime, true},
		{"48 h partition with no QOS", h(48), nil, nil, 48 * time.Hour, TimeLimitMaxTime, true},
		{"a 4 h lease under the 12 h QOS", h(48), h(12), h(4), 4 * time.Hour, TimeLimitLeaseLimit, true},
		{"Athena plgrid-now: 1 h partition, QOS without a wall limit", h(1), nil, nil, time.Hour, TimeLimitMaxTime, true},
		{"a lease alone", nil, nil, h(3), 3 * time.Hour, TimeLimitLeaseLimit, true},
		{"a wall limit alone", nil, h(2), nil, 2 * time.Hour, TimeLimitMaxWall, true},
		// Ties name the first of maxTime, maxWall, leaseLimit.
		{"maxTime equal to maxWall", h(12), h(12), nil, 12 * time.Hour, TimeLimitMaxTime, true},
		{"maxWall equal to leaseLimit", h(48), h(4), h(4), 4 * time.Hour, TimeLimitMaxWall, true},
		// Whole minutes: 2 h 30 min 59 s rounds down to 2 h 30 min.
		{"rounded down to whole minutes", nil, nil, ptr.To(2*time.Hour + 30*time.Minute + 59*time.Second), 2*time.Hour + 30*time.Minute, TimeLimitLeaseLimit, true},
		{"none set", nil, nil, nil, 0, "", false},
		{"below one minute", nil, nil, ptr.To(59 * time.Second), 0, TimeLimitLeaseLimit, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, bound, ok := AllocationTimeLimit(tc.maxTime, tc.maxWall, tc.lease)
			if ok != tc.ok || bound != tc.bound || (tc.ok && got != tc.want) {
				t.Errorf("AllocationTimeLimit = %v, %q, %v; want %v, %q, %v", got, bound, ok, tc.want, tc.bound, tc.ok)
			}
		})
	}
}

func TestMaxAllocationGPUs(t *testing.T) {
	part := func(n *int32) PartitionProfile { return PartitionProfile{Name: "p", GPUsPerNode: n} }
	for _, tc := range []struct {
		name   string
		part   PartitionProfile
		limits QOSLimits
		want   int32
		ok     bool
	}{
		// Athena plgrid-now: 8 GPUs per node under a QOS of one GPU per user.
		{"a cap below gpusPerNode is the bound", part(ptr.To(int32(8))), QOSLimits{MaxGPUsPerUser: ptr.To(int32(1))}, 1, true},
		{"a cap above gpusPerNode leaves the node's count", part(ptr.To(int32(4))), QOSLimits{MaxGPUsPerUser: ptr.To(int32(8))}, 4, true},
		{"no cap: gpusPerNode", part(ptr.To(int32(4))), QOSLimits{}, 4, true},
		{"a cap of zero", part(ptr.To(int32(4))), QOSLimits{MaxGPUsPerUser: ptr.To(int32(0))}, 0, true},
		// The QOS cap bounds the request without gpusPerNode (review of PR #155, N4).
		{"no gpusPerNode: the QOS cap", part(nil), QOSLimits{MaxGPUsPerUser: ptr.To(int32(1))}, 1, true},
		{"neither: no bound", part(nil), QOSLimits{}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := MaxAllocationGPUs(tc.part, tc.limits)
			if got != tc.want || ok != tc.ok {
				t.Errorf("MaxAllocationGPUs = %d, %v; want %d, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}
