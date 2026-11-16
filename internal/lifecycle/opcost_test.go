package lifecycle

// The release cost (HBEP-10, The release cost; #589, PR 4, AC 2). Every figure is worked out by hand from
// the literal times of opfixture_test.go: the plan is made at T0+1h (10:00) and the hard end is T0+4h
// (13:00). A figure that cannot be known is unknown, with its reason, and never a zero.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func startedAlloc(gpus int32, hardEnd *time.Time) *v1alpha1.Allocation {
	a := &v1alpha1.Allocation{}
	a.Spec.ForProvider.Batch = &v1alpha1.BatchParameters{GPUs: gpus}
	a.Status.AtProvider.StartTime = opTime(opT0)
	a.Status.AtProvider.HardEnd = optTimePtr(hardEnd)
	return a
}

func pendingAlloc(submit, estimate *time.Time) *v1alpha1.Allocation {
	a := &v1alpha1.Allocation{}
	a.Status.AtProvider.SubmitTime = optTimePtr(submit)
	a.Status.AtProvider.StartEstimate = optTimePtr(estimate)
	return a
}

func TestTheLeaseLeftAndItsGPUHours(t *testing.T) {
	for _, tc := range []struct {
		name    string
		hardEnd time.Time
		gpus    int32
		left    time.Duration
		hours   string
	}{
		// 13:00 - 10:00 = 3h, times 4 GPUs = 12 GPU-hours.
		{"three hours on four GPUs", opHardEnd, 4, 3 * time.Hour, "12.0"},
		// 12:20 - 10:00 = 2h20m, times 2 GPUs = 4h40m = 4.67 GPU-hours, to one decimal 4.7.
		{"2h20m on two GPUs rounds to one decimal", opT0.Add(3*time.Hour + 20*time.Minute), 2, 2*time.Hour + 20*time.Minute, "4.7"},
		// 11:30 - 10:00 = 1h30m, times 3 GPUs = 4.5 GPU-hours.
		{"1h30m on three GPUs", opT0.Add(150 * time.Minute), 3, 90 * time.Minute, "4.5"},
		// 9:30 is before 10:00: the lease is over, so nothing is left. The hard end is known, so zero is the value.
		{"a hard end that has passed", opT0.Add(30 * time.Minute), 4, 0, "0.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := releaseCost(costInput{at: opAt, alloc: startedAlloc(tc.gpus, tp(tc.hardEnd))})
			if c.LeaseLeft == nil || c.LeaseLeft.Duration == nil || c.LeaseLeft.Duration.Duration != tc.left || c.LeaseLeft.Unknown != "" {
				t.Errorf("lease left %+v, want %s", c.LeaseLeft, tc.left)
			}
			if c.GPUHoursLeft == nil || c.GPUHoursLeft.GPUHours != tc.hours || c.GPUHoursLeft.Unknown != "" {
				t.Errorf("GPU-hours left %+v, want %s", c.GPUHoursLeft, tc.hours)
			}
			if c.LeaseLeft.ObservedAt == nil || !c.LeaseLeft.ObservedAt.Time.Equal(opAt) {
				t.Errorf("the lease figure's observedAt is %v, want the plan's time %v", c.LeaseLeft.ObservedAt, opAt)
			}
			if c.QueueAge != nil || c.StartEstimate != nil {
				t.Error("a started Allocation has no queue age and no start estimate")
			}
		})
	}
}

func TestAnUnobservedHardEndIsUnknownNotZero(t *testing.T) {
	c := releaseCost(costInput{at: opAt, alloc: startedAlloc(4, nil)})
	want := "the job's hard end is not observed yet"
	if c.LeaseLeft.Duration != nil || c.LeaseLeft.Unknown != want {
		t.Errorf("lease left %+v, want unknown %q and no duration", c.LeaseLeft, want)
	}
	if c.GPUHoursLeft.GPUHours != "" || c.GPUHoursLeft.Unknown != want {
		t.Errorf("GPU-hours left %+v, want unknown %q and no value", c.GPUHoursLeft, want)
	}
}

func TestAPendingAllocationsQueueAgeAndEstimate(t *testing.T) {
	// Submitted at 9:10, observed at 10:00: 50 minutes in the queue. The estimate is 11:00, advisory.
	c := releaseCost(costInput{at: opAt, alloc: pendingAlloc(tp(opT0.Add(10*time.Minute)), tp(opT0.Add(2*time.Hour)))})
	if c.QueueAge == nil || c.QueueAge.Duration == nil || c.QueueAge.Duration.Duration != 50*time.Minute {
		t.Errorf("queue age %+v, want 50m", c.QueueAge)
	}
	if c.StartEstimate == nil || c.StartEstimate.Time == nil || !c.StartEstimate.Time.Time.Equal(opT0.Add(2*time.Hour)) || !c.StartEstimate.Advisory {
		t.Errorf("start estimate %+v, want 11:00, advisory", c.StartEstimate)
	}
	if c.LeaseLeft != nil || c.GPUHoursLeft != nil {
		t.Error("a pending Allocation has no lease left")
	}
	// No submit time, no estimate.
	c = releaseCost(costInput{at: opAt, alloc: pendingAlloc(nil, nil)})
	if c.QueueAge.Duration != nil || c.QueueAge.Unknown != "the scheduler has not accepted the job yet" {
		t.Errorf("queue age with no submit time %+v, want unknown with its reason", c.QueueAge)
	}
	if c.StartEstimate.Time != nil || c.StartEstimate.Unknown != "the platform gives no start estimate for the job" || !c.StartEstimate.Advisory {
		t.Errorf("start estimate with none %+v, want unknown, advisory", c.StartEstimate)
	}
}

func waitsOf(minutes ...int) []v1alpha1.QueueWait {
	var out []v1alpha1.QueueWait
	for i, m := range minutes {
		out = append(out, v1alpha1.QueueWait{
			JobID: string(rune('a' + i)), SubmitTime: metav1.NewTime(opT0), StartTime: metav1.NewTime(opT0.Add(time.Duration(m) * time.Minute)),
		})
	}
	return out
}

func providerWith(waits []v1alpha1.QueueWait) *v1alpha1.Provider {
	p := &v1alpha1.Provider{}
	p.Status.QueueWaits = waits
	p.Status.LastSuccessfulStatusRead = opTime(opT0.Add(55 * time.Minute))
	return p
}

func TestTheProvidersQueueWaits(t *testing.T) {
	// Twenty waits, 1 to 20 minutes, in a scrambled order: sorted they are 1..20. The median of an even
	// count is the mean of the 10th and 11th, 10m and 11m, so 10m30s. The p90 by nearest rank is the
	// ceil(0.9 * 20) = 18th, 18m. The largest is 20m.
	scrambled := []int{7, 1, 20, 13, 4, 17, 10, 2, 19, 15, 6, 12, 3, 18, 9, 14, 5, 16, 8, 11}
	for _, tc := range []struct {
		name             string
		minutes          []int
		median, p90, max time.Duration
		count            int32
	}{
		{"twenty waits", scrambled, 10*time.Minute + 30*time.Second, 18 * time.Minute, 20 * time.Minute, 20},
		// Three waits, 5, 15 and 25 minutes: the median is the middle one, 15m; the p90 is the ceil(2.7) = 3rd, 25m.
		{"three waits, the p90 is the third", []int{25, 5, 15}, 15 * time.Minute, 25 * time.Minute, 25 * time.Minute, 3},
		// Two waits, 10 and 20: the median is 15m; the p90 is the ceil(1.8) = 2nd, 20m.
		{"two waits", []int{20, 10}, 15 * time.Minute, 20 * time.Minute, 20 * time.Minute, 2},
		// Ten waits, 1 to 10: the p90 is the ceil(9) = 9th, 9m, not the largest; the median is the mean of 5m and 6m.
		{"ten waits, the p90 is not the largest", []int{3, 1, 2, 4, 5, 6, 7, 8, 9, 10}, 5*time.Minute + 30*time.Second, 9 * time.Minute, 10 * time.Minute, 10},
		{"one wait", []int{12}, 12 * time.Minute, 12 * time.Minute, 12 * time.Minute, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := releaseCost(costInput{at: opAt, alloc: startedAlloc(1, tp(opHardEnd)), prov: providerWith(waitsOf(tc.minutes...))})
			q := c.QueueWaits
			if q == nil || q.Median == nil || q.P90 == nil || q.Max == nil {
				t.Fatalf("queue waits %+v", q)
			}
			if q.Median.Duration != tc.median || q.P90.Duration != tc.p90 || q.Max.Duration != tc.max || q.Count != tc.count {
				t.Errorf("median %s, p90 %s, max %s, count %d; want %s, %s, %s, %d", q.Median.Duration, q.P90.Duration, q.Max.Duration, q.Count, tc.median, tc.p90, tc.max, tc.count)
			}
			if q.ObservedAt == nil || !q.ObservedAt.Time.Equal(opT0.Add(55*time.Minute)) {
				t.Errorf("observedAt %v, want the Provider's last successful read, 9:55", q.ObservedAt)
			}
		})
	}
}

func TestQueueWaitsThatAreNotKnownAreUnknown(t *testing.T) {
	c := releaseCost(costInput{at: opAt, alloc: startedAlloc(1, tp(opHardEnd)), prov: providerWith(nil)})
	if q := c.QueueWaits; q.Median != nil || q.P90 != nil || q.Max != nil || q.Count != 0 || q.Unknown != "the Provider has recorded no queue waits yet" {
		t.Errorf("no waits recorded: %+v, want unknown with its reason and no figure", q)
	}
	c = releaseCost(costInput{at: opAt, alloc: startedAlloc(1, tp(opHardEnd))})
	if q := c.QueueWaits; q.Median != nil || q.Unknown != "the Provider is not read, so its queue waits are not known" {
		t.Errorf("no Provider: %+v, want unknown with its reason", q)
	}
}

func TestStartToReadyIsTheClassesOwnFigure(t *testing.T) {
	h100 := v1alpha1.AcceleratorClass{GPUModel: "H100", CPUArch: "x86_64"}
	gh200 := v1alpha1.AcceleratorClass{GPUModel: "GH200", CPUArch: "aarch64"}
	list := []v1alpha1.ClassStartToReady{{AcceleratorClass: gh200, Duration: "7m0s", MeasuredTime: metav1.NewTime(opT0)}, {AcceleratorClass: h100, Duration: "12m0s", MeasuredTime: metav1.NewTime(opT0.Add(30 * time.Minute))}}
	c := releaseCost(costInput{at: opAt, alloc: startedAlloc(1, tp(opHardEnd)), class: &h100, startToReady: list})
	f := c.StartToReady
	if f.Duration == nil || f.Duration.Duration != 12*time.Minute || !f.ObservedAt.Time.Equal(opT0.Add(30*time.Minute)) {
		t.Errorf("start-to-ready %+v, want the class's own 12m measured at 9:30", f)
	}
	const unknown = "not measured yet; the controller plans with 30 min"
	for name, in := range map[string]costInput{
		"no entry for the class": {class: &v1alpha1.AcceleratorClass{GPUModel: "A100", CPUArch: "x86_64"}, startToReady: list},
		"no class at all":        {startToReady: list},
		"an empty list":          {class: &h100},
	} {
		in.at, in.alloc = opAt, startedAlloc(1, tp(opHardEnd))
		if f := releaseCost(in).StartToReady; f.Duration != nil || f.Unknown != unknown {
			t.Errorf("%s: %+v, want unknown %q", name, f, unknown)
		}
	}
}

func TestTheEstimateIsAlwaysUnknownAndAdvisory(t *testing.T) {
	const want = "the estimate operation is beta step 4 (HBEP-4 amendment pending)"
	for name, in := range map[string]costInput{
		"a started Allocation": {alloc: startedAlloc(1, tp(opHardEnd))},
		"a pending Allocation": {alloc: pendingAlloc(tp(opT0), nil)},
		"no Allocation":        {},
	} {
		in.at = opAt
		e := releaseCost(in).EstimateNow
		if e == nil || e.Time != nil || e.Unknown != want || !e.Advisory {
			t.Errorf("%s: estimate %+v, want unknown %q, advisory", name, e, want)
		}
	}
}

func TestTheLastReadyInstanceIsMarked(t *testing.T) {
	if !releaseCost(costInput{at: opAt, lastReady: true}).LastReady {
		t.Error("lastReady is set for the last ready Instance")
	}
	if releaseCost(costInput{at: opAt}).LastReady {
		t.Error("lastReady is not set for any other target")
	}
}
