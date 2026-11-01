package lifecycle

// Boundary tests of the no-overlap decision (HBEP-7, Handover, No-overlap mode;
// Provisioning, Per-user limits). The mode holds exactly when no allowed
// Provider can take a primary's replacement beside it, so each limit has an
// exact-fit case, where two Instances still fit and the replacement runs
// beside the primary (an ordinary handover, with ReplacementLate when it is
// late), and a case one unit over, where they do not. Each limit stands alone:
// one QOS sets nothing else.
//
// The rows grew out of the probes of the review of PR #172, which found that
// the mutants "the existing replacement is not skipped in the slot count", "a
// slot test of < for <=", "the GPU test of >= for >" and "the CPU or memory
// test removed" all passed the suite.

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// TestCapsExceeded is the decision's arithmetic alone: an Instance of the
// rig's size (one GPU, 16 CPUs, 120Gi) beside its replacement of the same
// size, against each cap on its own, at the exact fit and one unit over.
// Expected values are written out by hand: 2 GPUs, 32 CPUs, 240Gi.
func TestCapsExceeded(t *testing.T) {
	inst := v1alpha1.BatchParameters{GPUs: 1, CPUsPerTask: 16, Memory: resource.MustParse("120Gi")}
	from := map[string]string{
		v1alpha1.LimitMaxGPUsPerUser: "example-gpu", v1alpha1.LimitMaxCPUsPerUser: "example-cpu", v1alpha1.LimitMaxMemPerUser: "example-mem",
	}
	for _, tc := range []struct {
		name   string
		limits v1alpha1.QOSLimits
		want   []string // the message holds each; none: the Instances fit
	}{
		{name: "no limit at all", limits: v1alpha1.QOSLimits{}},
		{name: "GPUs 2 fits exactly", limits: v1alpha1.QOSLimits{MaxGPUsPerUser: i32(2), From: from}},
		{name: "GPUs 1 is one over", limits: v1alpha1.QOSLimits{MaxGPUsPerUser: i32(1), From: from},
			want: []string{"need 2 GPUs, above the maxGpusPerUser 1 of QOS example-gpu"}},
		{name: "CPUs 32 fits exactly", limits: v1alpha1.QOSLimits{MaxCPUsPerUser: i32(32), From: from}},
		{name: "CPUs 31 is one over", limits: v1alpha1.QOSLimits{MaxCPUsPerUser: i32(31), From: from},
			want: []string{"need 32 CPUs, above the maxCpusPerUser 31 of QOS example-cpu"}},
		{name: "memory 240Gi fits exactly", limits: v1alpha1.QOSLimits{MaxMemPerUser: memQ("240Gi"), From: from}},
		{name: "memory 239Gi is one Gi over", limits: v1alpha1.QOSLimits{MaxMemPerUser: memQ("239Gi"), From: from},
			want: []string{"240Gi of memory, above the maxMemPerUser 239Gi of QOS example-mem"}},
		{name: "all three over name all three", limits: v1alpha1.QOSLimits{MaxGPUsPerUser: i32(1), MaxCPUsPerUser: i32(16), MaxMemPerUser: memQ("120Gi"), From: from},
			want: []string{"2 GPUs, above the maxGpusPerUser 1", "32 CPUs, above the maxCpusPerUser 16", "240Gi of memory, above the maxMemPerUser 120Gi", "on Provider example-provider"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := capsExceeded(&providerView{name: "example-provider", limits: tc.limits}, "example-instance", inst, inst)
			if len(tc.want) == 0 {
				if got != "" {
					t.Fatalf("capsExceeded = %q, want none: the two Instances fit", got)
				}
				return
			}
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Fatalf("capsExceeded = %q, want it to contain %q", got, w)
				}
			}
		})
	}
}

// boundaryCase is one limit of a Provider's QOS on its own.
type boundaryCase struct {
	name string
	qos  v1alpha1.QOSProfile
	// noOverlap: the two Instances do not fit, so the deployment is in
	// no-overlap mode for the primary. queued: a submit slot is free, only a
	// per-user cap blocks, so the replacement is created at its planned time
	// and pends; without it, no slot is free and none is created before the
	// primary's job leaves it.
	noOverlap, queued bool
	// why is the text the NoOverlapSlot message holds.
	why string
}

// TestNoOverlapModeBoundaries runs HBEP-7's timeline of handover_test.go (the
// primary's drain start 09:50:20, its replacement planned at 09:19:30, late at
// 09:49:30) for a Provider whose QOS sets one limit. The replacement waits
// 3 h in the queue when it is submitted, so it never starts in the test.
//   - In the overlap rows the replacement exists from 09:19:30, unmarked; it
//     is late at 09:49:30 (ReplacementLate); and at the drain start the
//     deployment is not in the mode.
//   - In the no-overlap rows there is no ReplacementLate at 09:49:30, and at
//     09:50:30 the condition is NoOverlapSlot naming the limit; the queued rows
//     have the replacement from 09:19:30, marked as a successor.
func TestNoOverlapModeBoundaries(t *testing.T) {
	for _, tc := range []boundaryCase{
		// maxSubmitPerUser 2: the primary's slot and one more. The replacement
		// holds the second slot itself, so it must not count against its own.
		{name: "submit limit 2 leaves one slot beside the primary", qos: v1alpha1.QOSProfile{Name: "example-s2", MaxSubmitPerUser: i32(2)}},
		{name: "submit limit 1 leaves none", qos: v1alpha1.QOSProfile{Name: "example-s1", MaxSubmitPerUser: i32(1)},
			noOverlap: true, why: "no free submit slot for a replacement: 1 Allocations not ended, at its maxSubmitPerUser 1 of QOS example-s1"},
		{name: "GPU cap 2 fits one GPU and one GPU", qos: v1alpha1.QOSProfile{Name: "example-g2", MaxGPUsPerUser: i32(2)}},
		{name: "GPU cap 1 does not", qos: v1alpha1.QOSProfile{Name: "example-g1", MaxGPUsPerUser: i32(1)},
			noOverlap: true, queued: true, why: "need 2 GPUs, above the maxGpusPerUser 1 of QOS example-g1"},
		{name: "CPU cap 32 fits 16 and 16", qos: v1alpha1.QOSProfile{Name: "example-c32", MaxCPUsPerUser: i32(32)}},
		{name: "CPU cap 31 does not", qos: v1alpha1.QOSProfile{Name: "example-c31", MaxCPUsPerUser: i32(31)},
			noOverlap: true, queued: true, why: "need 32 CPUs, above the maxCpusPerUser 31 of QOS example-c31"},
		{name: "memory cap 240Gi fits 120Gi and 120Gi", qos: v1alpha1.QOSProfile{Name: "example-m240", MaxMemPerUser: memQ("240Gi")}},
		{name: "memory cap 239Gi does not", qos: v1alpha1.QOSProfile{Name: "example-m239", MaxMemPerUser: memQ("239Gi")},
			noOverlap: true, queued: true, why: "240Gi of memory, above the maxMemPerUser 239Gi of QOS example-m239"},
	} {
		t.Run(tc.name, func(t *testing.T) { runBoundaryCase(t, tc) })
	}
}

func runBoundaryCase(t *testing.T, tc boundaryCase) {
	rg := newRig(t, append(qosOpts([]v1alpha1.QOSProfile{tc.qos}, "", tc.qos.Name), withMaxTime(4*time.Hour))...)
	// The primary starts at once; whatever is submitted after it waits 3 h.
	rg.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 3 * time.Hour})
	p := hoStart(rg, nil)

	// 09:19:30, the replacement's planned time.
	hoStepTo(rg, hoT(9, 19, 20), time.Minute, nil)
	hoStepTo(rg, hoT(9, 19, 40), 10*time.Second, nil)
	reps := hoRepl(rg, p.Name)
	wantReps := 1
	if tc.noOverlap && !tc.queued {
		wantReps = 0 // the one-slot path holds the successor back
	}
	if len(reps) != wantReps {
		t.Fatalf("%d replacements at 09:19:40, want %d", len(reps), wantReps)
	}
	if wantReps == 1 {
		if marked := reps[0].Annotations[annotationNoOverlapSuccessor] != ""; marked != tc.noOverlap {
			t.Fatalf("the replacement's no-overlap mark is %v, want %v", marked, tc.noOverlap)
		}
	}

	// 09:49:30, the replacement's lateness deadline: late beside the primary,
	// a planned gap in the mode.
	hoStepTo(rg, hoT(9, 49, 20), time.Minute, nil)
	hoStepTo(rg, hoT(9, 49, 30), 10*time.Second, nil)
	c := rg.condition(hoMD, v1alpha1.ConditionDegraded)
	if late := c.Reason == v1alpha1.ReasonReplacementLate; late == tc.noOverlap {
		t.Fatalf("at 09:49:30 Degraded = %s/%s %q, want ReplacementLate = %v", c.Status, c.Reason, c.Message, !tc.noOverlap)
	}

	// 09:50:30, after the drain start.
	hoStepTo(rg, hoT(9, 50, 30), 10*time.Second, nil)
	c = rg.condition(hoMD, v1alpha1.ConditionDegraded)
	gap := c.Reason == v1alpha1.ReasonNoOverlapSlot
	if gap != tc.noOverlap {
		t.Fatalf("at 09:50:30 Degraded = %s/%s %q, want NoOverlapSlot = %v", c.Status, c.Reason, c.Message, tc.noOverlap)
	}
	if tc.noOverlap && (!strings.Contains(c.Message, tc.why) || !strings.Contains(c.Message, "Provider "+rigProvider)) {
		t.Fatalf("at 09:50:30 the message is %q, want it to name Provider %s and %q", c.Message, rigProvider, tc.why)
	}
}
