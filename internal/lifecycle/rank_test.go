package lifecycle

// The ranking's pure rules (HBEP-7, Over-allocation and qualification (beta), The ranking point; #616, PR 3): the
// margin, the worst peer, and which ends are clean. Every expected value is written out by hand in its row.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestTheMarginIsLowerByMoreThanTenPercent: cand x 10 < peer x 9, in integer nanoseconds. Against 20 ms the line is
// 18 ms: 18.0 ms is not below it, 17.9 ms is, as are the nanosecond on either side. An unmeasured side never outranks
// and is never outranked.
func TestTheMarginIsLowerByMoreThanTenPercent(t *testing.T) {
	peer := &v1alpha1.Instance{}
	peer.Status.Throughput = &v1alpha1.Throughput{TimePerOutputToken: "20ms"}
	for _, c := range []struct {
		cand string
		want bool
	}{
		{"18ms", false},        // 180 is not below 180: exactly 10% lower is not more than 10%
		{"18.000001ms", false}, // one nanosecond above the line
		{"17.999999ms", true},  // one nanosecond below it
		{"17.9ms", true},       // 179 is below 180
		{"18.1ms", false},      // 181
		{"20ms", false},        // equal
		{"25ms", false},        // slower
		{"0s", false},          // no measurement
		{"not a duration", false},
	} {
		cand := &v1alpha1.Instance{}
		cand.Status.Throughput = &v1alpha1.Throughput{TimePerOutputToken: v1alpha1.Duration(c.cand)}
		if got := outranks(cand, peer); got != c.want {
			t.Errorf("%s against 20ms: outranks %v, want %v", c.cand, got, c.want)
		}
	}
	if outranks(&v1alpha1.Instance{}, peer) {
		t.Error("a candidate with no throughput outranks")
	}
	if outranks(peer, &v1alpha1.Instance{}) {
		t.Error("a peer with no throughput is outranked")
	}
}

// TestTheWorstPeerIsTheSlowestThenTheNearestHardEndThenTheName: peers a (20 ms, hard end 12:00), b (20 ms, 11:00), c
// (20 ms, 11:00), d (19 ms, 10:00) and e, which has no measurement and counts as the best. The worst is the highest
// time per output token: of a, b and c, the nearest hard end, b and c, and of those the lowest name, b.
func TestTheWorstPeerIsTheSlowestThenTheNearestHardEndThenTheName(t *testing.T) {
	mk := func(name, tpot string, end time.Time) *v1alpha1.Instance {
		in := &v1alpha1.Instance{}
		in.Name = name
		if tpot != "" {
			in.Status.Throughput = &v1alpha1.Throughput{TimePerOutputToken: v1alpha1.Duration(tpot)}
		}
		e := metav1.NewTime(end)
		in.Status.LeaseEnd = &e
		return in
	}
	a, b, c := mk("a", "20ms", hoT(12, 0, 0)), mk("b", "20ms", hoT(11, 0, 0)), mk("c", "20ms", hoT(11, 0, 0))
	d, e := mk("d", "19ms", hoT(10, 0, 0)), mk("e", "", hoT(9, 0, 0))
	if got := worstPeer([]*v1alpha1.Instance{a, c, d, e, b}); got.Name != "b" {
		t.Errorf("worst peer %s, want b", got.Name)
	}
	if got := worstPeer([]*v1alpha1.Instance{d, e}); got.Name != "d" {
		t.Errorf("worst of d (19 ms) and e (none): %s, want d", got.Name)
	}
	if got := worstPeer([]*v1alpha1.Instance{e}); got.Name != "e" {
		t.Errorf("the only peer, with no measurement: %s, want e", got.Name)
	}
}

// TestARankedOutEndIsACleanEnd: like a drain and a hedge loss it is no failure: it releases nothing, adds nothing to
// the class's streak, and its drain ends with RankedOut when the peer carries the mark.
func TestARankedOutEndIsACleanEnd(t *testing.T) {
	end := time.Now()
	for _, c := range []struct {
		reason string
		failed bool
	}{
		{v1alpha1.ReasonDrained, false},
		{v1alpha1.ReasonHedgeLost, false},
		{v1alpha1.ReasonRankedOut, false},
		{v1alpha1.ReasonAllocationFailed, true},
		{v1alpha1.ReasonQualificationFailed, true},
	} {
		in := member("x", "", end, v1alpha1.InstanceEnded, nil)
		in.Status.EndReason = c.reason
		e := metav1.NewTime(end)
		in.Status.EndTime = &e
		if got := releaseRequired(in); got != c.failed {
			t.Errorf("%s: releaseRequired %v, want %v", c.reason, got, c.failed)
		}
		if got := failedEnd(in); got != c.failed {
			t.Errorf("%s: failedEnd %v, want %v", c.reason, got, c.failed)
		}
		if got := cleanEnd(c.reason); got == c.failed {
			t.Errorf("%s: cleanEnd %v, want %v", c.reason, got, !c.failed)
		}
	}
	marked := member("y", "", end, v1alpha1.InstanceDraining, nil)
	marked.Annotations = map[string]string{annotationRankedOut: "candidate"}
	if got := drainEndReason(marked); got != v1alpha1.ReasonRankedOut {
		t.Errorf("a marked peer's drain ends %s, want RankedOut", got)
	}
	if got := drainEndReason(member("z", "", end, v1alpha1.InstanceDraining, nil)); got != v1alpha1.ReasonDrained {
		t.Errorf("an unmarked drain ends %s, want Drained", got)
	}
}

// TestTheStartTimeoutExemptionIsKeyedOnTheSelection: an Instance waits at its ranking point, and startTimeout does
// not judge it, only when it has passed its checks, its tunnel is connected, and it is surplus in the selection. A
// chosen Instance with the same conditions is not exempt, nor is a surplus one whose tunnel is down or whose checks
// are not done. Each row is by hand: instances 1 with P, and X the extra; where P is ready X is surplus.
func TestTheStartTimeoutExemptionIsKeyedOnTheSelection(t *testing.T) {
	tunnel := func(on bool) selOpt {
		return func(in *v1alpha1.Instance, _ *v1alpha1.Allocation) {
			s := metav1.ConditionFalse
			if on {
				s = metav1.ConditionTrue
			}
			in.Status.Conditions = append(in.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionTunnelConnected, Status: s})
		}
	}
	for _, c := range []struct {
		name   string
		peer   v1alpha1.InstancePhase
		opts   []selOpt
		exempt bool
	}{
		{"surplus, passed, tunnel up", v1alpha1.InstanceReady, []selOpt{passedChecksOpt(), tunnel(true)}, true},
		{"surplus, passed, tunnel down", v1alpha1.InstanceReady, []selOpt{passedChecksOpt(), tunnel(false)}, false},
		{"surplus, checks not done, tunnel up", v1alpha1.InstanceReady, []selOpt{tunnel(true)}, false},
		// P started at 09:00 and has not passed, so X, passed, is the better of the two: it is chosen.
		{"chosen, passed, tunnel up", v1alpha1.InstanceStarting, []selOpt{passedChecksOpt(), tunnel(true)}, false},
		{"chosen, passed, tunnel down", v1alpha1.InstanceStarting, []selOpt{passedChecksOpt(), tunnel(false)}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newSelFx()
			f.add("P", c.peer, hardEnd(hoT(12, 10, 0)), jobStartedAt(hoT(9, 0, 0)))
			x := f.add("X", v1alpha1.InstanceVerifying, append([]selOpt{jobStartedAt(hoT(10, 0, 0))}, c.opts...)...)
			if got := f.pass(1, 1).waitingAtRankingPoint(x); got != c.exempt {
				t.Errorf("exempt %v, want %v", got, c.exempt)
			}
		})
	}
}
