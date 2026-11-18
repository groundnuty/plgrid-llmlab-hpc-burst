package lifecycle

// The extra's checks, completed (HBEP-7, Over-allocation and qualification (beta), Its checks, as amendment #743 words
// them; #725: #712 and #713 part 1). The slot check leaves one slot for each primary, of this deployment or another, that
// holds no reservation yet but will once Running, and the per-user caps check counts the future replacement of each
// Allocation, of this deployment or another, that will hand over: each chosen primary, a Running one included, and each
// replacement hedge's winner whose own replacement already holds a reserved slot while its predecessor still serves.
// Every count is worked out by hand in the row, never read from slotAsk, reservedOn, capsFit or the selection. One GPU
// per Instance, a 12 h job limit unless a row says (drain window 2 h 10 min, so a primary's expected drain start is
// about 17:50 on the first day), and "the end time before the drain start" is extraMD's 14:00: that deployment's
// primary holds no slot and needs no replacement.

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// gpuCapOpts is a Provider whose only limit is a per-user cap of n GPUs, with no submit limit (#725: the caps are the
// refusal under test, so a slot limit must not be what refuses the extra), and a 12 h job limit.
func gpuCapOpts(n int32) []rigOption {
	return append(qosOpts([]v1alpha1.QOSProfile{{Name: "example-gpu", MaxGPUsPerUser: i32(n)}}, "", "example-gpu"), withMaxTime(12*time.Hour))
}

// noSlotReasonOn fails when the deployment shows SubmitLimitReached: a refused extra is silent.
func noSlotReasonOn(t *testing.T, rg *rig, md string) {
	t.Helper()
	if d := rg.condition(md, v1alpha1.ConditionDegraded); d.Reason == v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("%s Degraded = %s/%s %q, want no SubmitLimitReached: a refused extra is silent", md, d.Status, d.Reason, d.Message)
	}
}

// TestTheExtrasCapsCheckKeepsRoomForItsPrimarysReplacement (#712): a QOS caps the GPUs per user and no submit limit
// exists. md (k = 1) makes its primary P at 08:00:10 and asks for the extra X in the same pass; P is pending, so it is
// listed, and its replacement, made at its drain start, will need a GPU more beside P (they overlap):
//   - cap 2, no end time: P, X and P's future replacement are 1 + 1 + 1 = 3 GPUs > 2, no X (the old count, 2, admitted it,
//     and P's replacement then pended behind a cap of our own);
//   - cap 3: 3 <= 3, X is made;
//   - cap 2, end time 14:00, before P's drain start (17:50:10): there is no replacement, 1 + 1 = 2 <= 2, X is made.
//
// Each deployment is fully served at the end (its primary ready) and shows no SubmitLimitReached.
func TestTheExtrasCapsCheckKeepsRoomForItsPrimarysReplacement(t *testing.T) {
	endBefore := metav1.NewTime(hoT(14, 0, 0))
	for _, c := range []struct {
		name string
		cap  int32
		end  *metav1.Time
		want int // Allocations, the extra's included
	}{
		{"cap 2, no end time: 1 + 1 + 1 = 3 > 2, no extra", 2, nil, 1},
		{"cap 3, no end time: 1 + 1 + 1 = 3 <= 3, the extra", 3, nil, 2},
		{"cap 2, end time before the drain start: no replacement, 1 + 1 = 2 <= 2, the extra", 2, &endBefore, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t, gpuCapOpts(c.cap)...)
			md := withK(mdSpec(hoMD), ptrTo(int32(1)))
			md.Spec.EndTime = c.end
			rg.create(md)
			rg.run(10*time.Minute, 10*time.Second)
			if n := len(rg.allAllocations()); n != c.want {
				t.Fatalf("%d Allocations after 10 minutes of passes, want %d", n, c.want)
			}
			if n := rg.readyCount(hoMD); n != 1 {
				t.Fatalf("%d ready, want the primary: a refusal on a deployment that is not served proves nothing", n)
			}
			noSlotReasonOn(t, rg, hoMD)
		})
	}
}

// TestACutShortExtrasRetryCountsARunningPrimarysReplacement (#712, the retry after P runs): the cap is 3 GPUs, so at 08:00:10
// md (k = 1, no end time) makes its primary P and its extra X: 1 + 1 + P's future replacement 1 = 3 <= 3. X's Allocation's
// create is cut short (X's Instance exists, no Allocation), and every retry is cut short too, until P's job runs: P is Running
// at 08:00:20. An operator then lowers the cap to 2, and the controller restarts (X's own reservation is lost) with the cut
// lifted. X's retry counts P (1, listed), X (1) and P's future replacement (1, a Running primary's: its reserved slot holds
// the SUBMIT limit's room only, and nothing else holds the caps' room): 3 > 2, refused, silently, and X is ended with no
// Allocation. A check that counted pending primaries alone would count 2 <= 2 and make X's Allocation.
func TestACutShortExtrasRetryCountsARunningPrimarysReplacement(t *testing.T) {
	log := newCauseLog()
	cut := &allocCutter{}
	rg := newRig(t, append(gpuCapOpts(3), withInterceptors(cut.funcs(log.funcs())))...)
	cut.arm(hoMD, 1) // P's Allocation is made, X's is cut short
	rg.create(withK(mdSpec(hoMD), ptrTo(int32(1))))
	rg.clock.Advance(10 * time.Second)
	stepTolerantAll(rg) // 08:00:10
	if cut.failures() != 1 || len(rg.instances(hoMD)) != 2 || len(rg.allocations(hoMD)) != 1 {
		t.Fatalf("%d cut creates, %d Instances, %d Allocations, want 1, 2 and 1: X's Instance exists without its Allocation (it was admitted: 3 GPUs within the cap of 3)", cut.failures(), len(rg.instances(hoMD)), len(rg.allocations(hoMD)))
	}
	pAlloc := rg.allocations(hoMD)[0].Name
	var xName string
	for _, in := range rg.instances(hoMD) {
		if in.Spec.AllocationRef.Name != pAlloc {
			xName = in.Name
		}
	}
	for range 3 { // 08:00:20 to 08:00:40, X's retries cut short, until P's job runs
		rg.clock.Advance(10 * time.Second)
		stepTolerantAll(rg)
	}
	if a, ok := rg.allocation(pAlloc); !ok || a.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("P's Allocation %+v at %s, want Running: the row is about a RUNNING primary's replacement", a, rg.clock.Now().Format(time.TimeOnly))
	}
	if _, made := rg.allocation(rg.instance(xName).Spec.AllocationRef.Name); made {
		t.Fatal("X's Allocation was made while the cut was armed")
	}
	rg.setGPUCap("example-gpu", 2)
	cut.disarm()
	rg.restartController()
	for range 6 { // 08:00:50 to 08:01:40
		tick10(rg)
	}
	if got := instOrNil(rg, xName); got != nil && !ended(got) {
		t.Fatalf("X is %s at %s, want it ended (or gone): its Allocation would be the third GPU against a cap of 2, with P's replacement counted", got.Status.Phase, rg.clock.Now().Format(time.TimeOnly))
	}
	if n := len(rg.liveInstances(hoMD)); n != 1 {
		t.Fatalf("md has %d live Instances, want its primary alone", n)
	}
	if n := len(rg.allocations(hoMD)); n != 1 {
		t.Fatalf("md has %d Allocations, want P's alone: no Allocation for the extra", n)
	}
	noSlotReasonOn(t, rg, hoMD)
}

// TestTheExtrasCapsCheckCountsOtherDeploymentsHandovers (#743's caps row): the cap is 3 GPUs per user and no submit limit.
// md-o (k = 0, no end time) has a primary that will hand over; md-e (k = 1, end time 14:00, before its drain start, so no
// replacement of its own) makes its primary and asks for the extra. md-e's pass comes after md-o's (the names sort so).
//   - md-o's primary is Running (08:01:10, its job running since 08:00:20): md-o's listed GPUs 1, md-e's primary 1 and the
//     extra 1 are 3 <= 3, and md-o's future replacement is the 4th: 4 > 3, no X. A check that skipped Running primaries would
//     count 3 and make X.
//   - md-o's primary is still pending (a 10 h queue wait): the same 4 > 3, no X, by the pending primary's replacement.
//   - md-o ends at 14:00, before its drain start: no replacement, 1 + 1 + 1 = 3 <= 3, X is made (the control: the row's
//     refusal is the replacement's).
func TestTheExtrasCapsCheckCountsOtherDeploymentsHandovers(t *testing.T) {
	for _, c := range []struct {
		name    string
		pending bool // md-o's primary waits in the queue
		oEnds   bool // md-o ends before its drain start
		want    int  // md-e's Allocations
	}{
		{"another deployment's Running primary that will hand over: 1 + 1 + 1 + 1 = 4 > 3, no extra", false, false, 1},
		{"another deployment's pending primary that will hand over: 4 > 3, no extra", true, false, 1},
		{"another deployment's Running primary that will not hand over: 3 <= 3, the extra", false, true, 2},
		{"another deployment's pending primary that will not hand over: 3 <= 3, the extra", true, true, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t, gpuCapOpts(3)...)
			if c.pending {
				rg.batch.Enqueue(pending10h(1)...)
			}
			o := mdSpec("md-o")
			if c.oEnds {
				o = noHandover(o)
			}
			rg.create(o)
			for range 7 {
				tick10(rg) // to 08:01:10: md-o's primary is ready, or still pending
			}
			a := rg.allocations("md-o")
			if len(a) != 1 {
				t.Fatalf("md-o has %d Allocations at 08:01:10, want its primary's", len(a))
			}
			if running := a[0].Status.AtProvider.Phase == v1alpha1.AllocationRunning; running == c.pending {
				t.Fatalf("md-o's Allocation is %s at 08:01:10, want %s: the row's premise", a[0].Status.AtProvider.Phase, map[bool]string{true: "pending", false: "Running"}[c.pending])
			}
			rg.create(extraMD("md-e"))
			tick10(rg) // 08:01:20: md-e's primary, and its extra if admitted
			tick10(rg)
			if n := len(rg.allocations("md-e")); n != c.want {
				t.Fatalf("md-e has %d Allocations, want %d", n, c.want)
			}
			noSlotReasonOn(t, rg, "md-e")
		})
	}
}

// TestTheExtrasCapsCheckCountsAReplacementHedgesWinnersReplacement (#743's caps row for a winner): P of D serves on A, and
// D's replacement hedge, R1 on A (a 10 h queue wait) and W on B, ends with W the winner: W runs on B beside P, and holds
// the winner's own slot there, since D has no end time. B's QOS caps the GPUs at 3 per user, with no submit limit. md-e
// (k = 1, end time 09:30, before its primary's drain start of about 09:52, so no replacement of its own) is created on
// B alone, after the win: W 1 + md-e's primary 1 + the extra 1 = 3 <= 3, and W's future replacement is the 4th, 4 > 3: no
// X. With a cap of 4, the same count 4 <= 4 makes X. P is on A, where no cap applies, so only W counts on B.
func TestTheExtrasCapsCheckCountsAReplacementHedgesWinnersReplacement(t *testing.T) {
	for _, c := range []struct {
		name string
		cap  int32
		want int
	}{
		{"cap 3: 1 + 1 + 1 + W's replacement 1 = 4 > 3, no extra", 3, 1},
		{"cap 4: 4 <= 4, the extra", 4, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHedgeRigB(t, 4*time.Hour, &v1alpha1.QOSProfile{Name: "example-b", MaxGPUsPerUser: i32(c.cap)})
			p := replacementStart(t, h, 10*time.Hour)
			setWaits(h.rig, rigProvider, 20, 10*time.Hour)
			h.clock.Advance(10 * time.Second)
			h.Step() // 08:00:20: P's job starts, R1 on A (pending 10 h), W on B
			var r1, w string
			for _, r := range hoRepl(h.rig, p) {
				switch r.Labels[v1alpha1.LabelProvider] {
				case rigProvider:
					r1 = r.Name
				case hedgeB:
					w = r.Name
				}
			}
			if r1 == "" || w == "" {
				t.Fatalf("P's replacements at 08:00:20: %+v, want R1 on A and a member on B", hoRepl(h.rig, p))
			}
			r1Alloc := h.mustInstance(r1).Spec.AllocationRef.Name
			h.until(5*time.Minute, 10*time.Second, "W won and R1's Allocation is gone", func() bool {
				_, there := h.allocation(r1Alloc)
				return h.mustInstance(w).Status.Phase == v1alpha1.InstanceReady && !there
			})
			if h.mustInstance(p).Labels[v1alpha1.LabelProvider] != rigProvider {
				t.Fatal("P is not on A: the row is about a winner on another Provider than its predecessor's")
			}
			n, res := h.slotsOn(hedgeB)
			if n != 1 || len(res) != 1 || !res[0].winner || res[0].deployment != rigNS+"/"+hoMD {
				t.Fatalf("B counts %d listed and reserved %+v, want W alone and W's own slot, held from its win by %s/%s", n, res, rigNS, hoMD)
			}
			e := withK(mdSpec("md-e"), ptrTo(int32(1)))
			e.Spec.Providers.Names = []string{hedgeB}
			end := metav1.NewTime(hoT(9, 30, 0))
			e.Spec.EndTime = &end
			h.create(e)
			for range 3 {
				tick10(h.rig)
			}
			if got := len(h.allocations("md-e")); got != c.want {
				t.Fatalf("md-e has %d Allocations, want %d", got, c.want)
			}
			noSlotReasonOn(t, h.rig, "md-e")
		})
	}
}

// TestTheExtrasSlotLeavesOneForAnotherDeploymentsPendingPrimary (#713 part 1; #743's slot row): a Provider with a submit limit
// of 6 and no GPU cap. md-a (k = 0, no end time) makes its primary at 08:00:10, pending; md-e (k = 1, instances 2, no end
// time) is reconciled after it (the names sort so) and makes its two primaries, then asks for the extra:
//   - listed: md-a's primary 1 + md-e's two 2 = 3; reserved 0 (nothing is Running); md-e's own primaries that will hold a slot
//     once Running 2; md-a's pending primary, which will too, 1; the extra 1; H 0 (md-a has its primary):
//     3 + 0 + 2 + 1 + 1 + 0 = 7 > 6, no X. Without md-a's future slot it is 6 <= 6, and X would take the slot md-a's primary
//     reserves once it runs;
//   - md-a ends at 14:00, before its drain start: it will hold none, 3 + 0 + 2 + 0 + 1 + 0 = 6 <= 6, X is made.
//
// md-a's Allocation is not Running when md-e is checked (asserted), so the reserved slots hold nothing of it.
func TestTheExtrasSlotLeavesOneForAnotherDeploymentsPendingPrimary(t *testing.T) {
	for _, c := range []struct {
		name  string
		aEnds bool
		want  int // md-e's Allocations
	}{
		{"md-a's primary will hand over: 7 > 6, no extra", false, 2},
		{"md-a ends before its drain start: 6 <= 6, the extra", true, 3},
	} {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t, limitOpts(6)...)
			rg.batch.Enqueue(pending10h(6)...)
			a := mdSpec("md-a")
			if c.aEnds {
				a = noHandover(a)
			}
			rg.create(a)
			e := withK(mdSpec("md-e"), ptrTo(int32(1)))
			e.Spec.Instances = 2
			rg.create(e)
			tick10(rg) // 08:00:10: md-a's primary, then md-e's two and its extra if admitted
			if as := rg.allocations("md-a"); len(as) != 1 || as[0].Status.AtProvider.Phase == v1alpha1.AllocationRunning {
				t.Fatalf("md-a has %+v at 08:00:10, want one Allocation, not Running: the row's premise", as)
			}
			if n := len(rg.allocations("md-e")); n != c.want {
				t.Fatalf("md-e has %d Allocations, want %d", n, c.want)
			}
			noSlotReasonOn(t, rg, "md-e")
			if d := rg.condition("md-e", v1alpha1.ConditionDegraded); strings.Contains(d.Message, "maxSubmitPerUser") {
				t.Fatalf("md-e Degraded %q names the limit: a refused extra is silent", d.Message)
			}
		})
	}
}
