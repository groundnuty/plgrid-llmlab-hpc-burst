package lifecycle

// HBEP-7's growth rows (Test plan, Hedging, A hedge is added) through the rig: Providers A (helios-gpu) and B, in
// different classes, both able to host, A first. The rig submits an Allocation in the step after its creation, so a
// member's submission time is its creation plus 10 s; every expected time below is worked out by hand from that and
// from the waits each row seeds. Each row asserts no member one step before its hedge time and the member at it.

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// setWaits replaces a Provider's queue waits with n waits of w each, as its controller would have written them.
func setWaits(rg *rig, provider string, n int, w time.Duration) {
	rg.t.Helper()
	var prov v1alpha1.Provider
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: provider}, &prov); err != nil {
		rg.t.Fatal(err)
	}
	prov.Status.QueueWaits = nil
	for i := 0; i < n; i++ {
		submit := rigStart.Add(-24*time.Hour + time.Duration(i)*time.Minute)
		prov.Status.QueueWaits = append(prov.Status.QueueWaits, v1alpha1.QueueWait{
			JobID: "old-" + string(rune('a'+i)), SubmitTime: metav1.NewTime(submit), StartTime: metav1.NewTime(submit.Add(w)),
		})
	}
	if err := rg.c.Status().Update(rg.ctx, &prov); err != nil {
		rg.t.Fatal(err)
	}
}

// observedWaits is how many queue waits a Provider's status holds.
func observedWaits(rg *rig, provider string) int {
	rg.t.Helper()
	var prov v1alpha1.Provider
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: provider}, &prov); err != nil {
		rg.t.Fatal(err)
	}
	return len(prov.Status.QueueWaits)
}

// membersOn returns the live members on prov of the hedge whose first member is first.
func (rg *rig) membersOn(md, first, prov string) []v1alpha1.Instance {
	var out []v1alpha1.Instance
	for _, in := range rg.onProvider(md, prov) {
		if in.Annotations[v1alpha1.AnnotationHedgeGroup] == first {
			out = append(out, in)
		}
	}
	return out
}

// stepUntilClock steps the rig in steps of step until the clock reads at, checking each (nil: nothing) after
// every step.
func stepUntilClock(rg *rig, at time.Time, step time.Duration, each func()) {
	rg.t.Helper()
	for rg.clock.Now().Before(at) {
		rg.clock.Advance(min(step, at.Sub(rg.clock.Now())))
		rg.Step()
		if each != nil {
			each()
		}
	}
}

// missingPrimary creates hoMD on A and B, A's waits all w (none for w 0), with A's first job pending 10 h, at
// 08:00:00, and runs its first pass: M1 is created on A at 08:00:00. It returns M1's name.
func missingPrimary(t *testing.T, h *hedgeRig, w time.Duration, edit func(*v1alpha1.ModelDeployment)) string {
	t.Helper()
	n := 0
	if w > 0 {
		n = 20
	}
	return missingPrimaryOnWaits(t, h, n, w, edit)
}

// missingPrimaryOnWaits is missingPrimary with A's waits n of w each, none for n 0. Twenty waits of 0 s is a Provider
// whose observed waits are all zero, which missingPrimary's w 0 is not: that Provider has observed none.
func missingPrimaryOnWaits(t *testing.T, h *hedgeRig, n int, w time.Duration, edit func(*v1alpha1.ModelDeployment)) string {
	t.Helper()
	h.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
	h.Step() // the Providers' first status: Ready
	if n > 0 {
		setWaits(h.rig, rigProvider, n, w)
	}
	md := mdSpec(hoMD)
	onBoth(md)
	if edit != nil {
		edit(md)
	}
	h.create(md)
	h.Step()
	firsts := h.onProvider(hoMD, rigProvider)
	if len(firsts) != 1 || !firsts[0].CreationTimestamp.Time.Equal(hoT(8, 0, 0)) {
		t.Fatalf("after the first pass, A has %+v, want the missing primary, created at 08:00:00", firsts)
	}
	return firsts[0].Name
}

// TestAMissingPrimaryHedgesByItsWaits: its deadline is its creation, 08:00:00, plus the 30 min margin, 08:30:00.
//   - A's waits 10 h: its expected start, now plus 10 h, is after 08:30:00, so B's member is made in its first pass.
//   - A's waits 20 min: submitted at 08:00:10, its expected start is 08:20:10, so B's member comes at 08:20:10,
//     none at 08:20:00.
//   - no wait on A: its expected start is its deadline, 08:30:00, so B's member comes then, none at 08:29:50, and
//     never in its first pass.
//
// B's member names M1 as its hedge and no predecessor.
func TestAMissingPrimaryHedgesByItsWaits(t *testing.T) {
	for _, c := range []struct {
		name string
		wait time.Duration
		at   time.Time // B's member's time; 08:00:00 is the first pass
	}{
		{"A's waits 10 h", 10 * time.Hour, hoT(8, 0, 0)},
		{"A's waits 20 min", 20 * time.Minute, hoT(8, 20, 10)},
		{"no wait on A", 0, hoT(8, 30, 0)},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHedgeRig(t)
			m1 := missingPrimary(t, h, c.wait, nil)
			check := func() {
				got := h.membersOn(hoMD, m1, hedgeB)
				due := !h.clock.Now().Before(c.at)
				if due != (len(got) == 1) {
					t.Fatalf("at %s B has %d members of M1's hedge, want %v", h.clock.Now().Format(time.TimeOnly), len(got), due)
				}
				if len(got) == 1 && got[0].Annotations[v1alpha1.AnnotationPredecessor] != "" {
					t.Fatalf("B's member names a predecessor, %q: a missing primary's hedge has none", got[0].Annotations[v1alpha1.AnnotationPredecessor])
				}
			}
			check()
			stepUntilClock(h.rig, c.at.Add(-10*time.Second), 10*time.Second, check)
			h.clock.Advance(10 * time.Second)
			h.Step()
			check()
		})
	}
}

// TestAMemberNotPredictedLateWaitsTheMinimumWait: the science agent's ruling on #627's main merge, 17 Nov 2026 (HBEP-7,
// A member's hedge time): a member not predicted late isn't judged late before its submission plus 10 min
// (hedgeMinWait). M1's deadline is its creation, 08:00:00, plus the 30 min margin, 08:30:00, and the rig submits its
// Allocation at 08:00:10.
//   - A's 20 waits all 0 s: M1's expected start is its submission, 08:00:10, and it is not predicted late. Without the
//     minimum wait B's member came in the pass after the submission; with it M1's hedge time is 08:10:10, so B has no
//     member at 08:00:20 (one pass after the submission), none at 08:10:00, and its member at 08:10:10.
//   - A's waits 10 h: M1 is predicted late, its expected start is after its deadline, so B's member is made in its
//     first pass, 08:00:00: the minimum wait doesn't delay it.
func TestAMemberNotPredictedLateWaitsTheMinimumWait(t *testing.T) {
	for _, c := range []struct {
		name string
		wait time.Duration
		at   time.Time // B's member's time; 08:00:00 is the first pass
	}{
		{"A's 20 waits all 0 s", 0, hoT(8, 10, 10)},
		{"A's waits 10 h: predicted late", 10 * time.Hour, hoT(8, 0, 0)},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHedgeRig(t)
			m1 := missingPrimaryOnWaits(t, h, 20, c.wait, nil)
			check := func() {
				got := h.membersOn(hoMD, m1, hedgeB)
				due := !h.clock.Now().Before(c.at)
				if due != (len(got) == 1) {
					t.Fatalf("at %s B has %d members of M1's hedge, want %v", h.clock.Now().Format(time.TimeOnly), len(got), due)
				}
			}
			check()
			stepUntilClock(h.rig, c.at.Add(-10*time.Second), 10*time.Second, check)
			h.clock.Advance(10 * time.Second)
			h.Step()
			check()
		})
	}
}

// replacementStart starts P on A with a 12 h lease, as HBEP-7's hedging fixture has it: created at 08:00:00, its job
// starting at 08:00:20, so its hard end is 20:00:20 and its drain start 17:50:20. A's second job, P's replacement R1,
// pends for r1Wait. waitsAt is when A's waits (all w, none for w 0) are written: before R1's planned time.
func replacementStart(t *testing.T, h *hedgeRig, r1Wait time.Duration) string {
	t.Helper()
	h.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: r1Wait})
	h.Step()
	md := mdSpec(hoMD)
	onBoth(md)
	h.create(md)
	h.clock.Advance(10 * time.Second)
	h.Step() // 08:00:10: P is created
	ps := h.instances(hoMD)
	if len(ps) != 1 {
		t.Fatalf("%d Instances at 08:00:10, want P", len(ps))
	}
	return ps[0].Name
}

// TestAReplacementHedgesByItsWaits: P's lease (12 h) ends 20:00:20, its drain start is 17:50:20, and once P is ready
// at 08:01:10 its class's start-to-ready time is 50 s, so the replacement's deadline is 17:49:30.
//   - A's waits 10 h, known as P's job starts at 08:00:20: R1 is planned at P's start (clamped) and made at 08:00:20;
//     its expected start, now plus 10 h, is after the deadline, so B's member is made in the same pass, naming R1's
//     hedge and P.
//   - A's waits 2 h: R1 is planned at 17:50:20 minus (2 h + 50 s + 30 min), 15:19:30, and submitted at 15:19:40, so
//     its expected start is 17:19:40: B's member comes then, none at 17:19:30.
//   - no wait on A, none observed at all (P's own 0 s wait is cleared once it is recorded): R1 is made as soon as no
//     wait is observed, and its expected start is the deadline minus the margin, 17:19:30, hours after its submission
//     plus 10 min, so the minimum wait never moves it: B's member comes at 17:19:30, none at 17:19:20.
//   - one 0 s wait on A (P's own, left in A's status): R1 is planned from it at 17:49:30 minus (0 + 50 s + 30 min) =
//     17:19:30 and submitted at 17:19:40, so its expected start is its submission, and the minimum wait moves its hedge
//     time to the submission plus 10 min: B's member comes at 17:29:40, none at 17:29:30. Before the minimum wait it
//     came at R1's first pass, 17:19:30.
//
// Each row asserts how many waits A's status holds (20, 1 or 0), since "no wait" and "one 0 s wait" differ only there.
func TestAReplacementHedgesByItsWaits(t *testing.T) {
	t.Run("A's waits 10 h: a member in R1's pass", func(t *testing.T) {
		h := newHedgeRig(t, withMaxTime(12*time.Hour))
		p := replacementStart(t, h, 10*time.Hour)
		setWaits(h.rig, rigProvider, 20, 10*time.Hour)
		h.clock.Advance(10 * time.Second)
		h.Step() // 08:00:20: P's job starts, R1 is made, and B's member with it
		reps := hoRepl(h.rig, p)
		var r1 string
		for _, r := range reps {
			if r.Labels[v1alpha1.LabelProvider] == rigProvider {
				r1 = r.Name
			}
		}
		if r1 == "" {
			t.Fatalf("no replacement of P on A at 08:00:20: %+v", reps)
		}
		got := h.membersOn(hoMD, r1, hedgeB)
		if len(got) != 1 || got[0].Annotations[v1alpha1.AnnotationPredecessor] != p {
			t.Fatalf("B's members of R1's hedge at 08:00:20: %+v, want one naming P %s", got, p)
		}
	})
	for _, c := range []struct {
		name   string
		wait   time.Duration
		seen   int // the waits A's status holds, asserted: 20 seeded, 1 P's own 0 s wait, 0 none
		r1Wait time.Duration
		at     time.Time // zero: never
		fine   time.Time // zero: 30 s before at; where the 10 s steps begin
	}{
		{"A's waits 2 h", 2 * time.Hour, 20, 10 * time.Hour, hoT(17, 19, 40), time.Time{}},
		{"A's waits 2 h, R1 starts at 17:09:40", 2 * time.Hour, 20, time.Hour*1 + 50*time.Minute, time.Time{}, time.Time{}},
		{"no wait on A", 0, 0, 10 * time.Hour, hoT(17, 19, 30), time.Time{}},
		{"one 0 s wait on A", 0, 1, 10 * time.Hour, hoT(17, 29, 40), hoT(17, 19, 0)},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHedgeRig(t, withMaxTime(12*time.Hour))
			p := replacementStart(t, h, c.r1Wait)
			stepUntilClock(h.rig, hoT(8, 1, 10), 10*time.Second, nil) // P ready at 08:01:10
			switch {
			case c.wait > 0:
				setWaits(h.rig, rigProvider, 20, c.wait)
			case c.seen == 0:
				setWaits(h.rig, rigProvider, 0, 0) // P's own 0 s wait, recorded when its job started, is cleared
			}
			end := c.at
			if end.IsZero() {
				end = hoT(17, 30, 0)
			}
			r1 := func() string {
				for _, r := range hoRepl(h.rig, p) {
					if r.Labels[v1alpha1.LabelProvider] == rigProvider {
						return r.Name
					}
				}
				return ""
			}
			check := func() {
				if got := observedWaits(h.rig, rigProvider); got != c.seen {
					t.Fatalf("at %s A's status holds %d waits, want %d: the row is not the world it names", h.clock.Now().Format(time.TimeOnly), got, c.seen)
				}
				n := 0
				if r := r1(); r != "" {
					n = len(h.membersOn(hoMD, r, hedgeB))
				}
				due := !c.at.IsZero() && !h.clock.Now().Before(c.at)
				if due != (n == 1) {
					t.Fatalf("at %s B has %d members of R1's hedge, want %v", h.clock.Now().Format(time.TimeOnly), n, due)
				}
			}
			// In 10 s steps around R1's planned time, 15:19:30, so it is made then and submitted at 15:19:40.
			stepUntilClock(h.rig, hoT(15, 19, 20), 5*time.Minute, check)
			stepUntilClock(h.rig, hoT(15, 20, 0), 10*time.Second, check)
			// The 10 s steps near the member's time: 30 s before it, or from c.fine (R1's planned time is 17:19:30 in the
			// last row, so its submission and its floor, 10 min on, are read at 10 s).
			fine := end.Add(-30 * time.Second)
			if !c.fine.IsZero() {
				fine = c.fine
			}
			stepUntilClock(h.rig, fine, 5*time.Minute, check)
			stepUntilClock(h.rig, end, 10*time.Second, check)
			if c.wait == 2*time.Hour {
				if r := r1(); r == "" {
					t.Fatal("no R1 on A")
				} else if in := h.mustInstance(r); !in.CreationTimestamp.Time.Equal(hoT(15, 19, 30)) {
					t.Fatalf("R1 was made at %s, want 15:19:30", in.CreationTimestamp.Format(time.TimeOnly))
				}
			}
		})
	}
}

// TestAReplacementWhosePredecessorIsGoneWaitsTheMinimumWait: a replacement hedge whose predecessor is gone has its start
// deadline passed (startDeadline: now). So a member predicted late, its expected start after now, still hedges at once,
// and one that isn't, with near-zero waits or none observed, is held to its submission plus 10 min (hedgeMinWait): the
// science agent's ruling on #627, 17 Nov 2026, which says flooring it is intended. R1 is a real replacement of P, made
// and submitted by the rig with A's waits as the row says. The pass is then read as it would be with P gone from its
// Instances, and R1's hedge time is read as growHedges reads it (needOf, startDeadline, hedgeTime).
//   - A's 20 waits all 0 s: R1 is planned at 17:19:30 and submitted at 17:19:40, so its expected start, 17:19:40, is not
//     after the passed deadline (now, 17:19:50): its hedge time is 17:29:40, not now.
//   - no wait observed on A: R1's expected start is the deadline minus the margin, long past, so its hedge time is its
//     submission plus 10 min.
//   - A's waits 10 h: R1's expected start, its submission plus 10 h, is after the passed deadline: its hedge time is now.
func TestAReplacementWhosePredecessorIsGoneWaitsTheMinimumWait(t *testing.T) {
	for _, c := range []struct {
		name  string
		setup func(h *hedgeRig) // from 08:00:10, with P made, until R1 is made and submitted
		late  bool              // predicted late: the hedge time is now; else R1's submission plus 10 min
	}{
		{"A's 20 waits all 0 s", func(h *hedgeRig) {
			stepUntilClock(h.rig, hoT(8, 1, 10), 10*time.Second, nil) // P ready
			setWaits(h.rig, rigProvider, 20, 0)
			stepUntilClock(h.rig, hoT(17, 19, 0), 5*time.Minute, nil)
			stepUntilClock(h.rig, hoT(17, 19, 50), 10*time.Second, nil) // R1 made at 17:19:30, submitted at 17:19:40
		}, false},
		{"no wait observed on A", func(h *hedgeRig) {
			stepUntilClock(h.rig, hoT(8, 1, 10), 10*time.Second, nil)
			setWaits(h.rig, rigProvider, 0, 0) // P's own 0 s wait, recorded when its job started, is cleared
			stepUntilClock(h.rig, hoT(8, 5, 0), 10*time.Second, nil)
		}, false},
		{"A's waits 10 h: predicted late", func(h *hedgeRig) {
			setWaits(h.rig, rigProvider, 20, 10*time.Hour)
			stepUntilClock(h.rig, hoT(8, 0, 40), 10*time.Second, nil) // 08:00:20: P's job starts and R1 is made
		}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHedgeRig(t, withMaxTime(12*time.Hour))
			pName := replacementStart(t, h, 10*time.Hour)
			c.setup(h)
			var r1 string
			for _, r := range hoRepl(h.rig, pName) {
				if r.Labels[v1alpha1.LabelProvider] == rigProvider {
					r1 = r.Name
				}
			}
			if r1 == "" {
				t.Fatalf("no replacement of P on A at %s", h.clock.Now().Format(time.TimeOnly))
			}
			a, ok := h.allocation(h.mustInstance(r1).Spec.AllocationRef.Name)
			if !ok || a.Status.AtProvider.SubmitTime == nil {
				t.Fatalf("R1's Allocation is not submitted at %s: %+v", h.clock.Now().Format(time.TimeOnly), a)
			}
			submit := a.Status.AtProvider.SubmitTime.Time
			ps := h.passOf(hoMD)
			if ps.instanceNamed(pName) == nil {
				t.Fatalf("P is not among the pass's Instances before it is taken out")
			}
			var kept []*v1alpha1.Instance
			for _, in := range ps.insts {
				if in.Name != pName {
					kept = append(kept, in)
				}
			}
			ps.insts = kept // P gone
			var need hedgeNeed
			found := false
			for _, g := range groupHedges(ps.insts, func(in *v1alpha1.Instance) bool { return countable(in) && !ps.stale[in.UID] }) {
				if g.key == r1 {
					need, found = ps.needOf(g), true
				}
			}
			if !found {
				t.Fatalf("no hedge of R1 %s in the pass", r1)
			}
			deadline, margin, replacement, ok := ps.startDeadline(need)
			if !need.predGone || !ok || !replacement || !deadline.Equal(ps.now) {
				t.Fatalf("with P gone R1's hedge reads predGone %v, ok %v, replacement %v, deadline %s: want gone, ok, a replacement, deadline now %s",
					need.predGone, ok, replacement, deadline.Format(time.TimeOnly), ps.now.Format(time.TimeOnly))
			}
			got := ps.hedgeTime(need.first, deadline, margin, replacement)
			want := submit.Add(10 * time.Minute)
			if c.late {
				want = ps.now
			}
			if !got.Equal(want) {
				t.Errorf("R1's hedge time is %s, want %s (now %s, R1 submitted %s)", got.Format(time.TimeOnly), want.Format(time.TimeOnly),
					ps.now.Format(time.TimeOnly), submit.Format(time.TimeOnly))
			}
		})
	}
}

// TestAHedgeSkipsAProviderItCannotUse: a missing primary with A's waits all 10 h is due a member on B in its first
// pass. It gets none, with hedgeWidth 1, with B's partition too short (1 h), with B not Ready, or with B's per-user
// GPU cap (1) already held by another deployment of ours there. The deployment's Degraded condition is the same as
// with hedgeWidth 1: a Provider a hedge passes over sets no reason of its own.
func TestAHedgeSkipsAProviderItCannotUse(t *testing.T) {
	for _, c := range []struct {
		name string
		rig  func(t *testing.T) *hedgeRig
		edit func(*v1alpha1.ModelDeployment)
		prep func(h *hedgeRig) // after the Providers' first status
	}{
		{"hedgeWidth 1", func(t *testing.T) *hedgeRig { return newHedgeRig(t) },
			func(md *v1alpha1.ModelDeployment) { md.Spec.HedgeWidth = ptrTo(int32(1)) }, nil},
		{"B's partition too short", func(t *testing.T) *hedgeRig { return newHedgeRigB(t, time.Hour, nil) }, nil, nil},
		{"B not Ready", func(t *testing.T) *hedgeRig { return newHedgeRig(t) }, nil,
			func(h *hedgeRig) { h.setProviderNotReady(hedgeB) }},
		{"B's GPU cap held by another deployment", func(t *testing.T) *hedgeRig {
			return newHedgeRigB(t, 4*time.Hour, &v1alpha1.QOSProfile{Name: "example-b1", MaxGPUsPerUser: ptrTo(int32(1))})
		}, nil, func(h *hedgeRig) {
			other := mdSpec("md-x")
			other.Spec.Providers.Names = []string{hedgeB}
			h.create(other)
			h.until(time.Minute, 10*time.Second, "md-x on B", func() bool { return len(h.allocations("md-x")) == 1 })
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			var msgs [2]string
			for i, width := range []int32{2, 1} {
				h := c.rig(t)
				h.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
				h.Step()
				if c.prep != nil {
					c.prep(h)
				}
				setWaits(h.rig, rigProvider, 20, 10*time.Hour)
				md := mdSpec(hoMD)
				onBoth(md)
				if c.edit != nil {
					c.edit(md)
				}
				w := width
				if md.Spec.HedgeWidth == nil {
					md.Spec.HedgeWidth = &w
				}
				h.create(md)
				if err := h.reconcileOne(hoMD); err != nil {
					t.Fatal(err)
				}
				if n := len(h.onProvider(hoMD, hedgeB)); n != 0 {
					t.Fatalf("width %d: %d Instances of %s on B, want none", width, n, hoMD)
				}
				if n := len(h.onProvider(hoMD, rigProvider)); n != 1 {
					t.Fatalf("width %d: %d Instances on A, want the missing primary", width, n)
				}
				cd := h.condition(hoMD, v1alpha1.ConditionDegraded)
				msgs[i] = string(cd.Status) + " " + cd.Reason + " " + cd.Message
			}
			if msgs[0] != msgs[1] {
				t.Fatalf("Degraded with hedging, %q, differs from without, %q: a passed-over Provider set a reason", msgs[0], msgs[1])
			}
		})
	}
}

// newHedgeRigB is newHedgeRig with B's partition's maxTime and, when qos is set, B's partition under that QOS.
func newHedgeRigB(t *testing.T, bMax time.Duration, qos *v1alpha1.QOSProfile) *hedgeRig {
	var opts []rigOption
	if qos != nil {
		q := *qos
		opts = append(opts, withProfile(func(s *v1alpha1.PlatformProfileSpec) { s.QOS = append(s.QOS, q) }))
	}
	rg := newRig(t, append([]rigOption{withMaxTime(4 * time.Hour)}, opts...)...)
	b := rg.addProvider(hedgeB, hedgeBSlurm, bMax,
		func(pp *v1alpha1.PartitionProfile) {
			pp.AcceleratorClass.GPUModel = hedgeBGPU
			if qos != nil {
				pp.QOS = qos.Name
			}
		},
		func(c *pfake.Config) { c.Partitions[0].GPUModel = hedgeBGPU })
	return &hedgeRig{rig: rg, b: b, slow: map[string]bool{}}
}

// setProviderNotReady writes a Provider's Ready condition False, as its controller would on a failed read.
func (rg *rig) setProviderNotReady(name string) {
	rg.t.Helper()
	var prov v1alpha1.Provider
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: name}, &prov); err != nil {
		rg.t.Fatal(err)
	}
	now := metav1.NewTime(rg.clock.Now())
	cs := prov.Status.Conditions
	for i := range cs {
		if cs[i].Type == v1alpha1.ConditionProviderReady {
			cs[i].Status, cs[i].Reason, cs[i].Message, cs[i].LastTransitionTime = metav1.ConditionFalse, v1alpha1.ReasonLoginHostInUse, "login host in use", now
		}
	}
	if err := rg.c.Status().Update(rg.ctx, &prov); err != nil {
		rg.t.Fatal(err)
	}
}

// TestNoMemberWhileOneStarted: A's waits 10 h, but A's first job starts at once (a queue wait of 0): M1's job has
// started, so its hedge gets no member, though its expected start is long after the deadline.
func TestNoMemberWhileOneStarted(t *testing.T) {
	h := newHedgeRig(t)
	h.slow = map[string]bool{}
	h.batch.Enqueue(pfake.Script{})
	h.Step()
	md := mdSpec(hoMD)
	onBoth(md)
	h.create(md)
	h.Step() // 08:00:00: M1 is made, no wait yet, its hedge time 08:30:00
	m1 := h.onProvider(hoMD, rigProvider)[0].Name
	h.slow[h.mustInstance(m1).Spec.AllocationRef.Name] = true
	h.clock.Advance(10 * time.Second)
	h.Step() // 08:00:10: M1's job starts
	setWaits(h.rig, rigProvider, 20, 10*time.Hour)
	stepUntilClock(h.rig, hoT(8, 40, 0), time.Minute, func() {
		if n := len(h.membersOn(hoMD, m1, hedgeB)); n != 0 {
			t.Fatalf("at %s B has %d members of M1's hedge, whose job has started", h.clock.Now().Format(time.TimeOnly), n)
		}
	})
}

// TestAFrozenHedgeGetsNoMember: while an executing Operation holds a hedge (the hook #589's execution sets), it gets
// no member, though it is due one in its first pass (A's waits 10 h); once released, it gets one in the next pass.
func TestAFrozenHedgeGetsNoMember(t *testing.T) {
	h := newHedgeRig(t)
	frozen := true
	h.r.hedgeFrozen = func(hedgeGroup) bool { return frozen }
	m1 := missingPrimary(t, h, 10*time.Hour, nil)
	if n := len(h.membersOn(hoMD, m1, hedgeB)); n != 0 {
		t.Fatalf("a frozen hedge got %d members on B", n)
	}
	frozen = false
	h.clock.Advance(10 * time.Second)
	h.Step()
	if n := len(h.membersOn(hoMD, m1, hedgeB)); n != 1 {
		t.Fatalf("the released hedge has %d members on B, want 1", n)
	}
}

// TestLoweringTheWidthCancelsNone: a hedge of two (A's waits 10 h) keeps both members when hedgeWidth is lowered to 1.
func TestLoweringTheWidthCancelsNone(t *testing.T) {
	h := newHedgeRig(t)
	h.b.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
	m1 := missingPrimary(t, h, 10*time.Hour, nil)
	if n := len(h.membersOn(hoMD, m1, hedgeB)); n != 1 {
		t.Fatalf("B has %d members, want 1", n)
	}
	h.mutateMD(hoMD, func(md *v1alpha1.ModelDeployment) { md.Spec.HedgeWidth = ptrTo(int32(1)) })
	stepUntilClock(h.rig, hoT(8, 10, 0), time.Minute, nil)
	if a, b := len(h.onProvider(hoMD, rigProvider)), len(h.onProvider(hoMD, hedgeB)); a != 1 || b != 1 {
		t.Fatalf("after lowering hedgeWidth to 1: %d on A and %d on B, want both members kept", a, b)
	}
	if c := h.condition(hoMD, v1alpha1.ConditionDegraded); strings.Contains(c.Message, "hedge") {
		t.Fatalf("Degraded mentions the hedge: %q", c.Message)
	}
}

// TestARestartAddsTheSameMember: A's waits 20 min, and M1 is submitted at 08:00:10, so M1's hedge is due its member
// at 08:20:10 (TestAMissingPrimaryHedgesByItsWaits). The controller restarts at 08:10:00; the member still comes at
// 08:20:10, none before: the submission time and the waits are in status.
func TestARestartAddsTheSameMember(t *testing.T) {
	h := newHedgeRig(t)
	m1 := missingPrimary(t, h, 20*time.Minute, nil)
	h.clock.Advance(10 * time.Second)
	h.Step() // 08:00:10: M1 is submitted
	none := func() {
		if n := len(h.membersOn(hoMD, m1, hedgeB)); n != 0 {
			t.Fatalf("at %s B has %d members, want none before 08:20:10", h.clock.Now().Format(time.TimeOnly), n)
		}
	}
	stepUntilClock(h.rig, hoT(8, 10, 0), time.Minute, none)
	h.restartController()
	stepUntilClock(h.rig, hoT(8, 20, 0), 10*time.Second, none)
	h.clock.Advance(10 * time.Second)
	h.Step()
	if n := len(h.membersOn(hoMD, m1, hedgeB)); n != 1 {
		t.Fatalf("at 08:20:10 after a restart B has %d members, want 1", n)
	}
}

// TestAGrownMemberKeepsTheHedgesKey (#627's review): A's waits all 10 h, so a missing primary's M1 on A gets M2 on B
// (pending 10 h) in its first pass, 08:00:00. M1's job starts at 08:00:10 and fails 30 s later, so M2 is left, with B
// having no wait observed: its hedge time is the deadline, 08:30:00, when M3 is made on A. M3 names the hedge's key,
// M1, not M2, so M2 and M3 stay one primary: across ten passes both live, nothing is cancelled and nothing more made.
func TestAGrownMemberKeepsTheHedgesKey(t *testing.T) {
	h := newHedgeRig(t)
	fail := 30 * time.Second
	h.batch.Enqueue(pfake.Script{FailAfter: &fail}, pfake.Script{QueueWait: 10 * time.Hour})
	h.b.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
	h.Step()
	setWaits(h.rig, rigProvider, 20, 10*time.Hour)
	md := mdSpec(hoMD)
	onBoth(md)
	h.create(md)
	h.Step() // 08:00:00: M1 on A, M2 on B
	m1 := h.onProvider(hoMD, rigProvider)[0].Name
	if n := len(h.membersOn(hoMD, m1, hedgeB)); n != 1 {
		t.Fatalf("B has %d members of M1's hedge at 08:00:00, want 1", n)
	}
	stepUntilClock(h.rig, hoT(8, 1, 0), 10*time.Second, nil)
	if in := h.instancesByName()[m1]; in != nil && !ended(in) {
		t.Fatalf("M1 is %s at 08:01:00, want failed: the test proves nothing", in.Status.Phase)
	}
	stepUntilClock(h.rig, hoT(8, 30, 0), time.Minute, nil)
	onA := h.onProvider(hoMD, rigProvider)
	if len(onA) != 1 || onA[0].Annotations[v1alpha1.AnnotationHedgeGroup] != m1 {
		t.Fatalf("at 08:30:00 A has %+v, want one member, M3, naming the hedge's key %s", onA, m1)
	}
	m3 := onA[0].Name
	known := map[string]bool{}
	for _, in := range h.instances(hoMD) {
		known[in.Name] = true
	}
	for range 10 {
		h.clock.Advance(10 * time.Second)
		h.Step()
		if a, b := len(h.onProvider(hoMD, rigProvider)), len(h.onProvider(hoMD, hedgeB)); a != 1 || b != 1 {
			t.Fatalf("at %s A has %d live and B %d, want M3 and M2", h.clock.Now().Format(time.TimeOnly), a, b)
		}
		if in := h.mustInstance(m3); ended(in) {
			t.Fatalf("M3 ended (%s) at %s: the hedge read as two primaries", in.Status.EndReason, h.clock.Now().Format(time.TimeOnly))
		}
		// M1's failure tombstone goes at the end of its restart window; no Instance may appear.
		for _, in := range h.instances(hoMD) {
			if !known[in.Name] {
				t.Fatalf("Instance %s appeared at %s: a member was made again", in.Name, h.clock.Now().Format(time.TimeOnly))
			}
		}
	}
}

// TestAGrownReplacementMemberKeepsTheHedgesKey (#627's review): P on A (12 h lease); A's and B's waits all 10 h. At
// 08:00:20 R1 is made on A and R2 on B (pending 1 h). R1's job starts at 08:00:30 and fails at 08:01:00; R2 is left,
// its expected start (08:00:30 + 10 h) after the deadline, so R3 is made on A at once, naming the hedge's key, R1.
// When R2 starts and wins, R3, pending, is that one hedge's loser and ends HedgeLost: no member stays beside P.
func TestAGrownReplacementMemberKeepsTheHedgesKey(t *testing.T) {
	h := newHedgeRig(t, withMaxTime(12*time.Hour))
	fail := 30 * time.Second
	h.batch.Enqueue(pfake.Script{}, pfake.Script{FailAfter: &fail}, pfake.Script{QueueWait: 10 * time.Hour})
	h.b.batch.Enqueue(pfake.Script{QueueWait: time.Hour})
	h.Step()
	md := mdSpec(hoMD)
	onBoth(md)
	h.create(md)
	h.clock.Advance(10 * time.Second)
	h.Step() // 08:00:10: P
	p := h.instances(hoMD)[0].Name
	setWaits(h.rig, rigProvider, 20, 10*time.Hour)
	setWaits(h.rig, hedgeB, 20, 10*time.Hour)
	h.clock.Advance(10 * time.Second)
	h.Step() // 08:00:20: R1 on A, R2 on B
	var r1 string
	for _, r := range hoRepl(h.rig, p) {
		if r.Labels[v1alpha1.LabelProvider] == rigProvider {
			r1 = r.Name
		}
	}
	if r1 == "" || len(h.membersOn(hoMD, r1, hedgeB)) != 1 {
		t.Fatalf("at 08:00:20 P's replacements are %+v, want R1 on A and its member on B", hoRepl(h.rig, p))
	}
	r2 := h.membersOn(hoMD, r1, hedgeB)[0].Name
	var r3 string
	h.until(5*time.Minute, 10*time.Second, "R3 on A", func() bool {
		for _, in := range h.onProvider(hoMD, rigProvider) {
			if in.Annotations[v1alpha1.AnnotationPredecessor] == p && in.Name != r1 {
				r3 = in.Name
				return true
			}
		}
		return false
	})
	if g := h.mustInstance(r3).Annotations[v1alpha1.AnnotationHedgeGroup]; g != r1 {
		t.Fatalf("R3 names hedge %q, want the hedge's key %s, not R2 %s", g, r1, r2)
	}
	h.until(90*time.Minute, time.Minute, "R2 ready", func() bool { return h.mustInstance(r2).Status.Phase == v1alpha1.InstanceReady })
	h.clock.Advance(10 * time.Second)
	h.Step()
	if in := h.instancesByName()[r3]; in != nil && in.Status.EndReason != v1alpha1.ReasonHedgeLost {
		t.Fatalf("R3 is %s, ended for %q, once R2 won; want HedgeLost", in.Status.Phase, in.Status.EndReason)
	}
	for _, in := range h.onProvider(hoMD, rigProvider) {
		if in.Name != p {
			t.Fatalf("Instance %s still serves on A beside P after R2 won", in.Name)
		}
	}
}

// TestAPassedOverProviderRecordsNothing: growth's search is quiet (HBEP-7, Hedging (beta), A Provider passed over).
// B's QOS allows one job of ours, which another deployment's primary holds; a missing primary with A's waits all 10 h
// is due its member on B. A pass's growth is refused there and records nothing for any condition: no submit-limit
// words for B (noSlot) and no Degraded reason, from the pass's own state after growHedges.
func TestAPassedOverProviderRecordsNothing(t *testing.T) {
	h := newHedgeRigB(t, 4*time.Hour, &v1alpha1.QOSProfile{Name: "example-b1", MaxSubmitPerUser: ptrTo(int32(1))})
	h.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
	h.Step()
	other := mdSpec("md-x")
	other.Spec.Providers.Names = []string{hedgeB}
	h.create(other)
	h.until(time.Minute, 10*time.Second, "md-x on B", func() bool { return len(h.allocations("md-x")) == 1 })
	setWaits(h.rig, rigProvider, 20, 10*time.Hour)
	md := mdSpec(hoMD)
	onBoth(md)
	h.create(md)
	h.Step()
	m1 := h.onProvider(hoMD, rigProvider)
	if len(m1) != 1 || len(h.onProvider(hoMD, hedgeB)) != 0 {
		t.Fatalf("A has %d and B %d of hoMD's Instances, want the missing primary on A alone", len(m1), len(h.onProvider(hoMD, hedgeB)))
	}
	p := h.passOf(hoMD)
	if err := p.growHedges(); err != nil {
		t.Fatal(err)
	}
	if n := len(h.onProvider(hoMD, hedgeB)); n != 0 {
		t.Fatalf("growth placed %d members on a full B", n)
	}
	if w, ok := p.noSlot[hedgeB]; ok {
		t.Fatalf("growth recorded B for SubmitLimitReached: %q", w)
	}
	if len(p.degraded) != 0 {
		t.Fatalf("growth recorded Degraded reasons %+v", p.degraded)
	}
}

// TestAnExtraGetsNoMember (#616; agreed with k616): with k = 1 and A and B allowed, a deployment of one makes R and PR 4's
// extra X on A at 08:00:00, X's job pends 10 h and R's starts at 08:00:10 (the first Allocation by name takes the first
// script, so the roles are the jobs', not the selection's). R is chosen, started and X is surplus, left alone within
// overAllocation's k of 1. A then holds R's 0 s wait, so X's hedge time is its submission plus hedgeMinWait, 08:10:10:
// without the filter its hedge would get a member on B then. It gets none through 08:20:00, and X stays live on A.
func TestAnExtraGetsNoMember(t *testing.T) {
	h := newHedgeRig(t)
	h.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour})
	h.Step()
	md := mdSpec(hoMD)
	onBoth(md)
	md.Spec.OverAllocation = ptrTo(int32(1))
	h.create(md)
	h.Step() // 08:00:00: R and the extra X on A
	onA := h.onProvider(hoMD, rigProvider)
	if len(onA) != 2 {
		t.Fatalf("%d Instances on A at 08:00:00, want R and the extra", len(onA))
	}
	var m string
	stepUntilClock(h.rig, hoT(8, 20, 0), 10*time.Second, func() {
		if n := len(h.onProvider(hoMD, hedgeB)); n != 0 {
			t.Fatalf("at %s B has %d of the deployment's Instances: a surplus Instance's hedge grew", h.clock.Now().Format(time.TimeOnly), n)
		}
	})
	for _, in := range h.onProvider(hoMD, rigProvider) {
		if a, ok := h.allocation(in.Spec.AllocationRef.Name); ok && a.Status.AtProvider.StartTime == nil {
			m = in.Name
		}
	}
	if m == "" {
		t.Fatal("no pending surplus Instance left on A at 08:20:00: the extra was cancelled, so the test proves nothing")
	}
}

// TestASurplusHedgeGetsNoNewMember (#616; agreed with k616): hedgeWidth 3, and A, B and C allowed. A deployment of two
// makes R and M1 on A at 08:00:00, M1 pending 10 h. R's job starts at 08:00:10, so A has one observed wait, 0 s, and M1
// is submitted at 08:00:10. M1's hedge time is its submission plus hedgeMinWait, 08:10:10 (the 0 s wait alone would
// have made it 08:00:10), and there M1's hedge gets M2 on B (pending 10 h). Its count is then lowered to 1, so R is
// chosen and M1's hedge is surplus, and the clock is moved to 08:40:10, past M2's hedge time, 08:30:00 (C has no wait),
// with the Providers' controllers syncing every minute and no reconcile pass in between. A pass's growth adds no third
// member on C, though C is usable and M1's hedge is due one.
//
// On main a surplus hedge of two never reaches growth: step 7, which runs first, takes it whole (#635's scaleUnits).
// So growth is called on its own here, on the pass's state: the filter's contract, for when #616 keeps such a hedge.
func TestASurplusHedgeGetsNoNewMember(t *testing.T) {
	h := newHedgeRig(t)
	h.addProvider("example-c", "gpu-c", 4*time.Hour, nil, nil)
	h.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour})
	h.b.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
	h.Step()
	md := mdSpec(hoMD)
	md.Spec.Providers.Names = []string{rigProvider, hedgeB, "example-c"}
	md.Spec.Instances = 2
	md.Spec.HedgeWidth = ptrTo(int32(3))
	h.create(md)
	h.Step() // 08:00:00
	h.clock.Advance(10 * time.Second)
	h.Step() // 08:00:10: R's job starts and its 0 s wait is recorded; M1 is submitted
	// M1's hedge time is its submission plus hedgeMinWait, 08:10:10: its hedge gets M2 on B then, not at 08:00:10.
	stepUntilClock(h.rig, hoT(8, 10, 10), 10*time.Second, nil)
	onB := h.onProvider(hoMD, hedgeB)
	if len(onB) != 1 || onB[0].Annotations[v1alpha1.AnnotationHedgeGroup] == "" {
		t.Fatalf("B has %+v at 08:10:10, want M1's hedge member: the fixture has no hedge of two, so the test proves nothing", onB)
	}
	m1, m2 := onB[0].Annotations[v1alpha1.AnnotationHedgeGroup], onB[0].Name
	h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	// To 08:40:10 with no reconcile pass, the Providers' controllers syncing each minute so their heartbeats stay
	// fresh: a Provider unsynced for 30 min reads unusable, and growth would have nowhere to go.
	for range 30 {
		h.clock.Advance(time.Minute)
		if err := h.prov.Sync(h.ctx); err != nil {
			t.Fatal(err)
		}
		for _, x := range h.extra {
			if err := x.prov.Sync(h.ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	p := h.passOf(hoMD)
	chosen := p.primaries()
	if len(chosen) != 1 || hedgeKey(chosen[0]) == m1 {
		t.Fatalf("the chosen primaries are %v, want R alone: M1's hedge must be surplus, or the test proves nothing", names(chosen))
	}
	if pv := p.pvByName["example-c"]; pv == nil || !pv.usable() {
		t.Fatal("C is not usable at 08:40:10: growth could place nothing there anyway, so the test proves nothing")
	}
	if err := p.growHedges(); err != nil {
		t.Fatal(err)
	}
	if n := len(h.onProvider(hoMD, "example-c")); n != 0 {
		t.Fatalf("C has %d of the deployment's Instances: growth added a member to a surplus hedge", n)
	}
	for _, n := range []string{m1, m2} {
		if in := h.instancesByName()[n]; in == nil || ended(in) {
			t.Fatalf("member %s of the surplus hedge is gone or ended: growth must not touch it", n)
		}
	}
}
