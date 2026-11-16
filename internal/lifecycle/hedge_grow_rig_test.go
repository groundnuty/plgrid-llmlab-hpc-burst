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
	h.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
	h.Step() // the Providers' first status: Ready
	if w > 0 {
		setWaits(h.rig, rigProvider, 20, w)
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
//   - no wait on A: R1 is made at P's start, and its expected start is the deadline minus the margin, 17:19:30: B's
//     member comes then, none at 17:19:20.
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
		r1Wait time.Duration
		at     time.Time // zero: never
	}{
		{"A's waits 2 h", 2 * time.Hour, 10 * time.Hour, hoT(17, 19, 40)},
		{"A's waits 2 h, R1 starts at 17:09:40", 2 * time.Hour, time.Hour*1 + 50*time.Minute, time.Time{}},
		{"no wait on A", 0, 10 * time.Hour, hoT(17, 19, 30)},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHedgeRig(t, withMaxTime(12*time.Hour))
			p := replacementStart(t, h, c.r1Wait)
			stepUntilClock(h.rig, hoT(8, 1, 10), 10*time.Second, nil) // P ready at 08:01:10
			if c.wait > 0 {
				setWaits(h.rig, rigProvider, 20, c.wait)
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
			stepUntilClock(h.rig, end.Add(-30*time.Second), 5*time.Minute, check)
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
