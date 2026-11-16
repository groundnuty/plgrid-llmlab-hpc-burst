package e2e

// Over-allocation's and the throughput gate's API (beta; HBEP-7, What the controller reads and Over-allocation and
// qualification (beta); #616 PR A): the ModelDeployment's spec.overAllocation, the gate's verdict and the qualification
// failure count on its status.startOutcomes, and the Instance's status.gate. Every expected value is written out here.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestOverAllocationDefaultsAndBounds: a deployment written without overAllocation reads back 1, the API server's
// default; 0 and 1 are accepted and read back as written (0 turns over-allocation off), and -1 and 2 are refused.
func TestOverAllocationDefaultsAndBounds(t *testing.T) {
	ns := newNamespace(t)
	md := newModelDeployment(ns, "over-default")
	must(t, c.Create(bg(), md), "create without overAllocation")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	if got.Spec.OverAllocation == nil || *got.Spec.OverAllocation != 1 {
		t.Fatalf("overAllocation read back as %v, want the default 1", got.Spec.OverAllocation)
	}
	if k := got.Spec.OverAllocationCount(); k != 1 {
		t.Errorf("OverAllocationCount() = %d, want 1", k)
	}
	for _, tc := range []struct {
		name string
		n    int32
		want string // "" accepts
	}{
		{"zero", 0, ""},
		{"one", 1, ""},
		{"minus one", -1, "should be greater than or equal to 0"},
		{"two", 2, "should be less than or equal to 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			md := newModelDeployment(ns, uniq("over"))
			v := tc.n
			md.Spec.OverAllocation = &v
			err := c.Create(bg(), md)
			if tc.want != "" {
				requireRefused(t, err, "overAllocation", tc.want)
				return
			}
			must(t, err, "create with overAllocation in bounds")
			back := &v1alpha1.ModelDeployment{}
			must(t, c.Get(bg(), objKey(md), back), "get")
			if back.Spec.OverAllocation == nil || *back.Spec.OverAllocation != tc.n {
				t.Errorf("overAllocation %d read back as %v", tc.n, back.Spec.OverAllocation)
			}
		})
	}
}

// TestStartOutcomeGateTakesFourVerdicts: an outcome's gate is one of Passed, Missed, Suspended and RecordOnly, and is
// kept as written with the matched entry's date; any other word is refused, and an outcome without a gate is fine (the
// Instance never reached it).
func TestStartOutcomeGateTakesFourVerdicts(t *testing.T) {
	ns := newNamespace(t)
	gated := func(uid, verdict, date string) v1alpha1.StartOutcome {
		o := outcome(uid, "Ready", "")
		o.Gate, o.ReferenceDate = verdict, date
		return o
	}
	md, err := withStartOutcomes(t, ns, "gates", classOutcomes(0, 5,
		gated("uid-1", v1alpha1.GatePassed, "2026-11-10"),
		gated("uid-2", v1alpha1.GateMissed, "2026-11-11"),
		gated("uid-3", v1alpha1.GateSuspended, "2026-11-12"),
		gated("uid-4", v1alpha1.GateRecordOnly, ""),
		outcome("uid-5", "Ready", "")))
	must(t, err, "write one outcome of each verdict, and one with none")
	got := &v1alpha1.ModelDeployment{}
	must(t, c.Get(bg(), objKey(md), got), "get")
	outs := got.Status.StartOutcomes[0].Outcomes
	for i, want := range [][2]string{{"Passed", "2026-11-10"}, {"Missed", "2026-11-11"}, {"Suspended", "2026-11-12"}, {"RecordOnly", ""}, {"", ""}} {
		if outs[i].Gate != want[0] || outs[i].ReferenceDate != want[1] {
			t.Errorf("outcome %d reads back with gate %q and referenceDate %q, want %q and %q", i+1, outs[i].Gate, outs[i].ReferenceDate, want[0], want[1])
		}
	}
	for _, verdict := range []string{"Failed", "passed", "RecordsOnly", "Unknown"} {
		t.Run("refuse "+verdict, func(t *testing.T) {
			_, err := withStartOutcomes(t, ns, uniq("badgate"), classOutcomes(0, 1, gated("uid-1", verdict, "")))
			requireRefused(t, err, "gate", "Unsupported value")
		})
	}
}

// TestQualificationFailuresAgree: qualificationFailures is the number of outcomes kept whose result is Failed and whose
// reason is VerificationFailed or QualificationFailed, absent counting as 0 (rule
// modeldeployments:$.status.startOutcomes[]#2, beside #606's failures rule). A failure of any other reason is no
// qualification failure, and a Ready outcome never is.
func TestQualificationFailuresAgree(t *testing.T) {
	ns := newNamespace(t)
	ready := outcome("uid-1", "Ready", "")
	verification := outcome("uid-2", "Failed", "VerificationFailed")
	qualification := outcome("uid-3", "Failed", "QualificationFailed")
	registration := outcome("uid-4", "Failed", "RegistrationTimeout")
	const want = "qualificationFailures is the number of outcomes kept whose result is Failed"
	with := func(qf, failures int32, outs ...v1alpha1.StartOutcome) v1alpha1.ClassStartOutcomes {
		cl := classOutcomes(failures, int32(len(outs)), outs...)
		cl.QualificationFailures = qf
		return cl
	}
	for _, tc := range []struct {
		name string
		cl   v1alpha1.ClassStartOutcomes
		ok   bool
	}{
		{"none, with no outcome", with(0, 0), true},
		{"none, with a Ready outcome", with(0, 0, ready), true},
		{"a VerificationFailed counts", with(1, 1, ready, verification), true},
		{"a QualificationFailed counts", with(1, 1, ready, qualification), true},
		{"both count", with(2, 2, verification, qualification, ready), true},
		{"a RegistrationTimeout is a start-up failure but no qualification failure", with(0, 1, registration, ready), true},
		{"one of each: two of three failures qualify", with(2, 3, verification, qualification, registration), true},
		{"a VerificationFailed missing from the count", with(0, 1, verification), false},
		{"a QualificationFailed missing from the count", with(0, 1, qualification, ready), false},
		{"a count above the qualifying outcomes", with(2, 1, qualification), false},
		{"a count with no outcome", with(1, 0), false},
		{"a RegistrationTimeout counted", with(1, 1, registration), false},
		{"a Ready outcome counted", with(1, 0, ready), false},
		{"every failure counted, though one is a RegistrationTimeout", with(3, 3, verification, qualification, registration), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := withStartOutcomes(t, ns, uniq("qf"), tc.cl)
			if tc.ok {
				must(t, err, "write "+tc.name)
				return
			}
			requireRefused(t, err, want)
		})
	}
	t.Run("a count is kept as written", func(t *testing.T) {
		md, err := withStartOutcomes(t, ns, "qf-roundtrip", with(2, 3, verification, qualification, registration))
		must(t, err, "write")
		got := &v1alpha1.ModelDeployment{}
		must(t, c.Get(bg(), objKey(md), got), "get")
		if cl := got.Status.StartOutcomes[0]; cl.QualificationFailures != 2 || cl.Failures != 3 || cl.Count != 3 {
			t.Fatalf("read back %d qualification failures, %d failures, count %d; want 2, 3, 3", cl.QualificationFailures, cl.Failures, cl.Count)
		}
	})
	t.Run("at most 20", func(t *testing.T) {
		var outs []v1alpha1.StartOutcome
		for i := 0; i < 21; i++ {
			outs = append(outs, outcome("uid-"+string(rune('a'+i)), "Failed", "QualificationFailed"))
		}
		_, err := withStartOutcomes(t, ns, uniq("qf20"), with(21, 21, outs...))
		requireRefused(t, err, "qualificationFailures")
	})
}

// writeInstanceGate writes the gate record to a fresh Instance's status and returns the error of that write.
func writeInstanceGate(t *testing.T, gate *v1alpha1.InstanceGate) error {
	t.Helper()
	in := newInstance(newNamespace(t), uniq("gate"))
	must(t, c.Create(bg(), in), "create")
	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), got), "get")
	got.Status.Gate = gate
	return c.Status().Update(bg(), got)
}

// miss is one measurement that missed, at a fixed time.
func miss(worker string, tpot, ttft time.Duration) v1alpha1.GateMiss {
	return v1alpha1.GateMiss{Worker: worker, Time: metav1.NewTime(time.Date(2026, 11, 15, 9, 0, 0, 0, time.UTC)),
		Throughput: v1alpha1.Throughput{TimePerOutputToken: v1alpha1.Duration(tpot.String()), TimeToFirstToken: v1alpha1.Duration(ttft.String()), TargetSessions: 8}}
}

// TestInstanceGateRoundTrips: an Instance's gate record holds a verdict, the matched entry's date and the measurements
// that missed, one per worker and attempt; a first miss with no verdict yet is the record of a retry due.
func TestInstanceGateRoundTrips(t *testing.T) {
	ns := newNamespace(t)
	in := newInstance(ns, "gate")
	must(t, c.Create(bg(), in), "create")
	got := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), got), "get")

	// A retry due: one miss, no verdict.
	got.Status.Gate = &v1alpha1.InstanceGate{ReferenceDate: "2026-11-10", Misses: []v1alpha1.GateMiss{miss("worker-0", 41*time.Millisecond, 410*time.Millisecond)}}
	must(t, c.Status().Update(bg(), got), "write a first miss")
	back := &v1alpha1.Instance{}
	must(t, c.Get(bg(), objKey(in), back), "get")
	g := back.Status.Gate
	if g == nil || g.Verdict != "" || g.ReferenceDate != "2026-11-10" || len(g.Misses) != 1 ||
		g.Misses[0].Worker != "worker-0" || g.Misses[0].Throughput.TimePerOutputToken != "41ms" ||
		g.Misses[0].Throughput.TimeToFirstToken != "410ms" || g.Misses[0].Throughput.TargetSessions != 8 {
		t.Fatalf("read back %+v, want a first miss of worker-0 at 41ms and 410ms with no verdict", g)
	}

	// The final verdict, after the retry missed too: a miss for worker-0 and one for worker-1.
	must(t, c.Get(bg(), objKey(in), got), "get")
	got.Status.Gate = &v1alpha1.InstanceGate{Verdict: v1alpha1.GateMissed, ReferenceDate: "2026-11-10", Misses: []v1alpha1.GateMiss{
		miss("worker-0", 41*time.Millisecond, 410*time.Millisecond), miss("worker-0", 42*time.Millisecond, 420*time.Millisecond), miss("worker-1", 45*time.Millisecond, 400*time.Millisecond)}}
	must(t, c.Status().Update(bg(), got), "write the final verdict")
	must(t, c.Get(bg(), objKey(in), back), "get")
	if g := back.Status.Gate; g == nil || g.Verdict != "Missed" || len(g.Misses) != 3 || g.Misses[2].Worker != "worker-1" {
		t.Fatalf("read back %+v, want Missed with three misses, the last of worker-1", g)
	}

	// Each of the four verdicts is accepted, with no date for RecordOnly.
	for _, v := range []string{v1alpha1.GatePassed, v1alpha1.GateMissed, v1alpha1.GateSuspended, v1alpha1.GateRecordOnly} {
		must(t, writeInstanceGate(t, &v1alpha1.InstanceGate{Verdict: v}), "write the verdict "+v)
	}
	// Refused: another verdict, and a worker that is no worker-<index>.
	requireRefused(t, writeInstanceGate(t, &v1alpha1.InstanceGate{Verdict: "Failed"}), "verdict", "Unsupported value")
	requireRefused(t, writeInstanceGate(t, &v1alpha1.InstanceGate{Misses: []v1alpha1.GateMiss{miss("worker-x", time.Millisecond, time.Millisecond)}}), "worker")
	requireRefused(t, writeInstanceGate(t, &v1alpha1.InstanceGate{Misses: []v1alpha1.GateMiss{miss("worker-", time.Millisecond, time.Millisecond)}}), "worker")
	requireRefused(t, writeInstanceGate(t, &v1alpha1.InstanceGate{Misses: []v1alpha1.GateMiss{miss("instance", time.Millisecond, time.Millisecond)}}), "worker")
	must(t, writeInstanceGate(t, &v1alpha1.InstanceGate{Misses: []v1alpha1.GateMiss{miss("worker-12", time.Millisecond, time.Millisecond)}}), "worker-12")
	// At most 16 misses: two a worker, eight workers.
	var sixteen []v1alpha1.GateMiss
	for i := 0; i < 16; i++ {
		sixteen = append(sixteen, miss("worker-0", time.Millisecond, time.Millisecond))
	}
	must(t, writeInstanceGate(t, &v1alpha1.InstanceGate{Misses: sixteen}), "16 misses")
	requireRefused(t, writeInstanceGate(t, &v1alpha1.InstanceGate{Misses: append(sixteen, miss("worker-0", time.Millisecond, time.Millisecond))}), "must have at most 16 items")
	// A miss needs its measurement and its time.
	noTime := miss("worker-0", time.Millisecond, time.Millisecond)
	noTime.Time = metav1.Time{}
	requireRefused(t, writeInstanceGate(t, &v1alpha1.InstanceGate{Misses: []v1alpha1.GateMiss{noTime}}), "time")
}
