package scenario_test

// Issue #193's checks on their own, in an ordinary go test (no
// HPCBURST_REHEARSAL): each is fed a fixture that a correct run gives, which
// must pass, and fixtures that break one rule each, which must fail. A check
// that passes a broken fixture would pass a broken product too.

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// TestHBEP3Tag: the tag from hand vectors, the hash taken with sha256sum
// outside Go: printf %s '<ns>/<name>/<uid>' | sha256sum | cut -c1-12.
func TestHBEP3Tag(t *testing.T) {
	for _, c := range []struct{ ns, name, uid, want string }{
		{"default", "rehearsal-queued-x7k2p", "0b9a3c1e-5d2f-4e8a-9b7c-123456789abc", "hb-rehearsal-queued-x7k2p-754bd4d66be4"},
		{"example-ns", "a-very-long-allocation-name-that-passes-forty-characters", "00000000-1111-2222-3333-444444444444", "hb-a-very-long-allocation-name-that-passes--2a98f9f18820"},
	} {
		if got := hbep3Tag(c.ns, c.name, types.UID(c.uid)); got != c.want {
			t.Errorf("hbep3Tag(%s, %s, %s) = %s, want %s", c.ns, c.name, c.uid, got, c.want)
		}
	}
}

func gapFixture() gapInput {
	ds := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	in := gapInput{drainStart: ds, succReady: ds.Add(80 * time.Second), succReadySeen: ds.Add(80*time.Second + 300*time.Millisecond), primaryUID: "p", succUID: "s"}
	for i := -10; i < 100; i++ {
		at := ds.Add(time.Duration(i) * time.Second)
		switch {
		case i < 0:
			in.probes = append(in.probes, gapProbe{start: at, status: 200, emitter: "instance", instanceUID: "p"})
		case i < 82:
			in.probes = append(in.probes, gapProbe{start: at.Add(200 * time.Millisecond), status: 503, emitter: "front", code: "model_inactive", detail: "no_ready_target", retryAfter: "30"})
		default:
			in.probes = append(in.probes, gapProbe{start: at, status: 200, emitter: "instance", instanceUID: "s"})
		}
	}
	return in
}

func TestGapProblems(t *testing.T) {
	if rep, p := gapProblems(gapFixture()); len(p) != 0 || rep.inGap < minGapProbes {
		t.Fatalf("a correct gap: %v (%d in the gap)", p, rep.inGap)
	}
	for name, breakIt := range map[string]func(*gapInput){
		"no request in the gap": func(in *gapInput) {
			var keep []gapProbe
			for _, p := range in.probes {
				if p.status != 503 {
					keep = append(keep, p)
				}
			}
			in.probes = keep
		},
		"the primary serves inside the gap": func(in *gapInput) {
			in.probes[40] = gapProbe{start: in.probes[40].start, status: 200, emitter: "instance", instanceUID: "p"}
		},
		"a 503 with another detail": func(in *gapInput) { in.probes[40].detail = "not_attached" },
		"a 503 with no Retry-After": func(in *gapInput) { in.probes[40].retryAfter = "" },
		"a 503 after the successor served": func(in *gapInput) {
			in.probes[105] = gapProbe{start: in.probes[105].start, status: 503, emitter: "front", code: "model_inactive", detail: "no_ready_target", retryAfter: "30"}
		},
		"a cut (502) in the gap": func(in *gapInput) {
			in.probes[40] = gapProbe{start: in.probes[40].start, status: 502, emitter: "front", code: "upstream_closed"}
		},
		"an unanswered probe": func(in *gapInput) { in.probes[40] = gapProbe{err: "connection reset"} },
		"the successor serves before its ready time": func(in *gapInput) {
			in.probes[60] = gapProbe{start: in.probes[60].start, status: 200, emitter: "instance", instanceUID: "s"}
		},
		"the successor never serves": func(in *gapInput) { in.probes = in.probes[:90] },
	} {
		in := gapFixture()
		breakIt(&in)
		if _, p := gapProblems(in); len(p) == 0 {
			t.Errorf("%s: gapProblems found nothing", name)
		}
	}
}

func slotFixture(ds time.Time) []mdObservation {
	at := func(s int) time.Time { return ds.Add(time.Duration(s) * time.Second) }
	return []mdObservation{
		{at: at(-200), degraded: "False", degradedReason: "AsDesired"},
		{at: at(-100), degraded: "False", degradedReason: "AsDesired"},
		{at: at(1), degraded: "True", degradedReason: v1alpha1.ReasonNoOverlapSlot, degradedMessage: "NoOverlapSlot: Instance a drained | CapacityDropped: 0 of 1 ready"},
		{at: at(40), degraded: "True", degradedReason: v1alpha1.ReasonNoOverlapSlot},
		{at: at(85), degraded: "False", degradedReason: "AsDesired"},
	}
}

func TestNoOverlapSlotProblems(t *testing.T) {
	ds := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	ready := ds.Add(80 * time.Second)
	if p := noOverlapSlotProblems(slotFixture(ds), ds, ready); len(p) != 0 {
		t.Fatalf("a correct condition history: %v", p)
	}
	for name, breakIt := range map[string]func([]mdObservation) []mdObservation{
		"NoOverlapSlot before the drain start": func(o []mdObservation) []mdObservation {
			o[1].degraded, o[1].degradedReason = "True", v1alpha1.ReasonNoOverlapSlot
			return o
		},
		"ReplacementLate as the reason": func(o []mdObservation) []mdObservation {
			o[1].degraded, o[1].degradedReason = "True", v1alpha1.ReasonReplacementLate
			return o
		},
		"ReplacementLate in the message": func(o []mdObservation) []mdObservation {
			o[2].degradedMessage += " | ReplacementLate: the replacement has not started"
			return o
		},
		"cleared before the successor is ready": func(o []mdObservation) []mdObservation {
			o[3].degraded, o[3].degradedReason = "False", "AsDesired"
			return o
		},
		"never NoOverlapSlot": func(o []mdObservation) []mdObservation { return append(o[:2], o[4]) },
		"NoOverlapSlot as the reason of a Degraded that is False": func(o []mdObservation) []mdObservation {
			o[2].degraded, o[3].degraded = "False", "False"
			return o
		},
		"NoOverlapSlot only after the successor is ready": func(o []mdObservation) []mdObservation {
			return []mdObservation{o[0], {at: ds.Add(90 * time.Second), degraded: "True", degradedReason: v1alpha1.ReasonNoOverlapSlot}}
		},
	} {
		if p := noOverlapSlotProblems(breakIt(slotFixture(ds)), ds, ready); len(p) == 0 {
			t.Errorf("%s: noOverlapSlotProblems found nothing", name)
		}
	}
}

func attrFixture() (http.Header, stack.UsageRow, wantAttr) {
	h := http.Header{}
	h.Set(hdrRequestID, "r1")
	h.Set(hdrEmitter, "instance")
	h.Set(hdrProvider, "prov")
	h.Set(hdrDeploymentID, "0123456789ab")
	h.Set(hdrInstance, "default/inst")
	h.Set(hdrInstanceUID, "uid-1")
	row := stack.UsageRow{RequestID: "r1", Emitter: "instance", Provider: "prov", DeploymentID: "0123456789ab" + strings.Repeat("c", 52),
		Instance: "default/inst", InstanceUID: "uid-1", Status: 200}
	return h, row, wantAttr{emitter: "instance", provider: "prov", deploymentID: "0123456789ab", instance: "default/inst", instanceUID: "uid-1"}
}

func TestAttributionProblems(t *testing.T) {
	h, row, w := attrFixture()
	if p := attributionProblems(200, h, row, w); len(p) != 0 {
		t.Fatalf("a correct reply: %v", p)
	}
	fh := http.Header{}
	fh.Set(hdrRequestID, "r2")
	fh.Set(hdrEmitter, "front")
	frow := stack.UsageRow{RequestID: "r2", Emitter: "front", Status: 503}
	if p := attributionProblems(503, fh, frow, wantFront()); len(p) != 0 {
		t.Fatalf("a correct front error: %v", p)
	}
	for name, breakIt := range map[string]func(http.Header, *stack.UsageRow){
		"no request ID":             func(h http.Header, _ *stack.UsageRow) { h.Del(hdrRequestID) },
		"another request's record":  func(_ http.Header, r *stack.UsageRow) { r.RequestID = "r9" },
		"no Instance header":        func(h http.Header, _ *stack.UsageRow) { h.Del(hdrInstance) },
		"a record naming another":   func(_ http.Header, r *stack.UsageRow) { r.InstanceUID = "uid-2" },
		"a full ID in the header":   func(h http.Header, r *stack.UsageRow) { h.Set(hdrDeploymentID, r.DeploymentID) },
		"a short ID in the record":  func(_ http.Header, r *stack.UsageRow) { r.DeploymentID = "0123456789ab" },
		"another status recorded":   func(_ http.Header, r *stack.UsageRow) { r.Status = 500 },
		"the wrong emitter":         func(h http.Header, r *stack.UsageRow) { h.Set(hdrEmitter, "front"); r.Emitter = "front" },
		"no provider in the record": func(_ http.Header, r *stack.UsageRow) { r.Provider = "" },
	} {
		h, row, w := attrFixture()
		breakIt(h, &row)
		if p := attributionProblems(200, h, row, w); len(p) == 0 {
			t.Errorf("%s: attributionProblems found nothing", name)
		}
	}
	fh.Set(hdrProvider, "prov")
	if p := attributionProblems(503, fh, frow, wantFront()); len(p) == 0 {
		t.Errorf("a front error before a target that names a provider: attributionProblems found nothing")
	}
}

func TestOneSlotProblems(t *testing.T) {
	t0 := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	sb := func(s int, name string) fakeslurm.Call {
		return fakeslurm.Call{Words: []string{"sbatch", "--parsable", "--job-name=" + name, "--no-requeue", "--hold", "--partition=one"}, Time: at(s)}
	}
	fixture := func() ([]fakeslurm.Call, []fakeslurm.Job, []submitAttempt) {
		return []fakeslurm.Call{sb(0, "hb-a"), sb(400, "hb-b")},
			[]fakeslurm.Job{
				{ID: 1, Name: "hb-a", Partition: "one", Submit: at(0), End: at(380), Finished: true},
				{ID: 2, Name: "hb-b", Partition: "one", Submit: at(400)},
			},
			[]submitAttempt{
				{at: at(0), name: "hb-a", partition: "one", jobID: 1},
				{at: at(400), name: "hb-b", partition: "one", jobID: 2},
			}
	}
	calls, jobs, atts := fixture()
	if p := oneSlotProblems(calls, jobs, atts, "one", 2, at(390)); len(p) != 0 {
		t.Fatalf("a correct one-slot history: %v", p)
	}
	for name, breakIt := range map[string]func(c *[]fakeslurm.Call, j *[]fakeslurm.Job, a *[]submitAttempt){
		// Review of PR #247: a refused try retried under the same tag.
		"a refused sbatch at 200 retried under the same tag hb-b": func(c *[]fakeslurm.Call, _ *[]fakeslurm.Job, _ *[]submitAttempt) {
			*c = append(*c, sb(200, "hb-b"))
		},
		"a refused sbatch under another tag": func(c *[]fakeslurm.Call, _ *[]fakeslurm.Job, _ *[]submitAttempt) {
			*c = append(*c, sb(200, "hb-c"))
		},
		"an attempt that exited 1": func(_ *[]fakeslurm.Call, _ *[]fakeslurm.Job, a *[]submitAttempt) {
			*a = append(*a, submitAttempt{at: at(200), name: "hb-b", partition: "one", exit: 1})
		},
		"an attempt the runner failed": func(_ *[]fakeslurm.Call, _ *[]fakeslurm.Job, a *[]submitAttempt) {
			*a = append(*a, submitAttempt{at: at(200), name: "hb-b", partition: "one", err: "command timed out"})
		},
		// Review of PR #247: the predecessor ended at 380 and completed until
		// 410; a sample at 390 saw it last, the sbatch at 400 still did.
		"a successor submitted while the predecessor still completed": func(_ *[]fakeslurm.Call, _ *[]fakeslurm.Job, a *[]submitAttempt) {
			(*a)[1].holders = []int{1}
		},
		"the successor submitted before the predecessor was last seen holding the slot": func(_ *[]fakeslurm.Call, j *[]fakeslurm.Job, _ *[]submitAttempt) {
			(*j)[1].Submit = at(300)
		},
		"a requeued job, whose new execution no sbatch shows": func(_ *[]fakeslurm.Call, j *[]fakeslurm.Job, _ *[]submitAttempt) {
			(*j)[0].RestartCount = 1
		},
		"a job no recorded attempt registered": func(_ *[]fakeslurm.Call, _ *[]fakeslurm.Job, a *[]submitAttempt) {
			*a = (*a)[:1]
		},
	} {
		c, j, a := fixture()
		breakIt(&c, &j, &a)
		if p := oneSlotProblems(c, j, a, "one", 2, at(390)); len(p) == 0 {
			t.Errorf("%s: oneSlotProblems found nothing", name)
		}
	}
}
