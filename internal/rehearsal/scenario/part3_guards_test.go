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

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
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

func event(action, reason, kind, name, controller, typ string) eventsv1.Event {
	return eventsv1.Event{ObjectMeta: metav1.ObjectMeta{Name: "e-" + reason}, Action: action, Reason: reason, Type: typ,
		Regarding: corev1.ObjectReference{Kind: kind, Namespace: stack.Namespace, Name: name}, ReportingController: controller, Note: "n"}
}

func TestEventProblems(t *testing.T) {
	want := []wantEvent{
		{action: "ready", reason: "Ready", kind: "Instance", ns: stack.Namespace, name: "p", controller: lifecycleController},
		{action: "degraded", reason: "CapacityDropped", kind: "ModelDeployment", ns: stack.Namespace, name: "md", controller: lifecycleController, related: "p"},
	}
	forbid := []wantEvent{{action: "degraded", reason: "ReplacementLate", kind: "ModelDeployment", ns: stack.Namespace, name: "md"}}
	good := func() []eventsv1.Event {
		cd := event("degraded", "CapacityDropped", "ModelDeployment", "md", lifecycleController, provider.EventKindWarning)
		cd.Related = &corev1.ObjectReference{Kind: "Instance", Name: "p"}
		return []eventsv1.Event{
			event("ready", "Ready", "Instance", "p", lifecycleController, provider.EventKindOrdinary), cd,
			// The Provider controller's untyped diagnostics are left out.
			{ObjectMeta: metav1.ObjectMeta{Name: "x"}, Reason: "SubmittedJob", Type: provider.EventKindOrdinary,
				Regarding: corev1.ObjectReference{Kind: "Allocation", Name: "a"}, ReportingController: "provider"},
		}
	}
	if m, p := eventProblems(good(), want, forbid); len(m) != 0 || len(p) != 0 {
		t.Fatalf("a correct set: missing %v, problems %v", m, p)
	}
	for name, breakIt := range map[string]func([]eventsv1.Event) []eventsv1.Event{
		"an expected event missing":  func(e []eventsv1.Event) []eventsv1.Event { return e[1:] },
		"ready emitted as a Warning": func(e []eventsv1.Event) []eventsv1.Event { e[0].Type = provider.EventKindWarning; return e },
		"degraded emitted as Normal": func(e []eventsv1.Event) []eventsv1.Event { e[1].Type = provider.EventKindOrdinary; return e },
		"a compound reason":          func(e []eventsv1.Event) []eventsv1.Event { e[0].Reason = "ready/Ready"; return e },
		"no action":                  func(e []eventsv1.Event) []eventsv1.Event { e[0].Action = ""; return e },
		"no related Instance":        func(e []eventsv1.Event) []eventsv1.Event { e[1].Related = nil; return e },
		"regarding another object":   func(e []eventsv1.Event) []eventsv1.Event { e[1].Regarding.Name = "other"; return e },
		"a note over 1,024 bytes":    func(e []eventsv1.Event) []eventsv1.Event { e[0].Note = strings.Repeat("x", 1025); return e },
		"a forbidden ReplacementLate": func(e []eventsv1.Event) []eventsv1.Event {
			return append(e, event("degraded", "ReplacementLate", "ModelDeployment", "md", lifecycleController, provider.EventKindWarning))
		},
		"a TunnelOrphaned": func(e []eventsv1.Event) []eventsv1.Event {
			return append(e, event("degraded", v1alpha1.EventReasonTunnelOrphaned, "Allocation", "a", lifecycleController, provider.EventKindWarning))
		},
	} {
		if m, p := eventProblems(breakIt(good()), want, forbid); len(m) == 0 && len(p) == 0 {
			t.Errorf("%s: eventProblems found nothing", name)
		}
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

func gaugeFixture() []sample {
	uid := map[string]string{"instance_uid": "u", "worker": "worker-0", "generation": "1", "worker_generation": "1"}
	var ss []sample
	ss = append(ss, sample{name: "hpcburst_instance_info", value: 1, labels: map[string]string{"instance_uid": "u", "namespace": "default",
		"instance": "i", "model_deployment": "md", "provider": "p", "gpu_model": "g"}})
	for _, g := range engineGauges {
		if notReported[g] {
			ss = append(ss, sample{name: "hpcburst_instance_metrics_unknown", value: 1,
				labels: map[string]string{"instance_uid": "u", "worker": "worker-0", "metric": g, "reason": "not_reported"}})
			continue
		}
		for _, n := range seriesNames(g) {
			v := 1.0
			if g == gGenerated {
				v = 9000
			}
			ss = append(ss, sample{name: n, value: v, labels: uid})
		}
	}
	return append(ss, sample{name: "hpcburst_instance_metrics_read_timestamp_seconds", value: 1, labels: map[string]string{"instance_uid": "u", "worker": "worker-0"}})
}

func TestGaugeProblems(t *testing.T) {
	v := gaugeView{uid: "u", namespace: "default", name: "i", deployment: "md", provider: "p", gpuModel: "g", reported: true, minGenerated: 8000}
	if p := gaugeProblems(gaugeFixture(), v); len(p) != 0 {
		t.Fatalf("a correct exposition: %v", p)
	}
	for name, breakIt := range map[string]func([]sample) []sample{
		"no info series": func(s []sample) []sample { return s[1:] },
		"an unreported gauge exported as 0": func(s []sample) []sample {
			return append(s, sample{name: gPrefixQ, value: 0, labels: map[string]string{"instance_uid": "u", "worker": "worker-0"}})
		},
		"a gauge both present and unknown": func(s []sample) []sample {
			return append(s, sample{name: "hpcburst_instance_metrics_unknown", value: 1,
				labels: map[string]string{"instance_uid": "u", "worker": "worker-0", "metric": gKV, "reason": "stale"}})
		},
		"a gauge neither present nor unknown": func(s []sample) []sample {
			var out []sample
			for _, x := range s {
				if x.name != gWaiting {
					out = append(out, x)
				}
			}
			return out
		},
		"the summary without its count": func(s []sample) []sample {
			var out []sample
			for _, x := range s {
				if x.name != gTPOT+"_count" {
					out = append(out, x)
				}
			}
			return out
		},
		"the generated tokens below what was served": func(s []sample) []sample {
			for i := range s {
				if s[i].name == gGenerated {
					s[i].value = 10
				}
			}
			return s
		},
	} {
		if p := gaugeProblems(breakIt(gaugeFixture()), v); len(p) == 0 {
			t.Errorf("%s: gaugeProblems found nothing", name)
		}
	}
	stale := []sample{{name: "hpcburst_instance_info", value: 1, labels: map[string]string{"instance_uid": "s", "namespace": "default", "instance": "j",
		"model_deployment": "md", "provider": "p", "gpu_model": "g"}}}
	for _, g := range engineGauges {
		stale = append(stale, sample{name: "hpcburst_instance_metrics_unknown", value: 1,
			labels: map[string]string{"instance_uid": "s", "worker": "worker-0", "metric": g, "reason": "stale"}})
	}
	sv := gaugeView{uid: "s", namespace: "default", name: "j", deployment: "md", provider: "p", gpuModel: "g", staleReason: "stale"}
	if p := gaugeProblems(stale, sv); len(p) != 0 {
		t.Fatalf("a correct pending Instance: %v", p)
	}
	if p := gaugeProblems(stale[:5], sv); len(p) == 0 {
		t.Errorf("a pending Instance missing unknown samples: gaugeProblems found nothing")
	}
}

func TestParseExposition(t *testing.T) {
	ss, err := parseExposition("# HELP x y\n# TYPE x counter\nx_total{a=\"1\",b=\"q\\\"z\"} 3\nup 1\nh_bucket{le=\"+Inf\"} 2\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(ss) != 3 || ss[0].name != "x_total" || ss[0].labels["b"] != `q"z` || ss[0].value != 3 || ss[1].name != "up" || ss[2].labels["le"] != "+Inf" {
		t.Errorf("parseExposition: %+v", ss)
	}
	if _, err := parseExposition("x{a=\"1\" 3\n"); err == nil {
		t.Errorf("an unterminated label set parsed")
	}
}

func staleFixture(uid string) []sample {
	ss := []sample{{name: "hpcburst_instance_info", value: 1, labels: map[string]string{"instance_uid": uid, "namespace": "default", "instance": "j",
		"model_deployment": "md", "provider": "p", "gpu_model": "g"}}}
	for _, g := range engineGauges {
		ss = append(ss, sample{name: "hpcburst_instance_metrics_unknown", value: 1,
			labels: map[string]string{"instance_uid": uid, "worker": "worker-0", "metric": g, "reason": "stale"}})
	}
	return ss
}

func TestPendingGaugeProblems(t *testing.T) {
	v := gaugeView{uid: "s", namespace: "default", name: "j", deployment: "md", provider: "p", gpuModel: "g", staleReason: "stale"}
	if p, none := pendingGaugeProblems(staleFixture("s"), v); len(p) != 0 || none {
		t.Fatalf("a pending Instance shown stale: %v (none %v)", p, none)
	}
	if p, none := pendingGaugeProblems(staleFixture("s")[:1], v); len(p) != 0 || !none {
		t.Fatalf("a pending Instance with its info series alone: %v (none %v), want no problem and none", p, none)
	}
	for name, ss := range map[string][]sample{
		"no info series":        staleFixture("s")[1:],
		"some unknowns only":    staleFixture("s")[:4],
		"a gauge read as 0":     append(staleFixture("s"), sample{name: gRunning, labels: map[string]string{"instance_uid": "s", "worker": "worker-0"}}),
		"unknown, not as stale": append(staleFixture("s")[:1], sample{name: "hpcburst_instance_metrics_unknown", value: 1, labels: map[string]string{"instance_uid": "s", "worker": "worker-0", "metric": gRunning, "reason": "not_reported"}}),
		"the info series twice": append(staleFixture("s"), staleFixture("s")[0]),
	} {
		if p, _ := pendingGaugeProblems(ss, v); len(p) == 0 {
			t.Errorf("%s: pendingGaugeProblems found nothing", name)
		}
	}
}

func TestRestartReadProblems(t *testing.T) {
	neither := []sample{}
	for _, s := range gaugeFixture() {
		if s.name != gWaiting {
			neither = append(neither, s)
		}
	}
	reads := [][]sample{gaugeFixture(), staleFixture("u"), gaugeFixture()}
	if p, stale, fresh, unserved := restartReadProblems(append(reads, nil), "u"); len(p) != 0 || stale != 1 || fresh != 2 || unserved != 1 {
		t.Fatalf("fresh, stale, fresh, none: %v, %d stale, %d fresh, %d unserved", p, stale, fresh, unserved)
	}
	if p, _, _, _ := restartReadProblems([][]sample{gaugeFixture(), neither}, "u"); len(p) == 0 {
		t.Errorf("a read with a gauge neither known nor unknown: restartReadProblems found nothing")
	}
	both := append(gaugeFixture(), sample{name: "hpcburst_instance_metrics_unknown", value: 1, labels: map[string]string{"instance_uid": "u", "worker": "worker-0", "metric": gKV, "reason": "stale"}})
	if p, _, _, _ := restartReadProblems([][]sample{both}, "u"); len(p) == 0 {
		t.Errorf("a read with a gauge both known and unknown: restartReadProblems found nothing")
	}
	reported := append(gaugeFixture(), sample{name: gPrefixHit, value: 0, labels: map[string]string{"instance_uid": "u", "worker": "worker-0"}})
	if p, _, _, _ := restartReadProblems([][]sample{reported}, "u"); len(p) == 0 {
		t.Errorf("a read with an unexported gauge at 0: restartReadProblems found nothing")
	}
	if p, _, _, _ := restartReadProblems([][]sample{gaugeFixture()[1:]}, "u"); len(p) == 0 {
		t.Errorf("a read with gauges and no info series: restartReadProblems found nothing")
	}
	// A stale read whose unexported gauges say not_reported is correct too.
	if p, stale, fresh, _ := restartReadProblems([][]sample{gaugeFixture(), withReason(staleFixture("u"), "not_reported", notReported)}, "u"); len(p) != 0 || stale != 0 || fresh != 1 {
		t.Errorf("fresh, then stale with not_reported on the unexported gauges: %v, %d stale, %d fresh", p, stale, fresh)
	}
	// Review of PR #247: the reads of the probe, a fresh read and then the
	// stale read with another reason.
	exported := map[string]bool{}
	for _, g := range engineGauges {
		exported[g] = !notReported[g]
	}
	all := map[string]bool{}
	for _, g := range engineGauges {
		all[g] = true
	}
	for name, read := range map[string][]sample{
		"not_reported on the exported gauges": withReason(staleFixture("u"), "not_reported", exported),
		"agent_unreachable on every gauge":    withReason(staleFixture("u"), "agent_unreachable", all),
		"no reason on one gauge":              withReason(staleFixture("u"), "", map[string]bool{gRunning: true}),
	} {
		if p, _, _, _ := restartReadProblems([][]sample{gaugeFixture(), read}, "u"); len(p) == 0 {
			t.Errorf("a fresh read, then %s: restartReadProblems found nothing", name)
		}
	}
}

// withReason sets the reason of the unknown samples of the named gauges.
func withReason(ss []sample, reason string, gauges map[string]bool) []sample {
	for i := range ss {
		if ss[i].name == "hpcburst_instance_metrics_unknown" && gauges[ss[i].labels["metric"]] {
			ss[i].labels["reason"] = reason
		}
	}
	return ss
}
