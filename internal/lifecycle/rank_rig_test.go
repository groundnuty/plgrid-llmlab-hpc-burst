package lifecycle

// HBEP-7's over-allocation Test plan rows that the selection and the ranking answer (Over-allocation and
// qualification (beta), How it counts and The ranking point; #616, PR 3). PR 4 creates the extra; here each extra X
// is seeded through the package's own create path (seedExtra, provisionWith with no predecessor and no hedge), as
// #611's tests seed a hedge's second member.
//
// The fixture is HBEP-7's: one batch Provider A (the rig's), a 12 h limit, drainWindow 2 h 10 min and handoverMargin
// 30 min (the rig's defaults), instances 1, overAllocation 1, targetSessions 8, typical context 1,024, typical output
// 128 tokens, max-num-seqs 16. The rig clock starts at 08:00:00, which is the rows' 00:00: "00:45" is 08:45 on the
// rig's clock (rkAt).
//
// The shared timeline, worked out from the rig's (handover_test.go's header): the Instance and its Allocation are
// created at the pass of 08:00:10 and submitted at the provider's pass of 08:00:20. A job with a queue wait of w
// starts at 08:00:20 + w. The agent starts the worker when it sees the job, and the worker runs after the engine's
// StartDelay d; verification, qualification and warm-up take 40 s after the worker runs, and at that pass the Instance
// is ready, or reaches its ranking point (the agent's warm-up is reported done in the same round, with a delay of 10 s
// or more; with none the worker takes a round more, and it is 50 s, as handover_test.go's header says). So a job that
// started at s reaches its ranking point at s + d + 40 s. P's wait is 10 min, so it starts at 08:10:20; with the delay
// rkPassDelay, 29 min, it reaches its point at 08:40:00. X's wait is 15 min, so it starts at 08:15:20, and with the
// same delay it reaches its point at 08:45:00: the rows' "P passes at 00:40" and "X passes at 00:45" (rkCalibration
// and each row check them).
//
// A measurement is the time per output token the fake engine reports for each of the warm-up's concurrent requests,
// as (total - first token) / (127 tokens), so 20 ms, 18.0 ms and 17.9 ms are exact (rkWarmUp).

import (
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// rkPassDelay is the engine's StartDelay that makes a job reach its ranking point 29 min 40 s after it started (see
// the shared timeline above).
const rkPassDelay = 29 * time.Minute

// rkAt is a time of the HBEP rows' clock, whose 00:00 is the rig's 08:00:00.
func rkAt(h, m, s int) time.Time { return hoT(8+h, m, s) }

// rkRig is the rig with per-Allocation engines: the time per output token each warm-up measures, the engine's
// StartDelay, and an optional health double. noWait says the Provider has observed no queue wait ("no wait observed
// on A"); otherwise its status keeps 20 waits of 1 h each, as the fixture says.
type rkRig struct {
	*rig
	tpot   map[string]time.Duration
	delay  map[string]time.Duration
	health map[string]func(time.Time, afake.Process) agentapi.Health
	memo   map[string]string
	// defaultDelay is the StartDelay of an engine whose Instance has none of its own set (engineOf): rkPassDelay.
	defaultDelay time.Duration
	noWait       bool
	// allowErr: a pass that fails is kept in passErr instead of failing the test, for a row that injects a fault.
	allowErr bool
	passErr  error
}

func newRkRig(t *testing.T, noWait bool, opts ...rigOption) *rkRig {
	rg := newRig(t, append([]rigOption{withMaxTime(12 * time.Hour)}, opts...)...)
	h := &rkRig{rig: rg, tpot: map[string]time.Duration{}, delay: map[string]time.Duration{}, health: map[string]func(time.Time, afake.Process) agentapi.Health{}, memo: map[string]string{}, defaultDelay: rkPassDelay, noWait: noWait}
	rg.engine = func(alloc string) afake.Engine {
		d, ok := h.delay[alloc]
		if !ok {
			d = h.defaultDelay
		}
		tp, ok := h.tpot[alloc]
		if !ok {
			tp = 20 * time.Millisecond
		}
		return afake.Engine{StartDelay: d, WarmUpRound: rkWarmUp(tp), Health: h.health[alloc]}
	}
	return h
}

// rkWarmUp is a warm-up whose concurrent requests each take 100 ms to the first token and 100 ms + 127 × tpot in all
// (the output is 128 tokens), the same in every round, so it settles in round 2 and measures exactly tpot.
func rkWarmUp(tpot time.Duration) func(int32, agentapi.WarmUpRequest) agentapi.WarmUpRound {
	return func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
		round := afake.DefaultWarmUpRound(n, req)
		for i := range round.Requests {
			if q := &round.Requests[i]; q.Shape == agentapi.ShapeConcurrent {
				q.Total = agentapi.Duration(time.Duration(q.TimeToFirstToken) + time.Duration(q.OutputTokens-1)*tpot)
			}
		}
		return round
	}
}

// rkMD is HBEP-7's deployment, edited.
func rkMD(edit func(*v1alpha1.ModelDeployment)) *v1alpha1.ModelDeployment {
	md := mdSpec(hoMD)
	md.Spec.OverAllocation = ptrTo(int32(1))
	md.Spec.Workload = &v1alpha1.Workload{
		ContextTokens: v1alpha1.ContextTokens{Typical: 1024}, OutputTokens: v1alpha1.OutputTokens{Typical: 128}, TargetSessions: 8,
	}
	md.Spec.Source.Served.Flags = append(md.Spec.Source.Served.Flags, v1alpha1.EngineFlag{Name: "max-num-seqs", Value: "16"})
	if edit != nil {
		edit(md)
	}
	return md
}

// step is one round of the rig: the provider, the Provider's queue waits as the row says, the agents, the controller.
func (h *rkRig) step() {
	h.t.Helper()
	if err := h.prov.Sync(h.ctx); err != nil {
		h.t.Fatalf("provider sync: %v", err)
	}
	if h.noWait {
		hoSetWaits(h.rig)
	} else {
		hoSetWaits(h.rig, slicesOf(20, time.Hour)...)
	}
	h.syncAgents()
	_, err := h.r.Reconcile(h.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: hoMD}})
	if h.passErr = err; err != nil && !h.allowErr {
		h.t.Fatalf("reconcile at %s: %v", h.clock.Now().Format(time.RFC3339), err)
	}
}

func slicesOf(n int, d time.Duration) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = d
	}
	return out
}

// tick moves the clock by d and runs one round.
func (h *rkRig) tick(d time.Duration) {
	h.t.Helper()
	h.clock.Advance(d)
	h.step()
}

// run moves the clock by total in ticks of tick, running a round after each (rig.run, with this rig's step).
func (h *rkRig) run(total, tick time.Duration) {
	h.t.Helper()
	for end := h.clock.Now().Add(total); h.clock.Now().Before(end); {
		h.tick(tick)
	}
}

// until ticks until cond holds, failing after limit (rig.until, with this rig's step).
func (h *rkRig) until(limit, tick time.Duration, what string, cond func() bool) {
	h.t.Helper()
	for end := h.clock.Now().Add(limit); !cond(); {
		if !h.clock.Now().Before(end) {
			h.t.Fatalf("%s: not reached within %s (now %s)", what, limit, h.clock.Now().Format(time.TimeOnly))
		}
		h.tick(tick)
	}
}

// stepTo moves the clock to target in ticks of at most each, running a round after every one, and calls fn after it.
func (h *rkRig) stepTo(target time.Time, each time.Duration, fn func()) {
	h.t.Helper()
	for h.clock.Now().Before(target) {
		h.tick(min(each, target.Sub(h.clock.Now())))
		if fn != nil {
			fn()
		}
	}
}

// rkXWait is the queue wait of an extra seeded at 08:00:20 that should start at 08:15:20: X is created one pass
// after P, so the provider submits its job at 08:00:30, and the 15 min of the rows' "X starts at 00:15" are 14 min
// 50 s of wait. Seeding it after P's submission also fixes which job takes which scripted wait, which two Allocations
// created in one pass do not (the fake provider submits in the order of their random names).
const rkXWait = 14*time.Minute + 50*time.Second

// start creates the deployment, steps to 08:00:10, which makes its missing primary P, and to 08:00:20, which submits
// P's job, and then seeds the extra X. scripts are the queue waits the jobs get in the order they are submitted:
// P's, X's, then any replacement's.
func (h *rkRig) start(edit func(*v1alpha1.ModelDeployment), scripts ...pfake.Script) (p, x string) {
	h.t.Helper()
	h.batch.Enqueue(scripts...)
	h.create(rkMD(edit))
	h.tick(10 * time.Second)
	h.tick(10 * time.Second)
	ins := h.instances(hoMD)
	if len(ins) != 1 || !h.clock.Now().Equal(rkAt(0, 0, 20)) {
		h.t.Fatalf("at %s, %d Instances, want the missing primary's one at 08:00:20", h.clock.Now().Format(time.TimeOnly), len(ins))
	}
	return ins[0].Name, h.seedExtra()
}

// seedExtra makes the extra through the create path PR 4's pass uses, and returns its Instance's name.
func (h *rkRig) seedExtra() string {
	h.t.Helper()
	have := map[string]bool{}
	for _, in := range h.instances(hoMD) {
		have[in.Name] = true
	}
	p := h.passOf(hoMD)
	if ok, err := p.provisionWith(nil, placeOpts{only: rigProvider}); err != nil || !ok {
		h.t.Fatalf("seeding the extra: placed %v, %v", ok, err)
	}
	for _, in := range h.instances(hoMD) {
		if !have[in.Name] {
			return in.Name
		}
	}
	h.t.Fatal("the seeded extra is not among the Instances")
	return ""
}

// alloc returns the name of the Instance's Allocation, remembered from the first time it is asked, so that it is
// still known when an ended Instance's tombstone has been deleted with its Allocation.
func (h *rkRig) alloc(instance string) string {
	if a, ok := h.memo[instance]; ok {
		return a
	}
	a := h.mustInstance(instance).Spec.AllocationRef.Name
	h.memo[instance] = a
	return a
}

// phase is the Instance's phase, or "gone" once its tombstone is deleted.
func (h *rkRig) phase(instance string) v1alpha1.InstancePhase {
	var in v1alpha1.Instance
	switch err := h.c.Get(h.ctx, client.ObjectKey{Namespace: rigNS, Name: instance}, &in); {
	case apierrors.IsNotFound(err):
		return "gone"
	case err != nil:
		h.t.Fatal(err)
	}
	return in.Status.Phase
}

// engine sets what the Instance's Allocation's engine does, before its job starts: the time per output token its
// warm-up measures and the StartDelay (0 keeps rkPassDelay).
func (h *rkRig) engineOf(instance string, tpot, startDelay time.Duration) {
	a := h.alloc(instance)
	h.tpot[a] = tpot
	if startDelay != 0 {
		h.delay[a] = startDelay
	}
}

// readyAt is when the Instance became ready, or the zero time.
func (h *rkRig) readyAt(instance string) time.Time {
	if r := h.mustInstance(instance).Status.ReadyTime; r != nil {
		return r.Time
	}
	return time.Time{}
}

// jobStart is when the Instance's job started, or the zero time.
func (h *rkRig) jobStart(instance string) time.Time {
	if a, ok := h.allocation(h.alloc(instance)); ok && a.Status.AtProvider.StartTime != nil {
		return a.Status.AtProvider.StartTime.Time
	}
	return time.Time{}
}

// rankedOutEvents returns the RankedOut events that regard the Instance.
func (h *rkRig) rankedOutEvents(instance string) []RecordedEvent {
	var out []RecordedEvent
	for _, e := range h.rec.Find(eventReplaced, v1alpha1.ReasonRankedOut) {
		if e.Name == instance {
			out = append(out, e)
		}
	}
	return out
}

// kept reports whether the Instance's Allocation still exists and is not being deleted.
func (h *rkRig) kept(instance string) bool { return !h.released(h.alloc(instance)) }

// idleSince is the Allocation's idle-since anchor.
func (h *rkRig) idleSince(instance string) (time.Time, bool) {
	a, ok := h.allocation(h.alloc(instance))
	if !ok {
		return time.Time{}, false
	}
	since, found, err := v1alpha1.IdleSince(a)
	if err != nil {
		h.t.Fatal(err)
	}
	return since, found
}

// uidOf is the Instance's UID.
func (h *rkRig) uidOf(instance string) types.UID { return h.mustInstance(instance).UID }
