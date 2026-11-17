package lifecycle

import (
	"fmt"
	"slices"
	"strings"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// What the throughput gate does with a miss (HBEP-7, Over-allocation and qualification (beta), The throughput gate;
// #616, PR 2).
//
// A miss is measured twice. A worker's first miss is recorded in status.gate.misses and restarts that worker at
// rung 1, forced (forceWorker): it never climbs to the whole-Instance rung and never ends in RestartLoop, so the
// retry always measures. The retry decides: every worker passing gives the verdict Passed; a second miss of any
// worker records the verdict Missed and fails the Instance with QualificationFailed, whose message names both
// measurements, the limits and the entry's date, and whose Allocation is released at once; an entry suspended
// meanwhile gives the verdict Suspended and passes (suspension.go).
//
// The gate judges an Instance's first qualification and that retry only. Once the verdict is set, a later warm-up,
// after any restart, a candidate waiting at its ranking point included, only updates status.throughput. RecordOnly is
// final too, even when its cause later clears (an entry added afterwards, a suspension lifted, an aged entry
// replaced): re-judging would gate an Instance that has served for hours, a late release the owner's concern rules
// out, and HBEP-7 says a restart never releases an Allocation for throughput.

// signalThroughputMissed is the restart signal of the gate's first miss.
const signalThroughputMissed = "ThroughputMissed"

// gateWork is what the gate decided for a settled warm-up: at most one of the three.
type gateWork struct {
	// record is the verdict's change to the status, made in the write that records the warm-up: Passed, Suspended or
	// RecordOnly. nil when the Instance is not judged (its verdict is set, or it has been ready).
	record func(*v1alpha1.InstanceStatus)
	// restart is the first miss's restart of one worker.
	restart *gateRestart
	// fail is the second miss's failure.
	fail *gateFail
	// ref and counts are the entry and the verdict, Passed or Missed, that count toward the entry's suspension.
	// recordVerdict writes the count before the Instance's own write (suspension.go); nil for work that counts
	// nothing.
	ref    *entryRef
	counts string
}

// gateRestart is a first miss: the worker to restart, the message of its restart event, and the status change that
// records the miss, made in the same write as the restart's record.
type gateRestart struct {
	worker string
	msg    string
	record func(*v1alpha1.InstanceStatus)
}

// gateFail is a second miss: the failure message, and the status change that records the second measurement and the
// verdict Missed, made in the write that fails the Qualified condition, so an Instance recorded as Missed is always
// one that failed.
type gateFail struct {
	msg    string
	record func(*v1alpha1.InstanceStatus)
}

// gateJudge judges the settled warm-ups of an Instance's workers (one per worker, in worker order) and says what to
// do. It returns no work for an Instance whose verdict is set, and for one that has been ready: its first
// qualification is behind it. An error is a read that failed, never a verdict.
func (p *pass) gateJudge(in *v1alpha1.Instance, sh shapes, ws []gateWorker) (gateWork, error) {
	if g := in.Status.Gate; (g != nil && g.Verdict != "") || in.Status.ReadyTime != nil {
		return gateWork{}, nil
	}
	var profile *v1alpha1.PlatformProfile
	pv := p.pvOf(in.Labels)
	if pv != nil {
		profile = pv.profile
	}
	if profile == nil || len(profile.Spec.ExpectedPerformance) == 0 {
		// No profile holds a reference yet (none ships one): nothing is keyed, so nothing is read and nothing
		// can fail the warm-up's judgement; the gate records only, as in alpha.
		return gateFinal(in, recordOnly("", "no entry matches: the platform profile holds no expected performance"), metav1.NewTime(p.now)), nil
	}
	key, mismatch, err := p.ranKey(in, sh, ws)
	if err != nil {
		return gateWork{}, err
	}
	if mismatch != "" {
		return gateFinal(in, recordOnly("", "the warm-up did not run the shape its key names: "+mismatch), metav1.NewTime(p.now)), nil
	}
	names := make([]string, len(ws))
	tps := make([]*v1alpha1.Throughput, len(ws))
	for i, w := range ws {
		names[i], tps[i] = w.name, w.tp
	}
	v := judgeGate(profile, key, measuresOf(names, tps), p.now, func(entry v1alpha1.ReferenceKey, date string) bool {
		return p.referenceSuspended(entryRef{profile: profile.Name, key: entry, date: date})
	})
	if p.listFull() && v.status != v1alpha1.GateSuspended && v.status != v1alpha1.GateRecordOnly {
		// No further entry could be suspended, so nothing is gated: the caps err toward recording only.
		v = recordOnly(v.date, fmt.Sprintf("the deployment's list of suspended entries is full (%d), so the gate records only until a listing is lifted", maxSuspendedReferences))
	}
	var ref *entryRef
	if v.entry != nil {
		ref = &entryRef{profile: profile.Name, key: *v.entry, date: v.date}
	}
	if v.status != "" {
		w := gateFinal(in, v, metav1.NewTime(p.now))
		if v.status == v1alpha1.GatePassed {
			w.ref, w.counts = ref, v1alpha1.GatePassed
		}
		return w, nil
	}
	w := p.gateMiss(in, v, ws)
	if w.fail != nil {
		w.ref, w.counts = ref, v1alpha1.GateMissed
	}
	return w, nil
}

// gateFinal is the work of a final verdict: Passed, Suspended or RecordOnly.
func gateFinal(in *v1alpha1.Instance, v gateVerdict, now metav1.Time) gateWork {
	return gateWork{record: func(s *v1alpha1.InstanceStatus) {
		if c := apimeta.FindStatusCondition(s.Conditions, v1alpha1.ConditionQualified); c != nil && c.Status == metav1.ConditionTrue {
			cond(&s.Conditions, v1alpha1.ConditionQualified, metav1.ConditionTrue, v1alpha1.ReasonPassed, withGate(c.Message, v.text), in.Generation, now)
		}
		g := gateOf(s)
		g.Verdict, g.ReferenceDate = v.status, v.date
		s.Gate = &g
	}}
}

// gateOf is a copy of the status's gate record, or an empty one.
func gateOf(s *v1alpha1.InstanceStatus) v1alpha1.InstanceGate {
	if s.Gate == nil {
		return v1alpha1.InstanceGate{}
	}
	g := *s.Gate
	g.Misses = slices.Clone(s.Gate.Misses)
	return g
}

// gateMissOf is the worker's recorded miss, its first measurement.
func gateMissOf(in *v1alpha1.Instance, worker string) (v1alpha1.GateMiss, bool) {
	if in.Status.Gate == nil {
		return v1alpha1.GateMiss{}, false
	}
	for _, m := range in.Status.Gate.Misses {
		if m.Worker == worker {
			return m, true
		}
	}
	return v1alpha1.GateMiss{}, false
}

// gateMiss decides on a miss. A worker with a recorded miss that misses again is a second miss, and any second miss
// fails the Instance. Otherwise each first miss is recorded and restarted; two workers missing at once go one per
// pass, the lowest first, since the restart's spacing (restart.backoff) allows one a pass and a miss recorded
// without its restart would be judged, on the same measurement, as the retry.
func (p *pass) gateMiss(in *v1alpha1.Instance, v gateVerdict, ws []gateWorker) gateWork {
	var firsts, seconds []string
	for _, w := range v.missed {
		if _, retried := gateMissOf(in, w); retried {
			seconds = append(seconds, w)
		} else {
			firsts = append(firsts, w)
		}
	}
	if len(seconds) > 0 {
		return p.gateFailure(in, v, ws, seconds)
	}
	w := firsts[0]
	now := metav1.NewTime(p.now)
	tp := workerOf(ws, w).tp
	miss := v1alpha1.GateMiss{Worker: w, Throughput: *tp, Time: now}
	who := ""
	if len(ws) > 1 {
		who = w + " "
	}
	return gateWork{restart: &gateRestart{
		worker: w,
		msg:    fmt.Sprintf("the throughput gate's first miss of the entry dated %s: %s; the restart measures again", v.date, v.detail),
		record: func(s *v1alpha1.InstanceStatus) {
			g := gateOf(s)
			g.Misses, g.ReferenceDate = append(g.Misses, miss), v.date
			s.Gate = &g
			if c := apimeta.FindStatusCondition(s.Conditions, v1alpha1.ConditionQualified); c != nil && c.Status == metav1.ConditionTrue {
				text := fmt.Sprintf("%smissed the entry dated %s: %s; restarted once and measured again", who, v.date, v.detail)
				cond(&s.Conditions, v1alpha1.ConditionQualified, metav1.ConditionTrue, v1alpha1.ReasonPassed, withGate(c.Message, text), in.Generation, now)
			}
		},
	}}
}

// workerOf is the named worker's entry.
func workerOf(ws []gateWorker, name string) gateWorker {
	for _, w := range ws {
		if w.name == name {
			return w
		}
	}
	return gateWorker{name: name}
}

// gateFailure is the work of a second miss. Its message names both measurements, both limits and the entry's date.
func (p *pass) gateFailure(in *v1alpha1.Instance, v gateVerdict, ws []gateWorker, seconds []string) gateWork {
	var parts []string
	var second []v1alpha1.GateMiss
	for _, w := range seconds {
		first, _ := gateMissOf(in, w)
		tp := workerOf(ws, w).tp
		second = append(second, v1alpha1.GateMiss{Worker: w, Throughput: *tp, Time: metav1.NewTime(p.now)})
		label := "the Instance "
		if len(ws) > 1 {
			label = w + " "
		}
		parts = append(parts, fmt.Sprintf("%smissed twice: the first measurement was time per output token %s and time to first token %s, and the retry after a worker restart time per output token %s and time to first token %s",
			label, first.Throughput.TimePerOutputToken, first.Throughput.TimeToFirstToken, tp.TimePerOutputToken, tp.TimeToFirstToken))
	}
	msg := fmt.Sprintf("the throughput gate: %s; the limits of the entry dated %s are time per output token %s and time to first token %s",
		strings.Join(parts, "; "), v.date, v.limits.tpot, v.limits.ttft)
	return gateWork{fail: &gateFail{msg: msg, record: func(s *v1alpha1.InstanceStatus) {
		g := gateOf(s)
		g.Misses = append(g.Misses, second...)
		g.Verdict, g.ReferenceDate = v1alpha1.GateMissed, v.date
		s.Gate = &g
	}}}
}

// restartForGate is a first miss's restart: forced at the worker rung, recorded like any restart, with the miss
// recorded in the same write.
func (p *pass) restartForGate(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, r *gateRestart, st settings) error {
	return p.climbWith(in, alloc, signalThroughputMissed, forceWorker, r.worker, st, r.record, r.msg)
}

// failGate is a second miss's failure: QualificationFailed through failCheck, so its Allocation is released at once
// and its release cause written first (releasecause_test.go), with the second measurement and the verdict Missed
// recorded in the write that fails the condition. Later Instances of the pass see this one as a miss through its
// recorded verdict (suspension.go), until a pass records its outcome.
func (p *pass) failGate(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, f *gateFail) error {
	return p.failCheckWith(in, alloc, v1alpha1.ConditionQualified, f.msg, f.record)
}

// resumeGateFailure carries out a failure the gate decided and a pass did not finish: an Instance recorded as
// Missed whose end was interrupted would otherwise pass its checks again, warm up (nothing judges it now) and
// become ready. It reports whether it acted.
func (p *pass) resumeGateFailure(in *v1alpha1.Instance, alloc *v1alpha1.Allocation) (bool, error) {
	if g := in.Status.Gate; g == nil || g.Verdict != v1alpha1.GateMissed {
		return false, nil
	}
	msg := "the throughput gate recorded the verdict Missed"
	if c := apimeta.FindStatusCondition(in.Status.Conditions, v1alpha1.ConditionQualified); c != nil && c.Status == metav1.ConditionFalse {
		msg = c.Message
	}
	return true, p.fail(in, alloc, v1alpha1.ReasonQualificationFailed, msg)
}
