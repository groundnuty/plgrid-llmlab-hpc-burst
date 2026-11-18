package lifecycle

import (
	"fmt"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// An Instance behind a router (beta, opt-in; HBEP-7, Several workers; HBEP-5, Worker health; the
// science agent's rulings on #429). With one worker:
//   - the desired worker carries verifiedGeneration once the worker's current generation is verified
//     and warmed up, and the agent's readiness endpoint, which Envoy checks, answers 200 only then;
//   - the controller writes status.healthyWorkers from the report of the router's /clusters, taking it
//     only from a report whose router part echoes the desired router generation, so a stale report
//     cannot undo the router rung's 0;
//   - a worker counts only on a read of /clusters taken after its current process started and at most two
//     of the agent's router polls old at the report; past that window nobody counts, healthyWorkers is 0
//     and the front skips the Instance until a fresh read (identityHealthy; the science agent's ruling on
//     #764, 18 Nov 2026, and #784's window);
//   - a worker the router holds failing its active check for health.failureThreshold reports in a row,
//     while its current generation is verified, enters the worker rung (RouterCheckFailing); a
//     restarted worker is failing by design during its own warm-up, and an ejection alone is no signal;
//   - the router's exit, or its probe failing health.failureThreshold reports in a row while /clusters
//     holds a worker healthy, enters the router rung: healthyWorkers 0 first, then the restart recorded
//     under part router, then its generation raised; the phase stays as it is, ready, or starting or
//     verifying before the first ready, nothing is verified again, and after attemptsPerRung tries the
//     ladder climbs to the whole-Instance rung (the science agent's ruling on #455);
//   - the first entry to ready waits for the router's probe to pass.

// routed reports whether the Instance runs behind a router.
func routed(in *v1alpha1.Instance) bool { return in.Spec.Launch.Router != "" }

// currentRouter returns the report's router part when it echoes the desired router generation, or nil.
func currentRouter(in *v1alpha1.Instance, av agentView) *agentapi.RouterReport {
	if av.report == nil || av.report.Router == nil {
		return nil
	}
	rr := av.report.Router
	if rr.Generation != restartGeneration(in, v1alpha1.PartRouter) {
		return nil
	}
	return rr
}

// verifiedWorker reports whether the Instance's worker-0 is verified and warmed up at its current
// generation. With one worker the Instance's conditions are the worker's: a worker restart sets
// Verified and WarmedUp Unknown before it raises the generation (restart), so both True means the
// current generation passed.
func verifiedWorker(in *v1alpha1.Instance) bool {
	return isTrue(in.Status.Conditions, v1alpha1.ConditionVerified) && isTrue(in.Status.Conditions, v1alpha1.ConditionWarmedUp)
}

// verifiedGeneration is DesiredWorker.VerifiedGeneration for worker-0 of an Instance behind a router.
func verifiedGeneration(in *v1alpha1.Instance) *int64 {
	if !routed(in) || !verifiedWorker(in) {
		return nil
	}
	g := restartGeneration(in, v1alpha1.WorkerName(0))
	return &g
}

// readState says what a report tells about the router's view of its workers: whether there is a read, and
// whether it is current at the report.
type readState int

const (
	// readNone: the report says nothing current about the router's workers: no report, a report that does
	// not echo the desired router generation, a router that is not running, or no /clusters read yet.
	readNone readState = iota
	// readFresh: a read at most routerPollWindow old at the report's own time, or a router that has exited or stopped (no worker is healthy behind it, a current fact).
	readFresh
	// readStale: a read older than that, or one whose age cannot be judged (a probe interval or a report
	// time that is not known): no worker counts.
	readStale
)

// routerPollsStale is how many of the agent's router polls old a /clusters read may be at the report and
// still count (`@hpc-burst-science-agent`, 18 Nov 2026, #764, for #784, replacing two probe intervals).
const routerPollsStale = 2

// routerPollWindow is how old a /clusters read may be at the report and still count: routerPollsStale of
// the agent's router polls, a poll being the probe interval plus its two bounded reads, the admin's
// /clusters and the probe through the router, each at most agentapi.RouterReadTimeout (internal/agent
// watchRouterHealth: a timer of one probe interval, then both reads, then the next round). The read is
// stamped when its round begins, so one failed round whose reads time out leaves the last good read up
// to two polls old before the next good one posts; two bare probe intervals were shorter than that and
// wrote a 0 in the gap. The agent and the controller share the timeout's one number.
func routerPollWindow(probeInterval time.Duration) time.Duration {
	return routerPollsStale * (probeInterval + 2*agentapi.RouterReadTimeout)
}

// identityHealthy is the one join of the router's view to the workers' processes, for healthyWorkers
// (healthyCount) and the KV cache size (kvRouterWorkers) alike (`@hpc-burst-science-agent`, 18 Nov 2026,
// #764, the ruling on healthyWorkers; HBEP-7, Several workers). The router's report names a worker and no
// process, so its healthy flag could be the flag of a worker's predecessor. A worker counts only if all of
// these hold:
//   - the router holds it healthy and not ejected, in a report that echoes the desired router generation;
//   - the router's /clusters read postdates its current process's start (the report's StartTime, both on
//     the agent's clock), so a read taken after the start shows the replacement's own state: the agent
//     holds a restarted worker out of the router (its readiness answers 503) until its new generation is
//     verified. The agent cannot stamp a generation on an Envoy host, which has none, so the join is on
//     time. A worker with no report or no StartTime is a process the controller cannot place: not counted;
//   - the worker's own report says it is running: the agent keeps an exited worker's StartTime, so a
//     worker that died after the router's last read would otherwise still count under the router's flag;
//   - the read is at most routerPollWindow old at the report's own time (exactly that old is still fresh)
//     and not after it: a read stamped later than the report's own time, which a backward step of the
//     agent's clock makes of a predecessor's cached read, cannot be placed against any process and is
//     stale. A failed /clusters read is not flagged in the report: the agent keeps the last good Workers and
//     ClustersReadAt (internal/agent routerRead) and the age says how old it is, so one failed read inside
//     the window changes nothing.
//
// state is readNone when the report says nothing current (today's ok == false), readStale past the window,
// for a read dated after the report, or when the window cannot be judged (names are nil: none counts),
// readFresh otherwise.
func identityHealthy(in *v1alpha1.Instance, av agentView) (names []string, state readState) {
	rr := currentRouter(in, av)
	switch {
	case rr == nil:
		return nil, readNone
	case rr.Phase == agentapi.PartExited || rr.Phase == agentapi.PartStopped:
		return nil, readFresh
	case rr.Phase != agentapi.PartRunning || rr.ClustersReadAt == nil:
		return nil, readNone
	}
	if av.probeInterval <= 0 || av.reportTime.IsZero() || av.reportTime.Before(*rr.ClustersReadAt) ||
		av.reportTime.Sub(*rr.ClustersReadAt) > routerPollWindow(av.probeInterval) {
		return nil, readStale
	}
	for _, w := range rr.Workers {
		if !w.Healthy || w.Ejected {
			continue
		}
		if wr := reportOf(av.report, w.Name); wr != nil && wr.Phase == agentapi.PartRunning && wr.StartTime != nil && rr.ClustersReadAt.After(*wr.StartTime) {
			names = append(names, w.Name)
		}
	}
	return names, readFresh
}

// healthyCount is the Instance's healthy workers from a report that echoes the desired router generation,
// by identityHealthy: the workers the router holds healthy and not ejected on a fresh read that came after
// their current process started, and 0 once the router has exited. ok is false when the report says
// nothing current (no read yet): the field is then not written, and stays absent until the first read.
func healthyCount(in *v1alpha1.Instance, av agentView) (int32, bool) {
	names, state := identityHealthy(in, av)
	switch state {
	case readFresh:
		return int32(len(names)), true
	case readStale:
		// Past the window nobody counts: healthyWorkers 0, so the front skips the Instance (it is not
		// eligible, internal/front/inventory/build.go) and its balancer keeps its in-flight count
		// (HBEP-5, Load bound and cap), until a fresh read writes the real count (`@hpc-burst-science-agent`,
		// 18 Nov 2026, #764; team-lead, the same day).
		return 0, true
	}
	return 0, false
}

// writeWorkerCounts writes status.healthyWorkers and status.kvCacheTokens from a report, in ONE status
// update when either changes, and none when neither does. The front's cap is healthyWorkers times
// launch.maxNumSeqs and its token budget is kvCacheTokens times healthyWorkers, so two updates would
// let it read a mixed pair between them: 2 workers at 400000 before 2 workers at 100000 lands, when a
// 100000 worker rejoins a 400000 one (PR 780's review). Both fields count the workers of identityHealthy;
// healthyWorkers is written behind a router only (healthyCount), the KV cache size by kvcache.go's rules.
func (p *pass) writeWorkerCounts(in *v1alpha1.Instance, av agentView) error {
	var changes []func(*v1alpha1.InstanceStatus)
	if routed(in) {
		if n, ok := healthyCount(in, av); ok && (in.Status.HealthyWorkers == nil || *in.Status.HealthyWorkers != n) {
			changes = append(changes, func(s *v1alpha1.InstanceStatus) { s.HealthyWorkers = &n })
		}
	}
	if kv := kvTokens(in, av); !equalInt64Ptr(in.Status.KVCacheTokens, kv) {
		changes = append(changes, func(s *v1alpha1.InstanceStatus) { s.KVCacheTokens = kv })
	}
	if len(changes) == 0 {
		return nil
	}
	return p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
		for _, f := range changes {
			f(s)
		}
	})
}

// equalInt64Ptr reports whether two optional numbers are both absent or equal.
func equalInt64Ptr(a, b *int64) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// routerCheckSignal is the worker's own signal behind a router: the router holds it failing its active
// check for health.failureThreshold reports in a row while its current generation is verified.
func routerCheckSignal(in *v1alpha1.Instance, av agentView, st settings) (string, string) {
	rr := currentRouter(in, av)
	if !routed(in) || rr == nil || rr.Phase != agentapi.PartRunning || !verifiedWorker(in) {
		return "", ""
	}
	for _, w := range rr.Workers {
		if w.Name == v1alpha1.WorkerName(0) && w.FailedActiveCheck && w.ConsecutiveFailedChecks >= st.failureThreshold {
			return v1alpha1.ReasonRouterCheckFailing, fmt.Sprintf("the router held worker-0 failing its active check for %d reports in a row", w.ConsecutiveFailedChecks)
		}
	}
	return "", ""
}

// routerSignal is the router's own signal: its exit, or its probe failing health.failureThreshold
// reports in a row while it holds a worker healthy (HBEP-7, Several workers). A report that does not
// echo the desired router generation says nothing: a raised generation not yet applied raises nothing.
func routerSignal(in *v1alpha1.Instance, av agentView, st settings) (string, string) {
	rr := currentRouter(in, av)
	if rr == nil {
		return "", ""
	}
	if rr.Phase == agentapi.PartExited {
		code := "unknown"
		if rr.ExitCode != nil {
			code = strconv.Itoa(*rr.ExitCode)
		}
		return v1alpha1.ReasonPartExited, fmt.Sprintf("the router exited without being told to, with code %s", code)
	}
	if rr.Phase != agentapi.PartRunning || rr.Probe == nil || rr.Probe.OK || rr.Probe.ConsecutiveFailures < st.failureThreshold {
		return "", ""
	}
	for _, w := range rr.Workers {
		if w.Healthy {
			return v1alpha1.ReasonProbeFailing, fmt.Sprintf("the router's probe failed %d times in a row while it held %s healthy", rr.Probe.ConsecutiveFailures, w.Name)
		}
	}
	return "", ""
}

// routerProbePasses reports whether the agent's probe through the router passes, at the desired router
// generation: the first entry to ready waits for it.
func routerProbePasses(in *v1alpha1.Instance, av agentView) bool {
	rr := currentRouter(in, av)
	return rr != nil && rr.Phase == agentapi.PartRunning && rr.Probe != nil && rr.Probe.OK
}

// routerAdmitsReady reports whether a report at the desired router generation lets the Instance enter
// ready: the router holds worker-0 healthy, its first pass since its verification, from which a worker
// that rejoins counts again (HBEP-5, Inside an Instance, Worker health). A healthy worker's count of
// failed checks is 0, so the failures counted while it was unverified, during its own warm-up, are never
// judged against it once it is verified (HBEP-7, Several workers; Q5, as corrected; #455's review). A
// router that is not running admits nothing: the router rung, which runs before ready too, restarts it
// where the Instance is.
func routerAdmitsReady(in *v1alpha1.Instance, av agentView) bool {
	rr := currentRouter(in, av)
	if rr == nil || rr.Phase != agentapi.PartRunning {
		return false
	}
	// Every worker: worker-0 with one, each with several (HBEP-7, Several workers, Becoming ready).
	healthy := map[string]bool{}
	for _, w := range rr.Workers {
		healthy[w.Name] = w.Healthy
	}
	for _, w := range workerNames(in) {
		if !healthy[w] {
			return false
		}
	}
	return true
}

// routerRung applies the router's signal to an Instance behind a router, ready or not yet: the router
// rung. It returns true when it acted, so the worker's signals wait.
func (p *pass) routerRung(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView, st settings) (bool, error) {
	if !routed(in) || av.unreachable {
		return false, nil
	}
	signal, msg := routerSignal(in, av, st)
	if signal == "" {
		return false, nil
	}
	var tries int32
	var last metav1.Time
	for _, r := range p.recentRestarts(in, st.restartWindow) {
		if r.Part == v1alpha1.PartRouter {
			tries++
		}
		if r.Time.After(last.Time) {
			last = r.Time
		}
	}
	if !last.IsZero() {
		if next := last.Add(st.restartBackoff); p.now.Before(next) {
			p.at(next)
			return true, nil
		}
	}
	if tries >= st.attemptsPerRung {
		// The router rung is spent: the whole-Instance rung, which stops and starts every part.
		return true, p.restart(in, alloc, signal, v1alpha1.PartInstance, st,
			fmt.Sprintf("%s; the router rung is spent within %s", msg, st.restartWindow))
	}
	gen := restartGeneration(in, v1alpha1.PartRouter) + 1
	p.r.Recorder.Event(in, nil, eventFailing, v1alpha1.ReasonRouterRestart,
		fmt.Sprintf("signal %s (%s): the lifecycle controller restarts the router, generation %d; its workers keep running, and in-flight requests are cut", signal, msg, gen))
	now := metav1.NewTime(p.now)
	zero := int32(0)
	if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
		// Unroutable first: the front takes no worker as healthy until the restarted router holds one.
		s.HealthyWorkers = &zero
		s.Restarts = append(p.recentRestarts(in, st.restartWindow), v1alpha1.InstanceRestart{Part: v1alpha1.PartRouter, Generation: gen, Time: now, Signal: signal})
		if n := len(s.Restarts); n > maxRestartsKept {
			s.Restarts = s.Restarts[n-maxRestartsKept:]
		}
		cond(&s.Conditions, v1alpha1.ConditionHealthy, metav1.ConditionFalse, signal, msg, in.Generation, now)
	}); err != nil {
		return true, err
	}
	if err := p.setMeta(in, func(n *v1alpha1.Instance) {
		if n.Annotations == nil {
			n.Annotations = map[string]string{}
		}
		n.Annotations[v1alpha1.RestartGenerationAnnotation(v1alpha1.PartRouter)] = strconv.FormatInt(gen, 10)
	}); err != nil {
		return true, err
	}
	p.at(p.now.Add(st.restartBackoff))
	return true, nil
}
