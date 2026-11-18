package lifecycle

// status.healthyWorkers joins the router's view to each worker's current process and reads it at most two
// probe intervals old (identityHealthy, routerhealth.go; `@hpc-burst-science-agent`, 18 Nov 2026, #764,
// the ruling on healthyWorkers, with team-lead's answer for the read past the window). Each test feeds
// hand-built reports to writeWorkerCounts over kvcache_test.go's fake API and reads healthyWorkers back
// from it. Every expected count is a literal typed here; none is computed by the join under test. The
// probe interval is kvProbe, 10 s, so a read is fresh for 20 s.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// afterFailedRead is the report the agent sends dt after av's, when its /clusters read failed in between.
// A failed read is not flagged: the agent keeps the last good Workers and ClustersReadAt (internal/agent,
// routerRead logs the error and changes neither), and records only its probe, so the router part is the
// same read with a newer probe and the report's own time is dt later. The row that uses it checks that
// shape, so a "failed read" that is really a fresh one cannot pass for it.
func afterFailedRead(t *testing.T, av agentView, dt time.Duration) agentView {
	t.Helper()
	ir, rr := *av.report, *av.report.Router
	at := av.reportTime.Add(dt)
	rr.Probe = &agentapi.RouterProbe{Time: at, OK: true}
	ir.Router = &rr
	out := av
	out.report, out.reportTime, out.reportAt, out.now = &ir, at, at, at
	if !out.report.Router.ClustersReadAt.Equal(*av.report.Router.ClustersReadAt) || len(out.report.Router.Workers) != len(av.report.Router.Workers) || !at.After(av.reportTime) {
		t.Fatalf("afterFailedRead built a report that is not a failed read's: read %v then %v, report time %v then %v", av.report.Router.ClustersReadAt, out.report.Router.ClustersReadAt, av.reportTime, out.reportTime)
	}
	return out
}

// Two workers the router holds healthy. worker-1's process is replaced after the router's last read while
// the router still flags worker-1 healthy, which is the flag of its predecessor: healthyWorkers is 1, not
// 2, until a read taken after the replacement's start counts it again; then it is 2. (Mutant: the join by
// name alone counts 2 at the second step.)
func TestAReplacedWorkerDoesNotCountForHealthyWorkersUnderItsPredecessorsFlag(t *testing.T) {
	rg := newKVRig(t, 2)
	read := kvNow.Add(-10 * time.Second) // the router's last read, fresh
	replaced := kvWorker{name: "worker-1", gen: 1, healthy: true, started: kvNow.Add(-5 * time.Second)}
	rg.run([]kvStep{
		kvstep("both serving", kvView(0, true, kvw("worker-0", 0, nil, true), kvw("worker-1", 0, nil, true)), "unset", "2", 1),
		kvstep("worker-1 replaced after the router's read, still flagged healthy", kvViewRead(read, 0, true, kvw("worker-0", 0, nil, true), replaced), "unset", "1", 1),
		kvstep("the same report again writes nothing", kvViewRead(read, 0, true, kvw("worker-0", 0, nil, true), replaced), "unset", "1", 0),
		kvstep("the router reads again after the replacement started: it counts", kvView(0, true, kvw("worker-0", 0, nil, true), replaced), "unset", "2", 1),
	})
	// Both replaced after the read: no worker counts, and the count is a written 0, not an absent field.
	rg = newKVRig(t, 2)
	both := func(started time.Time) agentView {
		return kvViewRead(read, 0, true,
			kvWorker{name: "worker-0", gen: 1, healthy: true, started: started}, kvWorker{name: "worker-1", gen: 1, healthy: true, started: started})
	}
	rg.run([]kvStep{
		kvstep("both older than the read", both(kvNow.Add(-time.Hour)), "unset", "2", 1),
		kvstep("both replaced after it", both(kvNow.Add(-5*time.Second)), "unset", "0", 1),
	})
}

// A count of 2, then the agent's /clusters read fails: the report carries the last good read, now older.
// Inside the window (up to two probe intervals, 20 s, exactly 20 s still inside) healthyWorkers stays 2 and
// the pass writes nothing; a second past it, it is 0; a fresh read writes 2 again. (Mutant: a failed read
// inside the window clears the set, which writes 0 at the first later step.)
func TestOneFailedReadInsideTheWindowChangesNothing(t *testing.T) {
	rg := newKVRig(t, 2)
	good := kvView(0, true, kvw("worker-0", 0, nil, true), kvw("worker-1", 0, nil, true))
	rg.run([]kvStep{
		kvstep("a fresh read: 2", good, "unset", "2", 1),
		kvstep("one probe interval later, the read failed: still 2, no write", afterFailedRead(t, good, kvProbe), "unset", "2", 0),
		kvstep("exactly two probe intervals after the read: still inside", afterFailedRead(t, good, 2*kvProbe), "unset", "2", 0),
	})
	// A report each side of the window's end, from a fresh count, and then a fresh read.
	rg = newKVRig(t, 2)
	later := kvNow.Add(3 * kvProbe)
	again := kvViewRead(later, 0, true, kvw("worker-0", 0, nil, true), kvw("worker-1", 0, nil, true))
	again.reportTime, again.reportAt, again.now = later, later, later
	rg.run([]kvStep{
		kvstep("a fresh read: 2", good, "unset", "2", 1),
		kvstep("a second past two probe intervals: the Instance is skipped, 0", afterFailedRead(t, good, 2*kvProbe+time.Second), "unset", "0", 1),
		kvstep("a fresh read, both healthy: 2 again", again, "unset", "2", 1),
	})
}

// Past the window nobody counts: healthyWorkers 0, written in the same update as the KV cache size, so the
// front skips the Instance and its balancer keeps its in-flight count (HBEP-5, Load bound and cap), until
// a fresh read writes the real count. The stale report's own flags say ONE worker healthy while 2 is
// written, so the three answers are told apart: keeping the old count gives 2, counting the stale flags
// gives 1, and the right one is 0. (Mutants: keep-old-set writes nothing past the window, so 2; not-skipped
// counts the stale read's flags, so 1.)
func TestPastTheWindowNobodyCountsAndTheInstanceIsSkipped(t *testing.T) {
	rg := newKVRig(t, 2)
	fresh := kvView(0, true, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", 0, tok(380000), true))
	// The last good read is 21 s old at this report: worker-1 was unhealthy in it, which the controller
	// never saw (it judges the latest report of each pass).
	stale := kvViewRead(kvNow.Add(-2*kvProbe-time.Second), 0, true, kvw("worker-0", 0, tok(400000), true), kvw("worker-1", 0, tok(380000), false))
	rg.run([]kvStep{
		kvstep("a fresh read, both healthy", fresh, "380000", "2", 1),
		kvstep("the read is past two probe intervals, its flags say one healthy: 0 and no size, in ONE update", stale, "unset", "0", 1),
		kvstep("the same stale report writes nothing more", stale, "unset", "0", 0),
		kvstep("a later fresh read with both healthy: 2 again", fresh, "380000", "2", 1),
	})
	if want := []string{"2/380000", "0/unset", "2/380000"}; len(*rg.updates) != 3 || (*rg.updates)[0] != want[0] || (*rg.updates)[1] != want[1] || (*rg.updates)[2] != want[2] {
		t.Errorf("the status updates carried (healthyWorkers/kvCacheTokens) %v, want %v", *rg.updates, want)
	}
	// A probe interval that is not known judges nothing fresh, and a worker the report does not place
	// is not counted: both are 0, the safe side.
	rg = newKVRig(t, 2)
	noInterval := fresh
	noInterval.probeInterval = 0
	rg.run([]kvStep{kvstep("no probe interval", noInterval, "unset", "0", 1)})
}

// No read yet: nothing is written and the field stays absent, whether the report has no router part, a
// router that has not read /clusters, or a router that is not running yet; the first read writes the count.
// A router that has exited has nobody healthy: 0, written, as before.
func TestNoRouterReadYetLeavesHealthyWorkersAbsent(t *testing.T) {
	rg := newKVRig(t, 2)
	unread := kvView(0, true, kvw("worker-0", 0, nil, true), kvw("worker-1", 0, nil, true))
	unread.report.Router.ClustersReadAt = nil
	starting := kvView(0, true, kvw("worker-0", 0, nil, true), kvw("worker-1", 0, nil, true))
	starting.report.Router.Phase = agentapi.PartStarting
	exited := kvView(0, true, kvw("worker-0", 0, nil, true), kvw("worker-1", 0, nil, true))
	exited.report.Router.Phase = agentapi.PartExited
	rg.run([]kvStep{
		kvstep("no router part", kvView(0, false, kvw("worker-0", 0, nil, true), kvw("worker-1", 0, nil, true)), "unset", "-", 0),
		kvstep("a router that has read nothing yet", unread, "unset", "-", 0),
		kvstep("a router still starting", starting, "unset", "-", 0),
		kvstep("the first read", kvView(0, true, kvw("worker-0", 0, nil, true), kvw("worker-1", 0, nil, true)), "unset", "2", 1),
		kvstep("the router has exited: nobody is healthy, 0", exited, "unset", "0", 1),
	})
}
