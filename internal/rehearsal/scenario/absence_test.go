package scenario_test

// PR #247's absence checks, proven on the lifecycle controller's
// raised-events counter (#256; HBEP-8, Metrics, Controller metrics: "Proving
// that an event was not raised"). A list of Events cannot prove an absence,
// since client-go's recorder delivers asynchronously; the counter rises in the
// call that hands an event to the recorder. Each check meets HBEP-8's four
// conditions or is inconclusive, which fails the test as a raised event does:
//
//  1. its first read is taken before the change that could raise the event
//     (absenceBase, taken at once after the controller that the check covers
//     starts to serve, before the steps that end Instances or reach a drain
//     start);
//  2. both reads come from one process while it serves
//     (process_start_time_seconds and both hpcburst_collector_success series,
//     which eventcount.Absence compares);
//  3. between the reads, a pass of the emitting loop for each subject
//     observed the change and finished emitting, and began after the event
//     was due: a tunnel-state read that listed the witness Instance
//     (orphans_test.go), a reconcile pass of each ModelDeployment whose
//     status.lastReconcileTime is at or after the drain start
//     (part3_events_test.go);
//  4. the series' other sources are quiet, which each check states.

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// tunnelOrphanGrace is the controller's TunnelOrphanGrace: a proxy listed
// online with no current Instance is an orphan once it has been so for this
// long (`@hpc-burst-science-agent`, 1 Nov 2026; cmd/hpcburst-controller,
// tunnel_state.go). A TunnelOrphaned event is due at the earliest this long
// after the Instance ended.
const tunnelOrphanGrace = 2 * time.Minute

// The lifecycle controller's series the absence checks read: HBEP-8's types
// and their Kubernetes types (Warning for degraded, Normal for replaced).
var (
	keyTunnelOrphaned = eventcount.Key{Controller: eventcount.ControllerLifecycle, Reason: v1alpha1.EventReasonTunnelOrphaned,
		Action: "degraded", Type: provider.EventKindWarning}
	keyReplacementLate = eventcount.Key{Controller: eventcount.ControllerLifecycle, Reason: v1alpha1.ReasonReplacementLate,
		Action: "degraded", Type: provider.EventKindWarning}
	keyHandedOver = eventcount.Key{Controller: eventcount.ControllerLifecycle, Reason: v1alpha1.EventReasonHandedOver,
		Action: "replaced", Type: provider.EventKindOrdinary}
)

// countReads reads the lifecycle controller's /metrics for the series keys,
// from one body, and waits until the body is a serving copy's (both success
// gauges and the process's start time) that has every series: a copy exports
// them only while it serves, and creates them at 0.
func (r *rig) countReads(what string, keys ...eventcount.Key) map[eventcount.Key]eventcount.Read {
	r.t.Helper()
	url := r.ports.url(12, "/metrics")
	var out map[eventcount.Key]eventcount.Read
	stack.WaitState(r.t, stack.WaitDeadline, "the controller's /metrics from a serving copy, for "+what, func() (bool, string) {
		sent := time.Now()
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(url)
		if err != nil {
			return false, err.Error()
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		resp.Body.Close()
		answered := time.Now()
		if err != nil || resp.StatusCode != http.StatusOK {
			return false, fmt.Sprintf("status %d, %v", resp.StatusCode, err)
		}
		reads := map[eventcount.Key]eventcount.Read{}
		for _, k := range keys {
			rd, err := eventcount.ParseRead(body, k)
			if err != nil {
				return false, err.Error()
			}
			rd.Sent, rd.Answered = sent, answered
			serving := rd.HasProcessStart
			for _, c := range eventcount.LifecycleCollectors {
				serving = serving && rd.Collectors[c]
			}
			if !serving || !rd.HasCount {
				return false, fmt.Sprintf("serving %v, %s present %v", serving, k, rd.HasCount)
			}
			reads[k] = rd
		}
		out = reads
		return true, ""
	})
	return out
}

// absenceBase is the first read of the absence checks that cover one serving
// controller, and what the test knew then.
type absenceBase struct {
	what  string
	reads map[eventcount.Key]eventcount.Read
	// change is just after the reads: the steps that could raise the events
	// begin after it.
	change time.Time
	// first is when the first read was answered.
	first time.Time
	// live holds the UIDs of the Instances not ended in a list taken just
	// after the first read. An end is final, so each had not ended at the
	// first read either: it was live then, or not yet created.
	live map[string]bool
	// startToReady holds each ModelDeployment's per-class start-to-ready
	// times, read just after the first read: the values the controller judged
	// ReplacementLate by then, before a later measurement overwrites its
	// class's entry (part3_absence_test.go).
	startToReady map[string]map[v1alpha1.AcceleratorClass]time.Duration
	// orphanable lists the proxies frps listed online for an Instance that
	// had ended or was gone at the first read: one a TunnelOrphaned event
	// could be raised for before the first read.
	orphanable []string
}

// baseline takes the first reads of the absence checks of a controller that
// serves now.
func (r *rig) baseline(what string, keys ...eventcount.Key) *absenceBase {
	r.t.Helper()
	b := &absenceBase{what: what, reads: r.countReads(what, keys...)}
	b.change = time.Now()
	for _, rd := range b.reads {
		b.first = rd.Answered // one body answers every key
	}
	b.live = map[string]bool{}
	for _, in := range r.instances() {
		if in.Status.Phase != v1alpha1.InstanceEnded {
			b.live[string(in.UID)] = true
		}
	}
	for uid, online := range r.frpsProxies() {
		if online && !b.live[uid] {
			b.orphanable = append(b.orphanable, uid)
		}
	}
	var mds v1alpha1.ModelDeploymentList
	ctx, cancel := stack.CallCtx(r.t)
	err := r.c.List(ctx, &mds)
	cancel()
	if err != nil {
		r.t.Fatalf("list ModelDeployments for the first read's start-to-ready times: %v", err)
	}
	b.startToReady = map[string]map[v1alpha1.AcceleratorClass]time.Duration{}
	for i := range mds.Items {
		if mds.Items[i].Namespace == namespace {
			b.startToReady[mds.Items[i].Name] = classStartToReady(&mds.Items[i])
		}
	}
	for k, rd := range b.reads {
		r.t.Logf("ABSENCE first read %s (%s): %g at %s", k, what, rd.Count, ts(rd.Answered))
	}
	return b
}

// judgeAbsence takes the second read and applies the four conditions. A
// raised event or an inconclusive check fails the test; the verdict is
// returned for the caller's report.
func (r *rig) judgeAbsence(b *absenceBase, k eventcount.Key, change time.Time, unmet []string, barriers []eventcount.Barrier, quiet string) eventcount.Verdict {
	r.t.Helper()
	second := r.countReads("the second read of "+k.String(), k)[k]
	a := eventcount.Absence{Key: k, First: b.reads[k], Second: second, Change: change, Collectors: eventcount.LifecycleCollectors,
		Barriers: barriers, Quiet: quiet}
	v, why := a.Judge()
	if len(unmet) > 0 && v != eventcount.Raised {
		v, why = eventcount.Inconclusive, fmt.Sprintf("%s: inconclusive: %s; %s", k, strings.Join(unmet, "; "), why)
	}
	r.t.Logf("ABSENCE %s (first read %s): %s", v, b.what, why)
	switch v {
	case eventcount.NotRaised:
	case eventcount.Raised:
		r.t.Errorf("FINDING: %s", why)
	default:
		r.t.Errorf("the absence check is inconclusive, never a pass: %s", why)
	}
	return v
}

// instanceEnded is when the watch first delivered the Instance ended: a
// bound its end lies before. ok is false when the watch saw no such version.
func (r *rig) instanceEnded(uid types.UID) (time.Time, bool) {
	o, ok := r.watch.first(uid, time.Time{}, func(o observation) bool { return o.phase == v1alpha1.InstanceEnded })
	return o.at, ok
}
