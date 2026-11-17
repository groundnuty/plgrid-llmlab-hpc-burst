package front

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// What the probe reports beyond routing: HBEP-6's degraded event about a
// path, and HBEP-8's heartbeat of the probe itself, with its state in
// /healthz. A probe failure is not a prober failure (HBEP-8, Observers):
// PathProbeFailing says a path is bad, HeartbeatMissed that the probe stopped.

// recorderPathEvents is Run's PathEvents: HBEP-6's degraded event, reason
// PathProbeFailing, regarding the Instance, through the front's Recorder. The
// probe calls PathProbeFailing once per loss of a ready Instance whose
// TunnelConnected is True, so the event needs no gate of its own.
type recorderPathEvents struct{ rec Recorder }

// Probed implements PathEvents.
func (recorderPathEvents) Probed(ProbeResult) {}

// PathProbeFailing implements PathEvents.
func (e recorderPathEvents) PathProbeFailing(in *v1alpha1.Instance, r ProbeResult) {
	if e.rec == nil {
		return
	}
	last := probeFailureText(r)
	e.rec.Event(in, true, eventDegraded, reasonPathProbeFailing,
		fmt.Sprintf("the front's probe of Instance %s through its tunnel failed %d times in a row while its TunnelConnected is True; it is not eligible until a probe passes; the controller restarts nothing on this (last: %s)",
			r.Instance, r.Failures, last))
}

// The closed set a failed probe's cause falls in. The note of PathProbeFailing
// says one of these, never the text of the failed dial or reply, which can name
// an address, a path or a status line that frps sent: the event goes to a
// durable log that is kept and may be exported (HBEP-8, Allocation events, No
// job ID, path or name in a note). The text stays in the front's log.
const (
	probeCauseTimeout = "timeout"
	probeCauseRefused = "refused"
	probeCauseNoProxy = "noProxy"
	probeCauseOther   = "other"
)

// probeCauseOf classifies a failed probe's error by its kind, never by its
// text.
func probeCauseOf(err error) string {
	var ne net.Error
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errNoProxy):
		return probeCauseNoProxy
	case errors.Is(err, syscall.ECONNREFUSED):
		return probeCauseRefused
	case errors.Is(err, os.ErrDeadlineExceeded), errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return probeCauseTimeout
	}
	return probeCauseOther
}

// probeFailureText is the fixed description of a failed probe's last result for
// a note: its HTTP status when /health answered, else one text of the closed
// set of causes. A result with no cause is "other".
func probeFailureText(r ProbeResult) string {
	if r.Status != 0 {
		return fmt.Sprintf("its health check answered status %d", r.Status)
	}
	switch r.Cause {
	case probeCauseTimeout:
		return "no answer within the probe's deadline"
	case probeCauseRefused:
		return "the connection was refused"
	case probeCauseNoProxy:
		return "frps has no proxy for the Instance"
	}
	return "the probe failed for another reason, which the front's log names"
}

// probeStaleAfter is how many probeIntervals the probe's last result may age
// before the probe counts as stale (HBEP-8, Observers: older than three probe
// intervals).
const probeStaleAfter = 3

// heartbeatEventEvery is how often HeartbeatMissed repeats while the probe
// stays stale (HBEP-8, Repeating conditions).
const heartbeatEventEvery = time.Hour

// staleLocked reports whether the probe's last result is older than
// probeStaleAfter probeIntervals at now. The caller holds p.mu.
func (p *Prober) staleLocked(now time.Time) bool {
	return now.Sub(p.beat) > probeStaleAfter*p.interval
}

// raiseHeartbeatMissed raises HBEP-8's failing event, reason
// HeartbeatMissed, regarding the front's Lease, while the probe is stale:
// when the staleness starts, then at most once an hour while it lasts; a round
// that ends clears it, so the next staleness raises its event at once. It
// waits for the front to hold its Lease, like UsageWriteFailed, and does
// nothing with no Recorder.
//
// The Lease keeper calls it on every beat that holds the Lease, so it runs
// apart from the probe's loop, which a stalled probe would stop (HBEP-8,
// Observers: the component that judges a heartbeat does not depend on it).
// Staleness changes no registration: routing keeps HBEP-6's rule, which
// changes eligibility only on failed probes.
func (p *Prober) raiseHeartbeatMissed(rec Recorder, lease *leaseRef) {
	if rec == nil || lease == nil || !lease.held() {
		return
	}
	p.mu.Lock()
	now := p.clk.Now()
	last := p.beat
	emit := p.staleLocked(now) && (p.missedAt.IsZero() || !now.Before(p.missedAt.Add(heartbeatEventEvery)))
	if emit {
		p.missedAt = now
	}
	p.mu.Unlock()
	if !emit {
		return
	}
	rec.Event(lease.object(), true, eventFailing, reasonHeartbeatMissed,
		fmt.Sprintf("the front's end-to-end probe has given no result since %s, more than %d probe intervals (%s each): each Instance's probe state shows as unknown in /healthz, and routing keeps the probe's last verdicts",
			usage.Time(last), probeStaleAfter, p.interval))
}

// probeHealth is the /healthz body's probe member (HBEP-8, Observers: the
// front's end-to-end probe).
type probeHealth struct {
	// State is "ok", or "stale" while the probe's last result is older than
	// three probeIntervals.
	State string `json:"state"`
	// LastResult is when the probe last ended a round, in UTC.
	LastResult string `json:"lastResult"`
	// Instances holds each probed Instance by namespace/name.
	Instances map[string]instanceProbeHealth `json:"instances"`
}

// instanceProbeHealth is one Instance's probe state.
type instanceProbeHealth struct {
	UID string `json:"uid"`
	// State is "passing" or "failing" from its latest probe, or "unknown"
	// before its first result and while the probe is stale.
	State string `json:"state"`
	// Failures is the run of failed probes behind its latest verdict.
	Failures int `json:"failures"`
	// Registered is routing's view: whether its transport counts as
	// registered. Staleness leaves it as the last verdicts set it.
	Registered bool `json:"registered"`
	// LastResult is when its latest probe ended, in UTC; absent before one.
	LastResult string `json:"lastResult,omitempty"`
}

// Health returns the probe's state for /healthz, judged at the front's clock
// now.
func (p *Prober) Health() probeHealth {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.clk.Now()
	stale := p.staleLocked(now)
	h := probeHealth{State: "ok", LastResult: usage.Time(p.beat), Instances: map[string]instanceProbeHealth{}}
	if stale {
		h.State = "stale"
	}
	for uid, st := range p.state {
		ih := instanceProbeHealth{UID: string(uid), State: "unknown", Failures: st.fails, Registered: p.tun.Registered(uid)}
		if st.has {
			ih.LastResult = usage.Time(st.at)
			if !stale {
				ih.State = "failing"
				if st.passed {
					ih.State = "passing"
				}
			}
		}
		h.Instances[st.name] = ih
	}
	return h
}
