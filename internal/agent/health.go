package agent

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// recordHealth turns one probe and one metrics read of a running worker into
// the worker's health sample (HBEP-7, Reports: for health). It is the one
// place that decides what a failed metrics read reports, so the contract's
// coming change (a metricsReadAt time and gauges that can be absent) changes
// only this function.
//
// Until then: the probe's result is always fresh, and a failed metrics read
// carries the last successful read's values.
func (w *worker) recordHealth(now time.Time, perr error, h agentapi.Health, serr error) {
	if serr == nil {
		w.lastHealth = h
	} else {
		h = w.lastHealth
		h.ResponsesByStatus = cloneCounts(w.lastHealth.ResponsesByStatus)
	}
	if perr == nil {
		w.failures = 0
	} else {
		w.failures++
	}
	h.Time = now
	h.ProbeOK = perr == nil
	h.ConsecutiveProbeFailures = w.failures
	w.health = &h
}
