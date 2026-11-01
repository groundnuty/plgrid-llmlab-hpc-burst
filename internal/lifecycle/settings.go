package lifecycle

import (
	"fmt"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The profile's defaults, as the CRD schema writes them (HBEP-4, Platform
// profile schema). The API server applies them to a stored object; the
// controller applies them again, so an object built without the API server, as
// in a unit test, means the same. A test compares each with the committed CRD.
const (
	defaultProbeInterval       = 10 * time.Second
	defaultFailureThreshold    = 3
	defaultNoProgressTimeout   = 30 * time.Minute
	defaultErrorOnlyWindow     = time.Minute
	defaultAgentTimeout        = 2 * time.Minute
	defaultRegistrationTimeout = 15 * time.Minute
	defaultStartTimeout        = time.Hour
	defaultStopGrace           = 10 * time.Second
	defaultAttemptsPerRung     = 2
	defaultRestartWindow       = 30 * time.Minute
	defaultRestartBackoff      = 30 * time.Second
	defaultHandoverMargin      = 30 * time.Minute
)

// Framework defaults that are not profile values (HBEP-7).
const (
	// defaultStartToReady is a class's start-to-ready time until one is
	// measured (HBEP-7, Handover).
	defaultStartToReady = 30 * time.Minute
	// defaultContextShort and defaultOutputLongest are the workload's schema
	// defaults (HBEP-7, What the controller reads).
	defaultContextShort  = 512
	defaultOutputLongest = 4096
	// warmUpTolerance and warmUpFloor: warm-up is done when no fixed-length
	// shape in a round is slower than in the round before by more than
	// max(10% of that round's, 100 ms) (HBEP-7, Instance start; science-agent
	// ruling, 1 Nov 2026, issue #125).
	warmUpTolerance = 0.10
	warmUpFloor     = 100 * time.Millisecond
	// endingSoonLead: the ending-soon event fires when a ready Instance's
	// drain start is less than this away and no replacement is ready.
	endingSoonLead = time.Hour
	// maxRequeue is the longest a key waits before its next pass (HBEP-7,
	// Reconcile loop, step 11).
	maxRequeue = 30 * time.Second
	// keyEnv is the variable the engine reads its key from (HBEP-7, What it
	// runs).
	keyEnv = "VLLM_API_KEY"
)

// settings are a platform profile's values that the lifecycle controller
// applies, defaults filled.
type settings struct {
	handoverMargin time.Duration
	maxIdle        *time.Duration

	probeInterval       time.Duration
	failureThreshold    int32
	noProgressTimeout   time.Duration
	errorOnlyWindow     time.Duration
	agentTimeout        time.Duration
	registrationTimeout time.Duration
	startTimeout        time.Duration

	stopGrace       time.Duration
	attemptsPerRung int32
	restartWindow   time.Duration
	restartBackoff  time.Duration
}

// resolveSettings fills a profile's values with their defaults. A duration
// that does not parse, or an unsupported attemptsPerRung, is an error naming
// the field; the settings returned with it hold defaults (or the nearest
// supported value) in its place, for the Instances already on the platform.
func resolveSettings(p v1alpha1.PlatformProfileSpec) (settings, error) {
	var s settings
	var err error
	d := func(field string, v *v1alpha1.Duration, def time.Duration) time.Duration {
		if err != nil || v == nil {
			return def
		}
		got, perr := v.AsDuration()
		if perr != nil {
			err = fmt.Errorf("profile %s %q: %w", field, *v, perr)
			return def
		}
		return got
	}
	s.handoverMargin = d("handoverMargin", p.HandoverMargin, defaultHandoverMargin)
	if p.MaxIdle != nil {
		v := d("maxIdle", p.MaxIdle, 0)
		s.maxIdle = &v
	}
	h := p.Health
	if h == nil {
		h = &v1alpha1.HealthThresholds{}
	}
	s.probeInterval = d("health.probeInterval", h.ProbeInterval, defaultProbeInterval)
	s.noProgressTimeout = d("health.noProgressTimeout", h.NoProgressTimeout, defaultNoProgressTimeout)
	s.errorOnlyWindow = d("health.errorOnlyWindow", h.ErrorOnlyWindow, defaultErrorOnlyWindow)
	s.agentTimeout = d("health.agentTimeout", h.AgentTimeout, defaultAgentTimeout)
	s.registrationTimeout = d("health.registrationTimeout", h.RegistrationTimeout, defaultRegistrationTimeout)
	s.startTimeout = d("health.startTimeout", h.StartTimeout, defaultStartTimeout)
	s.failureThreshold = defaultFailureThreshold
	if h.FailureThreshold != nil {
		s.failureThreshold = *h.FailureThreshold
	}
	r := p.Restart
	if r == nil {
		r = &v1alpha1.RestartPolicy{}
	}
	s.stopGrace = d("restart.stopGrace", r.StopGrace, defaultStopGrace)
	s.restartWindow = d("restart.window", r.Window, defaultRestartWindow)
	s.restartBackoff = d("restart.backoff", r.Backoff, defaultRestartBackoff)
	s.attemptsPerRung = defaultAttemptsPerRung
	if r.AttemptsPerRung != nil {
		s.attemptsPerRung = *r.AttemptsPerRung
	}
	// status.restarts keeps at most 16 entries, so both rungs' attempts must
	// fit in it, or the ladder could never climb (review of PR 88). A profile
	// outside 1..8 is refused, and the ladder of an Instance already on it
	// runs with the nearest supported value.
	if s.attemptsPerRung < 1 || s.attemptsPerRung > maxAttemptsPerRung {
		if err == nil {
			err = fmt.Errorf("profile restart.attemptsPerRung %d is outside 1..%d: status.restarts keeps %d restarts, so both rungs must fit", s.attemptsPerRung, maxAttemptsPerRung, maxRestartsKept)
		}
		s.attemptsPerRung = min(max(s.attemptsPerRung, 1), maxAttemptsPerRung)
	}
	return s, err
}

// maxAttemptsPerRung is the most attempts per rung the bounded restart history
// can count for both rungs of the alpha ladder.
const maxAttemptsPerRung = maxRestartsKept / 2
