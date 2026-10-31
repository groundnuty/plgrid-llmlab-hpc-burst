package providerctl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Defaults of the controller's timings.
const (
	// DefaultRejectBackoff is how long after a Rejected attempt the next one
	// waits (HBEP-7, open question 11: "the next attempt waits for backoff";
	// pkg/provider/fake.Controller's RejectBackoff).
	DefaultRejectBackoff = time.Minute
	// DefaultTimeout bounds one reconcile's calls to the platform: above the
	// sbatch time-out of 60 s plus the file staging and the settle.
	DefaultTimeout = 5 * time.Minute
	// DefaultMaxConcurrentReconciles is the number of Allocations reconciled
	// at once; the platform's own bound (four commands per connection) holds
	// below it.
	DefaultMaxConcurrentReconciles = 4
)

// FrontSettings are the launcher's front and agent-endpoint settings that go
// into each Allocation's agent.json (HBEP-4, Submission; docs/development.md,
// agent.json). They name a private address, so they are configuration, never
// in the repository.
type FrontSettings struct {
	// Mode is the dial-home mode.
	Mode agentapi.DialHomeMode
	// Endpoint is host:port: the front's dial-home listener in direct mode,
	// the control-plane host's ssh server (port 22) in ssh mode.
	Endpoint string
	// CertPin is the agent endpoint certificate's pin (agentapi.PinOf).
	CertPin string
	// SSHUser and AgentPort are the ssh mode's user on the control-plane host
	// and the agent endpoint's loopback port.
	SSHUser   string
	AgentPort int
}

// ProfileSource reads and validates a Provider's profile, settings and
// credentials. *slurm.ProfileReader implements it.
type ProfileSource interface {
	Read(ctx context.Context, provider string) (slurm.Validated, error)
}

// PlatformFactory makes the Platform for a validated Provider: in production a
// slurm.Batch over a new ssh transport. The returned Closer releases the
// transport.
type PlatformFactory func(v slurm.Validated) (Platform, io.Closer, error)

// Options configure one Provider's controller.
type Options struct {
	// Provider is the Provider's name. Only Allocations whose
	// providerConfigRef names it are reconciled.
	Provider string
	// Profiles reads the Provider's profile on every Connect and status sync.
	Profiles ProfileSource
	// NewPlatform makes the Platform; it is called again when the Provider's
	// ssh credentials change.
	NewPlatform PlatformFactory
	// Clock reads the time; nil means the wall clock.
	Clock Clock
	// CredentialsDir is the controller's credentials directory, which holds
	// each Allocation's credential (agentapi.AllocationCredentialPath).
	CredentialsDir string
	// AgentBuildsDir holds the agent builds, agentapi.AgentBuildName(cpuArch).
	AgentBuildsDir string
	// Front is the front and agent-endpoint settings; nil means no front is
	// configured, and nothing is submitted.
	Front *FrontSettings
	// PollInterval and BusyInterval are the reconcile intervals: 30 s, and
	// 5 s while an attempt is unresolved, a job is held, or the Allocation is
	// being deleted (HBEP-4, Transport, Poll rate).
	PollInterval, BusyInterval time.Duration
	// RejectBackoff is how long after a Rejected attempt the next waits.
	RejectBackoff time.Duration
	// Timeout bounds one reconcile's platform calls.
	Timeout time.Duration
	// CreationGracePeriod is crossplane-runtime's wait, after a Create that
	// succeeded, before it trusts an Observe that reports the job missing.
	// Zero keeps crossplane's default (30 s); a negative value means none.
	// The claim record already keeps a fresh submission from reading as
	// missing, so tests that move a manual clock turn it off.
	CreationGracePeriod time.Duration
	// MaxConcurrentReconciles bounds the workers.
	MaxConcurrentReconciles int
	// Identity is written into the submission records it claims.
	Identity string
	// Log and Recorder; nil means none.
	Log      logging.Logger
	Recorder event.Recorder
}

func (o *Options) defaults() error {
	if o.Provider == "" {
		return errors.New("providerctl: no Provider name")
	}
	if o.Profiles == nil || o.NewPlatform == nil {
		return errors.New("providerctl: Profiles and NewPlatform are required")
	}
	if o.Clock == nil {
		o.Clock = slurm.RealClock
	}
	if o.PollInterval <= 0 {
		o.PollInterval = slurm.DefaultPollInterval
	}
	if o.BusyInterval <= 0 {
		o.BusyInterval = slurm.DefaultBusyInterval
	}
	if o.RejectBackoff <= 0 {
		o.RejectBackoff = DefaultRejectBackoff
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.MaxConcurrentReconciles <= 0 {
		o.MaxConcurrentReconciles = DefaultMaxConcurrentReconciles
	}
	if o.Identity == "" {
		o.Identity = "provider-" + o.Provider
	}
	if o.Log == nil {
		o.Log = logging.NewNopLogger()
	}
	if o.Recorder == nil {
		o.Recorder = event.NewNopRecorder()
	}
	return nil
}

// holder keeps the Provider's current validated configuration and its
// Platform. The Platform is made again when the ssh credentials change;
// otherwise a changed profile is passed to it in place.
type holder struct {
	o *Options

	mu       sync.Mutex
	v        slurm.Validated
	platform Platform
	closer   io.Closer
	lastErr  error
}

// get reads the Provider's profile and returns it with the Platform. A read or
// validation failure is returned, and remembered for the Provider's status.
func (h *holder) get(ctx context.Context) (slurm.Validated, Platform, error) {
	v, err := h.o.Profiles.Read(ctx, h.o.Provider)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastErr = err
	if err != nil {
		return v, nil, err
	}
	if h.platform == nil || !reflect.DeepEqual(h.v.SSH, v.SSH) {
		p, c, err := h.o.NewPlatform(v)
		if err != nil {
			h.lastErr = fmt.Errorf("make the platform: %w", err)
			return v, nil, h.lastErr
		}
		if h.closer != nil {
			_ = h.closer.Close()
		}
		h.platform, h.closer = p, c
	} else if b, ok := h.platform.(*slurm.Batch); ok {
		b.Configure(v.Profile, v.Settings)
	}
	h.v = v
	return v, h.platform, nil
}

// current returns the last Platform without reading the profile; nil before
// the first successful get.
func (h *holder) current() (Platform, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.platform, h.lastErr
}

func (h *holder) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closer != nil {
		_ = h.closer.Close()
		h.closer, h.platform = nil, nil
	}
}
