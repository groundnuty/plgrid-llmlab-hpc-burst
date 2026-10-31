// Package inventory turns the control plane's objects into the front's
// routing view (HBEP-5, Request path step 3; Reachability; Draining): which
// Instances are routable and eligible, when each starts draining, and which
// managed catalog entries are attached to a model name. Build is a pure
// function of a snapshot and the time, so the routing rules are tested
// without an API server; watch.go feeds it from the control plane.
package inventory

import (
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Snapshot is the front's copy of the objects it reads.
type Snapshot struct {
	Deployments []v1alpha1.ModelDeployment
	Instances   []v1alpha1.Instance
	Providers   []v1alpha1.Provider
	Profiles    []v1alpha1.PlatformProfile
}

// Inputs are what Build needs besides the snapshot.
type Inputs struct {
	Now time.Time
	// HasTransport reports whether an Instance UID has a registered transport
	// (HBEP-5, Reachability). Nil: none has.
	HasTransport func(uid types.UID) bool
	// Joined are the Instances that were routable at the previous build. An
	// Instance becomes routable only once ready with its transport, and then
	// stays routable through a skip (HBEP-5, Request path, Routable).
	Joined map[types.UID]bool
	// MaxRequestDuration is the front's own setting, for the drain-window
	// check and as the ceiling of a deployment's routing.maxRequestDuration.
	MaxRequestDuration time.Duration
}

// View is the routing view at one time.
type View struct {
	Models    map[string]*Model        // by model name (spec.model)
	Providers map[string]*ProviderInfo // every Provider, by name
	// Joined are the Instances routable in this view; pass them to the next
	// Build.
	Joined map[types.UID]bool
	// NextChange is the earliest drain start after Now (zero: none). The view
	// must be rebuilt then, although no object changed.
	NextChange time.Time
	// ShortProfiles are the PlatformProfiles with a batch Provider whose
	// drainWindow is below MaxRequestDuration plus their drainMargin.
	ShortProfiles []ShortProfile
	// Problems are objects the build could not use, one sentence each, for
	// the log. They carry names, never credentials.
	Problems []string
}

// Model is everything attached to one model name.
type Model struct {
	Name    string
	Served  []*Deployment
	Managed []*ManagedTarget
}

// Deployment is a served ModelDeployment and its routable Instances.
type Deployment struct {
	Key        string // namespace/name
	Namespace  string
	Name       string
	UID        types.UID
	ServedName string // spec.model: the name its workers serve
	Desired    int    // spec.instances
	// TightPercent and LoosePercent are the bounds as KubeAI percents (125
	// is c = 1.25), from spec.routing.loadBound.
	TightPercent, LoosePercent int
	// MaxRequestDuration is the deadline that applies to its requests: the
	// shorter of the front's and spec.routing.maxRequestDuration.
	MaxRequestDuration time.Duration
	Members            []*Instance // routable, sorted by Key
}

// Instance is one routable Instance.
type Instance struct {
	Key          string // namespace/name: its name in the balancer group
	Namespace    string
	Name         string
	UID          types.UID
	Deployment   string // the Deployment's Key
	Provider     string
	DeploymentID string // full form
	DisplayID    string
	Cap          int // routing.instanceCap, else launch.maxNumSeqs x workers
	LeaseEnd     time.Time
	DrainStart   time.Time
	// Eligible is the part of eligibility the control plane decides: phase
	// ready, AgentUnreachable not true, and a registered transport. The
	// router adds its pauses and the request's level.
	Eligible bool
}

// ManagedTarget is one attached managed catalog entry under one Provider.
type ManagedTarget struct {
	Key                string // namespace/name@provider
	Deployment         string // the managed-source ModelDeployment's namespace/name
	Namespace          string
	Name               string
	Provider           string
	CatalogName        string
	ManagedID          string // full form
	DisplayID          string
	CredentialsFile    string // the Provider's credentialsRef.file
	MaxRequestDuration time.Duration
}

// ProviderInfo is what the front reads of a Provider.
type ProviderInfo struct {
	Name     string
	Kind     v1alpha1.PlatformKind // from its PlatformProfile; "" when unknown
	Platform string                // its platform label
	Labels   map[string]string
	Profile  *v1alpha1.PlatformProfile
}

// ShortProfile is a profile that fails the drain-window check (HBEP-5,
// Draining, The check).
type ShortProfile struct {
	Name            string
	ResourceVersion string
	DrainWindow     time.Duration
	Required        time.Duration // MaxRequestDuration + drainMargin
}

// Workers is the number of workers of an alpha Instance: one, with no router
// (HBEP-7's alpha scope; HBEP-5, Inside an Instance). The cap is
// launch.maxNumSeqs times this.
const Workers = 1
