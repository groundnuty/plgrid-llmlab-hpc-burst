// Package inventory turns the control plane's objects into the front's
// routing view (HBEP-5, Request path step 3; Reachability; Draining): which
// Instances are routable and eligible, when each starts draining, and which
// managed catalog entries are attached to a model name. Build is a pure
// function of a snapshot and the time, so the routing rules are tested
// without an API server; watch.go feeds it from the control plane.
package inventory

import (
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Snapshot is the front's copy of the objects it reads.
type Snapshot struct {
	Deployments []v1alpha1.ModelDeployment
	Instances   []v1alpha1.Instance
	Workloads   []v1alpha1.Workload
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
	// Workloads are the Workload objects by their model (spec.model) and then
	// their name, kept apart from Models: a Workload of a model no
	// ModelDeployment names still resolves, so a request that names it is
	// refused for what it is (HBEP-5, Workloads, Naming it), not as a model that
	// is not attached.
	Workloads map[string]map[string][]*Workload
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
	// ManagedSources are the managed-source ModelDeployments that attach the
	// model, with a target or not: a Workload's serving set counts them, so a
	// serving managed deployment whose catalog entry is not listed now gives
	// no_ready_target, not not_attached (HBEP-5, Workloads, Who serves it).
	ManagedSources []*ManagedSource
}

// Workload is one Workload object (HBEP-5, Workloads). Only what the front
// routes on is read: its identity and its model.
type Workload struct {
	Key       string // namespace/name
	Namespace string
	Name      string
	Model     string // spec.model
	// Charge is c, what one request of the Workload is charged against an Instance's token budget (HBEP-5, Workloads,
	// The count in tokens): shape.contextTokens.typical plus shape.outputTokens.typical, as the Workload had it when
	// this view was built.
	Charge int64
	// ManagedFirst is spec.routeOrder ManagedFirst, false for OwnFirst, the default and the value of a Workload that sets none; ManagedSessions is
	// spec.managedSessions, 0 when it is unset. Both are as the Workload had them when this view was built (HBEP-5, Workloads, Route order).
	ManagedFirst    bool
	ManagedSessions int
}

// ManagedSource is a managed-source ModelDeployment that attaches a model.
type ManagedSource struct {
	Key       string // namespace/name
	Namespace string
	// Workloads is its spec.workloads: the Workloads it serves; empty serves
	// every Workload of its model in its namespace.
	Workloads []string
}

// serves reports whether a ModelDeployment in namespace ns with spec.workloads
// list serves the Workload: it is in the Workload's own namespace, and lists the
// Workload or lists none (HBEP-5, Workloads, Who serves it; HBEP-7).
func serves(ns string, list []string, w *Workload) bool {
	return ns == w.Namespace && (len(list) == 0 || slices.Contains(list, w.Name))
}

// Serves reports whether the served ModelDeployment serves the Workload.
func (d *Deployment) Serves(w *Workload) bool { return serves(d.Namespace, d.Workloads, w) }

// Serves reports whether the managed-source ModelDeployment serves the Workload.
func (s *ManagedSource) Serves(w *Workload) bool { return serves(s.Namespace, s.Workloads, w) }

// Serves reports whether the target's ModelDeployment serves the Workload.
func (t *ManagedTarget) Serves(w *Workload) bool { return serves(t.Namespace, t.Workloads, w) }

// Deployment is a served ModelDeployment and its routable Instances.
type Deployment struct {
	Key        string // namespace/name
	Namespace  string
	Name       string
	UID        types.UID
	ServedName string // spec.model: the name its workers serve
	Desired    int    // spec.instances
	// TightPercent and LoosePercent are the bounds as KubeAI percents (125
	// is c = 1.25), from spec.routing.loadBound. A factor with more than two
	// decimals is rounded down to a whole percent (Percent), so the bound in
	// force is never above the configured one.
	TightPercent, LoosePercent int
	// MaxRequestDuration is the deadline that applies to its requests: the
	// shorter of the front's and spec.routing.maxRequestDuration.
	MaxRequestDuration time.Duration
	// Workloads is spec.workloads: the Workloads it serves; empty serves every
	// Workload of its model in its namespace (Serves).
	Workloads []string
	Members   []*Instance // routable, sorted by Key
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
	// KVCacheTokens is status.kvCacheTokens, the engine's KV cache size per worker; nil when the Instance has none (an
	// engine run with disable-log-stats, or before the agent reports it). TokenBudget reads it.
	KVCacheTokens *int64
	// CapWorkers is the workers Cap counts: the healthy workers behind a router, else Workers.
	CapWorkers int
	LeaseEnd   time.Time
	DrainStart time.Time
	// Eligible is the part of eligibility the control plane decides: phase
	// ready, AgentUnreachable not true, a registered transport, and a Provider
	// that the deployment's spec.providers selects now. A member whose Provider
	// was deselected stays a member, not eligible, so its open requests keep
	// their count. The router adds its pauses and the request's level.
	Eligible bool
}

// TokenBudget is the Instance's token budget B (HBEP-5, Workloads, The count in tokens) as its two factors: perWorker, its
// status.kvCacheTokens, and workers, the workers its cap counts. B is their product, which the front never forms: it can be larger
// than any int64 (two workers of 2^62 tokens each), and a budget that is known stays known, so the balancer compares the factors
// exactly (balancer.Budget). ok is false when B is unknown, and the Instance then meets the count rule alone: it has no
// kvCacheTokens (or one below 1, the field's minimum), or no worker its cap counts.
func (i *Instance) TokenBudget() (perWorker int64, workers int, ok bool) {
	if i.KVCacheTokens == nil || *i.KVCacheTokens < 1 || i.CapWorkers < 1 {
		return 0, 0, false
	}
	return *i.KVCacheTokens, i.CapWorkers, true
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
	// Workloads is its managed-source ModelDeployment's spec.workloads (Serves).
	Workloads []string
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
