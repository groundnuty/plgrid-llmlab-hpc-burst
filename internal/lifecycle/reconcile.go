package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Config is what the controller binary tells the lifecycle controller.
type Config struct {
	// CredentialsDir is the controller's credentials directory, mode 700
	// (docs/development.md, Runtime conventions).
	CredentialsDir string
	// FrontConfigured says whether a front address and dial-home mode are
	// configured. While it is false no Allocation is created, and every served
	// ModelDeployment is Degraded with reason FrontNotConfigured (HBEP-11,
	// Single-user host).
	FrontConfigured bool
	// AgentDigest returns the SHA-256, 64 lowercase hex, of the allocation
	// agent's build for a CPU architecture (HBEP-7, Provisioning).
	AgentDigest func(cpuArch string) (string, error)
}

// Reconciler is the lifecycle controller's reconcile loop, keyed per
// ModelDeployment (HBEP-7, Reconcile loop).
type Reconciler struct {
	// Client writes, and reads Providers and PlatformProfiles.
	Client client.Client
	// Reader reads the ModelDeployment, its Instances and Allocations and the
	// records: an uncached reader in production (doc.go).
	Reader      client.Reader
	Clock       Clock
	Backend     *Backend
	Checkpoints CheckpointSource
	Recorder    Recorder
	Config      Config

	memOnce sync.Once
	mem     *memory
}

func (r *Reconciler) memory() *memory {
	r.memOnce.Do(func() { r.mem = newMemory(r.Clock.Now().UTC().Truncate(time.Second)) })
	return r.mem
}

// degradedReason is one reason the ModelDeployment is degraded now.
type degradedReason struct{ reason, message string }

// pass is one reconcile of one ModelDeployment.
type pass struct {
	r   *Reconciler
	ctx context.Context
	now time.Time
	key types.NamespacedName
	mem *memory

	// md is nil when the ModelDeployment is gone: its Instances and
	// Allocations are then shut down (HBEP-7, Release: deletion).
	md       *v1alpha1.ModelDeployment
	shutdown bool

	providers []*providerView
	pvByName  map[string]*providerView
	plan      servedPlan
	specErr   string
	ids       map[v1alpha1.AcceleratorClass]*classID
	classList []v1alpha1.AcceleratorClass

	allocs map[string]*v1alpha1.Allocation
	insts  []*v1alpha1.Instance

	degraded []degradedReason
	// sizingRefused collects Providers that refused the spec's size.
	sizingRefused []string
	s2r           []v1alpha1.ClassStartToReady
	next          time.Time
}

// at asks for the next pass no later than t.
func (p *pass) at(t time.Time) {
	if t.After(p.now) && (p.next.IsZero() || t.Before(p.next)) {
		p.next = t
	}
}

func (p *pass) degrade(reason, format string, a ...any) {
	p.degraded = append(p.degraded, degradedReason{reason: reason, message: fmt.Sprintf(format, a...)})
}

// Reconcile runs one pass (HBEP-7, Reconcile loop). Every step re-reads what it
// acts on and is idempotent.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	p := &pass{r: r, ctx: ctx, now: r.Clock.Now().UTC().Truncate(time.Second), key: req.NamespacedName, mem: r.memory()}
	var md v1alpha1.ModelDeployment
	err := r.Reader.Get(ctx, req.NamespacedName, &md)
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return ctrl.Result{}, fmt.Errorf("get ModelDeployment %s: %w", req.NamespacedName, err)
	default:
		p.md = &md
	}
	if err := p.observe(); err != nil {
		return ctrl.Result{}, err
	}
	if err := p.run(); err != nil {
		return ctrl.Result{}, err
	}
	wait := maxRequeue
	if !p.next.IsZero() {
		wait = min(maxRequeue, max(time.Second, p.next.Sub(p.now)))
	}
	return ctrl.Result{RequeueAfter: wait}, nil
}

func (p *pass) run() error {
	md := p.md
	switch {
	case md == nil:
		p.shutdown = true
	case md.Spec.Source.Managed != nil:
		p.shutdown = true
	case !md.DeletionTimestamp.IsZero(), md.Spec.Instances == 0:
		p.shutdown = true
	case md.Spec.EndTime != nil && !p.now.Before(md.Spec.EndTime.Time):
		p.shutdown = true
	}
	if md != nil && md.Spec.EndTime != nil {
		p.at(md.Spec.EndTime.Time)
	}
	if md != nil && md.Spec.Source.Served != nil {
		if err := p.prepareServed(); err != nil {
			return err
		}
	}
	if err := p.instances(); err != nil {
		return err
	}
	if md != nil && md.Spec.Source.Served != nil && !p.shutdown {
		if err := p.capacity(); err != nil {
			return err
		}
		if err := p.replacements(); err != nil {
			return err
		}
	}
	if err := p.retention(); err != nil {
		return err
	}
	if md == nil {
		return nil
	}
	if md.Spec.Source.Managed != nil {
		return p.writeManagedStatus()
	}
	return p.writeStatus()
}

// observe lists the ModelDeployment's Allocations (by its model-deployment
// label, then spec.modelDeploymentRef) and Instances (by owner, or by label
// once the ModelDeployment is gone) (HBEP-7, Reconcile loop, step 3).
func (p *pass) observe() error {
	sel := client.MatchingLabels{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(p.key.Name)}
	var al v1alpha1.AllocationList
	if err := p.r.Reader.List(p.ctx, &al, client.InNamespace(p.key.Namespace), sel); err != nil {
		return fmt.Errorf("list Allocations: %w", err)
	}
	p.allocs = map[string]*v1alpha1.Allocation{}
	for i := range al.Items {
		a := &al.Items[i]
		if a.Spec.ModelDeploymentRef != nil && a.Spec.ModelDeploymentRef.Name == p.key.Name {
			p.allocs[a.Name] = a
		}
	}
	var il v1alpha1.InstanceList
	if err := p.r.Reader.List(p.ctx, &il, client.InNamespace(p.key.Namespace), sel); err != nil {
		return fmt.Errorf("list Instances: %w", err)
	}
	for i := range il.Items {
		in := &il.Items[i]
		if ownedBy(in, p.key.Name, p.md) {
			p.insts = append(p.insts, in)
		}
	}
	sort.Slice(p.insts, func(i, j int) bool {
		a, b := p.insts[i], p.insts[j]
		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			return a.CreationTimestamp.Before(&b.CreationTimestamp)
		}
		return a.Name < b.Name
	})
	return nil
}

// ownedBy reports whether the Instance belongs to the ModelDeployment: its
// controller owner reference names it (and its UID, while it exists).
func ownedBy(in *v1alpha1.Instance, name string, md *v1alpha1.ModelDeployment) bool {
	for _, o := range in.OwnerReferences {
		if o.Controller != nil && *o.Controller && o.Kind == "ModelDeployment" && o.Name == name {
			return md == nil || o.UID == md.UID
		}
	}
	return false
}

// prepareServed reads the Providers, checks the spec and computes the current
// IDs (HBEP-7, Reconcile loop, steps 1 and 2).
func (p *pass) prepareServed() error {
	pvs, err := p.r.loadProviders(p.ctx, p.md)
	if err != nil {
		return err
	}
	p.pvByName = map[string]*providerView{}
	for _, pv := range pvs {
		p.pvByName[pv.name] = pv
		if pv.managed {
			continue // a served deployment ignores managed Providers (HBEP-7, step 1)
		}
		p.providers = append(p.providers, pv)
		if pv.problem != "" {
			p.degrade(v1alpha1.ReasonProviderUnavailable, "%s", pv.problem)
		}
	}
	plan, err := checkServed(p.md)
	var u *unsupported
	switch {
	case errors.As(err, &u):
		p.specErr = u.msg
		p.degrade(v1alpha1.ReasonSpecUnsupported, "%s", u.msg)
	case err != nil:
		return err
	default:
		p.plan = plan
	}
	if !p.r.Config.FrontConfigured {
		p.degrade(v1alpha1.ReasonFrontNotConfigured, "no front address and dial-home mode are configured, so no Allocation is submitted (HBEP-11, Single-user host); record one with hpcburst up --front-mode and --front-addr")
	}
	p.classList = classes(p.providers)
	p.ids = map[v1alpha1.AcceleratorClass]*classID{}
	if p.specErr != "" || len(p.classList) == 0 {
		return nil
	}
	for _, cid := range p.r.currentIDs(p.ctx, p.md, p.classList) {
		c := cid
		p.ids[c.class] = &c
		if c.problem != "" {
			if !c.retryAt.IsZero() {
				p.at(c.retryAt)
			}
			p.degrade(reasonIdentityUnresolved, "class %s/%s has no current deployment ID: %s", c.class.GPUModel, c.class.CPUArch, c.problem)
			continue
		}
		blocked, err := p.r.classBlocked(p.ctx, p.md, c.id)
		if err != nil {
			return err
		}
		if blocked != "" {
			p.ids[c.class].blocked = blocked
			p.degrade(v1alpha1.ReasonVerificationFailed, "provisioning in class %s/%s is stopped until the serving spec changes: %s", c.class.GPUModel, c.class.CPUArch, blocked)
		}
	}
	return nil
}

// pvOf returns the view of an object's Provider, by its provider label; nil
// when the Provider is not (or no longer) allowed.
func (p *pass) pvOf(labels map[string]string) *providerView {
	if p.pvByName == nil {
		return nil
	}
	return p.pvByName[labels[v1alpha1.LabelProvider]]
}

// liveOn returns the Instances that are not ended on an Allocation.
func (p *pass) liveOn(alloc string) []*v1alpha1.Instance {
	var out []*v1alpha1.Instance
	for _, in := range p.insts {
		if in.Spec.AllocationRef.Name == alloc && !ended(in) {
			out = append(out, in)
		}
	}
	return out
}

func ended(in *v1alpha1.Instance) bool { return in.Status.Phase == v1alpha1.InstanceEnded }

// agentGrace is how long after its own start the controller waits before it
// judges a registration missing: every agent registers again within one poll
// and one report interval (agentapi's doc, registration is soft state).
const agentGrace = agentapi.LongPollTimeout + agentapi.ReportInterval
