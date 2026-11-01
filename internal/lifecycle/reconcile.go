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
	invOnce sync.Once
	inv     *inventory
	// slotsOnce and slotTab: the per-Provider submit-slot locks and
	// expectations every worker shares (slots.go).
	slotsOnce sync.Once
	slotTab   *slotTable
}

func (r *Reconciler) memory() *memory {
	r.memOnce.Do(func() { r.mem = newMemory(r.Clock.Now().UTC().Truncate(time.Second)) })
	return r.mem
}

// inventory is what the metrics collector reads: the Instances the passes saw
// that have not ended (HBEP-8, Metrics).
func (r *Reconciler) inventory() *inventory {
	r.invOnce.Do(func() { r.inv = &inventory{} })
	return r.inv
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
	// stale marks Instances an earlier ModelDeployment of this name owned.
	stale map[types.UID]bool
	// deferred: a placement waits for a warm Allocation's agent to register
	// again after the controller's restart; it is not a missing Provider.
	deferred bool

	degraded []degradedReason
	// sizingRefused collects the Providers that refused the spec's size, by
	// name, with the refusal.
	sizingRefused map[string]string
	// noSlot collects the Providers that had no free submit slot, by name,
	// naming each one's count and limit (SubmitLimitReached).
	noSlot map[string]string
	// overlaps memoizes the no-overlap decision per primary (nooverlap.go).
	overlaps map[types.UID]overlapCheck
	// overlapErrs collects the failed no-overlap checks of the replacement
	// rules (overlapOrDefer); run returns them at the end of the pass.
	overlapErrs []error
	next        time.Time
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
	if err := r.sweepSecrets(ctx); err != nil {
		return ctrl.Result{}, err
	}
	if p.md == nil && len(p.insts) == 0 && len(p.allocs) == 0 {
		return ctrl.Result{}, nil // a deleted deployment with nothing left: no more passes
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
	} else if err := p.loadReferencedProviders(); err != nil {
		return err
	}
	p.recordInventory()
	// An Instance's failure does not hold back the rest of the pass; it is
	// returned at the end, so the key is retried with backoff.
	instErr := p.instances()
	if err := p.clearDialHome(); err != nil {
		return errors.Join(instErr, err)
	}
	if md != nil && md.Spec.Source.Served != nil && !p.shutdown {
		if err := p.capacity(); err != nil {
			return errors.Join(instErr, err)
		}
		if err := p.replacements(); err != nil {
			return errors.Join(instErr, err, errors.Join(p.overlapErrs...))
		}
		// A failed no-overlap check waits for the next pass without holding
		// back retention and the status write.
		instErr = errors.Join(instErr, errors.Join(p.overlapErrs...))
	}
	if err := p.retention(); err != nil {
		return errors.Join(instErr, err)
	}
	if md == nil {
		return instErr
	}
	if md.Spec.Source.Managed != nil {
		return errors.Join(instErr, p.writeManagedStatus())
	}
	return errors.Join(instErr, p.writeStatus())
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
	p.stale = map[types.UID]bool{}
	for i := range il.Items {
		in := &il.Items[i]
		owned, stale := ownedBy(in, p.key.Name, p.md)
		if owned {
			p.insts = append(p.insts, in)
		}
		if stale {
			// An earlier ModelDeployment of the same name owned it: it is
			// drained and ended for clean-up only, and never counts for this
			// one (capacity, status, start-to-ready, failure streak).
			p.insts = append(p.insts, in)
			p.stale[in.UID] = true
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

// recordInventory records the deployment's Instances that have not ended for
// the metrics collector, each with its identity, and forgets the others. What
// the pass learns of an Instance's agent is added in agent(), and an Instance
// that ends in this pass is dropped in end().
func (p *pass) recordInventory() {
	inv := p.r.inventory()
	live := map[types.UID]bool{}
	for _, in := range p.insts {
		if ended(in) {
			continue
		}
		live[in.UID] = true
		e := instanceEntry{uid: in.UID, namespace: in.Namespace, name: in.Name, md: p.key.Name, provider: in.Labels[v1alpha1.LabelProvider]}
		if pv := p.pvOf(in.Labels); pv != nil && pv.hasClass {
			e.gpuMdl = pv.class.GPUModel
		}
		inv.setIdentity(p.key, e)
	}
	inv.keep(p.key, live)
}

// ownedBy reports whether the Instance belongs to the ModelDeployment: its
// controller owner reference names it, and its UID while it exists. stale is
// true for one a deleted ModelDeployment of the same name owned, which a
// recreated one must still drain and end (review of PR 88).
func ownedBy(in *v1alpha1.Instance, name string, md *v1alpha1.ModelDeployment) (owned, stale bool) {
	for _, o := range in.OwnerReferences {
		if o.Controller != nil && *o.Controller && o.Kind == "ModelDeployment" && o.Name == name {
			if md == nil || o.UID == md.UID {
				return true, false
			}
			return false, true
		}
	}
	return false, false
}

// referencedProviders returns the Providers the deployment's Instances and
// Allocations name.
func (p *pass) referencedProviders() []string {
	var out []string
	for _, in := range p.insts {
		out = append(out, in.Labels[v1alpha1.LabelProvider])
	}
	for _, a := range p.allocs {
		out = append(out, a.Labels[v1alpha1.LabelProvider])
	}
	return out
}

// loadReferencedProviders reads, for a deleted or managed-source deployment,
// the Providers and profiles its remaining objects name: their drain windows
// and maxIdle still apply (review of PR 88). None of them takes anything new.
func (p *pass) loadReferencedProviders() error {
	extra := p.referencedProviders()
	p.pvByName = map[string]*providerView{}
	if len(extra) == 0 {
		return nil
	}
	pvs, err := p.r.loadProviders(p.ctx, &v1alpha1.ModelDeployment{}, extra)
	if err != nil {
		return err
	}
	for _, pv := range pvs {
		p.pvByName[pv.name] = pv
	}
	return nil
}

// prepareServed reads the Providers, checks the spec and computes the current
// IDs (HBEP-7, Reconcile loop, steps 1 and 2).
func (p *pass) prepareServed() error {
	pvs, err := p.r.loadProviders(p.ctx, p.md, p.referencedProviders())
	if err != nil {
		return err
	}
	p.pvByName = map[string]*providerView{}
	for _, pv := range pvs {
		p.pvByName[pv.name] = pv
		if pv.managed || pv.extra {
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
	if !p.shutdown {
		p.checkTimeLimits()
	}
	if !p.r.Config.FrontConfigured {
		p.degrade(v1alpha1.ReasonFrontNotConfigured, "no front address and dial-home mode are configured, so no Allocation is submitted (HBEP-11, Single-user host); record one with hpcburst up --front-mode and --front-addr")
	}
	p.classList = classes(p.providers)
	p.ids = map[v1alpha1.AcceleratorClass]*classID{}
	if p.specErr != "" || len(p.classList) == 0 {
		return nil
	}
	ids, err := p.r.currentIDs(p.ctx, p.md, p.classList)
	if err != nil {
		return err
	}
	for _, cid := range ids {
		c := cid
		p.ids[c.class] = &c
		if c.problem != "" {
			if !c.retryAt.IsZero() {
				p.at(c.retryAt)
			}
			if c.transient {
				continue // a transient Hub failure sets no reason; it is retried (HBEP-3)
			}
			p.degrade(v1alpha1.ReasonIdentityUnresolved, "class %s/%s has no current deployment ID: %s", c.class.GPUModel, c.class.CPUArch, c.problem)
			continue
		}
		blocked, err := p.r.classBlocked(p.ctx, p.md, c.id, resolveShapes(p.md))
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
