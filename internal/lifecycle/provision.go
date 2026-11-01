package lifecycle

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Provisioning (HBEP-7, Provisioning). The Instance is the durable record of
// the intent: it is created first, naming the Allocation it will run in, then
// the Allocation under that name, then the Allocation's credential file. A
// controller stopped between two steps finishes them on its next pass: a
// pending Instance whose Allocation does not exist gets it (ensureAllocation),
// and an Allocation without its credential file gets one (ensureCredentials),
// so nothing is submitted twice and no Allocation is orphaned. On a Provider
// with a submit limit, a slot is reserved under the Allocation's name before
// the Instance is created (slots.go).

// canProvision reports whether new Allocations may be created at all.
func (p *pass) canProvision() bool {
	return p.md != nil && p.specErr == "" && p.r.Config.FrontConfigured && !p.shutdown
}

// provision places one Instance: a new primary (pred nil) or the replacement of
// pred. It tries pred's Provider first, then the allowed Providers in order,
// taking the first usable one whose class has a current ID and is neither
// stopped nor paused. On a Provider with a warm Allocation of this deployment
// that has lease enough, it places the Instance there (HBEP-7, open question
// 14). It returns false when no Provider could take it.
func (p *pass) provision(pred *v1alpha1.Instance) (bool, error) {
	return p.provisionWith(pred, placeOpts{})
}

// placeOpts narrow where provisionWith places an Instance (HBEP-7, Handover,
// No-overlap mode).
type placeOpts struct {
	// skip names a Provider not to try: the predecessor's, when its per-user
	// caps keep the replacement from running beside the predecessor while
	// another Provider has room.
	skip string
	// only, when set, names the one Provider to try: the predecessor's, on
	// the queued path, where only a per-user cap blocks the overlap.
	only string
	// successorOf, when set, is written on the new Instance as
	// annotationNoOverlapSuccessor: it is a no-overlap successor.
	successorOf string
	// gapOf, when set with successorOf, names the primary that drained in the
	// mode and is written as annotationNoOverlapGap: the gap has begun.
	gapOf string
}

func (p *pass) provisionWith(pred *v1alpha1.Instance, o placeOpts) (bool, error) {
	if !p.canProvision() {
		return false, nil
	}
	order := p.providers
	if pred != nil {
		if pv := p.pvOf(pred.Labels); pv != nil {
			order = append([]*providerView{pv}, order...)
		}
	}
	for _, pv := range order {
		if !pv.usable() || pv.tooShort != "" || pv.name == o.skip || (o.only != "" && pv.name != o.only) {
			continue
		}
		cid := p.ids[pv.class]
		if cid == nil || cid.id == "" || cid.blocked != "" {
			continue
		}
		if paused, until := p.loopPaused(pv.class, pv.settings); paused {
			p.degrade(v1alpha1.ReasonRestartLoop, "provisioning in class %s/%s pauses until %s after repeated failures", pv.class.GPUModel, pv.class.CPUArch, until.UTC().Format(time.RFC3339))
			p.at(until)
			continue
		}
		params, err := partitionSizing(pv, p.plan.gpus, p.md.Spec.LeaseLimit)
		if err != nil {
			var u *unsupported
			if asUnsupported(err, &u) {
				if p.sizingRefused == nil {
					p.sizingRefused = map[string]string{}
				}
				p.sizingRefused[pv.name] = u.msg
			} else {
				p.degrade(v1alpha1.ReasonProviderUnavailable, "Provider %s: %v", pv.name, err)
			}
			continue
		}
		warm, deferUntil := p.warmAllocation(pv, params)
		if warm == nil && !deferUntil.IsZero() {
			p.at(deferUntil)
			p.deferred = true
			return false, nil
		}
		if warm != nil {
			if err := p.createInstance(pv, cid, pred, warm.Name, o.successorOf, o.gapOf); err != nil {
				return false, err
			}
			// Placed: the Allocation is no longer idle (v1alpha1.ClearIdleSince).
			return true, p.writeIdleSince(warm, nil)
		}
		name, err := randomName(pv.name)
		if err != nil {
			return false, err
		}
		// The slot is reserved before the Instance, the durable intent, so
		// no two workers both pass the count (slots.go).
		ok, err := p.reserveSlot(pv, name)
		if err != nil {
			return false, err
		}
		if !ok {
			continue
		}
		if err := p.createInstance(pv, cid, pred, name, o.successorOf, o.gapOf); err != nil {
			p.releaseSlot(pv, name)
			return false, err
		}
		return true, p.createAllocation(pv, name, params)
	}
	return false, nil
}

// warmAllocation returns a Running Allocation of this deployment on the
// Provider that hosts no live Instance and has lease left above twice the
// class's start-to-ready time plus the drain window (HBEP-7, open question 14).
//
// The Allocation must fit the launch: the same partition (so the same class)
// and GPU count, and at least the CPUs and memory a new Allocation would
// request (review of PR 88: a spec edit can change the GPUs an Instance needs).
// One whose agent has not registered again yet, during the grace after the
// controller's own restart, is waited for rather than passed over: deferUntil
// says until when.
func (p *pass) warmAllocation(pv *providerView, params v1alpha1.BatchParameters) (warm *v1alpha1.Allocation, deferUntil time.Time) {
	need := 2*startToReady(p.md, pv.class) + drainWindowOf(pv, p.md)
	for _, a := range p.allocs {
		obs := a.Status.AtProvider
		if a.Labels[v1alpha1.LabelProvider] != pv.name || !a.DeletionTimestamp.IsZero() ||
			obs.Phase != v1alpha1.AllocationRunning || obs.HardEnd == nil || len(p.liveOn(a.Name)) > 0 ||
			obs.HardEnd.Sub(p.now) <= need || !fits(a, params) || p.releasePending(a) {
			continue
		}
		if len(p.r.Backend.registrations(a.UID)) > 0 {
			return a, time.Time{}
		}
		if grace := p.mem.started.Add(agentGrace); p.now.Before(grace) {
			deferUntil = grace
		}
	}
	return nil, deferUntil
}

// releasePending reports whether an Instance on the Allocation failed, so the
// Allocation is to be released (a release that failed or was interrupted is
// made on a later pass): it is never warm capacity.
func (p *pass) releasePending(a *v1alpha1.Allocation) bool {
	for _, in := range p.insts {
		if in.Spec.AllocationRef.Name == a.Name && releaseRequired(in) {
			return true
		}
	}
	return false
}

// fits reports whether an Allocation's submission parameters hold an Instance
// that needs params.
func fits(a *v1alpha1.Allocation, params v1alpha1.BatchParameters) bool {
	b := a.Spec.ForProvider.Batch
	return b != nil && b.Partition == params.Partition && b.GPUs == params.GPUs &&
		b.CPUsPerTask >= params.CPUsPerTask && b.Memory.Cmp(params.Memory) >= 0
}

func drainWindowOf(pv *providerView, md *v1alpha1.ModelDeployment) time.Duration {
	var routing *v1alpha1.Routing
	if md != nil {
		routing = md.Spec.Routing
	}
	w, err := v1alpha1.InstanceDrainWindow(pv.profile.Spec, routing)
	if err != nil {
		return v1alpha1.DefaultDrainWindow
	}
	return w
}

// createInstance creates a pending Instance of the class's current ID on the
// named Allocation, owned by the ModelDeployment, with the drain finalizer. A
// no-overlap successor carries annotationNoOverlapSuccessor with successorOf,
// which names the Provider and the limit, and annotationNoOverlapGap with
// gapOf once the gap has begun.
func (p *pass) createInstance(pv *providerView, cid *classID, pred *v1alpha1.Instance, alloc, successorOf, gapOf string) error {
	name, err := randomName(truncate(p.md.Name, 40))
	if err != nil {
		return err
	}
	in := &v1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: p.md.Namespace,
			Labels: map[string]string{
				v1alpha1.LabelProvider:        pv.name,
				v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(p.md.Name),
				v1alpha1.LabelDeploymentID:    cid.id.Display(),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1alpha1.GroupVersion.String(), Kind: "ModelDeployment", Name: p.md.Name, UID: p.md.UID,
				Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true),
			}},
			Finalizers: []string{v1alpha1.FinalizerDrain},
		},
		Spec: v1alpha1.InstanceSpec{
			DeploymentID: cid.id, IdentityRecord: cid.record,
			AllocationRef: v1alpha1.ObjectReference{Name: alloc},
			Launch:        renderLaunch(p.md, p.plan),
		},
	}
	if pred != nil {
		in.Annotations = map[string]string{v1alpha1.AnnotationPredecessor: pred.Name}
		if successorOf != "" {
			in.Annotations[annotationNoOverlapSuccessor] = successorOf
			if gapOf != "" {
				in.Annotations[annotationNoOverlapGap] = gapOf
			}
		}
	}
	if err := p.r.Client.Create(p.ctx, in, owner); err != nil {
		return fmt.Errorf("create Instance on Allocation %s: %w", alloc, err)
	}
	in.Status.Phase = v1alpha1.InstancePending
	if err := p.r.Client.Status().Update(p.ctx, in, owner); err != nil {
		return fmt.Errorf("set Instance %s pending: %w", in.Name, err)
	}
	p.insts = append(p.insts, in)
	return nil
}

// createAllocation creates the Allocation, then its credential file.
func (p *pass) createAllocation(pv *providerView, name string, params v1alpha1.BatchParameters) error {
	a := &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: p.md.Namespace,
			Labels: map[string]string{
				v1alpha1.LabelProvider:        pv.name,
				v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(p.md.Name),
			},
		},
		Spec: v1alpha1.AllocationSpec{
			ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: pv.name},
			ManagementPolicies:      v1alpha1.DefaultManagementPolicies(),
			ForProvider:             v1alpha1.AllocationParameters{Batch: &params},
			ModelDeploymentRef:      &v1alpha1.ObjectReference{Name: p.md.Name},
			RetentionPolicy:         p.md.Spec.RetentionPolicy.DeepCopy(),
		},
	}
	if err := p.r.Client.Create(p.ctx, a, owner); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil // it exists, so it holds its slot
		}
		p.releaseSlot(pv, name)
		return fmt.Errorf("create Allocation %s: %w", name, err)
	}
	p.allocs[a.Name] = a
	return p.r.ensureCredential(a)
}

// ensureAllocation creates the Allocation a pending Instance names, after a
// pass that created the Instance stopped before the Allocation. It reserves
// the slot under that name first: one this controller reserved before it
// stopped is still held, and after a restart the slot is reserved anew. With
// no free slot the Instance waits, and the deployment shows SubmitLimitReached.
func (p *pass) ensureAllocation(in *v1alpha1.Instance) error {
	pv := p.pvOf(in.Labels)
	if pv == nil || !pv.usable() || pv.tooShort != "" || !p.canProvision() {
		return nil // retried on a later pass, when the Provider is usable again
	}
	params, err := partitionSizing(pv, in.Spec.Launch.GPUs, p.md.Spec.LeaseLimit)
	if err != nil {
		return nil
	}
	name := in.Spec.AllocationRef.Name
	ok, err := p.reserveSlot(pv, name)
	if err != nil {
		return err
	}
	if !ok {
		p.degrade(v1alpha1.ReasonSubmitLimitReached, "Instance %s waits for a submit slot for its Allocation %s: %s", in.Name, name, p.noSlot[pv.name])
		return nil
	}
	return p.createAllocation(pv, name, params)
}

// ensureCredential writes an Allocation's credential file, 256 random bits as
// 64 lowercase hex characters, when it does not exist (docs/development.md,
// Runtime conventions). The provider waits for it before it submits.
func (r *Reconciler) ensureCredential(a *v1alpha1.Allocation) error {
	if a.UID == "" {
		return fmt.Errorf("allocation %s has no UID yet", a.Name)
	}
	path := agentapi.AllocationCredentialPath(r.Config.CredentialsDir, string(a.UID))
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	key, err := agentapi.NewKey()
	if err != nil {
		return fmt.Errorf("credential of Allocation %s: %w", a.Name, err)
	}
	if err := agentapi.WriteKeyFile(path, key); err != nil {
		return fmt.Errorf("write the credential of Allocation %s: %w", a.Name, err)
	}
	return nil
}

// ensureCredentials writes the credential file of every Allocation not yet
// submitted that lacks one: a pass stopped between the create and the write.
func (p *pass) ensureCredentials() error {
	for _, a := range p.allocs {
		if !a.DeletionTimestamp.IsZero() || a.Annotations[v1alpha1.AnnotationJobID] != "" {
			continue
		}
		if err := p.r.ensureCredential(a); err != nil {
			return err
		}
	}
	return nil
}

const nameAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

// randomName returns prefix-<5 random characters>, as HBEP-3 names
// Allocations (<provider>-<5 random characters>).
func randomName(prefix string) (string, error) {
	var sb strings.Builder
	sb.WriteString(strings.TrimRight(prefix, "-.") + "-")
	for range 5 {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(nameAlphabet))))
		if err != nil {
			return "", err
		}
		sb.WriteByte(nameAlphabet[n.Int64()])
	}
	return sb.String(), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
