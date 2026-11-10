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
			// An Instance placed on a warm Allocation creates no Allocation, so nothing is marked: the mark
			// is written once, in an Allocation's create request, and an existing one is never marked later
			// (HBEP-7, Handover, No-overlap mode: "fixed at creation"). A reusable warm Allocation also
			// means the primary has room beside it (decideOverlap), so this is not a queued-path successor.
			if err := p.createInstance(pv, cid, pred, warm.Name, o); err != nil {
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
		//
		// The queued-successor mark is judged from this same read: whether the primary's Allocation
		// still holds its slot is what the reservation's own list says (reserveSlotFor).
		ok, holds, err := p.reserveSlotFor(pv, name, p.primaryAllocKey(o.primaryName(pred)))
		if err != nil {
			return false, err
		}
		if !ok {
			continue
		}
		if err := p.createInstance(pv, cid, pred, name, o); err != nil {
			p.releaseSlot(pv, name)
			return false, err
		}
		return true, p.createAllocation(pv, name, params, markValue(holds, o.successorOf))
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
// One marked RelayEndedForGood is passed over (relayEnded): step 7, which
// places, runs before step 9, which releases an empty one, so one pass would
// otherwise place an Instance on it and the next would fail that Instance,
// which counts toward RestartLoop (issue #332).
// One whose agent has not registered again yet, during the grace after the
// controller's own restart, is waited for rather than passed over: deferUntil
// says until when.
func (p *pass) warmAllocation(pv *providerView, params v1alpha1.BatchParameters) (warm *v1alpha1.Allocation, deferUntil time.Time) {
	need := 2*startToReady(p.md, pv.class) + drainWindowOf(pv, p.md)
	for _, a := range p.allocs {
		obs := a.Status.AtProvider
		if a.Labels[v1alpha1.LabelProvider] != pv.name || !a.DeletionTimestamp.IsZero() ||
			obs.Phase != v1alpha1.AllocationRunning || obs.HardEnd == nil || len(p.liveOn(a.Name)) > 0 ||
			obs.HardEnd.Sub(p.now) <= need || !fits(a, params) || p.releasePending(a) || relayEnded(a) {
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

// relayEnded reports whether a host-dialed relay of the Allocation's job ended
// for good (RelayEndedForGood, issue #304, set once and never cleared): nothing
// placed on it can be reached, step 5 ends any Instance placed on it, and step
// 9 releases it once it is empty, so it is never warm capacity.
//
// SubmittedAsOther is left out on purpose, though step 5 ends an Instance
// placed on such an Allocation too. Nothing releases an empty one
// (releaseEmpty reads RelayEndedForGood only), so skipping it here alone would
// leave it holding its submit slot until its retention date or hard end, and a
// deployment at its Provider's limit would then starve, where now the failed
// placement releases it and the replacement serves
// (TestAMarkedEmptyAllocationDoesNotStarveAProviderAtItsSubmitLimit). Skipping
// it and releasing it empty belong together, and that is a ruling for the
// science agent (issue #332).
func relayEnded(a *v1alpha1.Allocation) bool {
	return a.GetCondition(v1alpha1.ConditionRelayEndedForGood).Status == "True"
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

// primaryName names the primary a no-overlap successor made with these options
// replaces: the one that drained, for a retry (gapOf), else the predecessor. It
// is "" for an Instance that is no no-overlap successor.
func (o placeOpts) primaryName(pred *v1alpha1.Instance) string {
	switch {
	case o.successorOf == "":
		return ""
	case o.gapOf != "":
		return o.gapOf
	case pred != nil:
		return pred.Name
	}
	return ""
}

// primaryAllocKey returns the key (namespace/name) of the Allocation of the
// named primary Instance, or "" when the name is empty or the Instance is not
// known. It names the Allocation whose state the queued-successor mark reads.
func (p *pass) primaryAllocKey(primary string) string {
	if primary == "" {
		return ""
	}
	for _, in := range p.insts {
		if in.Name == primary {
			return p.key.Namespace + "/" + in.Spec.AllocationRef.Name
		}
	}
	return ""
}

// markValue is the value of v1alpha1.AnnotationQueuedSuccessor for an Allocation
// about to be created for a no-overlap successor, or a retry of one:
// successorOf (the Provider and the limit) when the primary's own Allocation, on
// the same Provider, still holds its slot, else "" (HBEP-7, Handover, No-overlap
// mode; issue #195). holds is the reservation's own answer (reserveSlotFor), so
// the mark follows the very list the slot count is made from, for the Provider
// the spec names and the phase it has now, not the pass's first observation.
// The primary is the one the chain replaces, read through any failed
// successors: a retry names it directly. It is judged when the Allocation is
// created, and again for one created later for a pending Instance
// (ensureAllocation), never afterwards. On the one-slot path the successor is
// created after the primary's job left the slot, so it is not marked.
func markValue(holds bool, successorOf string) string {
	if holds {
		return successorOf
	}
	return ""
}

// createInstance creates a pending Instance of the class's current ID on the
// named Allocation, owned by the ModelDeployment, with the drain finalizer. A
// no-overlap successor carries annotationNoOverlapSuccessor with o.successorOf,
// which names the Provider and the limit, and annotationNoOverlapGap with
// o.gapOf once the gap has begun.
func (p *pass) createInstance(pv *providerView, cid *classID, pred *v1alpha1.Instance, alloc string, o placeOpts) error {
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
		if o.successorOf != "" {
			in.Annotations[annotationNoOverlapSuccessor] = o.successorOf
			if o.gapOf != "" {
				in.Annotations[annotationNoOverlapGap] = o.gapOf
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

// createAllocation creates the Allocation, then its credential file. A
// non-empty queuedOf is the value of v1alpha1.AnnotationQueuedSuccessor: the
// Allocation is created with the mark, so no recorder ever sees it without.
func (p *pass) createAllocation(pv *providerView, name string, params v1alpha1.BatchParameters, queuedOf string) error {
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
	if queuedOf != "" {
		a.Annotations = map[string]string{v1alpha1.AnnotationQueuedSuccessor: queuedOf}
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
	name := in.Spec.AllocationRef.Name
	params, err := partitionSizing(pv, in.Spec.Launch.GPUs, p.md.Spec.LeaseLimit)
	if err != nil {
		// The Provider's limits changed between the two creates (a zero GPU
		// cap, say), and the pending Instance already counts as a primary, so
		// provision never reports the refusal: report it here, as provision
		// does, or the deployment shows AsDesired while the Instance waits for
		// ever (review of PR 171, Astra B4).
		var u *unsupported
		if asUnsupported(err, &u) {
			p.degrade(v1alpha1.ReasonSpecUnsupported, "Instance %s waits for its Allocation %s: %s", in.Name, name, u.msg)
		} else {
			p.degrade(v1alpha1.ReasonProviderUnavailable, "Instance %s waits for its Allocation %s: Provider %s: %v", in.Name, name, pv.name, err)
		}
		return nil
	}
	// A successor, or a retry of one, names the primary it replaces: the one that drained once the gap has
	// begun, else its predecessor. The mark is judged now, as for any Allocation at its creation, from the
	// reservation's own read.
	holder := ""
	successorOf := in.Annotations[annotationNoOverlapSuccessor]
	if successorOf != "" {
		primary := in.Annotations[annotationNoOverlapGap]
		if primary == "" {
			primary = in.Annotations[v1alpha1.AnnotationPredecessor]
		}
		holder = p.primaryAllocKey(primary)
	}
	ok, holds, err := p.reserveSlotFor(pv, name, holder)
	if err != nil {
		return err
	}
	if !ok {
		p.degrade(v1alpha1.ReasonSubmitLimitReached, "Instance %s waits for a submit slot for its Allocation %s: %s", in.Name, name, p.noSlot[pv.name])
		return nil
	}
	return p.createAllocation(pv, name, params, markValue(holds, successorOf))
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
