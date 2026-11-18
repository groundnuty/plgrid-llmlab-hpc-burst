package lifecycle

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sort"
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

// errDeploymentGone is what the creation of an Instance or an Allocation returns when the deployment is gone or being deleted by the time
// it is about to be created (confirmDeployment): nothing is created, and the pass stops creating.
var errDeploymentGone = errors.New("the deployment is gone or being deleted")

// confirmDeployment reads the deployment again, from the API and not a cache, immediately before an Instance or an Allocation is
// created, and says errDeploymentGone when it is gone, being deleted, or another deployment under the same name (issue #503; the
// science agent's ruling of 13 Nov 2026). A pass reads the deployment once, at its start, and works from lists it reads after that, so a
// delete that lands in between, with the garbage collector marking the Instances deleting, would otherwise read as a live deployment
// with no live Instance, and capacity would make a replacement for a deployment that was just deleted. Reading it last, right before
// each create, leaves only a delete that lands between this read and the create itself. That residual is bounded: an Instance made in it
// is owned by the deleted deployment, so the garbage collector takes it and the shutdown rules end it; an Allocation made in it has no
// Instance, so retention releases it at once (releaseEmpty: a pending Allocation with no Instance, and, once its job has started, the
// deleted deployment's rule), and its job, if the Provider has submitted it by then, is cancelled by the Allocation's finalizer. The
// pass also stops creating (p.shutdown); that changes nothing it writes, since a pass that finds the deployment gone here is working
// from a copy that the deletion has made stale, so its status write conflicts in any case.
func (p *pass) confirmDeployment() error {
	var md v1alpha1.ModelDeployment
	err := p.r.Reader.Get(p.ctx, p.key, &md)
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return fmt.Errorf("get ModelDeployment %s before a create: %w", p.key, err)
	case md.UID == p.md.UID && md.DeletionTimestamp.IsZero():
		return nil
	}
	p.shutdown = true
	return errDeploymentGone
}

// canProvision reports whether new Allocations may be created at all.
func (p *pass) canProvision() bool {
	return p.md != nil && p.specErr == "" && p.r.Config.FrontConfigured && !p.shutdown
}

// provision places one Instance: a new primary (pred nil) or the replacement of pred, by the order
// placementOrder gives (HBEP-7, Provisioning, Where; open question 14 for warm Allocations). It returns false when
// no Provider could take it.
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
	// hedgeGroup, when set, names the first member of the hedge the new
	// Instance joins, written as AnnotationHedgeGroup in its create request
	// (HBEP-7, Hedging (beta), How it is found).
	hedgeGroup string
	// skipProviders are Providers not to try: those of the hedge's live
	// members, since a hedge has at most one live member per Provider.
	skipProviders map[string]bool
	// quiet sets no condition for a Provider passed over (no Degraded reason,
	// no SubmitLimitReached record): a hedge's member is extra capacity
	// (HBEP-7, Hedging (beta), A Provider passed over).
	quiet bool
	// caps checks the Provider's per-user caps for the new Allocation beside
	// ours there, inside the slot's reservation (capsFit).
	caps bool
	// extra marks the over-allocation extra (HBEP-7, Over-allocation and qualification (beta), When the extra is
	// submitted; #616 PR 4): a Provider that refuses it is passed over in silence, since a refused extra is k = 0
	// there in effect and no degradation. It implies quiet, which skips each of the three records a refusal
	// would otherwise leave: p.noSlot (reserveSlotFor's quiet, so no SubmitLimitReached text), p.sizingRefused
	// (the sizing branch's `if o.quiet { continue }`) and a Degraded reason (the loopPaused branch's
	// `if !o.quiet`, and the same sizing branch for ProviderUnavailable). The fourth place, noProvider(), is
	// never reached: capacity() calls provisionWith for the extra outside its missing-primary loop, and reads
	// only the Provider that loop placed on.
	extra bool
	// alsoLeave is how many of the deployment's own chosen primaries on the Provider hold no reserved slot yet but
	// will once Running (extraAlsoLeave), left free beside the new Allocation: the extra's slot is beyond its own
	// handovers (HBEP-7, Its checks, a slot beyond its own handovers). Set with extra, on the Provider named by only.
	alsoLeave int
	// deadline, when set, is the start deadline of the replacement a hedge member is for, which the pass read from
	// the hedge's need: a deferral never runs past it. It carries the deadline of a replacement hedge whose
	// predecessor is gone, which a nil predecessor cannot give (growHedges; Astra's review of #724, point 3).
	deadline time.Time
	// headroom is the extra's admission headroom (extraChecks): how many more slots the reservation leaves free, read
	// under the Provider's lock (slotAsk.headroom). Set with extra.
	headroom func(context.Context) (int, error)
	// qosAlsoLeave and qosHeadroom are alsoLeave and headroom counted over the Allocations under the extra's own QOS,
	// for a Provider whose partition sets qosByGpus (extraChecks; HBEP-7, Per-user limits). Set with extra.
	qosAlsoLeave int
	qosHeadroom  func(context.Context) (int, error)
}

// placeOnWarm creates the Instance that goes on the deployment's warm Allocation, under the Provider's own lock, the one
// every reservation takes (slots.go), and clears the Allocation's idle anchor: the Allocation is no longer idle
// (v1alpha1.ClearIdleSince). The Instance becomes a Running primary that reserves a handover slot and ends its
// deployment's missing primary, so both the reserved slots and the admission headroom an extra reads change with
// it; made outside the lock, another pass could read the one before and the other after (#708, the delta review of the
// headroom). The lock is held across two API writes of no other lock of this package.
func (p *pass) placeOnWarm(pv *providerView, cid *classID, pred *v1alpha1.Instance, warm *v1alpha1.Allocation, o placeOpts) error {
	l := p.r.slots().slotLock(pv.name)
	l.Lock()
	defer l.Unlock()
	if err := p.createInstance(pv, cid, pred, warm.Name, o); err != nil {
		return err
	}
	return p.writeIdleSince(warm, nil)
}

// provisionWith places one Instance by the steps placementOrder gives: the first that places it ends the walk. A warm
// step that finds an unregistered warm Allocation (warmStep: stepDeferred) holds the NEW SUBMISSIONS that come
// after it, and no more: the walk goes on to the later warm steps, so a registered spare on a later Provider is still
// taken, and retention cannot release it during the wait (HBEP-7, Provisioning, Where; amendment #719 as reworded,
// and Astra's review of #724, point 4). With nothing placed and a hold standing, the placement is deferred until the
// hold's end. It returns false when no step could take it.
func (p *pass) provisionWith(pred *v1alpha1.Instance, o placeOpts) (bool, error) {
	if !p.canProvision() {
		return false, nil
	}
	if o.extra {
		o.quiet = true // see placeOpts.extra
	}
	var holdUntil time.Time // when the submissions held by a deferral may go on; zero: none are held
	hold := func(until time.Time) {
		if holdUntil.IsZero() || until.Before(holdUntil) {
			holdUntil = until
		}
	}
	steps := p.placementOrder(pred, o)
	for i, st := range steps {
		if st.kind == stepSubmit && !holdUntil.IsZero() {
			continue // held: a warm Allocation of this deployment is awaited
		}
		t := p.targetOf(st.pv, o, st.kind == stepSubmit)
		if t == nil {
			continue
		}
		var res stepResult
		var until time.Time
		var err error
		switch {
		case st.kind == stepWarm:
			res, until, err = p.warmStep(st.pv, t, pred, o)
		default:
			if until, ok := p.waitsForWarmBefore(pred, st, steps[i+1:], o); ok {
				hold(until)
				continue
			}
			res, err = p.submitOn(st.pv, t, pred, o)
		}
		if res == stepDeferred {
			hold(until)
			continue
		}
		if res != stepPassed || err != nil {
			return res == stepPlaced, err
		}
	}
	if !holdUntil.IsZero() {
		p.at(holdUntil)
		p.deferred = true // a placement that waits is not a missing Provider
	}
	return false, nil
}

// warmOf returns the deployment's warm Allocations on the Provider: Running, not deleting, hosting no live
// Instance, not pending release (releasePending), whose relay has not ended for good (relayEnded) and not committed for
// release by an Executing Operation (committedForRelease: the Operation ends it, so nothing is placed on it and the
// extra's bound does not count it), the latest hard end first, then by name (HBEP-7, Over-allocation and qualification
// (beta), When the extra is submitted: "Warm Allocations count toward k only here, in the extra's creation bound"). It
// checks no fit, no lease and no registration: warmFor, which places on one, keeps those, and the extra's bound only
// asks whether one exists. One predicate, so the placement and the bound never disagree about what is warm. An
// Allocation with no hard end yet sorts last.
func (p *pass) warmOf(pv *providerView) []*v1alpha1.Allocation {
	var out []*v1alpha1.Allocation
	for _, a := range p.allocs {
		if a.Labels[v1alpha1.LabelProvider] != pv.name || !a.DeletionTimestamp.IsZero() ||
			a.Status.AtProvider.Phase != v1alpha1.AllocationRunning || len(p.liveOn(a.Name)) > 0 ||
			p.releasePending(a) || relayEnded(a) || p.committedForRelease(a) {
			continue
		}
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		hi, hj := out[i].Status.AtProvider.HardEnd, out[j].Status.AtProvider.HardEnd
		switch {
		case hi != nil && hj == nil:
			return true
		case hi == nil && hj != nil:
			return false
		case hi != nil && !hi.Equal(hj):
			return hi.After(hj.Time)
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// warmFor returns the warm Allocation provisioning places an Instance of the given size on, and when the first one
// has not registered, until when to wait for it (HBEP-7, open question 14; #616 PR 4, built to #604 PR 3's contract).
// It is warmOf plus the placement's checks: a Running Allocation of this deployment that hosts no live Instance has
// lease left above twice the class's start-to-ready time plus the drain window, fits the launch (the same partition,
// so the same class, and GPU count, and at least the CPUs and memory a new Allocation would request: review of PR
// 88), and has a registered agent. One marked RelayEndedForGood is passed over in warmOf (relayEnded): step 7, which
// places, runs before step 9, which releases an empty one, so one pass would otherwise place an Instance on it and the
// next would fail that Instance, which counts toward RestartLoop (issue #332). One whose agent has not registered
// again yet, during the grace after the controller's own restart, is waited for rather than passed over.
//
// For a replacement (pred != nil) the warm Allocation's drain start must be later than its predecessor's BY AT LEAST the
// lead an Instance needs (leadOf: the drain window, handoverMargin and health.startTimeout; HBEP-7, Hedging (beta), Started
// losers are kept, and Over-allocation and qualification (beta), The lowest-ranked is kept; the science agent's ruling on
// #708's Fable review, amendment #711): "a replacement goes there only when the warm Allocation's drain start is later than
// its predecessor's by at least the lead an Instance needs, or it would drain with or soon after it, before its own
// replacement could start". One that drains with the predecessor, as the ranked-out extra's does, would end the replacement at
// the very moment it is meant to take over, and one that drains moments after it would leave the deployment without a ready
// Instance until the replacement's own replacement, planned only then, starts and is ready. At exactly the lead it is used.
// The pick is the first of warmOf's, the latest hard end first, so it is the same on every pass.
func (p *pass) warmFor(pv *providerView, params v1alpha1.BatchParameters, pred *v1alpha1.Instance) (warm *v1alpha1.Allocation, deferUntil time.Time) {
	need := 2*startToReady(p.md, pv.class) + drainWindowOf(pv, p.md)
	for _, a := range p.warmOf(pv) {
		obs := a.Status.AtProvider
		if obs.HardEnd == nil || obs.HardEnd.Sub(p.now) <= need || !fits(a, params) || !p.drainsAfter(pv, a, pred) {
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

// drainsAfter reports whether the warm Allocation's drain start (its hard end minus the Provider's drain window,
// as v1alpha1.DrainStart reads it) is later than the predecessor's (drainStart: its lease end, the Provider's profile and
// the deployment's routing) by at least the lead an Instance needs on the warm Allocation's Provider (leadOf). It is true
// with no predecessor, and when the predecessor has no drain start to compare with (it never started, so nothing serves
// that the replacement must outlive).
func (p *pass) drainsAfter(pv *providerView, warm *v1alpha1.Allocation, pred *v1alpha1.Instance) bool {
	if pred == nil {
		return true
	}
	ps, ok := p.drainStart(pred, p.pvOf(pred.Labels))
	if !ok {
		return true
	}
	var routing *v1alpha1.Routing
	if p.md != nil {
		routing = p.md.Spec.Routing
	}
	if pv.profile == nil || warm.Status.AtProvider.HardEnd == nil {
		return false
	}
	ws, err := v1alpha1.DrainStart(warm.Status.AtProvider.HardEnd.Time, pv.profile.Spec, routing)
	if err != nil {
		return false
	}
	lead, _ := leadOf(pv, p.md)
	return !ws.Before(ps.Add(lead))
}

// releasePending reports whether an Instance on the Allocation failed, so the
// Allocation is to be released (a release that failed or was interrupted is
// made on a later pass): it is never warm capacity.
func (p *pass) releasePending(a *v1alpha1.Allocation) bool {
	if allocationFailed(a) {
		return true // a job that failed or was lost, or a relay ended for good: retention releases it by its own reason
	}
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
// (TestAMarkedEmptyAllocationDoesNotStarveAProviderAtItsSubmitLimit). Releasing
// it empty would release capacity that retention keeps, which HBEP-7 open
// question 8 (Approvals) reserves for the owner, whose "Yes, release it" (#331)
// covered RelayEndedForGood only (`@hpc-burst-science-agent`, 10 Nov 2026).
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
	// Beta, opt-in: the router the Instance runs for its life (#429).
	in.Spec.Launch.Router = p.r.Config.InstanceRouter
	if o.hedgeGroup != "" {
		in.Annotations = map[string]string{v1alpha1.AnnotationHedgeGroup: o.hedgeGroup}
	}
	if pred != nil {
		if in.Annotations == nil {
			in.Annotations = map[string]string{}
		}
		in.Annotations[v1alpha1.AnnotationPredecessor] = pred.Name
		if o.successorOf != "" {
			in.Annotations[annotationNoOverlapSuccessor] = o.successorOf
			if o.gapOf != "" {
				in.Annotations[annotationNoOverlapGap] = o.gapOf
			}
		}
	}
	stampOwner(&in.ObjectMeta, p.md) // last: the blocks above may assign a whole annotation map
	if err := p.confirmDeployment(); err != nil {
		return err
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
	stampOwner(&a.ObjectMeta, p.md) // after the mark, which assigns the annotation map
	if err := p.confirmDeployment(); err != nil {
		p.releaseSlot(pv, name)
		return err
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
//
// A replacement or successor (an Instance that names a predecessor) holds a slot while it waits
// (slots_reserved.go), so one whose Allocation can never be made on its Provider, for a lasting cause, is
// ended rather than left waiting for ever: its Provider dropped from the deployment's list or managed, a
// partition too short for its lease (lastingCause), or its sizing refused. The primary's reserved slot then
// returns, and replacements() plans it again, on its Provider first (HBEP-7, Provisioning, Per-user limits;
// the science agent's ruling on #599). A Provider problem, which can pass, keeps it waiting, and so does a
// deployment's first primary, which holds no slot while it waits.
func (p *pass) ensureAllocation(in *v1alpha1.Instance) error {
	if !p.canProvision() {
		return nil
	}
	pv := p.pvOf(in.Labels)
	name := in.Spec.AllocationRef.Name
	handover := in.Annotations[v1alpha1.AnnotationPredecessor] != ""
	// This Instance's lease is judged at its own launch, which its Allocation is sized by (partitionSizingFor below), not at
	// the deployment's current plan (atLaunch).
	if atOwn := p.atLaunch(pv, in); atOwn != nil && atOwn.tooShort != pv.tooShort {
		if atOwn.tooShort != "" {
			p.degrade(v1alpha1.ReasonPartitionTooShort, "%s", atOwn.tooShort)
		}
		pv = atOwn
	}
	if pv != nil && handover {
		if reason, why := lastingCause(pv); reason != "" {
			return p.end(in, nil, reason, fmt.Sprintf("its Allocation %s was never made and can't be: %s", name, why), true)
		}
	}
	if pv == nil || !pv.usable() || pv.tooShort != "" {
		return nil // retried on a later pass, when the Provider is usable again
	}
	params, err := partitionSizingFor(pv, in.Spec.Launch.GPUs, v1alpha1.LaunchWorkers(in.Spec.Launch), p.md.Spec.LeaseLimit)
	if err != nil {
		// The Provider's limits changed between the two creates (a zero GPU
		// cap, say), and the pending Instance already counts as a primary, so
		// provision never reports the refusal: report it here, as provision
		// does, or the deployment shows AsDesired while the Instance waits for
		// ever (review of PR 171, Astra B4).
		var u *unsupported
		if asUnsupported(err, &u) && handover {
			return p.end(in, nil, v1alpha1.ReasonSpecUnsupported, fmt.Sprintf("its Allocation %s was never made and can't be: Provider %s refuses its sizing: %s", name, pv.name, u.msg), true)
		}
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
	// It takes the reserved slot of its predecessor's chain, and a no-overlap successor its own, which is held
	// for it while its Allocation waits for the slot its predecessor frees (slots_reserved.go).
	// An Instance with no Allocation is one of two things: a successor, a replacement or a hedge's member, which waits as
	// it always did, or a deployment's first-round Instance. When the units that have an Allocation already number the
	// deployment's desired count (shortageOf, never the selection's tie) it is THE EXTRA, whose retry takes the extra's own
	// checks, the bound, the slot beyond its primary's handover, the admission headroom and the caps, silently, and is ended
	// when one refuses (#708, finding 4); otherwise it is a primary, which waits for a slot.
	takers := p.takersOf(p.instanceNamed(in.Annotations[v1alpha1.AnnotationPredecessor]))
	var x extraAsk
	var capsOf *v1alpha1.BatchParameters
	quiet := false
	member := in.Annotations[v1alpha1.AnnotationHedgeGroup] != ""
	switch {
	case !handover && !member && p.shortage() == 0:
		var admitted bool
		if x, admitted = p.extraChecks(pv, in, params.GPUs); !admitted {
			return p.endCutShortExtra(in, "the bound on extras")
		}
		capsOf, quiet = &params, true
	case member:
		// A hedge member's retry reserves as its first creation did: the caps check beside our Allocations there, and no
		// condition for a refusal (a member is extra capacity), since another job may have filled its QOS since (review of #756,
		// Astra 4; in alpha too).
		capsOf, quiet = &params, true
	}
	ok, holds, err := p.reserveSlotAs(pv, params.GPUs, name, holder, takers, &params, capsOf, quiet, x)
	if err != nil {
		return err
	}
	if !ok && quiet && member {
		return p.endCutShortMember(in)
	}
	if !ok && quiet {
		return p.endCutShortExtra(in, "the slot beyond its primary's handover, the admission headroom or the caps")
	}
	if !ok {
		p.degrade(v1alpha1.ReasonSubmitLimitReached, "Instance %s waits for a submit slot for its Allocation %s: %s", in.Name, name, p.noSlot[pv.name])
		return nil
	}
	if err := p.createAllocation(pv, name, params, markValue(holds, successorOf)); err != nil && !errors.Is(err, errDeploymentGone) {
		return err
	}
	return nil
}

// endCutShortMember ends a pending hedge member whose Allocation was never made and whose slot or caps check now refuses
// it: a member is extra capacity, so it is dropped in silence, as a member that finds no room at its creation is passed over.
func (p *pass) endCutShortMember(in *v1alpha1.Instance) error {
	return p.end(in, nil, v1alpha1.ReasonDrained, "scaled down: its Provider refuses the hedge member (the slot or the caps)", true)
}

// endCutShortExtra ends a pending extra whose Allocation was never made and whose checks refuse it now, as step 7 ends
// a pending surplus beyond k: over-allocation's own trim, outcome-neutral, with no Degraded and no SubmitLimitReached
// (a refused extra is silent, D7).
func (p *pass) endCutShortExtra(in *v1alpha1.Instance, why string) error {
	return p.applyScaleTarget(scaleTarget{instance: in, effect: effectCancelPending, why: "scaled down: its Provider refuses the extra (" + why + ")", cause: scaleOverAllocationTrim})
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
