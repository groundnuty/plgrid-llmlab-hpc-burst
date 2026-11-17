package lifecycle

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Per-user submit limits, with slots reserved atomically (HBEP-7, Provisioning,
// Per-user limits; science-agent decision, 1 Nov 2026, from PR #151's review).
//
// The controller plans per Provider within the submit limit of the QOS its
// Allocations run under (maxSubmitPerUser of the QOS limits that apply,
// v1alpha1.EffectiveQOSLimits). It counts every Allocation of that Provider, of
// every ModelDeployment in every namespace: from its creation until its phase
// is Ended with its job no longer completing, or its deletion completes, so
// Submitting, SubmissionUnknown, held, pending, running, Failed, Lost, Refused,
// completing and deleting ones all count.
//
// Before it creates an Allocation, a worker reserves a slot under the
// Provider's lock, which every worker takes. The count it checks is the listed
// Allocations of the Provider that have not ended, or whose job is still
// completing, plus this controller's
// creations that the list does not show yet: Kubernetes' expectations pattern
// (ExpectCreations raises, CreationObserved lowers, ExpectationsTimeout of
// 5 min expires; pkg/controller/controller_utils.go at v1.35.0, lines 72,
// 127-169 and 236-266), keyed by the Allocation's namespace and name as
// UIDTrackingControllerExpectations keys deletions, since the name is chosen
// before the create. A creation that fails gives its slot back; one that the
// list shows is counted once, as listed. An Allocation deleted before any list
// showed it keeps its expectation until the expiry, which over-counts for at
// most 5 min and never under-counts.
//
// The Instance is the durable intent and is created before its Allocation
// (provision.go), so the slot is reserved before the Instance, under the name
// the Instance will hold. A pass cut short between the two is finished by
// ensureAllocation, which reserves under the same name: a name already
// reserved or listed counts once, so the second reservation is free.
//
// With one active replica no two workers create past the limit. Across a
// leader hand-over each replica has its own expectations, and a Lease does not
// fence (HBEP-11), so one extra creation can race: Slurm's own limit then
// refuses it (SubmitLimitReached) or the GPU cap makes it pend.
//
// The count also holds the slots reserved for handovers (slots_reserved.go;
// HBEP-7, A serving primary's replacement slot is reserved; issue #598), worked
// out under the same lock from the same moment's reads.

// expectationsTimeout is ExpectationsTimeout of Kubernetes' controller_utils.go
// (v1.35.0, line 72): an expectation not observed within it lapses.
const expectationsTimeout = 5 * time.Minute

// listTimeout is how long the reads under a Provider's lock may take, all of
// them together: the Allocations' List and the reserved slots' reads.
const listTimeout = 30 * time.Second

// slotTable holds the per-Provider locks and the expectations.
//
// Lock order (internal/lifecycle): a Provider's lock (slotLock) is taken with
// no other lock of this package held, and only one Provider's lock at a time.
// While it is held, slotTable.mu may be taken, as a leaf, to read or change the
// expectations and the lock map; nothing else of this package (memory.mu,
// Backend.mu, the identity cache, the Hub cache, the metrics views, the
// secrets sweep) is taken while either is held, and neither is taken while one
// of those is held. The Provider's lock is held across the count's reads from
// the API and no other call: one List of the Allocations of every namespace,
// and, for a Provider with a submit limit, one List of the Instances that carry
// the Provider's label, one List of the Instances of each deployment with an
// Instance among them (deploymentViews, by its model-deployment label, in its
// namespace) and one Get of the ModelDeployment of each primary among them
// (reservedOn), all under one context bounded by listTimeout. slotTable.mu is
// never held across any of them.
type slotTable struct {
	// listTimeout bounds the List call made under a Provider's lock, so one
	// hung API call cannot hold every worker that reserves on that Provider.
	listTimeout time.Duration
	mu          sync.Mutex
	// locks holds one lock per Provider name, created on first use.
	locks map[string]*sync.Mutex
	// expect holds, per Provider, the Allocations (namespace/name) this
	// controller reserved a slot for and its list has not shown yet, with
	// when each was raised.
	expect map[string]map[string]time.Time
	// params holds, beside expect, the size of each expected Allocation whose
	// reservation checked the Provider's per-user caps (slotAsk.params), so a
	// later check counts it before the list shows it. It is cleared wherever
	// expect is (observe, release).
	params map[string]map[string]v1alpha1.BatchParameters
}

func newSlotTable() *slotTable {
	return &slotTable{listTimeout: listTimeout, locks: map[string]*sync.Mutex{}, expect: map[string]map[string]time.Time{},
		params: map[string]map[string]v1alpha1.BatchParameters{}}
}

func (t *slotTable) slotLock(provider string) *sync.Mutex {
	t.mu.Lock()
	defer t.mu.Unlock()
	l, ok := t.locks[provider]
	if !ok {
		l = &sync.Mutex{}
		t.locks[provider] = l
	}
	return l
}

// slotCount is a Provider's count against its submit limit at one moment.
type slotCount struct {
	// listed are the Allocations of the Provider the list shows that have not
	// ended, by namespace/name.
	listed map[string]bool
	// expected are the reserved creations the list does not show yet.
	expected int
}

func (c slotCount) total() int { return len(c.listed) + c.expected }

// holds reports whether the Allocation already holds a slot: listed, or
// reserved and not yet listed.
func (t *slotTable) holds(provider, key string, c slotCount) bool {
	_, ok := t.expect[provider][key]
	return c.listed[key] || ok
}

// observe lowers the expectations the list shows (CreationObserved), whatever
// their phase, and those older than expectationsTimeout, and returns the
// count. seen holds every listed Allocation of the Provider, ended or not.
// The caller holds t.mu.
func (t *slotTable) observe(provider string, seen, listed map[string]bool, now time.Time) slotCount {
	exp := t.expect[provider]
	for k, raised := range exp {
		if seen[k] || now.Sub(raised) >= expectationsTimeout {
			delete(exp, k)
			delete(t.params[provider], k)
		}
	}
	return slotCount{listed: listed, expected: len(exp)}
}

// reserve reserves a slot on the Provider for the Allocation key
// (namespace/name) under the limit: it returns true when the Allocation
// already holds one or a slot is free, and then counts it until the list shows
// it or the reservation is released. With no limit it reserves nothing and
// returns true. count is the Provider's count before this reservation. It
// reserves with no profile and no takers, so only no-overlap successors'
// reserved slots are counted (slotAsk).
//
// now is read once the lock is held and the list is back, and that one time
// both expires the old expectations and stamps the new one. A time taken when
// the pass began could be more than expectationsTimeout old by then (a slow
// identity resolution or Hub call before the reservation), and a reservation
// stamped with it would lapse at once: another worker could then take the same
// slot before this one's Allocation is created (review of PR 171, Astra B1).
func (t *slotTable) reserve(ctx context.Context, r client.Reader, provider, key string, limit *int32, now func() time.Time) (ok bool, count int, err error) {
	a, err := t.reserveAsk(ctx, r, slotAsk{provider: provider, key: key, limit: limit}, now)
	return a.ok, a.count, err
}

// slotAsk is a reservation's question about one Provider.
type slotAsk struct {
	provider string
	// key is the Allocation (namespace/name) to reserve a slot for.
	key string
	// holder, when set, is an Allocation (namespace/name) the answer reports on:
	// whether it still holds its slot on the Provider, from the very list the
	// count is made from (slotAnswer.holds).
	holder string
	// takers are the Allocations (namespace/name) whose reserved slots the new
	// one takes: its predecessor's, and those of the predecessors before it. A
	// replacement takes its predecessor's reserved slot (HBEP-7).
	takers []string
	limit  *int32
	// profile is the Provider's, for the drain starts of the reserved slots'
	// primaries (reservedOn); with none, no primary's slot is reserved.
	profile *v1alpha1.PlatformProfileSpec
	// params, when set, is the new Allocation's size: the reservation then also
	// checks the Provider's per-user caps (caps) for it beside our Allocations
	// there (capsFit), and refuses it when they would be exceeded. A hedge's
	// member asks it (HBEP-7, Hedging (beta), Which Provider is next).
	params *v1alpha1.BatchParameters
	caps   v1alpha1.QOSLimits
}

// slotAnswer is a reservation's answer.
type slotAnswer struct {
	ok bool
	// count is the Provider's count before this reservation: the Allocations
	// that hold a slot, the expectations and the reserved slots counted.
	count int
	// reserved is how many of count are reserved slots, and reservedBy the
	// deployments (namespace/name) whose handovers hold them.
	reserved   int
	reservedBy []string
	// holds is whether slotAsk.holder still holds its slot: its phase is not
	// Ended, its deletion has not completed, and its spec.providerConfigRef
	// names the Provider, as listProviderAllocations reads every Allocation. The
	// queued-successor mark is judged from this answer, so the mark and the
	// reservation agree about the primary even if its Allocation ends between
	// the pass's first observation and the reservation (PR 214 review, Astra
	// B1), and about which Provider it is on (B2).
	holds bool
	// capsWhy, when the reservation was refused for the per-user caps
	// (slotAsk.params), says which and by how much.
	capsWhy string
}

// reserveAsk reserves a slot as reserve does, counting the reserved slots the
// question's profile and takers leave (counted), and reports on its holder.
// With no limit and no holder nothing is read.
func (t *slotTable) reserveAsk(ctx context.Context, r client.Reader, q slotAsk, now func() time.Time) (slotAnswer, error) {
	if q.limit == nil && q.holder == "" && q.params == nil {
		return slotAnswer{ok: true}, nil
	}
	l := t.slotLock(q.provider)
	l.Lock()
	defer l.Unlock()
	rd, err := t.readBounded(ctx, r, q.provider, q.limit != nil, q.profile)
	if err != nil {
		return slotAnswer{}, err
	}
	holds := q.holder != "" && rd.listed[q.holder]
	if q.limit == nil && q.params == nil {
		return slotAnswer{ok: true, holds: holds}, nil
	}
	at := now()
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.observe(q.provider, rd.seen, rd.listed, at)
	if t.holds(q.provider, q.key, c) {
		return slotAnswer{ok: true, count: c.total(), holds: holds}, nil
	}
	if q.params != nil {
		if ok, why := capsFit(q.provider, q.caps, rd, t.params[q.provider], *q.params); !ok {
			return slotAnswer{count: c.total(), holds: holds, capsWhy: why}, nil
		}
	}
	var n int
	var by []string
	total := c.total()
	if q.limit != nil {
		n, by = counted(rd.reserved, q.key, q.takers, t.expect[q.provider])
		total += n
		if total >= int(*q.limit) {
			return slotAnswer{count: total, reserved: n, reservedBy: by, holds: holds}, nil
		}
	}
	if t.expect[q.provider] == nil {
		t.expect[q.provider] = map[string]time.Time{}
	}
	t.expect[q.provider][q.key] = at
	if q.params != nil {
		if t.params[q.provider] == nil {
			t.params[q.provider] = map[string]v1alpha1.BatchParameters{}
		}
		t.params[q.provider][q.key] = *q.params
	}
	return slotAnswer{ok: true, count: total, reserved: n, reservedBy: by, holds: holds}, nil
}

// capsFit reports whether a new Allocation of size add fits the Provider's
// per-user caps on GPUs, CPUs and memory beside our Allocations there that
// hold a slot (rd.listed) and those reserved and not listed yet (expected,
// with their sizes), and if not, why (HBEP-7, Hedging (beta), Which Provider is
// next: "the member and our Allocations on that Provider that have not ended
// must fit its QOS's per-user caps"; the extra's check is the same, #616). "Ours"
// is every Allocation the controller lists on the Provider, of every
// deployment, the stricter reading (#616, Q9), so no new Allocation pends
// behind a cap our own jobs hold. An Allocation with no batch parameters adds
// nothing.
func capsFit(provider string, caps v1alpha1.QOSLimits, rd slotReads, expected map[string]v1alpha1.BatchParameters, add v1alpha1.BatchParameters) (bool, string) {
	gpus, cpus, mem := add.GPUs, add.CPUsPerTask, add.Memory.DeepCopy()
	sum := func(b *v1alpha1.BatchParameters) {
		gpus += b.GPUs
		cpus += b.CPUsPerTask
		mem.Add(b.Memory)
	}
	for k, a := range rd.allocs {
		if rd.listed[k] && a.Spec.ForProvider.Batch != nil {
			sum(a.Spec.ForProvider.Batch)
		}
	}
	for k, b := range expected {
		if !rd.listed[k] {
			sum(&b)
		}
	}
	var over []string
	if q := caps.MaxGPUsPerUser; q != nil && gpus > *q {
		over = append(over, fmt.Sprintf("%d GPUs, above the %s %d of QOS %s", gpus, v1alpha1.LimitMaxGPUsPerUser, *q, caps.From[v1alpha1.LimitMaxGPUsPerUser]))
	}
	if q := caps.MaxCPUsPerUser; q != nil && cpus > *q {
		over = append(over, fmt.Sprintf("%d CPUs, above the %s %d of QOS %s", cpus, v1alpha1.LimitMaxCPUsPerUser, *q, caps.From[v1alpha1.LimitMaxCPUsPerUser]))
	}
	if q := caps.MaxMemPerUser; q != nil && mem.Cmp(*q) > 0 {
		over = append(over, fmt.Sprintf("%s of memory, above the %s %s of QOS %s", mem.String(), v1alpha1.LimitMaxMemPerUser, q.String(), caps.From[v1alpha1.LimitMaxMemPerUser]))
	}
	if len(over) == 0 {
		return true, ""
	}
	return false, fmt.Sprintf("our Allocations on Provider %s with a new one would need %s", provider, strings.Join(over, " and "))
}

// release gives back the slot reserved for the Allocation key: its creation
// failed. A reservation the list already shows is not affected, since it is
// counted as listed.
func (t *slotTable) release(provider, key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.expect[provider], key)
	delete(t.params[provider], key)
}

// count returns the Provider's count against its submit limit now, excluding
// the Allocations in skip (namespace/name), without reserving, with no profile
// and no takers (countAsk).
func (t *slotTable) count(ctx context.Context, r client.Reader, provider string, now time.Time, skip ...string) (int, error) {
	n, _, _, err := t.countAsk(ctx, r, slotAsk{provider: provider}, now, skip...)
	return n, err
}

// countAsk returns the Provider's count against its submit limit now, without
// reserving: the Allocations that hold a slot, not counting those in skip
// (namespace/name), plus the expectations, plus the reserved slots the
// question's takers leave (counted), and of those how many are reserved, with
// the deployments that hold them. It
// is the no-overlap path's question, whether a successor could run beside its
// predecessor (nooverlap.go, slotFor); the predecessor is then the taker, so
// its own reserved slot, held for the very replacement being judged, never
// keeps that replacement from running beside it.
func (t *slotTable) countAsk(ctx context.Context, r client.Reader, q slotAsk, now time.Time, skip ...string) (n, reserved int, by []string, err error) {
	l := t.slotLock(q.provider)
	l.Lock()
	defer l.Unlock()
	rd, err := t.readBounded(ctx, r, q.provider, true, q.profile)
	if err != nil {
		return 0, 0, nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.observe(q.provider, rd.seen, rd.listed, now)
	n = c.total()
	for _, k := range skip {
		if t.holds(q.provider, k, c) {
			n--
		}
	}
	reserved, by = counted(rd.reserved, "", q.takers, t.expect[q.provider])
	return n + reserved, reserved, by, nil
}

// slotReads are the reads a count is made from, under the Provider's lock.
type slotReads struct {
	// seen holds every listed Allocation of the Provider, listed those that
	// hold a slot, and allocs the Allocations themselves, all by namespace/name.
	seen, listed map[string]bool
	allocs       map[string]*v1alpha1.Allocation
	// reserved are the Provider's reserved slots (reservedOn).
	reserved []reservedSlot
}

// readBounded makes the count's reads under one context bounded by the table's
// timeout: the Allocations, and, withReserved, the reserved slots.
func (t *slotTable) readBounded(ctx context.Context, r client.Reader, provider string, withReserved bool, profile *v1alpha1.PlatformProfileSpec) (slotReads, error) {
	ctx, cancel := context.WithTimeout(ctx, t.listTimeout)
	defer cancel()
	var rd slotReads
	var err error
	if rd.seen, rd.listed, rd.allocs, err = listProviderAllocations(ctx, r, provider); err != nil {
		return slotReads{}, err
	}
	if withReserved {
		if rd.reserved, err = reservedOn(ctx, r, provider, profile, rd.allocs); err != nil {
			return slotReads{}, err
		}
	}
	return rd, nil
}

// holdsSlot reports whether the Allocation still holds a submit slot as the
// count reads it: its phase is not Ended, or its job is still completing
// (status.atProvider.jobCompleting). A job whose state still carries COMPLETING
// has the phase of its base state, Ended for a cancel or a lease end, but its
// processes and its epilog may run: the Allocation counts until the flag clears,
// and then until its deletion completes (HBEP-7, Provisioning, Per-user limits;
// HBEP-4, Phases, A completing job). The queued-successor mark reads it too
// (queuedMark), so the two never disagree about a primary that has left the slot.
// An Allocation whose deletion has completed is not listed at all.
//
// The Provider's controller writes jobCompleting from the same value that gates
// the deletion finalizer, so on a deleting Allocation it is also true while
// another job with the tag completes (internal/providerctl, completingBlocker):
// the slot and the finalizer never disagree.
func holdsSlot(a *v1alpha1.Allocation) bool {
	o := &a.Status.AtProvider
	return o.Phase != v1alpha1.AllocationEnded || o.JobCompleting
}

// listProviderAllocations lists the Allocations of every namespace whose
// providerConfigRef names the Provider: seen holds all of them, listed those
// that hold a slot (holdsSlot), and allocs the Allocations themselves. A
// deleting Allocation counts until its deletion completes.
func listProviderAllocations(ctx context.Context, r client.Reader, provider string) (seen, listed map[string]bool, allocs map[string]*v1alpha1.Allocation, err error) {
	var al v1alpha1.AllocationList
	if err := r.List(ctx, &al); err != nil {
		return nil, nil, nil, fmt.Errorf("list Allocations for Provider %s's submit limit: %w", provider, err)
	}
	seen, listed, allocs = map[string]bool{}, map[string]bool{}, map[string]*v1alpha1.Allocation{}
	for i := range al.Items {
		a := &al.Items[i]
		if ref := a.Spec.ProviderConfigReference; ref == nil || ref.Name != provider {
			continue
		}
		k := a.Namespace + "/" + a.Name
		seen[k] = true
		allocs[k] = a
		if holdsSlot(a) {
			listed[k] = true
		}
	}
	return seen, listed, allocs, nil
}

// slots returns the controller's slot table.
func (r *Reconciler) slots() *slotTable {
	r.slotsOnce.Do(func() { r.slotTab = newSlotTable() })
	return r.slotTab
}

// submitLimitText names a Provider's submit limit and what set it: the QOS, or
// the host-dialed cap (hostDialedLimits).
func submitLimitText(pv *providerView) string {
	limit, from := *pv.limits.MaxSubmitPerUser, pv.limits.From[v1alpha1.LimitMaxSubmitPerUser]
	if from == hostDialedLimitSource {
		return fmt.Sprintf("%s %d (%s)", v1alpha1.LimitMaxSubmitPerUser, limit, from)
	}
	return fmt.Sprintf("%s %d of QOS %s", v1alpha1.LimitMaxSubmitPerUser, limit, from)
}

// reserveSlotFor reserves a slot on the Provider for the Allocation name in
// the deployment's namespace, taking the reserved slots of takers
// (namespace/name; slotAsk.takers), and reports, from the reservation's own
// list, whether the Allocation holder (namespace/name; "" for none) still holds
// its slot. When none is free it records the Provider, its limit and the
// handovers holding reserved slots there, for SubmitLimitReached, and returns
// false. With params, the reservation also checks the Provider's per-user caps
// for an Allocation of that size (capsFit). quiet records nothing for a
// condition: a hedge's member that finds no room is extra capacity, and the
// Provider passed over sets no reason (HBEP-7, Hedging (beta)).
func (p *pass) reserveSlotFor(pv *providerView, name, holder string, takers []string, params *v1alpha1.BatchParameters, quiet bool) (ok, holds bool, err error) {
	q := slotAsk{provider: pv.name, key: p.key.Namespace + "/" + name, holder: holder, takers: takers, limit: pv.limits.MaxSubmitPerUser,
		params: params, caps: pv.limits}
	if pv.profile != nil {
		q.profile = &pv.profile.Spec
	}
	a, err := p.r.slots().reserveAsk(p.ctx, p.r.Reader, q, p.reserveTime)
	if err != nil || a.ok || quiet {
		return a.ok, a.holds, err
	}
	if a.capsWhy != "" {
		// Refused for the per-user caps, not the submit limit: no SubmitLimitReached words, and a Provider with
		// no submit limit has none to name.
		return false, a.holds, nil
	}
	if p.noSlot == nil {
		p.noSlot = map[string]string{}
	}
	p.noSlot[pv.name] = fmt.Sprintf("Provider %s has %d Allocations still holding a slot, at its %s%s", pv.name, a.count-a.reserved, submitLimitText(pv), reservedText(a.reserved, a.reservedBy))
	return false, a.holds, nil
}

// reserveTime is the clock's time, as a pass reads it: whole seconds, UTC. A
// reservation reads it when it is made, never the pass's start time (p.now).
func (p *pass) reserveTime() time.Time { return p.r.Clock.Now().UTC().Truncate(time.Second) }

// releaseSlot gives back a slot whose creation failed.
func (p *pass) releaseSlot(pv *providerView, name string) {
	p.r.slots().release(pv.name, p.key.Namespace+"/"+name)
}
