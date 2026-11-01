package lifecycle

import (
	"context"
	"fmt"
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
// is Ended or its deletion completes, so Submitting, SubmissionUnknown, held,
// pending, running, Failed, Lost, Refused and deleting ones all count.
//
// Before it creates an Allocation, a worker reserves a slot under the
// Provider's lock, which every worker takes. The count it checks is the listed
// Allocations of the Provider that have not ended plus this controller's
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

// expectationsTimeout is ExpectationsTimeout of Kubernetes' controller_utils.go
// (v1.35.0, line 72): an expectation not observed within it lapses.
const expectationsTimeout = 5 * time.Minute

// listTimeout is how long the List under a Provider's lock may take.
const listTimeout = 30 * time.Second

// slotTable holds the per-Provider locks and the expectations.
//
// Lock order (internal/lifecycle): a Provider's lock (slotLock) is taken with
// no other lock of this package held, and only one Provider's lock at a time.
// While it is held, slotTable.mu may be taken, as a leaf, to read or change the
// expectations and the lock map; nothing else of this package (memory.mu,
// Backend.mu, the identity cache, the Hub cache, the metrics views, the
// secrets sweep) is taken while either is held, and neither is taken while one
// of those is held. The Provider's lock is held across one List call to the
// API, bounded by listTimeout, and no other call.
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
}

func newSlotTable() *slotTable {
	return &slotTable{listTimeout: listTimeout, locks: map[string]*sync.Mutex{}, expect: map[string]map[string]time.Time{}}
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
		}
	}
	return slotCount{listed: listed, expected: len(exp)}
}

// reserve reserves a slot on the Provider for the Allocation key
// (namespace/name) under the limit: it returns true when the Allocation
// already holds one or a slot is free, and then counts it until the list shows
// it or the reservation is released. With no limit it reserves nothing and
// returns true. count is the Provider's count before this reservation.
//
// now is read once the lock is held and the list is back, and that one time
// both expires the old expectations and stamps the new one. A time taken when
// the pass began could be more than expectationsTimeout old by then (a slow
// identity resolution or Hub call before the reservation), and a reservation
// stamped with it would lapse at once: another worker could then take the same
// slot before this one's Allocation is created (review of PR 171, Astra B1).
func (t *slotTable) reserve(ctx context.Context, r client.Reader, provider, key string, limit *int32, now func() time.Time) (ok bool, count int, err error) {
	if limit == nil {
		return true, 0, nil
	}
	l := t.slotLock(provider)
	l.Lock()
	defer l.Unlock()
	seen, listed, err := t.listBounded(ctx, r, provider)
	if err != nil {
		return false, 0, err
	}
	at := now()
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.observe(provider, seen, listed, at)
	if t.holds(provider, key, c) {
		return true, c.total(), nil
	}
	if c.total() >= int(*limit) {
		return false, c.total(), nil
	}
	if t.expect[provider] == nil {
		t.expect[provider] = map[string]time.Time{}
	}
	t.expect[provider][key] = at
	return true, c.total(), nil
}

// release gives back the slot reserved for the Allocation key: its creation
// failed. A reservation the list already shows is not affected, since it is
// counted as listed.
func (t *slotTable) release(provider, key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.expect[provider], key)
}

// count returns the Provider's count against its submit limit now, excluding
// the Allocations in skip (namespace/name), without reserving. It is for the
// no-overlap path of PR 3b, which asks whether a successor could run beside
// its predecessor; this PR's tests exercise it on the table alone.
func (t *slotTable) count(ctx context.Context, r client.Reader, provider string, now time.Time, skip ...string) (int, error) {
	l := t.slotLock(provider)
	l.Lock()
	defer l.Unlock()
	seen, listed, err := t.listBounded(ctx, r, provider)
	if err != nil {
		return 0, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.observe(provider, seen, listed, now)
	n := c.total()
	for _, k := range skip {
		if t.holds(provider, k, c) {
			n--
		}
	}
	return n, nil
}

// listBounded is listProviderAllocations with the table's timeout.
func (t *slotTable) listBounded(ctx context.Context, r client.Reader, provider string) (seen, listed map[string]bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, t.listTimeout)
	defer cancel()
	return listProviderAllocations(ctx, r, provider)
}

// listProviderAllocations lists the Allocations of every namespace whose
// providerConfigRef names the Provider: seen holds all of them, listed those
// whose phase is not Ended. A deleting Allocation counts until its deletion
// completes.
func listProviderAllocations(ctx context.Context, r client.Reader, provider string) (seen, listed map[string]bool, err error) {
	var al v1alpha1.AllocationList
	if err := r.List(ctx, &al); err != nil {
		return nil, nil, fmt.Errorf("list Allocations for Provider %s's submit limit: %w", provider, err)
	}
	seen, listed = map[string]bool{}, map[string]bool{}
	for i := range al.Items {
		a := &al.Items[i]
		if ref := a.Spec.ProviderConfigReference; ref == nil || ref.Name != provider {
			continue
		}
		k := a.Namespace + "/" + a.Name
		seen[k] = true
		if a.Status.AtProvider.Phase != v1alpha1.AllocationEnded {
			listed[k] = true
		}
	}
	return seen, listed, nil
}

// slots returns the controller's slot table.
func (r *Reconciler) slots() *slotTable {
	r.slotsOnce.Do(func() { r.slotTab = newSlotTable() })
	return r.slotTab
}

// submitLimitText names a Provider's submit limit and the QOS that set it.
func submitLimitText(pv *providerView) string {
	return fmt.Sprintf("%s %d of QOS %s", v1alpha1.LimitMaxSubmitPerUser, *pv.limits.MaxSubmitPerUser, pv.limits.From[v1alpha1.LimitMaxSubmitPerUser])
}

// reserveSlot reserves a slot on the Provider for the Allocation name in the
// deployment's namespace. When none is free it records the Provider and its
// limit for SubmitLimitReached and returns false.
func (p *pass) reserveSlot(pv *providerView, name string) (bool, error) {
	ok, n, err := p.r.slots().reserve(p.ctx, p.r.Reader, pv.name, p.key.Namespace+"/"+name, pv.limits.MaxSubmitPerUser, p.reserveTime)
	if err != nil || ok {
		return ok, err
	}
	if p.noSlot == nil {
		p.noSlot = map[string]string{}
	}
	p.noSlot[pv.name] = fmt.Sprintf("Provider %s has %d Allocations not ended, at its %s", pv.name, n, submitLimitText(pv))
	return false, nil
}

// reserveTime is the clock's time, as a pass reads it: whole seconds, UTC. A
// reservation reads it when it is made, never the pass's start time (p.now).
func (p *pass) reserveTime() time.Time { return p.r.Clock.Now().UTC().Truncate(time.Second) }

// releaseSlot gives back a slot whose creation failed.
func (p *pass) releaseSlot(pv *providerView, name string) {
	p.r.slots().release(pv.name, p.key.Namespace+"/"+name)
}
