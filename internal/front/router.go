package front

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/balancer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
)

// Router holds the routing view and one balancer group per served
// ModelDeployment. It rebuilds the view after each burst of control-plane
// changes and of transport registrations (#417), at every drain start and every
// pause that ends, and pushes each group's members, eligibility, bound and caps
// into it (HBEP-5, Load bound and cap: recomputed after each burst of joins,
// leaves and changes of eligibility, and before any read that needs it).
type Router struct {
	cfg     Config
	clk     clock.Clock
	trans   Transports
	rec     Recorder
	log     *slog.Logger
	metrics *Metrics // nil: none
	lease   *leaseRef

	mu        sync.Mutex
	snap      inventory.Snapshot
	view      *inventory.View
	builtAt   time.Time
	groups    map[string]*balancer.Group
	rings     map[string]*balancer.Ring // model name -> ring over its served Instances
	pauses    map[string]*pause         // target key -> pause
	keyUID    map[string]types.UID      // an Instance's key (namespace/name) -> its UID, from the snapshot
	timer     clock.Timer
	reported  map[string]string // short profile -> resourceVersion already reported
	problems  map[string]bool   // problems already logged
	leaseEnds map[types.UID]time.Time
	mds       map[string]*v1alpha1.ModelDeployment // for events
	profiles  map[string]*v1alpha1.PlatformProfile

	// usageFailing: a usage write has failed and none has succeeded since.
	// usageNote is the event's message, from the latest failure, and
	// usageEventAt when the event last went out (zero: not yet in this run
	// of failures).
	usageFailing bool
	usageNote    string
	usageEventAt time.Time

	// beforePick, when set, runs between reading the view and picking from
	// the live groups: a seam for tests that force a join in between.
	beforePick func()

	// The coalesced rebuilds of a burst of registration changes (#417). dirty
	// says a registration changed since the last rebuild began; running, that a
	// goroutine rebuilds while it is set (drain); gen counts the rebuilds that
	// finished, and waiting the readers waiting for the lock, for the one-rebuild
	// bound (lockFresh). rebuilds counts every rebuild; afterBuild, when set,
	// runs inside one right after the view is built, and beforeStop in drain
	// after its last rebuild and before it stops: the seams of the tests.
	dirty      atomic.Bool
	running    atomic.Bool
	gen        atomic.Uint64
	waiting    atomic.Int32
	rebuilds   atomic.Int64
	afterBuild func()
	beforeStop func()
}

type pause struct {
	failures int
	until    time.Time
	// uid and epoch are the Instance the run of failures is toward and the registration epoch it was accumulated in
	// (#418): the epoch when the run began, at its first failure. A dial routed in one epoch can fail after the next has
	// begun (frps's 404 before it registers the proxy again), so the run's start, not its latest failure, says which
	// registration it was accumulated against. A managed target has neither, and its pause is never cleared by an epoch.
	uid   types.UID
	epoch uint64
}

// epochs is what the router reads of a Transports that keeps registration epochs (Tunnel, HBEP-6): the UID's current
// epoch. A Transports without them (StaticTransports) clears no pause by epoch.
type epochs interface {
	Epoch(uid types.UID) uint64
}

// NewRouter returns a router with an empty view. metrics may be nil.
func NewRouter(cfg Config, clk clock.Clock, trans Transports, rec Recorder, log *slog.Logger, metrics *Metrics) *Router {
	r := &Router{
		cfg: cfg, clk: clk, trans: trans, rec: rec, log: log, metrics: metrics, lease: &leaseRef{},
		groups: map[string]*balancer.Group{}, rings: map[string]*balancer.Ring{}, pauses: map[string]*pause{},
		reported: map[string]string{}, problems: map[string]bool{}, leaseEnds: map[types.UID]time.Time{},
	}
	// The Transports calls this on every registration change, the Tunnel's
	// probe from its verdict, synchronously: it marks the view stale and returns
	// at once, so a burst of N changes costs a rebuild or two, never N in a row
	// under the lock that routing needs (#417).
	trans.Subscribe(r.markDirty)
	return r
}

// markDirty records that a registration changed, and starts the goroutine that
// rebuilds the view unless one runs. It takes no lock.
func (r *Router) markDirty() {
	r.dirty.Store(true)
	if r.running.CompareAndSwap(false, true) {
		go r.drain()
	}
}

// drain rebuilds the view while it is stale, then stops. A change that lands
// after its last look re-starts it (markDirty), and one that lands between its
// last look and its stop is seen by the re-check, so no change is left without a
// rebuild after it. Between two rebuilds it lets every reader waiting for the
// lock go first, so a second rebuild of its own never comes ahead of a reader
// that waited for the first (lockFresh has the bound).
func (r *Router) drain() {
	for {
		for r.dirty.Load() {
			r.yieldToReaders(0)
			r.mu.Lock()
			r.syncLocked()
			r.mu.Unlock()
		}
		if r.beforeStop != nil {
			r.beforeStop()
		}
		r.running.Store(false)
		if !r.dirty.Load() || !r.running.CompareAndSwap(false, true) {
			return
		}
	}
}

// syncLocked rebuilds the view when a registration changed since the last
// rebuild began.
func (r *Router) syncLocked() {
	if r.dirty.Load() {
		r.rebuildLocked()
	}
}

// lockFresh takes the lock for a reader of the view (#417): when a rebuild
// finished while it waited for the lock, it reads that view, though changes since
// may have marked it stale; else it brings a stale view up to date itself. Its
// caller unlocks. A reader waits for one rebuild, the one in progress when it
// came, and for at most two under a barging reader: a reader R2 that comes as
// that rebuild ends, after it, can take the lock before the woken R
// (sync.Mutex), find no rebuild finished during its own wait and the view marked
// again, and rebuild it itself while R waits (the Fable review of PR #424).
func (r *Router) lockFresh() {
	gen := r.gen.Load()
	r.lockAsReader()
	if r.gen.Load() == gen {
		r.syncLocked()
	}
}

// lockAsReader takes the lock as a reader: counted in waiting while it waits,
// so that a rebuild about to start lets it go first (yieldToReaders).
func (r *Router) lockAsReader() {
	r.waiting.Add(1)
	r.mu.Lock()
	r.waiting.Add(-1)
}

// controlPlaneYield bounds how long a control-plane rebuild (Update, Refresh)
// lets waiting readers go first, so that a steady stream of requests cannot
// hold back a new snapshot. The drain's rebuilds yield without a bound: while
// they wait, each reader brings the view up to date itself.
const controlPlaneYield = 100 * time.Millisecond

// yieldToReaders waits, before a rebuild takes the lock, until no reader waits
// for it, or for max when it is not 0. A reader that came while a rebuild ran
// then takes the lock ahead of this one; without it, a rebuild that follows at
// once would take the lock first (sync.Mutex lets a running goroutine take it
// ahead of one it wakes). It governs the drain's rebuilds and the control
// plane's; a reader that rebuilds itself does not yield (lockFresh's bound).
func (r *Router) yieldToReaders(max time.Duration) {
	var waited time.Duration
	for r.waiting.Load() > 0 && (max == 0 || waited < max) {
		const step = 50 * time.Microsecond
		time.Sleep(step) // a yield to the readers, not a wait on time
		waited += step
	}
}

// Update replaces the snapshot and rebuilds the view. The watch's loop calls it
// once per burst of control-plane changes (startWatch).
func (r *Router) Update(s inventory.Snapshot) {
	r.yieldToReaders(controlPlaneYield)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snap = s
	r.rebuildLocked()
}

// Refresh rebuilds the view from the current snapshot at the current time: the
// timer's, at a drain start or a pause's end.
func (r *Router) Refresh() {
	r.yieldToReaders(controlPlaneYield)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rebuildLocked()
}

func (r *Router) hasTransport(uid types.UID) bool {
	_, ok := r.trans.Dialer(uid)
	return ok
}

// rebuildLocked rebuilds the view from the snapshot and the transports' current
// registrations. It clears dirty first, before it reads any registration: a
// change during the rebuild sets it again, and the next rebuild reads it, so the
// last registration state of a burst always reaches the view.
func (r *Router) rebuildLocked() {
	r.dirty.Store(false)
	defer func() { r.rebuilds.Add(1); r.gen.Add(1) }()
	now := r.clk.Now()
	var joined map[types.UID]bool
	prev := r.view
	if prev != nil {
		joined = prev.Joined
	}
	v := inventory.Build(r.snap, inventory.Inputs{Now: now, HasTransport: r.hasTransport, Joined: joined, MaxRequestDuration: r.cfg.MaxRequestDuration.D()})
	if r.afterBuild != nil {
		r.afterBuild()
	}
	r.view, r.builtAt = v, now
	r.indexLocked()
	for k, p := range r.pauses {
		if !p.until.IsZero() && !now.Before(p.until) {
			delete(r.pauses, k)
		}
	}
	r.clearStalePausesLocked()
	live := map[string]bool{}
	r.rings = map[string]*balancer.Ring{}
	for _, m := range v.Models {
		var all []string
		for _, d := range m.Served {
			live[d.Key] = true
			r.applyLocked(d, prev, now)
			for _, i := range d.Members {
				all = append(all, i.Key)
				r.metrics.known(i.Provider, d.Key)
			}
		}
		for _, t := range m.Managed {
			r.metrics.known(t.Provider, t.Deployment)
		}
		r.rings[m.Name] = balancer.NewRing(all)
	}
	for k, g := range r.groups {
		if !live[k] {
			g.Update(balancer.State{})
			delete(r.groups, k)
		}
	}
	r.reportShortLocked(v.ShortProfiles)
	r.logProblemsLocked(v.Problems)
	r.scheduleLocked(v.NextChange, now)
}

func (r *Router) indexLocked() {
	// From the snapshot, not the view: a failure can be reported for an
	// Instance the view does not hold as a member at that moment (one that has
	// not joined, with no transport yet), and its pause must still record it.
	r.keyUID = map[string]types.UID{}
	for i := range r.snap.Instances {
		in := &r.snap.Instances[i]
		r.keyUID[in.Namespace+"/"+in.Name] = in.UID
	}
	r.leaseEnds = map[types.UID]time.Time{}
	for i := range r.snap.Instances {
		in := &r.snap.Instances[i]
		if in.Status.LeaseEnd != nil {
			r.leaseEnds[in.UID] = in.Status.LeaseEnd.Time
		}
	}
	r.mds = map[string]*v1alpha1.ModelDeployment{}
	for i := range r.snap.Deployments {
		md := &r.snap.Deployments[i]
		r.mds[md.Namespace+"/"+md.Name] = md
	}
	r.profiles = map[string]*v1alpha1.PlatformProfile{}
	for i := range r.snap.Profiles {
		r.profiles[r.snap.Profiles[i].Name] = &r.snap.Profiles[i]
	}
}

// applyLocked pushes a deployment's state into its group and reports the
// members that left at their drain start.
func (r *Router) applyLocked(d *inventory.Deployment, prev *inventory.View, now time.Time) {
	g := r.groups[d.Key]
	if g == nil {
		g = balancer.New()
		ref := &v1alpha1.ModelDeployment{}
		ref.Namespace, ref.Name, ref.UID = d.Namespace, d.Name, d.UID
		g.OnDrift = func(total, members, open int64) { r.drift(ref, total, members, open) }
		r.groups[d.Key] = g
	}
	st, ready := r.stateLocked(d, now)
	released := g.Update(st)
	if released > 0 {
		r.log.Info("released the open requests of Instances that left the group", "deployment", d.Key, "released", released)
	}
	for _, old := range prevMembers(prev, d) {
		if !contains(st.Members, old.Key) && !now.Before(old.DrainStart) && ready < d.Desired {
			msg := fmt.Sprintf("Instance %s left routing at its drain start %s (lease end %s); %d of %d desired Instances are eligible",
				old.Key, old.DrainStart.UTC().Format(time.RFC3339), old.LeaseEnd.UTC().Format(time.RFC3339), ready, d.Desired)
			r.log.Warn(msg, "deployment", d.Key)
			if md := r.mds[d.Key]; md != nil {
				r.rec.Event(md, true, eventDegraded, reasonCapacityDrop, msg)
			}
		}
	}
}

// stateLocked is the deployment's group state: its routable Instances, which
// are eligible now (the control plane's part and no pause), and the adaptive
// bound (HBEP-5, open question 3, option 5): while the eligible Instances
// meet spec.instances, the tight bound and each Instance's cap; below, the
// loose bound and no cap.
func (r *Router) stateLocked(d *inventory.Deployment, now time.Time) (balancer.State, int) {
	st := balancer.State{Eligible: map[string]bool{}, Attrs: map[string]balancer.Attrs{}}
	ready := 0
	for _, i := range d.Members {
		st.Members = append(st.Members, i.Key)
		st.Attrs[i.Key] = balancer.Attrs{Provider: i.Provider, DisplayID: i.DisplayID}
		if i.Eligible && !r.pausedLocked(i.Key, now) {
			st.Eligible[i.Key] = true
			ready++
		}
	}
	if ready >= d.Desired {
		st.Percent = d.TightPercent
		st.Caps = map[string]int{}
		for _, i := range d.Members {
			st.Caps[i.Key] = i.Cap
		}
	} else {
		st.Percent = d.LoosePercent
	}
	return st, ready
}

// clearStalePausesLocked clears each pause, or run of failures, that was accumulated in an earlier registration epoch
// of its Instance than the one its transport is registered in now (#418; the science agent's ruling (a) of 11 Nov 2026;
// HBEP-5, Pauses). A registration in the current epoch is made only by a probe that passed in it (Tunnel.Bump takes the
// registration away, and setRegisteredAt registers a pass only in the epoch it began in), so a registered transport in a
// later epoch is a re-registration whose first probe passed there. A pause from the current epoch is untouched, and so
// is one whose Instance registered again and has not passed a probe since: it has no registration. The router runs this
// at every rebuild, which every registration change triggers (Transports.Subscribe).
func (r *Router) clearStalePausesLocked() {
	ep, ok := r.trans.(epochs)
	if !ok {
		return
	}
	for k, p := range r.pauses {
		if p.uid == "" || ep.Epoch(p.uid) <= p.epoch || !r.hasTransport(p.uid) {
			continue
		}
		paused := !p.until.IsZero()
		delete(r.pauses, k)
		r.log.Info("a pause from an earlier registration is cleared: the Instance registered again, and a probe passed in the new registration",
			"target", k, "paused", paused, "failures", p.failures)
	}
}

func (r *Router) pausedLocked(key string, now time.Time) bool {
	p := r.pauses[key]
	return p != nil && now.Before(p.until)
}

// ReportConnect records the outcome of opening a connection to a target. A
// target whose connections fail pauseAfterFailures times in a row is not
// eligible for pauseDuration, then is again; an Instance keeps its count
// meanwhile, and nothing removes a target for good (HBEP-5, Pauses; L-4.14).
func (r *Router) ReportConnect(key string, failed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.pauses[key]
	if !failed {
		if p != nil && p.until.IsZero() {
			delete(r.pauses, key)
		}
		return
	}
	if p == nil {
		p = &pause{uid: r.keyUID[key]}
		if ep, ok := r.trans.(epochs); ok && p.uid != "" {
			p.epoch = ep.Epoch(p.uid)
		}
		r.pauses[key] = p
	}
	p.failures++
	if p.failures < r.cfg.PauseAfterFailures {
		return
	}
	now := r.clk.Now()
	p.failures, p.until = 0, now.Add(r.cfg.PauseDuration.D())
	r.log.Warn("target paused after failed connections", "target", key, "until", p.until.UTC().Format(time.RFC3339))
	r.metrics.paused(r.targetLabelsLocked(key))
	r.rebuildLocked()
}

// failures is the target's count of failed connections in a row since its
// last success or pause.
func (r *Router) failures(key string) int {
	r.lockFresh()
	defer r.mu.Unlock()
	if p := r.pauses[key]; p != nil {
		return p.failures
	}
	return 0
}

// targetLabelsLocked returns the provider and the ModelDeployment of a target
// by its key, for a metric's labels; both are empty when the view no longer
// holds it.
func (r *Router) targetLabelsLocked(key string) (provider, deployment string) {
	if r.view == nil {
		return "", ""
	}
	for _, m := range r.view.Models {
		for _, d := range m.Served {
			for _, i := range d.Members {
				if i.Key == key {
					return i.Provider, d.Key
				}
			}
		}
		for _, t := range m.Managed {
			if t.Key == key {
				return t.Provider, t.Deployment
			}
		}
	}
	return "", ""
}

// Paused reports whether a target is paused now.
func (r *Router) Paused(key string) bool {
	r.lockFresh()
	defer r.mu.Unlock()
	return r.pausedLocked(key, r.clk.Now())
}

// scheduleLocked arms one timer for the next drain start or pause end.
func (r *Router) scheduleLocked(next, now time.Time) {
	for _, p := range r.pauses {
		if p.until.After(now) && (next.IsZero() || p.until.Before(next)) {
			next = p.until
		}
	}
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	if next.IsZero() {
		return
	}
	r.timer = r.clk.AfterFunc(next.Sub(now), r.Refresh)
}

// LeaseEnd returns an Instance's current lease end from the control plane,
// for a request in flight on it: a later hard end, once observed, moves it.
func (r *Router) LeaseEnd(uid types.UID) (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.leaseEnds[uid]
	return t, ok
}

// usageEventEvery is how often a UsageWriteFailed event repeats while the
// failure lasts.
const usageEventEvery = time.Hour

// usageFailed notes a usage failure and raises its event when it can. The
// event regards the front's Lease (HBEP-8, Events), so it waits until the
// front holds the Lease: until then the failure is counted by the sink,
// logged and shown in /healthz, and nothing else (@hpc-burst-science-agent,
// 31 Oct 2026; no fallback to a ModelDeployment).
func (r *Router) usageFailed(requestID string, err error) {
	r.mu.Lock()
	r.usageFailing = true
	r.usageNote = "usage records are not being written (request " + requestID + "): " + err.Error()
	r.mu.Unlock()
	r.flushUsageEvent()
}

// flushUsageEvent raises the UsageWriteFailed event of a failure that lasts,
// if the front holds its Lease and the event has not gone out in the last
// hour: a repeating event, since client-go rarely merges events whose objects
// changed (@hpc-burst-science-agent, 31 Oct 2026). The Lease keeper calls it
// after every beat that holds the Lease, so a failure that began before the
// Lease existed has its event as soon as the Lease does, and one that lasts
// has it hourly with no new request.
func (r *Router) flushUsageEvent() {
	if !r.lease.held() {
		return
	}
	r.mu.Lock()
	now := r.clk.Now()
	emit := r.usageFailing && (r.usageEventAt.IsZero() || !now.Before(r.usageEventAt.Add(usageEventEvery)))
	note := r.usageNote
	if emit {
		r.usageEventAt = now
	}
	r.mu.Unlock()
	if emit {
		r.rec.Event(r.lease.object(), true, eventFailing, reasonUsageWriteFailed, note)
	}
}

// usageWritten ends a usage failure: the next one raises its event at once.
func (r *Router) usageWritten() {
	r.mu.Lock()
	r.usageFailing = false
	r.usageEventAt = time.Time{}
	r.mu.Unlock()
}

// drift runs under the group's lock, possibly inside rebuildLocked, so it
// takes no router lock: the reference was captured when the group was made.
func (r *Router) drift(md *v1alpha1.ModelDeployment, total, members, open int64) {
	msg := fmt.Sprintf("balancer counts disagree for %s/%s: KubeAI total %d, members %d, open %d", md.Namespace, md.Name, total, members, open)
	r.log.Error(msg)
	r.rec.Event(md, true, eventDegraded, reasonCountsDrift, msg)
}

func (r *Router) reportShortLocked(short []inventory.ShortProfile) {
	for _, s := range short {
		if r.reported[s.Name] == s.ResourceVersion && s.ResourceVersion != "" {
			continue
		}
		r.reported[s.Name] = s.ResourceVersion
		msg := fmt.Sprintf("PlatformProfile %s has drainWindow %s, below the front's maxRequestDuration plus its drainMargin (%s): long requests on its Instances can be cut at the lease end",
			s.Name, s.DrainWindow, s.Required)
		r.log.Warn(msg)
		if p := r.profiles[s.Name]; p != nil {
			r.rec.Event(p, true, eventDegraded, reasonDrainWinShort, msg)
		}
	}
}

func (r *Router) logProblemsLocked(problems []string) {
	for _, p := range problems {
		if !r.problems[p] {
			r.problems[p] = true
			r.log.Warn(p)
		}
	}
}

// Status is the router's state for the health endpoint.
type Status struct {
	BuiltAt     time.Time      `json:"builtAt"`
	Deployments map[string]int `json:"deployments"` // routable Instances per served deployment
	Managed     int            `json:"managedTargets"`
	Waiting     int            `json:"waiting"`
	// Instances counts the Instances the watch holds, and LeaseEnds those
	// with status.leaseEnd, so a check can see the watch read the status.
	Instances int `json:"instances"`
	LeaseEnds int `json:"leaseEnds"`
}

// Status returns a summary of the view.
func (r *Router) Status() Status {
	r.lockFresh()
	defer r.mu.Unlock()
	s := Status{BuiltAt: r.builtAt, Deployments: map[string]int{}, Instances: len(r.snap.Instances), LeaseEnds: len(r.leaseEnds)}
	if r.view == nil {
		return s
	}
	for _, m := range r.view.Models {
		for _, d := range m.Served {
			s.Deployments[d.Key] = len(d.Members)
			if g := r.groups[d.Key]; g != nil {
				s.Waiting += g.Waiting()
			}
		}
		s.Managed += len(m.Managed)
	}
	return s
}

func prevMembers(prev *inventory.View, d *inventory.Deployment) []*inventory.Instance {
	if prev == nil {
		return nil
	}
	for _, m := range prev.Models {
		for _, pd := range m.Served {
			if pd.Key == d.Key {
				return pd.Members
			}
		}
	}
	return nil
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
