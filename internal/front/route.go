package front

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/balancer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/naming"
)

// target is where one request goes: one of our Instances with its
// reservation, or a managed catalog entry.
type target struct {
	inst    *inventory.Instance
	dep     *inventory.Deployment
	res     *balancer.Reservation
	managed *inventory.ManagedTarget
	// mres is the managed target's reservation: it carries the Workload's count for a request that names one, and the in-flight series for every
	// request (managedRes).
	mres *managedRes
	// workload is the name of the Workload the request resolved to, "" when it
	// named none (HBEP-5, Workloads); the usage record carries it.
	workload string
	// wres is that resolution, which the request's later choices reuse (Resolution); nil when it named none.
	wres *Resolution
	// pin is the request's hold on its conversation's pin, set on the target of the first choice of a request of a Workload with sessionAffinity Route and
	// ended once, when the request ends (Proxy.forward); nil for every other target.
	pin *pinRef
}

// Resolution is what a request's first choice resolved its Workload to: the Workload's namespace and name, and the ModelDeployments, by namespace and
// name, that served it then (HBEP-5, Workloads, Who serves it). Every later choice of the same request, a retry's or a resend's, reuses it and never
// looks the name up again, so a Workload deleted and another of the same name created in a different namespace between two attempts cannot move
// the request to that namespace's deployments (the review of #778, finding 1). A deployment of the set that has gone since is skipped like any other
// target that is not eligible; one that joined the Workload's namespace since is not in the set.
type Resolution struct {
	Namespace, Name string
	// uid is the resolved Workload object's UID: the front's state of a Workload (its managed count and FIFO) is kept per object, so a Workload deleted and
	// created again under the same name starts with none of the deleted one's.
	uid     types.UID
	serving map[string]bool // namespace/name of each ModelDeployment of the serving set, served or managed-source
	// charge is the Workload's c when it resolved: its typical context plus its typical output, which every attempt of the request reserves against
	// an Instance's token budget (HBEP-5, Workloads, The count in tokens). A request keeps the definition it came with, so an edit of the Workload
	// changes no request already resolved (the science agent's Q15 ruling on #764).
	charge int64
	// managedFirst and managedMax are the Workload's route order and managedSessions as it had them when the request resolved: a request keeps the
	// definition it came with, whatever an edit does meanwhile (the science agent's Q15 ruling). managedFirst is set only with a managedSessions above
	// 0, which admission requires of ManagedFirst; a ManagedFirst Workload with none, which no admitted object is, is routed as OwnFirst. managedMax 0
	// is no bound on the overflow.
	managedFirst bool
	managedMax   int
	// sticky is sessionAffinity Route; pinTTL and maxPins are as the Workload had them when the request resolved. pin is the conversation's pin, set once
	// by the request's first choice (pins.go, Router.chooseSticky) before any later choice and never changed: every attempt of the request then routes to
	// its target alone, the first request's retries and resends included.
	sticky  bool
	pinTTL  time.Duration
	maxPins int
	pin     *pinTarget
}

// load is the charge a choice reserves for a request with this resolution: the Workload's c, named, or the zero value for a request that named none.
func (res *Resolution) load() balancer.Charge {
	if res == nil {
		return balancer.Charge{}
	}
	return balancer.Charge{Named: true, Tokens: res.charge, Workload: res.Name}
}

// key is the target's name for pauses and exclusion.
func (t *target) key() string {
	if t.managed != nil {
		return t.managed.Key
	}
	return t.inst.Key
}

// release ends the target's reservation, an Instance's or a managed target's; safe to call more than once.
func (t *target) release() {
	if t == nil {
		return
	}
	if t.res != nil {
		t.res.Release()
	}
	t.mres.release()
}

// routeError is a row of HBEP-5's Errors table raised while choosing.
type routeError struct {
	status     int
	code       string
	detail     string
	message    string
	retryAfter string
	dep        *inventory.Deployment // the ModelDeployment named by a capacity error
	cutBy      string                // the front's cut, for a request cut while it waited
	// workload is the Workload the request had resolved to when it was refused,
	// "" when it named none or the refusal came before one resolved: the three
	// Workload 400s never carry one.
	workload string
}

// forWorkload sets the Workload the refused request had resolved to.
func (e *routeError) forWorkload(name string) *routeError {
	e.workload = name
	return e
}

func errUnknownProvider(p string) *routeError {
	return &routeError{status: http.StatusBadRequest, code: codeInvalidModelName, detail: "unknown_provider", message: "the model name names an unknown provider: " + echoedName(p)}
}

// The three 400s of a request that names a Workload wrongly (HBEP-5, Errors; Workloads, Naming it). Each is a refusal, never a fallback to no
// Workload.
func errUnknownWorkload(model, name string) *routeError {
	return &routeError{status: http.StatusBadRequest, code: codeInvalidModelName, detail: "unknown_workload",
		message: "the model " + echoedName(model) + " has no Workload named " + echoedName(name)}
}

func errWorkloadAmbiguous(model, name string, keys []string) *routeError {
	return &routeError{status: http.StatusBadRequest, code: codeInvalidModelName, detail: "workload_ambiguous",
		message: "the model " + echoedName(model) + " has a Workload named " + echoedName(name) + " in more than one namespace: " + strings.Join(keys, ", ")}
}

func errWorkloadConflict(header, bySuffix, byHeader string) *routeError {
	return &routeError{status: http.StatusBadRequest, code: codeInvalidModelName, detail: "workload_conflict",
		message: "the model name's #" + echoedName(bySuffix) + " and the " + echoedName(header) + " header name different Workloads: " + echoedName(byHeader)}
}

// The rows of a sticky Workload (HBEP-5, Errors; Workloads, Sticky routes). A pin is never moved, so a request that can't be placed under its pin is
// refused, never sent elsewhere.
func errSessionHeaderRequired(workload string) *routeError {
	return (&routeError{status: http.StatusBadRequest, code: codeInvalidRequest, detail: "session_header_required",
		message: "the Workload " + echoedName(workload) + " pins each conversation to one target, so its requests must carry the session header"}).forWorkload(workload)
}

func errPinsUnavailable(workload string) *routeError {
	return (&routeError{status: http.StatusServiceUnavailable, code: codeCapacity, detail: "pins_unavailable", retryAfter: "10",
		message: "the pin journal can't be read or written, so the request is not sent: a pinned conversation is never moved"}).forWorkload(workload)
}

func errPinsFull(workload string) *routeError {
	return (&routeError{status: http.StatusServiceUnavailable, code: codeCapacity, detail: "pins_full", retryAfter: "10",
		message: "the Workload " + echoedName(workload) + " holds its maxPins live pins: a new conversation is not pinned until one expires"}).forWorkload(workload)
}

func errPinnedGone(model, workload string) *routeError {
	return (&routeError{status: http.StatusServiceUnavailable, code: codeModelInactive, detail: "pinned_target_gone",
		message: "the conversation's pinned target of the model " + echoedName(model) + " is gone, and a pinned conversation is never moved: it takes a target again when its pin expires"}).forWorkload(workload)
}

func errNotAttached(model string) *routeError {
	return &routeError{status: http.StatusServiceUnavailable, code: codeModelInactive, detail: "not_attached", retryAfter: "30",
		message: "the model " + echoedName(model) + " is not active: no provider is attached to it"}
}

func errNoReadyTarget(model string) *routeError {
	return &routeError{status: http.StatusServiceUnavailable, code: codeModelInactive, detail: "no_ready_target", retryAfter: "30",
		message: "the model " + echoedName(model) + " has no ready target at the requested level"}
}

// choose resolves a parsed name to a target (HBEP-5, Request path steps 3 to
// 6). exclude are the targets the request already failed on, for its resend
// and its retry (open question 5). It never widens to a broader level. It is the
// request's first choice: a Workload it names is looked up here.
func (r *Router) choose(ctx context.Context, n naming.Name, key string, exclude []string) (*target, *routeError) {
	return r.chooseAmong(ctx, n, nil, key, exclude, true)
}

// chooseInstance is choose among Instances only, never a managed provider: a
// request already written to an Instance is sent again only to another
// Instance (HBEP-5, open question 5: "to another ready Instance"), never run
// again on LLMLab.
func (r *Router) chooseInstance(ctx context.Context, n naming.Name, key string, exclude []string) (*target, *routeError) {
	return r.chooseAmong(ctx, n, nil, key, exclude, false)
}

// chooseAgain is choose for a later choice of a request: res is the resolution its first choice made (target.res), nil for a request that named no
// Workload. A named Workload is not looked up again.
func (r *Router) chooseAgain(ctx context.Context, n naming.Name, res *Resolution, key string, exclude []string) (*target, *routeError) {
	return r.chooseAmong(ctx, n, res, key, exclude, true)
}

// chooseInstanceAgain is chooseInstance for a later choice of a request.
func (r *Router) chooseInstanceAgain(ctx context.Context, n naming.Name, res *Resolution, key string, exclude []string) (*target, *routeError) {
	return r.chooseAmong(ctx, n, res, key, exclude, false)
}

// chooseAmong chooses a target. res is nil for a request's first choice, which looks up the Workload the name or the header names; it is the first
// choice's Resolution for every later one, which then reuses it. A request that names no Workload takes the ModelDeployments' group queue when it must
// wait, as before; one that does takes its Workload's own FIFO and, toward a managed target, its Workload's count (workload_route.go).
func (r *Router) chooseAmong(ctx context.Context, n naming.Name, res *Resolution, key string, exclude []string, withManaged bool) (*target, *routeError) {
	if res == nil {
		p, rerr := r.plan(n, nil, key, exclude, withManaged)
		if rerr != nil {
			return nil, rerr
		}
		if p.res == nil {
			if r.beforePick != nil {
				r.beforePick()
			}
			return r.chooseUnnamed(ctx, p)
		}
		if p.res.sticky {
			return r.chooseSticky(ctx, n, p.res, key, exclude, withManaged, p.noServing)
		}
		res = p.res
	}
	return r.chooseNamed(ctx, n, res, key, exclude, withManaged)
}

// plan is what one attempt to place a request reads from the view: the Resolution of its Workload (nil when it names none), the ModelDeployments that
// may serve it with their groups, the model's ring, and the managed targets that are eligible for it now.
type plan struct {
	n           naming.Name
	key         string
	exclude     []string
	withManaged bool
	res         *Resolution
	serving     []*inventory.Deployment
	ring        *balancer.Ring
	groups      map[string]*balancer.Group
	managed     []*inventory.ManagedTarget
	wname       string
	// noServing is the first choice of a request of a sticky Workload that no deployment serves now: not an answer yet, since the conversation may hold a
	// pin, and a pin is not moved by an edit of spec.workloads (HBEP-5, Sticky routes). chooseSticky answers not_attached when it has none.
	noServing bool
	// pinID is the full deployment ID that a pinned conversation's Instances must have, "" when it is not pinned to a ModelDeployment. pinWait says the
	// pinned target exists and may take the request later: paused, verifying, unreachable, or in a handover's gap, it is waited for, never given up on.
	pinID   string
	pinWait bool
}

// match is the filter on a deployment's members for this attempt: its level and exclusions, and for a conversation pinned to a ModelDeployment, the pinned
// deployment ID. The ID is read from the view's own Instances, so a member that joined since is judged by its display form.
func (p *plan) match() balancer.Match {
	base := levelMatch(p.n, p.exclude)
	if p.pinID == "" {
		return base
	}
	ids := map[string]string{}
	for _, d := range p.serving {
		for _, i := range d.Members {
			ids[i.Key] = i.DeploymentID
		}
	}
	return func(member string, a balancer.Attrs) bool {
		if !base(member, a) {
			return false
		}
		if full, ok := ids[member]; ok {
			return full == p.pinID
		}
		return len(p.pinID) >= len(a.DisplayID) && a.DisplayID != "" && p.pinID[:len(a.DisplayID)] == a.DisplayID
	}
}

// plan reads the view for one attempt. A request waits for one rebuild, two at most under a barging reader (#417; lockFresh). The view it routes on
// may be a rebuild behind the transports: a target whose registration was taken away since has no dialer when the proxy asks for one, and a dialer it
// holds carries the epoch it was handed in, which the dial checks after frps's 200 and before any byte (Tunnel.Dialer, Tunnel.dial; HBEP-5,
// Reachability). So no route is served from a view older than its epoch. res is nil for a request's first choice, which looks up the Workload the name
// or the header names; it is that choice's Resolution for every later attempt, a retry's, a resend's or a waiter's, which reuses it.
func (r *Router) plan(n naming.Name, res *Resolution, key string, exclude []string, withManaged bool) (*plan, *routeError) {
	r.lockFresh()
	v := r.view
	if v == nil {
		r.mu.Unlock()
		return nil, errNoReadyTarget(n.Model)
	}
	if n.Provider != "" && v.Providers[n.Provider] == nil {
		r.mu.Unlock()
		return nil, errUnknownProvider(n.Provider)
	}
	// The Workload the request names, if it names one, is found before the model's attachment is read: the errors come in HBEP-5's order,
	// grammar, unknown_provider, the Workload's three, then not_attached and no_ready_target (the science agent's ruling on #764, Q9). It is found
	// once, by the first choice; a later one carries the first's resolution.
	fresh := false
	var wl *inventory.Workload
	if res == nil && (n.Workload != "" || n.HeaderWorkload != "") {
		var rerr *routeError
		if wl, rerr = resolveWorkload(v, n, r.cfg.WorkloadHeader); rerr != nil {
			r.mu.Unlock()
			return nil, rerr
		}
		if wl.Sticky && !n.KeyFromHeader {
			// A Workload that pins each conversation needs a key that holds still: the opening hash can change within a run (HBEP-5, Sticky routes).
			r.mu.Unlock()
			return nil, errSessionHeaderRequired(wl.Name)
		}
		fresh = true
	}
	m := v.Models[n.Model]
	if res != nil && res.pin != nil {
		return r.planPinned(m, n, res, key, exclude, withManaged)
	}
	var serving []*inventory.Deployment
	var sources []*inventory.ManagedSource
	switch {
	case fresh && m != nil:
		serving, sources = servingSet(m, wl)
	case res != nil && m != nil:
		serving, sources = savedServingSet(m, res)
	case m != nil:
		serving, sources = m.Served, m.ManagedSources
	}
	if fresh {
		res = newResolution(wl, serving, sources)
	}
	wname := ""
	if res != nil {
		wname = res.Name
	}
	empty := len(serving)+len(sources) == 0
	switch {
	case m == nil && res == nil:
		r.mu.Unlock()
		return nil, errNotAttached(n.Model)
	case fresh && empty && res.sticky:
		// Not an answer yet: the conversation may hold a pin, and an edit of spec.workloads moves none (chooseSticky answers it).
		r.mu.Unlock()
		return &plan{n: n, key: key, exclude: exclude, withManaged: withManaged, res: res, wname: wname, noServing: true}, nil
	case fresh && empty:
		// The Workload resolved, and no deployment serves it (or its model has none): not_attached, as HBEP-5's Errors table says.
		r.mu.Unlock()
		return nil, errNotAttached(n.Model).forWorkload(wname)
	case res != nil && !fresh && empty:
		// A later choice of a request whose serving set has gone: nothing is eligible, as for any other target that is not.
		r.mu.Unlock()
		return nil, errNoReadyTarget(n.Model).forWorkload(wname)
	}
	ring := r.rings[m.Name]
	groups := make(map[string]*balancer.Group, len(serving))
	for _, d := range serving {
		groups[d.Key] = r.groups[d.Key]
	}
	now := r.clk.Now()
	var managed []*inventory.ManagedTarget
	for _, t := range m.Managed {
		if withManaged && (res == nil || res.serving[t.Deployment]) && levelMatches(n, t.Provider, t.DisplayID) && !slices.Contains(exclude, t.Key) && !r.pausedLocked(t.Key, now) {
			managed = append(managed, t)
		}
	}
	r.mu.Unlock()
	return &plan{n: n, key: key, exclude: exclude, withManaged: withManaged, res: res, serving: serving, ring: ring, groups: groups, managed: managed, wname: wname}, nil
}

// planPinned is plan for a request whose conversation holds a pin: the pinned target alone, looked up in the model's whole list of ModelDeployments, so an
// edit of spec.workloads moves nothing (HBEP-5, Sticky routes). It is called with Router.mu held and releases it. A target that's gone is the ModelDeployment
// deleted, a managed target with no entry listed or no managed-source ModelDeployment, or no current deployment ID of the ModelDeployment equal to the
// pinned one, as after a finished rollout. A ModelDeployment whose status lists no current ID yet is not read as gone: nothing says it no longer is current.
// Paused, verifying, unreachable, full or in a no-overlap handover's gap, the target is waited for (plan.pinWait).
func (r *Router) planPinned(m *inventory.Model, n naming.Name, res *Resolution, key string, exclude []string, withManaged bool) (*plan, *routeError) {
	pn := res.pin
	p := &plan{n: n, key: key, exclude: exclude, withManaged: withManaged, res: res, wname: res.Name}
	if pn.managed() {
		listed := false
		now := r.clk.Now()
		if m != nil {
			for _, t := range m.Managed {
				if t.Provider != pn.Provider || t.ManagedID != pn.ManagedID {
					continue
				}
				listed = true
				if withManaged && levelMatches(n, t.Provider, t.DisplayID) && !slices.Contains(exclude, t.Key) {
					p.pinWait = true // the target exists and may take the request: paused or not
					if !r.pausedLocked(t.Key, now) {
						p.managed = append(p.managed, t)
					}
				}
			}
		}
		r.mu.Unlock()
		if !listed {
			return nil, errPinnedGone(n.Model, res.Name)
		}
		return p, nil
	}
	var d *inventory.Deployment
	if m != nil {
		for _, c := range m.Served {
			if c.UID == pn.MD && (len(c.CurrentIDs) == 0 || slices.Contains(c.CurrentIDs, pn.ID)) {
				d = c
			}
		}
	}
	if d == nil {
		r.mu.Unlock()
		return nil, errPinnedGone(n.Model, res.Name)
	}
	any := false
	for _, i := range d.Members {
		if i.DeploymentID != pn.ID {
			continue
		}
		any = true
		if levelMatches(n, i.Provider, i.DisplayID) && !slices.Contains(exclude, i.Key) {
			p.pinWait = true
		}
	}
	p.pinWait = p.pinWait || !any // no Instance of the ID at all: a handover's gap
	p.pinID, p.serving, p.ring, p.groups = pn.ID, []*inventory.Deployment{d}, r.rings[m.Name], map[string]*balancer.Group{d.Key: r.groups[d.Key]}
	r.mu.Unlock()
	return p, nil
}

// chooseUnnamed places a request that names no Workload: our own ModelDeployments, then an eligible managed target, then the group queue of the
// first deployment on the walk that has an eligible Instance (HBEP-5, Request path step 5). It is unchanged by the Workload build.
func (r *Router) chooseUnnamed(ctx context.Context, p *plan) (*target, *routeError) {
	tg, wait := r.chooseServed(p.serving, p.ring, p.groups, p.key, levelMatch(p.n, p.exclude), balancer.Charge{})
	if tg != nil {
		return tg, nil
	}
	if len(p.managed) > 0 {
		return r.managedTarget(nil, p), nil
	}
	if wait == nil {
		return nil, errNoReadyTarget(p.n.Model)
	}
	return r.waitServed(ctx, wait, p.groups[wait.Key], p.n, p.key, p.exclude, balancer.Charge{})
}

// servingSet is the ModelDeployments a request may resolve to: all of the
// model's when it names no Workload, else those in the Workload's namespace that
// list it or list none, served and managed-source alike (HBEP-5, Workloads, Who
// serves it).
func servingSet(m *inventory.Model, wl *inventory.Workload) ([]*inventory.Deployment, []*inventory.ManagedSource) {
	if wl == nil {
		return m.Served, m.ManagedSources
	}
	var served []*inventory.Deployment
	for _, d := range m.Served {
		if d.Serves(wl) {
			served = append(served, d)
		}
	}
	var sources []*inventory.ManagedSource
	for _, s := range m.ManagedSources {
		if s.Serves(wl) {
			sources = append(sources, s)
		}
	}
	return served, sources
}

// newResolution records the serving set a Workload resolved to.
func newResolution(wl *inventory.Workload, served []*inventory.Deployment, sources []*inventory.ManagedSource) *Resolution {
	res := &Resolution{Namespace: wl.Namespace, Name: wl.Name, uid: wl.UID, serving: map[string]bool{}, charge: wl.Charge,
		managedFirst: wl.ManagedFirst && wl.ManagedSessions > 0, managedMax: wl.ManagedSessions,
		sticky: wl.Sticky, pinTTL: wl.PinTTL, maxPins: wl.MaxPins}
	for _, d := range served {
		res.serving[d.Key] = true
	}
	for _, s := range sources {
		res.serving[s.Key] = true
	}
	return res
}

// savedServingSet is the part of the model's ModelDeployments, in the view now, that a Resolution's serving set holds: the set the request resolved
// to, less any deployment that has gone, and no deployment that is new.
func savedServingSet(m *inventory.Model, res *Resolution) ([]*inventory.Deployment, []*inventory.ManagedSource) {
	var served []*inventory.Deployment
	for _, d := range m.Served {
		if res.serving[d.Key] {
			served = append(served, d)
		}
	}
	var sources []*inventory.ManagedSource
	for _, s := range m.ManagedSources {
		if res.serving[s.Key] {
			sources = append(sources, s)
		}
	}
	return served, sources
}

// resolveWorkload finds the Workload a request names, by the name's suffix, the
// header, or both, among the Workloads whose spec.model is the request's model.
// The three refusals come in the order HBEP-5 lists them: a name no Workload has
// (a header that is no DNS label has none), a name two namespaces have, and a
// suffix and a header that name different Workloads. It returns nil, nil for a
// request that names none.
func resolveWorkload(v *inventory.View, n naming.Name, header string) (*inventory.Workload, *routeError) {
	var names []string
	for _, name := range []string{n.Workload, n.HeaderWorkload} {
		if name != "" && !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	byName := v.Workloads[n.Model]
	for _, name := range names {
		if len(byName[name]) == 0 {
			return nil, errUnknownWorkload(n.Model, name)
		}
	}
	for _, name := range names {
		if ws := byName[name]; len(ws) > 1 {
			keys := make([]string, len(ws))
			for i, w := range ws {
				keys[i] = w.Namespace
			}
			return nil, errWorkloadAmbiguous(n.Model, name, keys)
		}
	}
	if len(names) > 1 {
		return nil, errWorkloadConflict(header, n.Workload, n.HeaderWorkload)
	}
	if len(names) == 0 {
		return nil, nil
	}
	return byName[names[0]][0], nil
}

// chooseServed walks our own ModelDeployments in the order the key's ring over
// all their routable Instances visits them (KubeAI's ring: 256 points per
// Instance), and takes the first that can place the request. wait is the
// first deployment on the walk that has an eligible Instance, all at their
// caps.
//
// ch is the request's charge against each Instance's token budget: its Workload's
// c, or none (Resolution.load); a member that does not admit it is skipped as one
// at its cap is.
//
// serving are the deployments the request may resolve to: all of the model's, or
// a Workload's (servingSet). The ring is the model's one ring, over every
// Instance of it. An Instance of a deployment outside serving is skipped, as one
// of a deployment with no group is, so the walk visits the serving deployments
// in the order a ring of only their Instances would: each Instance's points sit
// where they sit whatever else is on the ring (HBEP-5, Workloads, Pinning; the
// science agent's ruling on #764, Q7; the fork's own ring is the oracle in
// pin_workload_test.go).
func (r *Router) chooseServed(serving []*inventory.Deployment, ring *balancer.Ring, groups map[string]*balancer.Group, key string, match balancer.Match, ch balancer.Charge) (*target, *inventory.Deployment) {
	if ring == nil || len(serving) == 0 {
		return nil, nil
	}
	byInst := map[string]*inventory.Instance{}
	byDep := map[string]*inventory.Deployment{}
	for _, d := range serving {
		byDep[d.Key] = d
		for _, i := range d.Members {
			byInst[i.Key] = i
		}
	}
	var wait *inventory.Deployment
	tried := map[string]bool{}
	for _, ik := range ring.Walk(key) {
		i := byInst[ik]
		if i == nil {
			continue
		}
		d := byDep[i.Deployment]
		if d == nil || tried[d.Key] {
			continue
		}
		tried[d.Key] = true
		g := groups[d.Key]
		if g == nil {
			continue
		}
		res, err := g.TryPickFor(key, match, ch)
		switch {
		case err == nil:
			// The group is live and the view this request read is not: a
			// member that joined since is looked up in the current view, and
			// one that has already left is given back (PR #95 review: Astra
			// B3).
			inst := byInst[res.Member]
			if inst == nil {
				inst = r.instance(res.Member)
			}
			if inst == nil {
				// Quiet: the caller may hold a Workload's mutex (workload_route.go), which a wake would take again. The member is not
				// routable now, so the give-back frees no capacity a waiting request could use.
				res.ReleaseQuiet()
				continue
			}
			return &target{inst: inst, dep: d, res: res}, nil
		case errors.Is(err, balancer.ErrAllAtCap) && wait == nil:
			wait = d
		}
	}
	return nil, wait
}

// waitServed holds the request in the deployment's FIFO for at most
// queueTimeout (HBEP-5, Load bound and cap).
func (r *Router) waitServed(ctx context.Context, d *inventory.Deployment, g *balancer.Group, n naming.Name, key string, exclude []string, ch balancer.Charge) (*target, *routeError) {
	byInst := map[string]*inventory.Instance{}
	for _, i := range d.Members {
		byInst[i.Key] = i
	}
	match := levelMatch(n, exclude)
	qctx, cancel := context.WithTimeout(ctx, r.cfg.QueueTimeout.D())
	defer cancel()
	res, err := g.PickOrWaitFor(qctx, key, match, ch)
	if err == nil {
		inst := byInst[res.Member]
		if inst == nil {
			// The member joined after this request read the view: look it up.
			inst = r.instance(res.Member)
		}
		if inst == nil {
			res.Release()
			return nil, errNoReadyTarget(n.Model)
		}
		return &target{inst: inst, dep: d, res: res}, nil
	}
	return nil, waitError(ctx, d, "an Instance of "+d.Key, "every Instance of "+d.Key+" is at its cap")
}

// waitError is the answer for a request whose wait ended with no placement. If its context ended, the client went away, or the front cut the request
// while it waited: its own deadline (maxRequestDuration below queueTimeout) or its shutdown. A front cut is answered and attributed, never an empty
// response (PR #95 review: Astra B10). Otherwise queueTimeout passed: 503 capacity. forWhat completes "the request was cut while it waited for ..." and
// full is the capacity message. dep is the ModelDeployment the request waited for, nil for a Workload's FIFO, whose record names none (Q10).
func waitError(ctx context.Context, dep *inventory.Deployment, forWhat, full string) *routeError {
	if ctx.Err() != nil {
		cut := cutOf(ctx)
		if cut == cutClient {
			return &routeError{status: statusClientClosed, code: "client_closed", message: "the client went away while the request waited"}
		}
		if cut == cutFrontShutdown {
			return &routeError{status: http.StatusServiceUnavailable, code: codeShuttingDown, detail: detailOf(cut), retryAfter: "5", cutBy: cut, dep: dep,
				message: "the front is shutting down; send the request again"}
		}
		return &routeError{status: http.StatusGatewayTimeout, code: codeRequestTimeout, detail: detailOf(cut), cutBy: cut, dep: dep,
			message: "the request was cut while it waited for " + forWhat + ": " + detailOf(cut)}
	}
	return &routeError{status: http.StatusServiceUnavailable, code: codeCapacity, retryAfter: "10", dep: dep, message: full}
}

// instance looks a member up in the current view.
// It is called only for a member that joined the live group after the view the
// request read, so the view is already the one that added it: it rebuilds
// nothing.
func (r *Router) instance(key string) *inventory.Instance {
	r.lockAsReader()
	defer r.mu.Unlock()
	if r.view == nil {
		return nil
	}
	for _, m := range r.view.Models {
		for _, d := range m.Served {
			for _, i := range d.Members {
				if i.Key == key {
					return i
				}
			}
		}
	}
	return nil
}

// managedTarget is the target for the key's home among the plan's eligible managed targets, counted in flight (the series). ws is the request's
// Workload, nil for a request that names none; its count is reserved by the caller, under its mutex (takeManagedLocked), before this is called.
func (r *Router) managedTarget(ws *wlState, p *plan) *target {
	t := chooseManaged(p.managed, p.key)
	t.mres = &managedRes{r: r, ws: ws, workload: p.wname, provider: t.managed.Provider, deployment: t.managed.Deployment}
	r.metrics.inFlightAdd(p.wname, t.managed.Provider, t.managed.Deployment, "", +1)
	t.workload, t.wres = p.wname, p.res
	return t
}

// chooseManaged takes the key's home on a ring of the managed targets, with
// no bound: LLMLab's capacity cannot be read (HBEP-5, The LLMLab route). A
// Workload's managedSessions bounds it by a count the caller reserves
// (takeManagedLocked).
func chooseManaged(ts []*inventory.ManagedTarget, key string) *target {
	keys := make([]string, len(ts))
	byKey := map[string]*inventory.ManagedTarget{}
	for i, t := range ts {
		keys[i] = t.Key
		byKey[t.Key] = t
	}
	return &target{managed: byKey[balancer.NewRing(keys).Home(key)]}
}

// levelMatch is a request's filter on a deployment's members.
func levelMatch(n naming.Name, exclude []string) balancer.Match {
	return func(member string, a balancer.Attrs) bool {
		return !slices.Contains(exclude, member) && levelMatches(n, a.Provider, a.DisplayID)
	}
}

// levelMatches reports whether a target on provider with deployment ID
// display matches the requested level.
func levelMatches(n naming.Name, provider, display string) bool {
	switch n.Level {
	case naming.LevelProvider:
		return provider == n.Provider
	case naming.LevelPinned:
		return provider == n.Provider && display == n.DeploymentID
	}
	return true
}
