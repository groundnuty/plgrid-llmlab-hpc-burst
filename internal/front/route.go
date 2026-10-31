package front

import (
	"context"
	"errors"
	"net/http"

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
}

// key is the target's name for pauses and exclusion.
func (t *target) key() string {
	if t.managed != nil {
		return t.managed.Key
	}
	return t.inst.Key
}

// release ends the target's reservation; safe to call more than once.
func (t *target) release() {
	if t != nil && t.res != nil {
		t.res.Release()
	}
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
}

func errUnknownProvider(p string) *routeError {
	return &routeError{status: http.StatusBadRequest, code: codeInvalidModelName, detail: "unknown_provider", message: "the model name names an unknown provider: " + p}
}

func errNotAttached(model string) *routeError {
	return &routeError{status: http.StatusServiceUnavailable, code: codeModelInactive, detail: "not_attached", retryAfter: "30",
		message: "the model " + model + " is not active: no provider is attached to it"}
}

func errNoReadyTarget(model string) *routeError {
	return &routeError{status: http.StatusServiceUnavailable, code: codeModelInactive, detail: "no_ready_target", retryAfter: "30",
		message: "the model " + model + " has no ready target at the requested level"}
}

// choose resolves a parsed name to a target (HBEP-5, Request path steps 3 to
// 6). exclude is a target the request already failed to reach, for the one
// resend. It never widens to a broader level.
func (r *Router) choose(ctx context.Context, n naming.Name, key, exclude string) (*target, *routeError) {
	r.mu.Lock()
	v := r.view
	if v == nil {
		r.mu.Unlock()
		return nil, errNoReadyTarget(n.Model)
	}
	if n.Provider != "" && v.Providers[n.Provider] == nil {
		r.mu.Unlock()
		return nil, errUnknownProvider(n.Provider)
	}
	m := v.Models[n.Model]
	if m == nil {
		r.mu.Unlock()
		return nil, errNotAttached(n.Model)
	}
	ring := r.rings[m.Name]
	groups := make(map[string]*balancer.Group, len(m.Served))
	for _, d := range m.Served {
		groups[d.Key] = r.groups[d.Key]
	}
	now := r.clk.Now()
	var managed []*inventory.ManagedTarget
	for _, t := range m.Managed {
		if levelMatches(n, t.Provider, t.DisplayID) && t.Key != exclude && !r.pausedLocked(t.Key, now) {
			managed = append(managed, t)
		}
	}
	r.mu.Unlock()
	if r.beforePick != nil {
		r.beforePick()
	}

	tg, wait := r.chooseServed(m, ring, groups, n, key, exclude)
	if tg != nil {
		return tg, nil
	}
	if len(managed) > 0 {
		return chooseManaged(managed, key), nil
	}
	if wait == nil {
		return nil, errNoReadyTarget(n.Model)
	}
	return r.waitServed(ctx, wait, groups[wait.Key], m, n, key, exclude)
}

// chooseServed walks our own ModelDeployments in the order the key's ring over
// all their routable Instances visits them (KubeAI's ring: 256 points per
// Instance), and takes the first that can place the request. wait is the
// first deployment on the walk that has an eligible Instance, all at their
// caps.
func (r *Router) chooseServed(m *inventory.Model, ring *balancer.Ring, groups map[string]*balancer.Group, n naming.Name, key, exclude string) (*target, *inventory.Deployment) {
	if ring == nil || len(m.Served) == 0 {
		return nil, nil
	}
	byInst := map[string]*inventory.Instance{}
	byDep := map[string]*inventory.Deployment{}
	for _, d := range m.Served {
		byDep[d.Key] = d
		for _, i := range d.Members {
			byInst[i.Key] = i
		}
	}
	match := levelMatch(n, exclude)
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
		res, err := g.TryPick(key, match)
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
				res.Release()
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
func (r *Router) waitServed(ctx context.Context, d *inventory.Deployment, g *balancer.Group, m *inventory.Model, n naming.Name, key, exclude string) (*target, *routeError) {
	byInst := map[string]*inventory.Instance{}
	for _, i := range d.Members {
		byInst[i.Key] = i
	}
	match := levelMatch(n, exclude)
	qctx, cancel := context.WithTimeout(ctx, r.cfg.QueueTimeout.D())
	defer cancel()
	res, err := g.PickOrWait(qctx, key, match)
	if err == nil {
		inst := byInst[res.Member]
		if inst == nil {
			// The member joined after this request read the view: look it up.
			inst = r.instance(res.Member)
		}
		if inst == nil {
			res.Release()
			return nil, errNoReadyTarget(m.Name)
		}
		return &target{inst: inst, dep: d, res: res}, nil
	}
	if ctx.Err() != nil {
		// The client went away, or the front cut the request while it
		// waited: its own deadline (maxRequestDuration below queueTimeout)
		// or its shutdown. A front cut is answered and attributed, never
		// an empty response (PR #95 review: Astra B10).
		cut := cutOf(ctx)
		if cut == cutClient {
			return nil, &routeError{status: statusClientClosed, code: "client_closed", message: "the client went away while the request waited"}
		}
		if cut == cutFrontShutdown {
			return nil, &routeError{status: http.StatusServiceUnavailable, code: codeShuttingDown, retryAfter: "5", cutBy: cut, dep: d,
				message: "the front is shutting down; send the request again"}
		}
		return nil, &routeError{status: http.StatusGatewayTimeout, code: codeRequestTimeout, detail: detailOf(cut), cutBy: cut, dep: d,
			message: "the request was cut while it waited for an Instance of " + d.Key + ": " + detailOf(cut)}
	}
	return nil, &routeError{status: http.StatusServiceUnavailable, code: codeCapacity, retryAfter: "10", dep: d,
		message: "every Instance of " + d.Key + " is at its cap"}
}

// instance looks a member up in the current view.
func (r *Router) instance(key string) *inventory.Instance {
	r.mu.Lock()
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

// chooseManaged takes the key's home on a ring of the managed targets, with
// no bound: LLMLab's capacity cannot be read (HBEP-5, The LLMLab route).
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
func levelMatch(n naming.Name, exclude string) balancer.Match {
	return func(member string, a balancer.Attrs) bool {
		return member != exclude && levelMatches(n, a.Provider, a.DisplayID)
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
