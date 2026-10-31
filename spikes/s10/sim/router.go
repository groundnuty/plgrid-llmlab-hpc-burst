package sim

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10/front"
)

// KeyKind is a choice of conversation key.
type KeyKind string

const (
	// KeyHeaderOpening: the session header when the client sends one,
	// otherwise the hash of the opening.
	KeyHeaderOpening KeyKind = "header>opening"
	// KeyHeaderNone: the session header when sent, otherwise no affinity
	// (a fresh key per request, still placed and counted by the balancer).
	KeyHeaderNone KeyKind = "header>none"
	// KeyOpening: hash of the system prompt plus the first user message.
	KeyOpening KeyKind = "opening"
	// KeyKubeAI100c: KubeAI's own default, the first 100 characters of the
	// first user message (taken here as 25 tokens).
	KeyKubeAI100c KeyKind = "kubeai-100c"
	// KeyPrefix2048: hash of the request's first 2,048 tokens.
	KeyPrefix2048 KeyKind = "prefix-2048"
	// KeySystem: hash of the system prompt only.
	KeySystem KeyKind = "system"
)

// AllKeys lists every key choice in report order.
var AllKeys = []KeyKind{KeyHeaderOpening, KeyHeaderNone, KeyOpening, KeyKubeAI100c, KeyPrefix2048, KeySystem}

// KubeAI100cTokens is 100 characters at about 4 characters per token.
const KubeAI100cTokens = 25

// Key returns the conversation key of a request, or ok=false for none.
func (k KeyKind) Key(cs *convState, reqID int) (key string, ok bool) {
	header := cs.c.Header
	switch k {
	case KeyHeaderOpening:
		if header {
			return "s:" + strconv.Itoa(cs.c.ID), true
		}
		return SegsKey(cs.c.Sys, cs.firstUser), true
	case KeyHeaderNone:
		if header {
			return "s:" + strconv.Itoa(cs.c.ID), true
		}
		return "r:" + strconv.Itoa(reqID), true
	case KeyOpening:
		return SegsKey(cs.c.Sys, cs.firstUser), true
	case KeyKubeAI100c:
		return PrefixKey(cs.firstUser, KubeAI100cTokens), true
	case KeyPrefix2048:
		return PrefixKey(cs.ctx.Segs, 2048), true
	case KeySystem:
		return SegsKey(cs.c.Sys), true
	}
	panic(fmt.Sprintf("unknown key kind %q", k))
}

// PolicyKind is a routing rule.
type PolicyKind string

const (
	RoundRobin PolicyKind = "round-robin"
	LeastLoad  PolicyKind = "least-load"
	Affinity   PolicyKind = "affinity"
	CHWBL      PolicyKind = "chwbl"
)

// AffinityPercent makes KubeAI's bound effectively infinite: pure
// consistent hashing on the same ring.
const AffinityPercent = 1 << 40

// Policy is a routing rule with its key and bound.
type Policy struct {
	Kind    PolicyKind
	Key     KeyKind
	Percent int // CHWBL MeanLoadPercentage (125 = bound 1.25x the mean)
	// ReleaseOnDrain makes the front release a draining Instance's open
	// requests from the balancer's count when the drain starts
	// (front.Balancer.Drain), so KubeAI's mean covers members only. Without
	// it a drain keeps them counted until they finish, as KubeAI does
	// (front.Balancer.Remove). The copied algorithm is unchanged.
	ReleaseOnDrain bool

	// Cap, when positive, is a hard per-Instance limit on in-flight requests
	// (spike S10b, option 3), at the engine's maximum sequences: a request
	// whose CHWBL choice holds Cap requests goes to the next Instance on the
	// ring that holds fewer, and waits at the front when none does. CHWBL only.
	Cap int
	// AdaptiveLoose, when positive, makes the bound adaptive (spike S10b,
	// option 4): Percent while every desired Instance is ready, AdaptiveLoose
	// while fewer are. The deployment checks readiness every ControlStep
	// seconds. CHWBL only.
	AdaptiveLoose int
	ControlStep   float64
	// CapFullOnly, with Cap and AdaptiveLoose set (spike S10c, option 5),
	// makes the cap follow the bound's readiness rule: Cap applies while every
	// desired Instance is ready, together with Percent, and no cap applies
	// while fewer are, together with AdaptiveLoose. Without it Cap applies
	// throughout, as in option 3.
	CapFullOnly bool
}

// DefaultControlStep is the adaptive bound's control period in seconds when
// Policy.ControlStep is zero. It is an assumption.
const DefaultControlStep = 10.0

func (p Policy) controlStep() float64 {
	if p.ControlStep > 0 {
		return p.ControlStep
	}
	return DefaultControlStep
}

// Name is the policy's label in reports.
func (p Policy) Name() string {
	switch p.Kind {
	case CHWBL:
		n := fmt.Sprintf("chwbl-%.2f", float64(p.Percent)/100)
		if p.AdaptiveLoose > 0 {
			n = fmt.Sprintf("chwbl-adaptive-%.2f-%.2f", float64(p.Percent)/100, float64(p.AdaptiveLoose)/100)
		}
		if p.Cap > 0 {
			n += fmt.Sprintf("+cap%d", p.Cap)
			if p.CapFullOnly {
				n += "-full-only"
			}
		}
		if p.ReleaseOnDrain {
			n += "+release-on-drain"
		}
		return n
	default:
		return string(p.Kind)
	}
}

// Bound returns c, the load bound factor (+Inf when there is none). For an
// adaptive policy it is the tight bound; the bound in effect is the
// router's.
func (p Policy) Bound() float64 {
	if p.Kind == CHWBL {
		return float64(p.Percent) / 100
	}
	return math.Inf(1)
}

// Hashed reports whether the policy uses a key.
func (p Policy) Hashed() bool { return p.Kind == Affinity || p.Kind == CHWBL }

// router places requests on member instances. For hashed policies every
// placement goes through the copied KubeAI group, behind the front's
// serializing, release-once wrapper (package front).
type router struct {
	pol     Policy
	names   []string
	members []int // instance indices, in join order
	rr      int
	group   *front.Balancer

	// percent is the CHWBL bound in effect: the policy's, or for an adaptive
	// policy whichever the last control step chose.
	percent int
	// lastSpilled is whether the latest route spilled past a full Instance.
	lastSpilled bool
	// capNow is the hard cap in force: the policy's, or for CapFullOnly
	// whatever the last control step chose (0 for none).
	capNow int
}

// errAllAtCap is route's signal that every member holds the hard cap.
var errAllAtCap = front.ErrAllAtCap

// bound is c in effect (+Inf when there is none).
func (r *router) bound() float64 {
	if r.pol.Kind == CHWBL {
		return float64(r.percent) / 100
	}
	return math.Inf(1)
}

// controlChange is what one control step did to the bound and the cap.
type controlChange struct {
	from, to       int // MeanLoadPercentage before and after
	capFrom, capTo int // the hard cap before and after (0 for none)
	changed        bool
}

// control applies the adaptive rule to the current readiness. changed is set
// when the bound or the cap changed.
func (r *router) control(ready, desired int) controlChange {
	if r.pol.AdaptiveLoose <= 0 {
		return controlChange{from: r.percent, to: r.percent, capFrom: r.capNow, capTo: r.capNow}
	}
	ch := controlChange{from: r.percent, capFrom: r.capNow}
	if r.pol.CapFullOnly && r.pol.Cap > 0 {
		rule := front.AdaptiveCapBound{Tight: r.pol.Percent, Loose: r.pol.AdaptiveLoose, Cap: r.pol.Cap}
		ch.to, ch.capTo = rule.Percent(ready, desired), rule.CapAt(ready, desired)
	} else {
		ch.to = front.AdaptiveBound{Tight: r.pol.Percent, Loose: r.pol.AdaptiveLoose}.Percent(ready, desired)
		ch.capTo = r.capNow
	}
	r.percent, r.capNow = ch.to, ch.capTo
	ch.changed = ch.from != ch.to || ch.capFrom != ch.capTo
	return ch
}

func newRouter(pol Policy, names []string, initial []int) *router {
	r := &router{pol: pol, names: names, members: append([]int(nil), initial...), percent: pol.Percent, capNow: pol.Cap}
	if pol.Hashed() {
		r.group = front.New(256)
		r.group.SetEndpoints(r.memberNames())
	}
	return r
}

func (r *router) memberNames() []string {
	out := make([]string, len(r.members))
	for i, m := range r.members {
		out[i] = r.names[m]
	}
	return out
}

func (r *router) index(name string) int {
	for i, n := range r.names {
		if n == name {
			return i
		}
	}
	panic("router: unknown instance " + name)
}

// route returns the chosen instance and the function that releases the
// balancer's load count when the request ends.
func (r *router) route(cs *convState, reqID int, inflight []int) (int, func(), error) {
	switch r.pol.Kind {
	case RoundRobin:
		m := r.members[r.rr%len(r.members)]
		r.rr++
		return m, func() {}, nil
	case LeastLoad:
		return r.leastLoad(inflight), func() {}, nil
	}
	key, _ := r.pol.Key.Key(cs, reqID)
	pct := r.percent
	if r.pol.Kind == Affinity {
		pct = AffinityPercent
	}
	r.lastSpilled = false
	var addr string
	var done func()
	var err error
	if r.capNow > 0 && r.pol.Kind == CHWBL {
		addr, done, r.lastSpilled, err = r.group.PickPrefixHashCapped(key, pct, r.capNow)
		if errors.Is(err, front.ErrAllAtCap) {
			return 0, nil, errAllAtCap
		}
	} else {
		addr, done, err = r.group.PickPrefixHash(key, pct)
	}
	if err != nil {
		return 0, nil, fmt.Errorf("kubeai pick: %w", err)
	}
	return r.index(addr), done, nil
}

// leastLoad picks the member with the fewest in-flight requests; ties go to
// the first member at or after a rotating start, so the result is
// deterministic.
func (r *router) leastLoad(inflight []int) int {
	n := len(r.members)
	best := -1
	for j := 0; j < n; j++ {
		m := r.members[(r.rr+j)%n]
		if best < 0 || inflight[m] < inflight[best] {
			best = m
		}
	}
	r.rr++
	return best
}

// remove takes an Instance out of the ring. With release, its open
// requests leave the balancer's count now (front.Balancer.Drain) and the
// number released is returned; otherwise they stay counted until they end.
func (r *router) remove(inst int, release bool) int {
	for i, m := range r.members {
		if m == inst {
			r.members = append(r.members[:i:i], r.members[i+1:]...)
			break
		}
	}
	if r.group == nil {
		return 0
	}
	if release {
		return r.group.Drain(r.names[inst])
	}
	r.group.Remove(r.names[inst])
	return 0
}

func (r *router) add(inst int) {
	r.members = append(r.members, inst)
	if r.group != nil {
		r.group.SetEndpoints(r.memberNames())
	}
}
