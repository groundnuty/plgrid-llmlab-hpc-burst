package sim

import (
	"fmt"
	"math"
	"strconv"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai/loadbalancer"
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
	// requests from the balancer's count when the drain starts, so KubeAI's
	// mean covers members only. The copied algorithm is unchanged.
	ReleaseOnDrain bool
}

// Name is the policy's label in reports.
func (p Policy) Name() string {
	switch p.Kind {
	case CHWBL:
		if p.ReleaseOnDrain {
			return fmt.Sprintf("chwbl-%.2f+release-on-drain", float64(p.Percent)/100)
		}
		return fmt.Sprintf("chwbl-%.2f", float64(p.Percent)/100)
	default:
		return string(p.Kind)
	}
}

// Bound returns c, the load bound factor (+Inf when there is none).
func (p Policy) Bound() float64 {
	if p.Kind == CHWBL {
		return float64(p.Percent) / 100
	}
	return math.Inf(1)
}

// Hashed reports whether the policy uses a key.
func (p Policy) Hashed() bool { return p.Kind == Affinity || p.Kind == CHWBL }

// router places requests on member instances. For hashed policies every
// placement goes through the copied KubeAI group.
type router struct {
	pol     Policy
	names   []string
	members []int // instance indices, in join order
	rr      int
	group   *loadbalancer.Group
}

func newRouter(pol Policy, names []string, initial []int) *router {
	r := &router{pol: pol, names: names, members: append([]int(nil), initial...)}
	if pol.Hashed() {
		r.group = loadbalancer.NewGroup(256)
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
	pct := r.pol.Percent
	if r.pol.Kind == Affinity {
		pct = AffinityPercent
	}
	addr, done, err := r.group.PickPrefixHash(key, pct)
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

func (r *router) remove(inst int) {
	for i, m := range r.members {
		if m == inst {
			r.members = append(r.members[:i:i], r.members[i+1:]...)
			break
		}
	}
	if r.group != nil {
		r.group.RemoveEndpoint(r.names[inst])
	}
}

func (r *router) add(inst int) {
	r.members = append(r.members, inst)
	if r.group != nil {
		r.group.SetEndpoints(r.memberNames())
	}
}
