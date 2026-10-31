package balancer

import (
	"fmt"
	"sort"

	"github.com/cespare/xxhash"
)

// Replication is the number of points each member has on a ring: KubeAI's
// default (HBEP-5, Request path and Load bound and cap).
const Replication = 256

// Ring replicates the order in which KubeAI's CHWBL visits members, so the
// front can walk onward from a member and skip the ones a filter excludes.
// KubeAI keeps its ring private and the fork stays unchanged, so the ring is
// rebuilt here with the same hashing
// (third_party/forked/kubeai/loadbalancer/balance_chwbl.go): each member has
// Replication points at xxhash(name + decimal index), a key sits at
// xxhash(key), and its home is the first point at or after it, wrapping.
//
// Rewritten from spike S10's spikes/s10/front/ring.go (this repository, PR
// #34); the tests check it against the fork's own picks, as S10b's did.
type Ring struct {
	points  []uint64 // sorted
	owner   map[uint64]string
	members int // distinct names
}

func ringHash(s string) uint64 { return xxhash.Sum64([]byte(s)) }

// NewRing places each name's Replication points on a new ring.
func NewRing(names []string) *Ring {
	r := &Ring{owner: make(map[uint64]string, len(names)*Replication)}
	distinct := make(map[string]bool, len(names))
	for _, n := range names {
		if distinct[n] {
			continue
		}
		distinct[n] = true
		for i := 0; i < Replication; i++ {
			h := ringHash(fmt.Sprintf("%s%d", n, i))
			r.owner[h] = n
			r.points = append(r.points, h)
		}
	}
	r.members = len(distinct)
	sort.Slice(r.points, func(i, j int) bool { return r.points[i] < r.points[j] })
	return r
}

// Walk returns the members in the order the ring visits them from key's
// position: the home first, then each other member at its first point.
func (r *Ring) Walk(key string) []string {
	if len(r.points) == 0 {
		return nil
	}
	h := ringHash(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	if i >= len(r.points) {
		i = 0
	}
	seen := make(map[string]bool)
	var out []string
	for n := 0; n < len(r.points) && len(seen) < r.members; n++ {
		name := r.owner[r.points[(i+n)%len(r.points)]]
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	return out
}

// Home returns the member that owns key, or "" on an empty ring.
func (r *Ring) Home(key string) string {
	w := r.Walk(key)
	if len(w) == 0 {
		return ""
	}
	return w[0]
}
