package front

import (
	"fmt"
	"sort"

	"github.com/cespare/xxhash"
)

// ring replicates the order in which KubeAI's CHWBL visits endpoints, so the
// front can walk onward from the endpoint CHWBL chose. KubeAI keeps the ring
// private and the fork stays unchanged, so the wrapper rebuilds it with the
// same hashing (third_party/forked/kubeai/loadbalancer/balance_chwbl.go):
// each endpoint has `replication` points at xxhash(name + index), a key sits
// at xxhash(key), and its home is the first point at or after it, wrapping.
// TestRingMatchesUpstream checks the replica against the fork's own picks.
type ring struct {
	points []uint64 // sorted replica points
	owner  map[uint64]string
}

func ringHash(s string) uint64 { return xxhash.Sum64([]byte(s)) }

func newRing(names []string, replication int) *ring {
	r := &ring{owner: map[uint64]string{}}
	for _, n := range names {
		for i := 0; i < replication; i++ {
			h := ringHash(fmt.Sprintf("%s%d", n, i))
			r.owner[h] = n
			r.points = append(r.points, h)
		}
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i] < r.points[j] })
	return r
}

// walk returns the endpoints in the order the ring visits them from key's
// position: the home first, then each other endpoint at its first point.
func (r *ring) walk(key string) []string {
	if len(r.points) == 0 {
		return nil
	}
	h := ringHash(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	if i >= len(r.points) {
		i = 0
	}
	seen := map[string]bool{}
	var out []string
	for n := 0; n < len(r.points); n++ {
		name := r.owner[r.points[(i+n)%len(r.points)]]
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	return out
}

// home returns the endpoint that owns key.
func (r *ring) home(key string) string {
	w := r.walk(key)
	if len(w) == 0 {
		return ""
	}
	return w[0]
}
