package lifecycle

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// memory holds what the controller keeps between passes in this process only,
// none of it a plan input: the API holds those (restart history in
// InstanceStatus.Restarts, the idle clock in the Allocation's idle-since
// annotation, the failure streak in the ended Instances' EndTime):
//   - when this process started: the grace for agents to register again, and
//     the baseline of agent reachability and token progress after a restart;
//   - which once-only events were emitted (ending soon, replacement late,
//     collision, restart loop);
//   - the Allocations this process released, whose credential files go once
//     they are gone.
type memory struct {
	mu      sync.Mutex
	started time.Time
	emitted map[string]bool
	// released are Allocations this process deleted, by UID with their name,
	// whose credential files go once the Allocation is gone.
	released map[types.UID]string
}

func newMemory(started time.Time) *memory {
	return &memory{started: started, emitted: map[string]bool{}, released: map[types.UID]string{}}
}

// once reports whether key is new, and marks it.
func (m *memory) once(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.emitted[key] {
		return false
	}
	m.emitted[key] = true
	return true
}

func (m *memory) releasedAlloc(uid types.UID, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.released[uid] = name
}

// goneAllocs returns the released Allocations whose names present does not
// hold any more, and forgets them.
func (m *memory) goneAllocs(present map[string]bool) []types.UID {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []types.UID
	for uid, name := range m.released {
		if !present[name] {
			out = append(out, uid)
			delete(m.released, uid)
		}
	}
	return out
}
