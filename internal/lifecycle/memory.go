package lifecycle

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// memory holds what the controller keeps between passes in this process only,
// none of it a plan input: the API holds those (restart history in
// InstanceStatus.Restarts, the idle clock in the Allocation's idle-since
// annotation, the failure streak in the ended Instances' EndTime, the drain
// start in an Instance annotation):
//   - when this process started: the grace for agents to register again, and
//     the baseline of agent reachability and token progress after a restart;
//   - which once-only events were emitted;
//   - the spec generation each deployment's IDs were computed for, and a
//     collided result kept until the spec or the colliding record changes
//     (a restart evaluates once more, then keeps it again);
//   - when the credentials directory was last swept.
type memory struct {
	mu          sync.Mutex
	started     time.Time
	emitted     map[string]bool
	generations map[types.UID]int64
	collisions  map[types.UID]collisionMemo
	lastSweep   time.Time
}

// collisionMemo is a deployment's IDs computed with a display-form collision.
type collisionMemo struct {
	gen        int64
	record, rv string
	result     []classID
}

func newMemory(started time.Time) *memory {
	return &memory{started: started, emitted: map[string]bool{},
		generations: map[types.UID]int64{}, collisions: map[types.UID]collisionMemo{}}
}

// specChanged reports whether the deployment's spec generation differs from the
// one its IDs were last computed for, and records gen.
func (m *memory) specChanged(uid types.UID, gen int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.generations[uid]
	m.generations[uid] = gen
	return ok && old != gen
}

func (m *memory) collision(uid types.UID, gen int64) (collisionMemo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.collisions[uid]
	return c, ok && c.gen == gen
}

func (m *memory) rememberCollision(uid types.UID, c collisionMemo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.collisions[uid] = c
}

func (m *memory) forgetCollision(uid types.UID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.collisions, uid)
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
