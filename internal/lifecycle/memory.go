package lifecycle

import (
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// memory holds what the controller keeps between passes in this process only,
// none of it a plan input: the API holds those (restart history in
// InstanceStatus.Restarts, the idle clock in the Allocation's idle-since
// annotation, the failure streak in the ended Instances' EndTime, the drain
// start in an Instance annotation):
//   - when this process started: the grace for agents to register again, and
//     the baseline of agent reachability and token progress after a restart;
//   - which once-only events were emitted, and when each repeating event
//     last fired (HBEP-8, Repeating conditions: the gate is in memory, so a
//     restart emits it once more, "when it starts" for that process);
//   - the spec generation each deployment's IDs were computed for, and a
//     collided result kept until the spec or the colliding record changes
//     (a restart evaluates once more, then keeps it again);
//   - when the credentials directory was last swept.
type memory struct {
	mu          sync.Mutex
	started     time.Time
	emitted     map[string]bool
	repeats     map[string]time.Time
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
	return &memory{started: started, emitted: map[string]bool{}, repeats: map[string]time.Time{},
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

// repeat reports whether the event of a condition that persists is due at
// now, and records it as fired: when the condition starts (key not held), and
// then at most once every provider.EventRepeatInterval while it lasts, never
// on every pass (HBEP-8, Repeating conditions).
func (m *memory) repeat(key string, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if last, ok := m.repeats[key]; ok && now.Sub(last) < provider.EventRepeatInterval {
		return false
	}
	m.repeats[key] = now
	return true
}

// prune forgets the repeat keys that begin with prefix and are not in live: the
// objects they stood for are gone, and a key for one is never cleared, since
// only a pass over its object clears it. The caller passes the keys of the
// objects a successful list found; a failed list prunes nothing.
func (m *memory) prune(prefix string, live map[string]bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.repeats {
		if strings.HasPrefix(k, prefix) && !live[k] {
			delete(m.repeats, k)
		}
	}
}

// clear forgets a condition that ended, so its next start fires at once.
func (m *memory) clear(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.repeats, key)
}
