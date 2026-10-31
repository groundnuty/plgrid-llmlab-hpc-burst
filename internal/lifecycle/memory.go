package lifecycle

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// memory holds what the controller keeps between passes in this process only.
// Each entry is a stand-in or a convenience, never a plan input:
//
//   - restart history (part, generation, time, signal) per Instance, which
//     the restart ladder counts within restart.window. Stand-in for
//     InstanceStatus.Restarts, asked of the contracts owner. After a restart
//     of the controller it is seeded from the restart generations, each
//     dated at the controller's start, which counts them inside the window
//     (the safe direction: the ladder climbs sooner, never later).
//   - when an Allocation was first seen with no live Instance: the idle clock
//     of retention. Stand-in for InstanceStatus.EndTime; after a controller
//     restart the clock starts again, so a release comes later, never
//     earlier.
//   - the failure streak per class behind RestartLoop.
//   - which once-only events were emitted (ending soon, replacement late).
type memory struct {
	mu         sync.Mutex
	started    time.Time
	restarts   map[types.UID][]restartRec
	seeded     map[types.UID]bool
	emptySince map[types.UID]time.Time
	failures   map[v1alpha1.AcceleratorClass][]time.Time
	emitted    map[string]bool
	// released are Allocations this process deleted, by UID with their name,
	// whose credential files go once the Allocation is gone.
	released map[types.UID]string
}

type restartRec struct {
	part   string
	gen    int64
	at     time.Time
	signal string
}

func newMemory(started time.Time) *memory {
	return &memory{
		started:    started,
		restarts:   map[types.UID][]restartRec{},
		seeded:     map[types.UID]bool{},
		emptySince: map[types.UID]time.Time{},
		failures:   map[v1alpha1.AcceleratorClass][]time.Time{},
		emitted:    map[string]bool{},
		released:   map[types.UID]string{},
	}
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

// history returns an Instance's restarts, seeding it from the restart
// generations the first time it is asked after a controller start.
func (m *memory) history(inst *v1alpha1.Instance) []restartRec {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.seeded[inst.UID] {
		m.seeded[inst.UID] = true
		var seed []restartRec
		for _, part := range []string{v1alpha1.WorkerName(0), v1alpha1.PartInstance} {
			for g := int64(1); g <= restartGeneration(inst, part); g++ {
				seed = append(seed, restartRec{part: part, gen: g, at: m.started, signal: "seeded"})
			}
		}
		m.restarts[inst.UID] = append(seed, m.restarts[inst.UID]...)
	}
	return append([]restartRec(nil), m.restarts[inst.UID]...)
}

func (m *memory) addRestart(uid types.UID, r restartRec) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seeded[uid] = true
	m.restarts[uid] = append(m.restarts[uid], r)
}

// idleSince returns when the Allocation was first seen hosting no live
// Instance, recording now the first time.
func (m *memory) idleSince(uid types.UID, now time.Time) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.emptySince[uid]
	if !ok {
		m.emptySince[uid] = now
		return now
	}
	return t
}

// busy forgets an Allocation's idle clock: it hosts a live Instance again.
func (m *memory) busy(uid types.UID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.emptySince, uid)
}

// classFailed records a failed Allocation in a class and returns the streak.
func (m *memory) classFailed(c v1alpha1.AcceleratorClass, at time.Time) []time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures[c] = append(m.failures[c], at)
	return append([]time.Time(nil), m.failures[c]...)
}

// classServed ends a class's failure streak: an Instance of it became ready.
func (m *memory) classServed(c v1alpha1.AcceleratorClass) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.failures, c)
}

func (m *memory) streak(c v1alpha1.AcceleratorClass) []time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]time.Time(nil), m.failures[c]...)
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

// forget unmarks a once-only key, so the event can fire again.
func (m *memory) forget(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.emitted, key)
}
