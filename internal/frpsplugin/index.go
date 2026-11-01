package frpsplugin

import (
	"context"
	"crypto/subtle"
	"fmt"
	"sync/atomic"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Index is what the Handler decides from. Every method answers from memory: the Handler gives each
// call a context that expires within its deadline, and refuses when the call has not returned by
// then, so an implementation that waits on the API server only makes frpc retry.
type Index interface {
	// Synced reports whether the Index has loaded the Allocations and Instances once. Until it
	// has, the Handler refuses every Login and NewProxy (HBEP-6: a login accepted too early could
	// be followed by a NewProxy that must be refused, and frpc retries a refused proxy only every
	// 30 s).
	Synced() bool
	// AllocationForToken returns the UID of the Allocation whose credential is token, when that
	// Allocation exists and has not ended. The comparison must take the same time whatever the
	// token (HBEP-6: constant time). The Handler passes only tokens of a key's form.
	AllocationForToken(ctx context.Context, token string) (allocationUID string, ok bool)
	// InstanceOnAllocation reports whether instanceUID is the UID of an Instance placed on the
	// Allocation that has not ended.
	InstanceOnAllocation(ctx context.Context, allocationUID, instanceUID string) bool
}

// Notifier hears the plugin's calls as triggers to re-read frps's proxy list (HBEP-6: about a second
// after each NewProxy and CloseProxy). ProxyChanged must not block; the Handler calls it inside
// its deadline and does not wait for it past that. op is "NewProxy" (accepted) or "CloseProxy".
//
// allocationUID is the UID of the Allocation whose credential admitted the proxy, on an accepted
// NewProxy: the Handler has it from the same Index lookup that admitted the call, so a listener can
// say which Allocation's tunnel a proxy came through (HBEP-8: TunnelOrphaned regards it). It is ""
// on CloseProxy, which carries no token and is a trigger only.
type Notifier interface {
	ProxyChanged(op, proxyName, allocationUID string)
}

// Feeder is how the controller fills an Index.
type Feeder interface {
	// Replace swaps in the whole current state at once. It refuses a snapshot that is not
	// well-formed and then keeps the old state.
	Replace(Snapshot) error
	// MarkSynced records that the controller's caches have synced, so that the Index answers.
	// Call it after the first Replace.
	MarkSynced()
}

// Snapshot is the state the Index answers from: the Allocations that may log in, each with the
// Instances placed on it. The feeder lists only Allocations that exist and have not ended, and only
// Instances that have not ended; an ended one is simply absent.
type Snapshot struct {
	Allocations []AllocationEntry
}

// AllocationEntry is one live Allocation.
type AllocationEntry struct {
	UID string
	// Credential is the Allocation's credential, read from the controller's mode-600 file
	// (agentapi.ReadKeyFile). Its form is a key's (agentapi.CheckKey).
	Credential string
	// Instances are the UIDs of the Instances placed on this Allocation.
	Instances []string
}

// MemIndex is the Index and the Feeder in one: an immutable snapshot swapped atomically, so a lookup
// never waits for a writer.
type MemIndex struct {
	synced atomic.Bool
	state  atomic.Pointer[indexState]
}

var (
	_ Index  = (*MemIndex)(nil)
	_ Feeder = (*MemIndex)(nil)
)

type indexState struct {
	allocs []allocState
	byUID  map[string]int
}

type allocState struct {
	uid       string
	cred      []byte
	instances map[string]struct{}
}

// Replace implements Feeder. A snapshot is refused when an Allocation has no UID or a credential
// that is not a key's form, when two Allocations share a UID or a credential, or when an Instance
// UID is empty or listed under two Allocations: each would make an admission ambiguous. The
// errors never contain a credential.
func (m *MemIndex) Replace(s Snapshot) error {
	st := &indexState{byUID: make(map[string]int, len(s.Allocations))}
	creds := make(map[string]string, len(s.Allocations))
	owner := map[string]string{} // Instance UID to Allocation UID
	for _, a := range s.Allocations {
		if a.UID == "" {
			return fmt.Errorf("index: an Allocation has no UID")
		}
		if err := agentapi.CheckKey([]byte(a.Credential)); err != nil {
			return fmt.Errorf("index: the credential of Allocation %q: %w", a.UID, err)
		}
		if _, dup := st.byUID[a.UID]; dup {
			return fmt.Errorf("index: Allocation %q is listed twice", a.UID)
		}
		if other, dup := creds[a.Credential]; dup {
			return fmt.Errorf("index: Allocations %q and %q have the same credential", other, a.UID)
		}
		creds[a.Credential] = a.UID
		as := allocState{uid: a.UID, cred: []byte(a.Credential), instances: make(map[string]struct{}, len(a.Instances))}
		for _, in := range a.Instances {
			if in == "" {
				return fmt.Errorf("index: Allocation %q lists an Instance with no UID", a.UID)
			}
			if other, dup := owner[in]; dup {
				return fmt.Errorf("index: Instance %q is listed under Allocations %q and %q", in, other, a.UID)
			}
			owner[in] = a.UID
			as.instances[in] = struct{}{}
		}
		st.byUID[a.UID] = len(st.allocs)
		st.allocs = append(st.allocs, as)
	}
	m.state.Store(st)
	return nil
}

// MarkSynced implements Feeder.
func (m *MemIndex) MarkSynced() { m.synced.Store(true) }

// Synced implements Index.
func (m *MemIndex) Synced() bool { return m.synced.Load() }

// AllocationForToken implements Index. It compares the token with every credential, in constant
// time each and with no early exit, so the time does not depend on which credential matches, or
// whether one does (it depends only on how many Allocations there are, which is not a secret).
func (m *MemIndex) AllocationForToken(_ context.Context, token string) (string, bool) {
	st := m.state.Load()
	if st == nil {
		return "", false
	}
	tb := []byte(token)
	found, hit := 0, 0
	for i := range st.allocs {
		eq := subtle.ConstantTimeCompare(tb, st.allocs[i].cred)
		found = subtle.ConstantTimeSelect(eq, i, found)
		hit |= eq
	}
	if hit == 0 {
		return "", false
	}
	return st.allocs[found].uid, true
}

// InstanceOnAllocation implements Index.
func (m *MemIndex) InstanceOnAllocation(_ context.Context, allocationUID, instanceUID string) bool {
	st := m.state.Load()
	if st == nil {
		return false
	}
	i, ok := st.byUID[allocationUID]
	if !ok {
		return false
	}
	_, ok = st.allocs[i].instances[instanceUID]
	return ok
}

// Trigger is a Notifier that coalesces: any number of calls before the controller drains it leave
// one pending signal on C. The controller waits about a second after a signal, and reads the list
// then (HBEP-6, Path state), besides its read every 10 s.
type Trigger struct {
	c chan struct{}
}

var _ Notifier = (*Trigger)(nil)

// NewTrigger returns a Trigger.
func NewTrigger() *Trigger { return &Trigger{c: make(chan struct{}, 1)} }

// ProxyChanged implements Notifier. It never blocks.
func (t *Trigger) ProxyChanged(string, string, string) {
	select {
	case t.c <- struct{}{}:
	default:
	}
}

// C delivers the pending signal, if any.
func (t *Trigger) C() <-chan struct{} { return t.c }
