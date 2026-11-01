package main

import (
	"sync"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
)

// admissions is the plugin's Notifier in the controller: it remembers which Allocation's
// credential admitted each proxy, and passes every call on to the trigger that wakes the
// path-state writer. HBEP-8's TunnelOrphaned regards "the Allocation whose credential admitted the
// tunnel", and only the plugin's accepted NewProxy knows it. The map is in memory: after a restart
// it starts empty and the event regards the lifecycle Lease instead.
type admissions struct {
	trigger *frpsplugin.Trigger

	mu      sync.Mutex
	byProxy map[string]string // proxy name (an Instance's UID) -> Allocation UID
}

var _ frpsplugin.Notifier = (*admissions)(nil)

func newAdmissions(t *frpsplugin.Trigger) *admissions {
	return &admissions{trigger: t, byProxy: map[string]string{}}
}

// ProxyChanged implements frpsplugin.Notifier. It never blocks past a mutex held for one map
// write. An accepted NewProxy sets the proxy's Allocation (the latest admission wins, since a
// proxy re-registers with whichever credential its frpc logs in with); a CloseProxy changes
// nothing here, because the proxy stays an orphan candidate until a list read says it is gone
// (retain), and it names no Allocation.
func (a *admissions) ProxyChanged(op, proxyName, allocationUID string) {
	if op == frpsplugin.OpNewProxy && allocationUID != "" {
		a.mu.Lock()
		a.byProxy[proxyName] = allocationUID
		a.mu.Unlock()
	}
	a.trigger.ProxyChanged(op, proxyName, allocationUID)
}

// allocationOf returns the UID of the Allocation that admitted the proxy, when the plugin
// admitted it in this process.
func (a *admissions) allocationOf(proxy string) (string, bool) {
	if a == nil {
		return "", false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	uid, ok := a.byProxy[proxy]
	return uid, ok
}

// retain forgets every proxy not in keep.
func (a *admissions) retain(keep map[string]struct{}) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for name := range a.byProxy {
		if _, ok := keep[name]; !ok {
			delete(a.byProxy, name)
		}
	}
}
