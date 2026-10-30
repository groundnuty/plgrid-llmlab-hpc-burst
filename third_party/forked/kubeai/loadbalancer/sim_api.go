// New file, not from KubeAI (plgrid-llmlab-hpc-burst spike S10).
//
// It exports a thin handle on KubeAI's unexported endpoint group so that a
// simulator in another module can drive the copied algorithm directly. Every
// call goes through upstream code paths: reconcileEndpoints, removeEndpoint
// and getBestAddr. Nothing here re-implements a selection rule.
//
// SetSelectionHook is a test seam. It lets a test stop a selector between
// CHWBL's load check and the reservation, to force an interleaving of
// concurrent selectors without relying on timing.

package loadbalancer

import (
	"context"

	v1 "github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai/api/k8s/v1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai/internal/apiutils"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai/internal/metrics"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Group wraps KubeAI's endpoint group.
type Group struct {
	g *group
}

// NewGroup creates a group whose hash ring holds `replication` points per
// endpoint (KubeAI's default is 256).
func NewGroup(replication int) *Group {
	return &Group{g: newEndpointGroup(v1.LoadBalancing{PrefixHash: v1.PrefixHash{Replication: replication}})}
}

// SetEndpoints reconciles the group to exactly these endpoint names, as
// KubeAI's controller does on every pod change. The address equals the name.
func (G *Group) SetEndpoints(names []string) {
	observed := make(map[string]endpoint, len(names))
	for _, n := range names {
		observed[n] = endpoint{address: n}
	}
	G.g.reconcileEndpoints(observed)
}

// RemoveEndpoint stops new selections of one endpoint while its in-flight
// requests keep their counters. Upstream calls removeEndpoint for a pod
// already gone from the cache; a terminating pod leaves through
// reconcileEndpoints, which changes the ring and the counters the same way.
func (G *Group) RemoveEndpoint(name string) {
	G.g.removeEndpoint(name)
}

// cancelled is a context that is already done, so getBestAddr returns an
// error instead of blocking when the group has no endpoints. A simulator
// runs on one goroutine and cannot wait for endpoints to appear.
var cancelled = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// PickPrefixHash selects an endpoint with KubeAI's PrefixHash (CHWBL)
// strategy for `key` and `meanLoadPercentage` (125 means a bound of 1.25x the
// mean). It returns the endpoint's address and upstream's release function,
// which decrements that endpoint's in-flight counter and the group total.
func (G *Group) PickPrefixHash(key string, meanLoadPercentage int) (string, func(), error) {
	req := &apiutils.Request{
		Prefix: key,
		LoadBalancing: v1.LoadBalancing{
			Strategy:   v1.PrefixHashStrategy,
			PrefixHash: v1.PrefixHash{MeanLoadPercentage: meanLoadPercentage},
		},
	}
	return G.g.getBestAddr(cancelled, req, false)
}

// PickLeastLoad selects an endpoint with KubeAI's LeastLoad strategy.
func (G *Group) PickLeastLoad() (string, func(), error) {
	req := &apiutils.Request{LoadBalancing: v1.LoadBalancing{Strategy: v1.LeastLoadStrategy}}
	return G.g.getBestAddr(cancelled, req, false)
}

// TotalInFlight returns KubeAI's own group-wide in-flight counter. After
// RemoveEndpoint it still includes the removed endpoint's open requests.
func (G *Group) TotalInFlight() int64 {
	return G.g.totalInFlight.Load()
}

// InFlight returns KubeAI's in-flight counter for a current member.
func (G *Group) InFlight(name string) (int64, bool) {
	G.g.mtx.RLock()
	defer G.g.mtx.RUnlock()
	ep, ok := G.g.endpoints[name]
	if !ok {
		return 0, false
	}
	return ep.inFlight.Load(), true
}

// Members returns the number of current members (the n in the bound).
func (G *Group) Members() int {
	G.g.mtx.RLock()
	defer G.g.mtx.RUnlock()
	return len(G.g.endpoints)
}

// hookCounter is an OpenTelemetry counter whose Add calls a hook with the
// endpoint attribute it is given.
type hookCounter struct {
	noop.Int64Counter
	hook func(endpoint string)
}

func (c hookCounter) Add(_ context.Context, _ int64, opts ...metric.AddOption) {
	attrs := metric.NewAddConfig(opts).Attributes()
	v, _ := attrs.Value(metrics.AttrEndpoint)
	c.hook(v.AsString())
}

// SetSelectionHook makes every CHWBL selection call hook(endpoint) after the
// load check has chosen the endpoint and before getBestAddr increments its
// in-flight counter, while the group's read lock is held. It works through
// the instrument upstream records at exactly that point
// (metrics.InferenceRequestsHashLookupFinal), so no upstream line changes.
// The returned function restores the no-op instrument. The hook is global:
// tests that set it must not run in parallel.
func SetSelectionHook(hook func(endpoint string)) (restore func()) {
	prev := metrics.InferenceRequestsHashLookupFinal
	metrics.InferenceRequestsHashLookupFinal = hookCounter{hook: hook}
	return func() { metrics.InferenceRequestsHashLookupFinal = prev }
}
