package front

import (
	"context"
	"net/http"
	"testing"

	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// The fixture of the Workload route-order, managed-count and FIFO tests (HBEP-764's PR 5; HBEP-5, Workloads, Route order and Waiting): a front with one
// served ModelDeployment of scModel, ns/md-a, and one managed-source ModelDeployment of scModel, ns/md-llm, behind an LLMLab double, and the Workload
// ns/alpha of scModel. Every request is held where the test wants it: ours on the engine doubles (holdOurs) and the managed ones at the LLMLab double
// (stallManaged), and a request is cut by its client's cancel, which completes it on its target. The rows count where requests arrived, from the doubles'
// own counts, never from a window.

const (
	wrCatalog = "llmlab-example-27b"
	wrKey     = "llmlab-upstream-key-for-workload-route-tests-3c71"
)

// wrOpts are the settings of a wrFix. The zero value is md-a with one Instance, a-0, at routing.instanceCap 100, alpha OwnFirst with no managedSessions,
// and the managed target, with nothing held.
type wrOpts struct {
	instances       []string // of md-a; default a-0
	cap             int32    // routing.instanceCap of each; default 100
	holdOurs        bool     // the engine doubles hold every request until freeOurs
	stallManaged    bool     // the LLMLab double holds every request until unstallManaged
	managedFirst    bool     // alpha's routeOrder is ManagedFirst
	managedSessions int32    // alpha's managedSessions; 0 sets none
	noManaged       bool     // no managed-source ModelDeployment
	edit            func(*Config)
}

type wrFix struct {
	*pwFix
	llm   *llmlabtest.Server
	hold  chan struct{}
	freed bool
}

func newWrFix(t *testing.T, o wrOpts) *wrFix {
	t.Helper()
	if len(o.instances) == 0 {
		o.instances = []string{"a-0"}
	}
	if o.cap == 0 {
		o.cap = 100
	}
	f := &wrFix{pwFix: newPwFixOn(t, o.edit, pwDep{name: "md-a", instances: o.instances, cap: o.cap}), hold: make(chan struct{})}
	if o.holdOurs {
		for _, in := range o.instances {
			f.engines["ns/"+in].Set(enginetest.Behaviour{Hold: f.hold})
		}
	}
	t.Cleanup(f.freeOurs)
	if !o.noManaged {
		f.llm = llmlabtest.Start(t, llmlabtest.Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{wrKey}, Models: []llmlabtest.Model{{ID: wrCatalog}}})
		f.profile("llmlab", v1alpha1.PlatformManaged, "", "")
		f.managedProvider("llm-a", "llmlab", "llmlab", f.llm.BaseURL(), wrKey, wrCatalog)
		f.managedMD("md-llm", scModel, "llmlab", wrCatalog, "llm-a")
		if o.stallManaged {
			f.llm.Stall(true)
			t.Cleanup(func() { f.llm.Stall(false) })
		}
	}
	w := f.workload("ns", "alpha", scModel)
	w.Spec.Shape.ContextTokens.Typical, w.Spec.Shape.OutputTokens.Typical = 100, 25
	if o.managedFirst {
		w.Spec.RouteOrder = v1alpha1.RouteManagedFirst
	}
	if o.managedSessions > 0 {
		w.Spec.ManagedSessions = ptr.To(o.managedSessions)
	}
	f.apply()
	return f
}

// freeOurs lets every request held on our engines finish.
func (f *wrFix) freeOurs() {
	if !f.freed {
		f.freed = true
		close(f.hold)
	}
}

// unstallManaged lets every request held at the LLMLab double go on.
func (f *wrFix) unstallManaged() { f.llm.Stall(false) }

// start sends one chat request naming model under the session key on its own goroutine, and returns it with the cancel that cuts it.
func (f *wrFix) start(model, session string, hdr ...string) (*scCall, context.CancelFunc) {
	f.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f.t.Cleanup(cancel)
	c := tbCall(f.harness, ctx, errChatBody(model, false, "hello there"), append([]string{"x-session-id", session}, hdr...)...)
	return c, cancel
}

// alpha is the model name that names the Workload.
func (f *wrFix) alpha() string { return scModel + "#alpha" }

// arrivedManaged is how many chat requests have reached the LLMLab double.
func (f *wrFix) arrivedManaged() int { return f.llm.Arrived(http.MethodPost, scChat) }

// openOurs is how many requests are open on our engines.
func (f *wrFix) openOurs() int {
	n := 0
	for _, e := range f.engines {
		n += e.Open()
	}
	return n
}

// waiting is the requests waiting at the front, in a group's queue or a Workload's FIFO.
func (f *wrFix) waiting() int { return f.router.Status().Waiting }

// settled waits until exactly managed requests have reached the LLMLab double, open are open on our engines, and waiting wait at the front.
func (f *wrFix) settled(managed, open, waiting int) {
	f.t.Helper()
	errWaitFor(f.t, "the requests to settle", func() bool {
		return f.arrivedManaged() == managed && f.openOurs() == open && f.waiting() == waiting
	})
}

// wlState is the router's state of ns/alpha, or nil when it has none.
func (f *wrFix) wlState() *wlState {
	f.router.wlMu.Lock()
	defer f.router.wlMu.Unlock()
	return f.router.wls["ns/alpha"]
}

// managedCount is alpha's count of requests in flight on managed targets, read under its mutex.
func (f *wrFix) managedCount() int {
	ws := f.wlState()
	if ws == nil {
		return 0
	}
	ws.lock()
	defer ws.mu.Unlock()
	return ws.managed
}
