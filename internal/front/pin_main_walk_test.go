package front

import (
	"fmt"
	"net/http"
	"slices"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai/loadbalancer"
)

// HBEP-5, Request path steps 5 and 6, as main routes them before any Workload
// exists: with several served ModelDeployments of one model, the front places
// all their routable Instances on one ring (KubeAI's, 256 points per Instance),
// and the key's home on it names the ModelDeployment; if that deployment has no
// Instance below its cap, the walk goes on to the next deployment on the ring;
// within the deployment, its own balancer group picks the Instance by the same
// kind of ring over its members alone.
//
// This test pins that behaviour BEFORE the Workload build touches the walk
// (route.go, chooseServed), so a later filter on the walk is proven against it.
// The oracle is the KubeAI fork's own group (third_party/forked/kubeai), which
// builds its ring itself: no expected value below comes from the front's
// balancer.Ring or Walk. The fork's pick at an unbounded load is a key's home
// on a ring of the names it is given.

// pwBound is a mean-load percentage so large that the fork's pick is the key's
// home (the balancer package's noBound).
const pwBound = 1 << 40

// pwHome is the key's home on the fork's own ring over names.
func pwHome(t *testing.T, names []string, key string) string {
	t.Helper()
	g := loadbalancer.NewGroup(256)
	g.SetEndpoints(names)
	addr, done, err := g.PickPrefixHash(key, pwBound)
	if err != nil {
		t.Fatalf("the fork's pick for %q over %v: %v", key, names, err)
	}
	done()
	return addr
}

// pwDep is one served ModelDeployment of a pwFix: its Instances' names, and
// its routing.instanceCap (0: the default, maxNumSeqs).
type pwDep struct {
	name      string
	instances []string
	cap       int32
}

// pwFix is a front with several served ModelDeployments of scModel on one
// batch Provider, each Instance on its own engine double.
type pwFix struct {
	*harness
	deps    []pwDep
	engines map[string]*enginetest.Engine // by the Instance's key, "ns/<name>"
}

// pwNames is the keys of every Instance of the given deployments, in the order
// given.
func pwNames(deps ...pwDep) []string {
	var out []string
	for _, d := range deps {
		for _, i := range d.instances {
			out = append(out, "ns/"+i)
		}
	}
	return out
}

func newPwFix(t *testing.T, deps ...pwDep) *pwFix {
	t.Helper()
	h := newHarness(t, nil)
	f := &pwFix{harness: h, deps: deps, engines: map[string]*enginetest.Engine{}}
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	for _, d := range deps {
		md := h.servedMD(d.name, scModel, int32(len(d.instances)), "helios-a")
		if d.cap > 0 {
			c := d.cap
			md.Spec.Routing = &v1alpha1.Routing{InstanceCap: &c}
		}
		for _, in := range d.instances {
			key := newKey(t)
			e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
			h.instance(d.name, in, "helios-a", e, key, instanceOpts{})
			f.engines["ns/"+in] = e
		}
	}
	h.apply()
	return f
}

// seen is how many requests each Instance's engine has received.
func (f *pwFix) seen() map[string]int {
	out := map[string]int{}
	for k, e := range f.engines {
		out[k] = len(e.Requests())
	}
	return out
}

// chat sends one non-streamed chat request under the session key and returns
// the Instance that answered it, once its usage record is written (the record
// is written after the reservation is released, so it is a barrier). Its extra
// headers are added.
func (f *pwFix) chat(t *testing.T, model, session string, hdr ...string) (string, *http.Response, string) {
	t.Helper()
	before := f.seen()
	resp := f.errPost(scChat, errChatBody(model, false, "hello there"), append([]string{"x-session-id", session}, hdr...)...)
	body := readAll(t, resp)
	if resp.StatusCode != 200 {
		return "", resp, body
	}
	f.record(resp.Header.Get("hpcburst-request-id"))
	var got string
	for k, n := range f.seen() {
		if n != before[k] {
			if got != "" {
				t.Fatalf("session %q reached two Instances: %s and %s", session, got, k)
			}
			got = k
		}
	}
	if got == "" {
		t.Fatalf("session %q reached no engine; the response said %q: %s", session, scInstanceOf(resp), body)
	}
	if h := scInstanceOf(resp); h != got {
		t.Errorf("session %q: the engine that saw it is %s, the attribution header says %q", session, got, h)
	}
	return got, resp, body
}

// pwSessions are the hand-picked conversation keys of these tests.
func pwSessions(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("pin-session-%02d", i)
	}
	return out
}

// pwDepOf is the ModelDeployment an Instance key belongs to.
func pwDepOf(deps []pwDep, inst string) string {
	for _, d := range deps {
		if slices.Contains(pwNames(d), inst) {
			return d.name
		}
	}
	return ""
}

// TestMainWalkPlacesEachSessionAtItsHomeOnTheForksRing: nothing at its cap.
// The deployment is the one of the Instance that is the key's home on the
// fork's ring over EVERY Instance of the model, and the Instance is that
// deployment's own home for the key, on the fork's ring over its Instances
// alone.
func TestMainWalkPlacesEachSessionAtItsHomeOnTheForksRing(t *testing.T) {
	a := pwDep{name: "md-a", instances: []string{"a-0"}}
	b := pwDep{name: "md-b", instances: []string{"b-0", "b-1"}}
	f := newPwFix(t, a, b)
	all := pwNames(a, b)
	homeDeps := map[string]int{}
	for _, s := range pwSessions(24) {
		d := pwDepOf(f.deps, pwHome(t, all, s))
		want := pwHome(t, pwNames(depNamed(f.deps, d)), s)
		homeDeps[d]++
		if got, _, body := f.chat(t, scModel, s); got != want {
			t.Errorf("session %q: reached %s, want %s (the fork's home over %v is in %s; over that deployment's Instances it is %s): %s", s, got, want, all, d, want, body)
		}
	}
	// The sessions must exercise both deployments, or the walk's first step
	// was never tested.
	if homeDeps["md-a"] == 0 || homeDeps["md-b"] == 0 {
		t.Fatalf("the 24 sessions are homed on %v, want both deployments", homeDeps)
	}
}

func depNamed(deps []pwDep, name string) pwDep {
	for _, d := range deps {
		if d.name == name {
			return d
		}
	}
	return pwDep{}
}

// TestMainWalkSpillsFromAFullDeploymentToTheSecond: the first deployment's only
// Instance, a-0, holds routing.instanceCap 1 and one request held open, so it is
// at its cap. Every other session then goes to the second deployment, md-b,
// whichever deployment is its home, and within md-b to the home the fork's ring
// over b-0 and b-1 alone gives. The sessions homed on a-0 are the spill; the
// rest are the control that md-b still takes its own.
func TestMainWalkSpillsFromAFullDeploymentToTheSecond(t *testing.T) {
	a := pwDep{name: "md-a", instances: []string{"a-0"}, cap: 1}
	b := pwDep{name: "md-b", instances: []string{"b-0", "b-1"}}
	f := newPwFix(t, a, b)
	all := pwNames(a, b)
	sessions := pwSessions(24)

	// Hold one request on a-0: a session whose home over all three is a-0.
	var holder string
	for _, s := range sessions {
		if pwHome(t, all, s) == "ns/a-0" {
			holder = s
			break
		}
	}
	if holder == "" {
		t.Fatal("none of the sessions is homed on a-0")
	}
	hold := make(chan struct{})
	f.engines["ns/a-0"].Set(enginetest.Behaviour{Hold: hold})
	held := scGo(f.harness, errChatBody(scModel, false, "hello there"), "x-session-id", holder)
	errWaitFor(t, "the held request to reach a-0", func() bool { return f.engines["ns/a-0"].Open() == 1 })
	f.engines["ns/a-0"].Set(enginetest.Behaviour{}) // later requests would not be held, but none reaches it

	spilled, native := 0, 0
	for _, s := range sessions {
		if s == holder {
			continue
		}
		want := pwHome(t, pwNames(b), s)
		if pwHome(t, all, s) == "ns/a-0" {
			spilled++
		} else {
			native++
		}
		if got, _, body := f.chat(t, scModel, s); got != want {
			t.Errorf("session %q (its home over %v is %s): reached %s with md-a full, want %s (the fork's home over md-b's Instances): %s", s, all, pwHome(t, all, s), got, want, body)
		}
	}
	if spilled == 0 || native == 0 {
		t.Fatalf("%d sessions were homed on the full a-0 and %d on md-b, want some of each", spilled, native)
	}
	if n := len(f.engines["ns/a-0"].Requests()); n != 1 {
		t.Errorf("a-0 saw %d requests, want only the held one", n)
	}

	close(hold)
	resp, body := held.wait(t)
	if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" {
		t.Errorf("the held request: status %d from %q, want 200 from ns/a-0: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
}

// TestMainWalkSpillOrderFollowsTheForksRingAmongThreeDeployments: with three
// deployments and the first on the walk full, a request goes to the SECOND on
// the walk, not to the one that happens to be listed first. The order is the
// fork's: the home among every Instance, then the home among those left once
// the full deployment's Instance is taken out of the ring.
func TestMainWalkSpillOrderFollowsTheForksRingAmongThreeDeployments(t *testing.T) {
	a := pwDep{name: "md-a", instances: []string{"a-0"}, cap: 1}
	b := pwDep{name: "md-b", instances: []string{"b-0"}}
	c := pwDep{name: "md-c", instances: []string{"c-0"}}
	f := newPwFix(t, a, b, c)
	all := pwNames(a, b, c)
	sessions := pwSessions(60)
	var holder string
	for _, s := range sessions {
		if pwHome(t, all, s) == "ns/a-0" {
			holder = s
			break
		}
	}
	if holder == "" {
		t.Fatal("none of the sessions is homed on a-0")
	}
	hold := make(chan struct{})
	f.engines["ns/a-0"].Set(enginetest.Behaviour{Hold: hold})
	held := scGo(f.harness, errChatBody(scModel, false, "hello there"), "x-session-id", holder)
	errWaitFor(t, "the held request to reach a-0", func() bool { return f.engines["ns/a-0"].Open() == 1 })
	f.engines["ns/a-0"].Set(enginetest.Behaviour{})

	second := map[string]int{}
	for _, s := range sessions {
		if s == holder || pwHome(t, all, s) != "ns/a-0" {
			continue
		}
		// a-0 is the home and is full: the next on the fork's walk is the home
		// of the ring without it.
		want := pwHome(t, pwNames(b, c), s)
		second[want]++
		if got, _, body := f.chat(t, scModel, s); got != want {
			t.Errorf("session %q homed on the full a-0: reached %s, want %s (the fork's home over %v): %s", s, got, want, pwNames(b, c), body)
		}
	}
	if second["ns/b-0"] == 0 || second["ns/c-0"] == 0 {
		t.Fatalf("the spilled sessions' second choices are %v, want both md-b and md-c", second)
	}
	close(hold)
	if resp, body := held.wait(t); resp.StatusCode != 200 {
		t.Errorf("the held request: status %d: %s", resp.StatusCode, body)
	}
}
