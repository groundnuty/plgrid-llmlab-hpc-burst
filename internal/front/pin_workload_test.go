package front

import (
	"fmt"
	"slices"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// HBEP-5, Workloads, Who serves it and Pinning; the science agent's ruling on #764, Q7. A request that names a Workload resolves only to the
// ModelDeployments in the Workload's own namespace that list it or list none; steps 5 and 6 then run on that smaller set, so the conversation key
// is placed as on a ring of only the serving deployments' Instances. The front does it by walking the model's one ring and skipping the Instances
// of deployments that do not serve (route.go, chooseServed).
//
// The oracle is the KubeAI fork's own group, as pin_main_walk_test.go's is: pwHome builds the ring itself, over the names it is given. No expected
// value here comes from the front's balancer.Ring or Walk. A test that computed both sides with the front's own Walk would prove nothing.

// pinDeps are five deployments of one model, the layout of the pinning tests:
//
//	md-a  a-0 a-1  lists alpha          (serves alpha)
//	md-b  b-0 b-1  lists beta           (serves beta, not alpha)
//	md-c  c-0 c-1  lists alpha          (serves alpha)
//	md-d  d-0      lists none           (serves every Workload of its namespace)
//	md-e  e-0      lists none, in "ns-two": serves no Workload of namespace "ns"
func pinDeps() []pwDep {
	return []pwDep{
		{name: "md-a", instances: []string{"a-0", "a-1"}, workloads: []string{"alpha"}},
		{name: "md-b", instances: []string{"b-0", "b-1"}, workloads: []string{"beta"}},
		{name: "md-c", instances: []string{"c-0", "c-1"}, workloads: []string{"alpha"}},
		{name: "md-d", instances: []string{"d-0"}},
		{name: "md-e", instances: []string{"e-0"}, ns: "ns-two"},
	}
}

func newPinFix(t *testing.T) *pwFix {
	t.Helper()
	f := newPwFix(t, pinDeps()...)
	f.workload("ns", "alpha", scModel)
	f.workload("ns", "beta", scModel)
	f.apply()
	return f
}

// pinSessions are 40 hand-picked conversation keys.
func pinSessions() []string {
	out := make([]string, 40)
	for i := range out {
		out[i] = fmt.Sprintf("pin-key-%02d", i)
	}
	return out
}

// servingNames are the Instances of the deployments of deps that serve the Workload of namespace "ns" (HBEP-5, Workloads): the oracle's own
// statement of the rule, over the layout above.
func servingNames(deps []pwDep, workload string) []string {
	var out []string
	for _, d := range deps {
		if d.space() == "ns" && (len(d.workloads) == 0 || slices.Contains(d.workloads, workload)) {
			out = append(out, pwNames(d)...)
		}
	}
	return out
}

// Every session of a Workload lands where the fork's own ring puts it among ONLY the serving deployments' Instances; a request that names no
// Workload is routed as before, on the ring over all of them. The sessions include ones whose home on the full ring is an Instance that does not
// serve the Workload, or one in another namespace, so the filter is exercised: without it they would land there.
func TestAWorkloadsSessionsLandAtTheirHomeOnTheForksRingOverOnlyTheServingInstances(t *testing.T) {
	f := newPinFix(t)
	all := pwNames(f.deps...)
	sessions := pinSessions()
	for _, workload := range []string{"alpha", "beta"} {
		serving := servingNames(f.deps, workload)
		if len(serving) == 0 || len(serving) >= len(all) {
			t.Fatalf("the serving set of %s is %v of %v", workload, serving, all)
		}
		offServing, lands := 0, map[string]int{}
		for _, s := range sessions {
			want := pwHome(t, serving, s)
			if !slices.Contains(serving, pwHome(t, all, s)) {
				offServing++
			}
			got, _, body := f.chat(t, scModel+"#"+workload, s)
			lands[got]++
			if got != want {
				t.Errorf("Workload %s, session %q: reached %s, want %s, the fork's home over %v: %s", workload, s, got, want, serving, body)
			}
			if !slices.Contains(serving, got) {
				t.Errorf("Workload %s, session %q: reached %s, which does not serve it", workload, s, got)
			}
		}
		// The guard: the filter was tested (sessions whose full-ring home is outside the serving set), and the serving set was used widely.
		if offServing < 8 {
			t.Fatalf("Workload %s: only %d of %d sessions have their home on the full ring outside the serving set, want at least 8: the filter is barely exercised", workload, offServing, len(sessions))
		}
		if len(lands) < 3 {
			t.Fatalf("Workload %s: the sessions reached only %v, want at least 3 Instances", workload, lands)
		}
	}
	// A request that names no Workload is placed on the ring over all five deployments: nothing is filtered.
	for _, s := range sessions {
		if got, _, body := f.chat(t, scModel, s); got != pwHome(t, all, s) {
			t.Errorf("no Workload, session %q: reached %s, want %s, the fork's home over all %d Instances: %s", s, got, pwHome(t, all, s), len(all), body)
		}
	}
}

// The header names the Workload as the suffix does: the same Instance for the same session.
func TestAWorkloadNamedByHeaderIsPinnedLikeOneNamedBySuffix(t *testing.T) {
	f := newPinFix(t)
	serving := servingNames(f.deps, "alpha")
	for _, s := range pinSessions()[:24] {
		got, _, body := f.chat(t, scModel, s, wlHeader, "alpha")
		if want := pwHome(t, serving, s); got != want {
			t.Errorf("session %q by header: reached %s, want %s: %s", s, got, want, body)
		}
	}
}

// With the Workload's first deployment on the walk at its cap, the request goes on to the next serving deployment on the fork's walk: the home
// on the ring of the serving Instances that are not full. md-a is full here (a cap of 1 on each of its two Instances, one request held on each),
// so a session whose home over the serving Instances is in md-a goes to the home among md-c's and md-d's, and md-b and md-e take nothing.
func TestAWorkloadSpillsToTheNextServingDeploymentOnTheForksWalk(t *testing.T) {
	deps := pinDeps()
	deps[0].cap = 1 // md-a
	f := newPwFix(t, deps...)
	f.workload("ns", "alpha", scModel)
	f.workload("ns", "beta", scModel)
	f.apply()
	serving := servingNames(f.deps, "alpha")
	notFull := servingNames([]pwDep{deps[2], deps[3]}, "alpha") // md-c and md-d
	sessions := pinSessions()

	// One request held on each of md-a's two Instances: the session that is home on each, over the serving ring.
	holds := map[string]chan struct{}{}
	var calls []*scCall
	for _, inst := range []string{"ns/a-0", "ns/a-1"} {
		var holder string
		for _, s := range sessions {
			if pwHome(t, serving, s) == inst {
				holder = s
				break
			}
		}
		if holder == "" {
			t.Fatalf("no session is home on %s over %v", inst, serving)
		}
		hold := make(chan struct{})
		holds[inst] = hold
		f.engines[inst].Set(enginetest.Behaviour{Hold: hold})
		calls = append(calls, scGo(f.harness, errChatBody(scModel+"#alpha", false, "hello there"), "x-session-id", holder))
		errWaitFor(t, "the held request to reach "+inst, func() bool { return f.engines[inst].Open() == 1 })
		f.engines[inst].Set(enginetest.Behaviour{})
	}

	spilled := 0
	for _, s := range sessions {
		want := pwHome(t, notFull, s) // md-a is full, so the walk skips it
		if slices.Contains(pwNames(deps[0]), pwHome(t, serving, s)) {
			spilled++
		} else if want != pwHome(t, serving, s) {
			t.Fatalf("session %q: the fork's home over %v is %s but over %v it is %s: a session not homed in md-a must not move", s, serving, pwHome(t, serving, s), notFull, want)
		}
		if got, _, body := f.chat(t, scModel+"#alpha", s); got != want {
			t.Errorf("session %q with md-a full: reached %s, want %s, the fork's home over %v: %s", s, got, want, notFull, body)
		}
	}
	if spilled < 5 {
		t.Fatalf("%d sessions were homed in the full md-a, want at least 5", spilled)
	}
	for _, inst := range []string{"ns/b-0", "ns/b-1", "ns-two/e-0"} {
		if n := len(f.engines[inst].Requests()); n != 0 {
			t.Errorf("%s, which does not serve alpha, saw %d requests", inst, n)
		}
	}
	for _, hold := range holds {
		close(hold)
	}
	for _, c := range calls {
		if resp, body := c.wait(t); resp.StatusCode != 200 {
			t.Errorf("a held request: status %d: %s", resp.StatusCode, body)
		}
	}
}

// A ModelDeployment that lists another Workload is never chosen, one that lists none serves all, and one in another namespace serves none, even
// with an empty list: the three rules of the serving set, each by the Instances that took no request and the ones that took some.
func TestTheServingSetIsTheListedOrEmptyListedInTheWorkloadsNamespace(t *testing.T) {
	f := newPinFix(t)
	took := map[string]int{}
	for _, s := range pinSessions() {
		got, _, _ := f.chat(t, scModel+"#alpha", s)
		took[got]++
	}
	for _, inst := range []string{"ns/b-0", "ns/b-1"} {
		if took[inst] != 0 {
			t.Errorf("%s of md-b, which lists only beta, took %d requests for alpha", inst, took[inst])
		}
	}
	if took["ns-two/e-0"] != 0 {
		t.Errorf("ns-two/e-0, in another namespace with an empty list, took %d requests for alpha", took["ns-two/e-0"])
	}
	if took["ns/d-0"] == 0 {
		t.Errorf("ns/d-0, whose list is empty, took none of the %d requests for alpha: an empty list serves every Workload (%v)", len(pinSessions()), took)
	}
	if took["ns/a-0"]+took["ns/a-1"] == 0 || took["ns/c-0"]+took["ns/c-1"] == 0 {
		t.Errorf("the listed deployments md-a and md-c took %v, want both used", took)
	}
	// And beta: md-b serves it, md-a and md-c do not.
	took = map[string]int{}
	for _, s := range pinSessions() {
		got, _, _ := f.chat(t, scModel, s, wlHeader, "beta")
		took[got]++
	}
	for _, inst := range []string{"ns/a-0", "ns/a-1", "ns/c-0", "ns/c-1", "ns-two/e-0"} {
		if took[inst] != 0 {
			t.Errorf("%s took %d requests for beta, which it does not serve", inst, took[inst])
		}
	}
	if took["ns/b-0"]+took["ns/b-1"] == 0 {
		t.Errorf("md-b, which lists beta, took none: %v", took)
	}
}

// The managed fallback ring uses only the managed targets that serve the Workload (HBEP-5, Workloads, Pinning; Who serves it: "That holds for
// managed-source ModelDeployments too"). Three managed-source deployments of one model name three catalog entries of one managed Provider:
// m1 lists alpha, m2 lists beta, m3 lists none, and m4, in another namespace, lists none. A session of alpha lands on the fork's home over m1's
// and m3's targets alone.
func TestAWorkloadsManagedSessionsLandOnTheForksRingOverOnlyTheServingTargets(t *testing.T) {
	const upstreamKey = "llmlab-upstream-key-for-workload-tests-5e2d"
	cats := map[string]string{"cat-1": "ns/m1@llmlab-a", "cat-2": "ns/m2@llmlab-a", "cat-3": "ns/m3@llmlab-a", "cat-4": "ns-two/m4@llmlab-a"}
	h := newHarness(t, nil)
	llm := llmlabtest.Start(t, llmlabtest.Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{upstreamKey},
		Models: []llmlabtest.Model{{ID: "cat-1"}, {ID: "cat-2"}, {ID: "cat-3"}, {ID: "cat-4"}}})
	h.profile("llmlab", v1alpha1.PlatformManaged, "", "")
	h.managedProvider("llmlab-a", "llmlab", "llmlab", llm.BaseURL(), upstreamKey, "cat-1", "cat-2", "cat-3", "cat-4")
	h.managedMDIn("ns", "m1", scModel, "llmlab", "cat-1", "llmlab-a").Spec.Workloads = []string{"alpha"}
	h.managedMDIn("ns", "m2", scModel, "llmlab", "cat-2", "llmlab-a").Spec.Workloads = []string{"beta"}
	h.managedMDIn("ns", "m3", scModel, "llmlab", "cat-3", "llmlab-a")
	h.managedMDIn("ns-two", "m4", scModel, "llmlab", "cat-4", "llmlab-a")
	h.workload("ns", "alpha", scModel)
	h.workload("ns", "beta", scModel)
	h.apply()

	f := &pwFix{harness: h}
	sessions := pinSessions()
	for _, tc := range []struct {
		workload string
		serving  []string
	}{
		{"alpha", []string{"ns/m1@llmlab-a", "ns/m3@llmlab-a"}},
		{"beta", []string{"ns/m2@llmlab-a", "ns/m3@llmlab-a"}},
	} {
		all := []string{"ns/m1@llmlab-a", "ns/m2@llmlab-a", "ns/m3@llmlab-a", "ns-two/m4@llmlab-a"}
		offServing, hit := 0, map[string]int{}
		for _, s := range sessions {
			want := pwHome(t, tc.serving, s)
			if !slices.Contains(tc.serving, pwHome(t, all, s)) {
				offServing++
			}
			before := len(llm.Requests())
			resp := f.errPost(scChat, errChatBody(scModel+"#"+tc.workload, false, "hello there"), "x-session-id", s)
			body := readAll(t, resp)
			if resp.StatusCode != 200 || scEmitterOf(resp) != "provider" {
				t.Fatalf("%s, session %q: status %d emitter %q, want 200 from a provider: %s", tc.workload, s, resp.StatusCode, scEmitterOf(resp), body)
			}
			reqs := llm.Requests()
			if len(reqs) != before+1 {
				t.Fatalf("%s, session %q: LLMLab double logged %d new requests, want 1", tc.workload, s, len(reqs)-before)
			}
			got := cats[reqs[len(reqs)-1].Model]
			hit[got]++
			if got != want {
				t.Errorf("%s, session %q: reached %s (catalog %q), want %s, the fork's home over the serving targets %v", tc.workload, s, got, reqs[len(reqs)-1].Model, want, tc.serving)
			}
		}
		if offServing < 6 {
			t.Fatalf("%s: only %d sessions are homed outside the serving targets on the full ring, want at least 6", tc.workload, offServing)
		}
		if len(hit) != 2 {
			t.Fatalf("%s: the sessions reached %v, want both serving targets", tc.workload, hit)
		}
	}
}
