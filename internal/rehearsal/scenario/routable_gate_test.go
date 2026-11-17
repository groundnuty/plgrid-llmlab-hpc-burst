package scenario_test

// The gate every scenario passes before its first request (#676, after #671's race).
//
// stack.Registered reads the front's tunnel: the Instance's transport is registered. The front places a
// request from its router's view, which is built from its own watch of the control plane and reads the same
// registration (Tunnel.Dialer, router.go hasTransport) together with the Instance's phase and conditions. A
// scenario that sends right after a Registered-only wait can meet the view before it holds the Instance
// eligible, and gets 503 model_inactive no_ready_target (the key search of #671). frontRoutes (#675) asks the
// front's own view through GET /v1/models; routedState puts it behind the registrations, and the rig's
// waitRouted and routedNow are the two forms a scenario uses: a wait of its own, or a step inside the wait
// it already has.
//
// What the gate proves. routable is a property of the MODEL (Router.Models: any eligible, unpaused Instance,
// or an attached managed entry), and the front reports nothing per Instance beyond the probe's registered
// flag, which is the registration routedState has already read. So the gate rules out no_ready_target and
// says nothing of WHICH Instance the ring places a key on once a replacement stands beside the primary. A
// scenario that needs a particular Instance settles that where it takes its key (stack.Client.KeyFor reads
// the Instance each usage record names), or in the record assertion of the request it sends with a key it
// already holds. Nothing here resends a request: a 503 is a result, not a state to wait out.

import (
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// registration is one Instance's registration at the front, as stack.Registered reports it.
type registration struct {
	name       string
	reg, known bool
}

// routedState says whether the front would place a request for the model now, and when not, why. Every
// Instance must be registered, and known to the front's probe; only then is routes asked, so that its GETs
// (each a usage record and a request count at the front) start once the registrations stand and are a
// handful, not one every 200 ms through a long wait.
func routedState(regs []registration, routes func() (bool, string)) (bool, string) {
	for _, g := range regs {
		switch {
		case !g.known:
			return false, "the front's probe does not report Instance " + g.name
		case !g.reg:
			return false, "the transport of Instance " + g.name + " is not registered"
		}
	}
	if ok, why := routes(); !ok {
		return false, "the front does not route the model yet: " + why
	}
	return true, ""
}

// routedNow is routedState for the Instances of the rig's own model (the client's, stack.Model), on this
// rig's front: the registrations are read from its /healthz, the model's routable flag from its client
// listener with the client key, the way startServing reads it. A scenario's own wait calls it as its last
// condition.
func (r *rig) routedNow(ins ...*v1alpha1.Instance) (bool, string) {
	r.t.Helper()
	return r.routedModelNow(r.fc.Model, ins...)
}

// routedModelNow is routedNow for another served model, as part 3's two deployments are.
func (r *rig) routedModelNow(model string, ins ...*v1alpha1.Instance) (bool, string) {
	r.t.Helper()
	var regs []registration
	for _, in := range ins {
		reg, known := stack.Registered(r.ports, in)
		regs = append(regs, registration{name: in.Name, reg: reg, known: known})
	}
	return routedState(regs, func() (bool, string) {
		return frontRoutes(r.fc.H.Ports.ClientAddr, r.fc.H.ClientKey, model)
	})
}

// waitRouted waits, to the usual deadline, until the Instances are registered and the front routes the
// model.
func (r *rig) waitRouted(what string, ins ...*v1alpha1.Instance) {
	r.t.Helper()
	names := make([]string, len(ins))
	for i, in := range ins {
		names[i] = in.Name
	}
	stack.WaitState(r.t, stack.WaitDeadline, what+": Instance "+strings.Join(names, ", ")+" registered and the front routing the model", func() (bool, string) {
		return r.routedNow(ins...)
	})
}

// routedState asks the front only after every registration stands, and says why it is not yet when one
// does not or the front does not route the model. A routes that is asked early, or its answer taken for
// the whole, is what each case here would turn red.
//
//livecheck:none a test of the rehearsal's readiness gate on stubs; nothing live runs
func TestRoutedStateAsksTheFrontOnlyOnceEveryRegistrationStands(t *testing.T) {
	yes := func() (bool, string) { return true, "" }
	no := func() (bool, string) { return false, "routable false" }
	cases := []struct {
		name      string
		regs      []registration
		routes    func() (bool, string)
		want      bool
		why       string // a substring of the reason when false
		wantAsked bool   // whether routes ran
	}{
		{"registered and routed", []registration{{"primary", true, true}}, yes, true, "", true},
		{"two Instances, both registered, routed", []registration{{"primary", true, true}, {"repl", true, true}}, yes, true, "", true},
		{"no Instance named: the front alone decides", nil, yes, true, "", true},
		{"registered, the front does not route: the gap of #671", []registration{{"primary", true, true}}, no, false, "does not route the model yet: routable false", true},
		{"not registered: the front is not asked", []registration{{"primary", false, true}}, yes, false, "transport of Instance primary is not registered", false},
		{"unknown to the probe: the front is not asked", []registration{{"primary", false, false}}, yes, false, "does not report Instance primary", false},
		{"the replacement is not registered: the front is not asked, whatever it would say", []registration{{"primary", true, true}, {"repl", false, true}}, yes, false, "transport of Instance repl is not registered", false},
		{"the primary is not registered: the front is not asked, with the replacement ready", []registration{{"primary", false, true}, {"repl", true, true}}, yes, false, "transport of Instance primary is not registered", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			asked := 0
			routes := func() (bool, string) {
				asked++
				return c.routes()
			}
			got, why := routedState(c.regs, routes)
			if got != c.want {
				t.Fatalf("routedState = %t (%q), want %t", got, why, c.want)
			}
			if (asked > 0) != c.wantAsked {
				t.Errorf("the front was asked %d times, want asked: %t", asked, c.wantAsked)
			}
			if c.want && why != "" {
				t.Errorf("a true answer carries the reason %q", why)
			}
			if !c.want && !strings.Contains(why, c.why) {
				t.Errorf("the reason %q does not say %q", why, c.why)
			}
		})
	}
}

// With the registrations standing, the gate is exactly frontRoutes: a double that lists the model not
// routable keeps it false with frontRoutes's own reason, and one that lists it routable opens it. The
// model name has a "/", as every LLMLab name does.
//
//livecheck:none a test of the rehearsal's readiness gate on a double; nothing live runs
func TestRoutedStateFollowsFrontRoutesOnceRegistered(t *testing.T) {
	const key = "client-key-0123456789"
	model := stack.Model
	standing := []registration{{"primary", true, true}}
	cases := []struct {
		name    string
		entries []modelsEntry
		want    bool
		why     string
	}{
		{"the model is routable", []modelsEntry{{model, true}}, true, ""},
		{"the model is listed not routable", []modelsEntry{{model, false}}, false, "routable false"},
		{"another model is routable", []modelsEntry{{"example-org/other", true}}, false, "does not list the model"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			addr := modelsDouble(t, key, c.entries...)
			got, why := routedState(standing, func() (bool, string) { return frontRoutes(addr, key, model) })
			if got != c.want {
				t.Fatalf("routedState = %t (%q), want %t", got, why, c.want)
			}
			if !c.want && !strings.Contains(why, c.why) {
				t.Errorf("the reason %q does not say %q", why, c.why)
			}
		})
	}
}
