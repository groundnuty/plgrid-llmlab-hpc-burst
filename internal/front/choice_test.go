package front

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HBEP-5, Test plan, Choice under model: with our deployment ready, LLMLab
// gets nothing; with every Instance at its cap, the next request goes to
// LLMLab; with no Instance ready, everything does. A conversation spilled to
// LLMLab returns home at its next turn once there is room. A served
// ModelDeployment whose provider selector matches the LLMLab provider's
// labels sends it nothing; only a managed-source ModelDeployment attaches it.
//
// Every "LLMLab gets nothing" below is paired with a request that does reach
// LLMLab through the same fixture and the same counter, so a zero means the
// request did not go there, not that the counter counts nothing.

const (
	chUpstreamKey = "llmlab-upstream-key-for-choice-tests-3b9c"
	chCatalog     = "llmlab-example-27b" // the name LLMLab lists for the managed entry
)

// chFix is an scFix with one Instance behind a ModelDeployment of scModel,
// and a managed-source ModelDeployment of the same model that attaches the
// LLMLab double through the Provider llmlab-a.
type chFix struct {
	*scFix
	llm *llmlabtest.Server
}

// newChFix builds it; instanceCap, when above 0, is the served deployment's
// routing.instanceCap.
func newChFix(t *testing.T, instanceCap int32) *chFix {
	t.Helper()
	o := scOpts{}
	if instanceCap > 0 {
		o.md = func(md *v1alpha1.ModelDeployment) { md.Spec.Routing = &v1alpha1.Routing{InstanceCap: &instanceCap} }
	}
	f := &chFix{scFix: newScFix(t, o)}
	f.llm = llmlabtest.Start(t, llmlabtest.Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{chUpstreamKey},
		Models: []llmlabtest.Model{{ID: chCatalog}}})
	f.profile("llmlab", v1alpha1.PlatformManaged, "", "")
	f.managedProvider("llmlab-a", "llmlab", "llmlab", f.llm.BaseURL(), chUpstreamKey, chCatalog)
	f.managedMD("md-llm", scModel, "llmlab", chCatalog, "llmlab-a")
	f.apply()
	return f
}

// llmChats is how many chat requests LLMLab has answered.
// llmChats counts chat requests the moment they reach LLMLab, answered or
// not, so an absence check also sees a held or slow one (llmlabtest.Arrived).
func (f *chFix) llmChats() int { return f.llm.Arrived(http.MethodPost, scChat) }

// notReady turns the one Instance into one that is not ready.
func (f *chFix) notReady() {
	f.inst(f.uids[0]).Status.Phase = v1alpha1.InstanceVerifying
	f.apply()
}

// wantInstance asserts a response came from our Instance.
func (f *chFix) wantInstance(t *testing.T, what string, resp *http.Response, body string) {
	t.Helper()
	if resp.StatusCode != 200 || scEmitterOf(resp) != "instance" || scInstanceOf(resp) != f.names[0] {
		t.Errorf("%s: status %d emitter %q instance %q, want 200 from %s: %s", what, resp.StatusCode, scEmitterOf(resp), scInstanceOf(resp), f.names[0], body)
	}
}

// wantLLMLab asserts a response came from LLMLab through llmlab-a.
func (f *chFix) wantLLMLab(t *testing.T, what string, resp *http.Response, body string) {
	t.Helper()
	if resp.StatusCode != 200 || scEmitterOf(resp) != "provider" || resp.Header.Get("hpcburst-provider") != "llmlab-a" || scInstanceOf(resp) != "" {
		t.Errorf("%s: status %d emitter %q provider %q instance %q, want 200 from provider llmlab-a: %s",
			what, resp.StatusCode, scEmitterOf(resp), resp.Header.Get("hpcburst-provider"), scInstanceOf(resp), body)
	}
}

func TestChoiceWithOurDeploymentReadyLLMLabGetsNothing(t *testing.T) {
	f := newChFix(t, 0)
	const n = 5
	for i := 0; i < n; i++ { // different conversations, so no key is favoured
		resp, body := scChatOnce(t, f.harness, fmt.Sprintf("conv-%d", i))
		f.wantInstance(t, fmt.Sprintf("request %d", i), resp, body)
	}
	if got := f.llmChats(); got != 0 {
		t.Errorf("LLMLab got %d chat requests with our deployment ready, want 0", got)
	}
	if got := len(f.engines[0].Requests()); got != n {
		t.Errorf("the engine saw %d requests, want %d", got, n)
	}

	// The control: the same counter and path see a request once the
	// Instance is not ready.
	f.notReady()
	resp, body := scChatOnce(t, f.harness, "conv-control")
	f.wantLLMLab(t, "the control", resp, body)
	if got := f.llmChats(); got != 1 {
		t.Errorf("LLMLab chat count after the control = %d, want 1", got)
	}
}

// TestChoiceAtCapSendsTheNextRequestToLLMLab: one Instance with
// routing.instanceCap 1, ready = desired = 1, so the tight bound and the cap
// are in force. One request is held on the Instance, which is then at its
// cap; the next request has no Instance below its cap and goes to LLMLab.
func TestChoiceAtCapSendsTheNextRequestToLLMLab(t *testing.T) {
	f := newChFix(t, 1)
	hold := make(chan struct{})
	f.setAll(enginetest.Behaviour{Hold: hold})
	held := scGo(f.harness, errChatBody(scModel, false, "hello there"), "x-session-id", "conv-held")
	errWaitFor(t, "the first request to reach the Instance", func() bool { return f.engines[0].Open() == 1 })
	if got := f.scLoad(scDep, f.names[0]); got != 1 {
		t.Fatalf("the Instance's count = %d, want 1 (its cap)", got)
	}

	resp, body := scChatOnce(t, f.harness, "conv-next")
	f.wantLLMLab(t, "the request past the cap", resp, body)
	if got := f.llmChats(); got != 1 {
		t.Errorf("LLMLab chat count = %d, want 1", got)
	}
	if got := len(f.engines[0].Requests()); got != 1 {
		t.Errorf("the engine saw %d requests, want only the held one", got)
	}

	close(hold)
	hresp, hbody := held.wait(t)
	f.wantInstance(t, "the held request", hresp, hbody)
}

func TestChoiceWithNoInstanceReadyEverythingGoesToLLMLab(t *testing.T) {
	f := newChFix(t, 0)
	f.notReady()
	const n = 4
	for i := 0; i < n; i++ {
		resp, body := scChatOnce(t, f.harness, fmt.Sprintf("conv-%d", i))
		f.wantLLMLab(t, fmt.Sprintf("request %d", i), resp, body)
	}
	if got := f.llmChats(); got != n {
		t.Errorf("LLMLab chat count = %d, want %d", got, n)
	}
	if got := len(f.engines[0].Requests()); got != 0 {
		t.Errorf("the engine saw %d requests while it was not ready, want 0", got)
	}
}

// TestChoiceSpilledConversationReturnsHome: a conversation (one x-session-id)
// spilled to LLMLab because the Instance was at its cap goes back to the
// Instance at its next turn, once the Instance has room.
func TestChoiceSpilledConversationReturnsHome(t *testing.T) {
	f := newChFix(t, 1)
	hold := make(chan struct{})
	f.setAll(enginetest.Behaviour{Hold: hold})
	held := scGo(f.harness, errChatBody(scModel, false, "hello there"), "x-session-id", "conv-other")
	errWaitFor(t, "the first request to reach the Instance", func() bool { return f.engines[0].Open() == 1 })

	// Turn 1 of the conversation: the Instance is full, so it goes to LLMLab.
	resp, body := scChatOnce(t, f.harness, "conv-spilled")
	f.wantLLMLab(t, "turn 1, with the Instance full", resp, body)

	// Room: the held request completes. Its record is written after its
	// reservation is released, so waiting for the record is a barrier.
	f.setAll(enginetest.Behaviour{})
	close(hold)
	hresp, _ := held.wait(t)
	f.record(hresp.Header.Get("hpcburst-request-id"))
	errWaitFor(t, "the Instance's count to drop to 0", func() bool { return f.scLoad(scDep, f.names[0]) == 0 })

	// Turn 2 of the same conversation: home again.
	resp, body = scChatOnce(t, f.harness, "conv-spilled")
	f.wantInstance(t, "turn 2, with room", resp, body)
	if got := f.llmChats(); got != 1 {
		t.Errorf("LLMLab chat count = %d, want only the one spilled turn", got)
	}
	if got := len(f.engines[0].Requests()); got != 2 {
		t.Errorf("the engine saw %d requests, want the held one and turn 2", got)
	}
}

// TestChoiceServedDeploymentSelectingTheLLMLabProviderSendsItNothing: a served
// ModelDeployment of another model whose provider selector matches the LLMLab
// provider's labels, with an Instance labelled with that Provider, attaches
// nothing and routes nothing to LLMLab: the request gets no_ready_target (the
// model is attached, but has no routable target) and LLMLab and the engine see
// nothing. Only the managed-source ModelDeployment attaches the Provider, which
// the control shows with the same counter.
func TestChoiceServedDeploymentSelectingTheLLMLabProviderSendsItNothing(t *testing.T) {
	f := newChFix(t, 0)
	const other = "example-selector-7b"
	md := f.servedMD("md-sel", other, 1)
	md.Spec.Providers.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{v1alpha1.LabelPlatform: "llmlab"}}
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: f.clk})
	f.instance("md-sel", "sel-0", "llmlab-a", e, key, instanceOpts{})
	f.apply()

	resp := f.errPost(scChat, errChatBody(other, false, "hello there"))
	body := readAll(t, resp)
	checkError(t, resp, body, wantError{status: 503, code: "model_inactive", detail: "no_ready_target", retryAfter: "30"})
	if got := f.llmChats(); got != 0 {
		t.Errorf("LLMLab got %d chat requests for a model only a served ModelDeployment names, want 0", got)
	}
	if got := len(e.Requests()); got != 0 {
		t.Errorf("the Instance labelled with the managed Provider saw %d requests, want 0", got)
	}

	// The control: scModel, attached by the managed-source ModelDeployment,
	// with our Instance not ready, reaches LLMLab through the same counter.
	f.notReady()
	cresp, cbody := scChatOnce(t, f.harness, "conv-control")
	f.wantLLMLab(t, "the control", cresp, cbody)
	if got := f.llmChats(); got != 1 {
		t.Errorf("LLMLab chat count after the control = %d, want 1", got)
	}
}
