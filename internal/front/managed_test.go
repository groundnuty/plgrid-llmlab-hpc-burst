package front

import (
	"net/http"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// HBEP-5, Test plan, Choice under model: "A managed-source ModelDeployment
// gets no Allocation, and its Ready follows the catalog entry (HBEP-7)", and
// Notes (routing policy): an entry reaches only a Provider on the platform
// the ModelDeployment names. Two checks keep an entry off a Provider of
// another platform, and each is tested alone, so either can fail on its own
// (issue #64):
//
//   - the guard: the allowed Provider carries another platform label, and the
//     ModelDeployment has no condition at all (a stale or missing one) - the
//     front itself refuses to route it;
//   - the filter: the Provider is on the platform, and the ModelDeployment's
//     Ready is False with reason ProviderNotOnPlatform - the lifecycle
//     controller's verdict alone keeps it from routing.
//
// The Provider's credentials point at an LLMLab double, so a request routed
// wrongly would show in the double's count; the same fixture minus its one
// fault routes (the control), so the zero is the fault's doing.

const (
	mgUpstreamKey = "llmlab-upstream-key-for-managed-tests-9d02"
	mgCatalog     = "llmlab-example-27b"
)

// mgFix is a front with a managed Provider "llm-a" behind an LLMLab double and
// a managed-source ModelDeployment of scModel naming platform "llmlab".
type mgFix struct {
	*harness
	llm *llmlabtest.Server
}

// newMgFix builds it with the Provider's platform label set to providerPlatform.
func newMgFix(t *testing.T, providerPlatform string) *mgFix {
	t.Helper()
	h := newHarness(t, nil)
	llm := llmlabtest.Start(t, llmlabtest.Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{mgUpstreamKey},
		Models: []llmlabtest.Model{{ID: mgCatalog}}})
	h.profile("llmlab", v1alpha1.PlatformManaged, "", "")
	h.managedProvider("llm-a", "llmlab", providerPlatform, llm.BaseURL(), mgUpstreamKey, mgCatalog)
	h.managedMD("md-llm", scModel, "llmlab", mgCatalog, "llm-a")
	h.apply()
	return &mgFix{harness: h, llm: llm}
}

// chat sends one chat request for scModel.
func (m *mgFix) chat(t *testing.T) (*http.Response, string) {
	t.Helper()
	return scGo(m.harness, errChatBody(scModel, false, "hello there"), "x-session-id", "conv-managed").wait(t)
}

// wantNotRouted asserts the model has no ready target and LLMLab saw nothing.
func (m *mgFix) wantNotRouted(t *testing.T) {
	t.Helper()
	resp, body := m.chat(t)
	checkError(t, resp, body, wantError{status: 503, code: "model_inactive", detail: "no_ready_target", retryAfter: "30"})
	if got := m.llm.Arrived(http.MethodPost, scChat); got != 0 {
		t.Errorf("LLMLab got %d chat requests, want 0", got)
	}
	if n := len(m.llm.Requests()); n != 0 {
		t.Errorf("LLMLab got %d requests of any kind, want 0", n)
	}
}

// wantRouted asserts the request reaches LLMLab through the Provider.
func (m *mgFix) wantRouted(t *testing.T) {
	t.Helper()
	resp, body := m.chat(t)
	if resp.StatusCode != 200 || scEmitterOf(resp) != "provider" || resp.Header.Get("hpcburst-provider") != "llm-a" {
		t.Fatalf("status %d emitter %q provider %q, want 200 from provider llm-a: %s", resp.StatusCode, scEmitterOf(resp), resp.Header.Get("hpcburst-provider"), body)
	}
	if got := m.llm.Arrived(http.MethodPost, scChat); got != 1 {
		t.Errorf("LLMLab chat count = %d, want 1", got)
	}
}

// TestManagedGuardKeepsAnEntryOffAProviderOfAnotherPlatform: the allowed
// Provider's platform label is "othercloud", the ModelDeployment names
// "llmlab", and no condition is set on the ModelDeployment.
func TestManagedGuardKeepsAnEntryOffAProviderOfAnotherPlatform(t *testing.T) {
	m := newMgFix(t, "othercloud")
	if c := m.md("md-llm").Status.Conditions; len(c) != 0 {
		t.Fatalf("the ModelDeployment carries conditions %+v: the guard must act alone", c)
	}
	m.wantNotRouted(t)

	// The control: the Provider relabelled onto the platform routes.
	m.snap.Providers[0].Labels[v1alpha1.LabelPlatform] = "llmlab"
	m.apply()
	m.wantRouted(t)
}

// TestManagedFilterKeepsAnEntryOffWhenReadyIsProviderNotOnPlatform: the
// Provider is on the platform, but the lifecycle controller set Ready=False
// with reason ProviderNotOnPlatform on the ModelDeployment.
func TestManagedFilterKeepsAnEntryOffWhenReadyIsProviderNotOnPlatform(t *testing.T) {
	m := newMgFix(t, "llmlab")
	md := m.md("md-llm")
	md.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonProviderNotOnPlatform}}
	m.apply()
	m.wantNotRouted(t)

	// The control: the condition gone (the controller found a Provider on the
	// platform) routes through the very same Provider.
	m.md("md-llm").Status.Conditions = nil
	m.apply()
	m.wantRouted(t)
}
