package front

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/shared"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// HBEP-5, Test plan, Keys:
//   - with no ModelDeployment of a model name, plain model returns 503
//     model_inactive although LLMLab serves that model; once a managed
//     ModelDeployment attaches the LLMLab provider, requests reach LLMLab;
//   - a client configured with its LLMLab key and only the front's base URL
//     works when the operator set the front's client key to that key;
//   - a request to an Instance without that Instance's key is refused by its
//     engine;
//   - a client key cannot write a ModelDeployment.
// And Security: no key is logged. The test plan has no row for the leak, so
// the last two tests below hold the front to the Security text.

const (
	kyModel    = "example-27b" // the name LLMLab lists and clients send
	kyUpstream = "llmlab-upstream-key-for-key-tests-5e0a"
)

// kyLLMLab starts an LLMLab double that lists kyModel and accepts kyUpstream.
func kyLLMLab(t *testing.T) *llmlabtest.Server {
	t.Helper()
	return llmlabtest.Start(t, llmlabtest.Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{kyUpstream},
		Models: []llmlabtest.Model{{ID: kyModel}}})
}

// kyLogged waits until the double has logged n requests, which it does after
// each answer ends, and returns them.
func kyLogged(t *testing.T, llm *llmlabtest.Server, n int) []llmlabtest.Request {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if reqs := llm.Requests(); len(reqs) >= n {
			return reqs
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the LLMLab double logged %d requests, want %d", len(llm.Requests()), n)
	return nil
}

// kyChat is a plain chat request for the model, posted with the client key.
func kyChat(t *testing.T, h *harness, model string) (int, string, http.Header) {
	t.Helper()
	resp := stPost(t, h, "/v1/chat/completions", fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hello there"}]}`, model))
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

// TestKeysPlainModelIsInactiveUntilAManagedDeploymentAttaches: LLMLab serves
// the model, and the Provider's catalog lists it, but no ModelDeployment names
// it, so the front sends nothing there. A managed ModelDeployment attaches the
// Provider, and the same request then reaches LLMLab, with the Provider's key.
func TestKeysPlainModelIsInactiveUntilAManagedDeploymentAttaches(t *testing.T) {
	h := newHarness(t, nil)
	llm := kyLLMLab(t)
	h.profile("llmlab", v1alpha1.PlatformManaged, "", "")
	h.managedProvider("llmlab-a", "llmlab", "llmlab", llm.BaseURL(), kyUpstream, kyModel)
	h.apply()

	status, body, _ := kyChat(t, h, kyModel)
	if status != http.StatusServiceUnavailable || !strings.Contains(body, `"model_inactive"`) || !strings.Contains(body, `"not_attached"`) {
		t.Fatalf("no ModelDeployment: status %d body %s, want 503 model_inactive not_attached", status, body)
	}
	// Arrived counts a request when it arrives; Requests sees one only after
	// its answer ends, so it cannot show that nothing came.
	if n := llm.Arrived("", ""); n != 0 {
		t.Fatalf("LLMLab saw %d requests although no ModelDeployment attaches it", n)
	}

	h.managedMD("md-managed", kyModel, "llmlab", kyModel, "llmlab-a")
	h.apply()
	status, body, header := kyChat(t, h, kyModel)
	if status != http.StatusOK {
		t.Fatalf("with a managed ModelDeployment: status %d: %s", status, body)
	}
	if header.Get("hpcburst-emitter") != "provider" || header.Get("hpcburst-provider") != "llmlab-a" {
		t.Errorf("attribution: emitter %q provider %q, want provider and llmlab-a", header.Get("hpcburst-emitter"), header.Get("hpcburst-provider"))
	}
	reqs := kyLogged(t, llm, 1)
	if got := reqs[0].Header.Get("Authorization"); got != "Bearer "+kyUpstream {
		t.Errorf("LLMLab got Authorization %q, want the Provider's upstream key", got)
	}
}

// TestKeysClientKeyEqualToTheLLMLabKeyWorks: the operator writes the client's
// LLMLab key into the front's client keys file; the client keeps its key and
// changes only its base URL. The front's own check still refuses any other key.
func TestKeysClientKeyEqualToTheLLMLabKeyWorks(t *testing.T) {
	const llmKey = "users-own-llmlab-key-8c11"
	h := newHarness(t, func(c *Config) {
		writeMode600(t, c.ClientKeysFile, fmt.Sprintf(`{"keys":[{"id":"alice","key":%q}]}`, llmKey))
	})
	h.clientKey = llmKey
	llm := llmlabtest.Start(t, llmlabtest.Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{llmKey}, Models: []llmlabtest.Model{{ID: kyModel}}})
	h.profile("llmlab", v1alpha1.PlatformManaged, "", "")
	h.managedProvider("llmlab-a", "llmlab", "llmlab", llm.BaseURL(), llmKey, kyModel)
	h.managedMD("md-managed", kyModel, "llmlab", kyModel, "llmlab-a")
	h.apply()

	client := ctClient(h.url, llmKey, nil)
	res, err := client.Chat.Completions.New(ctCtx(t), openai.ChatCompletionNewParams{
		Model:    shared.ChatModel(kyModel),
		Messages: []openai.ChatCompletionMessageParamUnion{openai.UserMessage("hello there")},
	})
	if err != nil {
		t.Fatalf("a client with its LLMLab key and the front's base URL failed: %v", err)
	}
	if res.Choices[0].Message.Content == "" {
		t.Errorf("an empty answer")
	}
	rec := h.records()
	if len(rec) != 1 || rec[0].ClientKeyID != "alice" {
		t.Errorf("usage records %+v, want one for client key alice", rec)
	}

	// Any other key is the front's 401, and nothing reaches LLMLab for it.
	wrong := ctClient(h.url, llmKey+"x", nil)
	_, err = wrong.Chat.Completions.New(ctCtx(t), openai.ChatCompletionNewParams{
		Model:    shared.ChatModel(kyModel),
		Messages: []openai.ChatCompletionMessageParamUnion{openai.UserMessage("hello there")},
	})
	status, obj := ctAPIError(t, err)
	if status != http.StatusUnauthorized || obj["code"] != "invalid_api_key" {
		t.Errorf("a wrong key: status %d error %v, want 401 invalid_api_key", status, obj)
	}
	if n := llm.Arrived("", ""); n != 1 {
		t.Errorf("LLMLab saw %d requests, want only the right key's one", n)
	}
}

// TestKeysEngineRefusesARequestWithoutItsInstanceKey: the front sends the
// Instance's key, never the client's, and the engine refuses a call without it.
func TestKeysEngineRefusesARequestWithoutItsInstanceKey(t *testing.T) {
	f := newStFix(t, enginetest.Behaviour{Tokens: 2})
	status, body, _ := kyChat(t, f.harness, "example-27b")
	if status != http.StatusOK {
		t.Fatalf("through the front: status %d: %s", status, body)
	}
	via := f.e.Requests()
	if len(via) != 1 {
		t.Fatalf("the engine saw %d requests, want 1", len(via))
	}
	auth := via[0].Header.Get("Authorization")
	instanceKey := strings.TrimPrefix(auth, "Bearer ")
	if instanceKey == "" || instanceKey == auth {
		t.Fatalf("the front sent Authorization %q to the engine, want Bearer <Instance key>", auth)
	}
	if strings.Contains(auth, f.clientKey) {
		t.Errorf("the client's key reached the engine")
	}

	const direct = `{"model":"example-27b","messages":[{"role":"user","content":"hello there"}]}`
	url := "http://" + f.e.Addr() + "/v1/chat/completions"
	call := func(authorization string) int {
		req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(direct))
		if err != nil {
			t.Fatal(err)
		}
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		resp, err := ctHTTP().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = readAll(t, resp)
		return resp.StatusCode
	}
	if got := call(""); got != http.StatusUnauthorized {
		t.Errorf("a call to the engine without a key: status %d, want 401", got)
	}
	if got := call("Bearer " + f.clientKey); got != http.StatusUnauthorized {
		t.Errorf("a call to the engine with the client's key: status %d, want 401", got)
	}
	if got := call("Bearer " + instanceKey); got != http.StatusOK { // the control: the key itself works
		t.Errorf("a call to the engine with its own key: status %d, want 200", got)
	}
}

// kyScan reports which of the needles appear in text.
func kyScan(text string, needles map[string]string) []string {
	var found []string
	for name, n := range needles {
		if strings.Contains(text, n) {
			found = append(found, name)
		}
	}
	return found
}

// kyUsageFiles is every file in the usage directory, as one string.
func kyUsageFiles(t *testing.T, dir string) string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var all strings.Builder
	for _, e := range ents {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		all.WriteString(e.Name() + ":\n")
		all.Write(b)
	}
	return all.String()
}

// TestKeysNeverLeakIntoLogsUsageOrEvents: both targets answer an error whose
// body carries the Authorization header the front sent them (llmlabtest's
// EchoKey; an engine ErrorBody that names its bearer). So the key was in a
// body the front read, and the test shows it did not spread: it is absent from
// the front's logs, from every file in the usage directory and from the
// recorded events. The client's own key, which the front read from the
// request, is absent too.
//
// The error body passes through to the client with every target credential
// replaced by [redacted] and all its other bytes unchanged
// (@hpc-burst-science-agent, 31 Oct 2026; PR #95 review, Astra B12). The
// body LLMLab sends a direct request holds the key, which proves the echo
// was on.
func TestKeysNeverLeakIntoLogsUsageOrEvents(t *testing.T) {
	// A profile whose drain window is below maxRequestDuration plus its
	// margin makes the front emit an event (DrainWindowShort), so the events
	// scanned below are not an empty list.
	h := newHarness(t, nil)
	llm := kyLLMLab(t)
	llm.EchoKey(true)
	llm.SetActive(kyModel, false) // an error from LLMLab: 503, with the echo
	instanceKey := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: instanceKey, Clock: h.clk})
	engineBody := `{"error":{"message":"refused (received Authorization: Bearer ` + instanceKey + `)","type":"engine","code":400}}`
	e.Set(enginetest.Behaviour{Status: http.StatusBadRequest, ErrorBody: engineBody})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "1h", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", "served-model", 1, "helios-a")
	h.instance("md", "md-0", "helios-a", e, instanceKey, instanceOpts{})
	h.profile("llmlab", v1alpha1.PlatformManaged, "", "")
	h.managedProvider("llmlab-a", "llmlab", "llmlab", llm.BaseURL(), kyUpstream, kyModel)
	h.managedMD("md-managed", kyModel, "llmlab", kyModel, "llmlab-a")
	h.apply()
	if len(h.rec.Events()) == 0 {
		t.Fatal("the front recorded no event; the event scan below would prove nothing")
	}

	var ids []string
	status, viaLLM, header := kyChat(t, h, kyModel)
	ids = append(ids, header.Get("hpcburst-request-id"))
	if status != http.StatusServiceUnavailable {
		t.Fatalf("LLMLab's error: status %d: %s", status, viaLLM)
	}
	if strings.Contains(viaLLM, kyUpstream) {
		t.Errorf("the upstream key reached the client in LLMLab's error: %s", viaLLM)
	}
	direct, err := http.NewRequest(http.MethodPost, llm.BaseURL()+"/chat/completions",
		strings.NewReader(fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hello there"}]}`, kyModel)))
	if err != nil {
		t.Fatal(err)
	}
	direct.Header.Set("Authorization", "Bearer "+kyUpstream)
	dresp, err := ctHTTP().Do(direct)
	if err != nil {
		t.Fatal(err)
	}
	want := readAll(t, dresp)
	if !strings.Contains(want, kyUpstream) {
		t.Fatalf("the body LLMLab sends holds no key (EchoKey on); the leak checks would prove nothing: %s", want)
	}
	if want = strings.ReplaceAll(want, kyUpstream, "[redacted]"); viaLLM != want {
		t.Errorf("LLMLab's error body changed on the way to the client beyond the redaction:\n got %s\nwant %s", viaLLM, want)
	}

	status, viaEngine, header := kyChat(t, h, "served-model")
	ids = append(ids, header.Get("hpcburst-request-id"))
	if want := strings.ReplaceAll(engineBody, instanceKey, "[redacted]"); status != http.StatusBadRequest || viaEngine != want {
		t.Fatalf("the engine's error: status %d body %q, want 400 and %q, the key redacted and nothing else changed", status, viaEngine, want)
	}

	for _, id := range ids {
		h.record(id) // the usage records are written
	}
	needles := map[string]string{"upstream key": kyUpstream, "Instance key": instanceKey, "client key": h.clientKey}
	if found := kyScan(h.logs.String(), needles); len(found) > 0 {
		t.Errorf("the front's logs hold: %v\n%s", found, h.logs.String())
	}
	usageText := kyUsageFiles(t, h.cfg.UsageDir)
	if !strings.Contains(usageText, ids[0]) {
		t.Fatalf("the usage files hold no record of the request; the scan would prove nothing:\n%s", usageText)
	}
	if found := kyScan(usageText, needles); len(found) > 0 {
		t.Errorf("the usage records hold: %v\n%s", found, usageText)
	}
	if found := kyScan(fmt.Sprintf("%+v", h.rec.Events()), needles); len(found) > 0 {
		t.Errorf("the recorded events hold: %v\n%+v", found, h.rec.Events())
	}
}

// kySeed converts the harness's typed objects to the unstructured ones the
// fake dynamic client holds, with the apiVersion and kind the typed ones leave
// empty.
func kySeed(t *testing.T, h *harness) []runtime.Object {
	t.Helper()
	var out []runtime.Object
	add := func(kind string, obj any) {
		m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if err != nil {
			t.Fatal(err)
		}
		u := &unstructured.Unstructured{Object: m}
		u.SetAPIVersion(v1alpha1.GroupVersion.String())
		u.SetKind(kind)
		out = append(out, u)
	}
	for i := range h.snap.Profiles {
		add("PlatformProfile", &h.snap.Profiles[i])
	}
	for i := range h.snap.Providers {
		add("Provider", &h.snap.Providers[i])
	}
	for i := range h.snap.Deployments {
		add("ModelDeployment", &h.snap.Deployments[i])
	}
	for i := range h.snap.Instances {
		add("Instance", &h.snap.Instances[i])
	}
	return out
}

// TestKeysAClientKeyCannotWriteAModelDeployment: the front has no write path
// to the control plane. It runs over a fake dynamic client behind
// inventory.NewWatch, serves a request routed from what the watch read, and
// is then offered writes in the forms a client could try; the fake client's
// recorded actions are only list and watch.
func TestKeysAClientKeyCannotWriteAModelDeployment(t *testing.T) {
	h := newHarness(t, nil)
	instanceKey := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: instanceKey}) // the real clock: Run's
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", "example-27b", 1, "helios-a")
	h.instance("md", "md-0", "helios-a", e, instanceKey, instanceOpts{leaseEnd: time.Now().Add(48 * time.Hour)})
	client := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		inventory.GVRModelDeployments: "ModelDeploymentList", inventory.GVRInstances: "InstanceList",
		inventory.GVRProviders: "ProviderList", inventory.GVRPlatformProfiles: "PlatformProfileList",
	}, kySeed(t, h)...)

	o := runOptions(t, "", h.host, "")
	o.Config.ClientKeysFile = h.cfg.ClientKeysFile
	o.Config.UsageDir = filepath.Join(h.dir, "usage-run")
	o.Dynamic = client
	o.Transports = h.trans
	dial, err := listenFree()
	if err != nil {
		t.Fatal(err)
	}
	o.DialHomeListener = dial
	ready := make(chan *Front, 1)
	o.Ready = func(f *Front) { ready <- f }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, o) }()
	defer func() { cancel(); <-done }()
	var f *Front
	select {
	case f = <-ready:
	case err := <-done:
		t.Fatalf("Run returned early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the front did not start")
	}
	base := "http://" + f.Client.String()
	do := func(method, path, body string) (int, string) {
		req, err := http.NewRequest(method, base+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+h.clientKey)
		req.Header.Set("Content-Type", "application/json")
		resp, err := ctHTTP().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, readAll(t, resp)
	}

	// One request must be routed from what the watch read: without it an
	// empty view would pass the check below with no effect at all.
	deadline := time.Now().Add(5 * time.Second)
	var status int
	var body string
	for time.Now().Before(deadline) {
		if status, body = do(http.MethodPost, "/v1/chat/completions", `{"model":"example-27b","messages":[{"role":"user","content":"hello there"}]}`); status == http.StatusOK {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status != http.StatusOK {
		t.Fatalf("a request routed from the watched objects: status %d: %s", status, body)
	}
	if n := len(e.Requests()); n != 1 {
		t.Fatalf("the engine saw %d requests, want the one routed", n)
	}

	// What a client could try as a write. None may reach the control plane.
	md := `{"apiVersion":"` + v1alpha1.GroupVersion.String() + `","kind":"ModelDeployment","metadata":{"name":"mine","namespace":"ns"},"spec":{"model":"evil","instances":1}}`
	attempts := []struct{ method, path, body string }{
		{http.MethodPost, "/apis/" + v1alpha1.GroupVersion.String() + "/namespaces/ns/modeldeployments", md},
		{http.MethodPut, "/apis/" + v1alpha1.GroupVersion.String() + "/namespaces/ns/modeldeployments/md", md},
		{http.MethodDelete, "/apis/" + v1alpha1.GroupVersion.String() + "/namespaces/ns/modeldeployments/md", ""},
		{http.MethodPut, "/v1/models/example-27b", md},
		{http.MethodDelete, "/v1/models/example-27b", ""},
	}
	for _, a := range attempts {
		if status, _ := do(a.method, a.path, a.body); status < 400 {
			t.Errorf("%s %s: status %d, want a refusal", a.method, a.path, status)
		}
	}

	verbs := map[string]int{}
	for _, a := range client.Actions() {
		verbs[a.GetVerb()]++
	}
	if verbs["list"] == 0 || verbs["watch"] == 0 {
		t.Errorf("the fake client saw verbs %v, want list and watch: the watch did not run", verbs)
	}
	for verb, n := range verbs {
		if verb != "list" && verb != "watch" {
			t.Errorf("the fake client saw %d %q actions; the front may only list and watch", n, verb)
		}
	}
}
