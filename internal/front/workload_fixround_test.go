package front

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
)

// The fix round of the review of #778 (Codex gpt-6.2-astra): four rows, each with its hand mutant in the hand-back.

// Finding 1: a retry never looks the Workload up again. A request that names "analysis" resolves, on its first choice, to team-a's Workload and
// team-a's deployments. Between the first attempt and the retry team-a's Workload is deleted and team-b's, of the same name and model, is
// created. The retry goes to a team-a deployment or fails as for no eligible target, never to team-b's. The expected Instances are typed by hand.

// swapOwner is the change between the two attempts: team-a/analysis deleted, team-b/analysis created, both of scModel.
func (f *pwFix) swapOwner() {
	f.dropWorkload("team-a", "analysis")
	f.workload("team-b", "analysis", scModel)
	f.apply()
}

func ownerFix(t *testing.T, aInstances ...string) (*pwFix, string) {
	t.Helper()
	f := newPwFix(t,
		pwDep{name: "md-a", ns: "team-a", instances: aInstances},
		pwDep{name: "md-b", ns: "team-b", instances: []string{"b-0"}})
	f.workload("team-a", "analysis", scModel)
	f.apply()
	var home string
	for i := 0; i < 200; i++ {
		s := fmt.Sprintf("owner-key-%d", i)
		if pwHome(t, pwNames(depNamed(f.deps, "md-a")), s) == "team-a/a-0" {
			home = s
			break
		}
	}
	if home == "" {
		t.Fatal("no session is home on team-a/a-0")
	}
	return f, home
}

// The request written to a-0 is lost before any response byte, and the retry goes to another Instance of the set the first choice resolved to: a-1.
func TestARetryAfterALostRequestStaysInTheNamespaceTheFirstChoiceResolved(t *testing.T) {
	for name, c := range map[string]struct {
		model string
		hdr   []string
	}{
		"named by the suffix": {scModel + "#analysis", nil},
		"named by the header": {scModel, []string{wlHeader, "analysis"}},
	} {
		t.Run(name, func(t *testing.T) {
			f, session := ownerFix(t, "a-0", "a-1")
			release := make(chan struct{})
			f.engines["team-a/a-0"].Set(enginetest.Behaviour{CloseAfterHold: release})
			call := scGo(f.harness, errChatBody(c.model, false, "hello there"), append([]string{"x-session-id", session}, c.hdr...)...)
			errWaitFor(t, "the request to reach team-a/a-0", func() bool { return f.engines["team-a/a-0"].Open() == 1 })
			f.swapOwner()
			close(release)
			resp, body := call.wait(t)
			if resp.StatusCode != 200 || scInstanceOf(resp) != "team-a/a-1" {
				t.Fatalf("the retry: status %d from %q, want 200 from team-a/a-1: %s", resp.StatusCode, scInstanceOf(resp), body)
			}
			if n := len(f.engines["team-b/b-0"].Requests()); n != 0 {
				t.Errorf("team-b/b-0, in the namespace of the Workload created after the first choice, saw %d requests, want 0", n)
			}
			rec := f.record(resp.Header.Get("hpcburst-request-id"))
			wantWorkload(t, "the retry", rec, str("analysis"))
			if rec.ModelDeployment != "team-a/md-a" {
				t.Errorf("record: modelDeployment %q, want team-a/md-a", rec.ModelDeployment)
			}
		})
	}
}

// With no other Instance in the set the retry fails as for no eligible target: the first failure is relayed, 502 upstream_closed, and nothing
// reaches team-b.
func TestARetryWithNoOtherInstanceInTheResolvedSetDoesNotCrossToAnotherNamespace(t *testing.T) {
	f, session := ownerFix(t, "a-0")
	release := make(chan struct{})
	f.engines["team-a/a-0"].Set(enginetest.Behaviour{CloseAfterHold: release})
	call := scGo(f.harness, errChatBody(scModel+"#analysis", false, "hello there"), "x-session-id", session)
	errWaitFor(t, "the request to reach team-a/a-0", func() bool { return f.engines["team-a/a-0"].Open() == 1 })
	f.swapOwner()
	close(release)
	resp, body := call.wait(t)
	checkError(t, resp, body, wantError{status: 502, code: "upstream_closed", detail: "tunnel"})
	if n := len(f.engines["team-b/b-0"].Requests()); n != 0 {
		t.Errorf("team-b/b-0 saw %d requests, want 0", n)
	}
}

// The resend after a refused dial goes the same way: the swap happens between the first choice and its dial, and the resend, among team-a's
// Instances, reaches a-1.
func TestAResendAfterARefusedDialStaysInTheNamespaceTheFirstChoiceResolved(t *testing.T) {
	f, session := ownerFix(t, "a-0", "a-1")
	f.engines["team-a/a-0"].Close()
	var once sync.Once
	f.router.beforePick = func() { once.Do(f.swapOwner) }
	resp := f.errPost(scChat, errChatBody(scModel+"#analysis", false, "hello there"), "x-session-id", session)
	body := readAll(t, resp)
	if resp.StatusCode != 200 || scInstanceOf(resp) != "team-a/a-1" {
		t.Fatalf("the resend: status %d from %q, want 200 from team-a/a-1: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
	if n := len(f.engines["team-b/b-0"].Requests()); n != 0 {
		t.Errorf("team-b/b-0 saw %d requests, want 0", n)
	}
}

// Control: a request that names the Workload after the swap resolves to team-b, so the rows above are the retry's doing and not a fixture that
// never changed hands.
func TestAfterTheSwapANewRequestNamingTheWorkloadResolvesToTheOtherNamespace(t *testing.T) {
	f, session := ownerFix(t, "a-0", "a-1")
	f.swapOwner()
	resp, body := f.say(scModel+"#analysis", "x-session-id", session)
	if resp.StatusCode != 200 || scInstanceOf(resp) != "team-b/b-0" {
		t.Fatalf("a new request: status %d from %q, want 200 from team-b/b-0: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
}

// Finding 2: a header of only a non-breaking space, U+00A0, is a value, not an empty header, so the request is refused with unknown_workload and
// never routed as one that names none. A space and a tab around nothing still name nothing.
func TestAHeaderOfANonBreakingSpaceIsRefusedAndOneOfSpaceAndTabNamesNothing(t *testing.T) {
	f := wlFront(t)
	f.workload("ns", "study-a", scModel)
	f.apply()
	nbsp := " "
	for name, hdr := range map[string]string{
		"only a non-breaking space":          nbsp,
		"a non-breaking space before a name": nbsp + "study-a",
		"a name before a non-breaking space": "study-a" + nbsp,
	} {
		t.Run(name, func(t *testing.T) {
			rec := f.refused(name, scModel, "unknown_workload", wlHeader, hdr)
			wantWorkload(t, name, rec, nil)
		})
	}
	// The unnamed route is not taken: no engine saw a request above (refused checks it), and a header of a space and a tab, which the server
	// trims to nothing, is served as a request that names none.
	rec := f.served("a space and a tab", "ns/a-0", scModel, wlHeader, " \t")
	wantWorkload(t, "a space and a tab", rec, nil)
}

// Finding 3: the walk over the values is one pass. Ten thousand values that all differ cost one comparison past the first, and the answer is
// unknown_workload; the same count of equal values cost one comparison each and name the one Workload.
func TestTheWorkloadHeaderValuesAreWalkedOnce(t *testing.T) {
	var compared atomic.Int64
	orig := sameWorkloadValue
	sameWorkloadValue = func(a, b string) bool { compared.Add(1); return orig(a, b) }
	t.Cleanup(func() { sameWorkloadValue = orig })

	const n = 10000
	differ := http.Header{}
	same := http.Header{}
	for i := 0; i < n; i++ {
		differ.Add(wlHeader, fmt.Sprintf("w%d", i))
		same.Add(wlHeader, "w")
	}
	if got := workloadHeaderValue(differ, wlHeader); got != "w0,w1" {
		t.Errorf("%d different values read as %q, want %q: the first and the first that differs", n, got, "w0,w1")
	}
	if c := compared.Load(); c != 1 {
		t.Errorf("%d different values took %d comparisons, want 1", n, c)
	}
	compared.Store(0)
	if got := workloadHeaderValue(same, wlHeader); got != "w" {
		t.Errorf("%d equal values read as %q, want %q", n, got, "w")
	}
	if c := compared.Load(); c != n-1 {
		t.Errorf("%d equal values took %d comparisons, want %d", n, c, n-1)
	}

	// Through the front: one request with thousands of different values is refused, and costs the one comparison.
	f := wlFront(t)
	f.workload("ns", "w0", scModel)
	f.apply()
	compared.Store(0)
	req, err := http.NewRequest(http.MethodPost, f.url+scChat, strings.NewReader(errChatBody(scModel, false, "hello there")))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+f.clientKey)
	// net/http's server reads accepts only a few hundred values of one header (500 is refused with 431), so the request carries 300 of them; the unit count above has the 10,000.
	const sent = 300
	for i := 0; i < sent; i++ {
		req.Header.Add(wlHeader, fmt.Sprintf("w%d", i))
	}
	resp, err := errClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body := readAll(t, resp)
	f.checkFrontError(t, resp, body, wantError{status: 400, code: "invalid_model_name", detail: "unknown_workload"})
	if c := compared.Load(); c != 1 {
		t.Errorf("a request with %d different header values took %d comparisons, want 1", sent, c)
	}
}

// Finding 3's choice, through the front: repeated values that all name the same Workload are accepted, and two different non-empty values are
// refused with unknown_workload, though each names a Workload that exists.
func TestRepeatedWorkloadHeaderValuesAreAcceptedWhenTheyAllNameTheSameWorkload(t *testing.T) {
	f := wlFront(t)
	f.workload("ns", "study-a", scModel)
	f.workload("ns", "study-b", scModel)
	f.apply()
	for name, c := range map[string]struct {
		lines  []string
		detail string // "" is served
	}{
		"two equal values":                {[]string{"study-a", "study-a"}, ""},
		"three equal values":              {[]string{"study-a", "study-a", "study-a"}, ""},
		"an empty line among equal ones":  {[]string{"study-a", "", "study-a"}, ""},
		"two different values, both real": {[]string{"study-a", "study-b"}, "unknown_workload"},
		"a repeat, then a different one":  {[]string{"study-a", "study-a", "study-b"}, "unknown_workload"},
	} {
		t.Run(name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, f.url+scChat, strings.NewReader(errChatBody(scModel, false, "hello there")))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+f.clientKey)
			for _, l := range c.lines {
				req.Header.Add(wlHeader, l)
			}
			resp, err := errClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body := readAll(t, resp)
			if c.detail == "" {
				if resp.StatusCode != 200 {
					t.Fatalf("status %d, want 200: %s", resp.StatusCode, body)
				}
				return
			}
			f.checkFrontError(t, resp, body, wantError{status: 400, code: "invalid_model_name", detail: c.detail})
		})
	}
}

// Finding 4: a namespace the configuration lists twice is watched once. One Workload, "analysis", in team-a, with the namespaces
// ["team-a", "team-a"]: the front resolves it and serves the request, and does not answer 400 workload_ambiguous.
func TestARepeatedNamespaceDoesNotMakeAWorkloadAmbiguous(t *testing.T) {
	h := newHarness(t, nil)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMDIn("team-a", "md-a", scModel, 1, "helios-a")
	h.instance("md-a", "a-0", "helios-a", e, key, instanceOpts{})
	h.workload("team-a", "analysis", scModel)

	objs := kySeed(t, h)
	for i := range h.snap.Workloads {
		m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&h.snap.Workloads[i])
		if err != nil {
			t.Fatal(err)
		}
		u := &unstructured.Unstructured{Object: m}
		u.SetAPIVersion("hpcburst.groundnuty.github.io/v1alpha1")
		u.SetKind("Workload")
		objs = append(objs, u)
	}
	client := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		inventory.GVRModelDeployments: "ModelDeploymentList", inventory.GVRInstances: "InstanceList", inventory.GVRWorkloads: "WorkloadList",
		inventory.GVRProviders: "ProviderList", inventory.GVRPlatformProfiles: "PlatformProfileList",
	}, objs...)
	w := inventory.NewWatch(client, []string{"team-a", "team-a"}, nil)
	ctx := t.Context()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	snap, problems := w.Snapshot()
	if len(problems) != 0 || len(snap.Workloads) != 1 {
		t.Fatalf("the snapshot of namespaces [team-a team-a]: %d Workloads, problems %v; want one Workload", len(snap.Workloads), problems)
	}
	h.router.Update(snap)
	f := &pwFix{harness: h}
	resp, body := f.say(scModel + "#analysis")
	if resp.StatusCode != 200 || scInstanceOf(resp) != "team-a/a-0" {
		t.Fatalf("status %d from %q, want 200 from team-a/a-0 (not 400 workload_ambiguous): %s", resp.StatusCode, scInstanceOf(resp), body)
	}
	wantWorkload(t, "a repeated namespace", h.record(resp.Header.Get("hpcburst-request-id")), str("analysis"))
}
