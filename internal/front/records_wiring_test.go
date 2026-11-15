package front

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// How the record fetch reaches the API and the rest of the front (#585): its calls, its paths and
// methods, and Run's wiring. The fixtures and helpers are records_test.go's.

// TestDeploymentRecordsAPICalls: the record fetch's only calls to the API are lists of apps/v1
// ControllerRevisions selected by the ID's display form and the identity and verification record kinds:
// one over all namespaces when the front watches all, or one per watched namespace and hpcburst-system.
// That is all the new access the front needs (#585's RBAC criterion; the front runs on the launcher's
// admin kubeconfig, and RBAC cannot select by label).
func TestDeploymentRecordsAPICalls(t *testing.T) {
	g := readGolden(t, "01-hbep-example")
	sel, err := labels.Parse("hpcburst.groundnuty.github.io/deployment-id=" + rfDisplay01 + ",hpcburst.groundnuty.github.io/record in (identity,verification)")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		watched, want []string
	}{
		{nil, []string{""}},
		{[]string{"ns-a", "ns-b"}, []string{"hpcburst-system", "ns-a", "ns-b"}},
		{[]string{"hpcburst-system", "ns-a", "ns-a"}, []string{"hpcburst-system", "ns-a"}},
	} {
		t.Run(strings.Join(c.watched, ","), func(t *testing.T) {
			h := newHarness(t, func(cfg *Config) { cfg.Namespaces = c.watched })
			h.addRecords(servedRecord(t, "ns-a", g))
			for _, id := range []string{g.sha256, rfDisplay01} {
				h.recs.ClearActions()
				resp, body := h.getRecords(id)
				checkOK(t, h, resp, body)
				var got []string
				for _, a := range h.recs.Actions() {
					l, ok := a.(k8stesting.ListAction)
					switch {
					case !ok || a.GetVerb() != "list":
						t.Errorf("a %s call, want lists only: %v", a.GetVerb(), a)
					case a.GetResource().Group != "apps" || a.GetResource().Version != "v1" || a.GetResource().Resource != "controllerrevisions":
						t.Errorf("a list of %v, want apps/v1 controllerrevisions", a.GetResource())
					case l.GetListRestrictions().Labels.String() != sel.String() || !l.GetListRestrictions().Fields.Empty():
						t.Errorf("selected by %q and fields %q, want %q", l.GetListRestrictions().Labels, l.GetListRestrictions().Fields, sel)
					default:
						got = append(got, a.GetNamespace())
					}
				}
				slices.Sort(got)
				if !slices.Equal(got, c.want) {
					t.Errorf("GET %s: lists in namespaces %q, want %q", id, got, c.want)
				}
			}
		})
	}
}

// TestDeploymentRecordsOtherPathsAndMethods: the route answers GET only; another method is HBEP-5's 400
// invalid_request, as on /v1/; another path under /hpcburst/v1/ is a plain 404, as outside the contract,
// but only after the key is checked.
func TestDeploymentRecordsOtherPathsAndMethods(t *testing.T) {
	g := readGolden(t, "01-hbep-example")
	h := newHarness(t, nil)
	h.addRecords(servedRecord(t, "ns-a", g))
	resp := h.errPost("/hpcburst/v1/deployments/"+g.sha256, "{}")
	checkError(t, resp, readAll(t, resp), wantError{status: http.StatusBadRequest, code: "invalid_request"})
	for _, path := range []string{"/hpcburst/v1/deployments", "/hpcburst/v1/other/" + g.sha256} {
		resp := h.errGet(path)
		body := readAll(t, resp)
		if resp.StatusCode != http.StatusNotFound || strings.Contains(body, "hpcburst_error") {
			t.Errorf("GET %s: %d %s, want a plain 404", path, resp.StatusCode, body)
		}
		checkAttribution(t, resp, efFrontOnly)
		req, err := http.NewRequest(http.MethodGet, h.url+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err = errClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		checkError(t, resp, readAll(t, resp), wantError{status: http.StatusUnauthorized, code: "invalid_api_key"})
	}
}

// TestRunServesDeploymentRecords: the front Run builds serves the route through its Records client, or
// its Kube client when Records is unset, as the command's wiring gives it.
func TestRunServesDeploymentRecords(t *testing.T) {
	g := readGolden(t, "01-hbep-example")
	for _, which := range []string{"records", "kube"} {
		t.Run(which, func(t *testing.T) {
			o := runOptions(t, "", agentapi.Host{StateDir: t.TempDir()}, "")
			dial, err := listenFree()
			if err != nil {
				t.Fatal(err)
			}
			o.DialHomeListener = dial
			cs := kubefake.NewSimpleClientset(servedRecord(t, "ns-a", g))
			if which == "records" {
				o.Records = cs
			} else {
				o.Kube = cs
			}
			ready := make(chan *Front, 1)
			o.Ready = func(f *Front) { ready <- f }
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- Run(ctx, o) }()
			var f *Front
			select {
			case f = <-ready:
			case err := <-done:
				t.Fatalf("Run returned early: %v", err)
			case <-time.After(30 * time.Second):
				cancel()
				<-done
				t.Fatal("the front did not start")
			}
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Errorf("Run: %v", err)
				}
			}()
			req, err := http.NewRequest(http.MethodGet, "http://"+f.Client.String()+"/hpcburst/v1/deployments/"+rfDisplay01, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer client-key")
			resp, err := errClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body := readAll(t, resp)
			var v struct {
				ID string `json:"id"`
			}
			if resp.StatusCode != http.StatusOK || json.Unmarshal([]byte(body), &v) != nil || v.ID != g.sha256 {
				t.Errorf("status %d, body %s: want 200 with id %s", resp.StatusCode, body, g.sha256)
			}
		})
	}
}
