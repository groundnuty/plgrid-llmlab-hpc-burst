package scenario_test

// Item 1 of issue #193, HBEP-5's attribution: every response, streamed or
// not, success or error, carries the hpcburst-* headers, and its usage record,
// keyed by the request ID, holds the same facts (HBEP-5, Attribution headers;
// HBEP-9). Every reply the test gets goes through attribute.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The attribution headers (HBEP-5, Attribution headers), in the canonical
// form net/http gives them.
const (
	hdrRequestID    = "Hpcburst-Request-Id"
	hdrEmitter      = "Hpcburst-Emitter"
	hdrProvider     = "Hpcburst-Provider"
	hdrDeploymentID = "Hpcburst-Deployment-Id"
	hdrInstance     = "Hpcburst-Instance"
	hdrInstanceUID  = "Hpcburst-Instance-Uid"
)

// wantAttr is what a reply must say about who answered it.
type wantAttr struct {
	emitter string // front, instance or provider
	// preTarget: a front error raised before a target was chosen carries
	// only the request ID and the emitter (HBEP-5).
	preTarget                                     bool
	provider, deploymentID, instance, instanceUID string // deploymentID: the display form
}

func wantInstance(d *depRun, in *v1alpha1.Instance) wantAttr {
	return wantAttr{emitter: "instance", provider: d.Provider, deploymentID: in.Spec.DeploymentID.Display(),
		instance: in.Namespace + "/" + in.Name, instanceUID: string(in.UID)}
}

func (p3 *part3) wantManaged() wantAttr {
	return wantAttr{emitter: "provider", provider: managedProvider, deploymentID: p3.managedID.Display()}
}

func wantFront() wantAttr { return wantAttr{emitter: "front", preTarget: true} }

// attributed is one reply the test checked.
type attributed struct {
	what    string
	status  int
	emitter string
}

// attribute checks one reply's headers against its usage record and against
// what the test expects, and keeps it for the summary.
func (p3 *part3) attribute(what string, rp reply, row usageRow, w wantAttr) {
	p3.t.Helper()
	for _, p := range attributionProblems(rp.status, rp.header, row, w) {
		p3.t.Errorf("%s: %s", what, p)
	}
	p3.replies = append(p3.replies, attributed{what: what, status: rp.status, emitter: row.Emitter})
}

// attributionProblems is HBEP-5's rule for one reply: the request ID and the
// emitter always; the provider and the deployment ID's display form once a
// target is chosen; the Instance and its UID for an Instance of ours; only
// the first two for a front error before a target. The record names the same
// request, emitter, provider, deployment (its full ID, whose first 12
// characters are the header's) and Instance, with the same status.
func attributionProblems(status int, h http.Header, row usageRow, w wantAttr) []string {
	var out []string
	field := func(name, header, record, want string) {
		if header != want || record != want {
			out = append(out, fmt.Sprintf("%s: header %q, usage record %q, want %q", name, header, record, want))
		}
	}
	id := h.Get(hdrRequestID)
	if id == "" || id != row.RequestID {
		out = append(out, fmt.Sprintf("request ID: header %q, usage record %q", id, row.RequestID))
	}
	if row.Status != status {
		out = append(out, fmt.Sprintf("status %d, usage record %d", status, row.Status))
	}
	field("emitter", h.Get(hdrEmitter), row.Emitter, w.emitter)
	recDisplay := row.DeploymentID
	if len(recDisplay) > v1alpha1.DisplayIDLength {
		recDisplay = recDisplay[:v1alpha1.DisplayIDLength]
	}
	if w.preTarget {
		for _, k := range []string{hdrProvider, hdrDeploymentID, hdrInstance, hdrInstanceUID} {
			if v := h.Values(k); len(v) > 0 {
				out = append(out, fmt.Sprintf("a front error before a target carries %s %q", k, v))
			}
		}
		if row.Provider != "" || row.DeploymentID != "" || row.Instance != "" || row.InstanceUID != "" {
			out = append(out, fmt.Sprintf("a front error before a target has a usage record naming provider %q, deployment %q, Instance %q (%q)",
				row.Provider, row.DeploymentID, row.Instance, row.InstanceUID))
		}
		return out
	}
	field("provider", h.Get(hdrProvider), row.Provider, w.provider)
	field("deployment ID", h.Get(hdrDeploymentID), recDisplay, w.deploymentID)
	if len(row.DeploymentID) != v1alpha1.FullIDLength {
		out = append(out, fmt.Sprintf("the usage record's deployment ID %q is not the full form", row.DeploymentID))
	}
	field("Instance", h.Get(hdrInstance), row.Instance, w.instance)
	field("Instance UID", h.Get(hdrInstanceUID), row.InstanceUID, w.instanceUID)
	return out
}

// sendAs posts body to path with the given client key ("" sends none),
// outside the rig's own key, for the front's error rows.
func (p3 *part3) sendAs(path, body, key string) reply {
	ctx, cancel := p3.requestCtx()
	defer cancel()
	return doRequest(ctx, p3.ports.client, path, body, key)
}

func doRequest(ctx context.Context, addr, path, body, key string) reply {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, strings.NewReader(body))
	if err != nil {
		return reply{err: err}
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: requestTimeout}).Do(req)
	if err != nil {
		return reply{err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return reply{status: resp.StatusCode, header: resp.Header, body: b, err: err, id: resp.Header.Get(hdrRequestID)}
}

// errorsAttributed sends one request for each front error the scenario can
// raise before a target, and checks each one's status, code, Retry-After and
// attribution (HBEP-5, Errors and Attribution headers).
func (p3 *part3) errorsAttributed() {
	r := p3.rig
	r.t.Helper()
	ok := r.h.clientKey
	cases := []struct {
		what, key, body          string
		status                   int
		code, detail, retryAfter string
	}{
		{"a wrong client key", strings.Repeat("e0", 32), chatBodyFor(queuedDep.Model, 1, 0, false), http.StatusUnauthorized, "invalid_api_key", "", ""},
		{"a body that is not JSON", ok, "not json", http.StatusBadRequest, "invalid_request", "", ""},
		{"a provider nobody configured", ok, chatBodyFor(queuedDep.Model+"@no-such-provider", 1, 0, false), http.StatusBadRequest, "invalid_model_name", "unknown_provider", ""},
		{"a model no deployment attaches", ok, chatBodyFor("example-org/not-attached", 1, 0, false), http.StatusServiceUnavailable, "model_inactive", "not_attached", "30"},
	}
	for _, c := range cases {
		rp := p3.sendAs("/v1/chat/completions", c.body, c.key)
		if rp.err != nil {
			r.t.Fatalf("%s: %v", c.what, rp.err)
		}
		e := errorOf(rp.body)
		if rp.status != c.status || e.Code != c.code || derefS(e.Detail) != orNil(c.detail) || rp.header.Get("Retry-After") != c.retryAfter {
			r.t.Errorf("%s: %d code %q detail %q Retry-After %q, want %d %q %q %q: %.300s", c.what, rp.status, e.Code, derefS(e.Detail),
				rp.header.Get("Retry-After"), c.status, c.code, c.detail, c.retryAfter, rp.body)
		}
		p3.attribute(c.what, rp, r.rowOf(rp.id), wantFront())
	}
}

func orNil(s string) string {
	if s == "" {
		return "<nil>"
	}
	return s
}

// attributionSummary logs how many replies were checked, by emitter and
// status, and fails if a kind the scenario produces was never checked.
func (p3 *part3) attributionSummary() {
	p3.t.Helper()
	n := map[string]int{}
	for _, a := range p3.replies {
		n[fmt.Sprintf("%s %d", a.emitter, a.status)]++
	}
	p3.t.Logf("attribution: %d replies checked against their usage records: %v", len(p3.replies), n)
	for _, k := range []string{"instance 200", "provider 200", "front 401", "front 400", "front 503"} {
		if n[k] == 0 {
			p3.t.Errorf("attribution: no %s reply was checked", k)
		}
	}
}
