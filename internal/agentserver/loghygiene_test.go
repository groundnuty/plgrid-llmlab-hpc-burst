package agentserver

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// TestNoSecretReachesALog sends a credential, a report with an Instance key and
// a desired state with a Hub token through the server, on the paths that log
// (accepted calls, refusals, a Backend failure, an oversize body, a failed
// handshake), with every log line, ErrorLog included, in one buffer.
func TestNoSecretReachesALog(t *testing.T) {
	const hubToken = "hub-token-SECRET-0123456789"
	x := newRig(t).serve()
	instanceKey, err := agentapi.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	wrongCredential, err := agentapi.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	secrets := map[string]string{
		"the Allocation's credential":    x.credential,
		"the presented wrong credential": wrongCredential,
		"the Instance key":               instanceKey,
		"the Hub token":                  hubToken,
	}

	x.register(x.good())
	x.backend.SetDesired(uidA, agentapi.DesiredState{Action: agentapi.ActionRun, Instance: &agentapi.DesiredInstance{
		Name: "inst-example", UID: "inst-uid-1", HubToken: hubToken,
	}})
	if resp, body := x.do(http.MethodGet, agentapi.PathDesired, x.good(), nil); resp.StatusCode != http.StatusOK || !strings.Contains(string(body), hubToken) {
		t.Fatalf("desired: status %d; the Hub token must reach the agent (%s)", resp.StatusCode, body)
	}
	report := agentapi.Report{Time: time.Unix(1, 0).UTC(), Instance: &agentapi.InstanceReport{UID: "inst-uid-1", Key: instanceKey}}
	if resp, body := x.do(http.MethodPost, agentapi.PathReport, x.good(), reportBody(t, report)); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("report: status %d (%s)", resp.StatusCode, body)
	}
	if got, ok := x.backend.LastReport(uidA); !ok || got.Instance == nil || got.Instance.Key != instanceKey {
		t.Fatalf("the Backend did not receive the Instance key: %+v", got)
	}

	// Paths that log louder: a refusal, a Backend failure that carries the report,
	// an oversize body, and a handshake that fails.
	wrong := x.good()
	wrong.credential = wrongCredential
	x.do(http.MethodPost, agentapi.PathReport, wrong, reportBody(t, report))
	x.backend.FailNext(fake.MethodReport, errString("backend failed"))
	x.do(http.MethodPost, agentapi.PathReport, x.good(), reportBody(t, report))
	x.do(http.MethodPost, agentapi.PathReport, x.good(), paddedReport(t, agentapi.MaxBodyBytes+1))
	c := x.clientFor(strings.Repeat("0", 64))
	if resp, err := c.Get(x.base + agentapi.PathDesired); err == nil {
		resp.Body.Close()
		t.Fatal("a client with the wrong pin got an answer")
	}
	if err := x.stop(); err != nil { // the server has logged everything it will
		t.Fatal(err)
	}

	logs := x.logs.String()
	// The check must have seen the server: a buffer with nothing in it, or only
	// part of the paths, would pass for the wrong reason.
	for _, want := range []string{
		"agent API call", "path=" + agentapi.PathReport, "code=Unauthenticated", "code=Internal",
		"tag=" + tagA, "agent_instance_id=" + instanceA, "backend failed", "TLS handshake error",
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("the logs do not contain %q, so the check did not see that path:\n%s", want, logs)
		}
	}
	for name, secret := range secrets {
		if strings.Contains(logs, secret) {
			t.Errorf("the logs contain %s", name)
		}
	}
	if strings.Contains(strings.ToLower(logs), "authorization") || strings.Contains(strings.ToLower(logs), "bearer") {
		t.Errorf("the logs mention the Authorization header:\n%s", logs)
	}
}

type errString string

func (e errString) Error() string { return string(e) }
