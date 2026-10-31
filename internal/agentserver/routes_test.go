package agentserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

func TestAnUnknownPathIs404WithAnErrorBodyAndNeedsNoCredential(t *testing.T) {
	x := newRig(t)
	for _, path := range []string{"/", "/agent/v1/", "/agent/v1/unknown", "/agent/v2/register", agentapi.PathRegister + "/"} {
		t.Run(path, func(t *testing.T) {
			rec := x.do(http.MethodGet, path, ident{}, nil) // no identity at all
			if rec.Code != http.StatusNotFound {
				t.Errorf("status = %d, want 404", rec.Code)
			}
			errorOf(t, rec.Code, rec.Header(), rec.Body.Bytes())
		})
	}
	x.wantBackendCalls(0)
}

func TestAWrongMethodIs405WithAnErrorBodyAndAllowHeader(t *testing.T) {
	x := newRig(t)
	cases := []struct{ method, path, allow string }{
		{http.MethodGet, agentapi.PathRegister, http.MethodPost},
		{http.MethodPut, agentapi.PathRegister, http.MethodPost},
		{http.MethodPost, agentapi.PathDesired, http.MethodGet},
		{http.MethodHead, agentapi.PathDesired, http.MethodGet},
		{http.MethodGet, agentapi.PathReport, http.MethodPost},
		{http.MethodDelete, agentapi.PathReport, http.MethodPost},
	}
	for _, c := range cases {
		t.Run(c.method+" "+c.path, func(t *testing.T) {
			rec := x.do(c.method, c.path, x.good(), nil)
			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("status = %d, want 405", rec.Code)
			}
			if got := rec.Header().Get("Allow"); got != c.allow {
				t.Errorf("Allow = %q, want %q", got, c.allow)
			}
			errorOf(t, rec.Code, rec.Header(), rec.Body.Bytes())
		})
	}
	x.wantBackendCalls(0)
}

func TestRegisterAnswers200WithTheBackendsResponse(t *testing.T) {
	x := newRig(t)
	rec := x.do(http.MethodPost, agentapi.PathRegister, x.good(), registerBody(t))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%s)", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != agentapi.ContentType {
		t.Errorf("Content-Type = %q, want %q", ct, agentapi.ContentType)
	}
	var resp agentapi.RegisterResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ProtocolVersion != "v1" {
		t.Errorf("ProtocolVersion = %q, want v1", resp.ProtocolVersion)
	}
	regs := x.backend.Registrations(uidA)
	if len(regs) != 1 || regs[0].Request.Node != "node-example" || regs[0].Request.CPUArch != "x86_64" {
		t.Errorf("the Backend's registration = %+v, want the request's Node and CPUArch", regs)
	}
}

func TestReportAnswers204AndTheBackendHoldsTheReport(t *testing.T) {
	x := newRig(t)
	x.register(x.good())
	sent := agentapi.Report{Time: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC), Instance: &agentapi.InstanceReport{UID: "inst-uid-1", Generation: 3}}

	rec := x.do(http.MethodPost, agentapi.PathReport, x.good(), reportBody(t, sent))
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Fatalf("status = %d, body %q, want 204 and no body", rec.Code, rec.Body)
	}
	got, ok := x.backend.LastReport(uidA)
	if !ok || !got.Time.Equal(sent.Time) || got.Instance == nil || got.Instance.UID != "inst-uid-1" || got.Instance.Generation != 3 {
		t.Errorf("the Backend's report = %+v (%v), want the one sent", got, ok)
	}
}

func TestDesiredAnswersTheBackendsStateWithItsVersion(t *testing.T) {
	x := newRig(t)
	x.register(x.good())
	want := agentapi.DesiredState{Action: agentapi.ActionIdle, Settings: agentapi.AgentSettings{ProbeInterval: agentapi.Duration(7 * time.Second)}}.WithVersion()
	x.backend.SetDesired(uidA, want)

	rec := x.do(http.MethodGet, agentapi.PathDesired, x.good(), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%s)", rec.Code, rec.Body)
	}
	var got agentapi.DesiredState
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Version != want.Version || got.Action != agentapi.ActionIdle || got.Settings.ProbeInterval != agentapi.Duration(7*time.Second) {
		t.Errorf("state = %+v, want %+v", got, want)
	}
}

func TestANotRegisteredAgentGets409WithThatCode(t *testing.T) {
	x := newRig(t) // authenticated, never registered
	for _, c := range []struct {
		method, path string
		body         []byte
	}{
		{http.MethodGet, agentapi.PathDesired, nil},
		{http.MethodPost, agentapi.PathReport, reportBody(t, agentapi.Report{Time: time.Unix(1, 0).UTC()})},
	} {
		rec := x.do(c.method, c.path, x.good(), c.body)
		if rec.Code != http.StatusConflict {
			t.Errorf("%s: status = %d, want 409", c.path, rec.Code)
		}
		if e := errorOf(t, rec.Code, rec.Header(), rec.Body.Bytes()); e.Code != agentapi.CodeNotRegistered {
			t.Errorf("%s: code = %q, want %q", c.path, e.Code, agentapi.CodeNotRegistered)
		}
	}
}

func TestABackendErrorOfTheProtocolGoesOutAsItIs(t *testing.T) {
	x := newRig(t)
	sent := agentapi.Errorf(agentapi.CodeUnavailable, "caches are not synced")
	x.backend.FailNext(fake.MethodRegister, fmt.Errorf("wrapped by the Backend: %w", sent))

	rec := x.do(http.MethodPost, agentapi.PathRegister, x.good(), registerBody(t))
	wantError(t, rec, agentapi.CodeUnavailable)
	if e := errorOf(t, rec.Code, rec.Header(), rec.Body.Bytes()); e.Message != sent.Message {
		t.Errorf("message = %q, want %q", e.Message, sent.Message)
	}
}

func TestAnyOtherBackendErrorIsInternalWithAGenericMessageAndTheCauseIsLogged(t *testing.T) {
	x := newRig(t)
	x.backend.FailNext(fake.MethodRegister, errors.New("etcdserver: request timed out"))

	rec := x.do(http.MethodPost, agentapi.PathRegister, x.good(), registerBody(t))
	wantError(t, rec, agentapi.CodeInternal)
	if strings.Contains(rec.Body.String(), "etcdserver") {
		t.Errorf("the agent was told the real error: %s", rec.Body)
	}
	if !strings.Contains(x.logs.String(), "etcdserver: request timed out") {
		t.Errorf("the real error was not logged:\n%s", x.logs)
	}
}

func TestAMalformedBodyIs400AndNeverReachesTheBackend(t *testing.T) {
	x := newRig(t)
	wrongType := map[string]string{agentapi.PathRegister: `{"node": 7}`, agentapi.PathReport: `{"time": 7}`}
	for _, path := range []string{agentapi.PathRegister, agentapi.PathReport} {
		for name, body := range map[string][]byte{"not JSON": []byte("{"), "empty": nil, "wrong type": []byte(wrongType[path])} {
			t.Run(path+" "+name, func(t *testing.T) {
				wantError(t, x.do(http.MethodPost, path, x.good(), body), agentapi.CodeBadRequest)
			})
		}
	}
	x.wantBackendCalls(0)
}

func TestARequestBodyIsReadOnlyAfterAuthentication(t *testing.T) {
	x := newRig(t)
	id := x.good()
	id.credential = strings.Repeat("cd", 32)
	// A body that would be a 400 is a 401 for an agent that is not
	// authenticated: the body is not even read.
	wantError(t, x.do(http.MethodPost, agentapi.PathRegister, id, []byte("{")), agentapi.CodeUnauthenticated)
}

// A desired state above the body limit is the controller's fault: EncodeBody
// calls it BadRequest, which would tell the agent to fix its own request.
func TestAResponseAboveTheLimitIsInternalNotBadRequest(t *testing.T) {
	x := newRig(t)
	x.register(x.good())
	huge := agentapi.DesiredState{Action: agentapi.ActionRun, Instance: &agentapi.DesiredInstance{
		Name: "inst-example", UID: "inst-uid-1",
		Launch: agentapi.Launch{Command: []string{strings.Repeat("x", agentapi.MaxBodyBytes+1)}},
	}}
	x.backend.SetDesired(uidA, huge)

	rec := x.do(http.MethodGet, agentapi.PathDesired, x.good(), nil)
	wantError(t, rec, agentapi.CodeInternal)
	if !strings.Contains(x.logs.String(), "above the") {
		t.Errorf("the encoding failure was not logged:\n%s", x.logs)
	}
}
