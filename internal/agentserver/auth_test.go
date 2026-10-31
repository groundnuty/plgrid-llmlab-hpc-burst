package agentserver

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// TestServerAnswersAsTheGateDoes runs every case through fake.Gate, which holds
// the authentication rule of pkg/agentapi, and through the server, and expects
// both to give the code the case names. The expected status is the one
// agentapi.Error.HTTPStatus gives that code, not the server's own mapping.
// An empty presented value is an absent header.
func TestServerAnswersAsTheGateDoes(t *testing.T) {
	const (
		credRight  = "right"
		credWrong  = "wrong"
		unrecorded = ""
		badID      = "1111111111111111111111111111111" // 31 hex characters
	)
	cases := []struct {
		name                               string
		credential, tag, jobID, instanceID string // what the agent presents
		recordedJob                        string
		want                               agentapi.Code // "" is accepted
	}{
		{"right credential", credRight, tagA, jobA, instanceA, jobA, ""},
		{"wrong credential", credWrong, tagA, jobA, instanceA, jobA, agentapi.CodeUnauthenticated},
		{"no credential", "", tagA, jobA, instanceA, jobA, agentapi.CodeUnauthenticated},
		{"unknown tag", credRight, "tag-unknown", jobA, instanceA, jobA, agentapi.CodeUnauthenticated},
		{"no tag", credRight, "", jobA, instanceA, jobA, agentapi.CodeUnauthenticated},
		{"wrong credential beats another job ID", credWrong, tagA, "job-2", instanceA, jobA, agentapi.CodeUnauthenticated},
		{"wrong credential beats a malformed instance ID", credWrong, tagA, jobA, badID, jobA, agentapi.CodeUnauthenticated},
		{"another job ID", credRight, tagA, "job-2", instanceA, jobA, agentapi.CodeJobMismatch},
		{"no job ID presented, one recorded", credRight, tagA, "", instanceA, jobA, agentapi.CodeJobMismatch},
		{"none recorded, one presented", credRight, tagA, jobA, instanceA, unrecorded, agentapi.CodeJobMismatch},
		{"none recorded, none presented", credRight, tagA, "", instanceA, unrecorded, agentapi.CodeJobMismatch},
		{"another job ID beats a malformed instance ID", credRight, tagA, "job-2", badID, jobA, agentapi.CodeJobMismatch},
		{"malformed agent instance ID", credRight, tagA, jobA, badID, jobA, agentapi.CodeBadRequest},
		{"upper-case agent instance ID", credRight, tagA, jobA, "ABCDEF0123456789ABCDEF0123456789", jobA, agentapi.CodeBadRequest},
		{"no agent instance ID", credRight, tagA, jobA, "", jobA, agentapi.CodeBadRequest},
	}
	calls := []struct {
		method, path, backendMethod string
	}{
		{http.MethodPost, agentapi.PathRegister, fake.MethodRegister},
		{http.MethodGet, agentapi.PathDesired, fake.MethodDesired},
		{http.MethodPost, agentapi.PathReport, fake.MethodReport},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			x := newRig(t)
			x.lookup.set(tagA, Allocation{Namespace: namespaceA, Name: nameA, UID: uidA, JobID: c.recordedJob})
			gate := fake.NewGate(fake.NewBackend())
			gate.Add(tagA, fake.GateAllocation{Namespace: namespaceA, Name: nameA, UID: uidA, Credential: x.credential, JobID: c.recordedJob})

			id := ident{tag: c.tag, jobID: c.jobID, instanceID: c.instanceID}
			switch c.credential {
			case credRight:
				id.credential = x.credential
			case credWrong:
				id.credential = strings.Repeat("cd", 32) // a key's form that is not the Allocation's
			}
			presented := gate.As(id.credential, id.tag, id.jobID)
			asCaller := agentapi.Caller{AgentInstanceID: id.instanceID}
			report := agentapi.Report{Time: time.Unix(1, 0).UTC()}

			for _, call := range calls {
				var gateErr error
				switch call.backendMethod {
				case fake.MethodRegister:
					_, gateErr = presented.Register(context.Background(), asCaller, agentapi.RegisterRequest{Node: "n", CPUArch: "x86_64"})
				case fake.MethodDesired:
					_, gateErr = presented.Desired(context.Background(), asCaller)
				case fake.MethodReport:
					gateErr = presented.Report(context.Background(), asCaller, report)
				}
				if got := codeOf(gateErr); got != c.want {
					t.Fatalf("fake.Gate %s answered %q, but the case expects %q: the case contradicts the Gate", call.backendMethod, got, c.want)
				}

				var body []byte
				switch call.path {
				case agentapi.PathRegister:
					body = registerBody(t)
				case agentapi.PathReport:
					body = reportBody(t, report)
				}
				rec := x.do(call.method, call.path, id, body)
				if c.want == "" {
					if rec.Code/100 != 2 {
						t.Errorf("%s: status %d (%s), want 2xx", call.path, rec.Code, rec.Body)
					}
					continue
				}
				wantError(t, rec, c.want)
				if n := x.backend.Calls(call.backendMethod); n != 0 {
					t.Errorf("%s: the Backend saw %d %s calls from a refused agent, want 0", call.path, n, call.backendMethod)
				}
			}
			if c.want != "" {
				x.wantBackendCalls(0)
			}
		})
	}
}

func codeOf(err error) agentapi.Code {
	var e *agentapi.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

func TestBackendSeesTheCallerBuiltFromTheAllocationAndTheHeaders(t *testing.T) {
	x := newRig(t)
	x.register(x.good())

	regs := x.backend.Registrations(uidA)
	if len(regs) != 1 {
		t.Fatalf("registrations = %d, want 1", len(regs))
	}
	want := agentapi.Caller{Namespace: namespaceA, Allocation: nameA, AllocationUID: uidA, Tag: tagA, JobID: jobA, AgentInstanceID: instanceA}
	if regs[0].Caller != want {
		t.Errorf("Caller = %+v, want %+v", regs[0].Caller, want)
	}
}

func TestEveryCallIsAuthenticatedAfresh(t *testing.T) {
	x := newRig(t)
	x.register(x.good())
	if rec := x.do(http.MethodGet, agentapi.PathDesired, x.good(), nil); rec.Code != http.StatusOK {
		t.Fatalf("desired before the rotation: status %d", rec.Code)
	}

	// The controller replaces the credential file: the old credential stops
	// working on the next call, with no session to end.
	rotated, err := agentapi.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := agentapi.WriteKeyFile(agentapi.AllocationCredentialPath(x.host.CredentialsDir(), uidA), rotated); err != nil {
		t.Fatal(err)
	}
	wantError(t, x.do(http.MethodGet, agentapi.PathDesired, x.good(), nil), agentapi.CodeUnauthenticated)
	if n := x.backend.Calls(fake.MethodDesired); n != 1 {
		t.Errorf("Desired calls = %d, want 1 (the one before the rotation)", n)
	}
	id := x.good()
	id.credential = rotated
	if rec := x.do(http.MethodGet, agentapi.PathDesired, id, nil); rec.Code != http.StatusOK {
		t.Errorf("desired with the rotated credential: status %d (%s)", rec.Code, rec.Body)
	}
}

func TestAMissingCredentialFileIsUnauthenticated(t *testing.T) {
	x := newRig(t)
	if err := os.Remove(agentapi.AllocationCredentialPath(x.host.CredentialsDir(), uidA)); err != nil {
		t.Fatal(err)
	}
	rec := x.do(http.MethodPost, agentapi.PathRegister, x.good(), registerBody(t))
	wantError(t, rec, agentapi.CodeUnauthenticated)
	x.wantBackendCalls(0)
}

func TestAMalformedCredentialFileIsInternalAndLogged(t *testing.T) {
	x := newRig(t)
	path := agentapi.AllocationCredentialPath(x.host.CredentialsDir(), uidA)
	if err := os.Chmod(path, 0o644); err != nil { // readable by others: ReadKeyFile refuses it
		t.Fatal(err)
	}
	rec := x.do(http.MethodPost, agentapi.PathRegister, x.good(), registerBody(t))
	wantError(t, rec, agentapi.CodeInternal)
	x.wantBackendCalls(0)
	if !strings.Contains(x.logs.String(), "no permission bit for the group or others may be set") {
		t.Errorf("the credential file's error was not logged:\n%s", x.logs)
	}
}

func TestALookupThatCannotAnswerIsUnavailable(t *testing.T) {
	x := newRig(t)
	x.lookup.err = errors.New("cache not synced")
	wantError(t, x.do(http.MethodPost, agentapi.PathRegister, x.good(), registerBody(t)), agentapi.CodeUnavailable)
	x.wantBackendCalls(0)
}

func TestAnAmbiguousTagIsUnauthenticatedAndLogged(t *testing.T) {
	x := newRig(t)
	x.lookup.err = errors.Join(ErrAmbiguous, errors.New("ns-a/x and ns-b/y share a job tag"))
	wantError(t, x.do(http.MethodPost, agentapi.PathRegister, x.good(), registerBody(t)), agentapi.CodeUnauthenticated)
	x.wantBackendCalls(0)
	if !strings.Contains(x.logs.String(), "ns-a/x and ns-b/y share a job tag") {
		t.Errorf("the ambiguity was not logged:\n%s", x.logs)
	}
}

func TestAnAllocationUIDThatIsNotAFileNameIsInternal(t *testing.T) {
	for _, uid := range []string{"", "..", "a/b", "../escape"} {
		t.Run("uid="+uid, func(t *testing.T) {
			x := newRig(t)
			x.lookup.set(tagA, Allocation{Namespace: namespaceA, Name: nameA, UID: uid, JobID: jobA})
			wantError(t, x.do(http.MethodPost, agentapi.PathRegister, x.good(), registerBody(t)), agentapi.CodeInternal)
			x.wantBackendCalls(0)
		})
	}
}

func TestOtherAuthorizationSchemesAreUnauthenticated(t *testing.T) {
	x := newRig(t)
	for name, value := range map[string]string{
		"basic":        "Basic " + x.credential,
		"bare token":   x.credential,
		"empty bearer": "Bearer ",
	} {
		t.Run(name, func(t *testing.T) {
			id := x.good()
			id.credential = "" // apply sets no Authorization header
			req := httptest.NewRequest(http.MethodPost, agentapi.PathRegister, bytes.NewReader(registerBody(t)))
			id.apply(req.Header)
			req.Header.Set("Authorization", value)
			rec := httptest.NewRecorder()
			x.srv.Handler().ServeHTTP(rec, req)
			wantError(t, rec, agentapi.CodeUnauthenticated)
			x.wantBackendCalls(0)
		})
	}
}
