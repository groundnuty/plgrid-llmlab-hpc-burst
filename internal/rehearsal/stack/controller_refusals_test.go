package stack

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// auditLine is one audit.k8s.io/v1 event as the API server writes it, with the fields ControllerAudit reads and others
// it must not print.
func auditLine(t *testing.T, user, verb, resource, sub, group string, code int) string {
	t.Helper()
	e := map[string]any{
		"kind": "Event", "level": "Metadata", "verb": verb,
		"user":           map[string]any{"username": user},
		"requestURI":     "/apis/" + group + "/v1alpha1/namespaces/example-ns/" + resource + "/example-object-name",
		"objectRef":      map[string]any{"apiGroup": group, "resource": resource, "subresource": sub, "name": "example-object-name", "namespace": "example-ns"},
		"responseStatus": map[string]any{"code": code},
		"sourceIPs":      []string{"192.0.2.7"},
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return string(b) + "\n"
}

func writeAudit(t *testing.T, dir, name, text string, gz bool) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0o700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, "logs", name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if !gz {
		if _, err := f.WriteString(text); err != nil {
			t.Fatal(err)
		}
		return
	}
	zw := gzip.NewWriter(f)
	if _, err := zw.Write([]byte(text)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
}

const hs = "system:serviceaccount:hpcburst-system:"

// allThree is the stack's controller identities, as the check asks for them.
var allThree = []string{"hpcburst-lifecycle", "hpcburst-provider", "hpcburst-front"}

// answeredByAll is one request of each controller identity that the API server answered 2xx.
func answeredByAll(t *testing.T) string {
	t.Helper()
	return auditLine(t, hs+"hpcburst-lifecycle", "get", "leases", "", "coordination.k8s.io", 200) +
		auditLine(t, hs+"hpcburst-provider", "list", "providers", "", "hpcburst.groundnuty.github.io", 200) +
		auditLine(t, hs+"hpcburst-front", "watch", "instances", "", "hpcburst.groundnuty.github.io", 200)
}

func TestControllerAuditCountsOnlyAControllersRefusals(t *testing.T) {
	state := filepath.Join(t.TempDir(), "example-state-dir")
	live := auditLine(t, hs+"hpcburst-lifecycle", "patch", "modeldeployments", "", "hpcburst.groundnuty.github.io", 403) +
		auditLine(t, hs+"hpcburst-lifecycle", "patch", "modeldeployments", "", "hpcburst.groundnuty.github.io", 403) +
		auditLine(t, hs+"hpcburst-lifecycle", "patch", "modeldeployments", "", "hpcburst.groundnuty.github.io", 200) + // answered
		auditLine(t, hs+"hpcburst-front", "update", "leases", "", "coordination.k8s.io", 403) +
		auditLine(t, hs+"hpcburst-provider", "update", "allocations", "status", "hpcburst.groundnuty.github.io", 403) +
		auditLine(t, "system:serviceaccount:hpcburst-agents:agent-a", "delete", "allocations", "", "hpcburst.groundnuty.github.io", 403) + // an agent's refusal is the agents' policy's
		auditLine(t, "example-admin", "delete", "allocations", "", "hpcburst.groundnuty.github.io", 403) +
		auditLine(t, hs+"hpcburst-lifecycle", "get", "operations", "", "hpcburst.groundnuty.github.io", 404) // not found is not a refusal
	writeAudit(t, state, "audit.log", live, false)
	// A rotated file, gzip, and one plain, are read too.
	writeAudit(t, state, "audit-2026-11-17T10-00-00.000.log.gz", auditLine(t, hs+"hpcburst-lifecycle", "patch", "modeldeployments", "", "hpcburst.groundnuty.github.io", 403), true)
	writeAudit(t, state, "audit-2026-11-17T11-00-00.000.log", auditLine(t, hs+"hpcburst-front", "update", "leases", "", "coordination.k8s.io", 403), false)
	rep, err := ControllerAudit(state)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"1 x hpcburst-provider update allocations/status (hpcburst.groundnuty.github.io)",
		"2 x hpcburst-front update leases (coordination.k8s.io)",
		"3 x hpcburst-lifecycle patch modeldeployments (hpcburst.groundnuty.github.io)",
	}
	if !slices.Equal(rep.Refusals, want) {
		t.Fatalf("refusals\n got %q\nwant %q", rep.Refusals, want)
	}
	if rep.Answered["hpcburst-lifecycle"] != 1 || rep.Answered["hpcburst-front"] != 0 || rep.Seen != 8 {
		t.Fatalf("answered %v, seen %d; want 1 for the lifecycle identity, 0 for the front and 8 requests of identities (the agent's and the admin's are not)", rep.Answered, rep.Seen)
	}
	// Nothing but counts and verbs and resources: no path, no object, no address, no user other than the identity's own name.
	all := strings.Join(rep.Problems(allThree), "\n") + "\n" + rep.Counts(allThree)
	for _, leak := range []string{state, "example-state-dir", "example-object-name", "example-ns", "192.0.2.7", "example-admin", "agent-a", "/apis/"} {
		if strings.Contains(all, leak) {
			t.Errorf("the output names %q: %s", leak, all)
		}
	}
}

// A log in which each identity was answered 2xx and none refused has no problem, and its tally says so.
func TestControllerAuditOfAStackWithNoneIsClean(t *testing.T) {
	state := t.TempDir()
	writeAudit(t, state, "audit.log", answeredByAll(t)+auditLine(t, hs+"hpcburst-lifecycle", "update", "instances", "status", "hpcburst.groundnuty.github.io", 201), false)
	rep, err := ControllerAudit(state)
	if err != nil {
		t.Fatal(err)
	}
	if p := rep.Problems(allThree); len(p) != 0 {
		t.Fatalf("a clean log has problems: %q", p)
	}
	if got, want := rep.Counts(allThree), "hpcburst-lifecycle: 2 answered 2xx, hpcburst-provider: 1 answered 2xx, hpcburst-front: 1 answered 2xx, 0 refused 403"; got != want {
		t.Fatalf("the tally\n got %q\nwant %q", got, want)
	}
}

// "No 403" proves nothing from a log that holds no 2xx request of an identity: each identity that was to run needs one, whatever
// the others did, and a request answered 401, 404 or 500 does not count for it.
func TestControllerAuditNeedsASuccessfulRequestFromEachIdentity(t *testing.T) {
	for _, missing := range allThree {
		t.Run("without "+missing, func(t *testing.T) {
			state := t.TempDir()
			var text strings.Builder
			for _, name := range allThree {
				if name == missing {
					for _, code := range []int{401, 404, 500} { // answered, but not successfully
						text.WriteString(auditLine(t, hs+name, "get", "providers", "", "hpcburst.groundnuty.github.io", code))
					}
					continue
				}
				text.WriteString(auditLine(t, hs+name, "get", "providers", "", "hpcburst.groundnuty.github.io", 200))
			}
			writeAudit(t, state, "audit.log", text.String(), false)
			rep, err := ControllerAudit(state)
			if err != nil {
				t.Fatal(err)
			}
			got := rep.Problems(allThree)
			if len(got) != 1 || !strings.Contains(got[0], hs+missing+" had answered 2xx") {
				t.Fatalf("a log with no 2xx request of %s: %q", missing, got)
			}
		})
	}
}

// An audit file that is there and empty, or that holds only requests of no controller identity, fails for all three identities.
func TestControllerAuditOfAnEmptyOrForeignLogFailsForEveryIdentity(t *testing.T) {
	for name, text := range map[string]string{
		"empty": "",
		"foreign": auditLine(t, "example-admin", "get", "pods", "", "", 200) +
			auditLine(t, "system:serviceaccount:hpcburst-agents:agent-a", "get", "allocations", "", "hpcburst.groundnuty.github.io", 403),
	} {
		t.Run(name, func(t *testing.T) {
			state := t.TempDir()
			writeAudit(t, state, "audit.log", text, false)
			rep, err := ControllerAudit(state)
			if err != nil {
				t.Fatal(err)
			}
			if rep.Seen != 0 || len(rep.Problems(allThree)) != 3 {
				t.Fatalf("seen %d, problems %q; want 0 requests and one problem for each identity", rep.Seen, rep.Problems(allThree))
			}
			rec := &recordingT{T: t}
			(&Host{State: state}).AssertControllerAudit(rec)
			if len(rec.errors) != 3 {
				t.Fatalf("AssertControllerAudit on such a log recorded %q; want one failure for each identity", rec.errors)
			}
		})
	}
}

// A missing log is an error, not "none": the log is the run's only evidence, and a stack that wrote none must not pass.
func TestControllerAuditOfAStackWithNoLogIsAnError(t *testing.T) {
	if _, err := ControllerAudit(t.TempDir()); err == nil {
		t.Fatal("a state directory with no audit log was read")
	}
	rec := &recordingT{T: t}
	(&Host{State: t.TempDir()}).AssertControllerAudit(rec)
	if len(rec.errors) != 1 || !strings.Contains(rec.errors[0], "audit log") {
		t.Fatalf("AssertControllerAudit on a state with no log recorded %q; want one failure", rec.errors)
	}
}

// The API server may be in the middle of its last line: that is no refusal and no error; a bad line before the end is an error.
func TestControllerAuditToleratesACutLastLineOnly(t *testing.T) {
	state := t.TempDir()
	good := auditLine(t, hs+"hpcburst-lifecycle", "patch", "modeldeployments", "", "hpcburst.groundnuty.github.io", 403)
	writeAudit(t, state, "audit.log", good+`{"kind":"Event","verb":"get","user":{"usern`, false)
	rep, err := ControllerAudit(state)
	if err != nil || len(rep.Refusals) != 1 {
		t.Fatalf("a cut last line: %q, %v", rep.Refusals, err)
	}
	writeAudit(t, state, "audit.log", `not json`+"\n"+good, false)
	if _, err := ControllerAudit(state); err == nil {
		t.Fatal("a line that is not an audit event, followed by another, was skipped")
	}
}

// AssertControllerAudit fails the test it is given once for each refusal, and passes a clean log.
func TestAssertControllerAuditFailsTheTest(t *testing.T) {
	state := t.TempDir()
	writeAudit(t, state, "audit.log", answeredByAll(t)+auditLine(t, hs+"hpcburst-lifecycle", "delete", "modeldeployments", "", "hpcburst.groundnuty.github.io", 403), false)
	rec := &recordingT{T: t}
	(&Host{State: state}).AssertControllerAudit(rec)
	if len(rec.errors) != 1 || !strings.Contains(rec.errors[0], "1 x hpcburst-lifecycle delete modeldeployments") {
		t.Fatalf("the failures it recorded: %q", rec.errors)
	}
	writeAudit(t, state, "audit.log", answeredByAll(t)+auditLine(t, hs+"hpcburst-lifecycle", "delete", "modeldeployments", "", "hpcburst.groundnuty.github.io", 200), false)
	rec = &recordingT{T: t}
	(&Host{State: state}).AssertControllerAudit(rec)
	if len(rec.errors) != 0 {
		t.Fatalf("a clean log failed the test: %q", rec.errors)
	}
}

// recordingT is a T whose Errorf is recorded and does not fail the test that runs it.
type recordingT struct {
	T
	errors []string
}

func (r *recordingT) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}
func (r *recordingT) Helper() {}
