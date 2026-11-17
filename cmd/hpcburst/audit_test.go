package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// auditEvent is the part of an audit.k8s.io/v1 event the tests read.
type auditEvent struct {
	Level string `json:"level"`
	Verb  string `json:"verb"`
	// User is who made the request, as the API server authenticated it (user.username): a controller's
	// ServiceAccount, an agent's, or the administrator.
	User struct {
		Username string `json:"username"`
	} `json:"user"`
	ObjectRef struct {
		APIGroup  string `json:"apiGroup"`
		Resource  string `json:"resource"`
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
	} `json:"objectRef"`
	// ResponseStatus is how the API server answered: the code is 403 for a request RBAC refused.
	ResponseStatus struct {
		Code int `json:"code"`
	} `json:"responseStatus"`
	hasBody    bool // requestObject or responseObject is present
	hasRequest bool // requestObject is present: a Request-level event has it and no responseObject
}

// readAudit returns the text of the audit log and its events; a line that is not a JSON object fails
// the test. With completeOnly, anything after the last newline is left out, because the API server
// may be in the middle of writing it.
func readAudit(t testing.TB, path string, completeOnly bool) (string, []auditEvent) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("audit log: %v", err)
	}
	if completeOnly {
		b = b[:bytes.LastIndexByte(b, '\n')+1]
	}
	var evs []auditEvent
	if len(b) == 0 {
		return "", evs
	}
	for n, line := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
		var fields map[string]json.RawMessage
		var e auditEvent
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			t.Fatalf("audit log line %d is not JSON (%v): %.200q", n+1, err, line)
		}
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("audit log line %d: %v: %.200q", n+1, err, line)
		}
		_, req := fields["requestObject"]
		_, resp := fields["responseObject"]
		e.hasBody, e.hasRequest = req || resp, req
		evs = append(evs, e)
	}
	return string(b), evs
}

func findAudit(evs []auditEvent, verb, resource, name string) *auditEvent {
	for i := range evs {
		if e := &evs[i]; e.Verb == verb && e.ObjectRef.Resource == resource && e.ObjectRef.Name == name {
			return e
		}
	}
	return nil
}
