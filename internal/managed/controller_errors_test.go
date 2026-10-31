package managed

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Any failure that is not a 401 or 403 is unknown: the pass returns nil and
// writes nothing at all. The tests below start from a Provider with a real
// status and a file whose expiry would flip CredentialsExpiring on the pass
// that follows, so "unchanged" is a statement about something that could have
// changed. The failed attempt still counts against the minute.

const unknownExpiry = "2026-11-02T12:00:00Z"

// unknownErrorsAt is a moment within a day of unknownExpiry.
var unknownErrorsAt = time.Date(2026, 11, 1, 13, 0, 0, 0, time.UTC)

// wantUnknownErrorPass runs a pass at unknownErrorsAt and checks that it
// returned nil and wrote nothing.
func wantUnknownErrorPass(t *testing.T, e *env, before *v1alpha1.Provider) {
	t.Helper()
	wait, err := e.trySyncAt(unknownErrorsAt)
	if err != nil {
		t.Fatalf("Sync = %v, want nil: an unknown error is not the pass's error", err)
	}
	if wait != time.Minute {
		t.Fatalf("Sync waited %v, want 1m", wait)
	}
	after := e.provider()
	if after.ResourceVersion != before.ResourceVersion || !apiequality.Semantic.DeepEqual(before.Status, after.Status) {
		t.Fatalf("an unknown error changed the status:\n before %+v\n after  %+v", before.Status, after.Status)
	}
}

// wantRecovery checks that the failed attempt held the gate for a minute and
// that the next read, which works, sees the expiry and the time. reads counts
// the reads the failing gateway received; it was n after the failed pass.
func wantRecovery(t *testing.T, e *env, reads func() int, n int) {
	t.Helper()
	if wait := e.syncAt(unknownErrorsAt.Add(10 * time.Second)); wait != 50*time.Second || reads() != n {
		t.Fatalf("10 s after the failed attempt: waited %v with %d reads, want 50s and %d: an attempt counts against the minute", wait, reads(), n)
	}
	e.syncAt(unknownErrorsAt.Add(time.Minute))
	if reads() != n+1 {
		t.Fatalf("a minute after the failed attempt: %d reads, want %d", reads(), n+1)
	}
	wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, unknownErrorsAt.Add(time.Minute))
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue, "ExpiresWithinADay")
}

func TestUnknownErrorsWriteNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		h    func(s *stub) http.HandlerFunc
	}{
		{"a 500", func(*stub) http.HandlerFunc { return answer(500, `{"error":"boom"}`) }},
		{"a 302 redirect", func(s *stub) http.HandlerFunc {
			return http.RedirectHandler(s.url+"/elsewhere", http.StatusFound).ServeHTTP
		}},
		{"a body that is not JSON", func(*stub) http.HandlerFunc { return answer(200, "this is not json") }},
		{"data null", func(*stub) http.HandlerFunc { return answer(200, `{"data": null}`) }},
		{"an entry with a numeric id", func(*stub) http.HandlerFunc { return answer(200, `{"data":[{"id":5}]}`) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			st := startStub(t, answer(200, okListing))
			e.pointAt(st.baseURL(), unknownExpiry)
			e.syncAt(at(12, 0, 0))
			wantEntry(t, e.entry("example-a"), true, "", at(12, 0, 0))
			e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionFalse, "NotExpiring")
			before := e.provider()

			st.set(tc.h(st))
			wantUnknownErrorPass(t, e, before)
			if n := st.count("/v1/models"); n != 2 {
				t.Fatalf("the gateway received %d reads, want 2: the failing one was made", n)
			}
			if n := st.count("/elsewhere"); n != 0 {
				t.Fatalf("a redirect target received %d requests, want 0: no redirect is followed", n)
			}

			st.set(answer(200, okListing))
			wantRecovery(t, e, func() int { return st.count("/v1/models") }, 2)
		})
	}

	t.Run("a connection refused", func(t *testing.T) {
		e := newEnv(t, withModels(model("example-a")))
		e.pointAt(e.gw.BaseURL(), unknownExpiry)
		e.syncAt(at(12, 0, 0))
		before := e.provider()

		e.pointAt(fmt.Sprintf("http://127.0.0.1:%d/v1", closedPort(t)), unknownExpiry)
		wantUnknownErrorPass(t, e, before)

		e.pointAt(e.gw.BaseURL(), unknownExpiry) // the gateway is back
		wantRecovery(t, e, e.gets, 1)
	})

	t.Run("a 504 from the gateway's no-byte cut", func(t *testing.T) {
		e := newEnv(t, withModels(model("example-a")))
		e.pointAt(e.gw.BaseURL(), unknownExpiry)
		e.syncAt(at(12, 0, 0))
		before := e.provider()

		undo := failReads(e.gw) // a manual stall, and a 100 ms cut
		wantUnknownErrorPass(t, e, before)
		undo()
		reqs := e.gw.Requests()
		if last := reqs[len(reqs)-1]; len(reqs) != 2 || last.Status != http.StatusGatewayTimeout {
			t.Fatalf("the gateway's log = %d requests, the last answered %d, want 2 and 504", len(reqs), last.Status)
		}
		wantRecovery(t, e, e.gets, 2)
	})
}
