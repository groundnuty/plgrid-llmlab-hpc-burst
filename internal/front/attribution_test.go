package front

import (
	"net/http"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// HBEP-5, Attribution headers: the headers are on every response, streamed or
// not, success or error; each response's usage record holds the same facts,
// keyed by request ID, so dropped headers lose nothing. Expected values are
// what the fixtures were configured with (the Instance's name, UID and
// deployment ID; the Provider's name; the catalog entry's managed ID).

// checkRecordFacts asserts a usage record names the target the fixture
// configured. deploymentID is the full ID; the header carries its first 12
// hex characters.
func checkRecordFacts(t *testing.T, rec usage.Record, requestID string, want usage.Record) {
	t.Helper()
	if rec.RequestID != requestID {
		t.Errorf("record requestID = %q, want the header's %q", rec.RequestID, requestID)
	}
	got := usage.Record{Emitter: rec.Emitter, Provider: rec.Provider, ModelDeployment: rec.ModelDeployment, DeploymentID: rec.DeploymentID,
		Instance: rec.Instance, InstanceUID: rec.InstanceUID, Status: rec.Status}
	if got != want {
		t.Errorf("usage record facts\n got %+v\nwant %+v", got, want)
	}
}

// TestAttributionOnInstanceReplies: a non-streamed success from an Instance
// carries all six headers, and the record holds the same facts.
func TestAttributionOnInstanceReplies(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	resp := s.post("/v1/chat/completions", errChatBody(efModel, false, "hello there"))
	if body := readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	id := checkAttribution(t, resp, s.instanceAttr("instance"))
	checkRecordFacts(t, s.record(id), id, usage.Record{Emitter: "instance", Provider: "helios-a", ModelDeployment: "ns/md",
		DeploymentID: efDeployID, Instance: "ns/md-0", InstanceUID: string(s.uid), Status: 200})

	// A second request has its own ID.
	resp2 := s.post("/v1/chat/completions", errChatBody(efModel, false, "hello again"))
	readAll(t, resp2)
	id2 := checkAttribution(t, resp2, s.instanceAttr("instance"))
	if id2 == id {
		t.Errorf("two requests share the request ID %q", id)
	}
}

// TestAttributionOnStreams: the headers are on the streamed response itself,
// before its first event is read, whether the stream completes or is cut.
func TestAttributionOnStreams(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	t.Run("a completed stream", func(t *testing.T) {
		s.e.Set(enginetest.Behaviour{Tokens: 4})
		resp := s.post("/v1/chat/completions", errChatBody(efModel, true, "hello there"))
		id := checkAttribution(t, resp, s.instanceAttr("instance")) // before the body is read
		if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
			t.Errorf("Content-Type = %q, want text/event-stream", ct)
		}
		readAll(t, resp)
		rec := s.record(id)
		checkRecordFacts(t, rec, id, usage.Record{Emitter: "instance", Provider: "helios-a", ModelDeployment: "ns/md",
			DeploymentID: efDeployID, Instance: "ns/md-0", InstanceUID: string(s.uid), Status: 200})
		if !rec.Stream {
			t.Errorf("record stream = false, want true")
		}
	})
	t.Run("a stream the engine cuts", func(t *testing.T) {
		s.e.Set(enginetest.Behaviour{Tokens: 6, KillAfter: 2})
		resp := s.post("/v1/chat/completions", errChatBody(efModel, true, "hello there"))
		id := checkAttribution(t, resp, s.instanceAttr("instance"))
		readAll(t, resp)
		rec := s.record(id)
		checkRecordFacts(t, rec, id, usage.Record{Emitter: "instance", Provider: "helios-a", ModelDeployment: "ns/md",
			DeploymentID: efDeployID, Instance: "ns/md-0", InstanceUID: string(s.uid), Status: 200})
	})
}

// TestAttributionOnInstanceErrors: an error from the engine passes through
// with emitter instance and the target's headers.
func TestAttributionOnInstanceErrors(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	s.e.Set(enginetest.Behaviour{Status: 500, ErrorBody: `{"error":{"message":"engine fault"}}`})
	resp := s.post("/v1/chat/completions", errChatBody(efModel, false, "hello there"))
	readAll(t, resp)
	if resp.StatusCode != 500 {
		t.Fatalf("status %d, want 500", resp.StatusCode)
	}
	id := checkAttribution(t, resp, s.instanceAttr("instance"))
	checkRecordFacts(t, s.record(id), id, usage.Record{Emitter: "instance", Provider: "helios-a", ModelDeployment: "ns/md",
		DeploymentID: efDeployID, Instance: "ns/md-0", InstanceUID: string(s.uid), Status: 500})
}

// TestAttributionOnLLMLabReplies: a reply served by LLMLab names the Provider
// and the entry's managed ID with emitter provider, and no Instance.
func TestAttributionOnLLMLabReplies(t *testing.T) {
	m := newErrManagedFix(t, nil)
	t.Run("a whole reply", func(t *testing.T) {
		resp := m.post("/v1/chat/completions", errChatBody(efModel, false, "hello there"))
		if body := readAll(t, resp); resp.StatusCode != 200 {
			t.Fatalf("status %d: %s", resp.StatusCode, body)
		}
		id := checkAttribution(t, resp, efManagedAttr)
		checkRecordFacts(t, m.record(id), id, usage.Record{Emitter: "provider", Provider: "llmlab-a", ModelDeployment: "ns/md-managed",
			DeploymentID: efManagedID, Status: 200})
	})
	t.Run("a stream", func(t *testing.T) {
		resp := m.post("/v1/chat/completions", errChatBody(efModel, true, "hello there"))
		id := checkAttribution(t, resp, efManagedAttr)
		readAll(t, resp)
		checkRecordFacts(t, m.record(id), id, usage.Record{Emitter: "provider", Provider: "llmlab-a", ModelDeployment: "ns/md-managed",
			DeploymentID: efManagedID, Status: 200})
	})
}

// TestAttributionOnFrontAnswers: what the front answers itself carries the
// request ID and emitter front, and no target: the 404 under /agent/v1/
// (HBEP-5, Listeners) and the model listing. The listing's ID is checked
// against its usage record; the 404 is answered before any exchange exists,
// so it has no record, and its ID is checked to differ between two requests.
func TestAttributionOnFrontAnswers(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	t.Run("404 under /agent/v1/", func(t *testing.T) {
		ids := map[string]bool{}
		for i := 0; i < 2; i++ {
			resp := s.post("/agent/v1/anything", `{}`)
			readAll(t, resp)
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("status %d, want 404", resp.StatusCode)
			}
			ids[checkAttribution(t, resp, efFrontOnly)] = true
		}
		if len(ids) != 2 {
			t.Errorf("two requests carry %d distinct request IDs, want 2", len(ids))
		}
	})
	t.Run("GET /v1/models", func(t *testing.T) {
		resp := s.errGet("/v1/models")
		readAll(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status %d, want 200", resp.StatusCode)
		}
		id := checkAttribution(t, resp, efFrontOnly)
		if rec := s.record(id); rec.Status != 200 || rec.Emitter != "front" {
			t.Errorf("usage record under %s: status %d emitter %q, want 200 front", id, rec.Status, rec.Emitter)
		}
	})
}

// TestUsageRecordAttributesAStreamWithoutHeaders: when a stream's headers are
// dropped on the way, the usage record still names the target. The test
// ignores every response header and reads the only record in the log.
func TestUsageRecordAttributesAStreamWithoutHeaders(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	s.e.Set(enginetest.Behaviour{Tokens: 4})
	resp := s.post("/v1/chat/completions", errChatBody(efModel, true, "hello there"))
	readAll(t, resp) // the headers are not looked at
	recs := s.records()
	if len(recs) != 1 {
		t.Fatalf("%d records, want 1", len(recs))
	}
	r := recs[0]
	want := usage.Record{Emitter: "instance", Provider: "helios-a", ModelDeployment: "ns/md",
		DeploymentID: efDeployID, Instance: "ns/md-0", InstanceUID: string(s.uid), Status: 200}
	checkRecordFacts(t, r, r.RequestID, want)
	if r.RequestID == "" {
		t.Errorf("the record has no request ID")
	}
	if !r.Stream || r.Path != "/v1/chat/completions" || r.RequestedModel != efModel || r.Level != "model" || r.ClientKeyID != "tester" {
		t.Errorf("record: stream %v path %q requestedModel %q level %q clientKeyID %q", r.Stream, r.Path, r.RequestedModel, r.Level, r.ClientKeyID)
	}
}
