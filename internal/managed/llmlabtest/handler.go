package llmlabtest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxBody is the largest request body the Server reads.
const maxBody = 8 << 20

// pollInterval is how often a waiting request re-reads the clock.
const pollInterval = 5 * time.Millisecond

// listedCreated is the "created" time every listed model shows,
// 2026-01-01T00:00:00Z.
const listedCreated = 1767225600

// The inference routes.
const (
	pathChat        = "/v1/chat/completions"
	pathCompletions = "/v1/completions"
	pathResponses   = "/v1/responses"
	pathEmbeddings  = "/v1/embeddings"
	pathModels      = "/v1/models"
)

// exchange is one request in flight.
type exchange struct {
	s       *Server
	w       http.ResponseWriter
	r       *http.Request
	rec     *Request
	arrival time.Time
	cut     time.Duration // the no-byte cut in force at arrival; 0: none
	latency Latency
	echo    string // the Authorization header, when EchoKey is on
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	arrival := s.now()
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	rec := &Request{Time: arrival, Method: r.Method, Path: r.URL.Path, Header: r.Header.Clone(), Body: body}
	defer func() { s.log(*rec) }()

	s.mu.Lock()
	x := &exchange{s: s, w: w, r: r, rec: rec, arrival: arrival, cut: s.cut, latency: s.latency}
	if s.echoKey {
		x.echo = r.Header.Get("Authorization")
	}
	s.mu.Unlock()

	path := r.URL.Path
	switch {
	case err != nil || len(body) > maxBody:
		x.fail(http.StatusBadRequest, "invalid_request_error", "the request body could not be read, or is above 8 MiB", "")
	case path == pathModels:
		if x.method(http.MethodGet) && x.admit() {
			x.list()
		}
	case strings.HasPrefix(path, pathModels+"/") && len(path) > len(pathModels)+1:
		if x.method(http.MethodGet) && x.admit() {
			x.entry(strings.TrimPrefix(path, pathModels+"/"))
		}
	case path == pathChat || path == pathCompletions || path == pathResponses || path == pathEmbeddings:
		if x.method(http.MethodPost) && x.admit() {
			x.infer(path, body)
		}
	default:
		x.fail(http.StatusNotFound, "invalid_request_error", "unknown route "+path, "")
	}
}

// method answers 405 unless the request has method m.
func (x *exchange) method(m string) bool {
	if x.r.Method == m {
		return true
	}
	x.w.Header().Set("Allow", m)
	x.fail(http.StatusMethodNotAllowed, "invalid_request_error", "method "+x.r.Method+" not allowed", "")
	return false
}

// admit waits out any stall, then checks the key. It returns false when it
// answered (504, 401 or 403) or the request ended.
func (x *exchange) admit() bool {
	if !x.waitFirstByte(func(now time.Time) bool { return !x.s.stalledLocked(now) }) {
		return false
	}
	status := x.s.keyStatus(x.r.Header.Get("Authorization"))
	switch status {
	case 0:
		return true
	case http.StatusForbidden:
		x.fail(status, "auth_error", "the key is not allowed to use this gateway", "")
	default:
		x.fail(status, "auth_error", "authentication error: invalid or expired key", "")
	}
	return false
}

// keyStatus returns 0 for an accepted key, or the status that refuses it.
func (s *Server) keyStatus(header string) int {
	scheme, key, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || key == "" {
		return http.StatusUnauthorized
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.keys[key]
	switch {
	case !ok:
		return http.StatusUnauthorized
	case st.reject != 0:
		return st.reject
	case !st.expiry.IsZero() && !s.now().Before(st.expiry):
		return http.StatusUnauthorized
	}
	return 0
}

// waitFirstByte blocks until ready holds, with the Server's lock held while it
// is called. It answers 504 when the no-byte cut passes first, and returns
// false then or when the request ends.
func (x *exchange) waitFirstByte(ready func(now time.Time) bool) bool {
	var deadline time.Time
	if x.cut > 0 {
		deadline = x.arrival.Add(x.cut)
	}
	switch x.s.wait(x.r.Context(), deadline, ready) {
	case waitReady:
		return true
	case waitCut:
		x.fail(http.StatusGatewayTimeout, "timeout", "the upstream sent no byte within the gateway's cut", "")
	}
	return false
}

type waitResult int

const (
	waitReady waitResult = iota
	waitCut
	waitGone
)

// wait blocks until ready holds (called with the lock held), the deadline
// passes (the deadline wins a tie), the request's context ends or the Server
// closes. A zero deadline never passes.
func (s *Server) wait(ctx context.Context, deadline time.Time, ready func(now time.Time) bool) waitResult {
	for {
		s.mu.Lock()
		now := s.now()
		cut := !deadline.IsZero() && !now.Before(deadline)
		ok := !cut && ready(now)
		changed := s.changed
		s.mu.Unlock()
		switch {
		case cut:
			return waitCut
		case ok:
			return waitReady
		}
		t := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return waitGone
		case <-s.closed:
			t.Stop()
			return waitGone
		case <-changed:
		case <-t.C:
		}
		t.Stop()
	}
}

// fail answers with an error body of the assumed shape (package doc). With
// EchoKey on, the message carries the request's Authorization header.
func (x *exchange) fail(status int, typ, message, param string) {
	if x.echo != "" {
		message += " (received Authorization: " + x.echo + ")"
	}
	var p *string
	if param != "" {
		p = &param
	}
	x.writeJSON(status, map[string]any{"error": map[string]any{
		"message": message, "type": typ, "param": p, "code": strconv.Itoa(status),
	}})
}

// writeJSON answers with v as JSON, non-ASCII and HTML characters written as
// they are.
func (x *exchange) writeJSON(status int, v any) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		panic("llmlabtest: encode a reply: " + err.Error())
	}
	x.w.Header().Set("Content-Type", "application/json")
	x.w.WriteHeader(status)
	x.rec.Status = status
	x.rec.FirstByte = x.s.now()
	_, _ = x.w.Write(b.Bytes())
}

type listedModel struct {
	ID       string `json:"id"`
	Object   string `json:"object"`
	Created  int64  `json:"created"`
	OwnedBy  string `json:"owned_by"`
	Revision string `json:"revision,omitempty"`
}

func toListed(m Model) listedModel {
	owner := m.OwnedBy
	if owner == "" {
		owner = "llmlab"
	}
	return listedModel{ID: m.ID, Object: "model", Created: listedCreated, OwnedBy: owner, Revision: m.Revision}
}

// list answers GET /v1/models: every listed model, active or not (L-6.5).
func (x *exchange) list() {
	x.s.mu.Lock()
	models := x.s.listedLocked()
	x.s.mu.Unlock()
	data := make([]listedModel, len(models))
	for i, m := range models {
		data[i] = toListed(m)
	}
	x.writeJSON(http.StatusOK, map[string]any{"object": "list", "data": data})
}

// entry answers GET /v1/models/{id}.
func (x *exchange) entry(id string) {
	x.s.mu.Lock()
	m, ok := x.s.models[id]
	found := ok && m.listed
	var listed Model
	if found {
		listed = m.Model
	}
	x.s.mu.Unlock()
	if !found {
		x.fail(http.StatusNotFound, "invalid_request_error", "the model does not exist", "model")
		return
	}
	x.writeJSON(http.StatusOK, toListed(listed))
}
