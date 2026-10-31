package agentserver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const (
	// maxDrainBytes and drainTimeout bound what the server reads and discards
	// of a request body it refuses as too large, so the agent reads the 400
	// instead of a reset connection. A body larger than this was never sent by
	// an agent, whose EncodeBody refuses it.
	maxDrainBytes = 2 * agentapi.MaxBodyBytes
	drainTimeout  = 10 * time.Second
)

// result is what a handler hands back to serveHTTP.
type result struct {
	// err is what the agent is told; nil when the handler wrote its answer.
	err *agentapi.Error
	// status overrides err.HTTPStatus() when it is not 0: the 404 and 405
	// answers, which have no code of their own.
	status int
	// cause is the real error behind an answer, for the log only.
	cause error
}

// callIdentity is what a log line says of the agent, cut by authenticate.
type callIdentity struct{ tag, jobID, agentInstanceID string }

// serveHTTP routes a request, writes the error its handler returns, and logs
// the call once.
func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
	var id callIdentity
	res := s.route(sw, r, &id)
	if res.err != nil {
		writeError(sw, res)
	}
	s.logCall(r, sw.status, res, id)
}

// route checks the path and the method, authenticates, and calls the handler.
// A path or method the API does not have is refused before authentication: the
// routes are public.
func (s *Server) route(w http.ResponseWriter, r *http.Request, id *callIdentity) result {
	var (
		method string
		handle func(http.ResponseWriter, *http.Request, agentapi.Caller) result
	)
	switch r.URL.Path {
	case agentapi.PathRegister:
		method, handle = http.MethodPost, s.register
	case agentapi.PathDesired:
		method, handle = http.MethodGet, s.desired
	case agentapi.PathReport:
		method, handle = http.MethodPost, s.report
	default:
		return result{err: agentapi.Errorf(agentapi.CodeBadRequest, "no such path"), status: http.StatusNotFound}
	}
	if r.Method != method {
		w.Header().Set("Allow", method)
		return result{err: agentapi.Errorf(agentapi.CodeBadRequest, "use %s", method), status: http.StatusMethodNotAllowed}
	}
	caller, res := s.authenticate(r.Context(), r, id)
	if res.err != nil {
		return res
	}
	return handle(w, r, caller)
}

func (s *Server) register(w http.ResponseWriter, r *http.Request, c agentapi.Caller) result {
	var req agentapi.RegisterRequest
	if res := decodeBody(w, r, &req); res.err != nil {
		return res
	}
	resp, err := s.backend.Register(r.Context(), c, req)
	if err != nil {
		return toResult(r.Context(), err)
	}
	return writeJSON(w, http.StatusOK, resp)
}

func (s *Server) report(w http.ResponseWriter, r *http.Request, c agentapi.Caller) result {
	var rep agentapi.Report
	if res := decodeBody(w, r, &rep); res.err != nil {
		return res
	}
	if err := s.backend.Report(r.Context(), c, rep); err != nil {
		return toResult(r.Context(), err)
	}
	w.WriteHeader(http.StatusNoContent)
	return result{}
}

// desired is the long poll (pkg/agentapi, Long poll). It answers at once when
// since is empty or differs from the Version, and otherwise asks the Backend
// again every PollInterval, until the Version changes, LongPollTimeout passes
// (then it answers with the state it last saw), or the request ends.
func (s *Server) desired(w http.ResponseWriter, r *http.Request, c agentapi.Caller) result {
	ctx := r.Context()
	since := r.URL.Query().Get(agentapi.QuerySince)
	d, err := s.backend.Desired(ctx, c)
	if err != nil {
		return toResult(ctx, err)
	}
	if since == "" || d.Version != since {
		return writeJSON(w, http.StatusOK, d)
	}
	timeout := time.NewTimer(s.longPoll)
	defer timeout.Stop()
	tick := time.NewTicker(s.pollInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return toResult(ctx, ctx.Err())
		case <-timeout.C:
			return writeJSON(w, http.StatusOK, d)
		case <-tick.C:
			if d, err = s.backend.Desired(ctx, c); err != nil {
				return toResult(ctx, err)
			}
			if d.Version != since {
				return writeJSON(w, http.StatusOK, d)
			}
		}
	}
}

// decodeBody reads the request body into v with agentapi.DecodeBody. A body
// above agentapi.MaxBodyBytes is not read to its end by DecodeBody, so the
// handler discards a bounded amount of the rest and closes the connection after
// the answer: a client that is still sending would otherwise see a reset, not
// the 400.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) result {
	body := &countingReader{r: r.Body}
	err := agentapi.DecodeBody(body, v)
	if err == nil {
		return result{}
	}
	if body.n > agentapi.MaxBodyBytes {
		drain(w, r.Body)
		w.Header().Set("Connection", "close")
	}
	return toResult(r.Context(), err)
}

// drain reads and discards up to maxDrainBytes of body, for at most
// drainTimeout. Neither a writer without read deadlines (a test recorder) nor
// an error from the discarded read matters: the connection closes after the
// answer either way.
func drain(w http.ResponseWriter, body io.Reader) {
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(drainTimeout))
	_, _ = io.CopyN(io.Discard, body, maxDrainBytes)
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// toResult turns an error into the answer: an *agentapi.Error goes out as it
// is, with its own status and body; a request whose context has ended is
// CodeUnavailable (the agent retries; the server may be stopping); any other
// error is CodeInternal with a generic message, and the real error goes to the
// log.
func toResult(ctx context.Context, err error) result {
	var e *agentapi.Error
	switch {
	case errors.As(err, &e) && e != nil:
		return result{err: e}
	case ctx.Err() != nil:
		return result{err: agentapi.Errorf(agentapi.CodeUnavailable, "the request ended or the server is stopping")}
	}
	return result{err: agentapi.Errorf(agentapi.CodeInternal, "internal error"), cause: err}
}

// writeJSON answers status with v, encoded by agentapi.EncodeBody. A body above
// the limit is the controller's fault, not the agent's, so it is CodeInternal.
func writeJSON(w http.ResponseWriter, status int, v any) result {
	b, err := agentapi.EncodeBody(v)
	if err != nil {
		return result{err: agentapi.Errorf(agentapi.CodeInternal, "internal error"), cause: err}
	}
	w.Header().Set("Content-Type", agentapi.ContentType)
	w.WriteHeader(status)
	// A failed write means the agent has gone; it retries, so there is nothing
	// to do but let the connection end.
	_, _ = w.Write(b)
	return result{}
}

// writeError answers with the error as the JSON body. The error is small, so
// encoding it cannot reach the limit.
func writeError(w http.ResponseWriter, res result) {
	status := res.status
	if status == 0 {
		status = res.err.HTTPStatus()
	}
	b, _ := agentapi.EncodeBody(res.err) // an Error of two short strings always encodes
	w.Header().Set("Content-Type", agentapi.ContentType)
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// logCall writes the one line about a call: method, path, status, error code,
// and the agent's tag, job ID and agent instance ID, plus the real error behind
// an answer when there is one. It never reads a body or the Authorization
// header. A success is Debug; a refusal or failure is louder.
func (s *Server) logCall(r *http.Request, status int, res result, id callIdentity) {
	level := slog.LevelDebug
	attrs := []any{
		"method", agentapi.TruncateUTF8(r.Method, maxLogged),
		"path", agentapi.TruncateUTF8(r.URL.Path, maxLogged),
		"status", status,
	}
	if res.err != nil {
		attrs = append(attrs, "code", string(res.err.Code))
		level = levelOf(res.err.Code)
	}
	attrs = append(attrs, "tag", id.tag, "job_id", id.jobID, "agent_instance_id", id.agentInstanceID)
	if res.cause != nil {
		attrs = append(attrs, "err", res.cause.Error())
	}
	s.log.Log(r.Context(), level, "agent API call", attrs...)
}

// levelOf is how loud a code is in the log: NotRegistered is normal after a
// controller restart, Internal is a bug.
func levelOf(c agentapi.Code) slog.Level {
	switch c {
	case agentapi.CodeNotRegistered:
		return slog.LevelInfo
	case agentapi.CodeInternal:
		return slog.LevelError
	}
	return slog.LevelWarn
}

// statusWriter records the status an answer is sent with, for the log.
type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status, w.wrote = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the connection's deadlines.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
