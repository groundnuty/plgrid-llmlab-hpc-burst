package enginedouble

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// handler is the double's HTTP surface.
type handler struct {
	e       *engine
	cfg     Config
	started time.Time // when the handler was made: the clock of health-fail-after
}

// Handler returns the vLLM v0.32.0 HTTP subset the agent reads. It panics on a
// Config that fails Validate. It is for tests: a test serves it on a Unix
// socket (a TCP listener needs a port in the test's block).
func Handler(cfg Config) http.Handler { return newHandler(cfg, nil) }

func newHandler(cfg Config, record func(LogEvent)) http.Handler {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		panic("enginedouble: " + err.Error())
	}
	return &handler{e: newEngine(cfg, record), cfg: cfg, started: time.Now()}
}

// guardedPrefixes are the paths that need the API key
// (vllm/entrypoints/serve/middleware/authenticate.py, GUARDED_PREFIX).
var guardedPrefixes = []string{"/v1", "/v2", "/inference", "/cohere"}

// instrumentatorExcluded are the handlers the metrics instrumentator does not
// count (vllm/entrypoints/serve/instrumentator/metrics.py, excluded_handlers).
var instrumentatorExcluded = map[string]bool{
	"/metrics": true, "/health": true, "/load": true, "/ping": true, "/version": true, "/server_info": true,
}

// routes maps each path to its method.
var routes = map[string]string{
	"/health":              http.MethodGet,
	"/version":             http.MethodGet,
	"/v1/models":           http.MethodGet,
	"/tokenize":            http.MethodPost,
	"/v1/chat/completions": http.MethodPost,
	"/v1/completions":      http.MethodPost,
	"/metrics":             http.MethodGet,
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	route := r.URL.Path
	method, known := routes[route]
	if !known {
		route = "none" // the instrumentator's label for an untemplated path
	}
	defer func() {
		if !instrumentatorExcluded[route] {
			h.e.countHTTP(httpKey{route, r.Method, fmt.Sprintf("%dxx", rec.status/100)})
		}
	}()

	if h.unauthorized(r) {
		writeJSON(rec, http.StatusUnauthorized, map[string]string{"error": "Unauthorized"})
		return
	}
	switch {
	case !known:
		writeJSON(rec, http.StatusNotFound, map[string]string{"detail": "Not Found"})
	case r.Method != method:
		writeJSON(rec, http.StatusMethodNotAllowed, map[string]string{"detail": "Method Not Allowed"})
	default:
		switch r.URL.Path {
		case "/health":
			h.health(rec)
		case "/version":
			h.version(rec)
		case "/v1/models":
			h.models(rec)
		case "/tokenize":
			h.tokenize(rec, r)
		case "/v1/chat/completions":
			h.chat(rec, r)
		case "/v1/completions":
			h.completion(rec, r)
		case "/metrics":
			h.metrics(rec)
		}
	}
}

// unauthorized is vLLM's AuthenticationMiddleware: with an API key set, a path
// under a guarded prefix needs "Authorization: Bearer <key>"; OPTIONS and
// every other path (/health, /version, /metrics, /tokenize) are open.
func (h *handler) unauthorized(r *http.Request) bool {
	if h.cfg.APIKey == "" || r.Method == http.MethodOptions {
		return false
	}
	guarded := false
	for _, p := range guardedPrefixes {
		if strings.HasPrefix(r.URL.Path, p) {
			guarded = true
		}
	}
	if !guarded {
		return false
	}
	scheme, param, _ := strings.Cut(r.Header.Get("Authorization"), " ")
	if !strings.EqualFold(scheme, "bearer") {
		return true
	}
	got, want := sha256.Sum256([]byte(param)), sha256.Sum256([]byte(h.cfg.APIKey))
	return subtle.ConstantTimeCompare(got[:], want[:]) != 1
}

// health is vllm/entrypoints/serve/instrumentator/health.py: 200 with an empty
// body, or 503 when the engine is dead. With health-fail-after=<dur> the engine
// is healthy until that long after the handler was made, then dead for good.
func (h *handler) health(w http.ResponseWriter) {
	if h.cfg.HealthFail || (h.cfg.HealthFailAfterSet && time.Since(h.started) >= h.cfg.HealthFailAfter) {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// version is vllm/entrypoints/serve/instrumentator/basic.py. With
// reports-nothing the route is absent, as in a server that does not have it.
func (h *handler) version(w http.ResponseWriter) {
	if h.cfg.ReportsNothing {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "Not Found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"version": Version})
}

// models is OpenAIServingModels.show_available_models
// (vllm/entrypoints/openai/models/serving.py): one ModelCard whose root is the
// model path, with max_model_len. With reports-nothing the field is missing.
func (h *handler) models(w http.ResponseWriter) {
	card := map[string]any{
		"id":       h.cfg.ServedName,
		"object":   "model",
		"created":  0,
		"owned_by": "vllm",
		"root":     h.cfg.ModelPath,
	}
	if !h.cfg.ReportsNothing {
		card["max_model_len"] = h.cfg.reportedMaxModelLen()
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": []any{card}})
}

// tokenize is vllm/entrypoints/serve/tokenize/api_router.py: a chat request
// ("messages", with add_generation_prompt true unless said otherwise) or a
// completion request ("prompt"), answered with TokenizeResponse
// (serve/tokenize/protocol.py): count, max_model_len and tokens.
func (h *handler) tokenize(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Prompt              *string   `json:"prompt"`
		Messages            []message `json:"messages"`
		AddGenerationPrompt *bool     `json:"add_generation_prompt"`
	}
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestError", err.Error())
		return
	}
	var text string
	switch {
	case req.Prompt != nil:
		text = *req.Prompt
	case len(req.Messages) > 0:
		text = renderChat(req.Messages, req.AddGenerationPrompt == nil || *req.AddGenerationPrompt, h.cfg.TemplateOverhead)
	default:
		writeError(w, http.StatusBadRequest, "BadRequestError", "a tokenize request needs messages or a prompt")
		return
	}
	_, ids := tokenize(text)
	writeJSON(w, http.StatusOK, map[string]any{
		"count":         len(ids),
		"max_model_len": h.cfg.reportedMaxModelLen(),
		"tokens":        nonNil(ids),
	})
}

func nonNil(ids []int32) []int32 {
	if ids == nil {
		return []int32{}
	}
	return ids
}

func decodeBody(r *http.Request, v any) error {
	b, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		return fmt.Errorf("reading the body: %w", err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("JSON decode error: %w", err)
	}
	return nil
}

// writeJSON writes v with the status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// writeError writes vLLM's error shape: {"error":{"message","type","param","code"}}.
func writeError(w http.ResponseWriter, status int, typ, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{
		"message": msg, "type": typ, "param": nil, "code": status,
	}})
}

// statusRecorder keeps the status for the instrumentator's count and passes
// Flush through, which a streamed answer needs.
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wrote {
		s.status, s.wrote = code, true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	s.wrote = true
	return s.ResponseWriter.Write(b)
}

func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
