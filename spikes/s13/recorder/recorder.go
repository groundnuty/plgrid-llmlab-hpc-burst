// Package recorder is the worker the router forwards to: it stores the exact bytes of every request
// body it receives, before anything reads them, and answers in vLLM's response shapes so the
// router completes the request and does not retry it.
//
// It stands in for the engine double (internal/agent/enginedouble), which decodes the body without
// keeping its bytes and lives in the root module, which a spike module cannot import without
// pulling in the root module's dependency graph.
package recorder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// CaseHeader names the request header that carries a case's key. The router copies request
// headers to the worker, so the key arrives without touching the body.
const CaseHeader = "X-S13-Case"

// Model is the only model the recorder serves.
const Model = "s13-model"

// MaxBody bounds what one request may send; a larger body is a 413.
const MaxBody = 8 << 20

// Arrival is one request as the recorder received it. Body is a copy of the bytes read from the
// connection, taken before any parse.
type Arrival struct {
	Seq           int         `json:"seq"`
	Listener      string      `json:"listener"`
	Method        string      `json:"method"`
	Path          string      `json:"path"`
	Case          string      `json:"case"`
	Header        http.Header `json:"header"`
	ContentLength int64       `json:"content_length"`
	Body          []byte      `json:"-"`
}

// Recorder holds every arrival, generation requests and the router's probes alike.
type Recorder struct {
	mu       sync.Mutex
	arrivals []Arrival
}

// Handler serves one worker listener; name tells the listeners apart in the record.
func (r *Recorder) Handler(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(req.Body, MaxBody+1))
		if err != nil {
			http.Error(w, "reading the body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if len(raw) > MaxBody {
			http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
			return
		}
		stored := bytes.Clone(raw) // the record's copy; nothing below reads it
		r.mu.Lock()
		r.arrivals = append(r.arrivals, Arrival{
			Seq: len(r.arrivals), Listener: name, Method: req.Method, Path: req.URL.Path,
			Case: req.Header.Get(CaseHeader), Header: req.Header.Clone(), ContentLength: req.ContentLength, Body: stored,
		})
		r.mu.Unlock()
		respond(w, req, raw)
	})
}

// ByCase returns the generation requests that carried key, in arrival order.
func (r *Recorder) ByCase(key string) []Arrival {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Arrival
	for _, a := range r.arrivals {
		if a.Case == key && a.Method == http.MethodPost {
			out = append(out, a)
		}
	}
	return out
}

// All returns every arrival.
func (r *Recorder) All() []Arrival {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Arrival(nil), r.arrivals...)
}

func writeJSON(w http.ResponseWriter, v string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, v)
}

// respond answers like vLLM v0.32.0 for the paths a router uses. body is the connection's bytes;
// the stream flag is read from them here only to choose the response's shape.
func respond(w http.ResponseWriter, req *http.Request, body []byte) {
	switch {
	case req.Method == http.MethodGet && (req.URL.Path == "/health" || req.URL.Path == "/ping"):
		w.WriteHeader(http.StatusOK)
	case req.Method == http.MethodGet && req.URL.Path == "/v1/models":
		writeJSON(w, `{"object":"list","data":[{"id":"`+Model+`","object":"model","created":0,"owned_by":"vllm","root":"`+Model+`","parent":null,"max_model_len":8192}]}`)
	case req.Method == http.MethodGet && req.URL.Path == "/metrics":
		// The gauges llm-d's EPP scrapes (llm-d-router v1.0.0, pkg/epp/server/options.go), idle and
		// with room, in vLLM v0.32.0's text format, so that the EPP keeps both workers as candidates.
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = io.WriteString(w, metricsText)
	case req.Method == http.MethodGet && req.URL.Path == "/version":
		writeJSON(w, `{"version":"0.32.0"}`)
	case req.Method == http.MethodPost && (req.URL.Path == "/v1/chat/completions" || req.URL.Path == "/v1/completions"):
		var shape struct {
			Stream        *bool `json:"stream"`
			StreamOptions *struct {
				IncludeUsage *bool `json:"include_usage"`
			} `json:"stream_options"`
		}
		if err := json.Unmarshal(bytes.Clone(body), &shape); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, fmt.Sprintf(`{"error":{"message":%q,"type":"BadRequestError","code":400}}`, err.Error()))
			return
		}
		chat := req.URL.Path == "/v1/chat/completions"
		if shape.Stream != nil && *shape.Stream {
			usage := shape.StreamOptions != nil && shape.StreamOptions.IncludeUsage != nil && *shape.StreamOptions.IncludeUsage
			stream(w, chat, usage)
			return
		}
		if chat {
			writeJSON(w, `{"id":"chatcmpl-s13","object":"chat.completion","created":1,"model":"`+Model+`","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"logprobs":null,"finish_reason":"stop","stop_reason":null}],"usage":{"prompt_tokens":1,"total_tokens":2,"completion_tokens":1}}`)
		} else {
			writeJSON(w, `{"id":"cmpl-s13","object":"text_completion","created":1,"model":"`+Model+`","choices":[{"index":0,"text":"ok","logprobs":null,"finish_reason":"stop","stop_reason":null}],"usage":{"prompt_tokens":1,"total_tokens":2,"completion_tokens":1}}`)
		}
	default:
		http.NotFound(w, req)
	}
}

const metricsText = `# HELP vllm:num_requests_running Number of requests in model execution batches.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{engine="0",model_name="` + Model + `"} 0.0
# HELP vllm:num_requests_waiting Number of requests waiting to be processed.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{engine="0",model_name="` + Model + `"} 0.0
# HELP vllm:kv_cache_usage_perc KV-cache usage. 1 means 100 percent usage.
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{engine="0",model_name="` + Model + `"} 0.0
# HELP vllm:cache_config_info Information of the LLMEngine CacheConfig
# TYPE vllm:cache_config_info gauge
vllm:cache_config_info{block_size="16",engine="0",num_gpu_blocks="4096"} 1.0
`

func stream(w http.ResponseWriter, chat, usage bool) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	var chunks []string
	if chat {
		head := `{"id":"chatcmpl-s13","object":"chat.completion.chunk","created":1,"model":"` + Model + `","choices":`
		chunks = []string{
			head + `[{"index":0,"delta":{"role":"assistant","content":""},"logprobs":null,"finish_reason":null}]}`,
			head + `[{"index":0,"delta":{"content":"ok"},"logprobs":null,"finish_reason":null}]}`,
			head + `[{"index":0,"delta":{},"logprobs":null,"finish_reason":"stop","stop_reason":null}]}`,
		}
		if usage {
			chunks = append(chunks, head+`[],"usage":{"prompt_tokens":1,"total_tokens":2,"completion_tokens":1}}`)
		}
	} else {
		head := `{"id":"cmpl-s13","object":"text_completion","created":1,"model":"` + Model + `","choices":`
		chunks = []string{head + `[{"index":0,"text":"ok","logprobs":null,"finish_reason":"stop","stop_reason":null}]}`}
		if usage {
			chunks = append(chunks, head+`[],"usage":{"prompt_tokens":1,"total_tokens":2,"completion_tokens":1}}`)
		}
	}
	fl, _ := w.(http.Flusher)
	for _, c := range append(chunks, "[DONE]") {
		_, _ = io.WriteString(w, "data: "+c+"\n\n")
		if fl != nil {
			fl.Flush()
		}
	}
}
