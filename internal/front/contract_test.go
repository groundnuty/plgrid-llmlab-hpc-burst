package front

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/responses"
	"github.com/openai/openai-go/v3/shared"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// HBEP-5, Test plan, Contract: an unmodified OpenAI client, configured as for
// LLMLab (base URL of the endpoint plus /v1, the API key being the client
// key), runs chat, completions, Responses, embeddings, streaming with usage,
// tool calls, /v1/models, and unknown and inactive models.
//
// The client is the OpenAI Go SDK, github.com/openai/openai-go/v3 v3.71.1
// (Apache-2.0), test-only. Two options only configure it: no retries, so a
// 503 with Retry-After: 30 is seen once and the engine sees each request once,
// and a client that keeps no connection, since these tests' servers come and
// go on the same few ports. Neither changes what the client sends.
//
// Expected values come from HBEP-5 and from what the doubles were told to
// send: Behaviour.Tokens completion tokens, and the whitespace-separated words
// of the request text as prompt tokens (enginetest's documented count).

// ctModel is the model name clients send.
const ctModel = "example-27b"

// ctHTTP is the HTTP client under every OpenAI client of these tests.
func ctHTTP() *http.Client {
	return &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: time.Minute}
}

// ctWire records the request bodies the OpenAI client wrote, as bytes.
type ctWire struct {
	mu     sync.Mutex
	bodies [][]byte
}

func (w *ctWire) middleware(req *http.Request, next option.MiddlewareNext) (*http.Response, error) {
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		_ = req.Body.Close()
		w.mu.Lock()
		w.bodies = append(w.bodies, b)
		w.mu.Unlock()
		req.Body = io.NopCloser(bytes.NewReader(b))
		req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
	}
	return next(req)
}

// last is the body of the client's latest request.
func (w *ctWire) last(t *testing.T) []byte {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.bodies) == 0 {
		t.Fatal("the client wrote no request body")
	}
	return w.bodies[len(w.bodies)-1]
}

// ctClient returns an unmodified OpenAI client set up as for LLMLab.
func ctClient(base, key string, w *ctWire) openai.Client {
	opts := []option.RequestOption{option.WithBaseURL(base + "/v1"), option.WithAPIKey(key),
		option.WithHTTPClient(ctHTTP()), option.WithMaxRetries(0)}
	if w != nil {
		opts = append(opts, option.WithMiddleware(w.middleware))
	}
	return openai.NewClient(opts...)
}

// ctFix is a front with one ready Instance of ctModel on an engine double
// that answers 4 tokens, and a ModelDeployment of "idle-model" whose only
// Instance is pending.
type ctFix struct {
	*harness
	e      *enginetest.Engine
	client openai.Client
	wire   *ctWire
}

const ctTokens = 4

func newCtFix(t *testing.T) *ctFix {
	t.Helper()
	h := newHarness(t, nil)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
	e.Set(enginetest.Behaviour{Tokens: ctTokens})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", ctModel, 1, "helios-a")
	h.instance("md", "md-0", "helios-a", e, key, instanceOpts{})
	h.servedMD("md-idle", "idle-model", 1, "helios-a")
	h.instance("md-idle", "md-idle-0", "helios-a", nil, "", instanceOpts{phase: v1alpha1.InstancePending})
	h.apply()
	w := &ctWire{}
	return &ctFix{harness: h, e: e, client: ctClient(h.url, h.clientKey, w), wire: w}
}

func ctCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// engineRequest is the engine's only request so far, whose body must be the
// client's bytes: the served name equals the requested one here, so nothing
// changes in the body (HBEP-5, Contract).
func (f *ctFix) engineRequest(t *testing.T, path string) enginetest.Request {
	t.Helper()
	reqs := f.e.Requests()
	if len(reqs) != 1 {
		t.Fatalf("the engine saw %d requests, want 1", len(reqs))
	}
	r := reqs[0]
	if r.Path != path {
		t.Errorf("the engine got %s, want %s", r.Path, path)
	}
	if want := f.wire.last(t); !bytes.Equal(r.Body, want) {
		t.Errorf("the engine's body differs from the client's:\n got %s\nwant %s", r.Body, want)
	}
	return r
}

// ctWords is the engine double's prompt count for one text.
func ctWords(s string) int64 { return int64(len(strings.Fields(s))) }

// ctReply is what the engine double says for n tokens: "t0 t1 ...", each
// followed by a space (enginetest's documented reply).
func ctReply(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteString("t" + string(rune('0'+i)) + " ")
	}
	return b.String()
}

func TestContractChat(t *testing.T) {
	f := newCtFix(t)
	const prompt = "tell me about the hpc burst framework"
	res, err := f.client.Chat.Completions.New(ctCtx(t), openai.ChatCompletionNewParams{
		Model:    shared.ChatModel(ctModel),
		Messages: []openai.ChatCompletionMessageParamUnion{openai.SystemMessage("you are brief"), openai.UserMessage(prompt)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := res.Choices[0].Message.Content, ctReply(ctTokens); got != want {
		t.Errorf("content %q, want %q", got, want)
	}
	if res.Choices[0].FinishReason != "stop" {
		t.Errorf("finish reason %q, want stop", res.Choices[0].FinishReason)
	}
	if want := ctWords("you are brief") + ctWords(prompt); res.Usage.PromptTokens != want || res.Usage.CompletionTokens != ctTokens {
		t.Errorf("usage prompt %d completion %d, want %d and %d", res.Usage.PromptTokens, res.Usage.CompletionTokens, want, ctTokens)
	}
	f.engineRequest(t, "/v1/chat/completions")
}

func TestContractChatStreamWithUsage(t *testing.T) {
	f := newCtFix(t)
	const prompt = "stream me a short reply please"
	stream := f.client.Chat.Completions.NewStreaming(ctCtx(t), openai.ChatCompletionNewParams{
		Model:         shared.ChatModel(ctModel),
		Messages:      []openai.ChatCompletionMessageParamUnion{openai.UserMessage(prompt)},
		StreamOptions: openai.ChatCompletionStreamOptionsParam{IncludeUsage: openai.Bool(true)},
	})
	var text strings.Builder
	var finish string
	var usage *openai.CompletionUsage
	for stream.Next() {
		c := stream.Current()
		if len(c.Choices) > 0 {
			text.WriteString(c.Choices[0].Delta.Content)
			if c.Choices[0].FinishReason != "" {
				finish = c.Choices[0].FinishReason
			}
		}
		if c.JSON.Usage.Valid() {
			u := c.Usage
			usage = &u
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("the stream failed: %v", err)
	}
	if text.String() != ctReply(ctTokens) {
		t.Errorf("streamed text %q, want %q", text.String(), ctReply(ctTokens))
	}
	if finish != "stop" {
		t.Errorf("finish reason %q, want stop", finish)
	}
	if usage == nil {
		t.Fatal("no usage in the stream although the client asked for it")
	}
	if usage.PromptTokens != ctWords(prompt) || usage.CompletionTokens != ctTokens {
		t.Errorf("stream usage prompt %d completion %d, want %d and %d", usage.PromptTokens, usage.CompletionTokens, ctWords(prompt), ctTokens)
	}
	f.engineRequest(t, "/v1/chat/completions")
}

// TestContractToolCallReachesTheEngineFieldForField sends a function tool with
// the fields clients use, and checks every one of them at the engine against
// the literals sent here, besides the whole body's bytes.
func TestContractToolCallReachesTheEngineFieldForField(t *testing.T) {
	f := newCtFix(t)
	params := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"city": map[string]any{"type": "string", "description": "the city"},
			"unit": map[string]any{"type": "string", "enum": []any{"c", "f"}},
		},
		"required":             []any{"city"},
		"additionalProperties": false,
	}
	_, err := f.client.Chat.Completions.New(ctCtx(t), openai.ChatCompletionNewParams{
		Model:    shared.ChatModel(ctModel),
		Messages: []openai.ChatCompletionMessageParamUnion{openai.UserMessage("what is the weather in Krakow")},
		Tools: []openai.ChatCompletionToolUnionParam{openai.ChatCompletionFunctionTool(shared.FunctionDefinitionParam{
			Name: "get_weather", Description: openai.String("Weather of a city"), Strict: openai.Bool(true), Parameters: params,
		})},
		ToolChoice:        openai.ChatCompletionToolChoiceOptionUnionParam{OfAuto: openai.String("auto")},
		ParallelToolCalls: openai.Bool(false),
	})
	if err != nil {
		t.Fatal(err)
	}
	r := f.engineRequest(t, "/v1/chat/completions")
	var got struct {
		Tools []struct {
			Type     string `json:"type"`
			Function struct {
				Name        string         `json:"name"`
				Description string         `json:"description"`
				Strict      bool           `json:"strict"`
				Parameters  map[string]any `json:"parameters"`
			} `json:"function"`
		} `json:"tools"`
		ToolChoice        string `json:"tool_choice"`
		ParallelToolCalls *bool  `json:"parallel_tool_calls"`
	}
	if err := json.Unmarshal(r.Body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Tools) != 1 {
		t.Fatalf("the engine got %d tools, want 1", len(got.Tools))
	}
	tool := got.Tools[0]
	if tool.Type != "function" || tool.Function.Name != "get_weather" || tool.Function.Description != "Weather of a city" || !tool.Function.Strict {
		t.Errorf("tool at the engine: %+v", tool)
	}
	if !reflect.DeepEqual(tool.Function.Parameters, params) {
		t.Errorf("tool parameters at the engine:\n got %v\nwant %v", tool.Function.Parameters, params)
	}
	if got.ToolChoice != "auto" || got.ParallelToolCalls == nil || *got.ParallelToolCalls {
		t.Errorf("tool_choice %q, parallel_tool_calls %v; want auto and false", got.ToolChoice, got.ParallelToolCalls)
	}
}

func TestContractCompletions(t *testing.T) {
	f := newCtFix(t)
	const prompt = "once upon a time in a cluster"
	res, err := f.client.Completions.New(ctCtx(t), openai.CompletionNewParams{
		Model:  openai.CompletionNewParamsModel(ctModel),
		Prompt: openai.CompletionNewParamsPromptUnion{OfString: openai.String(prompt)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Choices[0].Text != ctReply(ctTokens) || res.Choices[0].FinishReason != "stop" {
		t.Errorf("completion %q finish %q", res.Choices[0].Text, res.Choices[0].FinishReason)
	}
	if res.Usage.PromptTokens != ctWords(prompt) || res.Usage.CompletionTokens != ctTokens {
		t.Errorf("usage prompt %d completion %d, want %d and %d", res.Usage.PromptTokens, res.Usage.CompletionTokens, ctWords(prompt), ctTokens)
	}
	f.engineRequest(t, "/v1/completions")
}

func TestContractCompletionsStreamWithUsage(t *testing.T) {
	f := newCtFix(t)
	const prompt = "once upon a time in a cluster"
	stream := f.client.Completions.NewStreaming(ctCtx(t), openai.CompletionNewParams{
		Model:         openai.CompletionNewParamsModel(ctModel),
		Prompt:        openai.CompletionNewParamsPromptUnion{OfString: openai.String(prompt)},
		StreamOptions: openai.ChatCompletionStreamOptionsParam{IncludeUsage: openai.Bool(true)},
	})
	var text strings.Builder
	var finish string
	var usage *openai.CompletionUsage
	for stream.Next() {
		c := stream.Current()
		if len(c.Choices) > 0 {
			text.WriteString(c.Choices[0].Text)
			if c.Choices[0].FinishReason != "" {
				finish = string(c.Choices[0].FinishReason)
			}
		}
		if c.JSON.Usage.Valid() {
			u := c.Usage
			usage = &u
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("the stream failed: %v", err)
	}
	if text.String() != ctReply(ctTokens) || finish != "stop" {
		t.Errorf("streamed %q finish %q", text.String(), finish)
	}
	if usage == nil || usage.PromptTokens != ctWords(prompt) || usage.CompletionTokens != ctTokens {
		t.Errorf("stream usage %+v, want prompt %d completion %d", usage, ctWords(prompt), ctTokens)
	}
	f.engineRequest(t, "/v1/completions")
}

func TestContractResponses(t *testing.T) {
	f := newCtFix(t)
	const prompt = "summarise the design in one line"
	res, err := f.client.Responses.New(ctCtx(t), responses.ResponseNewParams{
		Model: shared.ResponsesModel(ctModel),
		Input: responses.ResponseNewParamsInputUnion{OfString: openai.String(prompt)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.OutputText() != ctReply(ctTokens) || res.Status != "completed" {
		t.Errorf("output %q status %q", res.OutputText(), res.Status)
	}
	if res.Usage.InputTokens != ctWords(prompt) || res.Usage.OutputTokens != ctTokens {
		t.Errorf("usage input %d output %d, want %d and %d", res.Usage.InputTokens, res.Usage.OutputTokens, ctWords(prompt), ctTokens)
	}
	f.engineRequest(t, "/v1/responses")
}

func TestContractResponsesStream(t *testing.T) {
	f := newCtFix(t)
	const prompt = "summarise the design in one line"
	stream := f.client.Responses.NewStreaming(ctCtx(t), responses.ResponseNewParams{
		Model: shared.ResponsesModel(ctModel),
		Input: responses.ResponseNewParamsInputUnion{OfString: openai.String(prompt)},
	})
	var text strings.Builder
	var types []string
	var completed *responses.Response
	for stream.Next() {
		ev := stream.Current()
		types = append(types, ev.Type)
		switch ev.Type {
		case "response.output_text.delta":
			text.WriteString(ev.Delta)
		case "response.completed":
			r := ev.Response
			completed = &r
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("the stream failed: %v", err)
	}
	if text.String() != ctReply(ctTokens) {
		t.Errorf("streamed text %q, want %q", text.String(), ctReply(ctTokens))
	}
	if completed == nil || types[len(types)-1] != "response.completed" {
		t.Fatalf("the stream did not end with response.completed: %v", types)
	}
	if completed.Usage.InputTokens != ctWords(prompt) || completed.Usage.OutputTokens != ctTokens {
		t.Errorf("response.completed usage %+v, want input %d output %d", completed.Usage, ctWords(prompt), ctTokens)
	}
	f.engineRequest(t, "/v1/responses")
}

func TestContractEmbeddings(t *testing.T) {
	f := newCtFix(t)
	const text = "embed these five words please"
	res, err := f.client.Embeddings.New(ctCtx(t), openai.EmbeddingNewParams{
		Model: ctModel,
		Input: openai.EmbeddingNewParamsInputUnion{OfString: openai.String(text)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Data) != 1 || !reflect.DeepEqual(res.Data[0].Embedding, []float64{0.25, -0.5}) {
		t.Errorf("embedding data %+v, want one vector [0.25 -0.5]", res.Data)
	}
	if res.Usage.PromptTokens != ctWords(text) {
		t.Errorf("prompt tokens %d, want %d", res.Usage.PromptTokens, ctWords(text))
	}
	f.engineRequest(t, "/v1/embeddings")
}

// TestContractModelsListsRoutable: GET /v1/models is the front's, OpenAI's
// list with the additive routable member (HBEP-5, Contract). A model with a
// ready Instance is routable; one whose Instance is pending is listed and not.
func TestContractModelsListsRoutable(t *testing.T) {
	f := newCtFix(t)
	page, err := f.client.Models.List(ctCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.e.Requests()) != 0 {
		t.Errorf("the engine saw a request for the listing: the front answers /v1/models itself")
	}
	routable := map[string]string{}
	for _, m := range page.Data {
		if m.Object != "model" {
			t.Errorf("model %s has object %q, want model", m.ID, m.Object)
		}
		routable[m.ID] = m.JSON.ExtraFields["routable"].Raw()
	}
	want := map[string]string{ctModel: "true", "idle-model": "false"}
	if !reflect.DeepEqual(routable, want) {
		t.Errorf("routable by model: %v, want %v", routable, want)
	}
	one, err := f.client.Models.Get(ctCtx(t), ctModel)
	if err != nil {
		t.Fatal(err)
	}
	if one.ID != ctModel || one.JSON.ExtraFields["routable"].Raw() != "true" {
		t.Errorf("GET /v1/models/%s: id %q routable %q", ctModel, one.ID, one.JSON.ExtraFields["routable"].Raw())
	}
}

// ctAPIError unwraps the OpenAI client's error and returns its status and its
// error object's members.
func ctAPIError(t *testing.T, err error) (int, map[string]any) {
	t.Helper()
	var apierr *openai.Error
	if !errors.As(err, &apierr) {
		t.Fatalf("want an API error, got %v", err)
	}
	var obj map[string]any
	if e := json.Unmarshal([]byte(apierr.RawJSON()), &obj); e != nil {
		t.Fatalf("the error object %q is not JSON: %v", apierr.RawJSON(), e)
	}
	return apierr.StatusCode, obj
}

// TestContractUnknownAndInactiveModels: an unknown model and an inactive one
// both fail with 503 model_inactive and look alike, as on LLMLab; detail tells
// them apart (HBEP-5, Errors).
func TestContractUnknownAndInactiveModels(t *testing.T) {
	f := newCtFix(t)
	for _, tc := range []struct{ model, detail string }{
		{"no-such-model", "not_attached"},
		{"idle-model", "no_ready_target"},
	} {
		_, err := f.client.Chat.Completions.New(ctCtx(t), openai.ChatCompletionNewParams{
			Model:    shared.ChatModel(tc.model),
			Messages: []openai.ChatCompletionMessageParamUnion{openai.UserMessage("hello")},
		})
		if err == nil {
			t.Fatalf("%s: the request succeeded", tc.model)
		}
		status, obj := ctAPIError(t, err)
		if status != http.StatusServiceUnavailable || obj["code"] != "model_inactive" || obj["detail"] != tc.detail || obj["type"] != "hpcburst_error" {
			t.Errorf("%s: status %d, error %v; want 503, model_inactive, detail %s", tc.model, status, obj, tc.detail)
		}
	}
	if n := len(f.e.Requests()); n != 0 {
		t.Errorf("the engine saw %d requests for models with no ready target", n)
	}
}
