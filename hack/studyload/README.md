# studyload

A load generator for an artificial study (HBEP-764, tracking issue #764, test item 3). It runs N concurrent multi-turn chat sessions for each declared workload against one OpenAI-compatible endpoint, each session with a fixed shape (sizes in words, turns, think time). Every request carries a per-session header (default `x-session-id`) and the workload's static headers. For every request it records the time to first token, the decode speed, the token counts the endpoint reported, and the front's attribution headers (HBEP-5), which say which route answered.

It is a test tool. It holds no HBEP-764 mechanism: it routes nothing, budgets nothing and names no workload header of its own (the R2 header is config, empty by default). It sends requests only to the `baseURL` you give it.

```
go run ./hack/studyload -config study.json -out records.jsonl
```

## Survey of existing tools

Four discriminators, read from each repo's own files (the licence from its LICENSE file, never from memory):

- (a) multi-turn sessions with a fixed per-workload shape (first-turn context tokens, later-turn user tokens, output tokens, turns) and a think time between turns, N sessions at once;
- (b) a per-SESSION request header (a distinct value per session, constant within it) plus a static per-workload header;
- (c) per-request capture of RESPONSE headers (the front's attribution headers);
- (d) the row can run in CI's `go` job against the framework's Go engine double, with no new toolchain (CI has Go only).

| Tool | URL, commit | Licence | (a) | (b) | (c) | (d) |
|---|---|---|---|---|---|---|
| vLLM `benchmarks/multi_turn` | github.com/vllm-project/vllm, v0.32.0 (5df63a72, the framework's pin) | Apache-2.0 (`LICENSE:1-2`) | partial: `generate_multi_turn.json` has `num_turns`, `common_prefix_num_tokens`, `prefix_num_tokens`, input and output `num_tokens`; no think time (only `--request-rate`, a Poisson rate per client, `benchmark_serving_multi_turn.py:1445-1450`); `min_tokens` is commented out (`:264-265`), so output length is not exact | yes: `X-Session-ID` = the conversation id (`:272`), static `--header` (`:1371`) | no: no response header is read | no: Python, aiohttp |
| `vllm bench serve` | same checkout, `vllm/benchmarks/serve.py` | Apache-2.0 | no: single-turn requests | static only: `--header` (`serve.py:1622`) | no: `response.headers` is read once, for an embeddings endpoint's `metadata` (`lib/endpoint_request_func.py:826`) | no: Python |
| GuideLLM | github.com/vllm-project/guidellm, a111aed1 (v0.8.1 is the latest tag) | Apache-2.0 (`LICENSE:1`) | yes, with a caveat: `turns=3`, `first_prompt_tokens` (`docs/en/guides/multiturn.md:76,83-86`), `delay*` think time (`schemas/data/deserializers/synthetic.py:301-309`); `concurrent,streams=N` means N requests in flight, not N sessions (`scheduler/worker.py:402-430`) | static only: `extras.headers` (`docs/en/guides/backends.md:188`); `conversation_id` is never sent as a header | no: no capture in `backends/openai/http.py` | no: Python |
| llmperf | github.com/ray-project/llmperf, f1d6bed4 (last commit Dec 2024) | Apache-2.0 (`LICENSE.txt`) | no: single-turn only (`token_benchmark_ray.py:409` `--num-concurrent-requests`, mean and stddev token sizes) | no: only `Authorization` (`ray_clients/openai_chat_completions_client.py:56`) | no | no: Python and Ray |
| genai-perf | github.com/triton-inference-server/perf_analyzer, `genai-perf/`, 5ec2b206 (0.0.17dev; superseded by AIPerf, `genai-perf/README.md:31`) | BSD-3-Clause (`genai-perf/LICENSE:1`) | yes: `--num-sessions`, `--session-concurrency`, `--session-turn-delay-mean` (`genai_perf/parser.py:677-715`); one mean input size for every turn | static only: `-H` (`parser.py:440`); the session id only groups payloads | no: `openai_client.cc:95-111` only checks for `text/event-stream` | no: a Python wrapper over a C++ `perf_analyzer` binary, CUDA 12 or Triton SDK |
| AIPerf | github.com/ai-dynamo/aiperf, c13b282c (0.14.0; v0.13.0 is the latest tag) | Apache-2.0 (`LICENSE`, `pyproject.toml:17`) | yes: `--user-context-prompt-length` (first turn), `--synthetic-input-tokens-mean` (later turns), `--output-tokens-mean`, `--conversation-turn-mean`, `--conversation-turn-delay-mean` (`docs/cli-options.md:823-846,951`); a session slot is held from first turn to last (`timing/concurrency.py:396-398`) | yes: `X-Correlation-ID` per session (`transports/base_transports.py:167-172`), renamed by `--session-header` (`docs/cli-options.md:658`), plus static `-H` (`:694`) | yes, opt in: `--export-http-trace` writes `response_headers` (`transports/aiohttp_trace.py:257`, `post_processors/record_export_jsonl_writer.py:126`); read from code, not run | no: Python 3.11+, `pip install aiperf` |

**Verdict: no tool meets (a)-(d).** All six are Python (genai-perf also drives C++), so none can be driven from a `go test` in a Go-only CI. Only AIPerf has (a), (b) and (c) in code, and only through an opt-in trace export. So `studyload` is a small Go tool that borrows the shape model, and it must record the response headers itself because the whole point is to tally the routes.

## Provenance

New code, standard library only (no new dependency). Borrowed, as a design and not as code:

- the shape model: first-turn context size, later-turn user size, output size, turns and a think time that is a lower bound, from AIPerf (`--user-context-prompt-length`, `--synthetic-input-tokens-mean`, `--output-tokens-mean`, `--conversation-turn-mean`, `--conversation-turn-delay-mean`; Apache-2.0) and vLLM's `generate_multi_turn.json` fields (`prefix_num_tokens`, `num_tokens`, `num_turns`; Apache-2.0);
- the per-session header (`X-Session-ID` in vLLM's multi-turn benchmark, `X-Correlation-ID` in AIPerf), here `x-session-id`, the name the front already reads (`internal/front/convkey`, `DefaultSessionHeaders`);
- the attribution header names: `internal/front/errors.go:85-89` (copied, because importing `internal/front` would pull the whole Kubernetes client into a load tool; `TestAttributionNamesMatchTheFront` reads that file and fails on drift).

## Config

JSON, unknown fields refused. Durations are Go duration strings. Sizes are in WORDS.

```json
{
  "baseURL": "https://endpoint.example/api/v1",
  "model": "the-served-model-name",
  "apiKeyFile": "/path/to/key-file",
  "sessionHeader": "x-session-id",
  "runId": "run1",
  "omitMinTokens": false,
  "requestTimeout": "10m",
  "workloads": [
    {
      "name": "long",
      "sessions": 3, "turns": 3,
      "contextTokens": 400, "turnTokens": 20, "outputTokens": 12,
      "thinkTime": "150ms", "startStagger": "2s",
      "headers": {"x-study-workload": "long"}
    },
    {
      "name": "short",
      "sessions": 5, "turns": 2,
      "contextTokens": 30, "turnTokens": 10, "outputTokens": 5,
      "thinkTime": "80ms",
      "headers": {"x-study-workload": "short"}
    }
  ]
}
```

- `baseURL` is the OpenAI-style base with its `/v1`; a request goes to `<baseURL>/chat/completions`. Required, `http` or `https`, no credentials.
- `apiKeyFile` is a file whose one line is the key. It is read when the run starts and sent as `Authorization: Bearer`. The key is never on a command line, and never printed, logged or recorded: an endpoint can echo it (in an attribution header, on any status), so every record has each occurrence of the key replaced by `(redacted)` in every text field before it is written or summarised. A redirect is never followed (Go would keep `Authorization` on a redirect to another port of the same host): the 3xx is the request's status, with error `status 307` or the like, and the session stops. Empty: no `Authorization` header.
- `sessionHeader` (default `x-session-id`) carries the session ID: `<runId>-<workload>-<NNN>`, distinct per session and constant within it. `runId` defaults to six random hex digits, so two runs never share an ID; set it for a repeatable one.
- A workload runs `sessions` conversations at once, each of `turns` streamed requests. The first user message has `contextTokens` words, each later one `turnTokens`; the assistant's reply text is appended as an assistant message before the next user turn; `thinkTime` passes between the end of a reply and the next request (a lower bound: the request goes out no sooner). Session `j` starts `j * startStagger` after the run starts, for ramping.
- `headers` are static request headers of the workload (name to value). The generator names none: HBEP-764's R2 will name the workload header, and until then it is whatever this config says. `x-study-workload` above is only an example. The session header, `Authorization` and the framing headers cannot be set here.
- Every request has `stream: true`, `stream_options.include_usage: true`, `max_tokens` = `min_tokens` = `outputTokens`. `omitMinTokens` leaves `min_tokens` out for an endpoint that refuses it.
- `requestTimeout` (default 10m) bounds each request, from sending to the last event.

The text is built from a fixed vocabulary of 256 common lowercase English words, one word per intended token, seeded by the session and the turn, so two sessions do not share a prompt prefix.

## Output

`-out FILE` (required, created with mode 600, and refused if it already exists) gets one JSON line per request:

```json
{"workload":"long","session":"run1-long-000","turn":2,"start":"2026-11-18T12:00:00.5Z","end":"2026-11-18T12:00:00.9Z","status":200,"ttftMs":38.2,"promptTokens":445,"completionTokens":12,"decodeTokensPerSec":310.4,"requestId":"req-1","emitter":"front","provider":"sano-a","deploymentId":"dep-1","instance":"ns/inst-1"}
```

- `ttftMs`: the time from sending the request to the first content (or reasoning) delta. Absent when none arrived.
- `promptTokens`, `completionTokens`: the endpoint's `usage`, which is the truth (see the limits). Absent when it sent none.
- `decodeTokensPerSec`: `(completionTokens - 1)` over the time from the first to the last content delta. Absent for one token or no usage.
- `requestId`, `emitter`, `provider`, `deploymentId`, `instance`: the `Hpcburst-Request-Id`, `-Emitter`, `-Provider`, `-Deployment-Id` and `-Instance` response headers; empty when the response had none. A route is (provider, deployment ID, instance). They are recorded for any status.
- `error`: a short class (`status 503`, `transport error`, `malformed event`, `error event`, `stream ended without [DONE]`), never body text. A session whose request fails stops (its conversation has no reply to continue from); the others go on.

stdout gets a summary, one line per workload (in config order) and one per route (sorted):

```
requests: 19
workload long: sessions 3 (0 with errors), requests 9, errors 0, ttft p50 38.2ms p95 51.0ms, speed per session p50 310.4 tok/s min 290.0 tok/s
route provider=sano-a deployment=dep-1 instance=ns/inst-1: sessions 3 (0 with errors), requests 9, errors 0, ...
```

Percentiles are nearest-rank. A session's speed is its tokens after the first token over the time spent after those first tokens, across its turns. The summary prints numbers, config names and attribution header values, and nothing else from a response.

Exit codes: 0 every request succeeded, 1 some request failed (the records say which), 2 a usage, config or file error, or an interrupted run.

## Limits

- **A word is not a token on a real tokenizer.** The generator sends one word per intended token, so `contextTokens: 400` is 400 words. A real tokenizer's count can differ (the framework's engine double counts exactly the words); `promptTokens`, the endpoint's own `usage`, is the truth, and a study should read it.
- **`min_tokens` may be ignored.** vLLM honours `min_tokens`, so `max_tokens = min_tokens` gives an exact output length. A managed endpoint in front of vLLM may drop or refuse the field (`omitMinTokens` for the second case); a model may then stop early, and `completionTokens` says what it did. The conversation's later prompts grow by what was actually generated.
- **The workload header is config until R2 names it.** The generator knows no workload header; the front cannot route by workload before HBEP-764's R2 exists.
- A decode speed over chunks that carry several tokens each is an estimate: the first chunk's extra tokens are counted as after the first.
- The 60 s wait, the barrier and every other timing device below belong to the test, not to the tool.

## Tests

```
go vet ./hack/studyload/ && go test ./hack/studyload/
```

`study_test.go` is the row: two workloads (`long`: 3 sessions, 3 turns, 400/20/12 words; `short`: 5 sessions, 2 turns, 30/10/5) run through `run()` against two engine doubles (`internal/agent/enginedouble`, imported from the test only) behind a test router. A wrapper in front of each double stands for the front: it sets `Hpcburst-*` headers (the doubles set none) and records what reached the engine (session and workload headers, `max_tokens`, `min_tokens`, each message's role and word count, the double's usage). Expected values are literals typed by hand, with the template arithmetic in comments (2 template words per message plus 7 generation-prompt words, so turn 1 of `long` is 409 prompt tokens). The assertions read the engine side and compare the generator's records with it. Concurrency has no timing window: the wrapper holds every first-turn request until all eight sessions' first requests have arrived (a 60 s deadline fails the row), which a generator running sessions one after another never satisfies. Think time is a lower bound only, measured from the engine's last write of a reply to the next request's arrival.

Each record must name exactly one engine request by its `Hpcburst-Request-Id`, with the session and turn that request really was, and every engine request must be named by exactly one record.

The other tests cover the config, the request, the timing arithmetic (on a scripted clock), each failure class, the CLI and its exit codes, an endpoint that echoes the key in every attribution header on a 200 and on an error (the key must reach no output), a redirect (the target must receive nothing), the stagger (each session's scheduled start offset, checked through a seam with no clock, and a real-time lower bound), and a failed turn stopping only its own session.
