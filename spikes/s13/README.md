# Spike S13: which request fields the Instance router delivers

Issue #312; HBEP-5, open question 2. Five routers: vllm-router 0.1.16 and 0.1.17; Envoy 1.40.2 and 1.33.2
as plain proxies with `ring_hash`; llm-d's EPP behind Envoy. No GPU, no cluster: every run is on one
host, on ports 29900-29906.

## Result in brief

- **Envoy delivers each of S13's requests as sent.** Envoy ran as a plain HTTP proxy with `ring_hash` on
  `x-session-id`, the key vllm-router's `consistent_hash` uses. On both 1.40.2 (the latest release,
  27 Oct 2026) and 1.33.2 (S2's pin), all 440 request variants reached a worker as their exact bytes,
  streamed and not. That includes the 10 that vllm-router refuses, and the escaped keys and duplicate
  members it rewrites. Both releases put every variant on the same worker, so placement is a function
  of the request, and both workers took requests (215 and 225). No fork, no build.
- **llm-d's EPP behind Envoy forwards what it accepts as its bytes** (432 of 432). Its own parser
  refuses, with 400, two content-part forms vLLM accepts: vLLM's simple URL parts and a bare string
  (4 cases, each streamed and not).
- **vllm-router delivers no request as sent.** 0.1.16 and 0.1.17, the latest release (PyPI and GitHub,
  4 Nov 2026), parse `/v1/chat/completions` and `/v1/completions` into typed structs and send the
  structs again. Of 440 variants, none reached a worker as its bytes under `consistent_hash`
  (HBEP-5's provisional default) or `round_robin`; the two policies gave the same differences on every
  variant. The one exception is 0.1.17 under `--policy kv_aware`, which forwards `/v1/completions` as
  the client's bytes (142 of 142) and still re-serialises chat.
- **What 0.1.17 still changes or refuses, of requests vLLM v0.32.0's validators accept.** Each item is a
  change of the bytes; where the change also alters what vLLM does, the item says so, from the pinned
  source:
  - `tools[].function.strict`, `tools[].function.defer_loading` and `tools[].defer_loading` are dropped.
    vLLM passes each tool's `model_dump()` to the chat template (`renderers/online_renderer.py:287`, `:305`),
    whose serialiser keeps both fields when set (`generate/base/protocol.py:298-313`). With a template
    that renders a tool's JSON, the prompt then differs from a direct call's.
  - A tool without `parameters` is refused with 422; vLLM makes the field optional.
  - `response_format` of type `structural_tag`, in both of vLLM's forms, is refused with 422 on chat.
  - `response_format.json_schema.description` and `stream_options.continuous_usage_stats` are dropped.
  - `"max_tokens": null` on `/v1/completions` is dropped. vLLM turns an explicit null into the
    default, 16 (`completion/protocol.py:430-436`), so the limit is the same either way: a byte change
    only.
  - In an assistant message, `refusal` and `audio` are dropped, and a `tool_calls` list holding a custom
    tool call (`"type":"custom"`) is dropped whole. The `tool` message that answers it then refers to a
    call the history no longer has.
  - `messages[].task` is dropped, and so is an unknown member of any object the router types: a
    message of a known role, a tool and its function, a tool call, `tool_choice`, `response_format`
    and its `json_schema`, `stream_options` and `structured_outputs`. Members inside what the router
    holds as raw JSON arrive: unknown top-level members and their contents, `dict` fields such as
    `chat_template_kwargs`, JSON schemas, 0.1.17's content parts, and messages of unknown roles.
  - Keys and duplicates: a key written with an escape arrives unescaped, a member written twice inside a
    `dict` or at the top level arrives once (the last, the one Python's `json` module also keeps), and
    a known field written twice is refused with 422 (`duplicate field`).
  - `reasoning_content` in an assistant message arrives as `reasoning`. vLLM does the same rename
    itself (`chat_completion/protocol.py:555-580`), so the engine sees the same message.
  - Explicit nulls are dropped. For the chat fields tested (`max_tokens`, `temperature`, `seed`, `user`,
    `tools`, `stop`) vLLM reads null as absent (`stop` through `sampling_params.py:574-575`), so nothing
    changes there. A message's `"name": null` arrives as no name.
  - Numbers in `float` fields are held as 32-bit floats: `1` arrives as `1.0`, and
    `0.12345678901234567890` as `0.12345679`. Integers above 2^64 inside a `dict[str, Any]` arrive as
    floats.
  - It adds every member of its own defaults the client did not set: ten to a minimal chat request and
    six to a minimal completions request (table below). Each is either vLLM's declared default for that
    field or a field vLLM v0.32.0 does not declare. The defaults are chat `logprobs`, `stream`,
    `include_reasoning`, `ignore_eos`, `skip_special_tokens` and `continue_final_message`
    (`chat_completion/protocol.py:227`, `:240`, `:266`, `:281`, `:283`, `:327`), and completions
    `echo`, `stream`, `ignore_eos` and `skip_special_tokens` (`completion/protocol.py:59`, `:68`,
    `:84`, `:86`). The undeclared ones are `no_stop_trim`, `separate_reasoning`, `stream_reasoning`
    and `return_hidden_states`, which vLLM logs at debug level and ignores
    (`serve/engine/protocol.py:61-75`). No vLLM code tests whether a field was set
    (`model_fields_set` appears nowhere in `vllm/entrypoints` or `vllm/renderers`), so no outcome
    changes.
- **0.1.16 also empties user content.** A user message whose content parts include anything but `text` and
  `image_url` (`input_audio`, `file`, `audio_url`, `video_url`, the embeds, `tool_reference`, vLLM's
  simple URL parts, a bare string) arrives as `"content":""`, with no error. An assistant message with
  `refusal` or `thinking` parts arrives with no content. 0.1.17 fixes both: it keeps content parts as raw JSON
  (`src/protocols/spec.rs:260-268`; upstream issue 322, closed 25 Oct 2026).
- **No vllm-router release fixes the rest.** Upstream `main` at 394a63e (10 Nov 2026, 08:43Z) still types
  chat requests (`server.rs:260-264`) and still declares `Function` with only `name`, `description` and
  `parameters` (`spec.rs:1939-1944`).
- **Recommendation: Envoy with `ring_hash` on `x-session-id` as the candidate router default.** It is
  reuse as released, it delivers every variant as its bytes, and it hashes on the key HBEP-5 already
  sets.
  S2c (#62) still has to measure its hit rate, balance and latency on GPUs before it becomes the
  default. A pinned vllm-router patch that forwards the client's bytes, fix (b) below, becomes the
  fallback. See "Choosing a router".

## Method

**The path.** The harness (`cmd/s13fields`) starts two recorders on 127.0.0.1:29900 and :29901, then
the router under test on :29902.
- **vllm-router** gets `--worker-urls` naming both recorders, `--policy <p>`,
  `--request-timeout-secs 10800` (HBEP-5) and its metrics on :29903.
- **A proxy stack** (`-exec scripts/stack.sh`) is one of two:
  - Envoy alone with `envoy/ring-hash.yaml`;
  - llm-d's EPP (gRPC :29904, health :29905, metrics :29906) behind Envoy 1.33.2 on :29902 (admin
    :29903). This is S2's `llmd-tuned` stack, generated by S2's own `genconf.py` from llm-d v1.0.0's
    shipped guide files, with the EPP started with S2's flags.

The harness sends each case through the router and compares what reached a recorder. Requests go one
at a time, each with `x-session-id` (HBEP-5) and an `X-S13-Case` header naming the variant. Every router
copies headers to the worker, so the recorder can tell the variants apart without touching the body.
After the run the harness sends TERM to the router's process group and waits for the group to exit.
`run.sh` then checks that no listener or process is left.

**The recorder** (`recorder/`) stands in for the engine double. `internal/agent/enginedouble` decodes the
body without keeping its bytes, and lives in the root module, which a spike module cannot import without
pulling in the root module's dependency graph. The recorder stores `bytes.Clone` of exactly what it read
from the connection before anything parses it. It then answers in vLLM's response shapes (a stream when
`stream` is true, with a usage chunk when asked), so the router completes the request and does not
retry. It also answers `/health`, `/v1/models` and `/metrics`, the last with the idle gauges the EPP
scrapes (`vllm:num_requests_running`, `vllm:num_requests_waiting`, `vllm:kv_cache_usage_perc`,
`vllm:cache_config_info`; llm-d-router `pkg/epp/server/options.go:172-173` and the core metrics
extractor).
- **What the routers asked the workers for, besides the cases:**
  - vllm-router under `consistent_hash` and `round_robin`: `GET /health`;
  - Envoy: nothing;
  - the EPP: `GET /metrics`, 74 times;
  - 0.1.17 under `kv_aware`: `GET /get_server_info`, `GET /kv_event_sources`, `POST /tokenize` and the
    two `/render` endpoints. The recorder answered those with 404 and the router routed without them,
    so that arm shows the forwarding path only, not KV-aware routing.

**The cases** (`cases/`) are literal request bodies, never marshalled. Each sets one field, or one small
group vLLM needs together, on a minimal request, so a router that refuses one field cannot hide what it
does to another. Each case is sent as written and, unless it sets `stream` itself, again with
`"stream":true,` inserted after its opening brace: a byte edit, not a re-encoding. That gives 225
cases and 440 variants, 298 on chat and 142 on completions.
- **The fields** are every field of vLLM v0.32.0's request models, enumerated from its source by
  `scripts/enumerate_fields.py` (Python's `ast`, nothing imported). That covers `ChatCompletionRequest`
  and `CompletionRequest`, the nested models they name (tools, tool choice, stream options, the three
  response formats, structured outputs, repetition detection), vLLM's message and content-part types,
  and the other two members of vLLM's message union (`renderers/chat_utils.py:393-397`). One is the
  `openai` package's TypedDicts at 2.25.0. The other is oss-harmony's `Message` and `Author` at 0.0.11.
  Each is the floor vLLM sets (`requirements/common.txt`). The Harmony `Message` holds its content as
  a list of the `Content` base class, which declares no field of its own, so a content element adds
  no row. The list is `fields/vllm-v0.32.0.tsv`, 258 rows, each with its file and line.
  `cases_test.go` fails if a field has no valid case on each endpoint that accepts it, or if a case's
  cover points outside its body. Two fields have no case: the PIL-image content part, which JSON
  cannot carry.
- **Beyond the fields:** unknown nested members (`tools[].function.x_ext`, an object inside a message,
  `cache_control` inside a content part, and others), `extra_body`-style top-level members, OpenAI fields
  vLLM does not declare, the router's own `session_params`, vLLM's earlier `reasoning_content`,
  explicit nulls, number spellings, string escapes, keys written with escapes, duplicate members,
  minimal requests, and one request shaped like an agent's tool-call turn.
- **Validity.** Every value is non-default and passes the request models' validators. That was checked
  by reading each `model_validator` of `ChatCompletionRequest` (`chat_completion/protocol.py:540-1080`)
  and `CompletionRequest` (`completion/protocol.py:416-640`), and the field constraints. The
  `logprob_token_ids` cases carry the `logprobs` they need (`:845-849`; completions `:534-538`).
  `prompt_logprobs` is sent plain only, since vLLM refuses it streamed (`:863-867`; `:553-557`).
  One case is invalid on purpose: a message with `"name": null`, since every message type declares
  `name` as a string. It is marked `invalid for vLLM` in the tables, covers no enumerated field, and
  stays to show what each router does with it. No vLLM ran.
- **Valid requests a server may still refuse.** Some cases pass validation but depend on how vLLM is
  started or on the model:
  - a request `chat_template` (`--trust-request-chat-template`, `renderers/online_renderer.py:685-695`);
  - `mm_processor_kwargs` and `media_io_kwargs` (trusted per-request multimodal kwargs,
    `generate/base/protocol.py:72-95`);
  - `prompt_embeds` (`--enable-prompt-embeds`, `renderers/base.py:973-975`, and a serialised tensor);
  - a non-zero `priority` (priority scheduling);
  - `suffix` (a model with fill-in-the-middle rendering, `completion/serving.py:119`);
  - the media content parts (a model that takes them, and URLs it can fetch);
  - the structural tags, whose payload check needs xgrammar (`generate/base/protocol.py:235-237`) and
    was not run.

  None of that changes what a router delivers.

**The comparison** (`rawjson/`). The first check is byte equality of the whole body. Then both bodies
are parsed into trees that keep every value's literal bytes, and walked path by path.
- **Canonicalisation, and why:** two things are ignored, object member order and insignificant
  whitespace. Nothing else is: a key spelled otherwise, and a member written a different number of
  times, are verdicts of their own. JSON objects are unordered (RFC 8259, sections 1 and 4), and
  vllm-router serialises its maps in no fixed order: the same `logit_bias` arrived with its members in
  different orders in different runs. So a byte comparison alone would flag every body and say nothing
  about any field.
- **Verdicts:** a value whose literal bytes match is `kept`. The same value spelled otherwise
  (`\u00e9` for `é`, `9e-1` for `0.9`) is `kept, re-encoded`. A change of JSON type, or of a number's form
  between integer and fraction (`1` and `1.0`, which Python reads as int and float), is `retyped`.
  Another value of the same type is `changed`. A member whose key arrived spelled otherwise (an
  escape written raw) is `key re-escaped`. A key that arrived fewer, or more, times than it was written
  is `duplicate removed` or `duplicate added`; the value compared at it is its last occurrence. A value
  missing at the worker is `dropped`; one present only at the worker is `injected`; a value dropped
  under one key and injected under a sibling is `renamed`. A variant that reached no worker is
  `refused`, with the router's status.

**Controls.**
- **Positive control, the recorder** (`run.sh`, first arm): every variant sent straight to the recorder
  arrived as its exact bytes, 440 of 440, with no canonicalisation. The cases include pretty-printed
  bodies, duplicate members, escaped keys, escapes (`\u00e9`, a surrogate pair, `\/`, `\u0001`), `9e-1`, `1.0` and a 20-digit float. A
  recorder that re-marshalled could not produce that result.
- **Positive control, the comparator:** `TestLossyProxyIsSeen` re-marshals a body through
  `encoding/json`'s generic model and deletes `tools[0].function.strict`. The comparator must report
  exactly the deletion, the int64 seed changed by float64, and the escaped `<a&b>`. The expected
  verdicts in `rawjson`'s tests are written by hand, not produced by the code under test.
- **On the real routers:** `tools[].function.strict` comes out `dropped` on 0.1.16, as the source
  predicts (`spec.rs:1839`).
- **The README's claims** are tests. `results/claims_test.go` recomputes every verdict from the
  committed bytes and fails if a claim here does not hold. For example, flipping the `strict` claim to
  `kept` fails both releases.

**Verifications that would prove nothing, and why they are not used.** One would be to compare the
router's output with a body rebuilt from the same parsed model. Here the expected value is always the
case's literal bytes, and the received value is the recorder's copy of the connection's bytes.
Another would be a recorder that re-marshals before storing. The recorder clones the bytes before any
parse, `TestStoresTheExactBytes` checks that, and the 440-of-440 control could not pass otherwise.

**Environment.** One 6-core VM shared with other work; every run at `nice -n 19` under a 4 GB memory
cap. Every router's listeners were checked from `ss` after start, all on 127.0.0.1: vllm-router
:29902-29903, Envoy :29902, the llm-d stack :29902-29906. Runs on
10 Nov 2026 from 12:34Z.

## Results

| run | variants | as their exact bytes | lost or changed a value | reached no worker |
|---|---|---|---|---|
| direct to the recorder (control) | 440 | 440 | 0 | 0 |
| vllm-router 0.1.16, consistent_hash | 440 | 0 | 430 | 10 |
| vllm-router 0.1.16, round_robin | 440 | 0 | 430 | 10 |
| vllm-router 0.1.17, consistent_hash | 440 | 0 | 430 | 10 |
| vllm-router 0.1.17, round_robin | 440 | 0 | 430 | 10 |
| vllm-router 0.1.17, kv_aware | 440 | 142 (all of completions) | 288 (all of chat that arrived) | 10 |
| Envoy 1.40.2, ring_hash on x-session-id | 440 | 440 | 0 | 0 |
| Envoy 1.33.2, ring_hash on x-session-id | 440 | 440 | 0 | 0 |
| llm-d EPP v1.0.0 behind Envoy 1.33.2 | 440 | 432 | 0 | 8 |

"Lost or changed" counts the injected members too, which every variant vllm-router delivered has. The 10 that reached
no vllm-router worker are the 422s: two structural-tag formats, a tool without `parameters`, the
Harmony message and a known field written twice, each streamed and not. The 8 the EPP refused are vLLM's simple image, audio and video
parts and a bare-string part, each streamed and not, with 400 "must have valid messages field". Envoy's
and the EPP's per-field tables (`results/envoy-*/fields.md`, `results/llm-d-epp-envoy-1.33.2/fields.md`)
have no verdict but `kept`, apart from those refusals.

| run | covers | kept | kept, re-encoded | key re-escaped | duplicate removed | dropped | retyped | changed | renamed | refused |
|---|---|---|---|---|---|---|---|---|---|---|
| Envoy 1.40.2, ring_hash on x-session-id, enumerated fields | 308 | 308 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Envoy 1.40.2, ring_hash on x-session-id, extensions and encodings | 48 | 48 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Envoy 1.33.2, ring_hash on x-session-id, enumerated fields | 308 | 308 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Envoy 1.33.2, ring_hash on x-session-id, extensions and encodings | 48 | 48 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| llm-d EPP v1.0.0 behind Envoy 1.33.2 (S2's tuned stack), enumerated fields | 308 | 303 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 5 |
| llm-d EPP v1.0.0 behind Envoy 1.33.2 (S2's tuned stack), extensions and encodings | 48 | 47 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 |

Regenerate it with `go run ./cmd/s13table results/envoy-1.40.2-ring_hash results/envoy-1.33.2-ring_hash results/llm-d-epp-envoy-1.33.2`.

The tables below are vllm-router's.

**Covers by verdict.** A cover is one field a case sets; its verdict is the worse of its plain and streamed variants. Of 308 covers of enumerated fields, 0.1.16 delivers 233 with their bytes and 0.1.17 delivers 271.

| run | covers | kept | kept, re-encoded | key re-escaped | duplicate removed | dropped | retyped | changed | renamed | refused |
|---|---|---|---|---|---|---|---|---|---|---|
| vllm-router 0.1.16, consistent_hash, enumerated fields | 308 | 233 | 0 | 0 | 0 | 24 | 35 | 0 | 0 | 16 |
| vllm-router 0.1.16, consistent_hash, extensions and encodings | 48 | 13 | 5 | 2 | 2 | 18 | 4 | 2 | 1 | 1 |
| vllm-router 0.1.17, consistent_hash, enumerated fields | 308 | 271 | 0 | 0 | 0 | 19 | 2 | 0 | 0 | 16 |
| vllm-router 0.1.17, consistent_hash, extensions and encodings | 48 | 16 | 5 | 2 | 2 | 16 | 3 | 2 | 1 | 1 |

**Every cover that either release did not deliver as sent**, with the bytes at the worker (— for none). Under HBEP-5's `consistent_hash`:

| case | field | sent | vllm-router 0.1.16, consistent_hash: received | verdict | vllm-router 0.1.17, consistent_hash: received | verdict |
|---|---|---|---|---|---|---|
| chat/logit_bias | logit_bias | `{"50256":-100,"198":2.5}` | `{"50256":-100.0,"198":2.5}` | retyped | `{"198":2.5,"50256":-100.0}` | retyped |
| chat/stream_options | stream_options | `{"include_usage":true,"continuous_usage_stats":true}` | `{"include_usage":true}` | dropped (stream only) | `{"include_usage":true}` | dropped (stream only) |
| chat/stream_options | stream_options.continuous_usage_stats | `true` | — | dropped (stream only) | — | dropped (stream only) |
| chat/response_format-json_schema | response_format.json_schema | `{"name":"s13_schema","description":"A schema","schema":{"typ…` | `{"name":"s13_schema","schema":{"type":"object","properties":…` | dropped | `{"name":"s13_schema","schema":{"type":"object","properties":…` | dropped |
| chat/response_format-json_schema | response_format.json_schema.description | `"A schema"` | — | dropped | — | dropped |
| chat/response_format-structural_tag | response_format(structural_tag).type | `"structural_tag"` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/response_format-structural_tag | response_format(structural_tag).format | `{"type":"triggered_tags","triggers":["<f>"],"tags":[{"begin"…` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/response_format-structural_tag-legacy | response_format(structural_tag, legacy).type | `"structural_tag"` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/response_format-structural_tag-legacy | response_format(structural_tag, legacy).structures | `[{"begin":"<f>","schema":{"type":"object"},"end":"</f>"}]` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/response_format-structural_tag-legacy | response_format(structural_tag, legacy).triggers | `["<f>"]` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/response_format-structural_tag-legacy | response_format(structural_tag, legacy).structures[].begin | `"<f>"` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/response_format-structural_tag-legacy | response_format(structural_tag, legacy).structures[].structural_tag_schema | `{"type":"object"}` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/response_format-structural_tag-legacy | response_format(structural_tag, legacy).structures[].end | `"</f>"` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/tools-function-strict | tools[].function.strict | `true` | — | dropped | — | dropped |
| chat/tools-function-defer_loading | tools[].function.defer_loading | `true` | — | dropped | — | dropped |
| chat/tools-defer_loading | tools[].defer_loading | `true` | — | dropped | — | dropped |
| chat/tools-no-parameters | tools[].function.name | `"get_time"` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/msg/user-part-input_audio | messages[].content[](input_audio).type | `"input_audio"` | — | retyped | `"input_audio"` | kept |
| chat/msg/user-part-input_audio | messages[].content[](input_audio).input_audio | `{"data":"UklGRg==","format":"wav"}` | — | retyped | `{"data":"UklGRg==","format":"wav"}` | kept |
| chat/msg/user-part-input_audio | messages[].content[](input_audio).input_audio.data | `"UklGRg=="` | — | retyped | `"UklGRg=="` | kept |
| chat/msg/user-part-input_audio | messages[].content[](input_audio).input_audio.format | `"wav"` | — | retyped | `"wav"` | kept |
| chat/msg/user-part-file | messages[].content[](file).type | `"file"` | — | retyped | `"file"` | kept |
| chat/msg/user-part-file | messages[].content[](file).file | `{"file_data":"JVBERi0=","file_id":"file-s13","filename":"a.p…` | — | retyped | `{"file_data":"JVBERi0=","file_id":"file-s13","filename":"a.p…` | kept |
| chat/msg/user-part-file | messages[].content[](file).file.file_data | `"JVBERi0="` | — | retyped | `"JVBERi0="` | kept |
| chat/msg/user-part-file | messages[].content[](file).file.file_id | `"file-s13"` | — | retyped | `"file-s13"` | kept |
| chat/msg/user-part-file | messages[].content[](file).file.filename | `"a.pdf"` | — | retyped | `"a.pdf"` | kept |
| chat/msg/user-part-audio_url | messages[].content[](audio_url).type | `"audio_url"` | — | retyped | `"audio_url"` | kept |
| chat/msg/user-part-audio_url | messages[].content[](audio_url).audio_url | `{"url":"https://example.com/a.wav"}` | — | retyped | `{"url":"https://example.com/a.wav"}` | kept |
| chat/msg/user-part-audio_url | messages[].content[](audio_url).audio_url.url | `"https://example.com/a.wav"` | — | retyped | `"https://example.com/a.wav"` | kept |
| chat/msg/user-part-video_url | messages[].content[](video_url).type | `"video_url"` | — | retyped | `"video_url"` | kept |
| chat/msg/user-part-video_url | messages[].content[](video_url).video_url | `{"url":"https://example.com/a.mp4"}` | — | retyped | `{"url":"https://example.com/a.mp4"}` | kept |
| chat/msg/user-part-video_url | messages[].content[](video_url).video_url.url | `"https://example.com/a.mp4"` | — | retyped | `"https://example.com/a.mp4"` | kept |
| chat/msg/user-part-image_embeds | messages[].content[](image_embeds).type | `"image_embeds"` | — | retyped | `"image_embeds"` | kept |
| chat/msg/user-part-image_embeds | messages[].content[](image_embeds).image_embeds | `"AAAA"` | — | retyped | `"AAAA"` | kept |
| chat/msg/user-part-image_embeds | messages[].content[](image_embeds).uuid | `"img-1"` | — | retyped | `"img-1"` | kept |
| chat/msg/user-part-audio_embeds | messages[].content[](audio_embeds).type | `"audio_embeds"` | — | retyped | `"audio_embeds"` | kept |
| chat/msg/user-part-audio_embeds | messages[].content[](audio_embeds).audio_embeds | `"AAAA"` | — | retyped | `"AAAA"` | kept |
| chat/msg/user-part-audio_embeds | messages[].content[](audio_embeds).uuid | `"aud-1"` | — | retyped | `"aud-1"` | kept |
| chat/msg/user-part-video_embeds | messages[].content[](video_embeds).type | `"video_embeds"` | — | retyped | `"video_embeds"` | kept |
| chat/msg/user-part-video_embeds | messages[].content[](video_embeds).video_embeds | `"AAAA"` | — | retyped | `"AAAA"` | kept |
| chat/msg/user-part-video_embeds | messages[].content[](video_embeds).uuid | `"vid-1"` | — | retyped | `"vid-1"` | kept |
| chat/msg/user-part-prompt_embeds | messages[].content[](prompt_embeds).type | `"prompt_embeds"` | — | retyped | `"prompt_embeds"` | kept |
| chat/msg/user-part-prompt_embeds | messages[].content[](prompt_embeds).data | `"AAAA"` | — | retyped | `"AAAA"` | kept |
| chat/msg/user-part-simple-image | messages[].content[](simple image).image_url | `"https://example.com/b.png"` | — | retyped | `"https://example.com/b.png"` | kept |
| chat/msg/user-part-simple-image | messages[].content[](simple image).uuid | `"img-2"` | — | retyped | `"img-2"` | kept |
| chat/msg/user-part-simple-audio | messages[].content[](simple audio).audio_url | `"https://example.com/b.wav"` | — | retyped | `"https://example.com/b.wav"` | kept |
| chat/msg/user-part-simple-video | messages[].content[](simple video).video_url | `"https://example.com/b.mp4"` | — | retyped | `"https://example.com/b.mp4"` | kept |
| chat/msg/user-part-simple-video | messages[].content[](simple video).uuid | `"vid-2"` | — | retyped | `"vid-2"` | kept |
| chat/msg/user-part-tool_reference | messages[].content[](tool_reference).type | `"tool_reference"` | — | retyped | `"tool_reference"` | kept |
| chat/msg/user-part-tool_reference | messages[].content[](tool_reference).name | `"get_weather"` | — | retyped | `"get_weather"` | kept |
| chat/msg/user-part-bare-string | ext:messages[].content[](str) | `"hello"` | — | retyped | `"hello"` | kept |
| chat/msg/assistant-part-refusal | messages[].content[](refusal).type | `"refusal"` | — | dropped | `"refusal"` | kept |
| chat/msg/assistant-part-refusal | messages[].content[](refusal).refusal | `"I cannot."` | — | dropped | `"I cannot."` | kept |
| chat/msg/assistant-part-thinking | messages[].content[](thinking).type | `"thinking"` | — | dropped | `"thinking"` | kept |
| chat/msg/assistant-part-thinking | messages[].content[](thinking).thinking | `"Let me see."` | — | dropped | `"Let me see."` | kept |
| chat/msg/assistant-part-thinking | messages[].content[](thinking).closed | `true` | — | dropped | `true` | kept |
| chat/msg/assistant-refusal | messages[](assistant).refusal | `"No."` | — | dropped | — | dropped |
| chat/msg/assistant-audio | messages[](assistant).audio | `{"id":"audio-s13"}` | — | dropped | — | dropped |
| chat/msg/assistant-audio | messages[](assistant).audio.id | `"audio-s13"` | — | dropped | — | dropped |
| chat/msg/assistant-tool_calls-custom | messages[](assistant).tool_calls[](custom).id | `"call_2"` | — | dropped | — | dropped |
| chat/msg/assistant-tool_calls-custom | messages[](assistant).tool_calls[](custom).type | `"custom"` | — | dropped | — | dropped |
| chat/msg/assistant-tool_calls-custom | messages[](assistant).tool_calls[](custom).custom | `{"name":"run","input":"ls"}` | — | dropped | — | dropped |
| chat/msg/assistant-tool_calls-custom | messages[](assistant).tool_calls[](custom).custom.name | `"run"` | — | dropped | — | dropped |
| chat/msg/assistant-tool_calls-custom | messages[](assistant).tool_calls[](custom).custom.input | `"ls"` | — | dropped | — | dropped |
| chat/msg/user-task | messages[](custom).task | `"s13-task"` | — | dropped | — | dropped |
| completions/logit_bias | logit_bias | `{"50256":-100,"198":2.5}` | `{"198":2.5,"50256":-100.0}` | retyped | `{"50256":-100.0,"198":2.5}` | retyped |
| completions/stream_options | stream_options | `{"include_usage":true,"continuous_usage_stats":true}` | `{"include_usage":true}` | dropped (stream only) | `{"include_usage":true}` | dropped (stream only) |
| completions/stream_options | stream_options.continuous_usage_stats | `true` | — | dropped (stream only) | — | dropped (stream only) |
| completions/max_tokens-null | max_tokens | `null` | — | dropped | — | dropped |
| chat/ext/messages-x_ext | ext:messages[].x_ext | `{"trace":"t-1","n":1}` | — | dropped | — | dropped |
| chat/ext/content-part-cache_control | ext:messages[].content[].cache_control | `{"type":"ephemeral"}` | — | dropped | `{"type":"ephemeral"}` | kept |
| chat/ext/system-part-cache_control | ext:messages[](system).content[].cache_control | `{"type":"ephemeral"}` | — | dropped | `{"type":"ephemeral"}` | kept |
| chat/ext/assistant-reasoning_content | ext:messages[](assistant).reasoning_content | `"The user greets."` | `messages[1].reasoning: "The user greets."` | renamed | `messages[1].reasoning: "The user greets."` | renamed |
| chat/msg/harmony | messages[](harmony).author | `{"role":"user","name":"alice"}` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/msg/harmony | messages[](harmony).author.role | `"user"` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/msg/harmony | messages[](harmony).author.name | `"alice"` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/msg/harmony | messages[](harmony).content | `[{"type":"text","text":"hello"}]` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/msg/harmony | messages[](harmony).channel | `"final"` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/msg/harmony | messages[](harmony).recipient | `"all"` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/msg/harmony | messages[](harmony).content_type | `"text"` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/ext/tools-function-x_ext | ext:tools[].function.x_ext | `{"a":1}` | — | dropped | — | dropped |
| chat/ext/tools-x_ext | ext:tools[].x_ext | `{"a":1}` | — | dropped | — | dropped |
| chat/ext/tool_calls-function-x_ext | ext:messages[].tool_calls[].function.x_ext | `1` | — | dropped | — | dropped |
| chat/ext/tool_choice-x_ext | ext:tool_choice.x_ext | `1` | — | dropped | — | dropped |
| chat/ext/response_format-json_schema-x_ext | ext:response_format.json_schema.x_ext | `1` | — | dropped | — | dropped |
| chat/ext/stream_options-x_ext | ext:stream_options.x_ext | `1` | — | dropped (stream only) | — | dropped (stream only) |
| chat/ext/structured_outputs-x_ext | ext:structured_outputs.x_ext | `1` | — | dropped | — | dropped |
| chat/ext/top-level-object | ext:x_top | `{"a":[1,2.50,"s"],"b":null}` | `{"a":[1,2.5,"s"],"b":null}` | kept, re-encoded | `{"a":[1,2.5,"s"],"b":null}` | kept, re-encoded |
| completions/ext/top-level-object | ext:x_top | `{"a":[1,2.50,"s"],"b":null}` | `{"a":[1,2.5,"s"],"b":null}` | kept, re-encoded | `{"a":[1,2.5,"s"],"b":null}` | kept, re-encoded |
| chat/enc/temperature-integer | ext:temperature as an integer literal | `1` | `1.0` | retyped | `1.0` | retyped |
| completions/enc/temperature-integer | ext:temperature as an integer literal | `1` | `1.0` | retyped | `1.0` | retyped |
| chat/enc/top_p-exponent | ext:top_p in exponent form | `9e-1` | `0.9` | kept, re-encoded | `0.9` | kept, re-encoded |
| chat/enc/temperature-20-digits | ext:a float beyond float64 precision | `0.12345678901234567890` | `0.12345679` | changed | `0.12345679` | changed |
| chat/enc/kwargs-numbers | ext:numbers inside a dict[str, Any] | `{"big":123456789012345678901234567890,"u64":1844674407370955…` | `{"f":1.0,"big":1.2345678901234568e+29,"i":1,"u64":1844674407…` | changed | `{"f":1.0,"neg":-0.0,"big":1.2345678901234568e+29,"u64":18446…` | changed |
| chat/enc/explicit-nulls | ext:max_tokens: null | `null` | — | dropped | — | dropped |
| chat/enc/explicit-nulls | ext:temperature: null | `null` | — | dropped | — | dropped |
| chat/enc/explicit-nulls | ext:stop: null | `null` | — | dropped | — | dropped |
| chat/enc/explicit-nulls | ext:user: null | `null` | — | dropped | — | dropped |
| chat/enc/explicit-nulls | ext:seed: null | `null` | — | dropped | — | dropped |
| chat/enc/explicit-nulls | ext:tools: null | `null` | — | dropped | — | dropped |
| chat/enc/message-name-null | ext:messages[].name: null | `null` | — | dropped | — | dropped |
| chat/enc/duplicate-known-field | ext:a field written twice | `0.3` | `HTTP 422` | refused | `HTTP 422` | refused |
| chat/enc/duplicate-in-dict | ext:a member written twice inside a dict[str, Any] | `{"a":1,"a":2}` | `{"a":2}` | duplicate removed | `{"a":2}` | duplicate removed |
| chat/enc/duplicate-unknown-top | ext:an unknown top-level member written twice | `2` | `2` | duplicate removed | `2` | duplicate removed |
| chat/enc/key-escaped-top | ext:a top-level key written with an escape | `0.3` | `0.3` | key re-escaped | `0.3` | key re-escaped |
| chat/enc/key-escaped-nested | ext:a key inside a dict[str, Any] written with an escape | `1` | `1` | key re-escaped | `1` | key re-escaped |
| chat/enc/string-escapes | ext:string escapes (an escaped e-acute, a surrogate pair, an escaped slash, a control character) | `"caf\u00e9 \ud83d\ude00 \/ <&> \u0001 \t"` | `"café 😀 / <&> \u0001 \t"` | kept, re-encoded | `"café 😀 / <&> \u0001 \t"` | kept, re-encoded |
| completions/enc/string-escapes | ext:string escapes (an escaped e-acute, a surrogate pair, an escaped slash, a control character) | `"caf\u00e9 \ud83d\ude00 \/ <&> \u0001 \t"` | `"café 😀 / <&> \u0001 \t"` | kept, re-encoded | `"café 😀 / <&> \u0001 \t"` | kept, re-encoded |
| chat/real/agent-turn | ext:agent turn: tools[].function.strict | `true` | — | dropped (stream only) | — | dropped (stream only) |
| chat/real/agent-turn | ext:agent turn: temperature 0 | `0` | `0.0` | retyped (stream only) | `0.0` | retyped (stream only) |

**Members injected into requests**, by how many of the delivered variants carry them. A variant lacks one only when it set that member itself:

| endpoint | injected path | value | vllm-router 0.1.16, consistent_hash: variants | vllm-router 0.1.17, consistent_hash: variants |
|---|---|---|---|---|
| /v1/chat/completions | `continue_final_message` | `false` | 286 of 288 | 286 of 288 |
| /v1/chat/completions | `ignore_eos` | `false` | 286 of 288 | 286 of 288 |
| /v1/chat/completions | `include_reasoning` | `true` | 286 of 288 | 286 of 288 |
| /v1/chat/completions | `logprobs` | `false` | 284 of 288 | 284 of 288 |
| /v1/chat/completions | `no_stop_trim` | `false` | 288 of 288 | 288 of 288 |
| /v1/chat/completions | `return_hidden_states` | `false` | 288 of 288 | 288 of 288 |
| /v1/chat/completions | `separate_reasoning` | `true` | 288 of 288 | 288 of 288 |
| /v1/chat/completions | `skip_special_tokens` | `true` | 286 of 288 | 286 of 288 |
| /v1/chat/completions | `stream` | `false` | 142 of 288 | 142 of 288 |
| /v1/chat/completions | `stream_reasoning` | `true` | 288 of 288 | 288 of 288 |
| /v1/completions | `echo` | `false` | 140 of 142 | 140 of 142 |
| /v1/completions | `ignore_eos` | `false` | 140 of 142 | 140 of 142 |
| /v1/completions | `no_stop_trim` | `false` | 142 of 142 | 142 of 142 |
| /v1/completions | `return_hidden_states` | `false` | 142 of 142 | 142 of 142 |
| /v1/completions | `skip_special_tokens` | `true` | 140 of 142 | 140 of 142 |
| /v1/completions | `stream` | `false` | 70 of 142 | 70 of 142 |

The full tables, one row per covered field per run, with the bytes sent and received, are
`results/<run>/fields.md`. The raw evidence, the bytes sent and received for each variant, is
`results/<run>/results.jsonl`. Regenerate the tables above with
`go run ./cmd/s13table results/vllm-router-0.1.16-consistent_hash results/vllm-router-0.1.17-consistent_hash`.


**Streaming** changes nothing: every case sent both ways has the same verdict for each of its fields,
streamed and not.

## Why

**Envoy** runs no filter but its router (`envoy.filters.http.router`), so nothing reads the body. It
goes to the worker as received.

**llm-d's EPP** gets the body from Envoy through `ext_proc` and parses it for scheduling
(`pkg/epp/framework/plugins/requesthandling/parsers/openai/openai.go:354-398` at v1.0.0, b9393d1).
Envoy then forwards the client's bytes. A body the parser rejects ends the request with 400. Its
content-part type holds `image_url`, `audio_url`, `input_audio` and `video_url` as objects
(`pkg/epp/framework/interface/requesthandling/types.go:745-756`), so a part whose `image_url` is a
string, or a part that is a bare string, fails the parse (`:779-791`, "content format not supported").

**vllm-router.** Lines are in `src/protocols/spec.rs` at v0.1.17 (3451e66); 0.1.16 (db02f35) has the
same declarations at other lines (`Function` is at `:1839`).
- `ChatCompletionRequest` (`:347`) and `CompletionRequest` (`:779`) keep unknown top-level members in
  `#[serde(flatten)] other` (`:529-530`, `:911-912`). Their typed fields with defaults are always
  serialised, which is where the injected members come from (`no_stop_trim` `:470`, `separate_reasoning`
  `:499`, `stream_reasoning` `:503`, `return_hidden_states` `:511`).
- Sampling fields are `f32` (`temperature` `:357`, `logit_bias: HashMap<String, f32>` `:397`).
  `max_tokens` is `Option<u32>` (`:381`, `:794`), serialised only when set, so null is lost.
- `Tool` (`:1932`) has `type` and `function`. `Function` (`:1939-1944`) has `name`, `description` and a
  required `parameters`.
- `ResponseFormat` (`:290`) knows `text`, `json_object` and `json_schema` only. `JsonSchemaFormat`
  (`:300`) has `name`, `schema` and `strict`. `StreamOptions` (`:1891`) has `include_usage`.
- `ChatMessage` (`:65-104`) types the known roles: system and user carry `role`, `content`, `name`;
  assistant carries `content`, `name`, `tool_calls`, `function_call`, `reasoning`; tool carries
  `content`, `tool_call_id`. Only an unknown role is kept whole (`Other(Value)`). The deserialiser
  (`:106`) reads `reasoning_content` as an alias of `reasoning` (`:152`). It also turns a `tool_calls`
  list that fails to parse into nothing (`.and_then(|tc| serde_json::from_value(tc).ok())`, `:139-141`);
  a custom tool call has no `function`, so `ToolCall` (`:1947`) fails on it.
- 0.1.16's user content parts were an enum of `text` and `image_url` (`:293-298`); any other part failed
  the parse, and the deserialiser substituted empty text.
- Forwarding: chat is `Json<ChatCompletionRequest>` (`server.rs:266-276`) and is sent with
  `.json(typed_req)` (`src/routers/http/router.rs:1958`). Completions arrive as `JsonWithRaw`
  (`server.rs:297-310`), but the raw bytes are kept only when `kv_aware_ops(policy)` is some
  (`router.rs:2796-2797`); then `completion_route.rs:22-41` sends them as the body.

## Choosing a router

| | Envoy, `ring_hash` on `x-session-id` | (a) a newer vllm-router | (b) vllm-router 0.1.17 with a pinned patch | (c) a fixed worker for affected requests |
|---|---|---|---|---|
| Delivers S13's requests as their bytes | yes: 440 of 440, on 1.40.2 and 1.33.2 | no release does; upstream `main` at 394a63e neither | only for requests its retained typed parse accepts: forwarding happens after the parse, so a field of a type the router's structs reject (as `parameters` is rejected today) is still refused; on the HTTP path at data-parallel size 1; not built or measured | only for requests the front recognises |
| Reuse | Envoy as released, one config file | full, if it existed | vllm-router 0.1.17 plus a patch of tens of lines in three files | vllm-router as released, plus new detection in the front |
| What we build and keep | the config (`envoy/ring-hash.yaml`) | nothing | the patch, a Rust build of the router's binary (`[[bin]] vllm-router`, `Cargo.toml:12-13`), and a rebase per release we adopt | a classifier of the router's losses, per router release, and a second path into the Instance that bypasses the router |
| Balance and session affinity | consistent hashing on the same key; hit rate and balance not measured (S2c) | as vllm-router | as vllm-router (S2 phase B) | lost for every request sent around the router |
| Risk | unmeasured on GPUs; no load awareness unless bounded loads (`hash_balance_factor`) are turned on | waiting on upstream with no date: PR 120 (chat JSON pass-through) drew "looks good" on 18 Mar 2026 and has been stale since 15 Sep; PRs 277 (optional `parameters`) and 206 (`structural_tag`) are open | the fork drifts: `spec.rs` grew from 4,293 to 5,202 lines between 0.1.16 and 0.1.17, 26 days apart | a loss the classifier does not know reaches the engine silently; the list above has more than 20 shapes |

llm-d's EPP behind Envoy is not in the table. It delivers what it accepts as its bytes, but it refuses two
content-part forms vLLM accepts. It is two processes, and its scorers sent each agent type to one worker
in S2 phase B (HBEP-5, open question 2). HBEP-5 keeps it as the candidate for multi-node Instances.

**(c) against pinning whole sessions.** (c) needs a classifier of the router's losses because it
diverts single requests. A more conservative form diverts whole sessions instead. The front hashes
each `x-session-id` to one worker of the Instance and sends every request of it there, with no inner
router. That needs no classifier and keeps the bytes. It still needs a path from the front to each
worker, which beta's router-as-entry-point design does not have (HBEP-5, Inside an Instance). It also
moves the in-Instance balance onto the front's ring, which S2 phase B did not measure. Envoy's
`ring_hash` gives the same session-to-worker pinning inside the Instance, with no new path.

**Recommendation: Envoy with `ring_hash` on `x-session-id` as the candidate default, with (b) as the
fallback.** Envoy is the only option measured to deliver the client's request unchanged with a router in
place. It is reuse as released: one config, no fork, no build. It hashes on the header the front already
sets. Placement quality is S2c's question: S2 phase B measured vllm-router's `consistent_hash` at a hit
rate of 0.902, and Envoy's ring has to match that on GPUs. If it does not, (b) keeps vllm-router's
placement and fixes the body. (a) does not exist today. (c) builds more than either.

**What Envoy needs in HBEP-5**, from this config (`envoy/ring-hash.yaml`):
- Listener on loopback, and no admin listener. The router needs none, so the open admin API risk of
  vllm-router (L-4.9) has no counterpart. If the agent wants Envoy's statistics, an admin on a mode-0600
  Unix socket is the shape to use (`admin.address.pipe`); Envoy 1.40.2 accepts it (`--mode validate`).
- Route timeout and stream idle timeout of 10,800 s: `maxRequestDuration` plus 1 h, as for vllm-router.
  Envoy's stream idle default is 5 min (S2's README), which would cut a slow first token. S2 phase A
  carried 2 h streams through Envoy 1.33.2 with the same route values, in the llm-d stack.
- Upstream connections idle out after 50 s, below the workers' `VLLM_HTTP_TIMEOUT_KEEP_ALIVE` of 120 s
  (L-2.5).
- Workers come from the config, so a restart reads them again. With vllm-router, the agent must
  re-register them (L-4.8). Membership changes would go through Envoy's file-based dynamic
  configuration; that was not tried here.
- Not measured, and open: active health checks or outlier detection, which this config has none of,
  and bounded loads (`common_lb_config.consistent_hashing_lb_config.hash_balance_factor`, which Envoy
  1.40.2 accepts).

**(b), concretely,** if S2c rules Envoy out. On v0.1.17:
1. `v1_chat_completions` takes `JsonWithRaw<ChatCompletionRequest>`, the extractor `v1_completions`
   already uses (`server.rs:297-310`, `:312-`), and passes the bytes to a chat route that carries them.
2. The chat route passes them into `route_prepared_request`, which already takes `raw_body`
   (`router.rs:1389-1396`). On an HTTP pool at data-parallel size 1, the request goes out with the
   client's bytes and `Content-Type`, as `CompletionRoute::forward_request` does (`completion_route.rs:22-41`),
   instead of `.json(typed_req)` (`router.rs:1958`).
3. `route_completion_raw` keeps the bytes under every policy, not only KV-aware ones
   (`router.rs:2796-2797`).
4. `Function.parameters` becomes optional (upstream PR 277's change), and `response_format` becomes a
   `Value`, which the router never reads; this lets valid requests through the typed parse.

The typed parse stays, for routing, so the guarantee is conditional. The bytes are kept only for
requests the parse accepts, and a field a later vLLM adds, of a type the structs reject, would still be
refused before forwarding. Upstream merged the same byte-forwarding for KV-aware completions on 1 Nov
2026 (PR 396: "forwarding does not rewrite bytes"), so the patch can be offered back. Its cost is also
conditional on what a reproducible fork needs, none of which exists yet:
- a builder with a pinned Rust toolchain (this host has none);
- the locked dependencies (`cargo build --locked` against the tag's `Cargo.lock`);
- the artifact's sha256, recorded with the patch under `third_party/forked/vllm-router/`;
- an acceptance run: S13's harness against the built binary (`-router <path>`), every valid case
  arriving as its bytes, and a rebase and rerun for each release adopted.

## Proposed HBEP-5 text (for `@hpc-burst-science-agent` to amend)

For open question 2, replacing the bullet "Open (blocks beta): nested-field preservation":

> - **Measured (S13, issue #312):** the router in front of several workers must deliver the client's
>   request as sent.
>   - vllm-router 0.1.16 and 0.1.17 (the latest release, 4 Nov 2026) do not. Both parse chat and
>     completions requests into typed structs and send them again. With vLLM v0.32.0 accepting them,
>     0.1.17 drops `tools[].function.strict` and `defer_loading`, explicit nulls, and the unknown
>     members of the objects it types (messages of known roles, tools, tool calls, `tool_choice`,
>     `response_format`, `stream_options`, `structured_outputs`). It re-escapes keys and collapses
>     duplicate members. It refuses a tool without `parameters`, a `structural_tag` response format,
>     a Harmony message and a known field written twice. It adds its own defaults for fields the
>     client did not set; vLLM reads them as its defaults or ignores them.
>   - Envoy, as a plain proxy with `ring_hash` on `x-session-id` (1.40.2 and 1.33.2), delivered all
>     440 of S13's request variants as their exact bytes.
>   - llm-d's EPP behind Envoy forwarded the bytes of every request it accepted, and refused two
>     content-part forms vLLM accepts.
> - **Proposed, candidate default for beta's Instances with several workers:** Envoy 1.40.2, `ring_hash`
>   on `x-session-id`, with these settings:
>   - the listener on loopback and no admin listener;
>   - route and stream idle timeouts of `maxRequestDuration` plus 1 h (10,800 s);
>   - upstream connections idling out after 50 s;
>   - the workers from its config file (S13's `envoy/ring-hash.yaml`).
>
>   Open (blocks beta): S2c (issue #62) measures its hit rate, balance across workers and latency on
>   GPUs, beside vllm-router's `consistent_hash`. Fallback, if Envoy's placement falls short:
>   vllm-router 0.1.17 with a pinned patch (`third_party/forked/vllm-router/`) that forwards the
>   client's bytes. That is the mechanism upstream merged for KV-aware completions, and it would be
>   offered upstream. Either router is accepted only when S13's harness shows every valid case reaching
>   the workers as its exact bytes.

The bullet "Decided, as the provisional default for beta's Instances with several workers" would then
name vllm-router as the fallback instead, with its three conditions. For Risks, the bullet "Beta: the
router re-serialises the body" becomes:

> - **Beta: the router must not change the body.** vllm-router 0.1.16 and 0.1.17 re-serialise it: they
>   drop declared nested fields, explicit nulls and the unknown members of the objects they type, and
>   add their own defaults for fields the client did not set (S13). Envoy's
>   `ring_hash` proxy forwards it unchanged. A router, or a new release of one, is accepted with S13's
>   harness.

In the Risks bullet on the router's admin API (L-4.9), Envoy's config has no admin listener. In the
Test plan's "No change but `model`", the sentence "In beta the check at the engine compares fields, not
bytes, because the router re-serialises the body (Risks)" becomes "In beta the check at the engine
compares bytes (open question 2)". In "Beta, the router", the sentence on `strict` becomes "Every
valid S13 case reaches the engine through the router as its exact bytes, streamed and not (open
question 2)".

## Open points

- Envoy's hit rate, balance and latency on GPUs are S2c's (#62). This spike shows only placement
  determinism and that both workers took requests.
- Envoy's membership changes, health checks and bounded loads are not measured (see "What Envoy needs").
- The vllm-router patch of (b) is not built or measured. Building it needs a Rust toolchain: a hosted
  runner, or a job.
- No vLLM ran. Validity rests on reading every request-model validator; the cases a server may still
  refuse, by its settings or model, are listed under Method. Neither changes what a router delivered.
- The launcher's dependencies are pinned for CPython 3.15 on x86_64 (`scripts/launcher-deps.txt`); the
  wheels are abi3, so another Python needs only that file regenerated.

## Reproduce

```
S13_HOME=<scratch dir> scripts/setup.sh   # fetch, check every hash and commit, re-run the enumeration,
                                          # build the harness and (S2's setup) the EPP: take the build lock
S13_HOME=<scratch dir> scripts/run.sh     # the control and eight router arms, under a minute here
go test ./...                             # comparator, recorder, case coverage, and the claims above
```

## Components and provenance

| Component | Origin | Licence | Version and pin |
|---|---|---|---|
| vllm-router 0.1.16 | https://github.com/vllm-project/router, PyPI `vllm-router` | Apache-2.0 | wheel `vllm_router-0.1.16-cp38-abi3-manylinux_2_28_x86_64.whl`, sha256 `f400a6d7ba80ab613f28b915a7b81ba8cddcb85732470e3c9737d3430ed6d251`; tag v0.1.16, commit db02f358da401b60d4ae14eb286163763a0705cc (the release S2 measured) |
| vllm-router 0.1.17 | same | Apache-2.0 | wheel `vllm_router-0.1.17-cp38-abi3-manylinux_2_28_x86_64.whl`, sha256 `85c96bea4d573a49b5a8ae4c72ee3dfe733aba26b6f1ff1256e6142f1f42694d`; tag v0.1.17, commit 3451e66a8ba5416d3e3e8eb3d9ee604211cde042 |
| The launcher's Python dependencies | PyPI; the pins S2 used (aiohttp 3.14.5, fastapi 0.162.0, orjson 3.13.0, requests 2.34.2, setproctitle 1.3.8, uvicorn 0.54.0) and what they resolve to | as published | `scripts/launcher-deps.txt`, 28 wheels by sha256, installed with `--require-hashes`. Only the launcher imports them; the router is the Rust extension in the wheel. |
| vLLM | https://github.com/vllm-project/vllm | Apache-2.0 | v0.32.0, commit 5df63a7203cdb9328d824a27b682e77c7b9928be; source read only, for the field list |
| openai-python | https://github.com/openai/openai-python, PyPI `openai` | Apache-2.0 | 2.25.0 wheel, sha256 `7c7c01a0a4b69771a29913e28d06bc1e9cee8781b4d206bb56cb946d8d2fcb23`; source read only, for the message TypedDicts |
| oss-harmony | https://github.com/oss-harmony/harmony, PyPI `oss-harmony` (a fork of openai-harmony) | Apache-2.0 | 0.0.11 sdist, sha256 `ccad467b71d61490df1f2204b7a064990833338615ac135536c5b3a8e2d03a15`, the floor vLLM v0.32.0 sets (`requirements/common.txt:52`); source read only, for the Harmony message fields |
| vllm-router `main` | same as above | Apache-2.0 | commit 394a63e0a3add03b9a78d4582b769375800c1b56 (10 Nov 2026); read only, to date the "no release fixes it" finding |
| Envoy 1.40.2 | https://github.com/envoyproxy/envoy, release asset `envoy-1.40.2-linux-x86_64` | Apache-2.0 | sha256 `1d64506e25c1844f146162ddb2ece7010fa4e784faddb40a079021491f1f03cc`, the value in the release's signed `checksums.txt.asc` and GitHub's asset digest; `--version` reports build b2d89e0ae53916f28770771fbdd49c5c99c5c5a2 |
| Envoy 1.33.2 | same | Apache-2.0 | S2's pin, sha256 `bd2ff87c5efc95fd7880d710915f573c1aab4d9c55d8da886dcd91df43fad741` (`spikes/s2/scripts/setup.sh`); build 2135e1a42f002a939d60581096291acb6abce695 |
| llm-d EPP | https://github.com/llm-d/llm-d-router | Apache-2.0 | v1.0.0, commit b9393d1, built from source by S2's `setup.sh` with S2's loopback patch (`third_party/forked/llm-d-router/`) |
| llm-d guide files | https://github.com/llm-d/llm-d, `guides/no-kubernetes-deployment` | Apache-2.0 | v1.0.0, commit 378b42d; read at run time by S2's `genconf.py`, not copied |
| S2's `setup.sh`, `genconf.py` and the EPP's flags | this repository, `spikes/s2` | this repository's | reused unchanged |
| Harness, recorder, cases, comparator, scripts | new in this spike | this repository's | Go standard library only |

Nothing from www-2027 is reused. From this repository, S2's setup and config generator are reused
unchanged, as is its way of pinning vllm-router from PyPI.
