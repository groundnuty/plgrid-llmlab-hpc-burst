# Spike S13: which request fields the Instance router delivers

Issue #312; HBEP-5, open question 2. No GPU, no cluster: every run is on one host, on ports 29900-29903.

## Result in brief

- **Neither release delivers a request as it was sent.** vllm-router 0.1.16 and 0.1.17, the latest
  release (PyPI and GitHub, 4 Nov 2026), parse `/v1/chat/completions` and `/v1/completions` into typed
  structs and send the structs again. Of 432 request variants, none reached a worker as its bytes under
  `consistent_hash` (HBEP-5's default) or `round_robin`; the two policies gave the same differences on
  every variant. The one exception is 0.1.17 under `--policy kv_aware`, which forwards `/v1/completions`
  as the client's bytes (143 of 143), and still re-serialises chat.
- **What 0.1.17 still loses or refuses, with vLLM v0.32.0 accepting it** (the engine then sees another
  request than the client sent):
  - `tools[].function.strict`, `tools[].function.defer_loading` and `tools[].defer_loading` are dropped.
    vLLM passes each tool's `model_dump()` to the chat template (`renderers/online_renderer.py:287`, `:305`),
    whose serialiser keeps both fields when set (`generate/base/protocol.py:298-313`). With a template
    that renders a tool's JSON, the prompt then differs from a direct call's.
  - A tool without `parameters` is refused with 422; vLLM makes the field optional.
  - `response_format` of type `structural_tag`, in both of vLLM's forms, is refused with 422 on chat.
  - `response_format.json_schema.description` and `stream_options.continuous_usage_stats` are dropped.
  - `"max_tokens": null` on `/v1/completions` is dropped. vLLM reads null as "the rest of the
    context" and absence as 16 tokens (`completion/protocol.py:63`), so such a request returns at most
    16 tokens.
  - In an assistant message, `refusal` and `audio` are dropped, and a `tool_calls` list holding a custom
    tool call (`"type":"custom"`) is dropped whole. The `tool` message that answers it then refers to a
    call the history no longer has.
  - `messages[].task` is dropped. Any extension member inside a message, a tool, a tool call,
    `tool_choice`, `response_format`, `stream_options` or `structured_outputs` is dropped.
  - Explicit nulls are dropped. For the chat fields tested (`max_tokens`, `temperature`, `seed`, `user`,
    `tools`, `stop`) vLLM reads null as absent (`stop` through `sampling_params.py:574-575`), so nothing
    changes there. A message's `"name": null` arrives as no name.
  - Numbers in `float` fields are held as 32-bit floats: `1` arrives as `1.0`, and
    `0.12345678901234567890` as `0.12345679`. Integers above 2^64 inside a `dict[str, Any]` arrive as
    floats.
- **0.1.16 also empties user content.** A user message whose content parts include anything but `text` and
  `image_url` (`input_audio`, `file`, `audio_url`, `video_url`, the embeds, `tool_reference`, vLLM's
  simple URL parts, a bare string) arrives as `"content":""`, with no error. An assistant message with
  `refusal` or `thinking` parts arrives with no content. 0.1.17 fixes both: it keeps content parts as raw JSON
  (`src/protocols/spec.rs:260-268`; upstream issue 322, closed 25 Oct 2026).
- **Both releases keep unknown top-level members** (an `extra_body`-style object, OpenAI's `store`,
  `metadata`, `service_tier`, `verbosity`), the content parts above in 0.1.17, unknown message roles,
  and every other enumerated field's value.
- **Both inject members into every request:** ten on chat, six on completions (table below). Each one is
  either vLLM's own default or a field vLLM v0.32.0 does not declare (`no_stop_trim`,
  `separate_reasoning`, `stream_reasoning`, `return_hidden_states`). vLLM ignores the latter, logging
  them at debug level (`serve/engine/protocol.py:61-75`), so the injections change no outcome measured
  here.
- **No release fixes it.** Upstream `main` at 394a63e (10 Nov 2026, 08:43Z) still types chat requests
  (`server.rs:260-264`) and still declares `Function` with only `name`, `description` and `parameters`
  (`spec.rs:1939-1944`).
- **Recommendation: fix (b).** Carry a small patch on 0.1.17 as a pinned fork. The patch forwards the
  client's bytes on the HTTP path for chat and completions under every policy, and relaxes the two typed
  fields that refuse valid requests. Upstream merged the same byte-forwarding for KV-aware completions on
  1 Nov 2026 (PR 396: "forwarding does not rewrite bytes"), so the patch is upstream-shaped and can be
  offered back. (a) is not available, and (c) costs more building. See "Choosing a fix".

## Method

**The path.** The harness (`cmd/s13fields`) starts two recorders on 127.0.0.1:29900 and :29901, then
the router on :29902 with `--worker-urls` naming both, `--policy <p>`, `--request-timeout-secs 10800`
(HBEP-5) and its metrics on :29903. It sends each case through the router and compares what reached a
recorder. Requests are sent one at a time, each with `x-session-id` (HBEP-5) and an `X-S13-Case` header
naming the variant. The router copies headers to the worker, so the recorder can tell the variants
apart without touching the body. The router's process group is stopped with TERM after the run, and
`run.sh` checks that no listener or process is left.

**The recorder** (`recorder/`) stands in for the engine double. `internal/agent/enginedouble` decodes the
body without keeping its bytes, and lives in the root module, which a spike module cannot import without
pulling in the root module's dependency graph. The recorder stores `bytes.Clone` of exactly what it read
from the connection before anything parses it. It then answers in vLLM's response shapes (a stream when
`stream` is true, with a usage chunk when asked), so the router completes the request and does not
retry. It also answers `/health` and `/v1/models`. Under `consistent_hash` and `round_robin`, the only other
requests the routers made were `GET /health`. Under `kv_aware`, 0.1.17 also called `GET /get_server_info`,
`GET /kv_event_sources`, `POST /tokenize` and the two `/render` endpoints; the recorder answered 404 and
the router routed without them, so that arm shows the forwarding path only, not KV-aware routing.

**The cases** (`cases/`) are literal request bodies, never marshalled. Each sets one field, or one small
group vLLM needs together, on a minimal request, so a router that refuses one field cannot hide what it
does to another. Each case is sent as written and, unless it sets `stream` itself, again with
`"stream":true,` inserted after its opening brace: a byte edit, not a re-encoding. That gives 432
variants in all, 289 on chat and 143 on completions.
- **The fields** are every field of vLLM v0.32.0's request models, enumerated from its source by
  `scripts/enumerate_fields.py` (Python's `ast`, nothing imported). That covers `ChatCompletionRequest`
  and `CompletionRequest`, the nested models they name (tools, tool choice, stream options, the three
  response formats, structured outputs, repetition detection), vLLM's message and content-part types,
  and, for the members of vLLM's message union that come from the `openai` package, that package's
  TypedDicts at 2.25.0, the floor vLLM sets. The list is `fields/vllm-v0.32.0.tsv`, 251 rows, each
  with its file and line. `cases_test.go` fails if a field has no case on each endpoint that accepts it,
  or if a case's cover points outside its body. Two fields have no case: the PIL-image content part,
  which JSON cannot carry.
- **Beyond the fields:** unknown nested members (`tools[].function.x_ext`, an object inside a message,
  `cache_control` inside a content part, and others), `extra_body`-style top-level members, OpenAI fields
  vLLM does not declare, the router's own `session_params`, vLLM's earlier `reasoning_content`, an
  `openai_harmony` message, explicit nulls, number spellings and string escapes, minimal requests, and
  one request shaped like an agent's tool-call turn.
- **Values** are chosen from each field's annotation and validators: non-default, and valid for vLLM as
  far as its source shows. No vLLM ran, so a value vLLM would still refuse is possible. That would not
  change what the router delivered.

**The comparison** (`rawjson/`). The first check is byte equality of the whole body. Then both bodies
are parsed into trees that keep every value's literal bytes, and walked path by path.
- **Canonicalisation, and why:** object member order and insignificant whitespace are ignored, and
  nothing else. JSON objects are unordered (RFC 8259, sections 1 and 4), and the routers serialise their maps
  in no fixed order: the same `logit_bias` arrived with its members in different orders in different
  runs. So a byte comparison alone would flag every body and say nothing about any field.
- **Verdicts:** a value whose literal bytes match is `kept`. The same value spelled otherwise
  (`\u00e9` for `é`, `9e-1` for `0.9`) is `kept, re-encoded`. A change of JSON type, or of a number's form
  between integer and fraction (`1` and `1.0`, which Python reads as int and float), is `retyped`.
  Another value of the same type is `changed`. A value missing at the worker is `dropped`; one present
  only at the worker is `injected`; a value dropped under one key and injected under a sibling is
  `renamed`. A variant that reached no worker is `refused`, with the router's status.

**Controls.**
- **Positive control, the recorder** (`run.sh`, first arm): every variant sent straight to the recorder
  arrived as its exact bytes, 432 of 432, with no canonicalisation. The cases include pretty-printed
  bodies, escapes (`\u00e9`, a surrogate pair, `\/`, `\u0001`), `9e-1`, `1.0` and a 20-digit float. A
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
parse, `TestStoresTheExactBytes` checks that, and the 432-of-432 control could not pass otherwise.

**Environment.** One 6-core VM shared with other work; every run at `nice -n 19` under a 4 GB memory
cap. The router listeners were checked from `ss` after start: 127.0.0.1:29902 and :29903 only. Runs on
10 Nov 2026 from 12:04Z.

## Results

| run | variants | as their exact bytes | lost or changed a value | reached no worker |
|---|---|---|---|---|
| direct to the recorder (control) | 432 | 432 | 0 | 0 |
| vllm-router 0.1.16, consistent_hash | 432 | 0 | 424 | 8 |
| vllm-router 0.1.16, round_robin | 432 | 0 | 424 | 8 |
| vllm-router 0.1.17, consistent_hash | 432 | 0 | 424 | 8 |
| vllm-router 0.1.17, round_robin | 432 | 0 | 424 | 8 |
| vllm-router 0.1.17, kv_aware | 432 | 143 (all of completions) | 281 (all of chat) | 8 |

"Lost or changed" counts the injected members too, which every delivered variant has. The 8 that reached
no worker are the 422s: two structural-tag formats, a tool without `parameters`, and the Harmony
message, each streamed and not.

**Covers by verdict.** A cover is one field a case sets; its verdict is the worse of its plain and streamed variants. Of 301 covers of enumerated fields, 0.1.16 delivers 233 with their bytes and 0.1.17 delivers 271.

| run | covers | kept | kept, re-encoded | dropped | retyped | changed | renamed | refused |
|---|---|---|---|---|---|---|---|---|
| vllm-router 0.1.16, consistent_hash, enumerated fields | 301 | 233 | 0 | 24 | 35 | 0 | 0 | 9 |
| vllm-router 0.1.16, consistent_hash, extensions and encodings | 44 | 13 | 5 | 18 | 4 | 2 | 1 | 1 |
| vllm-router 0.1.17, consistent_hash, enumerated fields | 301 | 271 | 0 | 19 | 2 | 0 | 0 | 9 |
| vllm-router 0.1.17, consistent_hash, extensions and encodings | 44 | 16 | 5 | 16 | 3 | 2 | 1 | 1 |

**Every cover that either release did not deliver as sent**, with the bytes at the worker (— for none). Under HBEP-5's `consistent_hash`:

| case | field | sent | vllm-router 0.1.16, consistent_hash: received | verdict | vllm-router 0.1.17, consistent_hash: received | verdict |
|---|---|---|---|---|---|---|
| chat/logit_bias | logit_bias | `{"50256":-100,"198":2.5}` | `{"198":2.5,"50256":-100.0}` | retyped | `{"198":2.5,"50256":-100.0}` | retyped |
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
| completions/logit_bias | logit_bias | `{"50256":-100,"198":2.5}` | `{"50256":-100.0,"198":2.5}` | retyped | `{"50256":-100.0,"198":2.5}` | retyped |
| completions/stream_options | stream_options | `{"include_usage":true,"continuous_usage_stats":true}` | `{"include_usage":true}` | dropped (stream only) | `{"include_usage":true}` | dropped (stream only) |
| completions/stream_options | stream_options.continuous_usage_stats | `true` | — | dropped (stream only) | — | dropped (stream only) |
| completions/max_tokens-null | max_tokens | `null` | — | dropped | — | dropped |
| chat/ext/messages-x_ext | ext:messages[].x_ext | `{"trace":"t-1","n":1}` | — | dropped | — | dropped |
| chat/ext/content-part-cache_control | ext:messages[].content[].cache_control | `{"type":"ephemeral"}` | — | dropped | `{"type":"ephemeral"}` | kept |
| chat/ext/system-part-cache_control | ext:messages[](system).content[].cache_control | `{"type":"ephemeral"}` | — | dropped | `{"type":"ephemeral"}` | kept |
| chat/ext/assistant-reasoning_content | ext:messages[](assistant).reasoning_content | `"The user greets."` | `messages[1].reasoning: "The user greets."` | renamed | `messages[1].reasoning: "The user greets."` | renamed |
| chat/ext/harmony-message | ext:messages[](harmony) | `{"author":{"role":"user"},"content":[{"type":"text","text":"…` | `HTTP 422` | refused | `HTTP 422` | refused |
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
| chat/enc/kwargs-numbers | ext:numbers inside a dict[str, Any] | `{"big":123456789012345678901234567890,"u64":1844674407370955…` | `{"i":1,"neg":-0.0,"big":1.2345678901234568e+29,"f":1.0,"u64"…` | changed | `{"big":1.2345678901234568e+29,"neg":-0.0,"f":1.0,"i":1,"u64"…` | changed |
| chat/enc/explicit-nulls | ext:max_tokens: null | `null` | — | dropped | — | dropped |
| chat/enc/explicit-nulls | ext:temperature: null | `null` | — | dropped | — | dropped |
| chat/enc/explicit-nulls | ext:stop: null | `null` | — | dropped | — | dropped |
| chat/enc/explicit-nulls | ext:user: null | `null` | — | dropped | — | dropped |
| chat/enc/explicit-nulls | ext:seed: null | `null` | — | dropped | — | dropped |
| chat/enc/explicit-nulls | ext:tools: null | `null` | — | dropped | — | dropped |
| chat/enc/message-name-null | ext:messages[].name: null | `null` | — | dropped | — | dropped |
| chat/enc/string-escapes | ext:string escapes (an escaped e-acute, a surrogate pair, an escaped slash, a control character) | `"caf\u00e9 \ud83d\ude00 \/ <&> \u0001 \t"` | `"café 😀 / <&> \u0001 \t"` | kept, re-encoded | `"café 😀 / <&> \u0001 \t"` | kept, re-encoded |
| completions/enc/string-escapes | ext:string escapes (an escaped e-acute, a surrogate pair, an escaped slash, a control character) | `"caf\u00e9 \ud83d\ude00 \/ <&> \u0001 \t"` | `"café 😀 / <&> \u0001 \t"` | kept, re-encoded | `"café 😀 / <&> \u0001 \t"` | kept, re-encoded |
| chat/real/agent-turn | ext:agent turn: tools[].function.strict | `true` | — | dropped (stream only) | — | dropped (stream only) |
| chat/real/agent-turn | ext:agent turn: temperature 0 | `0` | `0.0` | retyped (stream only) | `0.0` | retyped (stream only) |

**Members injected into requests**, by how many of the delivered variants carry them. A variant lacks one only when it set that member itself:

| endpoint | injected path | value | vllm-router 0.1.16, consistent_hash: variants | vllm-router 0.1.17, consistent_hash: variants |
|---|---|---|---|---|
| /v1/chat/completions | `continue_final_message` | `false` | 279 of 281 | 279 of 281 |
| /v1/chat/completions | `ignore_eos` | `false` | 279 of 281 | 279 of 281 |
| /v1/chat/completions | `include_reasoning` | `true` | 279 of 281 | 279 of 281 |
| /v1/chat/completions | `logprobs` | `false` | 279 of 281 | 279 of 281 |
| /v1/chat/completions | `no_stop_trim` | `false` | 281 of 281 | 281 of 281 |
| /v1/chat/completions | `return_hidden_states` | `false` | 281 of 281 | 281 of 281 |
| /v1/chat/completions | `separate_reasoning` | `true` | 281 of 281 | 281 of 281 |
| /v1/chat/completions | `skip_special_tokens` | `true` | 279 of 281 | 279 of 281 |
| /v1/chat/completions | `stream` | `false` | 138 of 281 | 138 of 281 |
| /v1/chat/completions | `stream_reasoning` | `true` | 281 of 281 | 281 of 281 |
| /v1/completions | `echo` | `false` | 141 of 143 | 141 of 143 |
| /v1/completions | `ignore_eos` | `false` | 141 of 143 | 141 of 143 |
| /v1/completions | `no_stop_trim` | `false` | 143 of 143 | 143 of 143 |
| /v1/completions | `return_hidden_states` | `false` | 143 of 143 | 143 of 143 |
| /v1/completions | `skip_special_tokens` | `true` | 141 of 143 | 141 of 143 |
| /v1/completions | `stream` | `false` | 70 of 143 | 70 of 143 |

The full tables, one row per covered field per run, with the bytes sent and received, are
`results/<run>/fields.md`. The raw evidence, the bytes sent and received for each variant, is
`results/<run>/results.jsonl`. Regenerate the tables above with
`go run ./cmd/s13table results/vllm-router-0.1.16-consistent_hash results/vllm-router-0.1.17-consistent_hash`.


**Streaming** changes nothing: every case sent both ways has the same verdict for each of its fields,
streamed and not.

## Why: the router's types

Lines are in `src/protocols/spec.rs` at v0.1.17 (3451e66); 0.1.16 (db02f35) has the same declarations
at other lines (`Function` is at `:1839`).
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

## Choosing a fix

| | (a) a newer release | (b) a pinned patch of 0.1.17 | (c) a fixed worker for affected requests |
|---|---|---|---|
| Delivers every field | no release does; upstream `main` at 394a63e neither | yes by construction, on the HTTP path at the router's default data-parallel size of 1; not yet measured | only for requests the front recognises |
| Reuse | full, if it existed | vllm-router 0.1.17 as released, plus a patch of tens of lines in three files | vllm-router as released, plus new detection in the front |
| What we build and keep | nothing | the patch, a Rust build of the router's own binary (`[[bin]] vllm-router`, `Cargo.toml:12-13`), and a rebase per release we adopt | a classifier of the router's losses, per router release, and a second path into the Instance that bypasses the router |
| Balance and session affinity | kept | kept: routing still reads the typed parse | lost for every request sent around the router |
| Risk | waiting on upstream with no date: PR 120 (chat JSON pass-through) drew "looks good" on 18 Mar 2026 and has been stale since 15 Sep; PRs 277 (optional `parameters`) and 206 (`structural_tag`) are open | the fork drifts: `spec.rs` grew from 4,293 to 5,202 lines between 0.1.16 and 0.1.17, 26 days apart | a loss the classifier does not know reaches the engine silently; the list above has more than 20 shapes |

**(b), concretely,** on v0.1.17:
1. `v1_chat_completions` takes `JsonWithRaw<ChatCompletionRequest>`, the extractor `v1_completions`
   already uses (`server.rs:297-330`), and passes the bytes to a chat route that carries them.
2. The chat route passes them into `route_prepared_request`, which already takes `raw_body`
   (`router.rs:1389-1396`). On an HTTP pool at data-parallel size 1, the request goes out with the
   client's bytes and `Content-Type`, as `CompletionRoute::forward_request` does (`completion_route.rs:22-41`),
   instead of `.json(typed_req)`.
3. `route_completion_raw` keeps the bytes under every policy, not only KV-aware ones
   (`router.rs:2796-2797`).
4. `Function.parameters` becomes optional (upstream PR 277's change), and `response_format` becomes a
   `Value`, which the router never reads; this lets valid requests through the typed parse.

The typed parse stays: routing still reads the model, the stream flag and the session key from it. With
HBEP-5's `consistent_hash` on `x-session-id`, the key is a header anyway. The Harmony message stays
refused: it has no `role`, and the router requires one. Whether to accept such messages is a choice for
vLLM's users of `gpt-oss`, and none is planned here. This spike did not build the patch: there is no
Rust toolchain on this host. The harness takes any router binary (`-router <path>`), so the patched
build can be measured with the same cases. That is the acceptance check proposed below.

**Recommendation: (b).** It is the only option that delivers the client's request with the router in
place, and it reuses vllm-router, the release HBEP-5 already chose on S2's evidence. The mechanism
matches what upstream merged for KV-aware completions, so it can be offered upstream and the fork
dropped when a release carries it. (a) does not exist today. (c) builds more than (b): a
release-specific loss classifier in the front, and a bypass path that gives up balancing and affinity
for exactly the requests it diverts.

## Proposed HBEP-5 text (for `@hpc-burst-science-agent` to amend)

For open question 2, replacing the bullet "Open (blocks beta): nested-field preservation":

> - **Measured (S13, issue #312), open (blocks beta):** neither vllm-router 0.1.16 nor 0.1.17 (the
>   latest release, 4 Nov 2026) delivers a request as sent. Both parse chat and completions requests
>   into typed structs and send them again, under every policy but 0.1.17's `kv_aware` for
>   completions. With vLLM v0.32.0 accepting them, 0.1.17 drops `tools[].function.strict` and
>   `defer_loading`, `tools[].defer_loading`, `response_format.json_schema.description`,
>   `stream_options.continuous_usage_stats`, an assistant message's `refusal`, `audio` and custom tool
>   calls, `messages[].task`, `"max_tokens": null` on completions (vLLM then caps the reply at 16
>   tokens), and every unknown member below the top level, except inside content parts and messages of
>   unknown roles. It refuses a tool without `parameters` and a
>   `structural_tag` response format with 422, and adds ten members to every chat request and six to
>   every completions request, which vLLM ignores or reads as its defaults. 0.1.16 also replaces user
>   content parts other than text and images with an empty string.
> - **Proposed decision:** beta's router is vllm-router 0.1.17 with a pinned patch
>   (`third_party/forked/vllm-router/`). The patch forwards the client's bytes on the HTTP path for chat
>   and completions under every policy, and makes `tools[].function.parameters` optional and
>   `response_format` opaque. It is the mechanism upstream merged for KV-aware completions (PR 396), and
>   it is offered upstream. The router default stays provisional until S13's harness shows every case
>   through the patched router as its exact bytes, except the cases the router still refuses by design.
>   The proposal names those cases.

For Risks, the bullet "Beta: the router re-serialises the body" becomes:

> - **Beta: the router re-serialises the body.** vllm-router 0.1.16 and 0.1.17 drop nested fields,
>   explicit nulls and unknown nested members, and inject defaults (S13). Mitigation: the patched router
>   of open question 2 forwards the client's bytes. Until it is in place, a request through the router
>   does not reach the engine as the client sent it.

For the Test plan, in "No change but `model`", the sentence "In beta the check at the engine compares
fields, not bytes, because the router re-serialises the body (Risks)" becomes "In beta the check at the
engine compares bytes through the patched router (open question 2)". In "Beta, the router", the
sentence on `strict` becomes "Every S13 case reaches the engine through the router as its exact bytes,
streamed and not (open question 2)".

## Open points

- The patch of (b) is not built or measured. Building it needs a Rust toolchain: a hosted runner, or a
  job.
- No vLLM ran. Values follow vLLM's annotations; a value vLLM v0.32.0 still refuses would not change what
  the router delivered.
- llm-d's EPP with Envoy, S2's other candidate, was not measured. The harness can measure it as one
  more arm.
- The launcher's dependencies are pinned for CPython 3.15 on x86_64 (`scripts/launcher-deps.txt`); the
  wheels are abi3, so another Python needs only that file regenerated.

## Reproduce

```
S13_HOME=<scratch dir> scripts/setup.sh   # fetch, check every hash and commit, re-run the enumeration, build
S13_HOME=<scratch dir> scripts/run.sh     # the control and five router arms, about 5 minutes
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
| vllm-router `main` | same as above | Apache-2.0 | commit 394a63e0a3add03b9a78d4582b769375800c1b56 (10 Nov 2026); read only, to date the "no release fixes it" finding |
| Harness, recorder, cases, comparator, scripts | new in this spike | this repository's | Go standard library only |

Nothing from www-2027 is reused beyond S2's way of pinning vllm-router from PyPI.
