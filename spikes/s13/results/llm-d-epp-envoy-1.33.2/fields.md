# llm-d EPP v1.0.0 behind Envoy 1.33.2 (S2's tuned stack)

- Router: `s13/scripts/stack.sh llm-d`, two workers.
- Router listeners: 127.0.0.1:29906, 127.0.0.1:29905, 127.0.0.1:29904, 127.0.0.1:29903, 127.0.0.1:29902, 127.0.0.1:29902. Stop: exited after TERM: <nil>.
- Started 2026-11-10T12:49:47Z.
- llm-d EPP v1.0.0 behind Envoy 1.33.2 (S2's tuned stack): 440 variants; 432 arrived as their exact bytes; 0 differ only in member order and insignificant whitespace; 0 differ only in re-encoded values or keys; 0 lost or changed a value; 8 did not reach a worker.
- Requests the router made to the workers besides the cases: GET /metrics (case none) x84; POST /v1/chat/completions (case warm-up) x1.

## Fields

One row per field a case covers; `plain` and `stream` are the verdicts without and with `"stream":true`.

| case | field | at | sent | received | plain | stream |
|---|---|---|---|---|---|---|
| chat/minimal | model | `model` | `"s13-model"` | `"s13-model"` | kept | kept |
| chat/minimal | messages | `messages` | `[{"role":"user","content":"hello"}]` | `[{"role":"user","content":"hello"}]` | kept | kept |
| chat/minimal | messages[](user).role | `messages[0].role` | `"user"` | `"user"` | kept | kept |
| chat/minimal | messages[](user).content | `messages[0].content` | `"hello"` | `"hello"` | kept | kept |
| completions/minimal | model | `model` | `"s13-model"` | `"s13-model"` | kept | kept |
| completions/minimal | prompt | `prompt` | `"hello"` | `"hello"` | kept | kept |
| completions/prompt-list-of-strings | prompt | `prompt` | `["hello","world"]` | `["hello","world"]` | kept | kept |
| completions/prompt-token-ids | prompt | `prompt` | `[1,2,3]` | `[1,2,3]` | kept | kept |
| completions/prompt-token-id-lists | prompt | `prompt` | `[[1,2],[3,4]]` | `[[1,2],[3,4]]` | kept | kept |
| chat/frequency_penalty | frequency_penalty | `frequency_penalty` | `0.1` | `0.1` | kept | kept |
| chat/logit_bias | logit_bias | `logit_bias` | `{"50256":-100,"198":2.5}` | `{"50256":-100,"198":2.5}` | kept | kept |
| chat/max_tokens | max_tokens | `max_tokens` | `32` | `32` | kept | kept |
| chat/n | n | `n` | `2` | `2` | kept | kept |
| chat/presence_penalty | presence_penalty | `presence_penalty` | `-0.3` | `-0.3` | kept | kept |
| chat/seed-int64-max | seed | `seed` | `9223372036854775807` | `9223372036854775807` | kept | kept |
| chat/seed-negative-beyond-2^53 | seed | `seed` | `-9007199254740993` | `-9007199254740993` | kept | kept |
| chat/stop-string | stop | `stop` | `"END"` | `"END"` | kept | kept |
| chat/stop-array | stop | `stop` | `["END","STOP"]` | `["END","STOP"]` | kept | kept |
| chat/stream-false | stream | `stream` | `false` | `false` | kept | — |
| chat/stream-true | stream | `stream` | `true` | `true` | — | kept |
| chat/stream_options | stream_options | `stream_options` | `{"include_usage":true,"continuous_usage_stats":true}` | `{"include_usage":true,"continuous_usage_stats":true}` | — | kept |
| chat/stream_options | stream_options.include_usage | `stream_options.include_usage` | `true` | `true` | — | kept |
| chat/stream_options | stream_options.continuous_usage_stats | `stream_options.continuous_usage_stats` | `true` | `true` | — | kept |
| chat/temperature | temperature | `temperature` | `0.3` | `0.3` | kept | kept |
| chat/top_p | top_p | `top_p` | `0.9` | `0.9` | kept | kept |
| chat/user | user | `user` | `"s13-user"` | `"s13-user"` | kept | kept |
| chat/use_beam_search | use_beam_search | `use_beam_search` | `true` | `true` | kept | kept |
| chat/top_k | top_k | `top_k` | `40` | `40` | kept | kept |
| chat/min_p | min_p | `min_p` | `0.05` | `0.05` | kept | kept |
| chat/repetition_penalty | repetition_penalty | `repetition_penalty` | `1.1` | `1.1` | kept | kept |
| chat/watermarking | watermarking | `watermarking` | `true` | `true` | kept | kept |
| chat/length_penalty | length_penalty | `length_penalty` | `0.8` | `0.8` | kept | kept |
| chat/stop_token_ids | stop_token_ids | `stop_token_ids` | `[7,8]` | `[7,8]` | kept | kept |
| chat/include_stop_str_in_output | include_stop_str_in_output | `include_stop_str_in_output` | `true` | `true` | kept | kept |
| chat/ignore_eos | ignore_eos | `ignore_eos` | `true` | `true` | kept | kept |
| chat/min_tokens | min_tokens | `min_tokens` | `3` | `3` | kept | kept |
| chat/skip_special_tokens | skip_special_tokens | `skip_special_tokens` | `false` | `false` | kept | kept |
| chat/spaces_between_special_tokens | spaces_between_special_tokens | `spaces_between_special_tokens` | `false` | `false` | kept | kept |
| chat/truncate_prompt_tokens | truncate_prompt_tokens | `truncate_prompt_tokens` | `512` | `512` | kept | kept |
| chat/truncation_side | truncation_side | `truncation_side` | `"left"` | `"left"` | kept | kept |
| chat/prompt_logprobs | prompt_logprobs | `prompt_logprobs` | `2` | `2` | kept | — |
| chat/allowed_token_ids | allowed_token_ids | `allowed_token_ids` | `[1,2,3]` | `[1,2,3]` | kept | kept |
| chat/bad_words | bad_words | `bad_words` | `["bad"]` | `["bad"]` | kept | kept |
| chat/echo | echo | `echo` | `true` | `true` | kept | kept |
| chat/priority | priority | `priority` | `5` | `5` | kept | kept |
| chat/request_id | request_id | `request_id` | `"s13-request-1"` | `"s13-request-1"` | kept | kept |
| chat/session_id | session_id | `session_id` | `"s13-session"` | `"s13-session"` | kept | kept |
| chat/return_tokens_as_token_ids | return_tokens_as_token_ids | `return_tokens_as_token_ids` | `true` | `true` | kept | kept |
| chat/return_token_ids | return_token_ids | `return_token_ids` | `true` | `true` | kept | kept |
| chat/routed_experts_prompt_start | routed_experts_prompt_start | `routed_experts_prompt_start` | `4` | `4` | kept | kept |
| chat/return_token_offsets | return_token_offsets | `return_token_offsets` | `true` | `true` | kept | kept |
| chat/mm_payload | mm_payload | `mm_payload` | `"none"` | `"none"` | kept | kept |
| chat/return_mm_kwargs | return_mm_kwargs | `return_mm_kwargs` | `true` | `true` | kept | kept |
| chat/cache_salt | cache_salt | `cache_salt` | `"s13-salt"` | `"s13-salt"` | kept | kept |
| chat/kv_transfer_params | kv_transfer_params | `kv_transfer_params` | `{"do_remote_prefill":false,"remote_block_ids":[1,2]}` | `{"do_remote_prefill":false,"remote_block_ids":[1,2]}` | kept | kept |
| chat/ec_transfer_params | ec_transfer_params | `ec_transfer_params` | `{"engine":"e1"}` | `{"engine":"e1"}` | kept | kept |
| chat/vllm_xargs | vllm_xargs | `vllm_xargs` | `{"a":"x","b":3,"c":0.5,"d":["y",4]}` | `{"a":"x","b":3,"c":0.5,"d":["y",4]}` | kept | kept |
| chat/repetition_detection | repetition_detection | `repetition_detection` | `{"max_pattern_size":8,"min_pattern_size":2,"min_count":3}` | `{"max_pattern_size":8,"min_pattern_size":2,"min_count":3}` | kept | kept |
| chat/repetition_detection | repetition_detection.max_pattern_size | `repetition_detection.max_pattern_size` | `8` | `8` | kept | kept |
| chat/repetition_detection | repetition_detection.min_pattern_size | `repetition_detection.min_pattern_size` | `2` | `2` | kept | kept |
| chat/repetition_detection | repetition_detection.min_count | `repetition_detection.min_count` | `3` | `3` | kept | kept |
| chat/thinking_token_budget | thinking_token_budget | `thinking_token_budget` | `256` | `256` | kept | kept |
| chat/stream_interval | stream_interval | `stream_interval` | `4` | `4` | kept | kept |
| chat/add_special_tokens | add_special_tokens | `add_special_tokens` | `false` | `false` | kept | kept |
| chat/response_format-text | response_format | `response_format` | `{"type":"text"}` | `{"type":"text"}` | kept | kept |
| chat/response_format-text | response_format.type | `response_format.type` | `"text"` | `"text"` | kept | kept |
| chat/response_format-json_object | response_format.type | `response_format.type` | `"json_object"` | `"json_object"` | kept | kept |
| chat/response_format-json_schema | response_format.json_schema | `response_format.json_schema` | `{"name":"s13_schema","description":"A schema","schema":{"type":"object…` | `{"name":"s13_schema","description":"A schema","schema":{"type":"object…` | kept | kept |
| chat/response_format-json_schema | response_format.json_schema.name | `response_format.json_schema.name` | `"s13_schema"` | `"s13_schema"` | kept | kept |
| chat/response_format-json_schema | response_format.json_schema.description | `response_format.json_schema.description` | `"A schema"` | `"A schema"` | kept | kept |
| chat/response_format-json_schema | response_format.json_schema.json_schema | `response_format.json_schema.schema` | `{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"…` | `{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"…` | kept | kept |
| chat/response_format-json_schema | response_format.json_schema.strict | `response_format.json_schema.strict` | `true` | `true` | kept | kept |
| chat/response_format-structural_tag | response_format(structural_tag).type | `response_format.type` | `"structural_tag"` | `"structural_tag"` | kept | kept |
| chat/response_format-structural_tag | response_format(structural_tag).format | `response_format.format` | `{"type":"triggered_tags","triggers":["<f>"],"tags":[{"begin":"<f>","co…` | `{"type":"triggered_tags","triggers":["<f>"],"tags":[{"begin":"<f>","co…` | kept | kept |
| chat/response_format-structural_tag-legacy | response_format(structural_tag, legacy).type | `response_format.type` | `"structural_tag"` | `"structural_tag"` | kept | kept |
| chat/response_format-structural_tag-legacy | response_format(structural_tag, legacy).structures | `response_format.structures` | `[{"begin":"<f>","schema":{"type":"object"},"end":"</f>"}]` | `[{"begin":"<f>","schema":{"type":"object"},"end":"</f>"}]` | kept | kept |
| chat/response_format-structural_tag-legacy | response_format(structural_tag, legacy).triggers | `response_format.triggers` | `["<f>"]` | `["<f>"]` | kept | kept |
| chat/response_format-structural_tag-legacy | response_format(structural_tag, legacy).structures[].begin | `response_format.structures[0].begin` | `"<f>"` | `"<f>"` | kept | kept |
| chat/response_format-structural_tag-legacy | response_format(structural_tag, legacy).structures[].structural_tag_schema | `response_format.structures[0].schema` | `{"type":"object"}` | `{"type":"object"}` | kept | kept |
| chat/response_format-structural_tag-legacy | response_format(structural_tag, legacy).structures[].end | `response_format.structures[0].end` | `"</f>"` | `"</f>"` | kept | kept |
| chat/structured_outputs-json | structured_outputs | `structured_outputs` | `{"json":{"type":"object"},"disable_any_whitespace":true,"disable_addit…` | `{"json":{"type":"object"},"disable_any_whitespace":true,"disable_addit…` | kept | kept |
| chat/structured_outputs-json | structured_outputs.json | `structured_outputs.json` | `{"type":"object"}` | `{"type":"object"}` | kept | kept |
| chat/structured_outputs-json | structured_outputs.disable_any_whitespace | `structured_outputs.disable_any_whitespace` | `true` | `true` | kept | kept |
| chat/structured_outputs-json | structured_outputs.disable_additional_properties | `structured_outputs.disable_additional_properties` | `true` | `true` | kept | kept |
| chat/structured_outputs-json | structured_outputs.whitespace_pattern | `structured_outputs.whitespace_pattern` | `"[ ]?"` | `"[ ]?"` | kept | kept |
| chat/structured_outputs-regex | structured_outputs.regex | `structured_outputs.regex` | `"[a-z]+"` | `"[a-z]+"` | kept | kept |
| chat/structured_outputs-choice | structured_outputs.choice | `structured_outputs.choice` | `["yes","no"]` | `["yes","no"]` | kept | kept |
| chat/structured_outputs-grammar | structured_outputs.grammar | `structured_outputs.grammar` | `"root ::= \"yes\" \| \"no\""` | `"root ::= \"yes\" \| \"no\""` | kept | kept |
| chat/structured_outputs-json_object | structured_outputs.json_object | `structured_outputs.json_object` | `true` | `true` | kept | kept |
| chat/structured_outputs-structural_tag | structured_outputs.structural_tag | `structured_outputs.structural_tag` | `"{\"type\":\"structural_tag\",\"format\":{\"type\":\"const_string\",\"…` | `"{\"type\":\"structural_tag\",\"format\":{\"type\":\"const_string\",\"…` | kept | kept |
| chat/logprobs-top_logprobs | logprobs | `logprobs` | `true` | `true` | kept | kept |
| chat/logprobs-top_logprobs | top_logprobs | `top_logprobs` | `5` | `5` | kept | kept |
| chat/logprob_token_ids | logprob_token_ids | `logprob_token_ids` | `[11,12]` | `[11,12]` | kept | kept |
| chat/max_completion_tokens | max_completion_tokens | `max_completion_tokens` | `48` | `48` | kept | kept |
| chat/tools | tools | `tools` | `[{"type":"function","function":{"name":"get_weather","description":"We…` | `[{"type":"function","function":{"name":"get_weather","description":"We…` | kept | kept |
| chat/tools | tools[].type | `tools[0].type` | `"function"` | `"function"` | kept | kept |
| chat/tools | tools[].function | `tools[0].function` | `{"name":"get_weather","description":"Weather for a city","parameters":…` | `{"name":"get_weather","description":"Weather for a city","parameters":…` | kept | kept |
| chat/tools | tools[].function.name | `tools[0].function.name` | `"get_weather"` | `"get_weather"` | kept | kept |
| chat/tools | tools[].function.description | `tools[0].function.description` | `"Weather for a city"` | `"Weather for a city"` | kept | kept |
| chat/tools | tools[].function.parameters | `tools[0].function.parameters` | `{"type":"object","properties":{"city":{"type":"string"}},"required":["…` | `{"type":"object","properties":{"city":{"type":"string"}},"required":["…` | kept | kept |
| chat/tools-function-strict | tools[].function.strict | `tools[0].function.strict` | `true` | `true` | kept | kept |
| chat/tools-function-defer_loading | tools[].function.defer_loading | `tools[0].function.defer_loading` | `true` | `true` | kept | kept |
| chat/tools-defer_loading | tools[].defer_loading | `tools[0].defer_loading` | `true` | `true` | kept | kept |
| chat/tools-no-parameters | tools[].function.name | `tools[0].function.name` | `"get_time"` | `"get_time"` | kept | kept |
| chat/tool_choice-none | tool_choice | `tool_choice` | `"none"` | `"none"` | kept | kept |
| chat/tool_choice-auto | tool_choice | `tool_choice` | `"auto"` | `"auto"` | kept | kept |
| chat/tool_choice-required | tool_choice | `tool_choice` | `"required"` | `"required"` | kept | kept |
| chat/tool_choice-named | tool_choice | `tool_choice` | `{"type":"function","function":{"name":"get_weather"}}` | `{"type":"function","function":{"name":"get_weather"}}` | kept | kept |
| chat/tool_choice-named | tool_choice.type | `tool_choice.type` | `"function"` | `"function"` | kept | kept |
| chat/tool_choice-named | tool_choice.function | `tool_choice.function` | `{"name":"get_weather"}` | `{"name":"get_weather"}` | kept | kept |
| chat/tool_choice-named | tool_choice.function.name | `tool_choice.function.name` | `"get_weather"` | `"get_weather"` | kept | kept |
| chat/reasoning_effort-high | reasoning_effort | `reasoning_effort` | `"high"` | `"high"` | kept | kept |
| chat/reasoning_effort-max | reasoning_effort | `reasoning_effort` | `"max"` | `"max"` | kept | kept |
| chat/include_reasoning | include_reasoning | `include_reasoning` | `false` | `false` | kept | kept |
| chat/parallel_tool_calls | parallel_tool_calls | `parallel_tool_calls` | `false` | `false` | kept | kept |
| chat/documents | documents | `documents` | `[{"title":"Doc","text":"Body."}]` | `[{"title":"Doc","text":"Body."}]` | kept | kept |
| chat/chat_template | chat_template | `chat_template` | `"{% for m in messages %}{{ m.content }}{% endfor %}"` | `"{% for m in messages %}{{ m.content }}{% endfor %}"` | kept | kept |
| chat/chat_template_kwargs | chat_template_kwargs | `chat_template_kwargs` | `{"enable_thinking":false,"depth":2}` | `{"enable_thinking":false,"depth":2}` | kept | kept |
| chat/media_io_kwargs | media_io_kwargs | `media_io_kwargs` | `{"video":{"num_frames":8}}` | `{"video":{"num_frames":8}}` | kept | kept |
| chat/mm_processor_kwargs | mm_processor_kwargs | `mm_processor_kwargs` | `{"fps":2}` | `{"fps":2}` | kept | kept |
| chat/return_prompt_text | return_prompt_text | `return_prompt_text` | `true` | `true` | kept | kept |
| chat/msg/system-string-name | messages[](system).role | `messages[0].role` | `"system"` | `"system"` | kept | kept |
| chat/msg/system-string-name | messages[](system).content | `messages[0].content` | `"Be brief."` | `"Be brief."` | kept | kept |
| chat/msg/system-string-name | messages[](system).name | `messages[0].name` | `"sys1"` | `"sys1"` | kept | kept |
| chat/msg/system-array-content | messages[](system).content | `messages[0].content` | `[{"type":"text","text":"Be brief."}]` | `[{"type":"text","text":"Be brief."}]` | kept | kept |
| chat/msg/system-array-content | messages[].content[](text).type | `messages[0].content[0].type` | `"text"` | `"text"` | kept | kept |
| chat/msg/system-array-content | messages[].content[](text).text | `messages[0].content[0].text` | `"Be brief."` | `"Be brief."` | kept | kept |
| chat/msg/developer | messages[](developer).role | `messages[0].role` | `"developer"` | `"developer"` | kept | kept |
| chat/msg/developer | messages[](developer).content | `messages[0].content` | `"Be terse."` | `"Be terse."` | kept | kept |
| chat/msg/developer | messages[](developer).name | `messages[0].name` | `"dev1"` | `"dev1"` | kept | kept |
| chat/msg/user-name | messages[](user).role | `messages[0].role` | `"user"` | `"user"` | kept | kept |
| chat/msg/user-name | messages[](user).content | `messages[0].content` | `"hello"` | `"hello"` | kept | kept |
| chat/msg/user-name | messages[](user).name | `messages[0].name` | `"alice"` | `"alice"` | kept | kept |
| chat/msg/user-part-text | messages[](user).content | `messages[0].content` | `[{"type":"text","text":"hello"}]` | `[{"type":"text","text":"hello"}]` | kept | kept |
| chat/msg/user-part-text | messages[].content[](text).text | `messages[0].content[0].text` | `"hello"` | `"hello"` | kept | kept |
| chat/msg/user-part-image_url | messages[].content[](image_url).type | `messages[0].content[1].type` | `"image_url"` | `"image_url"` | kept | kept |
| chat/msg/user-part-image_url | messages[].content[](image_url).image_url | `messages[0].content[1].image_url` | `{"url":"https://example.com/a.png","detail":"low"}` | `{"url":"https://example.com/a.png","detail":"low"}` | kept | kept |
| chat/msg/user-part-image_url | messages[].content[](image_url).image_url.url | `messages[0].content[1].image_url.url` | `"https://example.com/a.png"` | `"https://example.com/a.png"` | kept | kept |
| chat/msg/user-part-image_url | messages[].content[](image_url).image_url.detail | `messages[0].content[1].image_url.detail` | `"low"` | `"low"` | kept | kept |
| chat/msg/user-part-input_audio | messages[].content[](input_audio).type | `messages[0].content[0].type` | `"input_audio"` | `"input_audio"` | kept | kept |
| chat/msg/user-part-input_audio | messages[].content[](input_audio).input_audio | `messages[0].content[0].input_audio` | `{"data":"UklGRg==","format":"wav"}` | `{"data":"UklGRg==","format":"wav"}` | kept | kept |
| chat/msg/user-part-input_audio | messages[].content[](input_audio).input_audio.data | `messages[0].content[0].input_audio.data` | `"UklGRg=="` | `"UklGRg=="` | kept | kept |
| chat/msg/user-part-input_audio | messages[].content[](input_audio).input_audio.format | `messages[0].content[0].input_audio.format` | `"wav"` | `"wav"` | kept | kept |
| chat/msg/user-part-file | messages[].content[](file).type | `messages[0].content[0].type` | `"file"` | `"file"` | kept | kept |
| chat/msg/user-part-file | messages[].content[](file).file | `messages[0].content[0].file` | `{"file_data":"JVBERi0=","file_id":"file-s13","filename":"a.pdf"}` | `{"file_data":"JVBERi0=","file_id":"file-s13","filename":"a.pdf"}` | kept | kept |
| chat/msg/user-part-file | messages[].content[](file).file.file_data | `messages[0].content[0].file.file_data` | `"JVBERi0="` | `"JVBERi0="` | kept | kept |
| chat/msg/user-part-file | messages[].content[](file).file.file_id | `messages[0].content[0].file.file_id` | `"file-s13"` | `"file-s13"` | kept | kept |
| chat/msg/user-part-file | messages[].content[](file).file.filename | `messages[0].content[0].file.filename` | `"a.pdf"` | `"a.pdf"` | kept | kept |
| chat/msg/user-part-audio_url | messages[].content[](audio_url).type | `messages[0].content[0].type` | `"audio_url"` | `"audio_url"` | kept | kept |
| chat/msg/user-part-audio_url | messages[].content[](audio_url).audio_url | `messages[0].content[0].audio_url` | `{"url":"https://example.com/a.wav"}` | `{"url":"https://example.com/a.wav"}` | kept | kept |
| chat/msg/user-part-audio_url | messages[].content[](audio_url).audio_url.url | `messages[0].content[0].audio_url.url` | `"https://example.com/a.wav"` | `"https://example.com/a.wav"` | kept | kept |
| chat/msg/user-part-video_url | messages[].content[](video_url).type | `messages[0].content[0].type` | `"video_url"` | `"video_url"` | kept | kept |
| chat/msg/user-part-video_url | messages[].content[](video_url).video_url | `messages[0].content[0].video_url` | `{"url":"https://example.com/a.mp4"}` | `{"url":"https://example.com/a.mp4"}` | kept | kept |
| chat/msg/user-part-video_url | messages[].content[](video_url).video_url.url | `messages[0].content[0].video_url.url` | `"https://example.com/a.mp4"` | `"https://example.com/a.mp4"` | kept | kept |
| chat/msg/user-part-image_embeds | messages[].content[](image_embeds).type | `messages[0].content[0].type` | `"image_embeds"` | `"image_embeds"` | kept | kept |
| chat/msg/user-part-image_embeds | messages[].content[](image_embeds).image_embeds | `messages[0].content[0].image_embeds` | `"AAAA"` | `"AAAA"` | kept | kept |
| chat/msg/user-part-image_embeds | messages[].content[](image_embeds).uuid | `messages[0].content[0].uuid` | `"img-1"` | `"img-1"` | kept | kept |
| chat/msg/user-part-audio_embeds | messages[].content[](audio_embeds).type | `messages[0].content[0].type` | `"audio_embeds"` | `"audio_embeds"` | kept | kept |
| chat/msg/user-part-audio_embeds | messages[].content[](audio_embeds).audio_embeds | `messages[0].content[0].audio_embeds` | `"AAAA"` | `"AAAA"` | kept | kept |
| chat/msg/user-part-audio_embeds | messages[].content[](audio_embeds).uuid | `messages[0].content[0].uuid` | `"aud-1"` | `"aud-1"` | kept | kept |
| chat/msg/user-part-video_embeds | messages[].content[](video_embeds).type | `messages[0].content[0].type` | `"video_embeds"` | `"video_embeds"` | kept | kept |
| chat/msg/user-part-video_embeds | messages[].content[](video_embeds).video_embeds | `messages[0].content[0].video_embeds` | `"AAAA"` | `"AAAA"` | kept | kept |
| chat/msg/user-part-video_embeds | messages[].content[](video_embeds).uuid | `messages[0].content[0].uuid` | `"vid-1"` | `"vid-1"` | kept | kept |
| chat/msg/user-part-prompt_embeds | messages[].content[](prompt_embeds).type | `messages[0].content[0].type` | `"prompt_embeds"` | `"prompt_embeds"` | kept | kept |
| chat/msg/user-part-prompt_embeds | messages[].content[](prompt_embeds).data | `messages[0].content[0].data` | `"AAAA"` | `"AAAA"` | kept | kept |
| chat/msg/user-part-simple-image | messages[].content[](simple image).image_url | `messages[0].content[0].image_url` | `"https://example.com/b.png"` | — | refused | refused |
| chat/msg/user-part-simple-image | messages[].content[](simple image).uuid | `messages[0].content[0].uuid` | `"img-2"` | — | refused | refused |
| chat/msg/user-part-simple-audio | messages[].content[](simple audio).audio_url | `messages[0].content[0].audio_url` | `"https://example.com/b.wav"` | — | refused | refused |
| chat/msg/user-part-simple-video | messages[].content[](simple video).video_url | `messages[0].content[0].video_url` | `"https://example.com/b.mp4"` | — | refused | refused |
| chat/msg/user-part-simple-video | messages[].content[](simple video).uuid | `messages[0].content[0].uuid` | `"vid-2"` | — | refused | refused |
| chat/msg/user-part-tool_reference | messages[].content[](tool_reference).type | `messages[0].content[0].type` | `"tool_reference"` | `"tool_reference"` | kept | kept |
| chat/msg/user-part-tool_reference | messages[].content[](tool_reference).name | `messages[0].content[0].name` | `"get_weather"` | `"get_weather"` | kept | kept |
| chat/msg/user-part-bare-string | ext:messages[].content[](str) | `messages[0].content[0]` | `"hello"` | — | refused | refused |
| chat/msg/assistant-content-name | messages[](assistant).role | `messages[1].role` | `"assistant"` | `"assistant"` | kept | kept |
| chat/msg/assistant-content-name | messages[](assistant).content | `messages[1].content` | `"Hi."` | `"Hi."` | kept | kept |
| chat/msg/assistant-content-name | messages[](assistant).name | `messages[1].name` | `"bot"` | `"bot"` | kept | kept |
| chat/msg/assistant-part-refusal | messages[].content[](refusal).type | `messages[1].content[0].type` | `"refusal"` | `"refusal"` | kept | kept |
| chat/msg/assistant-part-refusal | messages[].content[](refusal).refusal | `messages[1].content[0].refusal` | `"I cannot."` | `"I cannot."` | kept | kept |
| chat/msg/assistant-part-thinking | messages[].content[](thinking).type | `messages[1].content[0].type` | `"thinking"` | `"thinking"` | kept | kept |
| chat/msg/assistant-part-thinking | messages[].content[](thinking).thinking | `messages[1].content[0].thinking` | `"Let me see."` | `"Let me see."` | kept | kept |
| chat/msg/assistant-part-thinking | messages[].content[](thinking).closed | `messages[1].content[0].closed` | `true` | `true` | kept | kept |
| chat/msg/assistant-refusal | messages[](assistant).refusal | `messages[1].refusal` | `"No."` | `"No."` | kept | kept |
| chat/msg/assistant-audio | messages[](assistant).audio | `messages[1].audio` | `{"id":"audio-s13"}` | `{"id":"audio-s13"}` | kept | kept |
| chat/msg/assistant-audio | messages[](assistant).audio.id | `messages[1].audio.id` | `"audio-s13"` | `"audio-s13"` | kept | kept |
| chat/msg/assistant-function_call | messages[](assistant).function_call | `messages[1].function_call` | `{"name":"get_weather","arguments":"{\"city\":\"Krakow\"}"}` | `{"name":"get_weather","arguments":"{\"city\":\"Krakow\"}"}` | kept | kept |
| chat/msg/assistant-function_call | messages[](assistant).function_call.name | `messages[1].function_call.name` | `"get_weather"` | `"get_weather"` | kept | kept |
| chat/msg/assistant-function_call | messages[](assistant).function_call.arguments | `messages[1].function_call.arguments` | `"{\"city\":\"Krakow\"}"` | `"{\"city\":\"Krakow\"}"` | kept | kept |
| chat/msg/assistant-function_call | messages[](function).role | `messages[2].role` | `"function"` | `"function"` | kept | kept |
| chat/msg/assistant-function_call | messages[](function).name | `messages[2].name` | `"get_weather"` | `"get_weather"` | kept | kept |
| chat/msg/assistant-function_call | messages[](function).content | `messages[2].content` | `"sunny"` | `"sunny"` | kept | kept |
| chat/msg/assistant-tool_calls-function | messages[](assistant).tool_calls | `messages[1].tool_calls` | `[{"id":"call_1","type":"function","function":{"name":"get_weather","ar…` | `[{"id":"call_1","type":"function","function":{"name":"get_weather","ar…` | kept | kept |
| chat/msg/assistant-tool_calls-function | messages[](assistant).tool_calls[](function).id | `messages[1].tool_calls[0].id` | `"call_1"` | `"call_1"` | kept | kept |
| chat/msg/assistant-tool_calls-function | messages[](assistant).tool_calls[](function).type | `messages[1].tool_calls[0].type` | `"function"` | `"function"` | kept | kept |
| chat/msg/assistant-tool_calls-function | messages[](assistant).tool_calls[](function).function | `messages[1].tool_calls[0].function` | `{"name":"get_weather","arguments":"{\"city\":\"Krakow\"}"}` | `{"name":"get_weather","arguments":"{\"city\":\"Krakow\"}"}` | kept | kept |
| chat/msg/assistant-tool_calls-function | messages[](assistant).tool_calls[](function).function.name | `messages[1].tool_calls[0].function.name` | `"get_weather"` | `"get_weather"` | kept | kept |
| chat/msg/assistant-tool_calls-function | messages[](assistant).tool_calls[](function).function.arguments | `messages[1].tool_calls[0].function.arguments` | `"{\"city\":\"Krakow\"}"` | `"{\"city\":\"Krakow\"}"` | kept | kept |
| chat/msg/assistant-tool_calls-function | messages[](tool).role | `messages[2].role` | `"tool"` | `"tool"` | kept | kept |
| chat/msg/assistant-tool_calls-function | messages[](tool).tool_call_id | `messages[2].tool_call_id` | `"call_1"` | `"call_1"` | kept | kept |
| chat/msg/assistant-tool_calls-function | messages[](tool).content | `messages[2].content` | `"sunny"` | `"sunny"` | kept | kept |
| chat/msg/assistant-tool_calls-custom | messages[](assistant).tool_calls[](custom).id | `messages[1].tool_calls[0].id` | `"call_2"` | `"call_2"` | kept | kept |
| chat/msg/assistant-tool_calls-custom | messages[](assistant).tool_calls[](custom).type | `messages[1].tool_calls[0].type` | `"custom"` | `"custom"` | kept | kept |
| chat/msg/assistant-tool_calls-custom | messages[](assistant).tool_calls[](custom).custom | `messages[1].tool_calls[0].custom` | `{"name":"run","input":"ls"}` | `{"name":"run","input":"ls"}` | kept | kept |
| chat/msg/assistant-tool_calls-custom | messages[](assistant).tool_calls[](custom).custom.name | `messages[1].tool_calls[0].custom.name` | `"run"` | `"run"` | kept | kept |
| chat/msg/assistant-tool_calls-custom | messages[](assistant).tool_calls[](custom).custom.input | `messages[1].tool_calls[0].custom.input` | `"ls"` | `"ls"` | kept | kept |
| chat/msg/assistant-reasoning | messages[](custom).reasoning | `messages[1].reasoning` | `"The user greets."` | `"The user greets."` | kept | kept |
| chat/msg/tool-array-content | messages[](tool).content | `messages[2].content` | `[{"type":"text","text":"sunny"}]` | `[{"type":"text","text":"sunny"}]` | kept | kept |
| chat/msg/custom-role | messages[](custom).role | `messages[0].role` | `"s13-custom"` | `"s13-custom"` | kept | kept |
| chat/msg/custom-role | messages[](custom).content | `messages[0].content` | `"A note."` | `"A note."` | kept | kept |
| chat/msg/custom-role | messages[](custom).name | `messages[0].name` | `"c1"` | `"c1"` | kept | kept |
| chat/msg/custom-tool_call_id | messages[](custom).tool_call_id | `messages[0].tool_call_id` | `"call_9"` | `"call_9"` | kept | kept |
| chat/msg/custom-tool_calls | messages[](custom).tool_calls | `messages[0].tool_calls` | `[{"id":"call_3","type":"function","function":{"name":"f","arguments":"…` | `[{"id":"call_3","type":"function","function":{"name":"f","arguments":"…` | kept | kept |
| chat/msg/developer-tools | messages[](custom).tools | `messages[0].tools` | `[{"type":"function","function":{"name":"get_weather","parameters":{"ty…` | `[{"type":"function","function":{"name":"get_weather","parameters":{"ty…` | kept | kept |
| chat/msg/user-task | messages[](custom).task | `messages[0].task` | `"s13-task"` | `"s13-task"` | kept | kept |
| chat/msg/user-content-array-for-custom | messages[](custom).content | `messages[0].content` | `[{"type":"text","text":"A note."}]` | `[{"type":"text","text":"A note."}]` | kept | kept |
| completions/frequency_penalty | frequency_penalty | `frequency_penalty` | `0.1` | `0.1` | kept | kept |
| completions/logit_bias | logit_bias | `logit_bias` | `{"50256":-100,"198":2.5}` | `{"50256":-100,"198":2.5}` | kept | kept |
| completions/max_tokens | max_tokens | `max_tokens` | `32` | `32` | kept | kept |
| completions/n | n | `n` | `2` | `2` | kept | kept |
| completions/presence_penalty | presence_penalty | `presence_penalty` | `-0.3` | `-0.3` | kept | kept |
| completions/seed-int64-max | seed | `seed` | `9223372036854775807` | `9223372036854775807` | kept | kept |
| completions/seed-negative-beyond-2^53 | seed | `seed` | `-9007199254740993` | `-9007199254740993` | kept | kept |
| completions/stop-string | stop | `stop` | `"END"` | `"END"` | kept | kept |
| completions/stop-array | stop | `stop` | `["END","STOP"]` | `["END","STOP"]` | kept | kept |
| completions/stream-false | stream | `stream` | `false` | `false` | kept | — |
| completions/stream-true | stream | `stream` | `true` | `true` | — | kept |
| completions/stream_options | stream_options | `stream_options` | `{"include_usage":true,"continuous_usage_stats":true}` | `{"include_usage":true,"continuous_usage_stats":true}` | — | kept |
| completions/stream_options | stream_options.include_usage | `stream_options.include_usage` | `true` | `true` | — | kept |
| completions/stream_options | stream_options.continuous_usage_stats | `stream_options.continuous_usage_stats` | `true` | `true` | — | kept |
| completions/temperature | temperature | `temperature` | `0.3` | `0.3` | kept | kept |
| completions/top_p | top_p | `top_p` | `0.9` | `0.9` | kept | kept |
| completions/user | user | `user` | `"s13-user"` | `"s13-user"` | kept | kept |
| completions/use_beam_search | use_beam_search | `use_beam_search` | `true` | `true` | kept | kept |
| completions/top_k | top_k | `top_k` | `40` | `40` | kept | kept |
| completions/min_p | min_p | `min_p` | `0.05` | `0.05` | kept | kept |
| completions/repetition_penalty | repetition_penalty | `repetition_penalty` | `1.1` | `1.1` | kept | kept |
| completions/watermarking | watermarking | `watermarking` | `true` | `true` | kept | kept |
| completions/length_penalty | length_penalty | `length_penalty` | `0.8` | `0.8` | kept | kept |
| completions/stop_token_ids | stop_token_ids | `stop_token_ids` | `[7,8]` | `[7,8]` | kept | kept |
| completions/include_stop_str_in_output | include_stop_str_in_output | `include_stop_str_in_output` | `true` | `true` | kept | kept |
| completions/ignore_eos | ignore_eos | `ignore_eos` | `true` | `true` | kept | kept |
| completions/min_tokens | min_tokens | `min_tokens` | `3` | `3` | kept | kept |
| completions/skip_special_tokens | skip_special_tokens | `skip_special_tokens` | `false` | `false` | kept | kept |
| completions/spaces_between_special_tokens | spaces_between_special_tokens | `spaces_between_special_tokens` | `false` | `false` | kept | kept |
| completions/truncate_prompt_tokens | truncate_prompt_tokens | `truncate_prompt_tokens` | `512` | `512` | kept | kept |
| completions/truncation_side | truncation_side | `truncation_side` | `"left"` | `"left"` | kept | kept |
| completions/prompt_logprobs | prompt_logprobs | `prompt_logprobs` | `2` | `2` | kept | — |
| completions/allowed_token_ids | allowed_token_ids | `allowed_token_ids` | `[1,2,3]` | `[1,2,3]` | kept | kept |
| completions/bad_words | bad_words | `bad_words` | `["bad"]` | `["bad"]` | kept | kept |
| completions/echo | echo | `echo` | `true` | `true` | kept | kept |
| completions/priority | priority | `priority` | `5` | `5` | kept | kept |
| completions/request_id | request_id | `request_id` | `"s13-request-1"` | `"s13-request-1"` | kept | kept |
| completions/session_id | session_id | `session_id` | `"s13-session"` | `"s13-session"` | kept | kept |
| completions/return_tokens_as_token_ids | return_tokens_as_token_ids | `return_tokens_as_token_ids` | `true` | `true` | kept | kept |
| completions/return_token_ids | return_token_ids | `return_token_ids` | `true` | `true` | kept | kept |
| completions/routed_experts_prompt_start | routed_experts_prompt_start | `routed_experts_prompt_start` | `4` | `4` | kept | kept |
| completions/return_token_offsets | return_token_offsets | `return_token_offsets` | `true` | `true` | kept | kept |
| completions/mm_payload | mm_payload | `mm_payload` | `"none"` | `"none"` | kept | kept |
| completions/return_mm_kwargs | return_mm_kwargs | `return_mm_kwargs` | `true` | `true` | kept | kept |
| completions/cache_salt | cache_salt | `cache_salt` | `"s13-salt"` | `"s13-salt"` | kept | kept |
| completions/kv_transfer_params | kv_transfer_params | `kv_transfer_params` | `{"do_remote_prefill":false,"remote_block_ids":[1,2]}` | `{"do_remote_prefill":false,"remote_block_ids":[1,2]}` | kept | kept |
| completions/ec_transfer_params | ec_transfer_params | `ec_transfer_params` | `{"engine":"e1"}` | `{"engine":"e1"}` | kept | kept |
| completions/vllm_xargs | vllm_xargs | `vllm_xargs` | `{"a":"x","b":3,"c":0.5,"d":["y",4]}` | `{"a":"x","b":3,"c":0.5,"d":["y",4]}` | kept | kept |
| completions/repetition_detection | repetition_detection | `repetition_detection` | `{"max_pattern_size":8,"min_pattern_size":2,"min_count":3}` | `{"max_pattern_size":8,"min_pattern_size":2,"min_count":3}` | kept | kept |
| completions/repetition_detection | repetition_detection.max_pattern_size | `repetition_detection.max_pattern_size` | `8` | `8` | kept | kept |
| completions/repetition_detection | repetition_detection.min_pattern_size | `repetition_detection.min_pattern_size` | `2` | `2` | kept | kept |
| completions/repetition_detection | repetition_detection.min_count | `repetition_detection.min_count` | `3` | `3` | kept | kept |
| completions/thinking_token_budget | thinking_token_budget | `thinking_token_budget` | `256` | `256` | kept | kept |
| completions/stream_interval | stream_interval | `stream_interval` | `4` | `4` | kept | kept |
| completions/add_special_tokens | add_special_tokens | `add_special_tokens` | `false` | `false` | kept | kept |
| completions/response_format-text | response_format | `response_format` | `{"type":"text"}` | `{"type":"text"}` | kept | kept |
| completions/response_format-text | response_format.type | `response_format.type` | `"text"` | `"text"` | kept | kept |
| completions/response_format-json_object | response_format.type | `response_format.type` | `"json_object"` | `"json_object"` | kept | kept |
| completions/response_format-json_schema | response_format.json_schema | `response_format.json_schema` | `{"name":"s13_schema","description":"A schema","schema":{"type":"object…` | `{"name":"s13_schema","description":"A schema","schema":{"type":"object…` | kept | kept |
| completions/response_format-json_schema | response_format.json_schema.name | `response_format.json_schema.name` | `"s13_schema"` | `"s13_schema"` | kept | kept |
| completions/response_format-json_schema | response_format.json_schema.description | `response_format.json_schema.description` | `"A schema"` | `"A schema"` | kept | kept |
| completions/response_format-json_schema | response_format.json_schema.json_schema | `response_format.json_schema.schema` | `{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"…` | `{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"…` | kept | kept |
| completions/response_format-json_schema | response_format.json_schema.strict | `response_format.json_schema.strict` | `true` | `true` | kept | kept |
| completions/response_format-structural_tag | response_format(structural_tag).type | `response_format.type` | `"structural_tag"` | `"structural_tag"` | kept | kept |
| completions/response_format-structural_tag | response_format(structural_tag).format | `response_format.format` | `{"type":"triggered_tags","triggers":["<f>"],"tags":[{"begin":"<f>","co…` | `{"type":"triggered_tags","triggers":["<f>"],"tags":[{"begin":"<f>","co…` | kept | kept |
| completions/response_format-structural_tag-legacy | response_format(structural_tag, legacy).type | `response_format.type` | `"structural_tag"` | `"structural_tag"` | kept | kept |
| completions/response_format-structural_tag-legacy | response_format(structural_tag, legacy).structures | `response_format.structures` | `[{"begin":"<f>","schema":{"type":"object"},"end":"</f>"}]` | `[{"begin":"<f>","schema":{"type":"object"},"end":"</f>"}]` | kept | kept |
| completions/response_format-structural_tag-legacy | response_format(structural_tag, legacy).triggers | `response_format.triggers` | `["<f>"]` | `["<f>"]` | kept | kept |
| completions/response_format-structural_tag-legacy | response_format(structural_tag, legacy).structures[].begin | `response_format.structures[0].begin` | `"<f>"` | `"<f>"` | kept | kept |
| completions/response_format-structural_tag-legacy | response_format(structural_tag, legacy).structures[].structural_tag_schema | `response_format.structures[0].schema` | `{"type":"object"}` | `{"type":"object"}` | kept | kept |
| completions/response_format-structural_tag-legacy | response_format(structural_tag, legacy).structures[].end | `response_format.structures[0].end` | `"</f>"` | `"</f>"` | kept | kept |
| completions/structured_outputs-json | structured_outputs | `structured_outputs` | `{"json":{"type":"object"},"disable_any_whitespace":true,"disable_addit…` | `{"json":{"type":"object"},"disable_any_whitespace":true,"disable_addit…` | kept | kept |
| completions/structured_outputs-json | structured_outputs.json | `structured_outputs.json` | `{"type":"object"}` | `{"type":"object"}` | kept | kept |
| completions/structured_outputs-json | structured_outputs.disable_any_whitespace | `structured_outputs.disable_any_whitespace` | `true` | `true` | kept | kept |
| completions/structured_outputs-json | structured_outputs.disable_additional_properties | `structured_outputs.disable_additional_properties` | `true` | `true` | kept | kept |
| completions/structured_outputs-json | structured_outputs.whitespace_pattern | `structured_outputs.whitespace_pattern` | `"[ ]?"` | `"[ ]?"` | kept | kept |
| completions/structured_outputs-regex | structured_outputs.regex | `structured_outputs.regex` | `"[a-z]+"` | `"[a-z]+"` | kept | kept |
| completions/structured_outputs-choice | structured_outputs.choice | `structured_outputs.choice` | `["yes","no"]` | `["yes","no"]` | kept | kept |
| completions/structured_outputs-grammar | structured_outputs.grammar | `structured_outputs.grammar` | `"root ::= \"yes\" \| \"no\""` | `"root ::= \"yes\" \| \"no\""` | kept | kept |
| completions/structured_outputs-json_object | structured_outputs.json_object | `structured_outputs.json_object` | `true` | `true` | kept | kept |
| completions/structured_outputs-structural_tag | structured_outputs.structural_tag | `structured_outputs.structural_tag` | `"{\"type\":\"structural_tag\",\"format\":{\"type\":\"const_string\",\"…` | `"{\"type\":\"structural_tag\",\"format\":{\"type\":\"const_string\",\"…` | kept | kept |
| completions/logprobs | logprobs | `logprobs` | `3` | `3` | kept | kept |
| completions/logprob_token_ids | logprob_token_ids | `logprob_token_ids` | `[11,12]` | `[11,12]` | kept | kept |
| completions/suffix | suffix | `suffix` | `"s13-suffix"` | `"s13-suffix"` | kept | kept |
| completions/prompt_embeds | prompt_embeds | `prompt_embeds` | `"AAAAAA=="` | `"AAAAAA=="` | kept | kept |
| completions/max_tokens-null | max_tokens | `max_tokens` | `null` | `null` | kept | kept |
| chat/continue_final_message | add_generation_prompt | `add_generation_prompt` | `false` | `false` | kept | kept |
| chat/continue_final_message | continue_final_message | `continue_final_message` | `true` | `true` | kept | kept |
| chat/ext/messages-x_ext | ext:messages[].x_ext | `messages[0].x_ext` | `{"trace":"t-1","n":1}` | `{"trace":"t-1","n":1}` | kept | kept |
| chat/ext/content-part-cache_control | ext:messages[].content[].cache_control | `messages[0].content[0].cache_control` | `{"type":"ephemeral"}` | `{"type":"ephemeral"}` | kept | kept |
| chat/ext/system-part-cache_control | ext:messages[](system).content[].cache_control | `messages[0].content[0].cache_control` | `{"type":"ephemeral"}` | `{"type":"ephemeral"}` | kept | kept |
| chat/ext/assistant-reasoning_content | ext:messages[](assistant).reasoning_content | `messages[1].reasoning_content` | `"The user greets."` | `"The user greets."` | kept | kept |
| chat/msg/harmony | messages[](harmony).author | `messages[0].author` | `{"role":"user","name":"alice"}` | `{"role":"user","name":"alice"}` | kept | kept |
| chat/msg/harmony | messages[](harmony).author.role | `messages[0].author.role` | `"user"` | `"user"` | kept | kept |
| chat/msg/harmony | messages[](harmony).author.name | `messages[0].author.name` | `"alice"` | `"alice"` | kept | kept |
| chat/msg/harmony | messages[](harmony).content | `messages[0].content` | `[{"type":"text","text":"hello"}]` | `[{"type":"text","text":"hello"}]` | kept | kept |
| chat/msg/harmony | messages[](harmony).channel | `messages[0].channel` | `"final"` | `"final"` | kept | kept |
| chat/msg/harmony | messages[](harmony).recipient | `messages[0].recipient` | `"all"` | `"all"` | kept | kept |
| chat/msg/harmony | messages[](harmony).content_type | `messages[0].content_type` | `"text"` | `"text"` | kept | kept |
| chat/ext/tools-function-x_ext | ext:tools[].function.x_ext | `tools[0].function.x_ext` | `{"a":1}` | `{"a":1}` | kept | kept |
| chat/ext/tools-x_ext | ext:tools[].x_ext | `tools[0].x_ext` | `{"a":1}` | `{"a":1}` | kept | kept |
| chat/ext/tool_calls-function-x_ext | ext:messages[].tool_calls[].function.x_ext | `messages[1].tool_calls[0].function.x_ext` | `1` | `1` | kept | kept |
| chat/ext/tool_choice-x_ext | ext:tool_choice.x_ext | `tool_choice.x_ext` | `1` | `1` | kept | kept |
| chat/ext/response_format-json_schema-x_ext | ext:response_format.json_schema.x_ext | `response_format.json_schema.x_ext` | `1` | `1` | kept | kept |
| chat/ext/stream_options-x_ext | ext:stream_options.x_ext | `stream_options.x_ext` | `1` | `1` | — | kept |
| chat/ext/structured_outputs-x_ext | ext:structured_outputs.x_ext | `structured_outputs.x_ext` | `1` | `1` | kept | kept |
| chat/ext/top-level-object | ext:x_top | `x_top` | `{"a":[1,2.50,"s"],"b":null}` | `{"a":[1,2.50,"s"],"b":null}` | kept | kept |
| completions/ext/top-level-object | ext:x_top | `x_top` | `{"a":[1,2.50,"s"],"b":null}` | `{"a":[1,2.50,"s"],"b":null}` | kept | kept |
| chat/ext/extra_body-literal | ext:extra_body | `extra_body` | `{"top_k":5}` | `{"top_k":5}` | kept | kept |
| chat/ext/guided_json | ext:guided_json | `guided_json` | `{"type":"object"}` | `{"type":"object"}` | kept | kept |
| chat/ext/openai-sdk-fields | ext:store | `store` | `false` | `false` | kept | kept |
| chat/ext/openai-sdk-fields | ext:metadata | `metadata` | `{"k":"v"}` | `{"k":"v"}` | kept | kept |
| chat/ext/openai-sdk-fields | ext:prompt_cache_key | `prompt_cache_key` | `"pc-1"` | `"pc-1"` | kept | kept |
| chat/ext/openai-sdk-fields | ext:service_tier | `service_tier` | `"auto"` | `"auto"` | kept | kept |
| chat/ext/openai-sdk-fields | ext:verbosity | `verbosity` | `"low"` | `"low"` | kept | kept |
| chat/ext/openai-sdk-fields | ext:modalities | `modalities` | `["text"]` | `["text"]` | kept | kept |
| chat/ext/session_params | ext:session_params | `session_params` | `{"session_id":"s13-session"}` | `{"session_id":"s13-session"}` | kept | kept |
| chat/enc/pretty-printed-reordered | ext:whitespace and member order | `temperature` | `0.3` | `0.3` | kept | kept |
| chat/enc/temperature-integer | ext:temperature as an integer literal | `temperature` | `1` | `1` | kept | kept |
| completions/enc/temperature-integer | ext:temperature as an integer literal | `temperature` | `1` | `1` | kept | kept |
| chat/enc/top_p-exponent | ext:top_p in exponent form | `top_p` | `9e-1` | `9e-1` | kept | kept |
| chat/enc/temperature-20-digits | ext:a float beyond float64 precision | `temperature` | `0.12345678901234567890` | `0.12345678901234567890` | kept | kept |
| chat/enc/repetition_penalty-1.0 | ext:a float with a zero fraction | `repetition_penalty` | `1.0` | `1.0` | kept | kept |
| chat/enc/kwargs-numbers | ext:numbers inside a dict[str, Any] | `chat_template_kwargs` | `{"big":123456789012345678901234567890,"u64":18446744073709551615,"i":1…` | `{"big":123456789012345678901234567890,"u64":18446744073709551615,"i":1…` | kept | kept |
| chat/enc/explicit-nulls | ext:max_tokens: null | `max_tokens` | `null` | `null` | kept | kept |
| chat/enc/explicit-nulls | ext:temperature: null | `temperature` | `null` | `null` | kept | kept |
| chat/enc/explicit-nulls | ext:stop: null | `stop` | `null` | `null` | kept | kept |
| chat/enc/explicit-nulls | ext:user: null | `user` | `null` | `null` | kept | kept |
| chat/enc/explicit-nulls | ext:seed: null | `seed` | `null` | `null` | kept | kept |
| chat/enc/explicit-nulls | ext:tools: null | `tools` | `null` | `null` | kept | kept |
| chat/enc/message-name-null (invalid for vLLM) | ext:messages[].name: null | `messages[0].name` | `null` | `null` | kept | kept |
| chat/enc/duplicate-known-field | ext:a field written twice | `temperature` | `0.3` | `0.3` | kept | kept |
| chat/enc/duplicate-in-dict | ext:a member written twice inside a dict[str, Any] | `chat_template_kwargs` | `{"a":1,"a":2}` | `{"a":1,"a":2}` | kept | kept |
| chat/enc/duplicate-unknown-top | ext:an unknown top-level member written twice | `x_dup` | `2` | `2` | kept | kept |
| chat/enc/key-escaped-top | ext:a top-level key written with an escape | `temperature` | `0.3` | `0.3` | kept | kept |
| chat/enc/key-escaped-nested | ext:a key inside a dict[str, Any] written with an escape | `chat_template_kwargs.a` | `1` | `1` | kept | kept |
| chat/enc/string-escapes | ext:string escapes (an escaped e-acute, a surrogate pair, an escaped slash, a control character) | `messages[0].content` | `"caf\u00e9 \ud83d\ude00 \/ <&> \u0001 \t"` | `"caf\u00e9 \ud83d\ude00 \/ <&> \u0001 \t"` | kept | kept |
| completions/enc/string-escapes | ext:string escapes (an escaped e-acute, a surrogate pair, an escaped slash, a control character) | `prompt` | `"caf\u00e9 \ud83d\ude00 \/ <&> \u0001 \t"` | `"caf\u00e9 \ud83d\ude00 \/ <&> \u0001 \t"` | kept | kept |
| chat/real/agent-turn | ext:agent turn: tools[].function.strict | `tools[0].function.strict` | `true` | `true` | — | kept |
| chat/real/agent-turn | ext:agent turn: temperature 0 | `temperature` | `0` | `0` | — | kept |
| chat/real/agent-turn | ext:agent turn: system string content | `messages[0].content` | `"You are a coding agent."` | `"You are a coding agent."` | — | kept |
| chat/real/agent-turn | ext:agent turn: user content parts | `messages[1].content` | `[{"type":"text","text":"Read a.txt."}]` | `[{"type":"text","text":"Read a.txt."}]` | — | kept |

## Injected values

Values present at the worker and absent from the body sent, by path and value.

| path | value | variants | first case |
|---|---|---|---|

## Other differences

Diffs outside every covered field and not injected.

| variant | path | verdict | sent | received |
|---|---|---|---|---|

## Not delivered

| variant | status | the router's reply |
|---|---|---|
| chat/msg/user-part-simple-image#plain | 400 | `{"error":{"message":"error extracting request body: invalid chat completions request: must have valid messages field","type":"invalid_request_error","code":400}}` |
| chat/msg/user-part-simple-image#stream | 400 | `{"error":{"message":"error extracting request body: invalid chat completions request: must have valid messages field","type":"invalid_request_error","code":400}}` |
| chat/msg/user-part-simple-audio#plain | 400 | `{"error":{"message":"error extracting request body: invalid chat completions request: must have valid messages field","type":"invalid_request_error","code":400}}` |
| chat/msg/user-part-simple-audio#stream | 400 | `{"error":{"message":"error extracting request body: invalid chat completions request: must have valid messages field","type":"invalid_request_error","code":400}}` |
| chat/msg/user-part-simple-video#plain | 400 | `{"error":{"message":"error extracting request body: invalid chat completions request: must have valid messages field","type":"invalid_request_error","code":400}}` |
| chat/msg/user-part-simple-video#stream | 400 | `{"error":{"message":"error extracting request body: invalid chat completions request: must have valid messages field","type":"invalid_request_error","code":400}}` |
| chat/msg/user-part-bare-string#plain | 400 | `{"error":{"message":"error extracting request body: invalid chat completions request: must have valid messages field","type":"invalid_request_error","code":400}}` |
| chat/msg/user-part-bare-string#stream | 400 | `{"error":{"message":"error extracting request body: invalid chat completions request: must have valid messages field","type":"invalid_request_error","code":400}}` |
