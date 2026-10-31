// Package enginedouble is a socket-level double of the vLLM v0.32.0 engine and
// of the two programs the allocation agent starts around it, Apptainer and ssh.
// It is test support: only other packages' _test.go files import it, and no
// non-test code may (a test in this package reads the module's sources and
// fails if one does). It imports only the standard library and package
// agentapi, because the agent's tests link it into their own binary.
//
// # The engine
//
// Handler serves the HTTP subset the agent reads: /health, /version,
// /v1/models, /tokenize, /v1/chat/completions (streamed and not) and
// /metrics. Each behaviour follows the vLLM v0.32.0 source named in the
// comment of the code that implements it. Where the double is deliberately
// simpler than vLLM, this documentation says so.
//
// The tokenizer is deterministic. A token is a whitespace-separated word, and
// its id is the FNV-1a 32-bit hash of the word, modulo 100000, plus 1. The
// chat template renders each message as "<|role|> " + content + " <|assistant|>"
// and, with add_generation_prompt, appends a fixed generation prompt of
// TemplateOverhead-2 words. So one empty user message costs exactly
// TemplateOverhead tokens, and a one-message conversation costs
// TemplateOverhead + the content's words. Generated tokens come from a fixed
// vocabulary of single-word tokens with distinct ids.
//
// Generation follows the prompt's token ids. A greedy request (temperature 0
// or absent) gives the same output every time, as a walk over the vocabulary
// whose odd step varies from token to token: no output repeats one token
// next to itself, and no output is periodic. A sampled request seeds the walk
// at random. With min_tokens at or above max_tokens, an output has exactly
// max_tokens tokens and finish reason "length". Otherwise it stops naturally
// after max(NaturalLen, min_tokens) tokens, or at max_tokens if that comes
// first. ignore_eos does not change that: vLLM merges the server's default
// stop-token ids into every request, so only min_tokens guarantees a length
// (the early-stop mode sets NaturalLen). min_tokens above max_tokens is a 400,
// as in vLLM (sampling_params.py:694-701). The double does not simulate
// preemption or a prefill phase: a request waits TokenDelay before each token.
//
// # Reasoning and tool calls
//
// A model that reasons (reasoning-first=<n>, reasoning-forever) says n
// reasoning tokens before its answer, or reasons until max_tokens runs out
// with no answer and the finish reason "length". The reasoning is
// message.reasoning, and delta.reasoning in a stream, as vLLM v0.32.0 names
// them (chat_completion/protocol.py:69-79, generate/base/protocol.py:356-360);
// usage.completion_tokens counts the reasoning tokens. The completion is one
// walk over the vocabulary whose first tokens are the reasoning, so a greedy
// request's token ids start the same with or without the modes.
//
// A tool call is streamed in vLLM's delta form: a delta with the call's index,
// id, type and function name, then one delta per token of the arguments with
// the index and "arguments" only, the finish reason "tool_calls" on the last
// chunk. A call that max_tokens cuts off carries part of its arguments and the
// finish reason "length".
//
// # Modes
//
// ParseModes reads the comma-separated value of the environment variable
// HB_ENGINE_DOUBLE, for example "kv-low,slow-first=200ms". The modes are kv-low,
// greedy-diverge=<n>, greedy-repeat, early-stop=<n>, loop-above=<tokens>, tool-malformed,
// no-tool-call, reports-nothing, report-max-model-len=<n>, health-fail,
// errors-only, slow-first=<dur>, never-settle=<pct>, stall, token-delay=<dur>,
// reasoning-first=<n> and reasoning-forever;
// the startup modes, used by Main only, are start-delay=<dur>,
// exit-after=<dur>[:code] and ignore-term.
//
// # The executables
//
// A test binary becomes the fake programs by calling Dispatch first thing in
// TestMain. Install(dir) makes dir/apptainer and dir/ssh symlinks to the
// running test binary; Dispatch reads filepath.Base(os.Args[0]) and the
// environment variable HB_TEST_ROLE ("engine" for Main, "engine-child" for
// the second process of the ignore-term mode) to decide which program it is.
//
// ApptainerMain handles pull, build --sandbox and exec. Its exec runs the
// image's "vllm" command as the engine double (Main) in a separate process,
// after rewriting each container path that a bind covers to the host path. It
// honours --cleanenv and the APPTAINERENV_ and SINGULARITYENV_ prefixes, so
// under --cleanenv the engine's HB_ENGINE_DOUBLE and HB_ENGINE_DOUBLE_LOG
// reach it only as APPTAINERENV_HB_ENGINE_DOUBLE and
// APPTAINERENV_HB_ENGINE_DOUBLE_LOG. After the rewrite, /v1/models reports the
// host checkpoint path as root, where vLLM would report /hpcburst/model.
// Variables of HB_FAKE_APPTAINER_LOG, HB_FAKE_SSH_LOG and HB_ENGINE_DOUBLE_LOG
// name files that record each invocation as one JSON line; a variable whose
// name ends in KEY or TOKEN is recorded as its sha256, never in clear.
//
// SSHMain validates the options a dial-home forward must carry and forwards a
// local Unix socket to a TCP address, as ssh -N -L does.
package enginedouble
