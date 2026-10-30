# Spike S2, phase A: the router inside an Instance

Status: phase A (simulated engines, no cluster). Phase B (real vLLM on one GH200 job) is not done.
Revised after the cross-model review of PR #44; "Changes after the review" lists what moved.

## Question

HBEP-5 and HBEP-3 put one router in front of each Instance's workers. Candidates: the llm-d endpoint
picker (EPP) with Envoy in its file-discovery mode, vllm-router, or no router (one worker). Phase A asks,
for each router, with engines replaced by a simulator that streams one token per second:

1. Does a 2 h stream survive? Which deadline sits on the path, at which default, and what must be set?
2. Does removing a worker from discovery cut a stream already in flight on it? Do new requests then
   avoid it?
3. Does `x-session-id` keep the turns of one session on one worker, while sessions spread over workers?
   (Placement only; hit rates need real vLLM.)

## Result in brief

| Question | llm-d (EPP + Envoy 1.33.2) | vllm-router 0.1.16 | No router |
|---|---|---|---|
| 2 h stream, deadlines as shipped | **pass**: 7,300 tokens in 7,306 s | **cut at 1,800.0 s** after 1,794 tokens (read: unexpected EOF) | **pass**: 7,300 tokens in 7,325 s |
| 2 h stream, deadlines set | **pass**: 7,300 tokens in 7,306 s | **pass**: 7,300 tokens in 7,324 s | |
| Removing a worker cuts the stream in flight on it | **no** (2,100-token stream, 2,086 s after the removal) | **no** (2,100-token stream, 2,086 s after the removal) | not applicable |
| New requests avoid the removed worker | yes, at the first probe (30 s after the removal) and at +9 min | yes, at the first probe (30 s after the removal) and at +9 min | not applicable |
| `x-session-id` keeps a session on one worker | yes, with the session scorer (Alpha); the shipped scorers keep multi-turn sessions together by prefix and ignore the header | yes (`consistent_hash`); a chat request with none of its routing keys hashes to one worker, like all the others | not applicable |
| Sessions spread over workers | yes, 3/3/3 of 9 | uneven: 3/2/4, 2/3/4 and, in a first pass, 1/1/7 of 9 | not applicable |

The one deadline the evaluation flagged, llm-d's 1,000 s `ext_proc` message timeout, did not cut any stream
here: the Envoy source starts no such timer in `FULL_DUPLEX_STREAMED` mode, which llm-d's config uses, and the
2 h and 1,100 s first-token runs passed under it. That is source evidence plus passing runs; no experiment
here stalled an outstanding `ext_proc` exchange, so the timer's inertness under a stall is not shown by a run.
The one deadline that does cut is vllm-router's `--request-timeout-secs`, default 1,800 s, silently. Both routers pass the three
phase-A criteria when configured; the choice between them rests on phase B (hit rate, load balance) and on
the operational findings below (two processes and an Alpha plugin against one process and a skewed hash).

## Method

- **Engines.** `llm-d-inference-sim` v1.0.0, three per router stack (one for "no router"), on loopback.
  Flags: `--inter-token-latency 1s --time-to-first-token 1s --max-model-len 16384 --max-num-seqs 64`.
  A request sets `min_tokens == max_tokens` and `ignore_eos`, which pins the reply to exactly that many
  tokens. The default context window is 1,024 tokens: without `--max-model-len` a request for 7,300
  tokens would end after about 1,000 s with `finish_reason=length` and look complete.
- **Client.** `cmd/s2probe` (Go standard library only). No client-side deadline of any kind. The body is
  read as Server-Sent Events: an event counts only once its terminating blank line has arrived, as in the
  HTML Standard's event stream interpretation, and a body that ends inside an event is a cut. A stream
  counts as complete only if a `finish_reason` chunk and a complete `[DONE]` event both arrive, the
  content-chunk count equals the requested token count and no event is an error payload, an `event: error`,
  malformed JSON or follows `[DONE]`; a clean EOF without them, or `unexpected EOF`, is a cut. Counting one
  content chunk per token is valid for this simulator, which sends one token per chunk.
  Every content chunk's arrival time and inter-token gap is written to a CSV (`<label>.gaps.csv`).
  The runs recorded below were made with the earlier line-by-line reader, which would have accepted a
  body cut right after `data: [DONE]`. The current client reads the same simulator streams through each of
  the three stacks (no router, vllm-router, Envoy and EPP) as complete (`results/review-fixes/smoke-parser/`),
  so these streams do end with the blank line and the earlier verdicts stand; the long runs were not repeated.
- **Placement** is read from each simulator's own `/metrics` (`vllm:request_success_total`), one request
  at a time, never from the router. Session turns are interleaved across sessions in a fixed-seed
  random order per turn, so neither "the last worker used" nor a rotation that matches the session
  order can produce stickiness by accident.
- **Removal.** A session is bound to worker X by two requests that must land on the same worker. A
  2,100-token stream (35 min) then starts on the same session, so it runs on X. After 20 s, X is removed
  from discovery (EPP: atomic rewrite of the endpoints file; vllm-router: `POST /remove_worker`). The
  removal counts as effective only if the same session then lands on another worker, and 8 fresh
  sessions avoid X, at five probes. Only then is the survival of the stream judged. The
  session is the lever on purpose: a router that still held X would send the session back to X, whereas
  fresh sessions alone would avoid a busy X even before the removal.
  The first probe comes 30 s after the removal command returns (a settle time), and the later ones are
  planned for +2, +5 and +9 min. The recorded removal logs label the first probe "+0s" and the others with
  their planned offsets; they carry no measured times. The client now prints the measured offset of every
  probe. A removal run is a pass only if the stream lasted at least 1,800 s in total and kept running at least
  1,800 s after the removal (the issue: a stream shorter than about 30 min proves nothing), and its
  default is 2,100 tokens (35 min).
- **Controls.** A positive control, so that a pass can be told from a client that cannot see a cut:
  vllm-router with a 60 s deadline must cut a 150 s stream at about 60 s. A smoke check, which is **not** a
  control: llm-d with 5 s `ext_proc` timers streams 600 s. It cannot tell an armed timer from an inert one:
  messages answered promptly never wait 5 s, so a 600 s stream survives an armed per-message timer too. The
  `ext_proc` timer claims therefore rest on the Envoy source (below), not on that run. A discriminating
  test would stall an outstanding `ext_proc` exchange past 5 s under the 5 s configuration and repeat it
  where the timer is known to be armed; it was not run.
- **Minimum durations.** `run.sh` accepts a pass only if the stream lasted long enough to meet the deadlines
  under test: 7,200 s for the 2 h arms (`LONG_MIN_SEC`), a first byte after 1,000 s for the slow-first-token
  runs, 1,800 s for the removal runs. A shortened smoke run has to lower these, and is then not acceptance evidence.
- **Shared host.** Everything ran at `nice -n 10` on a 6-core VM shared with other work. Clients, Envoy and
  vllm-router listened on 127.0.0.1; the simulators and EPPs did so only from the patched builds on (see Incidents).

## Components and provenance

| Component | Project and URL | Licence | Version used |
|---|---|---|---|
| EPP (endpoint picker), file discovery | llm-d router, https://github.com/llm-d/llm-d-router | Apache-2.0 | v1.0.0, commit b9393d1, built from source (`CGO_ENABLED=0`) |
| Envoy | https://github.com/envoyproxy/envoy | Apache-2.0 | 1.33.2 release binary, the version llm-d's no-Kubernetes guide pins (`distroless-v1.33.2`) |
| llm-d guides (Envoy and EPP configs) | https://github.com/llm-d/llm-d, `guides/no-kubernetes-deployment` | Apache-2.0 | v1.0.0, commit 378b42d; read at run time, not copied into this repository |
| Engine double | llm-d-inference-sim, https://github.com/llm-d/llm-d-inference-sim | Apache-2.0 | v1.0.0, commit 7bda03f, built from source (`CGO_ENABLED=0`) |
| vllm-router | https://github.com/vllm-project/router | Apache-2.0 | 0.1.16 from PyPI (PyO3 wheel plus Python launcher), source commit db02f35 |
| Python launcher dependencies | aiohttp 3.14.5 (Apache-2.0 AND MIT), fastapi 0.162.0 (MIT), uvicorn 0.54.0 (BSD-3-Clause), orjson 3.13.0 (MPL-2.0 AND (Apache-2.0 OR MIT)), requests 2.34.2 (Apache-2.0), setproctitle 1.3.8 (BSD-3-Clause); PyPI | as listed | Only the vllm-router CLI wrapper imports them; Python 3.15.0 |

Reuse: all components are community software used as released, except for two small patches that make
the simulator and the EPP listen on 127.0.0.1 only (see Incidents). The patches, upstream's licence and a
note on their origin are under `third_party/forked/llm-d-inference-sim/` and `third_party/forked/llm-d-router/`;
`scripts/setup.sh` applies them and rebuilds a binary unless its build stamp (source commit, patch file and
patched tree) matches. Each patched upstream file carries a modification notice (HBEP-15). New in this spike: the test client, the stack scripts and the config
generator, all under `spikes/s2/`. The Envoy and EPP configs are generated at run time from llm-d's shipped
files by exact-line substitution (`scripts/genconf.py`), which fails if a line it expects has changed; none
of llm-d's files is copied into this repository.

## Environment

Single VM, 6 cores, 31 GB, no swap, shared with other sessions. Python 3.15.0 for the vllm-router launcher;
Go 1.27.3. Load average: spot checks of `uptime` between 08:48 and 09:17 on the run day read 3.3 to 5.1
(1 minute); from 09:48 to 11:05 it was sampled every minute: 1.3 to 8.2, mean 3.6. Available memory never fell
below 17 GB in the sampled period. The simulators and routers ran at `nice -n 10`. The SIGTERM-only run and the
parser smoke streams (review fixes) ran between 11:37 and 11:42 on the run day, with a 1-minute load of 2.9 to 3.1 in spot checks of `uptime`.

## Deadlines on the path

Paths are in the pinned sources: `guide` is `llm-d/guides/no-kubernetes-deployment/router/envoy/envoy.yaml`
(llm-d v1.0.0); `epp` is llm-d-router v1.0.0; `vr` is vllm-router v0.1.16. Envoy defaults come from its
API protos at v1.33.2 (`api/envoy/...`).

### llm-d: Envoy 1.33.2 and the EPP

| Deadline | Shipped value | Where | Effect on a long stream |
|---|---|---|---|
| Route total timeout | 86,400 s | guide:48 (`route.timeout`) | A total deadline: cuts at 24 h. Envoy's own default if unset is 15 s (`route_components.proto:1225`). |
| Route idle timeout (stream idle) | 86,400 s | guide:49 (`route.idle_timeout`) | Resets on every header or data event; a 1 token/s stream never trips it. If the line were absent, the connection manager's `stream_idle_timeout` would apply, default 5 min (`http_connection_manager.proto:498`). |
| `ext_proc` `message_timeout` | 1,000 s | guide:72 | **Not started under the shipped config, by the source.** Envoy does not start the timer when the body mode is `FULL_DUPLEX_STREAMED` (`processor_state.cc:26-27` in 1.33.2, `:33-34` in 1.40.2), and guide:68-69 sets both body modes to it; the suppression follows each direction's body mode (with its header and trailer callbacks), not SSE as such. The runs (5 s timers on a 600 s stream; a 1,100 s first-token wait) are consistent with that but would also pass with an armed timer and promptly answered messages, so they do not confirm it. Envoy rejects values above 1 h at start-up (`MessageTimeout: value must be inside range [0s, 1h0m0s]`). |
| `ext_proc` `grpc_service.timeout` | 10 s | guide:64 | Not applied to the `ext_proc` stream, by the source: the stream options set no timeout (`ext_proc.cc:399-403`). The 5 s run is consistent with it and does not discriminate. |
| `ext_proc` `failure_mode_allow` | false (unset) | guide, absent | Any error on the `ext_proc` stream, including a dead EPP, ends the client's request. See the EPP stop results (Section 5). |
| Cluster `connect_timeout` | 1,000 s (to workers), 86,400 s (to EPP) | guide:92, guide:105 | Connection set-up only. |
| EPP health check | 2 s timeout, 10 s interval, 3 failures | guide:115-122 | Marks the EPP unhealthy; affects routing to the EPP only. |
| Connection manager `request_timeout`, `request_headers_timeout`, `max_stream_duration`, `max_connection_duration` | unset, all disabled | `http_connection_manager.proto:543`, `:549`, `protocol.proto:263`, `:292` | None. |
| Connection `idle_timeout` | 1 h (unset) | `protocol.proto:247` | Applies only to connections with no active stream. |
| `ORIGINAL_DST` `cleanup_interval` | 5 s (unset) | `cluster.proto:1089` | Hosts not selected for 5 s are dropped from the cluster; relevant to removal, and it did not cut the in-flight stream. |
| EPP gRPC server, options set by the EPP | none of keep-alive, connection age, idle or stream timeout | `epp: internal/runnable/grpc.go`, `pkg/epp/server/runserver.go:262-273` | The EPP passes no timeout option, so grpc-go's inherited defaults apply (next three rows). |
| EPP gRPC server, inherited keep-alive (grpc-go v1.84.0, the version in `epp: go.mod:51`) | ping after 2 h without a read on the connection; the ack must arrive within 20 s, else the connection is closed | `grpc-go: internal/transport/defaults.go:38-39`, applied at `internal/transport/http2_server.go:238-243`; the timer restarts on any read (`:1253-1259`) | A **connection-liveness** limit, not a stream deadline: it fires only on a connection that has read nothing for 2 h and whose peer does not answer a ping. Envoy answers pings, and a stream that moves a chunk a second keeps the timer from firing. It does not contradict the 2 h runs. Max connection idle, age and age grace default to infinity (`defaults.go:35-37`). |
| EPP gRPC server, inherited `TCP_USER_TIMEOUT` | 20 s (the keep-alive timeout, set whenever keep-alive time is finite) | `grpc-go: internal/transport/http2_server.go:244-247` | Closes a connection whose sent data stays unacknowledged at the TCP level for 20 s. A liveness limit; not exercised here (the 90 s freeze below stops the EPP, which sends nothing meanwhile). |
| EPP gRPC server, inherited connection-establishment timeout | 120 s | `grpc-go: server.go:192`, applied to the handshake at `:1005-1009` and cleared after it | A **set-up** limit on the HTTP/2 handshake of a new connection; no effect on an established stream. |
| EPP session binding | 300 s idle TTL (`evictionTtlSeconds`, configurable), swept every 10 s | `epp: pkg/epp/framework/plugins/scheduling/util/sessionaffinity/session_id.go:48,69-70` | A session idle for more than the TTL (5 min by default) between requests loses its binding. Applies to the session scorer only. |
| EPP utilization filter | queue depth 5, KV use 0.8, metrics older than 200 ms are stale | `epp: pkg/epp/framework/plugins/flowcontrol/saturationdetector/utilization/config.go:30-36` | Not a stream deadline: workers over a threshold, or with stale metrics, are dropped from the candidates, and a bound session then migrates for good. |
| Simulator (fasthttp) | none | `llm-d-inference-sim: pkg/communication/http.go:115-120` | None. |

**Set to pass 2 h.** The shipped values already pass: nothing on the path is below the stream length. The
`tuned` stack lowers them to explicit margins anyway: route timeout and idle timeout 10,800 s (3 h),
`message_timeout` 3,600 s (the largest Envoy accepts), everything else as shipped.

### vllm-router 0.1.16

| Deadline | Default | Where | Effect on a long stream |
|---|---|---|---|
| `--request-timeout-secs` | **1,800 s** | `vr: src/main.rs:279-281`; the Python launcher's default is also 1,800 (`py_src/vllm_router/router_args.py:68`) while the docstring says 600 (`py_src/vllm_router/router.py:83`) | Becomes the total timeout of the router's HTTP client (`src/server.rs:926`), which covers the streamed body. It cuts any request at 1,800 s, with no log line. |
| Connect timeout | 10 s | `vr: src/server.rs:927` | Set-up only. |
| Idle connection pool | 50 s | `vr: src/server.rs:924` | See the keep-alive note under phase B. |
| TCP keep-alive | 30 s | `vr: src/server.rs:929` | None. |
| Request queue wait | 60 s | `vr: src/main.rs:565`, `src/server.rs:1031` | Applies when `--max-concurrent-requests` (32,768) is reached. |
| Health check | 5 s timeout, 60 s interval, 3 failures to mark unhealthy | `vr: src/main.rs:340-352` | A worker that fails three checks leaves the pool. |
| Circuit breaker | 10 failures, 60 s open, 120 s window | `vr: src/main.rs:319-330` | Pauses a worker. |
| Retries | 5 | `vr: src/main.rs:294` | Not read further. Observed: the stream cut at 1,800 s was not replayed on another worker. |
| Worker admin client | 30 s | `vr: src/core/worker.rs:13` | Health and load calls, not inference. |

**Set to pass 2 h.** `--request-timeout-secs 10800`. Nothing else on the path is a total or idle deadline.

### No router

The simulator has no deadline (above). A real vLLM server behind no router would add uvicorn's own
settings; phase B reads them.

## Results

### 1. Two-hour streams

Each stream asks for 7,300 tokens at 1 token/s (2 h 1 m 40 s plus 1 s to the first token), one stream per
stack, all five concurrently. Every inter-token gap is in `<stack>.gaps.csv`.

| Stack and deadlines | Result | Tokens | Elapsed | Inter-token gap min / p50 / p99 / max | Gaps over 2 s | Error |
|---|---|---|---|---|---|---|
| none (simulator directly) | **complete** | 7,300 / 7,300 | 7,325 s | 995 / 1,004 / 1,007 / 1,032 ms | 0 | none |
| llm-d as shipped (route 86,400 s, message timeout 1,000 s) | **complete** | 7,300 / 7,300 | 7,306 s | 983 / 1,001 / 1,007 / 1,044 ms | 0 | none |
| llm-d tuned (route and idle 10,800 s, message timeout 3,600 s) | **complete** | 7,300 / 7,300 | 7,306 s | 978 / 1,001 / 1,008 / 1,044 ms | 0 | none |
| vllm-router default (`--request-timeout-secs` 1,800) | **cut** | 1,794 / 7,300 | 1,800 s | 988 / 1,004 / 1,009 / 1,017 ms | 0 | read: unexpected EOF |
| vllm-router `--request-timeout-secs 10800` | **complete** | 7,300 / 7,300 | 7,324 s | 986 / 1,004 / 1,009 / 1,024 ms | 0 | none |

### 2. Slow first token

The simulator waits 1,100 s before it sends anything, past llm-d's 1,000 s message timeout: the client
received no response header and no byte until second 1,100. It then streams 30 tokens. Through llm-d as shipped: complete, 30/30 tokens, 1,129 s. Through vllm-router with default
deadlines (1,800 s total): complete, 30/30, 1,129 s. Neither router has an idle or no-first-byte
deadline below the total deadline.

### 3. Removal of a worker from discovery

A 2,100-token stream (35 min) runs on worker X, which is removed 20 s later. The removal is proven first
(same session now on another worker, fresh sessions off X), at +30 s, +2, +5 and +9 min; then the stream is judged.

| Router | How X is removed | Proof the removal worked | Stream on X | Result |
|---|---|---|---|---|
| llm-d, session scorer | atomic rewrite of the endpoints file; the EPP logs `endpoints file changed, reloading` the same second | at every probe the bound session went to another worker; 0 of 8 fresh sessions on X (they all share one prompt prefix, so they followed it to a single other worker) | **completed**, 2,100/2,100 tokens, 2,086 s after the removal, largest gap 1,027 ms | pass |
| vllm-router, `consistent_hash` | `POST /remove_worker?url=...` | same probes, 0 of 8 on X each time | **completed**, 2,100/2,100 tokens, 2,086 s after the removal, largest gap 1,017 ms | pass |

An earlier pair of runs with a 1,500-token (25 min) stream passed the same way; they ran on the first
builds of the simulator and EPP and are superseded by these, which are longer than the 1,800 s
that cut streams in the field.

Envoy's `ORIGINAL_DST` cluster drops a host that is not selected for 5 s (`cleanup_interval`), yet the stream
on the removed worker was not cut: its connection stays until the request ends.

### 4. Placement (`x-session-id`)

Nine sessions of five turns, turns interleaved across sessions in a random order, three workers. "Sticky"
means all five turns of a session landed on one worker. Placement is read from each simulator's own counter.

| Router and config | Header | Prompts | Every session sticky | Sessions per worker |
|---|---|---|---|---|
| llm-d, shipped scorers plus `session-affinity-scorer` (weight 10) | `x-session-id` | growing history | yes | 3/3/3 |
| same | `x-session-id` | unrelated prompt every turn | **yes**: only the header can explain it | 3/3/3 |
| llm-d, shipped scorers only | `x-session-id` | growing history | yes (prefix scorer) | 3/3/3 |
| same | `x-session-id` | unrelated prompt every turn | **no**: the header is not used | 3/3/3 by first turn, then scattered |
| same | none | growing history | yes (prefix scorer) | 3/3/3 |
| vllm-router `consistent_hash` | `x-session-id` | growing history | yes | 3/2/4 (another run: 1/1/7) |
| same | `x-session-id` | unrelated prompt every turn | yes | 2/3/4 |
| same | none of its routing keys | growing history | yes, **all nine on one worker** | 0/0/9 |

So the evaluation's open question, whether llm-d's header handling keeps `x-session-id`, is answered: the
EPP receives the header unchanged and its session scorer, configured with that header, honours it (the scorer
takes any header or request attribute as a source: `session_id.go:54-59`). vllm-router also honours it, but
`x-session-id` is not the only key it takes: it checks `x-session-id`, `x-user-id`, `x-tenant-id`,
`x-correlation-id`, `x-request-id` and `x-trace-id` in that order, then the body keys
`session_params.session_id`, `user`, `session_id` and `user_id` (`src/policies/hash_key.rs:10-27`). For a chat request
the routing text is only `session_params.session_id` or empty (`src/protocols/spec.rs:563-571`). What the run
shows is the case with none of those keys: every session hashed to the same worker. (A per-request id such as
`x-request-id`, if a front adds one and no `x-session-id` is sent, would be taken as the key and spread
requests without keeping sessions together; that case was not run.)

### 5. Controls and fault injection

| Test | Expected from the specs | Observed |
|---|---|---|
| Positive control: vllm-router with `--request-timeout-secs 60`, 150 s stream | cut at about 60 s | cut at 60.0 s, 59/150 tokens, `unexpected EOF`, no router log line. The same signature as the 1,800 s default cut, so the client does detect a router deadline. |
| Smoke check (not a control): llm-d with 5 s `message_timeout` and 5 s `grpc_service.timeout`, 600 s stream | streams, whether or not the timers are armed | complete, 600/600 tokens. Does not discriminate: a 600 s stream of promptly answered messages survives an armed 5 s per-message timer (see Method). |
| EPP **SIGTERM then SIGKILL after 10 s** (`stack.sh kill-epp`), 30 s into a 120-token stream | `failure_mode_allow` is false: the stream dies | stream cut after 40 s, when the EPP was killed 10 s after the SIGTERM, `unexpected EOF`; Envoy logged `ext_proc_error_gRPC_error_13`. The cut is the forced kill: see the next row for the TERM alone. |
| EPP **SIGTERM alone**, 30 s into a 150-token stream (`run.sh epp-term`, patched EPP) | the pinned EPP starts `GracefulStop` on SIGTERM and lets open streams finish (`runner.go:1178`, `internal/runnable/grpc.go:72`) | **the stream completed**, 150/150 tokens, 150 s, largest gap 1,088 ms. The EPP logged `gRPC server shutting down` at the SIGTERM and exited about 120 s later, when the stream ended (`gRPC server terminated`, 11:41:53.9 against 11:39:53.8). The EPP stayed alive for the whole stream. |
| new request while the EPP is stopping (5 s after the SIGTERM), and after it exited | the listener is closed: error | HTTP 500 in both cases, `ext_proc_error_gRPC_error_14` (unavailable), 1 ms |
| new request while the EPP is down (after the forced kill) | error | HTTP 500, `ext_proc_error_gRPC_error_14` |
| new request after the EPP restarts | works | complete |
| EPP frozen (SIGSTOP) for 90 s in a 150-token stream, tuned config (`message_timeout` 3,600 s) | nothing notices (no timer) | stream stalled for 91.0 s (one gap), then completed 150/150. Not a test of the 5 s timer: 90 s is far below the configured 3,600 s. |
| Worker 0 removed, then the router restarted (lesson L-4.8) | an EPP restart that keeps the endpoints file keeps the membership; vllm-router keeps it in memory | llm-d: after the EPP restart (`start-epp`, which keeps the file; `stack.sh start` would write a fresh one with all workers) 0 of 12 new sessions reached worker 0 (6 and 6 on the others). vllm-router: `/list_workers` showed 2 workers after the removal and all 3 after the restart; the removal was lost. |

The TERM-only run (one stream, one run) is in `results/epp-term/` (`run.log`, `epp-shutdown.log`,
`envoy-access.log`). It shows that the EPP is in the data path, that a graceful stop keeps the streams open
at the cost of refusing new requests from the TERM on, and that a supervisor with a short kill timeout
turns it into the forced case.

An earlier run of the first two controls was discarded: a leftover simulator from a smoke test held the new
stack's ports, answered its health checks, and was stopped by hand mid-stream, which cut the 5 s-timer
stream. `stack.sh` now refuses to start when a port is in use.

### 6. Incidents during the runs

- **Scrape interval.** An early llm-d placement run used a 500 ms metrics scrape (to spare CPU). The EPP's
  utilization filter treats metrics older than 200 ms as stale and drops the worker from the candidates; a
  bound session then migrated for good (`Session affinity - session migrated pod`) and one session of nine
  split. All results above use the shipped 50 ms scrape (about 5% of a core for the EPP and 4% per simulator,
  idle). The two llm-d streams of the 2 h run still use 500 ms; a single request is unaffected.
- **Alpha plugin.** `session-affinity-scorer` is Alpha in v1.0.0: the EPP refuses to start without
  `--allow-experimental-plugins`, and Envoy then answers 500 (`ext_proc_error_gRPC_error_14`).
- **Listen addresses.** The simulator and the EPP bind every interface and have no option to change it
  (upstream line numbers: `llm-d-inference-sim: pkg/communication/http.go:58`; `llm-d-router: internal/runnable/grpc.go:39`,
  `cmd/epp/runner/runner.go:588`, `:1364`). On a shared host that breaks a loopback-only rule. A patch
  (`third_party/forked/*/loopback-bind.patch`) fixes it. It was built and installed at about 10:05 on the run
  day. Until then, and for the 2 h streams until they ended (about 10:50), the simulators and EPPs listened on
  all interfaces of the VM. The placement runs, the slow-first-token run, the first removal runs (25 min,
  superseded) and the 2 h streams used the unpatched binaries. The second controls run, the EPP fault
  injections, the restart test and the final removal runs used the patched ones. Behaviour does not depend on
  the bind address.
- **Go build cache.** The patched builds used a RAM-backed build cache (`/dev/shm`, 2.7 GB, deleted
  afterwards) instead of the shared disk cache, because the root disk had under 3 GB free.

## Changes after the review

The cross-model review of PR #44 asked for these. Evidence is in `results/review-fixes/`.

- **SSE framing.** The client assembles events up to their blank line (see Method). Fourteen byte-level fixtures
  (`cmd/s2probe/sse_test.go`): nine fail against the earlier line-by-line client (truncated terminator with zero
  and with one trailing newline, the same cut after a split read, a missing separator before `[DONE]` and between
  tokens, an error payload, an `event: error`, malformed JSON, an event after `[DONE]`), and five valid streams
  (single-byte reads, one coalesced read, a terminator split across reads, CRLF, comments) pass on both and guard the new assembler.
- **Failure propagation.** `run.sh` returns every scenario's status; the shipped llm-d arm is asserted; a missing
  setup exits 2. `test-run.sh` injects a stack start failure, a cut, a too-short and a missing shipped result,
  and failures hidden inside `all`, next to an all-pass baseline. Against the earlier `run.sh`, a refused stack
  start and a cut shipped arm both exited 0 (`results/review-fixes/b2-old-run-sh.txt`).
- **EPP stop.** The cut recorded earlier was SIGTERM followed by SIGKILL after 10 s. SIGTERM alone was run
  (Section 5): the stream finished.
- **Inherited gRPC defaults** are in the deadline table, with their scope.
- **Rebuild and listeners.** Build stamps and the listener check (see Reproduce); both tested.
- **Modification notices** are in the three patched upstream files and the regenerated patches; they apply to the pinned commits.
- **Claims scoped.** The `ext_proc` timer: source evidence, the 5 s run relabelled a smoke check. Affinity: the
  no-routing-key case. Cache recovery after the TTL: conditional. Removal: measured offsets, minimum durations, an
  EPP restart "that preserves the endpoints file".
- **Not redone:** the 2 h, removal and placement runs. Only the TERM-only run and short parser smoke streams are new.

## What the results mean for the HBEPs

1. **Time-outs (HBEP-5 Risks, Time-outs, Test plan).** The rule "no hop sets a total deadline below the client's"
   holds for llm-d as shipped (24 h) and fails for vllm-router as shipped (30 min, silent). Whoever launches
   vllm-router must set `--request-timeout-secs`, and a start-up check should refuse the default. The
   evaluation's unknown about llm-d's 1,000 s `ext_proc` timeout is resolved for streams that keep moving:
   Envoy's source starts no such timer in `FULL_DUPLEX_STREAMED` mode, and no stream of 2 h, nor a
   1,100 s first-token wait, was cut under the shipped config. That the timer is also inert for a stalled
   exchange is a reading of the source, not a measurement. Keep the body modes at `FULL_DUPLEX_STREAMED`; the
   source says another mode arms the timer. grpc-go's inherited defaults (a 2 h keep-alive ping, a 20 s ack
   timeout, a 120 s handshake timeout) are liveness and set-up limits, not stream deadlines.
2. **Inner router (HBEP-5 open question 2).** Both candidates carry the 2 h stream and survive a removal. The
   differences that matter for the decision are not deadlines:
   - llm-d needs two processes (EPP and Envoy; binaries of 109 MB and 75 MB), literal IPv4 worker addresses, and for header
     affinity an Alpha plugin and its flag. The endpoints file is the only state, so an EPP restart that preserves
     the endpoints file keeps removals (lesson L-4.8): the file *is* the membership. Starting the stack afresh
     (`stack.sh start`) regenerates the file with every worker.
   - vllm-router is one process, but its membership lives in memory: the removal API changes nothing on disk,
     so after a restart the CLI list returns (tested; L-4.8 stays unsolved), and a skewed hash can pile sessions on one worker
     (7 of 9 in one run; L-4.4).
   - The EPP is in the data path of every stream. Killing it cuts every stream in flight (SIGTERM, then SIGKILL
     after 10 s: cut at the kill). A graceful stop does not: SIGTERM alone made the EPP refuse new requests at once
     (HTTP 500) while the open stream ran to its end (150/150 tokens), and it exited about 120 s later, when that
     stream ended; one stream, one run. A supervisor's kill timeout (10 s here) turns the graceful stop into the
     forced case. Freezing the EPP stalls streams silently, with no timer to notice. The Instance controller must
     treat an EPP restart as a drain: stop sending new requests, SIGTERM, wait for the open streams, and
     never SIGKILL on a short timeout.
3. **Passing the conversation key down (open question 2 candidate).** Sending the front's conversation key as
   `x-session-id` works with both routers. For vllm-router some routing key is required: a chat request with
   none of its keys (six headers, `session_params.session_id`) hashes with all the others to one worker, so
   `x-session-id` is one sufficient choice, not the only one. llm-d's session binding expires after 300 s
   without a request by default (`evictionTtlSeconds`), so agent pauses over five minutes lose it. After that
   the prefix scorer can find the cache again only if the worker's cache and the EPP's prefix index still hold
   the prefix; that was not tested.
4. **Drain (HBEP-5 Draining).** Removing a worker from discovery drains it in both routers: no new request
   starts there and the streams on it finish. That is the behaviour the lease-end drain needs, on both.
5. **Reachability (HBEP-6).** The EPP's gRPC and metrics ports, and the simulator's port, bind every interface
   with no authentication (`--secure-serving=false`). On a cluster network they must be bound to loopback or a
   private interface; a patch like the one here is needed for the EPP.

## What phase B must measure

Same two routers, one GH200 job, 2 to 4 workers of a small model, round robin as the baseline, `x-session-id`
set, on a multi-turn agent workload whose sessions grow turn by turn and whose prefixes differ between sessions:

- Prefix-cache hit rate and time to first token, per router and for round robin. This is the pass criterion
  "beats round robin on hit rate"; phase A could only check placement.
- Load balance under real KV pressure: sessions per worker, preemptions, queue depth. vllm-router's hash
  skewed 7/1/1 once in nine sessions; llm-d's utilization filter drops saturated workers and migrates their
  sessions for good, which costs hits.
- Whether llm-d's approximate prefix index needs the workers' KV events to keep up, and what its defaults do
  with 2 to 4 workers (the shipped config names no producer for it, yet the EPP logs show an approximate prefix index running).
- Keep-alive pairing (L-2.5): vllm-router keeps idle connections 50 s (`src/server.rs:924`); vLLM's server
  closes idle connections after 5 s by default. The side that reuses must have the shorter timeout.
- The slow-first-token case with a real prefill, and the largest first-token wait the deployments show,
  against the idle timeouts (llm-d route idle 86,400 s; vllm-router has none besides the total).
- Resource cost of each stack on a GH200 node: EPP and Envoy CPU and memory with real `/metrics` scraping
  (the simulator's answer is cheap), against vllm-router's single process.
- Whether the `x-session-id` header, and the 300 s binding TTL, survive the front (HBEP-5) in front of them.
- On the cluster: workers' literal IPv4 addresses in the endpoints file, written by the job script; the
  loopback patch for the EPP; Envoy and the EPP started by the job, not as containers (no root, Apptainer at most).

## arm64 (Helios GH200)

| Component | linux/arm64 | Evidence |
|---|---|---|
| EPP | yes (image); no release binary | `ghcr.io/llm-d/llm-d-router-endpoint-picker:v1.0.0` is a manifest list with `linux/amd64` and `linux/arm64`; the release has only SBOMs (`llm-d-router-endpoint-picker-v1.0.0-linux-arm64.spdx.json`). Pure Go: `Dockerfile.epp:32` builds with `CGO_ENABLED=0 GOARCH=${TARGETARCH}`, so a cross-build (`GOARCH=arm64`) needs no toolchain on the target. Not run here. |
| Envoy | yes | `envoy-1.33.2-linux-aarch_64` (71 MB) is a GitHub release asset (ELF `e_machine` 183 checked on the first 20 bytes); image `envoyproxy/envoy:distroless-v1.33.2` has `linux/arm64`. |
| llm-d-inference-sim | yes (image); no release binary | `ghcr.io/llm-d/llm-d-inference-sim:v1.0.0` has `linux/amd64` and `linux/arm64`; builds with `CGO_ENABLED=0` (`Makefile:135`). Only a phase-A tool. |
| vllm-router | yes | PyPI wheel `vllm_router-0.1.16-cp38-abi3-manylinux_2_28_aarch64.whl` (21 MB); its extension module is AArch64 (ELF `e_machine` 183). Needs glibc 2.28 or newer and Python 3.8 or newer. Container image: `vllm/vllm-router:v0.1.16` on Docker Hub is a multi-arch tag (`linux/amd64`, `linux/arm64`); nightly tags likewise. Only the x86-64 wheel was run here. |

Nothing was run on arm64. Both routers therefore need a first start on a GH200 node in phase B.

## Reproduce

```bash
export S2_HOME=<an empty work directory, about 1 GB>
spikes/s2/scripts/setup.sh                 # pin, fetch, patch, build; needs git, curl, go, python3 with venv
spikes/s2/scripts/run.sh all               # every scenario below; the 2 h streams run in the background (~2 h 15 min)
spikes/s2/scripts/run.sh summary           # table of every stream result
(cd spikes/s2 && go test ./...)            # the client's own verdict logic: SSE assembly, cut vs complete, removal verdict, metrics
spikes/s2/scripts/test-run.sh              # run.sh must exit non-zero on injected failures; a baseline must exit 0 (no servers, seconds)
spikes/s2/scripts/test-setup.sh "$S2_HOME/src"   # setup.sh rebuilds a leftover or stale binary (stub go, no compile, no network)
spikes/s2/scripts/test-listeners.sh        # the loopback-only listener check fails on 0.0.0.0, [::], children and unseen listeners
```

`setup.sh` takes optional phases (`sources patched envoy venv client`). Every start by `stack.sh` ends with a check
(`ss -ltnp`) that each process it launched listens on loopback only, and stops the stack otherwise.

Or one scenario at a time (all but `long` and `all` are short):

```bash
run.sh controls    # ~11 min   vllm-router 60 s deadline cuts; llm-d with 5 s ext_proc timers streams 600 s
run.sh placement   # ~25 min   eight placement cases
run.sh removal     # ~37 min per router (pass llmd-removal or vllmr-removal to run one)
run.sh epp-kill    # ~3 min    SIGTERM then SIGKILL after 10 s to the EPP under an in-flight stream
run.sh epp-term    # ~5 min    SIGTERM alone to the EPP under an in-flight stream; timeline in results/epp-term/
run.sh epp-stall   # ~4 min    freeze the EPP for 90 s under an in-flight stream
run.sh restart     # ~2 min    does a removal survive a router restart
run.sh ttft        # ~19 min   first token after 1,100 s
run.sh long        # ~2 h 5 min   five 2 h streams in parallel
```

`run.sh all` was not run as one command; its parts were run separately, some of them more than once while the harness was fixed (see Incidents). `run.sh` exits non-zero if an expectation is not met, a stack or an EPP fails to start, or a client crashes, and `all` still runs every scenario and then exits non-zero (`epp-kill`, `epp-term` and `epp-stall` record behaviour and have no pass criterion; their setup steps are still checked). Results land in `$S2_HOME/results/<scenario>/`:
one JSON verdict and one gaps CSV per stream, and the Envoy access log of each llm-d stack.
The results behind this README are in `spikes/s2/results/` (JSON verdicts, gzipped gap CSVs, Envoy access logs,
scenario logs, the minute-by-minute load); the paths in the logs are shown as `$S2_HOME`. `SIM_ITL` and `SIM_TTFT` change the simulators' pace; `LONG_TOKENS` and `REMOVAL_TOKENS` change the
stream lengths (the expectations for the default-deadline cut assume the full length).

## Unknowns left

- Hit rates, time to first token and load balance on real vLLM (phase B).
- Behaviour on arm64; the loopback patch on a cluster network; Envoy 1.40.2 (read in source, not run).
- A stream at a realistic byte rate: these streams are one small chunk per second, so a byte-count or
  throughput dependent deadline would not show.
- Concurrency: one 2 h stream per stack, plus a few short requests. Envoy's HTTP/2 stream limit (100) and the
  EPP's behaviour with hundreds of streams are not exercised.
- The 300 s session-binding TTL was read in source, not tested with a 5 min pause.
- The `ext_proc` message timer under a stalled exchange. Source evidence only (Envoy 1.33.2 and 1.40.2
  `processor_state.cc`); the 5 s run is not a discriminating test. It would need a stalled exchange past 5 s
  under the 5 s configuration and a comparison where the timer is known to be armed.
- The graceful EPP stop: one stream, one run. How the EPP behaves with many open streams, or when Envoy
  retries a refused new request, is not exercised.
- Whether a request routed by an `x-request-id` the front adds (vllm-router) loses session affinity.
- TLS between Envoy and the EPP (dropped in the guide's no-Kubernetes config) and any authentication.
- Scale of the finding "7 of 9 sessions on one worker": three runs only (7/1/1, 3/2/4, 2/3/4); it depends on
  the session IDs hashed.
