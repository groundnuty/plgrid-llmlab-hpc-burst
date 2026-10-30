# Spike S2, phase A: the router inside an Instance

Status: phase A (simulated engines, no cluster). Phase B (real vLLM on one GH200 job) is not done.

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
| New requests avoid the removed worker | yes, immediately and at +9 min | yes, immediately and at +9 min | not applicable |
| `x-session-id` keeps a session on one worker | yes, with the session scorer (Alpha); the shipped scorers keep multi-turn sessions together by prefix and ignore the header | yes (`consistent_hash`); without the header all chat traffic hashes to one worker | not applicable |
| Sessions spread over workers | yes, 3/3/3 of 9 | uneven: 3/2/4, 2/3/4 and, in a first pass, 1/1/7 of 9 | not applicable |

The one deadline the evaluation flagged, llm-d's 1,000 s `ext_proc` message timeout, does not apply to
streams: Envoy starts no such timer in `FULL_DUPLEX_STREAMED` mode, which llm-d's config uses. The one that
does cut is vllm-router's `--request-timeout-secs`, default 1,800 s, silently. Both routers pass the three
phase-A criteria when configured; the choice between them rests on phase B (hit rate, load balance) and on
the operational findings below (two processes and an Alpha plugin against one process and a skewed hash).

## Method

- **Engines.** `llm-d-inference-sim` v1.0.0, three per router stack (one for "no router"), on loopback.
  Flags: `--inter-token-latency 1s --time-to-first-token 1s --max-model-len 16384 --max-num-seqs 64`.
  A request sets `min_tokens == max_tokens` and `ignore_eos`, which pins the reply to exactly that many
  tokens. The default context window is 1,024 tokens: without `--max-model-len` a request for 7,300
  tokens would end after about 1,000 s with `finish_reason=length` and look complete.
- **Client.** `cmd/s2probe` (Go standard library only). No client-side deadline of any kind. A stream
  counts as complete only if a `finish_reason` chunk and `[DONE]` both arrive and the content-chunk
  count equals the requested token count; a clean EOF without them, or `unexpected EOF`, is a cut.
  Every content chunk's arrival time and inter-token gap is written to a CSV (`<label>.gaps.csv`).
- **Placement** is read from each simulator's own `/metrics` (`vllm:request_success_total`), one request
  at a time, never from the router. Session turns are interleaved across sessions in a fixed-seed
  random order per turn, so neither "the last worker used" nor a rotation that matches the session
  order can produce stickiness by accident.
- **Removal.** A session is bound to worker X by two requests that must land on the same worker. A
  2,100-token stream (35 min) then starts on the same session, so it runs on X. After 20 s, X is removed
  from discovery (EPP: atomic rewrite of the endpoints file; vllm-router: `POST /remove_worker`). The
  removal counts as effective only if the same session then lands on another worker, and 8 fresh
  sessions avoid X, at +30 s, +2, +5 and +9 min. Only then is the survival of the stream judged. The
  session is the lever on purpose: a router that still held X would send the session back to X, whereas
  fresh sessions alone would avoid a busy X even before the removal.
- **Controls** (so that a pass can be told from a test that cannot fail): vllm-router with a 60 s
  deadline must cut a 150 s stream at about 60 s; llm-d with 5 s `ext_proc` timers must stream 600 s.
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
`scripts/setup.sh` applies them. New in this spike: the test client, the stack scripts and the config
generator, all under `spikes/s2/`. The Envoy and EPP configs are generated at run time from llm-d's shipped
files by exact-line substitution (`scripts/genconf.py`), which fails if a line it expects has changed; none
of llm-d's files is copied into this repository.

## Environment

Single VM, 6 cores, 31 GB, no swap, shared with other sessions. Python 3.15.0 for the vllm-router launcher;
Go 1.27.3. Load average: spot checks of `uptime` between 08:48 and 09:17 on the run day read 3.3 to 5.1
(1 minute); from 09:48 to 11:05 it was sampled every minute: 1.3 to 8.2, mean 3.6. Available memory never fell
below 17 GB in the sampled period. The simulators and routers ran at `nice -n 10`.

## Deadlines on the path

Paths are in the pinned sources: `guide` is `llm-d/guides/no-kubernetes-deployment/router/envoy/envoy.yaml`
(llm-d v1.0.0); `epp` is llm-d-router v1.0.0; `vr` is vllm-router v0.1.16. Envoy defaults come from its
API protos at v1.33.2 (`api/envoy/...`).

### llm-d: Envoy 1.33.2 and the EPP

| Deadline | Shipped value | Where | Effect on a long stream |
|---|---|---|---|
| Route total timeout | 86,400 s | guide:48 (`route.timeout`) | A total deadline: cuts at 24 h. Envoy's own default if unset is 15 s (`route_components.proto:1225`). |
| Route idle timeout (stream idle) | 86,400 s | guide:49 (`route.idle_timeout`) | Resets on every header or data event; a 1 token/s stream never trips it. If the line were absent, the connection manager's `stream_idle_timeout` would apply, default 5 min (`http_connection_manager.proto:498`). |
| `ext_proc` `message_timeout` | 1,000 s | guide:72 | **Inert under the shipped config.** Envoy does not start the timer when the body mode is `FULL_DUPLEX_STREAMED` (`processor_state.cc:26-27` in 1.33.2, `:33-34` in 1.40.2), and guide:68-69 sets both body modes to it. Confirmed with 5 s timers (controls) and a 1,100 s first-token wait. Envoy rejects values above 1 h at start-up (`MessageTimeout: value must be inside range [0s, 1h0m0s]`). |
| `ext_proc` `grpc_service.timeout` | 10 s | guide:64 | Not applied to the `ext_proc` stream: the stream options set no timeout (`ext_proc.cc:399-403`). Confirmed with 5 s. |
| `ext_proc` `failure_mode_allow` | false (unset) | guide, absent | Any error on the `ext_proc` stream, including a dead EPP, ends the client's request. See the EPP-kill result. |
| Cluster `connect_timeout` | 1,000 s (to workers), 86,400 s (to EPP) | guide:92, guide:105 | Connection set-up only. |
| EPP health check | 2 s timeout, 10 s interval, 3 failures | guide:115-122 | Marks the EPP unhealthy; affects routing to the EPP only. |
| Connection manager `request_timeout`, `request_headers_timeout`, `max_stream_duration`, `max_connection_duration` | unset, all disabled | `http_connection_manager.proto:543`, `:549`, `protocol.proto:263`, `:292` | None. |
| Connection `idle_timeout` | 1 h (unset) | `protocol.proto:247` | Applies only to connections with no active stream. |
| `ORIGINAL_DST` `cleanup_interval` | 5 s (unset) | `cluster.proto:1089` | Hosts not selected for 5 s are dropped from the cluster; relevant to removal, and it did not cut the in-flight stream. |
| EPP gRPC server | no keep-alive, no max connection age, no stream timeout | `epp: internal/runnable/grpc.go`, `pkg/epp/server/runserver.go:262-273` | None. |
| EPP session binding | 300 s idle TTL, swept every 10 s | `epp: pkg/epp/framework/plugins/scheduling/util/sessionaffinity/session_id.go:69-70` | A session idle for more than 5 min between requests loses its binding. Applies to the session scorer only. |
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
| same | none | growing history | yes, **all nine on one worker** | 0/0/9 |

So the evaluation's open question, whether llm-d's header handling keeps `x-session-id`, is answered: the
EPP receives the header unchanged and its session scorer honours it. vllm-router also honours it
(`src/policies/hash_key.rs`, header list first), and without it falls back to the request body, which for chat
requests is empty (`src/protocols/spec.rs:563-571`), so every session hashes to the same worker.

### 5. Controls and fault injection

| Test | Expected from the specs | Observed |
|---|---|---|
| vllm-router with `--request-timeout-secs 60`, 150 s stream | cut at about 60 s | cut at 60.0 s, 59/150 tokens, `unexpected EOF`, no router log line. The same signature as the 1,800 s default cut, so the client does detect a router deadline. |
| llm-d with 5 s `message_timeout` and 5 s `grpc_service.timeout`, 600 s stream | if the timers applied, cut at 5 s | complete, 600/600 tokens |
| EPP stopped (SIGTERM) 30 s into a 120-token stream | `failure_mode_allow` is false: the stream dies | stream cut after 40 s, when the EPP exited (about 10 s after the SIGTERM), `unexpected EOF`; Envoy logged `ext_proc_error_gRPC_error_13` |
| new request while the EPP is down | error | HTTP 500, `ext_proc_error_gRPC_error_14` |
| new request after the EPP restarts | works | complete |
| EPP frozen (SIGSTOP) for 90 s in a 150-token stream | nothing notices (no timer) | stream stalled for 91.0 s (one gap), then completed 150/150 |
| Worker 0 removed, then the router restarted (lesson L-4.8) | llm-d keeps membership in a file; vllm-router in memory | llm-d: after the EPP restart 0 of 12 new sessions reached worker 0 (6 and 6 on the others). vllm-router: `/list_workers` showed 2 workers after the removal and all 3 after the restart; the removal was lost. |

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
  (`llm-d-inference-sim: pkg/communication/http.go:58`; `llm-d-router: internal/runnable/grpc.go:39`,
  `cmd/epp/runner/runner.go:588`, `:1364`). On a shared host that breaks a loopback-only rule. A patch
  (`third_party/forked/*/loopback-bind.patch`) fixes it. It was built and installed at about 10:05 on the run
  day. Until then, and for the 2 h streams until they ended (about 10:50), the simulators and EPPs listened on
  all interfaces of the VM. The placement runs, the slow-first-token run, the first removal runs (25 min,
  superseded) and the 2 h streams used the unpatched binaries. The second controls run, the EPP fault
  injections, the restart test and the final removal runs used the patched ones. Behaviour does not depend on
  the bind address.
- **Go build cache.** The patched builds used a RAM-backed build cache (`/dev/shm`, 2.7 GB, deleted
  afterwards) instead of the shared disk cache, because the root disk had under 3 GB free.

## What the results mean for the HBEPs

1. **Time-outs (HBEP-5 Risks, Time-outs, Test plan).** The rule "no hop sets a total deadline below the client's"
   holds for llm-d as shipped (24 h) and fails for vllm-router as shipped (30 min, silent). Whoever launches
   vllm-router must set `--request-timeout-secs`, and a start-up check should refuse the default. The
   evaluation's unknown about llm-d's 1,000 s `ext_proc` timeout is resolved: it does not bound streams in this
   configuration. Keep the body modes at `FULL_DUPLEX_STREAMED`; a change to another mode arms the timer.
2. **Inner router (HBEP-5 open question 2).** Both candidates carry the 2 h stream and survive a removal. The
   differences that matter for the decision are not deadlines:
   - llm-d needs two processes (EPP and Envoy; binaries of 109 MB and 75 MB), literal IPv4 worker addresses, and for header
     affinity an Alpha plugin and its flag. The endpoints file is the only state, so a restart keeps removals
     (lesson L-4.8): the file *is* the membership.
   - vllm-router is one process, but its membership lives in memory: the removal API changes nothing on disk,
     so after a restart the CLI list returns (tested; L-4.8 stays unsolved), and a skewed hash can pile sessions on one worker
     (7 of 9 in one run; L-4.4).
   - The EPP is in the data path of every stream: stopping it cuts all streams, freezing it stalls them
     silently, with no timer to notice. The Instance controller must treat an EPP restart as a drain.
3. **Passing the conversation key down (open question 2 candidate).** Sending the front's conversation key as
   `x-session-id` works with both routers, and is required for vllm-router (without it all chat traffic lands
   on one worker). llm-d's session binding expires after 300 s without a request, so agent pauses over five
   minutes lose it; the prefix scorer would still find the cache.
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
spikes/s2/scripts/run.sh all               # every scenario below; the 2 h streams run in the background (~2 h 10 min)
spikes/s2/scripts/run.sh summary           # table of every stream result
(cd spikes/s2 && go test ./...)            # the client's own verdict logic: cut vs complete, metrics parsing
```

Or one scenario at a time (all but `long` and `all` are short):

```bash
run.sh controls    # ~11 min   vllm-router 60 s deadline cuts; llm-d with 5 s ext_proc timers streams 600 s
run.sh placement   # ~25 min   eight placement cases
run.sh removal     # ~37 min per router (pass llmd-removal or vllmr-removal to run one)
run.sh epp-kill    # ~3 min    stop the EPP under an in-flight stream
run.sh epp-stall   # ~4 min    freeze the EPP for 90 s under an in-flight stream
run.sh restart     # ~2 min    does a removal survive a router restart
run.sh ttft        # ~19 min   first token after 1,100 s
run.sh long        # ~2 h 5 min   five 2 h streams in parallel
```

`run.sh all` was not run as one command; its parts were run separately, some of them more than once while the harness was fixed (see Incidents). `run.sh` exits non-zero if an expectation is not met (`epp-kill` and `epp-stall` record behaviour and have no pass criterion). Results land in `$S2_HOME/results/<scenario>/`:
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
- TLS between Envoy and the EPP (dropped in the guide's no-Kubernetes config) and any authentication.
- Scale of the finding "7 of 9 sessions on one worker": three runs only (7/1/1, 3/2/4, 2/3/4); it depends on
  the session IDs hashed.
