# Spike S2, phase B: the router inside an Instance, on real vLLM (GH200)

Phase A (`spikes/s2/`, PR #44) checked deadlines, removals and placement with simulated engines. This phase measures
what phase A could not: the prefix-cache hit rate and time to first token on real vLLM, for each router and for
round robin, on a multi-turn agent workload with `x-session-id`. Issue #29, phase B.

## Result in brief

Both candidate routers beat round robin on the prefix-cache hit rate, measured with four workers sharing one GPU.
That is #29's pass criterion, and it held in every arm and both regimes. vllm-router `consistent_hash` on
`x-session-id` came within 0.003 of the estimated reuse bound and spread sessions over all four workers. llm-d
matched it when the cache was ample. But its shipped scorers sent every session that shares a system prompt to the
same worker: one worker carried 54% of the requests while another got none. With the cache capped, that worker's hit
rate fell to 0.86.

Hit rate from the workers' own counters, on 1,152 requests per arm. B is the capped cache, A the cache as profiled.
B1 and B2 are two runs of the same arm with the same generator seed. They share the task structure and the think
times, not identical request traces: the run label is part of `x-session-id`, so the hash assignment differs between
them, and replies are sampled without a request seed (see Method). The reuse bound is an estimate, not a ceiling (see
Measurements); it was 0.905 in every arm (0.9051 to 0.9053).

| Router | Hit rate B1 / B2 | Hit rate A | TTFT p50 / p95, ms (B1) | Sessions per worker (B1) | Sticky sessions | Busiest worker's requests / mean (B1) |
|---|---|---|---|---|---|---|
| round robin (`rr`) | 0.635 / 0.654 | 0.702 | 81 / 184 | 22 / 25 / 18 / 31 | 0% | 1.00 |
| vllm-router `consistent_hash` (`vr-hash`) | **0.902 / 0.902** | 0.902 | 58 / 83 | 27 / 23 / 14 / 32 | 100% | 1.33 (B2: 1.17; A: 1.08) |
| llm-d, + session scorer (`llmd-sess`) | 0.880 / 0.884 | **0.905** | 52 / 88 | 0 / 18 / 26 / 52 | 100% | 2.17 |
| llm-d, shipped scorers (`llmd-prefix`) | 0.882 | **0.905** | 50 / 85 | 18 / 0 / 52 / 26 | 100% | 2.17 |
| vllm-router `cache_aware` (`vr-cache`) | 0.642 | 0.703 | 81 / 173 | 22 / 16 / 40 / 18 | 0% | 1.16 |

All 13 arms completed every request: 0 failed and 0 preempted. The full table, with the client's own cached-token
hit rate (equal to the workers' to three decimals in every arm), is in `results/gh200-1gpu/summary.md`.

- **Verdict against #29: pass.** Both candidates beat round robin on hit rate, with four workers sharing one GPU.
  - With the cache capped (B): +25 to +27 points for `consistent_hash`, +23 to +25 for llm-d.
  - With an ample cache (A): +20 points for both.
  - Round robin sends 74% of turns to a different worker from the previous turn's. With the cache capped, the copies it
    spreads over four workers evict each other.
- **Which router.** The hit rate supports either; balance decides. On this workload `consistent_hash` is the
  provisional choice: the same hit rate, every session sticky, and no idle worker. llm-d needs a change to its scorer
  weights, or a 4-GPU run showing that its queue and KV scorers undo the pile-up once a worker is loaded. A run with
  one GPU per worker must still validate hit rate, routing balance and latency before the choice is final.
- **TTFT** fell with the hit rate: p50 58 ms and p95 83 to 86 ms for `consistent_hash`, against 78 to 82 ms and
  137 to 184 ms for round robin. These numbers come from one shared GPU, so they are not dedicated-GPU figures.
  Which way the sharing biases them (for example, llm-d's idle fourth worker leaves three processes on the GPU) was
  not measured (see Deviation).


## Question

Inside one Instance, which router keeps the vLLM prefix cache warm, compared with round robin?

- llm-d's endpoint picker (EPP) with Envoy, in file-discovery mode, with the session-affinity scorer on
  `x-session-id` (Alpha, `--allow-experimental-plugins`) on top of the shipped scorers, prefix scorer included;
- vllm-router, `consistent_hash` on the header;
- round robin (vllm-router `round_robin`), the baseline.

Two more arms cost little and inform the choice: llm-d with the shipped scorers only (no Alpha plugin), and vllm-router's
`cache_aware` with the thresholds www-2027 ended on (`--cache-threshold 0.8 --balance-abs-threshold 1 --balance-rel-threshold 1.1`).

Pass (issue #29): the chosen router beats round robin on hit rate. The 2 h stream and the removal checks passed in phase A
and were not repeated.

## Deviation: one GPU, four workers on it

Issue #29 and the brief asked for four GPUs, one worker per GPU. On 30 Oct `sbatch --test-only` gave every 4-GPU shape,
and every 1- or 2-GPU shape in the normal QOS, a start estimate of 7 Nov: 90 of the 110 GH200 nodes had all four GPUs
allocated, and the partition's queue was ahead of us. The association also allows QOS `now` (at most 1 GPU, 72 CPUs,
122,400 MB, one job per user), which started at once. The requester chose that (option A): one GH200 GPU, four vLLM
workers sharing it, each with its own KV-cache budget.

What this keeps and what it loses:

- **The hit-rate comparison is a measured result for four workers sharing one GPU.** It is #29's pass criterion. Each
  worker is a separate vLLM server with its own prefix cache, its own KV blocks and its own scheduler, and the KV
  budgets are equal within a regime, so no arm had a cache advantage over another. The workers' timing is still
  coupled through the one GPU. The client waits for each reply before it advances a session, so shared compute can
  change the interleaving of requests, batching, block lifetimes and eviction order, and it changes the load signals
  that llm-d's scorers and `cache_aware` read. This phase did not test whether the same hit rates would hold with one
  GPU per worker.
- **TTFT and the cost of piling onto one worker are not dedicated-GPU measurements.** Whether the shared GPU makes
  them look better or worse than they would be on their own GPUs was not measured. A 4-GPU run is needed to validate
  hit rate, routing balance and latency.

## Method

**One job, everything inside it.** `scripts/job.sh` runs as one Slurm job. It starts four vLLM workers one after
another (each profiles its memory alone), then for each arm starts one router stack, runs the client through it, and
stops the stack. Every listener of the routers, the EPP and Envoy is on 127.0.0.1, and the job checks that from `/proc`
after each start (an arm whose router listens elsewhere is skipped). The client runs inside the job, so nothing on the
VM changed. The job ended by itself after the last arm. It now exits 1 when a required arm is skipped, a prefix-cache
reset or a metrics scrape fails, or a client fails or reports no result; it lists each problem in `failures.txt` and
keeps the partial results, and an arm whose caches were not reset is not run. The recorded run predates this and
exited 0 whatever happened: none of its 13 arms was skipped, every reset answered `{"success":true}` with HTTP 200,
and every client exited 0.

**Workload** (`cmd/s2bload`, the same generator and seed 1 in every arm). The arms share the generated task structure
and the think times; they do not replay identical request traces (see "What differs between arms"). An agent workload with shared
system prompts and growing per-session contexts, so neither of the proofs that #29 rules out applies (every request a
different prefix; every request the same prefix):

- 3 agent types with popularity 0.5 / 0.3 / 0.2. Each type has one 2,000-word system prompt that all its sessions share.
- 96 sessions of 12 turns, 32 sessions in flight at once. A session starts with a unique 250-word task; each later turn
  appends a tool result of 200 to 800 words. Each reply is 48 tokens (`min_tokens = max_tokens`, `ignore_eos`), and it
  is appended to the history, so turn t's prompt extends turn t-1's prompt plus the reply.
- 0.5 to 2.5 s of "tool time" between turns. Each request carries `x-session-id`. Prompts grow to about 10,000
  tokens by the last turn.
- Arms do not share any cache. The run label is the first line of every system prompt, and every worker's prefix cache
  is reset (`POST /reset_prefix_cache`, `VLLM_SERVER_DEV_MODE=1`) before each arm. All 52 resets answered
  `{"success":true}`.
- **What differs between arms.** The run label is also part of `x-session-id` (`<run>-s<NNN>`), so the same session
  hashes to a different worker in a different arm: `consistent_hash` placed 27 / 23 / 14 / 32 sessions per worker in
  B1 and 28 / 24 / 20 / 24 in B2. The label also changes the prompt token lengths a little, and the client samples
  replies (temperature 0.7) without a request seed before it appends them to the history. B1 and B2 start from
  separately reset prefix caches but share the worker processes, the job and the workload seed, and their spread also
  includes the changed hash assignment. The routing-key namespace was not separated from the run label, because that
  would change what the recorded runs did; a controlled repeat should separate them and replay fixed histories.

**Measurements.**

- **Hit rate** comes from the workers' own counters: the growth of `vllm:prefix_cache_hits_total` over that of
  `vllm:prefix_cache_queries_total` during the arm, summed over the four workers. The client's own sum of
  `usage.prompt_tokens_details.cached_tokens` over `prompt_tokens` is reported beside it as a cross-check.
- **Reuse bound** is an estimate of the share of prompt tokens a perfect router with an unbounded cache could serve
  from cache, not a theoretical ceiling. It counts the previous turn's prompt plus its reply, less the reply's last
  sampled token (whose KV is not computed yet), and the shared system prompt for every session but the first of its
  type, in whole 16-token blocks. The system prompt's length is not computed from tokenised histories: it is the
  largest cached-token count that a turn-0 request of that type reported in the same run, so it depends on the
  router and cache under test. A true ceiling would need the tokenised histories and the materialised KV. The
  estimate is about 0.9, so this workload is far from both trivial cases; the measured gain over round robin does
  not depend on it.
- **TTFT** is measured by the client, from sending the request to the first non-empty content delta, through the router.
- **A measured request** is one whose stream is complete and whose counts can be trusted. The client reads events
  with phase A's SSE framing (`cmd/s2bload/sse.go`, copied from `spikes/s2/cmd/s2probe/sse.go`), so a `[DONE]` or any
  event without its terminating blank line is an error. It then requires some content, a non-empty `finish_reason`,
  and a usage record with positive `prompt_tokens` and a `completion_tokens` equal to the 48 requested. The recorded
  runs were taken with the first version of the client, which accepted a `[DONE]` line without its blank line and
  did not require a finish reason or usage. This does not change the results: all 14,976 recorded requests have 48
  completion tokens and no error, and the workers' counters give the same hit rates as the client's records (equal to
  three decimals in every arm).
- **Placement** comes from the workers, not the router. A 65-line ASGI middleware (`py/s2b_placement.py`, loaded with vLLM's
  `--middleware`) logs, for each request, the session and turn tagged at the end of the newest user message. That gives
  sessions per worker (by first turn), the share of sessions whose turns all landed on one worker ("sticky"), and the
  busiest worker's request count over the mean.
- **KV pressure:** each worker's `kv_cache_usage_perc`, queue and preemption counters every 2 s.

**Two KV regimes.** A 1.5B model leaves room for a large cache. With that much room, round robin keeps finding each
session's older prefixes on every worker, and the gap to an affinity router narrows for a reason that does not carry
over to big models. So most arms ran with each worker's cache capped:

- **B (capped):** `--num-gpu-blocks-override 6000`, 96,000 tokens per worker. The 32 sessions in flight hold about
  186,000 tokens (32 times the mean prompt of 5,805 tokens). Split over four workers that fits one worker's cache; copied to every worker, as round robin does,
  it does not. This stands in for a large model, whose KV cache is the scarce resource. It reproduces the field failure
  of lesson L-4.3: contexts duplicated across workers, then evicted.
- **A (as profiled):** the cache vLLM sized itself at `--gpu-memory-utilization 0.2`: 33,010 blocks, 528,160 tokens
  per worker. Everything fits.

Regime B ran first, as the deciding one. Three of its arms were then repeated in reverse order (B2) to show the
run-to-run spread. Regime A followed.

**Arms** (all vllm-router arms use `--request-timeout-secs 10800`, after phase A):

| Arm | Router |
|---|---|
| `rr` | vllm-router `--policy round_robin` (baseline) |
| `vr-hash` | vllm-router `--policy consistent_hash` (keys on `x-session-id`) |
| `vr-cache` | vllm-router `--policy cache_aware --cache-threshold 0.8 --balance-abs-threshold 1 --balance-rel-threshold 1.1` (www-2027's final setting) |
| `llmd-sess` | llm-d EPP + Envoy, the guide's scorers (prefix 3, queue 2, KV use 2, no-hit LRU 2) plus `session-affinity-scorer` on `x-session-id`, weight 10 (Alpha; `--allow-experimental-plugins`) |
| `llmd-prefix` | llm-d EPP + Envoy, the guide's scorers only |

The llm-d configs come from llm-d's no-Kubernetes guide (v1.0.0), generated by phase A's `spikes/s2/scripts/genconf.py`:
loopback addresses, the ports, and the route and `ext_proc` timeouts raised (the "tuned" variant; no effect on requests this short).
The EPP scrapes each worker's `/metrics` every 50 ms, as shipped.

## Environment

| Item | Value |
|---|---|
| Node | Helios GH200 partition, aarch64, kernel with 64 KiB pages (`getconf PAGESIZE` 65536); job: 1 GPU, 72 CPUs, 120,000 MB, memfs, `--no-requeue`, QOS `now` |
| GPU | 1 x NVIDIA GH200 (97,871 MiB visible), driver 615.71.09, compute capability 9.0 |
| vLLM | 0.32.0 (`+cu129` aarch64 wheel from the vLLM GitHub release), torch 2.13.0+cu129, CUDA 12.9, Python 3.12, `module load ML-bundle/25.10` |
| Model | `Qwen/Qwen2.5-1.5B-Instruct`, revision `989aa7980e4cf806f80c7fef2b1adb7bc71aa306`, bf16, Apache-2.0. The recorded run named the model ID without `--revision`; `job.sh` now passes this revision (`MODEL_REV`) |
| Workers | 4 x `vllm serve`, `--gpu-memory-utilization 0.2 --max-model-len 16384 --enable-prefix-caching --enable-prompt-tokens-details`; chunked prefill, `max_num_batched_tokens` 8192; KV 96,000 tokens (B) or 528,160 tokens (A) per worker |
| vllm-router | 0.1.16, the PyPI aarch64 wheel in the same venv |
| EPP | llm-d-router v1.0.0 (commit b9393d1) with phase A's loopback patch, cross-built `CGO_ENABLED=0 GOARCH=arm64` (Go 1.27.3) |
| Envoy | 1.33.2 `envoy-1.33.2-linux-aarch_64` release binary, sha256 `81ec3689a82122eff0ca680c48176f4d351b6f9881a79cc9d9078a6fb4b0b6a8` (from the release's `checksums.txt.asc`) |
| Job | 25 min 02 s, 0.42 GPU-hours. Each arm's client ran for 77 to 90 s; starting the four workers took 3 min 8 s cold (the first one 84 s, then about 35 s each) and 2 min 20 s for regime A |

Envoy, the EPP and vllm-router had never run on this node type. All three started and served on the 64 KiB-page
kernel. The Envoy release binary's load segments are 64 KiB-aligned (`readelf -l`: `Align 0x10000`).

## Findings

1. **llm-d piles sessions by system prompt.** Every one of the 52 sessions of agent type 0 started on one worker,
   all 26 of type 1 on a second, and all 18 of type 2 on a third. The fourth worker got no request. It was the same
   in all five llm-d arms: joining the placement logs with the client's agent types puts each type on exactly one worker.
   - The first turn of a new session matches its type's 2,000-word system prompt on the worker that already served
     that type. The prefix scorer (weight 3) then outweighs the queue, KV-use and no-hit-LRU scorers (2 each).
   - Sampled every 2 s, `num_requests_waiting` was 0 on every worker in every llm-d arm and KV use stayed at or below
     0.45 (the largest sampled queue in any arm was 2, round robin, B1). A short queue between two samples is not
     excluded: the EPP scrapes every 50 ms, and its KV scorer scores one minus the utilisation continuously, so it had a
     signal below saturation. The placement is consistent with the prefix scorer's weight outweighing the load scorers
     at this load; this run did not isolate the scorers to show it.
   - Agent products send one long system prompt for all their users, so this is the expected shape behind LLMLab.
   - With the cache capped, the loaded worker's hit rate was 0.858 to 0.866, against 0.905 on the others: its 52
     sessions did not fit its cache.
2. **The session-affinity scorer added nothing here.** `llmd-sess` and `llmd-prefix` gave the same hit rates (0.880 and
   0.882 in B, 0.905 in A) and the same placement. Turn t's prompt extends turn t-1's, so the prefix scorer already
   keeps a session together. Phase A showed where the Alpha scorer matters: when the prompt changes under the same session.
3. **vllm-router `cache_aware` cannot see a chat prompt.** In 0.1.16 the routing text of a chat request is
   `session_params.session_id` or empty (`src/protocols/spec.rs:563-571`, used at `src/routers/http/router.rs:1084`).
   `cache_aware` scores an empty text as match rate 0 (`src/policies/cache_aware.rs:370-375`) and routes to the
   least-loaded worker. It behaved like round robin: 0.642 and 0.703, with each session visiting 3.9 workers on average.
   The www-2027 thresholds do not change that. The body field `session_params.session_id` would give it a key; that was not run.
4. **Both routers pass `x-session-id` on to the workers.** Every request seen by a worker's middleware carried the
   header, through vllm-router and through Envoy.
5. **vLLM listens off loopback.** Each worker had its API port on 127.0.0.1 and six more TCP listeners on the node's
   cluster address, although `--host 127.0.0.1` and `VLLM_HOST_IP=127.0.0.1` were set. They are presumably internal
   process-to-process ports; which component opens them was not traced. The routers, the EPP and Envoy listened
   on loopback only (`results/gh200-1gpu/arms/*/listeners.txt`, `listeners-smoke.txt`).
6. **Cost of the router processes**, from `ps` at the end of each arm (CPU time includes start-up):
   - the EPP: 93 to 98 MB resident, 10 CPU-seconds per arm of about 80 s, mostly its 50 ms scrapes of four workers;
   - Envoy: 63 MB, 3 to 4 CPU-seconds;
   - vllm-router: 68 to 99 MB, 2 CPU-seconds.

## What this means for HBEP-5

1. **Inner router (open question 2).** Phase B supports vllm-router `consistent_hash` on `x-session-id` as a
   provisional default for this workload. It gave the best hit rate under KV pressure, it is one process, and it
   installs from PyPI as an aarch64 wheel. Phase B ran on one shared GPU: a dedicated-GPU run must validate hit rate,
   routing balance and latency before the default is fixed.
   - Phase A's conditions still hold: set `--request-timeout-secs` above the longest stream, and re-add workers after
     a router restart (L-4.8).
   - The hash can skew with few sessions: 14 to 32 sessions per worker here, 7 of 9 on one worker in phase A.
   - It does not react to load: a hot session stays on its worker however busy that worker is.
2. **llm-d stays the alternative**, with one change before it is adopted: a scorer weighting that does not let a
   shared system prompt decide the worker, or a 4-GPU run showing that its queue and KV scorers spread the sessions
   once a worker queues. As shipped, a few agent products would each land on one worker.
3. **Do not use vllm-router `cache_aware` for chat traffic** unless the front sets `session_params.session_id`.
   Without it the policy is least-load.
4. **The front should pass its conversation key as `x-session-id`.** Both routers forward it, and the hash needs it.
   For vllm-router any of its keys would do (phase A).
5. **Reachability (HBEP-6).** vLLM opens six internal ports per server on the node's address, beyond `--host`. An
   Instance on a shared cluster network exposes them unless the provider's network isolates it.
6. **arm64.** The EPP, Envoy and vllm-router run on GH200 nodes with a 64 KiB-page kernel, with no container.

## Unknowns

- **Hit rate, routing balance and latency with one GPU per worker.** One GPU served all four workers here (see
  Deviation). A 4-GPU run is needed to validate all three, and to measure the cost of an unbalanced router.
- **Whether llm-d's load scorers break up the pile-up under load.** Here the sampled queue maxima were 0 to 2 and
  nothing was preempted. A heavier load, or the 4-GPU run, would show whether the hot worker sheds sessions, and at
  what hit-rate cost.
- **Scale and length.** 96 sessions of 12 turns with prompts up to 10,760 tokens. www-2027's agents ran 50,000 to
  100,000-token contexts for hours. Session pauses over llm-d's 300 s binding TTL were not exercised (sessions lasted
  16 to 26 s).
- **Run-to-run spread.** Only three B arms were repeated: round robin 0.635 and 0.654, `consistent_hash` 0.902 and
  0.902, llm-d 0.880 and 0.884. The gaps between routers are about ten times that spread. The repeats are not
  independent trials: they share the worker processes, the job and the seed, and they differ in hash assignment and
  sampled replies (see Method).
- **Untested settings.** `cache_aware` with `session_params.session_id`, or with its default thresholds;
  llm-d's precise prefix scorer (KV events).
- **Keep-alive pairing (L-2.5)** was not exercised: under continuous load no connection idled for vLLM's 5 s, and no
  request failed.
- **Which vLLM component opens the six non-loopback ports.**

## Components and provenance

| Component | Project and URL | Licence | Version | How it was used |
|---|---|---|---|---|
| vLLM | https://github.com/vllm-project/vllm | Apache-2.0 | 0.32.0+cu129 (aarch64 wheel) | an existing venv from www-2027's Helios serving, used read-only |
| vllm-router | https://github.com/vllm-project/router | Apache-2.0 | 0.1.16 (PyPI aarch64 wheel) | from the same venv; source read at commit db02f35 (phase A's pin) |
| EPP | llm-d router, https://github.com/llm-d/llm-d-router | Apache-2.0 | v1.0.0, b9393d1 | built from source for arm64 with phase A's patch (`third_party/forked/llm-d-router/`) |
| Envoy | https://github.com/envoyproxy/envoy | Apache-2.0 | 1.33.2 | release binary |
| llm-d guide configs | https://github.com/llm-d/llm-d, `guides/no-kubernetes-deployment` | Apache-2.0 | v1.0.0, 378b42d | generated at run time by phase A's `genconf.py`; not copied here |
| Model | https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct | Apache-2.0 | 989aa79 | downloaded in a CPU job with an existing `hf` CLI venv |
| Serving recipe | www-2027's Helios scripts (module `ML-bundle/25.10`, `VLLM_USE_FLASHINFER_SAMPLER=0`, `FLASHINFER_DISABLE_VERSION_CHECK=1`) | | | the env settings, reused |
| www-2027 lesson | `cache_aware` thresholds 0.8 / 1 / 1.1 | | | the `vr-cache` arm |

New in this phase, all under `spikes/s2b/`:

- `cmd/s2bload`: the workload client (Go standard library);
- `py/s2b_placement.py`: the placement middleware;
- `py/s2b_listeners.py`: listener lists from `/proc`;
- `scripts/job.sh`: the job;
- `scripts/analyze.py`: the analysis;
- `scripts/collect.sh`: the results filter;
- `scripts/pubcheck.py`: the publication check (private paths, account names, addresses), which `collect.sh` runs.

## Reproduce

```bash
spikes/s2b/scripts/test.sh    # every test, then checks that summary.md reproduces from the committed results
```

It runs the client's Go tests (workload structure, SSE framing and the usage and completion checks, cached-token
parsing), the middleware's (tag of the newest turn, body replayed unchanged), the `/proc` listener list's (IPv4, IPv6
wildcard, connected sockets ignored), the analysis' (hit rate from counters, reuse bound, placement figures), the
publication check's (paths, accounts, addresses, nested archives, `collect.sh` refusing a leak inside a file it
gzips), and `job.sh`'s exit status (a stubbed run). Expected values in the analysis tests are worked out by hand from
the fixtures. It ends with the publication check over every file git would commit under `spikes/s2b` and over the
diff against `origin/main`. `scripts/analyze.py spikes/s2b/results/gh200-1gpu` prints the full table.

Before every push, run `python3 -I -B spikes/s2b/scripts/pubcheck.py --ips --history origin/main` as well: it reads every
blob of every commit being published, not only the tip.

The job itself:

1. Stage into `$S2B_HOME/stage/`:
   - `bin/{epp,envoy,s2bload}` for arm64;
   - `py/`;
   - `genconf.py` from `spikes/s2/scripts/`;
   - `src/llm-d/guides/no-kubernetes-deployment/router/{envoy/envoy.yaml,epp/config.yaml}` from llm-d v1.0.0;
   - `job.sh`.
2. Put the model, at the documented revision, in `$S2B_HOME/hf_home`:

```bash
HF_HOME=$S2B_HOME/hf_home hf download Qwen/Qwen2.5-1.5B-Instruct --revision 989aa7980e4cf806f80c7fef2b1adb7bc71aa306
```

   `job.sh` serves that revision (`--revision "$MODEL_REV"`, offline), so another cached revision cannot be picked up
   by name. vLLM 0.32.0 uses the model revision for the tokenizer as well.
3. Submit:

```bash
sbatch -A <account> -p plgrid-gpu-gh200 -q now -N 1 --ntasks=1 --cpus-per-task=72 --mem=120000M --gres=gpu:1 -C memfs \
  -t 01:30:00 --no-requeue --export=ALL,S2B_HOME=<dir>,VLLM_VENV=<vllm 0.32.0 venv>,RESULT_TAG=<tag> $S2B_HOME/stage/job.sh
spikes/s2b/scripts/collect.sh <fetched results dir> spikes/s2b/results/<tag>     # keeps no logs, paths or node addresses
```

To rehearse without a GPU, run `MODE=sim SIM_BIN=<llm-d-inference-sim> VR_CMD=<vllm-router command> job.sh`: simulators
stand in for vLLM and the x86 binaries of phase A replace the arm64 ones. That checks the plumbing, not the numbers.
