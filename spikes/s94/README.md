# Issue #94, step 1: the allocation agent against a real vLLM on a GH200 GPU (HBEP-7, HBEP-8)

The agent (`hb-agent`, #87 and #115) had been tested only against engine doubles. Those doubles were built from a reading of vLLM v0.32.0's source:

- the metric names;
- `cache_config_info` reporting settings verbatim;
- `delta.reasoning`;
- how a stream ends;
- the warm-up shapes.

This step ran the real agent against a real vLLM v0.32.0 in a SIF on one Helios GH200 GPU. A controller double ran on the same node, over loopback, with no tunnel. The double recorded every report, and the run also captured the engine's own bytes to judge those assumptions against.

## Result in brief

- **The run.** `hb-agent` (main 469acd5, linux/arm64) took two Instances through staging, verification, qualification, warm-up, idle and stop.
  - Every qualification check passed on both.
  - The reasoning model's warm-up did not settle in five rounds. Under HBEP-7 that fails qualification (finding 3).
- **Settings.** The engine reported its settings verbatim, as HBEP-7 says. Every declared value it reports compares equal under HBEP-7's rule.
- **Health.** Every field, the five from #115 included, equals the engine's own `/metrics`. `metricsReadAt` advanced with every 10 s probe.
- **What the agent assumed.** Its assumptions about vLLM held:
  - the metric names;
  - `delta.reasoning`, never `reasoning_content`;
  - the stream's end: a finish reason, then the usage, then `[DONE]`.
- **Findings.** Eight, listed below. Their fixes are issue #125.

## Setup

**The driver.** `TestRealEngine` and `TestRealEnginePlan` live in `cmd/hb-agent` (`realengine_test.go`, `realengine_plan_test.go`). They stay there, not in this directory, for two reasons:

- they reuse that package's end-to-end harness (`startEndpoint`, `writeCert`): the real agent API server, with a pinned TLS certificate, over `pkg/agentapi/fake`'s `Backend`;
- they need the module's internal packages, which a spike module cannot import.

Both skip unless their variables are set, so `go test ./...` and CI only compile them. `TestRealEngineJudgements` checks the double's judgements on hand-made inputs.

**What the driver does:**

- **Starts the agent.** It runs the `hb-agent` binary under test (not a re-exec of the test binary), with `agent.json` in direct mode, the endpoint on loopback and a well-formed stand-in `frpcDigest`.
  - frpc is on main since #108. With no frps and no staged frpc, it retries and the Instance is unaffected.
- **Scripts the controller's sequence** for each Instance:
  1. run with verification, including the identity record's file list;
  2. qualify and warm up under the same verification generation;
  3. drop warm-up from the desired state once a round settles (HBEP-7's rule) or five rounds have run;
  4. hold three probe intervals;
  5. idle.

  It sets `keyStored` once a report carries the key, and stops the agent at the end.
- **Records every report** as it arrives, redacted (`Report.Redacted`).
- **Reads the engine directly** over the worker's socket, with the Instance's key, and keeps the raw bytes. These captures are the evidence for the mismatch list, not the agent's parsing of them:
  - `/version` and `/v1/models`;
  - `/metrics` before the checks and after warm-up;
  - a streamed chat that stops, a stream cut by `max_tokens`, a streamed tool call, and a non-streamed chat.

**The file lists** are the identity record's, read from the Hub by `pkg/hubfiles` on the VM (`TestRealEnginePlan`), not by the agent's stager.

**The Instances.** Two, one after another, on one Allocation. Each launch also declares:

- `--max-num-seqs=8 --gpu-memory-utilization=0.50 --block-size=16 --enable-prefix-caching --kv-cache-dtype=auto`;
- `--enable-auto-tool-choice --tool-call-parser=hermes`;
- the engine's caches pointed at the run directory, and `VLLM_USE_FLASHINFER_SAMPLER=0` (www-2027's Helios recipe, as in S2b).

Both run with tool calls on and 8 target sessions.

| | A | B |
|---|---|---|
| Model | `Qwen/Qwen3-0.6B` @ c1899de | `Qwen/Qwen2.5-1.5B-Instruct` @ 989aa79 (spike S2b's revision) |
| Reasoning parser | `--reasoning-parser=qwen3` | none |
| `--max-model-len` | 8192 | 4096 |
| Context shapes (short / typical / longest) | 128 / 1,024 / 4,096 | 128 / 1,024 / 3,072 |
| Output shapes (typical / longest) | 128 / 512 | 128 / 512 |

**The image.** `docker.io/vllm/vllm-openai@sha256:befccf0e7e4089aa23926bb6f887a85c0d46c2a99e6de5c5ffdf5cb7957c5b34`, the v0.32.0 tag's multi-arch index.

- The arm64 manifest has 40 layers, 10.3 GB compressed, built on CUDA 13.0.2. Apptainer picked it on the node.
- No vLLM SIF existed on the cluster, so the agent pulled it itself.

**Environment.**

| Item | Value |
|---|---|
| Partition | Helios GH200 |
| Job | 1 GPU, 72 CPUs, 120,000 MB, `-C memfs`, `--no-requeue`, QOS `now`, 57 min limit; it ran 10 min 40 s |
| Node | aarch64; NVIDIA GH200 120GB (97,871 MiB visible); driver 615.71.09, CUDA UMD 13.4; Apptainer 1.5.4 (setuid) |
| Agent | `hb-agent` main 469acd5, `CGO_ENABLED=0 GOARCH=arm64`, Go 1.27.3 |

Nothing was measured on the VM: it only built, submitted and fetched.

**The queue.** `sbatch --test-only` was run for 1 GPU on the largest active GH200 grant, with `--no-requeue`:

- the normal QOS estimated a start 16 days out;
- QOS `now` estimated 13–29 min, and the job started at once.

Two earlier submissions stopped at the job's own guards after 3 s each:

- one on finding 1;
- one on an unparsable nvidia-smi banner. The CUDA guard now checks the driver version: CUDA 13.0 needs driver 580 or later.

## Rerun

1. **Build and write the plan** on the VM, then copy the outputs to `<dir>` on the cluster:

   ```bash
   CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags "-X main.version=main-$(git rev-parse --short HEAD)" -o bin/hb-agent ./cmd/hb-agent
   CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c -trimpath -o bin/hbagent.test ./cmd/hb-agent
   HB_REAL_PLAN_OUT=$PWD/plan.json go test -count=1 -run '^TestRealEnginePlan$' ./cmd/hb-agent   # needs the Hub
   ```

   Copy `bin/`, `plan.json` and `spikes/s94/job.sh` to `<dir>`, and create `<dir>/logs`.
2. **Check the queue, then submit:**

   ```bash
   sbatch --test-only -A <account> -p plgrid-gpu-gh200 -q now -N 1 --ntasks=1 --cpus-per-task=72 --mem=120000M --gres=gpu:1 -C memfs -t 00:57:00 --no-requeue --wrap true
   sbatch -A <account> -p plgrid-gpu-gh200 -q now -N 1 --ntasks=1 --cpus-per-task=72 --mem=120000M --gres=gpu:1 -C memfs -t 00:57:00 --no-requeue \
     -J hb-i94 -o '<dir>/logs/job-%j.out' --export=ALL,H=<dir>,RUN_TAG=<tag> <dir>/job.sh
   ```

3. **What the job does.**
   - It points TMPDIR at `$MEMFS` and refuses to start if TMPDIR is not tmpfs.
   - It checks the driver version and the Hub and registry, then runs `TestRealEngine`.
   - It lists any process of the job still alive at the end.
   - The record goes to `<dir>/run-<tag>/`: `summary.json`, `reports.jsonl`, `raw/`, and the agent's and engine's logs.
4. **Reproduce the numbers below from the committed record:**

   ```bash
   python3 -I spikes/s94/analyze.py spikes/s94/results/r3
   go test -count=1 -run '^TestRealEngineJudgements$' ./cmd/hb-agent
   ```

## Results

All numbers come from `results/r3`.

### Timeline

Times are from each Instance's run instruction (`reports.jsonl.gz`, `job-excerpt.txt`).

| | A | B |
|---|---|---|
| Image | pulled, built into a SIF (8.8 GB) and recorded in about 5.9 min | the same SIF, rehashed in about 7 s |
| Checkpoint downloaded from the Hub | 1,433 MiB in 17 s | 2,944 MiB in 30 s |
| Staging done | +368 s | +36 s |
| Engine up (worker `running`) | 74 s later (cold) | 52 s later |
| Verification, then qualification | 0.1 s, then 2.7 s | 0.1 s, then 1.3 s |
| Warm-up | 23 s, 5 rounds | 5 s, 2 rounds |
| Idle | the worker stopped in 0.5 s with exit code 0 | the same |

After the stop, the agent exited 0 and no process of the job was left.

### Checks

| | A (Qwen3-0.6B, reasoning) | B (Qwen2.5-1.5B) |
|---|---|---|
| File rehash | 10 of 10 match | 10 of 10 match |
| Engine version, image digest | 0.32.0; the declared digest | the same |
| Template overhead | 8 tokens | 29 tokens |
| Fingerprint | `vllm-0.32.0-f5c1e941` | `vllm-0.32.0-79f7fb36` |
| health, chat, toolCall, corruption | passed | passed |
| behaviour | passed at 3,960 tokens; by hand, 8192 − 8 − 128 − 4096 = 3,960 | passed at 3,072, the longest context (no parser) |
| KV capacity at start-up | 428,176 tokens, which is 26,761 blocks × 16, and `cache_config_info`'s `kv_cache_size_tokens` | 1,636,800 tokens, also equal to `kv_cache_size_tokens` |
| Served model name | `hb-test/qwen3-0.6b` | `hb-test/qwen2.5-1.5b-instruct` |
| Warm-up | **not settled after 5 rounds** (finding 3) | settled at round 2 |

### Settings: declared against reported

The values come from `cache_config_info` and, for max-model-len, from `/v1/models` (`raw/*/before-metrics.txt.gz`, `before-models.json`). Each pair was compared under HBEP-7's rule. Both Instances reported the same forms.

| Flag | Declared | Reported | Verdict |
|---|---|---|---|
| max-model-len | 8192 / 4096 | 8192 / 4096 | equal (integer) |
| gpu-memory-utilization | 0.50 | 0.5 | equal (as numbers) |
| enable-prefix-caching | the flag, read as true | True | equal (as booleans) |
| block-size | 16 | 16 | equal |
| kv-cache-dtype | auto | auto | resolved, recorded |

Six declared inputs are not reported: `max-num-seqs`, `served-model-name`, `uds`, `tool-call-parser`, `enable-auto-tool-choice` and `reasoning-parser`. That was expected; nothing the engine exports confirms `max-num-seqs`.

### Health

This is each Instance's last sample, idle after warm-up.

| Field | A | B |
|---|---|---|
| probeOK / consecutiveProbeFailures | true / 0 (omitted) | true / 0 (omitted) |
| metricsReadAt | the sample's time; reads every 10.0 s; at most 9.6 s old in any report | the same; at most 7.6 s old |
| runningRequests / waitingRequests | 0 / 0 | 0 / 0 |
| generatedTokens | 17,570 | 4,308 |
| kvCacheUsage | 0 | 0 |
| **preemptions** (#115) | 0 | 0 |
| **prefixCacheQueriedTokens** (#115) | 82,361 | 34,638 |
| **prefixCacheHitTokens** (#115) | 960 | 1,184 |
| **timePerOutputTokenSecondsSum** (#115) | 0.14563 | 0.08389 |
| **timePerOutputTokenCount** (#115) | 76 | 38 |
| responsesByStatus | {"2xx": 136} | {"2xx": 74} |

- Every value equals the engine's own `/metrics`, read by the driver after the last sample (`raw/*/after-metrics.txt.gz`).
- Of the 31 samples taken while the worker ran, none lacked a value and none had a failed probe.
- No sample was taken during start-up: Health appears from the first report with the worker running.

### TTFT and TPOT

These come from the warm-up timings. TPOT is (total − TTFT) / (outputTokens − 1).

| | A, last round (5) | B, last round (2) |
|---|---|---|
| concurrent (8 sessions, 1,024-token context, 128 out): mean TTFT / TPOT | 48.5 ms / 2.20 ms | 60.4 ms / 2.44 ms |
| short / typical / longest (1 request each): TTFT | 19.5 / 33.4 / 31.6 ms | 18.7 / 31.6 / 25.7 ms |
| longOutput (512 out): TPOT | 1.82 ms | 2.19 ms |

- The engine's own mean TPOT per finished request (sum over count) was 1.92 ms for A and 2.21 ms for B.
- A's TTFT is at the first reasoning delta, as HBEP-7 rules.
- All of this is one GPU and small models; it is not a capacity figure.

## Findings

The fixes are issue #125.

1. **On Helios GH200 the job's TMPDIR is on Lustre.** The RAM disk of `-C memfs` is `$MEMFS` (tmpfs). See `job-excerpt.txt`, the first lines.
   - HBEP-7 puts the engine's socket in "the job's private temporary directory". The agent also keeps there its run directory, and Apptainer's layer cache and build tree during a pull.
   - On Lustre that pull costs tens of thousands of inodes, on a scratch quota already near its inode limit. The first submission stopped at the guard for exactly this.
   - `job.sh` now sets `TMPDIR=$MEMFS/t` before it starts the agent. The bootstrap (HBEP-4, Submission) has to do the same, or the profile has to name the job's temporary directory.
2. **The staging report overshoots its total.** See `reports.jsonl.gz`, `job-excerpt.txt` and `hub-headers.txt`.
   - The final reports show `bytesDone` of 1,448.8 MiB against a `bytesTotal` of 1,444.6 MiB (A), and 2,955.4 MiB against 2,944.4 MiB (B).
   - `bytesTotal` grows only by a response's `Content-Length` (`internal/agent/stage/download.go`, `if resp.ContentLength > 0`).
   - The Hub sends the non-LFS files gzip-encoded with no `Content-Length`: 4.3 MiB for A, 11.0 MiB for B. Go's client asks for gzip and decodes it, so the length is unknown, but `bytesDone` still counts the bytes.
   - So done ends at the whole checkpoint, and total at the LFS files only. Those are exactly the two sums of the Hub's listing.
3. **The reasoning model's warm-up does not settle, so by HBEP-7 its Instance fails qualification,** although every check passed (`summary.json`, `warmUp`). There are two causes:
   - **The tool-call shape failed in rounds 3 and 5.**
     - In round 3 the reply spent its whole budget reasoning: 4,224 output tokens, which is 128 + 4,096.
     - In round 5 the model answered in text after 180 tokens.
     - A shape with no successful request means "not settled".
   - **The 10% tolerance is within the noise of a single request.** With one request per shape, requests under 300 ms moved by more than 10% between rounds 3 and 4: short went from 0.208 to 0.253 s (+22%), typical from 0.214 to 0.266 s (+24%).
   - The non-reasoning model happened to settle, with a largest change of +6.9%. The rule needs a ruling; the test was not loosened.
4. **The warm-up's error text mislabels an exhausted reasoning budget.** Round 3's tool call says "the answer calls no tool". HBEP-7 names this case `ReasoningBudgetExhausted` for the qualification checks. Warm-up errors are free text, so this is a nit.
5. **The engine writes into the home directory** (`home-writes.txt`).
   - Apptainer binds `$HOME` by default. HBEP-7's "a clean environment plus the launch's variables" does not cover HOME.
   - FlashInfer, the CUDA compute cache, tilelang and one more JIT cache wrote under `~/.cache/flashinfer`, `~/.nv`, `~/.tilelang` and `~/.humming`. That happened although `XDG_CACHE_HOME`, `VLLM_CACHE_ROOT`, `TRITON_CACHE_DIR` and `TORCHINDUCTOR_CACHE_DIR` pointed at the run directory.
   - The worker's exec needs `--no-home`, or `--home` on the run directory.
6. **A model's `generation_config.json` changes the defaults of the agent's requests** (`engine-excerpt.txt`).
   - vLLM logged the override: Qwen2.5 sets `repetition_penalty` 1.1, and both models set temperature, top_k and top_p.
   - The agent sends `temperature: 0` but no repetition penalty, so its "greedy" requests, including HBEP-7's corruption check, carry 1.1.
   - The check passed. The HBEP does not state the assumption.
7. **Report and Health times carry the node's local offset** (`+01:00`), not `Z` (`reports.jsonl.gz`). That is valid RFC 3339, but anything that compares the strings misorders them.
8. **The open-file limit cannot be raised.** vLLM warns that the hard limit of 32,768 blocks its raise and that fd errors are possible under load (`engine-excerpt.txt`). This is a profile fact for HBEP-4.

## What matched the HBEPs

Everything below is from the raw captures in `raw/`.

- **Metric names (HBEP-8).** Each of these is present, as one series labelled `engine="0"` and `model_name`:
  - `vllm:num_requests_running` and `vllm:num_requests_waiting`;
  - `vllm:generation_tokens_total` and `vllm:kv_cache_usage_perc`;
  - `vllm:num_preemptions_total`;
  - `vllm:prefix_cache_queries_total` and `vllm:prefix_cache_hits_total`;
  - `vllm:request_time_per_output_token_seconds_sum` and `_count`.

  So are `vllm:cache_config_info` and `http_requests_total`.
- **HTTP statuses.** `http_requests_total` counts by status class (`2xx`) per handler: `/v1/chat/completions`, `/v1/models` and `/tokenize`. It does not count `/metrics`, `/health` or `/version`.
- **Verbatim settings (HBEP-7).** `cache_config_info` holds `str()` of each parsed field: `"0.5"` for a declared `0.50`, `"True"`, `"16"` and `"auto"`.
- **`delta.reasoning`.** Streamed reasoning arrives as `delta.reasoning`, and the non-streamed reply carries `message.reasoning`. No `reasoning_content` appeared.
  - Every reasoning stream's first non-empty delta is reasoning.
  - The content after it starts with `"\n\n"`.
- **How a stream ends (HBEP-5's completion rule).** It ends the same way for stop, length and tool calls:
  1. a chunk with an empty delta and the finish reason;
  2. with `include_usage`, a chunk with empty `choices` and the usage;
  3. `data: [DONE]`.

  Other details:
  - Every event, the last included, ends with a blank line.
  - The first chunk is the role with an empty content (`""`).
  - `system_fingerprint` is only on the usage chunk, so a stream without usage carries none. The agent reads the fingerprint from a non-streamed request.
- **Tool calls.** The first `delta.tool_calls` carries the id, type and name. Later ones carry pieces of the arguments, beside `content: null`.

## Unknowns

- **A failure mid-stream** was not exercised. vLLM's error event followed by `[DONE]` (HBEP-5) still rests on the source reading.
- **Scale.** The models were small and ran on one GPU at 8 sessions. Larger models, longer contexts and more sessions were not run.
- **The Docker Hub pull limit for the cluster's address.** The anonymous HEAD returned no rate-limit headers.
- **A kept Allocation's second Instance after a restart.** The second Instance here followed an idle. Restarts (the alpha ladder's rungs) were not run.

## Files

`results/r3/` holds the record of the run, redacted: no job IDs, hosts, users, accounts or paths.

| File | What it is |
|---|---|
| `plan.json` | the two launches and their identity-record file lists |
| `summary.json` | the double's summary: verification, qualification, warm-up rounds, the last Health sample, the throughput and the registration (node removed) |
| `reports.jsonl.gz` | all 132 reports as they arrived, redacted by `Report.Redacted`, run-directory paths replaced by `<run-dir>` |
| `raw/<model>/` | the engine's own bytes, each with a `.meta.json` holding the request, status and content type: `/version`, `/v1/models`, `/metrics` before and after (gzipped), and the stop, length and tool-call streams and a non-streamed chat |
| `job-excerpt.txt` | the job's pre-checks and the driver's progress lines |
| `engine-excerpt.txt` | the engine log lines the findings cite, with process IDs removed |
| `hub-headers.txt` | the Hub's response headers for a non-LFS and an LFS file, and the listing's sums (finding 2) |
| `home-writes.txt` | what the run wrote under the home directory, relative to it (finding 5) |

The other files:

- `job.sh`: the Slurm job.
- `analyze.py`: prints this README's numbers from a record (Python standard library only).

## Components and provenance

| Component | Project and URL | Version | Use |
|---|---|---|---|
| vLLM | https://github.com/vllm-project/vllm | v0.32.0, image `vllm/vllm-openai` (index digest above), arm64 | the engine under the agent |
| Apptainer | https://github.com/apptainer/apptainer | 1.5.4 on the nodes | run by the agent to pull and exec the image |
| Qwen3-0.6B | https://huggingface.co/Qwen/Qwen3-0.6B | c1899de | Instance A |
| Qwen2.5-1.5B-Instruct | https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct | 989aa79 | Instance B |
| Serving recipe | www-2027's Helios scripts (`VLLM_USE_FLASHINFER_SAMPLER=0`), as in spike S2b | | the launch's environment |

New in this step:

- `cmd/hb-agent/realengine_test.go` and `realengine_plan_test.go`: the driver, the plan and the judgements;
- `spikes/s94/job.sh` and `analyze.py`.
