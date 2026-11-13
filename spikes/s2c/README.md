# Spike S2c: the router inside an Instance, one GPU per worker (Athena A100)

Issue #62, retargeted on 13 Nov 2026 from Helios GH200, whose nodes are down, to Athena `plgrid-gpu-a100`. S2c
decides beta's router default (HBEP-5, Inside an Instance; HBEP-7, the router opt-in) between the two routers the
product can run, on four real vLLM workers with a GPU each.

## Status

**The harness is built and proven on the fake Slurm with the engine double. No live run yet.** The live run waits for
Athena's A100 queue and an owner go, and needs one staging step with its own go first (Running it live). There are
no results in this directory yet.

## Question

With one worker per GPU, which router keeps the vLLM prefix cache warm without piling sessions onto one worker, and
what does it cost in time to first token, compared with round robin?

The pass rule is issue #62's: a router passes if it beats round robin on hit rate, with no worker carrying more than
40% of the requests, and a TTFT p95 no worse than round robin's. If Envoy `ring_hash` passes, it becomes the default.

## Arms

| Arm | Router | Started as |
|---|---|---|
| `rr` | vllm-router 0.1.17 `--policy round_robin` (the baseline) | S2b's command, through the relay (below) |
| `vr-hash` | vllm-router 0.1.17 `--policy consistent_hash` on `x-session-id` (the fallback's placement) | the same |
| `envoy-bound` | Envoy 1.40.2 `ring_hash` on `x-session-id`, bounded loads at `hash_balance_factor` 125 | the agent's own configuration and start (below) |
| `envoy-nobound` | the same configuration with `hash_balance_factor` removed, so the bound's effect is measured | the same |
| `probe` | the dead-worker probe behind `envoy-bound` | `s2crouter probe` (below) |
| `relay-control` | no router: worker 0's traffic straight to its socket, then through the relay | the relay's cost, measured (below) |

llm-d is out: S2c decides between the two routers the product can run (the science agent on #62, 13 Nov 2026).
`job.sh`'s dispatch table (`arm_def`) takes another arm as one case and a start function.

## Method

**One job, everything inside it** (`scripts/job.sh`). It starts four vLLM workers, one per GPU, all at once. For
each arm it resets every worker's prefix cache, starts the router, runs the client through it and stops the router.
It ends by itself after the last item of `PLAN`.

**The workers run as the agent runs them.** `s2crouter worker` builds the agent's own `agent.WorkerExec`
(`internal/agent/apptainer.go`). That gives `apptainer exec --nv --cleanenv --no-home`, the staged checkpoint bound
read-only at `/hpcburst/model`, the run directory at `/hpcburst/run` and the cache directory at `/hpcburst/cache`.
The command is the one the lifecycle controller renders: `vllm serve /hpcburst/model --uds
/hpcburst/run/worker-<i>.sock --served-model-name ...`, the flags one argument each, then `--max-num-seqs`. Each
worker gets the i-th entry of the job's `CUDA_VISIBLE_DEVICES` as its own (the agent's `workerDevices`) and vLLM's
keep-alive at 120 s, as a worker behind a router does. It runs the product's staged image as it is: the Apptainer
sandbox of `examples/modeldeployment-qwen25-instruct.yaml`'s image, which the pre-staging row builds on A100.

**Envoy is the agent's own.** `s2crouter envoy` renders the bootstrap with `agent.RenderEnvoyConfig`, which
`agent.CheckEnvoyConfig` must accept. The workers sit on their engine sockets, health-checked through the agent's
readiness endpoint (`agent.NewReadiness`), with every worker verified. Envoy runs bare from the binary staged by
digest (`agent.StagedEnvoyPath`), and an arm changes at most one field:
- **When the arm's bound is what the agent renders**, `agent.StartEnvoy` starts it, exactly as the agent does. That
  covers the pin, the glibc check, the guard, the process group, the socket wait and the listener check.
- **Otherwise** (an edited configuration), the same steps run with the edited bytes. `agent.StartEnvoy` cannot run
  them: it renders its own, and #518's guard pins the factor to 125.
- **Every job first starts Envoy once through `agent.StartEnvoy` unchanged** (the product smoke). It records Envoy's
  `/proc` argument vector and environment, and every edited start must match them. It also records the node's glibc.
- Each arm records the agent's rendering, the bytes Envoy was started with, their sha256 and the paths that differ.
  That is at most `...consistent_hashing_lb_config.hash_balance_factor`.

**The bound.** HBEP-5 decided bounded loads at `hash_balance_factor: 125` provisionally (13 Nov 2026, #520), and
#518's PR 1 adds them to the agent's configuration. Until it merges, the bound arm's
factor is the harness parameter `BOUND`, default 125. Before the merge, `envoy-bound` is the edited start and
`envoy-nobound` the product's. After it, the other way round, with no change to the harness
(`cmd/s2crouter/bound_test.go` pins both cases).

**vllm-router and the relay.** The workers serve on Unix sockets, and vllm-router takes only `http://` worker URLs.
So the two vllm-router arms reach the workers through `s2crouter relay`, one loopback TCP listener per worker that
copies bytes to the worker's socket and counts connections. That adds one loopback hop to `rr`'s and `vr-hash`'s
time to first token; the Envoy arms have none. The relay control, S2c's own addition (the science agent accepted it on
#62 as an optional step), measures it: worker 0 alone takes a short load
(16 sessions of 6 turns, 4 in flight) straight to its socket, then the same through the relay, each after a cache
reset, and the summary gives the TTFT the relay added at p50, p95 and p99. The client (`cmd/s2cload`) is S2b's `s2bload` with one addition,
`-unix`, which dials the Envoy arms' listener, the agent's `router.sock`.

**The workload** is S2b's (`cmd/s2cload`), seed 1 in every arm:
- 3 agent types with popularity 0.5 / 0.3 / 0.2, each with a shared 2,000-word system prompt;
- 96 sessions of 12 turns, 32 in flight;
- tool results of 200 to 800 words and 48-token replies;
- 0.5 to 2.5 s of think time, and `x-session-id` on every request.

The run label salts every system prompt and every session key, and every worker's prefix cache is reset
(`POST /reset_prefix_cache`, `VLLM_SERVER_DEV_MODE=1`) before each arm, so no arm reuses another's blocks.

**Three steps** (the letter of a `PLAN` item):
- **B:** each worker's KV cache capped at 6,000 blocks (96,000 tokens), S2b's load. This is where affinity matters
  (S2b, regime B).
- **H:** the same cap with twice S2b's concurrency: 192 sessions, 64 in flight. Envoy gives each worker
  ⌈⌈(active requests + 1) × 1.25⌉ ÷ workers⌉ slots and skips only a worker above them (HBEP-5:133). With two
  workers, #518 measured the sixth request on one worker as the first to spill. In S2b's own samples, its workload
  kept 1.0 to 2.0 requests running per worker on average, peaking at 3 to 7, on one shared GH200. At four workers
  that gives about 2 to 3 slots each, so only S2b's peaks would cross the bound, and with a GPU per worker requests
  finish sooner and fewer overlap. The bound and no-bound arms could then look the same. H gives the bound load to
  act on. The summary reports each step's peak in-flight per worker, which shows whether it got there. The science
  agent accepted H on #62.
- **A:** 33,010 blocks, S2b's profiled size, S2b's load.

Both regimes' budgets are explicit (`--num-gpu-blocks-override`), so they equal S2b's whatever the A100's memory.

**The plan**, in this order: B1 (`rr`, `envoy-bound`, `envoy-nobound`, `vr-hash`), B2 (the same in reverse, for the
run-to-run spread), H1 (the four arms), the probe on H's workers, A1 (the four arms), then the relay control. The job's time limit is
75 minutes. A budget guard starts no arm once about 70 minutes are used, so A goes first if time runs short, as the
science agent ruled, and the optional relay control before it. A skipped arm is listed in `failures.txt`, and the job exits 1 with its other results kept.

**Signals.** Every signal the job's own code sends goes through a pidfd (`s2crouter signal` and `stop`,
`cmd/s2crouter/proc.go`), never by name, pattern or process group. Two signals come from elsewhere: coreutils
`timeout` ends the client it started (its own unreaped child), and the agent's `procgroup` stops the Envoy it started,
in Envoy's own process group, as the agent does. A process is signalled only if it is the job's own: the root's parent must be
`job.sh`, and every other member must descend from it. Its start time is read again after its pidfd is opened, so a PID
that bash has reaped and the kernel has handed to another process is left alone. The probe's kill of the victim is the
same.

**The dead-worker probe** (`s2crouter probe`, on `envoy-bound`). It follows #510's step (c), and its three checks are
the science agent's. Every worker's key is read from Envoy's own per-host counters, then:
1. A long stream runs on every worker, and the last worker is killed (SIGKILL to its process tree, through pidfds)
   while they stream. The siblings' streams must end uncut.
2. From the kill on, a request in the victim's key goes every 250 ms. Each must be answered 200, by a sibling, never
   a 503.
3. The job restarts the victim. The probe holds it unverified for 15 s (three of Envoy's health checks), counted from
   the moment its engine answers AND Envoy no longer holds it ejected. The dead worker's connect failures set off
   outlier detection, which ejects it for 30 s or more, and during that it takes nothing whatever the readiness says.
   So the hold starts only once the readiness gate alone keeps it out, and every read in the hold must show Envoy's
   active check of it failing. The probe then verifies it with one request straight to its socket, and only then flags
   it verified, which the agent's readiness endpoint reports to Envoy. Until then Envoy's `rq_success` for the victim
   must not grow, and its placement log must show no probe request. After it, a request in the victim's key must
   reach the victim again.

`probe.json` records each check and the timeline: when the victim was healthy again, when its ejection ended, when
the hold started, when it was verified and admitted, plus the longest probe request during the outage.

**A known failure, pending the science agent's ruling.** The dry run's probe sees one 503 in most runs (3 of 4 alone):
a request Envoy writes to the victim's pooled connection within about a millisecond of the kill, which Envoy answers
"upstream connect error or disconnect/reset before headers. reset reason: connection termination". The agent's retry
policy retries nothing written to a worker, by design (`internal/agent/envoyconf.go`; the residual accepted on #507).
`probe.json` counts such a request as `cut_at_kill`, apart from any later failure (`non_200_later`). The no-503 check
still counts both until the ruling.

## What each output means

The summary (`scripts/analyze.py RESULTS_DIR`) gives one row per arm:

| Column | Meaning | From |
|---|---|---|
| Hit rate (workers) | growth of `vllm:prefix_cache_hits_total` over `vllm:prefix_cache_queries_total` during the arm, summed over the workers | each worker's `/metrics`, before and after |
| Hit rate (client) | the client's sum of `cached_tokens` over `prompt_tokens`, beside it as a cross-check | `client.jsonl` |
| TTFT p50 / p95 / p99 | from sending a request to its first content delta, through the router, nearest rank | `client.jsonl` |
| Requests and share per worker | requests each worker served in the arm | each worker's placement log |
| Sessions per worker, sticky | sessions by their first turn's worker; the share of sessions whose turns all stayed on one worker | placement logs |
| Busiest / mean | the busiest worker's requests over the mean | placement logs |
| Peak in flight | the most requests one worker had open at once in the arm | placement logs (start and end of each request) |
| Router counts | the router's own count per worker | Envoy's `/clusters?format=json` per-host `rq_total`, or vllm-router's `vllm_router_processed_requests_total{worker}` mapped through the relays |
| Checks | the per-arm check below | |
| Pass rule | issue #62's three parts against the `rr` arm of the same step and repeat | |

**The per-arm check** (`analyze.py --check-arm`; `job.sh` runs it after every arm, and a failure fails the job)
proves which router served, from files alone:
1. The router's own count per worker equals that worker's own placement count.
2. Their sum equals the client's attempts.
3. For vllm-router, every policy decision is under the arm's policy label.
4. For Envoy, the configuration it was started with has the sha256 it recorded and the arm's bound, and differs from
   the agent's rendering in at most the bound's field.
5. The other router family served nothing.

An arm that silently fell back to another router fails it. The dry run shows that with fault seams.

## Environment and builds

| Component | Project and URL | Version | How it is used |
|---|---|---|---|
| vLLM | https://github.com/vllm-project/vllm | 0.32.0, the image `docker.io/vllm/vllm-openai@sha256:befccf0e7e4089aa23926bb6f887a85c0d46c2a99e6de5c5ffdf5cb7957c5b34` (the multi-architecture index S2b used) | the product's staged Apptainer sandbox, as it is; S2b ran the same vLLM version from a GH200 venv |
| Model | https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct | revision `989aa7980e4cf806f80c7fef2b1adb7bc71aa306` | the product's staged checkpoint, as S2b served it |
| Envoy | https://github.com/envoyproxy/envoy | 1.40.2, `envoy-1.40.2-linux-x86_64`, sha256 `1d64506e25c1844f146162ddb2ece7010fa4e784faddb40a079021491f1f03cc` | the agent's pin (`internal/agent/envoypin`), staged by digest, run bare |
| vllm-router | https://github.com/vllm-project/router | 0.1.17, the PyPI wheel `vllm_router-0.1.17-cp38-abi3-manylinux_2_28_x86_64.whl`, sha256 `85c96bea4d573a49b5a8ae4c72ee3dfe733aba26b6f1ff1256e6142f1f42694d` (spike S13's pin) | a venv; counters read at commit `3451e66` |
| The agent's pieces | this repository, `internal/agent` and `pkg/agentapi` | the commit the spike builds from | `RenderEnvoyConfig`, `CheckEnvoyConfig`, `StartEnvoy`, `NewReadiness`, `CheckEnvoySockets`, `WorkerExec`, `StagedEnvoyPath`; `envoypin`; `procgroup` |
| Workload client | spike S2b, `spikes/s2b/cmd/s2bload` | copied, plus `-unix` | `cmd/s2cload` |
| Placement middleware | spike S2b, `spikes/s2b/py/s2b_placement.py` | plus each request's end time | `py/s2c_placement.py` |
| Publication check | spike S2b, `spikes/s2b/scripts/pubcheck.py` | plus the A100 partition's public name | `scripts/pubcheck.py` |
| Engine double, fake Apptainer | this repository, `internal/agent/enginedouble` | | the dry run's workers, from a test binary |
| Fake Slurm | this repository, `internal/slurm/fakeslurm` | run mode | the dry run's cluster |

New in S2c, all under `spikes/s2c/`:
- `cmd/s2crouter`: the worker, Envoy, relay and probe launcher;
- `scripts/job.sh`, `shape.sh`, `submit-line.sh`, `submit.sh` and `queue-check.sh`;
- `scripts/analyze.py`;
- `dryrun/`: the double's worker wrapper and the fake-Slurm run.

**The module.** `spikes/s2c` is a Go module of its own. It imports the root module's `internal/agent`, `pkg/agentapi`
and, in tests, `internal/agent/enginedouble` and `internal/slurm/fakeslurm`, through `replace ... => ../..`. Go's
internal rule goes by import path, so that is allowed. The root's `go vet ./...` never compiles it: run `go vet ./...`
and `go build ./...` in `spikes/s2c` (`scripts/test.sh` does). It changes nothing outside `spikes/s2c`.

The job records, with no host name, path, job ID or account (`env.txt`):
- arch, kernel and the node's glibc version (from `getconf GNU_LIBC_VERSION`, version only);
- the number of GPUs and `nvidia-smi`'s name, memory and driver;
- Apptainer's version, Envoy's sha256 and version, vllm-router's version;
- the settings.

The product smoke's `product-start.json` says whether the agent's own Envoy start ran on the node. That closes
HBEP-5's first-compute-node item (Envoy bare, glibc 2.30 or later) if it does.

## The dry run

`scripts/test.sh` runs every test and the dry run (`dryrun/dryrun_test.go`). The job goes, with its own sbatch line
(`scripts/submit-line.sh`), to the fake Slurm in run mode (`internal/slurm/fakeslurm`). The job runs on the host with
`CUDA_VISIBLE_DEVICES=0,1,2,3`, the partition GPU-only.
- **The workers** are the engine double (`internal/agent/enginedouble`), reached through the agent's own Apptainer
  invocation, which the double's fake Apptainer takes as it is. The double lacks a prefix-cache reset and a placement
  log, so the dry run's worker wraps it:
  - a reset that starts a fresh engine and answers `{"success":true}` (its counters start again, where vLLM's carry
    on; the analysis reads only their growth over an arm);
  - the placement line, written in Go.
- **The routers** are the real Envoy 1.40.2 and vllm-router 0.1.17.
- **The workload** is S2b's shape, scaled down: 16 sessions of 3 turns, 4 in flight; H: 32, 8 in flight. It proves
  the plumbing, not the numbers, and the double reports no cached tokens, so the client's hit rate is empty there.

It requires:
- the job's shape;
- each worker on its own device, each device started once per step and the victim's once more;
- every arm's output in the live run's shape, with all five checks passing;
- the probe's checks and timeline.

Three fault seams must each end the job FAILED with the arm named in `failures.txt`:
- `client-to-other-router`: an Envoy arm whose client is sent to vllm-router;
- `strip-bound`: the bound arm run without its bound;
- `early-admit`: the probe's Envoy told every worker is verified, so the restarted victim is admitted early.

The job's stub tests (`scripts/test_job_status.sh`) cover the failures it must report. `scripts/test_queue_check.sh`
covers the queue check and the submit line.

## Running it live

Each step below needs its own go, and the order matters.

1. **Stage the product's image and checkpoint on Athena's A100 nodes:** the pre-staging row
   (`docs/alpha-runbook.md`, `alpha-athena-now-stage`) does exactly this, with the agent's stager.
2. **Build vllm-router 0.1.17's venv** for the compute node's Python: the wheel above with launcher dependencies
   pinned by hash for that Python (`scripts/setup-vr.sh` does it for CPython 3.15 on x86_64; the compute node's
   Python needs its own pinned list). This is a separate staging step with its own go, not part of this PR.
3. **Stage `$S2C_HOME/stage`:**
   - `bin/s2crouter` and `bin/s2cload`, built for linux/amd64 from this directory;
   - `py/s2c_placement.py`;
   - `scripts/job.sh` and `scripts/analyze.py`;
   - Envoy at `envoy/<sha256>/envoy`.
4. **Check the queue, nothing submitted:** `scripts/queue-check.sh`, with `S2C_PRIVATE_SETTINGS` and
   `S2C_ATHENA_LOCK` set, prints only the start estimate for the job's own shape.
5. **Submit** with `scripts/submit.sh` (`S2C_CONFIRM=1`), setting `IMAGE`, `CHECKPOINT_DIR`, `VR_BIN` and `S2C_HOME`
   to the staged paths. It keeps the job ID in a private record and never prints it.
6. **Fetch the results directory** and run `scripts/analyze.py <dir>`. Then run `scripts/pubcheck.py --ips` over
   whatever is committed.

The job's shape (`scripts/shape.sh`): `plgrid-gpu-a100`, one node, `--gres=gpu:4`, `--cpus-per-task=64`,
`--mem=500G` (the alpha rows' one-GPU share, times four), `--time=01:15:00`, `--no-requeue`.

## Still unknown

- Every number: the live run has not happened.
- How long four workers take to start on A100, and so whether all of the plan fits the 75 minutes.
- Whether H's load is enough for the bound to act. Its peak in-flight per worker will say.
- vllm-router's venv on Athena's compute nodes (step 2).
