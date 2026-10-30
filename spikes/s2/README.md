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

RESULTS-BRIEF

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
  1,500-token stream (25 min) then starts on the same session, so it runs on X. After 20 s, X is removed
  from discovery (EPP: atomic rewrite of the endpoints file; vllm-router: `POST /remove_worker`). The
  removal counts as effective only if the same session then lands on another worker, and 8 fresh
  sessions avoid X, at +30 s, +2, +5 and +9 min. Only then is the survival of the stream judged. The
  session is the lever on purpose: a router that still held X would send the session back to X, whereas
  fresh sessions alone would avoid a busy X even before the removal.
- **Controls** (so that a pass can be told from a test that cannot fail): vllm-router with a 60 s
  deadline must cut a 150 s stream at about 60 s; llm-d with 5 s `ext_proc` timers must stream 600 s.
- **Shared host.** Everything ran at `nice -n 10` on a 6-core VM shared with other work, loopback only.

## Components and provenance

| Component | Project and URL | Licence | Version used |
|---|---|---|---|
| EPP (endpoint picker), file discovery | llm-d router, https://github.com/llm-d/llm-d-router | Apache-2.0 | v1.0.0, commit b9393d1, built from source (`CGO_ENABLED=0`) |
| Envoy | https://github.com/envoyproxy/envoy | Apache-2.0 | 1.33.2 release binary, the version llm-d's no-Kubernetes guide pins (`distroless-v1.33.2`) |
| llm-d guides (Envoy and EPP configs) | https://github.com/llm-d/llm-d, `guides/no-kubernetes-deployment` | Apache-2.0 | v1.0.0, commit 378b42d; read at run time, not copied into this repository |
| Engine double | llm-d-inference-sim, https://github.com/llm-d/llm-d-inference-sim | Apache-2.0 | v1.0.0, commit 7bda03f, built from source (`CGO_ENABLED=0`) |
| vllm-router | https://github.com/vllm-project/router | Apache-2.0 | 0.1.16 from PyPI (PyO3 wheel plus Python launcher), source commit db02f35 |
| Python launcher dependencies | aiohttp, fastapi, uvicorn, orjson, requests, setproctitle (PyPI) | various permissive | latest at run time; only the CLI wrapper imports them |

Reuse: all four routers and engines are community software used unmodified. New in this spike: the
test client, the stack scripts and the config generator, all under `spikes/s2/`. No third-party code
is copied into the repository, so there is no `third_party/forked` entry. The Envoy and EPP configs are
generated from llm-d's shipped files by exact-line substitution (`scripts/genconf.py`), which fails if a
line it expects has changed.

## Environment

Single VM, 6 cores, 31 GB, no swap, shared with other sessions: the 1-minute load during the 2 h run
was between 2 and 6. Python 3.15.0 for the vllm-router launcher. ENVIRONMENT-NOTES

## Deadlines on the path

DEADLINES

## Results

RESULTS

## What the results mean for the HBEPs

MEANING

## What phase B must measure

PHASEB

## arm64 (Helios GH200)

ARM64

## Reproduce

```bash
export S2_HOME=<an empty work directory, about 1 GB>
spikes/s2/scripts/setup.sh                 # pin, fetch, build; needs git, curl, go, python3 with venv
spikes/s2/scripts/run.sh controls          # ~11 min
spikes/s2/scripts/run.sh placement         # ~25 min
spikes/s2/scripts/run.sh removal           # ~27 min per router (pass a stack name to run one)
spikes/s2/scripts/run.sh epp-kill          # ~3 min
spikes/s2/scripts/run.sh long              # ~2 h 5 min: five 2 h streams in parallel
spikes/s2/scripts/run.sh summary
```

`run.sh` exits non-zero if an expectation is not met. Results land in `$S2_HOME/results/<scenario>/`:
one JSON verdict and one gaps CSV per stream, and the Envoy access log of each llm-d stack.
`SIM_ITL` and `SIM_TTFT` change the simulators' pace; `LONG_TOKENS` and `REMOVAL_TOKENS` change the
stream lengths (the expectations for the default-deadline cut assume the full length).

## Unknowns left

UNKNOWNS
