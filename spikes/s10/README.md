# Spike S10: conversation key and load bound (simulation)

Issue #27. HBEP-5, open questions 1 (conversation key) and 3 (load bound).

## Question

HBEP-5 keeps each conversation on the Instance that holds its prefix cache, by consistent hashing with bounded loads (CHWBL) on a conversation key. Which key, a client session header or a hash of the conversation's opening, and which bound beat round robin on prefix-cache hit rate without any Instance ever exceeding the bound? And what happens when an Instance drains or leaves?

## Answer

RESULTS_ANSWER

## Method

### Harness: KubeAI's balancer, copied

`third_party/forked/kubeai/` holds KubeAI's CHWBL balancer at commit `89dfff46`, with its Apache-2.0 licence. `NOTES.md` there lists every change: import paths, trimmed type files, no-op metrics, and the `loadbalancer` directory moved out of `internal/`. The algorithm is unchanged. The simulator calls upstream's `getBestAddr`, `reconcileEndpoints` and `removeEndpoint` through a thin exported wrapper, `sim_api.go`, and uses upstream's release function to decrement load. Pure affinity is the same ring with an effectively infinite bound (`MeanLoadPercentage` 2^40), so it differs from CHWBL only in the bound.

### Simulator (`sim/`)

A discrete-event simulation in Go, one goroutine per run, deterministic for a seed.

**Cache** (`cache.go`), modelled on vLLM V1's block pool:
- the KV cache holds a fixed number of blocks. Experiments use 64-token blocks to keep runs fast; tests use 16. The block-size check is under Results;
- a full block is identified by a chained hash of its parent block and its own tokens, so equal hashes mean equal prefixes;
- a request hits the longest resident prefix of its own chain. The last prompt token is always computed, so at most (P−1)/B blocks hit;
- a running request pins its blocks. When it finishes, its blocks stay cached and unpinned, and are evicted in LRU order only when space is needed;
- a finished request's blocks are freed last block first, as vLLM does, so a chain loses its tail before its head;
- output tokens are cached too, because the next turn's prompt contains the reply;
- a request reserves blocks for its prompt and its whole output at admission. If they do not fit, it waits. Nothing is preempted.

**Instance** (`engine.go`):
- at most `MaxRunning` = 48 sequences run at once. Further requests wait in the Instance's FIFO queue;
- prefill runs one request at a time at 25,000 uncached tokens per second, so misses cost time;
- decode takes 30 ms per output token, whatever the batch size;
- load is in-flight requests, KubeAI's measure, counted from dispatch to completion. It includes requests waiting at the Instance.

An Instance is meant as one node with four GPUs serving a mid-size MoE model, about 1–3 M tokens of KV cache. These numbers are assumptions, not measurements.

**Workload** (`workload.go`): a closed loop of 160 users. Each runs agent conversations back to back, starting spread over the first 300 s; no conversation starts after 4,800 s. All parameters are assumptions:

| Client type | Share | System prompt | First user message | Session header |
|---|---|---|---|---|
| agent-a | 35% | 12,000-token static part + one of 12 project tails (800 tokens, Zipf) | 400-token fixed preamble + unique task | yes |
| agent-b | 25% | 6,000 static + one of 12 project tails (500, Zipf) | 150-token fixed preamble + unique task | no |
| agent-c | 15% | one 2,500-token prompt for all | unique task | no |
| script | 25% | unique, 300–3,000 tokens | unique task | no |

- Task: lognormal, median 300 tokens (30–4,000).
- Sessions: 60% short (2–6 turns), 40% long (15–60 turns).
- Each turn appends a tool result or user message (lognormal, median 900, 20–16,000) to the previous prompt and reply, so prefixes grow. The reply is lognormal, median 250 (20–2,000).
- Think or tool time: lognormal, median 3 s (0.2–120 s).
- Compaction: when the next prompt would pass 100,000 tokens, the context restarts as system prompt + a 3,000-token summary, which becomes the new first user message. This changes the opening hash but not the session header.

Three quarters of conversations share a system prompt with others; every opening (system prompt plus first user message) is distinct (test `TestGeneratorShape`).

**Keys** (`router.go`):

| Key | What is hashed |
|---|---|
| `header>opening` | the session header when sent (agent-a), else the opening |
| `header>none` | the session header when sent, else a fresh key per request (no affinity, still counted) |
| `opening` | system prompt + first user message |
| `kubeai-100c` | KubeAI's own default: the first 100 characters of the first user message, taken as 25 tokens |
| `prefix-2048` | the request's first 2,048 tokens (HBEP-5's "fixed token prefix") |
| `system` | the system prompt only (the collapse case) |

**Policies:** round robin; least load (fewest in flight, ties rotated; my own code, because KubeAI's iterates a Go map and is not deterministic); pure affinity; CHWBL at c = 1.25, 1.5 and 2.0. Every hashed policy runs with every key.

### The bound, and how it is checked

HBEP-5 says "a target above its bound passes the key to the next one on the ring", and KubeAI documents the bound as a percentage of the mean load. KubeAI checks a candidate's load *before* the new request against c·(T+1)/n, where T is the total in flight and n the number of endpoints.

- **Pass criterion (HBEP-5 rule):** at every placement, the chosen Instance's in-flight count before the request is at most c·(T+1)/n, with T and n taken over the *current members*, from the simulator's own counters.
- **Strict (secondary):** its count after the request is at most ⌈c·(T+1)/n⌉, the capacity in Mirrokni, Thorup and Zadimoghaddam's definition. KubeAI's `<=` allows one more request whenever c·(T+1)/n is a whole number.
- **Over time:** the share of loaded time in which some member holds more than c × the current mean. Completions elsewhere lower the mean after placement, so this can be non-zero without any placement breaking the rule.

What the pass check can and cannot show: on every placement the simulator asserts that KubeAI's counters equal its own (`crossCheck`), and fails the run otherwise. In steady state the HBEP-5 rule therefore sees KubeAI's own inputs and cannot fail; a zero there only proves the accounting is consistent. The check can fail in two places. One is a drain: `removeEndpoint` keeps the drained Instance's open requests in KubeAI's total while n drops, so KubeAI's mean is higher than the members' mean. The other is the fallback path, which returns the home Instance when no candidate passes. The strict count and the time above the bound are the measures that can show piles.

### Scenarios

- **Steady:** 26 configurations × KV cache of 750 k, 1.5 M and 3 M tokens per Instance × seeds 1, 2, 3. The measurement window is requests dispatched in 1,200–4,800 s, after the warm-up.
- **Events**, at 1.5 M and 3 M tokens:
  - *drain*: instance-1 leaves the ring at 3,000 s, and its in-flight requests finish;
  - *leave*: instance-1 leaves at 3,000 s, its in-flight requests are cut, and the clients send them again at once;
  - *handover*: a cold instance-4 joins at 3,000 s and instance-1 drains at 3,300 s (HBEP-5's make-before-break).

  Each is compared with the same seed and policy without the event. A conversation counts as re-homed when its first request after the event goes to a different Instance than its last request before it. A re-home is *forced* when its Instance left, *to new* when it moved onto the joining Instance, and *collateral* otherwise.

## Environment

RESULTS_ENV

## How to run

Go 1.27.x on PATH, then, from the repository root:

```sh
(cd third_party/forked/kubeai && go test ./...)   # the fork
cd spikes/s10
go test ./...                                     # the simulator's validation and the scaled pass test
go build -o s10sim ./cmd/s10sim
GOMAXPROCS=2 nice -n 10 ./s10sim -mode all -out results   # the full sweep: about 25 min on 2 cores
```

`-mode calibrate` prints the working set and one line per run; `-block` sets the cache block size.

RESULTS_BODY
