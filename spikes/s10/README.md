# Spike S10: conversation key and load bound (simulation)

Issue #27. HBEP-5, open questions 1 (conversation key) and 3 (load bound).

## Question

HBEP-5 keeps each conversation on the Instance that holds its prefix cache, by consistent hashing with bounded loads (CHWBL) on a conversation key. Which key, a client session header or a hash of the conversation's opening, and which bound beat round robin on prefix-cache hit rate without any Instance ever exceeding the bound? And what happens when an Instance drains or leaves?

## Answer

**Pass: the opening hash (system prompt plus first user message), with a client's session header used instead when it sends one, at a bound of c = 1.25.**

- **Hit rate.** It beats round robin in all three seeds wherever the KV cache can hold the active contexts:
  - tight cache: 85.9% against 43.4%, at 2.85× round robin's throughput;
  - ample cache: 95.2% against 51.1%.
- **Bound.** No placement exceeded the bound in steady state or when an Instance left.
- **Drain.** KubeAI's code exceeds the bound during every drain in the tight regime, by up to 6.5 requests at c = 1.25. Upstream `removeEndpoint` keeps the drained Instance's requests in the mean. Releasing them from the count at drain start, a front-side change, removes every violation.
- **Scarce cache.** When the working set is about twice the cluster's KV cache, no key or bound beats round robin reliably.

Keys that hash only the start of the request (KubeAI's default 100 characters, a fixed 2,048-token prefix, the system prompt alone) collapse 15–36% of conversations onto one key. They are the collapse case the issue warns about.

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

- A shared 6-core VM with 31 GB RAM and no GPU, running Linux. Go 1.27.3 builds it, with `GOMAXPROCS=2` and `nice -n 10`.
- Other spikes shared the VM. Each sweep step started only when the 1-minute load was 4.0 or less. During the runs, the load ranged from 2.4 to 11.9.
- The simulator's clock is simulated, so results do not depend on host load. A run is deterministic for its seed. The same configuration (CHWBL 1.25 / header>opening, 1.5 M, seed 1) gave a 90.7% hit rate in both the calibration run and the steady sweep.
- Wall time: the steady sweep took 718 s (234 runs) and the events sweep 922 s (both sizes, 120 runs).
- llm-d-inference-sim was not used; the issue allows a self-written simulator. Writing one keeps the balancer in-process and makes every cache rule explicit and testable by hand. KubeAI is the only third-party code: commit `89dfff46`, Apache-2.0, with xxhash v1.1.0 (MIT) and OpenTelemetry v1.47.0 (Apache-2.0); see `third_party/forked/kubeai/NOTES.md`.

## How to run

Go 1.27.x on PATH, then, from the repository root:

```sh
(cd third_party/forked/kubeai && go test ./...)   # the fork
cd spikes/s10
go test ./...                                     # the simulator's validation and the scaled pass test
go build -o s10sim ./cmd/s10sim
GOMAXPROCS=2 nice -n 10 ./s10sim -mode all -out results   # the full sweep: about 30 min on 2 cores
```

`-mode calibrate` prints the working set and one line per run; `-block` sets the cache block size.

## Tests

`go test ./...` in `spikes/s10` runs every check below in about 10 s. `-short` skips the scaled pass test. Expected values are worked out by hand in each test's comment, or come from the issue and HBEP-5, never from a simulator run.

| Issue requirement | Test |
|---|---|
| A simulator in which the cache can miss | `TestThrashMisses`: three conversations take turns on a cache that holds only two, so every request must miss, hit 0 (its control, `TestThrashControlHits`, hits with a large cache) |
| A single Instance with a huge cache hits nearly everything after the first turns | `TestHugeCacheHitsAllButTheNewTokens`: turn k hits exactly 1,600 + 320k of 1,760 + 320k tokens, 96.6% by turn 9 |
| Cache rules | `TestSharedSystemPromptHits`, `TestEvictionTakesTheTailFirst`, `TestRoundRobinLosesContextAffinityKeeps` |
| Not every conversation has the same opening | `TestGeneratorShape`: one distinct opening per conversation, plus client shares, header share and short-session share within ±3 points of the parameters |
| Growing prefixes, seeded workload | `TestContextGrowsByAppending`, `TestGeneratorIsSeeded`, `TestUserStreamIsSeeded`, `TestClosedLoopUsersRunOneConversationAtATime` |
| Collapse case | `TestKubeAIKeyCollapsesPreambleClients`, `TestSystemKeyCollapsesUnderAffinity`, `TestCHWBLSpillsCollapsedKeyWithinBound` |
| Drain and leave | `TestDrainMovesOnlyItsConversations` (forced count from a separate ring), `TestLeaveResendsCutRequests` (every turn completes exactly once), `TestJoinMovesKeysOnlyToNewInstance`, `TestDrainInflatesKubeAIMeanUnlessReleased` |
| Pass criterion | `TestPassCriterion`: a quarter-scale copy of the tight regime (40 users, 375 k tokens per Instance). CHWBL 1.25 on both keys must beat round robin's hit rate and place nothing above the bound, in each of seeds 1–3 |
| The fork | `third_party/forked/kubeai/loadbalancer/sim_api_test.go`: same key gives the same endpoint, spill at the bound worked by hand, release after removal |

The tests were checked by breaking the cache on purpose:
- a cache that never misses fails 5 tests;
- freeing a request's blocks head first fails `TestEvictionTakesTheTailFirst`;
- not caching output tokens fails 4 tests.

The re-homing counter's test failed on the bug it was written for: conversations started after the event were counted.

## Results

Full tables: `results/steady.md`, `results/events-*.md` and `results/drainfix-*.md`, with every raw per-run field in the matching `.csv`. Numbers below are means over seeds 1–3, with the per-seed range in brackets.

### Steady state

The working set of 160 users averages 6.2 M context tokens (round robin, all three sizes). Against the cluster's KV cache, that sets three regimes:

| KV per Instance | Working set / cluster KV | Regime |
|---|---|---|
| 750 k | 2.07× | scarce: no routing can keep the contexts |
| 1.5 M | 1.04× | tight: routing decides |
| 3 M | 0.51× | ample |

**Tight regime (1.5 M tokens per Instance).** Selected rows; the table has all 26:

| Policy / key | Hit rate | Throughput vs RR | Mean / p95 latency (s) | Moves between turns | Peak in-flight | Above bound (HBEP-5 rule) |
|---|---|---|---|---|---|---|
| round robin | 43.4% [42.9–44.0] | 1.00× | 46.4 / 166 | 75% | 131 | – |
| least load | 35.3% [34.4–36.2] | 1.30× | 34.5 / 56 | 73% | 39 | – |
| pure affinity / opening | 83.4% [81.8–84.3] | 1.63× | 26.8 / 111 | 0.4% | 125 | – |
| **CHWBL 1.25 / opening** | **85.9% [83.0–91.6]** | **2.85×** | **13.2 / 40** | 7.0% | 48 | **0** |
| CHWBL 1.25 / header>opening | 84.0% [79.1–90.7] | 2.84× | 13.2 / 37 | 9.8% | 46 | 0 |
| CHWBL 1.50 / opening | 77.4% [68.0–83.3] | 2.20× | 19.0 / 60 | 11.3% | 57 | 0 |
| CHWBL 2.00 / opening | 82.5% [81.2–83.3] | 2.15× | 18.8 / 78 | 10.7% | 73 | 0 |
| CHWBL 1.25 / header>none | 47.5% [45.9–49.5] | 1.55× | 28.1 / 60 | 52% | 49 | 0 |
| pure affinity / kubeai-100c | 71.1% [68.9–73.2] | 0.87× | 54.9 / 154 | 0.3% | 145 | – |
| CHWBL 1.25 / kubeai-100c | 51.0% [45.2–55.2] | 1.44× | 30.8 / 67 | 37% | 49 | 0 |
| CHWBL 1.25 / prefix-2048 | 65.9% [65.1–66.9] | 1.96× | 21.2 / 55 | 31% | 49 | 0 |
| pure affinity / system | 63.9% [61.9–67.1] | 0.84× | 56.4 / 144 | 0.0% | 149 | – |
| CHWBL 1.25 / system | 57.9% [54.3–62.7] | 1.63× | 26.5 / 60 | 31% | 49 | 0 |

Per seed, CHWBL 1.25 on the opening hash reached 91.6%, 83.2% and 83.0%, against round robin's 44.0%, 43.3% and 42.9%. Keyed on header>opening it reached 90.7%, 82.2% and 79.1%.

- **Round robin and pure affinity each grow one hot Instance.** In every seed, round robin's per-Instance hit rates are about 52% on three Instances and 17% on the fourth, whose peak is about 130 in flight against 16–19 on the others. Pure affinity shows the same: about 96% on three Instances, 19–21% and 116–125 in flight on one. The mechanism is a feedback loop. Many running requests pin KV blocks, so cached prefixes are evicted, so prompts miss, so prefill is slower, so more requests pile up. Round robin ignores load and keeps sending a fixed share of requests there. Affinity keeps its keys there. CHWBL 1.25 caps every Instance near the same peak (37–48), and all four stay at 69–94% hit.
- **The bound pays for itself in throughput.** CHWBL 1.25 moves 7–10% of later turns to another Instance, each a partial miss, yet serves 2.85× round robin's output and 1.75× pure affinity's. This matches HBEP-5's motivation: pure load balancing lost two to three times the throughput, and pure affinity piled sessions on one worker.
- **c = 1.5 did worse than 1.25 and 2.0** here, with a wide seed range (68–83%). It lets a pile grow far enough to start the feedback loop, then spills it. The bound is not monotonic in this regime, so a measured c should be re-checked on real engines.
- **Session header alone is not enough.** With 35% of conversations sending a header and no fallback (`header>none`), CHWBL 1.25 reaches only 47.5%. Falling back to the opening hash (`header>opening`) is as good as the opening hash alone, within seed noise.
- **Collapse keys.** A key that sees only the start of the request collapses whole client types onto one key:
  - KubeAI's default (first 100 characters of the first user message) puts 36% of conversations on one key, because agent-a's fixed preamble fills those characters;
  - the first 2,048 tokens fall inside the static system prompts of agent-a, agent-b and agent-c (top key 36%);
  - the system prompt alone puts 15% on one key (agent-c's prompt).

  Under pure affinity these keys pile load onto one Instance and fall *below* round robin's throughput (0.84–0.90×). Under CHWBL 1.25 the bound spreads them, but 31–37% of turns move and the hit rate drops to 51–66%. This is the collapse case the issue warns about.

**Ample regime (3 M).** Every policy keyed on the opening or the header with opening fallback reaches 95.2–96.0%, against round robin's 51.1% [50.0–52.1] and least load's 56.7%, and 3.3× round robin's throughput. Here a tight bound costs a little: CHWBL 1.25 moves 2.7–3.5% of turns and loses 0.7–0.8 points against c = 1.5, c = 2.0 or pure affinity. Round robin still forms its hot Instance.

**Scarce regime (750 k).** Nothing beats round robin reliably: round robin 27.9% [24.7–31.9], CHWBL 1.25 / opening 24.9% [18.7–35.2], pure affinity / opening 32.4% [30.3–35.8]. The ordering flips between seeds. When the working set is twice the cache, every Instance evicts contexts before their next turn whatever the routing.

### The bound

- **HBEP-5 rule, steady state: 0 placements above the bound**, for every CHWBL row at every size. As explained under Method, this zero shows only that KubeAI's counters agree with the simulator's and that the fallback path never fired.
- **The strict (Mirrokni) capacity is exceeded by one request** through KubeAI's `<=` at whole-number thresholds. For c = 1.25 / opening, that is 87–204 placements per run in the tight regime and 90–124 in the ample one: at most 0.7% and 0.33% of the requests in the measurement window. The count covers the whole run, so the true share is lower.
- **Over time, some Instance is above 1.25 × the current mean** for 25–43% of loaded time in the tight regime. Placement is checked against the mean at that moment; requests then finish elsewhere and the mean falls. The bound limits where new requests go; it does not keep load within c × the mean between placements.
- The largest load seen at a placement, relative to the mean, is 1.60. That figure is set by small loads: with a mean of 2.5, c × mean is 3.1, a load of 3 passes, and the next request makes it 4.

### Drain, leave and handover

instance-1 changes at 3,000 s. "Checked" is the number of conversations with a request both before and after the event: about 150 of the 160 users. Hit rates are over 5-minute windows, against the same seed and policy without the event.

**Ample regime (3 M tokens per Instance):**

| Policy / key | Event | Forced re-homes | Collateral re-homes | Requests cut | Hit 0–5 min after (no event) | Hit 5–15 min after (no event) | Placements above bound (HBEP-5 rule) | Max over bound (requests) |
|---|---|---|---|---|---|---|---|---|
| round robin | drain | 47 | 75 | 0 | 52.3% (51.2%) | 49.0% (50.8%) | – | – |
| pure affinity / opening | drain | 39 | 1 | 0 | 94.8% (96.0%) | 96.0% (96.0%) | – | – |
| CHWBL 1.25 / opening | drain | 40 | 4 | 0 | 94.4% (94.9%) | 95.5% (95.1%) | 16 | 1.5 |
| CHWBL 2.00 / opening | drain | 39 | 1 | 0 | 94.8% (96.0%) | 96.0% (96.0%) | 0 | 0 |
| CHWBL 1.25 / opening | leave | 41 | 4 | 26 | 94.4% (94.9%) | 95.5% (95.1%) | 0 | 0 |
| CHWBL 1.25 / opening | handover: join instance-4 | 0 (30 to the new Instance) | 8 | 0 | 93.8% (94.9%) | 94.5% (95.1%) | 37 | 1.4 |
| | then drain instance-1 at 3,300 s | 33 | 3 | 0 | | | | |

- **Re-homing is what consistent hashing promises.** A drain forces the drained Instance's conversations to move, 39–40 of about 150 (a quarter), and moves only 1–4 others. A join moves 30–33 conversations (a fifth) onto the new Instance. Round robin and least load move about three quarters of all conversations at every event, as they do between any two turns.
- **The dip is small and short when the cache is ample.** CHWBL and pure affinity lose about 0.5–1.2 points in the first 5 minutes and are back within 5–15 minutes. Each forced conversation recomputes its context once.
- **A leave behaves like a drain plus cut requests.** 25–30 in-flight requests are cut. The simulated clients send each one again at once, and it lands on the conversation's new home. The hit rate is the same as for the drain.

**Tight regime (1.5 M):** losing an Instance without a replacement drops the cluster into the scarce regime. Three Instances hold 4.5 M tokens against a 6.2 M working set.

| Policy / key | Event | Forced | Collateral | Hit 0–5 min after (no event) | Hit 5–15 min after (no event) | Above bound | Max over (req.) |
|---|---|---|---|---|---|---|---|
| round robin | drain | 13 | 94 | 41.5% (43.2%) | 42.3% (43.3%) | – | – |
| pure affinity / opening | drain | 18 | 0 | 79.1% (82.1%) | 79.3% (82.1%) | – | – |
| CHWBL 1.25 / opening | drain | 42 | 10 | 56.9% (85.6%) | 54.6% (91.2%) | 84 | 6.5 |
| CHWBL 2.00 / opening | drain | 29 | 23 | 78.4% (81.1%) | 76.5% (81.8%) | 35 | 3.3 |
| CHWBL 1.25 / opening | leave | 43 | 13 | 55.7% (85.6%) | 54.5% (91.2%) | 0 | 0 |
| CHWBL 1.25 / opening | handover (join, then drain) | 31 (+35 to the new Instance) | 20 + 6 | 91.0% (85.6%) | 89.9% (91.2%) | 58 | 2.0 |

- **Without a replacement the hit rate does not recover.** CHWBL 1.25 falls 29–37 points and stays down. With one Instance fewer, load per Instance rises a third, c = 1.25 spills more, and every spill copies a context onto a second Instance, which pushes the cache further into thrash. c = 2.0 and pure affinity lose only about 3–5 points here. The best bound depends on the regime: tight bounds win when the cache can hold the working set, looser ones once it cannot.
- **Make-before-break works.** With the replacement joining 300 s before the drain, CHWBL 1.25 keeps its hit rate, within seed noise of the no-event run. This supports HBEP-5's drain rule.
- In the tight regime, pure affinity forces only 18 of about 150 conversations to move, not a quarter. Its hot Instance holds most of the active conversations, because conversations homed there run longest (Little's law), and instance-1 was a cold Instance in all three seeds. The ring itself is balanced: on 200,000 random keys instance-1 gets 26%.

### KubeAI's drain accounting breaks the bound

During a drain, CHWBL places requests above the bound computed over the current members: 29–104 placements in every configuration at 1.5 M, up to 15.5 requests over, and 13–16 at 3 M with c = 1.25. At 3 M, c = 1.5 and 2.0 had none in the plain drain; in the handover's drain, c = 1.5 / opening had 30. A leave never breaks the bound. The cause is in upstream `removeEndpoint`: the removed endpoint's open requests stay in `totalInFlight` until they finish, while `n` drops, so KubeAI's mean exceeds the members' mean by up to 49 requests here (the "KubeAI mean inflation" column).

A front-side change fixes this without touching the copied algorithm. At drain start, the front releases the draining Instance's open requests from the balancer's count, and they finish uncounted (`ReleaseOnDrain`, test `TestDrainInflatesKubeAIMeanUnlessReleased`). With it:

| Size | Bound / key | Above bound (unfixed → released) | Hit 0–5 min after drain (unfixed → released) |
|---|---|---|---|
| 1.5 M | 1.25 / opening | 84 → 0 | 56.9% → 57.1% |
| 1.5 M | 1.50 / opening | 104 → 0 | 61.3% → 64.7% |
| 1.5 M | 2.00 / opening | 35 → 0 | 78.4% → 78.0% |
| 3 M | 1.25 / opening | 16 → 0 | 94.4% → 94.3% |
| 3 M | 1.25 / header>opening | 13 → 0 | 94.6% → 94.6% |

All twelve released configurations (both keys, three bounds, both sizes) had zero placements above the bound and zero inflation (`results/drainfix-*.md`).

## What this means for the HBEPs

- **HBEP-5, open question 1 (conversation key).** Honour a session header when the client sends one. Otherwise hash the *whole* opening: the system prompt plus the first user message. Do not hash a fixed token prefix or the first N characters (KubeAI's default): agent clients with long static system prompts or fixed preambles collapse onto a few keys, and under pure affinity that falls below round robin. A header alone, without the opening fallback, helps only the clients that send one. Compaction changes the opening, so a conversation with no header moves once after compacting. The header avoids that, but the effect was within noise here.
- **HBEP-5, open question 3 (bound).** Start at c = 1.25 on in-flight requests. It met the pass criterion and gave the best throughput where routing matters. Make c a deployment parameter and re-measure it on real engines: c = 1.5 was worse than both neighbours, and after a capacity loss looser bounds did better. The bound limits placements, not load over time: some Instance sat above 1.25 × the mean for a quarter to two fifths of the time.
- **HBEP-5, Risks ("affinity piles return").** Confirmed, and the bound fixes it. Pure affinity piled one Instance to about 120 in flight at 19–21% hit while three others idled, the failure HBEP-5's Motivation describes. HBEP-5's per-Instance caps (open question 6) are a second guard the simulation did not need, because the bound already held in-flight peaks near the engine's 48-sequence limit.
- **HBEP-5, draining, and the KubeAI reuse recorded in the evaluation.** Copying KubeAI's balancer is sound, but its drain path is not what HBEP-5 needs. At drain start, the front must release the draining Instance's open requests from the balancer's load count, or keep its own member-only count. Otherwise the bound is broken during every drain. This matches the HBEP-5 risk that "the load measure must count cut and abandoned requests": the measure must also *stop* counting what no longer competes for members.
- **HBEP-5 and HBEP-7, make-before-break.** The simulation supports it. A drain with the replacement already serving cost nothing measurable. A drain without one cost 29–37 points of hit rate for good when the cache was tight, because the cluster dropped into the scarce regime. HBEP-7's event for lost capacity is worth emitting.
- **Capacity (HBEP-4, HBEP-7).** None of this helps when the working set exceeds the cluster's KV cache by about 2× (the scarce regime): no routing beat round robin reliably. Prefix-cache affinity pays only when the KV cache can hold most of the active contexts. Sizing Instances' KV against the expected working set is a capacity question for HBEP-4 and HBEP-7.

## Still unknown

- **All parameters are assumptions:** the client mix, prompt and reply lengths, session lengths, think time, prefill rate, decode time and KV size. The ranking held across three cache sizes and three seeds, but the regime boundaries move with these numbers. Measured agent sessions would settle them; the real-vLLM GPU run in the issue's Dependencies is the next check.
- **The engine model is simple.** It has no preemption: a request reserves its output at admission and waits otherwise, where vLLM preempts and recomputes. It has one prefill at a time, decode speed independent of batch size, and no chunked-prefill interleaving. The hot-Instance feedback loop comes from this model. Whether real vLLM shows it as strongly needs the GPU run.
- **One worker per Instance.** HBEP-5's second level, the inner router across an Instance's workers (open question 2), is not modelled.
- **Requests above a cap wait at the Instance, not at the front** as HBEP-5 proposes. Other load measures, such as sessions or KV use (open question 3), were not compared.
- **The front-side drain release** was tested only in the simulator. Its interaction with retries (open question 5) and with a front restart (open question 11) is open.
- llm-d-inference-sim was not tried.



