# Spike S10: conversation key and load bound (simulation)

Issue #27. HBEP-5, open questions 1 (conversation key) and 3 (load bound).

## Question

HBEP-5 keeps each conversation on the Instance that holds its prefix cache, by consistent hashing with bounded loads (CHWBL) on a conversation key. Which key, a client session header or a hash of the conversation's opening, and which bound beat round robin on prefix-cache hit rate without any Instance ever exceeding the bound? And what happens when an Instance drains or leaves?

## Answer

**Pass under three conditions, for the opening hash (system prompt plus first user message), with a client's session header used instead when it sends one, at c = 2.0, HBEP-5's decided default. The conditions: (a) the bound is HBEP-5's KubeAI reading; (b) the front releases a draining Instance's requests at drain start, and serializes selection with a once-only release; (c) the cluster's KV cache holds about the working set or more. It passed at a working set of 1.04× and 0.51× the cluster's KV cache, failed at 2.07×, and was not tested between 1.04× and 2.07×.**

- **(a) Which bound.** HBEP-5's owner adopted KubeAI's reading: an Instance takes a request while its load before the request is at most c × the mean. The strict capacity reading, at most ⌈c × mean⌉ after the request, is *not* met. At c = 2.0, KubeAI places one request over it on 3.0% (opening; 3.2% on header>opening) of placements in the tight regime and 0.02% in the ample one, and between placements some Instance sits above 2 × the current mean for 49–52% of loaded time in the tight regime (1–4% in the ample one). Hard per-Instance limits (KV capacity, the engine's maximum sequences) are a separate mechanism.
- **(b) Front-side changes.** Both are now HBEP-5 requirements (owner decision on PR #34):
  - release a draining Instance's open requests from the balancer's count at drain start. Without it, KubeAI's count includes requests that no longer compete for members, and placements break the bound (the unreleased control: up to 75 placements above the bound in one run);
  - serialize selection and reservation, and release each request exactly once. Upstream checks and reserves under a read lock, so two concurrent selectors can both pass on the same loads (a forced interleaving breaks the bound on the plain group and holds with the wrapper), and its release decrements on every call.
- **(c) Cache regime.** Hit rate, means over seeds 1–3:
  - tight cache (1.5 M tokens per Instance, working set 1.04× cluster KV), every seed above round robin: opening 82.5% [81.2–83.3], header>opening 82.1% [81.2–83.3], against round robin's 43.4% [42.9–44.0], at 2.15× round robin's throughput;
  - ample cache (3 M, 0.51×), every seed above round robin: 96.0% on both keys against 51.1% [50.0–52.1], at 3.30×;
  - scarce cache (750 k, 2.07×): c = 2.0 / opening averages 32.4% against round robin's 27.9% but loses seed 1 (30.3% against 31.9%). No opening-keyed configuration beats round robin reliably there.
- **The bound's evidence.** With consistent counts and c ≥ 1, the HBEP-5 rule cannot fail: it is the inequality KubeAI applies, and the least-loaded member always satisfies it. So the zeros in steady state, after a leave and in a released drain are an accounting identity, not measurements. The empirical evidence is the unreleased drain control (3, 6 and 75 placements above the bound on seeds 1–3 at 1.5 M, c = 1.25; 35 in all at c = 2.0), the strict count, and the time above the bound. The release's own test is that KubeAI's total equals the members' (inflation 0).
- **Choice of c: a regime-dependent trade-off.** c = 1.25 gives the best throughput at full membership in the tight regime (2.85× against 2.15×) and keeps every Instance's hit rate high there; at c = 2.0 one Instance still runs hot (17–19% hit, about 70 in flight). c = 2.0 holds up better after an unreplaced drain (78.0% against 57.1% in the first 5 minutes), in the scarce regime, and at half the prefill rate. HBEP-5's owner set **c = 2.0 as the default, per deployment**, because it is robust in the degraded cases the design expects: late replacements (HBEP-5's drain rule lets capacity drop when a replacement is late), scarce cache and slower prefill. Operators may lower it to 1.25 where spare or warm capacity makes replacements reliable. The evidence is under "Choice of c".
- **Scarce cache and collapse keys.** At 750 k, keys that collapse many conversations onto one key do beat round robin's hit rate on every seed: c = 2.0 / system (40.0, 41.8, 40.6% against 31.9, 24.7, 27.2%), c = 2.0 / kubeai-100c, and pure affinity on the system, kubeai-100c and prefix-2048 keys. They cost 20–35% of round robin's throughput. Concentrating shared system prompts keeps them cached.

Keys that hash only the start of the request (KubeAI's default 100 characters, a fixed 2,048-token prefix, the system prompt alone) put 15–36% of conversations on one key. In the tight regime they are the collapse case the issue warns about; in the ample regime the collapse costs nothing; in the scarce regime it helps the hit rate (above).

## Method

### Harness: KubeAI's balancer, copied, behind the front's wrapper

`third_party/forked/kubeai/` holds KubeAI's CHWBL balancer at commit `89dfff46`, with its Apache-2.0 licence and upstream's copyright header where upstream has one. `ORIGIN.md` there carries HBEP-15's fields and lists every change: import paths, trimmed type files, no-op metrics, and the `loadbalancer` directory moved out of `internal/`. The algorithm is unchanged. Every copied line is also identical at tag `v0.24.0`, the three `loadbalancer` files byte for byte. The whole type files and upstream's OpenTelemetry version differ there, so the pin stays at the commit; `ORIGIN.md` gives the details.

The simulator calls upstream's `getBestAddr`, `reconcileEndpoints` and `removeEndpoint` through a thin exported wrapper, `sim_api.go`, and uses upstream's release function to decrement load. It reaches them through `front/`, our own code, which adds what HBEP-5's front needs on top: one mutex over selection, reservation and release; a once-guard on each request's release; and a drain that releases the draining Instance's open requests. Pure affinity is the same ring with an effectively infinite bound (`MeanLoadPercentage` 2^40), so it differs from CHWBL only in the bound.

### Simulator (`sim/`)

A discrete-event simulation in Go, one goroutine per run, deterministic for a seed.

**Cache** (`cache.go`), modelled on vLLM V1's block pool:
- the KV cache holds a fixed number of blocks. Experiments use 64-token blocks to keep runs fast; tests use 16. A rerun with 16-token blocks is under "Sensitivity";
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

Three quarters of conversations share a system prompt with others; every opening (system prompt plus first user message) is distinct. `TestUserStreamShape` checks this on the users' streams the experiments use, and `TestGeneratorShape` on the open-loop generator.

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

**Measures.** The window is 1,200–4,800 s. Hit rate is the share of prompt tokens served from cache, over requests dispatched in the window. Throughput is the output of requests that *complete* in the window, whenever they were dispatched (`TestThroughputCountsCompletionsInTheWindow`); the tables also give the dispatch-window figure, which the first version of this report used.

### The bound, and how it is checked

HBEP-5 says "a target above its bound passes the key to the next one on the ring", and KubeAI documents the bound as a percentage of the mean load. KubeAI checks a candidate's load *before* the new request against c·(T+1)/n, where T is the total in flight and n the number of endpoints. HBEP-5's owner adopted this reading as the requirement. Hard per-Instance limits (KV capacity, the engine's maximum sequences) are a separate mechanism.

- **HBEP-5 rule (the requirement):** at every placement, the chosen Instance's in-flight count before the request is at most c·(T+1)/n, with T and n taken over the *current members*, from the simulator's own counters.
- **Strict capacity (not required, reported):** its count after the request is at most ⌈c·(T+1)/n⌉, the capacity in Mirrokni, Thorup and Zadimoghaddam's definition. KubeAI's `<=` allows one more request whenever c·(T+1)/n is a whole number (fork test `TestKubeAIAdmitsOneOverStrictCapacityAtEquality`: four members, T = 15, a home at 5 against a threshold of 5 is accepted and ends at 6 against a capacity of 5). At c = 2.0 and four members the threshold (T+1)/2 is whole for every odd T, so this happens far more often than at 1.25.
- **Over time (reported):** the share of loaded time in which some member holds more than c × the current mean. Completions elsewhere lower the mean after a placement, so this can be non-zero without any placement breaking the rule.

**The HBEP-5 rule cannot fail when the counts are consistent.** It is an accounting identity, not a measurement:
- on every placement the simulator asserts that KubeAI's counters equal its own (`crossCheck`), and fails the run otherwise;
- with equal counts, the rule is the inequality `chwblLoadOK` itself applied to the chosen Instance;
- KubeAI falls back to the home Instance only when no candidate passes. For c ≥ 1 that cannot happen: the least-loaded member holds at most T/n < c·(T+1)/n.

So for any key and workload, with c ≥ 1, the rule holds whenever KubeAI's T and n are the members' T and n at the moment of the reservation. It fails in exactly two ways, and both are front design questions, not tuning:
- **counts that include non-members:** during a drain, KubeAI keeps the drained Instance's open requests in T while n drops (the unreleased control below);
- **counts that change between the check and the reservation:** concurrent selectors (below).

The zeros in steady state, in a leave, and in a drain with the release follow from the identity. They show the accounting is consistent; they are not evidence about the bound. The release's own test is that KubeAI's total equals the members' total throughout (`MaxKubeInflation == 0`). The empirical evidence about the bound is the unreleased drain control, the strict count and the time above the bound.

### Front requirements (HBEP-5's owner adopted both)

1. **Selection and reservation are serialized, and each request is released exactly once.**
   - Upstream `getBestAddr` runs the load check and the increment under the group's *read* lock, so selectors run concurrently. Fork test `TestConcurrentSelectorsBothPassOnThePlainGroup` forces the interleaving with a hook, not with timing: loads 25 (home), 18, 18, 18 give T = 79 and a bound of 1.25 × 80 / 4 = 25. Two selectors both pass the check at 25, both increment the home, and the second reservation finds 26 against 1.25 × 81 / 4 = 25.3125.
   - `spikes/s10/front` wraps the group without changing it. One mutex covers selection, reservation and release, so the counts a selector reads are the counts at its reservation. In the same scenario the second selector waits for the lock, finds the home at 26 and spills (`TestSerializedSelectorsHoldTheBound`). `TestPlainGroupBreaksTheBoundInTheSameScenario` runs the identical scenario on the plain group and requires the bound to break, so the scenario can fail. The simulator runs on one goroutine and cannot produce this case; these unit tests are the evidence for it.
   - Upstream's release function decrements every time it is called (fork test `TestUpstreamReleaseDecrementsOnEveryCall`: two calls leave the total at −1). A front that releases at drain start and again on completion undercounts load, which is the L-4.3 failure (a counter that misses requests lets piles re-form). The wrapper guards each request's release with a once-flag (`TestReleaseTwiceIsANoOp`).
2. **A draining Instance's open requests are released from the balancer's count at drain start.** `front.Balancer.Drain` removes the Instance from the ring and releases its open reservations; their later completions call the same once-guarded release and change nothing (`TestDrainReleasesOpenRequestsOnce`). This diverges from upstream on purpose. KubeAI keeps a terminating pod's requests counted until they finish, deliberately: its PR #747 ("fix: exclude terminating pods from routing", merged 2026-10-06) added that, and its test `TestReconcileStopsRoutingToTerminatingPods` asserts "the draining request remains accounted for". Upstream's main drain path is `reconcileEndpoints`, which drops pods with a `DeletionTimestamp`; `removeEndpoint`, which the simulator calls, serves pods already gone and has the same effect on the counters.

The simulator runs every hashed policy through `front.Balancer`, and every completion calls the once-guarded release, including completions of requests already released at drain start. A second decrement would make KubeAI's total differ from the simulator's and fail the run at the next placement's `crossCheck`.

### Scenarios

- **Steady:** 26 configurations × KV cache of 750 k, 1.5 M and 3 M tokens per Instance × seeds 1, 2, 3.
- **Events**, at 1.5 M and 3 M tokens:
  - *drain*: instance-1 leaves the ring at 3,000 s, and its in-flight requests finish;
  - *leave*: instance-1 leaves at 3,000 s, its in-flight requests are cut, and the clients send them again at once;
  - *handover*: a cold instance-4 joins at 3,000 s and instance-1 drains at 3,300 s (HBEP-5's make-before-break).

  Each is compared with the same seed and policy without the event. A conversation counts as re-homed when its first request after the event goes to a different Instance than its last request before it. A re-home is *forced* when its Instance left, *to new* when it moved onto the joining Instance, and *collateral* otherwise. Hit-rate windows start at each event's own time: the 10 minutes before it, then 0–5 and 5–15 minutes after it.
- **Sensitivity**, at 1.5 M, seeds 1–3, the baselines and every bound on the opening and header>opening keys: prefill rate × 0.5 (12,500 tokens/s) and × 2 (50,000), and 16-token blocks.

## Environment

- A shared 6-core VM with 31 GB RAM and no GPU, running Linux. Go 1.27.3 builds it, with `GOMAXPROCS=2` and `nice -n 10`.
- Other spikes shared the VM. Each sweep step started only when the 1-minute load was 4.0 or less. The sweep steps started at loads of 2.1–3.9; the load during the runs was not logged.
- The simulator's clock is simulated, so results do not depend on host load. A run is deterministic for its seed. Routing through the front wrapper changed no result: every row of the rerun steady, event and drain-release sweeps matches the first version's requests, hit tokens, output and bound counts exactly (234 steady, 240 event and 108 drain-release rows).
- Wall time: the steady sweep took 713 s (234 runs), the events sweep 952 s (240 runs, both sizes), the drain-release sweep 465 s (108 runs), and the three sensitivity runs 69, 142 and 504 s (30 runs each).
- llm-d-inference-sim was not used; the issue allows a self-written simulator. Writing one keeps the balancer in-process and makes every cache rule explicit and testable by hand. KubeAI is the only third-party code: commit `89dfff46`, Apache-2.0, with xxhash v1.1.0 (MIT) and OpenTelemetry v1.47.0 (Apache-2.0); see `third_party/forked/kubeai/ORIGIN.md`.

## How to run

Go 1.27.x on PATH, then, from the repository root:

```sh
(cd third_party/forked/kubeai && go test ./...)   # the fork
cd spikes/s10
go test ./...                                     # the simulator, the front wrapper and the scaled pass tests
./mutants.sh                                      # planted cache bugs; each must fail a test
go build -o s10sim ./cmd/s10sim
GOMAXPROCS=2 nice -n 10 ./s10sim -mode all -out results   # steady, events and drain release: about 40 min on 2 cores
for p in 12500 50000; do ./s10sim -mode sensitivity -kv 1500000 -prefill $p -name sens-prefill-$p -out results; done
./s10sim -mode sensitivity -kv 1500000 -block 16 -name sens-block-16 -out results
```

`-mode calibrate` prints the working set and one line per run.

## Tests

`go test ./...` in `spikes/s10` runs every check below, 28 tests, in about a minute on 2 cores. `-short` skips the scaled pass tests. Expected values are worked out by hand in each test's comment, or come from the issue and HBEP-5, never from a simulator run.

| Issue requirement | Test |
|---|---|
| A simulator in which the cache can miss | `TestThrashMisses`: three conversations take turns on a cache that holds only two, so every request must miss, hit 0 (its control, `TestThrashControlHits`, hits with a large cache) |
| A single Instance with a huge cache hits nearly everything after the first turns | `TestHugeCacheHitsAllButTheNewTokens`: turn k hits exactly 1,600 + 320k of 1,760 + 320k tokens, 96.6% by turn 9 |
| Cache rules | `TestSharedSystemPromptHits`, `TestEvictionTakesTheTailFirst`, `TestRoundRobinLosesContextAffinityKeeps` |
| Not every conversation has the same opening | `TestUserStreamShape` (the users' streams the experiments draw from) and `TestGeneratorShape` (the open-loop generator): one distinct opening per conversation, plus client shares, header share and short-session share within ±3 points of the parameters |
| Growing prefixes, seeded workload | `TestContextGrowsByAppending`, `TestGeneratorIsSeeded`, `TestUserStreamIsSeeded`, `TestClosedLoopUsersRunOneConversationAtATime` |
| Collapse case | `TestKubeAIKeyCollapsesPreambleClients`, `TestSystemKeyCollapsesUnderAffinity`, `TestCHWBLSpillsCollapsedKeyWithinBound` |
| Drain and leave | `TestDrainMovesOnlyItsConversations` (forced count from a separate ring), `TestLeaveResendsCutRequests` (every turn completes exactly once), `TestJoinMovesKeysOnlyToNewInstance`, `TestDrainInflatesKubeAIMeanUnlessReleased` (unreleased: inflation and violations above 0; released: inflation 0), `TestRingShareIsBalanced` |
| Pass criterion | `TestPassCriterion`: 40 users and 375 k tokens per Instance, a quarter of the users and the cache. It is not a copy of the tight regime: `MaxRunning` stays at 48, which 40 users never reach, so the sequence limit never binds and round robin hits 31% there against 43% at full scale. In each of seeds 1–3, CHWBL at c = 2.0 and 1.25 on both keys must beat round robin's hit rate; all 12 do. Its bound check is an identity (see "The bound"). `TestPassCriterionHoldsThroughDrains`: in the same setup, a drain and a handover. Without the release, KubeAI's total must exceed the members' in every run and the bound must break somewhere; with it, the totals must be equal |
| Front requirements | `front/`: `TestSerializedSelectorsHoldTheBound`, `TestPlainGroupBreaksTheBoundInTheSameScenario`, `TestReleaseTwiceIsANoOp`, `TestDrainReleasesOpenRequestsOnce` |
| Throughput | `TestThroughputCountsCompletionsInTheWindow`: requests that cross the window's edges are counted by completion |
| The fork | `third_party/forked/kubeai/loadbalancer/`: same key gives the same endpoint, spill at the bound worked by hand, release after removal (`sim_api_test.go`); one over the strict capacity at a whole-number threshold, a release that decrements on every call, and two selectors under the read lock that both pass (`concurrency_test.go`) |

At quarter scale the unreleased drain breaks the bound only at c = 1.25 (31 placements over seeds and keys in the drain, 21 in the handover). At c = 2.0 KubeAI's total still exceeds the members' in every run (by 2–8 requests), but no placement crosses the looser bound, so the test's bound control rests on the c = 1.25 runs. At full scale c = 2.0 breaks it too (below).

`mutants.sh` breaks the cache on purpose in a scratch copy and runs `go test -short ./sim/`:
- a cache that never misses fails 6 tests;
- freeing a request's blocks head first fails `TestEvictionTakesTheTailFirst`;
- not caching output tokens fails 4 tests.

The re-homing counter's test failed on the bug it was written for: conversations started after the event were counted.

## Results

Full tables: `results/steady.md`, `results/events-*.md`, `results/drainfix-*.md` and `results/sens-*.md`, with every raw per-run field in the matching `.csv`, and every event's raw measures in `results/*-per-event.csv`. Numbers below are means over seeds 1–3, with the per-seed range in brackets, unless a column says sum or max.

### Steady state

The working set of 160 users averages 6.2 M context tokens (round robin, all three sizes). Against the cluster's KV cache, that sets three regimes:

| KV per Instance | Working set / cluster KV | Regime |
|---|---|---|
| 750 k | 2.07× | scarce |
| 1.5 M | 1.04× | tight: routing decides |
| 3 M | 0.51× | ample |

**Tight regime (1.5 M tokens per Instance).** Selected rows; the table has all 26. Throughput counts output completed in the window; the dispatch-window figure agrees to within 0.01× on every row.

| Policy / key | Hit rate | Throughput vs RR | Mean / p95 latency (s) | Moves between turns | Peak in-flight (max) | Hot Instance (lowest per-Instance hit) | +1 over strict capacity (share of placements) |
|---|---|---|---|---|---|---|---|
| round robin | 43.4% [42.9–44.0] | 1.00× | 46.4 / 166 | 75% | 131 | 16–17% | – |
| least load | 35.3% [34.4–36.2] | 1.30× | 34.5 / 56 | 73% | 39 | – | – |
| pure affinity / opening | 83.4% [81.8–84.3] | 1.63× | 26.8 / 111 | 0.4% | 125 | 19–21% | – |
| **CHWBL 2.00 / opening** | **82.5% [81.2–83.3]** | **2.15×** | **18.8 / 78** | 10.7% | 73 | 17–19% | 3.0% |
| **CHWBL 2.00 / header>opening** | **82.1% [81.2–83.3]** | **2.13×** | **19.0 / 78** | 11.3% | 75 | 17.5–20% | 3.2% |
| CHWBL 1.25 / opening | 85.9% [83.0–91.6] | 2.85× | 13.2 / 40 | 7.0% | 48 | none (69–94% on all four) | 0.35% |
| CHWBL 1.25 / header>opening | 84.0% [79.1–90.7] | 2.84× | 13.2 / 37 | 9.8% | 46 | none (60–94% on all four) | 0.49% |
| CHWBL 1.50 / opening | 77.4% [68.0–83.3] | 2.20× | 19.0 / 60 | 11.3% | 57 | – | 0.99% |
| CHWBL 1.25 / header>none | 47.5% [45.9–49.5] | 1.55× | 28.1 / 60 | 52% | 49 | – | 0.59% |
| CHWBL 2.00 / header>none | 46.2% [39.0–52.7] | 1.30× | 36.1 / 94 | 52% | 78 | – | 2.1% |
| pure affinity / kubeai-100c | 71.1% [68.9–73.2] | 0.87× | 54.9 / 154 | 0.3% | 145 | – | – |
| CHWBL 2.00 / kubeai-100c | 45.8% [44.6–46.5] | 0.88× | 54.0 / 104 | 16% | 80 | – | 6.7% |
| CHWBL 2.00 / prefix-2048 | 53.2% [49.4–59.5] | 0.98× | 47.9 / 100 | 17% | 79 | – | 6.9% |
| pure affinity / system | 63.9% [61.9–67.1] | 0.84× | 56.4 / 144 | 0.0% | 149 | – | – |
| CHWBL 2.00 / system | 45.8% [43.0–49.9] | 0.87× | 54.0 / 104 | 16% | 79 | – | 7.3% |

Per seed, CHWBL 2.0 on the opening hash reached 83.0%, 81.2% and 83.3%, and on header>opening 83.3%, 81.2% and 81.8%, against round robin's 44.0%, 43.3% and 42.9%.

- **Round robin and pure affinity each grow one hot Instance.** In every seed, round robin's per-Instance hit rates are about 52% on three Instances and 16–17% on the fourth, whose peak is about 130 in flight against 16–19 on the others. Pure affinity shows the same: about 96% on three Instances, 19–21% and 116–125 in flight on one. The mechanism is a feedback loop. Many running requests pin KV blocks, so cached prefixes are evicted, so prompts miss, so prefill is slower, so more requests pile up. Round robin ignores load and keeps sending a fixed share of requests there. Affinity keeps its keys there.
- **c = 2.0 caps the pile but does not prevent it here; c = 1.25 does.** At c = 2.0 one Instance still runs at 17–20% hit with 70–75 in flight, above the engine's 48 sequences, while the other three reach 88–94%. The hot Instance is the same feedback loop; the bound only stops it, at each placement, from passing twice the mean load. At c = 1.25 every Instance stays at 37–48 in flight; on the opening key all four reach 69–94% hit, on header>opening 60–94%. So c = 1.25 serves 2.85× round robin's output against 2.15× for c = 2.0, and its latency is lower (p95 40 s against 78 s).
- **c = 1.5 did worse than 1.25 and 2.0** here, with a wide seed range (68–83%). It lets a pile grow far enough to start the feedback loop, then spills it. The bound is not monotonic in this regime.
- **Session header alone is not enough.** With 35% of conversations sending a header and no fallback (`header>none`), CHWBL reaches only 46–48%. Falling back to the opening hash (`header>opening`) is as good as the opening hash alone, within seed noise.
- **Collapse keys.** A key that sees only the start of the request collapses whole client types onto one key:
  - KubeAI's default (first 100 characters of the first user message) puts 36% of conversations on one key, because agent-a's fixed preamble fills those characters;
  - the first 2,048 tokens fall inside the static system prompts of agent-a, agent-b and agent-c (top key 36%);
  - the system prompt alone puts 15% on one key (agent-c's prompt).

  In the tight regime, under pure affinity, these keys pile load onto one Instance and fall *below* round robin's throughput (0.84–0.90×); at c = 2.0 too (0.87–0.98×). Under c = 1.25 the bound spreads them, but 31–37% of turns move and the hit rate drops to 51–66%. In the ample regime the same keys under pure affinity reach 96.0% and 3.21–3.28×, so the collapse costs nothing there.

**Ample regime (3 M).** Every policy keyed on the opening or the header with opening fallback reaches 95.2–96.0%, against round robin's 51.1% [50.0–52.1] and least load's 56.7%, at 3.3× round robin's throughput. c = 2.0 reaches 96.0% on both keys; c = 1.25 moves 2.7–3.5% of turns and loses 0.7–0.8 points against c = 1.5, c = 2.0 or pure affinity. Round robin still forms its hot Instance.

**Scarce regime (750 k).** No opening-keyed configuration beats round robin reliably: round robin 27.9% [24.7–31.9], c = 2.0 / opening 32.4% [30.3–35.8] (loses seed 1), c = 1.25 / opening 24.9% [18.7–35.2] (loses seeds 1 and 3), pure affinity / opening 32.4% (the same placements as c = 2.0). Collapse keys do beat round robin's hit rate on every seed, at a throughput cost:

| Configuration (750 k) | Hit rate by seed | Throughput vs RR |
|---|---|---|
| round robin | 31.9 / 24.7 / 27.2% | 1.00× |
| CHWBL 2.0 / system | 40.0 / 41.8 / 40.6% | 0.80× |
| CHWBL 2.0 / kubeai-100c | 40.3 / 39.5 / 39.3% | 0.80× |
| pure affinity / kubeai-100c | 37.1 / 33.2 / 37.5% | 0.67× |
| pure affinity / system | 36.5 / 34.7 / 37.8% | 0.66× |
| pure affinity / prefix-2048 | 34.6 / 33.8 / 39.3% | 0.74× |

Concentrating the shared system prompts on few Instances keeps them cached even when the conversations' own contexts are evicted. That raises the hit rate, but the concentrated Instances run hot and the cluster delivers less. On the hit-rate criterion alone, CHWBL 2.0 / system passes the issue's test here; on throughput it loses a fifth.

### The bound

- **HBEP-5 rule, steady state: 0 placements above the bound**, for every CHWBL row at every size. This is the accounting identity under "The bound, and how it is checked", not a measurement.
- **The strict capacity is exceeded by one request** through KubeAI's `<=` at whole-number thresholds. At c = 2.0 that is 3.0% of placements on the opening key and 3.2% on header>opening in the tight regime, 0.02% and 0.01% in the ample one. At c = 1.25 it is 0.35% and 0.49% tight, 0.20% and 0.16% ample. With four members, c = 2.0's threshold (T+1)/2 is whole for every odd T, which is why it crosses the strict capacity so much more often. The steady table's "+1 over ceil" column holds the sums over seeds and the whole run, with the share of placements.
- **Over time, some Instance is above c × the current mean** for 49–52% of loaded time at c = 2.0 on the opening key in the tight regime (52–56% on header>opening), and 1–4% in the ample one. At c = 1.25: 25–43% tight, 18–20% ample. Placement is checked against the mean at that moment; requests then finish elsewhere and the mean falls. The bound limits where new requests go; it does not keep load within c × the mean between placements.
- The largest load seen at a placement, relative to the mean after it, is 2.5 at c = 2.0 and 1.6 at c = 1.25. Small loads set these: with four members and 7 in flight, c = 2.0 accepts a member at 4 (4 ≤ 2 × 8 / 4), which ends at 5 against a mean of 2.

### Drain, leave and handover

instance-1 changes at 3,000 s. "Checked" is the number of conversations with a request both before and after the event: about 150 of the 160 users. Hit rates are over the 5 and 10 minutes after each event's own time, against the same seed and policy without the event. The bound columns are for the balancer without the drain release (the release is in the next section).

**Ample regime (3 M tokens per Instance):**

| Policy / key | Event | Forced re-homes | Collateral | To new | Requests cut | Hit 0–5 min after (no event) | Hit 5–15 min after (no event) | Placements above bound, unreleased (per seed) |
|---|---|---|---|---|---|---|---|---|
| round robin | drain | 47 | 75 | – | 0 | 52.3% (51.2%) | 49.0% (50.8%) | – |
| pure affinity / opening | drain | 39 | 1 | – | 0 | 94.8% (96.0%) | 96.0% (96.0%) | – |
| CHWBL 2.00 / opening | drain | 39 | 1 | – | 0 | 94.8% (96.0%) | 96.0% (96.0%) | 0, 0, 0 |
| CHWBL 1.25 / opening | drain | 40 | 4 | – | 0 | 94.4% (94.9%) | 95.5% (95.1%) | 8, 8, 0 |
| CHWBL 2.00 / opening | leave | 40 | 1 | – | 30 | 94.9% (96.0%) | 96.0% (96.0%) | 0 (identity) |
| CHWBL 2.00 / opening | handover: join instance-4 at 3,000 s | 0 | 1 | 30 | 0 | 95.2% (96.0%) | 95.6% (96.0%) | 0, 0, 0 |
| | then drain instance-1 at 3,300 s | 35 | 1 | – | 0 | 95.0% (95.9%) | 96.0% (96.0%) | |
| CHWBL 1.25 / opening | handover: join at 3,000 s | 0 | 8 | 30 | 0 | 93.8% (94.9%) | 94.5% (95.1%) | 22, 4, 11 |
| | then drain at 3,300 s | 33 | 3 | – | 0 | 93.8% (94.8%) | 95.0% (95.0%) | |

- **Re-homing is what consistent hashing promises.** A drain forces the drained Instance's conversations to move, 39–40 of about 150 (a quarter), and moves only 1–4 others. A join moves 30 conversations (a fifth) onto the new Instance. Round robin moves about three quarters of all conversations at every event, as it does between any two turns.
- **The dip is small and short when the cache is ample.** CHWBL and pure affinity lose about 0.5–1.2 points in the first 5 minutes after a drain and are back within 5–15 minutes. Each forced conversation recomputes its context once.
- **A leave behaves like a drain plus cut requests.** 25–30 in-flight requests are cut. The simulated clients send each one again at once, and it lands on the conversation's new home. The hit rate is the same as for the drain.

**Tight regime (1.5 M):** losing an Instance without a replacement drops the cluster into the scarce regime. Three Instances hold 4.5 M tokens against a 6.2 M working set.

| Policy / key | Event | Forced | Collateral | To new | Hit 0–5 min after (no event) | Hit 5–15 min after (no event) | Above bound, unreleased (per seed) | Max over (req.) |
|---|---|---|---|---|---|---|---|---|
| round robin | drain | 13 | 94 | – | 41.5% (43.2%) | 42.3% (43.3%) | – | – |
| pure affinity / opening | drain | 18 | 0 | – | 79.1% (82.1%) | 79.3% (82.1%) | – | – |
| CHWBL 2.00 / opening | drain | 29 | 23 | – | 78.4% (81.1%) | 76.5% (81.8%) | 0, 18, 17 | 3.3 |
| CHWBL 2.00 / header>opening | drain | 32 | 20 | – | 77.7% (81.4%) | 76.6% (81.5%) | 4, 18, 7 | 2.3 |
| CHWBL 1.25 / opening | drain | 42 | 10 | – | 56.9% (85.6%) | 54.6% (91.2%) | 3, 6, 75 | 6.5 |
| CHWBL 1.50 / opening | drain | 42 | 19 | – | 61.3% (76.3%) | 60.8% (76.8%) | 21, 64, 19 | 15.5 |
| CHWBL 2.00 / opening | leave | 30 | 21 | – | 77.8% (81.1%) | 75.7% (81.8%) | 0 (identity) | – |
| CHWBL 1.25 / opening | leave | 43 | 13 | – | 55.7% (85.6%) | 54.5% (91.2%) | 0 (identity) | – |
| CHWBL 2.00 / opening | handover: join instance-4 at 3,000 s | 0 | 26 | 42 | 84.0% (81.1%) | 82.5% (81.8%) | 0, 27, 12 | 4.0 |
| | then drain instance-1 at 3,300 s | 28 | 26 | – | 82.7% (82.2%) | 82.2% (81.6%) | | |
| CHWBL 1.25 / opening | handover: join at 3,000 s | 0 | 20 | 35 | 91.0% (85.6%) | 89.9% (91.2%) | 30, 6, 22 | 2.0 |
| | then drain at 3,300 s | 31 | 6 | – | 88.2% (90.5%) | 92.7% (91.4%) | | |

- **Without a replacement, c = 1.25 does not recover; c = 2.0 loses a few points.** c = 1.25 falls 29–37 points and stays down. With one Instance fewer, load per Instance rises a third, c = 1.25 spills more, and every spill copies a context onto a second Instance, which pushes the cache further into thrash. c = 2.0 and pure affinity lose 3–5 points. The best bound depends on the regime: tight bounds win when the cache holds the working set, looser ones once it cannot.
- **Make-before-break works, with a short dip at c = 1.25.** With the replacement joining 300 s before the drain, the drain's first 5 minutes cost c = 1.25 about 2 points on average (85.0, 94.3 and 85.1% against 91.8, 88.8 and 90.9% by seed: two seeds lose 6–7 points, one gains), and it is back at or above the no-event run within 5–15 minutes. At c = 2.0 the drain costs nothing measurable (83.1, 81.8, 83.1% against 83.1, 82.2, 81.2%). This supports HBEP-5's drain rule. The first version of this report measured the handover's drain row over 3,000–3,300 s, before the drain, which hid the dip.
- **c = 1.25's hit rate also swings in steady state.** In the 10 minutes before the drain, c = 1.25 / opening averaged 93.1, 72.6 and 49.9% by seed, against an 83–92% run mean. Transient piles form and dissolve at the tight bound; c = 2.0's windows stayed at 78–84%.
- In the tight regime, pure affinity forces only 18 of about 150 conversations to move, not a quarter. Its hot Instance holds most of the active conversations, because conversations homed there run longest (Little's law), and instance-1 was a cold Instance in all three seeds. The ring itself is balanced: on 200,000 distinct opening keys, instance-1 is home to 26.1% (`TestRingShareIsBalanced`).

### KubeAI's drain accounting, and the release

Without the release, CHWBL places requests above the bound computed over the current members during drains and handovers:
- at 1.5 M, in every c = 1.25 run (12 of 12, drain and handover, both keys), in 10 of 12 c = 2.0 runs, and in 8 of 12 c = 1.5 runs. The largest count in one run is 75 (c = 1.25 / opening, seed 3), then 64 (c = 1.5 / opening, seed 2); for c = 2.0 it is 28. The largest excess is 15.5 requests over the bound (c = 1.5), 6.5 at c = 1.25 and 4.0 at c = 2.0;
- at 3 M, in some c = 1.25 runs (drain: 8, 8 and 0 on the opening key), in one c = 1.5 handover run, and in no c = 2.0 run.

A leave never breaks the bound: cut requests are released at the cut, so the counts stay consistent. The cause is upstream's drain accounting: a terminating Instance's open requests stay in `totalInFlight` until they finish, while n drops. KubeAI's total exceeds the members' total by up to 49 requests here, which raises KubeAI's mean over the members' by up to 16 requests (the excess divided by three members).

The front-side release fixes the accounting without touching the copied algorithm: at drain start, `front.Balancer.Drain` releases the draining Instance's open requests from the count, and their completions later do nothing (`ReleaseOnDrain`). In all 24 released runs (drain and handover, both keys, three bounds, both sizes) KubeAI's total equalled the members' total at every placement. That is the release's test. Zero placements above the bound then follow from the accounting identity; they are not a separate measurement. The release changes the hit rate by no more than seed noise; its value is a correct bound, not cache hits:

| Size | Bound / key | Event | Above bound, unreleased (sum over seeds) | KubeAI total minus members' (max, unreleased → released) | Hit 0–5 min after the drain (unreleased → released; no event) |
|---|---|---|---|---|---|
| 1.5 M | 2.00 / opening | drain | 35 | 23 → 0 | 78.4% → 78.0% (81.1%) |
| 1.5 M | 2.00 / header>opening | drain | 29 | 28 → 0 | 77.7% → 77.1% (81.4%) |
| 1.5 M | 1.25 / opening | drain | 84 | 39 → 0 | 56.9% → 57.1% (85.6%) |
| 1.5 M | 1.50 / opening | drain | 104 | 49 → 0 | 61.3% → 64.7% (76.3%) |
| 1.5 M | 2.00 / opening | handover | 39 | 23 → 0 | 82.7% → 82.5% (82.2%) |
| 1.5 M | 2.00 / header>opening | handover | 54 | 21 → 0 | 82.6% → 81.9% (81.3%) |
| 1.5 M | 1.25 / opening | handover | 58 | 24 → 0 | 88.2% → 87.8% (90.5%) |
| 3 M | 2.00 / opening | drain | 0 | 35 → 0 | 94.8% → 94.8% (96.0%) |
| 3 M | 1.25 / opening | drain | 16 | 26 → 0 | 94.4% → 94.3% (94.9%) |
| 3 M | 1.25 / opening | handover | 37 | 24 → 0 | 93.8% → 93.7% (94.8%) |

The handover rows are the drain at 3,300 s, measured from 3,300 s. All 24 released runs are in `results/drainfix-*.md`.

### Sensitivity (1.5 M tokens per Instance, seeds 1–3)

One engine parameter changed at a time; the first row is the steady sweep. Hit rates are means over seeds.

| Variant | RR | c = 1.25 / opening | c = 1.5 / opening | c = 2.0 / opening | Pure affinity / opening | c = 1.25 / header>opening | c = 2.0 / header>opening | Throughput vs RR: c = 1.25 / opening | c = 2.0 / opening |
|---|---|---|---|---|---|---|---|---|---|
| prefill 25,000 tokens/s, block 64 (the steady sweep) | 43.4% | 85.9% | 77.4% | 82.5% | 83.4% | 84.0% | 82.1% | 2.85× | 2.15× |
| prefill × 0.5 (12,500) | 45.3% | 74.5% | 74.0% | 80.5% | 82.4% | 70.3% | 86.6% | 2.88× | 3.02× |
| prefill × 2 (50,000) | 36.5% | 89.6% | 87.5% | 84.8% | 85.0% | 89.8% | 82.0% | 1.61× | 1.41× |
| block 16 tokens | 41.1% | 86.2% | 74.4% | 82.8% | 83.5% | 84.7% | 81.3% | 2.86× | 2.14× |

- **The pass survives all three.** Every bound on both keys beats round robin on every seed in every variant; the lowest single run is 61.2% against round robin's 45.4% (c = 1.25 / header>opening at half the prefill rate).
- **The prefill rate flips the ranking of c.** At half the rate, c = 2.0 leads on hit rate (80.5% against 74.5% on the opening key, 86.6% against 70.3% on header>opening) and on throughput (3.02× against 2.88×), although its seed 2 drops to 1.46×. At twice the rate, c = 1.25 leads (89.6% against 84.8%, 1.61× against 1.41×). The throughput factor itself moves from about 1.4–1.6× to 2.9–3.0×: it measures how hard the hot-Instance feedback loop bites, which depends on how much a miss costs.
- **Block size changes nothing material.** With 16-token blocks, round robin drops 2.3 points and every CHWBL row moves by less than one point, except c = 1.5 (−3.0, within its wide seed range); throughput factors move by 0.01×.
- **Agreement with the review's independent run.** The Opus review reran the same variants with its own driver. Its hit rates agree with these to the tenth of a point in every cell it reported. Its throughput factors agree to within 0.01×; the difference is the throughput definition (it counted output by dispatch time, this report by completion time: 2.87× against 2.88× for c = 1.25 at half the prefill rate).

### Choice of c

HBEP-5's owner set c = 2.0 as the default, per deployment, because it is robust in the degraded cases the design expects: late replacements, scarce cache and slower prefill. Operators may lower it to 1.25 where spare or warm capacity makes replacements reliable. The evidence, opening key, means over seeds 1–3:

| Condition | c = 1.25 | c = 2.0 | Favours |
|---|---|---|---|
| Tight cache (1.5 M), full membership: hit rate / throughput vs RR | 85.9% / 2.85× | 82.5% / 2.15× | 1.25 |
| Tight: lowest per-Instance hit rate per seed; Instance peaks in flight | 69–90%; 37–48 | 17–19%; up to 70–73 on the hot Instance | 1.25 |
| Tight: p95 latency | 40 s | 78 s | 1.25 |
| Tight, unreplaced drain (released): hit 0–5 / 5–15 min after (no event) | 57.1% / 54.8% (85.6% / 91.2%) | 78.0% / 76.6% (81.1% / 81.8%) | 2.0 |
| Tight, handover drain (released): hit 0–5 min after (no event) | 87.8% (90.5%): a dip, recovered within 15 min | 82.5% (82.2%): no dip | 1.25 higher, 2.0 steadier |
| Ample cache (3 M): hit rate / throughput | 95.2% / 3.28× | 96.0% / 3.30× | 2.0, by 0.8 points |
| Scarce cache (750 k): hit rate; seeds above RR | 24.9%; 1 of 3 | 32.4%; 2 of 3 | 2.0 (neither passes) |
| Prefill × 0.5: hit rate / throughput | 74.5% / 2.88× | 80.5% / 3.02× | 2.0 |
| Prefill × 2: hit rate / throughput | 89.6% / 1.61× | 84.8% / 1.41× | 1.25 |
| Block 16: hit rate / throughput | 86.2% / 2.86× | 82.8% / 2.14× | 1.25 |
| Strict capacity: placements one over ⌈c × mean⌉, tight / ample | 0.35% / 0.20% | 3.0% / 0.02% | 1.25 tight, 2.0 ample |
| Time with some Instance above c × mean, tight / ample | 25–43% / 18–20% | 49–52% / 1–4% | 1.25 tight, 2.0 ample |
| Unreleased drain: placements above the bound, sum over seeds, 1.5 M / 3 M | 84 / 16 | 35 / 0 | 2.0 (moot with the release) |

In short, c = 1.25 is best for throughput at full membership in the tight regime and when prefill is fast; c = 2.0 holds up better after an unreplaced drain, in the scarce regime, in the ample regime and when prefill is slow. At c = 2.0 the tight regime still grows one hot Instance; HBEP-5's per-Instance caps (open question 6) are the guard for it.

## What this means for the HBEPs

- **HBEP-5, open question 1 (conversation key).** Honour a session header when the client sends one. Otherwise hash the *whole* opening: the system prompt plus the first user message. Do not hash a fixed token prefix or the first N characters (KubeAI's default): agent clients with long static system prompts or fixed preambles collapse onto a few keys, and in the tight regime that falls below round robin's throughput under pure affinity. A header alone, without the opening fallback, helps only the clients that send one. Compaction changes the opening, so a conversation with no header moves once after compacting; the header avoids that, but the effect was within noise here. The opening key assumes openings that stay fixed (see "Still unknown").
- **HBEP-5, open question 3 (bound).** In-flight requests, under KubeAI's reading of the bound. The owner set c = 2.0 as the default, per deployment, with 1.25 as an operator option where replacements are reliable. The evidence is the trade-off table under "Choice of c": 1.25 wins at full membership in the tight regime, 2.0 after an unreplaced drain, in the scarce regime and at half the prefill rate. Re-measure c on real engines, because the prefill rate and the cache regime flip its ranking. The bound limits placements, not load over time: at c = 2.0 in the tight regime, some Instance sat above 2 × the mean for about half the loaded time, and one Instance still ran hot (see below).
- **HBEP-5, front requirements.** Serialize selection and reservation, release each request exactly once, and release a draining Instance's requests from the count at drain start. All three are now HBEP-5 requirements. KubeAI's code does none of them, so the front wraps the copied balancer rather than using it as is.
- **HBEP-5, Risks ("affinity piles return").** With an exact in-flight count, the bound prevented the pile in simulation; the field causes (counter undercount, preemption) were not modelled. Pure affinity piled one Instance to about 120 in flight at 19–21% hit while three others idled, the failure HBEP-5's Motivation describes. c = 1.25 kept every Instance at 37–48 in flight. c = 2.0 capped the hot Instance at 70–75 in flight but did not stop it forming: in the tight regime one Instance still ran at 17–20% hit. HBEP-5's per-Instance caps (open question 6) are the guard against that remaining pile.
- **HBEP-5, draining, and the KubeAI reuse recorded in the evaluation.** Copying KubeAI's balancer is sound, but its drain accounting is not what HBEP-5 needs, by upstream's own design (PR #747). Without the release, placements broke the bound in all 12 drain and handover runs at c = 1.25 in the tight regime and in 10 of 12 at c = 2.0; at 3 M, c = 2.0 had none and c = 1.25 had some. This matches the HBEP-5 risk that "the load measure must count cut and abandoned requests": it must also *stop* counting what no longer competes for members, and count each request once.
- **HBEP-5 and HBEP-7, make-before-break.** The simulation supports it. A drain with the replacement already serving cost nothing measurable at c = 2.0 and a 2-point dip for 5 minutes at c = 1.25. A drain without one cost 3–5 points at c = 2.0, and 29–37 points for good at c = 1.25, when the cache was tight, because the cluster dropped into the scarce regime. HBEP-7's event for lost capacity is worth emitting.
- **Capacity (HBEP-4, HBEP-7).** Prefix-cache affinity on the opening key pays only when the cluster's KV cache holds about the working set or more. At 2× it no longer beat round robin reliably. Sizing Instances' KV against the expected working set is a capacity question for HBEP-4 and HBEP-7.

## Still unknown

The conditions below can flip a conclusion. Each was either measured here (with the result) or not modelled.

- **Prefill rate (measured).** It sets how hard the hot-Instance feedback loop bites, so it flips the ranking of c and changes the throughput factor (see "Sensitivity").
- **Cache regime (measured).** At about 2× working set to cluster KV, the opening key no longer beats round robin reliably, and collapse keys do on hit rate at a throughput cost. The regime boundaries move with every workload and engine parameter.
- **Front-side queueing (not modelled).** HBEP-5:81 puts requests above an Instance's cap in a queue at the front. Here they wait at the Instance and count as in flight. With a front queue, the in-flight count the bound reads saturates at the cap, so the ranking of c under HBEP-5's own queueing design is untested.
- **Full reservation versus incremental KV (not modelled).** A request here reserves blocks for its whole prompt and output at admission and waits otherwise. vLLM allocates blocks as decoding proceeds, interleaves chunked prefill, and preempts and recomputes when the cache is full. Those change when an Instance stalls, which drives the pile-up mechanism.
- **No preemption and no client cancellation (not modelled).** The field pile-ups (L-4.3, L-4.4) came from an in-flight counter that missed cut calls and from preemption at a full KV cache. The simulator's counter is exact and nothing is preempted, so neither cause was tested.
- **Stable versus rewritten openings (not modelled).** Every opening here is distinct and fixed for a conversation, apart from compaction. A client that rewrites its system prompt during a conversation (a dynamic date, a tool list), or many conversations with the same complete opening, would weaken the opening key against the session header.
- **One worker per Instance.** HBEP-5's second level, the inner router across an Instance's workers (open question 2), is not modelled.
- **Other load measures,** such as sessions or KV use (open question 3), were not compared.
- **The front-side drain release and the serialized wrapper** were tested in the simulator and in unit tests only. Their interaction with retries (open question 5) and with a front restart (open question 11) is open.
- **All parameters are assumptions:** the client mix, prompt and reply lengths, session lengths, think time, prefill rate, decode time and KV size. Measured agent sessions would settle them; the real-vLLM GPU run in the issue's Dependencies is the next check. llm-d-inference-sim was not tried.

## Deviations

- The fork follows HBEP-15's convention for copied code, which is on `main`: `third_party/forked/kubeai/` with upstream's `LICENSE` and an `ORIGIN.md`. The `LICENSES/` tree and the copy check that HBEP-15 also describes do not exist yet.
- The front-side drain release and the serializing, release-once wrapper (`front/`) are this spike's code, not KubeAI's. The copied algorithm is unchanged.
- Least load is the simulator's own deterministic version, not KubeAI's map-order one.
