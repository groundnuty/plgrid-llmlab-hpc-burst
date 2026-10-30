# Spike S10b: default load bound for HBEP-5 (c, hard cap, adaptive)

Issue #45, a follow-up to S10 (#27, PR #34). It builds on S10's simulator and its front wrapper (`spikes/s10`), and the forked KubeAI balancer under `third_party/forked/kubeai` is unchanged.

## Question

S10 left HBEP-5 a trade-off: c = 2.0 holds up after a drain, when cache is scarce and at half prefill, but at full membership with a tight cache one Instance carries 70-75 requests in flight against an engine limit of 48 and its hit rate falls to about 18%. Which default load-bound policy never exceeds the engine limit at full membership, and loses least after an unreplaced drain and in the scarce regime?

## Answer

**By the rule fixed before the runs, option 3 (c = 2.0 plus a hard cap of 48 in flight per Instance, spilling to the next Instance on the ring, queueing at the front when all are full) passes, and it passes by default: it is the only option that never exceeds the engine limit at full membership in all three regimes, and it has the largest loss after a drain. After an unreplaced drain its hit rate falls to 22.3% (0-5 min), 17.5% (5-15 min) and 21.1% (15-30 min, to the end of arrivals), against round robin's 41.5%, 42.3% and 41.7%. Taken as a standalone default, the cap is unfit for the drain case HBEP-5 expects. No other option passes.**

- **Condition 1, never above 48 in flight at full membership** (seeds 1-3; tight, scarce and half-prefill regimes):
  - Option 3 meets it: peak 48 and 0 s above the limit in every regime and seed. The cap binds: 9,856 / 1,865 / 13,265 spills over the three seeds in the three regimes.
  - Option 2 (c = 2.0, S10's default) fails everywhere: peaks of 75, 69 and 76, and 12,345 / 9,734 / 13,247 s above 48 summed over seeds (each run lasts a little over 4,800 s).
  - Options 1 (c = 1.25) and 4 (adaptive) are the same runs at full membership. They meet it in the tight regime (peak 46) and miss it by 1-2 requests in the other two: peaks of 49 (scarce, 6 s above 48 in all) and 50 (half prefill, 228 s). A bound relative to the mean cannot hold an absolute limit once the cluster is saturated: S10 measured a mean of 143 requests in flight over four Instances in the scarce regime, and 136-145 at half prefill.
- **Loss, among the options that meet condition 1.** With one qualifier the ranking is trivial, so the tables give all four. Hit rate 0-5 / 5-15 / 15-30 min after the drain: c = 2.0 77.1% / 76.6% / 76.0%, adaptive 62.5% / 57.8% / 57.8%, c = 1.25 52.9% / 51.6% / 50.5%, option 3 22.3% / 17.5% / 21.1%. The drop is sustained, not a transient. In the scarce regime all four are within 1.6 points (c = 2.0 31.7%, option 3 30.9%, c = 1.25 and adaptive 30.1%) and single seeds range over 21-38%.
- **If condition 1 is read on the tight regime alone,** options 1, 3 and 4 qualify and the rankings split: adaptive is best after the drain, option 3 is best in the scarce regime by 0.8 points, which is inside the seed spread. No weighting is invented, and none passes outright under that reading.
- **The cost of the cap at full membership** is 7 points of hit rate in the tight regime (75.2% against 82.1% for c = 2.0, and 84.0% for c = 1.25), for a p95 of 61 s against 78 s. The hottest Instance still has a low hit rate (36.7%, against 18.4% for c = 2.0 and 77.3% for c = 1.25).
- **Option 4 switches at the drain:** in all three seeds the bound went from 1.25 to 2.0 at 3,005.0 s, 5 s after the drain at 3,000 s and within one 10 s control step. It recovers only part of the way: 62.5% / 57.8% after the drain against 77.1% / 76.6% for c = 2.0, because it ran at 1.25 until the drain.
- **Reproduction.** 60 cells (round robin, c = 1.25 and c = 2.0, both keys, four regimes, three seeds) equal the rows S10 committed, on requests, prompt tokens, hit tokens and completed output (`results/regression-vs-s10.txt`). The additions changed none of S10's behaviour.

## Results

Key `header>opening`, means over seeds 1-3 unless a column says max or sum. Round robin runs in each regime as the baseline for throughput. Per-run values are in `results/cells.csv`, the tables' source in `results/summary.csv`, and the pass rule's inputs in `results/verdict.md` (which also gives the same for the plain `opening` key).

### Full membership

**Tight (1.5 M KV per Instance), full membership**

| option | hit rate [min, max] | throughput vs RR | p95 latency (s) | peak in flight (max over seeds) | time above 48 (s, sum over seeds) | hottest Instance: peak in flight / hit rate | placements above the bound (sum) | spills (sum) |
|---|---|---|---|---|---|---|---|---|
| round robin | 43.4% [42.9%, 44.0%] | 1.00x | 166.1 | 131 | 15305 | 129.3 / 17.0% | - | - |
| 1. c = 1.25 | 84.0% [79.1%, 90.7%] | 2.83x | 37.3 | 46 | 0 | 45.0 / 77.3% | 0 | 0 |
| 2. c = 2.0 | 82.1% [81.2%, 83.3%] | 2.13x | 78.5 | 75 | 12345 | 73.3 / 18.4% | 0 | 0 |
| 3. c = 2.0 + cap 48 | 75.2% [71.5%, 79.5%] | 2.10x | 61.2 | 48 | 0 | 48.0 / 36.7% | 0 | 9856 |
| 4. adaptive 1.25 / 2.0 | 84.0% [79.1%, 90.7%] | 2.83x | 37.3 | 46 | 0 | 45.0 / 77.3% | 0 | 0 |

**Scarce (750 k KV per Instance)**

| option | hit rate [min, max] | throughput vs RR | p95 latency (s) | peak in flight (max over seeds) | time above 48 (s, sum over seeds) | hottest Instance: peak in flight / hit rate | placements above the bound (sum) | spills (sum) |
|---|---|---|---|---|---|---|---|---|
| round robin | 27.9% [24.7%, 31.9%] | 1.00x | 125.4 | 122 | 15008 | 110.0 / 17.1% | - | - |
| 1. c = 1.25 | 30.1% [21.0%, 37.2%] | 1.07x | 71.0 | 49 | 6 | 48.7 / 16.7% | 0 | 0 |
| 2. c = 2.0 | 31.7% [22.7%, 37.9%] | 1.02x | 80.2 | 69 | 9734 | 64.3 / 17.0% | 0 | 0 |
| 3. c = 2.0 + cap 48 | 30.9% [21.7%, 38.0%] | 1.04x | 72.7 | 48 | 0 | 48.0 / 16.8% | 0 | 1865 |
| 4. adaptive 1.25 / 2.0 | 30.1% [21.0%, 37.2%] | 1.07x | 71.0 | 49 | 6 | 48.7 / 16.7% | 0 | 0 |

**Half prefill (12,500 tokens/s)**

| option | hit rate [min, max] | throughput vs RR | p95 latency (s) | peak in flight (max over seeds) | time above 48 (s, sum over seeds) | hottest Instance: peak in flight / hit rate | placements above the bound (sum) | spills (sum) |
|---|---|---|---|---|---|---|---|---|
| round robin | 45.3% [45.1%, 45.4%] | 1.00x | 375.9 | 146 | 18835 | 144.7 / 17.0% | - | - |
| 1. c = 1.25 | 70.3% [61.2%, 76.1%] | 2.58x | 110.0 | 50 | 228 | 49.3 / 48.4% | 0 | 0 |
| 2. c = 2.0 | 86.6% [85.4%, 87.4%] | 3.72x | 142.4 | 76 | 13247 | 74.0 / 18.4% | 0 | 0 |
| 3. c = 2.0 + cap 48 | 74.9% [73.8%, 75.5%] | 2.83x | 117.2 | 48 | 0 | 48.0 / 38.4% | 0 | 13265 |
| 4. adaptive 1.25 / 2.0 | 70.3% [61.2%, 76.1%] | 2.58x | 110.0 | 50 | 228 | 49.3 / 48.4% | 0 | 0 |

"Peak in flight" counts requests waiting at the Instance. "Placements above the bound" is zero for options 1, 2 and 4 by the accounting identity S10 describes; for option 3 no spill landed above the bound either.

### After an unreplaced drain (instance-1 drained at 3,000 s)

Hit rates are over the windows after 3,000 s (the 15-30 min window ends with arrivals at 4,800 s), with the same option's run without the drain in brackets. Throughput and p95 are from 3,000 s to the end of arrivals at 4,800 s. After the drain 3 Instances serve 160 users, so every option except the cap spends the whole 1,800 s above 48.

| option | hit 0-5 min after (no drain) | hit 5-15 min after (no drain) | hit 15-30 min after (no drain) | throughput vs RR from 3,000 s | p95 latency from 3,000 s (s) | peak in flight after the drain | time above 48 after the drain (s, sum over seeds) | held at the front (sum) |
|---|---|---|---|---|---|---|---|---|
| round robin | 41.5% (43.2%) | 42.3% (43.3%) | 41.7% (43.0%) | 1.00x | 183.8 | 140 | 5400 | - |
| 1. c = 1.25 | 52.9% (82.7%) | 51.6% (81.4%) | 50.5% (84.8%) | 1.62x | 79.3 | 65 | 5296 | 0 |
| 2. c = 2.0 | 77.1% (81.4%) | 76.6% (81.5%) | 76.0% (80.7%) | 2.12x | 110.7 | 103 | 5401 | 0 |
| 3. c = 2.0 + cap 48 | 22.3% (70.1%) | 17.5% (66.5%) | 21.1% (67.8%) | 1.06x | 83.1 | 48 | 0 | 11041 |
| 4. adaptive 1.25 / 2.0 | 62.5% (82.7%) | 57.8% (81.4%) | 57.8% (84.8%) | 1.64x | 92.8 | 96 | 5335 | 0 |

**Option 3's front queue.** The queue stayed empty at full membership. After the drain, per seed:

| seed | requests held at the front | placements (whole run) | queue max | wait mean / p95 / max (s) | spills (whole run) |
|---|---|---|---|---|---|
| 1 | 3,730 | 32,459 | 14 | 2.44 / 5.13 / 9.67 | 3,496 |
| 2 | 3,779 | 28,604 | 14 | 1.88 / 4.39 / 7.55 | 5,060 |
| 3 | 3,532 | 30,012 | 14 | 2.21 / 4.78 / 7.63 | 4,099 |

Waits run from arrival to placement and count in the latency figures. About 11-13% of requests wait, and spills are 11-18% of the run's placements. The same seeds spilled 2,746 / 3,615 / 3,495 times without the drain, so the drain adds 750 / 1,445 / 604 spills and 3,500-3,800 held requests, and the hit rate falls 48 points. Per 5-minute window on seed 1 (`results/windows.csv`), option 3 held 84-90% up to 3,000 s, then 27%, 18%, 17%, 15%, 16%, 16% for the six windows to 4,800 s, and recovered only once arrivals stopped. Its whole-window hit rate (65.3% over the seeds, 71.2% on seed 1) is higher than the post-drain figures because it processed far less prompt volume after the drain (27-36 M prompt tokens per window against 89-101 M before, and 46-61 M for c = 2.0 after). The mechanism is not isolated here. The candidate is the held requests: a request placed from the queue lands where a slot has just freed, which copies its context onto another Instance in a cache that is already short (3 Instances hold 4.5 M tokens against a 6.2 M working set, S10).

### The pass rule

Key header>opening, 3 seeds.

| option | peak in flight: tight / scarce / half prefill (max over seeds) | time above 48, summed over seeds (s): tight / scarce / half prefill | condition 1, tight | condition 1, all three | hit 0-5 min after drain (drop vs no drain) | hit 5-15 min after drain (drop) | scarce hit (vs RR) |
|---|---|---|---|---|---|---|---|
| c=1.25 | 46 / 49 / 50 | 0 / 6 / 228 | true | false | 52.9% (29.8 pts) | 51.6% (29.8 pts) | 30.1% (+2.2 pts) |
| c=2.0 | 75 / 69 / 76 | 12345 / 9734 / 13247 | false | false | 77.1% (4.4 pts) | 76.6% (4.9 pts) | 31.7% (+3.8 pts) |
| c=2.0+cap48 | 48 / 48 / 48 | 0 / 0 / 0 | true | true | 22.3% (47.8 pts) | 17.5% (48.9 pts) | 30.9% (+3.0 pts) |
| adaptive 1.25/2.0 | 46 / 49 / 50 | 0 / 6 / 228 | true | false | 62.5% (20.2 pts) | 57.8% (23.6 pts) | 30.1% (+2.2 pts) |

Qualifying on condition 1 in all three full-membership regimes: [c=2.0+cap48]. It is the only qualifier, so it passes by default; the drain and scarce comparison does not discriminate.
Qualifying on condition 1 in the tight regime only: [c=1.25 c=2.0+cap48 adaptive 1.25/2.0]. Best after the drain: adaptive 1.25/2.0. Best in the scarce regime: c=2.0+cap48. The rankings split.

### The plain `opening` key

The same sweep with the opening hash alone reproduces S10's "Choice of c" table (c = 1.25: 57.1% / 54.8% after the drain, scarce 24.9%; c = 2.0: 78.0% / 76.6%, scarce 32.4%) and gives the same verdict: only option 3 meets condition 1 in all three regimes (peaks 48, 49, 50 for c = 1.25 and 73, 67, 79 for c = 2.0), and its loss after the drain is again the largest (24.7% / 18.5%). See `results/verdict.md`.

## The two non-vacuous checks (committed tests, `spikes/s10b/study/study_test.go`)

1. **The cap binds in the tight regime at full membership** (`TestCapBindsInTheTightRegime`, seeds 1-3, full size). Spills of option 3: 2,746 / 3,615 / 3,495, all above 0 and all in the measurement window. The control that makes the count meaningful: plain c = 2.0 on the same seeds peaks at 71 / 74 / 75 in flight, so the workload does reach the limit. With the cap the peak is 48 and the time above 48 is 0. The test fails if plain c = 2.0 stays at or below 48, because then a cap that never binds would pass too.
2. **The adaptive option switches at the drain** (`TestAdaptiveSwitchesAtTheDrain`, drain regime). The switch is at 3,005.0 s for a drain at 3,000 s and a control step of 10 s, asserted to fall in (3,000, 3,010]. Every placement before it used 1.25 and every placement after it used 2.0. In the three full-membership regimes the bound never changes and the run equals option 1's exactly (requests, hit tokens, completed output, peak). A small hand-checked version is in `spikes/s10/sim` (`TestAdaptiveBoundSwitchesAtTheFirstTickAfterTheDrain`: drain at 2.3 s, step 1 s, switch at 2.5 s).

## Method

### What was added to S10 (the forked balancer is unchanged)

All code is in S10's module, because the simulator's router and engine are package-internal. Every new `Policy` field is zero by default, so S10's policies behave as before; `TestMatchesS10CommittedResults` checks that against S10's committed result files.

- `spikes/s10/front/ring.go`: a replica of KubeAI's hash ring (xxhash v1 of name + replica index, 256 points per Instance, home = first point at or after the key's hash). KubeAI keeps its ring private, and the fork stays unchanged, so the wrapper rebuilds it. `TestRingMatchesUpstream` checks the replica's home against the fork's own pick for 5,000 keys at 4 and 3 members. `TestRingWalkMatchesUpstreamUnderLoad` checks the walk order against the fork's picks from uneven loads at c = 1.25 and 2.0.
- `spikes/s10/front/cap.go`, option 3: `PickPrefixHashCapped`. In one critical section: KubeAI's CHWBL picks at c; if the choice already holds 48 requests (its count before the request), upstream's reservation is released and the request goes to the next Instance on the replica ring after the choice that holds fewer than 48. The spill target is reserved through upstream's own picker with a pin key whose ring home is the target and an infinite bound, so every spill is in KubeAI's counters and the bound's mean stays complete (a spill that bypassed them would be the L-4.3 failure; the simulator's `crossCheck` fails the run if the counts diverge). If every member holds 48, it returns `ErrAllAtCap` and reserves nothing; the engine keeps the request in a FIFO at the front and retries when a request completes or an Instance joins. A request's latency counts from its arrival, so the wait at the front is in p95.
  - Wrap-around past the end of the ring is unreachable: the choice passed the bound while at the cap, so every member before it on the walk failed the bound, which means it holds more than the choice, so it is at the cap too.
- `spikes/s10/front/cap.go`, option 4: `AdaptiveBound{Tight, Loose}.Percent(ready, desired)`: tight while ready >= desired, loose while fewer are ready. The simulator's deployment controller evaluates it every control step (10 s, an assumption), with ticks at 5, 15, ... s so that a drain never coincides with a tick. `desired` is the cluster's size at time 0 (4). An unreplaced drain leaves 3 ready, so the bound switches to 2.0 at the first tick after it.
- `spikes/s10/sim`: `Policy.Cap`, `Policy.AdaptiveLoose`, `Policy.ControlStep`; the front queue in the engine; and new measures: time with any Instance above the engine limit, spills, front waits, bound switches, and time-resolved peak, time above the limit, throughput and latency (60 s buckets) for windows that start at the drain. The bound checks (placements above the bound) use the bound in effect at each placement.

### Options, regimes, measures

All options use S10's key `header>opening` (the session header when the client sends one, else the hash of the opening: system prompt plus first user message), the serialized front wrapper, and the release of a draining Instance's requests at drain start (HBEP-5's requirement since S10). Seeds 1, 2, 3. The same four options also run with the plain `opening` key, to compare with S10's "Choice of c" table.

| Option | Policy |
|---|---|
| 1 | CHWBL, c = 1.25 |
| 2 | CHWBL, c = 2.0 |
| 3 | CHWBL, c = 2.0, hard cap of 48 in flight per Instance, spill to the next Instance on the ring, front queue |
| 4 | CHWBL, c = 1.25 while the ready Instances equal the desired 4, c = 2.0 while fewer are ready |

Round robin runs in each regime as the throughput baseline.

| Regime | S10 parameters |
|---|---|
| tight | 1.5 M KV tokens per Instance, 25,000 prefill tokens/s, full membership |
| drain | tight, with instance-1 drained at 3,000 s and no replacement (3 Instances for the rest of the run) |
| scarce | 750 k KV tokens per Instance, full membership |
| half prefill | tight with 12,500 prefill tokens/s, full membership |

The window is 1,200-4,800 s, as in S10. Measures per run:

- **hit rate**: prompt tokens served from cache, over requests dispatched in the window.
- **throughput against round robin**: output tokens of requests that complete in the window, as a ratio of round robin's in the same regime (means over seeds).
- **hottest Instance**: the Instance with the highest peak in flight in the window; its hit rate and that peak.
- **p95 latency**: from arrival, over requests dispatched in the window.
- **peak in flight**: the largest number in flight on any Instance, requests waiting at the Instance included, when a request was placed (whole run).
- **time above the engine limit**: time in which some Instance had more than 48 in flight (strictly more). "Running" can never exceed 48 in the simulator, because the engine queues the rest, so the in-flight count is the only measure that can show the pile.
- **placements above the bound**: placements where the chosen Instance's load before the request exceeded c x (T+1)/n, with the c in effect. For options 1, 2 and 4 this is zero by the accounting identity S10 describes; for option 3 it can count spills, and is reported separately.
- **after the drain**: hit rate over 0-5 and 5-15 min after 3,000 s, against the same option's run without the drain.

### Pass rule, fixed before the runs

From issue #45, with the readings fixed before any run (recorded in the working notes):

1. **Condition 1.** The option never exceeds the engine limit at full membership: in every seed, the whole-run peak in flight is at most 48 and the time above 48 is zero. Judged in all three full-membership regimes (tight, scarce, half prefill); the tight regime alone is also reported.
2. **Loss.** Among the options that meet condition 1: the highest mean hit rate over the two windows after the drain (0-5 and 5-15 min), and the highest hit rate in the scarce regime. If the two rankings name different options, both are reported and no weighting is invented. Throughput and p95 are secondary.
3. If no option meets condition 1, none passes.

## Environment

- The agents VM: 6 cores, 31 GB, no GPU. Go 1.27.3, `GOMAXPROCS=2`, `nice -n 10`, two simulations at a time.
- The sweep of 108 runs started when the 1-minute load was 2.7 and took 4 min 54 s. The load, sampled every 30 s during it (12 samples), ranged from 2.4 to 4.6; the sweep's own two workers account for about two of that, and other builders on the VM for the rest. The rule is to start heavy steps at 4.0 or less, and the sweep started under it; the load passed 4.0 during the run. An earlier run of the same sweep (before a wording change in the verdict text and the window series) reached a load of 5.5 and 5.2. All runs agree: every numeric column of `cells.csv` common to them is identical, as a deterministic simulator should give. The simulator's clock is simulated, so results do not depend on host load.

## How to run

Go 1.27.x on PATH, then from the repository root:

```sh
(cd third_party/forked/kubeai && go test ./...)   # the fork, unchanged
(cd spikes/s10 && go test ./...)                  # S10's tests plus the ring replica, the cap, the adaptive rule and the front queue; about 1 min on 2 cores
(cd spikes/s10b && go test ./...)                 # the two non-vacuous checks, the regression against S10's results, the pass rule; about 2 min; -short skips the full-size runs
cd spikes/s10b
go build -o s10bsim ./cmd/s10bsim
GOMAXPROCS=2 nice -n 10 ./s10bsim -out results   # 108 runs, about 5 min on 2 cores; rewrites results/
```

`s10bsim` reads S10's committed results from `../s10/results` for the regression check (`-s10-results`).

## Tests

| What | Test |
|---|---|
| The ring replica equals the fork's ring | `TestRingMatchesUpstream` (home of 5,000 keys at 4 and 3 members), `TestRingWalkMatchesUpstreamUnderLoad` (the fork's picks from uneven loads at c = 1.25 and 2.0, counting picks that leave the home) |
| A spill goes to the next Instance on the ring; all full means wait | `TestCapSpillsToTheNextOnTheRing` (the fork's pick on a group without the full Instances is the oracle), `TestCapBelowTheCapIsThePlainPick` |
| The cap binds, and plain c = 2.0 exceeds it on the same keys | `TestCapBindsWherePlainExceedsIt` |
| Spilled reservations count in KubeAI's totals and a drain releases them | `TestCapSpillsToTheNextOnTheRing` (total equals the members' sum at each step), `TestDrainReleasesSpilledReservations` |
| The adaptive rule | `TestAdaptiveBoundSwitchesWithReadiness` |
| Front queue, waits counted in latency | `TestCapSpillsThenQueuesAtTheFront` (hand-worked: 2 spills, 2 held, waits 0.996 and 1.995 s), `TestFrontWaitCountsInLatency` |
| Switch at the first tick after the drain | `TestAdaptiveBoundSwitchesAtTheFirstTickAfterTheDrain` |
| The cap binds in the real tight regime; the adaptive bound switches at the real drain | `TestCapBindsInTheTightRegime`, `TestAdaptiveSwitchesAtTheDrain` |
| S10's behaviour is unchanged | `TestMatchesS10CommittedResults` (round robin, c = 1.25 and c = 2.0, four regimes, seed 1, against S10's committed rows), `TestRegressionCheckCatchesADifference` (the check can fail) |
| The 300 s windows behind the drain table sum to each run's totals | `TestWindowSeriesSumsToTheRun` |
| The pass rule | `TestPassRule` (an option above the limit is excluded however good its hit rate; split rankings name no winner; none qualifying means none passes) |

## What this means for the HBEPs

- **HBEP-5, open question 3 (default bound).** S10's c = 2.0 default does not keep any Instance to the engine limit at full membership: its hottest Instance reaches 69-76 in flight in every regime. No bound relative to the mean does either once the cluster is saturated (c = 1.25 reaches 49-50).
- **HBEP-5, open question 6 (per-Instance caps).** A cap is the only mechanism here that holds the limit, and it binds in every full-membership regime. As specified in this spike (spill to the next Instance on the ring, queue at the front when all are full) it costs 7 points of hit rate at full membership in the tight regime and destroys affinity after an unreplaced drain. It should not be adopted as the default as it stands.
- **Untested, and the obvious next step:** cap only while every desired Instance is ready (option 4's readiness signal switching the cap rather than c), or a cap above 48 with a bounded wait before spilling. Neither was run.
- **Adaptive c** is a fair operator option where replacements are reliable. It does not hold the limit in saturated regimes, and after a drain it recovers less than c = 2.0 does.

## Still unknown

- Why the cap collapses after the drain is a hypothesis (above). The re-homing rate and the spill count per window were not measured.
- The cap is on requests in flight, queued ones included. A real engine's running count is lower, and 48 is S10's assumption.
- The control step (10 s) is an assumption. A shorter step would shorten the switch delay (5 s here); it cannot change the order of the options.
- Only the unreplaced drain was run for option 4. With a replacement joining, readiness recovers and the bound returns to 1.25; that was not simulated.
- Only two keys were run, and only c = 2.0 as the cap's base bound.

## Deviations

- Options 3 and 4 live in S10's module (`spikes/s10/front`, `spikes/s10/sim`), not in a new package under `spikes/s10b`, because the simulator's router and engine are package-internal and cannot call an outside package. The new `Policy` fields are zero by default and the forked balancer is unchanged. S10b's driver, tests and results are under `spikes/s10b`.
- The cap's ring walk uses a replica of the fork's ring, checked against the fork by test, because the fork does not export its ring and may not be changed. The spill target is reserved through the fork's own picker.
- Condition 1 has two readings (all three full-membership regimes, or the tight regime alone). The first was fixed before the runs as the primary; both are reported.
- The sweep's load went above 4.0 while it ran (see Environment).

## Provenance

| Component | Origin | Licence |
|---|---|---|
| CHWBL balancer, the fork | KubeAI, commit `89dfff46`, `third_party/forked/kubeai` (see its `ORIGIN.md`) | Apache-2.0 |
| xxhash v1.1.0 (the ring replica hashes with it, as the fork does) | https://github.com/cespare/xxhash | MIT |
| Simulator, front wrapper, key choices | S10 (`spikes/s10`) | this repository |
| Ring replica, capped pick with spill, front queue, adaptive rule, the study | new in S10b | this repository |

