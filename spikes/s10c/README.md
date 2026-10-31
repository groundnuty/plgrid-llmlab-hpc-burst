# Spike S10c: the adaptive bound with a cap at full membership (option 5)

A follow-up to S10b (issue #45, PR #50), asked for by `@hpc-burst-science-agent`, HBEP-5's author, as the last candidate ([PR #50 comment](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/pull/50#issuecomment-6523083922)). It builds on S10's simulator and front wrapper (`spikes/s10`) and S10b's harness (`spikes/s10b`). The forked KubeAI balancer under `third_party/forked/kubeai` is unchanged.

## Question

S10b found that only option 3 (c = 2.0 plus a hard cap of 48 in flight per Instance) never exceeds the engine limit at full membership in the tight, scarce and half-prefill regimes, and that it collapses after an unreplaced drain (hit rate 22.3% / 17.5% against round robin's 41.5% / 42.3%). Option 5 asks whether the cap can be kept where it holds the limit and dropped where it hurts:

- while the ready Instances equal the desired ones: c = 1.25, plus a hard cap of 48 per Instance, spilling to the next Instance on the ring and queueing at the front when all are full;
- while fewer are ready: c = 2.0 and no cap.

Same rule as S10b, fixed before the runs: (1) never above 48 in flight at full membership in the tight, scarce and half-prefill regimes; (2) among the options that meet it, the least loss after a drain and in the scarce regime; if the two rankings name different options, both are reported and no weighting is invented. Same key (session header, falling back to the hash of the opening), same four regimes, seeds 1-3.

## Answer

**Options 3 and 5 pass condition 1; no other option does. Between them the rule as fixed names no single winner: option 5 is best after the drain (62.5% / 57.8% against 22.3% / 17.5%), option 3 is best in the scarce regime (30.9% against 29.9%).** Option 5 meets condition 1 at almost no cost to hit rate, and it does not collapse after the drain, but it does not recover c = 2.0's hit rate there (77.1% / 76.6%).

**Decision (`@hpc-burst-science-agent`, HBEP-5's author, 30 Oct 2026, [PR #53 comment](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/pull/53#issuecomment-6523638676); approved with HBEP-5 in #63): HBEP-5's default load bound is option 5.** The rule's two rankings stay as reported above; the decision weighs them. Options 3 and 5 both meet condition 1. Option 5 loses far less after a drain (62.5% hit against 22.3%), and option 3's 1.0-point lead in the scarce regime is within the seed spread (the seeds range over 21-38%). The control that keeps the cap after the switch collapses (37.5% to 21.9%), which shows why the cap must drop at the switch.

- **Condition 1.** Option 5 peaks at 46 / 48 / 48 in flight in the tight / scarce / half-prefill regimes (max over seeds) and spends 0 s above 48 in every run. Options 1 and 4 miss it by 1-2 requests in the last two regimes (peaks 49 and 50, 6 s and 228 s above 48 summed over seeds), and option 2 is at 69-76.
- **Check 1, the cap binds: met, in the runs that need it.** The follow-up asks for a check so that a cap that never acts cannot pass condition 1 by itself. The objective review (`@hpc-burst-science-agent`, 30 Oct 2026, in the PR #53 comment above) records that the result meets the check's intent. In the tight regime plain c = 1.25 peaks at 43, 46 and 46 in flight (S10b's committed rows), below the cap of 48, so option 5 spills 0 requests there on all three seeds. A request spills only when its choice already holds 48, which needs a peak of 49, and there it is the same run as option 1 (and option 4): identical on every column of `cells.csv`, on all three seeds. The cap binds exactly where condition 1 needs it, in the runs where c = 1.25 alone reaches the limit: scarce seeds 2 and 3 (peaks of 49, 2 and 7 spills) and half-prefill seeds 2 and 3 (peaks of 50, 25 and 131 spills). There option 5 holds the peak at 48, and it is silent on the seeds where c = 1.25 peaks at 48 or less. The committed tests assert this (`TestCapDoesNotBindInTheTightRegime`, `TestOption5EqualsTheOptionItIsBuiltOnWhereTheCapIsSilent`, `TestCapBindsWhereC125ReachesTheLimit`).
- **Cost of the cap at full membership.** None in the tight regime (identical to option 1). Against option 4 (the same c = 1.25 without a cap) the scarce regime loses 0.2 points of hit rate (29.9% against 30.1%) and the half-prefill regime gains 0.1 (70.4% against 70.3%) with p95 108 s against 110 s. The cap moves 9 and 156 placements in total over three seeds, out of 62,534 and 76,946, and nothing is ever held at the front.
- **After the drain** option 5 is option 4: the bound and the cap switch together at 3,005.0 s on all three seeds (drain at 3,000 s, control step 10 s), and no request was spilled or held before or after it. Its hit rate over 0-5 / 5-15 / 15-30 min is 62.5% / 57.8% / 57.8%, against 77.1% / 76.6% / 76.0% for c = 2.0 and 41.5% / 42.3% / 41.7% for round robin. It loses 20.2 and 23.6 points against its own run without a drain, where c = 2.0 loses 4.4 and 4.9.
- **The cap is what collapses the drain.** The control is option 5 with the cap kept after the switch (c = 2.0 plus cap 48 from 3,005 s; identical to option 5 before it). Its hit rate is 37.5% / 21.9% / 21.5%, with 2,764 / 2,909 / 2,809 spills and 3,597 / 3,358 / 3,579 requests held at the front, and a peak of at most 48 after the drain. Option 5 is higher than the control on each seed. So switching the cap off with the bound is worth 25 to 36 points after the drain. The cost is that below full membership nothing holds the limit: the peak after the drain is 96, 96 and 84.
- **Per-seed spread after the drain is large:** option 5's 0-5 min hit rate is 62.6% / 80.5% / 44.2% on seeds 1 / 2 / 3, and the 5-15 min rate 46.8% / 79.6% / 47.1%. The means hide that one seed keeps its affinity and two do not. Option 4 has the same values: the drain runs equal option 4's on every column of `cells.csv`, on all three seeds.
- **Regression.** Options 3 and 4 run again on seed 1 in all four regimes: 8 cells, each equal byte for byte to the line in S10b's committed `cells.csv` (`results/regression-vs-s10b.txt`).

## Results

Key `header>opening`, means over seeds 1-3 unless a column says max or sum. Round robin and options 1-4 are S10b's committed rows (`../s10b/results/summary.csv`), not run again; option 5 is this study's run. The tables are `results/combined-tables.md`, rendered by `s10csim`.

**Tight (1.5 M KV per Instance), full membership**

| option | hit rate [min, max] | throughput vs RR | p95 latency (s) | peak in flight (max over seeds) | time above 48 (s, sum over seeds) | hottest Instance: peak in flight / hit rate | placements above the bound (sum) | spills (sum) |
|---|---|---|---|---|---|---|---|---|
| round robin | 43.4% [42.9%, 44.0%] | 1.00x | 166.1 | 131 | 15305 | 129.3 / 17.0% | - | - |
| 1. c = 1.25 | 84.0% [79.1%, 90.7%] | 2.83x | 37.3 | 46 | 0 | 45.0 / 77.3% | 0 | 0 |
| 2. c = 2.0 | 82.1% [81.2%, 83.3%] | 2.13x | 78.5 | 75 | 12345 | 73.3 / 18.4% | 0 | 0 |
| 3. c = 2.0 + cap 48 | 75.2% [71.5%, 79.5%] | 2.10x | 61.2 | 48 | 0 | 48.0 / 36.7% | 0 | 9856 |
| 4. adaptive 1.25 / 2.0 | 84.0% [79.1%, 90.7%] | 2.83x | 37.3 | 46 | 0 | 45.0 / 77.3% | 0 | 0 |
| 5. adaptive 1.25 / 2.0, cap 48 at full membership | 84.0% [79.1%, 90.7%] | 2.83x | 37.3 | 46 | 0 | 45.0 / 77.3% | 0 | 0 |

**Scarce (750 k KV per Instance)**

| option | hit rate [min, max] | throughput vs RR | p95 latency (s) | peak in flight (max over seeds) | time above 48 (s, sum over seeds) | hottest Instance: peak in flight / hit rate | placements above the bound (sum) | spills (sum) |
|---|---|---|---|---|---|---|---|---|
| round robin | 27.9% [24.7%, 31.9%] | 1.00x | 125.4 | 122 | 15008 | 110.0 / 17.1% | - | - |
| 1. c = 1.25 | 30.1% [21.0%, 37.2%] | 1.07x | 71.0 | 49 | 6 | 48.7 / 16.7% | 0 | 0 |
| 2. c = 2.0 | 31.7% [22.7%, 37.9%] | 1.02x | 80.2 | 69 | 9734 | 64.3 / 17.0% | 0 | 0 |
| 3. c = 2.0 + cap 48 | 30.9% [21.7%, 38.0%] | 1.04x | 72.7 | 48 | 0 | 48.0 / 16.8% | 0 | 1865 |
| 4. adaptive 1.25 / 2.0 | 30.1% [21.0%, 37.2%] | 1.07x | 71.0 | 49 | 6 | 48.7 / 16.7% | 0 | 0 |
| 5. adaptive 1.25 / 2.0, cap 48 at full membership | 29.9% [21.0%, 37.3%] | 1.07x | 71.0 | 48 | 0 | 48.0 / 16.5% | 0 | 9 |

**Half prefill (12,500 tokens/s)**

| option | hit rate [min, max] | throughput vs RR | p95 latency (s) | peak in flight (max over seeds) | time above 48 (s, sum over seeds) | hottest Instance: peak in flight / hit rate | placements above the bound (sum) | spills (sum) |
|---|---|---|---|---|---|---|---|---|
| round robin | 45.3% [45.1%, 45.4%] | 1.00x | 375.9 | 146 | 18835 | 144.7 / 17.0% | - | - |
| 1. c = 1.25 | 70.3% [61.2%, 76.1%] | 2.58x | 110.0 | 50 | 228 | 49.3 / 48.4% | 0 | 0 |
| 2. c = 2.0 | 86.6% [85.4%, 87.4%] | 3.72x | 142.4 | 76 | 13247 | 74.0 / 18.4% | 0 | 0 |
| 3. c = 2.0 + cap 48 | 74.9% [73.8%, 75.5%] | 2.83x | 117.2 | 48 | 0 | 48.0 / 38.4% | 0 | 13265 |
| 4. adaptive 1.25 / 2.0 | 70.3% [61.2%, 76.1%] | 2.58x | 110.0 | 50 | 228 | 49.3 / 48.4% | 0 | 0 |
| 5. adaptive 1.25 / 2.0, cap 48 at full membership | 70.4% [61.6%, 76.1%] | 2.58x | 107.8 | 48 | 0 | 48.0 / 48.2% | 0 | 156 |

**After an unreplaced drain (instance-1 drained at 3,000 s)**

| option | hit 0-5 min after (no drain) | hit 5-15 min after (no drain) | hit 15-30 min after (no drain) | throughput vs RR from 3,000 s | p95 latency from 3,000 s (s) | peak in flight after the drain | time above 48 after the drain (s, sum over seeds) | held at the front (sum) |
|---|---|---|---|---|---|---|---|---|
| round robin | 41.5% (43.2%) | 42.3% (43.3%) | 41.7% (43.0%) | 1.00x | 183.8 | 140 | 5400 | - |
| 1. c = 1.25 | 52.9% (82.7%) | 51.6% (81.4%) | 50.5% (84.8%) | 1.62x | 79.3 | 65 | 5295 | 0 |
| 2. c = 2.0 | 77.1% (81.4%) | 76.6% (81.5%) | 76.0% (80.7%) | 2.12x | 110.7 | 103 | 5400 | 0 |
| 3. c = 2.0 + cap 48 | 22.3% (70.1%) | 17.5% (66.5%) | 21.1% (67.8%) | 1.06x | 83.1 | 48 | 0 | 11041 |
| 4. adaptive 1.25 / 2.0 | 62.5% (82.7%) | 57.8% (81.4%) | 57.8% (84.8%) | 1.64x | 92.8 | 96 | 5334 | 0 |
| 5. adaptive 1.25 / 2.0, cap 48 at full membership | 62.5% (82.7%) | 57.8% (81.4%) | 57.8% (84.8%) | 1.64x | 92.8 | 96 | 5334 | 0 |

"Peak in flight" counts requests waiting at the Instance. Option 5's cap is the reason its peak is 48 in the scarce and half-prefill regimes, where options 1 and 4 reach 49 and 50. "Placements above the bound" is zero for every option by the accounting identity S10 describes.

Option 5's own measures, per run (`results/extras.csv`): the switch time of the bound and the cap, and the spills and holds before and after it.

| regime | seed | switch time (s) | bound before / after | cap before / after | spills before / from switch | held before / from switch |
|---|---|---|---|---|---|---|
| tight | 1 | - | - | - | 0 / 0 | 0 / 0 |
| tight | 2 | - | - | - | 0 / 0 | 0 / 0 |
| tight | 3 | - | - | - | 0 / 0 | 0 / 0 |
| drain | 1 | 3005.0 | 125 / 200 | 48 / 0 | 0 / 0 | 0 / 0 |
| drain | 2 | 3005.0 | 125 / 200 | 48 / 0 | 0 / 0 | 0 / 0 |
| drain | 3 | 3005.0 | 125 / 200 | 48 / 0 | 0 / 0 | 0 / 0 |
| scarce | 1 | - | - | - | 0 / 0 | 0 / 0 |
| scarce | 2 | - | - | - | 2 / 0 | 0 / 0 |
| scarce | 3 | - | - | - | 7 / 0 | 0 / 0 |
| half-prefill | 1 | - | - | - | 0 / 0 | 0 / 0 |
| half-prefill | 2 | - | - | - | 25 / 0 | 0 / 0 |
| half-prefill | 3 | - | - | - | 131 / 0 | 0 / 0 |

With no switch (full-membership regimes) all spills are in the "before" column; the bound stays 125 (c = 1.25) and the cap 48 throughout.

### The pass rule

Key header>opening, 3 seeds.

| option | peak in flight: tight / scarce / half prefill (max over seeds) | time above 48, summed over seeds (s): tight / scarce / half prefill | condition 1, tight | condition 1, all three | hit 0-5 min after drain (drop vs no drain) | hit 5-15 min after drain (drop) | scarce hit (vs RR) |
|---|---|---|---|---|---|---|---|
| c=1.25 | 46 / 49 / 50 | 0 / 6 / 228 | true | false | 52.9% (29.8 pts) | 51.6% (29.8 pts) | 30.1% (+2.2 pts) |
| c=2.0 | 75 / 69 / 76 | 12344 / 9734 / 13247 | false | false | 77.1% (4.4 pts) | 76.6% (4.9 pts) | 31.7% (+3.8 pts) |
| c=2.0+cap48 | 48 / 48 / 48 | 0 / 0 / 0 | true | true | 22.3% (47.8 pts) | 17.5% (48.9 pts) | 30.9% (+3.0 pts) |
| adaptive 1.25/2.0 | 46 / 49 / 50 | 0 / 6 / 228 | true | false | 62.5% (20.2 pts) | 57.8% (23.6 pts) | 30.1% (+2.2 pts) |
| adaptive+cap48 (cap at full only) | 46 / 48 / 48 | 0 / 0 / 0 | true | true | 62.5% (20.2 pts) | 57.8% (23.6 pts) | 29.9% (+2.0 pts) |

Qualifying on condition 1 in all three full-membership regimes: [c=2.0+cap48 adaptive+cap48 (cap at full only)]. Best after the drain: adaptive+cap48 (cap at full only). Best in the scarce regime: c=2.0+cap48. The rankings split.
Qualifying on condition 1 in the tight regime only: [c=1.25 c=2.0+cap48 adaptive 1.25/2.0 adaptive+cap48 (cap at full only)]. Best after the drain: adaptive 1.25/2.0. Best in the scarce regime: c=2.0+cap48. The rankings split.

The verdict table is computed from the CSV files, where seconds have one decimal, so c = 2.0's tight-regime time above 48 reads 12344 here and 12345 in S10b's README (12,344.6 s unrounded); no decision depends on it. Under the second reading (condition 1 in the tight regime only) the best option after the drain is a tie: options 4 and 5 are the same run after the drain, and the rule's code names the first of them. The scarce ranking among the passing options is small but one-sided: option 3 is ahead of option 5 on each seed (21.7 / 38.0 / 33.0% against 21.0 / 37.3 / 31.5%, a lead of 0.7 / 0.7 / 1.5 points), while the seeds themselves range over 21-38%. The rule gives no weighting between the 40-point lead of option 5 after the drain and the 1.0-point lead of option 3 in the scarce regime, and none is invented here.

## The three non-vacuous checks (committed tests, `spikes/s10c/study/study_test.go`)

| Check | Result | Test |
|---|---|---|
| The cap binds where plain c = 1.25 alone would break the limit (check 1 of the follow-up) | PASS, meeting the check's intent as the objective review decided: in the tight regime plain c = 1.25 peaks at 43 / 46 / 46, below the cap, so there are 0 spills and option 5 equals option 1; where c = 1.25 reaches 49 or 50 (scarce and half prefill, seeds 2 and 3) the cap spills 9 and 156 requests in total and holds the peak at 48 | `TestCapDoesNotBindInTheTightRegime`, `TestCapBindsWhereC125ReachesTheLimit` |
| After the drain option 5 switches to the uncapped c = 2.0, within one control step | PASS: 3,005.0 s on seeds 1-3, inside (3000, 3010]; bound 1.25 to 2.0 and cap 48 to none as one switch; every placement before it at 1.25, every one after it at 2.0; peak in flight after the drain 96 / 96 / 84, which a cap of 48 would have prevented; no switch in the full-membership regimes | `TestSwitchesToUncappedC2AtTheDrain` |
| The cap is not active below full membership: no spill counted after the switch | PASS: 0 spills and 0 held requests after the switch on every seed. The control (same run, cap kept) spills 2,764 / 2,909 / 2,809 times and holds 3,597 / 3,358 / 3,579 requests after the same switch time, so zero is not the trivial answer | `TestNoSpillAfterTheSwitch` |

Why the third check needs the control. Option 5 has no spill before the switch either (the cap is inert in the tight regime), so "no spill after" alone would also pass for a cap that never worked. The control run applies the same policy with the cap kept after the drain; it must spill, and it does. The hand-worked version of checks 2 and 3 is in `spikes/s10/sim` (`TestCapFullOnlySwitchesOffWithTheBound`: three Instances, a cap of 2, a drain at 1.2 s, a switch at 1.5 s; 1 spill before the switch and 0 after, against 1 and 1 spill and 2 held requests for the control; `TestCapFullOnlyReleasesHeldRequestsWhenTheCapGoesOff`: a request held at the front is placed at the switch, after 0.498 s, not when a request ends at 10 s).

## Method

### What was added (the forked balancer is unchanged)

- `spikes/s10/front/cap.go`: `AdaptiveCapBound{Tight, Loose, Cap}` with `Percent(ready, desired)` (option 4's rule) and `CapAt(ready, desired)` (the cap while ready >= desired, none below). Option 5's pick is the existing `PickPrefixHashCapped` with the bound and cap in force, or the plain `PickPrefixHash` when no cap is in force; neither is changed.
- `spikes/s10/sim`: `Policy.CapFullOnly`; the router holds the cap in force (`capNow`, the policy's cap unless `CapFullOnly`), set by the same control step that sets the bound, and `route` and the engine's placement check read it instead of the policy's constant. When the cap goes off, the engine places the requests held at the front at once. A `Switch` records the cap before and after. Metrics keep the times of spills and of holds. For options 1-4 the cap in force is the policy's cap throughout, which is why their runs are unchanged (the byte-for-byte check).
- `spikes/s10c`: `study` (option 5, the cap-kept control, a reader for S10b's `cells.csv`, the five-option evaluation, the tables, the comparison with S10b's rows) and `cmd/s10csim`. S10b's `study` package is not edited; `Evaluate` and `Missing` there are fixed to S10b's four options, so `study.Evaluate` here repeats them for five, and `TestEvaluateAgreesWithS10b` checks that for options 1-4 the two agree exactly on S10b's committed cells.

Option 5 and the control are built with S10b's `Run` and `NewCell`, so they are measured by S10b's code. Options 1-4 and round robin are read from the committed `cells.csv`; every cell, S10b's and option 5's, goes through the same write-and-parse path before the pass rule is applied (`TestReaderRoundTripsS10bCells` shows the path is lossless for S10b's file). The summary rows for options 1-4 are S10b's committed ones verbatim, and `TestTablesReproduceS10bREADME` shows that each of the 20 rows of S10b's README for round robin and options 1-4 appears in the combined tables unchanged.

### Measures and regimes

Unchanged from S10b: tight (1.5 M KV tokens per Instance, 25,000 prefill tokens/s), drain (tight, instance-1 drained at 3,000 s with no replacement), scarce (750 k KV), half prefill (12,500 tokens/s), window 1,200-4,800 s, engine limit 48, control step 10 s with ticks at 5, 15, ... s. Peak in flight, time above 48, hit rates after the drain and throughput against round robin are defined as in S10b's README.

## Environment

The agents VM: 6 cores, 31 GB, no GPU. Go 1.27.3, `GOMAXPROCS=2`, `nice -n 10`, two simulations at a time. The simulated clock makes the results independent of host load; repeated runs of the sweep gave identical files.

- The sweep behind `results/` (12 runs of option 5, 8 re-runs of options 3 and 4, 3 control runs) took about 1.5 min and started at a 1-minute load of 3.10; a reading taken about 90 s after it started showed 7.5, from other sessions and the sweep's own two workers. The first sweep, without the control, started at 3.97. Every file the sweep wrote was identical across the runs, as a simulated clock gives.
- Review fixes (after the first review): the regenerated sweep started at a 1-minute load of 0.96 and ended at 2.49; the three suites then started at loads of 2.47 (`spikes/s10`), 3.32 (`spikes/s10b`) and 3.84 (`spikes/s10c`), within the 4.0 rule. Every file the sweep wrote except `regression-vs-s10b.txt` (its blank last line) was byte-identical to the committed one.
- Test suites: `spikes/s10` started at load 2.55, `spikes/s10b` at 4.07 (just above the 4.0 rule; others' builders were running), `spikes/s10c` at 3.85. The root disk had 4.2 GB free throughout.

## How to run

Go 1.27.x on PATH, then from the repository root:

```sh
(cd spikes/s10 && go test ./...)    # S10's tests, plus the S10c rule and the hand-worked option 5 tests; about 1 min on 2 cores
(cd spikes/s10b && go test ./...)   # S10b's tests against the changed simulator; about 2 min
(cd spikes/s10c && go test ./...)   # the three checks, the byte-for-byte re-run of options 3 and 4, the reader and the rule; about 1.5 min; -short skips the full-size runs
cd spikes/s10c
go build -o s10csim ./cmd/s10csim
GOMAXPROCS=2 nice -n 10 ./s10csim -out results   # 23 runs, about 1.5 min; rewrites results/
```

`-seeds` takes each seed once (a repeated seed exits 2 before anything runs). The verdict needs exactly seeds 1, 2 and 3 for every option and regime: for any other set, `s10csim` still runs and writes the files, but `verdict.md` says INCOMPLETE (a missing run) or INVALID (a run twice, or a seed the verdict does not ask for), lists what is wrong and gives no verdict, and `combined-tables.md` holds the reason in place of tables.

`s10csim` reads `../s10b/results` and never writes there. It writes `cells.csv`, `summary.csv`, `extras.csv` (option 5), `rerun-options-3-4.csv`, `control-cap-kept.csv`, `regression-vs-s10b.txt`, `verdict.md` and `combined-tables.md`, then exits 1 if the re-run differs from S10b's rows.

## What this means for the HBEPs

- **HBEP-5, open questions 3 and 6 (default bound, per-Instance cap).** A cap that follows readiness is the only way found in S10b and S10c to hold the engine limit at full membership without paying c = 2.0's imbalance. Its measured price at full membership is under one point of hit rate (0.2 points lower in the scarce regime, 0.1 higher at half prefill, none in the tight regime), because the cap fires on 0.01-0.2% of placements.
- **The drain is still the weak case.** The 20 to 24 points that option 5 loses after an unreplaced drain are option 4's, and c = 2.0 alone loses 4 to 5. Adaptive switching does not buy c = 2.0's retention back, and below full membership the limit is not held (peak 84 to 96). The cap-kept control shows the cap is the larger problem there: keeping it after the switch costs another 25 to 36 points.
- **The choice between options 3 and 5.** The rule's split stands as measured: option 5 is ahead by 40 points of hit rate after an unreplaced drain and behind by 0.7-1.5 points per seed in the scarce regime. `@hpc-burst-science-agent`, HBEP-5's author, decided that HBEP-5's default load bound is option 5 (see Answer).

## Still unknown

- The control step (10 s) and the switch delay (5 s) are assumptions; a slower deployment controller lengthens the window in which a drained cluster still runs the tight bound and the cap. In these runs nothing spilled or was held in that window.
- Only the unreplaced drain was swept. The return after a replacement joins is tested on a small load, not swept: after a drain and then the join of a spare Instance, the bound goes back to c = 1.25 and the cap to 48 at the first control tick after readiness equals the desired count again (`TestOption5ReturnsToTheTightBoundAndTheCapAfterAReplacement`, option 5's own constants and 10 s step: drain at 12 s, switch to c = 2.0 at 15 s, join at 32 s, switch back at 35 s). The surviving Instance held 60 in flight, above 48, when the cap returned. It was not touched: nothing was cut, it was given no request until it fell below 48, the other Instances stopped at 48, and in that run 41 of the 100 later requests spilled to another Instance and 4 were held at the front until the survivor had room. The same sequence with a cap of 2 and exact, hand-worked placements is `TestCapFullOnlyComesBackWhenAReplacementJoins` in `spikes/s10/sim`. What a replacement does to the hit rate was not measured.
- The cap is on requests in flight, queued ones included; a real engine's running count is lower, and 48 is S10's assumption.
- Why one seed keeps its affinity after the drain (79.6% in minutes 5-15) and two do not (46.8%, 47.1%) was not investigated.
- Only the key `header>opening` was run, as asked. S10b's plain `opening` key was not repeated.

## Deviations

- The follow-up words check 1 as "at full membership in the tight regime, option 5's cap binds: spills are above 0". In the tight regime plain c = 1.25 never reaches the cap, so the spills there are 0. The objective review decided that the check's intent is met where c = 1.25 alone reaches the limit (see Answer); the tests assert both the 0 spills in the tight regime and the spills where the limit is reached.
- `AdaptiveCapBound`, `Policy.CapFullOnly` and the router and metrics changes are in S10's module (`spikes/s10/front`, `spikes/s10/sim`), as options 3 and 4 were in S10b, because the simulator's router and engine are package-internal. The new `Policy` field is false by default.
- The sweep for the first results started at a load of 3.97 and the `spikes/s10b` test run at 4.07.

## Provenance

| Component | Origin | Licence |
|---|---|---|
| CHWBL balancer, the fork | KubeAI, commit `89dfff46`, `third_party/forked/kubeai` (see its `ORIGIN.md`) | Apache-2.0 |
| xxhash v1.1.0 (the ring replica hashes with it, as the fork does) | https://github.com/cespare/xxhash | MIT |
| Simulator, front wrapper, ring replica, capped pick, front queue, adaptive rule, the study harness | S10 and S10b (`spikes/s10`, `spikes/s10b`) | this repository |
| `AdaptiveCapBound`, the readiness-driven cap in the router, spill and hold timestamps, the S10c study, tests and tables | new in S10c | this repository |
