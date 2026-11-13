# Spike S18: retries before the first byte, with fault injection (HBEP-5, open question 5)

Issue #528. HBEP-5's open question 5 asks whether the front should send a request again after it was written to an
Instance but before any byte of the reply reached the client: "retrying before the first byte reaches the client, to
another Instance, within a per-deployment budget, never after two Instances have died under one request. A spike with
fault injection decides." The guard matters because of HBEP-7's poison-request risk (`hbep/0007-lifecycle-controller/README.md:102`):
a request that kills every engine it reaches would, retried freely, take down one Instance after another.

The sweep ran on GitHub-hosted runners, in one run (43492532339, on the throwaway PR #531): the controls and all 140 cells,
every cell valid (its books balanced both ways, and every fault fired only where it was injected). A first run was
discarded (Deviations). **One retry, to another Instance, is worth having; no front-side rule bounds what a poison
request costs once the client retries, and the crash-correlation event should be the controller's.**

## Findings

- **One retry to another Instance rescues a request that failed before its first byte, at one duplicate execution.**
  Arm (b) answered every fired F2 and F3 request whole, at 1% and at 10% of requests, streamed or not, and every fired
  F1 request at 1%. At 10% it rescued 0.86 of F1's (0.78 streamed): the other Instance was sometimes dead or out
  itself. Arm (a), alpha, rescues none, by design. Each rescue begins the request on two engines, and the first
  engine's work is lost: a dead engine's partial work (F1), nothing (F2, refused before generating), a cancelled
  prefill (F3).
- **A budget below the fault rate starves rescues, and is no poison guard.** With faults at 0.5 a second (10% of 5
  requests a second), a refill of 0.1 a second rescued 0.15-0.37 of the fired F2 and F3 requests; a refill of 1 a
  second with a burst of 3 or 10 rescued all of them (one streamed cell 0.99), and with a burst of 1, 0.62-0.74. Under the poison request the starved bucket (burst 1, refill
  0.1 a second) cut the engines killed per poison request from 2 to 1.7, only because it was empty 30% of the time.
- **The two-deaths stop gives up every request whose tunnel fails twice, and saves nothing one retry does not.** A
  request cut on two connections in a row (F3x2, no engine dead) was given up by (d) every time: 0 of 50-56 rescued
  at 10%, every one a false stop. (b) gave them up too, while (c) with a burst of 10 rescued all. Under the poison
  request (b) and (d) both stopped at two dead engines. To the front a cut and a death look the same (Q2), so a rule
  that rescues the double cut lets a poison request reach a third engine: (c) with a burst of 10 killed all three.
- **With the clients people use, the front cannot bound what a poison request costs.** The OpenAI SDK retries a 5xx
  twice by default (Q6), and each retry is a new request to the front. With it, every arm lost all three Instances to
  each poison request: 3 deaths in 20 of 20 trials, against 1 under (a) and 2 under (b) without it. Under (a) that is
  partly the stand's: a dead double is a refused dial, which alpha resends at once to another Instance, where a real
  Instance's dead worker behind its live tunnel answers a 5xx that alpha passes through (Fidelity). The bound stands:
  the front's limit per request multiplies by the client's attempts, so with six or more Instances, (b) with the SDK
  can reach six (derived; the stand had three).
- **The crash-correlation event (HBEP-7) can be the controller's: worker deaths on two distinct Instances of one
  deployment within 30 s.** The controller sees an exit, so a cut tunnel never looks like a death to it. A poison
  request's deaths came at most 1.59 s apart (348 poison requests killed two or more engines), well inside 30 s, and the
  window joined them in every one. The front-side candidate, two closed attempts under one request, missed (a) with the
  SDK (0 of 20) and fired on every double cut: 574 times in the F3x2 cells, no engine dead.
- **For a stream, the window closes at its headers (Q1).** Every stream ran the double's early-headers mode, and every
  fault here fires before the headers by construction, so the streamed results match the non-streamed ones: the
  mechanism works before the headers. On vLLM a death in prefill comes after them and is not retried, so the streamed
  results are an upper bound.
- **The added time to first byte is small, and the stand's:** 0.2-1.9 ms for F1 and F2, 5 ms for F3 (part of it the
  cutter waiting for the engine's record), 11 ms for F3x2's two failed attempts. A rescue's real cost is the failed
  attempt's time to fail plus a second whole prefill, which the double cannot show.

## Question

Should the front retry a request whose upstream failed after the request was written but before any byte went to the
client? If so, under which budget, and what stops a poison request?

Two readings, both ruled by the science agent on #528 before any code:

- **For a stream, "the first byte" is the headers (Q1).** The front relays and flushes a streamed reply's status line at
  once (`internal/front/relay.go:108-112`), and vLLM sends it before prefill. So on vLLM a stream's retry window closes
  milliseconds after the request is written, and a death in prefill is outside it: only non-streamed requests, and
  streams that fail before their headers, can be retried. The engine double sends a stream's headers with its first
  token, which would overstate the window, so every stream here runs the double's `early-headers` mode (#529), which
  sends them at admission, as vLLM does. Streamed and non-streamed requests are measured and reported apart.
- **To the front, "an Instance died" can only mean "written, then closed before any header" (Q2).** A tunnel that
  closes with the engine alive (F3) looks the same, so arm (d)'s stop is also measured where nothing died: its false
  stops are its cost.

HBEP-7's crash-correlation event (`:102`) has no definition yet (Q3), so two candidates are scored from the records:
(i) front-side, two or more closed attempts under one front request; (ii) controller-side, two or more worker deaths of
one deployment within a window (30 s, 2 min and 10 min are scored).

## Method

### The stand

Everything runs on loopback, in one test binary (`run/`), since the engine double (`internal/agent/enginedouble`) is
imported only by tests.

- **The front is the real `internal/front.Proxy` and `Router`,** built as the front's own tests build them
  (`internal/front/harness_test.go`): a hand-built inventory snapshot of one served ModelDeployment with three
  Instances, fed to the Router as the watch does, and the Proxy served on a kernel-chosen port. Its configuration is
  the default (`pauseAfterFailures` 3, `pauseDuration` 30 s, and the rest).
- **Each Instance is an engine double,** a process of its own (`rig/doubles.go`), started from the test binary with
  the modes of its run, and reached through the spike's own `Transports` (`rig/transports.go`), the front's seam to
  HBEP-6's tunnel. Every signal to a double goes through a pidfd opened while the child was unreaped.
- **A controller emulation** (`rig/rig.go`) answers a double that dies on its own: after D1 (1 s) it sets the
  Instance's phase out of ready and hands the snapshot to the Router, as the controller's signal does; after D2 (5 s)
  it restarts the double, waits for its `/health`, and sets it ready again. vLLM's restart and verification take
  minutes; the spike's are seconds, so anything that depends on how long an Instance is out (the share of background
  requests refused while all Instances are down, for example) is the stand's, not vLLM's.
- **The client** (`rig/client.go`) puts each request's ID, `s18id-<run>-<n>/c<attempt>`, in its prompt. The doubles'
  records hold it (the `Tail` of `admitted` and `request`, #529), so the client's records and the engines' are
  reconciled both ways. Its SDK mode retries as the OpenAI SDK does by default (Q6): 2 retries, after 0.5 s and 1 s,
  on a connection error or a 408, 409, 429 or 5xx, each retry a new front request with its own attempt number.

### The arms

- **(a), alpha's rule, is the front as it is,** through the spike's `Transports` straight to each double: every
  decision is `forward`'s and `failedBeforeHeaders`'s.
- **(b)-(d) are a retry layer at each Instance's tunnel position** (`rig/layer.go`), the one seam the front offers
  (its `forward` loop is internal, and the brief keeps retry arms out of `internal/`). The front dials Instance i's
  layer; the layer sends the request to Instance i, and on a failure before any response header (the connection closed
  or reset after the write, or a 5xx) sends it to another ready Instance under its policy, and returns headers to the
  front only once it holds a good reply. It reports each failed upstream to the front's own `Router.ReportConnect`, as
  the front's failure path does, so the front's pauses act as in the product: the other Instances' outcomes (a closed
  connection, a 5xx or a refused dial count; a success resets), while home's the front counts itself from what the
  layer returns. A refused dial wrote nothing, so the layer resends it once with no budget, as alpha's own rule does.
  - **(b):** one retry, to another Instance.
  - **(c):** retries within a per-deployment token bucket (burst {1, 3, 10}, refill {0.1, 1} per second), at most one
    to each other Instance.
  - **(d):** (c), plus HBEP-5's stop: no retry after two closed attempts under one request.
- **The anchor:** the layer at budget 0 must equal arm (a) request for request (`TestTheLayerAtBudget0EqualsAlphaRequestForRequest`),
  which it does on no fault and on each of F1-F4: the same status, error code and detail, completeness, engine
  admissions and deaths. So the layer changes nothing but the retries.
- **The pauses are anchored too:** the layer at budget 0 leaves the front's pause of an Instance as arm (a) does after
  each of three F3 failures pinned to it (`TestTheLayerAtBudget0PausesAsAlphaDoes`). This control found a bias in the
  layer's first version, which also reported home's failure itself: on a give-up the front counted it again, so arms
  (b)-(d) paused Instances after two failures, not three. The first hosted run carried it (its unmarked requests
  refused under F3 with no engine dead), and was discarded; every number here is from the run after the fix. The other
  direction is anchored as well: a failure on the Instance the layer retried to counts toward that Instance's pause
  (`TestTheLayersRetryTargetCountsTowardItsOwnPause`, under (b), with both answering 500).
- **Fidelity gaps:**
  - the front attributes a retried reply, its usage record and its in-flight count to the first Instance;
  - when the layer rescues a request that failed on its first Instance, the front sees a success there and resets
    that Instance's failure count, where a front-side retry would count the failure: behind the layer a dead
    Instance is paused only when a request to it cannot be rescued, while under (a) three refused dials pause it. What
    the client sees is the same (a refused dial is resent at once under every arm); only which Instance the front
    picks next differs;
  - the layer's "another Instance" is the next ready one in order, not the ring's;
  - the layer's retries count against the front's `streamHeadersTimeout` and its deadlines, as a front-side retry's
    would;
  - **a double is the whole Instance,** so its death closes its socket, and a later request to it is a refused dial,
    which every arm resends at once to another Instance (alpha's own rule) while the controller emulation still routes
    to it (D1 is 1 s, the SDK's first retry comes after 0.5 s). On a real Instance the worker dies behind its live
    tunnel and forwarder, whose own 5xx alpha passes through unsent. That chain is what lets the SDK's retries reach a
    new Instance each time under (a); under (b)-(d) the front's own retry of that 5xx would do the same. So the poison
    costs with the SDK are the stand's, and the bound in Verdict 4 is the result.

### The faults

Each fault is aimed with a marker in the request's prompt. F1, F2 and F4 are the double's own modes (#529), so each
acts only after the double has written the request's `admitted` record:

- **F1:** `die-on=S18-F1-D<i>` on Instance i only: the engine process ends (KILL to itself) under a request aimed at
  it, before any reply byte.
- **F2:** `fail-on=S18-F2-D<i>`: Instance i's engine answers 500 before any byte.
- **F3:** the spike's cutter, below the layer: the tunnel closes after the marked request was written, once the engine
  has recorded it admitted, holding every reply byte meanwhile; once per request ID, so a retry passes. The engine
  lives, and records the request `cancelled`. **F3x2** cuts a request's first two connections, a flapping tunnel, to
  measure (d)'s false stops.
- **F4, the poison request:** `die-on=S18-F4` on every Instance: every engine that reads it dies.

### Validity

- **Every request is accounted for** at the client and at the doubles: every engine record belongs to a client attempt,
  and every attempt an Instance answered has an engine record (`rig.Reconcile`, run on every cell).
- **A fault fires only where it was injected,** proven by the engines' and the client's records, never the injector's:
  the double's `admitted` record and then its process gone (F1, F4), its `refused` record (F2), its `cancelled` record
  (F3). With the mode armed and nothing marked, and with another Instance's marker, nothing fires (`TestF1UnderAlpha`,
  `TestF2UnderAlpha`, `TestF3UnderAlpha`, `TestF4UnderAlpha`); with no fault, every request is whole and begun exactly
  once, under (a) and under the layer at budget 0 (`TestNoFault...`).
- **The baseline reproduces alpha exactly,** against HBEP-5's Errors table, not the code: F1, F3 and F4 under (a) are
  502 `upstream_closed` with detail `tunnel`, begun exactly once and never sent again; F2's engine 500 passes through.
- **Every poison trial fires:** a trial waits until the deaths its own records show are seen, every restart is ready,
  and the front pauses none of the Instances, before the next (`TestEveryPoisonTrialFires`). Without the pause's wait,
  a later trial goes to another Instance or is refused, and measures nothing.
- **No sleep is a proof:** every wait is on records or state.
- **Mutants of the validity rules,** each against a green baseline, all killed (`scripts/mut18.py`'s V1-V7, below).

### The sweep

Q5's, as accepted: 5 requests a second for 2 minutes a cell (612 requests with the session probes); faults at 1% and
10% of requests, each on its own; F4 as 20 poison requests a cell against a steady background of 5 a second, with and
without the SDK's retries; streamed and non-streamed apart. 140 cells (`rig.Sweep`), split into 20 hosted jobs of at
most about 30 minutes (`rig.Groups`); each cell's seed is its place in the sweep.

## Results

All from run 43492532339. Every figure is in `results/results.csv` (one row a cell) and `results/cells/` (each cell's
summary); a pair "x / y" is non-streamed / streamed. A marked request **fired** when its engine's own records show the
fault (a marked request whose target was out of routing went to another Instance and measures nothing); **rescued** is
the share of the fired requests whose last attempt was a whole 200.

### Single faults: rescued among the fired

| arm | F1 1% | F1 10% | F2 1% | F2 10% | F3 1% | F3 10% |
|---|---|---|---|---|---|---|
| (a) | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| (b) | 1.00 / 1.00 | 0.86 / 0.78 | 1.00 / 1.00 | 1.00 / 1.00 | 1.00 / 1.00 | 1.00 / 1.00 |
| (c) burst 1, refill 0.1/s | 0.67 / 0.67 | 0.33 / 0.30 | 0.33 / 0.67 | 0.18 / 0.15 | 0.60 / 0.57 | 0.17 / 0.18 |
| (c) burst 1, refill 1/s | 1.00 / 1.00 | 0.74 / 0.50 | 1.00 / 1.00 | 0.74 / 0.65 | 1.00 / 1.00 | 0.67 / 0.62 |
| (c) burst 3, refill 0.1/s | 1.00 / 1.00 | 0.52 / 0.48 | 1.00 / 1.00 | 0.26 / 0.27 | 1.00 / 1.00 | 0.21 / 0.29 |
| (c) burst 3, refill 1/s | 1.00 / 1.00 | 0.91 / 0.85 | 1.00 / 1.00 | 1.00 / 0.99 | 1.00 / 1.00 | 1.00 / 1.00 |
| (c) burst 10, refill 0.1/s | 1.00 / 1.00 | 0.77 / 0.78 | 1.00 / 1.00 | 0.36 / 0.30 | 1.00 / 1.00 | 0.37 / 0.36 |
| (c) burst 10, refill 1/s | 1.00 / 1.00 | 0.79 / 0.86 | 1.00 / 1.00 | 1.00 / 1.00 | 1.00 / 1.00 | 1.00 / 1.00 |

Fired per cell: F1 3-10 at 1% and 10-29 at 10% (the double's death takes its Instance out, so later marked requests
often go elsewhere); F2 3-10 and 51-72; F3 2-8 and 47-76. At 1% a cell has few fired requests, so its share is coarse.

- **Duplicates:** one per rescue. Under (b) at 10%, 19 of 22 fired F1 requests were begun twice, 64 of 64 F2's, 61 of 61
  F3's.
- **Added time to first byte, (b) at 10%, p50 of the rescued against the unmarked:** F1 44.6 against 42.7 ms (7.3
  against 6.1 streamed), F2 42.8 against 42.6 (6.3 against 6.0), F3 48.0 against 42.7 (11.5 against 6.1). A non-streamed
  reply's first byte comes at its end (8 tokens at 5 ms); a stream's with its first token. F3's time includes the cutter
  waiting for the engine's record (polled every 5 ms).
- **The unmarked requests' success is not comparable across arms where engines die.** Under F1 at 10%, (a) answered
  0.34 of the unmarked requests and (b) 0.83, nearly all the rest 503 `no_ready_target`: the front pauses an Instance
  for 30 s after three failed connections, which outlasts the stand's 5 s restart, so at times every Instance was out.
  Behind the layer a dead Instance is paused less (Fidelity), which lifts (b)-(d). On vLLM a restart takes minutes, and
  any death takes its Instance out for that long. Under F2 and F3, where no engine dies, every arm answered every
  unmarked request.

### A tunnel that fails twice under one request (F3x2): the two-deaths stop's cost

| arm | fired, 1% | rescued, 1% | fired, 10% | rescued, 10% | given up with no engine dead (false stops, (d) only) |
|---|---|---|---|---|---|
| (a) | 4 / 6 | 0.00 / 0.00 | 62 / 41 | 0.00 / 0.00 |  |
| (b) | 5 / 2 | 0.00 / 0.00 | 56 / 53 | 0.00 / 0.00 |  |
| (c) burst 3, refill 1/s | 5 / 6 | 1.00 / 0.83 | 81 / 55 | 0.48 / 0.55 |  |
| (c) burst 10, refill 1/s | 12 / 4 | 1.00 / 1.00 | 43 / 53 | 1.00 / 1.00 |  |
| (d) burst 3, refill 1/s | 5 / 2 | 0.00 / 0.00 | 52 / 56 | 0.00 / 0.00 | 52 / 56 |
| (d) burst 10, refill 1/s | 3 / 2 | 0.00 / 0.00 | 50 / 52 | 0.00 / 0.00 | 50 / 52 |

Under (b) and (d) a request cut twice is given up; under (c), which allows a third attempt, it is rescued when the bucket
has the tokens (burst 3 runs out at 10%). No engine died in any F3x2 cell.

### The poison request (F4)

| arm | engines dead per poison request, no client retry | with the SDK's 2 retries | last answer, no client retry | with the SDK |
|---|---|---|---|---|
| (a) | 1.00 / 1.00 | 3.00 / 3.00 | 20x502 | 20x502 |
| (b) | 2.00 / 2.00 | 3.00 / 3.00 | 20x502 | 20x503 |
| (c) burst 1, refill 0.1/s | 1.70 / 1.70 | 3.00 / 3.00 | 20x502 | 12x503 8x502 |
| (c) burst 10, refill 1/s | 3.00 / 3.00 | 3.00 / 3.00 | 20x502 | 20x503 |
| (d) burst 10, refill 1/s | 2.00 / 2.00 | 3.00 / 3.00 | 20x502 | 20x503 |

20 poison requests a cell, each to the next Instance in turn, each fired (the engine's own records show it begun and
never ended), each settled before the next. The 503s are the SDK's last retry finding no Instance ready, all three dead.
Each death also cuts the requests in flight on that engine; the layer's arms rescue them, and those are duplicates too
(26 under (b), 20 of them the poison's own).

### The crash-correlation candidates

Per poison request, how often each candidate fired. For (ii), two of its deaths, on distinct Instances, within 30 s of
each other, timed by the process ends (`death_span_ns` in each cell's summary; the largest span per cell is
`poison_death_span_max_s` in `results.csv`):

| arm, client | (i) front, one request | (ii) controller, distinct Instances | (iii) front, by content (derived) |
|---|---|---|---|
| (a) | 0/20 / 0/20 | 0/20 / 0/20 | 0/20 / 0/20 |
| (a) + SDK | 0/20 / 0/20 | 20/20 / 20/20 | 20/20 / 20/20 |
| (b) | 20/20 / 20/20 | 20/20 / 20/20 | 20/20 / 20/20 |
| (b) + SDK | 20/20 / 20/20 | 20/20 / 20/20 | 20/20 / 20/20 |
| (c) burst 1, refill 0.1/s | 14/20 / 14/20 | 14/20 / 14/20 | 14/20 / 14/20 |
| (c) burst 10, refill 1/s | 20/20 / 20/20 | 20/20 / 20/20 | 20/20 / 20/20 |
| (d) burst 10, refill 1/s | 20/20 / 20/20 | 20/20 / 20/20 | 20/20 / 20/20 |
| (d) + SDK | 20/20 / 20/20 | 20/20 / 20/20 | 20/20 / 20/20 |

- **(i), front-side, per request:** two or more closed attempts under one front request. It cannot see the SDK's
  retries, each a request of its own, so it missed every poison request under (a) with the SDK, where all three
  Instances died.
- **(ii), controller-side:** a worker death within 30 s after a death of another Instance of the same deployment. Its
  firing per poison request follows the deaths column, since the controller tells a death from a cut by the exit
  itself; what the run measured is the timing: a poison request's deaths came within milliseconds of each other when
  the front's retry caused them, and 0.5-1.59 s apart with the SDK's retries, so the 30 s window held every pair (348
  poison requests with two or more deaths). At cell level the window also joins deaths of consecutive trials, 30 s
  apart: 33 joined deaths under (b) for its 20 trials of 2 deaths each, where 20 are within a trial.
- **(iii), front-side by request content (derived, not an arm):** two or more written-then-closed failures for one
  request content across the client's retries (the request's base ID stands in for a body hash). It fires as (ii) does
  under F4.
- **False positives.** In the 24 F3x2 cells (no engine dead), (i) and (iii) each fired 574 times, once for every
  request cut twice and retried, and (ii) never. Under F1 (independent deaths, one Instance each), (ii) fired 0-9 times
  per cell at 1% (3-10 deaths) and 8-28 at 10% (10-29 deaths, one every 4-12 s): deaths that frequent are worth an event
  whatever their cause. (i) and (iii) never fired under F1.

## Verdict

1. **Retry: yes, once, to another ready Instance,** for a request written and then closed before any response header,
   or answered 5xx before any body byte. It rescued every fired F2 and F3 request, and F1's whenever another Instance
   was up, for one duplicate execution each.
2. **The budget is a storm limit, not a poison guard.** A per-deployment token bucket rescues only when both its knobs
   keep up with the failures. Of the fired F2 and F3 requests at 10%: its refill (at a fifth of the fault rate it
   rescued 0.15-0.37), and its burst, since failures arrive in clusters (at twice the fault rate, a burst of 1 rescued
   0.62-0.74, and bursts of 3 and 10 all, but one cell at 0.99). F1's are lower at every bucket, since the other
   Instance was sometimes dead too. It lowered the poison's cost only by being empty. The spike
   cannot size it for production fault rates. **The science agent's decision (#538) is no separate bucket:** the retry
   is bounded by the caps and the FIFO, like any request.
3. **"Never after two Instances have died under one request" holds by construction with one retry,** since a request
   then reaches at most two Instances, so it needs no counter of its own. As a separate stop on top of a larger budget,
   (d), it saved nothing (2 deaths per poison, as (b)) and gave up every double cut (Q2's false stop). A request whose
   tunnel fails twice is not rescued by (b) either; rescuing it means a third attempt, which a poison request turns into
   a third death. The front cannot have both.
4. **What F4 costs:** one Instance under alpha; two under one retry; three, every Instance of the stand, under a budget
   that allows more. With the OpenAI SDK's retries, three under every arm on the stand, where a dead double's refused
   dial is resent at once (Fidelity); on real Instances fewer may die. Deaths per poison request are at most the
   Instances, and at most the client's attempts times the front's attempts per request (derived from the cells; with
   more than three Instances, one retry with the SDK can reach six).
5. **What stops a poison request is not the front's per-request rule** once clients retry: each retry is a new
   request. Two levers remain:
   - **the controller,** through candidate (ii) as HBEP-7's crash-correlation event (below);
   - **the client:** the OpenAI SDK obeys a server's `x-should-retry: false` and does not retry then, and waits out a
     `Retry-After` of up to 2 minutes (`openai-python` `src/openai/_base_client.py`, `_should_retry` and
     `_calculate_retry_timeout`). The front's 502 after a written-then-closed failure carries neither today. Setting
     `x-should-retry: false` on that answer, after the front's own retry, would cap a poison request at two deaths
     for those clients. This is derived from the SDK's source, not measured, and it also turns off the client's own
     retry of an ordinary single death, which the front's retry has then already tried.
   - A front-side stop keyed on the request's content, (iii), would see the SDK's retries, but it fires on every
     double cut as (d) does. It is offered as a follow-up arm, not built.

## Proposed HBEP-5 text for open question 5

To replace the open item (`hbep/0005-llmlab-compatible-front/README.md:155`):

> - **Decided for beta** (spike S18, #528): the front sends a request once more, to another ready Instance, when it
>   failed before any byte of the response reached the client: written and then closed before the response headers,
>   or answered with a 5xx before any body byte. Once only, so a request reaches at most two Instances: that is the
>   bound "never after two Instances have died under one request", and no separate count is kept. A refused or
>   timed-out connection is resent as in alpha, outside this retry. A per-deployment token bucket limits these retries
>   in a storm of failures, with both its knobs sized to the failures: a refill at least their expected rate (S18
>   rescued 0.15-0.37 of them with a refill at a fifth of it) and a burst that absorbs failures arriving together (at
>   twice the rate, a burst of 1 rescued 0.62-0.74 of them, and a burst of 3 nearly all). The budget does not guard against a
>   poison request.
> - For a stream, the response's first byte is its headers, which the front relays at once and vLLM sends before
>   prefill: a stream is retried only when it failed before its headers, and a death in prefill is not retried.
> - A retry is a duplicate execution: the first engine's work is lost.
> - A poison request kills at most two Instances per request to the front. A client that retries sends it again as new
>   requests: with the OpenAI SDK's default of two retries, up to six Instances (S18: all three of its stand). HBEP-7's
>   crash-correlation event is the guard across requests.

**Decided** (the science agent, #538), departing from this text in two places: **no separate bucket** (the retry's
target is chosen as a new request's would be, and waits in the FIFO when every candidate is at its cap), and **the
give-up 502 carries `x-should-retry: false`** after the front's own retry, not when there was no other Instance to
retry to (Verdict, 5).

## The crash-correlation event (HBEP-7)

HBEP-7 names the event as a mitigation of the poison-request risk (`:102`) and does not define it. Proposed definition,
from candidate (ii): **a worker exit, other than one the controller asked for, within 30 s after an exit of a worker on
another Instance of the same ModelDeployment.** The controller sees the exits themselves, so a cut tunnel, which the
front cannot tell from a death, never raises it. A poison request's deaths came at most 1.59 s apart in the run (the
SDK's retries, 0.5 s and 1 s after an answer; a front-side retry's within milliseconds), so 30 s holds them with room
for a slower client's retries, and a longer window joins more independent crashes. It also fires on independent
crashes frequent enough to matter (one every few seconds). One Instance's own crash loop is not counted: that is the
restart ladder's. What the controller does on the event is HBEP-7's to decide; S18 measured its timing only.
**Decided** (#538): a `failing` event `CrashCorrelated`, an event only.

## Unit tests of the harness

- `rig/`: `TestPoisonCostCountsInstancesWindowAndContent`, `TestSelectAndBudget`, and `TestAnalyzeCountsFromTheRecords` (the
  analysis on hand-built records, every expected value counted by hand;
  it found that a false death was first judged by a 5 s window, which another request's death nearby fooled, and it is
  now judged from the engine's own record of the request) and `TestTheSweepIsQ5sAndItsGroupsCoverIt`.
- `run/`: the controls above, `TestTheLayersRetryTargetCountsTowardItsOwnPause`, and `TestEachArmOnEachFault` (each arm on
  each fault, the expectations derived from its policy).
- **The validity mutants** (`scripts/mut18.py`), all killed: V1 the F1 fault aimed at every Instance; V2 a request ID
  reused; V3 the layer at budget 0 retrying; V4 deaths counted from the layer's closed attempts; V5 an F4 trial not
  waiting for the front's pause (it first survived: the control's three trials each hit a different Instance, and four
  now revisit one); V6 the cutter cutting every marked write; V7 a false death judged by a time window; V8 the layer
  also reporting home's outcome (the bias the first hosted run carried; killed by `TestTheLayerAtBudget0PausesAsAlphaDoes`);
  V9 candidate (ii) counting one Instance dying again; V10 a poison's candidate (ii) counting deaths, not Instances;
  V11 candidate (iii) counting one front request only; V12 the layer reporting no Instance's outcome (killed by
  `TestTheLayersRetryTargetCountsTowardItsOwnPause`); V13 a poison's candidate (ii) with no window.

## Environment

- **The run:** GitHub Actions run 43492532339, on the do-not-merge PR #531, whose head merges this branch and adds one
  workflow file (`scripts/hosted-sweep.yml` here, copied there). One job ran the controls, 20 ran the cells. The raw
  records of every cell (the client's attempts, the engines' records, the process ends, the layer's attempts) are the
  run's artifacts; the summaries are committed in `results/cells/`, computed again from those records by
  `cmd/s18analyze` after the analysis gained candidates (ii) by distinct Instances and (iii): every measure the run had
  already computed came out the same in all 140 cells.
- **The runners:** `ubuntu-24.04`, 4 vCPUs and 16 GB each, on AMD EPYC 7763, 9V45 and 9V74 and Intel Xeon 6973P-C and
  6975P-C (as `lscpu` named them); Go 1.26.0.
- **The stand:** 3 Instances, the double's token delay 5 ms, 8 tokens a reply, at most 64 sequences an engine; D1 1 s,
  D2 5 s; the front's defaults (`pauseAfterFailures` 3, `pauseDuration` 30 s).

## Provenance

| What | Origin | Used for |
|---|---|---|
| The front, its Proxy and Router | this repository, `internal/front` | arm (a), and the front under every arm |
| The engine double, and its fault modes | this repository, `internal/agent/enginedouble` (#529) | the Instances, F1, F2 and F4 |
| The retry layer, the cutter, the controller emulation, the client | new, this spike | arms (b)-(d), F3, restarts, the load |
| The SDK's retry defaults | `openai-python` (https://github.com/openai/openai-python, Apache-2.0): `src/openai/_constants.py` (2 retries, 0.5 s doubling, at most 8 s) and `_base_client.py` (`_should_retry`: a connection error, 408, 409, 429, 5xx; `x-should-retry`; `Retry-After` up to 2 min), read at its main branch | the client's SDK mode, and Verdict 5 |

## Deviations from the issue

- **The double's fault modes are their own PR (#529),** test support under `internal/agent/enginedouble`, as Q4
  ruled; this branch is stacked on it.
- **F3 is the spike's, not the double's:** a cutter below the layer, which needs no change to the engine.
- **Arms (b)-(d) are a layer at the `Transports` seam,** not inside the front's `forward`, which the brief keeps out of
  `internal/`. The budget-0 anchors and the fidelity gaps above bound what that changes.
- **early-headers is the spike's default for every stream** (Q1, confirmed by the science agent); the double's own
  default is unchanged.
- **The first hosted run was discarded:** its layer counted a given-up request's failure twice toward the front's pause
  (found by the pause anchor, fixed, and a mutant kept), and it lost 13 F4 cells to a bound of 10 minutes a cell that
  three deaths per trial outran (fixed: an F4 cell's bound grows with its trials). Every figure here is from the second
  run.
- **Candidate (iii) is derived from the records, not an arm,** and the content key is the request's ID, not a body
  hash.
- **The client's SDK mode has no jitter and ignores `Retry-After`:** the SDK takes 0-25% off each backoff and waits out
  a `Retry-After` of up to 2 minutes. The front's 502 after a death carries none, so the poison's retries are as
  modelled; the front's 503 `no_ready_target` carries `Retry-After: 30`, which the SDK would wait out, where the model
  retried at once and found no Instance ready.
- **The controller is emulated** (D1, D2), and every crash-correlation candidate is scored from the records afterwards:
  nothing emits an event.
- **N = 3 throughout:** "another Instance" has no data at one or two, and the per-poison costs at six or more are
  derived.
