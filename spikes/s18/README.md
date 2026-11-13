# Spike S18: retries before the first byte, with fault injection (HBEP-5, open question 5)

Issue #528. HBEP-5's open question 5 asks whether the front should send a request again after it was written to an
Instance but before any byte of the reply reached the client: "retrying before the first byte reaches the client, to
another Instance, within a per-deployment budget, never after two Instances have died under one request. A spike with
fault injection decides." The guard matters because of HBEP-7's poison-request risk (`hbep/0007-lifecycle-controller/README.md:102`):
a request that kills every engine it reaches would, retried freely, take down one Instance after another.

FINDINGS: FILL from the hosted sweep.

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
  the front's failure path does, so the front's pauses act as in the product. A refused dial wrote nothing, so the
  layer resends it once with no budget, as alpha's own rule does.
  - **(b):** one retry, to another Instance.
  - **(c):** retries within a per-deployment token bucket (burst {1, 3, 10}, refill {0.1, 1} per second), at most one
    to each other Instance.
  - **(d):** (c), plus HBEP-5's stop: no retry after two closed attempts under one request.
- **The anchor:** the layer at budget 0 must equal arm (a) request for request (`TestTheLayerAtBudget0EqualsAlphaRequestForRequest`),
  which it does on no fault and on each of F1-F4: the same status, error code and detail, completeness, engine
  admissions and deaths. So the layer changes nothing but the retries.
- **Fidelity gaps:** the front attributes a retried reply, its usage record and its in-flight count to the first
  Instance, and a success there clears that Instance's failure count; the layer's "another Instance" is the next ready
  one in order, not the ring's; and the layer's retries count against the front's `streamHeadersTimeout` and its
  deadlines, as a front-side retry would.

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

Q5's, as accepted: 5 requests a second for 2 minutes a cell; faults at 1% and 10% of requests, each on its own; F4 as
20 poison requests a cell against a steady background of 5 a second, with and without the SDK's retries; streamed and
non-streamed apart. 140 cells (`rig.Sweep`), split into 16 hosted jobs of about 30 minutes (`rig.Groups`); each cell's
seed is its place in the sweep.

## Results

FILL from the hosted run.

## Verdict

FILL.

## Proposed HBEP-5 text for open question 5

FILL.

## The crash-correlation event (HBEP-7)

FILL.

## Unit tests of the harness

- `rig/`: `TestAnalyzeCountsFromTheRecords` (the analysis on hand-built records, every expected value counted by hand;
  it found that a false death was first judged by a 5 s window, which another request's death nearby fooled, and it is
  now judged from the engine's own record of the request) and `TestTheSweepIsQ5sAndItsGroupsCoverIt`.
- `run/`: the controls above, and `TestEachArmOnEachFault` (each arm on each fault, the expectations derived from its
  policy).
- **The validity mutants** (`scripts/mut18.py`), all killed: V1 the F1 fault aimed at every Instance; V2 a request ID
  reused; V3 the layer at budget 0 retrying; V4 deaths counted from the layer's closed attempts; V5 an F4 trial not
  waiting for the front's pause (it first survived: the control's three trials each hit a different Instance, and four
  now revisit one); V6 the cutter cutting every marked write; V7 a false death judged by a time window.

## Environment

FILL: the hosted runners.

## Provenance

| What | Origin | Used for |
|---|---|---|
| The front, its Proxy and Router | this repository, `internal/front` | arm (a), and the front under every arm |
| The engine double, and its fault modes | this repository, `internal/agent/enginedouble` (#529) | the Instances, F1, F2 and F4 |
| The retry layer, the cutter, the controller emulation, the client | new, this spike | arms (b)-(d), F3, restarts, the load |
| The SDK's retry defaults | the OpenAI Python SDK's documented defaults (2 retries, exponential backoff from 0.5 s) | the client's SDK mode |

## Deviations from the issue

FILL.
