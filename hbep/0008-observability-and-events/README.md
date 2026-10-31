# HBEP-8: Observability and events

## Summary

This HBEP decides what the framework measures and announces about the models it serves, and how a reader tells a missing reading from a zero. It covers:
- load and performance metrics that mean the same on every deployment;
- a typed event stream for the lifecycle of Allocations, Instances and ModelDeployments (HBEP-3);
- the liveness of every observer;
- per-request usage metering at the front;
- provider accounting read from the provider's own billing source.

It serves the user, the agents that operate deployments for them (HBEP-10), and the lifecycle controller (HBEP-7), which acts on the same signals.

## Motivation

This serves stage 4 of the user story in HBEP-2 (watch them). It also feeds stage 3 (keep instances running) and stage 7 (pay and give back).

A past project served models on several Slurm clusters behind its own fronts. Its signals existed somewhere but could not be read, compared or trusted:
- New instances served unannounced, and work parked behind a manual gate left GPUs idle for hours. Nothing said "ready" or "waiting".
- Watchers died silently with the session that started them, or kept their samples in temporary files that vanished. A summary later listed a dead watcher as live, so reports went unposted for hours.
- Empty samples were printed as 0, so busy routers looked idle. A rate check read renewable work claims as starts and always said OK. A window count returned zeros when it could not read its input.
- KV-cache utilisation was compared across deployments. Attention implementations differ per GPU type, so one percentage meant different loads. Streams with few chunks gave no per-token latency at all.
- Preemption counters reset when a server restarted, which invalidated before/after comparisons. A routing change was judged on an unchanged fingerprint alone, and it missed a several-fold throughput loss that prefix-cache hit rate and time to first token would have shown.
- A job ended by its time limit left its endpoint file behind, so the inventory kept a dead instance. Deliberate restarts were counted as crashes.
- Monitors read their list of targets once at start, so new deployments went uncounted for hours.
- Other tools parsed free-text state lines, and misread one kind of event as another.
- Tokens served and GPU-hours were rebuilt from logs afterwards. A center's usage report counted only finished jobs. A forecast counted pending jobs with an unknown start as starting now.

### Goals

- Metrics that compare across deployments, providers and GPU types: running and waiting requests per instance, time per output token, preemptions, prefix-cache hit rate, and time to first token.
- An event stream with the types started, ready, degraded, failing, ending soon and replaced, for every Allocation, Instance and ModelDeployment.
- Liveness for every observer. A scraper, prober or watcher that stops is itself an event.
- An explicit "unknown" for every sample that could not be read, never a zero.
- Per-request usage metering at the front, attributed to the client key and to the deployment and instance that answered (HBEP-9).
- Provider accounting (grant hours by Allocation) from the provider's own billing source, with running and pending Allocations included.
- Observation that follows the inventory (HBEP-10) as it changes.

### Non-goals

- Choosing a metrics database or dashboard. Design details may name one later.
- Alerting policy: which threshold pages whom. Thresholds are platform-profile parameters (HBEP-4); routing alerts to people is the user's choice.
- Delivery guarantees to event subscribers. HBEP-10 owns subscriptions, cursors and acknowledgement. This HBEP owns what an event is and which types exist.
- Logging prompts or responses. Usage metering records counts and identities only.
- Engine metrics inside a managed provider. LLMLab exposes none, so the framework measures it from the front.

## Proposal

### User stories

- A researcher compares an instance on Helios with one on Athena by running and waiting requests, time per output token and time to first token, on one scale.
- An agent subscribes to "ending soon" and starts a handover before an Allocation's hard end, instead of polling job lists.
- An agent operating overnight receives a `failing` event for one provider's prober, reason "heartbeat missed". It does not read the silence as health.
- At the end of a month, a researcher reads tokens served per model and per client key, and grant hours per Allocation, from records kept while serving.
- Before changing a routing setting, an operator compares per-session throughput, prefix-cache hit rate and time to first token before and after. The comparison cannot silently straddle an instance restart.

### Notes, constraints and caveats

- **Three sources.** Engine metrics come from each Instance. Front metrics (time to first token, errors, usage) come from the front, for every provider including managed ones. Provider facts (queue state, start estimate, hard end, billing) come from the provider plugin (HBEP-4).
- **KV-cache utilisation is recorded but not compared** across deployments. Load is compared by counts and times only.
- **Unknown is a value.** A sample is a value, "unknown" with a reason (unreachable, timed out, not reported by this engine), or "not applicable". An aggregate says how many of its samples were unknown. A check that cannot read its input reports unknown, never pass.
- **Every series belongs to one Instance** (HBEP-9 attribution). An engine restart starts a new series, so a rate never goes negative and a comparison across a restart is visible as one.
- **Six types, many reasons.** The types are the six in Goals, for any subject: an Allocation, Instance or ModelDeployment, and also an observer or controller. Everything finer is a reason code. A stopped observer is `failing` with reason "heartbeat missed"; a restart, a missed submission or a repeated crash (HBEP-7) are reasons under `failing`, `degraded` or `replaced`. A new type is an amendment to this HBEP; a new reason is not.
- **Events are typed.** Each has a type, a reason code, the resource's stable name, a time in UTC from the emitter's clock, and an optional message. Tools act on type and reason, never on the message.
- **Planned and unplanned stops differ.** An event for a stop that someone asked for names the actor and the operation (HBEP-10). A stop without them is a failure.
- **Usage records carry the request's own time,** not the time the record was written or copied.
- **Provider accounting** comes from the platform's own billing source, per provider, never from the framework's logs. Formulas differ per cluster (some charge the largest of a core, GPU or memory share), so the formula is platform-profile data. A pending Allocation with an unknown start is unknown, never "starting now".
- **Contingencies stay on.** Every check here is a platform-profile parameter with a threshold, on by default and cheap when idle, as for every contingency in this framework.

### Risks and mitigations

- **Observers die silently.** Every observer sends a heartbeat. A component that does not depend on it marks it down after a profile-set interval and emits an event. Observers run under the control plane (HBEP-11), never in an agent's session.
- **Heartbeats alias against check ticks.** A batched heartbeat read against a fixed tick gave false "down" alerts. The down interval spans several heartbeat periods, and the check reads the heartbeat's own time stamp, never a file's modification time.
- **Events taken as the only truth.** A subscriber that misses an event acts on stale state. Events are edges; resource status (HBEP-11) is the level truth, and a resuming subscriber re-reads status.
- **A metric some engines cannot give.** With few chunks, the front cannot derive time per token. Engine-side timing is used where reported; otherwise the value is unknown.
- **Metering on the request path** adds latency, or loses records when the front crashes. A lost record must show as a gap in the usage record, never as silence.
  - **Mitigation (candidate)**, from spike S8 (issue #25, PR #28): usage records go to the log with `sync_interval: always`. They are batched only above a few hundred records per second, since one node acknowledged about 1,200 publishes per second on the test disk under `always`. A wider batch widens what a front crash loses.
  - **Not tested by S8:** power loss and a kernel crash, the only cases where `always` differs from the default.
  - **Alpha** (HBEP-5): HBEP-11 has no durable log in alpha, so the front appends usage records to a local JSON Lines file. It syncs each record before the next, writing the records that wait behind a sync in one write, and keeps the counter below in blocks synced to a file of their own.
  - **Counter:** each usage record carries a monotonic counter that the front persists independently of the log, so a consumer can see a missing range.
  - **Still open:** detecting a final batch lost in a front crash before it was published, since no later record reveals it. S8 did not test a front crash.
- **Billing sources lag or undercount,** as a report of finished jobs only does. Where a center report and the scheduler's accounting differ, both are shown with the gap.
- **Managed providers are opaque.** LLMLab's periodic stalls show only in front-side time to first token. Its "degraded" events come from front measurements and probes, against the platform profile's thresholds.
- **Series multiply with handovers.** Each handover adds a series per metric, so retention and roll-up per deployment are needed (see Open questions).

### Open questions

- Metric names and units: vLLM's Prometheus names, OpenTelemetry's conventions for generative AI, or our own? A spike maps two engines' metrics onto the Goals list.
- How engine metrics leave the job: scraped through the dial-home tunnel, or pushed by a process in the job. Depends on HBEP-6.
- Where "degraded" and "failing" are defined: per metric in the platform profile, or per deployment. Discussion with HBEP-4. For alpha, HBEP-7's malfunction thresholds are platform-profile values with defaults (HBEP-4, the `health` group).
- How "ending soon" is timed: one lead time per platform profile, or derived from the handover's measured start-up time (HBEP-7). Lean: from the profile, repeated at fixed steps. For alpha, HBEP-7 emits it once, an hour before a ready Instance's drain start when no replacement is ready.
- Whether a usage count comes from the engine's usage report only, or also from the front's own count when the engine sends none, marked as such. Lean: both, marked. For alpha, HBEP-5 records the engine's count when the response carries one; otherwise the counts are null and the front's count of streamed events is recorded, marked as such.
- Retention of metrics, usage and events. Lean: usage for the life of the grant; per-instance series rolled up per deployment after a period; events as long as the audit log (HBEP-10).
- Whether each client key can read its own usage through the front, as LLMLab users may expect. Discussion with HBEP-5.
- The last level of liveness: an end-to-end probe of the front and control plane from outside both. Where it runs and who gets its alert.
- Probes versus live traffic for time to first token. Live traffic mixes prompt lengths; probes cost capacity. A measurement on a test deployment decides.
- Whether an observer that restarts emits the transitions it missed, or one `degraded` event with reason "gap". Lean: the "gap" event; status is the truth. S8's gap marker covers events evicted from the log. An observer's missed transitions were never published, so it still needs its own "gap" event.

## Design details

TBD. The Proposal fixes what is observed and how unknowns and events are shown. Storage, transport and schema format are decided by the open questions on names, transport and retention.

### Test plan

Tests for the failure modes in Risks, to be written with the design:
- Stop an observer: a `failing` event for it appears within the profile interval, and the instance it watched shows unknown, not healthy.
- Make an engine's metrics endpoint time out or return nothing: the sample is unknown with a reason, and the window aggregate counts it.
- Restart an engine: a new series starts under the new Instance ID, and no rate goes negative.
- End a job at its time limit: the Instance leaves the inventory with an event citing the time limit, and no endpoint stays listed.
- Drain an instance on purpose, then crash another: only the drain names an actor and an operation.
- Add a deployment while observers run: it is observed without restarting them.
- Send heartbeats in batches against a fixed check tick: no false `failing` event.
- Give the billing reader a pending Allocation with an unknown start: its hours are unknown.
- Stream a long reply with few chunks: time per token is the engine's value or unknown, never 0.

### Compatibility and upgrades

- New reason codes will appear, and new types only by amendment. Readers treat an unknown reason as its type's generic case, and an unknown type as "other", and keep working. TBD how versions are named.
- LLMLab exposes no metrics, so there is no client contract to keep for them. Usage reporting inside responses follows HBEP-5.

### Security

- Usage records hold a client key's ID, never the key, and counts, never prompts or responses.
- Events and metrics never carry credentials. Provider job IDs are Instance metadata (HBEP-3), visible only to identities that can read the Allocation.
- Who reads which usage: a client key its own, operators all. TBD with HBEP-11's access control.

### Graduation criteria

TBD. Lean: alpha when every Goals metric and event exists for one batch provider and LLMLab; beta when the Test plan's failure tests pass on two batch providers.

## Operational readiness

1. **Working:** every observer's heartbeat is current, and a probe request through the front appears in metrics and usage with the right deployment and instance. A `failing` event for an observer, or a probe that never appears, says it is not.
2. **Restart:** an observer re-reads the inventory and resumes. Engine counters start new series. Missed transitions: see Open questions.
3. **Silent failure:** an observer that runs but reads nothing. Its samples are unknown, and the share of unknown samples is itself a metric. A display that draws unknown as zero would hide this, so displays must draw unknown distinctly.
4. **Provider limits:** reading queue state and accounting meets rate limits and login throttles on some clusters. Reads are paced per platform profile (HBEP-4).
5. **Capacity:** at a full KV cache, waiting requests and preemptions rise; those are the signals. Behaviour of a full metrics store: TBD.
6. **Idle cost:** observers run on the control plane's host; nothing runs on providers. TBD in numbers.
7. **Rollback:** observation can be switched off per provider. Serving continues, and every reading that depended on it turns unknown.

## Drawbacks

- Every reader must handle three outcomes, not two.
- Per-instance series and per-request records add storage that a single-user setup must carry.

## Alternatives

- **Each engine's own dashboard per site.** The metrics exist, but without common names and labels they cannot be compared, and they cover neither the front nor the provider.
- **Free-text logs as the event source.** Cheap to write, but tools that parse them misread one event as another.
- **Rebuilding usage from logs afterwards.** Possible, but slow, and only as complete as the logs.
- **KV-cache utilisation as the load measure.** Simple, but it does not compare across attention implementations.

## Prior art and reuse

Events and accounting replace www-2027's exit-to-wake watchers, its append-only live archive, and its grant forecast from scheduler accounting, which counted the running jobs that the center's tool missed.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| Prometheus | [prometheus/prometheus](https://github.com/prometheus/prometheus) | Apache-2.0 | Candidate, decided by spike S9: scrapes engines through the front, with targets from HTTP service discovery over the inventory |
| NATS JetStream | [nats-io/nats-server](https://github.com/nats-io/nats-server) | Apache-2.0 | Candidate, supported by spike S8 (PR #28) except for power loss, which was not tested: an embedded durable log for events and usage records, with `sync_interval: always` |
| CloudEvents 1.0.2 and sdk-go | [cloudevents/sdk-go](https://github.com/cloudevents/sdk-go); [spec](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md) | sdk-go Apache-2.0; spec Apache-2.0 | Candidate, no spike assigned: the envelope for events and usage records |
| OTel GenAI semantic conventions | [semantic-conventions-genai](https://github.com/open-telemetry/semantic-conventions-genai) | Apache-2.0 | Candidate, no spike assigned: names for front metrics, pinned to a commit; no code |
| vLLM metric names | [vllm-project/vllm](https://github.com/vllm-project/vllm) | Apache-2.0 | Candidate, no spike assigned: engine metric names kept at the source and mapped by recording rules |
| Kubernetes Events | the control plane's API host (HBEP-11): [Kubernetes](https://github.com/kubernetes/kubernetes), on k3s or on the single-user host that HBEP-11's rule picks | Apache-2.0 | Candidate, no spike assigned: a mirror for people only, never the event stream |
| llm-d-inference-sim | [llm-d/llm-d-inference-sim](https://github.com/llm-d/llm-d-inference-sim) | Apache-2.0 | Candidate test dependency, no spike assigned: an engine double for the metric tests |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| Detect a gap when a cursor falls below the stream's first sequence | Synadia's JetStream check ([nats-unprocessed-critical](https://www.synadia.com/insights/checks/nats-unprocessed-critical)) | The "gap" event for observers that missed transitions (open questions) |

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| New instances served unannounced, and parked work left GPUs idle. | L-10.1 | Typed lifecycle events, including started and ready. |
| Watchers died silently with the session that started them. | L-10.2 | Observers send heartbeats and run under the control plane. |
| Untracked watchers woke nobody, and duplicates doubled alerts. | L-10.3 | Observers run under the control plane, never in an agent's session. |
| Empty samples were shown as zero, and reads hit the wrong target. | L-10.5 | "Unknown" with a reason is a value, never zero. |
| Checks that could not see their input reported OK. | L-10.6 | A check that cannot read its input reports unknown, never pass. |
| Counters reset by a restart invalidated before/after comparisons. | L-10.7 | An engine restart starts a new series. |
| A batched heartbeat read against a fixed tick gave false "down" alerts. | L-10.8 | The down interval spans several heartbeat periods. |
| Monitors read their targets once at start. | L-10.9 | Observation follows the inventory as it changes. |
| A job ended by its time limit stayed listed. | L-10.10 | The Instance leaves the inventory with an event citing the time limit. |
| Other tools misread free-text state lines. | L-10.14 | Typed events; tools act on type and reason, never on the message. |
| A deliberate restart was counted as a crash. | L-10.16 | A planned stop names an actor and an operation. |
| KV-cache percentages meant different loads on different deployments. | L-5.6 | KV-cache use is recorded but not compared. |
| A routing change was judged on the fingerprint alone. | L-4.5 | Per-session throughput, prefix-cache hit rate and time to first token, before and after. |
| Tokens served and GPU-hours were rebuilt from logs afterwards. | L-13.6 | Per-request usage metering at the front. |
| The center's tool counted only finished jobs. | L-13.1 | Accounting from the provider's billing source, with running and pending Allocations. |
| Billing formulas differ per cluster. | L-13.2 | The formula is platform-profile data. |
| A forecast counted pending jobs with an unknown start as starting now. | L-13.3 | A pending Allocation with an unknown start is unknown. |
| Spend counted by a record's modification time inflated an estimate. | L-13.7 | Usage records carry the request's own time. |
| A managed gateway's periodic stalls showed only in timings. | L-6.2 | "Degraded" events from front-side time to first token. |

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #8.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).
- 2026-10-30: Prior art and reuse filled from the state-of-the-art evaluation.
- 2026-10-30: spike S8 (PR #28) findings carried in.
- 2026-10-30: aligned with HBEP-11 becoming implementable: the API host row.
- 2026-10-30: alpha's thresholds for failing Instances and its "ending soon" timing noted from HBEP-7.
- 2026-10-30: the front's alpha usage sink and counts noted from HBEP-5.
- 2026-10-31: noted from HBEP-6 becoming implementable: in alpha the tunnel carries only what HBEP-5 routes to an Instance, its listed endpoints and its passthrough of every other JSON `POST` under `/v1/`, and engine metrics reach the controller in the agent's reports, not by a scrape through the tunnel (open question on how metrics leave the job). HBEP-6 proposes the event reasons `PathProbeFailing` (the front's probe, type `degraded`) and `TunnelOrphaned`; it records no tunnel heartbeat metric, since its frps plugin does not see pings.

## Amendments

None.
