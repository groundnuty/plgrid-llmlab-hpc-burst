# HBEP-8: Observability and events

## Summary

This HBEP decides what the framework measures and announces about the models it serves, and how a reader tells a missing reading from a zero. It covers:
- load and performance metrics that mean the same on every deployment;
- a typed event stream for the lifecycle of Allocations, Instances and ModelDeployments (HBEP-3);
- the liveness of every observer;
- per-request usage metering at the front;
- provider accounting read from the provider's own accounting.

It serves the user, the agents that operate deployments for them (HBEP-10), and the lifecycle controller (HBEP-7), which acts on the same signals.

In alpha:
- events are Kubernetes Events (`events.k8s.io/v1`), kept 7 days, with HBEP-8's type in `action`;
- metrics are Prometheus exposition from the front and the lifecycle controller, under `hpcburst_` names;
- usage records are the front's synced JSON Lines files;
- provider accounting is allocated GPU-hours and CPU-hours per Allocation, from the scheduler's accounting.

A durable event log with subscriptions comes in beta, with HBEP-10.

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
- Provider accounting (grant hours by Allocation) from the provider's own accounting, with running and pending Allocations included.
- Observation that follows the inventory (HBEP-10) as it changes.

### Non-goals

- Choosing a metrics database or dashboard. The framework exposes metrics; storing them is the user's choice.
- Alerting policy: which threshold pages whom. Thresholds are platform-profile parameters (HBEP-4); routing alerts to people is the user's choice.
- Delivery guarantees to event subscribers. HBEP-10 owns subscriptions, cursors and acknowledgement. This HBEP owns what an event is and which types exist.
- Logging prompts or responses. Usage metering records counts and identities only.
- Engine metrics inside a managed provider. LLMLab exposes none, so the framework measures it from the front.

## Proposal

### User stories

- A researcher compares an instance on Helios with one on Athena by running and waiting requests, time per output token and time to first token, on one scale.
- An agent subscribes to "ending soon" and starts a handover before an Allocation's hard end, instead of polling job lists. In alpha it watches the Instance's events, and re-reads status when it resumes.
- An agent operating overnight receives a `failing` event for one provider's reads, reason `HeartbeatMissed`. It does not read the silence as health.
- At the end of a month, a researcher reads tokens served per model and per client key, and allocated GPU-hours per Allocation, from records kept while serving.
- Before changing a routing setting, an operator compares per-session throughput, prefix-cache hit rate and time to first token before and after. The comparison cannot silently straddle an instance restart.

### Notes, constraints and caveats

- **Three sources.** Engine metrics come from each Instance, through the agent's health reports (HBEP-7). Front metrics (time to first token, errors, cuts, usage) come from the front, for every provider including managed ones. Provider facts (queue state, start estimate, hard end, accounting) come from the provider plugin (HBEP-4).
- **KV-cache utilisation is recorded but not compared** across deployments. Load is compared by counts and times only.
- **Unknown is a value.** A sample is a value, "unknown" with a reason (unreachable, stale, not reported by this engine), or "not applicable". An aggregate says how many of its samples were unknown. A check that cannot read its input reports unknown, never pass.
- **Every series belongs to one Instance** (HBEP-9 attribution), and to one engine process of it. An engine restart starts a new series, so a rate never goes negative and a comparison across a restart is visible as one.
- **Six types, many reasons.** The types are `started`, `ready`, `degraded`, `failing`, `ending-soon` and `replaced`, spelled as the lifecycle controller emits them (PR #88). They apply to any subject: an Allocation, Instance or ModelDeployment, and also an observer or controller. Everything finer is a CamelCase reason code. A stopped observer is `failing` with reason `HeartbeatMissed`. A restart, a missed submission or a repeated crash (HBEP-7) are reasons under `failing`, `degraded` or `replaced`. A new type is an amendment to this HBEP; a new reason is not.
- **Events are typed.** Each has a type, a reason code, the resource's stable name, a time in UTC from the emitter's clock, and an optional message. Tools act on type and reason, never on the message.
- **Status is the truth, events are best-effort.** An event can be merged with an earlier one, delayed or dropped (Design details, What the recorder merges and drops). A reader that must not miss a change reads status.
- **Planned and unplanned stops differ.** An event for a stop that someone asked for names the actor and the operation (HBEP-10). A stop without them is a failure. In alpha the actor is the emitting controller, in the event's `reportingController`.
- **Usage records carry the request's own time,** not the time the record was written or copied.
- **Provider accounting** comes from the platform's own accounting, per provider, never from the framework's logs. In alpha it is allocated time only, labelled "allocated, not billed". Billing formulas differ per cluster (some charge the largest of a core, GPU or memory share), so the formula is platform-profile data, open for beta. A pending Allocation with an unknown start is unknown, never "starting now".
- **Contingencies stay on.** Every check here is on by default and cheap when idle, as for every contingency in this framework.

### Risks and mitigations

- **Observers die silently.** Every observer has a heartbeat: a time stamp it writes on each successful read, or a `Lease` it renews. A component that does not depend on it judges the heartbeat's age, marks the readings it fed unknown, and emits a `failing` event with reason `HeartbeatMissed` (Design details, Observers). Observers run under the control plane (HBEP-11), never in an agent's session.
- **Heartbeats alias against check ticks.** A batched heartbeat read against a fixed tick gave false "down" alerts. Each down interval spans several heartbeat periods, and the check reads the heartbeat's own time stamp, never a file's modification time.
- **Events taken as the only truth.** A subscriber that misses an event acts on stale state. Events are edges; resource status (HBEP-11) is the level truth, and a resuming subscriber re-reads status. client-go's recorder merges and drops events by design (Design details), so no tool depends on seeing every one.
- **A metric some engines cannot give.** With few chunks, the front cannot derive time per token. Engine-side timing is used where reported; otherwise the value is unknown.
- **Metering on the request path** adds latency, or loses records when the front crashes. A lost record must show as a gap in the usage record, or in a failure count, never as silence.
  - **Alpha** (HBEP-5): the front appends usage records to a local JSON Lines file. Each record is synced before the request is reported done to the sink's caller. Records that end together share one write and one sync, so they are batched only when they arrive faster than the disk syncs (Design details, Usage records).
  - **Counter:** each usage record carries a monotonic counter that the front persists apart from the log, in blocks, so a consumer can see a missing range after a crash.
  - **A crash leaves a gap that bounds the loss without counting it.** The gap holds the reserved counters that were never used and the records lost in the crash, and a reader cannot tell them apart. A clean stop leaves no gap of its own, but the counters of a batch that failed were assigned, so they still show as one, and so can a reservation whose persistence is uncertain (Design details, Usage records).
  - **Beta:** spike S8 (issue #25, PR #28) measured NATS JetStream as the durable log, with `sync_interval: always`. One node acknowledged about 1,200 publishes per second on the test disk under `always`. S8 did not test power loss, a kernel crash or a front crash. Its findings are the beta design's evidence, with HBEP-10.
- **A full disk.** The usage files and the control plane's SQLite store share the host's disk. With SQLite, a full disk stops writes (HBEP-11, Operational readiness 5), so the `failing` event for the usage failure may not be stored either. The failure counter and the front's readiness are the reliable signals (Design details, Usage records).
- **Accounting sources lag or undercount,** as a report of finished jobs only does. The framework reads the scheduler's accounting, which counts running jobs. Where a center report and the scheduler's accounting differ, both are shown with the gap (beta).
- **Managed providers are opaque.** LLMLab's periodic stalls show only in the front's time to first token.
- **Series multiply with handovers.** Each handover adds a series per metric. Engine series carry only identity labels, and descriptive labels sit on one info series per Instance (Design details, Metrics). Retention and roll-up belong to whatever store the user picks.

### Open questions

"Science agent, 31 Oct 2026" marks a decision the science agent made that day for this HBEP to become implementable. "Proposed by this HBEP" marks the author's own choice, put to the review round. Questions decided in another HBEP name it.

1. **Metric names and units.**
   - **Decided for alpha:** our own names, prefixed `hpcburst_`, with units in the name, following Prometheus's naming conventions. vLLM's names are mapped to them in the agent and the controller (Design details, Metrics). Source: science agent, 31 Oct 2026.
   - **Open (blocks beta):** OpenTelemetry's conventions for generative AI, as names or as a mapping.
2. **How engine metrics leave the job.**
   - **Decided for alpha:** in the agent's health reports (HBEP-7, The allocation agent), never by a scrape through the tunnel. The tunnel carries only the front's routed requests and its passthrough (HBEP-6). Source: HBEP-6 and HBEP-7.
   - Spike S9 asked whether a scrape through the front and tunnel shows a gone Instance as down. Alpha has no such scrape, and the unknown rule answers the rest, so S9 is not needed for alpha.
3. **Where "degraded" and "failing" are defined.**
   - **Decided for alpha:** HBEP-7's malfunction signals, with thresholds in the platform profile's `health` group (HBEP-4, Platform profile schema). Source: HBEP-4 and HBEP-7.
4. **How "ending soon" is timed.**
   - **Decided for alpha:** once, an hour before a ready Instance's drain start, when no replacement is ready (HBEP-7, Events and conditions). Source: HBEP-7.
   - **Not needed for alpha:** a lead time from the profile, repeated at fixed steps.
5. **Usage counts.**
   - **Decided for alpha:** the engine's count when the response carries one; otherwise the counts are null and the front's count of streamed content events is recorded, marked `front_events`. Source: HBEP-5, Usage records.
6. **Retention of events, usage and metrics.**
   - **Decided for alpha:** events are kept 7 days: the launcher sets k3s's apiserver `event-ttl` to 168 h (Design details, Retention; HBEP-11). Source: science agent, 31 Oct 2026.
   - **Decided for alpha:** usage records are kept for the grant's life. Rotation keeps every file, and nothing in the framework deletes one. Source: science agent, 31 Oct 2026.
   - **Not needed for alpha:** retention of a metrics store, since the framework keeps none.
   - **Open (blocks beta):** event retention in HBEP-10's durable log, against the audit log's.
7. **Whether each client key can read its own usage through the front.**
   - **Not needed for alpha:** the operator reads the usage files on the host. Source: science agent, 31 Oct 2026.
8. **The last level of liveness: an end-to-end probe from outside the front and control plane.**
   - **Not needed for alpha:** the alpha host's own checks (Operational readiness 1) and the client's view are the outer level. Source: science agent, 31 Oct 2026.
9. **Probes or live traffic for time to first token.**
   - **Decided for alpha:** live traffic only. Each usage record carries `firstTokenMs`, and the front exports their distribution. Alpha sends no probe requests for timing; HBEP-6's probe checks the path, not timing. Source: science agent, 31 Oct 2026.
10. **What an observer that restarts emits.**
    - **Decided for alpha:** one `degraded` event with reason `ObservationGap`, from the lifecycle controller and the front, and status is the truth. The Provider managers emit none in alpha, since their recorder writes no `action` (open question 14). Its message names the gap's start, the observer's last heartbeat before the restart. S8's gap marker covers events evicted from a log; an observer's missed transitions were never published, so it still needs its own gap event. Source: the lean of this HBEP, confirmed by the science agent, 31 Oct 2026.
11. **The event stream's store.**
    - **Decided for alpha:** Kubernetes Events (`events.k8s.io/v1`), best-effort, with status as the truth. Source: science agent, 31 Oct 2026.
    - **Open (blocks beta):** JetStream for events and usage, with subscriptions and cursors, tied to HBEP-10, on S8's evidence.
12. **How an unknown sample is shown in Prometheus.**
    - **Decided for alpha:** an absent series plus an explicit gauge, `hpcburst_instance_metrics_unknown`, with the reason (Design details, Unknown samples). Source: proposed by this HBEP, 31 Oct 2026, from the comparison there; the science agent asked for one form to be chosen with its reason.
13. **Whether the front serves while usage cannot be written.**
    - **Decided for alpha:** it keeps serving, and the failure is loud: counted, raised as a `failing` event and stated in its readiness. Source: the science agent's lean, 31 Oct 2026, taken here.
14. **Typed events for Allocations.**
    - **Decided for alpha:** an Allocation's lifecycle shows in its Instance's events, whose reasons name the Allocation's outcome (`AllocationFailed`, `AllocationLost`, `AllocationRefused`, `Requeued`, `SubmittedAsOther`, `AllocationEndedEarly`; HBEP-7). The Provider controller's own events (HBEP-4) keep crossplane-runtime's recorder, which writes no `action`; readers treat them as type "other" (Compatibility). Source: proposed by this HBEP, 31 Oct 2026.
    - **Open (blocks beta):** typed events on the Allocation itself, from the Provider controller.
15. **The billing formula.**
    - **Open (blocks beta):** the platform profile's billing group (HBEP-4, open question 3). Alpha counts allocated time only.
16. **A per-grant accounting ledger.**
    - **Decided for alpha:** one accounting record per ended Allocation, kept after the Allocation is deleted (Design details, Provider accounting). Source: proposed by this HBEP, 31 Oct 2026, since an Allocation's status goes with it.
    - **Open (blocks beta):** totals per grant, and the comparison with a center's report.

## Design details

### Events

**Form.** Every event is an `events.k8s.io/v1` Event ([`events/v1/types.go`, lines 43–85](https://github.com/kubernetes/api/blob/v0.35.3/events/v1/types.go#L43-L85), accessed 2026-10-31):

| Field | Holds |
|---|---|
| `action` | HBEP-8's type: `started`, `ready`, `degraded`, `failing`, `ending-soon` or `replaced` |
| `reason` | the CamelCase reason code alone |
| `type` | `Warning` for `degraded` and `failing`, `Normal` for the other four |
| `regarding` | the resource the event is about |
| `related` | optional: a second resource, such as the Instance behind a deployment's event |
| `eventTime` | the emitter's clock; it serialises in UTC |
| `note` | the human message, at most 1,024 bytes |
| `reportingController` | the emitting component's field manager, such as `hpcburst-lifecycle`: the actor of a planned stop |

The API refuses a new event without `action` or `reason`, with either over 128 characters, or with a note over 1,024 ([`pkg/apis/core/validation/events.go`, lines 35–38 and 172–186](https://github.com/k3s-io/kubernetes/blob/v1.36.5-k3s1/pkg/apis/core/validation/events.go#L35-L38), in the Kubernetes that k3s v1.36.5+k3s1 builds, [`go.mod`, line 64](https://github.com/k3s-io/k3s/blob/v1.36.5%2Bk3s1/go.mod#L64); accessed 2026-10-31). So each emitter cuts its note to 1,024 bytes.

**Reasons in alpha.** The reason strings are the contracts' (`api/v1alpha1/conditions.go`), plus those marked proposed:

| Type | Reasons | Regarding | Emitter |
|---|---|---|---|
| `started` | `AgentRegistered` (proposed in PR #88) | Instance | lifecycle controller |
| `ready` | `Ready` (proposed in PR #88); `ShapesFit` (proposed in PR #88) | Instance; ModelDeployment | lifecycle controller |
| `degraded` | `ReplacementLate`, `RestartLoop`, `VerificationFailed`, `IdentityUnresolved`, `CapacityDropped` | ModelDeployment | lifecycle controller |
| `degraded` | `CapacityDropped`, `CountsDrift` (proposed in PR #95) | ModelDeployment | front |
| `degraded` | `DrainWindowShort` | PlatformProfile | front |
| `degraded` | `PathProbeFailing` (proposed, HBEP-6) | Instance | front |
| `degraded` | `TunnelOrphaned` (proposed, HBEP-6) | the Allocation whose credential admitted the tunnel, else the `lifecycle` `Lease` | lifecycle controller |
| `degraded` | `ObservationGap` (proposed) | the emitter's own `Lease` | the lifecycle controller and the front, at start |
| `failing` | `WorkerRestart`, `InstanceRestart`, `RestartLoop`, `RegistrationTimeout`, `StartTimeout`, `TunnelNotConnected`, `QualificationFailed`, `VerificationFailed`, `AllocationFailed`, `AllocationLost`, `AllocationRefused`, `Requeued`, `SubmittedAsOther`, `AllocationEndedEarly` | Instance | lifecycle controller |
| `failing` | `HeartbeatMissed` (proposed) | the stale observer's resource (Observers) | the component that judges it |
| `failing` | `UsageWriteFailed` (proposed) | the front's `Lease` | front |
| `replaced` | `HandedOver` (proposed in PR #88) | Instance | lifecycle controller |
| `ending-soon` | `NoReplacementReady` (proposed in PR #88) | Instance | lifecycle controller |

- An event about a deployment caused by one Instance regards the ModelDeployment, with the Instance in `related`.
- The managed catalog controller's events take these types too. Their reasons are the condition reasons it writes: `failing` for `FileRefused` and `NoCredentialsFile` (`CredentialsInvalid`), `KeyRejected` (`CredentialsRejected`), `Expired` (`CredentialsExpiring`) and `ReadsFailing` (`Ready`); `degraded` for `ExpiresWithinADay`, `CatalogEntrySkipped` and `CatalogTruncated`; and `ready` for `FileRead`, `KeyAccepted`, `NotExpiring` and `CatalogRead` when a failing or degraded condition clears.
- The Provider controller's events are HBEP-4's diagnostics, untyped in alpha (open question 14).

**Repeating conditions.** An event for a condition that persists, such as `CatalogTruncated`, `CatalogEntrySkipped`, `UsageWriteFailed` or `HeartbeatMissed`, is emitted when the condition starts, and then at most once an hour while it lasts, never on every read. Merging cannot be relied on to bound such events (What the recorder merges and drops). The hourly gate is kept in memory, so a restart or a new leader emits it once more, which is "when it starts" for that process. For the managed catalog controller's Provider conditions, a condition that clears emits `ready` with the reason it writes (`FileRead`, `KeyAccepted`, `NotExpiring`, `CatalogRead`); the other repeating conditions show their end in status only. `CatalogEntrySkipped` is one condition per entry and skip category; at most 10 named skip events are emitted per Provider per hour, then one summary event per hour whose note names as many of the remaining entries as fit in its 1,024 bytes, ending with "…". The end of a skip or a truncation emits nothing, since neither sets a Provider condition (science-agent rulings, 31 Oct 2026, for #114).

**What the recorder merges and drops.** The lifecycle controller gets its recorder from controller-runtime, which builds client-go's `events.k8s.io` broadcaster ([`pkg/cluster/cluster.go`, lines 295–299](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.23.1/pkg/cluster/cluster.go#L295-L299); [`pkg/internal/recorder/recorder.go`, lines 169–199](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.23.1/pkg/internal/recorder/recorder.go#L169-L199); accessed 2026-10-31). The front builds the same broadcaster itself. In client-go v0.35.3 ([`tools/events/event_broadcaster.go`](https://github.com/kubernetes/client-go/blob/v0.35.3/tools/events/event_broadcaster.go), accessed 2026-10-31):
- **Merging.** Two events are isomorphic when type, `action`, `reason`, `reportingController`, `reportingInstance`, `regarding` and `related` are equal; the note is not part of the key (lines 58–66 and 297–310). `regarding` and `related` are full object references, `resourceVersion` included ([`tools/reference/ref.go`, lines 94–101](https://github.com/kubernetes/client-go/blob/v0.35.3/tools/reference/ref.go#L94-L101)), so an event about an object that was written in between is a new event, not part of a series. The controllers write status on most passes, so merging happens mainly for an event repeated on an unchanged object. A second isomorphic event within 6 minutes turns the first into a series with a count of 2. Later ones only raise the count in memory, and the note of every event after the first is lost (lines 170–206). The count reaches the API every 30 minutes, and once the series has been quiet for 6 minutes (lines 48–53, 126–161 and 392–394).
- **Dropping.** The queue holds 1,000 events, and a new event is dropped silently when it is full (line 115): the watch distributor's `select` has an empty `default` branch, and nothing is logged ([apimachinery `pkg/watch/mux.go`, lines 280–289](https://github.com/kubernetes/apimachinery/blob/v0.35.3/pkg/watch/mux.go#L280-L289)). An event the API refuses, a refused note or a full store's error included, is not retried. A transport error is retried up to 12 times, 10 s apart with jitter, and then dropped (lines 49, 208–228 and 250–282). An event whose object reference cannot be built is dropped before it is queued ([`tools/events/event_recorder.go`, lines 64–71](https://github.com/kubernetes/client-go/blob/v0.35.3/tools/events/event_recorder.go#L64-L71)).
- **Time.** `eventTime` is the recorder's wall clock at the call, not a controller's injected clock (`event_recorder.go`, line 65).
- **Namespace.** An event about a cluster-scoped object lands in `default` (`event_recorder.go`, lines 91–96).
- **The legacy recorder differs.** crossplane-runtime's recorder, which the Provider controller uses, writes through client-go's `tools/record` with no `action` ([`pkg/event/event.go`, lines 93–106](https://github.com/crossplane/crossplane-runtime/blob/v2.4.3/pkg/event/event.go#L93-L106)). That path also has a spam filter, 25 events per object and then one per 5 minutes, and it folds more than 10 similar events within 10 minutes into one "combined" event ([`tools/record/events_cache.go`, lines 36–48](https://github.com/kubernetes/client-go/blob/v0.35.3/tools/record/events_cache.go#L36-L48)). Accessed 2026-10-31.

So a test checks for at least one event of a type and reason on a resource, never for a count or for every note. The message a test must see is checked on the condition. The order of a recorder call against a status write is checked on one shared sequence: the in-memory recorder and a client interceptor on status writes number their calls from one counter. The API cannot show that order.

**Observers.** Each observer has a heartbeat and a staleness rule. The component that judges it does not depend on it:

| Observer | Heartbeat and staleness rule | Condition (the truth) | Event when stale | Judged and emitted by | Readings shown unknown |
|---|---|---|---|---|---|
| The Provider controller's queue read | `lastSuccessfulStatusRead` older than `ProviderReadyStaleness`, 5 min (`api/v1alpha1/provider_types.go`) | effective readiness: readers judge it from the age of `lastSuccessfulStatusRead` against the bound (`ProviderReady`). The stored `Ready` may still say True, since a stopped Provider controller writes nothing and no other component may write that condition (HBEP-4, Provider status, Writers) | `failing`, `HeartbeatMissed`, regarding the Provider | the lifecycle controller | the Provider's queue waits and phases: the controller skips it for new Allocations (HBEP-7, Reconcile loop, step 1) |
| The agent's reports | no report for `health.agentTimeout` (HBEP-4) | Instance `AgentUnreachable` True, `ReportsMissing` | `failing`, `HeartbeatMissed`, regarding the Instance | the lifecycle controller | every engine gauge of the Instance (Unknown samples) |
| The managed catalog read | `lastSuccessfulStatusRead` older than `ProviderReadyStaleness`, 5 min | effective readiness from the same age (`ProviderReady`); the stored `Ready` may still say True, as above | `failing`, `HeartbeatMissed`, regarding the Provider | the lifecycle controller | the listing's freshness: `lastCatalogRead` shows its age. A ModelDeployment's `Ready` keeps following the last successful read (HBEP-4, Test plan) |
| The front's end-to-end probe | proposed by this HBEP: the probe's last result older than three probe intervals (HBEP-6, `probeInterval`) | none: the front's own state, in its `/healthz` | `failing`, `HeartbeatMissed`, regarding the front's `Lease` | the front | each Instance's probe state shows as unknown in `/healthz`. Routing keeps HBEP-6's rule, which changes eligibility only on failed probes |
| The front itself | its `Lease` `front` in `hpcburst-system` (proposed by this HBEP), renewed every 10 s with `leaseDurationSeconds` 30; stale once `renewTime` is older than that duration | the `Lease` | `failing`, `HeartbeatMissed`, regarding the `Lease` | the lifecycle controller | none: the launcher's supervisor restarts a front that exits |
| The lifecycle controller | its leader `Lease` `lifecycle`, stale once `renewTime` is older than its `leaseDurationSeconds` | the `Lease` | `failing`, `HeartbeatMissed`, regarding the `Lease` | the front | none; HBEP-11's reconcile times show a controller that renews but reconciles nothing |

- **When it fires.** `HeartbeatMissed` fires when the heartbeat goes stale, and then at most once an hour while it stays stale (Repeating conditions). A Provider's return shows in its effective readiness, an Instance's in `AgentUnreachable`.
- **A Provider never read** (science-agent ruling, 1 Nov 2026, for #111). A Provider with no `lastSuccessfulStatusRead` at all raises `HeartbeatMissed` once its `creationTimestamp` is older than `ProviderReadyStaleness`, through the same hourly gate. An observer that never starts has stopped.
- **An absent front `Lease`** (science-agent ruling, 1 Nov 2026, for #111), when the front was not started or not configured, raises no event. `hpcburst status` shows the front as absent.
- **The lifecycle controller's `/readyz`** (science-agent ruling, 1 Nov 2026, for #111) fails until its caches have synced and it leads its `Lease`. Alpha runs one replica, so a controller that does not lead cannot act; a standby replica after alpha would need its own state. `/healthz` stays liveness. `hpcburst status` reads `/readyz` for the controller's health.
- **`--leader-elect=false`,** a development and test mode of the lifecycle controller, holds no `Lease`, so it emits no `ObservationGap` (science-agent ruling, 1 Nov 2026, for #111). Its `/readyz` then needs only synced caches. The `lifecycle` `Lease` is then absent, and the front raises no `HeartbeatMissed` for it, as for an absent front `Lease`.
- **Events about cluster-scoped objects land in `default`.** Providers and PlatformProfiles are cluster-scoped (`api/v1alpha1/provider_types.go` and `platformprofile_types.go`, `kubebuilder:resource:scope=Cluster`), so their events are written in the `default` namespace (What the recorder merges and drops). Each emitter's RBAC allows creating events there.
- **A probe failure is not a prober failure.** `PathProbeFailing` (`degraded`) says a path is bad (HBEP-6). `HeartbeatMissed` (`failing`) says the prober stopped.
- **The front's `Lease`** gives front-level events a namespaced resource to regard, and gives the front a heartbeat. It follows Kubernetes' node heartbeats, where each kubelet renews its own `Lease` (Prior art). The front's recorder scheme registers `coordination.k8s.io/v1`, since client-go drops an event whose object reference it cannot build.

**Gaps after a restart.** At start, the lifecycle controller and the front each read their `Lease`'s previous `renewTime` before taking the `Lease`. Once they hold it, each emits one `degraded` event with reason `ObservationGap`, regarding it, whose message names that `renewTime`, the gap's start. The Provider managers emit none in alpha (open question 14). A first start, with no previous `Lease`, emits none, and so does a lifecycle controller run with `--leader-elect=false`, which holds no `Lease` (Observers). Missed transitions are not replayed; status is the truth.

**Retention.** The launcher passes `--kube-apiserver-arg=event-ttl=168h` to k3s (HBEP-11, amendment of 31 Oct 2026). The facts behind it, all accessed 2026-10-31:
- kube-apiserver keeps events for `event-ttl`, 1 h by default ([`pkg/controlplane/apiserver/options/options.go`, lines 129 and 162–163](https://github.com/k3s-io/kubernetes/blob/v1.36.5-k3s1/pkg/controlplane/apiserver/options/options.go#L129)). The same TTL backs `events.k8s.io` ([`pkg/registry/events/rest/storage_events.go`, line 55](https://github.com/k3s-io/kubernetes/blob/v1.36.5-k3s1/pkg/registry/events/rest/storage_events.go#L55); [`pkg/controlplane/instance.go`, line 434](https://github.com/k3s-io/kubernetes/blob/v1.36.5-k3s1/pkg/controlplane/instance.go#L434)) and is set on every event's key ([`pkg/registry/core/event/storage/storage.go`, lines 42–44](https://github.com/k3s-io/kubernetes/blob/v1.36.5-k3s1/pkg/registry/core/event/storage/storage.go#L42-L44)).
- k3s passes `--kube-apiserver-arg` values to the apiserver ([`pkg/daemons/control/server.go`, line 277](https://github.com/k3s-io/k3s/blob/v1.36.5%2Bk3s1/pkg/daemons/control/server.go#L277); [`pkg/util/args.go`, line 27](https://github.com/k3s-io/k3s/blob/v1.36.5%2Bk3s1/pkg/util/args.go#L27)) and sets no `event-ttl` of its own: `server.go` has none. Its apiserver talks etcd3 to the datastore's endpoint (`server.go`, lines 340–345). SQLite, through kine, is the default datastore ([k3s datastore docs](https://docs.k3s.io/datastore)).
- kine v0.16.5, which k3s v1.36.5+k3s1 uses ([`go.mod`, line 111](https://github.com/k3s-io/k3s/blob/v1.36.5%2Bk3s1/go.mod#L111)), grants a lease whose ID is its TTL ([`pkg/server/lease.go`, lines 13–19](https://github.com/k3s-io/kine/blob/v0.16.5/pkg/server/lease.go#L13-L19)), and deletes each leased key once its lease has elapsed ([`pkg/ttl/ttl.go`, lines 1–8 and 143–150](https://github.com/k3s-io/kine/blob/v0.16.5/pkg/ttl/ttl.go#L1-L8)).
- **A restart restarts the clock.** kine computes each expiry as now plus the TTL when it first sees the key, and at its start it seeds every leased key that way (`ttl.go`, lines 103–117 and 168–180). So an event lives at least 7 days, and up to 7 days after the host's last restart.
- **Store growth** is small at alpha scale. Merging seldom applies (above), so each emission counts as one stored object, of at most about 2 kB with its 1,024-byte note. Assume 20 Instance lifecycles a day at about 6 events each, and 20 persisting conditions emitted once an hour (Repeating conditions): about 600 events a day, under 10 MB over 7 days. Even 5,000 a day is about 70 MB. These are estimates, not measurements.

### Metrics

**Exposition.** Both components serve Prometheus's text format on loopback, with no authentication and no credential in any label:
- the front at `GET /metrics` on its `/healthz` listener (the launcher's port base + 4);
- the lifecycle controller from controller-runtime's metrics server, bound on loopback at the port base + 12, a new offset of the launcher's block, proposed by this HBEP (`docs/development.md`, Runtime conventions). Its collectors register on controller-runtime's registry, so the controller's own work-queue metrics come with them.

The library is Prometheus client_golang, already in `go.mod` at v1.23.2 as an indirect dependency. Names follow Prometheus's conventions: one application prefix, base units in the name, `_total` on counters, and ratios from 0 to 1 named `_ratio` ([naming practices](https://prometheus.io/docs/practices/naming/); [source, lines 16–38 and 107–112](https://github.com/prometheus/docs/blob/bf977977d5f8d90923929577842d6d0c13bb5295/docs/practices/naming.md#L16-L38); accessed 2026-10-31).

**Engine gauges per Instance.** The lifecycle controller exports them at scrape time from each worker's latest health sample (HBEP-7, The allocation agent). A Prometheus collector builds the series from the samples it holds, so a series exists only while its value is known. Each field aggregates the engine's series across its `engine` labels: counters and counts are summed, and KV-cache usage, a fraction, takes the maximum. A value that is NaN or infinite, or missing from any one contributor, makes that field's aggregate unknown, never a partial total; for the HTTP status counts that is the whole map. The time-per-output-token `_sum` and `_count` are known together or not at all, since a histogram's sum is unusable without its count (science-agent rulings, 31 Oct 2026, for #110). vLLM v0.32.0 defines the source metrics in [`vllm/v1/metrics/loggers.py`](https://github.com/vllm-project/vllm/blob/v0.32.0/vllm/v1/metrics/loggers.py) (accessed 2026-10-31). Its counters are `prometheus_client` Counters (line 475), which expose a `_total` suffix, as `main`'s capture of a real vLLM 0.32.0 shows (`internal/agent/enginedouble/metrics.go`, lines 12–14 and 163–168):

| Goal | `hpcburst_` metric (type) | `agentapi.Health` field | vLLM v0.32.0 metric (`loggers.py` lines) |
|---|---|---|---|
| running requests | `hpcburst_instance_running_requests` (gauge) | `runningRequests` (on `main`) | `vllm:num_requests_running`, gauge (527–535) |
| waiting requests | `hpcburst_instance_waiting_requests` (gauge) | `waitingRequests` (on `main`) | `vllm:num_requests_waiting`, gauge (537–545) |
| time per output token | `hpcburst_instance_time_per_output_token_seconds` (summary: `_sum` and `_count`, no quantiles) | `timePerOutputTokenSecondsSum` and `timePerOutputTokenCount` (new) | `vllm:request_time_per_output_token_seconds`, histogram, its `_sum` and `_count` (950–958) |
| preemptions | `hpcburst_instance_preemptions_total` (counter) | `preemptions` (new) | `vllm:num_preemptions_total`, counter (738–744) |
| prefix-cache hit rate | `hpcburst_instance_prefix_cache_queried_tokens_total` and `hpcburst_instance_prefix_cache_hit_tokens_total` (counters); the rate is hits over queries | `prefixCacheQueriedTokens` and `prefixCacheHitTokens` (new) | `vllm:prefix_cache_queries_total` and `vllm:prefix_cache_hits_total`, counters in tokens (660–677) |
| KV-cache usage (recorded, not compared) | `hpcburst_instance_kv_cache_usage_ratio` (gauge, 0 to 1) | `kvCacheUsage` (on `main`) | `vllm:kv_cache_usage_perc`, gauge, "1 means 100 percent usage" (618–623) |
| generated tokens | `hpcburst_instance_generation_tokens_total` (counter) | `generatedTokens` (on `main`) | `vllm:generation_tokens_total`, counter (798–805) |

- **The new Health fields** are pointers, nil when unknown, like those on `main`. The agent reads the four new vLLM metrics in the same `/metrics` read as the rest. HBEP-7's list of what the report carries (The allocation agent) carries them (amended with this HBEP).
- **Time to first token** is the front's, from live traffic (Front metrics). vLLM's own `vllm:time_to_first_token_seconds` (930–938) is not carried in alpha.
- **Labels.** Each engine series carries `instance_uid`, `worker`, `generation` (the whole Instance's restart generation) and `worker_generation` (the worker's), from the report. A restart at any rung therefore starts a new series. Descriptive labels live on one series per Instance, `hpcburst_instance_info{instance_uid, namespace, instance, model_deployment, provider, gpu_model} 1`, joined on `instance_uid`.
- **Freshness.** `hpcburst_instance_metrics_read_timestamp_seconds{instance_uid, worker}` is `Health.metricsReadAt`, absent while there has been no read.

**Unknown samples.** Three ways to show an unknown sample in Prometheus were compared:
- **NaN.** The text format allows it ([exposition formats, line 96](https://github.com/prometheus/docs/blob/bf977977d5f8d90923929577842d6d0c13bb5295/docs/instrumenting/exposition_formats.md#L96)). But PromQL arithmetic follows IEEE 754, so one NaN turns a `sum` or `avg` over a deployment into NaN, while `min` and `max` skip it silently ([operators](https://prometheus.io/docs/prometheus/latest/querying/operators/); [source, lines 41–44 and 488–491](https://github.com/prometheus/prometheus/blob/ef78ff64267724c8ecd1a1283cad49b365a8b67f/docs/querying/operators.md#L41-L44)). It either poisons the aggregate or hides the gap. Rejected.
- **A stale marker.** Prometheus writes it itself when a series it scraped before is missing from a scrape; no exporter can write one ([staleness](https://prometheus.io/docs/prometheus/latest/querying/basics/#staleness); [source, lines 565–593](https://github.com/prometheus/prometheus/blob/ef78ff64267724c8ecd1a1283cad49b365a8b67f/docs/querying/basics.md#L565-L593)). From the exporter's side it is the absent series, and it carries no reason.
- **An absent series plus an explicit gauge.** Chosen. Aggregates skip the missing value without being poisoned, and the gauge says that a value is missing and why.

All accessed 2026-10-31. So:
- an engine series is absent while its value is unknown: while the agent is unreachable, while `Health.MetricsFresh` is false (HBEP-7, Only fresh gauges act), or while the engine does not report that metric;
- `hpcburst_instance_metrics_unknown{instance_uid, worker, metric, reason} 1` stands in for each absent one, with `metric` the `hpcburst_` name and `reason` one of `agent_unreachable`, `stale` or `not_reported`;
- the share of unknown samples is then a query: the count of `hpcburst_instance_metrics_unknown` series over the count of engine metrics expected per worker.
- **A collector whose read fails** exposes none of its series and sets `hpcburst_collector_success{collector} 0`, which is 1 after a successful read, following node_exporter's `node_scrape_collector_success`. So `/metrics` answers even then, and "none" is told from "could not read". In alpha the one collector that reads at scrape time is `accounting`, whose read is the Allocation list; the engine gauges come from the inventory and reports held in memory, which cannot fail, so they have no success gauge, and a missing or stale report shows per worker as below (science-agent rulings, 1 Nov 2026, for #111 and from PR #142's review);
- an Instance with no report yet, new or after the controller restarted (reports are held in memory, HBEP-7), has, for each worker of its desired state (in alpha one, `worker-0`), every engine metric as `hpcburst_instance_metrics_unknown` with reason `stale`, unless `agent_unreachable` applies; `not_reported` is kept for a metric the engine does not export (science-agent ruling, 1 Nov 2026, for #111);

**Which Instances have series** (science-agent ruling, 1 Nov 2026, for #111). The names are this HBEP's:
- every Instance that has not ended has its `hpcburst_instance_info` series;
- each worker in the Instance's latest report has, for each engine metric, either that metric's series or `hpcburst_instance_metrics_unknown` with its reason. The reason is `stale` while the worker has no fresh read, for example while it starts, unless `agent_unreachable` applies;
- an ended Instance has no series.

Prometheus's advice to export 0 for series that may exist ([instrumentation practices, "Avoid missing metrics"](https://prometheus.io/docs/practices/instrumentation/#avoid-missing-metrics); [source, lines 257–265](https://github.com/prometheus/docs/blob/bf977977d5f8d90923929577842d6d0c13bb5295/docs/practices/instrumentation.md#L257-L265); accessed 2026-10-31) is for real zeros. The framework follows it for counts of its own events, such as cuts and write failures, which start at 0. An unknown reading is never one of them.

**Front metrics.** PR #95 exposes no metrics, so every line here is missing there:

| Metric | Type and labels | Notes |
|---|---|---|
| `hpcburst_front_requests_total` | counter: `code` (the HTTP status the client got), `endpoint` (`chat`, `completions`, `responses`, `embeddings`, `models` or `other`), `provider`, `model_deployment` | one per usage record; the last two are empty for an error raised before a target is chosen. The client's `model` is not a label, since its values are unbounded |
| `hpcburst_front_cuts_total` | counter: `cut_by` (HBEP-5's values: `front_deadline`, `front_idle`, `front_headers`, `tunnel`, `upstream`, `lease_end`, `client`, `front_shutdown`), `provider`, `model_deployment` | |
| `hpcburst_front_pauses_total` | counter: `provider`, `model_deployment` | each pause of a target (HBEP-5, Load bound and cap) |
| `hpcburst_front_time_to_first_token_seconds` | histogram: `provider`, `model_deployment`, `endpoint`; buckets 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600 and 7200 s | from `firstTokenMs`; a request without a first token is not observed. The buckets reach past S2's first token after 1,100 s and the 2 h default `maxRequestDuration` (HBEP-5) |
| `hpcburst_usage_records_total` | counter | records written and synced |
| `hpcburst_usage_write_failures_total` | counter | records not written: a failed write or sync of their batch, or a failed counter-block reservation |
| `hpcburst_usage_dir_free_bytes` | gauge | free space of `usageDir`'s file system |

No client key ID is a label: per-key usage is in the records.

**Controller metrics.** Besides the engine gauges, the lifecycle controller exports the accounting gauges (Provider accounting).

### Usage records

**The store in alpha** is the front's sink on the control-plane host, with HBEP-5's fields (HBEP-5, Usage records). It is also the attribution log (HBEP-9).

**Durability.**
- `Write` returns only after the batch that holds its record has been written and synced, and returns that batch's error. So each record is synced before the request is reported done to the sink's caller. Records that end together share one write and one sync.
- The sink keeps writing after a failure, and each later batch reports its own result. An error is never sticky.
- When a new file is created, its directory is synced too, so the file survives a crash.
- **Gaps.** A clean stop syncs the next unused counter after the last one assigned (HBEP-5, Counter), so it leaves no gap of its own. The counters of a batch that failed were assigned, so they still show as a gap.

**Failures are never silent.** A failed write or sync of a batch, a full disk included, or a failed counter-block reservation:
- adds the records it lost to `hpcburst_usage_write_failures_total`. A failed reservation drops its record before it has a counter. When the new block end never reached the disk, it leaves no gap, and the failure counter is its only trace. When its persistence is uncertain, as when the directory sync after the rename fails, the new end may survive, and a restart then skips that block: a gap;
- raises a `failing` event, reason `UsageWriteFailed`, regarding the front's `Lease`, at the first failure after a success, and then at most once an hour while failures last (Repeating conditions);
- is stated in the front's readiness: `/healthz` carries `usage` with `state` (`ok` or `failing`), `lostRecords`, `since` and the last error, and `hpcburst status` reports the front as "degraded: usage writes failing" while it says `failing`: the exit code stays 0, the overall health stays `healthy`, and only the front's line says degraded. It is not a readiness failure (science-agent rulings, 31 Oct 2026, for #112). Until the front holds its `Lease`, a failure is counted, logged and shown in `/healthz`, and the `UsageWriteFailed` event follows once the `Lease` is held; it never falls back to regarding a ModelDeployment, which a usage failure is not about.

**The front keeps serving** while usage cannot be written (open question 13). `/healthz` keeps answering 200 for it, so a usage failure never makes anything restart the front. A failed write leaves the log at its last whole line, as PR #95 does.

**Rotation keeps every file.**
- Records go to `usage-<YYYY-MM-DD>.jsonl` in `usageDir` (a name proposed by this HBEP), named by the UTC day of the write. The first record after midnight UTC opens the next day's file. A closed file is never written again, except by the repair below.
- Nothing in the framework deletes a file, and the only rewrite is the repair at open: it cuts a partial last line off the newest file, whatever its day, and touches no other. Files are kept for the grant's life. The counter runs across files, so readers order records by it.
- A legacy `usage.jsonl`, written before rotation, is the newest file while no daily file exists, so the repair applies to it; after that it is never written again.

**Sizing.**
- A record is about 0.7 to 0.9 kB: 725 bytes on average for 20,000 synthetic records with realistic name lengths, and 853 bytes for one with long names (measured 31 Oct 2026 on synthetic data). gzip shrinks the synthetic records about 8 times.
- No source gives alpha's request rate. The planning figure is an assumption, 100,000 requests a day, for one user (HBEP-11, open question 8):

| Requests per day | Per day | Per 30 days | Per 365 days |
|---|---|---|---|
| 10,000 | 8.5 MB | 0.26 GB | 3.1 GB |
| 100,000 (planning) | 85 MB | 2.6 GB | 31 GB |
| 1,000,000 | 850 MB | 26 GB | 310 GB |

- The alpha host's home file system is 98 GB. It had 37 GB free at 20:35 UTC on 31 Oct 2026, at 61% used, after reaching 99% earlier the same day (L-12.2).
- At the planning rate a year of records fits in that space. At ten times the rate it fills in about six weeks. `hpcburst_usage_dir_free_bytes` shows the trend.
- **Not needed for alpha:** compressing closed files, or moving them off the host. Both come with the durable log in beta.

### Provider accounting

Alpha counts **allocated** time, from the scheduler's accounting: elapsed time times the GPUs or CPUs the scheduler allocated. Every number is labelled "allocated, not billed". The billing formula is open (blocks beta).

- **What HBEP-4's plugin reads.** One accounting read per execution, after it is first seen started, retried on failure, and none at delete (HBEP-4, The Slurm dialect, Accounting). Its fields:
  - `tres.allocated`: the entry of type `cpu`, and the entry of type `gres` named `gpu`, from that read;
  - for the final state: `start_time` and `end_time` when the queue listing gives it, else accounting's `time.elapsed`, in seconds;
  - every record `-D` returns for the job, one per execution, to see whether it was requeued.
  - S7 captured `time.elapsed` and the `cpu` entry of `tres.allocated` from a real CPU job (`spikes/s7/slurm-json-real.md`). The `gres`/`gpu` entry's shape is the fake Slurm's (`internal/slurm/fakeslurm`), and is verified on the first GPU job (Graduation criteria).
  - The same list carries a `billing` entry, the site's own weight. It is not read in alpha.
- **Where the numbers live.**
  - **A live Allocation's status:** `status.atProvider.accounting` (proposed names) holds `allocatedCPUs`, `allocatedGPUs`, `elapsedSeconds` and `readTime`. While the job runs, elapsed time runs from the scheduler's start time to `readTime`; once it has ended, it is the queue listing's `end_time` minus `start_time` when the final state came from the queue listing, and otherwise accounting's `time.elapsed`, for its one execution. **A requeued job's hours are unknown:** a job with more than one execution, a `restart_cnt` above 0, or `Requeued` already seen in status. The plugin submits `--no-requeue`, but a site setting such as Slurm's `ForceRequeueOnFail` can still requeue a job after a launch failure; any detected requeue already fails the Allocation (HBEP-4), so per-execution sums are not needed in alpha (science-agent ruling, 31 Oct 2026, for #113). On data_parser v0.0.40, which has no `restart_cnt`, a reply that shows only the last execution reveals a requeue only through `Requeued` already in status. The status's accounting is written only when the allocated counts change, or at most once a minute, not on every poll. GPU-hours are `allocatedGPUs × elapsedSeconds / 3600`, and CPU-hours likewise.
  - **A pending Allocation** has no `accounting`: its hours are unknown, never 0.
  - **The kept record.** When the Allocation's last job has ended, and before its finalizer is removed (HBEP-4: the finalizer stays while any job with the tag is live), the Provider controller writes one record that outlives the Allocation: a `ControllerRevision` named `acct-<Allocation UID>` in the Allocation's namespace, with no owner reference (HBEP-11, Records outlive deletion). Its data holds the Provider, the partition, the tag, the job IDs, `allocatedCPUs`, `allocatedGPUs`, `elapsedSeconds`, the start and end times, and `basis: allocated`. It is created once, if absent, and never rewritten. The record step makes no extra read at delete: it uses the deleting pass's reads. Once the tag lookup has succeeded and shows no live job, the finalizer waits for the recorded job's final accounting read for at most `lostGrace` (HBEP-4), whether that read is missing or keeps failing, then writes the record with what is known, each missing field marked unknown. A failed tag lookup or `Observe` keeps the finalizer, since a failed read is unknown, never absence, and a tagged job may still be live (HBEP-4, Delete). So the record never blocks deletion indefinitely, and needs no scheduler call at delete. A `Lost` job is recorded with its elapsed time unknown.
  - **Metrics:** the lifecycle controller exports `hpcburst_allocation_allocated_gpu_seconds{provider, namespace, allocation}` and `hpcburst_allocation_allocated_cpu_seconds{provider, namespace, allocation}` (gauges, with the help text "allocated, not billed") for live Allocations, absent while unknown. Seconds are Prometheus's base unit; people read hours.
- A researcher's month is the kept records plus the live Allocations, read from the API (open question 16).

### Code that differs

What the code must change to match this HBEP, as read on 31 Oct 2026. Items marked done are on `main`:
- **PR #88, the lifecycle controller:** in its fix round: `ending-soon` emitted as `Normal`; `CapacityDropped` regarding the ModelDeployment, with the Instance in `related` (a `related` parameter on `Recorder`); the `reasons.go` comment; notes cut to 1,024 bytes. After #88 (#111): the observer events (`AgentUnreachable`, `HeartbeatMissed`, `ObservationGap`) and the metrics server at base + 12, with the 1 Nov 2026 rulings: a never-read Provider judged by its `creationTimestamp`, no event for an absent front `Lease`, no `ObservationGap` under `--leader-elect=false`, and the series of every Instance not ended; also, from PR #142, `hpcburst_collector_success{collector="accounting"}` in place of the invalid metric a failed Allocation list emits today, and `stale` for each desired worker of an Instance with no report yet. The controller's `/readyz` already needs synced caches and leadership on `main` (`internal/lifecycle/manager.go`, `readiness`). In #90's wiring: `providerctl.Options.Recorder` set, since it defaults to a no-op recorder.
- **The front:** done in PR #95: the reason is the code alone; `coordination.k8s.io/v1` is registered; the usage sink syncs before it returns and its failures are not sticky. Still on `main`, fixed in PR #117 (#112): notes are not cut to 1,024 bytes, `UsageWriteFailed` regards a ModelDeployment, and there is no `/metrics`, no daily file, no `/healthz` `usage` block and no `front` `Lease`. In #90's front part: HBEP-6's probe and `PathProbeFailing`.
- **The launcher:** done in PR #134 (#64's front supervision): `hpcburst status` reads the front's `/healthz` and shows "degraded: usage writes failing". Still to come, in no PR yet: reading the lifecycle controller's `/readyz` and showing an absent front.
- **The managed catalog controller:** done in PR #116 (#114): HBEP-8's types, the `ready` events, and the hourly gate with the skip cap. The hourly summary's note (`SkippedSummaryNote`, `pkg/provider/managedstatus.go`) carries a count only; naming the remaining entries comes with its next change.
- **`agentapi.Health`:** done in PR #115 (#110): the four new fields, the sum and maximum, the non-finite rule and the time-per-output-token pair. Not yet built: an engine that lacks a series entirely is not detected, so a missing contributor still gives a partial total (`internal/agent/engine/metrics.go`). No effect in alpha, which runs one engine per Instance (HBEP-7).
- **The Slurm plugin:** in PR #118 (#113), at its fix round: `tres.allocated`, the accounting read and the `acct-<UID>` record; a requeued job's hours unknown, not summed; the status write throttled; and the delete rule above (a failed tag lookup keeps the finalizer).
- **Contracts:** a `LeaseFront` constant next to `LeaseLifecycle`, in the next contracts PR.

### Test plan

Tests use the fake Slurm of spike S4, the engine double behind a real agent (`internal/agent/enginedouble`), and a real k3s for the event tests. Each event test checks for at least one event of the named type and reason on the named resource, since the recorder merges isomorphic events (Events). Messages are checked on conditions.

- **Event form.** Every event the lifecycle controller and the front emit has `action` set to one of the six types, `reason` set to the code alone, `type` Warning exactly for `degraded` and `failing`, and a note of at most 1,024 bytes. A note built from a long engine message is cut, and the event is stored.
- **Merging.** Emit the same event twice within a minute on an unchanged object: the API shows one event with a series count of 2. Emit it again after a write to the object: a second event appears. A test that asks for "at least one" passes in both.
- **Repeating conditions.** A condition that persists for three hours (a truncated catalog, a failing usage disk, a stale heartbeat), read every minute, gives at most four events, not one per read.
- **Retention.** On the launcher's k3s, an event created under a short `event-ttl` (a test setting) disappears after it, and the default launch passes `event-ttl=168h`. After a k3s restart, an event created before it is still present.
- **Stop an observer:** stop the Provider controller, then an agent's reports. The stopped controller's stored `Ready` may stay True; `ProviderReady` turns false by age. At least one `failing` event, reason `HeartbeatMissed`, appears regarding the Provider or the Instance within its staleness rule plus one reconcile. The readings it fed show unknown: the Provider is skipped, and the Instance's engine series are absent with `hpcburst_instance_metrics_unknown` set.
- **Batched heartbeats** against a fixed check tick, each within the staleness rule: no `HeartbeatMissed`.
- **Observers that never start.** A Provider created with no read at all raises at least one `HeartbeatMissed` once its `creationTimestamp` is older than `ProviderReadyStaleness`, and not before. With no front `Lease` in the namespace, no event regards the front, and `hpcburst status` shows the front as absent. A lifecycle controller run with `--leader-elect=false` creates no `Lease`, emits no `ObservationGap`, and the front raises no `HeartbeatMissed` for the absent `lifecycle` `Lease`.
- **Readiness.** The lifecycle controller's `/readyz` fails while a second copy holds the `lifecycle` `Lease`, and while its caches have not synced; its `/healthz` answers in both. `hpcburst status` reports the controller by its `/readyz`.
- **Series scope.** An Instance in `starting` has its info series, and each engine metric of `worker-0` is `hpcburst_instance_metrics_unknown` with `reason="stale"`, before its first report and until the first fresh read. Once the Instance ends, none of its series is exposed.
- **A failed Allocation list.** `/metrics` answers 200 with `hpcburst_collector_success{collector="accounting"} 0` and no accounting series; after the next successful list, 1 and the series again.
- **A restart of the lifecycle controller.** Until the agent's next report, each engine metric of `worker-0` is `hpcburst_instance_metrics_unknown` with reason `stale`.
- **An engine's metrics endpoint times out or returns nothing:** the series are absent and `hpcburst_instance_metrics_unknown` names the reason, never a 0.
- **An engine double that omits the prefix-cache counter lines:** those series are absent with reason `not_reported`, and the other series are present. A real vLLM v0.32.0 with prefix caching off still exports both counters, at 0, since it registers them unconditionally (`loggers.py`, lines 660–677): its series are present, and the hit rate is 0 over 0, with no value.
- **Restart an engine at each rung:** a new series starts under the new generation labels, and no counter series decreases.
- **End a job at its time limit:** the drain-start event fires, `replaced` with reason `HandedOver` when a replacement serves, or `degraded` with reason `CapacityDropped` when none does, with a note citing the hard end. The Instance leaves the inventory, and no endpoint stays listed. No new reason is involved.
- **Drain an instance on purpose, then crash another:** the drain's event names the lifecycle controller in `reportingController` and the operation in its reason (`HandedOver`); the crash is `failing`.
- **Add a deployment while observers run:** it is observed without restarting them.
- **Restart the lifecycle controller and the front:** each emits one `ObservationGap` naming its previous `renewTime`. A first start emits none.
- **Usage durability.** A `Write` returns only after its record is synced. Kill the front between records: the counter shows a gap no larger than one block, and no record that `Write` acknowledged is missing.
- **Usage failure.** A disk that refuses writes (a log file that fails, as PR #95's tests use): the front keeps serving, `hpcburst_usage_write_failures_total` rises by the records lost, `/healthz` says `failing` with status 200, and at least one `UsageWriteFailed` event appears. When the disk recovers, the next records are written, `Write` returns nil for them, and `/healthz` says `ok`. A reservation that fails before its rename adds to the failure counter and leaves no gap. One whose directory sync fails after the rename, followed by a restart, leaves a gap of at most one block. After a failed batch and a clean stop, the next start shows only the failed batch's counters as a gap.
- **Rotation.** Records across a UTC midnight go to two files, with the counter continuous across them; the closed file is never touched again. A front stopped with a partial last line in the previous day's file repairs that file, and only that one, at its next start; a legacy `usage.jsonl` with no daily file beside it is repaired the same way.
- **Front metrics.** Each request adds one to `hpcburst_front_requests_total` with its code; a cut adds to `hpcburst_front_cuts_total` with its `cut_by`; a stream without content events adds no time-to-first-token observation.
- **Accounting.** A pending Allocation has no `accounting`, and no gauge is exported for it. A running one's elapsed time follows the scheduler's start time. A requeued job's hours are unknown, on v0.0.40 and v0.0.42 alike, also when the reply shows only the last execution, and its record lists them as unknown. A running job polled ten times within a minute gets one status write. With the tag lookup succeeding and only the final accounting read failing, the record is written after `lostGrace` with unknowns, and the finalizer goes; with the lookup itself failing, the finalizer stays. Deleting an ended Allocation leaves `acct-<UID>` with its hours. A job whose final state came from the queue listing has the elapsed time `end_time − start_time`. When the final read does not come within `lostGrace`, the record is written with its elapsed time unknown, and the Allocation goes.
- **A long reply with few chunks:** time per output token is the engine's value or absent, never 0.

### Compatibility and upgrades

- **Versions.** The vocabulary of types and reasons, the metric names and the usage fields are versioned with the API, `v1alpha1`. Within it, reasons, metrics and usage fields may be added; renaming or removing one waits for a new API version.
- **Readers** treat an unknown reason as its type's generic case, and an event with no type (an empty `action`, as the Provider controller's) or an unknown type as "other", and keep working.
- **Usage files.** A file written before rotation, `usage.jsonl`, is the newest file while no daily file exists (Usage records, Rotation), and then stays as it is; the counter file continues. A record gains fields only by addition, so an earlier reader skips the new ones.
- **The event TTL** applies to events created after the flag is set. Events created before keep the TTL they were stored with.
- LLMLab exposes no metrics, so there is no client contract to keep for them. Usage reporting inside responses follows HBEP-5.

### Security

- Usage records hold a client key's ID, never the key, and counts, never prompts or responses. The files are mode 600 in a directory of mode 700, read by the operator on the host. A client key reading its own usage is not needed for alpha.
- Events and metrics never carry credentials, keys or prompts. Metrics are served on loopback only, with no client key ID in any label.
- Provider job IDs are Instance metadata (HBEP-3), and appear in accounting records, visible only to identities that can read the namespace's records (HBEP-11).
- Events are readable by the identities that HBEP-11's RBAC lets list events in the namespace.

### Graduation criteria

- **Alpha:**
  - every event in the Reasons table that alpha's code paths raise exists, with the form above, for Instances and ModelDeployments on one batch provider (Helios or Athena), and the front's events for LLMLab-served requests;
  - the engine gauges, with unknowns shown as above, for one deployment on each alpha partition, and the front's metrics for both kinds of target;
  - the Test plan passes, and the kept accounting record's GPU entry is checked once on a real GPU job on each alpha partition;
  - the launcher sets `event-ttl=168h`, and a usage file rotates across a UTC midnight.
- **Beta:**
  - the durable log for events and usage, with HBEP-10's subscriptions (open question 11);
  - typed Allocation events (open question 14);
  - the billing formula and per-grant totals (open questions 15 and 16);
  - OpenTelemetry's names (open question 1);
  - the observer tests pass on two batch providers.
- **Stable:** the Test plan runs in CI, and a deployment's usage, events and accounting have been read for a whole month with no hand-run step.

## Operational readiness

1. **Working:** every observer's heartbeat is within its rule: each Provider's `lastSuccessfulStatusRead`, each Instance's last report, the front's probe ages, and the `front` and `lifecycle` `Lease`s. Both `/metrics` endpoints answer, with every `hpcburst_collector_success` at 1. End to end, a request through the front appears in the usage file with its deployment and Instance, and in `hpcburst_front_requests_total`. A `HeartbeatMissed` event, an endpoint that does not answer, or a request missing from usage says it is not working.
2. **Restart:** an observer re-reads the inventory and resumes, and the lifecycle controller and the front emit one `ObservationGap` each. Engine counters start new series under new generation labels. The usage counter resumes at the persisted block end. A crash, a failed batch or a reservation of uncertain persistence leaves a gap; a clean stop does not. Events in the store survive a k3s restart, with their TTL restarted.
3. **Silent failure:**
   - a collector that cannot read: `hpcburst_collector_success` 0, with none of its series;
   - an observer that runs but reads nothing: its readings turn unknown, and the share of unknown samples is a query on `hpcburst_instance_metrics_unknown`. A display that draws an absent series as zero would hide this, so displays draw it distinctly;
   - a recorder that drops events: through a full queue, silently, or through a refused note or a store error, which it logs. The conditions still change, since status is the truth;
   - a usage write that fails: the failure counter and `/healthz`.
4. **Provider limits:** accounting adds one `sacct -j` read per execution, after the job is first seen started (HBEP-4, Poll rate); its other fields come from existing reads, which are paced per platform profile (HBEP-4).
5. **Capacity:**
   - a full KV cache: waiting requests and preemptions rise, and those are the signals;
   - a full disk: usage writes fail loudly, and the SQLite store stops writes, so events may not be stored (Risks; HBEP-11, Operational readiness 5; L-17.2). Sizing is in Usage records;
   - the event store holds an estimated 10 MB or less over 7 days at alpha scale (Retention).
6. **Idle cost:** no new process. Two loopback `/metrics` endpoints, built only when scraped; one `Lease` renewal every 10 s; heartbeat checks inside existing reconciles; one sync per batch of finished requests. Nothing runs on providers.
7. **Rollback:** an earlier binary ignores the new status fields and emits fewer events; the usage files and accounting records stay. Removing the launcher's flag returns new events to 1 h. Serving does not depend on any metric or event.

## Drawbacks

- Every reader must handle three outcomes, not two.
- Per-instance series and per-request records add storage that a single-user setup must carry.
- Alpha's events can be merged or dropped, so an agent that wants every transition must re-read status until HBEP-10's log exists.

## Alternatives

- **Each engine's own dashboard per site.** The metrics exist, but without common names and labels they cannot be compared, and they cover neither the front nor the provider.
- **Free-text logs as the event source.** Cheap to write, but tools that parse them misread one event as another.
- **Rebuilding usage from logs afterwards.** Possible, but slow, and only as complete as the logs.
- **KV-cache utilisation as the load measure.** Simple, but it does not compare across attention implementations.
- **JetStream as alpha's event and usage log.** S8 supports it, but it adds a process, and subscriptions and cursors that HBEP-10 has not designed. Kept for beta.
- **NaN for an unknown sample.** One value per series and no extra gauge, but it poisons or hides aggregates (Design details, Unknown samples).
- **Scraping engines through the tunnel.** It would show the engine's own series, but HBEP-6's tunnel carries only routed requests, and a gone Instance would read as a stale series.

## Prior art and reuse

Events and accounting replace www-2027's exit-to-wake watchers, its append-only live archive, and its grant forecast from scheduler accounting, which counted the running jobs that the center's tool missed.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| Kubernetes Events and client-go's events recorder | [kubernetes/kubernetes](https://github.com/kubernetes/kubernetes); [kubernetes/client-go](https://github.com/kubernetes/client-go) v0.35.3, through controller-runtime v0.23.1 | Apache-2.0 | Dependency: alpha's best-effort event stream (`events.k8s.io/v1`) |
| k3s, with kine and SQLite | [k3s-io/k3s](https://github.com/k3s-io/k3s) v1.36.5+k3s1; [k3s-io/kine](https://github.com/k3s-io/kine) v0.16.5 | Apache-2.0 | Dependency (HBEP-11): stores the events and expires them after `event-ttl` |
| Kubernetes `Lease` | `coordination.k8s.io/v1`, in [kubernetes/kubernetes](https://github.com/kubernetes/kubernetes) | Apache-2.0 | Dependency: the front's heartbeat, and the controllers' leader `Lease`s read as theirs |
| Prometheus client_golang | [prometheus/client_golang](https://github.com/prometheus/client_golang) v1.23.2 | Apache-2.0 | Dependency: the `/metrics` exposition of the front and the lifecycle controller |
| vLLM metric names | [vllm-project/vllm](https://github.com/vllm-project/vllm) v0.32.0 | Apache-2.0 | Names read by the agent and mapped to `hpcburst_` names; no code |
| NATS JetStream | [nats-io/nats-server](https://github.com/nats-io/nats-server) | Apache-2.0 | Beta, supported by spike S8 (PR #28) except for power loss, which was not tested: the durable log for events and usage records, with `sync_interval: always`, with HBEP-10 |
| CloudEvents 1.0.2 and sdk-go | [cloudevents/sdk-go](https://github.com/cloudevents/sdk-go); [spec](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md) | sdk-go Apache-2.0; spec Apache-2.0 | Beta candidate, no spike assigned: the envelope in HBEP-10's log. Not used in alpha |
| OTel GenAI semantic conventions | [semantic-conventions-genai](https://github.com/open-telemetry/semantic-conventions-genai) | Apache-2.0 | Beta candidate (open question 1): names for front metrics, pinned to a commit; no code |
| llm-d-inference-sim | [llm-d/llm-d-inference-sim](https://github.com/llm-d/llm-d-inference-sim) | Apache-2.0 | Not used in alpha: the repository's own engine double, built from a vLLM 0.32.0 capture, covers the metric tests |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| A component's heartbeat as a `Lease` it renews, judged by another | Kubernetes' node heartbeats, KEP-589 ([Efficient Node Heartbeats](https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/589-efficient-node-heartbeats), accessed 2026-10-31) | The front's `Lease`, and the controllers' `Lease`s read as heartbeats (Observers) |
| Prometheus naming: one prefix, base units, `_total`, `_ratio` | [Prometheus naming practices](https://prometheus.io/docs/practices/naming/), accessed 2026-10-31 | Every `hpcburst_` name |
| An info series per object, joined on an ID, so descriptive labels do not multiply series | kube-state-metrics' `*_info` series ([kubernetes/kube-state-metrics](https://github.com/kubernetes/kube-state-metrics)) | `hpcburst_instance_info` |
| Records that outlive their object, one per fact, immutable | HBEP-11's identity and verification records (`ControllerRevision`) | The accounting record `acct-<UID>` |
| Detect a gap when a cursor falls below the stream's first sequence | Synadia's JetStream check ([nats-unprocessed-critical](https://www.synadia.com/insights/checks/nats-unprocessed-critical)) | Beta: HBEP-10's gap marker. Alpha's `ObservationGap` covers an observer's own missed transitions |

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| New instances served unannounced, and parked work left GPUs idle. | L-10.1 | Typed lifecycle events, including `started` and `ready`. |
| Watchers died silently with the session that started them. | L-10.2 | Every observer has a heartbeat, judged by another component, and runs under the control plane. |
| Untracked watchers woke nobody, and duplicates doubled alerts. | L-10.3 | Observers run under the control plane, never in an agent's session. |
| Pipes that never delivered made alerts late or never. | L-10.4 | Events are best-effort by design, and status is the truth a reader re-reads. |
| Empty samples were shown as zero, and reads hit the wrong target. | L-10.5 | An unknown reading is an absent series with an explicit unknown gauge, never zero. |
| Checks that could not see their input reported OK. | L-10.6 | A check that cannot read its input reports unknown, never pass. |
| Counters reset by a restart invalidated before/after comparisons. | L-10.7 | Each restart starts new series under new generation labels. |
| A batched heartbeat read against a fixed tick gave false "down" alerts. | L-10.8 | Each down interval spans several heartbeat periods, read from the heartbeat's own time stamp. |
| Monitors read their targets once at start. | L-10.9 | Observation follows the inventory as it changes. |
| A job ended by its time limit stayed listed. | L-10.10 | The Instance's drain-start event cites the hard end, and it leaves the inventory. |
| Other tools misread free-text state lines. | L-10.14 | Typed events; tools act on `action` and `reason`, never on the note. |
| A deliberate restart was counted as a crash. | L-10.16 | A planned stop names its actor in `reportingController` and its operation in its reason. |
| Home file systems filled on shared hosts. | L-12.2 | Usage records are sized against the host's disk, and its free space is a metric. |
| A write refused for a full store can still take effect. | L-17.2 | A full disk's usage failure is signalled by the counter and readiness, not by an event alone. |
| KV-cache percentages meant different loads on different deployments. | L-5.6 | KV-cache use is recorded but not compared. |
| A routing change was judged on the fingerprint alone. | L-4.5 | Per-session throughput, prefix-cache hit rate and time to first token, before and after. |
| Tokens served and GPU-hours were rebuilt from logs afterwards. | L-13.6 | Per-request usage records, and allocated hours from the scheduler's accounting while serving. |
| The center's tool counted only finished jobs. | L-13.1 | Accounting from the scheduler, with running and pending Allocations. |
| Billing formulas differ per cluster. | L-13.2 | The formula is platform-profile data, open for beta; alpha counts allocated time only. |
| A forecast counted pending jobs with an unknown start as starting now. | L-13.3 | A pending Allocation's hours are unknown. |
| Spend counted by a record's modification time inflated an estimate. | L-13.7 | Usage records carry the request's own time. |
| A managed gateway's periodic stalls showed only in timings. | L-6.2 | The front's time to first token, per provider. |

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #8.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).
- 2026-10-30: Prior art and reuse filled from the state-of-the-art evaluation.
- 2026-10-30: spike S8 (PR #28) findings carried in.
- 2026-10-30: aligned with HBEP-11 becoming implementable: the API host row.
- 2026-10-30: alpha's thresholds for failing Instances and its "ending soon" timing noted from HBEP-7.
- 2026-10-30: the front's alpha usage sink and counts noted from HBEP-5.
- 2026-10-31: noted from HBEP-6 becoming implementable: in alpha the tunnel carries only what HBEP-5 routes to an Instance, its listed endpoints and its passthrough of every other JSON `POST` under `/v1/`, and engine metrics reach the controller in the agent's reports, not by a scrape through the tunnel (open question on how metrics leave the job). HBEP-6 proposes the event reasons `PathProbeFailing` (the front's probe, type `degraded`) and `TunnelOrphaned`; it records no tunnel heartbeat metric, since its frps plugin does not see pings.
- 2026-10-31: implementable. The science agent's decisions of 31 Oct 2026 written for alpha: Kubernetes Events with the type in `action`; client-go's merging and dropping; the observers and their heartbeats, with `HeartbeatMissed` and `ObservationGap`; a 7-day event TTL; Prometheus exposition under `hpcburst_` names, with vLLM v0.32.0's names mapped; unknown samples as absent series with an explicit gauge; the usage sink's durability, failures and rotation; and allocated accounting. Proposed by this HBEP for the review round: the front's `Lease` and the probe's staleness rule, the names `ObservationGap` and `UsageWriteFailed`, the metrics port, the daily file name, untyped Allocation events in alpha, and the kept accounting record. Every open question decided, decided for alpha, not needed for alpha, or open for beta. Written against the code of PRs #88 and #95 and of `main`. HBEP-5, HBEP-7 and HBEP-11 amended with it.
- 2026-10-31: fixes from the review round of PR #105 (Astra and a fresh Opus): effective readiness judged by age, since a stopped Provider controller writes no condition; the counter gaps a failed batch or an uncertain reservation leaves; silent queue drops; the merge key's `resourceVersion` and a rate rule for repeating events; the accounting record's elapsed time and its `lostGrace` bound; `ObservationGap` from the lifecycle controller and the front only; the time-limit test; the repair of the newest file; and Code that differs. HBEP-4 amended, and HBEP-7's and HBEP-11's amendments extended.
- 2026-10-31: amendment from building HBEP-8's follow-ups (#110, #112, #113, #114): engine aggregation, the skip cap, usage state and the `Lease` event, requeued jobs' hours unknown, and the accounting read.
- 2026-11-01: amendment from the questions of #111's builder: an absent front `Lease`, a Provider never read, the engine series' scope, `--leader-elect=false`, and the controller's `/readyz`.
- 2026-11-01: `hpcburst_collector_success`, the `namespace` label on the accounting gauges, and `stale` for each desired worker of an Instance with no report yet (rulings for #111 and #139; fixed in PR #142's review).

## Amendments

- **2026-10-31: rulings from building the follow-ups** (science-agent rulings while #110, #112, #113 and #114 were built).
  - **Engine gauges:** counts summed across engines, KV-cache usage the maximum; one non-finite or missing contributor makes the aggregate unknown; the TPOT sum and count both or neither.
  - **Repeating events:** the in-memory gate, `ready` on credential recoveries, and `CatalogEntrySkipped` capped at 10 named events an hour plus a summary.
  - **Usage state:** `/healthz` reports `lostRecords`; `hpcburst status` says "degraded: usage writes failing" (before, "unhealthy"); `UsageWriteFailed` waits for the front's `Lease`, with no ModelDeployment fallback.
  - **Accounting:** a requeued job's hours are unknown (before, its executions were summed); one accounting read per execution, and no extra read at delete (before, "no extra scheduler call"); after a successful tag lookup, a persistent read error is bounded by `lostGrace` like a missing one, while a failed lookup keeps the finalizer; the status is written on a count change or once a minute.
  - **Merge order:** this amendment merges before PR #117 and PR #118. PR #115 and PR #116 merged first, built on the same rulings.
- **2026-11-01: rulings for #111** (Events, Observers and Gaps after a restart; Metrics; Code that differs; Test plan). Science-agent rulings of 1 Nov 2026, answering the five questions of the builder of #111 (controller metrics and observer events). Names are this HBEP's; `HeartbeatMissed`, `hpcburst_instance_info` and `hpcburst_instance_metrics_unknown` are not yet on `main` at 8fe9aca, while `/readyz`, `--leader-elect` and `ProviderReadyStaleness` are.
  - **An absent front `Lease`** (the front not started or not configured) raises no event, and `hpcburst status` shows the front as absent. Before, the text did not say whether a missing `Lease` was stale.
  - **A Provider never read** raises `HeartbeatMissed` once its `creationTimestamp` is older than `ProviderReadyStaleness`, through the same hourly gate: an observer that never starts has stopped. Before, the rule judged only the age of a `lastSuccessfulStatusRead` that exists.
  - **Engine series scope.** Every Instance that has not ended has its info series; each worker in its latest report has, per metric, the series or the unknown gauge, `stale` while it has no fresh read; an ended Instance has none. Before, the text did not say which Instances and workers are exposed.
  - **`--leader-elect=false`** holds no `Lease`, so it emits no `ObservationGap`, and the front raises no `HeartbeatMissed` for the absent `lifecycle` `Lease`, as for an absent front `Lease` (review round of PR #127).
  - **The controller's `/readyz`** fails until its caches have synced and it leads its `Lease`; alpha's single replica cannot act without it. `/healthz` stays liveness, and `hpcburst status` reads `/readyz` for the controller's health. Before, the text named neither endpoint for the controller.
- **2026-11-01: collector failures and labels** (science-agent rulings for #111, while #139 was reviewed).
  - **`hpcburst_collector_success{collector}`** is new: 0 when a collector's read failed, 1 after a success, so a failed Allocation list no longer makes `/metrics` answer 500 and is told from "no Allocations".
  - **The accounting gauges** gain a `namespace` label, since Allocations in two namespaces can share a name; before, `{provider, allocation}`.
  - **`stale`** covers each desired worker of an Instance with no report yet, new or after a controller restart; `not_reported` is only for a metric the engine does not export.
  - **Only `accounting` has a success gauge in alpha;** the engine gauges come from memory and cannot fail to read.
  - **Merge order:** this amendment merges before PR #139 and PR #132.
