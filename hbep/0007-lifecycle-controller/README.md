# HBEP-7: Lifecycle controller

## Summary

The lifecycle controller keeps each ModelDeployment's desired state true: the requested number of serving instances, on the allowed providers, until the deployment's end. It provisions Allocations with hedging and over-allocation. It qualifies each Instance, and verifies and warms it with real-shape requests before it takes traffic. It checks health and restarts failures, plans make-before-break handovers from lease ends, and never releases a serving instance before its replacement serves while the deployment keeps its desired capacity. An Allocation left with no Instance follows its retention policy, and an operator can repurpose it to another ModelDeployment. Terms follow HBEP-3.

## Motivation

This HBEP serves stage 2 of the user story (HBEP-2), getting resources on HPC, and stage 3, keeping instances running through failures, restarts, time limits and handovers. Releases also serve stage 7.

Problems:
- Follow-ons pinned to the same node pended for days. Dependencies counted from the parent's start, not its end. Cancelling a parent started its pending follow-on at once.
- A slot freed before the replacement ran went to another user within seconds, and the replacement then waited days.
- One handover design cancelled the predecessor as soon as the follow-on answered, and gave back a large share of every lease.
- Users cannot extend time limits, and centers may not act at weekends. Serving jobs reached their end with no cover.
- Start estimates swung by hours, and actual starts often came earlier.
- An unwarmed follow-on tripped a client's load guard on its first requests. JIT compiles on unseen shapes stalled or killed engines, and warming up busy workers evicts live requests.
- Failing servers were waited out for hours instead of restarted.
- A multi-node rank 0 exited cleanly after an engine time-out while the step waited on the other node, so nothing restarted.
- A follow-on aborted itself seconds after start, on a failed status query of its ended predecessor. Stacks were down for over an hour unnoticed.
- Restart helpers written for one topology stopped their own workers on another. A router hung in graceful shutdown.
- Identity checks hashed the serve scripts, so a live script could not be edited, and an older job's restart picked up a newer script.
- Conversions of idle stacks between models were done by hand with per-job files, and job names stopped matching what they served.
- Editing a pending job's resources left it unschedulable for hours.
- A deliberate restart that was not recorded beforehand was counted as a crash.
- An interrupted multi-cluster release had already cancelled jobs on two clusters before it was reported as refused.

### Goals

- A declared deployment stays served until its end, with no hand-run handovers.
- Every instance passes verification and a real-shape warm-up before it takes traffic.
- Over-allocation keeps only Instances that pass qualification, and the best of those.
- While a deployment keeps its desired capacity, no serving instance is released before its replacement serves.
- Failing instances restart promptly, in the topology their spec declares.
- A controller restart or an interrupted operation is safe: actions are idempotent, and partial failures are reported item by item.
- Repurposing an Allocation to another ModelDeployment is one declared operation.
- An Allocation with no Instance is kept or released as its retention policy says, not by hand.

### Non-goals

- Raising priority at a center. The framework does not assume centers push our jobs; its lever is hedging.
- Deciding what to serve, or which Allocations to repurpose. Users and agents set desired state and ask for repurposes (HBEP-10).
- Routing (HBEP-5) and paths (HBEP-6).
- Choosing serving settings. Those are the serving spec (HBEP-3) and platform profiles (HBEP-4).

## Proposal

### User stories

- A researcher declares `example-27b` with two instances on Helios or Athena until a given date. The controller submits early to both and keeps the first allocation to start. It cancels the pending loser, then verifies and warms the instance, and the front starts routing to it. Before that lease's drain starts, a replacement already serves.
- A researcher needs four instances of `example-27b`. The controller obtains six Allocations and qualifies each Instance with an identity check and a short benchmark. One node fails outright, and its Allocation is released. The controller keeps the best four; the one that passed but ranked fifth is kept as warm capacity, as its retention policy says.
- An instance stalls with requests in flight. The controller records the restart as an event, then restarts the instance within minutes.
- An agent's work on `example-8b` ends while its Allocations have hours left, and `example-27b` needs the same GPU type. The agent repurposes the Allocations. The controller drains each `example-8b` Instance, starts an `example-27b` Instance in the same Allocation, then verifies and warms it, and the front routes to it. No new job is queued.
- An experiment must finish by a deadline. Its retention policy keeps every Allocation until then. Allocations whose model is done wait as warm capacity, or are repurposed, instead of going back to the queue.
- The controller restarts in the middle of a handover. It re-reads desired state and lists its allocations by label, and it submits nothing twice.

### Notes, constraints and caveats

- These are decided:
  - Hedging, as HBEP-3 defines it: submit early, on several providers if needed; keep the first allocation to start; cancel the pending losers. The winner milestone, first to start, is provisional (Open question 1).
  - Over-allocation with qualification, as HBEP-3 defines them: obtain N+k Allocations, qualify each Instance with an identity check plus a short benchmark against what the platform profile says to expect, and keep the best N. The rest are released, or kept as warm capacity, per the retention policy. The gate also catches broken nodes, not only slow ones.
  - Retention policy: keep an Allocation until a date, a grant's end or a deadline; or release it when idle.
  - Repurposing: an operator may move an Allocation to another ModelDeployment when a model's work finishes. Conversions between models are this operation.
  - Replacement before release, as HBEP-3 scopes it: while a deployment keeps its desired capacity, never release a serving instance before its replacement serves. The cases are listed under Release in Design details.
  - A handover replaces the Instance with a new one, with its own metadata. The deployment ID stays.
  - On batch providers, continuity comes through follow-ons.
  - Hedging across providers is our own controller, built on the Kubernetes API (HBEP-11).
- **Prior art for over-allocation.** Farley et al. call the strategy placement gaming: launch extra instances up front and keep the strongest ("More for your money: exploiting performance heterogeneity in public clouds", ACM SoCC 2012). Ou et al. take a keep-good, discard-bad approach to EC2 instance heterogeneity ("Is the same instance type created equal? Exploiting heterogeneity of public clouds", IEEE Transactions on Cloud Computing, 2013).
- An Allocation outlives its Instances (HBEP-3). Ending an Instance and releasing its Allocation are separate steps: the Instance ends, and the Allocation then follows its retention policy.
- Every provider action goes through the provider interface (HBEP-4). Submission rules, per-user job limits, time-limit effects on queueing and time zones are profile data. Timestamps reach the controller in UTC.
- An allocation's account cannot change once it runs, so the controller picks the account at submission. Each provider has one account (HBEP-3), so picking the account is picking the provider.
- The drain window is defined in HBEP-5. The controller plans so that a replacement serves before the predecessor's drain starts; the drain rule under Handover says what happens when it does not.

### Risks and mitigations

- **The node is lost.** Releasing before the replacement serves gives the slot to another user. Mitigation: the decided rule, checked by a test. A retention policy can also keep an Allocation after its Instance ends.
- **Cancelling a loser starts its dependents.** Mitigation: cancel dependents first, or use no provider-side dependencies (Open question 3).
- **A restart submits twice.** Every submission carries a tag in a field the provider stores, and the controller finds its allocations by that tag, never by scanning names. The tag supports recovery but does not enforce uniqueness: a provider can accept two submissions with one tag, and a lost submission reply leaves the outcome unknown until the controller lists by tag. The submission protocol stays open (Open question 11).
- **The scheduler requeues a job.** Slurm can requeue a batch job and run its script again under the same job ID. That can leave stale registrations, verification results or replacement work from the first execution behind. Each platform profile records its requeue behaviour (HBEP-4); identity across executions is HBEP-3's open question 13. Candidate: submit with no-requeue and treat a requeue as a failure.
- **Overlap costs grant hours.** A replacement can start hours early, and both then run. Mitigation: an overlap policy (Open question 4).
- **Warm-up misleads.** A warm-up on short prompts leaves long-context shapes cold. Mitigation: warm-up shapes come from the deployment spec, and warm-up runs on the new instance before admission, never on live workers.
- **Verification checks a proxy.** The engine's fingerprint differs per site and is sometimes missing. Mitigation: verification recomputes the deployment ID from the settings the instance reports (HBEP-9).
- **Qualification ranks on noise.** A short benchmark can rank nodes by chance, for example while a shared filesystem is busy. Mitigation: each Instance is judged against the profile's expected performance, not only against the others; the benchmark's length is Open question 12.
- **Warm capacity is forgotten.** Kept Allocations spend grant hours with nothing on them. Mitigation: every retention policy ends at a date, a grant's end, a deadline or an idle limit, and the inventory lists kept Allocations (HBEP-10).
- **A repurpose inherits the old model's state.** Files staged for the old model, or another engine, are left in the Allocation. Mitigation: the new Instance passes the same verification as any other, against its own deployment's identity record (HBEP-9).
- **Restart loops.** Mitigation: backoff, then a new allocation, then a degraded deployment and an event (HBEP-8).
- **A poison request kills each restarted instance.** Mitigation: the front's retry guard (HBEP-5) and a crash-correlation event.
- **A restart runs a different spec.** An edited script reached an older job's restart. Mitigation (candidate): each instance runs from an immutable launch bundle for its deployment ID, and a restart reuses it.
- **Edits to pending jobs break them.** Mitigation: never edit a submitted allocation; cancel and resubmit.
- **Our own jobs compete with each other.** Hedges, over-allocations and follow-ons of different deployments compete for the same nodes and per-user limits. Mitigation: plan per provider, with its per-user limits from the profile.
- **The controller dies quietly.** Instances keep serving until their leases end, and handovers are missed. Mitigation: for each deployment, an event when its planned submission time passes with no submission.

### Open questions

Each question below has a proposed answer taken from operating LLM serving on Slurm systems for a deadline-driven experiment. "Still open" names what that experience did not settle. The owner confirms or changes each proposal.

1. **Hedge winner milestone (a proposed change to HBEP-3's definition).**
   - **Proposed:** the winner is the first Allocation whose Instance serves, not the first to start. Pending losers are cancelled only then. A loser that starts before the winner serves becomes an over-allocation candidate (question 12), not waste.
   - **Why:** start-up failures were common: broken environment builds, out-of-memory at start, collective-communication failures in new steps, scripts that killed their own stack seconds after start, engines dying minutes into use. A cancelled job's queue age is lost, and a new submission can wait days. Holding a pending loser costs no grant hours.
   - **Still open:** the measured start-up failure rate per platform. An accepted answer amends HBEP-3.
2. **Lead time and hedge width.**
   - **Proposed:** submit as early as possible: the replacement when its predecessor starts, and on every provider the deployment allows whenever the observed waits on the preferred one exceed the time left. Submissions are eligible at once, so they gain queue age.
   - **Proposed:** use observed waits, never start estimates, for planning. Request time limits that backfill (shorter than the partition maximum) when the profile shows long ones wait longer.
   - **Why:** start estimates swung by hours and went "unknown" on every scheduler pass. Jobs with limits up to about 2.5 days started within a minute while 3-day ones waited hours. Age counts only from eligibility, so a dependency-held job lost priority to everyone else.
   - **Still open:** the backfill limit per platform, from the profile.
3. **Follow-on form.**
   - **Proposed:** independent submissions without dependencies or node pins, by default. Same-node pins only on platforms whose profile shows they start reliably.
   - **Why:** jobs pinned to their predecessor's node waited for days on nodes other users had planned. Chained dependencies on jobs that could sit in the queue blocked the rest. "After N minutes" dependencies counted from the parent's start, not its end. Node and URL changes no longer matter, because Instances dial home and clients use names.
   - **Still open:** none beyond per-platform profile values.
4. **Overlap policy.**
   - **Proposed:** keep the predecessor until its drain starts. An early release happens only under a retention policy that asks for it.
   - **Why:** a design that cancelled the predecessor as soon as its follow-on answered gave back about 6 h of every 48 h allocation. Overlap capacity was used, not wasted.
   - **Still open:** none.
5. **Warm-up.**
   - **Proposed:** warm up with real-shape requests: the deployment's short, typical and longest context lengths; its typical and longest outputs; tool calls; and concurrency up to the target sessions. Warm-up is done when a repeat of each shape is not slower than the one before. It includes a corruption check that compares repeated identical requests.
   - **Why:**
     - a fresh engine compiled kernels per new shape, stalling everything for 10–15 s;
     - one first request took about 600 s against 22 s a minute later;
     - an unwarmed replacement tripped a load guard on its first pass;
     - a newly joined worker stalled for 23 s;
     - long contexts were cold after short warm-ups.
   - **Still open:** shape lists per engine version.
6. **Health and restarts.**
   - **Proposed:** a malfunction gets a prompt restart; plain load does not. Malfunction signals:
     - the health endpoint fails;
     - only errors come back;
     - in-flight requests make no token progress past a threshold;
     - one worker sits at a full KV cache with requests waiting while others idle.
   - **Proposed restart ladder:** the Instance's router (seconds; it does not touch the workers) → a worker → the whole Instance inside its Allocation → a new Allocation. A forced kill follows a few seconds after a polite stop: one router held its connections for minutes after a stop signal.
   - **Proposed:** after a restart, the identity check runs again, and an event says that in-flight requests were cut.
   - **Why:** waiting out failing servers cost hours, and restarts cleared them in seconds to minutes. Router-only restarts took about 20 s; full stack restarts took 2–4 min, up to 17.
   - **Still open:** the thresholds, from fault injection.
7. **Repurposing fit.**
   - **Proposed fit check:**
     - GPU count and memory;
     - CPU architecture (engine builds and images differ for arm64 and x86);
     - the checkpoint staged on shared storage;
     - the engine version available on the platform.
   - **Proposed:** repurpose only an Allocation whose current deployment has no remaining work, reservation or fallback role, and only when the time saved exceeds the cost (stop, start, verify, warm up). A pending Allocation that starts directly as the new model costs nothing extra.
   - **Why:** each conversion took about 3 min to serve plus verification. Ad-hoc conversions cost hours of checks and, once, silently retired a deployment a later run needed.
   - **Still open:** several Instances per Allocation stays with HBEP-3's open question 8.
8. **Approvals.**
   - **Proposed: needs approval:**
     - releasing or cancelling a started Allocation before its hard end, outside the spec;
     - any release while a retention policy says keep.
   - **Proposed: needs none:**
     - cancelling pending hedge losers;
     - releasing at the deployment's end time;
     - restarting a failing Instance;
     - handovers.
   - **Why:** irreversible releases needed the owner's own word, and one cancellation relayed second-hand by mistake was caught only because the owner was asked directly. Resource steps that follow agreed rules (restarts, joins, step-downs) ran without asking.
   - **Still open:** whether a repurpose under an agreed policy needs one (HBEP-10).
9. **Spec change while serving.**
   - **Proposed:** the new deployment ID rolls out make-before-break, like a handover, at the next natural handover by default. It rolls out at once only when it fixes a malfunction.
   - **Why:** a setting fix was applied at each stack's next restart and never as a restart of its own, so live work was not cut. Edits to files a live deployment's identity depends on held live work, so changes went into new files.
   - **Still open:** none.
10. **Degraded mode.**
    - **Proposed:** no automatic spill to a provider that is not attached to the model name. Within the attached providers the front balances as usual, capacity drops, and an event fires. An agent decides whether to attach more.
    - **Why:** a gateway deployment and a self-hosted one of the same checkpoint behaved as different deployments, so mixing them silently changes results.
    - **Still open:** none.
11. **Submission protocol and concurrency.**
    - **Proposed:** one acting controller per provider, elected through a Kubernetes `Lease` (HBEP-11). That makes it the single writer for its provider's submissions, but it is never a sequential bottleneck. It follows Kubernetes controller practice:
      - **A work queue of keyed items** (one key per ModelDeployment or Allocation), processed by several workers at once. The same key is never processed by two workers at the same time; different keys run in parallel.
      - **Every reconcile is idempotent and level-triggered:** it re-reads the current state, so a repeated or reordered item does no harm.
      - **Failures requeue the item with rate-limited exponential backoff,** without blocking the other items.
      - **Writes to the control plane use optimistic concurrency** (resource versions), so parallel workers and other controllers never overwrite each other.
      - **Providers are independent shards.** Each provider's controller runs on its own, so one slow or unreachable cluster never delays another.
      - **The parallelism toward each provider is bounded by its platform profile:** concurrent submissions, logins and API calls. All of them share one multiplexed connection.
    - **Proposed:** before any resubmission, the controller lists submissions by tag. A duplicate found later is cancelled while still pending, with an event.
    - **Why:**
      - collisions came when several agents allocated names, ports or copies in parallel, and a single submitter per cluster kept the queue consistent;
      - too many parallel logins tripped the clusters' and a VM's connection limits, so the bound belongs to the provider;
      - hand-run sequential operations were slow; work on different deployments never needed to wait for each other.
    - **Still open:**
      - the provider's tag mechanism (job name or comment) per platform;
      - the worker count and the bounds per platform, from measurement.
12. **Over-allocation and qualification.**
    - **Proposed:** qualify every Instance, also when k is 0. The checks:
      - health, engine version, model names and maximum length;
      - a chat request with its engine fingerprint;
      - a tool-call smoke test;
      - KV capacity from start-up;
      - a corruption check;
      - a short behaviour check on the workload's own long-context shape (malformed or looping tool calls);
      - throughput at the target sessions against absolute gates from the platform profile, such as ms per token and time to first token.
    - **Proposed:** start with k = 1 per provider and adjust it from the measured qualification failure rate. Rank by throughput at the target sessions.
    - **Why:** the same checkpoint behaved differently per node and per deployment. One deployment failed a malfunction check that its twin passed, and malfunctions showed mostly on long loops. Measured KV per session was twice the estimate.
    - **Still open:** benchmark length per platform.
13. **Default retention policy.**
    - **Proposed:** keep until the deployment's end time, or until the grant's end or the deadline when one is set. Release-when-idle is opt-in, and a release needs certainty that no reader, reservation, fallback or re-run needs the Allocation.
    - **Why:** a released slot went to another user within seconds, a new submission started with no queue age, and Allocations kept through the deadline were repurposed as plans changed. Releases before new capacity was running prevented that reuse.
    - **Still open:** the owner's confirmation. This reverses the earlier candidate, "release when idle".
14. **Placing Instances on warm capacity.**
    - **Proposed:** the controller may place a replacement or an added Instance of the same deployment on a warm Allocation by itself. Moving an Allocation to a different model is a repurpose, decided by an operator under question 7's cost rule.
    - **Why:** idle capacity was given work at once. Model changes needed a stated gain.
    - **Still open:** none.

## Design details

**Reconciliation** (decided). The controller is level-triggered. It compares desired state (ModelDeployment specs) with observed state (Allocations from the providers, Instances from registrations and health), then acts. Its state lives in custom resources (HBEP-11). After a restart it re-lists both sides and resumes.

**Provisioning** (decided core). To get an instance, the controller submits Allocations early, on one or more providers. It keeps the first to start (provisional; Open question 1 proposes the first to serve) and cancels the pending losers. Timing and width: Open question 2 proposes submitting as early as possible, eligible at once, from observed waits.

**Over-allocation and qualification** (decided core). For N instances the controller may obtain N+k Allocations. It qualifies each Instance with an identity check (HBEP-9) plus a short benchmark against the expected performance in the platform profile (HBEP-4), and keeps the best N that pass. An Allocation whose Instance fails is released. The rest follow their retention policy: released, or kept as warm capacity. The gate catches broken nodes, not only slow ones. Hedging acts on pending Allocations, over-allocation on started ones. k, the benchmark and the ranking: TBD (Open question 12).

**Admission** (decided: both before traffic). A started instance is verified and warmed with real-shape requests before it is marked ready. Under over-allocation, qualification comes first, and only kept Instances are warmed. The front routes only to ready instances. Checks and shapes: TBD (Open question 5).

**Handover** (decided core). For each serving instance, the controller plans its replacement backwards from the lease's hard end. The submission time is the earlier of two times: the predecessor's start (proposed default, Open question 2), and the predecessor's latest drain start minus verification, warm-up, start-up and the observed queue wait. The replacement is a new instance of the same deployment ID. The predecessor is released only after the replacement serves. Overlap: TBD (Open question 4).

Drain rule (the same text in HBEP-3, HBEP-5 and HBEP-7): The framework cannot move a lease end, so an Instance starts draining at the latest at its hard end minus the drain window, whether or not its replacement is ready. Make-before-break is the goal: the controller plans the replacement to be ready before that drain start, and the old Instance never drains before its replacement is ready unless that deadline forces it. If the replacement is late, the old Instance still drains on time, capacity drops, and an event is emitted (HBEP-8).

A late replacement leaves the deployment degraded (Open question 10).

**Release** (decided). Replacement before release (HBEP-3) applies only while the deployment keeps its desired capacity:
- Handover, and replacing a failing instance, while desired instances are above zero: the old instance is released only after its replacement serves.
- Scale-down: the surplus instances drain and end, without replacement.
- Shutdown at the deployment's end time: every instance drains and ends, and pending Allocations are cancelled, without replacement.
- Deletion of the ModelDeployment: as shutdown, then the objects are removed (HBEP-11).
- Approved early termination (HBEP-10): as shutdown, before the end time.
- Repurposing: as scale-down for the old deployment, which is ending or scaling down. The Allocation then hosts the new deployment's Instance.
- In every case, an Allocation left with no Instance follows its retention policy (Retention).
- A lease end is not a release: under the drain rule, an instance whose replacement is late drains and ends with its lease.

**Retention** (decided). An Allocation that hosts no Instance follows its retention policy:
- keep it, as warm capacity, until a date, a grant's end or a deadline;
- or release it when idle.

A kept Allocation still ends at its hard end. The proposed default is keep until the deployment's end, the grant's end or the deadline (Open question 13). Placing Instances on warm capacity: Open question 14.

**Repurposing** (decided core). An operator, often an agent that needs throughput across several models, may move an Allocation to another ModelDeployment when a model's work finishes. The controller then:
1. drains the old Instance;
2. starts an Instance of the new deployment in the same Allocation;
3. verifies and warms it up before it takes traffic.

The Allocation keeps its name, and the new Instance has the new deployment's ID. Conversions between models are this operation. Fit: Open question 7. Approval: HBEP-10.

**Health and restarts:** TBD (Open question 6).

### Test plan

Against a simulated provider with queue delays, start failures, hard ends, dependency semantics, requeues and local-time clocks:
- Hedging: the first to start is kept and the pending losers are cancelled, and no cancellation ever starts a dependent job.
- Over-allocation of N+k on a fake with one broken node and one slow node: the broken one fails qualification and its Allocation is released, the best N are kept, and each remaining one follows its retention policy.
- The replacement fails verification: the predecessor keeps serving until its hard end minus the drain window, not earlier, then drains. No request is cut at the hard end, and a degraded event fires.
- Scale-down, shutdown at the end time and deletion: instances drain and end, no replacement is submitted, and each Allocation follows its retention policy.
- Retention: an Allocation kept until a date stays, with no Instance, until that date and is then released; one under release-when-idle is released after its idle limit; neither outlives its hard end.
- The controller is killed at each step of a handover: after restart there are no duplicate submissions and no orphan allocations.
- The reply to a submission is lost: the controller resolves the unknown outcome, and exactly one allocation remains.
- Two controllers reconcile one deployment at once: any duplicate submission is found by tag and reported, and no more allocations than desired remain.
- The scheduler requeues a serving allocation under the same job ID: no registration or verification from the first execution is reused, no replacement is planned twice, and the outcome follows HBEP-3's open question 13.
- No instance gets traffic before it is ready.
- A stalled instance with requests in flight is restarted within the threshold. A process that ignores TERM is killed after the grace period.
- Rank 0 exits cleanly while another rank hangs: the instance counts as failed.
- A repurpose drains the old Instance, starts the new deployment's Instance in the same Allocation, and routes to it only after verification and warm-up. The Allocation keeps its name, and the deployment ID changes.
- An operation interrupted on one of several providers reports each provider's outcome.

On real providers, one handover per platform, end to end through the front.

### Compatibility and upgrades

TBD. Stored state is custom resources, so the schema follows Kubernetes versioning (HBEP-11). A controller upgrade must not touch running allocations.

### Security

- Provider credentials belong to the provider plugins (HBEP-4), and allocation credentials to HBEP-6 and HBEP-9.
- The controller sees no prompts. Warm-up and qualification use synthetic prompts only.
- Who may change desired state, ask for a repurpose, or approve a release, is HBEP-10's and HBEP-11's.

### Graduation criteria

TBD. Candidate for alpha: the simulated test plan passes, and one real handover on Helios completes with no request cut.

## Operational readiness

1. **Working:** reconcile age per deployment, and ready instances against desired ones. End to end, each deployment answers a synthetic request through the front. Ready below desired past the start window says it is not working.
2. **Restart:** it re-reads ModelDeployments, lists Allocations by label from every provider, and lists Instances from registrations. Then it resumes planned handovers, and applies each empty Allocation's retention policy.
3. **Silent failure:** "planned" handovers that were never submitted, or instances marked ready that serve nothing. The submission-deadline event and the front's synthetic requests show the difference.
4. **Provider limits:** hard time limits, unknown start times, per-user job limits, fixed accounts, and center response times.
5. **Capacity:** no provider starts a replacement. The deployment degrades, and the question goes to agents (Open question 10).
6. **Idle cost:** pending hedges cost no grant hours, but they hold queue slots under per-user limits. Overlap costs the hours of both allocations. Over-allocation costs the k extra Allocations until qualification ends. Warm capacity costs each kept Allocation's hours until its policy releases it.
7. **Rollback:** scale the deployment to zero. Instances drain and end without replacement (Release); pending Allocations are cancelled, and started ones follow their retention policy, which an operator can set to release when idle.

## Drawbacks

- Hedging, over-allocation, overlap and warm capacity spend queue slots and grant hours to buy continuity and throughput.
- The controller depends on each provider's queue semantics, which differ and change.

## Alternatives

- **Hand-run handovers by operators or agents.** This was the starting point, and it led to most of the problems in Motivation.
- **Slurm dependencies alone.** Same-node pins pended for days, dependencies counted from the wrong moment, and a cancel started dependents.
- **Keep whatever starts.** Simpler than over-allocation, but a broken or slow node then serves until someone notices.
- **Models in pods on Slurm (interLink and other virtual-kubelet offloading, Slinky's slurm-bridge).** Pods hide lease ends and start estimates, which the controller needs. To be evaluated later; parts may become components.

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #7.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).
- 2026-10-28: open questions answered with proposals from past operation, for the owner to confirm.

## Amendments

None.
