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

1. **Hedge winner milestone (a proposed change to HBEP-3's definition).** HBEP-3 provisionally keeps the first allocation to start and cancels the losers then. Proposed change: cancel the losers only when the winner serves. The trade-off is queue age kept against time to serve. Cancelling at start loses the losers' queue age when the winner then fails, for example at environment build or verification. Cancelling at serving keeps that queue age, but the losers hold queue slots, and may start and spend grant hours, for the whole time the winner takes to serve. Owner decision, informed by measured start-up failure rates; an accepted change amends HBEP-3.
2. **Lead time and hedge width.** How early to submit before a needed start, and on how many providers. The inputs are the profile's typical waits, observed recent waits, and start estimates as planning input only. Measurement per provider.
3. **Follow-on form.** Independent hedged submissions, provider dependencies, or same-node pins? Spike per provider.
4. **Overlap policy.** Once the replacement serves, keep the predecessor until its drain starts (more capacity, more grant hours), or release it early? Owner decision, kept as a profile parameter.
5. **Warm-up.** Which shapes (context lengths, output lengths, tool calls, concurrency), and when is warm-up done (per-shape latency stops falling)? Measurement per engine version.
6. **Health and restarts.** Signals: the engine's health endpoint, an end-to-end request through the front's path, and token progress of in-flight requests. Thresholds, and a restart ladder: worker, then instance inside the allocation, then a new allocation. A grace period before a forced kill. Spike with fault injection.
7. **Repurposing fit.** Repurposing is decided (Design details). Which moves fit an Allocation: hardware fit, checkpoint staged, engine compatible? Whether an Allocation hosts several Instances at once is HBEP-3's open question 8. Decided with HBEP-3.
8. **Approvals.** Which controller actions need an approval (HBEP-10)? HBEP-10 proposes none for actions the spec already agreed to, such as a release at the deployment's end time. Candidate outside the spec: cancelling a running allocation early. Whether a repurpose under an agreed policy needs one is HBEP-10's open question.
9. **Spec change while serving.** A new spec has a new deployment ID. Is moving to it a make-before-break rollout like a handover? With HBEP-3.
10. **Degraded mode.** No replacement is ready by the predecessor's drain start. Does `model` spill automatically to an LLMLab provider attached to the name (HBEP-5), or does an agent decide? Owner decision.
11. **Submission protocol.** Tags do not enforce uniqueness, so how does the controller avoid and repair duplicates? Candidates: one acting controller per provider, through a Kubernetes `Lease` (HBEP-11); a list by tag before any resubmission; cancelling a duplicate found later, with an event. Spike per provider.
12. **Over-allocation and qualification.** How large k is per provider, from the spread of performance the profile records; the benchmark's shapes and length; how "best" is ranked; and whether every Instance is qualified when k is 0, since the gate also catches broken nodes. Measurement per platform.
13. **Default retention policy.** Candidate: release when idle, so that a deployment's end leaves nothing running unless a policy keeps it. How long "idle" lasts is a policy parameter. Owner decision.
14. **Placing Instances on warm capacity.** May the controller place a replacement or an added Instance on a warm Allocation by itself, or does only an operator's repurpose move one? With HBEP-10.

## Design details

**Reconciliation** (decided). The controller is level-triggered. It compares desired state (ModelDeployment specs) with observed state (Allocations from the providers, Instances from registrations and health), then acts. Its state lives in custom resources (HBEP-11). After a restart it re-lists both sides and resumes.

**Provisioning** (decided core). To get an instance, the controller submits Allocations early, on one or more providers. It keeps the first to start (provisional, Open question 1) and cancels the pending losers. Timing and width: TBD (Open question 2).

**Over-allocation and qualification** (decided core). For N instances the controller may obtain N+k Allocations. It qualifies each Instance with an identity check (HBEP-9) plus a short benchmark against the expected performance in the platform profile (HBEP-4), and keeps the best N that pass. An Allocation whose Instance fails is released. The rest follow their retention policy: released, or kept as warm capacity. The gate catches broken nodes, not only slow ones. Hedging acts on pending Allocations, over-allocation on started ones. k, the benchmark and the ranking: TBD (Open question 12).

**Admission** (decided: both before traffic). A started instance is verified and warmed with real-shape requests before it is marked ready. Under over-allocation, qualification comes first, and only kept Instances are warmed. The front routes only to ready instances. Checks and shapes: TBD (Open question 5).

**Handover** (decided core). For each serving instance, the controller plans its replacement backwards from the lease's hard end. The submission time is the predecessor's latest drain start, minus verification and warm-up, minus start-up, minus the queue lead time. The replacement is a new instance of the same deployment ID. The predecessor is released only after the replacement serves. Overlap: TBD (Open question 4).

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

A kept Allocation still ends at its hard end. The default policy is Open question 13; placing Instances on warm capacity is Open question 14.

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

## Amendments

None.
