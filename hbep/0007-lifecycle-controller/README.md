# HBEP-7: Lifecycle controller

## Summary

The lifecycle controller keeps each ModelDeployment's desired state true: the requested number of serving instances, on the allowed providers, until the deployment's end. It provisions Allocations with hedging, and verifies and warms each instance with real-shape requests before it takes traffic. It checks health and restarts failures, plans make-before-break handovers from lease ends, never releases a serving instance before its replacement serves, and converts allocations between models. Terms follow HBEP-3.

## Motivation

This HBEP serves stage 2 of the user story (HBEP-2), getting resources on HPC, and stage 3, keeping instances running through failures, restarts, time limits and handovers. Releases also serve stage 7.

Problems:
- Follow-ons pinned to the same node pended for days. Dependencies counted from the parent's start, not its end. Cancelling a parent started its pending follow-on at once.
- A slot freed before the replacement ran went to another user within seconds, and the replacement then waited days.
- One handover design cancelled the predecessor as soon as the follow-on answered, and gave back a large share of every lease.
- Time limits cannot be extended, and centers may not act at weekends. Serving jobs reached their end with no cover.
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
- No serving instance is released before its replacement serves.
- Failing instances restart promptly, in the topology their spec declares.
- A controller restart or an interrupted operation is safe: actions are idempotent, and partial failures are reported item by item.
- Moving an allocation from one model to another is one declared operation.

### Non-goals

- Raising priority at a center. The framework does not assume centers push our jobs; its lever is hedging.
- Deciding what to serve. Users and agents set desired state (HBEP-10).
- Routing (HBEP-5) and paths (HBEP-6).
- Choosing serving settings. Those are the serving spec (HBEP-3) and provider profiles (HBEP-4).

## Proposal

### User stories

- A researcher declares `example-27b` with two instances on Helios or Athena until a given date. The controller submits early to both and keeps the first allocation to start. It cancels the pending loser, then verifies and warms the instance, and the front starts routing to it. Before that lease's hard end, a replacement already serves.
- An instance stalls with requests in flight. The controller records the restart as an event, then restarts the instance within minutes.
- `example-8b` ends while its allocation has hours left, and `example-27b` needs the same GPU type. The controller converts the allocation instead of queueing a new one.
- The controller restarts in the middle of a handover. It re-reads desired state and lists its allocations by label, and it submits nothing twice.

### Notes, constraints and caveats

- These are decided:
  - Hedging, as HBEP-3 defines it: submit early, on several providers if needed; keep the first allocation to start; cancel the pending losers.
  - Never release a serving instance before its replacement serves.
  - A handover keeps the deployment ID. The instance and its metadata change.
  - On batch providers, continuity comes through follow-ons.
  - Hedging across providers is our own controller, built on the Kubernetes API (HBEP-11).
- Every provider action goes through the provider interface (HBEP-4). Submission rules, per-user job limits, time-limit effects on queueing and time zones are profile data. Timestamps reach the controller in UTC.
- An allocation's account cannot change once it runs, so the controller picks the account at submission.
- The drain window is defined in HBEP-5. The controller plans so that a replacement serves before the predecessor's drain starts.

### Risks and mitigations

- **The node is lost.** Releasing before the replacement serves gives the slot to another user. Mitigation: the decided rule, checked by a test.
- **Cancelling a loser starts its dependents.** Mitigation: cancel dependents first, or use no provider-side dependencies (Open question 3).
- **A restart submits twice.** Mitigation: every submission carries an idempotency key in a field the provider stores, and the controller finds its allocations by that label, never by scanning names.
- **Overlap costs grant hours.** A replacement can start hours early, and both then run. Mitigation: an overlap policy (Open question 4).
- **Warm-up misleads.** A warm-up on short prompts leaves long-context shapes cold. Mitigation: warm-up shapes come from the deployment spec, and warm-up runs on the new instance before admission, never on live workers.
- **Verification checks a proxy.** The engine's fingerprint differs per site and is sometimes missing. Mitigation: verification recomputes the deployment ID from the settings the instance reports (HBEP-9).
- **Restart loops.** Mitigation: backoff, then a new allocation, then a degraded deployment and an event (HBEP-8).
- **A poison request kills each restarted instance.** Mitigation: the front's retry guard (HBEP-5) and a crash-correlation event.
- **A restart runs a different spec.** An edited script reached an older job's restart. Mitigation (candidate): each instance runs from an immutable launch bundle for its deployment ID, and a restart reuses it.
- **Edits to pending jobs break them.** Mitigation: never edit a submitted allocation; cancel and resubmit.
- **Our own jobs compete with each other.** Hedges and follow-ons of different deployments compete for the same nodes and per-user limits. Mitigation: plan per provider, with its per-user limits from the profile.
- **The controller dies quietly.** Instances keep serving until their leases end, and handovers are missed. Mitigation: for each deployment, an event when its planned submission time passes with no submission.

### Open questions

1. **What "start" means for a hedge.** Cancel the losers when the winner runs, or when it serves? A start that fails at environment build loses the losers' queue age. Owner decision, informed by measured start-up failure rates.
2. **Lead time and hedge width.** How early to submit before a needed start, and on how many providers. The inputs are the profile's typical waits, observed recent waits, and start estimates as planning input only. Measurement per provider.
3. **Follow-on form.** Independent hedged submissions, provider dependencies, or same-node pins? Spike per provider.
4. **Overlap policy.** Once the replacement serves, keep the predecessor until its drain starts (more capacity, more grant hours), or release it early? Owner decision, kept as a profile parameter.
5. **Warm-up.** Which shapes (context lengths, output lengths, tool calls, concurrency), and when is warm-up done (per-shape latency stops falling)? Measurement per engine version.
6. **Health and restarts.** Signals: the engine's health endpoint, an end-to-end request through the front's path, and token progress of in-flight requests. Thresholds, and a restart ladder: worker, then instance inside the allocation, then a new allocation. A grace period before a forced kill. Spike with fault injection.
7. **Conversion.** Can an Allocation pass from one ModelDeployment to another (HBEP-3)? Which conversions are allowed: hardware fit, checkpoint staged, engine compatible? Decided with HBEP-3.
8. **Approvals.** Which controller actions need an approval (HBEP-10): cancelling a running allocation, a release at the deployment's end, a conversion?
9. **Spec change while serving.** A new spec has a new deployment ID. Is moving to it a make-before-break rollout like a handover? With HBEP-3.
10. **Degraded mode.** No replacement starts before the hard end. Does `model` spill to LLMLab automatically (HBEP-5), or does an agent decide? Owner decision.

## Design details

**Reconciliation** (decided). The controller is level-triggered. It compares desired state (ModelDeployment specs) with observed state (Allocations from the providers, Instances from registrations and health), then acts. Its state lives in custom resources (HBEP-11). After a restart it re-lists both sides and resumes.

**Provisioning** (decided core). To get an instance, the controller submits Allocations early, on one or more providers. It keeps the first to start and cancels the pending losers. Timing and width: TBD (Open questions 1 and 2).

**Admission** (decided: both before traffic). A started instance is verified and warmed with real-shape requests before it is marked ready. The front routes only to ready instances. Checks and shapes: TBD (Open question 5).

**Handover** (decided core). For each serving instance, the controller plans its replacement backwards from the lease's hard end. The submission time is the predecessor's drain start, minus verification and warm-up, minus start-up, minus the queue lead time. The replacement is a new instance of the same deployment ID. The predecessor is released only after the replacement serves. Overlap: TBD (Open question 4). If no replacement serves, the predecessor runs to its hard end and the deployment is reported degraded.

**Health, restarts and conversions:** TBD (Open questions 6 and 7).

### Test plan

Against a simulated provider with queue delays, start failures, hard ends, dependency semantics and local-time clocks:
- Hedging: the first to start is kept and the pending losers are cancelled, and no cancellation ever starts a dependent job.
- The replacement fails verification: the predecessor serves to its hard end, and a degraded event fires.
- The controller is killed at each step of a handover: after restart there are no duplicate submissions and no orphan allocations.
- No instance gets traffic before it is ready.
- A stalled instance with requests in flight is restarted within the threshold. A process that ignores TERM is killed after the grace period.
- Rank 0 exits cleanly while another rank hangs: the instance counts as failed.
- A conversion keeps the Allocation and changes the deployment ID.
- An operation interrupted on one of several providers reports each provider's outcome.

On real providers, one handover per provider profile, end to end through the front.

### Compatibility and upgrades

TBD. Stored state is custom resources, so the schema follows Kubernetes versioning (HBEP-11). A controller upgrade must not touch running allocations.

### Security

- Provider credentials belong to the provider plugins (HBEP-4), and allocation credentials to HBEP-6 and HBEP-9.
- The controller sees no prompts. Warm-up uses synthetic prompts only.
- Who may change desired state, or approve a release, is HBEP-10's and HBEP-11's.

### Graduation criteria

TBD. Candidate for alpha: the simulated test plan passes, and one real handover on Helios completes with no request cut.

## Operational readiness

1. **Working:** reconcile age per deployment, and ready instances against desired ones. End to end, each deployment answers a synthetic request through the front. Ready below desired past the start window says it is not working.
2. **Restart:** it re-reads ModelDeployments, lists Allocations by label from every provider, and lists Instances from registrations. Then it resumes planned handovers.
3. **Silent failure:** "planned" handovers that were never submitted, or instances marked ready that serve nothing. The submission-deadline event and the front's synthetic requests show the difference.
4. **Provider limits:** hard time limits, unknown start times, per-user job limits, fixed accounts, and center response times.
5. **Capacity:** no provider starts a replacement. The deployment degrades, and the question goes to agents (Open question 10).
6. **Idle cost:** pending hedges cost no grant hours, but they hold queue slots under per-user limits. Overlap costs the hours of both allocations.
7. **Rollback:** scale the deployment to zero. Instances drain and their allocations are released; pending ones are cancelled.

## Drawbacks

- Hedging and overlap spend queue slots and grant hours to buy continuity.
- The controller depends on each provider's queue semantics, which differ and change.

## Alternatives

- **Hand-run handovers by operators or agents.** This was the starting point, and it led to most of the problems in Motivation.
- **Slurm dependencies alone.** Same-node pins pended for days, dependencies counted from the wrong moment, and a cancel started dependents.
- **Models in pods on Slurm (interLink and other virtual-kubelet offloading, Slinky's slurm-bridge).** Pods hide lease ends and start estimates, which the controller needs. To be evaluated later; parts may become components.

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #7.

## Amendments

None.
