# HBEP-7: Lifecycle controller

## Summary

The lifecycle controller keeps each ModelDeployment's desired state true: the requested number of serving instances, on the allowed providers, until the deployment's end. It provisions Allocations with hedging and over-allocation, and the hedge winner is the first Allocation whose Instance serves. It qualifies each Instance, and verifies and warms it with real-shape requests before it takes traffic. It checks health and restarts malfunctions, plans make-before-break handovers from lease ends, and never releases a serving instance before its replacement serves while the deployment keeps its desired capacity. An Allocation left with no Instance follows its retention policy, keep by default, and an operator can repurpose it to another ModelDeployment. Inside each Allocation, the allocation agent starts, restarts and stops the Instance on the controller's word. Terms follow HBEP-3.

For alpha: one Instance per Allocation, one worker per Instance, no hedging across providers at once, k = 0 with every Instance qualified, and no repurposing across models.

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
- Calling providers. Each Provider's own controller does that (HBEP-4).
- Routing (HBEP-5) and paths (HBEP-6).
- Choosing serving settings. Those are the serving spec (HBEP-3) and platform profiles (HBEP-4).

## Proposal

### User stories

- A researcher declares `example-27b` with two instances on Helios or Athena until a given date. Helios' GH200 partition and Athena's A100 partition are two accelerator classes, so the deployment has two current IDs, one per class (HBEP-3). The controller submits early to both and keeps the first Allocation whose Instance serves. That Instance takes the current ID of its Allocation's class. The controller cancels the pending loser, and the front routes to the Instance under `example-27b`. Before that lease's drain starts, a replacement already serves. Hedging across providers at once comes after alpha.
- In alpha, the same researcher allows only a Helios provider and sets a lease limit of four hours, to try a handover in an afternoon. The controller submits one Allocation. It submits the replacement at the time open question 2 plans from the queue waits observed on that Provider, or when the first Allocation starts while none has been observed. The replacement's Instance is verified and warmed, and serves beside the first. At the first lease's drain start, the first Instance drains, and no request is cut.
- A researcher needs four instances of `example-27b`. The controller obtains six Allocations and qualifies each Instance with an identity check and a short benchmark. One node fails outright, and its Allocation is released. The controller keeps the best four; the one that passed but ranked fifth is kept as warm capacity, as its retention policy says. Over-allocation with k above 0 comes after alpha.
- An instance stalls with requests in flight. The controller records the restart as an event, then restarts the instance within minutes.
- An agent's work on `example-8b` ends while its Allocations have hours left, and `example-27b` needs the same GPU type. The agent repurposes the Allocations. The controller drains each `example-8b` Instance, starts an `example-27b` Instance in the same Allocation, then verifies and warms it, and the front routes to it. No new job is queued. Repurposing across models comes after alpha.
- An experiment must finish by a deadline. Its retention policy keeps every Allocation until then. Allocations whose model is done wait as warm capacity, or are repurposed, instead of going back to the queue.
- The controller restarts in the middle of a handover. It re-reads desired state and lists its allocations by label, and nothing is submitted twice.

### Notes, constraints and caveats

- These are decided:
  - Hedging, as HBEP-3 defines it: submit early, on several providers if needed; keep the first Allocation whose Instance serves; cancel the pending losers (open question 1; owner, 29 Oct 2026).
  - Over-allocation with qualification, as HBEP-3 defines them: obtain N+k Allocations, qualify each Instance with an identity check plus a short benchmark against what the platform profile says to expect, and keep the best N. The rest are released, or kept as warm capacity, per the retention policy. The gate also catches broken nodes, not only slow ones. Alpha runs k = 0 and qualifies every Instance (open question 12).
  - Retention policy: keep an Allocation until a date, a grant's end or a deadline; or release it when idle. Keep is the default (open question 13).
  - Repurposing: an operator may move an Allocation to another ModelDeployment when a model's work finishes. Conversions between models are this operation.
  - Replacement before release, as HBEP-3 scopes it: while a deployment keeps its desired capacity, never release a serving instance before its replacement serves. The cases are listed under Release in Design details.
  - A handover replaces the Instance with a new one, with its own metadata, in the same accelerator class. The deployment ID stays. A replacement placed in another class is a new Instance with that class's current ID (HBEP-3).
  - On batch providers, continuity comes through follow-ons: independent submissions, with no dependencies or node pins (open question 3).
  - Hedging across providers is our own controller, built on the Kubernetes API (HBEP-11).
- **Prior art for over-allocation.** Farley et al. call the strategy placement gaming: launch extra instances up front and keep the strongest ("More for your money: exploiting performance heterogeneity in public clouds", ACM SoCC 2012). Ou et al. take a keep-good, discard-bad approach to EC2 instance heterogeneity ("Is the same instance type created equal? Exploiting heterogeneity of public clouds", IEEE Transactions on Cloud Computing, 2013).
- An Allocation outlives its Instances (HBEP-3). Ending an Instance and releasing its Allocation are separate steps: the Instance ends, and the Allocation then follows its retention policy.
- The lifecycle controller acts on providers only through Allocation objects, which each Provider's controller reconciles with the core operations (HBEP-4). Submission rules, per-user job limits, time-limit effects on queueing and time zones are profile data. Timestamps reach the controller in UTC: Slurm prints local time (Europe/Warsaw) on Helios and Athena and UTC on Sano (spike S5, PR #19), and the provider plugin converts it (HBEP-4).
- An allocation's account cannot change once it runs, so the account is fixed at submission. Each provider has one account (HBEP-3), so picking the account is picking the provider.
- The drain window is the profile's `drainWindow` (HBEP-4), which HBEP-5 sets, or shorter for a ModelDeployment that sets `routing.maxRequestDuration` (Handover). The controller plans so that a replacement serves before the predecessor's drain starts; the drain rule under Handover says what happens when it does not.
- Nothing runs on a login node (HBEP-4, open question 14), and a job step launched from a login node dies with the `srun` that started it (L-3.5). So the Instance's processes are started inside the Allocation, by the allocation agent, which the Allocation's job runs (Design details).

### Risks and mitigations

- **The node is lost.** Releasing before the replacement serves gives the slot to another user. Mitigation: the decided rule, checked by a test. A retention policy can also keep an Allocation after its Instance ends.
- **Cancelling a loser starts its dependents.** Mitigation: no provider-side dependencies (open question 3).
- **A restart submits twice.** Every submission carries a tag in a field the provider stores, and the provider finds its jobs by that tag, never by scanning names. The tag supports recovery but does not enforce uniqueness: a provider can accept two submissions with one tag, and a lost submission reply leaves the outcome unknown until the provider lists by tag. Mitigation, from spike S4 (PR #32): in hold mode every job is submitted held, and only the recorded job's hold is ever lifted, so a duplicate stays held and is cancelled. In fallback mode only a job found by tag resolves an unknown outcome (open question 11).
- **The scheduler requeues a job.** Slurm can requeue a batch job and run its script again under the same job ID, and requeue is on by default on Helios, Athena and Sano (spike S5, PR #19). That can leave stale registrations, verification results or replacement work from the first execution behind. Each platform profile records its requeue behaviour (HBEP-4). HBEP-3 decides identity across executions (open question 13, from spike S4): submit every Allocation with `--no-requeue`, treat a requeue that still happens as a failure of its Instance, never lift a requeued job's hold again (except the known risk in HBEP-4), and replace it with a new Allocation. The failed Allocation is deleted, and its finalizer cancels the held job.
- **An ended job is read as missing.** Helios and Athena drop a finished job from `squeue` about 10 s after it ends (spike S5, PR #19). A controller that reads a missing job as never submitted would submit it again. Mitigation: the provider plugin reports an ended lease as existing and terminal, from accounting (HBEP-4), and nothing is resubmitted only because a job is missing from the queue.
- **A job without GPUs on a GPU-only platform.** Athena is meant for GPU jobs only, and its documentation warns that workloads without GPUs can lead to account suspension (spike S5, PR #19). Mitigation: every Allocation requests at least one GPU on a GPU partition, and the Slurm plugin refuses any other (HBEP-4). Still open: whether a GPU Allocation kept idle as warm capacity counts (open question 13).
- **Overlap costs grant hours.** A replacement submitted too early, on a short queue, runs beside its predecessor for most of the lease. Mitigation: the replacement is planned from observed queue waits (open question 2, owner decision of 30 Oct 2026), so on a short queue the overlap is only the planning margin; overlap capacity serves traffic (open question 4), and the cost is shown per deployment (Operational readiness 6).
- **Warm-up misleads.** A warm-up on short prompts leaves long-context shapes cold. Mitigation: warm-up shapes come from the deployment spec, and warm-up runs on the new instance before admission, never on live workers.
- **Verification checks a proxy.** The engine's fingerprint differs per site and is sometimes missing. Mitigation: verification compares the settings the Instance reports with the declared ones, and rehashes the checkpoint files (HBEP-3, HBEP-9). It never recomputes the deployment ID from reported settings.
- **Verification passes because it read nothing.** An engine that reports no settings would contradict none. Mitigation: the engine version, the image digest and the context length must be reported and match; any other declared setting that is not reported is recorded as not reported, never as matching (Design details).
- **A contradicted spec loops.** An engine that resolves a declared setting differently does so on every start. Mitigation: a settings contradiction stops provisioning in that accelerator class until the spec changes, with a condition and an event; it is never restarted. A workload shape that does not fit the engine's context limit stops the class too, but only until the shapes fit, so a workload edit lifts it (Instance start).
- **A reasoning model spends its budget thinking.** A check that asks for a short answer can get only reasoning back. Mitigation: with a reasoning parser declared, the tool-call smoke test and the behaviour check each ask for 4,096 more tokens, judge only the final answer, and report a reply with no final answer as `ReasoningBudgetExhausted`, never as a wrong answer (Instance start). The fixed 4,096 is a known limit of alpha.
- **A stale gauge reads as idle.** A failed `/metrics` read that left a zero in place would end a draining Instance early, or start the idle count. Mitigation: a gauge left out is unknown, never zero, and the controller takes no gauge-based action unless the gauges are fresh (Health and restarts).
- **Qualification ranks on noise.** A short benchmark can rank nodes by chance, for example while a shared filesystem is busy. Mitigation: each Instance is judged against the profile's expected performance, not only against the others. Alpha ranks nothing and gates on no throughput (open question 12).
- **Warm capacity is forgotten.** Kept Allocations spend grant hours with nothing on them. Mitigation: every retention policy ends at a date, a grant's end, a deadline or an idle limit; the profile's `maxIdle` caps idle time where a platform needs it; and the inventory lists kept Allocations (HBEP-10).
- **A repurpose inherits the old model's state.** Files staged for the old model, or another engine, are left in the Allocation. Mitigation: the new Instance passes the same verification as any other, against its own deployment's identity record (HBEP-9).
- **Restart loops.** Mitigation: a bounded number of attempts per rung, with backoff, then a new allocation, then a degraded deployment and an event (HBEP-8).
- **A poison request kills each restarted instance.** Mitigation: the front's retry guard (HBEP-5) and a crash-correlation event.
- **A restart runs a different spec.** An edited script reached an older job's restart. Mitigation: each Instance's launch (command line, environment, image digest and checkpoint) is fixed in its immutable spec when it is created, and every restart reuses it (Design details).
- **Edits to pending jobs break them.** Mitigation: never edit a submitted allocation; cancel and resubmit.
- **Our own jobs compete with each other.** Hedges, over-allocations and follow-ons of different deployments compete for the same nodes and per-user limits. Mitigation: plan per provider, with its per-user limits from the profile.
- **The controller dies quietly.** Instances keep serving until their leases end, and handovers are missed. Mitigation: for each deployment, the condition `ReplacementLate` and an event when a replacement has not started by its predecessor's drain start minus the class's start-to-ready time (Handover).
- **A path outage looks like dead Instances.** When the dial-home path or the control-plane host's endpoint is down, every agent stops reporting at once. Mitigation: an agent that stops reporting makes its Instance unreachable, not failed; the front stops routing to it, and the controller restarts nothing on that basis alone (HBEP-6).
- **Staged files vanish.** Scratch filesystems delete old files (Helios purges files older than 30 days; spike S5, PR #19). Mitigation: the provider checks the agent's binary by digest before every submission (HBEP-4), and the agent re-stages any image or checkpoint file whose rehash fails.

### Open questions

Each question below had a proposed answer taken from operating LLM serving on Slurm systems for a deadline-driven experiment. The owner approved the proposals on 29 Oct 2026, in a direct session recorded on PR #14, so they are decided. What that experience did not settle is marked for alpha or beta.

1. **Hedge winner milestone.**
   - **Decided:** the winner is the first Allocation whose Instance serves, not the first to start. Pending losers are cancelled only then. A loser that starts before the winner serves becomes an over-allocation candidate (question 12), not waste. This amends HBEP-3's definition. Source: owner, 29 Oct 2026 (PR #14).
   - **Why:** start-up failures were common: broken environment builds, out-of-memory at start, collective-communication failures in new steps, scripts that killed their own stack seconds after start, engines dying minutes into use. A cancelled job's queue age is lost, and a new submission can wait days. Holding a pending loser costs no grant hours.
   - **Not needed for alpha:** hedging across more than one provider at once. Alpha submits one Allocation per Instance it needs, so the rule has one candidate and takes effect with hedging. The measured start-up failure rate per platform, which sets the hedge width, comes with it.
2. **Lead time and hedge width.**
   - **Decided:** plan each replacement from observed queue waits. It is submitted at the predecessor's drain start minus (the p90 of recent observed queue waits on that Provider + start-up + verification + warm-up + a margin), never earlier than the predecessor's own start. On a long queue that is the predecessor's start, as in past operation; on a short queue the replacement overlaps only by the margin. With hedging, further providers are added when the observed waits on the preferred one exceed the time left. Submissions are eligible at once, so they gain queue age. Use observed waits, never start estimates, for planning. Source: owner, 29 Oct 2026 (PR #14), refined by the owner on 30 Oct 2026 to plan from observed waits instead of always submitting at the predecessor's start, which doubled grant hours on short queues.
   - **Why:** start estimates swung by hours and went "unknown" on every scheduler pass. Jobs with limits up to about 2.5 days started within a minute while 3-day ones waited hours. Age counts only from eligibility, so a dependency-held job lost priority to everyone else.
   - In alpha the replacement is planned this way on the predecessor's Provider (Handover). The p90 is taken over the last 20 queue waits observed there, which the Provider's status keeps (HBEP-4). With fewer than 20, the largest observed wait is used; with none, the replacement is submitted at the predecessor's start. Start-up, verification and warm-up are the class's last measured start-to-ready time, which the ModelDeployment's status keeps, or 30 min until one is measured. The margin is the profile's `handoverMargin`, 30 min by default. Source: science-agent decision, 30 Oct 2026, for the window and the fallbacks.
   - **Not needed for alpha:** submitting on further providers from observed waits, and time limits chosen to backfill. Alpha asks for the partition's `maxTime`, or the deployment's `leaseLimit` when shorter (HBEP-4, open question 11).
   - **Open (blocks beta):** the backfill limit per platform, from the profile, and whether the p90 window should differ per platform.
3. **Follow-on form.**
   - **Decided:** independent submissions without dependencies or node pins. Source: owner, 29 Oct 2026 (PR #14). Alpha uses neither on any platform.
   - **Why:** jobs pinned to their predecessor's node waited for days on nodes other users had planned. Chained dependencies on jobs that could sit in the queue blocked the rest. "After N minutes" dependencies counted from the parent's start, not its end. Node and URL changes no longer matter, because Instances dial home and clients use names.
   - **Not needed for alpha:** same-node pins on platforms whose profile shows they start reliably.
4. **Overlap policy.**
   - **Decided:** keep the predecessor until its drain starts. An early release happens only under a retention policy that asks for it. Source: owner, 29 Oct 2026 (PR #14).
   - **Why:** a design that cancelled the predecessor as soon as its follow-on answered gave back about 6 h of every 48 h allocation. Overlap capacity was used, not wasted.
5. **Warm-up.**
   - **Decided:** warm up with real-shape requests: the deployment's short, typical and longest context lengths; its typical and longest outputs; tool calls; and concurrency up to the target sessions. Warm-up is done when a repeat of each shape is not slower than the one before. It includes a corruption check that compares repeated identical requests. Source: owner, 29 Oct 2026 (PR #14).
   - **Why:**
     - a fresh engine compiled kernels per new shape, stalling everything for 10–15 s;
     - one first request took about 600 s against 22 s a minute later;
     - an unwarmed replacement tripped a load guard on its first pass;
     - a newly joined worker stalled for 23 s;
     - long contexts were cold after short warm-ups.
   - In alpha the shapes come from the ModelDeployment's `workload` (Design details).
   - **Not needed for alpha:** shape lists per engine version.
6. **Health and restarts.**
   - **Decided:** a malfunction gets a prompt restart; plain load does not. Malfunction signals:
     - the health endpoint fails;
     - only errors come back;
     - in-flight requests make no token progress past a threshold;
     - one worker sits at a full KV cache with requests waiting while others idle.
   - **Decided:** the restart ladder: the Instance's router (seconds; it does not touch the workers) → a worker → the whole Instance inside its Allocation → a new Allocation. A forced kill follows a few seconds after a polite stop: one router held its connections for minutes after a stop signal. After a restart, the identity check runs again, and an event says that in-flight requests were cut. Source: owner, 29 Oct 2026 (PR #14), for both.
   - **Why:** waiting out failing servers cost hours, and restarts cleared them in seconds to minutes. Router-only restarts took about 20 s; full stack restarts took 2–4 min, up to 17.
   - In alpha the thresholds are profile values with defaults (HBEP-4). An alpha Instance has one worker and no router, so its ladder is the worker, the whole Instance, then a new Allocation (Design details).
   - **Decided for alpha** (science-agent decision, 30 Oct 2026, from the review of PR #63): `health.noProgressTimeout` is 30 min, equal to the front's `idleTimeout` (HBEP-5). A request in prefill generates no token, and the front allows a first token after 1,100 s (S2 phase A). A shorter time-out would restart a healthy worker that the front still waits on.
   - **Open (blocks beta):** a no-progress signal that tells a long prefill from a stall, such as prefill progress from the engine's metrics, so that a stall is caught in minutes again. Source: science-agent decision, 30 Oct 2026, from the review of PR #63.
   - **Not needed for alpha:** tuning the thresholds from fault injection, which is beta; the router rung and the KV-skew signal, which need several workers.
7. **Repurposing fit.**
   - **Decided:** the fit check:
     - GPU count and memory;
     - CPU architecture (engine builds and images differ for arm64 and x86);
     - the checkpoint staged on shared storage;
     - the engine version available on the platform.
   - **Decided:** repurpose only an Allocation whose current deployment has no remaining work, reservation or fallback role, and only when the time saved exceeds the cost (stop, start, verify, warm up). A pending Allocation that starts directly as the new model costs nothing extra. Source: owner, 29 Oct 2026 (PR #14), for both.
   - **Why:** each conversion took about 3 min to serve plus verification. Ad-hoc conversions cost hours of checks and, once, silently retired a deployment a later run needed.
   - HBEP-3 decides its open question 8: several Instances per Allocation when its resources allow; alpha places one.
   - **Not needed for alpha:** repurposing across models. Alpha moves no Allocation between ModelDeployments; a warm Allocation hosts only its own deployment's Instances (question 14).
8. **Approvals.**
   - **Decided:** needs approval:
     - releasing or cancelling a started Allocation before its hard end, outside the spec;
     - any release while a retention policy says keep.
   - **Decided:** needs none:
     - cancelling pending hedge losers;
     - releasing at the deployment's end time;
     - restarting a failing Instance;
     - handovers.
   - Source: owner, 29 Oct 2026 (PR #14), for both.
   - Releasing an Allocation whose Instance failed follows the spec as well: a failed Instance's Allocation is released (Over-allocation and qualification, HBEP-3).
   - **Why:** irreversible releases needed the owner's own word, and one cancellation relayed second-hand by mistake was caught only because the owner was asked directly. Resource steps that follow agreed rules (restarts, joins, step-downs) ran without asking.
   - In alpha the controller takes no action that needs approval. Every action it takes follows the spec, and the single user changes the spec instead. HBEP-11 adds approval gates after alpha.
   - **Not needed for alpha:** whether a repurpose under an agreed policy needs approval (HBEP-10).
9. **Spec change while serving.**
   - **Decided:** each class's new deployment ID rolls out make-before-break, like a handover, at the next natural handover by default. It rolls out at once only when it fixes a malfunction. Source: owner, 29 Oct 2026 (PR #14); HBEP-3, open question 7.
   - **Why:** a setting fix was applied at each stack's next restart and never as a restart of its own, so live work was not cut. Edits to files a live deployment's identity depends on held live work, so changes went into new files.
10. **Degraded mode.**
    - **Decided:** no automatic spill to a provider that is not attached to the model name. Within the attached providers the front balances as usual, capacity drops, and an event fires. An agent decides whether to attach more. Source: owner, 29 Oct 2026 (PR #14).
    - **Why:** a gateway deployment and a self-hosted one of the same checkpoint behaved as different deployments, so mixing them silently changes results.
11. **Submission protocol and concurrency.**
    - **Decided:** one acting controller per provider, elected through a Kubernetes `Lease`, `provider-<name>` (HBEP-11). Source: owner, 29 Oct 2026 (PR #14). It is the Provider's controller (HBEP-4), the single writer for its provider's submissions, but never a sequential bottleneck. It follows Kubernetes controller practice:
      - **A work queue of keyed items** (one key per Allocation), processed by several workers at once. The same key is never processed by two workers at the same time; different keys run in parallel.
      - **Every reconcile is idempotent and level-triggered:** it re-reads the current state, so a repeated or reordered item does no harm.
      - **Failures requeue the item with rate-limited exponential backoff,** without blocking the other items.
      - **Writes to the control plane use optimistic concurrency** (resource versions), so parallel workers and other controllers never overwrite each other.
      - **Providers are independent shards.** Each provider's controller runs on its own, so one slow or unreachable cluster never delays another.
      - **The parallelism toward each provider is bounded:** concurrent commands share one multiplexed connection (HBEP-4).
    - The lifecycle controller follows the same practice, keyed per ModelDeployment (Design details).
    - **Decided: the submission protocol of spike S4** (issue #24, PR #32), implemented there against a fake Slurm. In hold mode it keeps at most one job per Allocation running, whatever the timing, because only the recorded job's hold is ever lifted. HBEP-3 holds its resource-model facts; HBEP-11 holds where the record lives; HBEP-4 holds the commands. Source: spike S4 (PR #32).
      - **The record.** The tag is the Allocation's external name, set before any submission (HBEP-3). The submission record and the job ID are annotations on the Allocation, each written with a resourceVersion precondition: a compare-and-swap, where a conflict means re-read, never overwrite. Status mirrors them for display (HBEP-11).
      - **Claim, then submit.** Before `sbatch`, the provider claims one attempt, numbered n, as InFlight, by compare-and-swap. A lost claim means no `sbatch`. In hold mode it then runs `sbatch --hold --no-requeue --parsable --job-name=<tag>`, with the options HBEP-4 lists.
      - **Outcomes.** Accepted: `sbatch` printed a job ID, which is written once as the job ID. Rejected: a definite refusal, or a command that never started; the next attempt waits for backoff. Unknown: a timeout, a signal, a transport error, unparseable output or a transient reason. Unknown is never Failed. It is resolved only by finding the tag.
      - **The guard.** While an attempt is InFlight within the in-flight window, or Unknown, Observe reports the Allocation as existing, never missing, keeps a stable status message, re-checks on a short poll, and submits nothing.
      - **Adoption.** Observe always looks the tag up first: in `squeue`, then in `sacct` with an explicit start time. With no job ID recorded and jobs carrying the tag, the provider adopts one, in this order: a live job that is not held, then a job that ran, then the lowest job ID. A live job that is not held comes first because only a recorded job's hold is ever lifted, so such a job proves a lost record. Jobs are sorted by job ID, never taken in output order. Adoption marks the attempt Done only when it is Unknown, or InFlight past the window; a claimant inside the window resolves its own attempt when its `sbatch` returns.
      - **Settle.** After every establish, and on every Observe while not deleting, the provider cancels every held job with the tag except the recorded one. Then it lifts the recorded job's hold once, only if all of these hold: no `hold-lifted` marker, restart count 0, never started, and reason `JobHeldUser`. It writes the marker by compare-and-swap. Immediately before the release it re-reads the marker and the job, under a per-job lock, and releases only a job that still meets all four, so a leader that stalls before that re-read cannot free a requeued job. The window after it is a known risk (HBEP-4, `LiftHold`). An admin hold is reported as a condition and never retried.
      - **Lost, and revisable.** A recorded job absent from both `squeue` and `sacct` for longer than the profile's `lostGrace` (default 2 min, above the measured gap between the two) is reported Lost, with a Warning event, and never resubmitted. If it reappears, as late accounting can make it, it returns to its real state, with an event.
      - **Hold mode** (profile `holdRelease: true`). Settle runs in the same reconcile as the submit, so the hold lasts milliseconds. On S4's fake it lasted 11 to 33 ms when the submitter lifted it, and 106 to 473 ms when a restarted controller adopted the job. A real cluster adds the `sbatch` latency and one `squeue` and one `scontrol` round trip over ssh. A held job gains no queue age, so queue age is lost only while no controller runs: 7.4 s across a `Lease` handover on the fake. Once the in-flight window has passed with no job carrying the tag in `squeue`, the attempt is dropped and the next one is n+1. A late registration lands held and is cancelled.
      - **Fallback mode** (profile `holdRelease: false`). `sbatch` runs without `--hold`, so the established job runs at once. The claim, the guard, compare-and-swap and adoption are unchanged. An InFlight claim older than the in-flight window with no job found becomes Unknown. Unknown needs positive resolution: the tag found in `squeue`, or in `sacct` with an explicit start. Elapsed time never permits a retry. Extra jobs that are not held go into a sticky duplicates list, with an event, and are cancelled on deletion.
      - **The operator condition.** In fallback mode, an attempt still Unknown after the profile's Unknown bound raises a Warning event and a condition saying that an operator must decide. The operator checks the cluster and sets `abandon-attempt` to n (HBEP-11). That marks the attempt Abandoned and allows attempt n+1.
      - **Deletion.** The finalizer stays while any job with the tag is live (held, pending, running or suspended), or an attempt is InFlight or Unknown. Deletion cancels every live job with the tag, not only the recorded one. A submission that returns during deletion records its job ID, never lifts its hold, and is cancelled. In hold mode, a registration that arrives after the window, once the finalizer is gone, leaves a held orphan: it never runs, and HBEP-11's orphan report lists it.
      - **Alpha precondition.** `holdRelease` is verified on the alpha platform; otherwise fallback mode runs. Spike S1 (issue #18) measured it true on Helios and Athena on 30 Oct 2026, with one trial per tested partition (commit 35babef on S1's branch). Sano is not verified, so it runs fallback mode until checked (HBEP-11).
    - **Why:**
      - collisions came when several agents allocated names, ports or copies in parallel, and a single submitter per cluster kept the queue consistent;
      - too many parallel logins tripped the clusters' and a VM's connection limits, so the bound belongs to the provider;
      - hand-run sequential operations were slow; work on different deployments never needed to wait for each other.
    - **From spike S4 (PR #32):** for Slurm the tag is in the job name (HBEP-3), because accounting keeps the job comment only where a site enables it. The `Lease` is an extra guard. It does not prevent duplicate jobs, since a deposed leader's submission can still land; in hold mode, holding every job and lifting only the recorded job's hold does (HBEP-11). The tag lookup finds jobs and resolves Unknown attempts.
    - Each platform's `inflightWindow` is set above the `sbatch` latency over ssh, which is measured before alpha (HBEP-4, Graduation criteria).
    - **Not needed for alpha:**
      - the worker count and the bounds per platform, from measurement; alpha uses code defaults (HBEP-4, Transport);
      - detecting a hold that a site's `job_submit` plugin drops at submission. It stays a documented hazard. S4 tried an event for it and removed it, because a peer's legitimate lift of the hold raced with it and gave false alarms. Alpha relies on `holdRelease` measured on each alpha partition.
12. **Over-allocation and qualification.**
    - **Decided:** qualify every Instance, also when k is 0. The checks:
      - health, engine version, model names and maximum length;
      - a chat request with its engine fingerprint;
      - a tool-call smoke test;
      - KV capacity from start-up;
      - a corruption check;
      - a short behaviour check on the workload's own long-context shape (malformed or looping tool calls);
      - throughput at the target sessions against absolute gates from the platform profile, such as ms per token and time to first token.
    - **Decided:** start with k = 1 per provider and adjust it from the measured qualification failure rate. Rank by throughput at the target sessions. Source: owner, 29 Oct 2026 (PR #14), for both.
    - **Why:** the same checkpoint behaved differently per node and per deployment. One deployment failed a malfunction check that its twin passed, and malfunctions showed mostly on long loops. Measured KV per session was twice the estimate.
    - In alpha k is 0, and every Instance passes every check but the throughput gate. Throughput at the target sessions is measured and recorded. It gates nothing, because no profile holds expected performance yet, and a reference must not come from the Instances it judges (HBEP-4, Test plan).
    - **Not needed for alpha:** k above 0, the best-N ranking, and the benchmark length per platform.
    - **Open (blocks beta):** the form and source of expected performance (HBEP-4, open question 3), which the absolute gates need.
13. **Default retention policy.**
    - **Decided:** keep until the deployment's end time, or until the grant's end or the deadline when one is set. Release-when-idle is opt-in, and a release needs certainty that no reader, reservation, fallback or re-run needs the Allocation. This reverses the earlier candidate, "release when idle". Source: owner, 29 Oct 2026 (PR #14).
    - **Why:** a released slot went to another user within seconds, a new submission started with no queue age, and Allocations kept through the deadline were repurposed as plans changed. Releases before new capacity was running prevented that reuse.
    - **Decided for alpha (science-agent decision, 30 Oct 2026):** Athena's profile sets `maxIdle` to 30 min, because its documentation warns that workloads not using GPUs lead to account suspension, and an idle GPU Allocation may count as one. Other platforms leave it unset. **Open (blocks beta):** Cyfronet's answer on idle GPU jobs, which may lengthen or remove the cap.
14. **Placing Instances on warm capacity.**
    - **Decided:** the controller may place a replacement or an added Instance of the same deployment on a warm Allocation by itself. Moving an Allocation to a different model is a repurpose, decided by an operator under question 7's cost rule. Source: owner, 29 Oct 2026 (PR #14).
    - **Why:** idle capacity was given work at once. Model changes needed a stated gain.
    - In alpha the warm Allocation must be in the class the Instance needs, with lease left above twice that class's last measured start-to-ready time (30 min until one is measured) plus the drain window.

## Design details

### Controllers

- **The lifecycle controller** is one controller-runtime manager. Alpha runs one replica, which the launcher restarts when it exits (HBEP-11, open question 6). It takes a `Lease` named `lifecycle`, in the namespace `hpcburst-system`, which the launcher creates, as a guard against a stale second copy. Every write it makes, its agent endpoint's included, names the field manager `hpcburst-lifecycle` (HBEP-11).
- **Its work queue is keyed per ModelDeployment.** Several workers run at once (four by default), and one key is never processed by two at once. It watches ModelDeployments, Allocations, Instances, Providers and PlatformProfiles. Each event maps to a ModelDeployment's key: an Allocation through its `model-deployment` label, an Instance through its owner, a Provider or PlatformProfile to every deployment that allows it.
- **What it writes:**
  - Allocations: it creates and deletes them, and writes their `hpcburst.groundnuty.github.io/idle-since` annotation, the idle anchor (Retention). The Provider's controller writes the status and the record's other annotations (HBEP-11);
  - Instances: it creates and deletes them, writes their phase, conditions, `status.leaseEnd`, `status.endTime`, `status.restarts` and `status.agent`, and writes restart generations as annotations. On every observe (Reconcile loop, step 3) it copies the Allocation's hard end into the Instance's `status.leaseEnd`, from which the front enforces the drain deadline (HBEP-5). A later hard end, once observed, is copied too, and moves the drain start;
  - Providers: only the condition `DialHomeUnreachable`, by server-side apply to the status under `hpcburst-lifecycle`, owning that one entry (The allocation agent; HBEP-4, open question 16);
  - identity records, and through its verification controller the verification records (HBEP-11);
  - ModelDeployment status, including each class's last measured start-to-ready time (Handover), and a managed source's status (Reconcile loop, step 1);
  - Events.
- **It never calls a provider.** The Provider's controller acts on each Allocation (HBEP-4; open question 11).
- **It serves the agent endpoint** for the allocation agents (The allocation agent).
- **Verification** (HBEP-9) runs as its own controller inside the same manager in alpha, as HBEP-11 places it (Controllers). It writes the verification records.

### What the controller reads

- **From the ModelDeployment** (HBEP-11): the model name, the source (a serving spec, or a managed catalog entry), the number of instances, the allowed providers, the end time and the retention policy. In alpha the allowed providers are tried in the order their names are listed, then those that only the label selector matches, by name.
- **From the ModelDeployment's `routing`** (HBEP-5): `routing.maxRequestDuration`, which shortens its Instances' drain window (Handover). The front reads the rest of `routing`.
- **Fields this HBEP adds to the ModelDeployment's spec** (an amendment to HBEP-11's API):
  - `leaseLimit` (duration, optional): the longest time limit of its Allocations, when shorter than the partition's `maxTime`.
  - `workload`: the shapes for warm-up and qualification.
    - `contextTokens.typical`, `outputTokens.typical` and `targetSessions` are required. `targetSessions` is at most 1024, so a report at the bounds fits its size limit (Reports).
    - `contextTokens.short` defaults to 512 tokens, and `outputTokens.longest` to 4,096 tokens.
    - `contextTokens.longest` defaults to the resolved context limit (the `max-model-len` the engine reports) minus `outputTokens.typical`, the output paired with it, minus the template overhead. It is therefore fixed at verification. The overhead is measured by rendering an empty conversation (one user message with no content, plus the generation prompt) through the engine's chat template. vLLM counts input and output tokens against one limit (`max_total_tokens` in [`vllm/renderers/params.py` at v0.32.0](https://github.com/vllm-project/vllm/blob/v0.32.0/vllm/renderers/params.py), accessed 2026-10-30), so a prompt that fills the context leaves no room for the answer.
    - **Every shape must fit.** The shapes are each context length with the typical output, and the typical context with the longest output (Instance start). A shape that does not fit is an error, never shortened. When the serving spec declares `max-model-len`, the controller checks the declared shapes against it before it provisions, and a shape that does not fit sets `Degraded` with reason `SpecUnsupported`. At verification each rendered request, overhead included, is checked against the context limit the engine reports, and one that does not fit fails verification and stops provisioning in that class (Instance start). That stop is keyed on the class's deployment ID plus a hash of the resolved shapes, so a workload edit that makes the shapes fit lifts it at the next reconcile, without a change to the serving spec. The behaviour check of a reasoning model runs at a shorter prompt (Instance start); the shapes themselves are never shortened.
    - `toolCalls` defaults to whether the serving spec enables automatic tool choice.
- **From the PlatformProfile** (HBEP-4): per partition, the accelerator class, `maxTime`, `gpusPerNode`, `cpusPerGpu`, `memPerGpu` and `imageFormat`. For the platform: `drainWindow`, `drainMargin`, `handoverMargin`, `maxIdle`, and the `health` and `restart` groups.
- **From the Provider's status** (HBEP-4): `queueWaits`, the last 20 observed queue waits, which plan replacements (Handover). For a managed Provider, its `catalog` and its `CredentialsRejected` condition, which a managed-source ModelDeployment's `Ready` follows (Reconcile loop, step 1).
- **The Instance's spec** gains `launch` (an amendment to HBEP-11's API). It is the command line and environment rendered from the serving spec, the image reference and digest, the checkpoint repository, revision and identity record, and the GPU count. It is written when the Instance is created, and it is immutable like the rest of the spec. Every start and restart of that Instance runs exactly this.
  - The command line always carries `--max-num-seqs`, from `launch.maxNumSeqs`: the serving spec's value, else 256, a framework default. So the front's per-Instance cap is always known (HBEP-5). `max-num-seqs` is capacity, not a deployment-ID input (HBEP-3's table), so the default changes no ID.

### Reconcile loop

One pass for one ModelDeployment. Every step is idempotent and re-reads what it acts on.
1. **Read** the ModelDeployment, its allowed Providers in order, and their PlatformProfiles.
   - **A served ModelDeployment** ignores the managed Providers in its selection, even when their labels match, as the front does (HBEP-5). A batch Provider that is missing or not `Ready` is skipped, with a condition naming it.
   - **A managed-source ModelDeployment** (`spec.source.managed`) is provisioned nothing: no Allocation and no Instance, whatever `spec.instances` says. The controller writes its status and stops here; the front routes to the catalog entry (HBEP-5). Its status says that it has no Instances by design: `desiredInstances` and `readyInstances` are 0, and `Progressing` is false with reason `ManagedSource`. `Ready` follows the catalog entry. It is true while an allowed managed Provider's status lists the entry's catalog name now and that Provider's `CredentialsRejected` is not true. Otherwise it is false, with reason `CatalogEntryUnlisted` or `CredentialsRejected`. When no allowed managed Provider carries `spec.source.managed.platform` as its platform label, it is false with reason `ProviderNotOnPlatform`, and the front routes the entry to no Provider off that platform (HBEP-5).
2. **Current IDs.** For each accelerator class that the allowed partitions offer, compute the class's current ID from the serving spec (HBEP-3), and create its identity record if absent (HBEP-11). Status lists the IDs by class. No Allocation is created for a class without a current ID.
   - When the ID cannot be computed, the ModelDeployment gets `Degraded` with reason `IdentityUnresolved`, whose message names the cause. The causes and their retry rules are HBEP-3's (Deployment ID, "When the ID cannot be computed", and the collision rule): transient Hub failures are retried with backoff and set no reason; a permanent Hub failure is evaluated again when the spec or token changes and at resync; a collision is evaluated again only when the spec or the identity records change, so it does not loop.
3. **Observe** its Allocations (by the `model-deployment` label), its Instances (by owner), and each agent's latest report.
4. **Shutdown.** At the end time, on deletion, or at zero desired instances, apply Release's shutdown case and stop here.
5. **Failed Allocations.** An Allocation whose provider phase is `Failed` (a requeue included), `Lost` or `Refused`, or which has the condition `SubmittedAsOther`, ends its Instance with that reason and is deleted; its finalizer cancels whatever carries its tag (HBEP-4). A `Lost` job is absent from the queue listing, so it is neither pending nor running. An Allocation that is `Ended` while its Instance is not `ended` ends the Instance with reason `AllocationEndedEarly`, raises a `failing` event with that reason, and is deleted; step 7 then provisions the replacement. An Instance already past its hard end ends as Handover's draining says, without that reason.
6. **Instances.** Advance each Instance through its phases (Instance start), apply the health rules (Health and restarts) and the drain deadline (Handover).
7. **Capacity.** A primary Instance is one that is not draining or ended, and that has no predecessor still counted.
   - Fewer primaries than desired: create one Allocation and its Instance, in phase `pending`, per missing primary (Provisioning).
   - More primaries than desired: scale down. Delete pending Allocations first, newest first; then drain the ready primaries with the nearest hard end. A surplus primary's replacement, if any, is deleted with it, never promoted.
8. **Replacements.** Every primary whose Allocation is `Running`, that has no live replacement, and whose replacement's planned submission time has come gets one (Handover).
9. **Retention.** Apply each empty Allocation's retention policy (Retention), and place Instances on warm Allocations where question 14 allows.
10. **Status.** Write the ready and desired counts, the current IDs, every ID that still has Instances, each class's last measured start-to-ready time, the conditions, and the generation reconciled with its time.
11. **Requeue** the key at the earliest coming deadline (a drain start, a replacement's planned submission time or lateness deadline, a registration or start timeout, a retention end), and after 30 s at the latest.

### Provisioning

- **Where.** A new primary goes to the first allowed Provider, in order, that is `Ready`, is batch, and whose partition's class has a current ID. A replacement goes to its predecessor's Provider first, which keeps the ID, and otherwise to the next one in order.
- **The Allocation's submission parameters** (HBEP-4, Submission):
  - `partition`: the Provider's one allowed partition;
  - `gpus`: `tensor-parallel-size` times `pipeline-parallel-size` from the serving spec, each 1 when absent. It must not exceed `gpusPerNode`, since alpha Instances are single-node. A `data-parallel-size` above 1 is refused, with `Degraded` reason `SpecUnsupported` and no Allocation, because alpha runs one engine worker per Instance. Beta sizes GPUs as DP × TP × PP;
  - `cpusPerTask` and `memory`: `gpus` times `cpusPerGpu` and `memPerGpu`;
  - `timeLimit`: the partition's `maxTime`, or `leaseLimit` when shorter;
  - `nodes`: 1;
  - `agentDigest`: the allocation agent's build for the partition's `cpuArch`.
- **Its retention policy** is the ModelDeployment's.
- **Its credential.** The controller generates a random 256-bit token per Allocation and keeps it in a mode-600 file on the control-plane host, named by the Allocation's UID (HBEP-11, Credentials). The Provider's controller places it on the platform (HBEP-4).
- **The Instance** is created with its Allocation, with the class's current ID and its `launch`. In alpha an Allocation's assignment never changes, so both exist from the start.
- **No front, no submission.** While no front address and dial-home mode are configured, no Allocation is submitted, and the ModelDeployment has `Degraded` with reason `FrontNotConfigured` (HBEP-11, Single-user host).
- **A Rejected submission** is retried by the Provider's controller with backoff (HBEP-4). The lifecycle controller marks the deployment degraded, with Slurm's reason. It does not switch providers by itself in alpha.
- **The hedge winner** is the first Allocation whose Instance serves (open question 1). In alpha each missing Instance has one Allocation, so the rule decides nothing until hedging arrives.

### The allocation agent

The process that an Allocation's job runs. It is a static binary, one build per CPU architecture, staged by digest (HBEP-4). It is a new term, added to HBEP-3's vocabulary by amendment. It is the framework's in-allocation half; the controller decides, and the agent carries out.
- **Start.** The batch script's bootstrap checks the binary's digest and runs it with the Allocation's directory (HBEP-4). Nothing else runs in the job.
- **Registration.** The agent connects to the control-plane host over HBEP-6's dial-home path. In alpha that is direct on the dial-home port, or through ssh on the configured ssh port, 22 by default, with a restricted key (HBEP-11, open question 8).
  - It presents the Allocation's credential, read from its mode-600 file, with its job ID, its node's CPU architecture and its GPUs.
  - The endpoint accepts it only when the credential is the Allocation's and the job ID is the one the provider recorded. That binds the registration to the provider's identity for the job (HBEP-6).
  - **Its identity** (science-agent decision, 31 Oct 2026). Each agent process draws a random `agentInstanceID` at its start, 32 lowercase hex characters, and sends it on every call. The registration also carries the scheduler's `restartCount` of the job, from `SLURM_RESTART_COUNT`, 0 when the scheduler sets none. The controller keeps both in the Instance's `status.agent`. A registration from the job the Instance was started under, with another `agentInstanceID` or a higher `restartCount`, is a second execution of the job: a requeue (Health and restarts). The kernel's boot ID is never used, because a requeued job can land on the same node without a reboot.
  - **Registration is soft state** (science-agent decision, 31 Oct 2026). The controller keeps registrations in memory. After its own restart it answers each agent's next call as not registered, and the agent registers again at once, within one poll, with the same `agentInstanceID`. That is the same execution: no Instance fails and no generation is raised. Until a long poll and a report interval have passed since its start, a restarted controller judges neither a registration timeout nor `AgentUnreachable`.
  - It reads the control-plane host's address, the dial-home mode, the pin of the agent endpoint's certificate, and the absolute staging and dial-home directories from `agent.json` in its directory (HBEP-4, Submission).
  - An agent that has not registered within `health.registrationTimeout` (15 min by default) of its job's start fails its Instance, with reason `RegistrationTimeout`, and the Allocation is released.
  - In ssh mode the agent keeps two forwards itself, `ssh -N -L` over the configured ssh port, 22 by default, each in its own reconnect loop, as spike S1's kit does. The port is configurable because the development VM's sshd is reached through a port forward on a gateway, at the gateway's address and port (owner decision, 31 Oct 2026; HBEP-6). Connection 1 forwards to the agent endpoint's loopback port, for the agent's control traffic. Connection 2 forwards to the front's dial-home listener's loopback port, for frpc's tunnel (HBEP-6). Two connections keep a large report off the token tunnel, since streams sharing one connection block each other (HBEP-6, Risks). Each local end is a loopback port the kernel chooses once, at the agent's start, and the agent keeps it for the Allocation's life. frpc starts only once connection 2's forward listens. If a port cannot be bound again and a new one is taken, the agent points its control client at it (connection 1), or rewrites frpc's configuration and restarts frpc (connection 2) (HBEP-6, Dial-home modes). Both use one dedicated, restricted key that the operator generated on the platform once, so that its private half never travels, and a pinned host key. Both are read by path, mode 600, from `~/.hpcburst/dial-home/` on the cluster, whose absolute path `agent.json` carries. They live outside `stagingDir`, on storage that is not purged, because Helios purges scratch files older than 30 days (spike S5, PR #19). The Provider's controller checks that they exist before any submission, and sets `DialHomeKeyMissing` otherwise (HBEP-4). When an Instance's registration over the dial-home path fails, a home directory that compute nodes cannot read included, the lifecycle controller sets `DialHomeUnreachable` on the Provider (HBEP-4, open question 16). The key's line in `authorized_keys` on the control-plane host (owner decision, 31 Oct 2026), one key per cluster, is `restrict,port-forwarding,permitopen="127.0.0.1:<agent endpoint port>",permitopen="127.0.0.1:<dial-home listener port>",permitlisten="127.0.0.1:1",command="/bin/false"`. The two ports are the launcher's port base + 3 and + 7: the agent endpoint and the front's dial-home listener, which in ssh mode defaults to loopback (`dialHomeListen`, HBEP-5). No ssh forward from a compute node has run yet (HBEP-6).
- **Desired state, pulled.** The agent long-polls for what its Allocation should run now: nothing, one Instance with its `launch` and the restart generation of each part, or stop. For that Instance it also says whether to verify, qualify or warm up now, with the identity record's file list and the workload's shapes. It converges on that state, as a kubelet converges on its pods, so a missed message is corrected by the next poll.
  - **The start instruction carries the file list** (science-agent ruling, 31 Oct 2026, for #87). For a first start and for a rung-2 restage, the desired state for that Instance generation carries `Checks.Verify` with `Files`. The agent starts no worker without it, so a worker never starts on unstaged files. It reports staging in progress with a message that the file list is awaited, and an Instance whose list never comes fails at `health.startTimeout` (Instance start). A worker restart (rung 1) compares settings only, so its `Checks.Verify` carries no files.
- **Reports.** At least every 15 s, and at every change, the agent posts:
  - for each part: its phase, process ID and start time, exit code, and the restart generation it applied;
  - staging progress;
  - for verification: the file rehash results, the engine's resolved settings, its version and the image digest it ran;
  - for qualification and warm-up: each check's outcome, with a failure's reason code in `CheckOutcome.Reason` and, for the behaviour check, the context it ran at in `CheckOutcome.ContextTokens`; the timings of every request by shape and round, the KV capacity the engine reported at start-up, the served model names, and the measured template overhead;
  - for health: a local health probe every `health.probeInterval`, and from the engine's metrics, the running and waiting requests, the generated-token counter, the KV-cache use and the responses by HTTP status, with `metricsReadAt`, the time of the last successful `/metrics` read. A gauge left out is unknown, never zero (science-agent ruling, 31 Oct 2026, for #87).

  A report is a full snapshot, so its size is bounded (science-agent decision, 31 Oct 2026). Both sides refuse a body above 4 MiB. `targetSessions` is at most 1024, and the agent cuts free-text fields short, so a report keeps the timings of all five warm-up rounds within the limit.
- **What it runs.**
  - Staging. It pulls the image by digest into `<stagingDir>/images/`, in the partition's `imageFormat`, which the desired state carries: a SIF file, or a sandbox directory built from the same digest where the partition cannot run a SIF, as on Athena A100 (HBEP-4, open question 15). Both run the same image, so the deployment ID does not change. It downloads missing checkpoint files from the Hub at the full commit id into `<stagingDir>/checkpoints/<repository>/<revision>/`. Each file goes to a temporary name, then is renamed into place, so two Allocations staging at once never see a partial file.
  - The worker. It starts it with Apptainer from the staged image, with GPU support, the checkpoint bound read-only, and a clean environment plus the launch's variables. The engine listens on a Unix socket in the job's private temporary directory, so no port can clash on a shared node (L-7.7). It runs with `--served-model-name` set to the ModelDeployment's model name, which the front's name rewrite relies on (HBEP-5), and with the Instance's own key in `VLLM_API_KEY`, which the agent sets from the key file; the key is never written in the spec (HBEP-3). It runs with `--max-num-seqs` set to its `launch.maxNumSeqs`, which is always set (What the controller reads). The Instance's serving path is frpc's tunnel, which dials that socket (HBEP-6).
  - The tunnel (HBEP-6). When it is told to run an Instance, the agent checks the staged frpc's SHA-256 and the staged launcher certificate's pin against `agent.json`, writes frpc's configuration, with the Allocation's credential, into the job's private temporary directory with mode 600, and starts frpc. frpc serves one proxy named by the Instance's UID from the engine's socket, and logs only to a file in the job's private temporary directory, never to the job's output files, because its log names the session's run ID (HBEP-6, Risks). The agent stops frpc when the Instance ends. frpc and the ssh forwards are the agent's transport, not parts of the Instance: the agent restarts them with backoff when they exit, without a controller decision, and a tunnel down is not a dead Instance.
  - Checks. When the desired state asks it to verify, qualify or warm up, the agent runs the checks and the warm-up rounds against the engine's local socket, with synthetic prompts, and reports each outcome. The controller judges the reports: it applies the thresholds and sets the verdicts (Instance start).
  - Restarts and stops. When a part's desired generation rises, it stops the part politely, kills it after `restart.stopGrace`, and starts it again from the same `launch`. It never restarts a part by itself: restarts are the controller's decisions, recorded first.
- **Keys.** The agent generates the Instance's key at Instance start (HBEP-9), passes it to the engine through the environment, and sends it over its channel until the desired state reports it stored (`keyStored`), so a lost report never leaves the front without the key. The controller keeps it in a mode-600 file named by the Instance's UID, which the front reads. The key file holds exactly 64 lowercase hex characters (256 random bits), with no newline, and is written atomically: a temporary file in the same directory, flushed, then renamed into place (science-agent decision, 31 Oct 2026). A reader refuses any other form. A Hub token for a gated checkpoint comes over the same channel. When the download runs in the agent's own process, the token stays in the agent's memory. When a child process downloads, it gets the token through its environment. The token is never on a command line or in a file on the platform (science-agent ruling, 31 Oct 2026, for #87). On the control-plane host it is a mode-600 credentials file (HBEP-11, Credentials; HBEP-3).
- **After the Instance.** The agent stays when its Instance ends, and idles until it is told to stop or the job reaches its hard end. That is what lets a kept Allocation host a later Instance.
- **Unreachable.** An agent that sends no report for `health.agentTimeout` makes its Instance's condition `AgentUnreachable`. The front stops routing new requests to it but keeps counting the ones it has (HBEP-5). The controller restarts nothing on that basis alone (HBEP-6: a path that is down is not a dead Instance). It returns when the reports resume. Its lease still ends on time, and the drain rule still applies.
- **The endpoint,** in the lifecycle controller, has three calls over HTTPS: `POST /agent/v1/register`, `GET /agent/v1/desired` (a long poll from the last generation seen) and `POST /agent/v1/report`. The version is in the path. The agent's own HTTPS connection carries them, never frpc's tunnel (HBEP-6).
  - It listens on loopback on the control-plane host. In ssh mode the agent reaches it through its first forward. In direct mode the front passes `/agent/v1/` through, on the dial-home port, to that loopback endpoint.
  - The launcher creates the endpoint's certificate, and `agent.json` carries its pin (HBEP-11). In direct mode the front's dial-home listener presents the same certificate, so one pin holds on both paths.
- **The frps plugin** (HBEP-6, Admission). The lifecycle controller's process also serves frps's server plugin, on loopback at the launcher's port base + 8, never on the agent endpoint's port, which the restricted key opens to every job. It handles `Login`, `NewProxy` and `CloseProxy`, never `Ping`, so a controller restart cuts no established tunnel; while it is down, a reconnecting frpc cannot log in and retries. It admits a tunnel's login by the Allocation's credential, and a proxy only for the UID of an Instance placed on that Allocation. It answers within 1 s from in-memory state, and refuses rather than waiting on the API server, because frps's plugin client has no timeout. The lifecycle controller writes each Instance's `TunnelConnected` condition from frps's proxy list (HBEP-6, Path state).

### Instance start

The Instance phases are HBEP-3's.
- **`pending`** until its Allocation is `Running` and its agent has registered. Registration has its own deadline, `health.registrationTimeout` from the job's start (The allocation agent).
- **`starting`:** the agent stages what is missing, rehashes the checkpoint files present, and starts the worker. An Instance that does not reach `ready` within `health.startTimeout` of its own start instruction fails (Health and restarts). When its `TunnelConnected` is not True at that moment, its reason is `TunnelNotConnected` (a proposed name), otherwise `StartTimeout`; the usual release, replacement and `RestartLoop` counting follow either way. `DialHomeUnreachable` keeps its own scope, a registration that never came (HBEP-4, open question 16). The start instruction is the first desired state that tells the agent to run that Instance, and its time is kept in the Instance's status, so the deadline survives a controller restart. An Instance placed on an older warm Allocation therefore gets the full timeout.
- **`verifying`.** Verification is HBEP-9's alpha checks, the file rehash and the settings comparison:
  - **File rehash:** every checkpoint file present matches its entry in the identity record. A listed file that was never staged, because the engine does not load it, is not a failure (HBEP-3).
  - **Settings comparison:** the engine version, the image digest and `max-model-len` must be reported. The version and the digest must equal the declared ones, and `max-model-len` must equal its declared value when the spec declares one. Every other declared input that the engine reports must equal its declared value, compared by value under one fixed rule (science-agent decision, 31 Oct 2026; PR #91):
    - **What vLLM v0.32.0 reports** is `str()` of each parsed, typed field, after its model hooks (`CacheConfig.metrics_info()`, `vllm/config/cache.py` lines 322–325; the hooks run in `VllmConfig.__post_init__`). So a boolean reads `True` or `False`, a float `4.0`, and some values are rewritten.
    - **The rule.** If both sides read as booleans (`true` or `false`, in any case), they are compared as booleans. Otherwise, if both read as decimal numbers, they are compared as numbers. Otherwise they are compared verbatim, as strings, so an alias such as `fp8` for `fp8_e4m3` is a contradiction, as the deployment ID treats it as another deployment.
    - **A declared `auto`** asks the engine to choose. It is not compared: the reported value is recorded as resolved. For example, `kv-cache-dtype: auto` can report `fp8_e4m3` for a checkpoint whose KV cache is FP8.
    - **`max-model-len`** is compared as an integer. Admission accepts it only as a plain positive decimal integer, refusing vLLM's `32k`, `auto` and `-1` forms, whose meaning only vLLM's parser knows.
    - **A value that a model hook rewrites** (in v0.32.0, `DeepseekV32ForCausalLM` reports a declared `kv-cache-dtype: bfloat16` as `auto`) is a contradiction under this rule, and fails verification with the setting and both values named. The operator then declares the form the engine reports. This is a known limit of alpha. A declared input it does not report is recorded as not reported. A setting it reports that the spec does not declare is recorded (HBEP-3).
  - The engine fingerprint is recorded. HBEP-3 mentions a render check through the template and tokenizer; it is HBEP-9's candidate, and not in alpha.
  - **One record per verification.** Each verification, at start and after every restart that reverifies, writes its own immutable verification record, with the reported settings, the measured template overhead, the GPU type, the fingerprint, the rehash outcome and the verdict, before the Instance can become ready (HBEP-11). The shape-misfit stop reads the context limit and the overhead from it. It is named `ver-<instance UID>-<generation>`, where the generation counts the Instance's verifications from 1. The Instance's status names the latest record. A record that already exists under its name, as after a controller restart, is read and never rewritten.
  - A failed rehash moves the staged copy aside and restarts the Instance at the whole-Instance rung, which stages and rehashes again. A second failure fails the Instance.
  - A settings contradiction fails the Instance at once. Provisioning in that class stops, with the condition `VerificationFailed` and an event, until the serving spec changes (Risks). The stop is keyed on the deployment ID alone, because only a spec change can fix it (science-agent ruling, 31 Oct 2026, for #88).
  - A shape that does not fit (What the controller reads) also fails the Instance at once and stops provisioning in the class, with `VerificationFailed` and an event. This stop is keyed on the deployment ID plus a hash of the resolved shapes. At each reconcile the controller checks the current shapes against the context limit and template overhead that the failed verification record holds. A `workload` edit that makes them fit lifts the stop, with one event (reason `ShapesFit`, proposed in PR #88). An edit to another misfit keeps it stopped, with a new event naming the new shape (science-agent ruling, 31 Oct 2026, for #88).
- **Qualification,** still in `verifying` (open question 12). The agent runs the checks and reports each outcome; the controller judges them. With k = 0, every Instance passes:
  - health, the engine version, the served model names and the maximum length;
  - one chat request, whose engine fingerprint is recorded;
  - a tool-call smoke test, when `workload.toolCalls` is set;
  - KV capacity from start-up of at least `targetSessions` times `contextTokens.typical` tokens;
  - the corruption check: one greedy request sent three times in a row gives three outputs that share their first 32 tokens (every token, if an output is shorter), and none is empty or a single repeated token. Identical text is not required, because vLLM does not guarantee reproducible results by default ([reproducibility](https://docs.vllm.ai/en/latest/usage/reproducibility.html), accessed 2026-10-30);
  - the behaviour check: one request at `contextTokens.longest` with the typical output returns a well-formed answer, and a well-formed tool call when tool calls are on, with no loop. A reasoning model's prompt is shorter (Reasoning models, below).

  A failed check fails the Instance, and its Allocation is released. A failure's reason code is in `CheckOutcome.Reason`, and the controller matches on that code, never on the message.
- **Warm-up,** still in `verifying` (open question 5). The agent runs the rounds and reports their timings; the controller decides when warm-up is done. Synthetic prompts only. Each round sends:
  - one request at each of the three context lengths with the typical output;
  - one at the typical context with the longest output;
  - one tool call, when tool calls are on;
  - then `targetSessions` concurrent requests of the typical shape.

  Warm-up is done when no shape in a round is more than 10% slower than in the round before; the tolerance absorbs timing noise. A warm-up that has not settled after five rounds fails qualification. The last round's time per output token and time to first token are recorded as the Instance's throughput at the target sessions.
- **Reasoning models** (science-agent ruling, 31 Oct 2026, for #87). These rules hold when the serving spec declares `reasoning-parser`, so the engine splits each reply into reasoning and a final answer:
  - time to first token counts the first delta of either kind, reasoning or content;
  - the tool-call smoke test and the behaviour check judge only the final answer (`content` and `tool_calls`), never the reasoning text;
  - each of those two checks asks for its own answer budget plus 4,096 reasoning tokens (`agentapi.ReasoningHeadroomTokens`). To make room, only the behaviour check's prompt is shortened: to max-model-len − template overhead − the typical output − 4,096, at most `contextTokens.longest` and at least `contextTokens.typical` (`agentapi.BehaviourContextTokens`). The max-model-len is the one the engine reported at verification. The agent reports the context the check actually ran at in `CheckOutcome.ContextTokens`, and the controller records it;
  - a reply that spends the whole budget reasoning, with no final answer, fails the check with reason `ReasoningBudgetExhausted` in `CheckOutcome.Reason`. It is never counted as a wrong answer. The Instance fails like any failed check, and the usual restart and `RestartLoop` limits bound repeats. It does not stop provisioning in the class;
  - warm-up shapes count reasoning tokens as output;
  - the fixed 4,096 is a known limit of alpha. A per-deployment setting comes after alpha.
- **`ready`** once verification, qualification and warm-up pass and its tunnel is connected. The conditions `Verified`, `Qualified`, `WarmedUp` and `TunnelConnected` are true, the latest verification record exists and passed, and the front routes to the Instance once its probe passes (HBEP-5, HBEP-6).

### Health and restarts

- **Malfunction signals,** from the agent's reports, with the profile's `health` thresholds:
  - the health probe fails `failureThreshold` times in a row;
  - only errors: over `errorOnlyWindow`, the engine answered requests and every answer was a server error;
  - no token progress: requests running, and the generated-token counter unchanged for `noProgressTimeout`. A request in prefill generates no token, so in alpha that time-out is 30 min, equal to the front's `idleTimeout`, and a prefill the front allows is never restarted (open question 6);
  - a part exits without being told to, whatever its exit code. A clean exit is a failure too (L-2.9);
  - one worker at a full KV cache with requests waiting while another idles, for `kvSkewTimeout`. This needs several workers, so it is not in alpha.

  Plain load never triggers a restart: long queues, high latency, or a full KV cache on the only worker with requests waiting.
- **Only fresh gauges act** (science-agent ruling, 31 Oct 2026, for #87). The controller takes no gauge-based action unless the gauges are fresh. Such actions are the idle anchor (through a draining Instance's end, Handover), KV pressure, the only-errors signal and the no-progress signal. Fresh means each of three ages is within two probe intervals (`health.probeInterval`): the read at sampling (the sample's time minus `metricsReadAt`), the sample at the report, and the report since receipt, on the controller's clock (`Health.MetricsFresh`). Stale or missing gauges are unknown, so no such action is taken on them, and a zero running-request count from a stale or missing gauge never feeds the idle anchor. The no-progress signal compares two samples' counters only when `metricsReadAt` advanced between them, so a run of failed reads does not look like a stall.
- **The alpha ladder:**
  1. restart the worker: the settings comparison and warm-up run again, with a new verification record;
  2. restart the whole Instance in its Allocation: stop, rehash, start, verify, qualify and warm up;
  3. a new Allocation: the Instance fails, a replacement Allocation and Instance are created (Provisioning), and the failed Allocation is released at once, since nothing on it serves.

  The router rung comes first once Instances have routers (HBEP-5, open question 2).
- **Attempts.** Each rung is tried `restart.attemptsPerRung` times within `restart.window`, `restart.backoff` apart, before the next rung.
- **Recorded first.** Before each restart, the controller emits an event (type `failing`, the rung as reason, naming the signal and the controller as actor), records the restart in the Instance's `status.restarts`, then raises that part's restart generation on the Instance by compare-and-swap. The agent acts on the generation. The event says that in-flight requests were cut. The attempts per rung are counted from `status.restarts`, so they survive a controller restart.
- **Not routable while restarting.** A restarting Instance leaves `ready` for `verifying`, so the front stops routing new requests to it, and returns after its checks. The front keeps it in its balancer group, and the cut requests leave its count as they fail (HBEP-5).
- **Restart and provisioning loops.** When three Allocations in a row in one class fail before or at rung 3 within `restart.window` (a refused or failed job, a failed check, or a failed whole-Instance restart), the deployment is degraded. Provisioning in that class pauses with backoff, and an event fires.
- **A requeue** fails the Instance, and its Allocation is deleted (HBEP-3, open question 13). The controller sees it from the provider's phase (`Failed`, reason `Requeued`), or from a second execution's registration: another `agentInstanceID`, or a higher `restartCount` (The allocation agent). Nothing from the first execution is reused.

### Handover

- **When the replacement is submitted** (open question 2). For each primary, the replacement Allocation and Instance are created at the primary's drain start minus (the p90 of the Provider's observed queue waits + the class's start-to-ready time + `handoverMargin`), and never before the primary's Allocation is `Running`.
  - The p90 is taken over the last 20 waits in the Provider's status (HBEP-4). With fewer than 20, the largest observed wait is used. With none, the replacement is created as soon as the primary's Allocation is `Running`.
  - The start-to-ready time is the class's last measured time from an Allocation's start to the first `ready` of the Instance created with it, which the ModelDeployment's status keeps. It is 30 min until one is measured.
  - Both inputs live in status, never only in memory (HBEP-11), so a restarted controller plans the same time.
  - On a short queue the replacement is ready about the margin before the drain starts, so the overlap is about the margin. On a long queue the plan reaches back to the primary's start, as in past operation.
  - The replacement carries an annotation naming its predecessor. Each primary has at most one live replacement.
- **Both serve.** When the replacement becomes `ready` before the primary's drain starts, both serve, and the front balances across them (open question 4). At the primary's drain start, the replacement becomes the primary, and its own replacement is planned the same way.
- **Lateness.** When the replacement's Allocation has not started by the predecessor's drain start minus the class's start-to-ready time, the deployment gets the condition `ReplacementLate` and an event. A replacement that starts later cannot be ready before the drain starts.
- **Same class, same ID.** A replacement in the same accelerator class is a new Instance of the same deployment ID. In another class, it is a new Instance with that class's current ID, and a name pinned to the old ID does not reach it (HBEP-3). The predecessor is released only after the replacement serves.

Drain rule (the same text in HBEP-3, HBEP-5 and HBEP-7): The framework cannot move a lease end, so an Instance starts draining at the latest at its hard end minus the drain window, whether or not its replacement is ready. Make-before-break is the goal: the controller plans the replacement to be ready before that drain start, and the old Instance never drains before its replacement is ready unless that deadline forces it. If the replacement is late, the old Instance still drains on time, capacity drops, and an event is emitted (HBEP-8).

- **Draining.** At its drain start, the hard end minus the Instance's drain window, the Instance becomes `draining`. The window is the profile's `drainWindow`, or the ModelDeployment's `routing.maxRequestDuration` plus the profile's `drainMargin` when that is shorter, as the front computes it (HBEP-5, Draining). The front also enforces this deadline itself, from the hard end copied into `status.leaseEnd` (HBEP-5). It becomes `ended` when the agent's fresh gauges report no running request (Health and restarts: Only fresh gauges act), or at the hard end. A stale or missing gauge is unknown, so it never ends a draining Instance, and never starts the idle count through `endTime` (Retention). The controller then tells the agent to stop the worker and removes the Instance's drain finalizer (HBEP-11). The Allocation follows its retention policy until its hard end.
- **A late replacement** leaves the deployment degraded (open question 10).

### Release

Replacement before release (HBEP-3) applies only while the deployment keeps its desired capacity:
- Handover, and replacing a failing instance, while desired instances are above zero: the old instance is released only after its replacement serves. An Allocation whose Instance failed serves nothing, so it is released at once.
- Scale-down: the surplus instances drain and end, without replacement.
- Shutdown at the deployment's end time: every instance drains and ends, and pending Allocations are cancelled, without replacement.
- Deletion of the ModelDeployment: as shutdown, then the objects are removed (HBEP-11).
- Approved early termination (HBEP-10): as shutdown, before the end time.
- Repurposing: as scale-down for the old deployment, which is ending or scaling down. The Allocation then hosts the new deployment's Instance.
- In every case, an Allocation left with no Instance follows its retention policy (Retention).
- A lease end is not a release: under the drain rule, an instance whose replacement is late drains and ends with its lease.

Releasing an Allocation means deleting it. Its finalizer cancels every job with its tag (HBEP-4, HBEP-11).

### Retention

An Allocation that hosts no Instance follows its retention policy:
- keep it, as warm capacity, until a date, a grant's end or a deadline;
- or release it when idle, after an idle limit.

The default is keep until the deployment's end time, or until the grant's end or the deadline when one is set (open question 13). A kept Allocation still ends at its hard end. Where the profile sets `maxIdle`, an empty Allocation is released after that long even under keep. Its agent idles meanwhile. Placing Instances on warm capacity follows open question 14.

**The idle anchor** (science-agent decision, 31 Oct 2026). Retention's `idleLimit` and the profile's `maxIdle` count from the Allocation's annotation `hpcburst.groundnuty.github.io/idle-since`, which only the lifecycle controller writes, with a resourceVersion precondition:
- when the Allocation's last Instance ends, it is set to the latest `status.endTime` of the Allocation's Instances;
- when the job starts with no Instance placed on it, it is set to the job's start (`status.atProvider.startTime`);
- when an Instance is placed on the Allocation, it is removed.

An Allocation is idle exactly while it carries the annotation. The anchor lives on the Allocation, not on an Instance, because Instances can be deleted while their Allocation is kept, and it survives a controller restart.

### Repurposing

An operator, often an agent that needs throughput across several models, may move an Allocation to another ModelDeployment when a model's work finishes. The controller then:
1. drains the old Instance;
2. starts an Instance of the new deployment in the same Allocation;
3. verifies and warms it up before it takes traffic.

The Allocation keeps its name, and the new Instance has the new deployment's ID. Conversions between models are this operation. Fit and cost: open question 7. Approval: HBEP-10. Not in alpha: an alpha Allocation stays with the ModelDeployment that created it.

### Events and conditions

- **Conditions are the level truth** (HBEP-11). Events are best-effort Kubernetes Events (`events.k8s.io/v1`) in alpha. Their `action` carries HBEP-8's type and their `reason` the reason code alone; their `type` is `Warning` for `degraded` and `failing`, and `Normal` otherwise (HBEP-8, Events).
- **On a ModelDeployment:**
  - `Ready`: ready Instances at least as many as desired, with reason `EnoughReady` when true and `NotEnoughReady` when false. For a managed source, the catalog entry is listed and its Provider's key is not rejected, with reason `CatalogEntryUnlisted`, `CredentialsRejected` or `ProviderNotOnPlatform` when false (Reconcile loop, step 1);
  - `Degraded`, true with one of these reasons, and false with `AsDesired`:
    - `ReplacementLate`, `CapacityDropped`, `RestartLoop`, `VerificationFailed`, `SubmissionRejected` and `ProviderUnavailable`;
    - `SpecUnsupported`: a `data-parallel-size` above 1, or a workload shape that does not fit the declared context limit. The operator edits the spec;
    - `FrontNotConfigured`: no front address and dial-home mode are configured, so no Allocation is submitted (Provisioning; HBEP-11);
    - `IdentityUnresolved`: a class's deployment ID cannot be computed, from a permanent Hub failure or a display-form collision, with HBEP-3's causes and retry rules. The operator fixes the token or the revision, or investigates the colliding record (Reconcile loop, step 2; HBEP-3);
  - `Progressing`: true with reason `Provisioning` while the controller creates, starts or replaces Allocations and Instances, and false with `Stable`; false with reason `ManagedSource` for a managed source, which has no Instances by design.
- **On an Instance:** `Verified`, `Qualified`, `WarmedUp`, `Healthy`, `AgentUnreachable` and `TunnelConnected` (a proposed name, written from frps's proxy list; HBEP-6, Path state). A qualification with no throughput reference records that in `Qualified`'s message.
- **Events:**
  - `started`, `ready`, `degraded`, `failing` and `replaced`, as their HBEP-8 types;
  - `ending-soon`, when a ready Instance's drain start is less than an hour away and no replacement is ready;
  - `failing` with reason `HeartbeatMissed` when an Instance's `AgentUnreachable` turns True, when a Provider's last successful read is older than `ProviderReadyStaleness`, and when the front's `Lease` is stale, once per episode (HBEP-8, Observers);
  - `degraded` with reason `ObservationGap` once at start, regarding the `lifecycle` `Lease` (HBEP-8, Gaps after a restart).
- **Tests of events** check for at least one event of a type and reason on a resource, since client-go's recorder merges isomorphic events and keeps only the first note (HBEP-8, What the recorder merges and drops). A message is checked on the condition.

### Test plan

Against the fake Slurm of spike S4, through the Slurm plugin (HBEP-4). The fake has queue delays, start failures, hard ends, requeues, finished jobs purged from the queue listing within seconds, and local-time clocks. A fake agent, and an engine double behind a real agent, stand in for the job:
- Hedging, once hedging is on: the first Allocation whose Instance serves is kept, a loser that starts earlier but fails its checks is released, and the pending losers are cancelled only then.
- Provisioning: the submission parameters follow the serving spec and the profile (GPUs, CPUs, memory, and the time limit capped by `leaseLimit`), and a GPU count above `gpusPerNode` is refused.
- The replacement is created at its planned time, never before the primary's Allocation starts, and never twice for one primary. With no wait observed on the Provider, that is the primary's start. With observed waits, it is the drain start minus the p90 wait (the largest wait while fewer than 20 are observed), the class's start-to-ready time and the margin. The expected times are worked out by hand from the fake's fixed waits, never by the planner's own code. On a short queue the replacement is ready about the margin before the drain starts.
- The controller restarts between the primary's start and the planned time: it plans the same time from the waits and start-to-ready times in status.
- Both serve, then the primary drains at its drain start, not earlier, and its replacement's own replacement is planned the same way.
- The replacement fails verification: the predecessor keeps serving until its hard end minus the drain window, not earlier, then drains. No request is cut at the hard end, and a degraded event fires.
- The replacement's Allocation has not started by the predecessor's drain start minus the class's start-to-ready time: `ReplacementLate` is set.
- An Allocation ends early (a cancel outside the framework) while its Instance serves: the Instance ends with `AllocationEndedEarly`, a `failing` event fires, the Allocation is deleted, and a replacement is provisioned. An Allocation that reaches its hard end with its Instance draining raises no such event.
- Scale-down, shutdown at the end time and deletion: instances drain and end, no replacement is submitted, and each Allocation follows its retention policy.
- Retention: an Allocation kept until a date stays, with no Instance, until that date and is then released; one under release-when-idle is released after its idle limit; one on a profile with `maxIdle` is released after it under keep; none outlives its hard end. An Allocation whose ended Instance is deleted, while the Allocation is kept, is still released at `maxIdle` counted from that Instance's `endTime`, read from the `idle-since` annotation after a controller restart; placing an Instance removes the annotation.
- The controller is killed at each step of a handover: after restart there are no duplicate submissions and no orphan allocations.
- The reply to a submission is lost: the attempt is Unknown, never Failed, the controller resolves it by tag, and exactly one job with the tag has its hold lifted.
- Two controllers reconcile one Allocation at once: no Allocation has more than one job whose hold is lifted, held duplicates are cancelled, and no more allocations than desired remain.
- In fallback mode, an Unknown attempt with no job found raises the operator condition after the Unknown bound, and no second job is submitted until an operator abandons the attempt.
- A recorded job absent from the queue listing and accounting is not Lost before `lostGrace`, is Lost after it, and the lifecycle controller then replaces it and deletes the Allocation.
- The scheduler requeues a serving allocation under the same job ID: no registration or verification from the first execution is reused, no replacement is planned twice, the Instance fails, the job's hold is never lifted again, the Allocation is deleted and its job cancelled, and the replacement is a new Allocation (HBEP-3, open question 13).
- A requeued job lands on the same node, with no reboot: its agent registers with a new `agentInstanceID` and a higher `restartCount`, and the Instance fails as a second execution. A fixture that changes the boot ID proves nothing here, since the controller never reads it.
- The controller restarts while agents serve: each agent registers again on its next poll with the same `agentInstanceID`. No established tunnel drops and no stream through it is cut (HBEP-6). No Instance fails, no restart generation rises, and no registration timeout or `AgentUnreachable` is judged before a long poll and a report interval have passed.
- A hedge across two accelerator classes, once hedging is on: the winner's Instance carries its class's current ID.
- A job ends and leaves the queue listing seconds later: the controller reads it as ended, from accounting, and submits nothing again for it.
- No instance gets traffic before it is ready.
- Spec checks: a `data-parallel-size` of 2 is refused with `SpecUnsupported`, and no Allocation is created. A workload whose longest context and typical output exceed a declared `max-model-len` is refused the same way. With the default longest context, every rendered warm-up request fits the limit the engine double reports.
- Timeouts: an Instance placed on a warm Allocation that started more than `health.startTimeout` ago gets the full timeout from its own start instruction. An agent that never registers fails its Instance after `health.registrationTimeout`. With a restricted key that lacks the second `permitopen`, the agent registers but frpc never connects: the Instance never becomes `ready`, fails at `health.startTimeout` with `TunnelNotConnected`, and is never counted as ready capacity. In ssh mode, when connection 2's local port is taken and a new one bound, frpc is restarted and logs in on the new port (HBEP-6).
- Verification records: a worker restart and a whole-Instance restart each write a new record, `ver-<instance UID>-2` then `-3`; the Instance's status names the latest, and the earlier records are unchanged. A controller killed after writing a record and before updating status reads that record on restart and writes no second one.
- Verification: a changed checkpoint file fails the rehash, is re-staged once, then fails the Instance; an engine that reports another `max-model-len` fails at once and stops provisioning in that class; an engine that reports nothing fails, because the version, the image digest and the context length are required.
- Qualification: an engine double with too little KV capacity, one whose three greedy outputs differ within their first 32 tokens, one that returns a single repeated token, and one that loops at the long context each fail, and their Allocations are released. One whose greedy outputs differ only after the 32nd token passes.
- Warm-up: an engine double that is slow on the first request of each shape settles in two rounds; one whose latency never settles fails after five.
- Shape misfit: an engine that reports a context limit the default workload does not fit fails verification and stops provisioning in the class with `VerificationFailed`. A `workload` edit that fits lifts the stop at the next reconcile, with at least one `ShapesFit` event and no change to the serving spec or the deployment ID. An edit to another misfit keeps it stopped, with at least one `VerificationFailed` event, and the condition's message names the new shape. A workload edit never lifts a settings contradiction.
- Reasoning models, with an engine double whose launch declares `reasoning-parser`: time to first token is taken at the first reasoning delta. A tool-call smoke test whose final answer calls the tool passes, whatever its reasoning says; one whose only tool call is in the reasoning text fails. The behaviour check asks for the typical output plus 4,096 tokens and runs at the shortened context, which the test works out by hand and finds in `CheckOutcome.ContextTokens`. A reply that spends its whole budget reasoning fails with `ReasoningBudgetExhausted`; its Instance fails, and the class keeps provisioning until `RestartLoop`.
- The file list: a start instruction without `checks.verify.files` starts no worker. Staging reports the list awaited, and the Instance fails at `health.startTimeout`. A rung-2 restage without the list does the same; a worker restart (rung 1) needs none.
- Stale gauges: a draining Instance whose report has zero running requests, but a `metricsReadAt` more than two probe intervals old, or no gauge at all, does not end and gets no `idle-since`. Neither does it trigger the only-errors or the no-progress signal. Each of the three ages is tested alone, at exactly two intervals (fresh) and just past them (stale). Fresh gauges with zero running requests end it.
- The Hub token: with the download in the agent's own process, the token is in no argument list, log, or file on the fake platform; a child process that downloads finds it only in its environment.
- Health: a stalled Instance with requests in flight is restarted at the worker rung within the threshold. Repeated failures climb the ladder in order. A process that ignores TERM is killed after `stopGrace`. Plain load with long queues restarts nothing. An engine double in prefill for 1,100 s, with a request running and no token generated, is not restarted; one with no token for 30 min is (HBEP-5's slow-prefill test runs with this controller active).
- Lease end: every observe copies the Allocation's hard end into the Instance's `status.leaseEnd`. A later hard end in the fake is copied, and moves the drain start.
- Drain window: on a profile with `drainWindow` 2 h 10 min and `drainMargin` 10 min, an Instance of a ModelDeployment with `routing.maxRequestDuration` 30 min becomes `draining` 40 min before its hard end; one with 3 h drains 2 h 10 min before it. The times are worked out by hand.
- Launch: a serving spec without `max-num-seqs` gives a `launch` with `--max-num-seqs` 256 and `launch.maxNumSeqs` 256, and the same deployment ID as the spec that declares 256.
- Image format: on a partition with `imageFormat: sandbox` the desired state tells the agent `sandbox`, and on one that states none, `sif`. The format is not an ID input: two partitions of one accelerator class that differ only in `imageFormat` give one spec one ID.
- No front: with no front address and dial-home mode configured, no Allocation is submitted, and `Degraded` has reason `FrontNotConfigured`.
- Report bounds: a `targetSessions` of 1025 is refused at admission. A report at the bounds, five warm-up rounds at 1024 sessions with every free-text field at its cut, fits under 4 MiB, and a body above 4 MiB is refused.
- Key files: the Instance key file holds 64 lowercase hex characters with no newline, mode 600. A reader refuses one with a trailing newline, and a writer killed mid-write leaves the old file or none, never a partial key.
- Managed source: a managed-source ModelDeployment gets no Allocation and no Instance, `desiredInstances` 0 and `Progressing` false with `ManagedSource`. Its `Ready` is true while the catalog entry is listed, and false with `CatalogEntryUnlisted` once it leaves the listing, or with `CredentialsRejected`. A served ModelDeployment whose selector matches a managed Provider's labels provisions on its batch Providers only, and sets no condition for the managed one.
- A part that exits cleanly without being told to counts as failed.
- Each restart's event is recorded before the raised generation is written, checked in the in-memory recorder's order. The API shows at least one event per rung's reason.
- The agent: a registration with a wrong credential, or with the right credential and another job ID, is refused. A missed desired-state message is corrected by the next poll. An agent that stops reporting makes its Instance unreachable, and nothing is restarted. When the desired state asks it to verify, qualify or warm up, the real agent runs the checks against the engine double's socket and reports them, and the controller's verdicts follow the reports.
- A restart runs the Instance's original `launch`, even after the ModelDeployment's serving spec has changed.
- An operation interrupted on one of several providers reports each provider's outcome.
- A repurpose, once it is on: the old Instance drains, the new deployment's Instance starts in the same Allocation, and the front routes to it only after verification and warm-up. The Allocation keeps its name, and the deployment ID changes.

On real providers: on each alpha platform, one deployment with a short `leaseLimit` and a short `routing.maxRequestDuration` (so its drain window fits the lease) completes one handover end to end through the front, with no request cut.

### Compatibility and upgrades

- Stored state is custom resources, so the schema follows Kubernetes versioning (HBEP-11). The fields this HBEP adds are at `v1alpha1`.
- A controller upgrade must not touch running allocations. The new controller re-reads everything. Each Instance keeps its immutable `launch`, and each Allocation keeps its agent digest.
- The agent's protocol is versioned in its path (`/agent/v1/`). Within v1, a newer endpoint serves older agents, and fields an older agent does not know are ignored. New Allocations get the agent build the controller is configured with.
- A downgrade restores the store (HBEP-11). A controller ignores status fields it does not know.

### Security

- Provider credentials belong to the provider plugins (HBEP-4).
- An Allocation's credential is generated by the controller, kept in a mode-600 file on the control-plane host, and placed on the platform in a mode-600 file (HBEP-4). It authenticates the agent's registration, which must also name the recorded job ID (HBEP-6). The Instance's key and a Hub token travel only over that authenticated channel, never in a file outside the job or on a command line. A Hub token is never written to a file on the platform: it stays in the agent's memory, or reaches a child process that downloads only through its environment (The allocation agent, Keys). On the control-plane host it is a mode-600 credentials file (HBEP-11, Credentials; HBEP-3).
- The controller sees no prompts. Warm-up and qualification use synthetic prompts only.
- Who may change desired state, ask for a repurpose, or approve a release, is HBEP-10's and HBEP-11's.

### Graduation criteria

- **Alpha:**
  - the Test plan's simulated cases pass, except those marked "once it is on";
  - on each alpha platform (Helios `plgrid-gpu-gh200` and Athena `plgrid-gpu-a100`), one deployment completes one handover end to end through the front with no request cut, and one restart at the worker rung;
  - Apptainer on the compute nodes is verified on each alpha partition (HBEP-4, open question 15). Spike S1's probe 2 verified it on both: Helios `plgrid-gpu-gh200` runs a SIF, and Athena `plgrid-gpu-a100` a sandbox directory (`imageFormat`). A partition where it is missing is out of alpha;
  - HBEP-6's dial-home path works for alpha, which needs HBEP-6 to be implementable.
- **Beta:**
  - hedging across two providers at once, with the start-up failure rate measured per platform (open question 1);
  - over-allocation with k = 1 and ranking, against expected performance (open question 12);
  - restart thresholds from fault injection (open question 6);
  - Instances with several workers behind a router (HBEP-5);
  - repurposing across models, with HBEP-10's approvals (open questions 7 and 8);
  - the backfill limit and the lead-time question decided (open question 2), and Athena's idle question answered (open question 13).
- **Stable:** the Test plan runs in CI, and one deployment has run to its end time across several handovers on two platforms with no hand-run step.

## Operational readiness

1. **Working:** reconcile age per deployment, and ready instances against desired ones. End to end, each deployment answers a synthetic request through the front. Ready below desired past `health.startTimeout`, `ReplacementLate`, or agents that stop reporting say it is not working.
2. **Restart:** it re-reads ModelDeployments, lists Allocations by label and Instances by owner, and reads the agents' next reports. Then it resumes planned handovers, and applies each empty Allocation's retention policy. Agents keep their Instances running while it is down; their reports resume when it returns.
3. **Silent failure:** "planned" handovers that were never submitted; Instances marked ready that serve nothing; a verification that read nothing. The lateness event, the front's synthetic requests, and the required settings in verification show the difference.
4. **Provider limits:** hard time limits, unknown start times, per-user job limits, fixed accounts, and center response times.
5. **Capacity:** no provider starts a replacement. The deployment degrades, and the question goes to agents (open question 10).
6. **Idle cost:** pending replacements cost no grant hours, but they hold queue slots under per-user limits. Overlap costs the hours of both Allocations while both run. On a short queue the replacement is planned from observed waits, so the two serve together for about the margin (open question 2). Before any wait is observed, or on a long queue, the replacement is submitted at the predecessor's start and can overlap it for most of the lease. Over-allocation costs the k extra Allocations until qualification ends. Warm capacity costs each kept Allocation's hours until its policy, or `maxIdle`, releases it. Each agent holds one connection to the control-plane host.
7. **Rollback:** scale the deployment to zero. Instances drain and end without replacement (Release); pending Allocations are cancelled, and started ones follow their retention policy, which an operator can set to release when idle.

## Drawbacks

- Hedging, over-allocation, overlap and warm capacity spend queue slots and grant hours to buy continuity and throughput.
- The controller depends on each provider's queue semantics, which differ and change.
- The allocation agent is a second component to build, stage and keep compatible with the controller.

## Alternatives

- **Hand-run handovers by operators or agents.** This was the starting point, and it led to most of the problems in Motivation.
- **Slurm dependencies alone.** Same-node pins pended for days, dependencies counted from the wrong moment, and a cancel started dependents.
- **Keep whatever starts.** Simpler than over-allocation, but a broken or slow node then serves until someone notices.
- **Models in pods on Slurm (interLink and other virtual-kubelet offloading, Slinky's slurm-bridge).** Pods hide lease ends and start estimates, which the controller needs. The evaluation (28 Oct 2026) confirmed this; parts may become components.
- **Restarts through `srun --overlap` from the login node.** A step launched that way dies with the login-node `srun` (L-3.5), and keeping it alive is a long-running process on the login node (HBEP-4, open question 14).
- **Control files on the shared filesystem,** as www-2027's holder scripts used. They need no new channel, but every command and report costs a login-node command, and latency is the poll interval. The agent pulls its desired state over the dial-home path instead.

## Prior art and reuse

The controller applies by policy what www-2027 operators did per job: holder batch scripts that restarted on a control file, follow-ons that published a next endpoint for clients to switch to, a hand-run hedge that cancelled the loser only after the winner passed identity and timing checks, verified and warmed spares, and real-shape warm-ups. The allocation agent replaces the holder script.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| vLLM | [vllm-project/vllm](https://github.com/vllm-project/vllm) | Apache-2.0 | Dependency: the engine each Instance starts |
| controller-runtime | [kubernetes-sigs/controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) | Apache-2.0 | Dependency (HBEP-11), used by spikes S3 and S4: the controller's keyed work queues, leader election and reconcile loop |
| Apptainer | [apptainer/apptainer](https://github.com/apptainer/apptainer) | BSD-3-Clause ([LICENSE.md](https://github.com/apptainer/apptainer/blob/main/LICENSE.md), accessed 2026-10-30) | Dependency on the compute nodes, provided by the center: the agent runs the engine image with it (HBEP-4, open question 15) |
| inference-perf | [kubernetes-sigs/inference-perf](https://github.com/kubernetes-sigs/inference-perf) | Apache-2.0 | Candidate after alpha, no spike assigned: the qualification benchmark (open question 12). Alpha measures throughput from its own warm-up |
| llm-d-inference-sim | [llm-d/llm-d-inference-sim](https://github.com/llm-d/llm-d-inference-sim) | Apache-2.0 | Candidate test dependency, no spike assigned: a GPU-free engine double for the test plan, where it reports what the checks read |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| Placement gaming: launch extra instances up front and keep the strongest | Farley et al., "More for your money: exploiting performance heterogeneity in public clouds", ACM SoCC 2012 | Over-allocation |
| Keep good instances and discard bad ones within one instance type | Ou et al., "Is the same instance type created equal? Exploiting heterogeneity of public clouds", IEEE Transactions on Cloud Computing, 2013 | Qualification and keep-best-N |
| Over-provisioning to absorb preemption (SpotHedge) | SkyServe ([arXiv 2411.01438](https://arxiv.org/abs/2411.01438)) | Cited beside over-allocation: it absorbs loss and does not qualify |
| Copy a workload to several clusters, keep the first admitted, withdraw the rest; add targets in rounds | Kueue MultiKueue ([kubernetes-sigs/kueue](https://github.com/kubernetes-sigs/kueue)) | Hedging, with the milestone moved to first to serve (open questions 1 and 2) |
| Submit a replacement once remaining time falls below a per-service window | GWDG SAIA ([services.py](https://raw.githubusercontent.com/gwdg/saia-hpc/main/scheduler_core/services.py)) | Prior art for handover; new here are lead time, hedging and qualification |
| Pilot demand law: split by site, a pending cap per site, ramp-up attenuation, halving at limits, a reserve, withdrawal with hysteresis; late binding; a randomised retire spread | GlideinWMS ([glideinFrontendElement.py](https://raw.githubusercontent.com/glideinWMS/glideinwms/master/frontend/glideinFrontendElement.py)) | Open questions 1 and 2; staggered handovers; late binding is the allocation agent pulling its work |
| A node agent that pulls its desired state and converges on it, level-triggered | Kubernetes kubelet | The allocation agent's desired state |
| MachineHealthCheck short-circuit (`triggerIf`) and external remediation | Cluster API ([kubernetes-sigs/cluster-api](https://github.com/kubernetes-sigs/cluster-api)) | Open question 6: incidents that hit a whole platform |
| `delete-machine` priority annotation; `readinessGates` and `minReadySeconds` | Cluster API | Open question 12: keep-best-N; the conditions Verified, Qualified and WarmedUp |
| A desired pool with limits and budgets, apart from single units of capacity | Karpenter NodePool ([kubernetes-sigs/karpenter](https://github.com/kubernetes-sigs/karpenter)) | Instance counts and limits per deployment |
| RepairPolicy as data; a DisruptionBlocked event; a kwok-style simulated provider | Karpenter | Restarts, the event when a replacement cannot be staged, and the test plan's simulated provider |
| Retry budgets that separate "never started" from "started, then lost" | Armada ([retry policies](https://raw.githubusercontent.com/armadaproject/armada/master/docs/retry_policies.md)) | Open question 6 |
| Desired counts from an external alert | RAMSES ([arXiv 2511.21413](https://arxiv.org/abs/2511.21413)) | Desired state set from outside the controller |
| A lease with a holder identity and a duration | Kubernetes `Lease` | Open question 11: one acting controller per provider |
| `--no-requeue`; a staged TERM, then cancel | SkyPilot's Slurm provisioner ([skypilot-org/skypilot](https://github.com/skypilot-org/skypilot)) | The requeue rule; a forced kill after a polite stop (open question 6) |

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| Follow-ons pinned to the predecessor's node pended for days. | L-1.1 | Open question 3: independent submissions without node pins. |
| A freed slot went to another user within seconds. | L-1.2 | Replacement before release; the predecessor stays until its drain starts. |
| One handover design gave back a large share of every lease. | L-1.21 | Open question 4: keep the predecessor until its drain starts. |
| Time limits cannot be extended, and centers may not act at weekends. | L-1.6 | Handovers are planned back from the hard end. |
| Dependencies counted from the parent's start, not its end. | L-1.8 | Open question 3: no provider-side dependencies. |
| Cancelling a parent started its pending follow-on at once. | L-1.20 | No dependencies are used. |
| Chained starts on queued jobs blocked each other. | L-1.13 | Open question 3: independent submissions. |
| Start estimates swung by hours. | L-1.9 | Open question 2: plan on observed waits, never on estimates. |
| The time limit decided how long a job queued. | L-1.12 | Open question 2: time limits that backfill, after alpha. |
| A cluster's GPU nodes were blocked for days. | L-1.15 | Hedging across providers. |
| Our own jobs competed with each other for nodes. | L-1.3, L-1.17 | Planning per provider, within its per-user limits. |
| A restart pinned to its old node pended forever. | L-1.18 | The last rung is a new Allocation, with no node pin. |
| Editing a pending job left it unschedulable. | L-1.5 | A submitted allocation is never edited; it is cancelled and resubmitted. |
| An unwarmed follow-on tripped a client's load guard. | L-5.3 | Real-shape warm-up on the new Instance before admission. |
| Compiles on unseen shapes stalled or killed engines. | L-2.8 | Warm-up ends only when a repeat of each shape is no slower. |
| Capacity measured on short contexts overstated long-context capacity. | L-5.2 | Warm-up and qualification shapes come from the deployment spec. |
| Measured KV cache per session was far above the estimate. | L-5.1 | Qualification checks KV capacity at the target sessions and measures throughput there. |
| KV-cache exhaustion stalled servers. | L-3.12 | KV capacity is checked before admission; plain load is never a restart signal. |
| Empty samples were shown as zero, so busy routers looked idle. | L-10.5 | A gauge left out is unknown, never zero, and the controller acts only on fresh gauges (Health and restarts). |
| Failing servers were waited out instead of restarted. | L-3.11 | Open question 6: prompt restarts on malfunction signals. |
| Full stack restarts took minutes. | L-3.10 | The ladder starts at the smallest part. |
| Rank 0 exited cleanly while another rank hung, so nothing restarted. | L-2.9 | A part that exits without being told to counts as failed, whatever its exit code. |
| A follow-on aborted itself on a failed status query of its ended predecessor. | L-3.1 | The controller reads ended jobs from accounting; no job script tracks another. |
| Restart helpers for one topology stopped their own workers on another. | L-3.2 | Restarts follow the topology the spec declares, carried out by the agent. |
| Parts sharing one step could not restart separately. | L-3.8 | Each part is its own process under the agent. |
| Hand-launched steps died with the login-node `srun`. | L-3.5 | Nothing is launched from a login node; the agent starts every part inside the job. |
| A router hung in graceful shutdown. | L-3.3 | A forced kill follows a few seconds after a polite stop. |
| A live serve script could not be edited, and an old job's restart ran a newer one. | L-3.7 | Each Instance's `launch` is immutable, and every restart reuses it. |
| Loopback ports clashed between jobs on shared nodes. | L-7.7 | The engine listens on a Unix socket in the job's own temporary directory. |
| Converting idle stacks between models was manual. | L-3.9 | Repurposing is one declared operation. |
| A deliberate restart was counted as a crash. | L-10.16 | The controller records a restart as an event before it acts. |
| An interrupted multi-cluster release had already cancelled jobs. | L-11.7 | Partial failures are reported item by item. |
| Releasing capacity saves grant hours but risks the node. | L-13.5 | Open question 13: keep by default; release-when-idle is opt-in. |
| A relayed cancellation was taken as the owner's decision. | L-14.3 | Open question 8: irreversible releases need approval. |

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #7.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).
- 2026-10-28: open questions answered with proposals from past operation, for the owner to confirm.
- 2026-10-30: Prior art and reuse filled from the state-of-the-art evaluation; spike S5 facts carried in (HBEP-4, HBEP-6, HBEP-7 only).
- 2026-10-30: aligned with HBEP-3 and HBEP-11 becoming implementable: requeue, cardinality, the S4 submission protocol, and the `Lease` as an extra guard.
- 2026-10-30: S4's implemented submission protocol written into open question 11; one current deployment ID per accelerator class carried into hedging and handover; verification compares settings and rehashes files, never recomputes the ID.
- 2026-10-30: design completed for implementable. The owner's approval of the proposals (29 Oct 2026, PR #14) recorded as decisions; the alpha scope marked; the controllers, the reconcile loop, provisioning, the allocation agent, Instance start with HBEP-9's alpha checks, health and restarts, handover, release and retention written for alpha.
- 2026-10-30: implementable. The owner's approval of the proposed answers (29 Oct 2026, PR #14) and spikes S1 (issue #18), S4 (PR #32), S5 (PR #19) and S7 (PR #33) carried in; every open question decided, not needed for alpha, or open for beta.
- 2026-10-30: amendment with HBEP-5 becoming implementable: a managed-source ModelDeployment is provisioned nothing and its status says so; a served one ignores managed Providers; the launch sets `--served-model-name`, `VLLM_API_KEY` and always `--max-num-seqs`; the controller reads `routing.maxRequestDuration` and `drainMargin` for the drain window and writes `status.leaseEnd`; a prefill-aware no-progress signal is open for beta; the real handover test shortens the drain window.
- 2026-10-31: amendment from the merged contracts (PR #71) and spike S1's probe 2 (issue #18): every ModelDeployment reason the code defines, `FrontNotConfigured` and `IdentityUnresolved` included; registration as soft state; a requeue told by `agentInstanceID` and `restartCount`; the Instance key file's form; the idle anchor; `launch.maxNumSeqs`; the partition's `imageFormat`; report bounds; the `Lease` namespace, the field manager and `DialHomeUnreachable`.
- 2026-10-31: amendment with HBEP-6 becoming implementable: two ssh forwards in ssh mode with the owner's key restriction, frpc as the agent's tunnel to the engine's socket, the frps plugin in the controller's process, and the `TunnelConnected` condition.
- 2026-10-31: the settings comparison by value: booleans, numbers, then strings; `auto` recorded as resolved (science-agent decision while building PR #87; review of PR #91).
- 2026-10-31: amendment from the science agent's rulings while #87 and #88 were built: the start instruction carries the file list; reasoning models' checks; only fresh gauges act; the shape-misfit stop keyed on the shapes; the Hub token in memory or the child's environment; the hold lift's re-read and its known risk; the template overhead in the verification record.
- 2026-10-31: amendment from the owner's decision on the dial-home route (issue #18): ssh mode's forwards run over the configured ssh port, 22 by default.
- 2026-10-31: amendment with HBEP-8 becoming implementable: the event's fields (`action` the type, `reason` the code alone, `Warning` for `degraded` and `failing`), the spelling `ending-soon`, the `HeartbeatMissed` and `ObservationGap` events, and test wording for merged events.

## Amendments

- **2026-10-30: what the front needs from the lifecycle controller.** Made with HBEP-5 becoming implementable, and from the review of PR #63.
  - **Managed sources** (Reconcile loop, step 1). A ModelDeployment with `spec.source.managed` is provisioned nothing. Its status says it has no Instances by design, and its `Ready` follows the catalog entry. A served ModelDeployment ignores managed Providers in its selection, even when labels match. Before, step 1 skipped a managed Provider with a condition naming it, and said nothing of a managed source.
  - **The launch** sets `--served-model-name` to the model name, which the front's rewrite relies on, and puts the Instance's key in `VLLM_API_KEY`. It always passes `--max-num-seqs`: the serving spec's value, else 256, a framework default recorded in `launch`. So the front's per-Instance cap is always known. `max-num-seqs` is capacity, not a deployment-ID input (HBEP-3's table), so the default changes no ID.
  - **The drain window.** The controller reads `routing.maxRequestDuration` from the ModelDeployment and `drainMargin` from the profile, and computes the window as the front does (HBEP-5, Draining). The real handover test shortens it through `routing.maxRequestDuration`.
  - **`status.leaseEnd`.** On every observe, the controller copies the Allocation's hard end into the Instance's `status.leaseEnd`, which the front reads to enforce the drain deadline. The front does not route to an Instance without it (HBEP-5).
  - **The no-progress watchdog.** HBEP-4's default for `health.noProgressTimeout` becomes 30 min, equal to the front's `idleTimeout`, so a healthy prefill is not restarted. A signal that tells prefill from a stall is open for beta (open question 6).
- **2026-10-31: decisions from building the contracts.** Science-agent decisions of 31 Oct 2026, made while the contracts were built (PR #71), with spike S1's probe 2 (issue #18, comment 6542078107). The names are those on `main`.
  - **The reasons** (Events and conditions). `Degraded` lists every reason the code defines, adding `FrontNotConfigured` (no front configured, HBEP-11) and `IdentityUnresolved` (no deployment ID, HBEP-3), with `AsDesired` when false. `Ready` and `Progressing` name their ordinary reasons, and a managed source's `Ready` gains `ProviderNotOnPlatform`.
  - **Registration is soft state.** After a controller restart each agent registers again on its next poll, with no Instance failed and no generation raised.
  - **A second execution of a job,** a requeue, is told by the agent's per-process random `agentInstanceID` plus the scheduler's `restartCount` (`SLURM_RESTART_COUNT`), never by the kernel's boot ID: a requeued job can land on the same node without a reboot.
  - **The Instance key file** is 64 lowercase hex characters, with no newline, mode 600, written atomically.
  - **The idle anchor** is the Allocation's `idle-since` annotation: the latest `endTime` when its last Instance ends, or the job's start when no Instance was placed, cleared on placement. Retention's `idleLimit` and the profile's `maxIdle` count from it. It lives on the Allocation because Instances can be deleted while their Allocation is kept.
  - **The launch** passes `--max-num-seqs` from `launch.maxNumSeqs`, the spec's value, else 256.
  - **The image format.** The agent stages and runs the image in the partition's `imageFormat`: a sandbox directory on Athena A100, built from the same digest (HBEP-4, open question 15). The alpha precondition on Apptainer is met on both alpha partitions.
  - **Report bounds:** `targetSessions` at most 1024, a 4 MiB body limit, and all five warm-up rounds kept.
  - **Controllers.** The `lifecycle` `Lease` lives in `hpcburst-system`. The controller writes under `hpcburst-lifecycle`, and on a Provider owns only the condition `DialHomeUnreachable`, set when an Instance's registration through the tunnel fails (HBEP-4, open question 16).
- **2026-10-31: the tunnel** (HBEP-6 becoming implementable; science-agent decisions of 31 Oct 2026, and the owner's decision on the key restriction).
  - **Two ssh forwards in ssh mode** (The allocation agent). Connection 1 carries the agent's control traffic to the agent endpoint; connection 2 carries frpc's tunnel to the front's dial-home listener. Each has its own reconnect loop and a kernel-chosen local port. S1c (PR #78) measured that streams sharing one connection block each other: one 4 MiB reply held the others for about 4.5 s on a 1 MB/s link, against under 0.15 s on separate connections, and a report may reach 4 MiB. Before, this HBEP named one forward.
  - **The key's line** is quoted: `restrict,port-forwarding`, `permitopen` for the launcher's port base + 3 and + 7 only, `permitlisten="127.0.0.1:1"` and `command="/bin/false"`, one key per cluster. Before, the text said only which ports it allows.
  - **frpc is the serving path.** The agent starts frpc for each Instance, after checking its digest and the launcher certificate's pin, and frpc dials the engine's socket. The agent restarts frpc and the forwards itself, as transport, not as parts. Before, "the agent forwards the serving path" named no mechanism.
  - **The frps plugin** runs in the lifecycle controller's process, on its own loopback port (base + 8), handles `Login`, `NewProxy` and `CloseProxy` but not `Ping`, and answers within 1 s from memory. A controller restart therefore cuts no established tunnel (Test plan). The controller writes the proposed Instance condition `TunnelConnected` from frps's proxy list (Events and conditions).
  - **Wording.** Registration goes over the dial-home path, the agent's own connection, never through frp's tunnel, so the text no longer says "registration through the tunnel". The trigger of `DialHomeUnreachable` is unchanged.
  - **frpc's log** goes only to a file in the job's private temporary directory, because it names the session's run ID, which is enough to attach a work connection (HBEP-6, Risks).
  - **Fixes from the review round of PR #86.** An Instance becomes `ready` only with `TunnelConnected` True, so a tunnel that never connects fails it at `health.startTimeout` with the proposed reason `TunnelNotConnected` (Instance start); before, the gate did not look at the tunnel. In ssh mode each forward keeps the local port it was given at the agent's start; frpc starts once connection 2 listens, and a port that must change moves the control client or restarts frpc with the new port.
- **2026-10-31: the settings comparison by value** (science-agent decisions while building PR #87, and the review of PR #91). A declared input the engine reports is compared under one rule: as booleans, else as numbers, else verbatim as strings. A declared `auto` is recorded as resolved, not compared; `max-model-len` is a plain integer, compared as one; a model hook's rewrite of a declared value fails verification, as a known limit. An undeclared setting the engine reports stays recorded and passes (HBEP-3).
- **2026-10-31: rulings from building the agent and the controller.** Science-agent rulings of 31 Oct 2026, made while the agent (#87, merged) and the lifecycle controller (#88, open) were built. Names are those on `main`, except `ShapesFit`, proposed in PR #88.
  - **The file list** (The allocation agent, Desired state). For a first start and a rung-2 restage, the desired state for that Instance generation carries `Checks.Verify` with `Files`, and the agent starts no worker without it. Before, the text said the desired state carries the file list, but not that a start waits for it.
  - **Reasoning models** (Instance start). With `reasoning-parser` declared: time to first token counts the first delta of either kind; the tool-call smoke test and the behaviour check judge only `content` and `tool_calls`, and each asks for 4,096 more tokens; only the behaviour check's prompt is shortened, within `contextTokens.typical` and `contextTokens.longest`, and its context is reported in `CheckOutcome.ContextTokens`; a reply with no final answer fails with `ReasoningBudgetExhausted`, an Instance failure, never a class stop; warm-up counts reasoning as output. The fixed 4,096 is a known limit of alpha. Before, the checks assumed every reply was its answer.
  - **Only fresh gauges act** (Reports; Health and restarts; Handover, Draining). The report carries `metricsReadAt`; a gauge left out is unknown, never zero. The idle anchor, KV pressure, the only-errors and the no-progress signals act only while `Health.MetricsFresh` holds: three ages, each within two probe intervals. A draining Instance ends on fresh gauges only. Before, a draining Instance ended "when the agent reports no running request", which a stale zero could satisfy.
  - **The shape-misfit stop** (What the controller reads; Instance start). It is keyed on the deployment ID plus a hash of the resolved shapes, so a `workload` edit that fits lifts it at the next reconcile, with one event, without a change to the serving spec. A settings contradiction stays keyed on the deployment ID alone. Before, a misfit stopped the class "like a settings contradiction", until the serving spec changed.
  - **The Hub token** (The allocation agent, Keys; Security). It stays in the agent's memory when the agent's own process downloads, and reaches a child process that downloads only through its environment; never a command line or a file on the platform. On the control-plane host it stays a mode-600 credentials file. Before, it "reaches the download only through the environment".
  - **The hold lift** (open question 11, Settle; Risks). Immediately before the release, the provider re-reads the marker and the job under a per-job lock, and releases only a job that still meets the four conditions. The window after the re-read is HBEP-4's known risk, and "never lift a requeued job's hold again" carries that exception.
  - **The verification record** (Instance start) also holds the measured template overhead, which the shape-misfit stop reads beside the context limit (review round of PR #96).
  - The settings comparison is PR #91's amendment above, unchanged.
- **2026-10-31: the ssh port** (The allocation agent, Registration). The owner's decision of 31 Oct 2026, recorded on [issue #18, comment 6546958511](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/18#issuecomment-6546958511): the compute nodes reach the development VM's sshd through a port forward on the gateway, at the gateway's address and port. So ssh mode's forwards run over the configured ssh port, 22 by default (HBEP-6). The key's restriction is on the control-plane host's side and is unchanged. Before, the forwards ran over port 22.
- **2026-10-31: events** (Events and conditions; Test plan). Made with HBEP-8 becoming implementable, from the science agent's decisions of 31 Oct 2026 and the code of PR #88.
  - **The fields.** An event's `action` carries HBEP-8's type and its `reason` the reason code alone; `type` is `Warning` for `degraded` and `failing`, and `Normal` otherwise. Before, "the reason carries HBEP-8's type and reason code", which PR #88's events already did not do.
  - **The spelling** of the sixth type is `ending-soon`, as PR #88 emits it. Its event is `Normal`. Before, the text wrote `ending soon`.
  - **Observer events.** The controller emits `failing` with reason `HeartbeatMissed` when an Instance's `AgentUnreachable` turns True, when a Provider's last successful read is older than `ProviderReadyStaleness`, and when the front's `Lease` is stale, once per episode. At start it emits one `degraded` event with reason `ObservationGap`. Before, `AgentUnreachable` changed the condition with no event.
  - **Test wording.** client-go's recorder merges isomorphic events within 6 minutes and keeps the first note only (HBEP-8). So the shape-misfit test asks for at least one event and finds the new shape in the condition's message, and the restart test checks the order in the in-memory recorder. Before, they asked for "a new event naming the new shape" and for the event "before the agent sees the raised generation", which the API cannot show.
