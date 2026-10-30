# HBEP-4: Provider interface and profiles

## Summary

This HBEP defines how the framework works with providers: our access to one platform each (HBEP-3). Providers share one core interface, with capability flags that say which core operations a provider supports. What is true for everyone on a platform is data in its platform profile. What one provider adds (its account, grant, credentials reference, allowed partitions and labels) is its provider settings. Upstream bugs go to a known-issues register tied to versions, with a regression test and a workaround expressed as configuration. It is written for authors of provider plugins and of the lifecycle controller (HBEP-7).

## Motivation

Stage 1 (hit the limit), stage 2 (get resources), stage 3 (keep it running) and stage 7 (pay and give back), as defined in HBEP-2.

Problems this addresses, in general terms:
- **Submission rules differ per cluster and are learned by failure:** required options, job-name rules, compute nodes without home directories, schedulers that print local time.
- **Scheduling facts break naive designs:**
  - start estimates swing by hours and flip to unknown;
  - a freed slot goes to another user at once;
  - cancelling a parent starts its pending follow-on at once;
  - a dependency offset counts from the parent's start, not its end;
  - users cannot extend a job's time limit, and a running job's account cannot be changed;
  - a fast-start partition allows one job per user, pending jobs included.
- **A managed gateway has its own failure modes:**
  - it cuts replies that send no byte for a fixed time, and stalls periodically;
  - models go inactive without the listing showing it;
  - unknown, inactive and blocked models return the same error;
  - it rewrites requests;
  - keys expire mid-run.
- **Upstream bugs:** a router's default total deadline cut long streams without a log line; an engine's keep-alive was shorter than the router's connection reuse; a compile on an unseen shape ran past the engine's step limit and killed it. Each workaround lived in notes or one-off scripts, and nothing turned it off once the bug was fixed.
- **Usage was hard to count:** the center's accounting tool counted only finished jobs, billing formulas differ per cluster, and behaviour at grant exhaustion was unknown until it happened.

### Goals

- One core interface that the lifecycle controller uses for every provider. Differences are flags and data, not branches on a provider's name.
- Every contingency as a profile parameter with thresholds, enabled by default and cheap when idle.
- For each upstream bug: a register entry tied to versions, pinned versions per profile, one regression test, and a workaround as configuration.
- A new platform is added by writing its platform profile, plus plugin code only where data cannot express a difference. A new provider on a known platform is only provider settings.
- Expected performance in each platform profile, as the reference for qualification (HBEP-3, HBEP-7).

### Non-goals

- Lifecycle decisions: when to hedge, over-allocate, hand over, drain, restart or repurpose (HBEP-7). This HBEP exposes what those decisions need.
- How instances are reached (HBEP-6). The profile records the reachability facts HBEP-6 relies on.
- Which providers serve a model name. That is routing policy (HBEP-3, HBEP-5).
- Center priority pushes. The framework never assumes one.
- The static kind's interface, which comes later.

## Proposal

### User stories

- An operator adds Cyfronet Athena. They write its platform profile (time limits, submission options, billing formula, container runtime, expected performance), and reuse plugin code where the profile can express the difference. Then they add a provider for their grant there: provider settings with the account, a credentials reference, the allowed partitions and the labels.
- The operator gets a second grant on Helios. It is a second provider: new provider settings over the same platform profile.
- The lifecycle controller hedges `example-27b` across a provider on Helios and one on Athena. It reads each provider's start estimate as advisory, submits on both, acts on the first start signal and cancels the pending loser.
- The controller qualifies a new Instance on Athena. Its benchmark falls well below the profile's expected performance for that node type, so the controller discards it (HBEP-7).
- LLMLab takes a model offline. The managed provider reports the model's state, which the listing does not show, and returns a defined error instead of an ambiguous one.
- An engine upgrade fixes a bug. The profile's pin passes the register entry's fixed-in version, the workaround turns off, and the regression test keeps running.
- An agent asks what an allocation has cost so far. The profile's billing formula and the allocation's actual running time give the answer.

### Notes, constraints and caveats

- **Two layers.**
  - A platform profile holds what is true for everyone on a platform: hardware, architecture, limits, time zone, filesystems, requeue behaviour, expected performance and known issues.
  - Provider settings add one provider's account, grant, credentials reference, allowed partitions and labels (organization, platform, partition, account; HBEP-3).
  - Every provider on a platform uses that platform's profile.
- **A provider is a plugin, a platform profile and provider settings.** Providers share a common core plus capability flags.
- **Cluster API.** The provider model follows Cluster API's idea of pluggable infrastructure providers behind common resources, applied to model serving.
- **Kinds** (HBEP-3) follow the platform: managed (a fixed catalog, no start or restart), batch (leases with unknown start and hard end, continuity through follow-ons) and static (later).
- **LLMLab will improve but keeps the same kinds of characteristics.** Its contingencies stay in its platform profile even when they rarely fire; thresholds keep them cheap.
- **Hedging is the framework's lever** (HBEP-3, HBEP-7). The core must therefore let the controller submit on several providers, read start estimates marked advisory, see promptly when an allocation starts, cancel a pending allocation with its dependents first, and read each allocation's hard end.
- **Agent primitives** (HBEP-10) require idempotent operations and reports of partial failure, so every core operation reports per-allocation results, and every one except submission is idempotent. For submission, a tag on the job supports recovery but does not enforce uniqueness (HBEP-7).
- **Spike S5 (PR #19).** S5 recorded login-node facts for Helios, Athena and Sano as draft platform profiles in `profiles/`. It ran no job, so every compute-node fact waits for spike S1 (issue #18; open question 16). The entries below that cite S5 rest on its measurements and on the centers' documentation.
- **The job transport.** Proposed: the Slurm command-line tools over one multiplexed ssh connection per login node are the default transport. `squeue --json` and `sacct --json` work for a plain user on all three platforms (spike S5, PR #19). A REST job API is unverified (open question 13).
- **The container runtime is a per-platform fact.** Apptainer is on the Helios and Sano login nodes. Athena's login node has no container runtime, and user namespaces are off on both Cyfronet login nodes (spike S5, PR #19). Compute nodes were not checked. Whether Instances need a launch path without containers is open question 15.

### Risks and mitigations

- **A stale profile acts on old facts with confidence.** Centers change policies without notice. Candidate: each field records its source and when it was last verified, and cheap probes re-check what can be checked (open question 7).
- **Start estimates are trusted.** They are advisory. Decisions act on actual starts.
- **The framework's own polling trips login throttles.** Too many parallel logins have tripped a cluster's connection limit. Candidate: a connection budget per platform in its profile, shared by every provider on that platform, with one shared connection per login. Spike S5 (PR #19) could not establish the login nodes' ssh limits, so the budget cannot yet be sized from them.
- **Local time in scheduler output** gives wrong end times, worst across a daylight-saving change. Spike S5 (PR #19) measured it: Slurm's text output in a login session is in Europe/Warsaw local time, with no zone marker, on Helios and Athena, and in UTC on Sano. Proposed: plugins read epoch times (`SLURM_TIME_FORMAT='%s'`, or the JSON output), parse text times only with the time zone the profile records, and convert every time to UTC, with tests across the change.
- **A finished job vanishes from the queue listing.** Helios and Athena purge a finished job from `squeue` about 10 s after it ends (MinJobAge), and Sano after 300 s (spike S5, PR #19). A plugin that polls only `squeue` loses the final state, and a controller that reads the job as missing may submit it again. Proposed: final state comes from accounting (`sacct`). A job missing from `squeue` is never read as completed or as never submitted; with no accounting record either, its state is unknown.
- **The scheduler requeues a job.** Requeue is on by default on all three platforms (`JobRequeue=1`; spike S5, PR #19), so Slurm may run a job's script again under the same job ID. Proposed: submit with `--no-requeue`, and record each platform's default in its profile. Identity across executions is HBEP-3's open question 13.
- **A job without GPUs on a GPU-only platform.** Athena is meant for GPU jobs only, and its documentation warns that workloads without GPUs can lead to account suspension (spike S5, PR #19). Proposed: a profile flag marks a platform as GPU-only, and the plugin refuses any submission without GPUs there.
- **Cancelling in the wrong order starts a follow-on early.** The core cancels dependents before their parent.
- **A partly applied operation is reported as failed.** An interrupted multi-provider cancel may already have cancelled some jobs. Results are reported per allocation, never as one status.
- **A submission is accepted but runs as something else,** for example with another partition or time limit, or becomes unschedulable after an in-place change. Candidate: read every allocation back after submit and after any change.
- **Plugins find their own jobs by scanning.** The newest matching job may belong to someone else. Candidate: tag every job with its Allocation's name and find jobs only by tag.
- **A workaround outlives its bug, or masks another fault,** for example a raised time-out hiding a hang. Candidate: each register entry names the symptom it covers and the version range it applies to.
- **Contingencies cost when idle.** Probing a shared managed gateway adds load to it. Candidate: each contingency's idle cost is stated in the profile, and probes are rate-limited.
- **Flags too coarse for real providers** push provider-specific code into the core. A new flag is an amendment to this HBEP.
- **Account data leaks into a public profile.** Accounts, grants and credentials references are provider settings, never platform-profile fields.
- **Expected performance goes out of date,** so qualification discards good Instances or keeps poor ones. The field records its source and date like any other (open question 7).

### Open questions

1. **Core operations.** Candidate set: describe, estimate, submit, status, cancel, follow-on, launch an instance, stop an instance, usage, remaining quota, and catalog for managed providers. Decided by walking stages 2, 3 and 7 against LLMLab, Helios and Athena in a spike.
2. **Capability flags.** Candidates: can start; can restart; has a hard end; gives start estimates; supports follow-on dependencies; supports node pinning; a hard end that can move later, for example when a center extends a time limit (the controller re-reads it and never plans for it, HBEP-3); can change a running job's account; compute nodes can connect out; compute nodes are reachable in; reports model state; exposes revisions and limits; rewrites requests; requires streaming. Decided with question 1.
3. **Profile schema.** Candidate field groups for a platform profile:
   - identity: organization, platform, partitions and node types, kind;
   - capability flags;
   - hardware: accelerator, CPU architecture, memory, interconnect;
   - scheduling and submission rules: limits, the scheduler's time zone, and requeue behaviour: whether jobs requeue by default, and whether the framework submits with no-requeue (HBEP-3, HBEP-7);
   - environment: container runtime, filesystems, host software;
   - reachability facts, including the known limitation on multi-node engine traffic (HBEP-6);
   - expected performance per node type, the reference for qualification. Its form is decided with HBEP-7's open question 12;
   - version pins;
   - billing formula and behaviour at grant exhaustion;
   - managed-gateway behaviour: no-byte cut, stall pattern, error mapping;
   - contingency parameters with thresholds;
   - references to known-issue entries.

   Provider settings are decided (owner decision): account, grant, credentials reference, allowed partitions and labels. The format of both (files, or a custom resource per HBEP-11) is open. Decided by writing the LLMLab, Helios and Athena platform profiles as drafts. Spike S5 (PR #19) drafted Helios, Athena and Sano; the LLMLab draft is still to come.
4. **Plugin granularity.** One plugin per scheduler type with per-platform profiles, or one plugin per platform? Decided by drafting the Helios and Athena platform profiles as data first: whatever data cannot express becomes plugin code.
5. **Plugin boundary.** In-process library or separate process, the language, and how plugins declare the core version they implement. Decided with HBEP-11.
6. **Register format.** Candidate entry fields: identifier, symptom, component and affected versions, fixed-in version, platform scope and stage (HBEP-2 dimensions), fix owner, detection check, workaround configuration, regression test, upstream link. Decided in design.
7. **Profile provenance and probes.** How a field records its source (documentation, measurement, assumption) and date; which fields are re-checked, how often and at what cost. Decided by a spike on one batch and one managed provider. A candidate form: the drafts from spike S5 (PR #19) give each fact a value, a method (measured, documented or unknown), a source and a date.
8. **Public and private profiles.** Resolved in part (owner decision): accounts, grants and credentials references are provider settings, never platform-profile data, so they never ship in this repository. Still open: which platform profiles ship here (LLMLab, Helios, Athena), and how a private cluster's platform profile stays local. Owner discussion.
9. **Engine-wide data.** Engine pins are per profile, but some known issues and the deployment-ID input rule (HBEP-3) hold for an engine version on every provider. Where does that data live? Decided with HBEP-3.
10. **A managed allocation's hard end:** none, or key expiry? Shared with HBEP-3 and HBEP-7.
11. **Overrides.** Which profile parameters a ModelDeployment may override, such as a time limit below the profile's maximum. Decided with HBEP-7.
12. **The partition label.** A label holds one value, but provider settings list the allowed partitions. How is a provider that allows several partitions labelled: one label per partition, or one provider per partition? Owner discussion.
13. **A REST job API.** Can a plain user reach one? Spike S5 (PR #19) found the slurmrestd binary on the Helios and Athena login nodes, none on Sano's, and no REST service that any center advertises. JWT is not enabled on any of the three, but slurmrestd can also run with local (Unix-socket) authentication, so availability is unverified. Until this is answered, ssh is the only transport known to work. Decided by asking each center.
14. **A long-running process on a login node.** May a lightweight process, such as a status poller or a relay, run on a login node? Cyfronet's documentation prohibits computation on its login nodes, kills it without notice, and does not address such a process. Sano publishes no policy (spike S5, PR #19). Candidate until answered: poll from the control plane over ssh, and leave no process on a login node. Decided by asking each center.
15. **Launch without containers.** Athena's login node has no container runtime, and user namespaces are off on both Cyfronet login nodes (spike S5, PR #19). A launch path that needs no container runtime is a candidate, not a decision. Its candidate parts are a static tunnel-agent binary and an engine environment built without a container. Decided by spike S1 (issue #18), which checks the compute nodes.
16. **Compute-node facts.** Spike S5 (PR #19) ran no job, so on all three platforms these are unknown: outbound and inbound reachability, user namespaces and container runtimes, node-local disk sizes, and GPU drivers. Decided by spike S1 (issue #18).

## Design details

- **A provider is a plugin, a platform profile and provider settings.** The lifecycle controller calls only the common core and reads capability flags; the operations themselves are open questions 1 and 2.
- **A platform profile is data:** the platform's characteristics, its expected performance, its known issues, its version pins and its contingency parameters. Each contingency has thresholds, is enabled by default and is cheap when idle. The schema is open question 3.
- **Provider settings are data:** the account, the grant, a credentials reference, the allowed partitions and the labels. They hold a reference to the credentials, never the credentials.
- **The known-issues register** holds upstream bugs tied to versions. Each entry has one regression test and a workaround expressed as configuration. A platform profile pins the versions it uses, and an entry's workaround applies only within its version range. The format is open question 6.

### Test plan

- **Simulated providers** exercise every core rule and the failure modes in Risks. Candidates:
  - a batch fake with swinging estimates, freed slots taken at once, a parent cancel that starts its follow-on, a requeue that reruns a job under the same job ID, a finished job that leaves the queue listing seconds after it ends, local-time output across a daylight-saving change, a one-job-per-user partition, a GPU-only platform and a login throttle;
  - a managed fake with a no-byte cut, periodic stalls, models going inactive, ambiguous errors, key expiry and request rewriting;
  - two providers on one fake platform, with separate accounts: their Allocations, usage and credentials never mix.
- **Final state and GPU-only rules.** A job purged from the queue listing is reported from accounting as ended, never as missing. A submission without GPUs to a GPU-only platform is refused before it reaches the provider.
- **One regression test per register entry.** Once CI exists, an entry without a test fails it.
- **A profile is never verified against values derived from itself.** Writing a profile from a probe's output and then checking the probe against that profile proves nothing; expectations come from documentation or an independent measurement. Expected performance likewise never comes from the Instances that qualification judges.
- **Conformance probes** against real providers stay cheap, read-only where possible, and rate-limited.

### Compatibility and upgrades

- The platform-profile schema, the provider-settings schema and the core interface are versioned. TBD: whether an unknown field is refused or ignored (refusal is the safer candidate).
- Version pins change by PR to the platform profile. A workaround switches off when the pin passes its fixed-in version, and its regression test stays.
- Client compatibility with LLMLab is HBEP-5's concern.

### Security

- Provider credentials (cluster keys, gateway keys) are never in platform profiles, on command lines or in logs. Provider settings hold only a reference to them. Where they live is decided with HBEP-9 and HBEP-11.
- A plugin acts with the user's credentials and can spend their grant, so irreversible actions go through approvals (HBEP-10).
- Platform profiles hold no account names, grant names, host names or addresses. Accounts and grants are provider settings (open question 8).

### Graduation criteria

TBD. Candidate: alpha with LLMLab and one Slurm system; beta with hedging across two Slurm systems; stable when every register entry's regression test runs in CI.

## Operational readiness

1. **Working:** per provider, the age of the last successful status read is the component signal; an allocation submitted and seen running by the controller is the end-to-end signal. A growing status age, or a submission never seen in status, says it is not working.
2. **Restart:** a plugin re-reads its allocations from the provider by their tags and from control-plane state, never from local files.
3. **Silent failure:** a submission that succeeds but runs with other settings, or a stale profile. Reading allocations back after submit, and profile probes, show the difference.
4. **Provider limits:** this HBEP is where they are recorded, per platform profile.
5. **Capacity and exhaustion:** TBD per profile: grant exhaustion, per-user job limits and a managed gateway's concurrency ceiling.
6. **Idle cost:** polling and probes only, bounded by the profile's connection budget and probe rate. No allocation is held while idle unless a retention policy keeps it (HBEP-7).
7. **Rollback:** disabling a provider stops new submissions. Its running allocations keep serving until their deployments hand over, because no instance is released before its replacement serves while its deployment keeps its desired capacity (HBEP-3). An instance whose replacement is late still drains before its lease end. Profile changes revert by PR.

## Drawbacks

- Profiles need upkeep, and many facts are learned only by failure.
- The register is maintenance work that grows with every engine and router version.

## Alternatives

- **Per-site code with no shared core.** Rejected: every lifecycle rule would be written once per site.
- **One profile per provider, with account data inside.** Rejected: two grants on one platform would copy every platform fact, and account data would sit in files meant to be public.
- **Kubernetes-native offloading** (interLink and other virtual-kubelet offloading, Slinky / slurm-bridge). The evaluation (28 Oct 2026) found that they hide the lease's hard end and start estimate. Models run in provider jobs, not pods (HBEP-3, HBEP-11).
- **Contingencies off by default, enabled when needed.** Rejected: LLMLab keeps the same kinds of characteristics, so the contingencies stay on.
- **Workarounds as code patches.** Rejected: workarounds are configuration, so they can be switched off when the bug is fixed.

## Prior art and reuse

The Slurm provider plugin grows from www-2027's per-cluster ssh over one shared master connection, its job listing converted to UTC, and its watchers for state changes, moved start times and grant use, while the platform profiles and the known-issues register grow from its handbook of standing rules and gotchas.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| ssh, multiplexed | [OpenSSH](https://www.openssh.com) (the client implementation is not chosen) | BSD-style (OpenSSH) | Dependency: the default transport, one connection per login node; the only transport spike S5 (PR #19) found working |
| crossplane-runtime | [crossplane/crossplane-runtime](https://github.com/crossplane/crossplane-runtime) | Apache-2.0 | Candidate, decided by spike S4: the managed reconciler under each provider plugin, with our own Slurm dialect layer. Not Crossplane core or compositions |
| FirecREST v2; slurmrestd through Slinky slurm-client | [eth-cscs/firecrest-v2](https://github.com/eth-cscs/firecrest-v2); [SlinkyProject/slurm-client](https://github.com/SlinkyProject/slurm-client) | BSD-3-Clause; Apache-2.0 | Optional transports, where a center offers them; none is verified on Helios, Athena or Sano (open question 13) |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| A core object plus a per-platform kind; contract versions in CRD labels and release metadata | Cluster API provider contracts ([contracts](https://cluster-api.sigs.k8s.io/developer/providers/contracts/infra-machine)) | The common core with platform profiles; open question 5 |
| Typed errors, plus a "queued" state never mapped to insufficient capacity | Karpenter ([kubernetes-sigs/karpenter](https://github.com/kubernetes-sigs/karpenter)) | Results of the core operations; open questions 1 and 2 |
| Management policies without Update; deterministic external names; Observe-only import | Crossplane ([crossplane/crossplane](https://github.com/crossplane/crossplane)) | A submitted job is never edited; jobs are found by tag |
| Queue-while-loading: a request for an unloaded model waits while a job starts | ALCF FIRST ([argonne-lcf/FIRST](https://github.com/argonne-lcf/FIRST)) | A managed-provider behaviour (cold-model queueing) for the profile |
| One cached status channel per cluster | ALCF FIRST (qstat cache) | The connection budget per platform |
| Pause a provider after repeated failures, with stepped backoff | HyperQueue autoalloc ([config.rs](https://raw.githubusercontent.com/It4innovations/hyperqueue/v0.27.0/crates/hyperqueue/src/server/autoalloc/config.rs)) | Contingency parameters per platform |
| `--no-requeue`; work as `srun --overlap` steps inside a held allocation; a staged TERM, then cancel; a table of unsupported features with reasons | SkyPilot's Slurm provisioner ([skypilot-org/skypilot](https://github.com/skypilot-org/skypilot)) | The requeue proposal; capability flags; the launch-an-instance operation |
| Epoch times from `squeue` and `sacct` via `SLURM_TIME_FORMAT='%s'` | FirecREST v2 ([squeue_command.py](https://github.com/eth-cscs/firecrest-v2/blob/master/src/lib/scheduler_clients/slurm/cli_commands/squeue_command.py)) | The time proposal: read epoch times, never parse local text where avoidable |

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| Submission rules differ per cluster and are learned by failure. | L-1.16 | Submission rules are platform-profile data. |
| Schedulers print local time, so end times came out wrong. | L-1.19 | Epoch times, the profile's time zone for any text, UTC throughout. |
| Start estimates swing by hours and flip to unknown. | L-1.9 | Estimates are advisory; decisions act on actual starts. |
| A freed slot goes to another user at once. | L-1.2 | The core reports starts promptly, so HBEP-7 can release only after a replacement serves. |
| Cancelling a parent starts its pending follow-on at once. | L-1.20 | The core cancels dependents before their parent. |
| A dependency offset counts from the parent's start, not its end. | L-1.8 | Dependency semantics are platform-profile data. |
| Users cannot extend a job's time limit. | L-1.6 | A capability flag: a hard end that can move is re-read, never planned for. |
| A running job's account cannot be changed. | L-1.7 | A capability flag; the account belongs to provider settings. |
| A fast-start partition allows one job per user, pending jobs included. | L-1.14 | Per-user job limits are platform-profile data. |
| The time limit decides how long a job queues. | L-1.12 | Limits and their queueing effect are platform-profile data. |
| An in-place edit left pending jobs unschedulable. | L-1.5 | Every allocation is read back after submit and after any change. |
| A managed gateway cuts replies that send no byte for a fixed time. | L-6.1 | A managed-gateway profile field: the no-byte cut. |
| A managed gateway stalls periodically. | L-6.2 | A managed-gateway profile field: the stall pattern. |
| A managed gateway's shared capacity binds under load. | L-6.3 | The gateway's concurrency ceiling is profile data (readiness question 5). |
| Gateway keys expire mid-run. | L-6.4 | Open question 10: key expiry as a managed allocation's hard end. |
| Models go inactive unannounced, and errors for different causes look alike. | L-6.5 | The managed provider reports model state and returns a defined error. |
| A managed gateway rewrites requests. | L-6.6 | A capability flag: rewrites requests. |
| Center-run endpoints hide their limits and revisions. | L-6.8 | A capability flag: exposes revisions and limits. |
| A router's default total deadline cut long streams without a log line. | L-4.1 | A known-issues entry with a version range, a test and a workaround as configuration. |
| An engine's keep-alive was shorter than a router's connection reuse. | L-2.5 | A known-issues entry. |
| A compile on an unseen shape ran past the engine's step limit. | L-2.8 | A known-issues entry. |
| A kernel build failed on nodes without a matching toolkit. | L-2.2 | A known-issues entry, with the environment in the profile. |
| Host software differs per cluster. | L-2.13 | Environment fields in the platform profile. |
| Filesystems behave differently per cluster. | L-12.5 | Environment fields in the platform profile. |
| Parallel logins tripped a login node's connection limit. | L-7.3 | A connection budget per platform, with one shared connection per login node. |
| A partly applied cancel was reported as refused. | L-11.7 | Results are reported per allocation, never as one status. |
| The center's accounting tool counted only finished jobs. | L-13.1 | Usage from the billing formula and each allocation's actual running time. |
| Billing formulas differ per cluster. | L-13.2 | The billing formula is platform-profile data. |
| Behaviour at grant exhaustion was unknown until it happened. | L-13.4 | A platform-profile field: behaviour at grant exhaustion. |

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #4.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).
- 2026-10-30: Prior art and reuse filled from the state-of-the-art evaluation; spike S5 facts carried in (HBEP-4, HBEP-6, HBEP-7 only).

## Amendments

None.
