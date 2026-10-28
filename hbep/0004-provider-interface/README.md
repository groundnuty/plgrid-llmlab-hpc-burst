# HBEP-4: Provider interface and profiles

## Summary

This HBEP defines how the framework works with providers, the places that serve models (HBEP-3). Providers share one core interface, with capability flags that say which core operations a provider supports. Each provider's characteristics and known issues are data in a provider profile. Upstream bugs go to a known-issues register tied to versions, with a regression test and a workaround expressed as configuration. It is written for authors of provider plugins and of the lifecycle controller (HBEP-7).

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
- A new provider is added by writing its profile, plus plugin code only where data cannot express a difference.

### Non-goals

- Lifecycle decisions: when to hedge, hand over, drain or restart (HBEP-7). This HBEP exposes what those decisions need.
- How instances are reached (HBEP-6). The profile records the reachability facts HBEP-6 relies on.
- Center priority pushes. The framework never assumes one.
- The static kind's interface, which comes later.

## Proposal

### User stories

- An operator adds Cyfronet Athena. They write its profile (time limits, submission options, billing formula, container runtime), and reuse plugin code where the profile can express the difference.
- The lifecycle controller hedges `example-27b` across Helios and Athena. It reads each provider's start estimate as advisory, submits on both, acts on the first start signal and cancels the pending loser.
- LLMLab takes a model offline. The managed provider reports the model's state, which the listing does not show, and returns a defined error instead of an ambiguous one.
- An engine upgrade fixes a bug. The profile's pin passes the register entry's fixed-in version, the workaround turns off, and the regression test keeps running.
- An agent asks what an allocation has cost so far. The profile's billing formula and the allocation's actual running time give the answer.

### Notes, constraints and caveats

- **Sites are providers:** plugins with their own profiles. Providers share a common core plus capability flags.
- **Kinds** (HBEP-3): managed (a fixed catalog, no start or restart), batch (leases with unknown start and hard end, continuity through follow-ons) and static (later).
- **LLMLab will improve but keeps the same kinds of characteristics.** Its contingencies stay in its profile even when they rarely fire; thresholds keep them cheap.
- **Hedging is the framework's lever** (HBEP-3, HBEP-7). The core must therefore let the controller submit on several providers, read start estimates marked advisory, see promptly when an allocation starts, cancel a pending allocation with its dependents first, and read each allocation's hard end.
- **Agent primitives** (HBEP-10) require idempotent operations and reports of partial failure, so every core operation reports per-allocation results, and every one except submission is idempotent. For submission, a tag on the job supports recovery but does not enforce uniqueness (HBEP-7).

### Risks and mitigations

- **A stale profile acts on old facts with confidence.** Centers change policies without notice. Candidate: each field records its source and when it was last verified, and cheap probes re-check what can be checked (open question 7).
- **Start estimates are trusted.** They are advisory. Decisions act on actual starts.
- **The framework's own polling trips login throttles.** Too many parallel logins have tripped a cluster's connection limit. Candidate: a per-provider connection budget in the profile, with one shared connection.
- **Local time in scheduler output** gives wrong end times, worst across a daylight-saving change. Candidate: plugins convert every time to UTC, with tests across the change.
- **Cancelling in the wrong order starts a follow-on early.** The core cancels dependents before their parent.
- **A partly applied operation is reported as failed.** An interrupted multi-provider cancel may already have cancelled some jobs. Results are reported per allocation, never as one status.
- **A submission is accepted but runs as something else,** for example with another partition or time limit, or becomes unschedulable after an in-place change. Candidate: read every allocation back after submit and after any change.
- **Plugins find their own jobs by scanning.** The newest matching job may belong to someone else. Candidate: tag every job with its Allocation's name and find jobs only by tag.
- **A workaround outlives its bug, or masks another fault,** for example a raised time-out hiding a hang. Candidate: each register entry names the symptom it covers and the version range it applies to.
- **Contingencies cost when idle.** Probing a shared managed gateway adds load to it. Candidate: each contingency's idle cost is stated in the profile, and probes are rate-limited.
- **Flags too coarse for real providers** push provider-specific code into the core. A new flag is an amendment to this HBEP.

### Open questions

1. **Core operations.** Candidate set: describe, estimate, submit, status, cancel, follow-on, launch an instance, stop an instance, usage, remaining quota, and catalog for managed providers. Decided by walking stages 2, 3 and 7 against LLMLab, Helios and Athena in a spike.
2. **Capability flags.** Candidates: can start; can restart; has a hard end; gives start estimates; supports follow-on dependencies; supports node pinning; a hard end that can move later, for example when a center extends a time limit (the controller re-reads it and never plans for it, HBEP-3); can change a running job's account; compute nodes can connect out; compute nodes are reachable in; reports model state; exposes revisions and limits; rewrites requests; requires streaming. Decided with question 1.
3. **Profile schema.** Candidate field groups:
   - identity: center, system, partition or node type, kind;
   - capability flags;
   - hardware: accelerator, CPU architecture, memory, interconnect;
   - scheduling and submission rules, including requeue behaviour: whether jobs requeue by default, and whether the framework submits with no-requeue (HBEP-3, HBEP-7);
   - environment: container runtime, filesystems, host software;
   - reachability facts, including the known limitation on multi-node engine traffic (HBEP-6);
   - version pins;
   - billing formula and behaviour at grant exhaustion;
   - managed-gateway behaviour: no-byte cut, stall pattern, error mapping;
   - contingency parameters with thresholds;
   - references to known-issue entries.

   Format (files in the repository, or a custom resource per HBEP-11) is open. Decided by writing the LLMLab, Helios and Athena profiles as drafts.
4. **Plugin granularity.** One plugin per scheduler type with per-system profiles, or one plugin per site? Decided by drafting the Helios and Athena profiles as data first: whatever data cannot express becomes plugin code.
5. **Plugin boundary.** In-process library or separate process, the language, and how plugins declare the core version they implement. Decided with HBEP-11.
6. **Register format.** Candidate entry fields: identifier, symptom, component and affected versions, fixed-in version, provider scope and stage (HBEP-2 dimensions), fix owner, detection check, workaround configuration, regression test, upstream link. Decided in design.
7. **Profile provenance and probes.** How a field records its source (documentation, measurement, assumption) and date; which fields are re-checked, how often and at what cost. Decided by a spike on one batch and one managed provider.
8. **Public and private profiles.** Which profiles ship in this repository (LLMLab, Helios, Athena), and how a private cluster's profile stays local. Leaning: account bindings and credentials are user configuration, never profile data. Owner discussion.
9. **Engine-wide data.** Engine pins are per profile, but some known issues and the deployment-ID input rule (HBEP-3) hold for an engine version on every provider. Where does that data live? Decided with HBEP-3.
10. **A managed allocation's hard end:** none, or key expiry? Shared with HBEP-3 and HBEP-7.
11. **Overrides.** Which profile parameters a ModelDeployment may override, such as a time limit below the profile's maximum. Decided with HBEP-7.

## Design details

- **A provider is a plugin plus a profile.** The lifecycle controller calls only the common core and reads capability flags; the operations themselves are open questions 1 and 2.
- **A profile is data:** the provider's characteristics, its known issues, its version pins and its contingency parameters. Each contingency has thresholds, is enabled by default and is cheap when idle. The schema is open question 3.
- **The known-issues register** holds upstream bugs tied to versions. Each entry has one regression test and a workaround expressed as configuration. A profile pins the versions it uses, and an entry's workaround applies only within its version range. The format is open question 6.

### Test plan

- **Simulated providers** exercise every core rule and the failure modes in Risks. Candidates:
  - a batch fake with swinging estimates, freed slots taken at once, a parent cancel that starts its follow-on, a requeue that reruns a job under the same job ID, local-time output across a daylight-saving change, a one-job-per-user partition and a login throttle;
  - a managed fake with a no-byte cut, periodic stalls, models going inactive, ambiguous errors, key expiry and request rewriting.
- **One regression test per register entry.** Once CI exists, an entry without a test fails it.
- **A profile is never verified against values derived from itself.** Writing a profile from a probe's output and then checking the probe against that profile proves nothing; expectations come from documentation or an independent measurement.
- **Conformance probes** against real providers stay cheap, read-only where possible, and rate-limited.

### Compatibility and upgrades

- The profile schema and the core interface are versioned. TBD: whether an unknown profile field is refused or ignored (refusal is the safer candidate).
- Version pins change by PR to the profile. A workaround switches off when the pin passes its fixed-in version, and its regression test stays.
- Client compatibility with LLMLab is HBEP-5's concern.

### Security

- Provider credentials (cluster keys, gateway keys) are never in profiles, on command lines or in logs. Where they live is decided with HBEP-9 and HBEP-11.
- A plugin acts with the user's credentials and can spend their grant, so irreversible actions go through approvals (HBEP-10).
- Public profiles hold no account names, grant names, host names or addresses (open question 8).

### Graduation criteria

TBD. Candidate: alpha with LLMLab and one Slurm system; beta with hedging across two Slurm systems; stable when every register entry's regression test runs in CI.

## Operational readiness

1. **Working:** per provider, the age of the last successful status read is the component signal; an allocation submitted and seen running by the controller is the end-to-end signal. A growing status age, or a submission never seen in status, says it is not working.
2. **Restart:** a plugin re-reads its allocations from the provider by their tags and from control-plane state, never from local files.
3. **Silent failure:** a submission that succeeds but runs with other settings, or a stale profile. Reading allocations back after submit, and profile probes, show the difference.
4. **Provider limits:** this HBEP is where they are recorded, per profile.
5. **Capacity and exhaustion:** TBD per profile: grant exhaustion, per-user job limits and a managed gateway's concurrency ceiling.
6. **Idle cost:** polling and probes only, bounded by the profile's connection budget and probe rate. No allocation is held while idle.
7. **Rollback:** disabling a provider stops new submissions. Its running allocations keep serving until their deployments hand over, because no instance is released before its replacement serves while its deployment keeps its desired capacity (HBEP-3). An instance whose replacement is late still drains before its lease end. Profile changes revert by PR.

## Drawbacks

- Profiles need upkeep, and many facts are learned only by failure.
- The register is maintenance work that grows with every engine and router version.

## Alternatives

- **Per-site code with no shared core.** Rejected: every lifecycle rule would be written once per site.
- **Kubernetes-native offloading** (interLink and other virtual-kubelet offloading, Slinky / slurm-bridge). To be evaluated after the proposals (HBEP-2). Models run in provider jobs, not pods (HBEP-3, HBEP-11).
- **Contingencies off by default, enabled when needed.** Rejected: LLMLab keeps the same kinds of characteristics, so the contingencies stay on.
- **Workarounds as code patches.** Rejected: workarounds are configuration, so they can be switched off when the bug is fixed.

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #4.

## Amendments

None.
