# HBEP-3: Resource model and naming

## Summary

This HBEP owns the framework's vocabulary. It defines the resources (Provider, ModelDeployment, Allocation, Instance), the three levels at which a client names a model, and the deployment ID: a hash, computed by the framework, of what is served. Every other HBEP uses these terms as defined here.

## Motivation

Stage 5 (one endpoint) and stage 6 (know what answered), as defined in HBEP-2; also stage 3, because a handover must not change a name a client depends on.

Problems this addresses, in general terms:
- A follow-on job on a new node got a new URL, so every client had to re-point.
- Job names stopped matching their contents after a job was reused for another model, and operators misread them.
- The engine's own fingerprint differed per site for the same checkpoint and flags, was missing on some replies, and could not be reproduced offline.
- A checkpoint served without a pinned revision could change its tokenizer, chat template or config under the same name.
- Proving that only one configuration answered under a name was hard, because nothing named the configuration itself.

### Goals

- One vocabulary, used unchanged by every HBEP.
- Names that a client can keep across handovers and restarts. A provider-qualified name is kept across handovers within its provider.
- A deployment ID that changes when what is served changes, and only then.
- Every response an Instance produces attributable to one deployment and one Instance. An error the front raises before it chooses an Instance names the front instead (HBEP-9).

### Non-goals

- Custom-resource schemas and controllers (HBEP-11).
- Choosing a deployment, an Instance or a worker for a request (HBEP-5).
- How responses carry the deployment and instance (HBEP-9).
- Handover and hedging mechanics (HBEP-7).
- The provider-profile schema (HBEP-4).

## Proposal

### User stories

- A client written for LLMLab sends `model: example-27b`. Any deployment serving that name answers, LLMLab included.
- An evaluation needs one fixed configuration. It sends `example-27b@helios/<deployment-id>`. The name resolves while that deployment has a ready Instance on Helios. Handovers within Helios keep it resolving, because a handover keeps the deployment ID. A provider-independent pinned form is open question 12.
- A user who wants to stay off the shared gateway sends `example-27b@athena`.
- A reviewer reads the deployment ID and instance on a response, and resolves the ID to the inputs that produced it.
- An operator agent lists a deployment's instances and reads each one's job ID, node, URL and lease end as metadata.

### Notes, constraints and caveats

- Naming follows Kubernetes practice: stable object names, with labels and selectors for grouping.
- The job ID is instance metadata. It is never part of a name or of the deployment ID.
- The engine's fingerprint is recorded beside the deployment ID and never replaces it: it is not unique per instance, differs per site and is sometimes missing.
- At each handover a new Instance, with its own job ID, node, URL and lease end, replaces the old one, and the deployment ID does not change. A handover replaces an Instance; it never mutates one.
- Every term the other HBEPs share is defined under Vocabulary below, including handover, follow-on, worker, the instance phases, and the two kinds of lease.

### Risks and mitigations

- **The ID hides a real difference.** An input left out of the hash, such as a kernel the engine picks per accelerator, a local patch or an environment variable, lets two behaviours share one ID. Mitigation depends on open question 1; candidate: hash resolved settings and refuse an instance whose reported settings differ.
- **The ID splits too finely.** If a capacity or transport setting is hashed, every tuning change breaks continuity and splits load. Mitigation: a written rule for which settings are inputs (open question 3), with a test per setting.
- **The checkpoint changes under its name.** A mutable reference such as a branch cannot be hashed meaningfully. The checkpoint revision is an input; candidate: resolve it to an immutable revision when the deployment is admitted.
- **Names drift from contents.** Names derive from what is served, so a reused allocation cannot keep a name that describes its old contents (open question 8).
- **Clients re-point after a handover.** URLs are instance metadata. Clients use names, which survive handovers within a provider (Naming levels).
- **A change to the hashing rules silently changes every ID.** Candidate: a scheme version as the first hashed input, changed only by amendment.
- **Model names that contain `@` or `/` make names ambiguous** (open question 5).

### Open questions

1. **Declared or resolved settings.** Does the hash cover the spec as declared, or the settings resolved on the target system (attention backend, kernels)? Does the same spec on two accelerator classes get one ID? This decides whether moving a deployment to another system is a handover. Decided by a spike that serves one spec on two accelerator classes and compares resolved settings and outputs. Leaning: resolved settings.
2. **Managed providers.** LLMLab does not expose its revisions or flags, and its gateway changes requests. What does the ID hash for a catalog entry, given that it must never equal a self-hosted ID for the same checkpoint? Candidate: provider, catalog name and any revision the provider reports, marked opaque. Decided with HBEP-4 and HBEP-9.
3. **Which settings are inputs.** Candidate rule: settings that change the model, the tokens it sees or the arithmetic layout are inputs; placement, capacity and transport settings are not. Where the per-engine-version list lives is shared with HBEP-4. Decided by a spike over vLLM's options.
4. **Hash and display form.** Candidate: SHA-256 over canonical JSON (sorted keys, defaults made explicit), a scheme version as the first input, and a fixed-length prefix in names. Decided in design, with the prefix length checked against the expected number of deployments.
5. **Name grammar.** Can LLMLab model names contain `@` or `/`? If so, candidate rule: split at the last `@`; provider names and deployment IDs never contain `@` or `/`. Decided by reading LLMLab's `/v1/models`.
6. **Which level is the provider.** Is `provider` a center, a system or a partition (`helios`, `cyfronet-helios` or a partition name)? Leaning: a system, such as `helios`, `athena` or `llmlab`. Partitions and node types are profile details, not names. Owner discussion.
7. **Changing a hashed field.** Is that a new ModelDeployment, or a rollout inside one, as a Kubernetes Deployment rolls out a new ReplicaSet? Either way the change gives a new deployment ID, and the identity record behind the old ID is never edited (HBEP-9). HBEP-11's admission restriction depends on the answer. Shared with HBEP-7.
8. **Cardinality.** May one Allocation hold several Instances, and may an Instance span Allocations? May an Allocation be reassigned to another deployment? Leaning: an Instance lives in one Allocation, since the lease bounds its life. Shared with HBEP-7.
9. **Missing terms.** Resolved in this draft: the shared terms are defined under Vocabulary. A term any HBEP still needs is added here by amendment.
10. **A managed allocation's hard end.** Is it none, or the expiry of the key that reaches the catalog? Shared with HBEP-4 and HBEP-7.
11. **Pooling workers across Instances.** One router over the workers of several Instances could balance load across them, but the front would then not know which Instance answered. It is not part of this design, because it breaks Instance attribution. It is reconsidered only by amendment.
12. **A provider-independent pinned name.** `model@provider/<deployment-id>` stops resolving once the deployment has no ready Instance on that provider, for example after its last Instance there is replaced on another provider. Should there be a pinned form without a provider, such as `model@*/<deployment-id>`? It depends on open question 1, which decides whether the ID changes across accelerator classes, and on the grammar of open question 5.
13. **Executions under one job ID.** Slurm can requeue a batch job and run its script again under the same job ID. Is the second execution the same Allocation and the same Instance? Candidates: submit with no-requeue and treat a requeue as a failure; or treat each execution as a new Instance in the same Allocation, which needs open question 8 to allow several Instances per Allocation over time. Shared with HBEP-4 and HBEP-7.

## Design details

### Vocabulary

- **Provider:** a place that can serve models. Kinds:
  - managed: a fixed catalog, no start or restart (LLMLab);
  - batch: Slurm clusters, with leases that start at an unknown time and end at a hard limit;
  - static: later; always-on machines.
- **Provider profile:** a provider's characteristics and known issues as data. HBEP-4 owns it.
- **ModelDeployment:** what to serve: model, serving spec, number of instances, where, until when.
- **Allocation:** one lease on a provider (a Slurm job, or a managed provider's catalog entry), with its start estimate and hard end.
- **Instance:** one copy of a deployment inside an Allocation, as a lifecycle resource: it exists from its submission to its end, through the Instance phases below. Clients reach it, through the front, only while it is `ready` or `draining`.
- **Naming levels:**
  - `model`: any deployment, load-balanced, following LLMLab's model names;
  - `model@provider`;
  - `model@provider/<deployment-id>`.
- **Deployment ID:** a hash computed by the framework over what is served: checkpoint revision, engine version, flags, chat template, attention backend and similar settings. Not the job ID and not the engine fingerprint. A handover keeps it.
- **Front:** the LLMLab-compatible API endpoint (HBEP-5).
- **Control plane:** Kubernetes API custom resources plus controllers (HBEP-11). Models run in provider jobs, not pods.
- **Hedging:** submit early, on several providers if needed; keep the first allocation to start; cancel the pending losers. The winner milestone, first to start, is provisional: HBEP-7's open question 1 proposes a change to it.
- **Replacement before release:** While a deployment keeps its desired capacity, the framework never releases a serving Instance before its replacement serves. That covers handovers, and the replacement of a failing Instance, while desired instances are above zero. Shutdown at the deployment's end time, scale-down, deletion and approved early termination drain and release without a replacement.
  - A lease end is not a release. An Instance whose replacement is late drains and ends with its lease, under the drain rule (Handover).
- **Dial home:** an instance opens an outbound tunnel to the front, because compute nodes are not reachable from outside (HBEP-6).
- **Lease** (of an Allocation): an Allocation's time window.
  - It runs from a start estimate, which may be unknown, to a **hard end**: the time limit as currently observed.
  - The framework plans against the currently observed hard end and never relies on an extension. If a provider extends a job (in Slurm, privileged users can raise a running job's time limit), the new hard end is used once observed, never planned for.
  - In the HBEPs, "lease" alone means this, and "lease end" means its hard end.
- **Owner lease** (HBEP-10): an agent's time-bound claim of responsibility for a resource. Always written in full as "owner lease". A Kubernetes `Lease` object, such as a controller's leader election, is written in code form.
- **Serving spec:** the part of a ModelDeployment that says what is served and how. That is the checkpoint and its revision, the engine and its version, engine flags, the chat template and the environment. The deployment ID's inputs come from it (open questions 1 and 3).
- **Engine fingerprint:** the identifier an engine reports on its responses, such as vLLM's `system_fingerprint`. It is recorded, and is never the identity.
- **Worker:** one engine server process, which may span several GPUs, inside an Instance.
  - An Instance is the endpoint the front reaches: its own router in front of one or more workers.
  - The front picks the deployment and the Instance; the Instance's router picks the worker (HBEP-5). So the front always knows which Instance answered.
  - Pooling workers across Instances is not part of this design (open question 11).
- **Instance phases:**
  - `pending`: the Allocation has not started;
  - `starting`;
  - `verifying`: identity check and warm-up (HBEP-7, HBEP-9);
  - `ready`: takes traffic;
  - `draining`: takes no new requests, while in-flight ones finish (HBEP-5);
  - `ended`.
  Health is reported separately, as conditions such as degraded or failing (HBEP-8), in the way a Kubernetes pod has a phase and conditions.
- **Handover:** replacing an Instance whose lease is ending, or which has failed, with a new Instance of the same deployment in another Allocation.
  - Drain rule (the same text in HBEP-3, HBEP-5 and HBEP-7): The framework cannot move a lease end, so an Instance starts draining at the latest at its hard end minus the drain window, whether or not its replacement is ready. Make-before-break is the goal: the controller plans the replacement to be ready before that drain start, and the old Instance never drains before its replacement is ready unless that deadline forces it. If the replacement is late, the old Instance still drains on time, capacity drops, and an event is emitted (HBEP-8).
  - HBEP-5 sets the drain window. The old Instance is released only as Replacement before release allows.
  - The deployment ID stays the same (HBEP-7).
- **Follow-on:** a batch-provider mechanism for a handover. It is a job submitted to start when, or before, its predecessor ends, on the same node or another one. A provider's profile lists which continuity mechanisms it supports (HBEP-4).
- **Conversation key:** what the front uses to keep a conversation on the deployment and Instance that hold its context cache. Either a client session header, or a hash of the conversation's opening (HBEP-5).
- **Path** and **tunnel agent** (HBEP-6):
  - a path is the network route from the front to an Instance;
  - the tunnel agent is the process inside the Allocation that opens and keeps the dial-home tunnel.

### Resource model

- An operator declares a ModelDeployment. The control plane turns it into Allocations on the providers its "where" allows, hedging when needed (HBEP-7).
- Each Instance runs inside an Allocation and is reachable through the front. The front picks the deployment and the Instance, and the Instance's own router picks a worker (HBEP-5). So the front always knows which Instance answered.
- At a handover a new Instance, in a new Allocation, replaces the old one. The framework releases the old Instance only after the new one serves; if the new one is late, the old one drains on time and ends with its lease (drain rule, under Handover). The deployment ID stays; the new Instance has its own metadata.
- A managed provider's catalog entry is an Allocation that the framework cannot start or restart. Its hard end is open question 10.

### Naming levels

| Name | Resolves to |
|---|---|
| `model` | any deployment serving that model, load-balanced; model names follow LLMLab's |
| `model@provider` | any deployment of that model on that provider |
| `model@provider/<deployment-id>` | exactly that deployment, on any of its ready instances on that provider |

No client-facing name contains a job ID, node, port or URL.

`model@provider/<deployment-id>` resolves while that deployment has a ready Instance on that provider. Handovers within the provider keep it resolving. A pinned form without a provider is open question 12.

### Deployment ID

- **Inputs:** what is served: checkpoint revision, engine version, flags, chat template, attention backend and similar settings.
- **Not inputs:** the job ID, node, URL and lease end, which are instance metadata; the engine fingerprint, which is recorded beside the ID.
- **Computed by the framework,** not reported by the engine.
- **Kept across a follow-on** that replaces a job.
- **Every response an Instance produces names its deployment and Instance.** An error the front raises before it chooses an Instance names the front instead (HBEP-9 defines how).
- The exact canonical form and hash are open (questions 1, 3 and 4).

### Test plan

- Golden vectors: fixed spec inputs and their expected IDs, written once and checked into the repository. They are never regenerated by the code under test, since an expectation computed by the same canonicaliser proves nothing.
- The same spec with fields in a different order gives the same ID.
- Changing each input changes the ID; changing each non-input (job ID, node, URL, lease end, instance count) does not.
- A simulated handover keeps the ID and replaces the Instance with a new one.
- The name parser handles model names containing `/` and `@`, per open question 5.
- If question 1 resolves to resolved settings: an instance that reports settings differing from its deployment's inputs is refused.

### Compatibility and upgrades

- `model` names follow LLMLab's, so clients written for LLMLab keep working. The other two levels are opt-in.
- Clients treat deployment IDs as opaque strings. A downgrade leaves issued IDs readable.
- A change to the hashing rules is an amendment with a new scheme version. IDs already issued stay valid as records.

### Security

- Credentials, such as a token for a model repository, are never hash inputs and never appear in names: a hash of a short secret can be reversed by guessing.
- Names and IDs carry no private paths or account names.
- Who may resolve an ID back to its inputs is decided in HBEP-9.

### Graduation criteria

TBD. Candidate: alpha when IDs are computed and named on responses for LLMLab and one batch provider; beta when handovers keep the ID on each of two batch providers; stable when the scheme is frozen with golden vectors.

## Operational readiness

1. **Working:** every serving instance carries its deployment's ID, and every response an Instance produces names a deployment and Instance known to the inventory. Such a response without them, or naming an unknown ID, is the failure signal.
2. **Restart:** IDs are recomputed from stored specs. A recomputed ID that differs from the stored one means the hashing rules drifted.
3. **Silent failure:** two instances under one ID that serve differently, because an input was missed. Comparing the engine fingerprints and reported settings recorded within one ID on one provider shows it.
4. **Provider limits:** framework names are not provider job names. Job-name rules (prefixes, length) belong to the provider profile (HBEP-4).
5. **Capacity and exhaustion:** N/A; naming holds no capacity.
6. **Idle cost:** none.
7. **Rollback:** a hashing change rolls back with its scheme version; issued IDs stay valid records.

## Drawbacks

- The rule for which settings are inputs needs upkeep for each engine version.
- Deployment IDs are opaque to people, so tools must show the inputs beside them.
- Three naming levels add parsing to the front.

## Alternatives

- **The job ID as identity.** Rejected: it changes at every follow-on.
- **The engine fingerprint as identity.** Rejected: it differs per site, is not unique per instance and is sometimes missing.
- **Names chosen by people.** Rejected: they drift from contents when allocations are reused.
- **Kubernetes' `pod-template-hash`.** A Deployment names each ReplicaSet by a hash of its pod template, and a replaced pod keeps that hash. The deployment ID follows the same idea, over what is served instead of over a pod template.

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #3.

## Amendments

None.
