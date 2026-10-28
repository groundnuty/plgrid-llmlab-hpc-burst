# HBEP-3: Resource model and naming

## Summary

This HBEP owns the framework's vocabulary. It defines the levels above our access to compute (Organization, Platform), the resources (Provider, ModelDeployment, Allocation, Instance), the three levels at which a client names a model, and the deployment ID: a hash, computed by the framework, of what is served. Every other HBEP uses these terms as defined here.

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
- Capacity and what runs on it with separate lives, as Kubernetes nodes and pods have: an Allocation can outlive its Instances and host another deployment's.

### Non-goals

- Custom-resource schemas and controllers (HBEP-11).
- Choosing a deployment, an Instance or a worker for a request (HBEP-5).
- How responses carry the deployment and instance (HBEP-9).
- Handover, hedging, over-allocation, retention and repurposing mechanics (HBEP-7).
- The schemas of platform profiles and provider settings (HBEP-4).

## Proposal

### User stories

In these stories, `helios-a`, `helios-b` and `athena-a` are names an operator gave its providers on Helios and Athena. The names mean nothing to the framework.
- A client written for LLMLab sends `model: example-27b`. The deployments of that model on the providers the operator attached to the name answer. The operator attached the LLMLab provider in one setup step, so LLMLab still answers.
- An evaluation needs one fixed configuration. It sends `example-27b@helios-a/<deployment-id>`. The name resolves while that deployment has a ready Instance on `helios-a`. Handovers within `helios-a` keep it resolving, because a handover keeps the deployment ID. A provider-independent pinned form is open question 12.
- A user who wants to stay off the shared gateway sends `example-27b@athena-a`.
- The operator holds two grants on Helios. They are two providers, `helios-a` and `helios-b`, each with its own credentials, and both carry the Helios platform label. A query by that label lists both.
- An agent's work on one model ends while its Allocations have hours left. It repurposes them to another model's deployment. No job is queued again, and no name still describes the old model.
- A reviewer reads the deployment ID and instance on a response, and resolves the ID to the inputs that produced it.
- An operator agent lists a deployment's instances and reads each one's job ID, node, URL and lease end as metadata.

### Notes, constraints and caveats

- Naming follows Kubernetes practice: stable object names, with labels and selectors for grouping.
- Provider names are arbitrary, like Kubernetes object names. Organization, platform, partition and account are labels on the Provider, and queries select by labels. Label selectors live in the API and in routing policy, never in client model names.
- An Allocation and its provider job are named without the model they serve, because an Allocation can be repurposed. What is served is named by the deployment ID of the Instance the Allocation hosts.
- The job ID is instance metadata. It is never part of a name or of the deployment ID.
- The engine's fingerprint is recorded beside the deployment ID and never replaces it: it is not unique per instance, differs per site and is sometimes missing.
- At each handover a new Instance, with its own job ID, node, URL and lease end, replaces the old one, and the deployment ID does not change. A handover replaces an Instance; it never mutates one.
- Every term the other HBEPs share is defined under Vocabulary below, including handover, follow-on, worker, the instance phases, and the two kinds of lease.

### Risks and mitigations

- **The ID hides a real difference.** An input left out of the hash, such as a kernel the engine picks per accelerator, a local patch or an environment variable, lets two behaviours share one ID. Mitigation depends on open question 1; candidate: hash resolved settings and refuse an instance whose reported settings differ.
- **The ID splits too finely.** If a capacity or transport setting is hashed, every tuning change breaks continuity and splits load. Mitigation: a written rule for which settings are inputs (open question 3), with a test per setting.
- **The checkpoint changes under its name.** A mutable reference such as a branch cannot be hashed meaningfully. The checkpoint revision is an input; candidate: resolve it to an immutable revision when the deployment is admitted.
- **Names drift from contents.** Names of what is served derive from it, and Allocation names never describe a model, so a repurposed Allocation keeps no stale name.
- **Clients re-point after a handover.** URLs are instance metadata. Clients use names, which survive handovers within a provider (Naming levels).
- **Plain `model` reaches nothing after a switch.** No provider is attached to a model name by default, LLMLab included. Until the operator attaches one, the name returns the inactive-model error (HBEP-5); it never falls back silently.
- **A change to the hashing rules silently changes every ID.** Candidate: a scheme version as the first hashed input, changed only by amendment.
- **Model names that contain `@` or `/` make names ambiguous** (open question 5).

### Open questions

1. **Declared or resolved settings.** Does the hash cover the spec as declared, or the settings resolved on the target system (attention backend, kernels)? Does the same spec on two accelerator classes get one ID? This decides whether moving a deployment to another system is a handover. Decided by a spike that serves one spec on two accelerator classes and compares resolved settings and outputs. Leaning: resolved settings.
2. **Managed providers.** LLMLab does not expose its revisions or flags, and its gateway changes requests. What does the ID hash for a catalog entry, given that it must never equal a self-hosted ID for the same checkpoint? Candidate: platform, catalog name and any revision the platform reports, marked opaque. The platform, not the provider: provider names are arbitrary, and two providers on LLMLab serve the same entry. Decided with HBEP-4 and HBEP-9.
3. **Which settings are inputs.** Candidate rule: settings that change the model, the tokens it sees or the arithmetic layout are inputs; placement, capacity and transport settings are not. Where the per-engine-version list lives is shared with HBEP-4. Decided by a spike over vLLM's options.
4. **Hash and display form.** Candidate: SHA-256 over canonical JSON (sorted keys, defaults made explicit), a scheme version as the first input, and a fixed-length prefix in names. Decided in design, with the prefix length checked against the expected number of deployments.
5. **Name grammar.** Can LLMLab model names contain `@` or `/`? If so, candidate rule: split at the last `@`; provider names and deployment IDs never contain `@` or `/`. Decided by reading LLMLab's `/v1/models`.
6. **Which level is the provider.** Resolved (owner decision): three levels. An Organization runs Platforms. A Provider is our access to one Platform: an account or grant, its credentials and settings. Provider names are arbitrary; organization, platform, partition and account are labels on the Provider. Two grants on one platform are two providers. `model@<provider>` uses the provider's name, and label selectors stay in the API and in routing policy (Vocabulary, Naming levels).
7. **Changing a hashed field.** Is that a new ModelDeployment, or a rollout inside one, as a Kubernetes Deployment rolls out a new ReplicaSet? Either way the change gives a new deployment ID, and the identity record behind the old ID is never edited (HBEP-9). HBEP-11's admission restriction depends on the answer. Shared with HBEP-7.
8. **Cardinality.** Resolved in part (owner decision): an Allocation may be reassigned to another ModelDeployment (Repurposing), and an Instance lives in one Allocation. So an Allocation may host Instances one after another. Still open: may one Allocation host several Instances at once? Candidate: yes, when its resources allow. Shared with HBEP-7.
9. **Missing terms.** Resolved in this draft: the shared terms are defined under Vocabulary. A term any HBEP still needs is added here by amendment.
10. **A managed allocation's hard end.** Is it none, or the expiry of the key that reaches the catalog? Shared with HBEP-4 and HBEP-7.
11. **Pooling workers across Instances.** One router over the workers of several Instances could balance load across them, but the front would then not know which Instance answered. It is not part of this design, because it breaks Instance attribution. It is reconsidered only by amendment.
12. **A provider-independent pinned name.** `model@<provider>/<deployment-id>` stops resolving once the deployment has no ready Instance on that provider, for example after its last Instance there is replaced on another provider. Should there be a pinned form without a provider, such as `model@*/<deployment-id>`? It depends on open question 1, which decides whether the ID changes across accelerator classes, and on the grammar of open question 5.
13. **Executions under one job ID.** Slurm can requeue a batch job and run its script again under the same job ID. Is the second execution the same Allocation and the same Instance? Candidates: submit with no-requeue and treat a requeue as a failure; or treat each execution as a new Instance in the same Allocation, which open question 8 allows, since an Allocation may host Instances one after another. Shared with HBEP-4 and HBEP-7.

## Design details

### Vocabulary

- **Organization:** a computing center or company that runs platforms, such as Cyfronet or a university computing center. The HBEPs also call it a center.
- **Platform:** a computing system that an organization runs, such as Helios, Athena, LLMLab or a small cluster.
- **Provider:** our access to one platform: an account or grant, its credentials and its settings.
  - Its name is arbitrary, like a Kubernetes object name, so it never contains `@` or `/`.
  - Organization, platform, partition and account are labels on it. Queries and routing policy (HBEP-5) select providers by these labels.
  - Two grants on one platform are two providers.
  - Its kind follows its platform:
    - managed: a fixed catalog, no start or restart. LLMLab is a platform of the Cyfronet organization, and a provider on it is a provider like any other;
    - batch: Slurm clusters, with leases that start at an unknown time and end at a hard limit;
    - static: later; always-on machines.
- **Platform profile:** what is true for everyone on a platform, as data: hardware, architecture, limits, time zone, filesystems, requeue behaviour, expected performance and known issues. "The profile" alone means this. HBEP-4 owns it.
- **Provider settings:** what one provider adds to its platform profile: the account, the grant, a credentials reference, the allowed partitions and the labels. HBEP-4 owns them.
- **ModelDeployment:** what to serve: model, serving spec, number of instances, where (which providers), until when.
- **Allocation:** capacity with a lifetime, like a Kubernetes node. It is one lease on a provider (a Slurm job, or a managed provider's catalog entry), with its start estimate and hard end.
  - Instances are placed on Allocations, as pods are on nodes.
  - An Allocation can outlive its Instances. Its retention policy then keeps or releases it, and it can later host Instances of a different ModelDeployment (Repurposing).
  - A managed catalog entry serves only its own model, so it is never repurposed.
- **Instance:** one copy of a deployment inside one Allocation, as a lifecycle resource: it exists from its creation to its end, through the Instance phases below. Clients reach it, through the front, only while it is `ready` or `draining`.
- **Naming levels:**
  - `model`: the deployments of that model on the providers the operator attached to the name, load-balanced; model names follow LLMLab's;
  - `model@<provider>`;
  - `model@<provider>/<deployment-id>`.
- **Deployment ID:** a hash computed by the framework over what is served: checkpoint revision, engine version, flags, chat template, attention backend and similar settings. Not the job ID, not the engine fingerprint, and never a key. A handover keeps it.
- **Front:** the LLMLab-compatible API endpoint (HBEP-5). Its client key is set by the operator (HBEP-5).
- **Control plane:** Kubernetes API custom resources plus controllers (HBEP-11). Models run in provider jobs, not pods.
- **Hedging:** submit early, on several providers if needed; keep the first allocation to start; cancel the pending losers. The winner milestone, first to start, is provisional: HBEP-7's open question 1 proposes a change to it. Hedging acts on pending Allocations; over-allocation acts on started ones.
- **Over-allocation:** obtain N+k Allocations for N Instances, qualify each Instance, and keep the best N. The rest are released, or kept as warm capacity, per their retention policy (HBEP-7).
- **Qualification:** the gate an Instance passes before it is kept: an identity check (HBEP-9) plus a short benchmark against the expected performance in its platform profile. It catches broken nodes, not only slow ones (HBEP-7).
- **Retention policy:** what happens to an Allocation that hosts no Instance. Either it is kept as warm capacity, ready to host an Instance, until a date, a grant's end or a deadline; or it is released when idle. A kept Allocation still ends at its hard end (HBEP-7).
- **Repurposing:** moving an Allocation to another ModelDeployment when a model's work finishes. The old Instance drains; an Instance of the new deployment starts in the same Allocation; it is verified and warmed up. An operator asks for it, often an agent that needs throughput across several models. Conversions between models are this operation (HBEP-7). Whether it needs approval is HBEP-10's question.
- **Replacement before release:** While a deployment keeps its desired capacity, the framework never releases a serving Instance before its replacement serves. That covers handovers, and the replacement of a failing Instance, while desired instances are above zero. Shutdown at the deployment's end time, scale-down, deletion, approved early termination and repurposing lower a deployment's capacity: their Instances drain and end without a replacement, and each Allocation left with no Instance follows its retention policy. Repurposing takes an Allocation from a deployment that is ending or scaling down.
  - A lease end is not a release. An Instance whose replacement is late drains and ends with its lease, under the drain rule (Handover).
- **Dial home:** an instance opens an outbound tunnel to the front, because compute nodes are not reachable from outside (HBEP-6).
- **Lease** (of an Allocation): an Allocation's time window.
  - It runs from a start estimate, which may be unknown, to a **hard end**: the time limit as currently observed.
  - The framework plans against the currently observed hard end and never relies on an extension. If a provider extends a job (in Slurm, privileged users can raise a running job's time limit), the new hard end is used once observed, never planned for.
  - In the HBEPs, "lease" alone means this, and "lease end" means its hard end.
- **Owner lease** (HBEP-10): an agent's time-bound claim of responsibility for a resource. Always written in full as "owner lease". A Kubernetes `Lease` object, such as a controller's leader election, is written in code form.
- **Serving spec:** the part of a ModelDeployment that says what is served and how. That is the checkpoint and its revision, the engine and its version, engine flags, the chat template and the environment. The deployment ID's inputs come from it (open questions 1 and 3).
- **Engine fingerprint:** the identifier an engine reports on its responses, such as vLLM's `system_fingerprint`. It is recorded, and is never the identity.
- **Instance key:** each Instance's own key, generated when it starts and kept in its identity package, the identity data the Instance holds (HBEP-9). The front uses it toward the Instance. It is never a deployment-ID input.
- **Worker:** one engine server process, which may span several GPUs, inside an Instance.
  - An Instance is the endpoint the front reaches: its own router in front of one or more workers.
  - The front picks the deployment and the Instance; the Instance's router picks the worker (HBEP-5). So the front always knows which Instance answered.
  - Pooling workers across Instances is not part of this design (open question 11).
- **Instance phases:**
  - `pending`: the Allocation has not started;
  - `starting`;
  - `verifying`: identity check, the qualification benchmark when one runs, and warm-up (HBEP-7, HBEP-9);
  - `ready`: takes traffic;
  - `draining`: takes no new requests, while in-flight ones finish (HBEP-5);
  - `ended`.
  Health is reported separately, as conditions such as degraded or failing (HBEP-8), in the way a Kubernetes pod has a phase and conditions.
- **Handover:** replacing an Instance whose lease is ending, or which has failed, with a new Instance of the same deployment in another Allocation.
  - Drain rule (the same text in HBEP-3, HBEP-5 and HBEP-7): The framework cannot move a lease end, so an Instance starts draining at the latest at its hard end minus the drain window, whether or not its replacement is ready. Make-before-break is the goal: the controller plans the replacement to be ready before that drain start, and the old Instance never drains before its replacement is ready unless that deadline forces it. If the replacement is late, the old Instance still drains on time, capacity drops, and an event is emitted (HBEP-8).
  - HBEP-5 sets the drain window. The old Instance is released only as Replacement before release allows.
  - The deployment ID stays the same (HBEP-7).
- **Follow-on:** a batch-provider mechanism for a handover. It is a job submitted to start when, or before, its predecessor ends, on the same node or another one. A platform profile lists which continuity mechanisms the platform supports (HBEP-4).
- **Conversation key:** what the front uses to keep a conversation on the deployment and Instance that hold its context cache. Either a client session header, or a hash of the conversation's opening (HBEP-5).
- **Path** and **tunnel agent** (HBEP-6):
  - a path is the network route from the front to an Instance;
  - the tunnel agent is the process inside the Allocation that opens and keeps the dial-home tunnel.

### Resource model

- A Provider is our access to one platform. Several providers can share a platform, each with its own name, labels and settings, and all of them use that platform's profile (HBEP-4).
- An operator declares a ModelDeployment. The control plane places its Instances on Allocations on the providers its "where" allows: Allocations it obtains, hedging and over-allocating when needed, or Allocations repurposed to it (HBEP-7).
- Each Instance runs inside one Allocation and is reachable through the front. The front picks the deployment and the Instance, and the Instance's own router picks a worker (HBEP-5). So the front always knows which Instance answered.
- An Allocation can outlive its Instances. With no Instance, its retention policy keeps it as warm capacity or releases it, and an operator can repurpose it to another ModelDeployment. Whether it hosts several Instances at once is open question 8.
- At a handover a new Instance, in another Allocation, replaces the old one. The framework releases the old Instance only after the new one serves; if the new one is late, the old one drains on time and ends with its lease (drain rule, under Handover). The deployment ID stays; the new Instance has its own metadata.
- A managed provider's catalog entry is an Allocation that the framework cannot start, restart or repurpose. Its hard end is open question 10.

### Naming levels

| Name | Resolves to |
|---|---|
| `model` | the deployments of that model on the providers the operator attached to that name, load-balanced. No provider is attached by default, LLMLab included. Model names follow LLMLab's |
| `model@<provider>` | any deployment of that model on the provider with that name |
| `model@<provider>/<deployment-id>` | exactly that deployment, on any of its ready instances on that provider |

No client-facing name contains a job ID, node, port, URL or label selector.

- **Provider names** are the operator's arbitrary names. Selecting providers by label, such as every provider on one platform, happens in the API and in routing policy, never in a client model name.
- **Attaching.** The operator attaches providers to a model name in routing policy, by name or by label selector (HBEP-5). A user switching from LLMLab who wants requests to keep reaching it attaches the LLMLab provider explicitly, as one setup step.
- **Pinned names.** `model@<provider>/<deployment-id>` resolves while that deployment has a ready Instance on that provider. Handovers within the provider keep it resolving. A pinned form without a provider is open question 12.

### Deployment ID

- **Inputs:** what is served: checkpoint revision, engine version, flags, chat template, attention backend and similar settings.
- **Not inputs:** the job ID, node, URL and lease end, which are instance metadata; the engine fingerprint, which is recorded beside the ID; keys, including each Instance's own key, so that rotating a key never changes the ID.
- **Computed by the framework,** not reported by the engine.
- **Kept across a follow-on** that replaces a job.
- **Every response an Instance produces names its deployment and Instance.** An error the front raises before it chooses an Instance names the front instead (HBEP-9 defines how).
- The exact canonical form and hash are open (questions 1, 3 and 4).

### Test plan

- Golden vectors: fixed spec inputs and their expected IDs, written once and checked into the repository. They are never regenerated by the code under test, since an expectation computed by the same canonicaliser proves nothing.
- The same spec with fields in a different order gives the same ID.
- Changing each input changes the ID; changing each non-input (job ID, node, URL, lease end, instance count, an Instance's key) does not.
- A simulated handover keeps the ID and replaces the Instance with a new one.
- A simulated repurpose keeps the Allocation and its name; the new Instance carries the new deployment's ID.
- Two providers on one platform: `model@<provider>` resolves to each separately, and a selector on the platform label lists both.
- Plain `model` with no provider attached returns the inactive-model error. Once the operator attaches the LLMLab provider, LLMLab answers.
- The name parser handles model names containing `/` and `@`, per open question 5.
- If question 1 resolves to resolved settings: an instance that reports settings differing from its deployment's inputs is refused.

### Compatibility and upgrades

- `model` names follow LLMLab's, so clients written for LLMLab keep working once the operator attaches the providers that should serve each name, LLMLab's among them if it should keep answering. The other two levels are opt-in.
- Clients treat deployment IDs as opaque strings. A downgrade leaves issued IDs readable.
- A change to the hashing rules is an amendment with a new scheme version. IDs already issued stay valid as records.

### Security

- Credentials, such as a token for a model repository or an Instance's own key, are never hash inputs and never appear in names: a hash of a short secret can be reversed by guessing.
- Deployment IDs carry no private paths or account names. The account is a label on the Provider, not its name, because provider names appear in client model names.
- Who may resolve an ID back to its inputs is decided in HBEP-9.

### Graduation criteria

TBD. Candidate: alpha when IDs are computed and named on responses for LLMLab and one batch provider; beta when handovers keep the ID on each of two batch providers; stable when the scheme is frozen with golden vectors.

## Operational readiness

1. **Working:** every serving instance carries its deployment's ID, and every response an Instance produces names a deployment and Instance known to the inventory. Such a response without them, or naming an unknown ID, is the failure signal.
2. **Restart:** IDs are recomputed from stored specs. A recomputed ID that differs from the stored one means the hashing rules drifted.
3. **Silent failure:** two instances under one ID that serve differently, because an input was missed. Comparing the engine fingerprints and reported settings recorded within one ID on one provider shows it.
4. **Provider limits:** framework names are not provider job names. Job-name rules (prefixes, length) belong to the platform profile (HBEP-4).
5. **Capacity and exhaustion:** N/A; naming holds no capacity.
6. **Idle cost:** none.
7. **Rollback:** a hashing change rolls back with its scheme version; issued IDs stay valid records.

## Drawbacks

- The rule for which settings are inputs needs upkeep for each engine version.
- Deployment IDs are opaque to people, so tools must show the inputs beside them.
- Three naming levels add parsing to the front.
- Plain `model` reaches nothing until the operator attaches providers: one more setup step for a user switching from LLMLab.

## Alternatives

- **The job ID as identity.** Rejected: it changes at every follow-on.
- **The engine fingerprint as identity.** Rejected: it differs per site, is not unique per instance and is sometimes missing.
- **Names chosen by people for what is served.** Rejected: they drift from contents when allocations are repurposed. Provider names are chosen by people, because they name access to a platform, which does not drift.
- **The provider as the platform** (one name per system). Rejected: two grants on one platform need two providers, with their own credentials and labels.
- **Label selectors in client model names.** Rejected: clients keep plain names, and which providers serve a name is the operator's routing policy.
- **An Allocation bound to one deployment for life.** Rejected: capacity left when one model's work ends would go back to the queue, and a new job would wait again.
- **Kubernetes' `pod-template-hash`.** A Deployment names each ReplicaSet by a hash of its pod template, and a replaced pod keeps that hash. The deployment ID follows the same idea, over what is served instead of over a pod template.

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #3.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).

## Amendments

None.
