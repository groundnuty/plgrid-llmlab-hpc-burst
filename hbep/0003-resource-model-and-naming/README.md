# HBEP-3: Resource model and naming

## Summary

This HBEP owns the framework's vocabulary. It defines the levels above our access to compute (Organization, Platform), the resources (Provider, PlatformProfile, ModelDeployment, Allocation, Instance), the three levels at which a client names a model, and the deployment ID: a hash, computed by the framework, of what is served. Every other HBEP uses these terms as defined here. HBEP-11 serves the resources as custom resources in the API group `hpcburst.groundnuty.github.io`, version `v1alpha1`.

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
- An evaluation needs one fixed configuration. It sends `example-27b@helios-a/<deployment-id>`. The name resolves while that deployment ID has a ready Instance on `helios-a`. Handovers within `helios-a` and one accelerator class keep it resolving, because such a handover keeps the deployment ID. A provider-independent pinned form is open question 12.
- A user who wants to stay off the shared gateway sends `example-27b@athena-a`.
- The operator holds two grants on Helios. They are two providers, `helios-a` and `helios-b`, each with its own credentials, and both carry the Helios platform label. A query by that label lists both.
- An agent's work on one model ends while its Allocations have hours left. It repurposes them to another model's deployment. No job is queued again, and no name still describes the old model.
- A reviewer reads the deployment ID and instance on a response, and resolves the ID to the inputs that produced it.
- An operator agent lists a deployment's instances and reads each one's job ID, node, URL and lease end as metadata.
- A maintainer fixes the chat template of a serving deployment. The ModelDeployment rolls out a new deployment ID in each accelerator class, and its Instances are replaced make-before-break at their next handovers. The old ID's record is unchanged.

### Notes, constraints and caveats

- Naming follows Kubernetes practice: stable object names, with labels and selectors for grouping.
- Provider names are arbitrary, like Kubernetes object names. Organization, platform, partition and account are labels on the Provider, and queries select by labels. Label selectors live in the API and in routing policy, never in client model names.
- An Allocation and its provider job are named without the model they serve, because an Allocation can be repurposed. What is served is named by the deployment ID of the Instance the Allocation hosts.
- The job ID is instance metadata. It is never part of a name or of the deployment ID.
- The engine's fingerprint is recorded beside the deployment ID and never replaces it: it is not unique per instance, differs per site and is sometimes missing.
- At each handover a new Instance, with its own job ID, node, URL and lease end, replaces the old one, and the deployment ID does not change. A handover replaces an Instance; it never mutates one.
- Every term the other HBEPs share is defined under Vocabulary below, including handover, follow-on, worker, the instance phases, and the two kinds of lease.
- The kinds keep the names below (owner decision, 30 Oct 2026). Crossplane also has a kind named `Provider`, in its own group, for an installed provider package. The two never collide, because ours live in `hpcburst.groundnuty.github.io`. Where the Slurm provider uses crossplane-runtime (spike S4, PR #32), read crossplane's `ProviderConfig` as our Provider.

### Risks and mitigations

- **The ID hides a real difference.** An input left out of the hash, such as a kernel the engine picks per accelerator, a local patch or an environment variable, lets two behaviours share one ID. Scheme `v1` therefore hashes the accelerator class (GPU model and CPU architecture) with the declared spec. One spec on two accelerator classes has two IDs, since the engine resolves kernels per GPU type (L-8.2). Within one accelerator class, each Instance's resolved settings are kept in its verification record beside the ID (HBEP-11), so a remaining difference is visible (Operational readiness 3). A declared setting that the engine contradicts fails verification. A later spike may move resolved settings into scheme `v2` (open question 1).
- **The ID splits too finely.** If a capacity or transport setting is hashed, every tuning change breaks continuity and splits load. Mitigation: a written rule for which settings are inputs (open question 3), with a test per setting. In scheme `v1` an engine flag that the rule does not list is an input: a split is visible, a hidden difference is not. A flag omitted and the same flag set to the engine's default also give two IDs, because only the API schema's defaults are made explicit.
- **The checkpoint changes under its name.** A mutable reference such as a branch cannot be hashed meaningfully. The revision must be a full commit id, and the checkpoint's per-file SHA-256 list is an input (Deployment ID).
- **Names drift from contents.** Names of what is served derive from it, and Allocation names never describe a model, so a repurposed Allocation keeps no stale name.
- **Clients re-point after a handover.** URLs are instance metadata. Clients use names, which survive handovers within a provider (Naming levels).
- **Plain `model` reaches nothing after a switch.** No provider is attached to a model name by default, LLMLab included. Until the operator attaches one, the name returns the inactive-model error (HBEP-5); it never falls back silently.
- **A change to the hashing rules silently changes every ID.** The scheme version is a field of the hashed object, changed only by amendment, and golden vectors catch an unintended change (Test plan).
- **Two deployment IDs share a display form.** Names carry 12 hex characters of the hash. The controller refuses to issue an ID whose display form an existing identity record holds with a different full hash, and raises an event (Deployment ID).
- **Model names that contain `@` or `/` make names ambiguous.** The grammar splits at the last `@` (open question 5). A model whose own name contains `@` is then reachable only in a qualified form; reading LLMLab's model list before beta shows whether any exists.

### Open questions

1. **Declared or resolved settings.** Decided: for alpha, scheme `v1` hashes the declared serving spec, the accelerator class (GPU model and CPU architecture) and the checkpoint's per-file SHA-256 list (spike S7, PR #33). The accelerator class is known before start, from the Allocation's partition, and is what the engine's kernel choices mostly depend on (L-8.2). The engine's resolved settings are recorded beside the hash, in each Instance's verification records, one per verification (HBEP-9, HBEP-11); the identity record never changes. An Instance whose reported settings contradict the declared ones fails verification. So in `v1` a ModelDeployment has one current ID per accelerator class (Deployment ID). An Instance placed in another class is a new Instance with that class's current ID: both IDs are current, so this is neither a rollout nor a handover that keeps the ID. Hashing the full resolved settings is left to a spike that serves one spec on two accelerator classes and compares resolved settings and outputs. If that refines the rule, it becomes scheme `v2`, and `v1` IDs stay valid records (Compatibility and upgrades).
2. **Managed providers.** Decided: for alpha, the ID of a catalog entry hashes the platform, the catalog name and any revision the platform reports, marked opaque. The platform, not the provider: provider names are arbitrary, and two providers on LLMLab serve the same entry. A catalog entry's canonical form has a different shape from a served deployment's (Deployment ID), so it never equals a self-hosted ID for the same checkpoint. HBEP-9's lean for the managed identity record agrees.
3. **Which settings are inputs.** Decided: for alpha, the candidate rule. Settings that change the model, the tokens it sees or the arithmetic layout are inputs; placement, capacity, transport and logging settings are not, and keys never are. Scheme `v1` holds the classification as a table per engine: vLLM's is under Deployment ID, and the code holds the same table, covered by golden vectors. A setting the table does not list is an input. Changing the table is a new scheme version. Batch-shaping settings count as capacity in `v1`; HBEP-9's measurement may move them in a later scheme. Where engine-wide data lives beyond alpha stays HBEP-4's open question 9.
4. **Hash and display form.** Decided: SHA-256 over the object's RFC 8785 serialisation (sorted keys, the API schema's defaults made explicit), with the scheme version `v1` as a field of the object. Names use the first 12 hex characters. The full hash is kept in the identity record. Deployment ID gives the exact object, a worked example, and why 12 hex characters are enough.
5. **Name grammar.** Decided: split at the last `@`. Provider names and deployment IDs never contain `@` or `/`. Provider names are validated as Kubernetes DNS-1123 labels, since they are object names. Reading LLMLab's `/v1/models`, to confirm no LLMLab name breaks the grammar, is a check before beta (Graduation criteria).
6. **Which level is the provider.** Decided: three levels (owner decision). An Organization runs Platforms. A Provider is our access to one Platform: an account or grant, its credentials and settings. Provider names are arbitrary; organization, platform, partition and account are labels on the Provider. Two grants on one platform are two providers. `model@<provider>` uses the provider's name, and label selectors stay in the API and in routing policy (Vocabulary, Naming levels).
7. **Changing a hashed field.** Decided: a rollout inside one ModelDeployment, as a Kubernetes Deployment rolls out a new ReplicaSet. In each accelerator class, Instances of the new current ID replace those of the old ID make-before-break, at the next natural handover by default (HBEP-7, open question 9). The identity record behind the old ID is never edited (HBEP-9). So HBEP-11's admission allows the edit to the ModelDeployment, and refuses only an edit to an existing identity record.
8. **Cardinality.** Decided: an Allocation may be reassigned to another ModelDeployment (Repurposing; owner decision), and an Instance lives in one Allocation. One Allocation may host several Instances at once when its resources allow. Alpha places one Instance per Allocation.
9. **Missing terms.** Decided: the shared terms are defined under Vocabulary. A term any HBEP still needs is added here by amendment.
10. **A managed allocation's hard end.** Decided: for alpha, none. Key expiry is a provider-settings warning (HBEP-4), not a lease end.
11. **Pooling workers across Instances.** Decided: not part of this design. One router over the workers of several Instances could balance load across them, but the front would then not know which Instance answered, which breaks Instance attribution. It is reconsidered only by amendment.
12. **A provider-independent pinned name.** Not needed for alpha: `model@<provider>/<deployment-id>` stops resolving once that ID has no ready Instance on that provider, for example after its last Instance there is replaced on another provider or in another accelerator class. Should there be a pinned form without a provider, such as `model@*/<deployment-id>`? It stays open. It depends on open question 1, whose `v1` answer gives one ID per accelerator class, and on the grammar of open question 5.
13. **Executions under one job ID.** Decided: from spike S4 (PR #32), submit with `--no-requeue`, and treat a requeue that still happens as a failure of that Instance. Requeue is on by default on every platform measured (spike S5, PR #19), and a site setting can still force one, so the check stays. The hold on the Allocation's job is lifted exactly once, so a requeued job that returns to a user hold stays held. The controller deletes the failed Allocation, its finalizer cancels the held job, and the replacement is a new Allocation. The alternative, each execution as a new Instance in the same Allocation, is dropped.

## Design details

### Vocabulary

- **Organization:** a computing center or company that runs platforms, such as Cyfronet or a university computing center. The HBEPs also call it a center.
- **Platform:** a computing system that an organization runs, such as Helios, Athena, LLMLab or a small cluster.
- **Provider:** our access to one platform: an account or grant, its credentials and its settings.
  - Its name is arbitrary, like a Kubernetes object name. It is a DNS-1123 label, so it never contains `@`, `/` or `.`.
  - Organization, platform, partition and account are labels on it. Queries and routing policy (HBEP-5) select providers by these labels.
  - Two grants on one platform are two providers.
  - Its kind follows its platform:
    - managed: a fixed catalog, no start or restart. LLMLab is a platform of the Cyfronet organization, and a provider on it is a provider like any other;
    - batch: Slurm clusters, with leases that start at an unknown time and end at a hard limit;
    - static: later; always-on machines.
- **Platform profile:** what is true for everyone on a platform, as data: hardware, architecture, limits, time zone, filesystems, requeue behaviour, expected performance and known issues. "The profile" alone means this. HBEP-4 owns it; the PlatformProfile kind serves it (HBEP-11).
- **Provider settings:** what one provider adds to its platform profile: the account, the grant, a credentials reference, the allowed partitions and the labels. HBEP-4 owns them; they are the spec of the Provider kind (HBEP-11).
- **ModelDeployment:** what to serve: model, serving spec, number of instances, where (which providers), until when.
- **Allocation:** capacity with a lifetime, like a Kubernetes node. It is one lease on a provider (a Slurm job, or a managed provider's catalog entry), with its start estimate and hard end.
  - Instances are placed on Allocations, as pods are on nodes.
  - An Allocation can outlive its Instances. Its retention policy then keeps or releases it, and it can later host Instances of a different ModelDeployment (Repurposing).
  - A managed catalog entry serves only its own model, so it is never repurposed.
  - On a batch provider its job carries a tag, set before any submission, and its job ID is observed state, written once (Allocation and job identity).
  - Its accelerator class is its partition's GPU model and CPU architecture, as the platform profile names them (HBEP-4).
- **Instance:** one copy of a deployment inside one Allocation, as a lifecycle resource: it exists from its creation to its end, through the Instance phases below. Clients reach it, through the front, only while it is `ready` or `draining`.
- **Naming levels:**
  - `model`: the deployments of that model on the providers the operator attached to the name, load-balanced; model names follow LLMLab's;
  - `model@<provider>`;
  - `model@<provider>/<deployment-id>`.
- **Deployment ID:** a hash computed by the framework over what is served: checkpoint revision and files, engine version, flags, chat template, attention backend and similar settings, and the accelerator class. Not the job ID, not the engine fingerprint, and never a key. A ModelDeployment has one current ID per accelerator class. A handover keeps it. Names show its display form, the first 12 hex characters.
- **Identity record** (HBEP-9): the inputs behind one deployment ID, which never change. Beside it, each verification of an Instance of that ID, at start and after every restart that reverifies, writes its own verification record, which never changes either (HBEP-11).
- **Front:** the LLMLab-compatible API endpoint (HBEP-5). Its client key is set by the operator (HBEP-5).
- **Control plane:** Kubernetes API custom resources plus controllers (HBEP-11). Models run in provider jobs, not pods.
- **Hedging:** submit early, on several providers if needed; keep the first Allocation whose Instance serves; cancel the pending losers (HBEP-7, open question 1; amended 2026-10-30). Hedging acts on pending Allocations; over-allocation acts on started ones.
- **Over-allocation:** obtain N+k Allocations for N Instances, qualify each Instance, and keep the best N. The rest are released, or kept as warm capacity, per their retention policy (HBEP-7).
- **Qualification:** the gate an Instance passes before it is kept: an identity check (HBEP-9) plus a short benchmark against the expected performance in its platform profile. It catches broken nodes, not only slow ones (HBEP-7).
- **Retention policy:** what happens to an Allocation that hosts no Instance. Either it is kept as warm capacity, ready to host an Instance, until a date, a grant's end or a deadline; or it is released when idle. A kept Allocation still ends at its hard end. Keep is the default (HBEP-7, open question 13; amended 2026-10-30).
- **Repurposing:** moving an Allocation to another ModelDeployment when a model's work finishes. The old Instance drains; an Instance of the new deployment starts in the same Allocation; it is verified and warmed up. An operator asks for it, often an agent that needs throughput across several models. Conversions between models are this operation (HBEP-7). Whether it needs approval is HBEP-10's question.
- **Rollout:** replacing a ModelDeployment's Instances of an old deployment ID with Instances of the new current ID of their accelerator class, which an edit to its serving spec gives, make-before-break (open question 7).
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
  - An Instance is the endpoint the front reaches: its own router in front of one or more workers. An Instance with one worker may have none, as in alpha (HBEP-7); the front then reaches the worker (HBEP-5).
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
- **Handover:** replacing an Instance whose lease is ending, or which has failed, with a new Instance of the same deployment in another Allocation of the same accelerator class.
  - Drain rule (the same text in HBEP-3, HBEP-5 and HBEP-7): The framework cannot move a lease end, so an Instance starts draining at the latest at its hard end minus the drain window, whether or not its replacement is ready. Make-before-break is the goal: the controller plans the replacement to be ready before that drain start, and the old Instance never drains before its replacement is ready unless that deadline forces it. If the replacement is late, the old Instance still drains on time, capacity drops, and an event is emitted (HBEP-8).
  - HBEP-5 sets the drain window. The old Instance is released only as Replacement before release allows.
  - The deployment ID stays the same (HBEP-7).
  - A replacement may land in another accelerator class. It is then a new Instance with that class's current ID, which is allowed, because both IDs are current. It does not serve a name pinned to the old ID.
- **Follow-on:** a batch-provider mechanism for a handover. It is a job submitted to start when, or before, its predecessor ends, on the same node or another one. A platform profile lists which continuity mechanisms the platform supports (HBEP-4).
- **Conversation key:** what the front uses to keep a conversation on the deployment and Instance that hold its context cache. Either a client session header, or a hash of the conversation's opening (HBEP-5).
- **Path** and **tunnel agent** (HBEP-6):
  - a path is the network route from the front to an Instance;
  - the tunnel agent is the process inside the Allocation that opens and keeps the dial-home tunnel.
- **Allocation agent** (HBEP-7; amended 2026-10-30): the process an Allocation's job runs. It registers the Allocation over the dial-home path, pulls what the Allocation should run from the control plane, and starts, restarts and stops the Instance's processes on the lifecycle controller's word. The tunnel agent is part of it. It is not the launcher, which starts the control plane (HBEP-11).

### Resource model

The five kinds, in API group `hpcburst.groundnuty.github.io`, version `v1alpha1` (HBEP-11 gives their schemas):

| Kind | Scope | What it is | Owned by |
|---|---|---|---|
| PlatformProfile | cluster | one platform's profile (HBEP-4) | none |
| Provider | cluster | our access to one platform: provider settings, labels and a credentials reference | none |
| ModelDeployment | namespaced | what to serve | none |
| Allocation | namespaced | one lease on a provider | none; it names the ModelDeployment it is assigned to |
| Instance | namespaced | one copy of a deployment in one Allocation | its ModelDeployment |

- **Scope.** Provider and PlatformProfile are cluster-scoped. Provider names appear in client model names, so they must be unique across the front, which a namespaced name cannot guarantee. A platform's profile is shared by all its providers.
- A Provider is our access to one platform. Several providers can share a platform, each with its own name, labels and settings, and all of them use that platform's profile (HBEP-4).
- An operator declares a ModelDeployment. The control plane places its Instances on Allocations on the providers its "where" allows: Allocations it obtains, hedging and over-allocating when needed, or Allocations repurposed to it (HBEP-7).
- **Ownership.** A ModelDeployment owns its Instances, as a ReplicaSet owns its pods. Each Instance is bound to one Allocation for life, as a pod to a node. No ModelDeployment owns an Allocation, because an Allocation can outlive its Instances and host another deployment's. An Allocation names the ModelDeployment it is assigned to, and a repurpose changes that assignment. A pending Allocation is cancelled when its deployment ends; a started one follows its retention policy (HBEP-7).
- Each Instance runs inside one Allocation and is reachable through the front. The front picks the deployment and the Instance, and the Instance's own router picks a worker (HBEP-5). So the front always knows which Instance answered.
- An Allocation can outlive its Instances. With no Instance, its retention policy keeps it as warm capacity or releases it, and an operator can repurpose it to another ModelDeployment. Alpha places one Instance per Allocation; the model allows several at once when its resources allow (open question 8).
- At a handover a new Instance, in another Allocation, replaces the old one. The framework releases the old Instance only after the new one serves; if the new one is late, the old one drains on time and ends with its lease (drain rule, under Handover). The deployment ID stays; the new Instance has its own metadata.
- A managed provider's catalog entry is an Allocation that the framework cannot start, restart or repurpose. In alpha it has no hard end; a key that is about to expire raises a provider-settings warning (open question 10, HBEP-4). In alpha the entries are kept in their Provider's status rather than as Allocation objects, since alpha never starts, releases or repurposes them (HBEP-4; amended 2026-10-30).

### Naming levels

| Name | Resolves to |
|---|---|
| `model` | the deployments of that model on the providers the operator attached to that name, load-balanced. No provider is attached by default, LLMLab included. Model names follow LLMLab's |
| `model@<provider>` | any deployment of that model on the provider with that name |
| `model@<provider>/<deployment-id>` | exactly that deployment, on any of its ready instances on that provider |

No client-facing name contains a job ID, node, port, URL or label selector.

- **Provider names** are the operator's arbitrary names. Selecting providers by label, such as every provider on one platform, happens in the API and in routing policy, never in a client model name.
- **Attaching.** The operator attaches providers to a model name in routing policy, by name or by label selector (HBEP-5). A user switching from LLMLab who wants requests to keep reaching it attaches the LLMLab provider explicitly, as one setup step.
- **Pinned names.** `model@<provider>/<deployment-id>` resolves while that deployment ID has a ready Instance on that provider. It pins one accelerator class, since each class has its own ID. Handovers within the provider and the class keep it resolving. A pinned form without a provider is open question 12.
- **Grammar** (open question 5):
  1. Split the name at its last `@`. A name with no `@` is a plain model name.
  2. The part before the `@` is the model name. It may contain `/` and `@`.
  3. The part after it is `<provider>` or `<provider>/<deployment-id>`, split at its first `/`.
  4. `<provider>` must be the name of an existing Provider. `<deployment-id>` must be exactly 12 lowercase hex characters, the display form.
  5. Anything else is an error, with HBEP-5's error form: an unknown provider, an empty part, a malformed ID, or a trailing `/`. An error never falls back to resolving the name as a plain model.

  So `org/m@helios-a/0123456789ab` is model `org/m` on `helios-a`, pinned; `a@b@helios-a` is model `a@b` on `helios-a`; `m@helios-a/` is an error. A plain name that contains `@` is parsed as a qualified one, so such a model is reachable only as `model@<provider>`.

### Deployment ID

- **Computed by the framework,** not reported by the engine, in scheme `v1` for alpha.
- **Inputs of a served deployment (scheme `v1`):**
  - the declared serving spec: checkpoint repository and full commit id; engine name, version and image digest; the engine flags and environment variables that are inputs under open question 3's rule; the chat template's content;
  - the accelerator class: GPU model and CPU architecture, as the platform profile names them for a partition (HBEP-4);
  - the checkpoint's per-file list: every file of the pinned revision, as its path relative to the checkpoint root and its SHA-256 (spike S7, PR #33).
- **Inputs of a managed catalog entry:** the platform, the catalog name, any revision the platform reports, and a marker that everything else is opaque (open question 2).
- **Not inputs:** the job ID, node, URL and lease end, which are instance metadata; the engine fingerprint, which is recorded beside the ID; the engine's resolved settings, recorded beside the ID per Instance; keys, including each Instance's own key and any token used to read a checkpoint, so that rotating a key never changes the ID.

#### The `v1` canonical object

The inputs form one JSON object. Every scalar value in it is a string. Booleans are `"true"` or `"false"`. A number is kept as the decimal string the user wrote, so `1.0` and `1` give two IDs.
- **Top level, both kinds:** `"scheme": "v1"`, and `"kind"`, which is `"served"` or `"managed"`.
- **A served deployment** adds these keys, all always present:
  - `checkpoint`: `{repository, revision}`, where `revision` is the full commit id;
  - `engine`: `{name, version, image_digest}`, with the digest written as `sha256:` and 64 lowercase hex characters;
  - `flags`: an object from each input flag's long name, without its leading dashes, to its value. A flag given more than once maps to an array of its values, in the order given. A value that is JSON, such as `hf-overrides`, stays the string given. `{}` when no flag is an input;
  - `env`: an object from each input environment variable's name to its value, `{}` when none;
  - `chat_template_sha256`: the SHA-256, in lowercase hex, of the chat template's UTF-8 text. It hashes the result of this lookup chain, which is not necessarily the template the engine serves: the serving spec's template when it gives one; otherwise the checkpoint's `chat_template.jinja`; otherwise its legacy `chat_template.json`; otherwise the `chat_template` string in its `tokenizer_config.json`, taking the entry named `default` from a list of named templates; otherwise empty content, so the field is the SHA-256 of zero bytes (`e3b0c442…`) and never the literal "". A list of named templates with no `default` entry is refused: the operator names the template in the spec. The engine may still serve a template when the chain gives empty content: vLLM v0.32.0 also falls back to built-in templates by model type (`vllm/renderers/hf.py`). The ID stays sound, because `files` and the image digest cover both sources. HBEP-9's render check compares a rendered conversation with the engine's resolved template, and does not fail only because the chain gave empty content;
  - `accelerator`: `{gpu_model, cpu_arch}`, as the PlatformProfile names them for the Allocation's partition. A partition without GPUs names `none`;
  - `files`: an array of `{path, sha256}`, one entry per file of the pinned revision, sorted bytewise by path. Paths are relative to the checkpoint root and use `/`.
- **A managed catalog entry** adds `platform`, `catalog_name`, `revision` (omitted when the platform reports none) and `"opaque": "true"`. A served object never has these keys, so the two kinds never hash the same bytes.
- **How the serving spec gives flags.** The ModelDeployment lists engine flags as `{name, value}` pairs, in order, with `name` the long flag name without its leading dashes (HBEP-11). No command line is parsed, but the engine's parser accepts other spellings of a flag, so the spec allows only one. A name is checked in this order:
  1. Underscores are normalised to dashes, if the engine's parser treats the two spellings as one flag (vLLM v0.32.0 does, in `vllm/utils/argparse_utils.py`; the library cites the line); otherwise a name with an underscore is refused.
  2. The normalised name must match `[a-z0-9][a-z0-9-]*`.
  3. It must be either the full long name of a flag the engine version defines, or an unknown name, which counts as an input.

  These are refused:
  - known short aliases (`tp`, `pp`, `dp` and any other the engine version defines);
  - every proper prefix of a table name that is not itself a table name, since vLLM's parser resolves abbreviations (argparse's `allow_abbrev`), so `api-k` would reach `--api-key`. Not refused as prefixes: `tokenizer`, a prefix of `tokenizer-mode` and a table name itself; `data-parallel-size`, which prefixes no table name; and `chat-template`, a table name, which is refused only because the framework passes it;
  - dotted sub-keys (`X.k`), since the parser folds them into a JSON value; give the JSON value under the long name;
  - `no-<name>` for any flag in the table (vLLM v0.32.0 adds a `no-` form for every boolean, and the form is never valid otherwise); booleans use their positive name with `"true"` or `"false"`.

  So for the flags the engine version defines, two spellings of one setting never give two IDs, and a refused flag cannot pass under another spelling. Env names must match `[A-Z_][A-Z0-9_]*`.
- **What admission checks** (HBEP-11). A flag name as written must match `[a-z0-9][a-z0-9_-]*`, and refusals compare names after replacing `_` with `-`, so `api_key` is refused as `api-key` is. Admission refuses the credential names, the aliases, `config`, `no-` forms and dotted sub-keys. Admission also refuses every proper prefix of a table name that is not itself a table name, in dashed and underscore spellings, from a list mirrored from the table; the library and the CRDs' check fail if the two drift. A boolean flag is given under its positive name, with `"true"` or `"false"`; the launcher passes `"false"` as vLLM's `--no-<name>`. The framework passes the checkpoint, its revision and the chat template itself, from the keys above, so the spec does not list `model`, `revision` or `chat-template`.
- **Serialisation and hash.** The object is serialised by the JSON Canonicalization Scheme (RFC 8785, JCS): keys sorted, no whitespace, and JCS's string rules. Non-ASCII characters are written as literal UTF-8, and only `"`, `\` and U+0000–U+001F are escapes (RFC 8785 §3.2.2.2). Every key is ASCII (the fixed keys, and flag and env names by the rules above), but values may hold any Unicode, so builders use a JCS implementation. Sorted-key JSON without whitespace equals JCS only if it writes non-ASCII as UTF-8, as Python's `json.dumps` does with `ensure_ascii=False`. The ID is the SHA-256 of those UTF-8 bytes, in lowercase hex. The name uses its first 12 characters; the identity record keeps all 64.
  - "Defaults made explicit" means the API schema's defaults. A field left out and the same field set to its schema default give one ID.
  - The engine's own defaults are not filled in. The engine reports them as resolved settings.
- **Worked example: the first golden vector.** A served deployment of `example-org/example-1b` at a fixed commit, on vLLM 0.32.0, with two input flags and no input environment variable, on a GH200 (`aarch64`) partition, with two files. The template, image and file hashes are placeholders: one digit repeated 64 times. The canonical bytes were written by hand and hashed with `sha256sum`, outside any code of ours:

  ```
  $ printf '%s' '{"accelerator":{"cpu_arch":"aarch64","gpu_model":"GH200"},"chat_template_sha256":"1111111111111111111111111111111111111111111111111111111111111111","checkpoint":{"repository":"example-org/example-1b","revision":"0123456789abcdef0123456789abcdef01234567"},"engine":{"image_digest":"sha256:2222222222222222222222222222222222222222222222222222222222222222","name":"vllm","version":"0.32.0"},"env":{},"files":[{"path":"config.json","sha256":"3333333333333333333333333333333333333333333333333333333333333333"},{"path":"model.safetensors","sha256":"4444444444444444444444444444444444444444444444444444444444444444"}],"flags":{"dtype":"bfloat16","max-model-len":"32768"},"kind":"served","scheme":"v1"}' > v1-served-example.json
  $ wc -c v1-served-example.json
  694 v1-served-example.json
  $ sha256sum v1-served-example.json
  0e1dd69efe8a57c3eadd5e0293fd8c0f0cc185629e79297b6530de361b887852  v1-served-example.json
  ```

  Its display form is `0e1dd69efe8a`. Python's `json.dumps` of the same object, with sorted keys and no whitespace, gives the same 694 bytes. That checks the key order, not the hash.
- **The `v1` table for vLLM.** Flag names are those of vLLM v0.32.0 ([`vllm/engine/arg_utils.py`](https://github.com/vllm-project/vllm/blob/v0.32.0/vllm/engine/arg_utils.py) and [`vllm/entrypoints/launchers/cli_args.py`](https://github.com/vllm-project/vllm/blob/v0.32.0/vllm/entrypoints/launchers/cli_args.py), accessed 2026-10-30). A flag or variable the table does not list is an input.

  | Flag or variable | `v1` | Reason |
  |---|---|---|
  | `dtype` | input | sets the arithmetic precision |
  | `quantization` | input | changes the weights the model computes with |
  | `kv-cache-dtype` | input | sets the precision of the attention cache |
  | `max-model-len` | input | bounds the tokens the model sees |
  | `tokenizer`, `tokenizer-mode`, `tokenizer-revision` | input | decide the tokens the model sees |
  | `trust-remote-code` | input | runs the checkpoint's own model code |
  | `load-format` | input | decides how weights load; `dummy` serves random weights |
  | `hf-overrides` | input | changes the model's configuration, RoPE scaling included. vLLM v0.32.0 has no `rope-scaling` flag; RoPE settings go here |
  | `enforce-eager`, `compilation-config` | input | change the compiled kernels, so the arithmetic |
  | `attention-backend` | input | picks the attention kernels |
  | `tensor-parallel-size`, `pipeline-parallel-size`, `enable-expert-parallel` | input | set the arithmetic layout |
  | `speculative-config` | input | changes how output tokens are computed |
  | `seed` | input | changes sampled outputs |
  | `generation-config`, `override-generation-config` | input | set the default sampling parameters |
  | `tool-call-parser`, `enable-auto-tool-choice`, `reasoning-parser` | input | turn the model's tokens into the response's tool calls and reasoning (HBEP-5 counts parsers in the ID) |
  | `chat-template-content-format`, `default-chat-template-kwargs` | input | change how messages render into tokens |
  | `VLLM_USE_DEEP_GEMM` | input | picks the matrix kernels |
  | `model`, `revision`, `chat-template` | not in `flags` | the framework passes them from `checkpoint` and the template, which are hashed there |
  | `enable-prefix-caching` | not an input | capacity: it reuses cache already computed, with numeric effects like batch shaping, which `v1` counts as capacity (open question 3) |
  | `max-num-seqs`, `max-num-batched-tokens`, `enable-chunked-prefill` | not an input | capacity: batch shaping (open question 3) |
  | `gpu-memory-utilization`, `kv-cache-memory-bytes`, `cpu-offload-gb` | not an input | capacity: a memory budget |
  | `host`, `port`, `uds` | not an input | transport: where the engine listens |
  | `served-model-name` | not an input | transport: the front maps model names (HBEP-5) |
  | `download-dir`, `distributed-executor-backend`, `CUDA_VISIBLE_DEVICES`, `VLLM_CACHE_ROOT`, `HF_HOME` | not an input | placement: paths, devices and how workers start |
  | `log-level`, `disable-log-stats`, `enable-log-requests`, `VLLM_LOGGING_LEVEL` | not an input | logging: changes no output |
  | `api-key`, `hf-token`, `HF_TOKEN`, `VLLM_API_KEY`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` | never | credentials (Not inputs). A credential is never written in the spec; it is referenced (HBEP-11). vLLM v0.32.0 reads the last three from the environment (`vllm/envs.py`) |
  | `config` | refused | it reads further options from a file, whose content the ID would not see; give each option as its own flag |
  | `lora-modules` | refused in alpha | it names other checkpoints without a pinned revision; a pinned form is for beta |

  **References to other checkpoints inside input values** carry the same risk as an unpinned checkpoint (L-8.3). For alpha:
  - a `tokenizer` other than the checkpoint's needs `tokenizer-revision` as a full commit id, or it is refused;
  - a `speculative-config` that names a draft model is refused (a pinned form is for beta).

- **Display form.** Names and labels use the first 12 lowercase hex characters of the hash. The full 64-character hash is kept in the identity record and in each Instance's spec. A Kubernetes label value holds at most 63 characters, so only the display form fits in a label.
- **Why 12 hex characters are enough.** They carry 48 bits. By the birthday bound, the chance that any two of n IDs share a display form is about n²/2⁴⁹.
  - For 10,000 IDs it is about 2 × 10⁻⁷. That is far more IDs than one control plane is expected to issue, since it holds tens to hundreds of objects (HBEP-11).
  - A collision is never silent. Before it issues an ID, the controller looks up the display form among the identity records. If a record there has a different full hash, it refuses the new ID, sets a condition on the ModelDeployment and raises an event. Resolving it then takes a new scheme version.

#### Current IDs, the file list and rollout

- **One current ID per accelerator class** (open question 1). When the controller first reconciles a ModelDeployment's serving spec, before any Instance exists, it computes one current ID for each accelerator class that the allowed partitions of its allowed providers offer, from their PlatformProfiles. The ModelDeployment's status lists them by class.
  - An Instance takes the current ID of its own Allocation's class.
  - A class added later, for example by a newly allowed provider, gets its ID then. That is not a rollout, since no other class's ID changes.
  - `model@<provider>/<deployment-id>` pins one class. The plain `model` name routes across all current IDs, and across an old ID while a rollout still serves it.
  - A handover keeps the ID within one class. A replacement placed in another class is a new Instance with that class's current ID (Vocabulary, Handover).
- **The per-file list.** For a checkpoint on the Hugging Face Hub, it comes from the Hub's listing at the full commit id, read with pagination (HBEP-9). An LFS file contributes its `lfs.oid`, which is a SHA-256. The controller downloads each other file, checks it against its git blob id, and hashes it itself, because the Hub gives SHA-256 only for LFS files. Those are the small files: configuration, tokenizer and template.
  - Staging on a provider then checks every staged file against the list, and every Instance start rehashes the files (HBEP-9). Both checks cover the files present, and each must match its entry. A listed file that was never staged, because the engine does not load it, is not a failure.
  - A gated repository needs a Hub token, referenced like provider credentials (HBEP-11).
  - Checkpoints from other sources are not in alpha.
- **Before the ID exists.** Until a class has its current ID, no Instance is created in an Allocation of that class. Allocations can still be requested, because they do not depend on what they will host.
- **Kept across a follow-on** that replaces a job in the same class.
- **Rollout** (open question 7). An edit to a ModelDeployment's serving spec gives each class a new current ID. The ModelDeployment's status lists every ID that still has Instances, as a Deployment lists its ReplicaSets. The controller replaces only Instances whose ID differs from their class's current ID, make-before-break, at their next natural handover by default (HBEP-7). An Instance never changes its ID.
- **Verification** (open question 1). At start, each Instance reports its resolved settings and rehashes its files (HBEP-9). A declared setting that the engine reports with another value fails verification, and so does a file whose hash differs from its entry. The ID is never recomputed from reported settings. The reported settings, the GPU type and the outcome form the verification result. Each verification, at start and after every restart that reverifies, is written as its own immutable record before the Instance can become ready, and the identity record is never changed (HBEP-11).
- **Every response an Instance produces names its deployment and Instance.** An error the front raises before it chooses an Instance names the front instead (HBEP-9 defines how).

### Allocation and job identity

From spike S4 (PR #32).
- **Name.** The controller generates an Allocation's name from its Provider's name and a random suffix, `<provider>-<5 random characters>`, so at most 69 characters. It never derives from the model or the ModelDeployment.
- **Tag.** The job tag is `hb-<n>-<h>`, at most 56 characters.
  - `<n>` is the Allocation's name cut to its first 40 characters, as S4's `Tag()` does.
  - `<h>` is the first 12 hex characters of the SHA-256 of the UTF-8 string `<namespace>/<name>/<uid>`: the Allocation's own namespace, full name and UID. So two Allocations whose names share 40 characters still get different tags.
  - Each platform profile's job-name rules must allow 56 characters (HBEP-4).
  - The provider sets the tag as the Allocation's external name before any submission.
  - It passes the tag to Slurm as `--job-name`. Slurm accounting keeps the job name; it keeps the job comment only when the site enables that (S4).
- **Job ID.** The job ID is observed state. It is written once, when the job is established, and never changed or removed. HBEP-11 says where it is kept.
- **Hold.** On a platform whose profile has `holdRelease` true, the provider submits each job held, with `--no-requeue`, and lifts the hold exactly once, on the job whose ID it recorded. Elsewhere it runs S4's fallback mode, without a hold. HBEP-7 gives the full submission protocol (open question 11, from S4).
- **Requeue** (open question 13). A requeue fails that Instance. The hold is never lifted again, so a requeued job that returns to a user hold stays held. The controller deletes the failed Allocation, whose finalizer cancels the held job (HBEP-11), and replaces it with a new Allocation.

### Test plan

- **Golden vectors.** Fixed inputs and their expected values, written once and checked into the repository. They are never regenerated by the code under test, since an expectation computed by the same canonicaliser or hash helper proves nothing. Each object is written by hand. Its canonical bytes come from an independent RFC 8785 implementation and are checked into the repository as the authoritative file. A second, independent JCS implementation must produce the same bytes, and `sha256sum` hashes the file.
  - The first is the worked example under Deployment ID.
  - Others cover a catalog entry, a repeated flag (an array), a boolean set to `"false"`, a JSON-valued flag, a non-empty `env`, a template taken from `tokenizer_config.json`, and a value with non-ASCII characters (literal UTF-8 under JCS).
  - Job tags for given namespace, name and UID strings, including a name longer than 40 characters.
- The same spec with fields in a different order gives the same ID. A field left out and the same field set to its schema default give the same ID.
- Changing each input changes the ID, including one file's SHA-256 in the per-file list, the accelerator class, and an engine flag the `v1` table does not list. Changing each non-input (job ID, node, URL, lease end, instance count, an Instance's key, a Hub token, and each flag the table marks as not an input) does not.
- A spec that lists a short alias such as `-tp`, or a key flag such as `api-key`, is refused.
- **Classes.** A ModelDeployment whose allowed providers offer two accelerator classes gets two current IDs, listed by class in its status. Each Instance carries its Allocation's class's ID.
- A catalog entry and a served deployment of the same checkpoint never get the same ID.
- **Display-form collision.** The test injects an identity record whose display form matches a new ID but whose full hash differs. The controller then refuses the new ID with a condition and an event. The record is injected because a real collision cannot be produced.
- **Rollout.** Editing a hashed field gives each class a new current ID. Old-ID Instances keep their ID until they are replaced, only Instances whose ID differs from their class's current ID are replaced, and the old ID's record is unchanged.
- A simulated handover within one class keeps the ID and replaces the Instance with a new one. A replacement in another class carries that class's current ID, and no rollout starts.
- A simulated repurpose keeps the Allocation and its name; the new Instance carries the new deployment's ID.
- Two providers on one platform: `model@<provider>` resolves to each separately, and a selector on the platform label lists both.
- Plain `model` with no provider attached returns the inactive-model error. Once the operator attaches the LLMLab provider, LLMLab answers.
- **The name parser.**
  - It accepts `example-27b`, `example-27b@helios-a`, `example-27b@helios-a/0123456789ab`, `org/m@helios-a/0123456789ab` and `a@b@helios-a`, with the parts given under Grammar.
  - It returns an error for `m@helios-a/`, `m@/0123456789ab`, an unknown provider, and an ID of the wrong length or with uppercase characters.
  - No error falls back to a plain model.
- Provider names with `.`, `@`, `/` or uppercase characters are refused.
- An Instance that reports a resolved setting contradicting a declared one fails verification. One that reports a setting the spec does not declare passes, and the setting is recorded.
- A requeued job, against S4's fake Slurm, fails its Instance. Its hold is not lifted again, the controller deletes the Allocation, whose finalizer cancels the job, and the replacement is a new Allocation.

### Compatibility and upgrades

- `model` names follow LLMLab's, so clients written for LLMLab keep working once the operator attaches the providers that should serve each name, LLMLab's among them if it should keep answering. The other two levels are opt-in.
- Clients treat deployment IDs as opaque strings. A downgrade leaves issued IDs readable.
- A change to the hashing rules, the input table or the display form's length is an amendment with a new scheme version. IDs already issued stay valid as records.
- The kinds start at `v1alpha1`. Their versioning and conversion are HBEP-11's.

### Security

- Credentials, such as a token for a model repository or an Instance's own key, are never hash inputs and never appear in names: a hash of a short secret can be reversed by guessing.
- Deployment IDs carry no private paths or account names. The account is a label on the Provider, not its name, because provider names appear in client model names.
- Job tags carry the Allocation's name and a hash, never a model, account or path.
- Who may resolve an ID back to its inputs is decided in HBEP-9.

### Graduation criteria

- **Alpha:**
  - the five kinds are served at `v1alpha1` (HBEP-11);
  - `v1` IDs match the golden vectors for a Hub checkpoint and for an LLMLab catalog entry;
  - the name parser passes its tests;
  - on one batch provider, an Allocation runs under its tag and hosts an Instance that carries its deployment ID, and a requeue there fails that Instance.
- **Beta:**
  - handovers keep the ID on each of two batch providers;
  - a rollout replaces Instances make-before-break;
  - LLMLab's `/v1/models` has been read, and no name there breaks the grammar (open question 5);
  - every response names its deployment and Instance (with HBEP-9).
- **Stable:** the scheme in use is frozen, with its golden vectors run in CI, and the kinds have reached a stable API version (HBEP-11).

## Operational readiness

1. **Working:** every serving instance carries its deployment's ID, and every response an Instance produces names a deployment and Instance known to the inventory. Such a response without them, or naming an unknown ID, is the failure signal.
2. **Restart:** IDs are recomputed from the inputs stored in identity records. A recomputed ID that differs from the stored one means the hashing rules drifted.
3. **Silent failure:** two instances under one ID that serve differently, because an input was missed or because `v1` does not hash resolved settings. Comparing the engine fingerprints, resolved settings and GPU types recorded within one ID shows it.
4. **Provider limits:** framework names are not provider job names. Job-name rules (prefixes, length) belong to the platform profile (HBEP-4), and the job tag's length is bounded to fit them (Allocation and job identity).
5. **Capacity and exhaustion:** N/A; naming holds no capacity.
6. **Idle cost:** none.
7. **Rollback:** a hashing change rolls back with its scheme version; issued IDs stay valid records.

## Drawbacks

- The rule for which settings are inputs needs upkeep for each engine version.
- Deployment IDs are opaque to people, so tools must show the inputs beside them.
- Three naming levels add parsing to the front.
- Plain `model` reaches nothing until the operator attaches providers: one more setup step for a user switching from LLMLab.
- In `v1`, one ModelDeployment that spans two accelerator classes has two deployment IDs, so `model@provider/<deployment-id>` pins one class. Within one class, a remaining difference in resolved settings shows only in the recorded settings.

## Alternatives

- **The job ID as identity.** Rejected: it changes at every follow-on.
- **The engine fingerprint as identity.** Rejected: it differs per site, is not unique per instance and is sometimes missing.
- **Names chosen by people for what is served.** Rejected: they drift from contents when allocations are repurposed. Provider names are chosen by people, because they name access to a platform, which does not drift.
- **The provider as the platform** (one name per system). Rejected: two grants on one platform need two providers, with their own credentials and labels.
- **Label selectors in client model names.** Rejected: clients keep plain names, and which providers serve a name is the operator's routing policy.
- **An Allocation bound to one deployment for life.** Rejected: capacity left when one model's work ends would go back to the queue, and a new job would wait again.
- **Kubernetes' `pod-template-hash`.** A Deployment names each ReplicaSet by a hash of its pod template, and a replaced pod keeps that hash. The deployment ID follows the same idea, over what is served instead of over a pod template.
- **A changed spec as a new ModelDeployment.** Rejected for alpha (open question 7): every fix would need a new object and a hand-run move of traffic, while a rollout keeps one object and replaces Instances make-before-break.
- **Resolved settings as inputs in `v1`.** Deferred to a later scheme (open question 1): the ID would then be known only after an Instance starts on each accelerator class.
- **A longer display form.** More characters cost length in names and job tags, and 12 already makes a collision unlikely and detectable.

## Prior art and reuse

The resource model replaces state that www-2027 operators kept by hand: per-job endpoint, control and configuration files on each cluster, and a script that listed jobs across clusters.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| Kubernetes custom resources, on the host HBEP-11 picks | [kubernetes/kubernetes](https://github.com/kubernetes/kubernetes); [k3s](https://github.com/k3s-io/k3s); [kcp](https://github.com/kcp-dev/kcp) | Apache-2.0 | Dependency: the API that serves the five kinds. HBEP-11 picks the single-user host, settled by spike S3 (PR #36); k3s is the team option |
| crossplane-runtime | [crossplane/crossplane-runtime](https://github.com/crossplane/crossplane-runtime) | Apache-2.0 | Dependency, from spike S4 (PR #32): the Allocation is a managed resource whose external name is its job tag, and whose `providerConfigRef` names a Provider. Not Crossplane core or compositions |
| model_signing | [sigstore/model-transparency](https://github.com/sigstore/model-transparency) | Apache-2.0 | Optional, from spike S7 (PR #33): its file mode gives the same per-file SHA-256 as our own hashing, so the ID does not depend on it, and never on its root digest |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| Name a replica set by a hash of its template; a replaced pod keeps the hash; a template change rolls out a new replica set | Kubernetes Deployment's `pod-template-hash` and rollouts | Deployment ID, hashed over what is served; open question 7 |
| A canonical JSON serialisation for hashing | RFC 8785, JSON Canonicalization Scheme ([rfc-editor.org/rfc/rfc8785](https://www.rfc-editor.org/rfc/rfc8785), accessed 2026-10-30) | The `v1` canonical form |
| Two address families, version-agnostic and per-version; per-cluster routes | KServe LLMInferenceService ([kserve/kserve](https://github.com/kserve/kserve)); ALCF FIRST ([argonne-lcf/FIRST](https://github.com/argonne-lcf/FIRST)) | Naming levels, kept inside the `model` field |
| ModelDeployment field names | KServe LLMInferenceService | ModelDeployment fields |
| v1beta2 condition vocabulary | Cluster API ([kubernetes-sigs/cluster-api](https://github.com/kubernetes-sigs/cluster-api)) | Allocation and Instance conditions |
| A desired pool kept apart from one unit of capacity with conditions and a finalizer (NodePool and NodeClaim) | Karpenter ([kubernetes-sigs/karpenter](https://github.com/kubernetes-sigs/karpenter)) | The Allocation as a NodeClaim-like resource |
| Launched, Registered and Initialized conditions plus a Ready summary | Karpenter ([NodeClaim docs](https://karpenter.sh/docs/concepts/nodeclaims/)) | Allocation conditions, beside the Instance phases |
| Management policies without Update; deterministic external names; a ProviderConfig that managed resources reference | Crossplane ([crossplane/crossplane](https://github.com/crossplane/crossplane)) | The job tag as the Allocation's external name; a submitted job is never edited; `providerConfigRef` names our Provider |
| `--no-requeue` | SkyPilot's Slurm provisioner ([skypilot-org/skypilot](https://github.com/skypilot-org/skypilot)) | Open question 13 |
| A scale subresource for external autoscalers | Slinky NodeSets ([SlinkyProject/slurm-operator](https://github.com/SlinkyProject/slurm-operator)); KubeAI ([kubeai-project/kubeai](https://github.com/kubeai-project/kubeai)) | ModelDeployment's number of instances |

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| A follow-on on a new node changed the URL, so every client had to re-point. | L-1.22 | URLs are Instance metadata; names survive handovers within a provider. |
| Job names stopped matching contents once jobs were reused for other models. | L-10.11 | Allocations and their job tags are named without a model; what is served is named by the deployment ID. |
| Engine fingerprints differed per site and could not be reproduced offline. | L-8.1 | The fingerprint is recorded beside the deployment ID, never used as it. |
| One checkpoint name hid real differences in kernels and local patches. | L-8.2 | Scheme `v1` hashes the declared spec, the accelerator class and every checkpoint file; a local patch is a different image digest; resolved settings are recorded beside the ID per Instance, and a contradicted declared setting fails verification. |
| Checkpoints were served without a pinned revision. | L-8.3 | Only a full commit id is admitted, and the per-file SHA-256 list is a hashed input. |
| Proving that only one configuration answered under a name was hard. | L-8.4 | Pinned names; every response names its deployment and Instance. |
| The route URL became part of the identity. | L-8.5 | A URL is never a name or a deployment-ID input. |
| Serving configuration was reconstructed afterwards from fingerprints. | L-8.7 | The framework computes the deployment ID from what is served. |
| A freed slot went to another user at once. | L-1.2 | Replacement before release: no serving Instance is released before its replacement serves. |
| Converting idle jobs between models was done by hand with per-job files. | L-3.9 | Repurposing: an Allocation moves to another ModelDeployment as one operation. |

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #3.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).
- 2026-10-30: Prior art and reuse filled from the state-of-the-art evaluation.
- 2026-10-30: implementable. Owner decisions on the API group, kinds and version folded in; spikes S4 (PR #32), S5 (PR #19) and S7 (PR #33) carried in; every open question decided or marked for alpha.
- 2026-10-30: amendment with HBEP-5 becoming implementable: an Instance with one worker may have no router.

## Amendments

- **2026-10-30, erratum:** `no-<name>` is refused for any flag in the table, and admission applies the prefix rule to every table name, as built in PRs #46 and #48. The prefix rule refuses every proper prefix of a table name that is not itself a table name, as `pkg/deploymentid/flagname.go` does, so `tokenizer` stays valid beside `tokenizer-mode`. Only spellings that were never valid are newly refused, so no ID changes.
- **2026-10-30: flag spellings, credentials and file references in `v1`.** Found by the reviews of the deployment-ID library (PR #46), from vLLM v0.32.0's source: the `never` list gains vLLM's credential environment variables; `config` and, for alpha, `lora-modules` are refused; abbreviations, dotted sub-keys and `no-` forms are refused; Golden vectors are produced by independent JCS implementations.
- **The scheme stays `v1`.** These two amendments change hashing rules, which a new scheme version would normally mark (Compatibility and upgrades). No `v1` ID had been issued when they were made, because the library (PR #46) was not merged and no deployment existed, so the scheme stays `v1` and the worked example's hash is unchanged.
- **2026-10-30: names and templates in `v1`.** Decided while building the deployment-ID library (#39, PR #46): flag and env names have ASCII charsets, enforced; known short aliases are refused; underscores normalise to dashes only where the engine treats them as one flag; an absent template hashes as empty content; a template list without `default` is refused.
- **2026-10-30: the hedge winner, the retention default, the allocation agent, managed entries in alpha, and one verification record per verification.** Made with HBEP-4 becoming implementable and HBEP-7's design.
  - Hedging keeps the first Allocation whose Instance serves, not the first to start. The owner approved this proposal of HBEP-7's open question 1 on 29 Oct 2026 (PR #14). That question said an accepted answer amends this HBEP.
  - Keep is the default retention policy, also approved on 29 Oct 2026 (PR #14, HBEP-7's open question 13).
  - The vocabulary gains the allocation agent, the process each Allocation's job runs (HBEP-7). Open question 9 asks that new terms come here by amendment.
  - In alpha a managed catalog entry is kept in its Provider's status, not as an Allocation object (HBEP-4).
  - HBEP-9's render check comes after alpha (HBEP-7); alpha verifies by the file rehash and the settings comparison.
  - Each verification of an Instance, at start and after every restart that reverifies, writes its own immutable record, and the Instance's status names the latest (HBEP-7, HBEP-11). A restarted Instance can report other settings, so one record per Instance would have held a stale verdict.
- **2026-10-30: an Instance with one worker may have no router.** Made with HBEP-5 becoming implementable. HBEP-7's alpha runs one worker per Instance and no router, and HBEP-5's front reaches that worker through the Instance's serving path. Routers in front of several workers come with beta (HBEP-5, open question 2; HBEP-7).
