# HBEP-9: Identity and attribution

## Summary

This HBEP decides what the framework records as the identity of a deployment, how each Instance proves that identity before it serves, and how every response names what produced it. It proposes:
- the fields of the deployment identity record, which HBEP-3's deployment ID is computed over, with the checkpoint revision always pinned;
- one verification that every Instance passes at start, before it joins any pool;
- attribution of every response with its deployment ID, Instance ID and engine fingerprint, or, for an error raised before any Instance is chosen, with the front as the emitting component;
- how a client chooses between "any deployment of this model" and "exactly this one".

It serves researchers who must say which model answered, for example in a paper's replication package, and the agents that operate deployments for them.

## Motivation

This serves stage 6 of the user story in HBEP-2 (know what answered). It also constrains stage 3, because a handover must keep the identity, and stage 5, because the front routes by it.

A past project served one set of models across several clusters and a managed gateway. Proving what answered took manual work afterwards:
- Checkpoints were served without a pinned revision. Identity rested on a cache holding one snapshot, though one model's tokenizer, chat template and configuration changed upstream after its weights did.
- Engine fingerprints differed per site for the same checkpoint and flags. They covered engine state after start-up, so no offline tool could reproduce them.
- Real differences hid behind one checkpoint name: attention implementations and kernels differed per GPU type, and one site needed a local engine patch.
- An allowlist of accepted fingerprints let a second configuration pass silently, and replies without a fingerprint were never compared.
- The managed gateway added a fixed prompt segment and filtered request fields, so its deployment of a model differed from a direct one.
- Verification was not uniform. Some instances were checked only by a test chat, with no identity record.
- The serving configuration of some turns had to be reconstructed after the fact from each reply's fingerprint.
- The route URL became part of the identity, so moving capacity meant re-pointing a forward behind the old URL.

### Goals

- A deployment identity record that lists everything about what is served that can change a response. HBEP-3's deployment ID is the hash over it.
- A pinned checkpoint revision in every record of a deployment the framework serves on batch and static providers. A branch name or "latest" is refused. Managed catalog entries record what their provider publishes (see Open questions).
- One verification for every Instance at start, recorded, before it serves any request.
- Attribution of every response, streamed or not, errors included.
  - A response from an Instance names its deployment ID, its Instance ID, and the engine fingerprint when the engine reports one.
  - An error raised before any Instance is chosen (unknown model, authentication failure, no ready Instance) names the component that emitted it, the front, and leaves the deployment and Instance fields absent. An Instance is never fabricated.
- A client choice, through the naming levels of HBEP-3, between any deployment of a model and exactly one.
- Per-site differences behind one checkpoint, and request changes made by a managed gateway, visible in the record rather than hidden by a shared name.

### Non-goals

- Bit-identical outputs across instances. Batching and kernels make outputs vary; identity states what was served, not that two runs agree.
- Defining the hash function or the name syntax. HBEP-3 owns both.
- Pinning what a managed provider serves. The framework records what it can observe there.
- Storing prompts or responses to prove attribution. The front records identities and counts (HBEP-8).

## Proposal

### User stories

- A researcher pins `example-27b@helios-a/<deployment-id>` for a whole study, where `helios-a` is the name the operator gave its provider on Helios. Handovers within that provider keep the name resolving (HBEP-3). Every response carries that ID, and the front's attribution log shows that no other deployment answered.
- A user sends `example-27b` and is served by whichever deployment the front picks. Each response says which one it was, so a mix can be found afterwards.
- An agent replaces an Instance before its Allocation ends. The new Instance verifies against the same record and keeps the deployment ID. Clients see only the Instance ID change.
- A maintainer changes the chat template. That gives a new deployment ID with its own identity record, never an edit to the record under the old ID.
- A reviewer of a paper fetches the identity record for a deployment ID and sees the checkpoint revision, engine version, flags and template.

### Notes, constraints and caveats

- **Record fields (proposed; they detail HBEP-3's inputs).**
  - Checkpoint: repository, pinned revision, and a hash of the weight, configuration, tokenizer and template files actually loaded.
  - Chat template: the one in use, from the revision or an override, hashed.
  - Engine: name, version, container image digest, and every local patch, hashed.
  - Settings that can change outputs: data type and quantisation, context length, attention backend, KV-cache data type, parallel layout, tool-call and reasoning parsers, default generation settings, speculative decoding.
  - Excluded: Instance and Allocation metadata (HBEP-3), such as job ID, node, port, URL and hard end; operational settings, such as router policy and session caps; and every key, including the Instance's own key.
- **The identity record behind a deployment ID is immutable.** Changing any record field gives a new deployment ID, with its own record; no record is edited under an existing ID. Whether the new ID is a new ModelDeployment or a rollout inside the existing one is HBEP-3's open question 7, and HBEP-11's admission restriction depends on it. Operational settings outside the record change in place.
- **Each Instance has its own key.** It is generated when the Instance starts and kept in its identity package: the identity data the Instance holds about itself. The front uses the key toward the Instance. The key is never an input to the deployment ID: rotating it must not change the ID, and a hash of a short secret can be guessed.
- **The engine fingerprint is recorded beside the ID, never used as it.** It is not unique per Instance, differs per site and is sometimes missing.
- **The URL is not identity.** Each handover brings a new Instance with a new URL; the deployment ID does not change.
- **Attribution is decided at serving time** by the front, which knows which Instance it sent the request to. Nothing is reconstructed afterwards.
- **No allowlist of fingerprints.** An Instance joins a pool because it passed verification against the record, never because its fingerprint was on a list.
- **Our own front changes no prompt and filters no field** (HBEP-5). A managed gateway that does is recorded as doing so.
- **A pinned name never falls back.** With no ready Instance, the front returns the inactive-model error, as HBEP-5 proposes, and no other deployment answers. That error names the front, with no deployment or Instance (Goals).

### Risks and mitigations

- **A record that misses a field** lets two different servings share one ID. The field list is reviewed against each engine's settings at each engine upgrade, with a test that changes each listed setting and expects a new ID.
- **A record that holds a noisy field** (a path, a time stamp) gives each start a new ID and breaks handovers. Fields are normalised; paths and times are excluded.
- **Verification that checks only liveness.** A test chat proves the engine answers, not what it serves. Verification compares resolved state with the record (see Open questions for which checks).
- **The engine resolves a setting differently than asked,** for example by choosing an attention backend per GPU type. A record that stores the request would then hide the difference. See HBEP-3's open question 1.
- **The Instance's router picks the worker,** and may not say which. The front still knows the Instance, because it picks the Instance itself (HBEP-3); only the worker may be unknown, which is HBEP-5's open question 7. Pooling workers across Instances would lose the Instance, so it is not part of this design (HBEP-3, open question 11).
- **A managed provider changes what it serves without notice.** The framework can see that only if the engine fingerprint or a probe changes. Such a change raises an event (HBEP-8).
- **Headers get dropped** by proxies or client libraries that keep only the body. The front's attribution log, keyed by request ID, holds the same facts.

### Open questions

- Declared or resolved values, and whether one spec on two GPU types is one deployment, are HBEP-3's open question 1. Input from this HBEP: kernels differ per GPU type even with the same settings, so record the GPU type and refuse "auto" for identity-relevant settings. Verification works either way: it compares the Instance's reported resolved settings with the record.
- Whether batch-shaping settings (maximum sequences, prefill budget) enter the record. They can change numerics slightly and are often tuned. A measurement on two settings of one deployment decides.
- Which checks verification runs at start. Lean: file hashes against the revision; engine version and image digest; resolved settings as the engine reports them; a fixed conversation rendered through the template and tokenizer, compared token by token with an offline render. Output checks are not identity checks, since outputs vary.
- Where attribution travels: response headers only, or also an extension field in the body for clients that log only bodies. Header names are hard to change, so the owner decides. Discussion with HBEP-5.
- Which deployments `model` may mix. Decided in part (HBEP-3): only those on the providers the operator attached to the name, none by default. Lean: every deployment of the model on those providers, with attribution on each response; a client that needs one identity pins it. Alternative: `model` mixes only deployments with the same record apart from provider.
- The identity record of a managed catalog entry, whose revision and settings are not published. Lean: known fields filled, the rest "unknown", a flag for gateway request changes, and an ID over the known fields plus platform and catalog name. The platform, not the provider, because provider names are arbitrary (HBEP-3, open question 2).
- How a client fetches the record for an ID: an endpoint on the front, an export for a replication package, or both. Discussion with HBEP-5.

## Design details

TBD. Decided so far: the deployment ID is a hash the framework computes over what is served (HBEP-3), not the job ID and not the engine fingerprint; a handover keeps it; every response from an Instance names the deployment and Instance that produced it, and an error the front raises before it chooses an Instance names the front. The record schema and the verification are decided by the open questions above.

### Test plan

- Change each record field in turn: each change gives a new deployment ID. Change a port, node or job ID: the ID stays.
- Submit a spec with a branch name as revision: admission refuses it.
- Start an Instance whose files differ from the pinned revision (one tokenizer file changed): verification fails, and it never joins a pool.
- Start an Instance where the engine resolves a setting other than the declared one: verification fails.
- Hand over an Instance: the deployment ID stays and the Instance ID changes, in responses and in the attribution log.
- Rotate an Instance's key: the deployment ID stays.
- Repurpose an Allocation: responses name the new deployment ID and a new Instance ID.
- Send a pinned request while its deployment has no ready Instance: no other deployment answers, and the error names the front with the deployment and Instance fields absent. The same holds for an unknown model and a failed authentication.
- Stream a reply and drop its headers: the attribution log still names the deployment and Instance for that request ID.
- Run two deployments of one checkpoint on two GPU types under `model`: each response's attribution tells them apart.

### Compatibility and upgrades

- Clients written for LLMLab keep working: attribution is added, nothing they read changes. The engine fingerprint, when present, stays in the OpenAI-compatible `system_fingerprint` field.
- An engine upgrade changes the engine version, so it gives a new deployment ID, never a change under a running ID.
- A change to the record's field list changes every ID. HBEP-3's hashing scheme version covers it, so IDs already issued stay valid for their records.

### Security

- Identity records are not secret, but must not leak local paths, user names or credentials; those are excluded by normalisation.
- The attribution log holds request IDs, client key IDs, deployment and Instance IDs, never prompts or responses. Retention: TBD with HBEP-8.
- An Instance proves its identity to the controller over its dial-home channel (HBEP-6). Who may claim to be an Instance of a deployment: TBD with HBEP-6.

### Graduation criteria

TBD. Lean: alpha when one batch provider verifies and attributes every response; beta when the Test plan passes on two GPU types and LLMLab's entries carry partial records.

## Operational readiness

1. **Working:** every ready Instance has a passed verification on record, and a probe request's attribution names the expected deployment and Instance. A ready Instance without a record, or a response without attribution, says it is not.
2. **Restart:** the controller re-reads records and verification results from the control plane (HBEP-11). A front restart loses no attribution already logged.
3. **Silent failure:** an Instance that serves a different template or file set than its record. Only the start verification and the token-level render check show it; a test chat does not.
4. **Provider limits:** verification runs inside the Allocation and spends its time. TBD in minutes per checkpoint size.
5. **Capacity:** N/A; verification is per start, not per request. Attribution adds a header per response.
6. **Idle cost:** none beyond the attribution log's storage.
7. **Rollback:** attribution has no off switch, since serving without it breaks stage 6. A change of header names or fields rolls back with the front (HBEP-5); the attribution log and verification records stay.

## Drawbacks

- Strict records give more deployment IDs: a template fix or an engine upgrade gives a new deployment ID.
- Verification adds start-up time to every Instance, inside a time-limited Allocation.

## Alternatives

- **The engine fingerprint as identity.** Free, but it differs per site, is not unique per Instance and is sometimes missing. Rejected.
- **The job ID or URL as identity.** Changes at every handover, so a study could not pin a deployment across handovers. Rejected.
- **Identity by model name only,** as LLMLab presents it. Hides every difference this HBEP exists to show.

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #9.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).

## Amendments

None.
