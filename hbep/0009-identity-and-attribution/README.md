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

- A researcher pins `example-27b@helios-a/<deployment-id>` for a whole study, where `helios-a` is the name the operator gave its provider on Helios. Handovers within that provider and its accelerator class keep the name resolving (HBEP-3). Every response carries that ID, and the front's attribution log shows that no other deployment answered.
- A user sends `example-27b` and is served by whichever deployment the front picks. Each response says which one it was, so a mix can be found afterwards.
- An agent replaces an Instance before its Allocation ends, in an Allocation of the same accelerator class. The new Instance verifies against the same record and keeps the deployment ID. Clients see only the Instance ID change.
- A maintainer changes the chat template. That gives a new deployment ID with its own identity record, never an edit to the record under the old ID.
- A reviewer of a paper fetches the identity record for a deployment ID and sees the checkpoint revision, engine version, flags and template.

### Notes, constraints and caveats

- **Record fields (decided in HBEP-3).** The record's inputs are exactly HBEP-3's `v1` canonical object, whose field list and worked example are under HBEP-3's Deployment ID:
  - checkpoint: the repository, the full commit id, and every file of the pinned revision with its SHA-256;
  - chat template: the SHA-256 of what HBEP-3's lookup chain gives, from an override or from the revision. The render check compares the engine's resolved template, which can differ when the chain gives empty content (HBEP-3);
  - engine: name, version and container image digest. A local engine patch enters only through the image digest: a patched engine is a different image;
  - the accelerator class, and the engine flags and environment variables that HBEP-3's `v1` table marks as inputs. That table covers the settings that can change outputs: data type and quantisation, context length, attention backend, KV-cache data type, parallel layout, tool-call and reasoning parsers, default generation settings and speculative decoding;
  - excluded: Instance and Allocation metadata (HBEP-3), such as job ID, node, port, URL and hard end; placement, capacity, transport and logging settings, such as router policy and session caps; and every key, including the Instance's own key.
- **The identity record behind a deployment ID is immutable.** Changing any record field gives a new deployment ID, with its own record; no record is edited under an existing ID. HBEP-3 decides that the new ID is a rollout inside the existing ModelDeployment (open question 7), so HBEP-11's admission refuses only an edit to an existing record. The record's inputs never change. Each verification of an Instance, at start and after every restart that reverifies, is its own immutable record beside it, labelled with the deployment ID and the Instance's UID (HBEP-7, HBEP-11). Neither is deleted with the ModelDeployment. Operational settings outside the record change in place.
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
- **The engine resolves a setting differently than asked,** for example by choosing an attention backend per GPU type. A record that stores the request would then hide the difference. HBEP-3 decides for alpha (open question 1): scheme `v1` hashes the request together with the accelerator class (GPU model and CPU architecture), so per-GPU-type kernel choices give different IDs. Each Instance's resolved settings are recorded beside the ID, so any remaining difference within one class shows in the record.
- **The Instance's router picks the worker,** and may not say which. The front still knows the Instance, because it picks the Instance itself (HBEP-3); only the worker may be unknown, which is HBEP-5's open question 7. Pooling workers across Instances would lose the Instance, so it is not part of this design (HBEP-3, open question 11).
- **A managed provider changes what it serves without notice.** The framework can see that only if the engine fingerprint or a probe changes. Such a change raises an event (HBEP-8).
- **Headers get dropped** by proxies or client libraries that keep only the body. The front's attribution log, keyed by request ID, holds the same facts.

### Open questions

- Declared or resolved values: HBEP-3 decides its open question 1 for alpha. Scheme `v1` hashes the declared spec with the accelerator class, so one spec on two GPU types gives two deployment IDs, and each Instance's resolved settings are recorded beside the ID. Still open here: whether to refuse "auto" for identity-relevant settings. Verification works either way: it compares the Instance's reported resolved settings with the record.
- Whether batch-shaping settings (maximum sequences, prefill budget) enter the record. They can change numerics slightly and are often tuned. A measurement on two settings of one deployment decides.
- **Which checks verification runs at start.** For alpha, HBEP-7 runs the file rehash and the settings comparison. The engine version, the image digest and the context length must be reported there, and the render check below comes after alpha.
  - **File hashes, from spike S7 (issue #30, PR #33) (candidate):**
    - The record holds one SHA-256 for every file of the pinned revision (HBEP-3), the configuration, tokenizer and template included.
    - **Rehash at every start,** of the files present, each against its entry. Measured on Helios x86 CPU nodes only: 167 GB took 46–77 s, inside a 5-minute budget set beforehand. Still due: Helios GH200, Athena A100 and Sano (CephFS).
    - **No size-and-mtime shortcut where rehash fits the budget.** Lustre timestamps have one-second resolution, and an edit in the same second as the last write left both unchanged. Where a platform breaks the budget, S7's fallback applies: record size, mtime, ctime and inode at least two seconds after the last write, and rehash when any differs.
  - **At staging, compare each staged file with the record's list (HBEP-3).** The list's entries come from the Hub:
    - an LFS file's `lfs.oid`, which is a SHA-256. The controller downloads each other file, checks it against its git blob id, and hashes it with SHA-256, because the Hub gives SHA-256 only for LFS files;
    - fetch the listing at a full commit id, with pagination, because the Hub's tree listing silently stops at 50 entries.
  - **Measuring bytes read:** use `rchar` for the bytes returned to the process, together with the Lustre RPC counters for the bytes read from the filesystem. On the measured installation `read_bytes` undercounted Lustre reads (2–11% of the bytes), for a reason not established.
  - **Lean for the rest:** engine version and image digest; resolved settings as the engine reports them; a fixed conversation rendered through the template and tokenizer, compared token by token with an offline render. Output checks are not identity checks, since outputs vary.
- Where attribution travels: response headers only, or also an extension field in the body for clients that log only bodies. Header names are hard to change, so the owner decides. Discussion with HBEP-5. Decided (owner decision, 30 Oct 2026): response headers named `hpcburst-*` (HBEP-5), with the front's usage records as the attribution log, and no body field.
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

## Prior art and reuse

Verification grows from www-2027's endpoint check, run on each new endpoint (health, versions, models and context length, a chat reply with its fingerprint, a tool-call smoke test), and from a scanner that looked for mixed deployments after the fact.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| vLLM | [vllm-project/vllm](https://github.com/vllm-project/vllm) | Apache-2.0 | Dependency: the engine whose version, image and resolved settings the record holds |
| model_signing | [sigstore/model-transparency](https://github.com/sigstore/model-transparency) | Apache-2.0 | Candidate, exercised by spike S7 (PR #33): not needed for hashing, acceptable if signing is wanted later. If used, set `Config.set_allow_symlinks(True)` (a Hugging Face cache snapshot is symlinks, and without it every call fails), set the thread count to the Allocation's CPUs, install pyOpenSSL explicitly (not declared), and note that it skips `.git*` paths by default, so the explicit file list is ours: a per-file SHA-256 manifest in file mode, over an explicit list of the files the engine loads |
| huggingface_hub | [huggingface/huggingface_hub](https://github.com/huggingface/huggingface_hub) | Apache-2.0 | Candidate, not exercised by S7, which used the Hub's tree endpoint directly: expected hashes from the Hub (paginated listing, full commit id), and `hf cache verify` at staging, untested |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| Response headers that name the serving worker | NVIDIA Dynamo's `worker_id` ([ai-dynamo/dynamo](https://github.com/ai-dynamo/dynamo)); AIBrix's `target-pod` ([vllm-project/aibrix](https://github.com/vllm-project/aibrix)); OpenTela's `X-Computing-Node` ([eth-easl/opentela](https://github.com/eth-easl/opentela)) | Attribution headers, which name the deployment and Instance instead |
| Model-equality testing | Gao et al. ([arXiv 2410.20247](https://arxiv.org/abs/2410.20247)) | Managed catalog entries: detecting changes, not proving identity |

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| Checkpoints were served without a pinned revision. | L-8.3 | A pinned revision in every record; a branch name is refused. |
| Engine fingerprints differed per site and could not be reproduced offline. | L-8.1 | The fingerprint is recorded beside the deployment ID, never used as it. |
| One checkpoint name hid differences in kernels and local patches. | L-8.2 | The accelerator class and every input setting are record fields; a local patch is a different image digest; resolved settings are recorded per Instance beside the record. |
| An allowlist of fingerprints let a second configuration pass. | L-8.4 | No allowlist; an Instance joins by verification against the record. |
| Verification was not uniform. | L-8.6 | One verification for every Instance at start. |
| Configuration was attributed after the fact. | L-8.7 | The front decides attribution at serving time. |
| The route URL became part of the identity. | L-8.5 | The URL is not identity. |
| A managed gateway's request changes made its deployment differ. | L-6.6 | The record flags a gateway's request changes. |
| Center-run endpoints hid their revisions and limits. | L-6.8 | A managed entry's unknown fields are recorded as unknown. |
| Live serve scripts could not be edited, because identity hashed them. | L-3.7 | Records are immutable; a change gives a new deployment ID. |

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #9.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).
- 2026-10-30: Prior art and reuse filled from the state-of-the-art evaluation.
- 2026-10-30: spike S7 (PR #33) carried in: per-file SHA-256, rehash at every start.
- 2026-10-30: aligned with HBEP-3 and HBEP-11 becoming implementable: a changed spec rolls out a new ID, and `v1` hashes declared settings and the accelerator class, with resolved settings recorded beside them.
- 2026-10-30: the record's fields point to HBEP-3's `v1` object (every file of the pinned revision; local patches only through the image digest); verification results are retained records of their own.
- 2026-10-30: the alpha checks at start (file rehash and settings comparison) noted from HBEP-7.
- 2026-10-30: one verification record per verification, at start and after every restart that reverifies, as HBEP-7 and HBEP-11 now say.
- 2026-10-30: HBEP-5's alpha proposal for where attribution travels noted.

## Amendments

None.
