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
- A pinned checkpoint revision in every record of a deployment the framework serves on batch and static providers. A branch name or "latest" is refused. Managed catalog entries record what their provider publishes (HBEP-3, open question 2; Open questions here).
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
- **Scheme `v1` is fixed.** The published release `v0.1.0-alpha.1` (14 Nov 2026) issues `v1` IDs, and `pkg/deploymentid` is frozen at `v1` (`pkg/deploymentid/deploymentid.go:106`, `:156`).
  - Any change to the record's fields or to the input table, batch-shaping settings for one, is a new scheme, `v2`. HBEP-3 owns the scheme, so the change is an amendment to HBEP-3 (Compatibility and upgrades), and IDs already issued stay valid for their records.
  - HBEP-3's October amendments kept `v1` only because no `v1` ID had been issued then (HBEP-3, Amendments, "The scheme stays `v1`"). That no longer holds.
  - Two measurements could justify a `v2`: the batch-shaping measurement (Open questions) and HBEP-3's two-class spike for resolved settings (HBEP-3, open question 1).
- **The identity record behind a deployment ID is immutable.** Changing any record field gives a new deployment ID, with its own record; no record is edited under an existing ID. HBEP-3 decides that the new ID is a rollout inside the existing ModelDeployment (open question 7), so HBEP-11's admission refuses only an edit to an existing record. The record's inputs never change. Each verification of an Instance, at start and after every restart that reverifies, is its own immutable record beside it, labelled with the deployment ID and the Instance's UID (HBEP-7, HBEP-11). Neither is deleted with the ModelDeployment. Operational settings outside the record change in place.
- **Each Instance has its own key.** It is generated when the Instance starts and kept in its identity package: the identity data the Instance holds about itself. The front uses the key toward the Instance. The key is never an input to the deployment ID: rotating it must not change the ID, and a hash of a short secret can be guessed.
- **The engine fingerprint is recorded beside the ID, never used as it.** It is not unique per Instance, differs per site and is sometimes missing.
- **The URL is not identity.** Each handover brings a new Instance with a new URL; the deployment ID does not change.
- **Attribution is decided at serving time** by the front, which knows which Instance it sent the request to. Nothing is reconstructed afterwards.
- **No allowlist of fingerprints.** An Instance joins a pool because it passed verification against the record, never because its fingerprint was on a list.
- **Our own front changes no prompt and filters no field** (HBEP-5). A managed gateway that does is recorded as doing so.
- **A pinned name never falls back.** With no ready Instance, the front returns the inactive-model error, as HBEP-5 decides, and no other deployment answers. That error names the front, with no deployment or Instance (Goals).

### Risks and mitigations

- **A record that misses a field** lets two different servings share one ID. The field list is reviewed against each engine's settings at each engine upgrade, with a test that changes each listed setting and expects a new ID.
- **A record that holds a noisy field** (a path, a time stamp) gives each start a new ID and breaks handovers. Fields are normalised; paths and times are excluded.
- **Verification that checks only liveness.** A test chat proves the engine answers, not what it serves. Verification compares resolved state with the record (see Open questions for which checks).
- **The engine resolves a setting differently than asked,** for example by choosing an attention backend per GPU type. A record that stores the request would then hide the difference. HBEP-3 decides for alpha (open question 1): scheme `v1` hashes the request together with the accelerator class (GPU model and CPU architecture), so per-GPU-type kernel choices give different IDs. Each Instance's resolved settings are recorded beside the ID, so any remaining difference within one class shows in the record.
- **The Instance's router picks the worker,** and may not say which. The front still knows the Instance, because it picks the Instance itself (HBEP-3); only the worker may be unknown, which is HBEP-5's open question 7. Pooling workers across Instances would lose the Instance, so it is not part of this design (HBEP-3, open question 11).
- **A managed provider changes what it serves without notice.** The framework can see that only if the engine fingerprint or a probe changes. In alpha nothing watches for it, and HBEP-8's alpha reasons have no such event. An event for it is a beta criterion, with the managed record (Graduation criteria).
- **Headers get dropped** by proxies or client libraries that keep only the body. The front's attribution log, keyed by request ID, holds the same facts.

### Open questions

Each question ends Decided, with its source, or Open, with what decides it. A "Lean" under an open item is a proposal, not a decision.

- **Declared or resolved values.**
  - **Decided** (HBEP-3, open question 1): scheme `v1` hashes the declared serving spec with the accelerator class and the per-file list. One spec on two GPU types gives two deployment IDs. Each Instance's resolved settings are kept in its verification records, beside the ID.
  - **Decided for alpha: "auto" is not refused** (HBEP-7, Instance start, settings comparison; science-agent decision, 31 Oct 2026, PR #91). A declared `auto` is not compared, and the value the engine reports is recorded as resolved (`internal/lifecycle/judge.go:257`). The exceptions are `max-model-len` and the parallel sizes (`tensor-parallel-size`, `pipeline-parallel-size`, `data-parallel-size`), whose `auto` admission refuses (`api/v1alpha1/modeldeployment_types.go:138`–`:139`; `TestParallelSizesArePlainPositiveIntegers`). Verification works either way: it compares the Instance's reported settings with the declared ones.
  - **Open: resolved settings as inputs.** HBEP-3's spike decides: one spec served on two accelerator classes, comparing resolved settings and outputs (HBEP-3, open question 1). If it refines the rule, that is scheme `v2` (Notes).
- **Batch-shaping settings** (maximum sequences, prefill budget). They can change numerics slightly and are often tuned.
  - **Decided for `v1`:** they are capacity, not inputs (HBEP-3, open question 3, and its `v1` table). `TestPropertyNonInputsDoNotChangeID` (`pkg/deploymentid/property_test.go:157`) checks that `max-num-seqs` leaves the ID unchanged.
  - **Open: whether a later scheme makes them inputs.** A measurement decides:
    - serve one deployment, on one accelerator class and one image, at two values of `max-num-seqs` and `max-num-batched-tokens`, under the same concurrent load;
    - send each the same fixed greedy requests, and compare the outputs token by token;
    - beside them, run two Instances at the same values, as the baseline, since outputs already vary with batch composition (HBEP-7, Instance start, the corruption check);
    - only a difference between the two settings beyond that baseline justifies a `v2`.
  - Making them inputs is a new scheme, `v2`, and HBEP-3's call (Notes).
- **Which checks verification runs at start.**
  - **Decided for alpha** (HBEP-7, Instance start): the file rehash and the settings comparison.
    - The engine version, the image digest and `max-model-len` must be reported and match. Any other declared input the engine reports must equal its declared value, compared by value: as booleans, else as numbers, else as strings.
    - A declared input the engine does not report is recorded as not reported, never as matching (HBEP-7, Risks: "Verification passes because it read nothing").
    - Each verification writes its own immutable record, `ver-<instance UID>-<generation>` (`api/v1alpha1/records.go:122`), before the Instance can become ready.
    - Built in `internal/lifecycle/verify.go` and `internal/lifecycle/judge.go`.
  - **File hashes, from spike S7 (issue #30, PR #33):**
    - The record holds one SHA-256 for every file of the pinned revision (HBEP-3), the configuration, tokenizer and template included.
    - **Decided: rehash at every start,** of the files present, each against its entry. Built in `internal/agent/stage/stage.go:13`, with S7's parallel readers, four by default, and no size-and-mtime shortcut.
    - **Open: the rehash time on the other platforms.** S7 measured Helios x86 CPU nodes only: 167 GB took 46–77 s, inside a 5-minute budget set beforehand. Alpha graduates on Athena (HBEP-7, Graduation criteria), so Athena A100 is due first, then Helios GH200 and Sano (CephFS). The same timing on each decides whether S7's fallback is needed there.
    - **S7's fallback, not built:** record size, mtime, ctime and inode at least two seconds after the last write, and rehash when any differs. It applies only where a platform breaks the budget. Lustre timestamps have one-second resolution, and an edit in the same second as the last write left both unchanged, so no shortcut is used where the rehash fits.
  - **At staging, compare each staged file with the record's list (HBEP-3).** Decided and built: each downloaded file is checked against the record's SHA-256 as it streams (`internal/agent/stage/stage.go:11`). The list's entries come from the Hub:
    - an LFS file's `lfs.oid`, which is a SHA-256. The controller downloads each other file, checks it against its git blob id, and hashes it with SHA-256, because the Hub gives SHA-256 only for LFS files;
    - fetch the listing at a full commit id, in pages, under the listing rule below.
  - **The listing rule** (science-agent decision, 31 Oct 2026, built in `pkg/hubfiles`, PR #68):
    - the request asks for `limit=1000`, the largest page the Hub accepts;
    - every `Link` header with `rel="next"` is followed, parsed by RFC 8288's rules. A `Link` that does not parse is an error, never the end of the list;
    - the next page must stay in the same snapshot: the same origin and path, so the same full commit id, and `recursive=true`;
    - a redirect of the listing may only stay on the Hub's origin, because nothing checks the listing afterwards.
    - **Why:** the Hub's page size depends on the query. Spike S7 saw a listing cut at 50 entries. On 31 Oct 2026 the Hub answered a request with no `limit` with all 77 entries of a checkpoint in one page, but with `expand=true` it defaulted to 50 entries plus a `Link` (PR #68, comment 6543291074). A client that ignores `Link` therefore stops short with no error (L-17.5).
  - **Measuring bytes read:** use `rchar` for the bytes returned to the process, together with the Lustre RPC counters for the bytes read from the filesystem. On the measured installation `read_bytes` undercounted Lustre reads (2–11% of the bytes), for a reason not established.
  - **Open for beta: the render check.** It is HBEP-9's candidate and not in alpha (HBEP-7, Instance start; HBEP-3, Amendments, 2026-10-30), and nothing builds it.
    - Lean: a fixed conversation rendered through the Instance's template and tokenizer, compared token by token with an offline render of the same files. It compares with the engine's resolved template, and does not fail only because HBEP-3's lookup chain gave empty content (HBEP-3, the `v1` canonical object, `chat_template_sha256`). Output checks are not identity checks, since outputs vary.
    - A spike decides it: the two renders compared on a checkpoint with its own template, and on one whose lookup chain gives empty content, where vLLM falls back to a built-in template.
- **Where attribution travels.** **Decided** (owner decision, 30 Oct 2026; HBEP-5, open question 7): response headers named `hpcburst-*` (HBEP-5, Attribution headers), the front's usage records as the attribution log (HBEP-8, Usage records), and no body field. The engine fingerprint stays in `system_fingerprint`. Built and tested in `internal/front/attribution_test.go`.
- **Which deployments `model` may mix.** **Decided** (owner decision, 30 Oct 2026; HBEP-5, open question 4; HBEP-3, Current IDs, the file list and rollout):
  - plain `model` reaches only the ModelDeployments of that name, on the providers the operator attached to it, none by default;
  - the front places a request on one ring over all their routable Instances, across every current ID, and an old ID while a rollout still serves it;
  - an attached managed provider takes a request only when no Instance of ours can.

  So `model` mixes deployment IDs, each response names its own, and a client that needs one identity pins it.
- **The identity record of a managed catalog entry,** whose revision and settings are not published.
  - **Decided for alpha** (HBEP-3, open question 2; issue #69). What the code does:
    - The ID is `ComputeManaged` (`pkg/deploymentid/deploymentid.go:158`) over HBEP-3's managed object: `scheme`, `kind` `managed`, `platform`, `catalog_name`, `opaque` `"true"`, and `revision` only when the platform reports one. The catalog client takes an entry's string `revision` member as its revision (`internal/managed/client.go:41`), and LLMLab's entries have none (issue #69; HBEP-3, Amendments, 2026-10-31).
    - The platform is the Provider's platform label, and a Provider without one gets no catalog (`internal/managed/controller.go:439`). Provider names are arbitrary, so two Providers on LLMLab give one entry the same ID.
    - The catalog loop writes each entry's full ID as `managedID` in the Provider's `status.catalog` (`pkg/provider/catalog.go:102`; `api/v1alpha1/provider_types.go:219`).
    - **Its identity record** (science-agent rulings on #574's plan, 14 Nov 2026): the managed catalog controller writes `id-<12 hex>` with the canonical managed object and the full hash in `hpcburst-system`, the controllers' namespace, since the ID belongs to no namespace and no Provider. Labels: `record=identity`, the deployment ID's display form and the platform; the catalog name goes in an annotation only, since it can hold characters a label value refuses (HBEP-3's vector 03, `example-ü-😀-27b`). A record is found by its deployment-ID label, or the platform label for one platform's records. It has no owner reference, so it outlives the entry and every Provider (HBEP-11, Identity records).
      - It's written the first time the controller sees the entry, before the entry is published in `status.catalog`. A failed write fails the pass and leaves the status unchanged, so no entry is published without its record.
      - Two Providers on one platform give one entry one ID, so one record.
      - A record whose stored hash differs from the computed one leaves the entry out of `status.catalog`, with a `CatalogEntrySkipped` naming the record (HBEP-3, the collision rule).
      - The release `v0.1.0-alpha.1` writes none, so an entry it dropped before an upgrade has no record.
  - **Open for beta: what is stored beside the ID.** The known fields, the rest marked "unknown", and a flag for a gateway's request changes (L-6.6) are not built.
    - Lean: keep them beside the ID, outside the hashed object, so `v1` IDs stay as issued. Adding them to the object would be a `v2` (Notes).
    - A probe decides the flag per managed platform: a fixed request whose prompt token count is known offline, compared with the count the platform's `usage` reports. An added prompt segment shows as a surplus.
    - The known fields and the flag, once decided, sit beside the record, outside the hashed object.
- **How long records are kept.** HBEP-11 leaves the retention policy for identity, verification and accounting records here (HBEP-11, Records outlive deletion).
  - **In alpha there is none:** the records have no owner reference, so they're kept until an operator deletes them, as HBEP-11 says, and nothing deletes them on a schedule.
  - **Open: a policy,** decided with the record fetch below, since both turn on who reads the records and for how long.
- **How a client fetches the record for an ID.** **Open: owner decision,** because an API path is hard to change. The options are an endpoint on the front, an export for a replication package, or both. Discussion with HBEP-5.
  - Today, identity and verification records are ControllerRevisions, read through the Kubernetes API under its RBAC and found by their `deployment-id` and `instance-uid` labels (HBEP-11, Ownership, records and deletion).
  - The same decision answers who may resolve an ID to its inputs, which HBEP-3 leaves here (HBEP-3, Security), and how records are exported, which HBEP-11 leaves here.
  - A managed entry's record sits in `hpcburst-system` (above); the fetch decision may copy records elsewhere without changing any ID.

## Design details

The mechanisms are decided and built under other HBEPs. Each item says where its detail lives; Open questions say what is still open.

- **The deployment ID and the identity record.** HBEP-3, Deployment ID, holds the `v1` canonical object, the vLLM input table, the RFC 8785 serialisation, the 12-character display form and the collision rule. Built in `pkg/deploymentid`.
  - The lifecycle controller computes one current ID per accelerator class (HBEP-7, Reconcile loop, step 2).
  - It stores each ID's record as the ControllerRevision `id-<12 hex>`, created if absent and never rewritten (`internal/lifecycle/identity.go:405`; `api/v1alpha1/records.go:103`). The API server refuses any change to its data (HBEP-11, Ownership, records and deletion).
- **The pinned revision.** Admission takes only a full 40-character commit id (`api/v1alpha1/modeldeployment_types.go:22`). A tokenizer from another repository needs its own pinned revision, and a draft model in `speculative-config` is refused (HBEP-3, Deployment ID).
- **The per-file list.** Read from the Hub at the full commit id under the listing rule above (`pkg/hubfiles`, PR #68). HBEP-3 (Current IDs, the file list and rollout) gives the failure classes and `IdentityUnresolved`.
- **Verification at start.** HBEP-7, Instance start: the staging check and the rehash (`internal/agent/stage`), the settings comparison (`internal/lifecycle/judge.go`), and one record per verification (`api/v1alpha1/records.go:122`).
  - An Instance is `ready` only once its latest verification record exists and passed.
  - Verification runs as its own controller in the lifecycle manager (HBEP-7, Controllers; HBEP-11).
- **Who may claim to be an Instance.** HBEP-6, Admission: the server plugin. frps admits a login only with a live Allocation's credential, and a proxy only for the UID of an Instance placed on that Allocation. The Instance takes traffic only after verification.
- **The Instance key.** The allocation agent generates it at Instance start and passes it to the engine through the environment (HBEP-7, The allocation agent, Keys). The front reads it from the controller's mode-600 file for that Instance UID (HBEP-5, Notes). It is never an ID input (HBEP-3, Deployment ID).
- **Attribution.** HBEP-5, Attribution headers: six `hpcburst-*` headers on a response from an Instance of ours; four on a managed provider's reply, which has no Instance to name (`TestAttributionOnLLMLabReplies`); and only the request ID and the emitter on an error the front raises before it chooses a target. Each usage record holds the same facts, keyed by request ID (HBEP-5, Usage records; HBEP-8, Usage records).
- **Managed catalog entries.** The ID and where it lives are under Open questions; HBEP-4 owns the catalog loop.
- **Naming.** HBEP-3, Naming levels, and HBEP-5's choice under `model` (open question 4).

### Test plan

- Change each record field in turn: each change gives a new deployment ID. Change a port, node or job ID: the ID stays.
- Submit a spec with a branch name as revision: admission refuses it.
- Read a listing from a Hub double that cuts each page below the requested limit and sends a `Link`: every page is read. A `Link` that does not parse, a next page on another path, and a redirect off the Hub's origin each fail the listing.
- Start an Instance whose files differ from the pinned revision (one tokenizer file changed): verification fails, and it never joins a pool.
- Start an Instance where the engine resolves a setting other than the declared one: verification fails.
- Hand over an Instance: the deployment ID stays and the Instance ID changes, in responses and in the attribution log.
- Rotate an Instance's key: the deployment ID stays.
- Repurpose an Allocation: responses name the new deployment ID and a new Instance ID.
- Send a pinned request while its deployment has no ready Instance: no other deployment answers, and the error names the front with the deployment and Instance fields absent. The same holds for an unknown model and a failed authentication.
- Stream a reply and drop its headers: the attribution log still names the deployment and Instance for that request ID.
- Run two deployments of one checkpoint on two GPU types under `model`: each response's attribution tells them apart.
- A managed catalog entry and a served deployment of the same checkpoint never get the same ID. Two Providers on one managed platform give one entry the same ID.
- The render check, once built: an Instance whose served template renders a fixed conversation differently from the offline render fails verification; one whose lookup chain gives empty content passes on its engine's resolved template.

### Compatibility and upgrades

- Clients written for LLMLab keep working: attribution is added, nothing they read changes. The engine fingerprint, when present, stays in the OpenAI-compatible `system_fingerprint` field.
- An engine upgrade changes the engine version, so it gives a new deployment ID, never a change under a running ID.
- Scheme `v1` is fixed, since the published release issues its IDs (Notes). A change to the record's field list is scheme `v2`, by amendment to HBEP-3, and IDs already issued stay valid for their records (HBEP-3, Compatibility and upgrades).

### Security

- Identity records are not secret, but must not leak local paths, user names or credentials; those are excluded by normalisation.
- The attribution log holds request IDs, client key IDs, deployment and Instance IDs, never prompts or responses. Retention: in alpha the attribution log is the usage files, kept for the grant's life (HBEP-8, Usage records).
- An Instance proves its identity to the controller over its dial-home channel (HBEP-6). Who may claim to be an Instance of a deployment is decided for alpha by HBEP-6 (Admission: the server plugin): a login needs a live Allocation's credential, and a proxy may claim only the UID of an Instance placed on that Allocation.
- Who may read records: in alpha, whoever the Kubernetes API's RBAC lets read ControllerRevisions in the namespace (HBEP-11). Any wider access comes with the record fetch, the owner's decision (Open questions).

### Graduation criteria

- **Alpha:** what the published release `v0.1.0-alpha.1` does. The CI of its tag passed, the `go`, `crd and envtest` and `rehearsal part 3` jobs included.
  - **Attribution on every response,** streamed or not, success or error, an LLMLab-served reply included, with the same facts in its usage record:
    - `internal/front/attribution_test.go`: `TestAttributionOnInstanceReplies`, `TestAttributionOnStreams`, `TestAttributionOnInstanceErrors`, `TestAttributionOnLLMLabReplies`, `TestAttributionOnFrontAnswers` and `TestUsageRecordAttributesAStreamWithoutHeaders`;
    - through the rehearsal stack: `internal/rehearsal/scenario/part3_attrib_test.go`.
  - **Verification before serving:**
    - no Instance is `ready` before its checks pass; a changed file is re-staged once, then fails the Instance; a contradicted `max-model-len` stops the class; an engine that reports nothing fails; each restart writes a new record: `internal/lifecycle/verify_test.go` (`TestVerifyNoInstanceReadyBeforeChecksPass`, `TestVerifyChangedFileRestagedOnceThenFails`, `TestVerifyMaxModelLenContradictionStopsClass`, `TestVerifyEngineReportsNothingFails`, `TestVerifyRecordsPerRestart`);
    - the by-value rule and a declared `auto`: `internal/lifecycle/judge_settings_test.go`.
  - **Scheme `v1`:**
    - the golden vectors and HBEP-3's worked example: `TestGoldenVectors` (`pkg/deploymentid/compute_test.go`) and `TestComputeReproducesHBEPWorkedExample` (`pkg/deploymentid/hbep_test.go`);
    - managed entries: `TestComputeManaged` and `TestManagedAndServedNeverCollide` (`pkg/deploymentid/managed_test.go`);
    - a branch name refused at admission: `TestCheckpointRevisionIsAFullCommitID` (`api/v1alpha1/e2e/modeldeployment_test.go`);
    - the listing rule: `pkg/hubfiles/link_test.go` and `pkg/hubfiles/redirect_test.go`.
- **Beta:**
  - the render check, built and run in every verification (Open questions);
  - the managed record: a managed entry's ID has its immutable identity record (#574), with what the open question decides beside it, and a change in what a managed platform serves raises an event (Risks);
  - the rehash timed on Athena A100, Helios GH200 and Sano, with S7's fallback built where a platform breaks its budget;
  - the batch-shaping measurement run, and HBEP-3's decision on a `v2` recorded;
  - the record fetch built, as the owner decides;
  - the whole Test plan passes on two GPU types.
- **Stable:** the scheme in use and the attribution headers are frozen, with their golden vectors in CI (HBEP-3, HBEP-5).

## Operational readiness

1. **Working:** every ready Instance has a passed verification on record, and a probe request's attribution names the expected deployment and Instance. A ready Instance without a record, or a response without attribution, says it is not.
2. **Restart:** the controller re-reads records and verification results from the control plane (HBEP-11). A front restart loses no attribution already logged.
3. **Silent failure:** an Instance that serves a different template or file set than its record. Only the start verification and the token-level render check show it; a test chat does not.
4. **Provider limits:** verification runs inside the Allocation and spends its time. Spike S7 measured the rehash at 46–77 s for 167 GB on Helios x86 CPU nodes, inside its 5-minute budget. Athena A100, Helios GH200 and Sano are not measured yet (Open questions). Each class's last measured start-to-ready time is kept in the ModelDeployment's status (HBEP-7, Handover).
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
| model_signing | [sigstore/model-transparency](https://github.com/sigstore/model-transparency) | Apache-2.0 | Candidate, exercised by spike S7 (PR #33), and not used in alpha, which hashes in `internal/agent/stage`: not needed for hashing, acceptable if signing is wanted later. If used, set `Config.set_allow_symlinks(True)` (a Hugging Face cache snapshot is symlinks, and without it every call fails), set the thread count to the Allocation's CPUs, install pyOpenSSL explicitly (not declared), and note that it skips `.git*` paths by default, so the explicit file list is ours: a per-file SHA-256 manifest in file mode, over an explicit list of the files the engine loads |
| huggingface_hub | [huggingface/huggingface_hub](https://github.com/huggingface/huggingface_hub) | Apache-2.0 | Candidate, not exercised by S7, which used the Hub's tree endpoint directly, and not used in alpha, where `pkg/hubfiles` does the same: expected hashes from the Hub (paginated listing, full commit id), and `hf cache verify` at staging, untested |

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
| A managed gateway's request changes made its deployment differ. | L-6.6 | The record flags a gateway's request changes (for beta; Open questions). |
| Center-run endpoints hid their revisions and limits. | L-6.8 | A managed entry's unknown fields are recorded as unknown (for beta; Open questions). |
| Live serve scripts could not be edited, because identity hashed them. | L-3.7 | Records are immutable; a change gives a new deployment ID. |
| The Hub's tree listing page size depends on the query. | L-17.5 | The listing asks for `limit=1000` and follows every `Link`; a `Link` that does not parse is an error. |

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
- 2026-10-31: the Hub listing rule written in full from the Hub listing (PR #68): `limit=1000`, every `Link` followed, an unparseable `Link` an error, one snapshot, redirects only on the Hub's origin. Lesson L-17.5 cited.
- 2026-10-31: HBEP-6, now implementable, decides for alpha who may claim an Instance's tunnel: frps's server plugin admits a login only with a live Allocation's credential, and a proxy only for the UID of an Instance placed on that Allocation. The Instance still takes traffic only after the controller's verification.
- 2026-10-31: HBEP-7's settings comparison is by value (booleans, numbers, then strings), with a declared `auto` recorded as resolved. That also keeps open question 1's "verification works either way" true for `auto`.
- 2026-10-31: the attribution log's retention noted from HBEP-8 becoming implementable: in alpha, the usage files, kept for the grant's life.
- 2026-11-14: implementable. What other HBEPs and the code had decided is carried in, with its source: scheme `v1` fixed by the published release `v0.1.0-alpha.1`, so any change to the record is a `v2` and HBEP-3's call; HBEP-7's alpha checks, with a declared `auto` recorded as resolved; HBEP-5's choice under `model`; the managed entry's ID as issue #69 built it, with no stored record; HBEP-6's admission for who may claim an Instance. Design details point to where each mechanism is held, and the alpha criteria name the release's tests. Open: resolved settings and batch shaping as inputs, the rehash time beyond Helios x86, the render check, the managed record beyond its ID, and the record fetch (owner decision). From its review round (Astra and Fable, PR #555): a managed reply carries four headers, not six; the parallel sizes refuse `auto` as `max-model-len` does; a managed entry's missing identity record is a gap HBEP-11 now states, for beta to close; and records have no retention policy in alpha.
- 2026-11-14: a managed entry's ID gets an identity record in `hpcburst-system` (for #574).

## Amendments

- **2026-11-14: managed identity records** (Open questions, the managed entry; Graduation criteria). An amendment by the science agent's rulings on #574's plan: a managed entry's ID gets an immutable identity record in `hpcburst-system`, written before the entry is published, with a collision leaving the entry out of the catalog with `CatalogEntrySkipped`. Before, no record was written, as a gap against HBEP-11.
