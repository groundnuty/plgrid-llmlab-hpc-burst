# HBEP-11: Kubernetes as the control plane

## Summary

The framework's control plane is the Kubernetes API: custom resources for the resource model of HBEP-3, in the API group `hpcburst.groundnuty.github.io` at version `v1alpha1`, and controllers that reconcile them against providers. Kubernetes is not the compute layer. Models run in provider jobs (Slurm jobs, managed catalog entries), never in pods. This HBEP decides what the framework takes from Kubernetes, what it must build itself, and how the control plane is deployed. Alpha is one user on one host, whose Kubernetes API our launcher starts. A team uses a small k3s cluster.

## Motivation

The control plane holds the state behind stage 2 (get resources), stage 3 (keep instances running) and stage 7 (pay and give back) of the user story in HBEP-2, and the agent primitives of HBEP-10.

A past project kept that state in endpoint files, process-ID files, scheduled entries, tunnel watchdogs and agents' notes:
- A job ended by its time limit left its endpoint file, so the inventory listed a dead instance.
- Nothing recorded who had started what. Clean-up by keyword sweep missed many processes, and a scheduled respawner revived killed ones.
- Monitors read their configuration once at start and missed new deployments for hours.
- Running scripts could not be changed safely, since there was no versioned spec for what a job should run.
- Every agent ran as one Unix user, so no action could be attributed.

Each needs one store of desired and observed state, with owners, watches, access control, validation and an audit trail. The Kubernetes API provides these generically, and agents already know its tools.

### Goals

- One store of desired and observed state for every resource in HBEP-3 and HBEP-10.
- Reconciliation that survives controller restarts and missed events.
- Clean-up by ownership, not by search.
- Access control and audit per identity.
- Validation before a spec is accepted.
- A deployment for a team (a small cluster) and one for a single user that needs no cluster administration.

### Non-goals

- Running models in pods or on Kubernetes nodes.
- The request path. The front serves requests and reads routing state from the control plane, but no request passes through the Kubernetes API server.
- Kubernetes scheduling of GPU work.
- Evaluating existing projects in detail. The state-of-the-art evaluation of existing projects (28 Oct 2026) is done; its picks are in Prior art and reuse below.

## Proposal

### User stories

- A team runs the control plane on a small k3s cluster. Each agent has its own identity, and the audit log shows which agent cancelled which Allocation.
- A single researcher runs one launcher, as a plain user, on a workstation or small VM. It starts a version-pinned Kubernetes API host, with its store and garbage collector, and then the framework's controllers.
- An agent lists everything it owns with one label selector. Deleting its ModelDeployment drains its Instances without replacement (HBEP-3) and cancels its pending Allocations. Each started Allocation is released or kept as its retention policy says (HBEP-7); a released Allocation's provider job is cancelled before its object disappears.
- An agent selects every Provider on one platform by its platform label, two grants included, and attaches them to a model name in routing policy (HBEP-5).
- The lifecycle controller crashes during a handover. On restart it re-reads every resource and every provider's job list, and continues from what it observes.
- An operator starts a second copy of a provider controller by mistake. It waits on the first copy's `Lease` and submits nothing.

### Notes, constraints and caveats

What the framework takes from Kubernetes:
- **Level-triggered reconciliation.** Controllers compare desired and observed state on every pass. A missed event is corrected on the next pass, and a restarted controller re-reads everything.
- **Owner references and garbage collection.** Deleting an owner deletes what it owns. Finalizers make a deletion wait until the provider job is cancelled. A bare API server does not collect garbage, so every host runs the garbage collector: the Kubernetes controller manager's, or kcp's own in-process one (Design details).
- **Labels and selectors** for the inventory, for pools and for bulk actions, and for providers: organization, platform, partition and account are labels on each Provider (HBEP-3).
- **Watches,** so the front and agents follow changes without polling, resuming from a resource version.
- **RBAC** per identity, verb, resource type and namespace.
- **Audit** of every API call by the API server.
- **Admission** that refuses invalid specs before they are stored, such as an unpinned revision (HBEP-9). For alpha it uses CRD schema validation and CEL rules, which spike S3 (PR #36) exercised, plus a ValidatingAdmissionPolicy to guard deletions.
  - An edit to a ModelDeployment's serving spec is allowed. It rolls out a new deployment ID in each accelerator class (HBEP-3, open question 7).
  - An existing identity record is never edited: the API server refuses it (Design details).
- Also: server-side dry run, optimistic concurrency on resource versions, a status subresource with conditions, API versions with conversion, and client libraries.
- **Controller concurrency follows Kubernetes practice:**
  - keyed work queues with several workers per controller;
  - one key never processed by two workers at once;
  - idempotent, level-triggered reconciles;
  - rate-limited backoff on failure;
  - providers as independent shards (HBEP-7, open question 11);
  - a leader `Lease` per provider controller, as an extra guard. Spike S4 (PR #32) showed that the `Lease` does not prevent duplicate jobs, since a deposed leader's submission can still land. What keeps one job per Allocation running, in hold mode, is that only the recorded job's hold is ever lifted (HBEP-7, open question 11).

What it does not solve:
- **Provider facts.** Start estimates, hard ends, queue state and billing come from provider plugins (HBEP-4). Kubernetes has no notion of a lease with an unknown start and a hard end.
- **Hedging across providers.** Our own controller does it (HBEP-7).
- **Reachability** of compute nodes (HBEP-6).
- **Guaranteed event delivery.** Kubernetes Events are best-effort and expire, after one hour by default; the alpha launcher keeps them 7 days (Single-user host). A watch cannot resume from a resource version that has been compacted; the client must list again. So HBEP-10's subscriptions need their own durable log, and status conditions are the level truth.
- **Bulk data.** Metrics, usage records and logs (HBEP-8) do not belong in the API store, whose objects are limited to about 1.5 MiB.
- **Approvals.** RBAC says who may act, not whether an action was approved. Approvals are our own resources (HBEP-10).
- **Actions on providers.** The API server's audit sees calls to it, not a controller's submission or cancellation on Slurm. Controllers record those, tied to the resource that caused them.
- **Secrets at rest.** Kubernetes Secrets are not encrypted in storage unless encryption at rest is configured. So in alpha the API stores references to credentials, never the credentials (Design details).
- **Its own operation.** The API server needs a host, backups and upgrades. Our launcher does that for one user.

Deployment options:
- **A small cluster,** k3s, for a team. Controllers and the front may run as pods there; models never do.
- **One host, for one user.** A launcher starts a Kubernetes API host as plain processes, then our controllers. Spike S3 (PR #36) measured the candidates against a small-VM budget set before measuring (Design details, Single-user host).
- **Placement.** The control plane gets a host of its own, apart from other teams' services: in the past project, a full disk on a shared node evicted another team's workloads.

### Risks and mitigations

- **Uninstalling the custom resource definitions deletes every resource of those kinds,** and their finalizers would cancel every provider job. For alpha, a ValidatingAdmissionPolicy refuses deleting the framework's CRDs. Uninstalling removes that policy first, as an explicit step. An approval for it comes with HBEP-10.
- **The API server is down.** The front keeps serving from its last routing table, marked stale (HBEP-8). Running jobs keep running; Allocations are provider leases with hard ends, and nothing in them depends on the control plane being up.
- **A controller is up but stuck.** Each status carries the generation it last reconciled and when. A stale heartbeat raises an event from a component that does not depend on it (HBEP-8, Observers); a leader `Lease` shows which copy is acting.
- **The control plane's view and the provider disagree,** for example a job that the controller never recorded. On start, controllers list provider jobs that carry the framework's tag and match them to Allocations. Unmatched jobs are reported as orphans and never cancelled without approval.
- **Too heavy for one user.** Measured by S3 for k3s, the single-user host: 363 MiB idle and 407 MiB with 100 objects, ready in 10.5 s cold and 6.0 s warm, against a budget of 600 MiB, 800 MiB and 30 s. S3 measured on a loaded 6-core VM, not on the 2 vCPU, 4 GiB budget VM, so start times there are checked at alpha (Graduation criteria).
- **A host that serves the API but does not garbage-collect.** A bare API server with etcd is ready and answers, yet deletes no dependents (spikes S3 and S4). Every host runs the garbage collector, and a probe checks that it works (Operational readiness).
- **Agents bypass the API** and act on providers directly, outside the inventory and the audit. Mitigation: only the controllers can read the provider credentials; agents act through the API.
- **Kubernetes conventions mislead.** Users expect a deployment to own pods and to restart them. Kinds and docs say plainly that Instances live in provider jobs.
- **Kind names overlap with Crossplane's.** Crossplane has its own `Provider` kind, for an installed provider package, in another group. Our docs say that crossplane-runtime's `ProviderConfig` maps to our Provider. On a team cluster that also runs Crossplane, tools must name ours fully, as `providers.hpcburst.groundnuty.github.io`.
- **The Allocation embeds crossplane-runtime's managed-resource fields** (S4). A change to them upstream changes our API. Mitigation: crossplane-runtime is pinned, and the alpha API may change; a later API version converts.

### Open questions

1. **The API group and the kinds' names.** Decided: by the owner, 30 Oct 2026. Group `hpcburst.groundnuty.github.io`, version `v1alpha1`, and the kinds Provider, PlatformProfile, ModelDeployment, Allocation and Instance, as HBEP-3 names them.
2. **The single-user host.** Decided (amended 2026-10-30, after S3's fix, PR #36): k3s as a plain user (`server --rootless --disable-agent`, SQLite). It passed S3's suite and budget with no system change. The assembled upstream stack stays the alternative, and kcp the fallback. Nothing else in this HBEP or in HBEP-3 depends on which one it is.
3. **Where controllers run in the team option:** as pods on the cluster, or on a host that holds the provider credentials. Not needed for alpha: alpha is single-user, and its controllers run on the one host.
4. **The store for HBEP-10's durable event log, for usage records and for metrics.** Not needed for alpha: status conditions are the level truth, and Kubernetes Events a best-effort mirror. The candidate for the event log is embedded NATS JetStream, outside the API store (spike S8, PR #28; HBEP-10). Usage records and metrics are HBEP-8's.
5. **How provider credentials are held.** Decided: for alpha, they are referenced, not stored. A Provider names a mode-600 file on the host, or a Secret in the team option. HBEP-1's content rules apply. Encryption at rest for the team option's Secrets is decided with question 3.
6. **Replicas and leader election.** Not needed for alpha: one replica per controller, restarted by the launcher on failure. Each provider controller still takes its leader `Lease`. Several replicas come with the team option (question 3).
7. **Which evaluated projects become components.** Decided: for the control plane, the Kubernetes API host (question 2), controller-runtime for our controllers, and crossplane-runtime under the Slurm provider (spike S4; HBEP-4). None of the other evaluated projects is a control-plane component; Alternatives says why.
8. **The front's place and reachability.** HBEP-2 (open question 3), HBEP-5 (open question 11) and HBEP-6 (open question 8, and its TLS-terminating front) leave parts of these to this HBEP. Decided for alpha: alpha has one user and one front, on the alpha host. That host must have an address that compute nodes reach, in one of two ways:
   - directly, on the dial-home port;
   - through ssh on the configured ssh port, 22 by default, with a restricted key, over two forwards: one for the agent's control traffic and one for the dial-home tunnel (HBEP-6, Dial-home modes). This is HBEP-6's development path, option B of spike S1 (issue #18).

   A host reachable by neither is out of alpha. The launcher checks this (Single-user host). Whether several users share a front is HBEP-2's question, and is not decided here.

## Design details

### API

- **Field names are camelCase,** per the Kubernetes API conventions (owner decision, 30 Oct 2026), for example `holdRelease`, `lostGrace`, `imageDigest` and `catalogName`. HBEP-3's canonical `v1` object keeps its own snake_case keys: it is a hash input, not a Kubernetes object, and the deployment-ID library maps the fields to it.

- Group `hpcburst.groundnuty.github.io`, version `v1alpha1` (owner decision, 30 Oct 2026). Each kind has a status subresource. ModelDeployment also has a scale subresource.

| Kind | Scope | Spec holds | Status holds |
|---|---|---|---|
| PlatformProfile | cluster | the platform's kind (managed, batch or static) and its profile data (HBEP-4) | when its facts were last checked |
| Provider | cluster | a PlatformProfile reference; the provider settings (account, grant, allowed partitions; HBEP-4); a credentials reference | the time of its last successful status read (`lastSuccessfulStatusRead`); `queueWaits`, the last 20 observed queue waits (HBEP-4); for a managed provider, `catalog` and `lastCatalogRead` (HBEP-4); conditions. The Provider's controller writes all of it, except the condition `DialHomeUnreachable`, which the lifecycle controller applies (Controllers) |
| ModelDeployment | namespaced | the model name, the serving spec (engine flags as ordered `{name, value}` pairs, HBEP-3), the number of instances, the allowed providers, the end time, the default retention policy, the lease limit and workload (HBEP-7), and the routing settings (HBEP-5) | the current deployment ID of each accelerator class, every ID that still has Instances, each class's last measured start-to-ready time (HBEP-7), ready and desired counts, conditions |
| Allocation | namespaced | crossplane-runtime's managed-resource fields, declared by us with crossplane's types and JSON names (not embedded): `providerConfigRef` naming a Provider, immutable; `managementPolicies` defaulting to `[Observe, Create, Delete, LateInitialize]`, the set the provider registers, immutable; `writeConnectionSecretToRef`. Exactly one of `batch` (the submission parameters, never edited; HBEP-4 lists them) or `managed` (a catalog name). The assigned ModelDeployment; the retention policy, optional with no schema default (the controller applies HBEP-7's default, `Keep`, and shows the effective value in status) | a mirror of the job ID and the submission record, for display only (they live in annotations: Ownership, records and deletion); phase, start and hard end; duplicates; conditions (S4) |
| Instance | namespaced | the full deployment ID, its identity record, its Allocation and its launch (HBEP-7), all immutable | the phase (HBEP-3), the time of its start instruction (HBEP-7), the name of its latest verification record and its verdict, its lease end (`leaseEnd`, copied from its Allocation's hard end by the lifecycle controller at every observe; HBEP-7), when it ended (`endTime`), its recent restarts (`restarts`), the agent registration it started under (`agent`, with `agentInstanceID` and `restartCount`; HBEP-7), conditions |

- **PlatformProfile schema.** HBEP-4 owns it (its open question 3). For alpha, the fields that the Slurm provider and the deployment ID read are typed. The rest of the profile is kept as an untyped block until HBEP-4 fixes the schema.
  - Per partition: the GPU model and the CPU architecture. Together they are the accelerator class, a deployment-ID input (HBEP-3).
  - `holdRelease` (bool): whether a plain user's `sbatch --hold` is honoured and that user can lift it. It is verified per platform. False runs S4's fallback mode (HBEP-7, open question 11).
  - `lostGrace` (duration, default 2 min): how long a recorded job may be absent from both the queue listing and accounting before it is reported Lost.
  - `inflightWindow` (duration): how long an unresolved submission attempt is waited on. It must exceed the worst remote `sbatch` time, because a local timeout does not stop a remote `sbatch`.
  - `unknownBound` (duration, S4's default 10 min): in fallback mode, how long an Unknown attempt may stay unresolved before an operator is asked.
  - From spikes S4 and S5: the time zone, the requeue default, how long a finished job stays in the queue listing, job-name rules, whether accounting keeps the job comment, and the JSON output version.
  - Read by the lifecycle controller and the file staging, typed by HBEP-4 (amended 2026-10-30): per partition, the longest time limit, the GPUs per node, the CPUs and memory per GPU, the image format (`imageFormat`, `sif` or `sandbox`; amended 2026-10-31), and the job's temporary directory with the node feature it needs (`tmpDir` and `tmpDirFeature`, proposed; amended 2026-11-01); the staging directory; the drain window; an optional idle cap; and the health and restart thresholds. HBEP-4's Platform profile schema gives their names, types and defaults.
- **Alpha precondition: `holdRelease` is verified on the alpha platform;** otherwise S4's fallback mode runs. Spike S1 (issue #18) measured it true on 30 Oct 2026 on Helios CPU, Helios GH200 and Athena A100, with one trial per partition: the hold was honoured as `JobHeldUser`, and the same user lifted it. So Helios and Athena run hold mode. Sano is not verified, so it runs fallback mode until checked.
- **Labels** under the group's prefix:
  - on Providers: `organization`, `platform`, `partition` and `account` (HBEP-3);
  - on Allocations and Instances: `provider` and `model-deployment`;
  - on Instances: `deployment-id`, with the display form, because a label value holds at most 63 characters;
  - on identity and verification records: `record` (`identity` or `verification`), `deployment-id` and `model-deployment`, and on verification records also `instance-uid`.
  - **Long values.** A value longer than 63 characters, such as a long ModelDeployment name, is written as its first 50 characters, then `-`, then the first 12 hex characters of the SHA-256 of the full value. The full value stays where the object names it in full: an owner reference, a spec field or an annotation.
- **Names.** Provider names are DNS-1123 labels (HBEP-3). Allocation and Instance names are generated by the controllers. An Allocation's name never derives from a model (HBEP-3, Allocation and job identity).

### Ownership, records and deletion

- A ModelDeployment owns its Instances through a controller owner reference, as a ReplicaSet owns its pods. Each Instance is bound to one Allocation for life.
- **Draining before deletion.** Each Instance carries a drain finalizer, `hpcburst.groundnuty.github.io/drain`. The lifecycle controller removes it only once the Instance is `ended`. So deleting a ModelDeployment drains its Instances before they go: garbage collection marks them for deletion, and each drains without a replacement (HBEP-3) and ends. The ModelDeployment may already be gone by then, since draining needs only the Instance and its Allocation.
- No ModelDeployment owns an Allocation (HBEP-3). An Allocation names the ModelDeployment it is assigned to. The lifecycle controller cancels a pending Allocation when its deployment ends, deletion included, and applies a started one's retention policy (HBEP-7).
- **The Allocation's record** (spike S4). The provider keeps an Allocation's submission record and job ID as annotations. It writes each with a resourceVersion precondition, so a conflict means re-read, never overwrite.
  - `crossplane.io/external-name`: the job tag (HBEP-3).
  - `hpcburst.groundnuty.github.io/submission`: the latest attempt, with its number, token, state, writer, time and reason (HBEP-7, open question 11).
  - `hpcburst.groundnuty.github.io/job-id`: the established job ID, written once, never changed or removed.
  - `hpcburst.groundnuty.github.io/hold-lifted`: the job ID whose hold was lifted. The hold is never lifted again, except the known risk in HBEP-4 (Risks: the hold lift's check-then-act window).
  - `hpcburst.groundnuty.github.io/abandon-attempt`: an operator's decision on an unresolved attempt, in fallback mode.
  - `hpcburst.groundnuty.github.io/idle-since`: when the Allocation became idle, the anchor of retention's `idleLimit` and the profile's `maxIdle` (HBEP-7, Retention). The provider does not write it: its one writer is the lifecycle controller, with the same precondition.
  - Status mirrors them for display. Annotations hold the record because they survive a restore without status. crossplane-runtime's critical-annotation updater re-applies every in-memory annotation on a conflict, which could roll the record back, so the provider replaces it with one that re-applies only crossplane's own keys (S4).
- **The Allocation's finalizer** cancels, through the provider plugin (HBEP-4), every live job carrying the Allocation's tag: held, pending, running or suspended, not only the recorded one. It stays while any job with the tag is live, or while a submission attempt is unresolved (HBEP-7, open question 11). The deletion completes only then.
- **Identity records** (HBEP-3, HBEP-9). Each deployment ID's inputs are stored as a `ControllerRevision`, a built-in Kubernetes kind for immutable snapshots, so no sixth kind is needed.
  - Its data holds the canonical `v1` object and the full hash.
  - **One record per ID per namespace, content-addressed.** It is named `id-<first 12 hex of the ID>`. A ModelDeployment that computes an ID creates its record only if it is absent; one that already exists is reused, never rewritten, since the same inputs give the same bytes. A record whose stored hash differs from the one recomputed for its name is a collision or a bug: the controller refuses the new ID, the ModelDeployment gets `Degraded` with reason `IdentityUnresolved`, and an event is raised (HBEP-3).
  - The API server refuses any change to a ControllerRevision's data (Kubernetes' own update validation, [`pkg/apis/apps/validation/validation.go` at v1.36.2](https://github.com/kubernetes/kubernetes/blob/v1.36.2/pkg/apis/apps/validation/validation.go), accessed 2026-10-30).
  - Each Instance's spec names its record and its full ID. A CEL rule (`self == oldSelf`) makes that spec immutable.
- **Verification records.** Each verification of an Instance, at start and after every restart that reverifies, is its own `ControllerRevision`, named `ver-<instance UID>-<generation>` (HBEP-7). It is written once, pass or fail, before the Instance can become ready.
  - Its data holds the resolved settings the Instance reported, its GPU type, its engine fingerprint, the outcome of the file rehash, the measured template overhead and the model's `generation_config` defaults from the staged `generation_config.json` (HBEP-7, Instance start), and the verdict (HBEP-3, HBEP-9).
  - The Instance's status names the latest record and mirrors its verdict.
- **Accounting records.** When an Allocation's last job has ended, the Provider controller writes its accounting as one `ControllerRevision`, named `acct-<Allocation UID>`, in the Allocation's namespace, before the Allocation's finalizer goes (HBEP-8, Provider accounting). It is written once and never rewritten.
- **Records outlive deletion.** Identity, verification and accounting records have no owner reference to the ModelDeployment, the Instance or the Allocation, so garbage collection never removes them. They carry the labels listed under API: the deployment ID's display form, and on verification records the Instance's UID. They live in the ModelDeployment's namespace, and are deleted only by an explicit operator action or by a retention policy for records, which HBEP-9 sets; until then they stay. Deleting the namespace deletes them too, so an operator exports them first (HBEP-9).
- Exporting records, for a replication package, is HBEP-9's.

### Admission

For alpha, no webhook server runs.
- **CRD schema and CEL rules:**
  - a revision is a full commit id (HBEP-9);
  - a Provider name is a DNS-1123 label;
  - an Instance's spec is immutable;
  - an Allocation's submission parameters are immutable;
  - a ModelDeployment's engine flags use long names without leading dashes, matching `[a-z0-9][a-z0-9_-]*`, and env names match `[A-Z_][A-Z0-9_]*`;
  - refusals compare names after replacing `_` with `-`, and refuse the credential names (flags and env), the short aliases, `config`, `no-` forms and dotted sub-keys (HBEP-3; the library applies the full prefix rule);
  - a ModelDeployment's serving spec may change, which rolls out new IDs (HBEP-3).
- **The job ID is not guarded here.** It is an annotation, and a CRD's validation rules reach no metadata beyond the name and generated name ([documentation source](https://github.com/kubernetes/website/blob/main/content/en/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions.md), accessed 2026-10-30). The provider's compare-and-swap writes keep it written once (Ownership, records and deletion).
- **A ValidatingAdmissionPolicy** refuses deleting the framework's CRDs. Such policies are stable since Kubernetes 1.30, and see the existing object on a delete ([documentation source](https://github.com/kubernetes/website/blob/main/content/en/docs/reference/access-authn-authz/validating-admission-policy.md), accessed 2026-10-30).
- **A webhook** is added only for a rule that neither form can express. The approval gates of HBEP-10 are one case, after alpha.

### Controllers

- Each controller is a controller-runtime manager, with the keyed work queues and backoff listed in Notes.
- **The Provider reference.** An Allocation's `providerConfigRef` names a cluster-scoped Provider, and the Slurm provider resolves it itself. crossplane-runtime's ProviderConfig usage helpers treat only a kind whose name starts with `Cluster` as cluster-scoped (`pkg/resource/enqueue_handlers.go`, v2.4.3), so the provider does not use them.
- **One manager per provider.** controller-runtime elects a leader per manager, so each provider's controller runs as its own manager with its own `Lease`, `provider-<name>`. S4 ran its provider this way. The prefix keeps a Provider named `lifecycle` from sharing the lifecycle controller's `Lease`, `lifecycle`.
- **Every `Lease` lives in the namespace `hpcburst-system`,** which the launcher creates before it starts the controllers (science-agent decision, 31 Oct 2026). Besides the leader `Lease`s, the front renews a heartbeat `Lease` there, `front` (HBEP-8, Observers).
- **Field managers.** Every write names its controller's field manager, so `managedFields` shows who wrote what. `hpcburst-provider` is every Provider's controller: it writes Provider status and keeps every condition it does not own. `hpcburst-lifecycle` is the lifecycle controller, its agent endpoint included. On a Provider it owns only the condition `DialHomeUnreachable`, which it sets by server-side apply to the status, owning that one list entry. Neither removes the other's condition (science-agent decision, 31 Oct 2026; HBEP-4).
- **Alpha runs one replica of each.** The launcher restarts a controller that exits. The `Lease` then guards against a stale second copy, for example one an earlier launcher left running. It does not prevent duplicate jobs, because a deposed leader's submission can still land (S4). In hold mode only the recorded job's hold is ever lifted, so a duplicate stays held and is cancelled (HBEP-7, open question 11).
- The lifecycle and hedging controller (HBEP-7) manages Allocations, including their retention and repurposing. Verification (HBEP-9) and observers (HBEP-8) run as controllers. The front (HBEP-5) watches Instances, ModelDeployments, Providers and PlatformProfiles to build its routing table. A model name's ModelDeployments are its routing policy (HBEP-5).
- Desired state is in each spec; observed state, with observation times, is in each status.

### Single-user host

Decided for alpha (amended 2026-10-30, after S3's fix, PR #36):
- **The alpha host: k3s with SQLite, run as a plain user.**
  - It runs as `k3s server --rootless --disable-agent`, with no root and no system change. It is the same software as the team option, in one process with the garbage collector built in.
  - S3 measured it at 363 MiB idle and 407 MiB with 100 objects, ready in 10.5 s cold and 6.0 s warm. All 11 e2e tests passed, the cascades included. A Kubernetes minor upgrade (1.35.9 to 1.36.5) kept all 103 objects.
  - Caveats:
    - k3s labels `--rootless` experimental. It works here because `--disable-agent` skips the user-namespace setup.
    - The components S3 disabled stay disabled: the scheduler, cloud controller manager, helm controller, kube-proxy, network policy controller and packaged add-ons (`spikes/s3/README.md`, k3s section). RSS with k3s's default components was not measured and would be higher.
    - On a machine that already runs another k3s, the launcher isolates its config file, registry file (`--private-registry`), data directory and ports. HBEP-11's launcher issue, #41, does this.
    - On a machine whose system k3s runs the same release, a user-level k3s can silently run root's unpacked binaries from `/var/lib/rancher` instead of its own. k3s re-executes itself from its data directory, and falls back to the system's files when its own lack them. The launcher reads what the process runs through `/proc/<pid>/exe`, and when that is not under its own data directory, `hpcburst up` fails and `hpcburst status` reports the host unhealthy (PR #49; L-17.3).
- **The alternative: the assembled upstream stack.** It is kept if k3s's rootless mode breaks in a later release.
  - Three processes: kube-apiserver, etcd, and kube-controller-manager limited to its garbage-collector and namespace controllers (`--controllers=garbage-collector-controller,namespace-controller`, as S3 ran it).
  - All three are version-pinned and started by our launcher, as a plain user.
  - S3's final run measured it at 365 MiB idle and 386 MiB with 100 objects, ready in 3.9 s cold and 3.0 s warm. A minor upgrade (1.35.0 to 1.36.2) kept all 103 objects with their UIDs.
- **Fallback: kcp,** if one process with no root becomes a hard requirement and k3s fails. S3's final run measured its release binary at 478 to 500 MiB, ready in 13.9 s cold and 8.8 s warm, and its upgrade kept every object. S3 did not check Leases, Events, webhooks, `ControllerRevision` or ValidatingAdmissionPolicy on kcp, and a kcp workspace serves fewer built-in APIs, so those are checked first if it is chosen.
- **The garbage collector is required on any host.** S3 and S4 both showed that a bare API server with etcd, as envtest runs it, deletes no dependents.
- **Little else depends on the choice.** Controllers, CRDs, admission policies and the front reach the Kubernetes API through a kubeconfig. Only the launcher knows which host runs, and only it handles the store-full case, which differs per host (Operational readiness 5).

**The launcher** is our own. It:
- verifies the pinned host binaries against their checksums, recorded in `build/dependencies.yaml` (HBEP-15), and starts them;
- binds the API server to loopback, because nothing outside the host talks to it. Instances reach the front, never the API (HBEP-6);
- issues one kubeconfig per identity: the user, each controller, and each agent identity (HBEP-10), with RBAC enabled;
- creates the certificate of the lifecycle controller's agent endpoint, which listens on loopback; `agent.json` carries its pin, and in direct mode the front passes `/agent/v1/` through to it on the dial-home port (HBEP-7). The certificate is self-signed and carries a fixed synthetic DNS name, proposed as `dial-home.hpcburst.invalid`, which frpc names as its `serverName` when it verifies the dial-home listener with this certificate as its trusted file (HBEP-6). It is public, and is staged with each Allocation (HBEP-4);
- starts frps, the dial-home tunnel's server (HBEP-6), as it starts the host: it verifies the pinned binary's checksum from `build/dependencies.yaml`, writes its configuration file (mode 600, with the password of frps's loopback web server), records it by process ID plus start time, judges its health through a pidfd, its three loopback ports and its `/healthz`, and restarts it when it exits. frps binds loopback only, at offsets of the launcher's port block (HBEP-6, Components);
- installs the CRDs and the admission policy, then starts the controllers and restarts any that exit;
- checks the front's reachability (open question 8). It warns when no front address and dial-home mode (direct, or ssh on the configured ssh port, 22 by default) are configured, and checks that a configured address answers on that port. While none is configured, the lifecycle controller submits no Allocation, and each ModelDeployment carries `Degraded` with reason `FrontNotConfigured` (decided while building the launcher, PR #49; the name in PR #71). The first Instance on each Provider then confirms reachability from a compute node by registering over its dial-home path. An Instance that cannot register fails at start, and the lifecycle controller sets the condition `DialHomeUnreachable` on its Provider (HBEP-4, open question 16);
- has the API server write its audit log at the Metadata level, with no request bodies, so a Secret's content is never logged;
- gives the lifecycle controller's `/metrics` the loopback port at its port base + 12 (HBEP-8, Metrics);
- has `hpcburst status` read the front's `/healthz`, and report the front as "degraded: usage writes failing" while its usage sink says `failing`, not as a readiness failure: the exit code stays 0 and the overall health `healthy`, and only the front's line says degraded (HBEP-8, Usage records);
- has `hpcburst status` read the lifecycle controller's `/readyz` for its health, which fails until its caches have synced and it leads its `Lease`; `/healthz` stays its liveness check; the launcher restarts it when it exits. While no front `Lease` exists, the front is shown as absent (HBEP-8, Observers);
- has the API server keep events 7 days: it passes `--kube-apiserver-arg=event-ttl=168h`, where the default is 1 h. k3s passes the flag through, and kine, its SQLite datastore, deletes each event once the TTL has elapsed. kine starts each event's clock again when the host restarts, so an event lives at least 7 days (HBEP-8, Retention, with the sources);
- backs up the store before every upgrade: an etcd snapshot, or a copy of the SQLite file;
- upgrades the host one minor version at a time, as S3 tested.

### Team option

A small k3s cluster. Controllers and the front may run as pods there; models never do. Where controllers run, and with how many replicas, is decided for beta (open questions 3 and 6).

### Credentials

- For alpha, the API holds references to credentials, never credentials. A Provider names a mode-600 file on the control-plane host, or, in the team option, a Secret.
- A Hub token for a gated checkpoint (HBEP-3) is referenced the same way.
- In alpha, "the control plane's secret store" that other HBEPs name means these files, or these Secrets.
- The controller refuses a file that others can read or that its user does not own. It then sets the condition `CredentialsInvalid` on the Provider and makes no provider call.
- No credential is copied into an object, an event, a log or a command line (HBEP-1, Content rules; HBEP-4).

### Durable event log

Not in alpha. Status conditions are the level truth, and Kubernetes Events are a best-effort mirror. The candidate is embedded NATS JetStream, outside the API store (S8, HBEP-10).

### Test plan

Against the single-user host (k3s as a plain user) and, for beta, the team option:
- Kill the lifecycle controller mid-handover: on restart it continues, and no second job is submitted.
- Delete a ModelDeployment: its Instances drain, no replacement is submitted, its pending Allocations are cancelled, and each started Allocation is released or kept as its retention policy says. A released Allocation's provider job is cancelled before its object disappears.
- **Drain finalizer.** Delete a ModelDeployment with a ready Instance: the Instance stays until it is `ended`, and only then goes.
- **Records survive deletion.** Delete a ModelDeployment and its Instances: their identity and verification records remain, found by the `deployment-id` and `instance-uid` labels, with their data unchanged. The check reads them by label after the Instances are gone; reading Instance status proves nothing, since it goes with the Instance.
- **Allocation finalizer.** Delete an Allocation while its submission attempt is unresolved, against S4's fake Slurm: the finalizer stays until the attempt resolves, every live job with the tag is cancelled, and only then does the object go.
- **Long label values.** A ModelDeployment with a 100-character name gets a 63-character `model-deployment` label, matching a golden value computed outside our code.
- **Garbage collection.** Delete an owner whose dependents no controller deletes: they are gone, checked by UID. A test that reads only the host's `/readyz` proves nothing here, since S3's bare host was ready and collected nothing.
- Select Providers by the platform label: every provider on that platform is returned, two grants included.
- Delete a custom resource definition while the admission policy is in place: refused.
- Stop the API server: the front keeps serving from its last table and marks it stale.
- Let a watch fall behind compaction: the client lists again and loses no state.
- Submit a spec with an unpinned revision: admission refuses it.
- **Identity records.** Edit a record's data: refused by the API server. Edit an Instance's spec: refused. Edit a ModelDeployment's serving spec: accepted, a new current ID appears for each class, and old-ID Instances keep theirs.
- An agent's identity tries to cancel another owner's Allocation without the right role: refused, and the audit log shows the attempt.
- Start the controllers beside a provider job with the framework's tag and no Allocation: it is reported as an orphan, not cancelled.
- **Lease.** Start a second copy of a provider controller: it makes no provider call while the first holds the `Lease`. Kill the first mid-submit: the second takes over and finds the job by its tag. Whatever it then submits, at most one job with the tag has its hold lifted, and any other is cancelled (S4).
- **Credentials.** A Provider whose credentials file is readable by others: refused with `CredentialsInvalid`, and no provider call is made.
- **Front reachability.** With no front address or dial-home mode, the launcher warns and the lifecycle controller submits no Allocation, with `FrontNotConfigured`. An Instance whose agent cannot reach the front fails at start, and its Provider carries `DialHomeUnreachable`. A later write of Provider status by the Provider's controller keeps that condition.
- **Leases and field managers.** Each Provider's `Lease` is `provider-<name>`, the lifecycle controller's is `lifecycle` and the front's heartbeat `Lease` is `front`, all in `hpcburst-system`; a Provider named `lifecycle` shares no `Lease`. Every field a controller writes shows `hpcburst-provider` or `hpcburst-lifecycle` in `managedFields`, and on a Provider `hpcburst-lifecycle` owns only the `DialHomeUnreachable` entry.
- **Another k3s's binaries.** On a machine whose system k3s runs the same release, a user-level k3s that runs a binary outside its own data directory makes `hpcburst up` fail. A check of `/readyz` alone proves nothing here, since a k3s running the borrowed binaries answers it too.
- **Event TTL.** The launcher's k3s command carries `event-ttl=168h`. On a host started with a short test TTL, an event disappears once it has passed; after a host restart, an event created before it is still listed.
- **Upgrade and downgrade.** Upgrade the host one minor version with objects present: every object is kept with its UID. Downgrade by restoring the backup with the earlier binaries: the objects from before the upgrade are back.
- **Store full.** Fill the store: on k3s, a size-limited filesystem holding its data directory; on the assembled stack, etcd's quota. Writes are refused, no controller submits a new job, and the launcher's health reports the write failures (or etcd's alarm).

S3 exercised validation, watches, conflicts, cascades and finalizers. Leases, Events, RBAC, the audit log, the deletion guard and resumption after compaction are first tested here.

### Compatibility and upgrades

- Custom resources are versioned from the start, beginning at `v1alpha1`.
  - While alpha, a new version may change incompatibly. The launcher then backs up the store and migrates stored objects.
  - From beta, versions convert, with a conversion webhook where schemas differ.
- On the single-user host, the launcher upgrades the controllers and the front together. Each must still ignore status fields it does not know, since a downgrade can meet them.
- The host is upgraded one minor version at a time, with the store backed up first. S3 kept every object across one minor upgrade on each measured host.
- **Downgrade:** restore the store from the backup taken before the upgrade, and start the earlier binaries. Changes made after the upgrade are lost. Provider jobs submitted after it are found by their tags as orphans (Risks). S3 did not test a downgrade, so the Test plan does.
- crossplane-runtime is pinned, because the Allocation embeds its fields.

### Security

- Provider credentials are readable only by the controllers that use them, and the API holds only references to them (Credentials).
- Each agent and each person has its own identity; RBAC limits each to its verbs and namespaces.
- On the single-user host the API server listens on loopback only.
- No prompt or response is stored in the API store. Requests never pass through it.
- The audit log is written at the Metadata level, so it records only metadata for Secrets, never their content.

### Graduation criteria

- **Alpha:**
  - the single-user host (k3s as a plain user) serves the five kinds at `v1alpha1`;
  - the alpha host passes the front-reachability check (open question 8), and the alpha platform has `holdRelease` verified, or runs fallback mode;
  - the Test plan passes on it with one batch provider;
  - the host starts within S3's budget on a 2 vCPU, 4 GiB VM.
- **Beta:**
  - the Test plan passes on the team option (k3s) as well as on the single-user host;
  - open questions 3 and 6 are decided;
  - an upgrade with the controllers running is tested.
- **Stable:**
  - the kinds have reached `v1`, and conversion from every earlier served version is tested;
  - a host upgrade, and a downgrade by restore, run in CI.

## Operational readiness

1. **Working:** the host's `/readyz` answers, and so does the controller manager's `/healthz`, as S3 checked on k3s and on the assembled stack. Each provider controller's `Lease` is current, and each resource's status shows a recent reconcile. End to end, the launcher deletes a probe owner and its dependent is gone within a minute, which shows the garbage collector works. A status older than its controller's resync period, or a probe dependent that stays, says it is not working.
2. **Restart:** controllers re-list resources and provider jobs, and continue from observed state. Nothing is held only in memory. The host restarts from its data directory, ready in 6.0 s warm on k3s (S3).
3. **Silent failure:**
   - **A write that reports an error may still have taken effect.** In the launcher's store-full test (PR #49), a create that returned "database or disk is full" left its object stored. Controllers re-read state after any write error and never assume the write failed (L-17.2).
   - **A /proc liveness check can misread a live process** as a zombie while one of its threads is in `execve` (k3s re-executes itself). The launcher decides liveness from a pidfd, which the kernel signals only once every thread has exited (PR #49). Any other liveness check should do the same; a scan of /proc threads is racy too (L-17.1).
   - **A user-level k3s that runs another k3s's binaries** looks healthy. The launcher checks `/proc/<pid>/exe` against its own data directory (Single-user host; L-17.3).
   - a controller that holds its `Lease` but reconciles nothing. Reconcile times in status show it.
   - a host that is ready but does not garbage-collect. The probe in item 1 shows it.
   - a write that returns an error but took effect. etcd can return a no-space error for a write that still succeeded ([etcd maintenance guide](https://github.com/etcd-io/website/blob/main/content/en/docs/v3.6/op-guide/maintenance.md), accessed 2026-10-30). Controllers re-read after every failed write, and never assume that it did nothing.
4. **Provider limits:** controllers poll providers at rates set in platform profiles (HBEP-4), within login throttles.
5. **Capacity:** the API store holds tens to hundreds of objects, since bulk data stays out. In S3, 100 objects added no measurable memory. Events kept 7 days add an estimated 10 MB or less at alpha scale, since an event for a persisting condition repeats at most once an hour (HBEP-8, Retention).
   - **Storage full.** etcd raises its NOSPACE alarm at its backend quota, and then accepts only reads and deletes ([etcd maintenance guide](https://github.com/etcd-io/website/blob/main/content/en/docs/v3.6/op-guide/maintenance.md), accessed 2026-10-30). With SQLite, a full disk stops writes.
   - Either way, controllers submit nothing new, because no submission starts without a recorded attempt (S4). Running jobs keep running to their hard ends.
   - The launcher's health reports the write failures on k3s, or the alarm on etcd. The host's disk is its own (Placement).
6. **Idle cost:** one small host, or about 0.36 GiB of memory on k3s (S3). No grant hours and no provider connections when nothing is deployed.
7. **Rollback:** stopping the control plane leaves provider jobs running until their hard ends, and the front serving from its last table. Established tunnels stay up, since HBEP-6's frps plugin is consulted only at logins and proxy registrations; a tunnel that reconnects meanwhile waits for the lifecycle controller to return (HBEP-6, Risks). Removing the framework needs releasing every Allocation first, with approval, and then removing the CRD-deletion policy.

## Drawbacks

- Kubernetes is a large dependency for a framework whose compute runs elsewhere.
- Users unfamiliar with Kubernetes face its concepts even in the single-user option.
- k3s's `--rootless` mode is experimental upstream; a later release could break the single-user host. The assembled stack stays as the alternative; if we ever fall back to it, we own its packaging: certificates, tokens, and flags that change across minor versions.

## Alternatives

- **Our own API and database.** Lighter for one user, with full control over event semantics. We would rebuild watches, RBAC, audit, garbage collection, admission and versioning, and agents would lose standard tooling.
- **Virtual-kubelet pods on Slurm.** Models as pods on a virtual node that submits Slurm jobs, managed by standard workload controllers. Rejected for the compute layer: squeezing leases into pods hides the facts the scheduler needs. A pod has no start estimate or hard end the controllers can plan with, and one pod cannot be submitted to several providers and the losers cancelled. A rolling update with `maxSurge` does start a replacement before its predecessor ends, but it cannot plan that from a lease end it does not see.
- **A bare API server with etcd (envtest).** Rejected: it has no garbage collector (S3, S4).
- **kcp embedded as a Go library, in one bundled binary.** Not pursued. S3 found that embedding pins our controllers' controller-runtime to kcp's own Kubernetes fork, and its build did not complete. The gain would be one process and about 38 MiB.
- **The other projects evaluated (28 Oct 2026).** None is a control-plane component:
  - interLink and other virtual-kubelet offloading aim to run pods on remote systems such as batch clusters (rejected above for the compute layer);
  - Slinky and slurm-bridge aim to join Slurm and Kubernetes scheduling;
  - KubeAI aims to serve models on Kubernetes;
  - the vLLM production stack aims to deploy vLLM with routing on Kubernetes;
  - the Gateway API Inference Extension aims at model-aware routing through Kubernetes gateways;
  - llm-d aims at distributed LLM serving on Kubernetes.

  Their routing and serving parts are weighed in HBEP-5 and HBEP-7.

## Prior art and reuse

The control plane replaces the files and scripts that held www-2027's state: per-job endpoint, control and configuration files on each cluster, process-ID files, scheduled respawners, and a script that listed jobs across clusters.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| kube-apiserver, etcd, kube-controller-manager | [kubernetes/kubernetes](https://github.com/kubernetes/kubernetes); [etcd-io/etcd](https://github.com/etcd-io/etcd) | Apache-2.0 | Alternative single-user host (k3s is the host, open question 2): pinned binaries the launcher would start. The controller manager runs only its garbage-collector and namespace controllers |
| k3s | [k3s-io/k3s](https://github.com/k3s-io/k3s) | Apache-2.0 | Dependency: the team option, and the single-user host as a plain user (S3, PR #36) |
| kcp | [kcp-dev/kcp](https://github.com/kcp-dev/kcp) | Apache-2.0 | Fallback single-user host (open question 2) |
| controller-runtime | [kubernetes-sigs/controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) | Apache-2.0 | Dependency: our controllers, their work queues and leader election; used by spikes S3 and S4 |
| crossplane-runtime | [crossplane/crossplane-runtime](https://github.com/crossplane/crossplane-runtime) | Apache-2.0 | Dependency under the Slurm provider (HBEP-4; spike S4): the Allocation's managed-resource fields and reconciler |
| NATS JetStream | [nats-io/nats-server](https://github.com/nats-io/nats-server) | Apache-2.0 | Candidate, not in alpha: the durable event log (spike S8, HBEP-10) |
| frps (frp v0.71.1) | [fatedier/frp](https://github.com/fatedier/frp) | Apache-2.0 | Dependency: the pinned release binary the launcher starts and supervises on loopback, the dial-home tunnel's server (HBEP-6) |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| An owner that owns its replicas; an immutable snapshot of a template as a `ControllerRevision` | Kubernetes ReplicaSet and StatefulSet history | A ModelDeployment owns its Instances; identity records |
| A config object that managed resources reference for credentials and settings | Crossplane's `ProviderConfig` ([crossplane/crossplane](https://github.com/crossplane/crossplane)) | An Allocation's `providerConfigRef` names our Provider |
| Admission rules in CEL, without a webhook server | Kubernetes ValidatingAdmissionPolicy ([documentation](https://kubernetes.io/docs/reference/access-authn-authz/validating-admission-policy/); [its source](https://github.com/kubernetes/website/blob/main/content/en/docs/reference/access-authn-authz/validating-admission-policy.md), accessed 2026-10-30) | The CRD-deletion guard |
| An API server and etcd started as plain processes, without nodes | controller-runtime's envtest, and the harness of spike S3 | The alternative single-user host (the assembled stack), with the controller manager added; the alpha launcher runs k3s |

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| A job ended by its time limit left its endpoint file behind. | L-10.10 | Observed state is reconciled into status; owner references remove dependents. |
| Nothing recorded who had started what. | L-10.17 | One store of desired and observed state, with labels and owners. |
| Keyword sweeps missed processes at the end. | L-15.2 | Clean-up by ownership and garbage collection. |
| A scheduled respawner revived killed processes. | L-15.1 | Controllers act only on desired state in one store. |
| Monitors read their configuration once at start. | L-10.9 | Watches follow changes. |
| Running scripts could not be changed safely. | L-3.7, L-3.13 | Versioned specs, validated at admission; a serving-spec change rolls out a new deployment ID. |
| Every agent ran as one Unix user. | L-11.1 | RBAC and audit per identity. |
| A full disk on a shared host evicted another team's workloads. | L-12.1 | The control plane gets a host of its own. |
| A /proc liveness check misread a live process as a zombie during `execve`. | L-17.1 | The launcher counts a zombie leader as dead only when no thread is alive (Operational readiness 3). |
| A write refused for a full store still took effect. | L-17.2 | Controllers re-read state after any write error (Operational readiness 3). |
| A user-level k3s silently ran root's unpacked binaries. | L-17.3 | The launcher checks `/proc/<pid>/exe` and refuses the host (Single-user host). |
| A library default that the text called inert would have stalled every Allocation. | L-17.4 | The Allocation declares crossplane's managed-resource fields itself, with the provider's policy set as an immutable default (API; PR #48). |

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #11.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).
- 2026-10-30: Prior art and reuse filled from the state-of-the-art evaluation.
- 2026-10-30: design completed for implementable.
- 2026-10-30: implementable. Owner decisions on the API group, kinds and version folded in; spikes S3 (PR #36, the host rule), S4 (PR #32), S5 (PR #19) and S8 (PR #28) carried in; every open question decided or marked for alpha.
- 2026-10-30: amendment with HBEP-5 becoming implementable: the ModelDeployment's `routing`, the PlatformProfile's `drainMargin` and the Instance's `status.leaseEnd`.
- 2026-10-31: amendment from the merged contracts (PR #71) and the launcher (PR #49): the API's new fields and the `idle-since` annotation, the field managers, the `Lease` names and namespace, the condition names, and the caveat that a user-level k3s can run root's binaries. Lessons L-17.1 to L-17.4 cited.
- 2026-10-31: amendment with HBEP-6 becoming implementable: the launcher starts and supervises frps on loopback, its certificate carries a fixed synthetic DNS name for frpc, and ssh mode uses two forwards.
- 2026-10-31: amendment from the owner's decision on the dial-home route (issue #18): ssh mode over the configured ssh port, 22 by default; the `hold-lifted` guarantee qualified with HBEP-4's known risk.
- 2026-10-31: amendment with HBEP-8 becoming implementable: the launcher keeps events 7 days (`event-ttl=168h`), with its test and the store's growth; a stale heartbeat's event has an emitter (HBEP-8, Observers); the `front` `Lease`, the `acct-` accounting records, the metrics port at base + 12, and `hpcburst status` reading the front's `/healthz`.
- 2026-10-31: `hpcburst status` reports a usage failure as degraded, not unhealthy (HBEP-8's amendment of the same day).
- 2026-11-01: `hpcburst status` reads the lifecycle controller's `/readyz`, and shows an absent front (HBEP-8's amendment of the same day); the partition's `tmpDir` and `tmpDirFeature` (HBEP-4); the verification record's contents aligned with HBEP-7's.

## Amendments

- **2026-10-30: the front's routing settings, the drain margin and the Instance's lease end.** Made with HBEP-5 becoming implementable.
  - The ModelDeployment's spec gains an optional `routing` with four fields: `loadBound.tight` (default `"1.25"`), `loadBound.loose` (default `"2.0"`), `instanceCap` and `maxRequestDuration`. The front reads them all. The lifecycle controller also reads `maxRequestDuration`, which shortens the drain window (HBEP-5, HBEP-7).
  - The PlatformProfile gains the typed field `drainMargin`, next to `drainWindow` (HBEP-4).
  - The Instance's status gains `leaseEnd`, which the lifecycle controller copies from the Allocation's hard end at every observe, and from which the front enforces the drain deadline (HBEP-5, HBEP-7).
  - A model name's routing policy is the set of ModelDeployments of that name, a managed one included, so no kind is added.
- **2026-10-30: camelCase field names** (owner decision, while building the CRDs, PR #48). The HBEPs' snake_case field names for CRD and profile fields become camelCase; Slurm's JSON field names and HBEP-3's hash-object keys keep their own spelling.
- **2026-10-30: decisions from building the CRDs and the launcher (PRs #48, #49).** The Allocation declares crossplane's managed-resource fields itself, with `managementPolicies` defaulting to the provider's registered set and immutable, and `providerConfigRef` immutable; the embedded default `["*"]` would have stalled every Allocation. ModelDeployment `spec.source` and Allocation spec are one-of served-or-managed and batch-or-managed. S5's facts are typed PlatformProfile fields. Retention has no schema default. The launcher warns, rather than refuses, without a front; the controller then submits nothing. A store-full write that reports an error may still have taken effect, so controllers re-read state after any write error (Operational readiness 3).
- **2026-10-30: single-user host settled.** Spike S3's fix (PR #36) showed that k3s runs as a plain user (`server --rootless --disable-agent`, SQLite) with no system change, and passes the suite and budget. By the rule this HBEP set, k3s is the single-user host. The assembled upstream stack stays as the alternative.
- **2026-10-30: fields for the Slurm provider and the lifecycle controller.** Made with HBEP-4 becoming implementable and HBEP-7's design. The kinds gain:
  - on the PlatformProfile, the typed fields that the lifecycle controller and the file staging read (HBEP-4, Platform profile schema). The fields this HBEP listed for the Slurm provider and the deployment ID are unchanged;
  - on the ModelDeployment, `leaseLimit` and `workload` (HBEP-7);
  - on the Instance, its immutable `launch` (HBEP-7);
  - on a managed Provider's status, its catalog (HBEP-4);
  - on the Provider's status, the last 20 observed queue waits (HBEP-4); on the ModelDeployment's status, each class's last measured start-to-ready time; on the Instance's status, the time of its start instruction (HBEP-7). They plan replacements and time-outs, and live in status so that nothing is held only in memory;
  - one verification record per verification, at start and after every restart that reverifies, named `ver-<instance UID>-<generation>`; the Instance's status names the latest (HBEP-7).

  The launcher also creates the agent endpoint's certificate (HBEP-7). HBEP-4 also lists the Allocation's submission parameters. Verification runs as its own controller in the lifecycle controller's manager in alpha (HBEP-7), as Controllers allows.
- **2026-10-31: names and writers from the merged contracts, and a launcher caveat.** Science-agent decisions of 31 Oct 2026, made while the contracts were built (PR #71), and the launcher's finding (PR #49). The names are those on `main`.
  - **The API table** gains, as the code names them: the PlatformProfile's `partitions[].imageFormat` (HBEP-4); the Provider's `queueWaits`, `catalog` and `lastCatalogRead`; the Instance's `endTime`, `restarts` and `agent`, with `agentInstanceID` and `restartCount` (HBEP-7); and the Allocation's annotation `hpcburst.groundnuty.github.io/idle-since`, whose one writer is the lifecycle controller.
  - **Field managers.** `hpcburst-provider` for every Provider's controller, and `hpcburst-lifecycle` for the lifecycle controller, which on a Provider owns only the condition `DialHomeUnreachable`, by server-side apply.
  - **Leases.** Each Provider's is `provider-<name>`, the lifecycle controller's is `lifecycle`, and both live in `hpcburst-system`, which the launcher creates.
  - **Conditions named.** The front check's ModelDeployment condition is `Degraded` with `FrontNotConfigured`; a failed registration sets `DialHomeUnreachable` on the Provider; a refused credentials file sets `CredentialsInvalid`; an identity-record collision sets `IdentityUnresolved` (HBEP-3). Before, the text named only "a condition", and said "admission stops" for a collision.
  - **A caveat for the single-user host.** On a machine whose system k3s runs the same release, a user-level k3s can silently run root's unpacked binaries from `/var/lib/rancher`. The launcher detects this through `/proc/<pid>/exe` and refuses to start the host.
- **2026-10-31: frps and the dial-home certificate** (HBEP-6 becoming implementable; science-agent decisions of 31 Oct 2026).
  - **The launcher starts frps** (Single-user host), as it starts the host: checksum from `build/dependencies.yaml`, a mode-600 configuration file, a record by process ID plus start time, health through a pidfd, its three loopback ports and `/healthz`, and a restart when it exits. This follows the launcher's existing pattern, so nothing in this HBEP contradicts it. Before, the launcher started only the host and the controllers.
  - **The certificate** gains a fixed synthetic DNS name, proposed as `dial-home.hpcburst.invalid`, because frpc verifies the dial-home listener by `serverName` against a trusted file, unlike the agent's pin. Before, it had no stated name.
  - **Ssh mode** uses two forwards, one for the agent's control traffic and one for the tunnel (open question 8). Before, one forward carried the tunnel.
  - **Wording.** Registration goes over the dial-home path, the agent's own connection, never through frp's tunnel, so the text no longer says "registration through the tunnel". The trigger of `DialHomeUnreachable` is unchanged.
  - **Rollback** (Operational readiness 7). Established tunnels survive a stopped control plane; a tunnel that reconnects meanwhile cannot log in until the lifecycle controller, which serves frps's plugin, returns (HBEP-6).
- **2026-10-31: the ssh port, and the hold lift's known risk.** The owner's decision of 31 Oct 2026, recorded on [issue #18, comment 6546958511](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/18#issuecomment-6546958511), with the review round of PR #96.
  - **The ssh port** (open question 8; Single-user host). The ssh dial-home path runs over the configured ssh port, 22 by default, because the development VM's sshd is reached through a port forward on its gateway, at the gateway's address and port (HBEP-6). The launcher checks that the configured address answers on that port. The key's restriction is unchanged. Before, this HBEP named port 22.
  - **`hold-lifted`** (the Allocation's record). "The hold is never lifted again" now carries one exception, HBEP-4's known risk: a leader that stalls after its last re-read can release a later hold of the same job, after an administrator requeued it with a hold.
- **2026-10-31: event retention** (Single-user host; Risks; Test plan; Operational readiness 5). Made with HBEP-8 becoming implementable, from the science agent's decision of 31 Oct 2026; the code agent builds it after PR #88, which edits the launcher's command.
  - **The launcher** passes `--kube-apiserver-arg=event-ttl=168h` to k3s, so the API server keeps events 7 days instead of its default 1 h. k3s passes the flag through and sets none of its own, and kine deletes each event once its TTL has elapsed; HBEP-8 (Retention) cites the sources. kine starts every event's clock again at a host restart, so an event lives at least 7 days. Before, events expired after the default hour.
  - **Store growth:** an estimated 10 MB or less of events over 7 days at alpha scale: about 600 events a day of at most about 2 kB each. Merging seldom applies, since an event's object reference carries its `resourceVersion`, so the estimate counts each emission, with a persisting condition's event repeated at most once an hour (HBEP-8, Retention).
  - **A stuck controller** (Risks). "A stale value raises an event" now names who raises it: a component that does not depend on the stale one, as HBEP-8's observer table assigns. Before, it named no emitter.
  - **Also from HBEP-8** (review round of PR #105). The front renews a heartbeat `Lease`, `front`, in `hpcburst-system`. Accounting records, `acct-<Allocation UID>`, are a third kind of record that outlives deletion. The launcher gives the lifecycle controller's `/metrics` the port base + 12, and `hpcburst status` reads the front's `/healthz`, its usage state included. Before, the `Lease`s were only the leaders', records were identity and verification only, and the launcher's port block and status named none of these.
- **2026-10-31: the front's usage state in `hpcburst status`** (science-agent ruling for #112). A failing usage sink is reported as "degraded: usage writes failing"; before, "unhealthy". The front keeps serving by design.
- **2026-11-01: the controller's health in `hpcburst status`, the partition's `tmpDir`, and the verification record.** Science-agent rulings of 1 Nov 2026, for #111 and from the first real-engine run (issue #94).
  - **Status** (HBEP-8, Observers). The launcher's status reads the lifecycle controller's `/readyz`, which fails until its caches have synced and it leads its `Lease`; `/healthz` stays liveness. A front with no `Lease` is shown as absent and raises no event. Before, the launcher's list named only the front's `/healthz`, and `main`'s status asks each controller for `/healthz` only.
  - **The profile's fields** (API table) gain the partition's `tmpDir`, the job's temporary directory, and `tmpDirFeature`, the node feature it needs, both proposed with HBEP-4's amendment of the same day. Before, the list ended at `imageFormat`.
  - **The verification record** also holds the measured template overhead and the model's `generation_config` defaults from the staged `generation_config.json`, as HBEP-7's record does (HBEP-7, amendments of 31 Oct and 1 Nov 2026). Before, this HBEP's list named neither.
