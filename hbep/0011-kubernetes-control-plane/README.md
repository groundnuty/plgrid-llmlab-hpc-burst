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
- **Admission** that refuses invalid specs before they are stored, such as an unpinned revision (HBEP-9). For alpha it uses CRD schema validation and CEL rules, which spike S3 (PR #36) exercised, plus a ValidatingAdmissionPolicy to guard deletions. Beta adds HBEP-10's three policies (Admission).
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

- **Uninstalling the custom resource definitions deletes every resource of those kinds,** and their finalizers would cancel every provider job. For alpha, a ValidatingAdmissionPolicy refuses deleting the framework's CRDs. Uninstalling removes that policy first, as an explicit step. That step stays a person's, `hpcburst guard remove`, outside HBEP-10's approvals, since no agent may write admission policies (HBEP-10, Identities and roles).
- **The API server is down.** The front keeps serving from its last routing table, marked stale (HBEP-8). Running jobs keep running; Allocations are provider leases with hard ends, and nothing in them depends on the control plane being up.
- **A controller is up but stuck.** Each status carries the generation it last reconciled and when. A stale heartbeat raises an event from a component that does not depend on it (HBEP-8, Observers); a leader `Lease` shows which copy is acting.
- **The control plane's view and the provider disagree,** for example a job that the controller never recorded. On start, controllers list provider jobs that carry the framework's tag and match them to Allocations. Unmatched jobs are reported as orphans and never cancelled without approval.
- **Too heavy for one user.** Measured by S3 for k3s, the single-user host: 363 MiB idle and 407 MiB with 100 objects, ready in 10.5 s cold and 6.0 s warm, against a budget of 600 MiB, 800 MiB and 30 s. S3 measured on a loaded 6-core VM, not on the 2 vCPU, 4 GiB budget VM, so start times there are checked at alpha (Graduation criteria).
- **A host that serves the API but does not garbage-collect.** A bare API server with etcd is ready and answers, yet deletes no dependents (spikes S3 and S4). Every host runs the garbage collector, and a probe checks that it works (Operational readiness).
- **Agents bypass the API** and act on providers directly, outside the inventory and the audit. Mitigation: only the controllers can read the provider credentials; agents act through the API.
- **Kubernetes conventions mislead.** Users expect a deployment to own pods and to restart them. Kinds and docs say plainly that Instances live in provider jobs.
- **An upgrade killed midway can leave k3s that never becomes ready.** Once, after an upgrading launcher was killed with SIGKILL, the new k3s did not become ready within 2 minutes (seen while #134 was built). Mitigation: `hpcburst down` cleans it up, and `hpcburst up` starts again; if it recurs, it gets its own issue.
- **Kind names overlap with Crossplane's.** Crossplane has its own `Provider` kind, for an installed provider package, in another group. Our docs say that crossplane-runtime's `ProviderConfig` maps to our Provider. On a team cluster that also runs Crossplane, tools must name ours fully, as `providers.hpcburst.groundnuty.github.io`.
- **The Allocation embeds crossplane-runtime's managed-resource fields** (S4). A change to them upstream changes our API. Mitigation: crossplane-runtime is pinned, and the alpha API may change; a later API version converts.

### Open questions

1. **The API group and the kinds' names.** Decided: by the owner, 30 Oct 2026. Group `hpcburst.groundnuty.github.io`, version `v1alpha1`, and the kinds Provider, PlatformProfile, ModelDeployment, Allocation and Instance, as HBEP-3 names them. HBEP-10 adds three for beta: Operation, Approval and ApprovalPolicy (API).
2. **The single-user host.** Decided (amended 2026-10-30, after S3's fix, PR #36): k3s as a plain user (`server --rootless --disable-agent`, SQLite). It passed S3's suite and budget with no system change. The assembled upstream stack stays the alternative, and kcp the fallback. Nothing else in this HBEP or in HBEP-3 depends on which one it is.
3. **Where controllers run in the team option:** as pods on the cluster, or on a host that holds the provider credentials. Not needed for alpha: alpha is single-user, and its controllers run on the one host.
4. **The store for HBEP-8's durable log, for usage records and for metrics.** Not needed for alpha: status conditions are the level truth, and Kubernetes Events a best-effort mirror. The candidate for the log is embedded NATS JetStream, outside the API store (spike S8, PR #28). The log is HBEP-8's, at its beta, for events and usage; HBEP-10's subscriptions read it at stable. Metrics are HBEP-8's. **Decided for beta** (amended 2026-11-16): NATS JetStream, embedded in the controller binary's `eventlog` subcommand, a process of its own beside the API host, with its store under the launcher's state directory (Single-user host; HBEP-8, The durable log). The usage files stay the record, and no metrics are stored.
5. **How provider credentials are held.** Decided: for alpha, they are referenced, not stored. A Provider names a mode-600 file on the host, or a Secret in the team option. HBEP-1's content rules apply. Encryption at rest for the team option's Secrets is decided with question 3.
6. **Replicas and leader election.** Not needed for alpha: one replica per controller, restarted by the launcher on failure. Each provider controller still takes its leader `Lease`. Several replicas come with the team option (question 3).
7. **Which evaluated projects become components.** Decided: for the control plane, the Kubernetes API host (question 2), controller-runtime for our controllers, and crossplane-runtime under the Slurm provider (spike S4; HBEP-4). None of the other evaluated projects is a control-plane component; Alternatives says why.
8. **The front's place and reachability.** HBEP-2 (open question 3), HBEP-5 (open question 11) and HBEP-6 (open question 8, and its TLS-terminating front) leave parts of these to this HBEP. Decided for alpha: alpha has one user and one front, on the alpha host. That host must have an address that compute nodes reach, in one of two ways, or run in HBEP-6's host-dialed mode:
   - directly, on the dial-home port;
   - through ssh on the configured ssh port, 22 by default, with a restricted key, over two forwards: one for the agent's control traffic and one for the dial-home tunnel (HBEP-6, Dial-home modes). This is HBEP-6's development path, option B of spike S1 (issue #18).
   - **Amended, 9 Nov 2026:** or by none. A host that compute nodes can't reach runs in HBEP-6's host-dialed mode: it dials each job through the login node. The alpha host runs that way (owner, 9 Nov 2026: "just dial whatever you want from this host").

   A host reachable by neither is out of alpha unless it runs in host-dialed mode. The launcher checks this (Single-user host). Whether several users share a front is HBEP-2's question, and is not decided here.

## Design details

### API

- **Field names are camelCase,** per the Kubernetes API conventions (owner decision, 30 Oct 2026), for example `holdRelease`, `lostGrace`, `imageDigest` and `catalogName`. HBEP-3's canonical `v1` object keeps its own snake_case keys: it is a hash input, not a Kubernetes object, and the deployment-ID library maps the fields to it.

- Group `hpcburst.groundnuty.github.io`, version `v1alpha1` (owner decision, 30 Oct 2026). Each of the five alpha kinds has a status subresource, and so does HBEP-10's Operation. ModelDeployment also has a scale subresource.

| Kind | Scope | Spec holds | Status holds |
|---|---|---|---|
| PlatformProfile | cluster | the platform's kind (managed, batch or static) and its profile data (HBEP-4) | when its facts were last checked |
| Provider | cluster | a PlatformProfile reference; the provider settings (account, grant, allowed partitions, an optional QOS, `qos`, proposed and amended 2026-11-01, and an optional grant end, `grantEnd`, proposed and amended 2026-11-15; HBEP-4); a credentials reference | the time of its last successful status read (`lastSuccessfulStatusRead`); `queueWaits`, the last 20 observed queue waits (HBEP-4); for a managed provider, `catalog` and `lastCatalogRead` (HBEP-4); conditions. The Provider's controller writes all of it, except the condition `DialHomeUnreachable`, which the lifecycle controller applies (Controllers) |
| ModelDeployment | namespaced | the model name, the serving spec (engine flags as ordered `{name, value}` pairs, HBEP-3), the number of instances, the workers per Instance (`workersPerInstance`, beta; HBEP-7, Several workers), the allowed providers, the end time, the default retention policy, the lease limit and workload (HBEP-7), and the routing settings (HBEP-5) | the current deployment ID of each accelerator class, every ID that still has Instances, each class's last measured start-to-ready time (HBEP-7), ready and desired counts, conditions |
| Allocation | namespaced | crossplane-runtime's managed-resource fields, declared by us with crossplane's types and JSON names (not embedded): `providerConfigRef` naming a Provider, immutable; `managementPolicies` defaulting to `[Observe, Create, Delete, LateInitialize]`, the set the provider registers, immutable; `writeConnectionSecretToRef`, refused at admission, since the provider identity has no access to Secrets. Exactly one of `batch` (the submission parameters, never edited; HBEP-4 lists them) or `managed` (a catalog name). The assigned ModelDeployment; the retention policy, optional with no schema default (the controller applies HBEP-7's default, `Keep`, and shows the effective value in status) | a mirror of the job ID and the submission record, for display only (they live in annotations: Ownership, records and deletion); phase, start and hard end; whether its job is completing (`jobCompleting`, proposed and amended 2026-11-01; HBEP-4); duplicates; conditions (S4) |
| Instance | namespaced | the full deployment ID, its identity record, its Allocation and its launch (HBEP-7; in beta with `launch.workers` and `launch.router`), all immutable | the phase (HBEP-3), the time of its start instruction (HBEP-7), the name of its latest verification record and its verdict, its lease end (`leaseEnd`, copied from its Allocation's hard end by the lifecycle controller at every observe; HBEP-7), when it ended (`endTime`), its recent restarts (`restarts`), the agent registration it started under (`agent`, with `agentInstanceID` and `restartCount`; HBEP-7), its healthy workers behind its router (`healthyWorkers`, beta; HBEP-5), each worker's verification state (`workers[]`, beta: the generation last verified, its record and verdict, warmed up, and the generation at which its router admitted it, `routerAdmittedGeneration`; HBEP-7), conditions |
| Operation (beta) | namespaced | one request to lower capacity of one ModelDeployment, planned before it runs; its name is its idempotency key (HBEP-10, Operations): the `action` (`Scale`, `End` with an optional `delete`, or `Release`; `Repurpose` joins with HBEP-7's repurposing), the target and the action's parameters, and `owner`, pinned to the creator at admission. Immutable, except `confirm.planID`, set once by the owner. Removed 7 days after it finishes | the phase; the plan (`status.plan`): each target with its kind, name, UID, generation and effect, whether it needs approval and the policies that apply, the release cost of every target that lowers capacity, the observation time of every fact, and the plan's `id`; the committed targets; and `targets[]`, one outcome per target. The lifecycle controller writes it, except the start estimate, which the Provider's controller fills in (Controllers, Field managers) |
| Approval (beta) | namespaced, in its Operation's namespace | one approver's approval of one plan (HBEP-10, Approvals): `operation`, the Operation's name and UID; `planID`; and `approver`, the approver's user name and groups, pinned to the requester at admission. Immutable | none defined by HBEP-10 |
| ApprovalPolicy (beta) | cluster | who may approve in one scope (HBEP-10, Approval policy): `scope`, exactly one of `global`, a Provider's name, a namespace's name, or a ModelDeployment's namespace and name, one policy per scope; and `approvers`, users, groups and ServiceAccounts. Only persons write it (Single-user host) | none defined by HBEP-10 |
| Workload (beta, proposed) | namespaced | one class of a study's requests (HBEP-3; HBEP-5, Workloads; HBEP-764), named by a DNS-1123 label: `model`, with no `#`; `shape`, the `contextTokens` and `outputTokens` fields of HBEP-7's `workload` block; `sessions`, optional and informational; `routeOrder`, `OwnFirst` by default or `ManagedFirst`; `managedSessions`, at least 1, required with `ManagedFirst`; and `sessionAffinity`, `Route` or absent, with `pinTTL`, a duration from 1 h to 7 days, 12 h by default, and `maxPins`, 1 to 1,000,000, 100,000 by default (beta, proposed; HBEP-5, Sticky routes). ModelDeployments name the ones they serve in `spec.workloads` (HBEP-7). The front lists and watches it, and agents create and update it | none |

- **PlatformProfile schema.** HBEP-4 owns it (its open question 3). For alpha, the fields that the Slurm provider and the deployment ID read are typed. The rest of the profile is kept as an untyped block until HBEP-4 fixes the schema.
  - Per partition: the GPU model and the CPU architecture. Together they are the accelerator class, a deployment-ID input (HBEP-3).
  - `holdRelease` (bool): whether a plain user's `sbatch --hold` is honoured and that user can lift it. It is verified per platform. False runs S4's fallback mode (HBEP-7, open question 11).
  - `lostGrace` (duration, default 2 min): how long a recorded job may be absent from both the queue listing and accounting before it is reported Lost.
  - `inflightWindow` (duration): how long an unresolved submission attempt is waited on. It must exceed the worst remote `sbatch` time, because a local timeout does not stop a remote `sbatch`.
  - `unknownBound` (duration, S4's default 10 min): in fallback mode, how long an Unknown attempt may stay unresolved before an operator is asked.
  - From spikes S4 and S5: the time zone, the requeue default, how long a finished job stays in the queue listing, job-name rules, whether accounting keeps the job comment, and the JSON output version.
  - Read by the lifecycle controller and the file staging, typed by HBEP-4 (amended 2026-10-30): per partition, the longest time limit, the GPUs per node, the CPUs and memory per GPU, the image format (`imageFormat`, `sif` or `sandbox`; amended 2026-10-31), and the job's temporary directory with the node feature it needs (`tmpDir` and `tmpDirFeature`, proposed; amended 2026-11-01), and the partition's own QOS (`qos`, proposed; amended 2026-11-01); the QOS list with each QOS's wall limit, submit limit, per-user caps and `OverPartQOS` flag (`qos`, proposed; amended 2026-11-01); the staging directory; the drain window; an optional idle cap; and the health and restart thresholds. HBEP-4's Platform profile schema gives their names, types and defaults.
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
  - `hpcburst.groundnuty.github.io/queued-successor` (proposed): the queued-successor mark, on an Allocation for a no-overlap successor, or a retry of one, of a primary whose Allocation on the same Provider has not ended (HBEP-7, Handover, No-overlap mode). Its one writer is the lifecycle controller, which writes it once, in the Allocation's create request, so no precondition applies, and never changes or removes it. The Provider's controller only reads it: a marked Allocation adds no queue wait (HBEP-4, Provider status).
  - Status mirrors them for display. Annotations hold the record because they survive a restore without status. crossplane-runtime's critical-annotation updater re-applies every in-memory annotation on a conflict, which could roll the record back, so the provider replaces it with one that re-applies only crossplane's own keys (S4).
  - `hpcburst.groundnuty.github.io/release-cause` (beta, proposed in HBEP-10): why the Allocation is released, as the rule followed, the reason the lifecycle controller already computes, or the Operation's namespace, name and UID. Its one writer is the lifecycle controller, which writes it before each delete of the Allocation, so the audit log holds the cause before the delete (HBEP-10, Audit; HBEP-7, Release).
- **The Allocation's finalizer** cancels, through the provider plugin (HBEP-4), every live job carrying the Allocation's tag: held, pending, running, suspended, or completing (HBEP-4), not only the recorded one. It stays while any job with the tag is live, or while a submission attempt is unresolved (HBEP-7, open question 11). The deletion completes only then.
- **Identity records** (HBEP-3, HBEP-9). Each deployment ID's inputs are stored as a `ControllerRevision`, a built-in Kubernetes kind for immutable snapshots, so no sixth kind is needed. A managed catalog entry's ID gets its record too, in the controllers' namespace, `hpcburst-system`, since it belongs to no namespace and no Provider: the managed catalog controller writes it the first time it sees the entry, before it publishes the entry in the Provider's `status.catalog`, and a failed write leaves the status unchanged. A record whose stored hash differs from the computed one leaves the entry out of `status.catalog`, with a `CatalogEntrySkipped` naming the record (HBEP-3, the collision rule). The release `v0.1.0-alpha.1` writes no managed record, so entries it dropped before an upgrade have none (HBEP-9, the managed entry).
  - Its data holds the canonical `v1` object and the full hash.
  - **One record per ID per namespace, content-addressed.** It is named `id-<first 12 hex of the ID>`. A ModelDeployment that computes an ID creates its record only if it is absent; one that already exists is reused, never rewritten, since the same inputs give the same bytes. A record whose stored hash differs from the one recomputed for its name is a collision or a bug: the controller refuses the new ID, the ModelDeployment gets `Degraded` with reason `IdentityUnresolved`, and an event is raised (HBEP-3).
  - The API server refuses any change to a ControllerRevision's data (Kubernetes' own update validation, [`pkg/apis/apps/validation/validation.go` at v1.36.2](https://github.com/kubernetes/kubernetes/blob/v1.36.2/pkg/apis/apps/validation/validation.go), accessed 2026-10-30).
  - Each Instance's spec names its record and its full ID. A CEL rule (`self == oldSelf`) makes that spec immutable.
- **Verification records.** Each verification of an Instance, at start and after every restart that reverifies, is its own `ControllerRevision`, named `ver-<instance UID>-<generation>` (HBEP-7). It is written once, pass or fail, before the Instance can become ready.
  - Its data holds the resolved settings the Instance reported, its GPU type, its engine fingerprint, the outcome of the file rehash, the measured template overhead and the model's `generation_config` defaults from the staged `generation_config.json`, the sampling values and seeds its checks used, or that they ran greedily (HBEP-7, Instance start; amended 2026-11-01), and the verdict (HBEP-3, HBEP-9).
  - The Instance's status names the latest record and mirrors its verdict.
- **Accounting records.** When an Allocation's last job has ended and no longer carries `COMPLETING` (HBEP-4), the Provider controller writes its accounting as one `ControllerRevision`, named `acct-<Allocation UID>`, in the Allocation's namespace, before the Allocation's finalizer goes (HBEP-8, Provider accounting). It is written once and never rewritten.
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
  - a ModelDeployment's serving spec may change, which rolls out new IDs (HBEP-3);
  - in beta, an Operation's spec is immutable except `confirm`, which may be set once, and an Approval is immutable (HBEP-10).
- **The job ID is not guarded here.** It is an annotation, and a CRD's validation rules reach no metadata beyond the name and generated name ([documentation source](https://github.com/kubernetes/website/blob/main/content/en/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions.md), accessed 2026-10-30). The provider's compare-and-swap writes keep it written once (Ownership, records and deletion).
- **A ValidatingAdmissionPolicy** refuses deleting the framework's CRDs. Such policies are stable since Kubernetes 1.30, and see the existing object on a delete ([documentation source](https://github.com/kubernetes/website/blob/main/content/en/docs/reference/access-authn-authz/validating-admission-policy.md), accessed 2026-10-30).
- **Beta adds three ValidatingAdmissionPolicies** (HBEP-10), written and installed like the CRD guard. A policy sees the whole object and the request's user, which a CRD's own rules cannot reach.
  - **The capacity guard,** `hpcburst-capacity-guard`, with `failurePolicy: Fail`. Its match condition takes every request whose user carries the group `system:serviceaccounts:hpcburst-agents`: every agent identity, and nothing else. It is an allowlist. An agent's update to a ModelDeployment, through the object or its scale subresource, is accepted only if every change in it is a higher `spec.instances`, a later `spec.endTime` or its removal, an edit of `spec.source.served` with `spec.source.managed` still absent, an edit of `spec.workloads` (beta, proposed; it routes only, HBEP-5, Workloads), or an edit of the purpose annotation. An agent's delete of a ModelDeployment is refused; at create, an agent may set any field (HBEP-10, The capacity guard).
  - **The owner pin.** It refuses an agent's create of a ModelDeployment whose owner annotation is not the requester, and any update, from any identity, that changes that annotation. It refuses any create of an Operation whose `spec.owner` is not the requester, and an update that sets `confirm` from anyone but `oldObject.spec.owner` (HBEP-10, Owner and purpose; Confirm).
  - **The approver pin.** It refuses an Approval's create unless `spec.approver` equals the requester's user name and groups (HBEP-10, Approvals).
- **A webhook** is added only for a rule that neither form can express. HBEP-10's gates need none: admission only pins identities and refuses the guarded writes, and the lifecycle controller checks approvals against the state it has just re-read (HBEP-10, Approvals).

### Controllers

- Each controller is a controller-runtime manager, with the keyed work queues and backoff listed in Notes.
- **The Provider reference.** An Allocation's `providerConfigRef` names a cluster-scoped Provider, and the Slurm provider resolves it itself. crossplane-runtime's ProviderConfig usage helpers treat only a kind whose name starts with `Cluster` as cluster-scoped (`pkg/resource/enqueue_handlers.go`, v2.4.3), so the provider does not use them.
- **One manager per provider.** controller-runtime elects a leader per manager, so each provider's controller runs as its own manager with its own `Lease`, `provider-<name>`. The supervisors that start those managers run only in the process that leads `lifecycle` (with `--leader-elect=false`, the one process), so a copy that does not lead `lifecycle` runs no Provider, with credentials and a front endpoint the leader never wrote. A `Lease` does not fence (client-go's `leaderelection.go`, lines 19–20; S4), so two copies can overlap only for one hand-over, which the job tag and the hold already cover (science-agent ruling, 1 Nov 2026, for #132). S4 ran its provider this way. The prefix keeps a Provider named `lifecycle` from sharing the lifecycle controller's `Lease`, `lifecycle`.
- **Every `Lease` lives in the namespace `hpcburst-system`,** which the launcher creates before it starts the controllers (science-agent decision, 31 Oct 2026). Besides the leader `Lease`s, the front renews a heartbeat `Lease` there, `front` (HBEP-8, Observers).
- **Field managers.** Every write names its controller's field manager, so `managedFields` shows who wrote what. `hpcburst-provider` is every Provider's controller: it writes Provider status and keeps every condition it does not own. `hpcburst-lifecycle` is the lifecycle controller, its agent endpoint included. On a Provider it owns only the condition `DialHomeUnreachable`, which it sets by server-side apply to the status, owning that one list entry. Neither removes the other's condition (science-agent decision, 31 Oct 2026; HBEP-4). In beta, each Provider's controller also fills in an Operation's start estimates, `status.estimates[]`, by server-side apply under its own manager, `hpcburst-provider-<provider>`: one shared manager name would let one Provider's apply remove another's entries. It is the one part of an Operation's status the lifecycle controller does not write (HBEP-10, The release cost; HBEP-4, `Estimate`).
- **Alpha runs one replica of each.** The launcher restarts a controller that exits. The `Lease` then guards against a stale second copy, for example one an earlier launcher left running. It does not prevent duplicate jobs, because a deposed leader's submission can still land (S4). In hold mode only the recorded job's hold is ever lifted, so a duplicate stays held and is cancelled (HBEP-7, open question 11).
- The lifecycle and hedging controller (HBEP-7) manages Allocations, including their retention and repurposing. Verification (HBEP-9) and observers (HBEP-8) run as controllers. The front (HBEP-5) watches Instances, ModelDeployments, Providers, PlatformProfiles and, in beta, Workloads to build its routing table. A model name's ModelDeployments are its routing policy (HBEP-5).
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
- verifies the pinned host binaries against their checksums, recorded in the launcher's pins files, `internal/launcher/k3s-pins.txt` and `internal/launcher/frp-pins.txt` (HBEP-15), and starts them;
- binds the API server to loopback, because nothing outside the host talks to it. Instances reach the front, never the API (HBEP-6);
- issues one kubeconfig per identity: the user, each controller, and each agent identity (HBEP-10), with RBAC enabled. Alpha does not yet: every user and controller shares k3s's admin credential, copied into the state directory (`internal/launcher/state.go:35`). In beta (HBEP-10, Identities and roles):
  - **each agent** is a ServiceAccount in the namespace `hpcburst-agents`. `hpcburst identity add <name>` creates it, binds the agent role, requests a token as `kubectl create token` does, and writes a kubeconfig of mode 600 under the launcher's state directory. `hpcburst identity renew <name>` replaces the token, whose life HBEP-10 leans to 30 days;
  - **each controller** has its own ServiceAccount in `hpcburst-system`, so the audit log tells a controller's write from a person's;
  - **the person** keeps the admin kubeconfig as the break-glass credential, never given to an agent;
- in beta, installs three ClusterRoles (HBEP-10, Identities and roles). A namespace's RoleBinding cannot grant cluster-scoped kinds, so their reads are a role of their own:
  - `hpcburst-agent-read`, bound cluster-wide by a ClusterRoleBinding: read Providers, PlatformProfiles and ApprovalPolicies;
  - `hpcburst-agent`, bound per namespace by a RoleBinding in the team option, and cluster-wide on the single-user host: read the namespaced framework kinds, ControllerRevisions, Events and Leases; create and update ModelDeployments, their scale subresource included; create and update Workloads (beta, proposed); create Operations, and update them to confirm; create Approvals;
  - `hpcburst-operator`, bound cluster-wide by a ClusterRoleBinding to persons other than the admin, in the team option: write ApprovalPolicies.

  Agents get no write to Allocations, Instances, Providers, PlatformProfiles, records, ApprovalPolicies, RBAC objects, admission policies or namespaces, and no delete of anything;
- in beta, has `hpcburst up` write the global ApprovalPolicy when none exists, naming the identity of the person's kubeconfig as a SelfSubjectReview reports it. With no policy at all, nothing is approvable (HBEP-10, Approval policy);
- creates the certificate of the lifecycle controller's agent endpoint, which listens on loopback; `agent.json` carries its pin, and in direct mode the front passes `/agent/v1/` through to it on the dial-home port (HBEP-7). The certificate is self-signed and carries a fixed synthetic DNS name, proposed as `dial-home.hpcburst.invalid`, which frpc names as its `serverName` when it verifies the dial-home listener with this certificate as its trusted file (HBEP-6). It is public, and is staged with each Allocation (HBEP-4);
- starts frps, the dial-home tunnel's server (HBEP-6), as it starts the host: it verifies the pinned binary's checksum from `internal/launcher/frp-pins.txt`, writes its configuration file (mode 600, with the password of frps's loopback web server), records it by process ID plus start time, judges its health through a pidfd, its three loopback ports and its `/healthz`, and restarts it when it exits. frps binds loopback only, at offsets of the launcher's port block (HBEP-6, Components);
- in beta, with `up --bulk-path` (proposed), starts a second frps for the bulk path beside the first, at base+13 to base+15, and issues a second certificate, for `bulk.dial-home.hpcburst.invalid`, with the same key, leaving the first unchanged (HBEP-6, The bulk path). It records the choice in its state, and `status`, `down`, `upgrade` and the open-files report cover the second frps;
- in beta's host-dialed mode, with `up --relay-keeper`, starts the relay keeper (`hpcburst-controller relaykeeper`; HBEP-6, The relay keeper) under the controllers' supervisor beside frps, as a sibling of the controller processes. It records the keeper by process ID plus start time, judges its health through a pidfd and its Unix socket, where a `ping` must get `pong` with the keeper's `E` within 1 s (a new check: the supervisor's checks today are a `host:port` `/healthz`), restarts it when it exits, and `down` stops it by that record and keeps its state file. `restore` clears the keeper's accepted `Lease` epochs. The keeper binds no port;
- in beta, with `up --event-log` (proposed; opt-in in beta, its default decided at stable), starts HBEP-8's durable log before the controllers and the front: `hpcburst-controller eventlog`, NATS JetStream embedded, under the controllers' supervisor as a sibling of the controller processes (HBEP-8, The durable log). It:
  - sets each stream's cap from its configuration, 2 GiB per stream by default, which `eventlog` applies at each start. It starts the log only when the free space on the store's file system, less what the streams can still grow and less what the upgrade's copy can still add over the previous one, stays at or above its free-space floor, 15 GB by default (HBEP-8, Retention, In the durable log; the science agent's decisions, 16 Nov 2026);
  - when the space does not allow it, starts the controllers and the front without the log, loudly: `status` reports "degraded: event log not started", naming the free space, the caps and the floor, with the exit code 0. It keeps the recorded `--event-log` choice, checks the space again at each health pass, and starts the log once it fits;
  - writes the log's configuration, mode 600: `sync_interval: always`, the store `<state>/eventlog/` (mode 700), loopback listeners at base+16 for clients and base+17 for monitoring, and a token, which it also writes to `<state>/eventlog/token`, mode 600, for the controllers and the front;
  - records the process by process ID plus start time, judges its health through a pidfd and `GET /healthz` on base+17, and restarts it when it exits;
  - grows the block to base..base+17, keeping base+13 to base+15 for the bulk path whether or not it runs;
  - records the choice in its state and keeps it across `upgrade`. `status` reports the log's health and its streams' sizes, and `down` stops it last;
- with `up --instance-router envoy` (beta), passes the controller's `--instance-router`, recorded in the launcher's state and kept across `upgrade`; `up`'s agent-builds check then requires Envoy's build for each partition's architecture (HBEP-5; HBEP-7, Several workers);
- **reports each tunnel process's open files:** frps, and the front's process, which holds both the client listener and the dial-home listener. Each needs descriptors per tunnel and per stream in flight (HBEP-6, open question 11). Both are Go binaries, whose runtime raises their own soft limit to just below the hard limit at start (Go 1.19 and later), so only the hard limit binds, and it stays the host's to set.
  - `hpcburst status` reports each process's soft and hard limits and its descriptors in use, from `/proc/<pid>/limits` and `/proc/<pid>/fd`. Once use passes 80% of the soft limit as `/proc` reports it, it reports the process as "degraded: open files", with the exit code 0, as for the usage sink.
  - `hpcburst up` warns when a tunnel process's hard limit is below HBEP-6's figure for the front at 300 tunnels × 8 streams (about 5,400 descriptors), naming the limit and the figure;
- installs the CRDs and the admission policies (the CRD guard; in beta also HBEP-10's three, Admission), then starts the controllers and restarts any that exit;
- checks the front's reachability (open question 8). In host-dialed mode it needs no front address. It binds the dial-home listener on loopback and leaves the relays' end-to-end check to the reach rows (#225). Otherwise it warns when no front address and dial-home mode (direct, or ssh on the configured ssh port, 22 by default) are configured, and checks that a configured address answers on that port. While none is configured, the lifecycle controller submits no Allocation, and each ModelDeployment carries `Degraded` with reason `FrontNotConfigured` (decided while building the launcher, PR #49; the name in PR #71). The first Instance on each Provider then confirms reachability from a compute node by registering over its dial-home path. An Instance that cannot register fails at start, and the lifecycle controller sets the condition `DialHomeUnreachable` on its Provider (HBEP-4, open question 16);
- has the API server write its audit log. In alpha every request is logged at the Metadata level, with no request bodies, so a Secret's content is never logged, and three rotated files of 100 MB are kept (`internal/launcher/k3s.go:106-110`). In beta it follows HBEP-10 (Audit):
  - **the policy:** the writes (create, update, patch, delete and deletecollection) of Operations, Approvals and ApprovalPolicies at `RequestResponse` and their reads at `Metadata`, with no `managedFields` in a logged body; the lifecycle controller's writes to Allocations (update, patch, delete and deletecollection; a create stays at `Metadata`) at `Request`, so the release cause is logged before the delete that follows; the controllers' `Lease` renewals and reads in `hpcburst-system` at `None`; everything else at `Metadata`, so a Secret's content is still never logged;
  - **rotation, kept for the grant** (owner, 14 Nov 2026): `--audit-log-maxbackup=0` and `--audit-log-maxage=0`, where 0 means no limit, so no rotated file is removed, and `--audit-log-compress`, so each rotated file is gzipped and only the file being written stays plain. Nothing in the framework deletes an audit file; HBEP-10's `hpcburst audit prune` (stable) deletes one only after a person confirms;
  - **its readers:** whoever may read the launcher's state directory, where the API server writes it (owner, 15 Nov 2026; HBEP-10, open question 10);
- gives the lifecycle controller's `/metrics` the loopback port at its port base + 12 (HBEP-8, Metrics);
- has `hpcburst status` read the front's `/healthz`, and report the front as "degraded: usage writes failing" while its usage sink says `failing`, not as a readiness failure: the exit code stays 0 and the overall health `healthy`, and only the front's line says degraded (HBEP-8, Usage records);
- passes the Hub's base URL from its configuration to the controllers as `HF_ENDPOINT`, default `https://huggingface.co`; the lifecycle controller writes it into each Allocation's `agent.json`, so the identity read and the staging use one Hub (HBEP-7, Instance start; HBEP-4, Files first);
- has `hpcburst status` read the lifecycle controller's `/readyz` for its health. Its two named checks are `caches`, which fails until the informers of every kind the lifecycle controller watches (ModelDeployment, Allocation, Instance, Provider, PlatformProfile and ControllerRevision, and in beta those it follows for HBEP-10's Operations; HBEP-7, Controllers) exist and have synced, and `leading`, which fails while the controller does not lead its `Lease` and always passes under `--leader-elect=false`. The controller starts those informers before its election, and the check looks at each of those kinds' informers itself, since controller-runtime's own cache sync waits only on informers that already exist. So a standby copy reports `caches` passing and `leading` failing, and `status` shows it as "not leading" rather than "starting". controller-runtime would otherwise create them only when the controllers start after election, and `caches` would pass with nothing to wait for. `status` reads each check's own path, never a verbose body, and reports "starting" or "not leading" accordingly. `/healthz` stays its liveness check, and the launcher restarts the controller when it exits. The front is shown as absent only when the host has no front configured; a configured front that is down is unhealthy (HBEP-8, Observers; science-agent rulings, 1 Nov 2026, for #147);
- has the API server keep events 7 days: it passes `--kube-apiserver-arg=event-ttl=168h`, where the default is 1 h. k3s passes the flag through, and kine, its SQLite datastore, deletes each event once the TTL has elapsed. kine starts each event's clock again when the host restarts, so an event lives at least 7 days (HBEP-8, Retention, with the sources);
- backs up the store before every upgrade: an etcd snapshot, or a copy of the SQLite file. With the durable log (beta), the backup also replaces the previous upgrade's copy of `<state>/eventlog/` once the log has stopped: it removes the old copy, then copies the store, so copies never pile up. The launcher refuses the upgrade when the free space, plus the old copy, less the store's size, would fall below the free-space floor, naming the sizes. `restore` puts both back (HBEP-8, The durable log, Backup and restore);
- upgrades the host one minor version at a time, as S3 tested;
- on `down` and `upgrade`, stops the front first and waits for it to exit, then the tunnel processes and the controllers. The front then lets its requests finish for up to 10 s and cuts the rest itself, as HBEP-5 says (a stream with `stream_cut`, detail `shutdown`; the rest with 503 `shutting_down`; each recorded with `cutBy: front_shutdown`), before frps closes their tunnels. With the durable log (beta), it stops the log last, so the front's and the controllers' last events and records reach it.

### Team option

A small k3s cluster. Controllers and the front may run as pods there; models never do. Where controllers run, and with how many replicas, is decided for beta (open questions 3 and 6).

### Credentials

- For alpha, the API holds references to credentials, never credentials. A Provider names a mode-600 file on the control-plane host, or, in the team option, a Secret.
- A Hub token for a gated checkpoint (HBEP-3) is referenced the same way.
- In alpha, "the control plane's secret store" that other HBEPs name means these files, or these Secrets.
- The controller refuses a file that others can read or that its user does not own. It then sets the condition `CredentialsInvalid` on the Provider and makes no provider call.
- No credential is copied into an object, an event, a log or a command line (HBEP-1, Content rules; HBEP-4).

### Durable event log

Not in alpha. Status conditions are the level truth, and Kubernetes Events are a best-effort mirror. In beta, with `up --event-log`, it is NATS JetStream, embedded in a process of its own outside the API store, which the launcher supervises (Single-user host; amended 2026-11-16). It is HBEP-8's (HBEP-8, The durable log): typed events, kept for the grant, and a rolling copy of the latest usage records, while the usage files stay the record. HBEP-10's subscriptions over it come at stable.

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
- **Readiness of a standby.** Run two copies. On the standby, `/readyz/caches` passes only after the informer of every kind the lifecycle controller watches has synced, `/readyz/leading` fails, and `hpcburst status` shows "not leading". With one kind's informer held unsynced through the manager's cache hook, the standby's `caches` fails and `status` shows "starting". A fixture with a single leader proves nothing, since the controllers create the informers after election either way.
- **Lease.** Start a second `hpcburst-controller`: while the first leads `lifecycle`, the second starts no Provider manager and makes no provider call. Kill the first mid-submit: the second takes `lifecycle`, then `provider-<name>`, and finds the job by its tag. Whatever it then submits, at most one job with the tag has its hold lifted, and any other is cancelled (S4).
- **Credentials.** A Provider whose credentials file is readable by others: refused with `CredentialsInvalid`, and no provider call is made.
- **Front reachability.** In host-dialed mode no front address is needed, and `FrontNotConfigured` never fires. Otherwise, with no front address or dial-home mode, the launcher warns and the lifecycle controller submits no Allocation, with `FrontNotConfigured`. An Instance whose agent cannot reach the front fails at start, and its Provider carries `DialHomeUnreachable`. A later write of Provider status by the Provider's controller keeps that condition.
- **Leases and field managers.** Each Provider's `Lease` is `provider-<name>`, the lifecycle controller's is `lifecycle` and the front's heartbeat `Lease` is `front`, all in `hpcburst-system`; a Provider named `lifecycle` shares no `Lease`. Every field a controller writes shows `hpcburst-provider`, `hpcburst-lifecycle` or, for an Operation's estimates, `hpcburst-provider-<provider>` in `managedFields`, and on a Provider `hpcburst-lifecycle` owns only the `DialHomeUnreachable` entry.
- **Another k3s's binaries.** On a machine whose system k3s runs the same release, a user-level k3s that runs a binary outside its own data directory makes `hpcburst up` fail. A check of `/readyz` alone proves nothing here, since a k3s running the borrowed binaries answers it too.
- **Event TTL.** The launcher's k3s command carries `event-ttl=168h`. On a host started with a short test TTL, an event disappears once it has passed; after a host restart, an event created before it is still listed.
- **Upgrade and downgrade.** Upgrade the host one minor version with objects present: every object is kept with its UID. Downgrade by restoring the backup with the earlier binaries: the objects from before the upgrade are back.
- **Store full.** Fill the store: on k3s, a size-limited filesystem holding its data directory; on the assembled stack, etcd's quota. Writes are refused, no controller submits a new job, and the launcher's health reports the write failures (or etcd's alarm).
- **The durable log (beta).** With `up --event-log`, the launcher starts the log before the controllers, restarts it after a SIGKILL, and stops it last on `down`. A configuration whose `sync_interval` is not `always` as parsed makes the log refuse to serve, and `status` reports it unhealthy. A check that reads the configuration file proves nothing, since nats-server takes the default for a setting it did not parse (S8, Finding 8). A client without the token is refused. With both caps at 2 GiB, an empty store and the 15 GB floor, `up` starts the log at 23.6 GB free on the store's file system; at 23.5 GB it starts the control plane without the log, `status` says "degraded: event log not started", and a later health pass starts the log once the space reads 23.6 GB (HBEP-8, Test plan, The caps fit the disk).
- **Upgrade with the log (beta).** The upgrade's backup holds one copy of the log's store beside the SQLite copy, replacing the previous upgrade's; after two upgrades one copy remains. With 18 GB free, a 1 GiB old copy and a 4 GiB store, the upgrade is refused under the 15 GB floor, naming the sizes; with 19 GB it runs. A restore with the earlier binaries puts both back.

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
- The audit log is written at the Metadata level, so it records only metadata for Secrets, never their content. In beta it logs bodies only for HBEP-10's kinds and the lifecycle controller's writes to Allocations, which hold no credentials (Single-user host; HBEP-10, Security).

### Graduation criteria

- **Alpha:**
  - **Alpha's platform** (owner decision, 11 Nov 2026, "Athena alone", recorded on #145 and approved on PR #373): alpha graduates on Athena. Where a criterion below names Helios, or each alpha platform or partition, its Helios part, where not already met, moves to beta, to run when GH200 returns, and its Athena part stays. A row on Athena `plgrid-now` counts for `plgrid-gpu-a100` when it runs one job in total, of one GPU, within `plgrid-now`'s 1 h limit, on the same A100 node type (extending the science-agent ruling of 10 Nov 2026 for #93, which covered conformance). The no-cut handover (HBEP-5, HBEP-7) and S1's one-hour run (HBEP-6) run on `plgrid-gpu-a100`. An Instance on `plgrid-now` uses the shipped test profile `profiles/athena-now.yaml` (HBEP-4, open question 8; HBEP-7, A partition too short).
  - the single-user host (k3s as a plain user) serves the five kinds at `v1alpha1`;
  - the alpha host passes the front-reachability check (open question 8), or, in host-dialed mode, the reach rows show a relay working end to end on each alpha partition, and the alpha platform has `holdRelease` verified, or runs fallback mode;
  - the Test plan passes on it with one batch provider;
  - the host starts within S3's budget on a 2 vCPU, 4 GiB VM.
- **Beta:**
  - the Helios parts of alpha's criteria, on GH200 when it returns (owner decision, 11 Nov 2026);
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
   - **A /proc liveness check can misread a live process** as a zombie while one of its threads is in `execve` (k3s re-executes itself). The launcher decides liveness from a pidfd, which the kernel signals once every thread has exited (PR #49), with one exception: on mainline Linux 6.15 and later, and on the stable kernels that took the backport (6.12.31 and 6.14.9 onwards; a distro kernel of any number may carry it), a lockless `pidfd_poll` can read it as exited for an instant while a non-leader thread runs `execve` (#690). `pidfd_poll` tests `task->exit_state && !delay_group_leader(task)` there (v6.15, `fs/pidfs.c:227`), where the earlier kernels tested `task->exit_state && (thread || thread_group_empty(task))` (the v6.14 tag, `fs/pidfs.c:204`; 6.12.30 and 6.14.8 still do); `de_thread` (v6.15, `fs/exec.c`; lines counted within the function), holding `write_lock_irq(&tasklist_lock)` (line 48) until line 111, hands the PID to the exec'ing thread with `exchange_tids` (line 88) before it sets the old leader's `exit_signal` to -1 (line 100), so a poll that took the old leader before the hand-over and reads `exit_signal` after it sees a zombie that is no longer a delayed group leader. The launcher therefore counts an exit only after confirming a readable poll with `waitid(P_PIDFD, ..., WEXITED|WNOHANG|WNOWAIT)` on the same pidfd. For the launcher's own child that is exact, since `waitid` applies the kernel's wait predicate under `tasklist_lock`, which `de_thread` holds for writing. For a process that is not the launcher's child (one an earlier invocation started), `waitid` answers `ECHILD` (or `EINVAL` on a kernel older than 5.4, where `waitid` takes no pidfd), and one more poll, final, follows: `waitid` has already taken `tasklist_lock` by then, which orders that poll after any hand-over in progress. **That ordering is a stated limit:** it is an implementation detail of the kernel's wait code (`__do_wait` takes `read_lock(&tasklist_lock)` before `do_wait_pid` checks parentage: `kernel/exit.c:1662` at v6.15, `:1580` at v6.8), verified at v6.8, v6.15 and master, and not a documented guarantee. If it ever lapsed, the branch would degrade to a plain re-poll, which is still exact on x86-64. Any other liveness check should read the pidfd the same way; a scan of /proc threads is racy too (L-17.1).
   - **A user-level k3s that runs another k3s's binaries** looks healthy. The launcher checks `/proc/<pid>/exe` against its own data directory (Single-user host; L-17.3).
   - a controller that holds its `Lease` but reconciles nothing. Reconcile times in status show it.
   - a host that is ready but does not garbage-collect. The probe in item 1 shows it.
   - a write that returns an error but took effect. etcd can return a no-space error for a write that still succeeded ([etcd maintenance guide](https://github.com/etcd-io/website/blob/main/content/en/docs/v3.6/op-guide/maintenance.md), accessed 2026-10-30). Controllers re-read after every failed write, and never assume that it did nothing.
4. **Provider limits:** controllers poll providers at rates set in platform profiles (HBEP-4), within login throttles.
5. **Capacity:** the API store holds tens to hundreds of objects, since bulk data stays out. In S3, 100 objects added no measurable memory. Events kept 7 days add an estimated 10 MB or less at alpha scale, since an event for a persisting condition repeats at most once an hour (HBEP-8, Retention).
   - **Storage full.** etcd raises its NOSPACE alarm at its backend quota, and then accepts only reads and deletes ([etcd maintenance guide](https://github.com/etcd-io/website/blob/main/content/en/docs/v3.6/op-guide/maintenance.md), accessed 2026-10-30). With SQLite, a full disk stops writes.
   - Either way, controllers submit nothing new, because no submission starts without a recorded attempt (S4). Running jobs keep running to their hard ends.
   - The launcher's health reports the write failures on k3s, or the alarm on etcd. The host's disk is its own (Placement).
   - In beta the durable log's capped streams share the host's disk: the events stream refuses new events when full, and the usage copy drops its oldest records (HBEP-8, Retention).
6. **Idle cost:** one small host, or about 0.36 GiB of memory on k3s (S3). No grant hours and no provider connections when nothing is deployed. In beta, `--event-log` adds one process, whose memory beta measures.
7. **Rollback:** stopping the control plane leaves provider jobs running until their hard ends, and the front serving from its last table. Established tunnels stay up in direct and ssh modes, since HBEP-6's frps plugin is consulted only at logins and proxy registrations (in host-dialed mode the relays stop with the control plane, and their streams are cut; in beta the relay keeper is part of the control plane and stops with it too); a tunnel that reconnects meanwhile waits for the lifecycle controller to return (HBEP-6, Risks). Removing the framework needs releasing every Allocation first, with approval, and then removing the CRD-deletion policy.

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
| NATS JetStream | [nats-io/nats-server](https://github.com/nats-io/nats-server) v2.15.2 | Apache-2.0 | Dependency in beta, with `up --event-log`: HBEP-8's durable log, embedded in `hpcburst-controller eventlog` (spike S8; HBEP-8, The durable log). Not in alpha |
| frps (frp v0.71.1) | [fatedier/frp](https://github.com/fatedier/frp) | Apache-2.0 | Dependency: the pinned release binary the launcher starts and supervises on loopback, the dial-home tunnel's server (HBEP-6) |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| An owner that owns its replicas; an immutable snapshot of a template as a `ControllerRevision` | Kubernetes ReplicaSet and StatefulSet history | A ModelDeployment owns its Instances; identity records |
| A config object that managed resources reference for credentials and settings | Crossplane's `ProviderConfig` ([crossplane/crossplane](https://github.com/crossplane/crossplane)) | An Allocation's `providerConfigRef` names our Provider |
| Admission rules in CEL, without a webhook server | Kubernetes ValidatingAdmissionPolicy ([documentation](https://kubernetes.io/docs/reference/access-authn-authz/validating-admission-policy/); [its source](https://github.com/kubernetes/website/blob/main/content/en/docs/reference/access-authn-authz/validating-admission-policy.md), accessed 2026-10-30) | The CRD-deletion guard; in beta, HBEP-10's capacity guard and identity pins |
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
- 2026-11-01: the Provider supervisors run only in the `lifecycle` leader's process (for #132), and a risk for an upgrade killed midway (seen for #134).
- 2026-11-01: erratum (with HBEP-15's move to implementable): frp's and k3s's checksums are recorded in the launcher's pins files, `internal/launcher/frp-pins.txt` and `internal/launcher/k3s-pins.txt`, not in `build/dependencies.yaml`, which never existed.
- 2026-11-01: the Provider's optional `qos`, the profile's QOS list and partition QOS (HBEP-4), and the verification record's check sampling values and seeds (HBEP-7), from the amendments of the same day.
- 2026-11-01: the controller's `/readyz` has two named checks, `caches` and `leading`, and an absent front means none configured (rulings for #147).
- 2026-11-01: `/readyz`'s `caches` check covers the informers of the kinds the lifecycle controller watches, which start before the election (ruled for #147).
- 2026-11-01: a completing job keeps the Allocation's finalizer, the `acct-` record waits until its flag clears, and the Allocation's status gains `jobCompleting` (HBEP-4's amendment of the same day, for #198).
- 2026-11-01: the Allocation's `queued-successor` mark, written once by the lifecycle controller in the create request and read by the Provider's controller (HBEP-7's and HBEP-4's amendments of the same day, for #195).
- 2026-11-09: open question 8 adds HBEP-6's host-dialed mode for a host that compute nodes can't reach.
- 2026-11-10: in beta's host-dialed mode the launcher supervises the relay keeper (HBEP-6).
- 2026-11-10: the relay keeper's opt-in and health check.
- 2026-11-11: alpha graduates on Athena alone; Helios's parts move to beta (owner decision).
- 2026-11-12: `status` reports frps's and the front's open files, and `up` warns on a low hard limit (from HBEP-6, open question 11).
- 2026-11-12: several workers per Instance, for beta (HBEP-7, Several workers).
- 2026-11-12: the bulk path, open question 12's contract (HBEP-6, The bulk path).
- 2026-11-13: erratum: the Instance's `status.workers[]` lists `routerAdmittedGeneration`, where the router's admission of a worker is kept (the science agent's ruling on #439, on Q2's state); it records where an already-ruled gate keeps its state and changes no behaviour.
- 2026-11-14: `down` and `upgrade` stop the front before the tunnels and the controllers (for #552).
- 2026-11-14: a managed catalog entry's ID has no identity record in alpha (for HBEP-9's implementable PR).
- 2026-11-14: a managed catalog entry's ID gets its identity record in `hpcburst-system` (for #574).
- 2026-11-15: amendment following HBEP-10 (PR #582): its three kinds; its three admission policies, with no webhook; agent and controller identities, the roles and the global ApprovalPolicy; the audit policy and rotation; the release cause; the Provider's grant end; the start estimate's writer; removing the CRD guard stays a person's step.
- 2026-11-16: with `up --event-log`, the launcher supervises HBEP-8's durable log, and the upgrade's backup copies its store (HBEP-8's amendment of the same day).
- 2026-11-16: revised by its Fable review (PR #615): the upgrade keeps one copy of the log's store, under the free-space floor, and a log refused for space leaves the control plane running without it, loudly, until it fits.
- 2026-11-17: erratum (#690): Operational readiness 3 said the kernel signals a pidfd only once every thread has exited. On mainline 6.15 and later, and on stable kernels that took the backport (6.12.31 and 6.14.9 onwards), it can read as exited for an instant during a non-leader `execve`; the launcher confirms a readable pidfd with `waitid` on it (and, for a process that is not its child, one re-poll, which relies on `waitid` having taken `tasklist_lock`: a stated limit) before it counts an exit. The intent, deciding liveness from the pidfd and never from /proc, is unchanged.
- 2026-11-17: erratum (the science agent's S3 ruling on #660, 17 Nov 2026): HBEP-10 left the audit policy to be fixed by the measurement of the log's size, and S3's measurement decided it: HBEP-10's kinds are logged at `RequestResponse` for their writes and at `Metadata` for their reads, with no `managedFields` in a logged body, and the "writes" to Allocations are update, patch, delete and deletecollection, so an Allocation's create stays at `Metadata`; the controllers' `Lease` renewals and reads are at `None` in `hpcburst-system` alone (HBEP-10, Audit).
- 2026-11-18: erratum (the ruling on the review of #748, point 5, 18 Nov 2026): an Allocation's `writeConnectionSecretToRef`, which this HBEP says it has none of and which stayed in the type for crossplane's wire format, is refused at admission, since the provider identity has no access to Secrets (HBEP-10, Audit; #660). With it set, crossplane-runtime's managed reconciler would publish a Secret on every pass, through a cluster-wide Secret informer that the identity's role cannot sync, so that Allocation would never be reconciled and the identity would be refused (403) on Secrets until it was deleted. The Allocation CRD gains a rule that no Allocation names one, with its test, its mutation row and `internal/rbacscan`'s pin. No Secret is granted.
- 2026-11-17: erratum (the science agent's rulings on #661): the estimates' field manager is one per Provider, `hpcburst-provider-<provider>`.
- 2026-11-18: amendment from HBEP-764: the Workload kind.
- 2026-11-19: amendment from HBEP-764: a Workload's `sessionAffinity` and `pinTTL`.

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
  - **The launcher starts frps** (Single-user host), as it starts the host: checksum from `internal/launcher/frp-pins.txt`, a mode-600 configuration file, a record by process ID plus start time, health through a pidfd, its three loopback ports and `/healthz`, and a restart when it exits. This follows the launcher's existing pattern, so nothing in this HBEP contradicts it. Before, the launcher started only the host and the controllers.
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
- **2026-11-01: the Provider supervisors follow the `lifecycle` leader** (science-agent ruling for #132). The supervisors that start each Provider's manager run only in the process that leads `lifecycle`; each Provider still has its own manager and `Lease`. Before, they ignored the `lifecycle` `Lease`, so two copies could split; since a `Lease` does not fence, they can still overlap for one hand-over. The `Lease` test now starts a second `hpcburst-controller`. PR #132 merges after this amendment. Also a short risk: an upgrade killed midway once left k3s never ready, which `down` and `up` recover.
- **2026-11-01: QOS limits and the checks' sampling** (API; Ownership, records and deletion, Verification records). Made with the HBEP-4 and HBEP-7 amendments of the same day; the new names are proposed there.
  - **The Provider's spec** gains an optional `qos`, the QOS its jobs are submitted under, which the plugin sends as `--qos`. Before, the provider settings were the account, the grant and the allowed partitions only.
  - **The profile's fields** gain the partition's own QOS (`partitions[].qos`) and the QOS list (`qos`), with each QOS's wall limit, submit limit, per-user caps on GPUs, CPUs and memory, and whether it carries `OverPartQOS`. Before, QOS facts sat only in the untyped block.
  - **The verification record** also holds the sampling values and seeds of the checks, or that they ran greedily, since a reasoning model's checks now sample with its own `generation_config.json` values (HBEP-7). Before, it held the file's defaults but not what the checks sent.
- **2026-11-01: `/readyz`'s checks and an absent front** (science-agent rulings for #147). The lifecycle controller's `/readyz` has two named checks, `caches` and `leading`, which `hpcburst status` reads by path. The front is absent only when none is configured; a configured front that is down is unhealthy. Before, the text named no checks, and a front that was not running read as absent.
- **2026-11-01: what `caches` waits for** (science-agent ruling for #147). The `caches` check passes only once the informers of every kind the lifecycle controller watches (ModelDeployment, Allocation, Instance, Provider, PlatformProfile and ControllerRevision) exist and have synced, checked kind by kind. The Provider managers' informers are not part of it, since those managers run only in the leader's process. They start before the election, so a standby copy shows "not leading" and a take-over finds its caches warm; a standby watches those kinds in the meantime. Before, the text did not say which caches, and controller-runtime's lazy informers would have let `caches` pass with nothing synced.
- **2026-11-01: a completing job is live** (API; Ownership, records and deletion: the Allocation's finalizer, Accounting records). Made with HBEP-4's amendment of the same day, for #198, from PR #200's review round. A job whose `job_state` list still carries `COMPLETING` keeps the Allocation's finalizer, and the Allocation's accounting record is written only once the flag clears. The Allocation's status gains `jobCompleting` (proposed in HBEP-4). Before, the finalizer's list of live states ended at suspended, and "has ended" read as the base state alone.
- **2026-11-01: the queued-successor mark** (Ownership, records and deletion, The Allocation's record). Made with HBEP-7's and HBEP-4's amendments of the same day, from the science agent's rulings for #195, settled in PR #205's review; the name is proposed. The Allocation's annotations gain `hpcburst.groundnuty.github.io/queued-successor`. The lifecycle controller writes it once, in the create request, and never changes it; the Provider's controller only reads it, and adds no queue wait for a marked Allocation. Before, no annotation of the record was set at creation.
- **2026-11-09: a host compute nodes can't reach** (Open questions 8; Single-user host; Test plan; Graduation criteria; Operational readiness). An amendment with HBEP-6's host-dialed mode, settled in PR #298's review.
  - **Reachability:** the alpha host dials its jobs through the login node, so it needs no address that compute nodes reach, and `FrontNotConfigured` doesn't fire in that mode. The launcher binds the dial-home listener on loopback and allows one host-dialed Provider per login host.
  - **Restarts:** stopping the control plane cuts host-dialed streams.

  Before, such a host was out of alpha.
- **2026-11-10: the relay keeper** (Single-user host; Operational readiness 7). An amendment with HBEP-6's of the same day. In beta's host-dialed mode the launcher starts, records, health-checks and restarts the relay keeper beside frps, and `down` stops it by its record. Before, the launcher supervised no relay process.
- **2026-11-10: the relay keeper's opt-in and health** (Single-user host). An amendment with HBEP-6's of the same day. The keeper starts only with `up --relay-keeper`, as the controller binary's `relaykeeper` subcommand, and its health is a live pidfd plus a `ping` answered within 1 s. Before, PR #339 named neither.
- **2026-11-11: alpha graduates on Athena alone** (Graduation criteria). An amendment by the owner's decision of 11 Nov 2026 ("Athena alone", recorded on #145 and approved on PR #373), with Helios GH200 down and no return date. Alpha's criteria now bind Athena, and their Helios parts not already met move to beta, to run when GH200 returns. Already met: a relay on Athena `plgrid-now` (10 Nov 2026, #305). A row on `plgrid-now` counts for `plgrid-gpu-a100` when it runs one job of one GPU within 1 h; the no-cut handover and S1's hour stay on `plgrid-gpu-a100`. Before, alpha needed both Helios and Athena.
- **2026-11-12: open files for the tunnel processes** (Single-user host). An amendment from HBEP-6's open question 11 (spikes S14 and S16): the front's process needs two descriptors per tunnel and two per stream in flight, about 5,400 at 300 tunnels × 8 streams, and frps one of each. Go's runtime already raises each process's soft limit to its hard limit, so the hard limit binds. `hpcburst status` now reports their use, degraded past 80%, and `up` warns when a hard limit is below the front's figure. Before, nothing reported their use or checked the hard limit.
- **2026-11-12: several workers per Instance, for beta** (API). An amendment, the science agent's design, with HBEP-7's Several workers section as its one home: the ModelDeployment gains `workersPerInstance`; the Instance's launch gains `workers` and `router`, immutable; and the Instance's status gains `healthyWorkers` and `workers[]`, all beta. `status.restarts` keeps up to 32 entries, and its part names admit `router`. Before, none of these existed.
- **2026-11-12: the bulk path** (Single-user host). An amendment, the science agent's design: `up --bulk-path` starts and supervises a second frps for the bulk path and issues a second certificate for the bulk name with the same key (HBEP-6, The bulk path). Before, the launcher ran one frps.
- **2026-11-12: the router's opt-in** (Single-user host). An amendment by the science agent's ruling on #454: `up --instance-router envoy` passes the controller's `--instance-router`, recorded in the launcher's state and kept across `upgrade`, and `up`'s agent-builds check then requires Envoy's build for each partition's architecture (HBEP-5; HBEP-7, Several workers). Before, no launcher option set the controller's `--instance-router`.
- **2026-11-14: the front stops first** (Single-user host). An amendment by the science agent's ruling on #552's plan: on `down` and `upgrade`, the launcher stops the front and waits for it before stopping frps and the controllers, so a stream in flight ends as HBEP-5's front shutdown says, with `cutBy: front_shutdown`. #552 found the supervisor stopping every child at once, so frps exited first and the stream ended as a tunnel cut. Before, the text set no order.
- **2026-11-14: managed entries have no identity record in alpha** (API, Identity records). An amendment from the review round of HBEP-9's implementable PR (#555): the code writes identity records for a served deployment's IDs only, never for a managed catalog entry's (`internal/lifecycle/identity.go:375`). The text now says so, and HBEP-9's beta criteria close the gap. Before, the text said each deployment ID's inputs are stored.
- **2026-11-14: managed identity records** (API, Identity records). An amendment by the science agent's rulings on #574's plan: a managed catalog entry's ID gets an immutable identity record in `hpcburst-system`, written before the entry is published, with a collision leaving the entry out of the catalog. Before, the alpha exception said a managed ID had no record.
- **2026-11-15: HBEP-10's kinds, identities, admission and audit** (Open questions 1; Risks; API; Ownership, records and deletion; Admission; Controllers, Field managers; Single-user host; Security; Prior art). An amendment following HBEP-10, made implementable by PR #582's review round, merged before the code of HBEP-10's beta.
  - **The API** gains, for beta, Operation and Approval, namespaced, and ApprovalPolicy, cluster-scoped, as HBEP-10's Design details define them; the Provider's optional `grantEnd` (proposed; HBEP-4, Provider settings); and the Allocation's `release-cause` annotation, whose one writer is the lifecycle controller, before each delete.
  - **Admission** gains three ValidatingAdmissionPolicies: the capacity guard, an allowlist that matches agent identities only; the owner pin; and the approver pin. HBEP-10's gates need no webhook, since the lifecycle controller checks approvals against the state it has just re-read.
  - **The single-user host** gives each agent a ServiceAccount in `hpcburst-agents` and a kubeconfig of mode 600 (`hpcburst identity add|renew`), and each controller a ServiceAccount in `hpcburst-system`; installs the roles `hpcburst-agent-read`, `hpcburst-agent` and `hpcburst-operator`; writes the global ApprovalPolicy at `up`; and logs HBEP-10's kinds at `RequestResponse` and the lifecycle controller's Allocation writes at `Request`, keeping every rotated audit file, gzipped, for the grant.
  - **`hpcburst-provider`** fills in an Operation's start estimate (HBEP-4, `Estimate`).
  - **Removing the CRD guard** stays a person's explicit step, `hpcburst guard remove`, outside HBEP-10's approvals, since no agent may write admission policies (HBEP-10's lean, approved by its review round).

  Before, the API had five kinds, a webhook was expected for HBEP-10's approval gates after alpha, the launcher's identities were named without how they are issued, the audit log was Metadata only with no stated rotation, and an approval for removing the CRD guard was to come with HBEP-10.
- **2026-11-16: HBEP-8's durable log** (Open questions 4; Single-user host; Durable event log; Test plan; Operational readiness 5 and 6; Prior art). An amendment with HBEP-8's of the same day, merged before the code. With `up --event-log` (proposed; opt-in in beta, its default decided at stable), the launcher starts and supervises `hpcburst-controller eventlog`, NATS JetStream embedded, before the controllers and the front. It caps each stream at 2 GiB by default, and starts the log only when the caps and the upgrade's one copy fit above a 15 GB floor; otherwise it starts the control plane without the log, loudly, and starts the log once the space allows (the science agent's decisions, 16 Nov 2026, as sizing, and on the Fable review of PR #615). It writes a mode-600 configuration with `sync_interval: always` and a token, keeps the store under `<state>/eventlog/`, binds loopback ports at base+16 and base+17, judges health by a pidfd and `/healthz`, restarts the log when it exits, and stops it after everything else. The upgrade's backup replaces its previous copy of the log's store beside the SQLite file, under the same floor, and `restore` puts both back. Before, the log was a candidate, the launcher ran none, and the block ended at base+12, or base+15 with the bulk path.
- **2026-11-18: the Workload kind** (API; the capacity guard; the agent role; the front's watches). An amendment from HBEP-764 and the owner's decision of 18 Nov 2026 to declare workloads as a new resource kind, revised by its review round (PR #769). A namespaced Workload holds one class of a study's requests, which ModelDeployments name in `spec.workloads`. The front watches Workloads, agents create and update them, and the capacity guard accepts an agent's edit of `spec.workloads`, which routes only. Before, the API had no kind for a study's workloads.
- **2026-11-19: a Workload's sticky routes** (API). An amendment from HBEP-764's R9: a Workload gains `sessionAffinity` (`Route` or absent), `pinTTL` (1 h to 7 days, 12 h by default) and `maxPins` (100,000 by default), for HBEP-5's sticky routes. Before, a Workload had no session affinity field.
