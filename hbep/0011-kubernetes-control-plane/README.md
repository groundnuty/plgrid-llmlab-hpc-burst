# HBEP-11: Kubernetes as the control plane

## Summary

The framework's control plane is the Kubernetes API: custom resources for the resource model of HBEP-3, and controllers that reconcile them against providers. Kubernetes is not the compute layer. Models run in provider jobs (Slurm jobs, managed catalog entries), never in pods. This HBEP decides what the framework takes from Kubernetes, what it must build itself, and how the control plane is deployed for a team and for a single user.

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
- Evaluating existing projects. That is a later step (see Open questions).

## Proposal

### User stories

- A team runs the control plane on a small k3s cluster. Each agent has its own identity, and the audit log shows which agent cancelled which Allocation.
- A single researcher runs one binary on a workstation or small VM. It bundles the API server, its storage, the controllers and the front.
- An agent lists everything it owns with one label selector. Deleting its ModelDeployment drains its Instances without replacement (HBEP-3) and cancels its pending Allocations. Each started Allocation is released or kept as its retention policy says (HBEP-7); a released Allocation's provider job is cancelled before its object disappears.
- An agent selects every Provider on one platform by its platform label, two grants included, and attaches them to a model name in routing policy (HBEP-5).
- The lifecycle controller crashes during a handover. On restart it re-reads every resource and every provider's job list, and continues from what it observes.

### Notes, constraints and caveats

What the framework takes from Kubernetes:
- **Level-triggered reconciliation.** Controllers compare desired and observed state on every pass. A missed event is corrected on the next pass, and a restarted controller re-reads everything.
- **Owner references and garbage collection.** Deleting an owner deletes what it owns. Finalizers make a deletion wait until the provider job is cancelled.
- **Labels and selectors** for the inventory, for pools and for bulk actions, and for providers: organization, platform, partition and account are labels on each Provider (HBEP-3).
- **Watches,** so the front and agents follow changes without polling, resuming from a resource version.
- **RBAC** per identity, verb, resource type and namespace.
- **Audit** of every API call by the API server.
- **Admission webhooks** that refuse invalid specs before they are stored, such as an unpinned revision (HBEP-9).
  - Candidate, depending on HBEP-3's open question 7: refuse an edit to an identity field of an existing ModelDeployment, if a changed spec must be a new ModelDeployment. If it is a rollout inside one instead, the edit is allowed and rolls out to a new deployment ID.
  - Either way, the identity record behind an existing deployment ID is never edited (HBEP-9).
- Also: server-side dry run, optimistic concurrency on resource versions, a status subresource with conditions, API versions with conversion, and client libraries.
- **Controller concurrency follows Kubernetes practice:**
  - keyed work queues with several workers per controller;
  - one key never processed by two workers at once;
  - idempotent, level-triggered reconciles;
  - rate-limited backoff on failure;
  - providers as independent shards (HBEP-7, open question 11).

What it does not solve:
- **Provider facts.** Start estimates, hard ends, queue state and billing come from provider plugins (HBEP-4). Kubernetes has no notion of a lease with an unknown start and a hard end.
- **Hedging across providers.** Our own controller does it (HBEP-7).
- **Reachability** of compute nodes (HBEP-6).
- **Guaranteed event delivery.** Kubernetes Events are best-effort and expire, after one hour by default. A watch cannot resume from a resource version that has been compacted; the client must list again. So HBEP-10's subscriptions need their own durable log, and status conditions are the level truth.
- **Bulk data.** Metrics, usage records and logs (HBEP-8) do not belong in the API store, whose objects are limited to about 1.5 MiB.
- **Approvals.** RBAC says who may act, not whether an action was approved. Approvals are our own resources (HBEP-10).
- **Actions on providers.** The API server's audit sees calls to it, not a controller's submission or cancellation on Slurm. Controllers record those, tied to the resource that caused them.
- **Secrets at rest.** Kubernetes Secrets are not encrypted in storage unless encryption at rest is configured. Provider credentials need it, or an external store.
- **Its own operation.** The API server needs a host, backups and upgrades.

Deployment options:
- **A small cluster,** for example k3s, for a team. Controllers and the front may run as pods there; models never do.
- **A control plane without nodes:** an API server with its storage, plus our controllers as plain processes. Garbage collection runs in the Kubernetes controller manager, so that runs too.
- **One bundled binary** for a single user: API server, storage, controllers and front in one process. k3s shows that one binary with an embedded SQLite store is practical.
- **Placement.** The control plane gets a host of its own, apart from other teams' services: in the past project, a full disk on a shared node evicted another team's workloads.

### Risks and mitigations

- **Uninstalling the custom resource definitions deletes every resource of those kinds,** and their finalizers would cancel every provider job. Lean: admission guards deletion of definitions and namespaces, which then needs an approval (HBEP-10).
- **The API server is down.** The front keeps serving from its last routing table, marked stale (HBEP-8). Running jobs keep running; Allocations are provider leases with hard ends, and nothing in them depends on the control plane being up.
- **A controller is up but stuck.** Each status carries the generation it last reconciled and when. A stale value raises an event; a leader `Lease` shows which replica is acting.
- **The control plane's view and the provider disagree,** for example a job that the controller never recorded. On start, controllers list provider jobs that carry the framework's tag and match them to Allocations. Unmatched jobs are reported as orphans and never cancelled without approval.
- **Too heavy for one user.** If the bundled binary is too large or slow to start on a small VM, the single-user option fails. A spike measures it (see Open questions).
- **Agents bypass the API** and act on providers directly, outside the inventory and the audit. Lean: the controllers hold the provider credentials, and agents do not.
- **Kubernetes conventions mislead.** Users expect a deployment to own pods and to restart them. Kinds and docs say plainly that Instances live in provider jobs.

### Open questions

- The API group and the kinds' names. They are hard to change, so the owner decides. HBEP-3 names the resources.
- The default for a single user: a control plane without nodes, or one bundled binary. A spike runs both on a small VM and measures memory, start-up time and upgrade steps.
- Where controllers run in the team option: as pods on the cluster, or on a host that holds the provider credentials.
- The store for HBEP-10's durable event log, for usage records and for metrics: outside the API store, but which.
- How provider credentials are held: Secrets with encryption at rest, or an external secret store.
- Whether controllers run with several replicas and leader election, or as one replica restarted on failure.
- Later evaluation of existing projects, which may change this HBEP or become components. Not evaluated here:
  - interLink and other virtual-kubelet offloading, which aim to run pods on remote systems such as batch clusters;
  - Slinky and slurm-bridge, which aim to join Slurm and Kubernetes scheduling;
  - KubeAI, which aims to serve models on Kubernetes;
  - the vLLM production stack, which aims to deploy vLLM with routing on Kubernetes;
  - the Gateway API Inference Extension, which aims at model-aware routing through Kubernetes gateways;
  - llm-d, which aims at distributed LLM serving on Kubernetes.

## Design details

Decided so far, as a sketch:
- HBEP-3's Provider, ModelDeployment, Allocation and Instance are custom resources.
- A Provider's name is the operator's choice. Its labels carry organization, platform, partition and account, and its spec holds the provider settings, with credentials by reference (HBEP-4).
- Lean: a ModelDeployment owns its Instances, as a ReplicaSet owns its pods, and each Instance is bound to one Allocation, as a pod to a node. No ModelDeployment owns an Allocation, because an Allocation can outlive its Instances and host another deployment's (HBEP-3); its retention policy decides when it is released (HBEP-7). A pending Allocation records the deployment that requested it and is cancelled when that deployment ends. HBEP-3 decides.
- An Allocation's finalizer cancels its provider job through the provider plugin (HBEP-4), and the deletion completes only when the provider confirms.
- The lifecycle and hedging controller (HBEP-7) manages Allocations, including their retention and repurposing. Verification (HBEP-9) and observers (HBEP-8) run as controllers. The front (HBEP-5) watches Instances to build its routing table.
- Desired state is in each spec; observed state, with observation times, is in each status.

Everything else is TBD.

### Test plan

- Kill the lifecycle controller mid-handover: on restart it continues, and no second job is submitted.
- Delete a ModelDeployment: its Instances drain, no replacement is submitted, its pending Allocations are cancelled, and each started Allocation is released or kept as its retention policy says. A released Allocation's provider job is cancelled before its object disappears.
- Select Providers by the platform label: every provider on that platform is returned, two grants included.
- Delete a custom resource definition without approval: refused.
- Stop the API server: the front keeps serving from its last table and marks it stale.
- Let a watch fall behind compaction: the client lists again and loses no state.
- Submit a spec with an unpinned revision: admission refuses it.
- An agent's identity tries to cancel another owner's Allocation without the right role: refused, and the audit log shows the attempt.
- Start the controllers beside a provider job with the framework's tag and no Allocation: it is reported as an orphan, not cancelled.

### Compatibility and upgrades

- Custom resources are versioned from the start, with conversion between versions.
- Controllers and the front may run different versions during an upgrade; each must accept the resource versions the other writes. TBD.
- Downgrade: TBD. A downgraded controller must ignore status fields it does not know.

### Security

- Provider credentials are readable only by the controllers that use them.
- Each agent and each person has its own identity; RBAC limits each to its verbs and namespaces.
- No prompt or response is stored in the API store. Requests never pass through it.
- The audit policy records only metadata for Secrets, never their content.

### Graduation criteria

TBD. Lean: alpha with one deployment option and one batch provider; beta when the Test plan passes on both the team and the single-user options.

## Operational readiness

1. **Working:** the API server's health endpoint answers, each controller's leader `Lease` is current, and each resource's status shows a recent reconcile. A status older than its controller's resync period says a controller is not working.
2. **Restart:** controllers re-list resources and provider jobs, and continue from observed state. Nothing is held only in memory.
3. **Silent failure:** a controller that holds its `Lease` but reconciles nothing. Reconcile times in status show it.
4. **Provider limits:** controllers poll providers at rates set in platform profiles (HBEP-4), within login throttles.
5. **Capacity:** the API store holds tens to hundreds of objects, since bulk data stays out. Storage full: TBD.
6. **Idle cost:** one small host or one process; no grant hours and no provider connections when nothing is deployed.
7. **Rollback:** stopping the control plane leaves provider jobs running until their hard ends, and the front serving from its last table. Removing the framework needs releasing every Allocation first, with approval.

## Drawbacks

- Kubernetes is a large dependency for a framework whose compute runs elsewhere.
- Users unfamiliar with Kubernetes face its concepts even in the single-user option.

## Alternatives

- **Our own API and database.** Lighter for one user, with full control over event semantics. We would rebuild watches, RBAC, audit, garbage collection, admission and versioning, and agents would lose standard tooling.
- **Virtual-kubelet pods on Slurm.** Models as pods on a virtual node that submits Slurm jobs, managed by standard workload controllers. Rejected for the compute layer: squeezing leases into pods hides the facts the scheduler needs. A pod has no start estimate or hard end the controllers can plan with, and one pod cannot be submitted to several providers and the losers cancelled. A rolling update with `maxSurge` does start a replacement before its predecessor ends, but it cannot plan that from a lease end it does not see. It may still suit auxiliary parts; see the later evaluation in Open questions.

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #11.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).

## Amendments

None.
