# HBEP-10: Agent primitives

## Summary

Coordination stays with agents: the framework does not decide whose work matters more. It gives agents, and the people they act for, a small set of primitives that make shared resources safe to operate:
- one inventory query;
- owner leases, which carry an owner and a purpose;
- reservations;
- event subscriptions with guaranteed delivery;
- dry run and explicit confirmation;
- approvals for irreversible actions;
- idempotency keys;
- reports of partial failure for operations across providers;
- an audit log;
- stable names.

This HBEP decides what each primitive guarantees, and builds each on the Kubernetes API (HBEP-11) wherever the API already has the mechanism. Beta builds first what guards against an eager release: an identity per agent, an approval policy, Operations that plan a release, show what it costs and run only once confirmed and approved, and an audit trail kept for the grant. Subscriptions, owner-lease expiry, reservations and orphans in the inventory come after, at stable. A tool server for language-model agents is later work (owner, 14 Nov 2026).

## Motivation

Agents act at stage 2 (get resources), stage 3 (keep instances running) and stage 7 (pay and give back) of the user story in HBEP-2, and read at stages 4 and 6.

**Releases are the costly mistake** (owner, 14 Nov 2026, [issue #10](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/10)). Agents that operated HPC capacity released it too eagerly, without weighing that getting it back can take days. They worked as on Kubernetes, where capacity is fixed and work can be scheduled whenever it is wanted. A Slurm queue is not like that: a freed slot goes to another user at once, and a resubmission starts with no queue age and a multi-day estimate (L-1.2). So the tool shows what a release costs before it runs, and guards it.

A past project ran a fleet of agents against several Slurm clusters, sharing one Unix user and hand-made state files. Its failures were coordination failures more than serving failures:
- No inventory said who had started what across hosts and clusters. Our own jobs competed with each other for nodes.
- Every agent ran as one Unix user, so jobs, processes and scheduled entries could not be attributed.
- Kill by pattern killed the wrong processes, including other agents' work. Recorded process IDs pointed at a parent or at another agent's process.
- A dry check ran for real: a refusal test wrote a real hold, and a defaulted empty key turned a "dry" run into a live one.
- An interrupted command across three clusters was reported as rejected, while two of its parts had already cancelled jobs before the owner chose the scope.
- A directive relayed by another agent was taken as the owner's decision. A conditional go was acted on while its condition was still open.
- A request channel went unwatched for long stretches. Messages addressed to a session were lost when the session restarted.
- Agents failed mid-operation from exhausted context, and their successors could not tell what had been done.
- Clean-up by keyword sweep missed many tunnels and loops, and a scheduled respawner revived killed processes the next day.
- Clients did not coordinate their load: two runners each kept to their own budget and together doubled the load on a shared gateway.

### Goals

- **Inventory:** one query returns every resource the framework holds, with owner, purpose, state and provider facts, across all providers.
- **Owner leases:** nothing the framework starts is anonymous. Every resource has an owner identity, a purpose and an expiry the owner renews.
- **Reservations:** an owner can book capacity for a window, and the framework honours the booking against other clients.
- **Subscriptions:** an agent receives every event it subscribed to, in order per resource, across restarts of both sides, or learns explicitly that it missed some.
- **Dry run and confirm:** every operation that lowers capacity can be planned without side effects, and executed only as planned.
- **Approvals:** an irreversible action runs only with an approval recorded from an approver's own identity.
- **Release cost:** before an irreversible action, its actor sees what it gives up and what getting the capacity back costs.
- **Idempotency:** a retried operation, by the same agent or a successor, does its work once.
- **Partial failure:** an operation over several targets reports each target's outcome.
- **Audit:** every action records who asked, for whom, what was planned, who approved it and what happened.
- **Stable names:** agents address resources by framework names (HBEP-3), never by job ID, process ID, node, port or session.

### Non-goals

- Scheduling between agents or between purposes. The framework enforces what agents book; it does not rank them.
- An agent runtime, or messaging between agents.
- Replacing the provider's own access control. A provider credential still bounds what any agent can do there.
- Event types and their content. HBEP-8 owns them; this HBEP owns delivery.
- A tool server for language-model agents. It comes last, and is postponed (owner, 14 Nov 2026; Design details, Later work).

## Proposal

### User stories

- An agent starting its shift runs the inventory query and sees every deployment, Allocation and Instance, which owner holds each, why, and when each Allocation ends. Allocations that a retention policy keeps with no Instance are listed too.
- An agent wants to scale `example-27b` from two Instances to one. The plan shows that this cancels a pending Allocation that has waited two days, that the Provider's observed queue waits reach three days at p90, and that Slurm's advisory estimate for the same job submitted now is four days. The plan needs approval. The person who operates the framework, the default approver, reads the plan and declines.
- The operator lets one agent approve on the Sano Provider alone. That agent scales its Sano deployments down and approves its own plans. Its plan on Helios waits for the operator.
- An agent's session dies mid-operation. Its successor holds the same identity, finds the Operation by its name, and reads what was done; nothing runs twice.
- An agent plans the release of three kept Allocations on two providers. The plan lists exactly which jobs would be cancelled, and what each costs to get back. The owner approves that plan. The confirm runs it, and the report shows one cancellation failed on one provider.
- An agent repurposes an Allocation under the agreed policy. No approval is asked, and the plan still shows what the move costs.
- Two experiment runners reserve sessions on one deployment for the same night. The front admits each up to its booking, and neither can double the load (stable).
- An agent subscribed to "ending soon" is restarted. It resumes from its cursor and receives the events it missed (stable).
- After a project ends, deleting its ModelDeployments, once approved, ends their Instances, cancels their pending Allocations and releases their started ones (HBEP-7, Release). No keyword sweep is needed.

### Notes, constraints and caveats

- **Owner lease is not Allocation.** HBEP-3 calls an Allocation "one lease on a provider". An owner lease is an agent's claim of responsibility for a framework resource. This HBEP always writes "owner lease".
- **Identity is a credential,** not a session, a process or a Unix user. Two agents on one host have two identities; a restarted agent keeps its own. On the Kubernetes API it is the authenticated user that the API server sees: `request.userInfo` in admission, and `user` in the audit log.
- **Facts come from the server.** Every answer carries the server's observation time in UTC and the server's IDs. An agent quotes them; it does not compute times or hashes.
- **The inventory includes what does not match** (stable). Provider jobs that carry the framework's tag but have no owning resource, and resources whose provider job is gone, are listed as orphans.
- **The inventory says how fresh each fact is.** Each provider fact carries when it was last observed, so a stale view looks stale.
- **"Guaranteed delivery":** at least once, ordered per resource, durable across restarts, resumable from a cursor, each event with a unique ID for de-duplication. A subscriber that falls behind retention receives an explicit gap marker and must re-read status (HBEP-8, HBEP-11). Nothing is dropped silently, with one known exception: the lost-tail case below.
- **How, from spike S8 (issue #25, PR #28).** Decided at this status change, and built at stable (Graduation criteria):
  - The log is embedded NATS JetStream with `sync_interval: always`, CloudEvents 1.0.2, and one subject per resource.
  - **Cursors are the subscriber's own.** Each subscriber keeps a cursor (stream sequence plus last event id) in the control plane and saves it after handling. A server-side durable consumer is not enough: S8 found that a message delivered, not acknowledged and then evicted vanishes from its state with no marker.
  - **Gaps and rewritten history are derived, not read from JetStream.**
    - A gap is the range of sequences evicted unread. It is found from the sequence jump, or from the stream's first sequence.
    - **The subscriber reads the whole stream and filters events on its own side.** A server-side subject filter would make other subjects' sequences look like gaps. That is S8 Finding 2, holding by construction; it was not tested.
    - A cursor whose event id no longer matches the log means rewritten history, and the subscriber re-reads status.
    - **Lost-tail exception:** that check catches a lost tail only for a cursor already past the lost range. A subscriber still behind it reads the refilled sequences as the originals, and cannot see the loss. This arises only if acknowledged writes are lost, that is, on power loss or a kernel crash, which is not tested.
  - **Handlers de-duplicate by event id,** because a crash replays recently handled events.
  - **Not yet tested:** power loss or a kernel crash, the only cases where the fsync policy differs from the default. S8 did not test them; a FUSE test without root might work but was not attempted.
- **Dry run is enforced by the server.** An Operation does nothing until its requester confirms its plan, whatever the client sends. Its plan reads provider facts but never changes a provider: it uses only status the controllers already keep, and Slurm's `sbatch --test-only`, which submits no job (Design details, The release cost).
- **Actions under an agreed spec need no approval.** The lifecycle controller (HBEP-7) cancels hedge losers, releases an old Allocation once its replacement serves, releases what over-allocation does not keep or retain, releases an Allocation when its retention policy says, and ends a deployment at its "until when" without approval, because the ModelDeployment's spec and the retention policy already agreed to them. Approvals apply to actions outside the spec. Replacement before release (HBEP-3) holds regardless of approvals while the deployment keeps its desired capacity. It does not cover shutdown at the end time, scale-down, deletion, an approved early termination or a repurpose: their Instances drain and end without a replacement, because they lower a deployment's capacity rather than keep it. Each Allocation left with no Instance then follows its retention policy.
- **An agent's edit that lowers capacity is not an agreed spec.** It is the request. Scale-down, an earlier end time and deletion make the controller release by following the new spec, so the capacity guard sends those edits through an Operation (Design details, The capacity guard).
- **The release cost is shown, never decided on.** Observed queue waits are the number planning uses (HBEP-7, open question 2), and a start estimate is advisory (HBEP-4, Risks: "Start estimates are trusted"). The plan shows both with their observation times, and neither decides anything.
- **Bulk actions select by label,** with a dry run that lists every match by name. There is no pattern matching on process or job names.

### Risks and mitigations

- **A relayed approval.** An approval counts only from the approver's own identity, checked at admission, and bound to one plan; a message saying someone approved is not one.
- **An approval applied to a changed plan.** The approval names the plan's ID. If state changes before execution, the recomputed plan has another ID, and the Operation turns stale.
- **An agent releases by editing the spec.** Scale-down, an earlier end time and deletion all make the controller release (HBEP-7, Release). Mitigation: the capacity guard refuses those edits from every agent identity, and they go through an Operation.
- **An agent writes itself into the approval policy.** Mitigation: the policy is a cluster-scoped kind that only persons may write (Design details, Identities and roles).
- **An agent keeps using the admin kubeconfig.** The guard matches agent identities only, so it never sees such an agent. Mitigation: each agent gets its own identity in beta's first step, and the admin kubeconfig stays with the person as the break-glass credential. Its writes show in the audit log under the admin's name.
- **A controller path added later releases on another spec edit.** Mitigation: a controller change that makes a spec edit release capacity adds that edit to the guard in the same PR.
- **Plans turn stale too often.** A plan that changes between confirm and approval wastes the approver's time. Mitigation: the plan's ID covers what is done and who must approve, never the cost figures, which age (Design details, The plan). Beta measures how often plans turn stale (Open questions 7).
- **An owner lease expires while others depend on the resource.** Expiry must not release a serving deployment that other clients use. See Open questions.
- **Owner leases that never expire** recreate the clean-up problem. Every owner lease has an expiry, and the inventory shows leases near it.
- **Idempotency keys reused for a different request** would return the wrong result. An Operation's name is its key, and the CLI refuses a name that exists with a different request.
- **Subscriptions nobody reads** give senders false comfort. Each subscription's backlog (the stream's last sequence minus its cursor) is visible to its owner and to the inventory. Under subscriber-kept cursors, an unread subscription holds no storage: retention evicts by age or size, and the subscriber gets a gap marker.
- **Agents treat a timed-out call as failed** while it ran. Every request that lowers capacity is an Operation with a name, so the agent can read what happened. Every other write is a declarative update the agent re-reads.
- **Too many approvals stall work overnight.** Only actions listed as irreversible need one; everything else is dry run and confirm. The operator can let an agent approve in a scope.
- **An estimate reads as a promise.** A `--test-only` estimate swings as the queue changes. Mitigation: it is labelled advisory and shown beside the Provider's observed waits.
- **The audit log fills the disk.** It is kept for months. Mitigation: rotated files are compressed, Lease renewals and the controllers' reads are not logged, and beta measures the growth (Design details, Audit). The host's disk is shared with HBEP-8's usage files, whose free space `hpcburst_usage_dir_free_bytes` shows.
- **A person's confirmation of a deletion is procedural.** An agent with a shell as the person's Unix user could run the prune. Accepted: agents that share a Unix user are mutually trusted (Security), and the prune asks at a terminal.

### Open questions

Each question ends Decided, with its source, or Open, with what decides it. A "Lean" under an open item is a proposal, not a decision. Where Design details builds a lean, the review round of the PR that makes this HBEP implementable approves it as the design (HBEP-1, Roles and approval); it is not an owner decision, and the owner can reverse it by amendment.

1. **The interface agents use.**
   - **Decided** (owner, 14 Nov 2026, issue #10): the Kubernetes API first, a command-line tool over it, and a tool server for language-model agents last, postponed.
   - Today the `hpcburst` CLI has launcher commands only, `up`, `status`, `upgrade`, `restore`, `down`, `guard` and `version` (`cmd/hpcburst/main.go:48`), and the API has none of this HBEP's kinds. Design details names the CLI's first commands.
2. **Which actions are irreversible.**
   - **Decided** (HBEP-7, open question 8; owner, 29 Oct 2026, PR #14): releasing or cancelling a started Allocation before its hard end, outside the spec; any release while a retention policy says keep. None is needed for cancelling pending hedge losers, releasing at the deployment's end time, restarts and handovers, or for releasing a failed Allocation or one with `RelayEndedForGood` (owner, 10 Nov 2026, recorded on PR #331).
   - **Decided** (owner, 14 Nov 2026): deleting audit records needs a person's confirmation. HBEP-11 already lets only an operator delete identity, verification and accounting records (Ownership, records and deletion).
   - **Open. Lean (Design details builds it):** cancelling a pending Allocation other than a hedge loser, since its queue age is lost; and taking a deployment's last ready Instance before its end time. Checked against what the controller does:
     - scale-down cancels pending Allocations first, newest first (`internal/lifecycle/handover.go:154`);
     - a pending Allocation left with no live Instance is cancelled at once (`internal/lifecycle/retention.go:71`);
     - deleting a deployment releases its empty Allocations at once, whatever their policy (`internal/lifecycle/retention.go:78`);
     - an earlier end time moves the date that a kept Allocation with no date of its own is kept until (`internal/lifecycle/retention.go:188`).

     So each edit that lowers capacity (scale-down, an earlier end, deletion) can cause one of the four. What decides: the owner, if beta shows the lean too wide or too narrow.
3. **Whether a repurpose under an agreed policy needs approval.**
   - **Decided** (owner, 14 Nov 2026): no. Agents decide and steer repurposes, with no person.
   - **The policy** is HBEP-7's repurposing rule (open question 7, owner, 29 Oct 2026): the fit check, and a repurpose only of an Allocation whose deployment has no remaining work, reservation or fallback role, when the time saved exceeds the cost. The lifecycle controller checks it, and refuses a repurpose that fails it.
   - **Who may ask** is whoever RBAC lets create Operations in the namespace, which the operator sets (Design details, Identities and roles). Placing Instances of the same deployment on warm capacity needs no request at all (HBEP-7, open question 14).
   - Repurposing is built with HBEP-7's beta. HBEP-7's beta criterion says "with HBEP-10's approvals"; it changes by amendment with that code.
4. **Who may approve.**
   - **Decided** (owner, 14 Nov 2026): approvers are set globally, and overridden per Provider (site), per namespace and per ModelDeployment, the narrowest winning. An approver may be a person or an agent. The default is a person, the human operator.
   - **Open: the order between a Provider and a namespace,** which do not nest. Lean: ModelDeployment, then namespace, then Provider, then global. A namespace is one project's own choice, and a Provider is shared by every namespace that uses it. What decides: the owner.
   - At a grant's end, an approver approves an early termination or a scale-down. That is not an override of replacement before release, which applies only while desired capacity is kept (HBEP-3).
5. **What an owner lease's expiry does.** Open, for stable. Lean: the resource becomes "orphaned", stops taking new Allocations and emits an event; release then needs the usual approval. Another identity may take the owner lease over. What decides: the owner, before stable's build.
6. **The unit of a reservation.** Open, for stable. Lean: concurrent sessions on a deployment for a window; future capacity is a ModelDeployment with a start time. The front admits by booking, so HBEP-5 changes with it. What decides: the owner, with HBEP-5.
7. **How confirm binds to a plan.** Open. Lean (Design details builds it): the confirm and every approval name the plan's ID; the confirm is written with the Operation's `resourceVersion` read with the plan, so a re-plan in between makes the API server refuse it; and the executing pass recomputes the plan, refusing to run on a different ID. The plan's ID hashes the targets' UIDs and `metadata.generation`, not their `resourceVersion`: every status write moves a `resourceVersion`, and the lifecycle controller writes each ModelDeployment's status on every pass (`internal/lifecycle/status.go:118`), at least every 30 s (HBEP-7, Reconcile loop, step 11). What decides: beta's measurement of how often plans turn stale before they are confirmed and approved.
8. **Push or pull delivery for subscriptions.**
   - **Decided** (spike S8, issue #25, PR #28): pull, as a long-lived ordered read from the subscriber's stored cursor. A callback with acknowledgement needs a re-sequencer, and cannot mark ranges that were delivered, not acknowledged and then evicted.
   - **Open, for stable:** retention per event class, and competing consumers of one subscription, which need partitioning by resource. The store is HBEP-11's choice, with JetStream its candidate (HBEP-11, open question 4).
9. **How long idempotency keys are kept, and their scope.** Open. Lean (Design details builds it): the key is the Operation's name, so its scope is the namespace, where Kubernetes refuses a second object of one name. It is kept until the Operation is removed, 7 days after it finishes. What decides: beta's build, from the longest Operation's retries.
10. **Audit retention and who may read it.**
    - **Decided** (owner, 14 Nov 2026): kept for the grant's length; deleted only after a person confirms; compressed when it is no longer in use, since grants last months.
    - **Open: who may read it.** Lean: whoever may read the launcher's state directory, where the API server writes it: the person, and agents that share the person's Unix user (Security). What decides: the owner.
11. **Whether names are ever reused.**
    - **Decided in part** (Kubernetes, and built code): an object's name can be reused once it is deleted; its UID never is. Allocation names carry a random suffix (`internal/lifecycle/provision.go:500`; HBEP-3, Allocation and job identity), records are named by UID (`ver-`, `acct-`; HBEP-11), and the controller deletes an Allocation with a UID precondition (`internal/lifecycle/writes.go:111`).
    - **Open. Lean (Design details builds it):** every plan, approval and release cause names its targets by namespace, name and UID, so a reused ModelDeployment name, which a person chooses, never matches an old record. What decides: this PR's review round.
12. **How the grant's length is known.** Open. A Provider's settings name its grant (`api/v1alpha1/provider_types.go:25`), which alpha records but does not read (HBEP-4, Provider settings), and hold no end date. Lean: an optional grant end in the Provider settings, by an amendment to HBEP-4 and HBEP-11 merged before the code that reads it. Until it exists, nothing is offered for deletion, so the retention holds without it. What decides: that amendment's review round.
13. **When subscriptions come, against HBEP-8's beta.** Open. HBEP-8's beta criteria name "the durable log for events and usage, with HBEP-10's subscriptions" (HBEP-8, Graduation criteria). This HBEP builds subscriptions at stable, after the guard. What decides: the science agent with the owner. Either HBEP-8's criterion moves by amendment, or subscriptions join this HBEP's beta.

## Design details

### What Kubernetes gives, and what is new

The API server already holds identities, access control, admission, concurrency, dry run and an audit log (HBEP-11, Notes). This design uses each where it fits, and adds three kinds and two admission policies.

| Primitive | Reused from Kubernetes | New here |
|---|---|---|
| Identity | ServiceAccounts and their tokens; the authenticated user in `request.userInfo` and the audit log | one identity per agent, which the launcher issues |
| Owner and purpose | annotations; a ValidatingAdmissionPolicy that pins the owner to the requester | the annotations' names |
| Dry run | server-side dry run (`dryRun=All`) for direct writes | Operations, whose plan shows effects that a dry-run write cannot |
| Confirm binds to plan | optimistic concurrency on the confirm write; UID preconditions on deletes | the plan's ID |
| Approvals | RBAC for who may write; a ValidatingAdmissionPolicy that pins the approver to the requester | the ApprovalPolicy and Approval kinds |
| The capacity guard | a ValidatingAdmissionPolicy, as HBEP-11's CRD guard | its three rules |
| Idempotency | object names: the API server refuses a second create of one name | none |
| Partial failure | the status subresource | per-target outcomes |
| Audit | the API server's audit log, its policy and its rotation | the release cause |
| Inventory | list calls with label selectors | the CLI's view |
| Stable names | UIDs | none |
| Subscriptions (stable) | list and watch from a `resourceVersion`, for level state | the durable log (S8), since a watch cannot resume past compaction (HBEP-11, Notes) |
| Owner leases (stable) | the `Lease` kind's holder and duration | what expiry does |
| Reservations (stable) | none | admission by booking at the front (HBEP-5) |

### Identities and roles

- **Each agent has its own identity:** a ServiceAccount in the namespace `hpcburst-agents`. The API server names it `system:serviceaccount:hpcburst-agents:<name>` and gives it the groups `system:serviceaccounts` and `system:serviceaccounts:hpcburst-agents`. Such tokens "are perfectly valid to use outside the cluster and can be used to create identities for long standing jobs that wish to talk to the Kubernetes API" ([authentication](https://github.com/kubernetes/website/blob/main/content/en/docs/reference/access-authn-authz/authentication.md), accessed 2026-11-14).
- **`hpcburst identity add <name>`** creates the ServiceAccount, binds the agent role, requests a token as `kubectl create token` does, and writes a kubeconfig of mode 600 under the launcher's state directory. Lean: a token lasts 30 days, and `hpcburst identity renew <name>` replaces it.
- **Today every user and every controller share one kubeconfig,** k3s's admin credential, copied into the state directory (`internal/launcher/state.go:35`). HBEP-11 (Single-user host) already says the launcher issues one kubeconfig per identity; this builds it.
- **The person** keeps the admin kubeconfig on the single-user host. It stays the break-glass credential and is never given to an agent.
- **The controllers get their own ServiceAccounts** in `hpcburst-system`, so the audit log tells a controller's write from a person's.
- **Roles.** The launcher installs two ClusterRoles, bound cluster-wide on the single-user host and per namespace in the team option:
  - `hpcburst-agent`: read every framework kind, ControllerRevisions, Events and Leases; create and update ModelDeployments, their scale subresource included; create Operations, and update them to confirm; create Approvals; read ApprovalPolicies. No write to Allocations, Instances, Providers, PlatformProfiles, records, ApprovalPolicies, RBAC objects, admission policies or namespaces, and no delete of anything.
  - `hpcburst-operator`: for persons other than the admin, in the team option: the agent role, plus writing ApprovalPolicies.

### Owner and purpose

- ModelDeployments and Operations carry two annotations: `hpcburst.groundnuty.github.io/owner`, the owner's user name as the API server reports it, and `hpcburst.groundnuty.github.io/purpose`, free text (Lean: at most 256 characters).
- The lifecycle controller copies both to the Allocations and Instances it creates, so the inventory shows them on every object.
- A ValidatingAdmissionPolicy refuses an agent's create of a ModelDeployment, and anyone's create of an Operation, whose owner differs from `request.userInfo.username`. A policy sees the whole object, annotations included, which a CRD's own rules cannot reach (HBEP-11, Admission). The CLI fills the owner in from a SelfSubjectReview, stable since Kubernetes 1.28 (authentication, above).
- A person's create may leave the owner out. The inventory then shows it as unknown, and the audit log names the creator.
- **Selection by owner** uses the label `hpcburst.groundnuty.github.io/owner-hash`: `u-` and the first 12 hex characters of the SHA-256 of the user name. A ServiceAccount's name holds `:`, which a label value refuses, so HBEP-11's long-value rule does not apply.

### The capacity guard

A ValidatingAdmissionPolicy, `hpcburst-capacity-guard`, with `failurePolicy: Fail`, written and installed like the CRD guard (`config/admission/crd-deletion-guard.yaml`). Its match condition takes every request whose user carries the group `system:serviceaccounts:hpcburst-agents`: every agent identity, and nothing else. Match conditions can read `request`, the admission request's attributes, user included ([ValidatingAdmissionPolicy](https://github.com/kubernetes/website/blob/main/content/en/docs/reference/access-authn-authz/validating-admission-policy.md), accessed 2026-11-14).

On a ModelDeployment it refuses:
- a lower `spec.instances` (`api/v1alpha1/modeldeployment_types.go:311`), whether written to the object or through its scale subresource (`modeldeployments/scale`, `:644`);
- a `spec.endTime` set where there was none, or moved earlier (`:341`);
- a delete.

Each refusal's message names the command that does the same through an Operation: `hpcburst scale`, `hpcburst end`.

- **Why these three.** Each makes the lifecycle controller release by following the new spec: scale-down cancels pending Allocations first (`internal/lifecycle/handover.go:154`), shutdown at the end time and a moved keep date release (`internal/lifecycle/retention.go:188`), and deletion releases at once (`:78`). The controller's own releases under an agreed spec stay ungated (Notes).
- **Not guarded,** because they release nothing: raising `spec.instances`, extending the end time, and editing the serving spec, which rolls out make-before-break (HBEP-3). Nor a ModelDeployment's default retention policy, since each Allocation copies it at creation (`internal/lifecycle/provision.go:388`) and keeps its own after (`internal/lifecycle/retention.go:90`).
- **Left to RBAC,** which denies agents every write to Allocations and Instances, and namespace deletion.
- **Persons and the controllers are outside the guard.** A person's direct edit is their own decision, and it shows in the audit log. The CLI sends a person's request through an Operation as well, so the person sees its cost.
- `hpcburst status` reports whether the guard is installed, as it does for the CRD guard.

### Operations

An Operation is a namespaced kind: one request to lower capacity, planned before it runs. Its name is its idempotency key.

- **Spec,** immutable once created (`self == oldSelf`), except `confirm`, which may be set once:
  - `action`: `Scale`, a ModelDeployment to fewer Instances; `End`, a ModelDeployment's end now, with `delete: true` to remove it; or `Release`, Allocations that host no live Instance, named or selected by label. `Repurpose` joins with HBEP-7's repurposing.
  - the target and the action's parameters;
  - the owner and purpose annotations (Owner and purpose);
  - `confirm.planID`, absent until the requester confirms.
- **Phases,** in status (Lean names): `Planning`, `Planned` (waiting for confirm), `AwaitingApproval`, `Executing`, `Done`, `PartlyFailed`, `Stale`, `Refused`.
- **Who runs it.** The lifecycle controller, in the reconcile pass of each ModelDeployment it acts on. One worker holds a key at a time (HBEP-7, Controllers), so executing a plan never races the controller's own scale-down or handover. A `Release` target maps to the ModelDeployment named by its Allocation's `model-deployment` label, as HBEP-7's work queue maps it.
- **Removal.** An Operation is removed 7 days after it finishes, as a Job's `ttlSecondsAfterFinished` removes a finished Job ([TTL after finished](https://github.com/kubernetes/website/blob/main/content/en/docs/concepts/workloads/controllers/ttlafterfinished.md), stable since Kubernetes 1.23, accessed 2026-11-14). The audit log holds its content by then (Audit).

#### The plan

The lifecycle controller writes `status.plan` without side effects:
- **each target:** its kind, name, UID and generation; its effect: cancel a pending Allocation, drain an Instance, drain the deployment's last ready Instance, release a kept Allocation, or keep an Allocation under its policy; whether the effect needs approval (What needs approval), and if so, the ApprovalPolicy that applies, with its generation and approvers;
- **the release cost** of each target that needs approval (The release cost);
- **the observation time** of every fact;
- **`id`:** a hash over the targets' UIDs and generations, their effects, which of them need approval, and the policies that apply. Cost figures are left out, since they age; the CLI shows their age instead.

The plan comes from the same code that executes: a `Scale` plan runs the controller's scale-down selection (`internal/lifecycle/handover.go:158`) on the state it read. It reads providers only through the status the controllers already keep, plus the proposed estimate, which submits nothing. The lifecycle controller writes an Operation's status only when its plan or phase changes. Another writer, such as the Provider's controller filling in an estimate, still moves the Operation's `resourceVersion`, so a confirm refused with a conflict re-reads the Operation and is retried when the plan ID is unchanged.

#### Confirm

- The requester sets `confirm.planID` by an update carrying the `resourceVersion` it read with the plan. A re-plan in between moves that version, and the API server refuses the update with a conflict, so the requester reads the new plan. The executing deletes carry a UID precondition (`Preconditions`, [`pkg/apis/meta/v1/types.go`, lines 749–756](https://github.com/kubernetes/apimachinery/blob/v0.35.3/pkg/apis/meta/v1/types.go#L749-L756), accessed 2026-11-14), as the controller's releases already do (`internal/lifecycle/writes.go:111`).
- An empty plan ID, or one that is not the current plan's, is refused.
- Only the Operation's owner may confirm it. The owner policy (Owner and purpose) refuses an update that sets `confirm` from any identity but the one in the owner annotation, so one agent cannot run another's plan on its own word (L-14.3).
- An Operation that is never confirmed never runs. A plan is the default, and running takes an explicit act that names the plan (L-11.6).

#### Approval and execution

- A plan with no target that needs approval runs once confirmed.
- Otherwise it waits in `AwaitingApproval` until each target that needs approval has an Approval for the plan's ID from an approver of the policy that applies to it. Nothing runs before then.
- **Before it acts,** the executing pass recomputes the plan from what it has just read. A different ID, as when a pending Allocation has started, another Instance has become ready or a policy has changed, turns the Operation `Stale`. Nothing runs, its approvals no longer count, and the requester plans again.
- **Then the pass acts as the lifecycle controller:** for `Scale`, it lowers `spec.instances`; for `End`, it sets `spec.endTime` to now, or deletes the ModelDeployment; and it applies HBEP-7's matching Release case in the same pass. A `Release` deletes the named Allocations. An approved `End` is HBEP-7's approved early termination: as shutdown, before the end time (HBEP-7, Release).

#### The report

- `status.targets[]` holds one outcome per target: done, failed with its reason, or not started.
- A target that fails is retried with backoff until it succeeds or the plan turns stale. The Operation is `PartlyFailed` meanwhile, never a single "rejected" (L-11.7).
- A released Allocation is done when its object is gone. Its finalizer allows that only once every job with its tag is cancelled (HBEP-11, Ownership, records and deletion).

### What needs approval

| Effect in a plan | Needs approval | Source |
|---|---|---|
| Cancel a pending Allocation, other than a hedge loser | yes | Lean (Open questions 2) |
| Drain a deployment's last ready Instance before its end time | yes | Lean (Open questions 2) |
| Release a started Allocation before its hard end, outside the spec | yes | HBEP-7, open question 8 |
| Release an Allocation that its retention policy keeps | yes | HBEP-7, open question 8 |
| Drain an Instance while another stays ready, its Allocation kept | no | |
| Release an Allocation under `ReleaseWhenIdle`, after its idle limit | no: the policy agreed to it | HBEP-7, open question 8 |
| Repurpose under the agreed policy | no | owner, 14 Nov 2026 |
| Delete audit records | a person, outside any policy | owner, 14 Nov 2026 |

So an `End` of a deployment that serves, or that waits in a queue, always needs approval: it drains the last ready Instance or cancels a pending Allocation. A `Scale` needs it when it cancels a pending Allocation or leaves no ready Instance. A `Release` of a kept Allocation needs it.

### The release cost

For each target that needs approval, the plan shows:
- **what is given up:**
  - a started Allocation's lease left, its hard end minus now (`status.atProvider.hardEnd`, `api/v1alpha1/allocation_types.go:404`), and the GPU-hours that leaves;
  - a pending Allocation's queue age, now minus its submit time (`submitTime`, `:382`), and its own advisory start estimate (`startEstimate`, `:392`);
  - for the last ready Instance: that the model serves nothing on its own capacity until a new Allocation starts and its Instance is ready;
- **what getting it back costs:**
  - the Provider's observed queue waits: the median, the p90 and the largest of the last 20 in its status (`api/v1alpha1/provider_types.go:132`; HBEP-4, Provider status). Planning uses this number (HBEP-7, open question 2);
  - Slurm's advisory estimate for the same job submitted now: `sbatch --test-only` with the Allocation's submission parameters, which "return[s] an estimate of when a job would be scheduled to run given the current job queue and all the other arguments specifying the job requirements. No job is actually submitted." ([`doc/man/man1/sbatch.1`, lines 2356–2359, Slurm 24.11.5](https://github.com/SchedMD/slurm/blob/slurm-24-11-5-1/doc/man/man1/sbatch.1#L2356-L2359), accessed 2026-11-14). It is labelled advisory, since estimates swing by hours (HBEP-4, Risks);
  - the class's last start-to-ready time (`api/v1alpha1/modeldeployment_types.go:604`), so the time to serve again is about the wait plus that.
- **An unknown figure** is shown as unknown, with why, never as zero (HBEP-8, Notes: unknown is a value).

The scale of the stake: on Helios a 1-GPU short job under QOS `now` started at once, where the normal QOS estimated a start in 16 days (HBEP-4, Platform profile schema, the facts from the first real-engine run, issue #94).

**The estimate needs a provider operation HBEP-4 lacks** (HBEP-4, open question 1, lists the core operations). Proposed, by an amendment to HBEP-4: an `estimate` operation that runs `sbatch --test-only` with an Allocation's parameters, read-only, over the same connection and within the same bounds as the other commands. It is the operation behind the capability flag "gives start estimates", which HBEP-4 lists and does not need for alpha (HBEP-4, open question 2); a managed provider has none. Only the Provider's controller holds the credentials (HBEP-11, Risks), so it runs the estimate when a plan asks, and writes the answer and its time into the Operation's status under its own field manager, as HBEP-11 has one controller own a single entry of another's status (HBEP-11, Controllers, Field managers). The plan does not wait for it: the plan's ID leaves cost figures out, so the lifecycle controller writes the plan at once with the estimate pending, and an estimate not filled in within a bound (Lean: 60 s) reads as unknown. Until that amendment and its code merge, plans show the estimate as unknown.

### Approval policy

ApprovalPolicy is a cluster-scoped kind, so only cluster-wide RBAC can write it: persons, never agents (Identities and roles).
- **`spec.scope`:** exactly one of `global`, a Provider's name, a namespace's name, or a ModelDeployment's namespace and name. One policy per scope (Lean: its name derives from the scope, with HBEP-11's long-value rule).
- **`spec.approvers`:** users, groups and ServiceAccounts, in any mix. A ServiceAccount is a user to the API server, so an agent approves as a person does.
- **Resolution.** For a target on Provider P, in namespace N, assigned to ModelDeployment M, the policy that applies is the first that exists of M, N, P and global (Open questions 4).
- **The default.** `hpcburst up` writes the global policy when none exists, naming the identity of the person's kubeconfig as a SelfSubjectReview reports it. With no policy at all, a target that needs approval waits: nothing is approvable without a policy.
- **The owner's example.** The operator writes a Provider policy for Sano that names one agent. That agent approves its own plans on Sano; its plans elsewhere wait for the operator.
- A plan records each policy it applied, with its generation, so a change to the policy makes the plan stale at execution.

### Approvals

An Approval is a namespaced kind, in its Operation's namespace, immutable once created:
- `spec.operation`: the Operation's name and UID;
- `spec.planID`;
- `spec.approver`: the approver's user name and groups.

A ValidatingAdmissionPolicy refuses an Approval's create unless `spec.approver` equals the request's user name and groups. So an approval is accepted only from the approver's own identity, and a message saying someone approved is not one (L-14.3).

- The lifecycle controller counts an Approval for a target only when its approver is in the policy that applies to the target, and its plan ID is the current plan's.
- One Approval covers every target of the plan whose policy names its approver.
- An approver may approve their own Operation. The approval is still a separate act, bound to the plan.
- **No webhook.** Admission only pins identities and refuses the guarded writes; the controller that executes checks approvals against the state it has just re-read. HBEP-11 (Admission) expected a webhook for these gates after alpha; this design needs none.

### Repurposes

A repurpose is an Operation with action `Repurpose`, built with HBEP-7's repurposing. It is planned, its cost is shown and it is confirmed like any other, and it needs no Approval (owner, 14 Nov 2026). The lifecycle controller refuses one that fails HBEP-7's rule (open question 7). The Allocation is kept, so no capacity is given back.

### Audit

The audit is the API server's audit log together with the release cause that the lifecycle controller writes. With both, the record says who asked, for whom, what was planned, who approved it and what happened.

- **Today** the launcher logs every API request at the Metadata level, with no bodies (`internal/launcher/k3s.go:55`): the user, the verb, the object, the time and the response code. That names who asked and who approved, but not the plan.
- **The policy** (proposed, by an amendment to HBEP-11). Audit policy levels run from `None` to `RequestResponse`, which logs "request metadata, request body and response body" ([auditing](https://github.com/kubernetes/website/blob/main/content/en/docs/tasks/debug/debug-cluster/audit.md), accessed 2026-11-14):
  - this HBEP's kinds, Operation, Approval and ApprovalPolicy, at `RequestResponse`, so the log holds each request with its purpose, the plan and the per-target outcomes from the controller's status writes, and each Approval with the plan it names;
  - the lifecycle controller's writes to Allocations at `Request`, so the release cause is logged before the delete that follows;
  - the controllers' Lease renewals and reads at `None`: they carry no decision, and they would fill most of the log;
  - everything else at `Metadata`, so a Secret's content is still never logged (HBEP-11, Security).
- **The release cause.** Before the lifecycle controller deletes an Allocation, it writes the annotation `hpcburst.groundnuty.github.io/release-cause`. It holds the rule followed, the reason the controller already computes (`internal/lifecycle/writes.go:106`, such as "released when idle, after 30m" or "its deployment was deleted"), or the Operation's namespace, name and UID. So a retention release names its policy in the log, with no approval asked.
- **What happened at the provider** is the Allocation's accounting record, `acct-<UID>`, with its job's final state and hours (HBEP-8, Provider accounting). The log holds the delete that cancelled it.
- **Kept for the grant** (owner, 14 Nov 2026). The launcher keeps every rotated file, with `--audit-log-maxbackup=0` and `--audit-log-maxage=0`. Today it keeps three old files of 100 MB (`internal/launcher/k3s.go:106-110`), and the API server's default removes files after 366 days. For both flags, 0 means no limit ([kube-apiserver](https://github.com/kubernetes/website/blob/main/content/en/docs/reference/command-line-tools-reference/kube-apiserver.md), accessed 2026-11-14).
- **Compressed when cold.** `--audit-log-compress`: "the rotated log files will be compressed using gzip" (same source). Only the file being written stays plain.
- **Deleted only after a person confirms.** Nothing in the framework deletes an audit file. `hpcburst audit prune` (stable) lists the rotated files whose last entry is older than the grant's end, and deletes them only after a person confirms at a terminal. With no grant end recorded, it lists none (Open questions 12).
- **Size.** Not measured. Beta measures a week of the log on the alpha host under this policy, before the policy is fixed.

### Server-side dry run

A direct write sent with `dryRun=All` (`kubectl --dry-run=server`) runs admission, the capacity guard included, and stores nothing: "dry-run requests will not be persisted in storage or have any other side effects" ([API concepts](https://github.com/kubernetes/website/blob/main/content/en/docs/reference/using-api/api-concepts.md), accessed 2026-11-14). So an agent checks a ModelDeployment edit, or whether the guard refuses it, with no effect. A dry-run write cannot show what the controllers would do, since they never see it; an Operation's plan does. The admission policies have no side effects, so they need no dry-run case.

### Inventory

`hpcburst inventory` reads the API and stores nothing. For every ModelDeployment, Allocation and Instance in the namespaces the caller can read, it lists:
- the owner and purpose;
- the phase, the Provider and the hard end;
- the effective retention policy, and the `idle-since` anchor (HBEP-7, Retention), so kept Allocations with no Instance stand out;
- each Provider's last successful read, with its age (HBEP-4, Provider status);
- the Operations waiting for approval.

`--json` gives agents the same. Orphans come at stable: HBEP-11 (Risks) has controllers report tagged jobs with no Allocation, and no code does yet; `TunnelOrphaned` covers tunnels only (HBEP-8, Reasons in alpha).

### The CLI

The first commands, in beta. They use the caller's own kubeconfig (`KUBECONFIG`, or `--kubeconfig`), and never fall back to the launcher's admin kubeconfig, so an agent never acts as the admin by accident:
- `hpcburst inventory`;
- `hpcburst scale <md> --instances N`, `hpcburst end <md> [--delete]` and `hpcburst release <allocation>... | -l <selector>`. Each creates an Operation, named by `--key` or else by a generated name that it prints, waits for the plan, prints it with its costs, and asks for confirmation at a terminal. Without a terminal it prints the plan and exits;
- `hpcburst confirm <operation> --plan <id>`;
- `hpcburst approvals`, the Operations that the caller may approve, and `hpcburst approve <operation> --plan <id>`;
- `hpcburst operation <name>`: the plan and the per-target report;
- `hpcburst identity add|renew <name>`, a launcher command on the host.

### Stable: subscriptions, owner leases, reservations, orphans

- **Subscriptions** follow S8 (Notes). Until they exist, agents follow state as controllers do: a list, then a watch from its `resourceVersion`, and a new list when the watch falls behind compaction (HBEP-11, Test plan). Status conditions are the truth, and Events, kept 7 days, are best-effort edges (HBEP-8, Events, Retention).
- **Owner leases.** Lean: a `Lease` per owned ModelDeployment, owned by it so that garbage collection removes it, with the owner as holder and the owner lease's duration; an admission policy lets only the holder renew it, and a take-over sets the new holder to the requester. What expiry does is open question 5.
- **Reservations** follow open question 6.
- **Orphans** in the inventory need the Provider's controller to list tagged jobs with no Allocation (HBEP-11, Risks).
- `hpcburst audit prune` (Audit), and `Repurpose` with HBEP-7's repurposing.

### Later work: the tool server

A tool server for language-model agents is postponed (owner, 14 Nov 2026: "tool server comes last in this and we can postpone building such a thing"). It is in none of this HBEP's stages. Lean: when it is built, it is a thin layer over the same API calls and the CLI's plan, confirm and approve steps, never a second path around the guard.

### Proposed event reasons

HBEP-8's Reasons table gains these only with the code that raises them:
- `PlanStale` (`degraded`, regarding the Operation): an approved or confirmed plan turned stale at execution;
- `OperationPartlyFailed` (`failing`, regarding the Operation): a target failed.

Events for stops that an Operation asked for keep their existing reasons, carry the Operation in `related`, and name it in the note, so a planned stop names its actor and its operation (HBEP-8, Notes). A plan waiting for approval shows in the Operation's phase and in `hpcburst approvals`, with no event.

### What other HBEPs change

Each change is an amendment, merged in its own PR before the code that needs it (HBEP-1, Building from an HBEP):
- **HBEP-11:** the three kinds in the API table; under Admission, the capacity guard and the two identity pins, and its sentence on a webhook for these gates; under Single-user host, the agent and controller identities, the roles, the global policy and the audit policy and rotation; and its Risks line "An approval for it comes with HBEP-10". Lean for that line: removing the CRD guard stays a person's explicit step (`hpcburst guard remove`), outside approvals, since no agent may write admission policies.
- **HBEP-4:** the `estimate` operation (The release cost), and an optional grant end in the Provider settings (Open questions 12).
- **HBEP-7:** Operations executed in the ModelDeployment's reconcile pass; the release cause written before each delete; and its beta repurposing with no approval (Open questions 3).
- **HBEP-8:** the reasons above, with their code, and the durable log's stage (Open questions 13).

### Test plan

Beta, against S4's fake Slurm and the single-user host, with two agent identities and one person:
- **The guard.** As an agent identity, each guarded write is refused, with a message naming the command: a lower `spec.instances` through the object and through `kubectl scale`, an end time set or moved earlier, and a delete. Raising `spec.instances` and extending the end time are accepted. The same writes from the person's identity pass. A test run under the admin or a controller identity proves nothing about the guard, since neither is in its group.
- **RBAC.** An agent identity cannot update or delete an Allocation or an Instance, write an ApprovalPolicy, or delete a namespace.
- **A plan has no side effects.** An unconfirmed Operation of each action is planned, and the fake Slurm's call log shows no submit, cancel or hold release, only reads and `--test-only`. Nothing in the API changes but the Operation's status. The plan lists every target by name.
- **Confirm.** A confirm whose `resourceVersion` predates a re-plan is refused with a conflict; one refused after only an estimate was filled in is retried with the same plan ID and accepted. An empty or wrong plan ID is refused. A confirm from an identity other than the Operation's owner is refused at admission. An Operation never confirmed never runs.
- **Stale plans.** Between approval and execution, start the pending Allocation, make another Instance ready, or edit the policy that applies: the Operation turns `Stale`, nothing runs, and its approval no longer counts.
- **What needs approval,** in fixtures whose expectations are worked out by hand from HBEP-7's rules, never by the planner's code: a scale-down that cancels a pending Allocation needs approval; one that drains a ready Instance while another stays ready, its Allocation under `Keep`, does not; an `End` does; releasing a kept empty Allocation does; one under `ReleaseWhenIdle` past its limit is released by the controller with no Operation, and its release cause names the rule.
- **Approvers.** An Approval that names another identity as approver is refused at admission. One from an identity outside the policy that applies does not count. A ModelDeployment policy overrides the global one, and a namespace policy a Provider policy. With no policy at all, the Operation waits. A test in which the requester approves its own plan proves nothing about relayed approvals; the test uses two identities, one outside the policy.
- **The owner's example.** A Provider policy names agent A for one Provider. A's plan there runs on A's own approval; A's plan on another Provider waits for the person.
- **The cost.** With the fake's fixed queue waits, the plan shows the median, the p90 and the largest, the lease left and the queue age, each worked out by hand from the fixture's times, and the fake's `--test-only` answer labelled advisory. A failing or slow estimate shows as unknown, never as zero. An expected value computed by the planner's own code proves nothing.
- **Partial failure.** Release two kept Allocations on two Providers, with one Provider failing its cancel: the report shows one done and one failed and retried, never a single refusal.
- **Idempotency.** Create an Operation, kill the client, and create it again under the same name: one Operation, and one execution. The same name with a different request is refused by the CLI.
- **Restart.** Kill the lifecycle controller mid-execution: on restart it re-reads the Operation and its targets, and finishes or reports each target. Nothing is released twice.
- **Audit.** After an approved `End`, the API server's audit log holds the Operation's create by the requester, the Approval's create by the approver with its plan ID, the plan and outcome writes, the release cause and each Allocation's delete, each under its own identity. Two agents running as one Unix user are told apart.
- **Rotation.** With a small `audit-log-maxsize`, rotated files are gzipped and none is removed.
- **Owner.** An agent's create of a ModelDeployment that names another owner is refused. The Allocations and Instances the controller creates carry the owner and purpose.

Stable:
- Stop a subscriber, emit events, restart it: it receives them all, in order per resource; past retention it receives a gap marker.
- Let an owner lease expire on a deployment that serves other clients: it is not released without approval.
- Create a provider job with the framework's tag and no owning resource: the inventory lists it as an orphan.
- Two runners reserve sessions on one deployment: the front admits each up to its booking.
- `hpcburst audit prune` with no grant end recorded lists nothing; with one, it deletes only after a person confirms.

### Compatibility and upgrades

- The three kinds start at `v1alpha1` and follow HBEP-11's versioning. Operations and Approvals are short-lived; the record that outlives them is the audit log, in Kubernetes' own `audit.k8s.io/v1` format.
- The guard matches agent identities only, so installing it changes nothing for a person or a controller.
- ModelDeployments created before the owner annotation show their owner as unknown.
- **Downgrade:** removing the kinds removes Operations and Approvals in flight; the audit log keeps what they recorded. Removing the guard's binding returns agents to direct edits, the unsafe practice of Motivation.

### Security

- Agent credentials are kubeconfigs of mode 600, one per identity, issued by the launcher; never on command lines or in logs. Agents never get the admin kubeconfig. Agents that share a Unix user are mutually trusted; separate credentials give them attribution only.
- An Approval is accepted only when the writing identity is its named approver, checked at admission, and it counts only when the policy that applies names that approver. A confirm is accepted only from the Operation's owner, checked at admission. Operation status, plans included, is written only by the controllers.
- ApprovalPolicies are written only by persons.
- The audit log logs bodies only for this HBEP's kinds and the lifecycle controller's writes to Allocations, which hold no credentials. Purposes are free text that others read; they must not hold credentials, prompts or private data.
- Admin operations (attaching providers, adding and removing models) are Kubernetes API writes under RBAC (HBEP-5, HBEP-11). This HBEP's approvals add a second gate for the irreversible ones.

### Graduation criteria

- **Alpha: none.** The release `v0.1.0-alpha.1` ships none of these primitives: no Operation, Approval or ApprovalPolicy kind in `api/v1alpha1/`; launcher commands only in the CLI (`cmd/hpcburst/main.go:48`); one admin kubeconfig for every user and controller (`internal/launcher/state.go:35`); and an audit log capped at four files of 100 MB (`internal/launcher/k3s.go:106-110`). The framework's alpha needs none: its controller takes no action that needs approval, and the single user changes the spec instead (HBEP-7, open question 8). This HBEP's first stage is beta, so its first `implemented` PR records stage `beta`. HBEP-1's lifecycle table names `alpha` as the stage that PR records, so that PR asks the approver.
- **Beta,** built in this order, each step usable on its own:
  1. **The guard:** agent identities and roles, the capacity guard, ApprovalPolicy with the person as default approver, Operations (`Scale`, `End`, `Release`) with a plan, a confirm and a per-target report, Approvals, and the release cost from status (lease left, queue age, the Provider's observed waits, start-to-ready).
  2. **The CLI** for it: `scale`, `end`, `release`, `confirm`, `approvals`, `approve`, `operation`, and `identity add`.
  3. **The audit:** the controllers' own identities, the audit policy and its rotation, and the release cause.
  4. **The estimate:** HBEP-4's `estimate` operation, shown in plans.
  5. **The inventory,** with the owner and purpose annotations.

  Beta is reached when the Test plan's beta rows pass on the single-user host with two agent identities and one person, and one approved `End` runs end to end on Athena, with its audit trail read back.
- **Stable:** owner leases with expiry and take-over; the durable log with subscriptions (S8); orphans and freshness in the inventory; reservations; `Repurpose` with HBEP-7's repurposing; `hpcburst audit prune` with the grant end; and the whole Test plan across two providers, with two agents working at once.

## Operational readiness

1. **Working:** the guard is installed (`hpcburst status`); a test Operation of a test agent identity is planned with current observation times; a guarded write from that identity is refused; the audit log grows, and its rotated files are compressed. A missing guard, a plan that stays `Planning`, or an audit log with no `RequestResponse` entries for Operations says it is not working.
2. **Restart:** Operations, Approvals and policies live in the control plane (HBEP-11), so a restarted controller re-reads them. An executing Operation resumes in its ModelDeployment's next pass, which recomputes the plan first.
3. **Silent failure:**
   - a plan that changed a provider: the provider plugin's call log shows it;
   - an execution that ran a different plan than the approved one: the plan IDs in the audit log show it;
   - an approval counted from an identity outside the policy: the audit log names both;
   - a guard that is not installed, so agents edit freely: `hpcburst status` reports it;
   - an agent still on the admin kubeconfig: its writes show in the audit log under the admin's name.
4. **Provider limits:** each `--test-only` estimate is one login-node command per Provider and plan, through the Provider's bounded connection (HBEP-4, Transport). The inventory reads the control plane's last observation and shows its age, never every provider live.
5. **Capacity:** the audit log grows for the whole grant; rotated files are gzipped, and the noisiest entries are not logged. The store holds Operations only for 7 days after they finish. With subscriptions (stable), a full durable log evicts its oldest events by retention, and subscribers behind them get a gap marker; retention per event class sizes it.
6. **Idle cost:** the three kinds' objects, the admission policies, and the audit log's disk. No grant hours, and no provider connections until a plan asks for an estimate. The durable log adds one process at stable.
7. **Rollback:** removing the guard's binding returns agents to direct edits. Operations in flight finish or are deleted. The primitives can be switched off only together with agent access, since agents without them fall back to the unsafe practices in Motivation.

## Drawbacks

- Agents must carry identities and Operation names, and wait for approvals.
- A plan and a confirm double the calls for every request that lowers capacity.
- A person's direct edit bypasses the guard, by design.

## Alternatives

- **A central scheduler that ranks agents' work.** It would decide whose run matters more, which the user, not the framework, knows. Rejected: coordination stays with agents.
- **Rules in agents' instructions only.** The past project fixed most of the problems above by written rules. Rules are skipped under pressure and do not bind new agents; primitives in the server do.
- **Provider access control alone.** A Slurm account says who may cancel a job, not why it runs, who approved its release, or what else belongs to the same purpose.
- **Gate only the controller's delete of an Allocation.** Rejected: an agent releases through the spec edits the controller follows (The capacity guard).
- **Approvers as RBAC grants of a custom verb `approve`,** checked through a ValidatingAdmissionPolicy's `authorizer`, as CertificateSigningRequest approvers are granted `approve` on `signers` ([CSR](https://github.com/kubernetes/website/blob/main/content/en/docs/reference/access-authn-authz/certificate-signing-requests.md), accessed 2026-11-14). It would add no kind. Rejected: RBAC is additive, so a wider grant can never be overridden, and the owner decided that the narrowest scope wins.
- **An admission webhook that checks an approval on every guarded write.** Rejected: it puts a server of ours in the request path that must look up plans and approvals, and the controller that executes already re-reads the state. HBEP-11 runs no webhook.
- **Audit records as ControllerRevisions in the API store.** Rejected: months of records would grow the store, and the lifecycle controller caches every ControllerRevision (HBEP-11, Single-user host, `/readyz`'s `caches`).

## Prior art and reuse

The primitives replace www-2027 practices: a shared gate on a common host (slots per class, load and memory floors, a hold file), scripts that ran dry by default and acted only with an explicit go flag, and restart notices and approvals sent by message.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| Kubernetes API: RBAC, ServiceAccounts, ValidatingAdmissionPolicy, server-side dry run, optimistic concurrency and delete preconditions, SelfSubjectReview, the audit log with its policy and gzip rotation, `Lease` | [kubernetes/kubernetes](https://github.com/kubernetes/kubernetes); [k3s](https://github.com/k3s-io/k3s) | Apache-2.0 | Dependency (HBEP-11): k3s as the single-user host and the team option. Identities, roles, the capacity guard, the identity pins, plan binding and the audit, with no code of ours in the request path |
| NATS JetStream | [nats-io/nats-server](https://github.com/nats-io/nats-server) | Apache-2.0 | Candidate, confirmed by spike S8 (PR #28), for stable: an embedded log; subscriptions are subscriber-kept cursors over stream sequences, with a thin layer of our own for gaps and order |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| A request object that a separate approver approves, and a controller that acts only on approved requests | Kubernetes CertificateSigningRequest ([CSR documentation](https://github.com/kubernetes/website/blob/main/content/en/docs/reference/access-authn-authz/certificate-signing-requests.md)) | Operations and Approvals |
| Removing a finished object after a TTL | Kubernetes Job `ttlSecondsAfterFinished` | Operations are removed 7 days after they finish |
| Audit levels per resource, and rotation that compresses | Kubernetes audit policy; kube-apiserver's `--audit-log-*` flags | The audit |
| Pre-drain and pre-terminate hooks; `paused`, `skip-remediation` and `remediate-machine` annotations | Cluster API ([kubernetes-sigs/cluster-api](https://github.com/kubernetes-sigs/cluster-api)) | Approval gates and switches |
| Management policies; Observe-only import | Crossplane ([crossplane/crossplane](https://github.com/crossplane/crossplane)) | Orphans imported without being managed (stable) |
| Usages for deletion ordering and protection | Crossplane (beta, needs Crossplane core) | Our own kind, when Crossplane core is absent |
| A lease with a holder identity and a duration; server-side dry run | Kubernetes `Lease`; Kubernetes API dry run | Owner leases (stable); dry run of direct writes |
| Dry-run estimates of a submission | Slurm's `sbatch --test-only` | The release cost's advisory estimate |
| Queue fields `guarantee` and `capability`; Open, Closing and Closed states | Volcano | Reservation vocabulary (stable) |

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| A freed slot went to another user at once; the replacement waited days. | L-1.2 | A release that gives up capacity needs approval, and its plan shows what getting the capacity back costs. |
| Releases and grants pull against each other. | L-13.5 | The plan shows both: the grant hours a release gives back, and the queue time to get the capacity again. |
| No inventory said who had started what. | L-10.17 | One inventory query across providers. |
| Every agent ran as one Unix user. | L-11.1 | Identity is a credential, not a Unix user: one ServiceAccount per agent. |
| Our own job took nodes our pinned follow-on needed. | L-1.3 | The inventory shows every holder and purpose; reservations book capacity. |
| Kill by pattern killed other agents' work. | L-11.2 | Bulk actions select by label, never by pattern. |
| Recorded process IDs pointed at the wrong process. | L-11.3 | Agents address resources by stable names, never by process ID. |
| A dry check ran for real. | L-11.6 | An Operation runs only once confirmed with its plan's ID. |
| An interrupted command had already acted on two clusters. | L-11.7 | Per-target reports; every request that lowers capacity is an Operation with a name. |
| A relayed directive was taken as the owner's decision. | L-14.3 | An approval is accepted only from the approver's own identity. |
| Rules changed mid-run, so earlier rulings went stale. | L-14.5 | An approval names its plan, and a changed plan turns stale. |
| Messages to a session were lost when it restarted. | L-14.1 | Subscriptions survive restarts and resume from a cursor. |
| A request channel went unwatched for long stretches. | L-14.4 | Each subscription's backlog is visible to its owner and the inventory. |
| Agents failed mid-operation, and successors could not tell what was done. | L-14.9 | Operation names as idempotency keys, and per-target reports. |
| Restart announcements were sent by hand. | L-14.7 | Subscribed events replace manual notices. |
| Clients did not coordinate their load on a shared gateway. | L-5.8 | Reservations, which the front honours. |
| Watchers died with the session that started them. | L-10.2 | Durable subscriptions resume from a cursor. |
| Alert pipes delivered late or never. | L-10.4 | At-least-once delivery, durable, with explicit gap markers. |
| Keyword sweeps missed processes, and a respawner revived killed ones. | L-15.2, L-15.1 | Deleting a ModelDeployment ends what it owns; no sweep is needed. |

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #10.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).
- 2026-10-30: Prior art and reuse filled from the state-of-the-art evaluation.
- 2026-10-30: spike S8 (PR #28) findings carried in.
- 2026-10-30: aligned with HBEP-11 becoming implementable: the API host row.
- 2026-10-30: HBEP-7's alpha takes no action that needs approval and repurposes nothing; noted under the repurpose question.
- 2026-10-30: admin operations are Kubernetes API writes in alpha, as HBEP-5 now says.
- 2026-11-14: implementable. The owner's decisions of 14 Nov 2026 on issue #10 folded in: approvers set globally and overridden per Provider, namespace and ModelDeployment, a person by default; no approval for a repurpose under policy; the audit kept for the grant, compressed when cold and deleted only after a person confirms; the API first, a CLI next and a tool server postponed. Design details written for beta on the Kubernetes API: agent identities and roles, the capacity guard, Operations with plans, release costs and confirms, ApprovalPolicy and Approvals, and the audit on the API server's log. S8's subscriptions decided for stable. Every other open question decided with its source, or left open with what decides it.

## Amendments

None.
