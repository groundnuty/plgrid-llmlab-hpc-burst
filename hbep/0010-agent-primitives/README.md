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

This HBEP decides what each primitive guarantees. The mechanisms are mostly open.

## Motivation

Agents act at stage 2 (get resources), stage 3 (keep instances running) and stage 7 (pay and give back) of the user story in HBEP-2, and read at stages 4 and 6.

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
- **Dry run and confirm:** every mutating operation can be planned without side effects, and executed only as planned.
- **Approvals:** an irreversible action runs only with an approval recorded from the approver's own identity.
- **Idempotency:** a retried operation, by the same agent or a successor, does its work once.
- **Partial failure:** an operation over several targets reports each target's outcome.
- **Audit:** every action records who asked, for whom, what was planned, who approved it and what happened.
- **Stable names:** agents address resources by framework names (HBEP-3), never by job ID, process ID, node, port or session.

### Non-goals

- Scheduling between agents or between purposes. The framework enforces what agents book; it does not rank them.
- An agent runtime, or messaging between agents.
- Replacing the provider's own access control. A provider credential still bounds what any agent can do there.
- Event types and their content. HBEP-8 owns them; this HBEP owns delivery.

## Proposal

### User stories

- An agent starting its shift runs the inventory query and sees every deployment, Allocation and Instance, which owner holds each, why, and when each Allocation ends. Allocations that a retention policy keeps with no Instance are listed too.
- An agent's session dies mid-handover. Its successor holds the same identity, finds the open operation by its idempotency key, and retries without submitting a second job.
- An agent plans the release of three Allocations on two providers. The dry run lists exactly which jobs would be cancelled. The owner approves that plan. The confirm runs it, and the report shows one cancellation failed on one provider.
- Two experiment runners reserve sessions on one deployment for the same night. The front admits each up to its booking, and neither can double the load.
- An agent subscribed to "ending soon" is restarted. It resumes from its cursor and receives the events it missed.
- After a project ends, deleting its ModelDeployments ends their Instances and cancels their pending Allocations. Each started Allocation is released as its retention policy says, and the inventory lists any still kept. No keyword sweep is needed.

### Notes, constraints and caveats

- **Owner lease is not Allocation.** HBEP-3 calls an Allocation "one lease on a provider". An owner lease is an agent's claim of responsibility for a framework resource. This HBEP always writes "owner lease".
- **Identity is a credential,** not a session, a process or a Unix user. Two agents on one host have two identities; a restarted agent keeps its own.
- **Facts come from the server.** Every answer carries the server's observation time in UTC and the server's IDs. An agent quotes them; it does not compute times or hashes.
- **The inventory includes what does not match.** Provider jobs that carry the framework's tag but have no owning resource, and resources whose provider job is gone, are listed as orphans.
- **The inventory says how fresh each fact is.** Each provider fact carries when it was last observed, so a stale view looks stale.
- **"Guaranteed delivery" (proposed meaning):** at least once, ordered per resource, durable across restarts, resumable from a cursor, each event with a unique ID for de-duplication. A subscriber that falls behind retention receives an explicit gap marker and must re-read status (HBEP-8, HBEP-11). Nothing is dropped silently, with one known exception: the lost-tail case below.
- **How, from spike S8 (issue #25, PR #28) (candidate, pending this HBEP's next status change):**
  - The log is embedded NATS JetStream with `sync_interval: always`, CloudEvents 1.0.2, and one subject per resource.
  - **Cursors are the subscriber's own.** Each subscriber keeps a cursor (stream sequence plus last event id) in the control plane and saves it after handling. A server-side durable consumer is not enough: S8 found that a message delivered, not acknowledged and then evicted vanishes from its state with no marker.
  - **Gaps and rewritten history are derived, not read from JetStream.**
    - A gap is the range of sequences evicted unread. It is found from the sequence jump, or from the stream's first sequence.
    - **The subscriber reads the whole stream and filters events on its own side.** A server-side subject filter would make other subjects' sequences look like gaps. That is S8 Finding 2, holding by construction; it was not tested.
    - A cursor whose event id no longer matches the log means rewritten history, and the subscriber re-reads status.
    - **Lost-tail exception:** that check catches a lost tail only for a cursor already past the lost range. A subscriber still behind it reads the refilled sequences as the originals, and cannot see the loss. This arises only if acknowledged writes are lost, that is, on power loss or a kernel crash, which is not tested.
  - **Handlers de-duplicate by event id,** because a crash replays recently handled events.
  - **Not yet tested:** power loss or a kernel crash, the only cases where the fsync policy differs from the default. S8 did not test them; a FUSE test without root might work but was not attempted.
- **Dry run is enforced by the server.** A dry-run request never reaches a provider, whatever the client sends.
- **Actions under an agreed spec need no approval.** The lifecycle controller (HBEP-7) cancels hedge losers, releases an old Allocation once its replacement serves, releases what over-allocation does not keep or retain, releases an Allocation when its retention policy says, and ends a deployment at its "until when" without approval, because the ModelDeployment's spec and the retention policy already agreed to them. Approvals apply to actions outside the spec. Replacement before release (HBEP-3) holds regardless of approvals while the deployment keeps its desired capacity. It does not cover shutdown at the end time, scale-down, deletion, an approved early termination or a repurpose: their Instances drain and end without a replacement, because they lower a deployment's capacity rather than keep it. Each Allocation left with no Instance then follows its retention policy.
- **Bulk actions select by label,** with a dry run that lists every match by name. There is no pattern matching on process or job names.

### Risks and mitigations

- **A relayed approval.** An approval counts only from the approver's own identity, bound to one plan; a message saying someone approved is not one.
- **An approval applied to a changed plan.** The approval names the plan. If state changes before confirm, the plan is stale and confirm refuses.
- **An owner lease expires while others depend on the resource.** Expiry must not release a serving deployment that other clients use. See Open questions.
- **Owner leases that never expire** recreate the clean-up problem. Every owner lease has an expiry, and the inventory shows leases near it.
- **Idempotency keys reused for a different request** would return the wrong result. A key reused with a different request is refused.
- **Subscriptions nobody reads** give senders false comfort. Each subscription's backlog (the stream's last sequence minus its cursor) is visible to its owner and to the inventory. Under subscriber-kept cursors, an unread subscription holds no storage: retention evicts by age or size, and the subscriber gets a gap marker.
- **Agents treat a timed-out call as failed** while it ran. Every mutating call is an operation with an ID, so the agent can ask what happened.
- **Too many approvals stall work overnight.** Only actions listed as irreversible need one; everything else is dry-run and confirm.

### Open questions

- The interface agents use: the Kubernetes API directly (HBEP-11), a framework CLI, or also a tool server for language-model agents. Lean: the API first, a CLI over it, a tool server later.
- Which actions outside a spec are irreversible. Lean: cancelling a running Allocation before its spec or retention policy says; cancelling a pending Allocation that holds queue age, other than a hedge loser; releasing the last ready Instance of a deployment before its end; deleting usage or audit records. HBEP-7 asks the same question about its own actions.
- Whether a repurpose under an agreed policy needs approval. A repurpose drains a serving Instance and moves its Allocation to another ModelDeployment (HBEP-7, open question 8). The same question covers the controller placing Instances on warm capacity by itself (HBEP-7, open question 14).
- Who may approve. Lean: approvers listed per namespace or per ModelDeployment, people only by default. At a grant's end, an approver approves an early termination or a scale-down. That is not an override of replacement before release, which applies only while desired capacity is kept (HBEP-3). Discussion with HBEP-7.
- What an owner lease's expiry does. Lean: the resource becomes "orphaned", stops taking new Allocations and emits an event; release then needs the usual approval. Another identity may take the owner lease over.
- The unit of a reservation: sessions, concurrent requests or tokens per second, and whether a booking can trigger new Allocations. Lean: concurrent sessions on a deployment for a window; future capacity is a ModelDeployment with a start time.
- How confirm binds to a plan: a hash of the plan, or the resource versions it read. Lean: the plan carries the resource versions; confirm sends the plan's ID back.
- Push or pull delivery for subscriptions. Leaning, from spike S8 (issue #25, PR #28): a long-lived ordered watch from the subscriber's stored cursor. A callback with acknowledgement needs a re-sequencer, and cannot mark ranges that were delivered, not acknowledged and then evicted. The store itself is HBEP-11's choice, and S8 makes JetStream its candidate. Still open: retention per event class, and competing consumers of one subscription, which need partitioning by resource.
- How long idempotency keys are kept, and their scope (per identity or global). Lean: per identity, at least as long as the longest operation's retries.
- Audit retention and who may read it. Lean: as long as the grant, readable by approvers.
- Whether names are ever reused for different content. Lean: never; a name refers to one resource for life.

## Design details

TBD. Decided so far: the ten primitives exist, coordination stays with agents, and the control plane is the Kubernetes API (HBEP-11). Mechanisms follow from the open questions above and from HBEP-11's choices for the durable event log and the audit store.

### Test plan

Tests for the failure modes in Motivation and Risks:
- Send a dry-run release: no provider call is made, proven by the provider plugin's call log; the plan lists every target by name.
- Change state between dry run and confirm: confirm refuses the stale plan.
- Confirm an irreversible action with an approval from a different identity than the listed approver: refused.
- Retry a submission with the same idempotency key after a client time-out: one provider job exists. Reuse the key with a different request: refused.
- Fail one provider in a release over two providers: the report shows one target done and one failed, never a single "rejected".
- Kill a client mid-operation: the operation's state is readable by ID afterwards.
- Stop a subscriber, emit events, restart it: it receives them all, in order per resource; past retention it receives a gap marker.
- Let an owner lease expire on a deployment that serves other clients: it is not released without approval.
- Let a retention policy release an Allocation with no Instance: no approval is asked, and the audit log names the policy.
- Run two agents as one Unix user with two identities: the audit log tells their actions apart.
- Create a provider job with the framework's tag and no owning resource: the inventory lists it as an orphan.

### Compatibility and upgrades

TBD. Operation and audit records outlive the components that wrote them, so their schemas are versioned from the first release.

### Security

- Agent credentials live in files readable only by that agent's Unix user, or in the control plane's secret store; never on command lines or in logs. Agents that share a Unix user are mutually trusted; separate credentials give them attribution only.
- Approvals and the audit log are writable only by the server, never directly by agents.
- Purposes are free text that others read. They must not hold credentials, prompts or private data.
- Admin operations on the front (adding and removing models) need keys (HBEP-5); this HBEP's approvals add a second gate for the irreversible ones.

### Graduation criteria

TBD. Lean: alpha when the inventory, owner leases, dry run and the audit log exist for one batch provider; beta when the Test plan passes across two providers with two agents working at once.

## Operational readiness

1. **Working:** the inventory query answers with observation times that are current, and a test subscription receives a test event. Stale observation times, or a subscription backlog that only grows, say it is not.
2. **Restart:** operations, owner leases, reservations, cursors and approvals live in the control plane (HBEP-11), so a restarted server re-reads them. Operations in flight resume or report their state.
3. **Silent failure:** a dry run that reached a provider, or a confirm that ran a different plan than the one approved. The provider plugin's call log and the audit log's plan references show the difference.
4. **Provider limits:** an inventory that reads every provider live meets login throttles. It reads the control plane's last observation and shows its age.
5. **Capacity:** a full durable event log. Reopened by spike S8: with subscriber-kept cursors, the log evicts the oldest events by retention whatever subscribers do, and subscribers behind them get a gap marker. Retention per event class sizes the log.
6. **Idle cost:** storage for the log, operations and audit; one process on the control plane's host.
7. **Rollback:** primitives can be switched off only together with agent access, since agents without them fall back to the unsafe practices in Motivation.

## Drawbacks

- Agents must carry identities and idempotency keys, and wait for approvals.
- Dry run and confirm double the calls for every mutating operation.

## Alternatives

- **A central scheduler that ranks agents' work.** It would decide whose run matters more, which the user, not the framework, knows. Rejected: coordination stays with agents.
- **Rules in agents' instructions only.** The past project fixed most of the problems above by written rules. Rules are skipped under pressure and do not bind new agents; primitives in the server do.
- **Provider access control alone.** A Slurm account says who may cancel a job, not why it runs, who approved its release, or what else belongs to the same purpose.

## Prior art and reuse

The primitives replace www-2027 practices: a shared gate on a common host (slots per class, load and memory floors, a hold file), scripts that ran dry by default and acted only with an explicit go flag, and restart notices and approvals sent by message.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| Kubernetes API (RBAC, server-side dry run, audit, `Lease`), on k3s or kcp | [k3s](https://github.com/k3s-io/k3s); [kcp](https://github.com/kcp-dev/kcp) | k3s Apache-2.0; kcp Apache-2.0 | Candidate host, as HBEP-11 decides: k3s for the team option, and the single-user host by spike S3 |
| NATS JetStream | [nats-io/nats-server](https://github.com/nats-io/nats-server) | Apache-2.0 | Candidate, confirmed by spike S8 (PR #28): an embedded log; subscriptions are subscriber-kept cursors over stream sequences, with a thin layer of our own for gaps and order |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| Pre-drain and pre-terminate hooks; `paused`, `skip-remediation` and `remediate-machine` annotations | Cluster API ([kubernetes-sigs/cluster-api](https://github.com/kubernetes-sigs/cluster-api)) | Approval gates and switches |
| Management policies; Observe-only import | Crossplane ([crossplane/crossplane](https://github.com/crossplane/crossplane)) | Orphans imported without being managed |
| Usages for deletion ordering and protection | Crossplane (beta, needs Crossplane core) | Our own kind, when Crossplane core is absent |
| A lease with a holder identity and a duration; server-side dry run | Kubernetes `Lease`; Kubernetes API dry run | Owner leases; dry run and confirm |
| Dry-run estimates of a submission | Slurm's `sbatch --test-only` | Dry-run plans |
| Queue fields `guarantee` and `capability`; Open, Closing and Closed states | Volcano | Reservation vocabulary |

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| No inventory said who had started what. | L-10.17 | One inventory query across providers. |
| Every agent ran as one Unix user. | L-11.1 | Identity is a credential, not a Unix user. |
| Our own job took nodes our pinned follow-on needed. | L-1.3 | The inventory shows every holder and purpose; reservations book capacity. |
| Kill by pattern killed other agents' work. | L-11.2 | Bulk actions select by label, never by pattern. |
| Recorded process IDs pointed at the wrong process. | L-11.3 | Agents address resources by stable names, never by process ID. |
| A dry check ran for real. | L-11.6 | The server enforces dry run. |
| An interrupted command had already acted on two clusters. | L-11.7 | Per-target reports; every mutating call is an operation with an ID. |
| A relayed directive was taken as the owner's decision. | L-14.3 | An approval counts only from the approver's own identity. |
| Rules changed mid-run, so earlier rulings went stale. | L-14.5 | An approval names its plan, and confirm refuses a stale plan. |
| Messages to a session were lost when it restarted. | L-14.1 | Subscriptions survive restarts and resume from a cursor. |
| A request channel went unwatched for long stretches. | L-14.4 | Each subscription's backlog is visible to its owner and the inventory. |
| Agents failed mid-operation, and successors could not tell what was done. | L-14.9 | Idempotency keys and operation IDs. |
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

## Amendments

None.
