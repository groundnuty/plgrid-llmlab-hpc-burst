# The agent commands: Operations, approvals and the inventory

`hpcburst scale`, `end`, `release`, `confirm`, `approvals`, `approve` and `operation` drive HBEP-10's Operations and Approvals without YAML, and `hpcburst inventory` reads what the framework holds. Each takes `--json` for agents. This page is the contract of those commands: what they send, what they print, and the shape of the JSON. The package is `internal/agentcli`; the inventory's lister and printer are in `internal/inventory`.

## The commands

| Command | What it does |
|---|---|
| `hpcburst scale <md> --instances N` | creates a `Scale` Operation for the ModelDeployment, waits for its plan, prints it, and asks to confirm |
| `hpcburst end <md> [--delete]` | the same for an `End`; `--delete` removes the ModelDeployment |
| `hpcburst release <allocation>...` or `-l <selector>` | the same for a `Release`, one Operation per ModelDeployment the Allocations belong to |
| `hpcburst confirm <operation> --plan <id>` | confirms the plan with that ID |
| `hpcburst approvals [-A]` | lists the Operations the caller may approve and has not approved yet |
| `hpcburst approve <operation> --plan <id>` | approves the plan with that ID |
| `hpcburst operation <name>` | shows the plan, the phase, the conditions and the per-target report |
| `hpcburst inventory [-n NS \| -A] [--owner USER]` | one read-only query: every ModelDeployment, Allocation and Instance the caller can read, each Provider's last read, and the Operations waiting for approval (see [The inventory](#the-inventory)) |

`scale`, `end` and `release` also take `--key`, `--purpose`, `--timeout` (default `2m`), `-n` (the namespace) and `--json`. Every command takes `--kubeconfig`, `-n` and `--json`. A positional argument may come before or after the flags.

Exit codes: `0` success, `1` a refusal or a failure (the reason is on stderr), `2` a usage error.

## The credential

Every command acts as the caller, with the caller's own kubeconfig.

- The kubeconfig is `--kubeconfig`, else `$KUBECONFIG`, and nothing else: never `~/.kube/config`, never the in-cluster configuration, never the launcher's state directory. With neither set, the command exits 2 and names both. `$KUBECONFIG` names one file.
- One SelfSubjectReview gives the caller's user name and groups. They become an Operation's `spec.owner` and an Approval's `spec.approver`, exactly.
- A ServiceAccount of `hpcburst-system`, which is a controller's identity, is refused with exit 1: a controller's credential is outside the capacity guard, so use your own identity.
- An identity outside the group `system:serviceaccounts:hpcburst-agents` is allowed, and the command prints `acting as <user>, outside the capacity guard` on stderr. This is the person with the admin kubeconfig, who passes it explicitly.
- The namespace is `-n`, else the kubeconfig's current context namespace, else `default`.

## Creating an Operation: `scale`, `end`, `release`

- **The name** is `--key` (for `release`, a prefix: see below), else `op-` and 10 random characters of `[a-z0-9]`. It is printed before the create (on stderr with `--json`), so an agent that is killed mid-call can find the Operation. A key must be a DNS-1123 subdomain, at most 253 characters.
- **Idempotency.** The name is the key. When the Operation exists, the CLI compares it with the request: the action, the ModelDeployment, the parameters and the owner. An absent `end` block equals `{delete: false}`, and a release's Allocation names are a set. The purpose is not compared. A match continues with the existing Operation, and a mismatch exits 1 with `Operation <name> exists with a different request`.
- **The wait.** The CLI waits for `status.plan`, up to `--timeout`. A `Refused` Operation prints its condition's message and exits 1.
- **Confirmation.**
  - With a terminal on stdin and stdout, and no `--json`, it asks `confirm plan <id>? [y/N]`. On `y` it confirms as `confirm` does, waits until the controller has recorded the confirm (`status.observedGeneration` reaches the generation the confirm gave the spec), and prints the phase, the Executable condition's reason and message, and, when the Operation waits, who must approve. It never waits out `--timeout` only because the phase did not move. Any other answer confirms nothing and exits 0.
  - Without a terminal, or with `--json`, it prints the plan and the line `confirm with: hpcburst confirm <name> --plan <id>`, then exits 0 with nothing confirmed. There is no `--yes`.
- **`release`.** It reads each named Allocation and groups them by `spec.modelDeploymentRef.name`, never by the `model-deployment` label, which a long name shortens. Named Allocations go into `release.allocations` (at most 64 per ModelDeployment); with `-l` the selector goes into `release.selector`, and the controller keeps only the Allocations of the Operation's own ModelDeployment. A selector that matches nothing exits 1 and creates nothing. For `release`, `--key K` is a prefix, never the whole name: each Operation is named `K-` and the first 8 hex characters of the SHA-256 of its ModelDeployment's name, even when the selection covers one ModelDeployment. The name then does not depend on how many ModelDeployments the selection covers, so a retry never makes a new Operation under another name. Each name is printed before its create. A retry of the same `release` by names recovers the Operations an earlier call made under the key before it looks for the Allocations: an Operation of the caller's, so named, whose Allocations are all among the names given. Only a name that no such Operation covers must still exist, so a retry works after one deployment's Operation has finished and its Allocation is gone. An Operation that holds an Allocation the command does not name is another request, and the key's name is then taken. A retry by selector continues on the Operations of the deployments that still have a matching Allocation; once none matches, it exits 1 and creates nothing. `scale` and `end` keep `K` exactly. A name past 253 characters is refused before anything is created.

## Confirm and approve

- `confirm` reads the Operation, requires `status.plan.id` to equal `--plan` and the phase to be `Planned` (a `Refused`, `Stale`, `Done` or running Operation is refused with its condition's message, and nothing is written), and updates `spec.confirm.planID` with the resourceVersion it just read. On a conflict it reads again and retries, at most 5 times, only while the plan ID is still `--plan`. If the ID changed it exits 1 and prints the new plan; it never confirms a plan the caller has not seen. An Operation already confirmed with the same ID exits 0, and one confirmed with another ID exits 1. It writes nothing but the Operation.
- `approvals` lists the Operations in `AwaitingApproval` with a target that needs approval under a policy naming the caller (its user, one of its groups, or its ServiceAccount) and no Approval yet from the caller for the current plan ID. `-A` lists every namespace.
- `approve` exits 1 when the plan ID differs, when the phase is not `AwaitingApproval`, or when the caller is an approver of no policy that applies to a target needing approval; the message names those policies and their approvers. There is no override: the controller still counts an Approval only under a policy that names its approver, so this refusal is a convenience and the controller's count is the authority. Otherwise it creates the Approval, named by the Operation's name cut to 242 characters (without a trailing `.` or `-`), `-`, and the first 10 hex characters of the SHA-256 of the user name, a newline and the plan ID. An Approval that exists with the same spec exits 0.

## `--json`

The output is the CLI's own structure, never the Kubernetes object. Every output carries `"schemaVersion": 1`; a change that breaks a field raises it. Times are RFC 3339 in UTC (`2026-11-20T09:00:00Z`), durations are Go duration strings (`1h30m0s`), and a field that is empty is left out. Errors go to stderr as text, with a non-zero exit, with one exception: `confirm` prints the new plan on stdout when the plan changed under it.

### Envelopes

| Command | Output |
|---|---|
| `scale`, `end`, `release` | `{"schemaVersion": 1, "operations": [Operation, ...]}`, one per Operation the command made or found |
| `operation` | `{"schemaVersion": 1, "operation": Operation}` |
| `confirm` | `{"schemaVersion": 1, "result": "confirmed" \| "alreadyConfirmed" \| "planChanged", "operation": Operation}` |
| `approvals` | `{"schemaVersion": 1, "operations": [Approvable, ...]}` |
| `approve` | `{"schemaVersion": 1, "result": "approved" \| "alreadyApproved", "approval": Approval}` |
| `inventory` | the [Inventory](#inventory-json) |

### Operation

| Field | Meaning |
|---|---|
| `name`, `namespace` | the Operation |
| `action` | `Scale`, `End` or `Release` |
| `modelDeployment` | the ModelDeployment's name |
| `scale` | `{"instances": N}`, on a `Scale` |
| `end` | `{"delete": bool}`, on an `End`; an Operation with no `end` block shows `false` |
| `release` | on a `Release`: `{"allocations": [name, ...]}` or `{"selector": "<label selector>"}` |
| `owner` | the requester's user name |
| `purpose` | the purpose annotation, when set |
| `confirmedPlan` | the plan ID the owner confirmed, when confirmed |
| `phase` | `Planning`, `Planned`, `AwaitingApproval`, `Executing`, `Done`, `PartlyFailed`, `Stale` or `Refused` |
| `conditions` | the status conditions, each `{"type", "status", "reason", "message", "lastTransitionTime"}` |
| `plan` | the Plan, once the controller wrote one |
| `targets` | the report: one outcome per committed target (below) |
| `finishedAt` | when the Operation finished, when it did |
| `confirmWith` | `hpcburst confirm <name> --plan <id>`, on an unconfirmed Operation that has a plan |

A target outcome is `{"kind", "name", "uid", "state", "reason", "message", "lastTransitionTime"}`. `state` is `NotStarted`, `Done` or `Failed`; `reason` and `message` say why a target failed.

### Plan

| Field | Meaning |
|---|---|
| `id` | the plan's ID, opaque: quote it, never parse it |
| `observedAt` | when the controller read the state the plan describes |
| `modelDeployment` | `{"name", "uid", "generation"}` |
| `targets` | the Plan targets |

A plan target is `{"kind", "name", "uid", "provider", "effect", "needsApproval", "approvalRule", "policies", "cost", "estimate"}`.

- `kind` is `Allocation`, `Instance` or `ModelDeployment`; `effect` is one of `CancelPending`, `DrainInstance`, `DrainLastReady`, `EndStarting`, `ReleaseKept`, `ReleaseStarted`, `Keep`, `SetEndTime` and `DeleteDeployment`.
- `policies` lists each ApprovalPolicy that applies to a target that needs approval: `{"name", "approvers": [Approver, ...]}`. An approver is `{"user": "..."}`, `{"group": "..."}` or `{"serviceAccount": {"namespace": "...", "name": "..."}}`.
- `cost` is present on a target that lowers capacity. `estimate` is present on a target that has both a `cost` and a `provider`: a target with no Provider, such as a ModelDeployment, can have no estimate.

### Cost

Each field is left out when it does not apply to the target.

| Field | Meaning |
|---|---|
| `note` | words for what is given up where no figure applies |
| `leaseLeft` | a started Allocation's hard end minus the plan's `observedAt`: a Figure with `duration` |
| `gpuHoursLeft` | the GPU-hours that leaves: a Figure with `gpuHours`, a decimal string such as `"36.0"` |
| `queueAge` | a pending Allocation's age in the queue: a Figure with `duration` |
| `startEstimate` | a pending Allocation's own estimate of its start: a Figure with `time`, always `advisory` |
| `lastReady` | `true` when the target is the deployment's last ready Instance: the model serves nothing on its own capacity until a new Allocation starts and its Instance is ready |
| `queueWaits` | the Provider's observed waits: `{"median", "p90", "max", "count", "observedAt"}`, or `{"unknown": "<reason>", "observedAt": ...}` |
| `startToReady` | the class's last start-to-ready time: a Figure with `duration` |

A **Figure** is one value (`duration`, `time` or `gpuHours`) or `unknown`, plus `observedAt` (when its inputs were read) and `advisory` (`true` on an estimate). **An unknown figure is `{"unknown": "<reason>"}`, with its `observedAt`; it is never a zero.** A figure is a snapshot at its `observedAt`: it is not recomputed, so read its age from it.

The plan's own `cost.estimateNow` is not shown. It stays unknown in the plan, and the `estimate` of the target replaces it.

### Estimate

The estimate is Slurm's advisory start time for the same job submitted now. It has three states, derived by the CLI.

```json
{"state": "known" | "pending" | "unknown", "time": "...", "unknown": "...", "observedAt": "...", "age": "...", "advisory": true}
```

- `known`: `time` is the estimate, `observedAt` is when it was read, and `age` is now minus that.
- `pending`: no estimate yet, and the plan is at most 60 s old.
- `unknown`: `unknown` says why.

`advisory` is always `true`: estimates swing by hours (HBEP-4, Risks). In the text output the same states read `estimate: <time> (advisory, read <age> ago)`, `estimate: pending` and `estimate: unknown: <reason>`.

#### The rule, for a reader of the API

An estimate entry belongs to a plan ID, a Provider and the targets it covers, and holds either a start time or an unknown reason, with the time it was read. For one target of a plan, observed at `plan.observedAt`, at a time `now` read once:

1. An entry for another plan ID is ignored, and so is an entry of another Provider, and an entry that lists targets and not this one.
2. With no entry, the estimate is `pending` while `now - plan.observedAt` is at most 60 s, and `unknown` with the reason `no estimate within 60 s` after that.
3. An entry that carries an unknown reason is a final `unknown` with that reason. It is never `pending`, whatever its age.
4. Otherwise the estimate is `known`: the entry's time, `advisory`, with its age, `now` minus the entry's read time.

With several entries for one target the latest read wins. Until HBEP-10's beta step 4 stores estimates, no entry exists, so every estimate reads `pending` and then `unknown`.

### Approvable (`approvals`)

`{"namespace", "name", "action", "modelDeployment", "owner", "planID", "targets", "approveWith"}`. Each of `targets` is `{"kind", "name", "effect", "policies": [name, ...]}`: a target that needs approval and the policies that name the caller. `approveWith` is `hpcburst approve <name> --plan <id>`.

### Approval (`approve`)

`{"name", "namespace", "operation", "planID", "approver": {"user", "groups"}}`. `approver` is what the API server reported for the caller.

## The inventory

`hpcburst inventory [-n NS | -A] [--owner USER] [--json]` is one query that reads the API and stores nothing. It lists what the caller can read, in four lists and a view of the Providers (HBEP-10, Inventory): every ModelDeployment, Allocation and Instance with its owner and purpose, the Allocations with their phase, Provider, hard end, retention and idle anchor, each Provider's last successful read with its age, and the Operations in `AwaitingApproval`. The package is `internal/inventory`: a lister that reads through the caller's client and returns rows, and a printer that turns rows into text or JSON. It takes the credential of every command above (`--kubeconfig`, else `$KUBECONFIG`, a controller's credential refused) and sends only reads: `list` calls, and the one SelfSubjectReview that says who the caller is.

- **Scope.** `-n` names the namespace, else the kubeconfig's current context namespace, else `default`. `-A` lists every namespace, with one cluster-wide list per kind; the API server answers it only for a caller allowed to list that kind cluster-wide, and refuses it otherwise (exit 1, naming the kind). `-n` and `-A` together exit 2. Providers and PlatformProfiles have no namespace and are always read whole.
- **`--owner USER`.** It selects by owner and leaves the rest of the query as it is. ModelDeployments are those whose owner annotation equals `USER`. Allocations and Instances are those with the label `hpcburst.groundnuty.github.io/owner-hash` equal to `u-` and the first 12 hex characters of the SHA-256 of `USER`, sent to the API server as a label selector. Operations are those whose `spec.owner` is `USER`. Providers are not owned and are all listed. An empty `USER` exits 2. A ModelDeployment with no owner annotation is listed in the full view with its owner `unknown`, and only `--owner` leaves it out (`@hpc-burst-science-agent`, 17 Nov 2026, #662, S5). Allocations and Instances that the lifecycle controller created before it began to copy the owner carry no label and no annotation, so they read `unknown` and `--owner` leaves them out: nothing is backfilled, and each is replaced in time.
- **Owner and purpose on a child are the child's own.** An Allocation or an Instance shows the annotations it carries. It never borrows its ModelDeployment's, so an object that predates the copy reads `unknown` under a deployment that has an owner.
- **The clock** is read once per command. Every age is that time minus a recorded time.
- **Order.** Each list is sorted by namespace, then name; Providers by name.

Exit codes are the commands' above: `0`, `1` for a failure to read (the message names the kind and the API server's answer), `2` for a usage error.

### What each row shows

An `unknown` is a fact the framework should hold and the API does not show yet. A field that does not apply is left out. In the text, an unknown reads `unknown (<reason>)`.

| Row | Shows |
|---|---|
| ModelDeployment | namespace and name; owner and purpose, from its annotations; `spec.instances`; the number of ready Instances, the controller's own count in `status.readyInstances`; `spec.endTime`; the Providers it uses |
| Allocation | namespace and name; its ModelDeployment, by `spec.modelDeploymentRef`; owner and purpose from its OWN annotations; the phase; the Provider, by `spec.providerConfigRef`; the hard end, `status.atProvider.hardEnd`; the retention; the idle anchor; the marker `kept, no Instance` |
| Instance | namespace and name; owner and purpose from its own annotations; the phase; its Allocation, by `spec.allocationRef` |
| Provider | name; `status.lastSuccessfulStatusRead` and its age |
| Operation | namespace and name; its ModelDeployment; the action; the plan ID; the owner (`spec.owner`) |

- **The Providers a ModelDeployment uses** are those of its Allocations that are not Ended, Failed, Lost or Refused, joined by `spec.modelDeploymentRef` (never by a label, which a long name shortens). A deployment with none lists none: `[]`.
- **The retention** is the Allocation's own policy: `mode` is `Keep` or `ReleaseWhenIdle`, and an unset mode is `Keep`. `until` is the policy's date and is left out when it sets none: a kept Allocation then runs to its deployment's end time, which the deployment's row shows. `idleLimit` is the policy's, left out unless it sets one. `profileMaxIdle` is the `maxIdle` of the PlatformProfile of the Allocation's Provider, which releases even a kept Allocation after that long idle; it is left out when the profile sets none, and `unknown` when the Provider or its profile cannot be read.
- **The idle anchor** `idleSince` is the Allocation's `idle-since` annotation: the time it began to host no Instance. It is left out when the annotation is absent, which means the Allocation is not idle. An annotation that is not an RFC 3339 time reads `unknown`.
- **The marker** `keptNoInstance` is `true` for an Allocation whose job runs (phase `Running`), whose mode is `Keep`, that carries the idle anchor, and that no Instance of the listing that is not Ended is on. It is how a kept Allocation with no Instance stands out. Both the anchor and the listing must say so: under `--owner` the listing holds only Instances that carry the label, so the anchor keeps an old, unlabelled Instance from making an Allocation look empty.
- **The age** of a Provider's read is the command's clock minus `lastSuccessfulStatusRead`, whole seconds, as a Go duration (`1m30s`). A Provider that was never read shows both as `unknown`, and a read time after the command's clock shows the age as `unknown`.

### Inventory (JSON)

Times are RFC 3339 in UTC and durations are Go duration strings, as above. A value that may be unknown is its value, or `{"unknown": "<reason>"}`; it is never empty or zero. A field that does not apply is left out, and the lists are always arrays, `[]` when empty.

```json
{"schemaVersion": 1, "observedAt": "...", "scope": {"namespace": "..."} | {"allNamespaces": true}, "selectedOwner": "...",
 "modelDeployments": [...], "allocations": [...], "instances": [...], "providers": [...], "operationsAwaitingApproval": [...]}
```

`observedAt` is the clock the command read once. `selectedOwner` is the `--owner` value, and is left out without it.

**modelDeployments**

| Field | Meaning |
|---|---|
| `namespace`, `name` | the ModelDeployment |
| `owner`, `purpose` | the annotations; `{"unknown": "no owner annotation"}` and `{"unknown": "no purpose annotation"}` when absent or empty |
| `instances` | `spec.instances` |
| `readyInstances` | the number of ready Instances; `{"unknown": "the controller has not reported on the ModelDeployment yet"}` before the controller's first status write |
| `endTime` | `spec.endTime`, left out when the deployment has none |
| `providers` | the names of the Providers it uses, sorted, each once |

**allocations**

| Field | Meaning |
|---|---|
| `namespace`, `name` | the Allocation |
| `modelDeployment` | the name in `spec.modelDeploymentRef`, or `{"unknown": "the Allocation names no ModelDeployment"}` |
| `owner`, `purpose` | the Allocation's own annotations, as above |
| `phase` | the observed phase, or `{"unknown": "no phase reported yet"}` |
| `provider` | the name in `spec.providerConfigRef`, or `{"unknown": "the Allocation names no Provider"}` |
| `hardEnd` | `status.atProvider.hardEnd`, or `{"unknown": "not observed yet"}` |
| `retention` | `{"mode", "until", "idleLimit", "profileMaxIdle"}`: `mode` is always there; the others only when they apply (above) |
| `idleSince` | the idle anchor (above) |
| `keptNoInstance` | `true` for the marker, otherwise left out |

**instances**: `{"namespace", "name", "owner", "purpose", "phase", "allocation"}`, with `phase` unknown as an Allocation's is and `allocation` the name in `spec.allocationRef`.

**providers**: `{"name", "lastSuccessfulStatusRead", "age"}`. Each of the last two is a time or a duration, or `{"unknown": "never read"}`.

**operationsAwaitingApproval**: `{"namespace", "name", "modelDeployment", "action", "planID", "owner"}`, one per Operation in phase `AwaitingApproval`. An Operation in any other phase is not listed. `planID` is `{"unknown": "the Operation has no plan"}` when the status holds none, which cannot happen in `AwaitingApproval`.
