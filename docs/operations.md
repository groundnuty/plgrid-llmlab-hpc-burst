# Operations

This guide is for running hpcburst day to day: what `hpcburst status` says, what each condition on a ModelDeployment, an Instance or an Allocation means, what the controllers do about it, and what you can do. It follows the code on `main`; each entry names the HBEP section that holds the rule.

Start from a host set up as in the [quickstart](quickstart.md). Every command here assumes its two directories and its alias, set in the shell you work in (quickstart, steps 2 and 4):

```
STATE=${XDG_STATE_HOME:-$HOME/.local/state}/hpcburst
BIN=${HPCBURST_BIN_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/hpcburst/k3s}
alias hbk='KUBECONFIG="$STATE/kubeconfig" "$BIN/k3s-1.36.5/k3s" kubectl'
```

`<name>` stands for a ModelDeployment, `<instance>`, `<allocation>` and `<provider>` for the objects of those kinds, and `<front-host>` for your dial-home address (`host-dialed` mode has none). The examples live in the `default` namespace, so the commands give none. Commands in blocks marked `cluster` run on a cluster's login node; the rest run on the control-plane host.

## Where to look

- **The host:** `hpcburst status` (next section). It answers for k3s, the controllers, the front, frps and the agent builds, never for a deployment.
- **A deployment:** its conditions, which are the level truth (HBEP-11). Describing an object prints them with its recent events:

  ```
  hbk get modeldeployments
  hbk describe modeldeployment <name>
  hbk get instances,allocations
  hbk describe instance <instance>
  hbk describe allocation <allocation>
  hbk describe provider <provider>
  ```

- **Events** are a best-effort mirror of the conditions, kept 7 days ([HBEP-8](../hbep/0008-observability-and-events/README.md), Events and Retention). Each carries its HBEP-8 type in `action` and the reason code in `reason`; `degraded` and `failing` are `Warning` events. Events about a Provider or a PlatformProfile, which have no namespace, land in `default`. A `degraded` event about a deployment regards the ModelDeployment; once the ModelDeployment is gone, the events of `VerificationFailed` and `CapacityDropped` regard the Instance instead ([HBEP-8](../hbep/0008-observability-and-events/README.md), Events).

  ```
  hbk get events --sort-by=.metadata.creationTimestamp
  ```

- **Logs:** each process the host runs writes to `logs/` in the state directory: `k3s.log`, `supervisor.log`, `controller.log`, `front.log` and `frps.log`.

## Reading `hpcburst status`

```
bin/hpcburst status --state-dir "$STATE"
```

`status` exits 0 when the host is healthy, 1 when it is not, and 3 when it is stopped ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Single-user host). Give it the same `--state-dir` as `up`; without it, it reads the launcher's default directory. `--json` prints the same report as JSON. `--no-api` skips the questions asked of the API server (its `/readyz`, the controller manager's `/healthz`, the write probe, the garbage-collector probe and the presence of the CRD deletion guard) and still checks the processes, their sockets and the supervised processes' own health ports. `--no-gc-probe` skips the garbage-collector probe only.

```
bin/hpcburst status --state-dir "$STATE" --json
bin/hpcburst status --state-dir "$STATE" --no-api
```

On a state directory that does not exist, it prints this and exits 3:

```text
stopped
  - no state directory: nothing was started here
```

On a directory that exists but holds no record of a start, such as one after `hpcburst down`, the reason differs, and the exit code is 3 too:

```text
stopped
  - no launcher record: no host is running from this state directory
```

### The lines, in the order they print

The first line is the verdict: `healthy`, `unhealthy` or `stopped`. Below it, one `  - ` line names each reason the host is not healthy. Every check below that fails adds one. The rest of the report follows:

| Line starts with | What it says |
|---|---|
| `k3s <version> in <dir>` | the k3s release the record runs, and the state directory read |
| `process k3s pid <n>:`, `process supervisor pid <n>:` | each recorded process, checked through a pidfd by PID, start time and boot ID: `ours`, `gone`, `foreign` or `unknown`. Anything but `ours` makes the host unhealthy |
| `listeners held by the process:` | the loopback ports k3s holds. A port of the block held by another process is a reason |
| `executable:` | the binary k3s runs. It must be under its own data directory: a user-level k3s on a machine with a system k3s can run root's files instead ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Single-user host) |
| `event TTL:` | how long the API server keeps events: `168h`, or `1h (default; restart to apply 168h)` on a host an earlier release started. That host is still healthy |
| `RSS <n> MiB` | k3s's resident memory |
| `apiserver /readyz:`, `controller manager /healthz:`, `write probe:`, `garbage-collector probe:` | the four API checks, `true` or `false` (absent under `hpcburst status --no-api`). The write probe writes an object; the garbage-collector probe makes an owner and a dependent and waits up to a minute for the dependent to go |
| `supervisor pid <n>: running=` | the process that keeps the controllers, the front and frps running and restarts any that exits |
| `supervised controller:`, `supervised front:`, `supervised frps:` | one line per supervised process: whether it runs, its PID, its state (`starting`, `running`, `backoff` after an exit, `stopped`), its restart count, its `/healthz`, the controller's readiness, and its last exit with code and time |
| `agent builds (<dir>):` | one line per CPU architecture that an applied PlatformProfile's partitions have, with the agent and frpc builds (below) |
| `CRD deletion guard:` | `present (hpcburst-protect-crds)` when the admission policy exists and a binding names it with a denying action, or `ABSENT (<what is missing>)` with the warning that nothing then stops a delete of a framework CRD, which would delete every resource of its kind. Absent is a state, not a fault: the host stays healthy, as with an absent front, and the next `hpcburst up` installs the guard, so a host that an earlier release started shows it absent until then. `status` asks only a host that every other check found healthy, so `hpcburst status --no-api` and an unhealthy host print no such line; a failure to ask adds a reason and makes the host unhealthy. `hpcburst status --json` has it as `crd_guard` ([What never to do](#what-never-to-do) says how to remove the guard) |
| `front:` | the recorded dial-home mode and address (`ssh <front-host>`; `host-dialed (no address: the host dials the jobs)` in host-dialed mode), `absent` when the host was started without `hpcburst up --front-config`, or `not recorded` |

`controllers: none (no controller binary was found when the host started)` replaces the supervisor's lines when `up` found no `hpcburst-controller` beside itself. That host runs k3s only.

### The controller's readiness

Once the controller's `/healthz` answers, the `supervised controller:` line ends with `readiness:` and one of five states. `status` reads the lifecycle controller's two named checks, `/readyz/caches` and `/readyz/leading`, by status code only ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Single-user host; [HBEP-8](../hbep/0008-observability-and-events/README.md), Observers; `internal/launcher/readyz.go`):

| State | Means | What to do |
|---|---|---|
| `ready` | both checks pass: its caches have synced and it leads its `Lease` | nothing |
| `starting` | its caches have not synced yet | wait; it follows every start and restart |
| `not leading` | its caches have synced, and this copy has not been elected leader of the `lifecycle` `Lease` in `hpcburst-system`. That alone does not show that another copy holds the `Lease` | wait a `Lease` duration, as after a restart; if it stays, read the `Lease`'s `spec.holderIdentity` (below): a holder other than this controller is another copy, such as a stale one an earlier launcher left running |
| `starting, or not leading its Lease: /readyz does not say which` | a controller from before the split check answered its aggregate `/readyz` with an error | rebuild `bin/` and restart the host (Down and up across an upgrade) |
| `not read` | the readiness paths could not be read at all; the reason line names the error | read `logs/controller.log` in the state directory |

Any state but `ready` makes the host unhealthy. A controller that does not lead cannot act, since alpha runs one replica.

```
hbk get lease lifecycle -n hpcburst-system -o yaml
```

### The front's line

`supervised front:` adds `degraded: usage writes failing` while the front's `/healthz` states its usage sink as failing: usage records are being lost, and `logs/front.log` says why. It adds `usage state unknown` when the `/healthz` body has no usage member, or one with a state HBEP-8 does not name (`internal/launcher/status_controllers.go`); that says nothing about whether records are written. Neither makes the host unhealthy: the front keeps serving, and `status` still exits 0 ([HBEP-8](../hbep/0008-observability-and-events/README.md), Usage records).

The last line, `front:`, is about the dial-home mode and address, not the process. `front: absent` means no front runs under this host, which is a state, not a fault. A front that was configured and is not running is listed as a supervised process that does not run, and makes the host unhealthy.

### Store-full hints

When the write probe fails, `status` adds a hint about the disk, because k3s's SQLite store waits rather than failing a write when its disk is full, so the probe times out with an error that says nothing about the disk (#192). The hint reads the free space of the store's own directory, `data/server/db` under the state directory: the blocks an unprivileged process may take, compared with 64 KiB, the launcher's threshold for "nearly full" (`internal/launcher/storefull.go`). The reason line then ends with one of:

- `store full (no free space on <dir>)`: no block is left for an unprivileged process;
- `store nearly full (<n> KiB free on <dir>)`: less than 64 KiB is left;
- `the free space on <dir> is unknown: <error>`: the space could not be read.

The hint points at the disk; it does not prove the disk is the cause, and its absence does not rule the store out: with more than 64 KiB free, the probe failed for a reason the hint does not see, and `logs/k3s.log` is the next place to look. When the disk is full, free space on that volume; nothing new is submitted while writes fail, and running jobs keep running to their hard ends ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Operational readiness 5). A write that reported an error may still have been stored, so check what you applied before you apply it again.

### The agent builds

Once a PlatformProfile is applied, `status` checks `agent-builds/` in the state directory for each CPU architecture its partitions have: `hb-agent-<cpuArch>` and `frpc-<cpuArch>`. Each shows `ok` with its digest, `MISSING`, `UNREADABLE`, `WRONG` (an agent built for another machine) or `MISMATCH` (found against expected digest). Any but `ok` makes the host unhealthy, because the first submission there would be refused, and `up` exits 4 while leaving the host running. Rebuild with the commands of [quickstart step 2](quickstart.md#2-build-and-fetch-the-binaries); the reason line names the one to run:

```
hack/build-agents.sh "$STATE/agent-builds"
hack/fetch-frp.sh --agent-builds "$STATE/agent-builds"
```

The line reads `not checked` with the cause when the API server was not asked (`hpcburst status --no-api`, or an unhealthy host). A Provider whose partition has no build also shows on the deployment, as `ProviderUnavailable`.

## Why a ModelDeployment is degraded

A served ModelDeployment carries three conditions ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Events and conditions):

- `Ready`: `EnoughReady` when at least as many Instances are ready as desired, else `NotEnoughReady`;
- `Progressing`: `Provisioning` while the controller creates, starts or replaces Instances, else `Stable`;
- `Degraded`: `AsDesired` when false, and one of the twelve reasons below when true.

When several reasons apply, `Degraded` carries the first in the order below, HBEP-7's ranking (Events and conditions, The ranking), and its message lists every reason that applies, each with its own messages: `SubmitLimitReached: <message> | CapacityDropped: 0 of 1 desired Instances are ready`. Read the whole message; the top reason is often not the only one.

A ModelDeployment with a managed source (an entry LLMLab serves) gets none of these. Its `Progressing` is false with `ManagedSource`, since it has no Instances by design, and its `Ready` is true with `CatalogEntryListed`, or false with `CatalogEntryUnlisted`, `CredentialsRejected` or `ProviderNotOnPlatform` ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Reconcile loop, step 1).

### `SpecUnsupported`

- **Means:** the spec asks for something alpha does not run. The message names it: a `data-parallel-size` above 1; a parallel size (`data-parallel-size`, `tensor-parallel-size` or `pipeline-parallel-size`), `max-num-seqs` or `max-model-len` that is not a positive integer; a `generation-config` other than `auto`, or `override-generation-config` or `hf-config-path`; an engine flag that the framework's launch sets itself; a missing `spec.workload`, or a workload value out of range; a workload shape that does not fit the declared `max-model-len`; a `leaseLimit` that does not parse; or a GPU count (`tensor-parallel-size` times `pipeline-parallel-size`) above the `gpusPerNode` or QOS GPU cap of every allowed Provider that is usable and not too short, when the Instance is first placed. A Provider that is unusable, or `PartitionTooShort`, does not count (`internal/lifecycle/handover.go`, `noProvider`). It is also set for a pending Instance, one created that still waits for its Allocation, whose own Provider can no longer host its GPU count, because the Provider's limits changed since: the message then starts "Instance <name> waits for its Allocation" (`internal/lifecycle/provision.go`, `ensureAllocation`).
- **The controller** creates no Allocation for the deployment while it holds, replacements included. Instances already running serve until they drain.
- **You:** edit the spec as the message says. For a GPU count, making a Provider that fits usable again also clears it, with no spec edit. The next pass reads it again ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Provisioning, and What the controller reads).

### `PartitionTooShort`

- **Means:** an allowed Provider's time limit is not longer than the lead an Instance needs. The time limit is the smallest of the partition's `maxTime`, the QOS's `maxWall` and the deployment's `leaseLimit`; the lead is the drain window plus `handoverMargin` plus `health.startTimeout`. The message names the partition, the limit, the bound that set it and the lead.
- **The controller** skips that Provider and creates no Allocation there. The reason stays while the Provider is allowed, even when another Provider serves the deployment.
- **You:** raise `spec.leaseLimit`, or shorten the drain window with a shorter `spec.routing.maxRequestDuration` (the window is that plus the profile's `drainMargin`, when shorter than `drainWindow`), or remove the Provider from `spec.providers`. With the shipped profiles Athena's `plgrid-now` is always too short: its 1 h limit is below the 1 h 45 min lead even with a 5-minute `maxRequestDuration` ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Provisioning, A partition too short).

### `FrontNotConfigured`

- **Means:** `hpcburst up` was given no `--front-mode` (and, in `direct` and `ssh` modes, no `--front-addr`), so the jobs would have no route home.
- **The controller** submits no Allocation for any deployment.
- **You:** stop the host and start it again with the dial-home flags of your mode, as in the quickstart's step 4. `--front-mode host-dialed` alone is enough in host-dialed mode. `up` does not remember them across `down` (Down and up across an upgrade) ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Single-user host).

### `VerificationFailed`

- **Means:** an Instance's verification found that the engine runs other settings than the spec declares (a settings contradiction), or that a workload shape does not fit the context limit the engine reported (a shape misfit). The message names the accelerator class and the verification record, `ver-<instance UID>-<generation>`.
- **The controller** ended that Instance at once with `VerificationFailed`, released its Allocation, and stopped provisioning in its class. A contradiction stops the class until the serving spec changes. A shape misfit stops it until the spec changes or the workload's resolved shapes change, and a change that makes the shapes fit lifts the stop with a `ready` event `ShapesFit`. The exception is a misfit whose verification record holds no context limit to test the shapes against: only a spec change lifts that one, and the message says so. A verification of this release always records the limit, so the exception applies only to a record without one (`internal/lifecycle/verify.go`, `stopLifts` and `classBlocked`; `judge.go`; [HBEP-7](../hbep/0007-lifecycle-controller/README.md), Instance start, verifying).
- **You:** read the record, which holds the settings the engine reported and the verdict:

  ```
  hbk get controllerrevisions -l hpcburst.groundnuty.github.io/record=verification
  hbk get controllerrevision <record> -o yaml
  ```

  Declare the value as the engine reports it (a model hook can rewrite one, HBEP-7's known limit), or fix the workload. The message says what lifts the stop: "until the serving spec changes" for a contradiction, and for a misfit whose record holds no context limit; "until the serving spec or the workload's resolved shapes change" for any other misfit, for which a `workload` edit that makes the shapes fit is enough.

### `IdentityUnresolved`

- **Means:** the deployment ID of an accelerator class cannot be computed. Either the Hub failed permanently (401, 403, 404, a size or blob mismatch, a bad token file), or the identity record `id-<first 12 hex>` already holds another full ID: a collision. The message names the cause.
- **The controller** creates no Allocation in that class. It evaluates a Hub failure again when the spec or the Hub token changes, and at the periodic resync; a collision only when the spec or the identity records change. A transient Hub failure (a transport error, 429, 5xx) sets no reason and is retried with backoff ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Reconcile loop, step 2; [HBEP-3](../hbep/0003-resource-model-and-naming/README.md), Deployment ID).
- **You:** fix the Hub token or the checkpoint revision. For a collision, read the record the message names:

  ```
  hbk get controllerrevision <record> -o yaml
  ```

### `RestartLoop`

- **Means:** three Instances in a row in one accelerator class ended failed, any end but `Drained`, since an Instance of that class was last ready, and the last three fell within the profile's `restart.window`. A refused submission ends no Instance, so it never counts.
- **The controller** pauses provisioning in that class: `restart.backoff`, doubled for each failure beyond the third, at most `restart.window`. The message names when the pause ends, and a `degraded` event `RestartLoop` fires once per streak ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Health and restarts, Restart and provisioning loops).
- **You:** find why the Instances failed: each ended Instance's end reason, in `hbk get instances` and `hbk describe instance <instance>`, with the table under [The Instance](#the-instance). The pause lifts by itself. The streak lasts until an Instance of the class becomes ready again, so a fault that persists adds to the same streak: a further failure within `restart.window` of the two before it pauses the class again, for twice as long, up to `restart.window` (`internal/lifecycle/health.go`, `classStreak` and `loopPaused`).

### `SubmissionRejected`

- **Means:** a Provider's last submission attempt for an Allocation was refused by Slurm for a reason other than a full submit limit. The message carries Slurm's reason, such as a used-up grant or a limit the profile does not type.
- **The controller:** the Provider's controller retries with backoff. The lifecycle controller does not switch Providers by itself in alpha, and nothing counts toward `RestartLoop` ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Provisioning; [HBEP-4](../hbep/0004-provider-interface/README.md), QOS limits).
- **You:** fix what Slurm names; `hbk describe allocation <allocation>` shows the attempt. The next retry submits once the cause is gone.

### `NoOverlapSlot`

- **Means:** a primary Instance reached its drain start with no replacement beside it, because no allowed Provider could run the replacement at the same time: no submit slot, or the two would exceed the QOS's per-user GPU, CPU or memory cap. Helios's QOS `now`, one GPU per user, is the case. It holds from the drain start until the successor is ready, while fewer Instances are ready than desired.
- **The controller** plans this gap: the primary drains as usual and no request in flight is cut. The successor is created at its planned time and pends until the predecessor's job frees the GPU, or, under a submit limit of 1, is created once the slot is free. Meanwhile the front sends the model's new requests to any target still eligible, another ready Instance or a managed entry of the same model; only when none is eligible does it answer 503 with `Retry-After: 30` and `model_inactive` (`internal/front/route.go`). `ReplacementLate` is not set in this mode ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Handover, No-overlap mode).
- **You:** nothing is broken. The gap lasts until the drained primary's job frees its GPU. Under the default `Keep` that job holds it to its hard end, or to the policy's `until` or the deployment's `endTime`. Each Allocation keeps the retention policy it was created with, a copy of the deployment's, and retention reads that copy (`internal/lifecycle/provision.go`, `retention.go`), so editing the deployment changes only Allocations created after the edit. To shorten the current gap, act on the predecessor's Allocation, the drained Instance's `spec.allocationRef.name`, once that Instance has ended: delete it, which cancels its job at once, or switch it to release when idle:

  ```
  hbk delete allocation <allocation>
  hbk patch allocation <allocation> --type=merge -p '{"spec":{"retentionPolicy":{"mode":"ReleaseWhenIdle","idleLimit":"2m","until":null}}}'
  ```

  For later gaps, set the same `retentionPolicy` on the deployment. In alpha the Helios Provider serves one deployment, and that deployment allows the Helios Provider alone.

### `SubmitLimitReached`

- **Means:** either Slurm refused a submission for a full submit limit (its reason name is `QOSMaxSubmitJobPerUserLimit`, `AssocMaxSubmitJobLimit`, `MaxSubmitJobsPerAccount`, `QOSGrpSubmitJobsLimit` or `AssocGrpSubmitJobsLimit`), or the controller's own count found no allowed Provider with a free slot for a missing Instance. The message names the Provider, the limit and Slurm's reason line. In `host-dialed` mode the count also holds a cap of 2 Allocations, pending or running, for each Provider, and its message names the limit as `maxSubmitPerUser 2 (host-dialed mode, HBEP-6)`; a QOS limit of 2 or less keeps its own QOS in the message ([HBEP-6](../hbep/0006-instance-reachability/README.md), Dial-home modes, Capacity).
- **The controller:** a refused Allocation stays `Submitting` and is retried after the Rejected backoff; with no free slot, no Allocation is created until one frees. Neither is a failure, and neither counts toward `RestartLoop` ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Provisioning, Per-user limits; [HBEP-4](../hbep/0004-provider-interface/README.md), QOS limits).
- **You:** see [A Provider refusing for a full slot](#a-provider-refusing-for-a-full-slot).

### `ProviderUnavailable`

- **Means:** an allowed Provider that is not a managed one cannot take an Allocation, and was skipped, even while another Provider serves the deployment. The message names why: it does not exist; its PlatformProfile does not exist or does not validate; its platform is not a batch one; it names a `qos` that the profile does not list; it allows no partition, or one its profile does not describe; it is not `Ready`; no agent build exists for its partition's CPU architecture; or the profile cannot size the partition, because it gives no per-GPU values (`gpusPerNode`, or the QOS's `maxGpusPerUser`, with `cpusPerGpu` and `memPerGpu`), no time limit at all, a `maxTime` that does not parse, or a QOS cap that leaves an Allocation no CPU or no memory. When a needed Instance cannot be placed and no other reason explains it, as with a selection that matches no batch Provider, it reads "no allowed batch Provider can take a new Allocation" (`internal/lifecycle/topology.go`, `provision.go`, `handover.go`).
- **The controller** skips that Provider and tries the next one allowed. A served deployment ignores managed Providers, even when their labels match ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Reconcile loop, step 1).
- **You:** look at the Provider:

  ```
  hbk describe provider <provider>
  ```

  `Ready` false with `ReadsFailing` means the Provider's controller has read the platform successfully for none of the last 5 minutes. A stopped Provider controller cannot write that, so the lifecycle controller judges readiness by the age of `status.lastSuccessfulStatusRead`, and a stored `Ready` true can be stale. The Provider's other conditions usually say why reads fail: `CredentialsInvalid` (the credentials file or its key is readable by others, or not yours), `PathUnresolved` (HOME or the staging path is unusable on the login node), `DialectMismatch`, or `DialHomeKeyMissing` (in ssh mode, a dial-home key file is missing on the platform, and nothing is submitted). In `host-dialed` mode, `Ready` false with reason `LoginHostInUse` means that another Provider holds the login host, which the message names, and only one host-dialed Provider runs per login host ([Host-dialed relays](#host-dialed-relays)). For a missing build, see [The agent builds](#the-agent-builds).

### `ReplacementLate`

- **Means:** a replacement's Allocation had not started by its predecessor's drain start minus the class's start-to-ready time, so it cannot be ready before the drain starts. The message names the Instance and its drain start.
- **The controller** emits a `degraded` event `ReplacementLate` once, and keeps the drain rule: the predecessor still drains at its hard end minus its drain window, ready replacement or not, so capacity can drop ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Handover, Lateness).
- **You:** see why the replacement waits (`hbk describe allocation <allocation>`, and its job's reason on the cluster). The plan takes the 90th percentile of the Provider's last 20 queue waits (the largest, with fewer than 20), so a queue that has grown since leaves plans late until the waits catch up. Allowing another Provider helps only replacements not created yet: a replacement that already exists stays on its Provider (`internal/lifecycle/handover.go`, `replacements`; `nooverlap.go`).

### `CapacityDropped`

- **Means:** fewer Instances are ready than desired, on a deployment that has had a ready Instance before. A new deployment that has never been ready shows `NotEnoughReady` on `Ready` without it.
- **The controller** is already replacing what is missing; this reason ranks last, so any cause it knows ranks before it in the message.
- **You:** read the rest of the message. Alone, it is a gap: an Instance restarting (a restart takes it from `ready` back to `verifying`), or a replacement still starting ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Events and conditions; Health and restarts).

## The Instance

An Instance moves through `pending`, `starting`, `verifying`, `ready`, `draining` and `ended` ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Instance start). Its conditions:

| Condition | Reasons | What it says |
|---|---|---|
| `Verified` | `Pending`, `Passed`, `Failed`, `WorkerRestart`, `InstanceRestart` | unknown with `Pending` from the agent's registration until checked; true with `Passed`; false with `Failed` when a verification fails; unknown with the restart's reason while it verifies again after a restart. Unknown means the check has not run, or not yet again; false means it ran and failed (`internal/lifecycle/instances.go`, `pending`; `verify.go`, `verified`; `health.go`, `restart`) |
| `Qualified`, `WarmedUp` | `Pending`, `Passed`, `Failed`, `WorkerRestart`, `InstanceRestart` | unknown with `Pending` until checked; true with `Passed`. A worker restart sets `WarmedUp` to unknown with `WorkerRestart` and keeps `Qualified`; a whole-Instance restart sets both to unknown with `InstanceRestart`. A failed check writes its own condition, `Qualified` or `WarmedUp`, false with `Failed` and the outcome as its message, and the Instance ends with `QualificationFailed`, in `status.endReason` and on `Healthy` (`internal/lifecycle/instances.go`, `pending` and `failCheck`; `verify.go`, `starting`; `health.go`, `restart`) |
| `Healthy` | `Healthy`, `ProbeFailing`, `ErrorsOnly`, `NoProgress`, `PartExited` and other signals | true with `Healthy` once ready; false with the signal behind a restart ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Health and restarts), or with the end reason of an Instance that failed |
| `AgentUnreachable` | `ReportsMissing`, `Reporting` | no report from the agent for `health.agentTimeout`. The front routes no new request to it, and nothing restarts on that basis alone. The controller also raises a `failing` event `HeartbeatMissed`, described [below](#heartbeatmissed) |
| `TunnelConnected` | `ProxyOnline`, `ProxyOffline`, `ProxyListUnreadable` | whether frps lists the Instance's proxy online. It gates only an Instance's first entry to `ready`: an Instance that has been ready returns to `ready` after a restart whatever it says. The front does not route on it, but on its own probe of the path ([HBEP-6](../hbep/0006-instance-reachability/README.md), Path state; `internal/lifecycle/verify.go`; `internal/front/inventory/build.go`) |

Before each restart the controller emits a `failing` event `WorkerRestart` or `InstanceRestart`, which names the signal and says that requests in flight were cut. The ladder restarts the worker, then the whole Instance, each `restart.attemptsPerRung` times within `restart.window`, then gives up on the Allocation.

### `HeartbeatMissed`

The controller raises a `failing` event `HeartbeatMissed` regarding an Instance when its `AgentUnreachable` turns true. It is a `Warning` event, and it ends nothing: it is not one of the end reasons below. The same reason is also raised, regarding other objects, when a Provider's status read goes stale and when the `front` or `lifecycle` `Lease` does ([HBEP-8](../hbep/0008-observability-and-events/README.md), Observers); this entry is about the Instance's.

- **Means:** no report from the Instance's agent reached the controller for `health.agentTimeout` (`2m` by default), so `AgentUnreachable` turned true with `ReportsMissing`; the condition's message and the event's note say "no report since <time>". That time is not always an actual last report: it is the latest of the controller's own start, the Instance's start instruction and the controller's receipt of the matching agent's latest report, read on the controller's clock, so for an agent that has not reported yet it is one of the first two (`internal/lifecycle/instances.go:426-437`; `backend.go:196-204`). The engine's readings from that agent are unknown, and the front routes no new request to the Instance. It does not say that the engine is down. Only an Instance whose agent has registered and that has not ended is judged ([HBEP-8](../hbep/0008-observability-and-events/README.md), Observers; `internal/lifecycle/instances.go`, `agent`).
- **The controller** writes the condition, then emits the event once, and again at most once an hour while the silence lasts. It restarts nothing on that alone ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), The allocation agent, Unreachable). From a silent agent it takes no health signal of a ready Instance and no verification, qualification or warm-up result, and a draining Instance cannot be seen idle, so it ends at its hard end. The start deadline `health.startTimeout` is checked before a silent agent's results are skipped: an Instance still starting at that deadline fails if it never was ready and is on its first start or has its tunnel down, and otherwise climbs the restart ladder (`internal/lifecycle/verify.go`, `starting`, `startSignal`, `malfunction`). When reports resume, `AgentUnreachable` turns false with `Reporting`, the Instance goes on, and a later silence raises the event again at once. After the controller's own restart, no agent is judged until it has had time to register again ([A controller restart](#a-controller-restart)) (`internal/lifecycle/instances.go`, `agent`; `health.go`, `ready`; `verify.go`, `starting`).
- **You:**
  1. Read the condition and the events: `hbk describe instance <instance>`. The message gives the last report's time.
  2. The condition says only that reports stopped arriving, not why (`internal/lifecycle/instances.go:424-445` judges the freshness of the reports and nothing else). When every started Instance goes silent at once, look at what the agents share before the Instances: the controller, which receives the reports, and the route and network between the cluster and the host. Start with `hpcburst status`, then [A controller restart](#a-controller-restart) and [A tunnel that won't connect](#a-tunnel-that-wont-connect), and check that the cluster itself is reachable.
  3. When only one is silent, `hbk describe allocation <allocation>` shows its job's state. A job that ended ends the Instance by itself, with an Allocation reason of [the table below](#its-failing-reasons). For a job that still runs, check the route to the host first, as in step 2.
  4. Nothing needs doing when reports resume. The controller does not restart or end an Instance for its silence alone, but silence shields it from nothing else. An Instance still starting meets its start deadline, as above. One that is not ready when its drain start comes ends there with `Drained`. A ready one is handed over at its drain start, and then, no longer seen idle, ends at its hard end. Any other Instance ends when its job does (`internal/lifecycle/instances.go:401-406`).

### Its failing reasons

An Instance that fails ends with one of these reasons in `status.endReason`, and a `failing` event with the same reason. The controller then releases its Allocation at once, since nothing on it serves, which cancels the job, and creates a replacement. Each failed end counts toward its class's `RestartLoop` streak ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Reconcile loop, step 5; Release).

| Reason | Means | What to do |
|---|---|---|
| `RegistrationTimeout` | no agent registered within `health.registrationTimeout` of the job's start. The Provider also gets `DialHomeUnreachable` true | the dial-home route ([A tunnel that won't connect](#a-tunnel-that-wont-connect)) |
| `StartTimeout` | not ready within `health.startTimeout` of its start instruction, with its tunnel connected or not yet read | the engine's start: `hbk describe instance <instance>` shows which check had not passed |
| `TunnelNotConnected` | as `StartTimeout`, but its `TunnelConnected` was false or unknown at the deadline | [A tunnel that won't connect](#a-tunnel-that-wont-connect) |
| `VerificationFailed` | a settings contradiction or a shape misfit, which also stops the class ([`VerificationFailed`](#verificationfailed)), or a checkpoint rehash that failed again after restaging, which does not | the verification record; for a rehash, the checkpoint staged on the cluster |
| `QualificationFailed` | a qualification or warm-up check failed, a warm-up that has not settled after five rounds included | the failed check's condition, `Qualified` or `WarmedUp` (false with `Failed`, the outcome as its message), and the events |
| `RestartLoop` | the worker and whole-Instance rungs are spent within `restart.window` | the `failing` events before it name each signal |
| `AllocationFailed`, `AllocationLost`, `AllocationRefused` | the Allocation's phase became `Failed`, `Lost` (gone from the queue and from accounting for longer than `lostGrace`) or `Refused` | the Allocation's `status.atProvider.reason` |
| `Requeued` | the scheduler requeued the job, or a second execution of it registered | nothing from the first execution is reused; a requeue is a site's action |
| `SubmittedAsOther` | the job was established with another partition, time limit or GPU count than requested ([The Allocation](#the-allocation)) | the profile or the Provider against what the site applies |
| `AllocationEndedEarly` | the Allocation ended before its hard end while the Instance had not | the job's end on the cluster (`sacct`) |

`Drained` is the normal end: a drain, a lease end or a shutdown. It is not a failure.

### A reasoning model that exhausts its budget

A model served with a `reasoning-parser` engine flag can fail qualification with the reason `ReasoningBudgetExhausted` in the message of its `QualificationFailed` end.

- **Means:** a check asked for a short answer and got only reasoning back: the reply holds reasoning, no final answer and no tool call, and stopped at its output limit. Only the final answer is judged, so this is never counted as a wrong answer. The two checks ask for different budgets. The behaviour check (`behaviour`) asks for the workload's `outputTokens.typical` plus 4,096 tokens, and shortens its prompt to leave room for them. The tool-call smoke test (`toolCall`) asks for a fixed 128 plus 4,096. The 4,096 is fixed in alpha, a known limit; a per-deployment setting comes after alpha ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Instance start, Reasoning models). The message names the check and, for the behaviour check, the context it ran at: "the behaviour check failed (`ReasoningBudgetExhausted`) at <n> context tokens: the model ran out of reasoning budget ..." (`internal/agent/engine/qualify.go:103-124,205-221`; `internal/agent/engine/chat.go:234`; `internal/lifecycle/judge.go:387`).
- **The controller** fails the Instance like any failed qualification: it ends with `QualificationFailed`, releases the Allocation and creates a replacement. The failure counts toward the class's `RestartLoop` streak, which bounds the repeats, and does not stop provisioning in the class, so a model that always runs out of budget fails again on each replacement until `RestartLoop` pauses the class or the workload changes (`internal/lifecycle/judge.go:383-395`).
- **You:** for a reasoning model, declare `spec.workload.outputTokens.typical` as the typical total output, reasoning included. Reasoning tokens are generated tokens, so that describes the workload as it is. The same figure sets the behaviour check's budget (`internal/agent/engine/qualify.go:210-213`) and its prompt: the check runs at the smaller of `contextTokens.longest` and `max-model-len` minus the template overhead minus `outputTokens.typical` minus 4,096, and never below `contextTokens.typical` (`pkg/agentapi/checks.go:73-75`; `qualify.go:218`). It also feeds the rest of the workload's shapes:
  - the output length of every warm-up request (`internal/agent/engine/warmup.go:56-75`);
  - the fit of each context length beside it in `max-model-len` (`internal/lifecycle/spec.go:214-220`): a shape that does not fit is refused, never shortened, as [`SpecUnsupported`](#specunsupported) when the spec declares `max-model-len`, or at verification as [`VerificationFailed`](#verificationfailed);
  - the default of `contextTokens.longest`, which is `max-model-len` minus this figure minus the overhead (`spec.go:232`).

  The tool-call smoke test's budget is not set by the workload. A model that still runs out of budget has met alpha's known limit: the per-deployment reasoning budget comes after alpha.

## The Allocation

An Allocation is one Slurm job, found by its tag. Its phase is in `status.atProvider.phase`: `Submitting`, `SubmissionUnknown`, `Held`, `Pending`, `Running`, `Ended`, `Failed`, `Lost` or `Refused` ([HBEP-4](../hbep/0004-provider-interface/README.md), Core operations, Phases). The Provider's controller writes its conditions:

| Condition | Reasons | Means | The controller | You |
|---|---|---|---|---|
| `JobCompleting` | `Completing`, `NotCompleting` | the job's state carries Slurm's `COMPLETING` flag. On a job whose base state has ended (`CANCELLED`, `COMPLETED`, `FAILED`, `TIMEOUT`, `NODE_FAIL` and the like) its nodes are still cleaning up; the flag can also show on a `RUNNING` job, and on a requeued job that is `PENDING` again. The message names the job and its base state, and for an ended job since when | keeps the job live until the flag clears: the Allocation keeps its finalizer and its submit slot, and its `acct-` record waits | normally nothing: the flag lasts seconds. If it stays, see [A stuck COMPLETING job](#a-stuck-completing-job) |
| `AdminHold` | `JobHeldAdmin`, `NotHeldByAdmin` | an administrator holds the job; its phase is `Held` | never lifts an admin hold; the Instance stays `pending` | ask the site why, or delete the Allocation: its finalizer cancels the job, and the controller submits a new Allocation of the same name for the waiting Instance |
| `SubmittedAsOther` | `ReadBackMismatch` | the first read of the established job found another partition, time limit or GPU count than requested; the message names each field with both values | the lifecycle controller ends the Instance with `SubmittedAsOther` and deletes the Allocation; the condition is never cleared | compare the site's rewrite with the profile, and correct the profile or the Provider |
| `SubmissionResolved` | `NeedsOperator`, `Resolved` | only in fallback mode (a profile with `holdRelease: false`; the shipped profiles hold): a submission attempt is still unknown after the profile's `unknownBound` | submits nothing new for the Allocation until the attempt resolves | follow the message: check the cluster for jobs named with the Allocation's tag, then record your decision as the message says (below) |

A `Refused` Allocation whose `status.atProvider.reason` is `JobRenamed` was stored under another name than its tag, as on a site that shortens job names; the Provider's controller cancels that job ([HBEP-4](../hbep/0004-provider-interface/README.md), The Slurm dialect).

For `NeedsOperator`, look for the tag on the cluster, then set the annotation with the attempt number the message gives ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), open question 11). `sacct` starts at midnight unless given a start time, so give it one that covers the attempt:

```cluster
squeue --me --name=<tag>
sacct -X --name=<tag> -S now-7days
```

```
hbk annotate allocation <allocation> hpcburst.groundnuty.github.io/abandon-attempt=<attempt> --overwrite
```

Keep `--overwrite`. The Provider's controller leaves the annotation in place after it acts on it (`internal/providerctl/external.go`), so a later attempt of the same Allocation needs a new value; without `--overwrite` kubectl refuses to change it, and the attempt stays unresolved, with `NeedsOperator` and the Allocation's finalizer.

## Common procedures

### Stopping a deployment

Scale it to zero, then watch its Instances and Allocations go:

```
hbk scale modeldeployment <name> --replicas=0
hbk get instances,allocations
```

At zero desired Instances the controller applies the shutdown case ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Release): a ready Instance drains, so requests in flight finish within the drain window; any other Instance ends; an Allocation whose job has not started is cancelled. A started Allocation then follows its retention policy:

- `ReleaseWhenIdle`: it is released `idleLimit` after its last Instance ended (the examples use 30 minutes);
- `Keep`, the default: it is kept, its job still running, until `retentionPolicy.until`, else the deployment's `endTime`, else the job's hard end;
- either way, a profile's `maxIdle` releases it earlier: Athena's is 30 minutes, Helios sets none.

So a kept job can hold its GPU for hours after the scale-down. To end it now, delete the Allocation once it hosts no live Instance; its finalizer cancels every job with its tag. Deleting the ModelDeployment instead releases its kept Allocations once their Instances have ended, except one whose own `retentionPolicy.until` is set: that date still holds (`internal/lifecycle/retention.go`, `keepUntil`).

```
hbk delete allocation <allocation>
```

Scale back up with `hbk scale modeldeployment <name> --replicas=1`. A new Instance goes through the whole start again.

### A stuck COMPLETING job

A job keeps Slurm's `COMPLETING` flag until every node has run its epilog and reported. A node that never finishes keeps the flag, and the Allocation's `JobCompleting` condition stays true. Until it clears, the Allocation keeps its finalizer, so its deletion does not complete; it holds its Provider's submit slot, so another Instance there may wait with `SubmitLimitReached`; and its `acct-` record is not written ([HBEP-4](../hbep/0004-provider-interface/README.md), Risks, A job stuck in `COMPLETING`).

Nothing in the framework ends the wait, on purpose. Read the condition's message for the job and since when, then look at the job on the cluster:

```cluster
squeue --me --states=COMPLETING
scontrol show job <job-id>
```

Slurm itself sends the kill again after `KillWait` plus twice `MessageTimeout`. Cancelling a completing job whose base state has ended does nothing, since Slurm answers that it has already ended ([HBEP-4](../hbep/0004-provider-interface/README.md), Phases, A completing job). A job that carries the flag and has no end time yet, such as a `RUNNING` one, is still cancelled when its Allocation is deleted, and the deletion then waits for its flag (`internal/providerctl/delete.go`). A node that does not finish its cleanup is the site's to fix: report the job and its node to the cluster's administrators. Once the flag clears, the finalizer completes, the record is written and the slot frees, with no action on the host.

### A Provider refusing for a full slot

The deployment shows `SubmitLimitReached`. Either Slurm refused a submission, and the Allocation stays `Submitting` with Slurm's reason line in its message, or the controller's own count found the Provider's submit limit used up and created no Allocation.

1. Count the jobs on that login. Every job of the login counts toward Slurm's limit, including jobs of the owner's other work and of other Providers that use the same login:

   ```cluster
   squeue --me
   ```

2. Count the framework's own Allocations on that Provider. The controller counts every one of every deployment, from its creation until its phase is `Ended` and its job is no longer completing, or until its deletion completes ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Provisioning, Per-user limits):

   ```
   hbk get allocations -l hpcburst.groundnuty.github.io/provider=<provider>
   ```

3. Then wait, or reduce the demand. The controller retries after the Rejected backoff, and nothing fails meanwhile. A group limit (`QOSGrpSubmitJobsLimit`, `AssocGrpSubmitJobsLimit`) is shared with the other users of the QOS or the grant, and clears as their jobs end. To need fewer slots, lower `spec.instances`; allowing another Provider in `spec.providers` helps Instances whose Allocation is not created yet, since a refused Allocation stays on its Provider. A kept Allocation with no Instance holds a slot too ([Stopping a deployment](#stopping-a-deployment)). In `host-dialed` mode a Provider holds at most 2 Allocations at once, because each running Allocation takes a pair of relays and a login node carries 4; the third waits for one of the two to end ([Host-dialed relays](#host-dialed-relays)).

### A tunnel that won't connect

Jobs reach the host over the dial-home route you chose at `up` (in `host-dialed` mode the host reaches the jobs, through relays: [Host-dialed relays](#host-dialed-relays)), and each Instance's traffic runs through an frp tunnel to the host's frps ([HBEP-6](../hbep/0006-instance-reachability/README.md)). A broken route shows in one of three ways:

- the Instance ends with `RegistrationTimeout`, and its Provider has `DialHomeUnreachable` true: the agent never reached the host;
- the Instance ends with `TunnelNotConnected`: the agent registered, but its proxy never came online in frps;
- a ready Instance's tunnel drops: the front's own probe of the path fails, the front stops sending it new requests, and nothing restarts. The front emits a `degraded` event `PathProbeFailing` when the probe fails while `TunnelConnected` is still true; `TunnelConnected` turns false once frps lists the proxy offline. frpc reconnects by itself.

Check, in this order:

1. `hpcburst status`: the `front:` line shows the mode and address the record holds (host-dialed mode records none), `supervised frps:` must run and hold its ports, and the controller must be `ready`, since its process admits frpc's logins. While it is down, a reconnecting frpc is refused and retries.
2. `TunnelConnected` with `ProxyListUnreadable` means the controller could not read frps's proxy list, which it reads from frps's web port: check the `supervised frps:` line and `logs/frps.log`.
3. In `ssh` mode, the Provider's `DialHomeKeyMissing` (`hbk describe provider <provider>`): the key files must be in `~/.hpcburst/dial-home` on the cluster. The restricted key's lines name the launcher's port block, so a host started with another `hpcburst up --port-base` needs new keys ([quickstart, Alpha's limits](quickstart.md#alphas-limits)).
4. In `direct` mode, `dialHomeListen` in the front's configuration must be reachable from the compute nodes, not only from your machine.
5. In `host-dialed` mode there is no route to check from the compute nodes, and `DialHomeKeyMissing` does not apply, since the mode uses no dial-home key. Look at the relays, which carry both the agent's control traffic and the tunnel: [Host-dialed relays](#host-dialed-relays).

### Host-dialed relays

In `host-dialed` mode no job dials your host. The host dials each job, and the job's two connections run back over what it opened ([HBEP-6](../hbep/0006-instance-reachability/README.md), Dial-home modes). The controller keeps two relays for every running Allocation, one per channel, so control and tokens never share a connection: `control` carries the agent's control traffic and ends at the agent endpoint, `serving` carries frpc's tunnel and ends at the front's dial-home listener. Each relay is an `ssh` process of the controller, started on a connection of its own to the Provider's login node (never the Provider's shared master), and it dies with the controller process. It runs an `srun` step inside the job. The step is the staged agent binary itself, started as `relay control` or `relay serving`; it meets the agent on a Unix socket on the node, and the two ends run one multiplexed session over the step's input and output. Model weights and images never cross a relay. On the login node each relay is an ssh session and an `srun` client that compute nothing ([HBEP-4](../hbep/0004-provider-interface/README.md), open question 14). What a controller restart costs is in [A controller restart](#a-controller-restart).

- **See them.** The relays are children of the controller process. Take its PID from the `supervised controller:` line of `hpcburst status` and list its children with `pgrep -a -P <pid>`. A running Allocation shows two, whose command lines end in `relay control` and `relay serving`, with the job's ID in the `srun` part before them.
- **The socket on the node.** The agent makes one Unix socket, for both channels, in a directory named `hb-relay-<random>` (mode 700) under the job's `$TMPDIR`, which the site removes with the job, so a killed agent leaves nothing behind; under `/dev/shm` only when the `$TMPDIR` path would pass a socket's 107 bytes, which the agent logs as a warning (`the relay socket's directory is under the fallback`). A directory under `/dev/shm` outlives an agent that is killed. It writes the socket's path into `<stagingDir>/alloc/<tag>/relay-socket` (mode 600), by a temporary file and a rename. Each relay reads that file, checks that the agent's greeting names its Allocation, says which channel it serves and with what fence, and gets `ok` or `superseded` back. The agent refuses a peer that is another user. To look at the socket from the login node, read `relay-socket`, then list the directory it names with an `srun` step in the job, with the overlap option as the relays use; the line is in [development.md](development.md#runtime-conventions).
- **Read the log.** `logs/controller.log` has one line when a relay starts (`relay started`, with the Allocation's tag, the channel, the job, the ssh process's PID and the relay's fence in `epoch` and `seq`), one when it ends (`relay ended`, with how long it was up in `up`, the ssh client's exit code in `exit`, what the controller does next in `end` and the reason in `cause`), one when a relay ends for good (`a relay ended for good`, with the same `end` and `cause`) and one for each start that is refused (`a relay did not start`, with the reason in `why`). `end` is `retried`, `superseded`, `the job's end` or `failed`. The `cause` of a retried end is `agent not ready`, `unexpected output before the preamble`, `no preamble`, or, for an ssh that failed, the kind of failure, such as `authentication was refused` or `the connection was closed`. The log never carries the ssh or `srun` error text itself, which names the login node and the user. The agent's own lines are in the job's `job.err`, in the Allocation's directory `alloc/<tag>` on the cluster: `host-dialed: listening for the relays` at its start, `relay session up` for each relay it accepts, and `a relay was refused as superseded`.
- **What ends a relay.** The `end` in the log says what the controller does next:
  - **Retried**, after a backoff of 1 s, doubling to 30 s, with jitter (the first start is jittered too, since every relay starts together after a restart); the backoff starts over once a relay has stayed up for 60 s. The ends are ssh dropping or the login node killing it, unexpected output or no preamble, `agent not ready`, and a job not listed under its tag in the latest queue listing, which can miss a running job for a while (HBEP-4's absence grace).
  - **Final**, never started again: `superseded`, the job listed as ended or completing, `srun`'s refusal that the job does not exist or is completing, `srun` refusing the step because the job's step limit is reached, and more than 120 restarts of one channel within an hour.
  - The streams on a dropped relay are cut: a token stream in flight ends, the front records it as cut, and frpc logs in again once the relay is back. A relay that has no job to land on does not start (below).
- **`agent not ready` is ordinary while the agent starts.** The step starts as soon as the job runs, and the agent may not have published its socket yet. The relay writes `hb-relay: agent not ready: ...` to its standard error and exits with code 4, and the controller starts it again after its backoff. So the first seconds of a job can show a few `relay started` and `relay ended` pairs, with a short `up`, `end` `retried` and `cause` `agent not ready`, and nothing needs doing. The same line comes from a `relay-socket` that is missing, is not the user's own or not mode 600, from a socket nobody answers, and from an agent that greets for another Allocation. If the pairs go on after the agent has started, the agent is not running in that job or has died: read `job.err`, where `host-dialed: listening for the relays` shows that it bound its listeners. A channel that keeps failing this way ends for good after more than 120 restarts within an hour.
- **Unexpected output before the preamble.** After `ok`, the relay writes one line, `hpcburst-relay/1 <Allocation UID>`, and the host side requires it first, within 30 s. Any other first output, such as text that a login shell prints to standard output for a non-interactive ssh, ends the relay as `unexpected output before the preamble`, which the controller retries; nothing is skipped, and the log never carries the bytes. If it repeats, the login node's shell start-up files print to standard output: remove that output. A relay that sends nothing for 30 s ends as `no preamble`, a different cause: nothing arrived in time, so look at the login node and the job.
- **`superseded` after a hand-over is expected and final.** The agent keeps one session per channel, the one with the highest fence, and refuses a relay with a lower one. The fence is a pair: E, the `provider-<name>` `Lease`'s `leaseTransitions` when the controller took the lead, and N, a count of relay starts. The refused relay writes `hb-relay: superseded: ...`, exits with code 5, and the controller logs `end` `superseded` and does not start it again. A new copy of the controller therefore wins over an old one that still runs, whichever relay arrives first, and the two copies cannot take turns cutting each other's streams. The old copy stops its relays by PID before it releases its `Lease`, on a requested stop and after a renewal that failed alike, and its E is that of its own acquisition, never its successor's. Nothing needs doing; the new copy's relays stay up.
- **The fence needs the `provider-<name>` `Lease` to outlive its jobs.** E only ever grows while the `Lease` lives. If the `Lease` is deleted and made again, its `leaseTransitions` starts again at 0, below the E that the running agents keep, so every relay of the new leader is refused as `superseded`, for good and with no `RelayFailed`: the Allocations of running jobs have no path until their jobs end. Do not delete the Provider's `Lease` while its jobs run (`hpcburst down` and `up` keep it, in the k3s data directory). If it was deleted, restart each affected Allocation's job: delete the Allocation (its finalizer cancels the job by its tag), and the lifecycle controller makes a new one, whose agent starts with no fence.
- **`RelayFailed` needs action.** A relay that ends for good for any other reason than `superseded` or its job's end raises a Warning event, reason `RelayFailed`, regarding the Allocation, naming the channel and the cause. Like the Provider controller's other events, it is untyped in alpha ([HBEP-8](../hbep/0008-observability-and-events/README.md), open question 14). `hbk describe allocation <allocation>` shows it. The controller starts no relay of that channel again while the Allocation's job runs, unless it restarts, and nothing restarts the Instance on that basis alone. What the Instance shows depends on the channel. When the control relay ended, the agent cannot report: an Instance that never registered ends with `RegistrationTimeout`, and a ready one turns `AgentUnreachable` and takes no new requests. When the serving relay ended, the agent still reports, so `AgentUnreachable` stays false: an Instance that never came online ends with `TunnelNotConnected`, and a ready one takes no new requests ([A tunnel that won't connect](#a-tunnel-that-wont-connect)). Either way the Instance stays unusable until its Allocation ends or its relays return. The causes:
  - `srun refused the step: step limit reached for this job`: the job has used up the steps Slurm allows it, and every relay start and restart is one. The job cannot take another relay: end the Allocation ([Stopping a deployment](#stopping-a-deployment)). The wording of the refusal on a real cluster is not measured yet ([HBEP-4](../hbep/0004-provider-interface/README.md), open question 16), so until a live run shows it, a step-limit refusal can also show as the restart limit.
  - `more than 120 restarts of the <channel> channel`: the channel kept failing for over an hour. The `cause` of the `relay ended` lines before it says why: an ssh that cannot reach the login node, unexpected output, or an agent that never became ready. Fix that, then end the Allocation, or stop the host and start it again with the same flags ([Down and up across an upgrade](#down-and-up-across-an-upgrade)), which starts both relays of every running Allocation again and cuts the streams in flight.
- **`a relay did not start`.** The reason is in `why`. Before each start the controller reads the Provider's latest queue listing under the Allocation's tag, the one its own poll keeps, with no `squeue` of its own, and starts a relay only for the job the Allocation recorded, listed as running with the tag as its name, so a relay never lands on another job.
  - `job <id> is <phase>, not running yet`: the job is listed pending or held. The relay tries again after its backoff.
  - `the listing under the tag could not be read: ...`: the next start reads again.
  - `job <id> is completing` and `job <id> is <phase>, not running`: the job has ended or is ending. The relay ends for good as the job's end, with no event, and the controller stops the Allocation's relays when its job is no longer running anyway. `job <id> is not listed under tag <tag>` is retried after the backoff, starting no `srun`: a listing can miss a running job within the Provider's absence grace, and the relay starts again once the job is listed. An `srun` refusal that the job does not exist or is completing ends a relay the same way.

  A single refusal near the job's start or end needs nothing. If one of the last three appears while the job runs, the queue and the Allocation disagree, and the controller starts no relay for that job again: compare the queue with the job ID and tag that `hbk describe allocation <allocation>` shows, and do not edit the Allocation's annotations ([What never to do](#what-never-to-do)).

  ```cluster
  squeue --me
  ```

- **A third Allocation waits at `SubmitLimitReached`.** In `host-dialed` mode a Provider has at most 2 Allocations, pending or running, the handover pair. The controller applies that as a submit limit, so a third is not created and the deployment shows `SubmitLimitReached`, with a message that names `maxSubmitPerUser 2 (host-dialed mode, HBEP-6)`. A QOS limit that is lower keeps its own QOS in the message. Nothing fails meanwhile; the third Allocation is created once one of the two ends ([A Provider refusing for a full slot](#a-provider-refusing-for-a-full-slot)).
- **A relay waits for a slot.** Behind that cap, a login node carries at most 4 relays in alpha, 2 Allocations. The login node is the host name as `ssh -G` resolves it through the Provider's own ssh settings, so an alias and a full name of one node count as one, and the count is shared by every Provider of the controller that uses it. An Allocation's pair of slots is reserved whole before either relay starts and held through its relays' restarts and backoffs, so a relay that drops never hands its slot to another Allocation and none is left with half a pair. An Allocation that finds the node's slots taken waits for a pair and logs nothing: the sign is an Allocation with no relays in `pgrep -a -P <pid>` while two others' relays run on the same login node. Its slots free when the Allocation's job ends or the Allocation is deleted. Until its relays run, that job's agent cannot reach the host, and an Instance still unregistered at its registration deadline ends with `RegistrationTimeout`, with `DialHomeUnreachable` on its Provider, as for any broken route above. A per-platform budget is HBEP-4's open question 17.
- **One host-dialed Provider per login host.** The login host is the Provider's host name as `ssh -G` resolves it through its own ssh settings, so two Providers of one cluster, as two partitions of it are, name the same one. The host goes to the oldest of them by creation time, then by name, whichever asks first, so a controller restart never moves it; a younger Provider waits while an older one has not yet said which host it uses. The Provider's controller refuses the other: it submits nothing, its `Ready` condition goes false with reason `LoginHostInUse` and a message that names the login host and the Provider that holds it, and the planner skips it as `ProviderUnavailable`, so no Allocation is created for it ([HBEP-6](../hbep/0006-instance-reachability/README.md), Dial-home modes, Capacity). `hbk describe provider <provider>` shows it. The refused Provider does no harm: the planner skips it, and the Provider that holds the host keeps its relays. To use both, they must be on different login hosts.

### A controller restart

The supervisor restarts the controller when it exits, so a restart needs nothing from you. `hpcburst status` counts it on the `supervised controller:` line, with the last exit's detail, code and time. While it starts, its readiness reads `starting`, then `not leading` until it is elected leader of the `lifecycle` `Lease`, which can take a `Lease` duration after the old process's last renewal, then `ready`.

The restarted controller re-lists every object and continues from what it observes; nothing it needs is held only in memory ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Operational readiness 2). On taking its `Lease` it emits one `degraded` event `ObservationGap`, regarding the `Lease`, whose message names when the gap began; transitions missed meanwhile are not replayed, and the conditions are the truth ([HBEP-8](../hbep/0008-observability-and-events/README.md), Gaps after a restart). The agents register again within a poll (in `host-dialed` mode, once their control relay is back), and no `AgentUnreachable` is judged until then. In `direct` and `ssh` modes established tunnels stay up and no stream is cut. In `host-dialed` mode they do not: the controller owns the relays, and they die with its process, so a restart cuts the streams in flight on them. The owner accepted that for alpha (9 Nov 2026, "Accept cuts for alpha", [PR 298](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/pull/298#issuecomment-6749435073)). The front counts each as cut, and a request already written is never replayed. The new leader starts both relays of every running Allocation within the backoff, 1 s doubling to 30 s with jitter, and frpc logs in again once the serving relay is back. A relay keeper, a separate process that would keep the relays across a restart, is beta ([HBEP-6](../hbep/0006-instance-reachability/README.md), open question 13). A loss of the Provider's own `Lease` (`provider-<name>`) does the same to that Provider's relays: a copy that stops leading stops its relays by PID before it releases the `Lease`. A `ready` on the `supervised controller:` line speaks for the `lifecycle` `Lease` alone, so check the relays with `pgrep -a -P <pid>` ([Host-dialed relays](#host-dialed-relays)). If an old copy still runs while the new one leads (an unfenced hand-over, [HBEP-11](../hbep/0011-kubernetes-control-plane/README.md)), the agent keeps the relay with the higher fence. The old copy's relay is refused as `superseded` and not started again, so the two copies do not take turns cutting streams, and the new copy's relays stay up. Events that repeat at most once an hour, such as `HeartbeatMissed`, fire once more.

```
hbk get leases -n hpcburst-system
```

A `restarts` count that keeps rising is a crash loop: `logs/controller.log` in the state directory says why.

### Down and up across an upgrade

`hpcburst down` stops the supervisor, which stops the controllers, the front and frps, then k3s, and confirms that nothing of theirs still listens. It removes the host's record and kubeconfigs, and keeps the data directory, the record of the k3s release that wrote the store, and the agent endpoint's certificate, whose pin the running agents hold. It does not call the clusters: jobs keep running to their hard ends. While the host is down no front serves requests, and the tunnels drop until frps returns.

To run a new build of the framework, stop the host, rebuild, and start it with **the same flags as before**, since `down` forgot them. Without `hpcburst up --front-mode` (and `--front-addr`, in `direct` and `ssh` modes) every deployment shows `FrontNotConfigured`; in `host-dialed` mode the flags are `--front-mode host-dialed` alone, with no `--front-addr` and no `--front-key`, and the jobs' connections stay down after `up` until the controller has started the relays again; with another `--port-base` the agents' restricted keys and recorded ports no longer match. `--manifests config/crd` applies the CRDs again with server-side apply, which is safe to repeat. `up` also installs the CRD deletion guard on every start, whatever `--manifests` names, so a restart never leaves the host without it ([What never to do](#what-never-to-do)).

```
bin/hpcburst down --state-dir "$STATE"
go build -o bin/ ./cmd/hpcburst ./cmd/hpcburst-controller ./cmd/hpcburst-front
hack/build-agents.sh "$STATE/agent-builds"
bin/hpcburst up --state-dir "$STATE" --bin-dir "$BIN" --manifests config/crd \
  --front-config ~/.config/hpcburst/front.json \
  --front-mode ssh --front-addr <front-host> --front-key ~/.config/hpcburst/dial-home-key
bin/hpcburst status --state-dir "$STATE"
```

`up` starts the k3s release that last wrote the store, and refuses another. A new k3s release goes through `upgrade` instead, one step at a time: the next minor, or a newer patch of the running one. It refuses an unhealthy host, snapshots every object, stops the host, backs the store up under `backups/` in the state directory, starts the new release, compares every object, and only then starts the controllers again. It prints the backup's path ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Compatibility and upgrades).

```
bin/hpcburst upgrade --state-dir "$STATE" --bin-dir "$BIN" --to <version>
```

The only way back is to stop the host and restore that backup; `up`, with the same flags as before, then starts the backup's release. Changes made since the upgrade are lost:

```
bin/hpcburst down --state-dir "$STATE"
bin/hpcburst restore --state-dir "$STATE" --backup <backup>
```

Some jobs on the clusters may now have no Allocation to end them. Find them by tag, never by submission time: an Allocation the backup holds looks its tag up and adopts a job of that tag even when it was submitted after the upgrade (`internal/providerctl/external.go`, `establish`), so cancelling every recent job can cancel one the restored store still owns. A job's name is its Allocation's tag: `hb-`, the first 40 characters of the Allocation's name, a hyphen and 12 hex characters of a hash that includes the Allocation's UID (`api/v1alpha1/records.go`). The tag is also the Allocation's `crossplane.io/external-name` annotation. With the restored host up, list the restored tags, list the jobs on each cluster, and cancel only an `hb-` job whose name is not among the tags. Do this only on a login from which no other hpcburst host submits, since its jobs carry `hb-` names too:

```
hbk get allocations -o jsonpath='{range .items[*]}{.metadata.annotations.crossplane\.io/external-name}{"\n"}{end}'
```

```cluster
squeue --me --noheader --format="%i %j"
scancel <job-id>
```

`hpcburst down --grace` and `hpcburst upgrade --grace` set how long each waits after SIGTERM before SIGKILL, 40 s by default; both refuse a grace too short for the front to flush its usage records.

### Where the accounting records are

The Provider's controller writes one record per Allocation that outlives it: a ControllerRevision named `acct-<Allocation UID>` in the Allocation's namespace ([HBEP-8](../hbep/0008-observability-and-events/README.md), Provider accounting). It writes it while it deletes the Allocation, once no job with the Allocation's tag is live, a completing one included, and before the finalizer goes (`internal/providerctl/delete.go`). When the recorded job's final accounting read is missing or keeps failing, it waits up to the profile's `lostGrace` (2 minutes in the shipped profiles), then writes what it knows (`internal/providerctl/accounting.go`). So a record appears only once its Allocation is being deleted: released by the controller, by retention or by you, never while it is kept. It holds the Provider, the partition, the tag, the job IDs, `allocatedCPUs`, `allocatedGPUs`, `elapsedSeconds`, the start and end times, `basis: allocated`, and `unknown`, the fields that could not be read. It is written once and never changed.

```
hbk get controllerrevisions -l hpcburst.groundnuty.github.io/record=accounting
hbk get controllerrevision <record> -o yaml
```

A live Allocation's numbers so far are in its `status.atProvider.accounting`. The identity records (`id-`) and verification records (`ver-`) are ControllerRevisions of the same kind, labelled `identity` and `verification`. No record has an owner, so garbage collection never removes them, but deleting the namespace does: export them first ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Ownership, records and deletion).

## What never to do

- **Never remove a finalizer by hand.** An Allocation's finalizer, `hpcburst.groundnuty.github.io/cancel-all`, is what cancels every job with its tag and writes the `acct-` record. Remove it, and the object goes while its job may run on, held by no one, with no record of its hours. An Instance's finalizer, `hpcburst.groundnuty.github.io/drain`, is what lets it drain before it goes. A deletion that waits is waiting for a job, usually a [stuck COMPLETING job](#a-stuck-completing-job) or an unresolved submission (`SubmissionResolved`): fix that instead ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Ownership, records and deletion).
- **Never edit `status`.** The controllers write status and act on it: an Allocation's phase and `jobCompleting` decide its Instance's end, the slot count and the finalizer; an Instance's phase and conditions decide what the front routes to. An edit is overwritten at the next pass, or acted on before that. Change the spec, or use the one annotation meant for you, `abandon-attempt`.
- **Never edit the annotations the controllers write** on an Allocation, such as its job ID, its submission record or its tag: they are its record of which job is its own ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), The Allocation's record).
- **Never delete the framework's CRDs.** That deletes every resource of their kinds, and the finalizers would cancel every job ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Risks). An admission policy refuses it: `hpcburst up` installs it on every start, whatever `--manifests` names, and `hpcburst status` says whether it is there ([The lines, in the order they print](#the-lines-in-the-order-they-print)). To uninstall the framework, remove the guard first, with the one command that does:

  ```
  bin/hpcburst guard remove --state-dir "$STATE"
  ```

  Its help gives the warning to heed: it removes the binding, then the policy, and framework CRDs can then be deleted, which deletes every resource of those kinds and cancels their provider jobs. Run it only to uninstall the framework. It is repeatable and needs the host running. `down`, `upgrade` and a restart never remove the guard, and the next `up` installs it again.
- **Never expect `down` to stop the jobs.** It stops the host only. Scale the deployments to zero, or delete them, while the host still runs, and wait for `hbk get instances,allocations` to be empty first ([quickstart, step 7](quickstart.md#7-stop)).
