# Operations

This guide is for running hpcburst day to day: what `hpcburst status` says, what each condition on a ModelDeployment, an Instance or an Allocation means, what the controllers do about it, and what you can do. It follows the code on `main`; each entry names the HBEP section that holds the rule.

Start from a host set up as in the [quickstart](quickstart.md). Every command here assumes its two directories and its alias, set in the shell you work in (quickstart, steps 2 and 4):

```
STATE=${XDG_STATE_HOME:-$HOME/.local/state}/hpcburst
BIN=${HPCBURST_BIN_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/hpcburst/k3s}
alias hbk='KUBECONFIG="$STATE/kubeconfig" "$BIN/k3s-1.36.5/k3s" kubectl'
```

`<name>` stands for a ModelDeployment, `<instance>`, `<allocation>` and `<provider>` for the objects of those kinds, and `<front-host>` for your dial-home address. The examples live in the `default` namespace, so the commands give none. Commands in blocks marked `cluster` run on a cluster's login node; the rest run on the control-plane host.

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

- **Events** are a best-effort mirror of the conditions, kept 7 days ([HBEP-8](../hbep/0008-observability-and-events/README.md), Events and Retention). Each carries its HBEP-8 type in `action` and the reason code in `reason`; `degraded` and `failing` are `Warning` events. Events about a Provider or a PlatformProfile, which have no namespace, land in `default`.

  ```
  hbk get events --sort-by=.metadata.creationTimestamp
  ```

- **Logs:** each process the host runs writes to `logs/` in the state directory: `k3s.log`, `supervisor.log`, `controller.log`, `front.log` and `frps.log`.

## Reading `hpcburst status`

```
bin/hpcburst status --state-dir "$STATE"
```

`status` exits 0 when the host is healthy, 1 when it is not, and 3 when it is stopped ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Single-user host). Give it the same `--state-dir` as `up`; without it, it reads the launcher's default directory. `--json` prints the same report as JSON. `--no-api` skips the questions asked of the API server (its `/readyz`, the controller manager's `/healthz`, the write probe and the garbage-collector probe) and still checks the processes, their sockets and the supervised processes' own health ports. `--no-gc-probe` skips the garbage-collector probe only.

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
| `front:` | the recorded dial-home mode and address (`ssh <front-host>`), `absent` when the host was started without `hpcburst up --front-config`, or `not recorded` |

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

The last line, `front:`, is about the dial-home address, not the process. `front: absent` means no front runs under this host, which is a state, not a fault. A front that was configured and is not running is listed as a supervised process that does not run, and makes the host unhealthy.

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

- **Means:** the spec asks for something alpha does not run. The message names it: a `data-parallel-size` above 1; a `generation-config` other than `auto`, or `override-generation-config` or `hf-config-path`; an engine flag that the framework's launch sets itself; a missing `spec.workload`, or a workload value out of range; a workload shape that does not fit the declared `max-model-len`; a `leaseLimit` that does not parse; or a GPU count (`tensor-parallel-size` times `pipeline-parallel-size`) above the `gpusPerNode` or QOS GPU cap of every allowed Provider that is usable and not too short. A Provider that is unusable, or `PartitionTooShort`, does not count (`internal/lifecycle/handover.go`, `noProvider`).
- **The controller** creates no Allocation for the deployment while it holds, replacements included. Instances already running serve until they drain.
- **You:** edit the spec as the message says. For a GPU count, making a Provider that fits usable again also clears it, with no spec edit. The next pass reads it again ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Provisioning, and What the controller reads).

### `PartitionTooShort`

- **Means:** an allowed Provider's time limit is not longer than the lead an Instance needs. The time limit is the smallest of the partition's `maxTime`, the QOS's `maxWall` and the deployment's `leaseLimit`; the lead is the drain window plus `handoverMargin` plus `health.startTimeout`. The message names the partition, the limit, the bound that set it and the lead.
- **The controller** skips that Provider and creates no Allocation there. The reason stays while the Provider is allowed, even when another Provider serves the deployment.
- **You:** raise `spec.leaseLimit`, or shorten the drain window with a shorter `spec.routing.maxRequestDuration` (the window is that plus the profile's `drainMargin`, when shorter than `drainWindow`), or remove the Provider from `spec.providers`. With the shipped profiles Athena's `plgrid-now` is always too short: its 1 h limit is below the 1 h 45 min lead even with a 5-minute `maxRequestDuration` ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Provisioning, A partition too short).

### `FrontNotConfigured`

- **Means:** `hpcburst up` was given no `--front-mode` and `--front-addr`, so the jobs would have no route home.
- **The controller** submits no Allocation for any deployment.
- **You:** stop the host and start it again with the dial-home flags, as in the quickstart's step 4. `up` does not remember them across `down` (Down and up across an upgrade) ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Single-user host).

### `VerificationFailed`

- **Means:** an Instance's verification found that the engine runs other settings than the spec declares (a settings contradiction), or that a workload shape does not fit the context limit the engine reported (a shape misfit). The message names the accelerator class and the verification record, `ver-<instance UID>-<generation>`.
- **The controller** ended that Instance at once with `VerificationFailed`, released its Allocation, and stopped provisioning in its class. A contradiction stops the class until the serving spec changes. A shape misfit stops it until the spec changes or a `workload` edit makes the shapes fit, which lifts the stop with a `ready` event `ShapesFit` ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Instance start, verifying).
- **You:** read the record, which holds the settings the engine reported and the verdict:

  ```
  hbk get controllerrevisions -l hpcburst.groundnuty.github.io/record=verification
  hbk get controllerrevision <record> -o yaml
  ```

  Declare the value as the engine reports it (a model hook can rewrite one, HBEP-7's known limit), or fix the workload. The message says "until the serving spec changes" for both causes; for a shape misfit a `workload` edit is enough.

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

- **Means:** either Slurm refused a submission for a full submit limit (its reason name is `QOSMaxSubmitJobPerUserLimit`, `AssocMaxSubmitJobLimit`, `MaxSubmitJobsPerAccount`, `QOSGrpSubmitJobsLimit` or `AssocGrpSubmitJobsLimit`), or the controller's own count found no allowed Provider with a free slot for a missing Instance. The message names the Provider, the limit and Slurm's reason line.
- **The controller:** a refused Allocation stays `Submitting` and is retried after the Rejected backoff; with no free slot, no Allocation is created until one frees. Neither is a failure, and neither counts toward `RestartLoop` ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Provisioning, Per-user limits; [HBEP-4](../hbep/0004-provider-interface/README.md), QOS limits).
- **You:** see [A Provider refusing for a full slot](#a-provider-refusing-for-a-full-slot).

### `ProviderUnavailable`

- **Means:** an allowed batch Provider cannot take an Allocation, and was skipped. The message names why: it does not exist; its PlatformProfile does not exist or does not validate; its platform is not a batch one; it allows no partition, or one its profile does not describe; it is not `Ready`; or no agent build exists for its partition's CPU architecture. Without a more specific reason, it reads "no allowed batch Provider can take a new Allocation".
- **The controller** skips that Provider and tries the next one allowed. A served deployment ignores managed Providers, even when their labels match ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Reconcile loop, step 1).
- **You:** look at the Provider:

  ```
  hbk describe provider <provider>
  ```

  `Ready` false with `ReadsFailing` means the Provider's controller has read the platform successfully for none of the last 5 minutes. A stopped Provider controller cannot write that, so the lifecycle controller judges readiness by the age of `status.lastSuccessfulStatusRead`, and a stored `Ready` true can be stale. The Provider's other conditions usually say why reads fail: `CredentialsInvalid` (the credentials file or its key is readable by others, or not yours), `PathUnresolved` (HOME or the staging path is unusable on the login node), `DialectMismatch`, or `DialHomeKeyMissing` (in ssh mode, a dial-home key file is missing on the platform, and nothing is submitted). For a missing build, see [The agent builds](#the-agent-builds).

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
| `Verified` | `Pending`, `Passed`, `Failed`, `WorkerRestart`, `InstanceRestart` | false with `Pending` from the agent's registration until checked; true with `Passed`; false with `Failed` when a verification fails; false with the restart's reason while it verifies again after a restart |
| `Qualified`, `WarmedUp` | `Pending`, `Passed`, `WorkerRestart`, `InstanceRestart` | false with `Pending` until checked; true with `Passed`. A worker restart resets `WarmedUp` only, a whole-Instance restart both. A failed check writes neither: the Instance ends with `QualificationFailed`, in `status.endReason` and on `Healthy` (`internal/lifecycle/verify.go`, `health.go`) |
| `Healthy` | `Healthy`, `ProbeFailing`, `ErrorsOnly`, `NoProgress`, `PartExited` and other signals | true with `Healthy` once ready; false with the signal behind a restart ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Health and restarts), or with the end reason of an Instance that failed |
| `AgentUnreachable` | `ReportsMissing`, `Reporting` | no report from the agent for `health.agentTimeout`. The front routes no new request to it, and nothing restarts on that basis alone |
| `TunnelConnected` | `ProxyOnline`, `ProxyOffline`, `ProxyListUnreadable` | whether frps lists the Instance's proxy online. It gates only an Instance's first entry to `ready`: an Instance that has been ready returns to `ready` after a restart whatever it says. The front does not route on it, but on its own probe of the path ([HBEP-6](../hbep/0006-instance-reachability/README.md), Path state; `internal/lifecycle/verify.go`; `internal/front/inventory/build.go`) |

Before each restart the controller emits a `failing` event `WorkerRestart` or `InstanceRestart`, which names the signal and says that requests in flight were cut. The ladder restarts the worker, then the whole Instance, each `restart.attemptsPerRung` times within `restart.window`, then gives up on the Allocation.

### Its failing reasons

An Instance that fails ends with one of these reasons in `status.endReason`, and a `failing` event with the same reason. The controller then releases its Allocation at once, since nothing on it serves, which cancels the job, and creates a replacement. Each failed end counts toward its class's `RestartLoop` streak ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Reconcile loop, step 5; Release).

| Reason | Means | What to do |
|---|---|---|
| `RegistrationTimeout` | no agent registered within `health.registrationTimeout` of the job's start. The Provider also gets `DialHomeUnreachable` true | the dial-home route ([A tunnel that won't connect](#a-tunnel-that-wont-connect)) |
| `StartTimeout` | not ready within `health.startTimeout` of its start instruction, with its tunnel connected or not yet read | the engine's start: `hbk describe instance <instance>` shows which check had not passed |
| `TunnelNotConnected` | as `StartTimeout`, but its `TunnelConnected` was false or unknown at the deadline | [A tunnel that won't connect](#a-tunnel-that-wont-connect) |
| `VerificationFailed` | a settings contradiction or a shape misfit, which also stops the class ([`VerificationFailed`](#verificationfailed)), or a checkpoint rehash that failed again after restaging, which does not | the verification record; for a rehash, the checkpoint staged on the cluster |
| `QualificationFailed` | a qualification or warm-up check failed, a warm-up that has not settled after five rounds included | the check outcome in the Instance's conditions and events |
| `RestartLoop` | the worker and whole-Instance rungs are spent within `restart.window` | the `failing` events before it name each signal |
| `AllocationFailed`, `AllocationLost`, `AllocationRefused` | the Allocation's phase became `Failed`, `Lost` (gone from the queue and from accounting for longer than `lostGrace`) or `Refused` | the Allocation's `status.atProvider.reason` |
| `Requeued` | the scheduler requeued the job, or a second execution of it registered | nothing from the first execution is reused; a requeue is a site's action |
| `SubmittedAsOther` | the job was established with another partition, time limit or GPU count than requested ([The Allocation](#the-allocation)) | the profile or the Provider against what the site applies |
| `AllocationEndedEarly` | the Allocation ended before its hard end while the Instance had not | the job's end on the cluster (`sacct`) |

`Drained` is the normal end: a drain, a lease end or a shutdown. It is not a failure.

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

3. Then wait, or reduce the demand. The controller retries after the Rejected backoff, and nothing fails meanwhile. A group limit (`QOSGrpSubmitJobsLimit`, `AssocGrpSubmitJobsLimit`) is shared with the other users of the QOS or the grant, and clears as their jobs end. To need fewer slots, lower `spec.instances`; allowing another Provider in `spec.providers` helps Instances whose Allocation is not created yet, since a refused Allocation stays on its Provider. A kept Allocation with no Instance holds a slot too ([Stopping a deployment](#stopping-a-deployment)).

### A tunnel that won't connect

Jobs reach the host over the dial-home route you chose at `up`, and each Instance's traffic runs through an frp tunnel to the host's frps ([HBEP-6](../hbep/0006-instance-reachability/README.md)). A broken route shows in one of three ways:

- the Instance ends with `RegistrationTimeout`, and its Provider has `DialHomeUnreachable` true: the agent never reached the host;
- the Instance ends with `TunnelNotConnected`: the agent registered, but its proxy never came online in frps;
- a ready Instance's tunnel drops: the front's own probe of the path fails, the front stops sending it new requests, and nothing restarts. The front emits a `degraded` event `PathProbeFailing` when the probe fails while `TunnelConnected` is still true; `TunnelConnected` turns false once frps lists the proxy offline. frpc reconnects by itself.

Check, in this order:

1. `hpcburst status`: the `front:` line shows the mode and address the record holds, `supervised frps:` must run and hold its ports, and the controller must be `ready`, since its process admits frpc's logins. While it is down, a reconnecting frpc is refused and retries.
2. `TunnelConnected` with `ProxyListUnreadable` means the controller could not read frps's proxy list, which it reads from frps's web port: check the `supervised frps:` line and `logs/frps.log`.
3. In `ssh` mode, the Provider's `DialHomeKeyMissing` (`hbk describe provider <provider>`): the key files must be in `~/.hpcburst/dial-home` on the cluster. The restricted key's lines name the launcher's port block, so a host started with another `hpcburst up --port-base` needs new keys ([quickstart, Alpha's limits](quickstart.md#alphas-limits)).
4. In `direct` mode, `dialHomeListen` in the front's configuration must be reachable from the compute nodes, not only from your machine.

### A controller restart

The supervisor restarts the controller when it exits, so a restart needs nothing from you. `hpcburst status` counts it on the `supervised controller:` line, with the last exit's detail, code and time. While it starts, its readiness reads `starting`, then `not leading` until it is elected leader of the `lifecycle` `Lease`, which can take a `Lease` duration after the old process's last renewal, then `ready`.

The restarted controller re-lists every object and continues from what it observes; nothing it needs is held only in memory ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Operational readiness 2). On taking its `Lease` it emits one `degraded` event `ObservationGap`, regarding the `Lease`, whose message names when the gap began; transitions missed meanwhile are not replayed, and the conditions are the truth ([HBEP-8](../hbep/0008-observability-and-events/README.md), Gaps after a restart). The agents register again within a poll, and no `AgentUnreachable` is judged until then. Established tunnels stay up. Events that repeat at most once an hour, such as `HeartbeatMissed`, fire once more.

```
hbk get leases -n hpcburst-system
```

A `restarts` count that keeps rising is a crash loop: `logs/controller.log` in the state directory says why.

### Down and up across an upgrade

`hpcburst down` stops the supervisor, which stops the controllers, the front and frps, then k3s, and confirms that nothing of theirs still listens. It removes the host's record and kubeconfigs, and keeps the data directory, the record of the k3s release that wrote the store, and the agent endpoint's certificate, whose pin the running agents hold. It does not call the clusters: jobs keep running to their hard ends. While the host is down no front serves requests, and the tunnels drop until frps returns.

To run a new build of the framework, stop the host, rebuild, and start it with **the same flags as before**, since `down` forgot them. Without `hpcburst up --front-mode` and `--front-addr` every deployment shows `FrontNotConfigured`; with another `--port-base` the agents' restricted keys and recorded ports no longer match. `--manifests config/crd` applies the CRDs again with server-side apply, which is safe to repeat.

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
- **Never delete the framework's CRDs.** That deletes every resource of their kinds, and the finalizers would cancel every job ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Risks). The admission policy in `config/admission/` refuses it, but `hpcburst up --manifests config/crd` does not apply it, so apply it once yourself:

  ```
  hbk apply -f config/admission/crd-deletion-guard.yaml
  ```
- **Never expect `down` to stop the jobs.** It stops the host only. Scale the deployments to zero, or delete them, while the host still runs, and wait for `hbk get instances,allocations` to be empty first ([quickstart, step 7](quickstart.md#7-stop)).
