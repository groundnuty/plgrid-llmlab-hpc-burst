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

`status` exits 0 when the host is healthy, 1 when it is not, and 3 when it is stopped ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Single-user host). Give it the same `--state-dir` as `up`; without it, it reads the launcher's default directory. `--json` prints the same report as JSON. `--no-api` skips the questions asked of the API server (its `/readyz`, the controller manager's `/healthz`, the write probe, the garbage-collector probe, the presence of the CRD deletion guard, the agents' roles and identities, and the controllers' identities) and still checks the processes, their sockets and the supervised processes' own health ports. `--no-gc-probe` skips the garbage-collector probe only.

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
| `supervised controller:`, `supervised front:`, `supervised frps:` | one line per supervised process: whether it runs, its PID, its state (`starting`, `running`, `backoff` after an exit, `stopped`), its restart count, its `/healthz`, the controller's readiness, for frps and the front their open files ([below](#the-tunnel-processes-open-files)), and its last exit with code and time |
| `supervised eventlog:` | with `hpcburst up --event-log` (beta), the durable log's line: whether it runs, its PID, its state (`gated` while it is held back for space), its `/healthz` on base+17, each stream's messages and bytes against its cap, and what is degraded ([below](#the-event-logs-lines-beta)) |
| `agent builds (<dir>):` | one line per CPU architecture that an applied PlatformProfile's partitions have, with the agent and frpc builds (below) |
| `CRD deletion guard:` | `present (hpcburst-protect-crds)` when the admission policy exists and a binding names it with a denying action, or `ABSENT (<what is missing>)` with the warning that nothing then stops a delete of a framework CRD, which would delete every resource of its kind. Absent is a state, not a fault: the host stays healthy, as with an absent front, and the next `hpcburst up` installs the guard, so a host that an earlier release started shows it absent until then. `status` asks only a host that every other check found healthy, so `hpcburst status --no-api` and an unhealthy host print no such line; a failure to ask adds a reason and makes the host unhealthy. `hpcburst status --json` has it as `crd_guard` ([What never to do](#what-never-to-do) says how to remove the guard) |
| `admission policy <name>:` | one line for each of the capacity guard (`hpcburst-capacity-guard`), the owner pin (`hpcburst-owner-pin`) and the approver pin (`hpcburst-approver-pin`): `present` when the policy exists and a binding that names it denies, or `ABSENT (<what is missing>)`: what that policy bounds is then not bounded, and the next `hpcburst up` installs it. Absent is a state, not a fault, like the CRD guard. `hpcburst guard remove` removes the CRD guard only. `hpcburst status --json` has them as `admission_policies`, a list of `name`, `present` and `detail` ([What an agent may and may not do](#what-an-agent-may-and-may-not-do-to-a-modeldeployment)) |
| `agent roles:` | `present (...)` when the namespace `hpcburst-agents`, the ClusterRoles `hpcburst-agent-read`, `hpcburst-agent` and `hpcburst-operator` and their two bindings are what the framework installs; `present, WRITES OFF (<why>)` when the role `hpcburst-agent` is the narrow, read-only form that `up` leaves where it could not probe a policy (below), so the agents cannot write; or `DIFFER FROM THE MANIFEST (<what differs>)`, naming the object: a rule added or removed, a binding with another subject, a role that is not there. Each is a state, not a fault (the host stays healthy and `status` exits 0), and the next `hpcburst up` sets them back. Asked like the guard: `hpcburst status --no-api` and an unhealthy host print nothing, and a failure to ask adds a reason. `hpcburst status --json` has it as `agent_rbac`, with `writes` true for the wide form ([Giving an agent its own identity](#giving-an-agent-its-own-identity)) |
| `agent identity <name>:` | one line per agent identity, a ServiceAccount of `hpcburst-agents` joined with its kubeconfig in `<state-dir>/identities/`: `valid, expires <time> (<file>)` when the API server accepts its token (one SelfSubjectReview made with that kubeconfig names it), or `NOT VALID, expires <time> (<why>)`: the token expired or was rejected (401), its ServiceAccount or its file is gone, the file names another server, or the file cannot be used (`unreadable: <why>`, a corrupt certificate authority, say). Any of those is one identity's state, whatever the other identities and the host are, not a fault: the host stays healthy and `status` exits 0; `hpcburst identity renew <name>` writes a new token. The expiry is the one the token carries. `agent identities: none` when there are none. `hpcburst status --json` has them as `identities` |
| `controller identity <name>:` | one line for each of the controllers' own identities (`hpcburst-lifecycle`, `hpcburst-provider` and `hpcburst-front`, [The controllers' own identities](#the-controllers-own-identities)), joined with its kubeconfig in `<state-dir>/identities/system/`: `valid, expires <time> (<file>)` when one SelfSubjectReview made with that kubeconfig names its ServiceAccount, or `NOT VALID, expires <time> (<why>)`: the file is missing (a host that an earlier release started has none until its next `down` and `up`, or its `upgrade`), the token was rejected or expired, or the file names another server. A token with 30 days or fewer left, or expired, adds a second line, `WARNING: controller identity <name>: <n> days left, ...`; nothing renews a token but a new start. Each is a state, not a fault: the host stays healthy and `status` exits 0. `hpcburst status --json` has them as `controller_identities`, with `expires`, `expiring` and `warning` |
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

With `hpcburst up --event-log` (beta) the line also says `degraded: usage log behind` while the front's copy of its usage records in the durable log is behind: its `/healthz` states `usage.log` as `down`, or `usage.behindSince` is five minutes old or more, which is how long the records not yet in the log have stayed above none ([HBEP-8](../hbep/0008-observability-and-events/README.md), The durable log, When it is down; `internal/launcher/status_controllers.go`). It is a line like the failing sink's: the host stays `healthy`, `status` still exits 0, and a front with both says `degraded: usage writes failing; usage log behind`. Nothing is lost, since the usage files keep every record and the front copies what the log lacks as soon as it answers; the [event log's section](#the-event-log-beta) says what the copy is and where to look. A front started without `--event-log` says `log: down` in its `/healthz` too, and `status` says nothing about it: the host's recorded choice is what tells the two apart.

The last line, `front:`, is about the dial-home mode and address, not the process. `front: absent` means no front runs under this host, which is a state, not a fault. A front that was configured and is not running is listed as a supervised process that does not run, and makes the host unhealthy.

### The event log's lines (beta)

With `hpcburst up --event-log` the supervisor runs the durable log ([HBEP-8](../hbep/0008-observability-and-events/README.md), The durable log) and `status` adds the `supervised eventlog:` line. It says one of four things:

- **Healthy:** `supervised eventlog: running=true pid <n>, running, restarts 0, GET /healthz on 127.0.0.1:<base+17>: true, <stream> <n> messages <n> bytes (cap <n>)`, once for each of the two streams. Both streams exist, and the sizes are nats-server's own, in logical bytes, which is what the caps count.
- **`degraded: event log not started: <free> free on the store's file system, caps <events>+<usage>, floor <floor> (it needs <n> free: ...)`:** the caps do not fit the disk, so the supervisor did not start the log. The controllers and the front run without it, the host stays `healthy` and `status` exits 0. The supervisor asks again every 10 s and starts the log once the free space is enough, with no new `up`; the token and the configuration are written all the same. `up` itself exits 0 and prints `warning: the event log was not started`. See [the arithmetic](#the-event-log-beta).
- **`degraded: event log full: <the events stream> holds <n> of its <n> cap and refuses new events, keeping every old one; ...`:** the events stream has no room for another event of the size it holds on average (a stream refuses an event once its bytes and that event would pass the cap, so it is full before its bytes equal the cap). The log keeps serving and nothing is lost from it; each emitter counts the events the stream refused in `hpcburst_events_unlogged_total` and raises `EventsNotLogged` ([the events copy](#the-events-copy-in-the-log)), and a refusal is never retried. The remedy is to raise `--event-log-events-cap`: `hpcburst down`, then `hpcburst up --event-log` with the new value, which the log applies at its start. Lowering the events cap evicts nothing from the events stream; it only keeps the stream refusing until it is under the cap.
- **`unhealthy`, with the reason `eventlog: unhealthy: the event log refuses to serve: ...`:** the log exited because a setting is not what HBEP-8 says. The reason names each setting (`sync_interval`, a stream's `discard`, its `max_age`, ...), from `<state-dir>/eventlog/refused.txt`, which the log wrote before it exited with status 3; `logs/eventlog.log` has the same lines. The supervisor starts it again with its backoff and it refuses again until the setting is right. The configuration is written by `up` at each start, so a refusal about the file means it was edited; a refusal about a stream means the store holds a stream another program made.

### The tunnel processes' open files

The `supervised frps:`, `supervised frps-bulk:` (with the bulk path) and `supervised front:` lines carry the process's descriptors and its limits on them: `open files 812 in use, soft limit 1048576, hard limit 1048576`. `status` reads them from `/proc/<pid>/limits` and `/proc/<pid>/fd` of the process it checked, not of one found by name ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Single-user host; [HBEP-6](../hbep/0006-instance-reachability/README.md), open question 11). frps needs one descriptor per tunnel and one per stream in flight. The front's process needs two of each, for the client's connection and the upstream one: about 5,400 at 300 tunnels × 8 streams, above a common hard limit of 4,096. Both are Go programs, which raise their own soft limit to the hard limit at start, so the hard limit is what binds.

- **`degraded: open files`** ends the line once the descriptors in use pass 80% of the soft limit (exactly 80% does not). It is not a health problem: the host stays `healthy`, no reason is added, and `status` still exits 0. When the front has also lost usage records, the line says `degraded: usage writes failing; open files`. Check whether the tunnels or the streams in flight exceed what the host was sized for (HBEP-6, open question 11), and raise the hard limit, which the session or service that runs `hpcburst up` sets (an unprivileged process can only lower it); then `hpcburst down` and `up`.
- **`open files unknown (<cause>)`** replaces the numbers when they could not be read: the process exited meanwhile, or the limits file or the fd directory was refused. It says nothing about whether the descriptors are low; it does not change the health either.
- **`hpcburst up` warns** (stderr, and `warnings` of `--json`) when frps's or the front's hard limit is below 5,400, naming the limit and that figure, and starts anyway. It also warns when it could not read the limit.

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

With Instances behind a router (beta, opt-in: `hpcburst up --instance-router envoy`), `status` also checks `envoy-<cpuArch>` for each of those architectures, against the digest `internal/agent/envoypin/envoy-pins.txt` pins, since every job is then staged with it ([HBEP-5](../hbep/0005-llmlab-compatible-front/README.md), Inside an Instance; the procedure is [The router and several workers](#the-router-and-several-workers-beta)). Fetch both architectures' builds with:

```
hack/fetch-envoy.sh --agent-builds "$STATE/agent-builds"
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

- **Means:** the spec asks for something alpha does not run. The message names it: a `data-parallel-size` above 1; a parallel size (`data-parallel-size`, `tensor-parallel-size` or `pipeline-parallel-size`), `max-num-seqs` or `max-model-len` that is not a positive integer; a `generation-config` other than `auto`, or `override-generation-config` or `hf-config-path`; an engine flag that the framework's launch sets itself; a missing `spec.workload`, or a workload value out of range; a workload shape that does not fit the declared `max-model-len`; a `leaseLimit` that does not parse; or a GPU count (`tensor-parallel-size` times `pipeline-parallel-size`) above the `gpusPerNode` or QOS GPU cap of every allowed Provider that is usable and not too short, when the Instance is first placed. A Provider that is unusable, or `PartitionTooShort`, does not count (`internal/lifecycle/handover.go`, `noProvider`). It is also set for a pending Instance, one created that still waits for its Allocation, whose own Provider can no longer host its GPU count, because the Provider's limits changed since: the message then starts "Instance <name> waits for its Allocation" (`internal/lifecycle/provision.go`, `ensureAllocation`). Beta, `spec.workersPerInstance` is refused too: above 1 on a host started without the router, above what a node's GPUs give, or beyond what a profile's restart ladder can keep ([The router and several workers](#the-router-and-several-workers-beta)).
- **The controller** creates no Allocation for the deployment while it holds, replacements included. Instances already running serve until they drain.
- **You:** edit the spec as the message says. For a GPU count, making a Provider that fits usable again also clears it, with no spec edit. The next pass reads it again ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Provisioning, and What the controller reads).

### `PartitionTooShort`

- **Means:** an allowed Provider's time limit is not longer than the lead an Instance needs. The time limit is the smallest of the partition's `maxTime`, the QOS's `maxWall` and the deployment's `leaseLimit`; the lead is the drain window plus `handoverMargin` plus `health.startTimeout`. The message names the partition, the limit, the bound that set it and the lead.
- **The controller** skips that Provider and creates no Allocation there. The reason stays while the Provider is allowed, even when another Provider serves the deployment.
- **You:** raise `spec.leaseLimit`, or shorten the drain window with a shorter `spec.routing.maxRequestDuration` (the window is that plus the profile's `drainMargin`, when shorter than `drainWindow`), or remove the Provider from `spec.providers`. With the shipped profiles Athena's `plgrid-now` is always too short: its 1 h limit is below the 1 h 45 min lead even with a 5-minute `maxRequestDuration` ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Provisioning, A partition too short).

### `FrontNotConfigured`

- **Means:** `hpcburst up` was given no `--front-mode` (and, in `direct` and `ssh` modes, no `--front-addr`), so the jobs would have no route home.
- **The controller** submits no Allocation for any deployment.
- **You:** stop the host and start it again with the dial-home flags of your mode: the quickstart's step 4 gives `host-dialed`, and its [Alpha's limits](quickstart.md#alphas-limits) says what `direct` and `ssh` take. `--front-mode host-dialed` alone is enough in host-dialed mode. `up` does not remember them across `down` (Down and up across an upgrade) ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Single-user host).

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
| `Healthy` | `Healthy`, `ProbeFailing`, `ErrorsOnly`, `NoProgress`, `PartExited`, `RouterCheckFailing`, `KVSkew` and other signals | true with `Healthy` once ready; false with the signal behind a restart ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Health and restarts), or with the end reason of an Instance that failed |
| `AgentUnreachable` | `ReportsMissing`, `Reporting` | no report from the agent for `health.agentTimeout`. The front routes no new request to it, and nothing restarts on that basis alone. The controller also raises a `failing` event `HeartbeatMissed`, described [below](#heartbeatmissed) |
| `TunnelConnected` | `ProxyOnline`, `ProxyOffline`, `ProxyListUnreadable` | whether frps lists the Instance's proxy online. It gates only an Instance's first entry to `ready`: an Instance that has been ready returns to `ready` after a restart whatever it says. The front does not route on it, but on its own probe of the path ([HBEP-6](../hbep/0006-instance-reachability/README.md), Path state; `internal/lifecycle/verify.go`; `internal/front/inventory/build.go`) |

Before each restart the controller emits a `failing` event `WorkerRestart` or `InstanceRestart`, which names the signal and says that requests in flight were cut. The ladder restarts the worker, then the whole Instance, each `restart.attemptsPerRung` times within `restart.window`, then gives up on the Allocation.

Behind a router (beta, opt-in), the router's exit, or its probe failing while it holds a worker healthy, restarts only the router: a `failing` event `RouterRestart`, `status.healthyWorkers` set to 0 so the front routes nothing to the Instance, and the phase left `ready`. After `restart.attemptsPerRung` router restarts the ladder climbs to the whole Instance. A worker the router holds failing its check while verified is restarted at the worker rung, with the signal `RouterCheckFailing` ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Several workers). Each is described below, and [The router and several workers](#the-router-and-several-workers-beta) is the procedure.

### `RouterRestart`

The controller raises a `failing` event `RouterRestart` regarding an Instance that runs behind a router (beta, opt-in; [The router and several workers](#the-router-and-several-workers-beta)). It is a `Warning` event, and it ends nothing: it is not one of the end reasons below.

- **Means:** the Instance's router, Envoy, exited without being told to, or the agent's probe through it failed `health.failureThreshold` reports in a row while it held a worker healthy. The event's note names the signal, `PartExited` with the exit code or `ProbeFailing`, and the router's new generation, and says that its workers keep running and that requests in flight are cut. The Instance's `Healthy` condition is false with the same signal as its reason.
- **The controller** writes `status.healthyWorkers` as 0 first, so the front routes nothing to the Instance, then records a `router` entry in `status.restarts` and raises the router's generation. The phase stays `ready` and nothing verifies again; `status.healthyWorkers` returns once the restarted router holds a worker healthy. It restarts the router `restart.attemptsPerRung` times within `restart.window`, `restart.backoff` apart. After that the ladder climbs to the whole Instance, with `InstanceRestart` events, and on to `RestartLoop`. The router's rung runs before the Instance is first ready too, bounded by `health.startTimeout` (`internal/lifecycle/routerhealth.go`, `routerRung`; [HBEP-7](../hbep/0007-lifecycle-controller/README.md), Several workers).
- **You:** one event needs nothing. When they repeat, read the `Healthy` condition and the events (`hbk describe instance <instance>`), then the agent's log on the cluster: `job.err` in `<stagingDir>/alloc/<tag>/` holds `router exited without being told to` with the exit code, or `router did not start` with the reason. An exit code of 127 is a start that failed or was refused, a node whose glibc is older than 2.30 among them (the refusals in [the procedure](#the-router-and-several-workers-beta)).

### `RouterCheckFailing`

`RouterCheckFailing` is a health signal, not an event reason of its own. It is the signal behind a `failing` event `WorkerRestart` (or `InstanceRestart`) of an Instance that runs behind a router, and the reason of its `Healthy` condition while that restart runs.

- **Means:** the router's active check of one worker failed `health.failureThreshold` reports in a row while that worker's current generation was verified. The restart's note says "the router held worker-<i> failing its active check for <n> reports in a row". Envoy's ejection of a worker alone is not a signal, and a restarted worker is unhealthy by design while it warms up: until the router first holds it healthy, `health.startTimeout` from its restart judges it, not this check.
- **The controller** restarts that worker alone, at the worker rung, which counts its own `restart.attemptsPerRung` within `restart.window`. With several workers the Instance stays `ready` and routable on the others, and a worker that spends its rung takes the Instance to the whole-Instance rung. With one worker the Instance leaves `ready` for `verifying`, as in alpha (`internal/lifecycle/routerhealth.go`, `routerCheckSignal`; `workers_ready.go`, `workerRouterCheck`).
- **You:** one needs nothing. When one worker repeats it, read its entry in `status.workers[]` (`verdict`, `warmedUp`) and the events: the engine on that node answers its health check badly, which restarts alone cannot mend, and the ladder ends in `RestartLoop` and a replacement.

With several workers, a worker at a full KV cache with requests waiting and its token counter flat, while a sibling the router routes to idles, for `health.kvSkewTimeout` (2 min by default), is restarted at the worker rung with the signal `KVSkew`; the Instance stays `ready` on its siblings, and the stuck worker's in-flight requests are cut. A worker that is still generating tokens never raises it, however full: that is load, and the router does not move a session off its worker for it ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Health and restarts; [HBEP-5](../hbep/0005-llmlab-compatible-front/README.md)).

When a serving worker (its Instance `ready` or `draining`, and the worker verified and warmed up since its last start) exits without being told to within 30 s after a serving worker of another Instance of the same ModelDeployment did, the controller also raises a `failing` event `CrashCorrelated` regarding the Instance whose exit made the pair, with the other in `related`: a request may be crashing every engine it reaches. It is an event only; each Instance still restarts by its own ladder. The window is judged on the exit times the agents report. A worker that fails to start never counts: that is verification's and provisioning's. Nor does a drain's own stop of its worker, which the controller asks for. One Instance crashing again and again is `RestartLoop`'s, not this ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Health and restarts: Crash correlation).

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
| `RelayEndedForGood` | in `host-dialed` mode, a relay of the Allocation ended for good, other than superseded or by its job's end, so nothing reaches the Instance; the Allocation's condition of that name was set ([`RelayFailed` releases the Allocation](#host-dialed-relays)) | the cause in the event's message: the job's step limit, or a path that drops soon after it is up |

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
| `RelayEndedForGood` | `RelayFailed` | only in `host-dialed` mode: a relay of the Allocation ended for good for another reason than `superseded` or its job's end; the message is the `RelayFailed` event's, naming the channel and the cause | starts no relay of that channel again; the lifecycle controller ends the Instance with `RelayEndedForGood` and deletes the Allocation; the condition is never cleared | read the cause ([Host-dialed relays](#host-dialed-relays)) |
| `SubmissionResolved` | `NeedsOperator`, `Resolved` | only in fallback mode (a profile with `holdRelease: false`; the shipped profiles hold): a submission attempt is still unknown after the profile's `unknownBound` | submits nothing new for the Allocation until the attempt resolves | follow the message: check the cluster for jobs named with the Allocation's tag, then record your decision as the message says (below) |

A `Refused` Allocation whose `status.atProvider.reason` is `JobRenamed` was stored under another name than its tag, as on a site that shortens job names; the Provider's controller cancels that job ([HBEP-4](../hbep/0004-provider-interface/README.md), The Slurm dialect).

The Provider's controller also raises typed events regarding the Allocation (beta; [HBEP-8](../hbep/0008-observability-and-events/README.md), Events, Allocation events), with `reportingController` `hpcburst-provider`. Each is raised in the pass that first sees the change, so one that was missed while the controller was down is announced late and the steps in between are not replayed; the conditions and `status.atProvider` stay the truth. Their notes name the job's facts (states, times in UTC, partition, QOS and counts) and never the job ID, which is in `status.atProvider.jobID` and the `job-id` annotation. No note of these events, or of the lifecycle controller's, the front's and the managed controller's events, names a path, a user name, an account or a grant: where a message from another component would be copied (a profile reader's error, a relay's cause, an agent's check), the note gives the reason code and a fixed description, and the detail is in the object's status (`status.atProvider.reason`, a condition's message) and the component's log:

- a `started` event `JobSubmitted` when a submission's job is first recorded, a `started` event `JobAdopted` when a job found by its tag is recorded instead, and a `started` event `HoldLifted` when the recorded job's hold is lifted so that it pends;
- a `ready` event `JobStarted` when the job runs, and a `ready` event `JobReappeared` when a job reported `Lost` shows again;
- an `ending-soon` event `HardEndNear` an hour before a running job's hard end, once for each hard end, so a hard end that moves later raises it again;
- an `ended` event `JobEnded` when the job ended at or after its hard end, or while its Instance was draining, whatever Slurm's state (`TIMEOUT`, `COMPLETED` or `CANCELLED`), and when the Allocation was deleted for its retention, by an Operation or by a person. A deletion's note carries the release cause, and the event names the Operation in `related` when the cause is one. A deletion that follows a failure raises no `JobEnded`, since the failure's event has fired;
- a `failing` event `AllocationEndedEarly` when the job ended before its hard end with no Instance of it draining and no deletion asked for, a `failing` event `AllocationFailed` when it failed, a `failing` event `Requeued` when Slurm requeued it, a `failing` event `SubmittedAsOther` when the read-back differs from the request, a `failing` event `AllocationRefused` when the Allocation is `Refused`, and a `failing` event `JobLost` when the job has been absent from every read for `lostGrace`;
- a `degraded` event `AdminHold` while an administrator holds the job, and a `degraded` event `JobCompleting` once the job has carried `COMPLETING` for 10 minutes. Both repeat at most once an hour while the condition lasts.

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

So a kept job can hold its GPU for hours after the scale-down. To end it now, delete the Allocation once it hosts no live Instance; its finalizer cancels every job with its tag. Deleting the ModelDeployment instead releases its Allocations at once, as soon as their Instances have ended, whatever their `retentionPolicy`: `ReleaseWhenIdle`'s idle limit and `Keep`'s `until` are for a deployment that still exists (HBEP-7, Release; `internal/lifecycle/retention.go`, `releaseEmpty`; before #503 a `ReleaseWhenIdle` job ran on for its whole idle limit after the deletion).

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
  - **Final**, never started again: `superseded`, the job listed as ended or completing, `srun`'s refusal that the job does not exist or is completing, `srun` refusing the step because the job's step limit is reached, and more than 120 restarts of one channel within an hour, counting only restarts whose session came up and then dropped. A relay that fails before its session comes up (the login node down for maintenance, ssh refused, `agent not ready`, no preamble) is never capped: it keeps retrying at the 30 s cap of its backoff, for as long as the job runs.
  - The streams on a dropped relay are cut: a token stream in flight ends, the front records it as cut, and frpc logs in again once the relay is back. A relay that has no job to land on does not start (below).
- **`agent not ready` is ordinary while the agent starts.** The step starts as soon as the job runs, and the agent may not have published its socket yet. The relay writes `hb-relay: agent not ready: ...` to its standard error and exits with code 4, and the controller starts it again after its backoff. So the first seconds of a job can show a few `relay started` and `relay ended` pairs, with a short `up`, `end` `retried` and `cause` `agent not ready`, and nothing needs doing. The same line comes from a `relay-socket` that is missing, is not the user's own or not mode 600, from a socket nobody answers, and from an agent that greets for another Allocation. If the pairs go on after the agent has started, the agent is not running in that job or has died: read `job.err`, where `host-dialed: listening for the relays` shows that it bound its listeners. A channel that keeps failing this way ends for good after more than 120 restarts within an hour.
- **Unexpected output before the preamble.** After `ok`, the relay writes one line, `hpcburst-relay/1 <Allocation UID>`, and the host side requires it first, within 30 s. Any other first output, such as text that a login shell prints to standard output for a non-interactive ssh, ends the relay as `unexpected output before the preamble`, which the controller retries; nothing is skipped, and the log never carries the bytes. If it repeats, the login node's shell start-up files print to standard output: remove that output. A relay that sends nothing for 30 s ends as `no preamble`, a different cause: nothing arrived in time, so look at the login node and the job.
- **`superseded` after a hand-over is expected and final.** The agent keeps one session per channel, the one with the highest fence, and refuses a relay with a lower one. The fence is a pair: E, the `provider-<name>` `Lease`'s `leaseTransitions` when the controller took the lead, and N, a count of relay starts. The refused relay writes `hb-relay: superseded: ...`, exits with code 5, and the controller logs `end` `superseded` and does not start it again. A new copy of the controller therefore wins over an old one that still runs, whichever relay arrives first, and the two copies cannot take turns cutting each other's streams. The old copy stops its relays by PID before it releases its `Lease`, on a requested stop and after a renewal that failed alike, and its E is that of its own acquisition, never its successor's. Nothing needs doing; the new copy's relays stay up.
- **The fence needs the `provider-<name>` `Lease` to outlive its jobs.** E only ever grows while the `Lease` lives. If the `Lease` is deleted and made again, its `leaseTransitions` starts again at 0, below the E that the running agents keep, so every relay of the new leader is refused as `superseded`, for good and with no `RelayFailed`: the Allocations of running jobs have no path until their jobs end. Do not delete the Provider's `Lease` while its jobs run (`hpcburst down` and `up` keep it, in the k3s data directory). If it was deleted, restart each affected Allocation's job: delete the Allocation (its finalizer cancels the job by its tag), and the lifecycle controller makes a new one, whose agent starts with no fence.
- **`RelayFailed` releases the Allocation.** A relay that ends for good for any other reason than `superseded` or its job's end raises a Warning event, reason `RelayFailed`, regarding the Allocation, naming the channel and the cause. Like the Provider controller's other events, it is untyped in alpha ([HBEP-8](../hbep/0008-observability-and-events/README.md), open question 14). `hbk describe allocation <allocation>` shows it. The controller starts no relay of that channel again while the Allocation's job runs, unless it restarts, so the Instance cannot serve. At the same moment the controller sets the Allocation's condition `RelayEndedForGood` True, with reason `RelayFailed` and the event's message, and never clears it ([The Allocation](#the-allocation)). It writes the condition itself, so a controller restart right after the end does not lose it: a status patch that reads the Allocation from the API server first, writes nothing when its UID or its recorded job is not the relay's or when it no longer runs (ended, completing or being deleted), and is retried on a conflict, within 30 s. Only the copy that leads writes it: a copy that loses the Provider's `Lease` stops writing before its relays stop, even in the middle of a write. If that write fails, as while the API server is unreachable, the controller's next observation of the Allocation sets the condition instead. That comes within one poll interval (30 s by default; up to a minute while its observations fail) plus the observation's own run, which the reconcile timeout bounds (5 min by default). While the controller cannot connect to the platform (its profile or credentials cannot be read), it makes no observation, so only the direct write marks the Allocation then. If the direct write fails and the controller restarts before an observation sets the condition, the failure is forgotten: the new copy starts both relays again, and a lasting cause, such as the step limit, ends them for good once more. The lifecycle controller's next pass, which an Allocation's change starts at once, ends the Allocation's Instance with `RelayEndedForGood` and a `failing` event, and releases the Allocation, whose finalizer cancels its job; the planner makes a replacement as for any failed Instance, and the failure counts toward the class's `RestartLoop` streak (issue #304). An empty Allocation that its retention policy keeps is released the same way, since nothing placed on it could be reached. Nothing needs doing to free the job's GPU: read the cause, since a replacement on the same path can fail the same way. The causes:
  - `srun refused the step: step limit reached for this job`: the job has used up the steps Slurm allows it, and every relay start and restart is one. The job cannot take another relay, and its Allocation is released. The wording of the refusal on a real cluster is not measured yet ([HBEP-4](../hbep/0004-provider-interface/README.md), open question 16), so until a live run shows it, a step-limit refusal can also show as the restart limit.
  - `more than 120 restarts of the <channel> channel ... after sessions that came up; the last end: ...`: the channel's sessions came up and dropped more than 120 times within an hour, and the event names the last end's cause: a path that drops soon after it is up, such as a login node that kills long ssh sessions. Failures before a session comes up never reach this cap. The Allocation is released, and its replacement's relays meet the same path: fix that before the class's `RestartLoop` pauses provisioning.
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

### The bulk path (beta)

On one frp session a large non-streamed reply holds an Instance's token streams back for about frp's 6 MiB stream window over the path rate: spike S15 measured a gap p99 of 137 ms in `direct` mode and 206 ms in `host-dialed` mode, against 52 ms. The bulk path gives each Instance's non-streamed replies a tunnel of their own ([HBEP-6](../hbep/0006-instance-reachability/README.md), The bulk path; [issue 448](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/448)). It is off unless you ask for it, and works in `direct` and `host-dialed` mode only.

- **Turn it on:** `hpcburst up --bulk-path`. `up` refuses it in `ssh` mode, with no `--front-mode`, with no front to run (`--front-config`) and with no frps, each with its reason. The port block grows from base..base+12 to base..base+15, so the base must be 65520 or lower.
  - A second frps, `frps-bulk`, runs under the supervisor on base+13 to base+15 with `transport.tcpMux = false`. Its files are `frps-bulk/frps.toml`, `frps-bulk/web-password` and `logs/frps-bulk.log` in the state directory.
  - A second certificate, `agent-endpoint/tls-bulk.crt`, is issued for the agent endpoint's key and names `bulk.dial-home.hpcburst.invalid` alone. The first certificate is never written, so an Allocation staged before keeps its tunnel.
  - The controller stages that certificate as `alloc/<tag>/dial-home-bulk.crt` and writes `"bulk": {"serverName": "bulk.dial-home.hpcburst.invalid"}` into each new Allocation's `agent.json`. The agent then runs a second frpc for the Instance, the bulk frpc, to the second frps through the same dial-home listener, which tells the two apart by the TLS server name.
  - The front sends each routed POST request whose top-level `stream` is not `true` through the Instance's bulk tunnel while it is registered, and everything else through the token tunnel as before. Routing, the cap and pauses are the token tunnel's.
- **See it.** `hpcburst status` has a `supervised frps-bulk:` line, checked as the first frps's is (its process, its three loopback ports, `/healthz`), and the front's line also needs its bulk plugin path to answer. The front's `/metrics` adds `hpcburst_front_bulk_requests_total{path="bulk"|"fallback"}` and `hpcburst_front_bulk_registered`; without the bulk path neither exists.
- **What happens on a fault.**
  - A bulk dial that fails before any byte is sent (the bulk frpc gone, frps-bulk restarting) takes the bulk tunnel out of use until its next passing probe, counts one `fallback`, and the request goes through the same Instance's token tunnel. Nothing counts toward a pause.
  - A bulk request closed before its response headers gets 502 `upstream_closed` and is not sent again; the bulk tunnel is re-probed. Nothing counts toward a pause.
  - A restart of frps-bulk drops only the bulk tunnels; requests take the token tunnels meanwhile.
  - An Allocation staged before the bulk path was turned on runs no bulk frpc, and all its requests take the token tunnel.
- **Turn it off (rollback).** `hpcburst down`, then `hpcburst up` without `--bulk-path`. frps-bulk stops, and every request takes the token tunnel. An Allocation staged with the bulk block keeps trying its bulk frpc against a front that now serves only the first certificate: its TLS fails and it retries with backoff, never in a tight loop, and its token tunnel is unaffected. The second certificate stays on disk, so turning the bulk path on again reuses it.

### The event log (beta)

The durable log keeps typed events for the grant and a rolling copy of the usage records ([HBEP-8](../hbep/0008-observability-and-events/README.md), The durable log). It is opt-in in beta. The front copies each synced usage record into it ([the usage copy](#the-usage-copy-in-the-log)), and the lifecycle controller, every Provider's manager and the front publish each typed event they raise to it ([the events copy](#the-events-copy-in-the-log)).

- **Turn it on:** `hpcburst up --event-log`, with `--event-log-events-cap` and `--event-log-usage-cap` (2 GiB each by default) and `--event-log-floor` (15 GB by default). Sizes are a whole number and a unit: GiB and MiB are powers of 1,024, GB and MB powers of 1,000, and a bare G is refused. The choice and the sizes are recorded and kept by `upgrade`; a change takes `down`, then `up` with the new value. `up` refuses the sizes without `--event-log`, `--event-log` without a controller binary, and a port base above 65518, since the block grows to base..base+17 (base+16 for clients, base+17 for monitoring; base+13 to base+15 stay the bulk path's whether or not it runs).
- **What runs:** `hpcburst-controller eventlog` under the supervisor, started before the controllers and the front and stopped last. It listens on loopback only and refuses a client without its token. Its store, configuration and token are in `<state-dir>/eventlog/` (the directory mode 700, the files 600); the token is 32 random bytes in hex, is never on a command line, and is passed to the controllers and the front as a token-file flag. With it the front copies its usage records and publishes its events, and the lifecycle controller and every Provider manager publish theirs, each over a connection of its own to the log at base+16; without it nothing publishes. Anyone who can read the state directory can read the log.
- **The caps must fit the disk.** The supervisor starts the log, at `up` and at every restart, only when `free - (caps - store) - (caps - copy) >= floor`, with each bracket at least 0. `free` is the free space of the file system holding `<state-dir>/eventlog`, `store` is that directory's size on disk (which stands for the streams' bytes while the log is down), `copy` is the size of the previous upgrade's copy, `caps` the two caps added. With both caps at 2 GiB, an empty store and no copy that is 15 GB + 4,294,967,296 + 4,294,967,296 = 23,589,934,592 bytes: 23.6 GB free starts the log and 23.5 GB does not. With 1 GiB in the store, or a 1 GiB copy, it is 15 GB + 7,516,192,768 = 22,516,192,768 bytes.
- **Upgrade and restore.** Before an upgrade stops anything, it refuses when `free + copy - store` would fall under the floor, naming the four sizes. Once the host has stopped (the log last), it removes the previous copy of `<state-dir>/eventlog/` and copies the store to `<state-dir>/backups/eventlog/`, file by file, synced, with `copy.json` naming the SQLite backup it sits beside; so one copy remains however many upgrades were made. A copy that fails stops the upgrade before the new release is recorded, and the error says how to start the old release again. `hpcburst restore --backup <dir>` puts the log's store back only with the backup the copy sits beside, and says so; with an older backup it leaves the live log store alone and says that too, since the log is then ahead of the restored API store.
- **Turn it off (rollback).** `hpcburst down`, then `hpcburst up` without `--event-log`. Nothing that serves depends on the log, and its store stays in `<state-dir>/eventlog/`.

#### The usage copy in the log

The usage files stay the record ([HBEP-8](../hbep/0008-observability-and-events/README.md), The durable log, Who writes and When it is down). The log's usage stream (HB_USAGE) is a rolling copy of them: it keeps the newest records up to its cap (`--event-log-usage-cap`) and drops the oldest to make room, so a publish to it never fails for space. The front's forwarder does the copying, and a record's write never waits for it.

- **What it copies.** Each batch of records the sink has synced, as the file holds it, byte for byte, in messages of up to 256 records and under 1 MiB, each with the id `usage-<first counter>-<last counter>`. It copies only what the sink has synced, so the log never holds a record the files lack. A record too long to send (files written before the front bounded the record's client text, HBEP-5, Usage records, #649, may hold one) is not copied: the files hold it, `logs/front.log` names its counter, and it is counted once in `hpcburst_usage_skipped_total` (a counter that starts at 0 on a front with the log). It is not lag: the gauge below and `degraded: usage log behind` mean a copy that is behind, and one such line does not set them. Such a line never stops the front from starting, with the log or without it.
- **After a start, an outage or a failed publish** it reads the stream's last message, takes `<last>` from its id, and copies the files from the next counter they hold after it. Counters have gaps (a crash's reserved block), and a gap is crossed, never waited for. A log that lost its tail to a power loss or a restore is copied again from the files, so the stream catches up to them.
- **On an empty stream** it starts at the oldest daily file of the newest set of files whose sizes add up to the usage cap or less (the newest file alone when it is over the cap), so a new log on a host with months of files does not push the whole history through its rolling copy. The sizes are the files' bytes, and the stream adds about a hundred bytes of header per message, so the oldest records of that set can be among the ones the cap drops.
- **Where to look.** `/healthz`'s `usage` member has `log` (`connected` or `down`), `loggedCounter` (the last counter the log holds, null until the front has read it) and, while records wait, `behindSince`. `/metrics` has `hpcburst_usage_unlogged_records`: the last synced counter less the last logged one, so it counts a gap's missing counters as records; it has no series until `loggedCounter` is known and the front knows where its own files end, since an unknown is not a 0, and none on a front started without the log. If the front could not read the last counter of the usage files at its start (`logs/front.log` says so, as a warning), it holds back, publishing nothing and exporting no gauge value, until it writes its first record; then it copies everything the log lacks. `logs/front.log` has the forwarder's warnings: the log is unreachable or refused the token, with the error, once and again each minute while it lasts.
- **What to do about `degraded: usage log behind`.** The log being down is the usual cause: see the log's own line for why (not started for space, refusing its settings, or not running). With the log up, check `logs/front.log` for the forwarder's error. Nothing is lost while it lasts, and the gauge returns to 0 once the copy has caught up.

#### The events copy in the log

Every typed event the framework raises, one with a type in its `action`, also goes to the log's events stream (HB_EVENTS) as a CloudEvent, beside the Kubernetes Event, from the same call and with the same fields ([HBEP-8](../hbep/0008-observability-and-events/README.md), The durable log, Who writes). The Kubernetes Events go on as before and keep their 7 days; the log keeps the events for the grant and never evicts one. An event with no type, such as the Provider controller's diagnostics, stays in the cluster's own events only.

- **What is published.** One message per event, on the subject `hb.ev.<kind in lower case>.<UID of the object the event is about>`, with the event's id as its `Nats-Msg-Id`. An Allocation event that marks a transition has the id `<Allocation UID>/<reason>/<fact>`, so a transition raised again after a failed status write keeps its id; every other event has a fresh UUID. The message's `time` is the publishing process's clock, read just before the Kubernetes recorder's own, so it differs from the Event's `eventTime` by microseconds: match the two on the object's UID, the reason, the action and the note, with the times within 1 s.
- **Delivery is at least once.** Each controller and the front hold a queue of 1,000 events and publish them in order. An event is retried with backoff, and its connection to the log made again, for at most 100 s from when it was queued, which is inside the log's two-minute window for duplicates, so a retry of a publish whose acknowledgement was lost stores nothing more. After the window a retry would store a second copy, which is why none is made. A log that is down for less than that costs nothing; a restart of the log is ridden out by these retries, not by the supervisor's start barrier, which covers only the start. Nothing waits for the log: a failed publish never fails a reconcile or a request.
- **An event the log cannot take is counted, never lost silently.** `hpcburst_events_unlogged_total{controller}` counts it, one series for each of `lifecycle`, `front` and `provider-<name>`, each starting at 0 on a process that runs with the log (and absent on one that runs without). It counts an event when the queue is full, when its 100 s run out, when the events stream is full, when the event's regarding object has no UID (it has no subject: `logs/controller.log` or `logs/front.log` names its kind and name), and when the Provider controller refuses an event's note for naming a job ID. An event the stream refuses for space ("maximum bytes exceeded", HB_EVENTS being capped under `discard: new`) is final for that event: it is counted at once and not retried, since the stream stays full until its cap is raised (`--event-log-events-cap`, [above](#the-event-logs-lines-beta)). The event is still in the cluster's own events for their 7 days.
- **`EventsNotLogged`.** A `degraded` Warning event regarding the process's own `Lease` (`lifecycle`, `front`, or `provider-<name>`, in `hpcburst-system`), whose note gives how many events were not logged and since when. It is raised at the first loss after the process started or after the log last took an event, so a log that was refused at start or is not up yet raises it; then at most once an hour while losses last; and a closing event with the first event the log takes after it returns, so the log itself shows the span it lacks. The closing stays owed until it is stored: if the log goes down again before it is, the next closing says since the earliest loss no stored closing has reported, with the counts of the runs added. A run of losses that includes a full publisher queue ends only when the log takes an event, of any controller of the process, while the queue is below half its size, so a live but slow log under a burst raises one event for the run and one closing, not a pair for every dropped event. It goes through the same recorder as every event and is published like one; when it cannot be published itself it is counted, and never raises another. A refused note, or an event with no subject, is counted and raises none, since the log did not fail. A Provider that is deleted takes its series with it, and one that changes kind keeps counting and raising under the same name through the manager of its new kind: its series go with the last manager of the name. An event a dropped manager left queued, lost afterwards, counts nothing and raises nothing, and `logs/controller.log` has one line for it. The `lifecycle` Lease is absent when the controller runs with leader election turned off: the loss is then counted, one line in `logs/controller.log` says that `EventsNotLogged` has no Lease to regard, and nothing is raised.
- **What to do.** Read `hpcburst status`: the log's own line says whether it runs, was not started for space, or is full. An `EventsNotLogged` in `kubectl get events -n hpcburst-system` with the log running again means the log was down or full for a while, and the events of that span are in the cluster's own events only. A crash of a controller loses the events still queued in that process; its next start's `ObservationGap` names the gap.

### The router and several workers (beta)

An Instance runs one engine worker with nothing in front of it, unless you ask for more. Two beta features change that, and both are off unless you switch them on: the router, Envoy run in the job in front of the Instance's workers (`hpcburst up --instance-router envoy`), and several workers, copies of the engine's launch on the node's GPUs behind it (`spec.workersPerInstance`). Several workers always run behind the router. The quickstart's [router section](quickstart.md#beta-instances-behind-a-router) starts the host with the router; this section covers what to do next, what to read and how to turn both off ([HBEP-5](../hbep/0005-llmlab-compatible-front/README.md), Inside an Instance; [HBEP-7](../hbep/0007-lifecycle-controller/README.md), Several workers).

- **Turn it on:** fetch Envoy's build into the agent builds, from a clone of the repository since the release archive does not ship it, then start the host with the flag. `up` refuses a second host, so stop a running one first, and give the new one **the same flags as before** ([Down and up across an upgrade](#down-and-up-across-an-upgrade)):

  ```
  hack/fetch-envoy.sh --agent-builds "$STATE/agent-builds"
  bin/hpcburst down --state-dir "$STATE"
  bin/hpcburst up --state-dir "$STATE" --bin-dir "$BIN" --manifests config/crd \
    --front-config ~/.config/hpcburst/front.json \
    --front-mode host-dialed --instance-router envoy
  ```

  - `up` and `status` then check `envoy-<cpuArch>` for each architecture ([The agent builds](#the-agent-builds)); a missing or wrong build is named with the fetch command above.
  - A router is a property of each Instance, recorded when the controller creates it as `spec.launch.router`. Only Instances created after the host restarted with the flag get one, so a running Instance keeps what it was made with.
  - Then ask for several workers on a ModelDeployment: `spec.workersPerInstance`, from 1 to 8, absent being 1. Each worker uses `tensor-parallel-size` times `pipeline-parallel-size` GPUs of the Instance's one node, and the Instance's Allocation asks for the count times that.

    ```
    hbk patch modeldeployment <name> --type=merge -p '{"spec":{"workersPerInstance":2}}'
    ```

    The count applies to Instances created after the change, a replacement at a handover or a scale-up. Running Instances keep their count, so a deployment can run mixed counts for a while.
- **What is refused, and why.** A refusal is the deployment's `Degraded` reason [`SpecUnsupported`](#specunsupported), with its cause in the message. The controller creates no Allocation for the deployment while it holds, replacements included, and Instances already running serve until they drain. The node's glibc is the exception: the agent on the node refuses that one, so its cause is in the agent's log and the deployment shows only the effect.
  - **Several workers on a host started without the router.** The message says "workersPerInstance <n> needs the lifecycle controller's --instance-router envoy". Several workers run behind a router, and a job is staged with Envoy only when the host was started with the flag. Start the host with it, as above, or set the count back to 1.
  - **A count over what the node gives.** The Allocation's GPU count is `spec.workersPerInstance` times `tensor-parallel-size` times `pipeline-parallel-size`, on one node. It is refused on a Provider, never capped, above the partition's `gpusPerNode` or the QOS's `maxGpusPerUser`, and the message names the count, the limit and where the limit comes from: "above the <m> per node of partition <p>", or "above the maxGpusPerUser <q> of QOS <name> on Provider <provider>". With one GPU per worker the nodes hold at most 8 workers on Athena's A100 nodes and 4 on Helios's GH200 nodes, and fewer where the QOS the Provider submits under caps a user's GPUs: `plgrid-now` (`profiles/athena-now.yaml`) and Helios's QOS `now`, which the shipped Helios example submits under (`profiles/helios.yaml`), allow one, so no count above 1 fits under either. A Provider that names a QOS without that cap gets the node's count. It holds while every allowed Provider that is usable refuses; a Provider that fits, made usable, clears it ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Several workers, Sizing).
  - **A profile whose restart ladder cannot fit the count.** Each worker's rung, the router's and the whole Instance's are kept in `status.restarts`, which holds 32 entries. A Provider whose profile sets `restart.attemptsPerRung` so that (workers + 2) times it is above 32 refuses the count: the message says "(workers + 2) x attempts". The default of 2 attempts fits all eight workers.
  - **A node whose glibc is older than 2.30.** Envoy runs bare in the job, so before it starts the router the agent reads what the Envoy build needs and the node's glibc, and refuses an older node. The agent's log, which the job keeps as `job.err` in `<stagingDir>/alloc/<tag>/`, says `router did not start` with "Envoy needs glibc 2.30 or later, and this node's glibc is <version>: Envoy cannot run bare here". The controller sees the router part exit with code 127, the start-failed code, and treats it as the router's exit: `failing` events `RouterRestart` ([below](#routerrestart)) up to `restart.attemptsPerRung` times, then the whole Instance's restarts, then the Instance ends with `RestartLoop` and its Allocation is released. The replacement lands on a node of the same kind, so either turn the router off (below) or use a partition whose nodes have a newer glibc.
- **What to read.** An Instance's own object holds both, in `status`:

  ```
  hbk get instance <instance> -o yaml
  ```

  - `status.healthyWorkers`: how many of its workers the router holds healthy and not ejected, from the agent's report of Envoy's `/clusters`. It is absent without a router and until the first report that carries the router's view. The front's cap for the Instance is `spec.launch.maxNumSeqs` times it, and an Instance at 0 is not eligible, so the front routes nothing to it.
  - `status.workers[]`: one entry for each of its `spec.launch.workers`, written as each worker is verified, qualified and warmed up, and absent for an Instance of one worker, whose state is the Instance's own conditions. An entry has the worker's `name` (`worker-<i>`), the restart generation last verified in `verifiedGeneration`, its latest verification record in `record` and that record's verdict in `verdict`, whether its warm-up is done in `warmedUp`, and the generation at which the router first held it healthy in `routerAdmittedGeneration`.
  - A worker's verification record is named `ver-<instance UID>-<worker>-<generation>`, and is read as in [`VerificationFailed`](#verificationfailed). With one worker it keeps its name, `ver-<instance UID>-<generation>`.
  - A restart of one worker shows as a `failing` event `WorkerRestart` that names the worker, and a `worker-<i>` entry in `status.restarts`. The Instance stays `ready` on its other workers, and its own `Verified`, `Qualified` and `WarmedUp` conditions stay true while the worker verifies and warms up again. With one worker, a worker restart leaves `ready` for `verifying`, as in alpha.
- **Turn it off (rollback).** `hpcburst down`, then `hpcburst up` without `--instance-router`, with the other flags as before:

  ```
  bin/hpcburst down --state-dir "$STATE"
  bin/hpcburst up --state-dir "$STATE" --bin-dir "$BIN" --manifests config/crd \
    --front-config ~/.config/hpcburst/front.json \
    --front-mode host-dialed
  ```

  - New Instances get no router, and a deployment that asks for several workers gets no new Instance at all (next bullet). Running Instances keep `spec.launch.router` and `spec.launch.workers`, and their router and workers, until they end: the controller serves each from its own copy, whatever the host's flag, and its router rung still acts.
  - Every deployment whose `spec.workersPerInstance` is above 1 turns `Degraded` with `SpecUnsupported` at once and gets no new Allocation, so an Instance that drains is not replaced. Set the count back before you restart the host, or right after:

    ```
    hbk patch modeldeployment <name> --type=merge -p '{"spec":{"workersPerInstance":null}}'
    ```

  - The Envoy builds stay in `agent-builds/`, and `status` stops checking for them. A later `up` with the flag reuses them.

### Hedging across Providers (beta)

A ModelDeployment that allows two or more batch Providers hedges: when a missing primary or a replacement is predicted to start late on its Provider, from that Provider's observed queue waits, the controller submits one more Allocation for the same need on the next Provider in the deployment's order. The first of them to serve wins. Pending losers are cancelled at once, with no approval, and a loser whose job has started ends with `HedgeLost` and keeps its Allocation as warm capacity under the deployment's retention policy ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Hedging (beta)).

- **It is on by default.** `spec.hedgeWidth`, the most Providers one hedge spans, is 2 unless you set it, so every deployment with two allowed Providers hedges.
- **To turn it off,** set the width to 1:

  ```
  hbk patch modeldeployment <name> --type=merge -p '{"spec":{"hedgeWidth":1}}'
  ```

  Lowering the width applies to members added after it and cancels none, so it releases nothing that already runs.
- **What to read.** While a hedge has no winner, `Progressing` names its Providers ("hedge <first member> spans Providers A, B"). Each loser raises a `replaced` event with reason `HedgeLost` regarding it, with the winner in `related`, and a cancelled loser's Allocation carries the release cause `Instance <loser> ended: hedge lost to <winner>, on Provider <Provider> (hedge <first member>)`. A hedge sets no `Degraded` reason of its own, and a Provider it passes over (not `Ready`, too short, its class paused, its per-user caps or submit slots full) sets none either.
- **A kept loser is reused.** A kept hedge loser or a ranked-out Allocation takes the deployment's next missing primary before any new submission, and a replacement when it drains at least an Instance's lead (the drain window plus `handoverMargin` plus `health.startTimeout`) after the Instance it replaces ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Provisioning, Where).

### A controller restart

The supervisor restarts the controller when it exits, so a restart needs nothing from you. `hpcburst status` counts it on the `supervised controller:` line, with the last exit's detail, code and time. While it starts, its readiness reads `starting`, then `not leading` until it is elected leader of the `lifecycle` `Lease`, which can take a `Lease` duration after the old process's last renewal, then `ready`.

The restarted controller re-lists every object and continues from what it observes; nothing it needs is held only in memory ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Operational readiness 2). On taking its `Lease` it emits one `degraded` event `ObservationGap`, regarding the `Lease`, whose message names when the gap began; transitions missed meanwhile are not replayed, and the conditions are the truth ([HBEP-8](../hbep/0008-observability-and-events/README.md), Gaps after a restart). In beta each Provider's manager raises the same event regarding its own `provider-<name>` `Lease` when it takes it. The agents register again within a poll (in `host-dialed` mode, once their control relay is back), and no `AgentUnreachable` is judged until then. In `direct` and `ssh` modes established tunnels stay up and no stream is cut. In `host-dialed` mode they do not: the controller owns the relays, and they die with its process, so a restart cuts the streams in flight on them. The owner accepted that for alpha (9 Nov 2026, "Accept cuts for alpha", [PR 298](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/pull/298#issuecomment-6749435073)). The front counts each as cut, and a request already written is never replayed. The new leader starts both relays of every running Allocation within the backoff, 1 s doubling to 30 s with jitter, and frpc logs in again once the serving relay is back. A relay keeper, a separate process that would keep the relays across a restart, is beta ([HBEP-6](../hbep/0006-instance-reachability/README.md), open question 13). A loss of the Provider's own `Lease` (`provider-<name>`) does the same to that Provider's relays: a copy that stops leading stops its relays by PID before it releases the `Lease`. A `ready` on the `supervised controller:` line speaks for the `lifecycle` `Lease` alone, so check the relays with `pgrep -a -P <pid>` ([Host-dialed relays](#host-dialed-relays)). If an old copy still runs while the new one leads (an unfenced hand-over, [HBEP-11](../hbep/0011-kubernetes-control-plane/README.md)), the agent keeps the relay with the higher fence. The old copy's relay is refused as `superseded` and not started again, so the two copies do not take turns cutting streams, and the new copy's relays stay up. Events that repeat at most once an hour, such as `HeartbeatMissed`, fire once more.

```
hbk get leases -n hpcburst-system
```

A `restarts` count that keeps rising is a crash loop: `logs/controller.log` in the state directory says why.

### Down and up across an upgrade

`hpcburst down` stops the supervisor, which stops the controllers, the front and frps, then k3s, and confirms that nothing of theirs still listens. It removes the host's record and its admin `kubeconfig`, and keeps the data directory, the identities' kubeconfigs in `identities/` (the agents', and the controllers' in `identities/system/`, which the next `up` issues again), the record of the k3s release that wrote the store, and the agent endpoint's certificate, whose pin the running agents hold. It does not call the clusters: jobs keep running to their hard ends. While the host is down no front serves requests, and the tunnels drop until frps returns.

To run a new build of the framework, stop the host, rebuild, and start it with **the same flags as before**, since `down` forgot them. Without `hpcburst up --front-mode` (and `--front-addr`, in `direct` and `ssh` modes) every deployment shows `FrontNotConfigured`; in `host-dialed` mode the flags are `--front-mode host-dialed` alone, with no `--front-addr` and no `--front-key`, and the jobs' connections stay down after `up` until the controller has started the relays again; with another `--port-base` the agents' restricted keys and recorded ports no longer match. `--manifests config/crd` applies the CRDs again with server-side apply, which is safe to repeat. `up` also installs the CRD deletion guard on every start, whatever `--manifests` names, so a restart never leaves the host without it ([What never to do](#what-never-to-do)). It installs the three admission policies and the agents' namespace, roles and bindings on every start too, in the order that opens the agents' writes only once the policies are enforced, and sets them back to the framework's if they were edited ([Giving an agent its own identity](#giving-an-agent-its-own-identity)); the agents' identities and their kubeconfigs in `$STATE/identities/` stay across a restart, and so do the controllers' kubeconfigs in `$STATE/identities/system/`, which every `up` and `upgrade` issues again with new tokens ([The controllers' own identities](#the-controllers-own-identities)).

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

`hpcburst down --grace` and `hpcburst upgrade --grace` set how long each waits after SIGTERM before SIGKILL, 70 s by default: the front stops first, with up to 30 s to cut its requests and flush its usage records, then the controller and the relay keeper, with up to 34 s each to release their Leases and stop the relays. Both refuse a grace too short for that. A stop ends as soon as every process has exited.

### Giving an agent its own identity

Each agent works as its own Kubernetes identity, not as the host's admin ([HBEP-10](../hbep/0010-agent-primitives/README.md), Identities and roles; [HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Single-user host). An identity is a ServiceAccount in the namespace `hpcburst-agents`. Two commands make and renew it; both need the host running, take the same `--state-dir` as `up`, and print the kubeconfig's path and the token's expiry, never the token (`--json` prints both as JSON):

```
bin/hpcburst identity add agent-a --state-dir "$STATE"
bin/hpcburst identity renew agent-a --state-dir "$STATE"
```

- **`add`** creates the ServiceAccount `agent-a`, asks the API server for a token that lasts 30 days (it reports the expiry the API server answered with, which is shorter if the server caps tokens), and writes `$STATE/identities/agent-a.kubeconfig`: mode 600, in a directory of mode 700. The file holds this host's loopback server, its CA and the token, and none of the admin's credential. The agent works with `KUBECONFIG="$STATE/identities/agent-a.kubeconfig"`.
- **`renew`** asks for a new token for an identity that exists and rewrites the file. The old token stays valid until it expires: a token cannot be revoked before then other than by deleting the ServiceAccount from the namespace `hpcburst-agents`, which ends the identity.
- **The name** must be a DNS-1123 label, else the command exits 2. `add` of a name that exists exits 1 with "use renew" and leaves the file as it was; `renew` of a name with no ServiceAccount exits 1; a host that is not running exits 1. The directory `identities/` stays when `down` stops the host, and the tokens stay valid across a restart.

**What the roles grant.** `up` installs the roles on every start, from `config/rbac/agents.yaml`, and resets them to it: a rule added or removed by hand, a binding with another subject, or a binding whose `roleRef` differs is set back at the next `up`, and `status` says so until then. The two agent roles are bound to the GROUP `system:serviceaccounts:hpcburst-agents`, which every ServiceAccount of the namespace belongs to, and never to one agent by name: a ServiceAccount made in another namespace gets nothing of them, and no agent can hold the roles and be outside the group that HBEP-10's admission policies match.

| Role | Bound to | Grants |
|---|---|---|
| `hpcburst-agent-read` | the agents' group | `get`, `list` and `watch` of Providers, PlatformProfiles and ApprovalPolicies |
| `hpcburst-agent` | the agents' group | `get`, `list` and `watch` of ModelDeployments, Allocations, Instances, Operations and Approvals, of ControllerRevisions, of Events (core and `events.k8s.io`) and of Leases; and the writes the admission policies bound: `create`, `update` and `patch` of ModelDeployments (the scale subresource: `get`, `update`, `patch`) and of Operations, and `create` of Approvals. It has this wide form once `up` has seen the policies enforced, and the narrow, read-only form (`config/rbac/agents-narrow.yaml`) before |
| `hpcburst-operator` | no one | all of `get`, `list`, `watch`, `create`, `update`, `patch` and `delete` of ApprovalPolicies |

An agent never gets `delete` or `deletecollection`, any `*/status`, writes to Allocations or Instances, ServiceAccounts or their tokens, Secrets, CertificateSigningRequests, `impersonate`, RBAC or admission objects, or writes to Namespaces. The person keeps the host's admin kubeconfig (`$STATE/kubeconfig`) to act on the framework themselves; an agent that is handed it is outside every limit here ([HBEP-10](../hbep/0010-agent-primitives/README.md), Security).

#### What an agent may and may not do to a ModelDeployment

Three admission policies bound the agent role's writes (`config/admission/`; [HBEP-10](../hbep/0010-agent-primitives/README.md), The capacity guard, Owner and purpose, Confirm, Approvals). The capacity guard applies to the agents' group alone; the owner pin and the approver pin apply to every identity, the person's included. Every refusal names its policy (`ValidatingAdmissionPolicy 'hpcburst-capacity-guard' ... denied request: <message>`), and the message says what to do instead. The guard is an allowlist: an agent's update is accepted only if every change in it is on the list, and every other field must stay as it is, so a field a later release adds to the CRD is refused until someone allowlists it. A merge patch, a server-side apply, the object's update and `kubectl scale` are all judged on what they would store.

| An agent's write | Result | What to do instead |
|---|---|---|
| a higher `spec.instances`, through the object, a patch, an apply or the scale subresource | accepted | |
| a lower count (`spec.instances`, or scale, to any count, 0 included) | refused: "a lower count is an Operation of action Scale" | create an Operation of action Scale, a Kubernetes object ([HBEP-10](../hbep/0010-agent-primitives/README.md)) |
| a later `spec.endTime`, or its removal | accepted | |
| an `endTime` set where there was none, or moved earlier | refused: "an earlier end is an Operation of action End" | create an Operation of action End |
| an edit of `spec.source.served`, or any write that leaves `spec.source` as it is (the purpose or a higher count of a deployment with a managed source) | accepted; an edit of the serving spec rolls out make-before-break | |
| a change of `spec.hedgeWidth`, up or down | accepted: lowering it cancels nothing, and raising it reserves capacity, which the owner allowed (HBEP-10, The capacity guard) | |
| `spec.source` switched to `managed`, or any other change of `spec.source` | refused: "an agent may edit spec.source.served only" | the person edits it |
| the purpose annotation `hpcburst.groundnuty.github.io/purpose` (at most 256 characters, checked when it is set or changed: a longer purpose that an object carries from before the pin is left alone, so the object stays updatable) | accepted | |
| `kubectl.kubernetes.io/last-applied-configuration`, beside any accepted edit | accepted: kubectl's client-side apply rewrites it on every apply | |
| any other spec field (`model`, `providers`, `retentionPolicy`, `leaseLimit`, `workload`, `routing` including `maxRequestDuration`, and every field the CRD gains, unless the guard names it), a label, any other annotation, a finalizer, an owner reference | refused, each naming what it may not change | the person makes the change |
| the owner annotation `hpcburst.groundnuty.github.io/owner` | refused for every identity, the person's too: it is fixed at create | |
| a delete or a collection delete | refused: "plan it as an Operation of action End with delete: true" (RBAC refuses it first, since the role has no `delete`) | create an Operation of action End with `delete: true` |
| a create | accepted with the agent's own user name in the owner annotation (as the API server reports it, such as `system:serviceaccount:hpcburst-agents:agent-a`) and no `ownerReferences`; refused without the annotation or with another's, and with an owner reference (the garbage collector would delete the deployment when its owner goes, which releases its Allocations) | |
| a write of a status, an Allocation or an Instance | refused: "agents never write ..." (RBAC refuses it first) | the lifecycle controller does |

An **Operation** is created with `spec.owner` set to the creator's own user name, with no owner reference or finalizer, no `spec.confirm` and, whatever status it carries, no status that is kept. Only its owner confirms it, only after the controller has written a plan in its status, and only with that plan's `status.plan.id`: a confirm by another identity (the person too), with no plan or with another plan's id, is refused by the owner pin. An **Approval** is accepted only if `spec.approver.username` is the requester's user name and `spec.approver.groups` are the requester's groups, in any order. The lifecycle controller plans an Operation, counts its Approvals, and, once every target that needs one has it, checks the plan once more and commits it ([A stale Operation](#a-stale-operation)).

**How `up` opens the agents' writes.** No moment of `up` lets the agent role grant a write before the policy that bounds it is enforced, because the API server learns of a policy after its create returns. In this order, `up` installs the CRD deletion guard; applies the agent role in its NARROW, read-only form, FIRST, so that on a restart the wide role an earlier `up` persisted is narrowed before anything else, and a policy or binding removed by hand in the meantime never meets the wide role; installs or resets the three policies; waits up to 60 seconds until each policy refuses its own server-side dry-run probe; and only then applies the WIDE form, before it starts the controllers. `up` returns only when all three are enforced and the wide role is applied. If a probe never refuses, `up` fails with an error that names the policy, stops the host as it does for its other failures, and leaves the role narrow. If the ModelDeployment, Operation or Approval CRD does not exist (`hpcburst up` without `--manifests` on a new host), the policy cannot be probed: `up` warns that the agents' writes stay off, does not fail, and keeps the role narrow until a later `up` finds the CRD and probes it; `status` prints `agent roles: present, WRITES OFF`. The probes are dry-run creates: the admin's create of an Operation or an Approval that names someone else, and, for the capacity guard, the admin's create of a ModelDeployment with an owner reference while impersonating an agent (with `system:masters` too, to get past RBAC). A policy deleted by hand, or its binding, is back after the next `up`; `status` reports it as absent until then.

### The controllers' own identities

The processes that call the API server on the host, other than the launcher, each run as an identity of their own, so that the API server's audit log tells a controller's write from a person's ([HBEP-10](../hbep/0010-agent-primitives/README.md), Audit; [HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Single-user host). Each is a ServiceAccount in the namespace `hpcburst-system`, with a ClusterRole and a ClusterRoleBinding of the same name, and a Role and RoleBinding of that name in `hpcburst-system` for its Lease, all from `config/rbac/controllers.yaml`:

| Identity | Process | Its kubeconfig |
|---|---|---|
| `hpcburst-lifecycle` | `hpcburst-controller`: the lifecycle and verification managers, the observers, the agent API and the tunnel state | `identities/system/hpcburst-lifecycle.kubeconfig`, the controller's kubeconfig flag |
| `hpcburst-provider` | `hpcburst-controller`: every Provider manager | `identities/system/hpcburst-provider.kubeconfig`, the controller's provider-kubeconfig flag |
| `hpcburst-front` | `hpcburst-front` | `identities/system/hpcburst-front.kubeconfig`, the front's kubeconfig flag |

The relay keeper and the durable log build no API client. The launcher keeps the admin kubeconfig for itself, and `$STATE/kubeconfig` stays for the person; no supervised process is told either.

- **Issued at every start.** `up` and `upgrade`, after k3s is ready and right before the supervisor starts, install the ServiceAccounts, roles and bindings, reset each to the file (a rule added by hand does not survive), and ask the API server for a token per ServiceAccount that lasts **one year** (it reports the expiry the API server answered with). Each is written into `$STATE/identities/system/<name>.kubeconfig`: mode 600, in a directory of mode 700, replaced atomically, holding this host's loopback server, its CA and the token. The supervisor passes each child the PATH of its file, never a token, and refuses to start when one is missing. A host that an earlier release started has neither the roles nor the files until its next `up` or its `upgrade`.
- **`down` leaves the files.** The next `up` issues new tokens to the same paths, with later expiries. A supervisor that restarts a crashed child reuses the files it has.
- **No rotation.** Nothing renews a token while the host runs. `hpcburst status` lists each identity with its expiry and prints a WARNING when 30 days or fewer are left; `hpcburst down` and `hpcburst up` issue new ones.
- **The stated risk** ([#660](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/660), the science agent's ruling S1; the owner's stance that security is light): a controller's token that leaks stays valid until it expires, up to a year. An unbound token cannot be revoked other than by deleting its ServiceAccount from `hpcburst-system`, which ends that identity until the next `up` makes it again. The files are readable by the host's user only, and no token is ever logged, printed or put on a command line.
- **What each role grants** is exactly what its component calls, enumerated from the code, call site by call site. Every controller's Lease lives in `hpcburst-system` alone, so the Lease verbs are the namespaced Role's: a Lease write in any other namespace is Forbidden. The lifecycle's and the front's Leases have fixed names, and their rules name them (the front's token cannot take the `lifecycle` Lease); the Provider managers' are one per Provider, made at run time, so that role covers the Leases of the namespace. No role has a wildcard, a Secret, a delete other than the lifecycle controller's own of Instances, Allocations and the ModelDeployment an End deletes, or any RBAC, CRD, admission or token object. A change to a controller that adds an API call extends its role in the same change.
- **What proves it, and what does not.** The controller's test harness runs the real binary on a test API server under these identities and fails any test on a refusal (403) in the API server's audit log, so it proves the rules it exercises: the leader Leases, the watches of the cached kinds, the Provider controller's writes, the tunnel state's Instance status write, and events. It does not exercise the lifecycle's ModelDeployment, Operation, Instance and Allocation reads and writes, or the front. Those are proved by the alpha stand-in's operations row, which runs the whole stack through `hpcburst up` and so under the identities, and at its end reads the API server's audit log: it fails on a refusal (403) of any controller identity, and on a log that is missing, empty or holds no successful request of each of the three identities, so a stack whose controller never reached the API server cannot pass. The other rehearsals run under the identities too but do not read the log. They are not required checks. CI's `launcher-k3s` starts a fake controller and the real front: it proves that the identities are issued and usable, and that the front's start-up paths run under its role, and nothing of the real controller.

### A stale Operation

An agent's **Operation** is committed only if the state it was planned against is still the state the controller reads when the last approval is in. The controller recomputes the plan once, after the last approval and before it writes anything. A different plan ID turns the Operation `Stale`: a pending Allocation started, another Instance became ready, a policy that applies was edited, or an earlier Operation on the same ModelDeployment changed the state. The controller raises a `degraded` event `PlanStale` regarding the Operation, and its note says what differed.

- **Means:** nothing was done to the ModelDeployment, its Instances or its Allocations. The Operation is finished (`status.finishedAt` is set) and `Stale` is final: its Approvals no longer count, and an Approval added afterwards changes nothing.
- **You:** plan again. Create a new Operation for the same change, read its new plan, confirm it and have it approved again. A finished Operation is not removed yet: it will be removed 7 days after it finished, once the audit log holds its content ([HBEP-10](../hbep/0010-agent-primitives/README.md), Removal).

### A partly failed Operation

An approved **Operation** acts on the targets it committed to, one by one, and reports each in `status.targets`: `Done`, `NotStarted` or `Failed`. A target fails when the controller's own write fails, or when a released Allocation stays present with its deletion asked for and the Provider's `Synced` condition false: the Provider could not cancel the job (the finalizer cancels every job with the tag before the Allocation goes). The Operation is `PartlyFailed` while any target is `Failed`, and the controller raises a `failing` event `OperationPartlyFailed` regarding it when that starts, then at most once an hour while it lasts.

- **Means:** the other targets were done, and the failed one is retried with backoff: it is never a single refusal. The target's `reason` and `message` carry the Provider's own (for a cancel that failed, `ReconcileError` and Slurm's text, cut to 1,024 bytes).
- **You:** read the Allocation named by the failed target, `hbk describe allocation <allocation>`: its `Synced` condition says what the Provider could not do, and the job's state is in `status.atProvider`. Fix that, such as the login node's reach to the scheduler. The Operation goes on by itself: the target is retried until it is `Done`, and the Operation is `Done` with it.

A failure that no retry can fix is **final**, and is never retried ([HBEP-10](../hbep/0010-agent-primitives/README.md), The report, as amended by #665, #666 and #668). The target is `Failed` with one of these reasons, and the Operation is `PartlyFailed` and finished (`status.finishedAt` is set, so it no longer holds the deployment's one-at-a-time slot) once every target is `Done` or final. A final failure is recorded only by a pass that acts, and no later pass acts on that target again, nor releases its Allocation under the Operation's name:

| Reason | Means | What happened |
|---|---|---|
| `JobStartedSinceCommit` | an Allocation approved for cancelling in the queue had its job start after the commit | not released: the approval was for a queued job, which is priced differently from a running one; its retention policy keeps or releases it |
| `SpecChangedSinceCommit` | the deployment's spec changed after the commit so that what the Operation was approved for no longer holds: a Scale's `spec.instances` is above its count, an End's `spec.endTime` is not set or is later than now | nothing more was acted on; the targets already done stay done, and the spec is never written over |
| `LastReadySinceCommit` | an Instance that a Scale to a count above zero was to cancel or end turned ready and is the last ready one when the Operation acts, or the Allocation it keeps from emptying | not drained: draining the last ready Instance needs an approval of its own. A Scale to zero is the shutdown, like an End, and drains every Instance |

- **You:** read `status.targets` for the targets that failed, and plan again if the change is still wanted, from the deployment as it is now.

### The audit log

The API server writes an audit log of every request, and with the controllers' own identities (above) it says who did what: who asked for an Operation, what was planned, who approved it, and what each controller wrote ([HBEP-10](../hbep/0010-agent-primitives/README.md), Audit; issue #660). `hpcburst up` writes the policy to `$STATE/audit-policy.yaml` at every start, from `config/audit/policy.yaml` (embedded, so the launcher installs the repository's file), and starts k3s with it; an edit of the copy does not survive the next start.

- **Where.** The log being written is `$STATE/logs/audit.log`, in a directory of mode 700, and the file is mode 600: the host's user alone reads it. Rotated files sit beside it, `audit-<time>.log.gz`.
- **What each level records.** The first rule that matches a request decides:

  | Level | What | Records |
  |---|---|---|
  | `None` | `get`, `update`, `list` and `watch` of Leases (`coordination.k8s.io`) in the namespace `hpcburst-system` by the three controller identities | nothing: a leader election's renewals carry no decision and would fill most of the log. A Lease `create` is still logged, at Metadata, and so is any Lease request in another namespace, which no controller role grants. A level is chosen from the request before the API server answers, so a Lease request **refused** inside `hpcburst-system` is not logged either; one in `kube-system` or any other namespace leaves a line at Metadata, refused or not |
  | `RequestResponse` | the writes (`create`, `update`, `patch`, `delete`, `deletecollection`) of Operations, their `status`, Approvals and ApprovalPolicies, by every user | the request and the answer with their bodies: the Operation with its spec and purpose, the plan and the per-target outcomes in each of the controller's status writes, each Approval with the plan ID it names. The reads (`get`, `list`, `watch`) of these kinds fall to Metadata: the controllers list them on every reconcile pass, and a list logged with its whole answer was most of an idle controller's log |
  | `Request` | `update`, `patch`, `delete` and `deletecollection` of Allocations by the lifecycle controller | the request body, so a release cause is logged before the delete that follows. An Allocation's `create` stays at Metadata |
  | `Metadata` | everything else | who, the verb, the object, the time and the answer, never a body: no Secret's content is ever logged |

  Each request is logged once, when it is answered (`RequestReceived` is omitted), and no logged body carries the objects' `managedFields` (`omitManagedFields`), which are about a quarter of a body's bytes.
- **Rotation, and nothing removed.** A file is rotated at 100 MB. The framework removes none. `hpcburst up` passes k3s's API server three arguments of its own, which are arguments of kube-apiserver and no flags of `hpcburst up`: `audit-log-maxbackup=0` and `audit-log-maxage=0`, which mean no limit on the number or the age of the rotated files, so the log is kept for the grant, and `audit-log-compress=true`, which gzips each rotated file once it is cold. Only the file being written stays plain. A rotated file is whole when its `.log` twin has gone; until then the plain file is the complete one. Nothing here deletes a file: clearing space is a person's step.
- **Reading it.** One JSON event per line. `zcat $STATE/logs/audit-*.log.gz` and `cat $STATE/logs/audit.log` together are the whole log. `user.username` is the identity (a controller's ServiceAccount, an agent's, or the person's), `objectRef` the object, `requestObject` and `responseObject` the bodies where the level has them.
- **Size.** Measured in the stand-in (the real lifecycle controller on a test API server, `internal/audit/e2e`), not on a live host: its figures per Operation and per minute of an idle controller are in the pull request that installed the policy, labelled "projected from the stand-in" when they are projected to a week. **The policy stays proposed until a live week on the alpha host** has measured the log under it (`@hpc-burst-science-agent`, 17 Nov 2026, #660).
- **What proves it.** The levels are judged by `k8s.io/apiserver`'s own audit policy evaluator on the embedded policy (`config/audit/policy_test.go`), and the log by its FILES: `internal/audit/e2e` runs a controller, two agents and the person against a test API server that writes the log, and finds each write under its own user name, no Secret value in any file (a random marker, as a whole token, plain and gunzipped), the rotated files gzip with none removed, and no Lease read or renewal; `cmd/hpcburst/controller_identities_k3s_test.go` shows that `up` writes the policy and starts k3s with the flags, and that an agent's Operation create is in the file at `RequestResponse`.

### Where the accounting records are

The Provider's controller writes one record per Allocation that outlives it: a ControllerRevision named `acct-<Allocation UID>` in the Allocation's namespace ([HBEP-8](../hbep/0008-observability-and-events/README.md), Provider accounting). It writes it while it deletes the Allocation, once no job with the Allocation's tag is live, a completing one included, and before the finalizer goes (`internal/providerctl/delete.go`). When the recorded job's final accounting read is missing or keeps failing, it waits up to the profile's `lostGrace` (2 minutes in the shipped profiles), then writes what it knows (`internal/providerctl/accounting.go`). So a record appears only once its Allocation is being deleted: released by the controller, by retention or by you, never while it is kept. It holds the Provider, the partition, the tag, the job IDs, `allocatedCPUs`, `allocatedGPUs`, `elapsedSeconds`, the start and end times, `basis: allocated`, and `unknown`, the fields that could not be read. It is written once and never changed. In beta it also holds `allocatedBilling` (the scheduler's billing count), `billingUnit` (the partition's, when the record is written), `grant` and `account` (the Provider's), each absent and named in `unknown` when it was not known; a record written before them has none of them and counts as unknown.

```
hbk get controllerrevisions -l hpcburst.groundnuty.github.io/record=accounting
hbk get controllerrevision <record> -o yaml
```

`hpcburst accounting [--json]` totals the billed units of the kept records and the live Allocations per grant and account, each unit (GPU-hours, CPU-hours) on its own line and never added to another, with the count of unknown members beside each sum (a pending Allocation, a billing count, an elapsed time or a billing unit not read, a record written before the beta fields) and the whole days left to the Providers' `grantEnd` (the earliest of a line's, as the Provider specs say now: a record whose own grant differs from its Provider's current grant still shows that Provider's grant end). It reads the API with `$STATE/kubeconfig` (`--kubeconfig FILE` names another) and writes nothing; every figure is allocated, not billed, and its output names grants and accounts, so it stays on the host ([HBEP-8](../hbep/0008-observability-and-events/README.md), Provider accounting, Billing).

A live Allocation's numbers so far are in its `status.atProvider.accounting`. The identity records (`id-`) and verification records (`ver-`) are ControllerRevisions of the same kind, labelled `identity` and `verification`. No record has an owner, so garbage collection never removes them, but deleting the namespace does: export them first ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Ownership, records and deletion).

### A deployment ID's records, through the front

Any client with a key reads a deployment ID's identity record and verification records from the front: `GET /hpcburst/v1/deployments/<id>` on the client listener, with the client key as for any request, and the ID's full form or its 12-character display form, of a served deployment or a managed entry ([HBEP-9](../hbep/0009-identity-and-attribution/README.md), the record fetch). An ID with no identity record answers 404 `deployment_not_found`, and a record the front cannot read 503 `records_unavailable`. The answer's `canonical` is the identity record's object as RFC 8785 text: the front serves records as they are stored, without checking them, and a client checks the ID by hashing that text, whose SHA-256 is the full ID. Saving the answer is how records are exported, before a namespace is deleted or for a replication package.

The front reads them uncached, with lists of `apps/v1` ControllerRevisions selected by `hpcburst.groundnuty.github.io/deployment-id=<display form>` and `hpcburst.groundnuty.github.io/record in (identity,verification)`: one list over all namespaces when it watches all, or one per namespace it watches and `hpcburst-system`, where managed entries' records live. That is the only access the endpoint adds. The front runs as its own identity, `hpcburst-front` ([The controllers' own identities](#the-controllers-own-identities)), whose role holds `list` on `controllerrevisions` in the `apps` group for those namespaces. Kubernetes RBAC cannot narrow a rule by label, so that rule reaches the `acct-` records too.

## What never to do

- **Never remove a finalizer by hand.** An Allocation's finalizer, `hpcburst.groundnuty.github.io/cancel-all`, is what cancels every job with its tag and writes the `acct-` record. Remove it, and the object goes while its job may run on, held by no one, with no record of its hours. An Instance's finalizer, `hpcburst.groundnuty.github.io/drain`, is what lets it drain before it goes. A deletion that waits is waiting for a job, usually a [stuck COMPLETING job](#a-stuck-completing-job) or an unresolved submission (`SubmissionResolved`): fix that instead ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Ownership, records and deletion).
- **Never edit `status`.** The controllers write status and act on it: an Allocation's phase and `jobCompleting` decide its Instance's end, the slot count and the finalizer; an Instance's phase and conditions decide what the front routes to. An edit is overwritten at the next pass, or acted on before that. Change the spec, or use the one annotation meant for you, `abandon-attempt`.
- **Never edit the annotations the controllers write** on an Allocation, such as its job ID, its submission record or its tag: they are its record of which job is its own ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), The Allocation's record).
- **Never delete the framework's CRDs.** That deletes every resource of their kinds, and the finalizers would cancel every job ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Risks). An admission policy refuses it: `hpcburst up` installs it on every start, whatever `--manifests` names, and `hpcburst status` says whether it is there ([The lines, in the order they print](#the-lines-in-the-order-they-print)). To uninstall the framework, remove the guard first, with the one command that does:

  ```
  bin/hpcburst guard remove --state-dir "$STATE"
  ```

  Its help gives the warning to heed: it removes the binding, then the policy, and framework CRDs can then be deleted, which deletes every resource of those kinds and cancels their provider jobs. Run it only to uninstall the framework. It is repeatable and needs the host running. `down`, `upgrade` and a restart never remove the guard, and the next `up` installs it again.
- **Never give an agent the admin kubeconfig** (`$STATE/kubeconfig`). It is the person's. Give each agent its own, with `hpcburst identity add` ([Giving an agent its own identity](#giving-an-agent-its-own-identity)); the policies and roles bound to that identity keep it from lowering capacity, and the admin kubeconfig has none of those limits.
- **Never expect `down` to stop the jobs.** It stops the host only. Scale the deployments to zero, or delete them, while the host still runs, and wait for `hbk get instances,allocations` to be empty first ([quickstart, step 7](quickstart.md#7-stop)).
