# Alpha runbook

The live runs that alpha's graduation criteria still need, each one command, ready the hour the owner's dial-home route exists (issue #194; the criteria are tracked on #145). Every run is a row of `hack/live-checks.sh`: it prints its queue estimate and runs only with `--yes`, takes the one-job slot only after the builds, cancels its own jobs by tag on every exit, and prints the slot use to post.

```
hack/live-checks.sh --list                    # every row, and the two aliases below
hack/live-checks.sh --plan <base>..<head>     # which rows cover what a range changed; runs nothing
hack/live-checks.sh --run <row>...            # shape and queue estimate, then "not run: pass --yes" (exit 4)
hack/live-checks.sh --run <row>... --yes      # the run
```

`tunnel-hour` and `gpu-accounting` are aliases: each stands for its `-helios` and `-athena` rows, run one after the other. A skipped row counts as a failure, and so does a test that cannot start for lack of a setting.

## State of the wiring

| Row | State |
|---|---|
| `gpu-accounting-helios`, `gpu-accounting-athena` | Complete. Runs through the Provider's controller; needs no dial-home route. |
| `alpha-athena-handover`, `alpha-helios-nooverlap`, `tunnel-hour-helios`, `tunnel-hour-athena` | The judging, the settings, the gating, the slot handling, the cleanup and the dry runs are in. The scenario that starts the stack and drives it waits for the one-host rehearsal (PR #161); until it is wired a live run fails at its first step with "the live scenario steps are not wired yet" and has used no cluster. |
| `alpha-helios-nooverlap` | Needs #174 (no-overlap queue-wait growth), which is on main (#208), so in every checkout that has this row. It needs no setting for it; it needs the owner's go (below). |

## Settings

The settings file is named by `HPCBURST_LIVE_SETTINGS`: `KEY=VALUE` lines, mode 600, owned by you (`hack/live-checks.sh`, header). Values never appear here, on the screen or in a post. Each missing setting is reported in one refusal (exit 3).

| Setting | Needed by | What it is |
|---|---|---|
| `LIVE_OUT_DIR` | every row | a private directory for logs and records |
| `LIVE_LOCK_DIR` | every row | the directory with the hosts' lock files |
| `LIVE_BUILD_LOCK` | optional | the shared build lock; builds take it, test runs never do |
| `ACCOUNT_ATHENA_A100` | Athena rows | the grant account for `plgrid-gpu-a100` |
| `ACCOUNT_HELIOS_GH200` | Helios rows | the grant account for `plgrid-gpu-gh200` |
| `LIVE_CONF_CREDENTIALS_ATHENA`, `LIVE_CONF_CREDENTIALS_HELIOS` | rows on that host | the Provider's ssh credentials file, mode 600 |
| `LIVE_DIALHOME_MODE` | the four stack rows | `direct` or `ssh` (HBEP-6, Dial-home modes) |
| `LIVE_DIALHOME_ADDR` | the four stack rows | `host:port` of the route, as the compute nodes see it |
| `LIVE_DIALHOME_KEY` | stack rows, `ssh` mode | the restricted dial-home key file, mode 600 |
| `LIVE_BIN_DIR` | the four stack rows | the directory with `k3s-1.36.5/` and the pinned frp (`hack/fetch-k3s.sh`, `hack/fetch-frp.sh`) |
| `LIVE_TUNNEL_LEASE_MINUTES` | optional, `tunnel-hour-*` | the tunnel rows' lease in minutes, 106 to 240; default 137 (below). The owner's choice when giving the go: the shape, the hold note, the timeout and the test follow it |
| `LIVE_OWNER_GO_HELIOS_NOW` | `alpha-helios-nooverlap`, `tunnel-hour-helios` | today's UTC date (`YYYY-MM-DD`), set only after the owner has said yes to the hold; a missing or older date is refused (exit 3) |

One environment variable, not a setting: `LIVE_PENDING_MAX_SECONDS` (default 7200) is how long a job may wait in the queue before the run cancels it. Athena's `plgrid-gpu-a100` queue was estimated at a start on 13 Nov for one GPU (#145, 1 Nov 2026): set it from the estimate that `--run` prints, which is the reason it is printed first. The script's timeout for a row is the row's run time plus this wait, except `gpu-accounting-helios`, whose start-wait bound of 10 minutes is its first clock and is already in its timeout.

## The one-job slot, the jobs a run holds, and the owner's go

Helios QOS `now` and Athena `plgrid-now` allow one job per user, and the owner shares them (#145, 1 Nov 2026): use them sparingly, batch checks, leave nothing held, and post each use. Before a row takes a slot the script reads it, refuses (exit 3) if it already holds a job, reads it again after the builds, and prints `slot use (<slot>): start <UTC> end <UTC> duration <hh:mm:ss>` when the run ends. That line goes in the post.

"One cluster job at a time" holds as one live run per host, plus the slot refusal before the start, plus the cleanup of every tag. It cannot mean one job, because two rows are about a successor beside a predecessor. The most jobs each row holds at once:

| Row | Most jobs at once | Where |
|---|---|---|
| `alpha-athena-handover` | 2: the predecessor and its replacement, together for the handover | `plgrid-gpu-a100`. **Not a one-job slot:** that partition has none, so the pair takes no one's slot. |
| `alpha-helios-nooverlap` | 2: the running predecessor and the pending replacement | QOS `now` on `plgrid-gpu-gh200`, the owner's shared slot |
| `tunnel-hour-helios` | 1 | QOS `now`, the shared slot |
| `tunnel-hour-athena` | 1 | `plgrid-gpu-a100`, no slot |
| `gpu-accounting-helios` | 1, for about 15 minutes at most (the 10-minute start wait and the job's few minutes) | QOS `now`, the shared slot |
| `gpu-accounting-athena` | 1 | `plgrid-gpu-a100`, no slot |

On Helios the replacement of `alpha-helios-nooverlap` is created at its planned time and pends until the predecessor's job ends and frees the GPU (the partition's QOS allows 2,000 submitted jobs, QOS `now` one GPU: `hbep/0007-lifecycle-controller/README.md:463`, `:608`). So the pending replacement sits in the owner's shared slot only for the planned gap, from its planned submission to the predecessor's end, and the run never queues a third job. Under `ReleaseWhenIdle` with a 2-minute `idleLimit` the predecessor's job is released that long after its Instance ends, so it does not hold the GPU to its hard end (`:471`; the default `Keep` would).

**The owner's go.** `alpha-helios-nooverlap` and `tunnel-hour-helios` hold QOS `now`'s one GPU for hours. Each prints, before its estimate, "holds QOS now's GPU for about 2 h 40 min; the owner's go is needed (the development rule on #145)" (2 h 20 min for the hour's row at the default lease; the note follows `LIVE_TUNNEL_LEASE_MINUTES`) and refuses unless `LIVE_OWNER_GO_HELIOS_NOW` holds today's UTC date. Set it only when the owner has said yes for this run; tomorrow's run needs a new go. The script reads the date again just before each protected row, and again after that row's builds, and refuses (exit 3) a row whose go has gone stale, also when an earlier row of the same command ran past UTC midnight. The hold is the lease plus, for no-overlap, 45 minutes for the successor to start and become ready, rounded up to ten minutes. It is a few hours at most, not the 12 h `maxWall` of QOS `now`.

**The Helios Provider serves one deployment, and that deployment allows it alone.** QOS `now`'s one GPU per user makes a second deployment's primary on the Helios Provider pend behind the first, an unmarked pend that, while fewer than 20 waits are kept, could set the plan for a normal-mode deployment there (`hbep/0007-lifecycle-controller/README.md:468`, "Alpha's fence", from PR #205). So `alpha-helios-nooverlap`'s ModelDeployment names the Helios Provider and no other. The verdict fails a deployment that allows any other Provider, or none, and the dry run shows it with a deployment that also allows Athena. The other stack rows are held to their own cluster the same way, because a run that could place its Instance elsewhere measures nothing.

Cleanup is by tag: each Allocation's job tag is written to `job-tags.txt` before anything can submit it, and the script cancels those jobs, and only those, on every exit, a signal included.

## The shortest lease

Every stack row's deployment sets a short `leaseLimit` and a short `routing.maxRequestDuration`, so that a handover or a gap fits in hours, not in the 48 h of the partitions. The lease cannot be shorter than the lead an Instance needs, or the Provider is skipped with `PartitionTooShort` and no Allocation is made. The lead is the drain window plus `handoverMargin` plus `health.startTimeout` (`internal/lifecycle/timelimit.go:22`), and an Allocation hosts an Instance only when its time limit is longer than the lead, in whole minutes (`timelimit.go:42`). The drain window is `routing.maxRequestDuration` plus the profile's `drainMargin` when that is shorter than its `drainWindow` (`api/v1alpha1/writers.go:87`).

With a `routing.maxRequestDuration` of 5 minutes and the shipped profiles' values (`drainMargin` 10 minutes, `handoverMargin` 30 minutes, `health.startTimeout` 1 hour) the lead is 1 h 45 min, the figure `profiles/athena.yaml:39` gives, and the shortest lease **1 h 46 min**, which the two handover rows use. Each stream they hold across a drain start must finish within the 5 minutes.

**The tunnel hour is one healthy session, not one stream.** The front cuts a request at the deployment's deadline (HBEP-5), and a deployment can only shorten the front's, so no stream can last an hour behind a 5-minute deadline. The science agent's ruling of 2 Nov 2026 reads `hbep/0006-instance-reachability/README.md:354` and `:375` as an hour of one tunnel staying healthy under real use, so the 5-minute deadline stays and the hour is what the tunnel carries across many requests.

**The tunnel rows' lease is chosen by measurement.** The hour of traffic (61 minutes) must end before the drain start, 15 minutes before the lease ends, so the Instance must be ready within `lease - 76 minutes` of its job's start (`LatestReady`): 30 minutes under the shortest lease, 1 h 46 min, and 61 minutes under 2 h 17 min (a start of up to `health.startTimeout`, the hour, the drain window and a minute). The rule: take GH200's measured start-to-ready with the cold SIF build included; under 20 minutes, keep 1 h 46 min, else 2 h 17 min. The measurement is the second real-engine run on Helios GH200 ([issue #94, comment 6557821833](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/94#issuecomment-6557821833)): a cold build with an empty image cache and a slow Hub; the job started at 11:42:26Z and its first Instance had qualified and warmed up at 12:06:05Z, **23 min 39 s** later. That is over 20 minutes, so the default is **2 h 17 min**, and the Helios row holds the slot for about 2 h 20 min. A failed hour costs that hold and a rerun, which is worse than the margin. The owner may choose the other value, or any from 106 to 240 minutes, with `LIVE_TUNNEL_LEASE_MINUTES` when giving the go; the hold note, the shape and the test follow it, and a start slower than the lease allows fails the row with "the hour ended after the Instance's drain start".

## The runs

Durations are plans, from the lease each row sets (`internal/alpha/rows.go`); the first run replaces them with a measurement. The queue wait comes on top of every one.

### `gpu-accounting-athena`, `gpu-accounting-helios`

```
hack/live-checks.sh --run gpu-accounting --yes
```

- **Settings:** the account, the credentials file, `LIVE_OUT_DIR`, `LIVE_LOCK_DIR`. No dial-home route.
- **Duration and slot:** one GPU job of 5 minutes' limit per cluster, holding its GPU for about 20 seconds; the run is about 10 minutes once the job starts. Helios uses QOS `now`: at most 15 minutes of the slot (below); Athena's `plgrid-gpu-a100` has no slot.
- **Two guards on Helios** (the science agent's rule of 2 Nov 2026, the same two as `conformance-helios-gh200`, which run the shared functions of `hack/live-checks.sh`):
  1. **The slot precondition.** The row starts only when `squeue --me` shows no job in QOS `now`, running or pending, the owner's or ours; otherwise it exits 3 with "skipped: the now slot is in use" before anything is built, estimated, submitted or cancelled (`check_slot`).
  2. **The start-wait bound.** The job must start within 10 minutes of its acceptance (`HPCBURST_CONF_START_WAIT`, the 14th column of the row's shape). Past it the test cancels the job by its tag, checks that none is left, writes `not-run.txt` and skips; the script then reports `NOT RUN`, not `FAIL`, runs the same cleanup and exits 3. No accounting record is written and nothing is posted except the slot use. Try again later. The bound is the row's one clock there, so `LIVE_PENDING_MAX_SECONDS` does not apply to it; it applies to `gpu-accounting-athena`, which has no slot and no bound.
- **What it does:** one GPU job goes through the Provider's controller. The job writes the GPUs it was given: the count of its `CUDA_VISIBLE_DEVICES`, which Slurm sets to the GPUs it gave the job (the agent forwards it for that reason, `internal/agent/apptainer.go:110`), and the count `nvidia-smi -L` lists. When the job has ended, the Allocation is deleted, which writes the kept record `acct-<UID>`; the record's `allocatedGPUs` is compared with the job's own count. The record is built from the scheduler's accounting, so it is not compared with the scheduler. `nvidia-smi` is posted and not judged: it lists the whole node where a cluster does not confine devices to the job. A 1-GPU GH200 job registered one GPU through `nvidia-smi` (`spikes/s94/results/r3/summary.json:988`), so Helios confines them; no Athena run shows it, which is why both counts are recorded. Where `CUDA_VISIBLE_DEVICES` is unset the `nvidia-smi` count is the witness, and the post says which was used.
- **Posts:** the run's `posts.md` (the record's GPU entry, the job's count and which variable gave it, the `nvidia-smi` count, the request) and the slot use. To #145 and #93 (the conformance runs of both clusters).
- **Pass criteria:**
  - `hbep/0008-observability-and-events/README.md:395`: "the Test plan passes, and the kept accounting record's GPU entry is checked once on a real GPU job on each alpha partition;"
  - `hbep/0008-observability-and-events/README.md:331`: the kept record holds "`allocatedGPUs`" and "`basis: allocated`".
  - Checked: the record's partition is the row's; its basis is `allocated`; the job asked for and saw at least one GPU (a CPU job would agree at 0); the record's GPU entry equals the count the job was given, which equals the request; an unknown entry fails.

### `alpha-athena-handover`

```
hack/live-checks.sh --run alpha-athena-handover --yes
```

- **Settings:** the account, the credentials file, the four dial-home and binary settings.
- **Duration, slot and jobs:** two Allocations of a 1 h 46 min lease on `plgrid-gpu-a100`, one GPU each, together for the handover; the run is about 2 h once the first job starts (the first Instance's start, the serving until its drain start 15 minutes before the lease ends, the handover, the drain). At most 2 jobs. No slot: the partition has none, and no one's slot is taken.
- **Its deployment:** the Athena Provider alone, `leaseLimit` 1 h 46 min, `routing.maxRequestDuration` 5 minutes.
- **What it does:** one deployment through the front and the real tunnel, with streams open across the predecessor's drain start; the successor takes over; one restart at the worker rung.
- **Posts:** `posts.md` (handovers completed, streams in flight at the drain start and cut, worker-rung restarts, responses with attribution) and the run's lines (no slot). To #145 and #93.
- **Pass criteria:**
  - `hbep/0007-lifecycle-controller/README.md:635`: "on Athena `plgrid-gpu-a100`, one deployment completes one handover end to end through the front with no request cut;"
  - `hbep/0007-lifecycle-controller/README.md:637`: "on each alpha platform, Helios `plgrid-gpu-gh200` and Athena `plgrid-gpu-a100`, one restart at the worker rung;"
  - `hbep/0005-llmlab-compatible-front/README.md:450`: "on Athena `plgrid-gpu-a100`, through the front, one handover cuts no request (with HBEP-7)."
  - Checked: the deployment is as above; at least one stream was in flight at the drain start (`hbep/0007-lifecycle-controller/README.md:597`: "A run with no stream in flight at the drain start proves nothing here."); none was cut or left unfinished; exactly one handover; the successor ready before the predecessor ended; exactly one worker-rung restart; at least one response, and every response attributed (a run that recorded none has shown nothing); exactly two jobs in the cluster's own record, by job ID, none a duplicate under a tag, both on `plgrid-gpu-a100` with the lease as their time limit.

### `alpha-helios-nooverlap`

```
hack/live-checks.sh --run alpha-helios-nooverlap --yes     # with LIVE_OWNER_GO_HELIOS_NOW set to today's date
```

- **Settings:** the account, the credentials file, the four dial-home and binary settings and the owner's go, `LIVE_OWNER_GO_HELIOS_NOW`.
- **Duration, slot and jobs:** two Allocations in turn under QOS `now` on `plgrid-gpu-gh200`, one GPU, a 1 h 46 min lease each at most. The predecessor serves to its drain start, drains, and is released 2 minutes after its Instance ends; the successor starts when the GPU frees and the run ends when it is ready. **Holds QOS now's GPU for about 2 h 40 min; the owner's go is needed (the development rule on #145).** At most 2 jobs: the running predecessor and the pending replacement, the replacement only for the planned gap (above).
- **Its deployment:** the Helios Provider alone, `leaseLimit` 1 h 46 min, `routing.maxRequestDuration` 5 minutes, `ReleaseWhenIdle` with a 2-minute `idleLimit` (`hbep/0007-lifecycle-controller/README.md:614`).
- **What it does:** a stream in flight at the drain start; the replacement is queued ahead and pends until the predecessor's GPU frees; one restart at the worker rung on Helios (`hbep/0007-lifecycle-controller/README.md:637`, "on each alpha platform").
- **Posts:** `posts.md` (the gap from the drain start to the successor's `ready`, the successor's queue wait once the GPU freed, streams cut, `NoOverlapSlot` seen, 503 `no_ready_target` answers) and the slot use. To #145 and #94.
- **Pass criteria:**
  - `hbep/0007-lifecycle-controller/README.md:636`: "on Helios `plgrid-gpu-gh200` under QOS `now`, one deployment goes through no-overlap mode end to end through the front with no request cut, and the gap is measured (Handover, No-overlap mode)."
  - `hbep/0007-lifecycle-controller/README.md:614`: "The gap from the drain start to the successor's `ready`, and the successor's queue wait once the GPU frees, are measured and recorded."
  - `hbep/0007-lifecycle-controller/README.md:471`: "the gap is the time from the primary's drain start until its successor is `ready`."
  - Checked: the deployment is as above, with the Helios Provider alone (a deployment that also allows Athena fails); a stream in flight at the drain start, none cut; exactly two Allocations; a positive gap, with both ends seen in order; `NoOverlapSlot` shown; from the cluster's times, the successor was submitted before the predecessor ended and started after it (`hbep/0007-lifecycle-controller/README.md:608`: "it never starts before the predecessor's job ends"); both jobs on `plgrid-gpu-gh200` with the lease as their time limit; exactly one restart at the worker rung; no third job and no second job under either tag in the cluster's record.

### `tunnel-hour-helios`, `tunnel-hour-athena`

```
hack/live-checks.sh --run tunnel-hour --yes
```

- **Settings:** as `alpha-athena-handover`, for each cluster, and for `tunnel-hour-helios` the owner's go, `LIVE_OWNER_GO_HELIOS_NOW`.
- **Duration, slot and jobs:** one job per cluster, one GPU, with the 2 h 17 min lease by default (S1's 1 h 25 min limit is under the lead an Instance needs); the run is about 1 h 45 to 2 h 15 once the job starts, the hour running from the Instance's ready. At most 1 job, and no second job of any tag. `tunnel-hour-helios` **holds QOS now's GPU for about 2 h 20 min; the owner's go is needed (the development rule on #145)**; Athena has no slot.
- **Its deployment:** the row's Provider alone, `leaseLimit` 2 h 17 min (or the owner's value), `routing.maxRequestDuration` 5 minutes.
- **What it does:** S1's one-hour run over this wiring (the stack's frps, plugin, agent and front, not the spike's kit), in the dial-home mode set. For 61 minutes the scenario drives the traffic below through the front while a look at the tunnel session is taken every minute, and measures S1's path rate, node to front, with the 16 streams open.
- **Posts:** `posts.md` (the requests and the hour they span, the longest run back to back, the peak of streams at 20 tokens/s, the longest idle stretch, requests cut, the number of distinct frps `lastStartTime` values across the looks, TunnelConnected True in how many, the path rate in bytes and tokens per second under load with the seconds behind it, and S1's FLAG line when the byte rate is under 629,146 B/s) and the slot use. To #145.
- **Pass criteria:**
  - `hbep/0006-instance-reachability/README.md:354`: "S1's one-hour runs pass with this wiring on Helios and on Athena, in the mode each uses, with the path rate measured."
  - `hbep/0006-instance-reachability/README.md:375`: "S1's one-hour runs pass with this wiring on Helios and on Athena, with the path rate from compute node to front measured;"
  - S1's verdict (branch `spike/s1-frp-dial-home`, `spikes/s1-frp/README.md:284`, C2, and `:285`, C3): the path stays reachable and no stream is cut over the hour; (`:288`, C6) the path rate in the token direction is "`ok`, at least 30 s asked, at least 29 s timed at the VM's receiver", and moved bytes.
  - Checked, per the ruling:
    1. **One tunnel session for the hour:** frps's `lastStartTime` for the Instance's proxy is the same in every look, from one at or before the first request to one at or after the last, with no look more than 2 minutes after the previous; `TunnelConnected` True in every look. A run in which frpc logged in again mid-hour but every request passed is no pass: the changed `lastStartTime` fails it (dry fault `reconnect`).
    2. **Traffic that tests an hour:** requests back to back (a run of at least 3, no pause over 10 s); one request of 4 minutes or more, within the deadline; 16 streams open at once at 20 tokens/s each or more; an idle stretch of 2 minutes or more with no request open (heartbeats only).
    3. **No cut:** no response with a `cutBy`, no `stream_cut`, every request complete and within the deployment's 5-minute deadline (a longer one fails: the front would have cut it); the requests span an hour and end before the Instance's drain start, which must be observed.
    4. **The path rate**, in bytes and in tokens a second, node to front, measured under load (S1's C6).
    Also: the deployment is as above; the cluster's own record names exactly one job (by job ID: a held duplicate under the tag, or a second tunnel job, fails) on the row's partition, with the lease as its time limit, and both its start and end time, an hour or more apart: a missing time fails.

## Before the first real run

1. `hack/live-checks.sh --list` and `--plan origin/main..HEAD` name the rows; `--run <row>` (no `--yes`) prints the shape and the estimate, or the settings it lacks.
2. Run `gpu-accounting` first: short, no route needed, and it is the only row fully wired.
3. Set `LIVE_PENDING_MAX_SECONDS` from the estimate before a queued run.
4. Post each result where the row says, with its slot use. The text of `posts.md` is private until you copy it; it holds counts, durations and fixed labels, never a host, account or job ID.

## What the dry runs show, and what they cannot

The package tests and `hack/livechecks` run every row's own compiled test, through the script, over a fake Slurm in run mode with stand-in steps. The healthy run passes; each of 87 faults (a cut stream, no stream in flight, a second restart, a successor that starts beside its predecessor, a lane an hour short, a deployment that also allows Athena, the default lease, a lease at the lead, no `ReleaseWhenIdle`, a node with two GPUs against a record of one, and so on) fails its row with exit 1, while a node whose `nvidia-smi` lists eight GPUs to a job given one still passes, and `hack/mutate-alpha.sh` changes each criterion's condition and requires a test to fail. A dry binary refuses live settings, and the script never passes the dry switch on.

They cannot show that the real controllers, front and tunnel behave: the stand-in steps report what the real scenario will report, and the criteria judge it. That is the part PR #161 supplies.
