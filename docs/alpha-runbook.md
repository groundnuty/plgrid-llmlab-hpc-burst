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
| `alpha-helios-nooverlap` | Also needs #174. The script refuses it (exit 3) until `LIVE_ISSUE_174_LANDED=yes` is set. |

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
| `LIVE_ISSUE_174_LANDED` | `alpha-helios-nooverlap` | `yes` once #174 is on main |

One environment variable, not a setting: `LIVE_PENDING_MAX_SECONDS` (default 7200) is how long a job may wait in the queue before the run cancels it. Athena's `plgrid-gpu-a100` queue was estimated at a start on 13 Nov for one GPU (#145, 1 Nov 2026): set it from the estimate that `--run` prints, which is the reason it is printed first. The script's timeout for a row is the row's run time plus this wait.

## The one-job slot, and two jobs at once

Helios QOS `now` and Athena `plgrid-now` allow one job per user, and the owner shares them (#145, 1 Nov 2026): use them sparingly, batch checks, leave nothing held, and post each use. Before a row takes a slot the script reads it, refuses (exit 3) if it already holds a job, reads it again after the builds, and prints `slot use (<slot>): start <UTC> end <UTC> duration <hh:mm:ss>` when the run ends. That line goes in the post.

"One cluster job at a time" holds as one live run per host, plus the slot refusal. It cannot mean one job, because two of the rows are about a successor beside a predecessor:

- a handover runs the predecessor and its replacement together on Athena, whose `plgrid-gpu-a100` has no one-job slot;
- no-overlap mode on Helios holds a pending replacement beside the running job in `now` (the partition's QOS allows 2,000 submitted jobs, the QOS `now` one GPU: `hbep/0007-lifecycle-controller/README.md:594`). The slot holds two jobs for the predecessor's remaining life. Say so in the post.

Cleanup is by tag: each Allocation's job tag is written to `job-tags.txt` before anything can submit it, and the script cancels those jobs, and only those, on every exit, a signal included.

## The runs

Durations are plans, from the lease each row sets (`internal/alpha/rows.go`) and S1's limit; the first run replaces them with a measurement. The queue wait comes on top of every one.

### `gpu-accounting-athena`, `gpu-accounting-helios`

```
hack/live-checks.sh --run gpu-accounting --yes
```

- **Settings:** the account, the credentials file, `LIVE_OUT_DIR`, `LIVE_LOCK_DIR`. No dial-home route.
- **Duration and slot:** one GPU job of 5 minutes' limit per cluster, holding its GPU for about 20 seconds; the run is about 10 minutes once the job starts. Helios uses QOS `now` (slot use of that length); Athena's `plgrid-gpu-a100` has no slot.
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
- **Duration and slot:** two Allocations of a 40-minute lease on `plgrid-gpu-a100`, one GPU each, together for the handover; the run is about 1 h 30 once the first job starts. No slot: the partition has none.
- **What it does:** one deployment through the front and the real tunnel, with streams open across the predecessor's drain start; the successor takes over; one restart at the worker rung.
- **Posts:** `posts.md` (handovers completed, streams in flight at the drain start and cut, worker-rung restarts, responses with attribution) and the run's lines (no slot). To #145 and #93.
- **Pass criteria:**
  - `hbep/0007-lifecycle-controller/README.md:620`: "on Athena `plgrid-gpu-a100`, one deployment completes one handover end to end through the front with no request cut;"
  - `hbep/0007-lifecycle-controller/README.md:622`: "on each alpha platform, Helios `plgrid-gpu-gh200` and Athena `plgrid-gpu-a100`, one restart at the worker rung;"
  - `hbep/0005-llmlab-compatible-front/README.md:450`: "on Athena `plgrid-gpu-a100`, through the front, one handover cuts no request (with HBEP-7)."
  - Checked: at least one stream was in flight at the drain start (`hbep/0007-lifecycle-controller/README.md:592`: "A run with no stream in flight at the drain start proves nothing here."); none was cut or left unfinished; exactly one handover; the successor ready before the predecessor ended; exactly one worker-rung restart; every response attributed; both jobs on `plgrid-gpu-a100` in the cluster's own record.

### `alpha-helios-nooverlap`

```
hack/live-checks.sh --run alpha-helios-nooverlap --yes     # with LIVE_ISSUE_174_LANDED=yes
```

- **Settings:** the account, the credentials file, the four dial-home and binary settings, and `LIVE_ISSUE_174_LANDED`.
- **Duration and slot:** two sequential Allocations under QOS `now` on `plgrid-gpu-gh200`, one GPU, a 40-minute lease each, with the gap between; the run is about 2 h. **Slot use:** the predecessor's job and the queued successor, for the whole run. This is the longest hold of the shared slot; agree it with the owner first.
- **What it does:** one deployment with `ReleaseWhenIdle`, a short `idleLimit` and a stream in flight at the drain start. The replacement is queued ahead and pends until the predecessor's GPU frees.
- **Posts:** `posts.md` (the gap from the drain start to the successor's `ready`, the successor's queue wait once the GPU freed, streams cut, `NoOverlapSlot` seen, 503 `no_ready_target` answers) and the slot use. To #145 and #94.
- **Pass criteria:**
  - `hbep/0007-lifecycle-controller/README.md:621`: "on Helios `plgrid-gpu-gh200` under QOS `now`, one deployment goes through no-overlap mode end to end through the front with no request cut, and the gap is measured (Handover, No-overlap mode)."
  - `hbep/0007-lifecycle-controller/README.md:600`: "The gap from the drain start to the successor's `ready`, and the successor's queue wait once the GPU frees, are measured and recorded."
  - `hbep/0007-lifecycle-controller/README.md:466`: "the gap is the time from the primary's drain start until its successor is `ready`."
  - Checked: a stream in flight at the drain start, none cut; exactly two Allocations; a positive gap, with both ends seen in order; `NoOverlapSlot` shown; from the cluster's times, the successor was submitted before the predecessor ended and started after it (`hbep/0007-lifecycle-controller/README.md:594`: "it never starts before the predecessor's job ends"); both jobs on `plgrid-gpu-gh200`.

### `tunnel-hour-helios`, `tunnel-hour-athena`

```
hack/live-checks.sh --run tunnel-hour --yes
```

- **Settings:** as `alpha-athena-handover`, for each cluster.
- **Duration and slot:** one job of 1 h 25 limit per cluster (S1's limit), one GPU; the run is about 1 h 45 once the job starts. Helios holds the QOS `now` slot for that time; Athena has none. Run the Helios row when the owner has agreed the hold.
- **What it does:** S1's one-hour run over this wiring (the stack's frps, plugin, agent and front, not the spike's kit), in the dial-home mode set, with streams held behind frpc for the hour and S1's path-rate measurement, node to front.
- **Posts:** `posts.md` (lanes held and the shortest, streams cut, tunnel drops, the path rate in bytes per second with the bytes and seconds behind it, and S1's FLAG line when the rate is under 629,146 B/s) and the slot use. To #145.
- **Pass criteria:**
  - `hbep/0006-instance-reachability/README.md:354`: "S1's one-hour runs pass with this wiring on Helios and on Athena, in the mode each uses, with the path rate measured."
  - `hbep/0006-instance-reachability/README.md:375`: "S1's one-hour runs pass with this wiring on Helios and on Athena, with the path rate from compute node to front measured;"
  - S1's verdict (branch `spike/s1-frp-dial-home`, `spikes/s1-frp/README.md:284`, C2): every lane has "a completed stream of at least 3600 s"; (`:285`, C3) "No stream is cut over that hour, on any lane."; (`:288`, C6) the path rate in the token direction is "`ok`, at least 30 s asked, at least 29 s timed at the VM's receiver", and moved bytes.
  - Checked: every lane lasted an hour and completed, none was cut, the tunnel never dropped, the rate was measured in the node-to-front direction and meets C6, the job is on the row's partition, and the cluster's own record shows the job ran an hour or more. A rate under 629,146 B/s is flagged and posted, not failed (S1's FLAG).

## Before the first real run

1. `hack/live-checks.sh --list` and `--plan origin/main..HEAD` name the rows; `--run <row>` (no `--yes`) prints the shape and the estimate, or the settings it lacks.
2. Run `gpu-accounting` first: short, no route needed, and it is the only row fully wired.
3. Set `LIVE_PENDING_MAX_SECONDS` from the estimate before a queued run.
4. Post each result where the row says, with its slot use. The text of `posts.md` is private until you copy it; it holds counts, durations and fixed labels, never a host, account or job ID.

## What the dry runs show, and what they cannot

The package tests and `hack/livechecks` run every row's own compiled test, through the script, over a fake Slurm in run mode with stand-in steps. The healthy run passes; each of 37 faults (a cut stream, no stream in flight, a second restart, a successor that starts beside its predecessor, a lane an hour short, a node with two GPUs against a record of one, and so on) fails its row with exit 1, while a node whose `nvidia-smi` lists eight GPUs to a job given one still passes, and `hack/mutate-alpha.sh` changes each criterion's condition and requires a test to fail. A dry binary refuses live settings, and the script never passes the dry switch on.

They cannot show that the real controllers, front and tunnel behave: the stand-in steps report what the real scenario will report, and the criteria judge it. That is the part PR #161 supplies.
