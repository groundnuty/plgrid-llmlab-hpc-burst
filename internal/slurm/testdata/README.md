# Fixtures of internal/slurm

Slurm JSON replies for the parser tests (`parse_test.go`, `parse_real_test.go`). Three directories, two of them synthetic and one real:

- `v0.0.42` (Slurm 24.11, Helios): built from S7's real captures, `spikes/s7/slurm-json-real.md`. A synthetic file here is S7's text with placeholders and derived variants, not a reply as Slurm sent it.
- `v0.0.40` (Slurm 23.11, Athena and Sano): **synthetic.** Written from the shapes of S4's fake, `spikes/s4/fakeslurm/` (`squeue.go` `squeueJob`, `sacct.go` `sacctJob` and `accountingRecords`, `output.go` `meta` and `exitCodeFor`) and `spikes/s4/internal/slurm/parse_test.go`. Not a capture.
- `v0.0.40-athena-real` (Slurm 23.11.7, Athena): **real replies** of `squeue --json` and `sacct --json`, recorded by the conformance run of issue #59 and cut down by allowlist; see its own README. These are the only real v0.0.40 documents in the repository; nothing from Sano is captured.

**Nothing in `v0.0.42` or `v0.0.40` was produced by the parser, by `internal/slurm/fakeslurm`, or by any code of this repository.** Those files come from S7's text and S4's source by a throwaway script that is not committed; the tests' expected values are written out by hand (instants checked with `date -u -d @<epoch>`). Agreement between a fixture and the code that reads it is therefore not circular. `v0.0.40-athena-real` is Athena's own output, cut down by allowlist (see its README), and `parse_real_test.go` reads it.

## Placeholders

S7 redacted the job ID, user, account, group, cluster, nodes and paths. Every `<redacted>` became an obviously fake value:

| S7 field | Value here |
|---|---|
| job ID | `4242` for S7's jobs; `4301`-`4322` for the derived jobs |
| user, `user_name`, `meta.client.user` | `example-user` |
| account | `example-account` |
| group, `group_name`, `meta.client.group` | `example-group` |
| cluster, `meta.slurm.cluster` | `example-cluster` |
| node names (`nodes`, `batch_host`, `scheduled_nodes`, `job_resources`) | `example-node` |
| `allocating_node` | `example-login-node` |
| `command`, `standard_error`, `standard_output`, `current_working_directory` (squeue); `stdout`, `stderr`, `working_directory` (sacct) | `/scratch/example-user/example/...` |
| `standard_input`, `stdin` | `/dev/null` |
| `association_id`, `group_id`, `user_id` | integers `99001`, `99002`, `99003` |
| `mail_user` | `example-user@example.invalid` |
| `mcs_label` | `example-label` |
| `meta.command` | S7's own (`<id>` in the sacct echo became `4242`) |

`TestFixturesHoldNothingPrivate` fails if a fixture holds `redacted`, a path other than `/dev/null` or `/scratch/example-user/...`, or a job ID outside the fake set.

## Values that are not S7's

S7 gives these as types or not at all; the values are invented and no test depends on them:

- squeue `last_backfill` (1793351240) and `last_update` (1793351246).
- squeue `job_resources`: S7 gives only its shape (`select_type`, `nodes.allocation[]`, ...). The entries (`CPU`, `ALLOCATED`, 2 CPUs, 1024 MB) are placeholders.
- sacct: the 32 keys S7 lists without values hold empty or placeholder values (`{}`, `""`, `[]`, `allocation_nodes` 1, `qos` `normal`, `priority` `{set: true, number: 6286}`, `steps` `[]`, as S7 says for `-X`).
- `tres.requested` in the derived sacct GPU jobs (S7 shows only `tres.allocated`), and the TRES IDs of the `gres` entries (1001 `gpu`, 1002 `gpu:a100`). A `gres` entry is `{type: "gres", name: "gpu" or "gpu:a100", id, count}` in the shape of S7's `cpu` entry.
- The `errors` objects of `*-errors.json` (`description`, `error_number`, `error`, `source`). The parser only counts them.
- Partition `example-gpu-partition` (job 4308); S7 names no GPU partition. The CPU partition is S7's `plgrid`.

## v0.0.42

### `squeue-running.json`
S7, "`squeue --json -j <id>` while the job is RUNNING". Top-level keys and `meta` as S7 shows them (`last_backfill`, `last_update`, `meta`, `errors`, `warnings`, `jobs`); one job object with all 128 values S7 lists, in S7's order. Job 4242, name `s1-frp`, RUNNING, limit 10 min, submit 1793351243, start 1793351245, `end_time` 1793351845 (the projected end, start + limit), priority 6286, `tres_req_str` `cpu=2,mem=1G,node=1,billing=2`. Changed from S7: only the placeholders above.

### `squeue-derived.json`
Ten job objects, each a copy of 4242's with the overrides below, listed out of ID order on purpose (4304, 4310, 4301, 4308, 4303, 4309, 4302, 4307, 4305, 4306). `meta.command` is `["squeue","--me","--json"]` (the Provider's listing), otherwise S7's `meta`. All other values stay S7's, except that pending jobs have no allocation (`batch_host`, `nodes`, `job_resources` emptied).

| ID | Why | Overrides |
|---|---|---|
| 4301 | held by the user | `job_state` `["PENDING"]`, `state_reason` `JobHeldUser`, `hold` true, `priority` set 0, submit 1793351300, `start_time` and `end_time` unset |
| 4302 | held by an admin | as 4301 with `JobHeldAdmin` |
| 4303 | pending with a start estimate | `PENDING`, `state_reason` `Resources`, `start_time` 1793355000, `end_time` 1793355600, submit 1793351300 |
| 4304 | requeued, plain PENDING | `PENDING`, `state_reason` `BeginTime`, `restart_cnt` 1, `start_time` and `end_time` set to 0 |
| 4305 | one GPU, untyped | `tres_req_str` and `tres_alloc_str` `cpu=8,mem=64G,node=1,billing=8,gres/gpu=1` |
| 4306 | two GPUs, typed | `...,gres/gpu:a100=2` |
| 4307 | untyped and typed entries of one request | `...,gres/gpu=2,gres/gpu:a100=2` (Slurm lists both) |
| 4308 | Helios GH200 after the end | `job_state` `["FAILED","COMPLETING"]` (S7: seen on GH200), `state_reason` `NonZeroExitCode`, exit code 3, `end_time` 1793351253, partition `example-gpu-partition`, one GPU |
| 4309 | `end_time` later than start + limit | `end_time` 1793353645 (start + 40 min) with `time_limit` 10. A test-only construct: Slurm moves both together. It is the only way to tell "HardEnd is `end_time`" from "HardEnd is start + limit", because S7's job has `end_time` = start + limit |
| 4310 | across the Europe/Warsaw DST change | submit 1792885800 (2026-10-24T23:50:00Z), start 1792888200 (2026-10-25T00:30:00Z), `end_time` 1792891800 (01:30:00Z), `time_limit` 60. In Warsaw: 02:30 CEST to 02:30 CET |

### `squeue-empty.json`, `squeue-errors.json`, `squeue-no-jobs.json`
S7's `meta` and top-level keys. `-empty`: `"jobs": []`, exit-0 shape of a job Slurm still knows is gone (S7, "What `squeue --json -j <id>` returns once the job is not listed"). `-errors`: `jobs` `[]` and one object in `errors`. `-no-jobs`: no `jobs` key. All three carry the matching `data_parser`, so a refusal is not a dialect mismatch.

### `sacct-completed.json`
S7, "`sacct --json -j <id> -X -S now-1days` after the job ended". Top-level keys `jobs`, `meta`, `errors`, `warnings`; one job object with S7's 46 keys in S7's order and all the selected values S7 gives (job 4242, name `s7-hash-ckpt`, COMPLETED, 4 CPUs, limit 60 min, submission 1793349952, start 1793349957, end 1793350184, elapsed 227, `restart_cnt` 0, `tres.allocated` only). The other keys hold the placeholders above.

### `sacct-derived.json`
Nine copies of the sacct object with overrides, out of ID order. `meta.command` `["sacct","--json","-X","-D","--name=hb-example","-S","-E"]`.

| ID | Why | Overrides |
|---|---|---|
| 4301, 4302 | held (user, admin) | `state` `PENDING`, reason `JobHeldUser` / `JobHeldAdmin`, `hold` true, `priority` 0, start and end 0, limit 10 min, no allocation |
| 4303 | requeued, one record | COMPLETED with `restart_cnt` 1, limit 10 min, start 1793351245, end 1793351253 |
| 4304 | one GPU, untyped | RUNNING (`time.end` 0), limit 120 min, `tres.requested` and `tres.allocated` with `gres/gpu` 1 |
| 4305 | two GPUs, typed | the same with `gpu:a100` 2 |
| 4306 | GPU from `allocated` only | no `tres.requested` key (as S7's document), `allocated` has `gpu` 1 |
| 4307 | both entries | `gpu` 2 and `gpu:a100` 2 |
| 4308 | failed | FAILED, reason `NonZeroExitCode`, exit code 3, partition `example-gpu-partition`, one GPU |
| 4310 | across the DST change | COMPLETED, submission 1792885800, start 1792888200, end 1792891800, elapsed 3600 |

### `sacct-requeue.json`
A requeue under `-D`, in the shape of S4's fake (the earlier execution is `["PENDING","REQUEUED"]` and `restart_cnt` counts executions). Job 4320: second execution (COMPLETED, `restart_cnt` 1, submission 1793351300) listed **first**, then the control job 4321 (one COMPLETED record), the lone record 4322 (`["PENDING","REQUEUED"]`), then 4320's first execution (submission 1793351243, end 1793351300). The test also reverses the array.

### `sacct-empty.json`, `sacct-errors.json`, `sacct-no-jobs.json`
As for squeue.

## v0.0.40

Written from S4's fake: `squeueJob` keys (`job_id`, `name`, `comment`, `job_state`, `restart_cnt`, `requeue`, `submit_time`, `start_time`, `end_time`, `user_name`, `partition`, `account`, `state_reason`, `hold`, `priority`, `time_limit`, `exit_code`), `sacctJob` keys (`job_id`, `name`, `user`, `partition`, `account`, `comment`, `state`, `time`, `exit_code`, `flags`), `meta` with `plugin.data_parser` `data_parser/v0.0.40` and `accounting_storage`, S4's `exitCodeFor`, and S4's default priority 1000 and 0 for a hold. The job IDs and the scenario of each ID are the same as in v0.0.42, with the same epochs, so one table of expected values serves both.

Added, because S4's fake does not emit them:

- `tres_req_str` in every squeue job (S7's name and form).
- sacct `tres` (`allocated` and `requested`) in the GPU jobs only.
- squeue jobs 4301-4310 and sacct jobs 4301-4310, 4320-4322 as in v0.0.42.

What is deliberately **absent**, as in S4's fake and HBEP-4:

- sacct has no `restart_cnt`, `hold` or `priority`. A held pending job (4301, 4302) shows only through `state.reason`. The tests add `hold` and `priority` to such a record inline to try each signal.
- A running squeue job has `end_time` unset (S4: `end` is filled only for an end state), so the parser's hard end is start + limit. Job 4309 sets `end_time`. Slurm 23.11.7 on Athena does project `end_time` while a job runs (start + limit, `v0.0.40-athena-real/squeue-running.json`); this synthetic file keeps S4's unset value.
- Job 4308's `["FAILED","COMPLETING"]` is S7's observation on Helios (24.11), used for v0.0.40 too; COMPLETING is a flag in both versions. S4's fake emits one-element state lists.
- `sacct-requeue.json`: 4320's earlier record is `["PENDING","REQUEUED"]`, 4322 is S4's `parse_test.go` shape, one record `["REQUEUED"]`; the later execution of 4320 comes first.

Files: `squeue-running.json`, `squeue-derived.json` (ten jobs in the same out-of-order sequence), `squeue-empty.json`, `squeue-errors.json`, `squeue-no-jobs.json`, `sacct-completed.json` (S4-shaped copy of S7's job 4242, same times), `sacct-derived.json` (eight jobs: no 4303, which needs `restart_cnt`), `sacct-requeue.json`, `sacct-empty.json`, `sacct-errors.json`, `sacct-no-jobs.json`. `squeue-errors.json` and `sacct-errors.json` carry the same invented error object as in v0.0.42.

## Variants made inline

`parse_test.go` derives a few more cases from these files at run time (`editDoc`, `onlyJob`): each base state of the phase table, each hold signal alone, restart signals, wrong-typed fields, a changed or missing `data_parser`, a reply that lists a job and an error. They start from S7's documents (4242) or from 4303 / 4301, and say in the test what they change.
