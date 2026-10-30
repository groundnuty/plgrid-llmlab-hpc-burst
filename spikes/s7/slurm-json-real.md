# Real Slurm JSON from Helios (for the S4 fake Slurm)

Captured on 2026-10-30 from a real Helios CPU job of spike S7, through the login node. The job ID, user, account, group, cluster name, node names and paths are redacted or left out.

## What this file does and does not hold

| Asked for | Status |
|---|---|
| `squeue --json -j <id>` while the job is RUNNING | **Not captured.** Both S7 jobs had finished (3 to 4 minutes each) before this request reached the S7 agent. |
| The moment the job leaves `squeue` and the moment `sacct` first shows a terminal state | **Not measured.** A bound is given below. |
| One `sacct --json -j <id> -X -S now-1days` document | Captured, structure and selected values below. |
| `squeue --json -j <id>` after the job ended | Captured: it gives no JSON, see below. |

Capturing the first two needs a live job. A one-CPU job that sleeps about 40 seconds would do it, while a poller runs `squeue --json` once during the run and then every 2 s for up to 60 s after it ends. That is one more Helios job, which this request ruled out, so it was not submitted.

## Versions and configuration (read with `scontrol show config`, `sacct --version`)

| Item | Value |
|---|---|
| Slurm | 24.11.5 (`squeue` and `sacct` report the same) |
| JSON data parser | `data_parser/v0.0.42` |
| Accounting storage | `accounting_storage/slurmdbd` |
| `MinJobAge` | **10 sec**: the controller forgets a finished job 10 seconds after it ends, so `squeue` stops listing it very quickly |
| Scheduler | `sched/backfill`; `SelectType=select/cons_tres` |
| Job accounting gather | `jobacct_gather/cgroup` |
| Job completion plugin | none (`JobCompType = (null)`) |

## `squeue --json -j <id>` after the job has gone

Queried minutes after the job ended. Exit status 1, nothing on standard output, standard error:

```
slurm_load_jobs error: Invalid job id specified
```

A poller that asks `squeue --json -j <id>` for a finished job therefore gets an error exit, not an empty `jobs` list. Treat that exit as "the controller no longer has the job" and ask `sacct`.

## The squeue-to-sacct gap

Not measured. Evidence that bounds it:

- Config says `squeue` forgets the job at about end + 10 s (`MinJobAge`; the purge runs periodically, so it can be later).
- The job (run 2 of S7) ended at 08:49:44 UTC. The first `sacct` query made after the poller saw the job gone was made no later than 08:50:37 UTC (the modification time of the saved output on the agents VM; the VM and cluster clocks were not compared, so treat the bound as approximate) and already returned `COMPLETED`.
- So `sacct` showed the terminal state at most about 53 s after the job ended, and at most about 43 s after `squeue` could have forgotten it. The true gap may be zero. The same held for the first S7 run, with a coarser bound (the poller's interval was 90 s).

## `sacct --json -j <id> -X -S now-1days` after the job ended

One job, state COMPLETED, 4 CPUs, 6144 MB (1536 MB per CPU, the partition default), time limit 60 minutes, run 3 min 47 s. Redacted as asked: top-level structure, the job object's keys, and the values for the fields S4 named.

Notes on the document:

- Top level keys: `jobs`, `meta`, `errors`, `warnings`. `errors` and `warnings` were empty lists.
- `meta.command` listed the arguments cut after `-S` (the value `now-1days` is missing from the echoed command).
- State is `state.current`, a **list** of strings (`["COMPLETED"]`), with `state.reason`. There is no top-level `job_state` in sacct JSON.
- `exit_code` and `derived_exit_code` are objects: `status` is a list (`["SUCCESS"]`), `return_code` and `signal.id` are `{set, infinite, number}` triples, `signal.name` is a string.
- Times are in `time` as integer epoch seconds (`start`, `end`, `submission`, `eligible`), `elapsed` in seconds, and `limit` and `planned` as `{set, infinite, number}` triples (the limit in minutes, the planned wait in seconds).
- `comment` is an object with three strings (`administrator`, `job`, `system`), not a string.
- `restart_cnt` is present, an integer.
- `-X` leaves out the job steps, but the job object still carries a `steps` key.

```json
{
 "top_level_keys": [
  "jobs",
  "meta",
  "errors",
  "warnings"
 ],
 "meta": {
  "plugin": {
   "type": "",
   "name": "",
   "data_parser": "data_parser/v0.0.42",
   "accounting_storage": "accounting_storage/slurmdbd"
  },
  "client": {
   "source": "",
   "user": "<redacted>",
   "group": "<redacted>"
  },
  "command": [
   "sacct",
   "--json",
   "-j",
   "<id>",
   "-X",
   "-S"
  ],
  "slurm": {
   "version": {
    "major": "24",
    "micro": "5",
    "minor": "11"
   },
   "release": "24.11.5",
   "cluster": "<redacted>"
  }
 },
 "errors": [],
 "warnings": [],
 "jobs_length": 1,
 "job_object_keys": [
  "account",
  "comment",
  "allocation_nodes",
  "array",
  "association",
  "block",
  "cluster",
  "constraints",
  "container",
  "derived_exit_code",
  "time",
  "exit_code",
  "extra",
  "failed_node",
  "flags",
  "group",
  "het",
  "job_id",
  "name",
  "licenses",
  "mcs",
  "nodes",
  "partition",
  "hold",
  "priority",
  "qos",
  "qosreq",
  "required",
  "kill_request_user",
  "restart_cnt",
  "reservation",
  "script",
  "stdin_expanded",
  "stdout_expanded",
  "stderr_expanded",
  "stdout",
  "stderr",
  "stdin",
  "state",
  "steps",
  "submit_line",
  "tres",
  "used_gres",
  "user",
  "wckey",
  "working_directory"
 ],
 "job_object_selected_values": {
  "name": "s7-hash-ckpt",
  "comment": {
   "administrator": "",
   "job": "",
   "system": ""
  },
  "state": {
   "current": [
    "COMPLETED"
   ],
   "reason": "None"
  },
  "exit_code": {
   "status": [
    "SUCCESS"
   ],
   "return_code": {
    "set": true,
    "infinite": false,
    "number": 0
   },
   "signal": {
    "id": {
     "set": false,
     "infinite": false,
     "number": 0
    },
    "name": ""
   }
  },
  "derived_exit_code": {
   "status": [
    "SUCCESS"
   ],
   "return_code": {
    "set": true,
    "infinite": false,
    "number": 0
   },
   "signal": {
    "id": {
     "set": false,
     "infinite": false,
     "number": 0
    },
    "name": ""
   }
  },
  "restart_cnt": 0,
  "flags": [
   "STARTED_ON_BACKFILL",
   "START_RECEIVED"
  ],
  "failed_node": "",
  "kill_request_user": "",
  "time": {
   "elapsed": 227,
   "eligible": 1793349952,
   "end": 1793350184,
   "planned": {
    "set": true,
    "infinite": false,
    "number": 5
   },
   "start": 1793349957,
   "submission": 1793349952,
   "suspended": 0,
   "system": {
    "seconds": 0,
    "microseconds": 0
   },
   "limit": {
    "set": true,
    "infinite": false,
    "number": 60
   },
   "total": {
    "seconds": 0,
    "microseconds": 0
   },
   "user": {
    "seconds": 0,
    "microseconds": 0
   }
  },
  "required": {
   "CPUs": 4,
   "memory_per_cpu": {
    "set": true,
    "infinite": false,
    "number": 1536
   },
   "memory_per_node": {
    "set": false,
    "infinite": false,
    "number": 0
   }
  },
  "partition": "plgrid",
  "hold": false,
  "tres": {
   "allocated": [
    {
     "type": "cpu",
     "name": "",
     "id": 1,
     "count": 4
    },
    {
     "type": "mem",
     "name": "",
     "id": 2,
     "count": 6144
    },
    {
     "type": "energy",
     "name": "",
     "id": 3,
     "count": -2
    },
    {
     "type": "node",
     "name": "",
     "id": 4,
     "count": 1
    },
    {
     "type": "billing",
     "name": "",
     "id": 5,
     "count": 4
    }
   ]
  }
 }
}
```

## Running-job squeue JSON and measured squeue→sacct gap (captured in S1 step 0)

Captured on 2026-10-30 from the S1 step-0 probe jobs (direct mode, probe only, 2 CPUs, 1 GB, 10 min limit, `--no-requeue`), through the login node, by a poller on the agents VM. The job ID, user, account, group, cluster name, node names and paths are redacted. This section answers the two items the first part of this file marks "Not captured" and "Not measured". The Helios document is from a Helios CPU job (Slurm 24.11.5, `data_parser/v0.0.42`). The same poller also ran against one Helios GH200 job (same Slurm) and one Athena A100 job (Slurm 23.11.7); their timings are given as a comparison, their documents were not kept.

### How it was measured

- **Poller.** Every poll was one short ssh call to the login node under the per-host lock, made from the VM; the lock was never held across a sleep. A poll ran `squeue --json -j <id>` and then `sacct --json -j <id> -X -S now-1hours` (the `sacct` half only from the first RUNNING poll on). Polls started exactly every 2.0 s (start to start, measured); a call took up to 1.7 s, so there was never more than one call in flight. Each poll carries the VM's UTC time around the call and the cluster's own `date` read three times inside the call.
- **Job end.** Taken from `sacct`'s `time.end` (integer epoch seconds, cluster clock). The VM and cluster clocks agree to better than 0.1 s (the cluster's `date` read at the end of each call matched the VM's end-of-call stamp to 0.01 s). Times below are the cluster clock relative to the job end; the resolution is the poll period, so every number carries about ±2 s.
- **Left squeue** means the first poll whose `squeue` answer no longer lists the job. **sacct terminal** means the first poll whose `state.current` is a terminal state.
- The job ran about 8 s (`PROBE_ONLY`, exit 3 by design, so Slurm shows FAILED), so RUNNING was seen by several polls. A longer job would not change the shapes below. The job end has whole-second resolution, so the true end can be up to 1 s later than the value used.

### Helios CPU: the last polls around the job end

| VM UTC (whole poll) | `squeue` call, cluster clock, s from job end | `squeue --json -j` said | `sacct` call, s from job end | `sacct --json -j -X` said |
|---|---|---|---|---|
| 09:07:30.020-31.053 | -2.9 to -2.0 | RUNNING | -2.0 to -2.0 | RUNNING |
| 09:07:32.019-33.549 | -0.9 to +0.5 | RUNNING | +0.5 to +0.5 | RUNNING |
| 09:07:34.020-35.242 | +1.1 to +2.2 | not listed | +2.2 to +2.2 | FAILED |

`not listed` is a poll whose `squeue` answer contains no job object (its exact shape is in the section on the not-listed answer below).

**Measured gap, Helios CPU.** In the poll that ended at end+0.5 s, `squeue` and `sacct` both still said RUNNING. In the next poll (its `squeue` call from end+1.1 s to end+2.2 s, its `sacct` call right after) `squeue` no longer listed the job **and** `sacct` already said FAILED. So:

- end to left-squeue: at most 2.2 s (the `squeue` call that did not list the job returned at end+2.2 s);
- end to sacct terminal: at most 2.2 s;
- squeue→sacct gap: **0 s within the 2 s resolution.** There was no poll in which `squeue` had forgotten the job and `sacct` could not yet answer a terminal state. A poller that asks `sacct` as soon as `squeue` stops listing the job got the terminal state on its first try, on all three platforms below.
- `squeue` never showed a terminal or COMPLETING state for this job: it went from RUNNING to not listed within one 2 s poll. The ID was still known to the controller at that point (the answer was an empty list, not an error; see below).

### Comparison: the same poller on the other two platforms

| Platform (Slurm) | last `squeue` call that said RUNNING returned at (s from end) | first `squeue` terminal state, call returned at | first `sacct` terminal, call returned at | first `squeue` call not listing the job returned at |
|---|---|---|---|---|
| Helios CPU (24.11.5) | +0.5 s | none seen (went straight to not listed) | FAILED, by +2.2 s | not listed, by +2.2 s |
| Helios GH200 (24.11.5) | +0.7 s | FAILED, by +2.7 s | FAILED, by +2.8 s | not listed, by +3.4 s |
| Athena A100 (23.11.7) | -1.4 s | FAILED, by +0.6 s | FAILED, by +3.0 s | not listed, by +12.5 s |

- **Helios** (24.11.5) stopped listing the job by 2.2 s after the end on CPU and by 3.4 s on GH200, but it still answered with an empty list and exit 0, not an error: the controller still knew the ID. On GH200 `squeue` showed `["FAILED","COMPLETING"]` for one poll, and `sacct` already said FAILED in that same poll.
- **Athena** (23.11.7) kept listing the finished job for about 12 s, in state FAILED with `state_reason` `NonZeroExitCode`, and `sacct` caught up one poll (2 s) after `squeue` first said FAILED (that poll: `squeue` FAILED, `sacct` still RUNNING). So on Athena `sacct` trailed `squeue`'s first terminal state by one poll, and `squeue` stopped listing the job about 10 s after `sacct` said terminal, which fits `MinJobAge` = 10 s (read with `scontrol show config` on both clusters). The two sources never disagreed for longer than one poll.
- At the very start, `sacct` can trail `squeue`: on Athena the first poll after RUNNING had `squeue` RUNNING and `sacct` PENDING.
- **Inference, not tested:** `squeue`'s documented default state filter is pending, running and completing (`man squeue`, both clusters). Helios 24.11.5 stopped listing the FAILED job at once, as that filter would, and answered with the empty list, while Athena 23.11.7 listed the same kind of job in state FAILED under the same command. A Helios `squeue` with `--states=all` was not tried, because that needs another job. So treat "not listed" as "not in the default states" rather than "forgotten" on Helios.
- **Not measured:** when the Helios answer turns from the empty list into the error. It had turned by 0.4 min after the GH200 job end (the polls stopped at the first empty list). `MinJobAge` is 10 s on both clusters (same `scontrol show config` read), which fits the turn at about the end+10 s, but this was not observed. The S7 note of `MinJobAge` 10 s on Helios is consistent with these polls.

### What `squeue --json -j <id>` returns once the job is not listed

| Platform | Shortly after the job end | Minutes later (0.4 after the GH200 job end, 1.6 after the CPU job end) |
|---|---|---|
| Helios (24.11.5) | **Phases seen:** listed (RUNNING; on GH200 also `["FAILED","COMPLETING"]` for one poll), then exit 0 with JSON `"jobs": []` (top-level keys `jobs`, `last_backfill`, `last_update`, `meta`, `errors`, `warnings`; `errors` and `warnings` empty), the ID still known. The empty list was the last answer the poller saw; it stopped there. | exit 1, no JSON, stderr `slurm_load_jobs error: Invalid job id specified` (same as the first part of this file): the ID is purged |
| Athena (23.11.7) | exit 1, no JSON, stderr `slurm_load_jobs error: Invalid job id specified` (first not-listed poll) | not queried |

A poller must therefore treat BOTH an empty `jobs` list (exit 0) and the error exit (exit 1) as "`squeue` no longer lists the job", then ask `sacct`. A fake Slurm should model all three Helios phases (listed, empty list with exit 0, then the error), and on Athena the listed-as-FAILED phase before the error. A fake that returns only the error never exercises the empty-list path that a real Helios returned right after the end.

### `squeue --json -j <id>` while the job is RUNNING (Helios CPU, one document)

The raw document had 6 top-level keys and one job object with 128 keys. Top level, with only the shape of each value:

```json
{
 "last_backfill": {
  "set": "bool",
  "infinite": "bool",
  "number": "int"
 },
 "last_update": {
  "set": "bool",
  "infinite": "bool",
  "number": "int"
 },
 "meta": "see meta block",
 "errors": [],
 "warnings": [],
 "jobs": "list of 1 job object(s)"
}
```

The `meta` block (the same layout as in the `sacct` document above; `command` echoes the `squeue` arguments only up to `-j`: the job ID is cut, as `-S`'s value is in the `sacct` echo):

```json
{
 "plugin": {
  "type": "",
  "name": "",
  "data_parser": "data_parser/v0.0.42",
  "accounting_storage": "accounting_storage/slurmdbd"
 },
 "client": {
  "source": "",
  "user": "<redacted>",
  "group": "<redacted>"
 },
 "command": [
  "squeue",
  "--json",
  "-j"
 ],
 "slurm": {
  "version": {
   "major": "24",
   "micro": "5",
   "minor": "11"
  },
  "release": "24.11.5",
  "cluster": "<redacted>"
 }
}
```

Notes on this document, for the S4 fake Slurm:

- State is `job_state`, a **list** of strings (`["RUNNING"]`), with `state_reason` (`"None"` while running) and `state_description`. The `sacct` form is `state.current`; there is no `job_state` there.
- `squeue` has `start_time`, `submit_time`, `eligible_time`, `accrue_time` and `end_time` as `{set, infinite, number}` triples (epoch seconds). **While RUNNING, `end_time` is the projected end (start plus the time limit), not an actual end;** the actual end appears only in `sacct`.
- `time_limit` is a triple in minutes (here 10). `cpus`, `cpus_per_task`, `node_count`, `tasks`, `memory_per_node` (MB, here 1024, because `--mem=1G` was used) and `memory_per_cpu` (`set: false` in that case) are triples.
- `exit_code` and `derived_exit_code` exist while running, with `status` `["SUCCESS"]` and return code 0. `restart_cnt` is 0; `requeue` is false (`--no-requeue`).
- `tres_req_str` and `tres_alloc_str` are strings (`"cpu=2,mem=1G,node=1,billing=2"`); there is no per-GPU triple here (a CPU job).
- The job object is flat and large (128 keys), unlike the `sacct` object (46 keys) with its nested `time` and `required` objects. The key names differ too: `squeue` has `cpus`, `node_count`, `current_working_directory`, `standard_output`; `sacct` has `required.CPUs`, `working_directory`, `stdout`.
- `job_resources` holds the node allocation (a per-node list with cpu counts, memory, and core status); only its shape is kept below because it carries the node name.

The job object's values. Fields that identify a person, account, node, path or job are `<redacted>`; everything else is the value as returned:

```
"account": "<redacted>"
"accrue_time": {"set": true, "infinite": false, "number": 1793351243}
"admin_comment": ""
"allocating_node": "<redacted>"
"array_job_id": {"set": true, "infinite": false, "number": 0}
"array_task_id": {"set": false, "infinite": false, "number": 0}
"array_max_tasks": {"set": true, "infinite": false, "number": 0}
"array_task_string": ""
"association_id": "<redacted>"
"batch_features": ""
"batch_flag": true
"batch_host": "<redacted>"
"flags": ["EXACT_TASK_COUNT_REQUESTED", "EXACT_CPU_COUNT_REQUESTED", "ACCRUE_COUNT_CLEARED", "JOB_WAS_RUNNING", "EXACT_MEMORY_REQUESTED", "USING_DEFAULT_QOS", "USING_DEFAULT_WCKEY", "BACKFILL_ATTEMPTED"]
"burst_buffer": ""
"burst_buffer_state": ""
"cluster": "<redacted>"
"cluster_features": ""
"command": "<redacted>"
"comment": ""
"container": ""
"container_id": ""
"contiguous": false
"core_spec": 0
"thread_spec": 32766
"cores_per_socket": {"set": false, "infinite": false, "number": 0}
"billable_tres": {"set": true, "infinite": false, "number": 2.0}
"cpus_per_task": {"set": true, "infinite": false, "number": 2}
"cpu_frequency_minimum": {"set": false, "infinite": false, "number": 0}
"cpu_frequency_maximum": {"set": false, "infinite": false, "number": 0}
"cpu_frequency_governor": {"set": false, "infinite": false, "number": 0}
"cpus_per_tres": ""
"cron": ""
"deadline": {"set": true, "infinite": false, "number": 0}
"delay_boot": {"set": true, "infinite": false, "number": 0}
"dependency": ""
"derived_exit_code": {"status": ["SUCCESS"], "return_code": {"set": true, "infinite": false, "number": 0}, "signal": {"id": {"set": false, "infinite": false, "number": 0}, "name": ""}}
"eligible_time": {"set": true, "infinite": false, "number": 1793351243}
"end_time": {"set": true, "infinite": false, "number": 1793351845}
"excluded_nodes": ""
"exit_code": {"status": ["SUCCESS"], "return_code": {"set": true, "infinite": false, "number": 0}, "signal": {"id": {"set": false, "infinite": false, "number": 0}, "name": ""}}
"extra": ""
"failed_node": ""
"features": ""
"federation_origin": ""
"federation_siblings_active": ""
"federation_siblings_viable": ""
"gres_detail": []
"group_id": "<redacted>"
"group_name": "<redacted>"
"het_job_id": {"set": true, "infinite": false, "number": 0}
"het_job_id_set": ""
"het_job_offset": {"set": true, "infinite": false, "number": 0}
"job_id": "<redacted>"
"job_resources": "<structure only, see below>"
"job_size_str": []
"job_state": ["RUNNING"]
"last_sched_evaluation": {"set": true, "infinite": false, "number": 1793351245}
"licenses": ""
"mail_type": []
"mail_user": "<redacted>"
"max_cpus": {"set": true, "infinite": false, "number": 0}
"max_nodes": {"set": true, "infinite": false, "number": 0}
"mcs_label": "<redacted>"
"memory_per_tres": ""
"name": "s1-frp"
"network": ""
"nodes": "<redacted>"
"nice": 0
"tasks_per_core": {"set": false, "infinite": true, "number": 0}
"tasks_per_tres": {"set": true, "infinite": false, "number": 0}
"tasks_per_node": {"set": true, "infinite": false, "number": 0}
"tasks_per_socket": {"set": false, "infinite": true, "number": 0}
"tasks_per_board": {"set": true, "infinite": false, "number": 0}
"cpus": {"set": true, "infinite": false, "number": 2}
"node_count": {"set": true, "infinite": false, "number": 1}
"tasks": {"set": true, "infinite": false, "number": 1}
"partition": "plgrid"
"prefer": ""
"memory_per_cpu": {"set": false, "infinite": false, "number": 0}
"memory_per_node": {"set": true, "infinite": false, "number": 1024}
"minimum_cpus_per_node": {"set": true, "infinite": false, "number": 2}
"minimum_tmp_disk_per_node": {"set": true, "infinite": false, "number": 0}
"power": {"flags": []}
"preempt_time": {"set": true, "infinite": false, "number": 0}
"preemptable_time": {"set": true, "infinite": false, "number": 0}
"pre_sus_time": {"set": true, "infinite": false, "number": 0}
"hold": false
"priority": {"set": true, "infinite": false, "number": 6286}
"priority_by_partition": []
"profile": ["NOT_SET"]
"qos": "normal"
"reboot": false
"required_nodes": ""
"required_switches": 0
"requeue": false
"resize_time": {"set": true, "infinite": false, "number": 0}
"restart_cnt": 0
"resv_name": ""
"scheduled_nodes": "<redacted>"
"selinux_context": ""
"shared": []
"sockets_per_board": 0
"sockets_per_node": {"set": false, "infinite": false, "number": 0}
"start_time": {"set": true, "infinite": false, "number": 1793351245}
"state_description": ""
"state_reason": "None"
"standard_error": "<redacted>"
"standard_input": "<redacted>"
"standard_output": "<redacted>"
"submit_time": {"set": true, "infinite": false, "number": 1793351243}
"suspend_time": {"set": true, "infinite": false, "number": 0}
"system_comment": ""
"time_limit": {"set": true, "infinite": false, "number": 10}
"time_minimum": {"set": true, "infinite": false, "number": 0}
"threads_per_core": {"set": false, "infinite": false, "number": 0}
"tres_bind": ""
"tres_freq": ""
"tres_per_job": ""
"tres_per_node": ""
"tres_per_socket": ""
"tres_per_task": "cpu=2"
"tres_req_str": "cpu=2,mem=1G,node=1,billing=2"
"tres_alloc_str": "cpu=2,mem=1G,node=1,billing=2"
"user_id": "<redacted>"
"user_name": "<redacted>"
"maximum_switch_wait_time": 0
"wckey": ""
"current_working_directory": "<redacted>"
```

The shape of `job_resources` (strings stand for values, the node name among them):

```json
{
 "select_type": [
  "str"
 ],
 "nodes": {
  "count": "int",
  "select_type": [
   "str"
  ],
  "list": "str",
  "whole": "bool",
  "allocation": [
   {
    "index": "int",
    "name": "str",
    "cpus": {
     "count": "int",
     "used": "int"
    },
    "memory": {
     "used": "int",
     "allocated": "int"
    },
    "sockets": [
     {
      "index": "int",
      "cores": [
       {
        "index": "int",
        "status": [
         "str"
        ]
       }
      ]
     }
    ]
   }
  ]
 },
 "cpus": "int",
 "threads_per_core": {
  "set": "bool",
  "infinite": "bool",
  "number": "int"
 }
}
```

### Evidence and limits

- Three jobs, one platform each, about 8 s long. The comparison rows are one observation each, not a distribution.
- The poll period (2.0 s) bounds every timing. A gap shorter than a poll would not show.
- The job end is `sacct`'s `time.end`, which has whole-second resolution.
- The poller, its raw per-poll outputs and the job IDs stay in the agents VM's private scratch; this section carries none of them.
