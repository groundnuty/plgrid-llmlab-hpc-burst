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
