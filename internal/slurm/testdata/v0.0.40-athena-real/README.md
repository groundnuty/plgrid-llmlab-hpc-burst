# Real data_parser v0.0.40 replies (Athena, Slurm 23.11.7)

Captured on 2026-10-31 by the conformance run of issue #59 (`internal/providerctl/conformance`) on Athena's `plgrid-gpu-a100` partition, one job of 1 GPU, 4 CPUs, 32 GiB and a 5 min limit, submitted with `--hold`. HBEP-4 notes that Athena's real v0.0.40 documents had not been kept; these are.

They are cut down by allowlist, not by redaction: each file keeps only the conformance run's own job objects and, of each, only the keys the plugin reads (`parse.go`), plus `meta.plugin.data_parser` and `meta.slurm.release`. Everything else was dropped, so no user, account, group, node, path, cluster or command is left. The job IDs became 4501 and 4502 and the names `hb-example-<which>-allocation-000000000000`. Every other value, the epoch times included, is as Slurm returned it.

| File | Reply | What it shows |
|---|---|---|
| `squeue-held.json` | `squeue --me --json` right after `sbatch --hold` | PENDING, `hold` false, priority 0, reason `JobHeldUser`: the hold shows in the priority and the reason, not in `hold` |
| `squeue-running.json` | the same job, running | `end_time` minus `start_time` is 300 s, the 5 min limit: the projected end |
| `squeue-completed-listed.json` | the first job just after it ended, and the second still pending | Athena lists an ended job in its end state (S7) |
| `sacct-completed.json` | `sacct --json -X -D -j <id>` of the first job | `state.current` is a list, times are integers, `time.limit` a triple, no `restart_cnt` |
| `sacct-completed-after-hold.json` | the same for the second job | accounting keeps the reason `JobHeldUser` after the job completed |
