# Draft platform profiles (spike S5)

These are draft platform profiles for the three batch platforms. They come from spike S5 (issue #17) and were collected on 2026-10-30.

- They are inputs to HBEP-4's open question 3 (the profile schema), not that schema. The three files share their main groups (`slurm`, `partitions`, `containers`, `storage`, `time`, `login_node_policy`, `reachability`), and each adds groups for what it found.
- Every fact records `value`, `method`, `source` and `date`. `method` is one of:
  - `measured`: a read-only command on the login node;
  - `documented`: the centre's documentation, with the URL as the source;
  - `unknown`: the reason is given.
- When a command was available, the fact was measured even where documentation exists. Where the two disagree, a `note` says so.
- No Slurm job was run. Every fact about compute nodes is therefore unknown, and the S1 test job (issue #18) is the next source for them.
- As HBEP-4 requires, the profiles hold no host names, addresses, user names, accounts, grants or job IDs. Raw command output is kept privately.

## The facts that matter most

| Fact | Helios | Athena | Sano |
|---|---|---|---|
| Slurm version | 24.11.5 | 23.11.7 | 23.11.4 |
| `squeue --json` / `sacct --json` for a user | yes / yes | yes / yes | yes / yes |
| JWT (`scontrol token`), so slurmrestd is usable | no (`AuthAltTypes` unset) | no | no |
| slurmrestd binary on the login node | present, no service found | present, not advertised | absent |
| MinJobAge: how long a finished job stays in `squeue` | 10 s | 10 s | 300 s |
| `JobRequeue` default | 1 (requeue on) | 1 | 1 |
| Time zone of Slurm's printed times | Europe/Warsaw, no zone marker | Europe/Warsaw | UTC |
| GPU partition for general users, max time | `plgrid-gpu-gh200`, 2 days, aarch64, 4 GPUs per node | `plgrid-gpu-a100`, 2 days, x86_64, 8 A100 per node | `gpu`, 7 days (QOS caps it at 3 or 5 days), x86_64, 4 A100 per node |
| Short partition | `plgrid-now`, 12 h, one job per user | `plgrid-now`, 1 h, one job and one GPU per user | `test`, 2 h |
| `sbatch --test-only` without an account | works on CPU `plgrid`; GPU needs an explicit account | needs an explicit account | works (`test` partition); `gpu` needs the site's required flags |
| Container runtime (login node) | Apptainer 1.5.4, setuid; no Pyxis or enroot | none; no Pyxis or enroot | Apptainer 1.5.3; no Pyxis or enroot |
| User namespaces (login node) | off (`max_user_namespaces=0`) | off | on, restricted by AppArmor |
| Long-running user process on a login node | not stated; docs say computations are killed | not allowed (documented) | unknown (no public docs) |

## What this means for the design (for the HBEP author to weigh)

- **Job state must come from accounting.** MinJobAge is 10 s on both Cyfronet systems, so a provider plugin that polls `squeue` loses the final state of a job almost at once. It has to fall back to `sacct`. The evaluation's crossplane-runtime trap (an Observe that reports a purged job as missing, so the reconciler resubmits it) applies here directly.
- **The ssh transport is the only job API.** No platform enables JWT, so a REST job API (slurmrestd) is not available to plain users anywhere. The multiplexed ssh transport is therefore the only one that works today.
- **Requeue is on by default everywhere.** HBEP-7's requeue risk is real, so the plugin should submit with `--no-requeue`.
- **Scheduler time zones differ by platform.** HBEP-4's time-zone field is needed, and plugins must convert to UTC.
- **The container runtime is not uniform.** It must be a per-platform profile fact, and each instance launcher needs a container-free path. Athena has no container runtime on its login node, and on the Helios GH200 (aarch64) nodes the runtime is unknown. frpc and a static engine environment are candidates for that path.
- **Explicit accounts are needed.** GPU partitions on both Cyfronet systems refuse jobs without an account suffix such as `-gpu-a100`. That belongs in provider settings, not the profile.

## Unknown, and why

Each profile ends with an `unknown:` list. All three share these:
- compute-node outbound internet and inbound reachability: needs a job, which is spike S1;
- user namespaces and the container runtime on compute nodes: needs a job;
- the login node's ssh connection limits: `sshd_config` is not readable, or holds no such setting.

Specific to each platform:
- **Helios:** whether slurmrestd runs anywhere; the size of the node-local disk behind `-C localfs`; the GPU model on `plgrid-gpu-rtx`; whether a lightweight poller is allowed on the login node.
- **Athena:** the sizes of `$SCRATCH_LOCAL` and `$MEMFS` on a compute node; GPU driver and CUDA versions; `sbatch --test-only` with a valid account; the login node's per-user limits.
- **Sano:** memory per A100 (40 or 80 GB); the interconnect between the two GPU nodes; the size and speed of node-local `/scratch`; login-node policy (there are no public docs).
