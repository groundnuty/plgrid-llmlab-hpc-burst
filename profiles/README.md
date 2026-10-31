# Draft platform profiles (spike S5)

These are draft platform profiles for the three batch platforms. They come from spike S5 (issue #17) and were collected on 2026-10-30.

- They are inputs to HBEP-4's open question 3 (the profile schema), not that schema. The three files share their main groups (`slurm`, `partitions`, `containers`, `storage`, `time`, `login_node_policy`, `reachability`), and each adds groups for what it found.
- Every fact records `value`, `method`, `source` and `date`. `method` is one of:
  - `measured`: a read-only command on the login node;
  - `documented`: the centre's documentation, with the URL as the source;
  - `unknown`: the reason is given.
- When a command was available, the fact was measured even where documentation exists. Where the two disagree, a `note` says so.
- The partition entries group their provenance (`method`, `source`, `date`) over the whole entry instead of per field. The final envelope is HBEP-4's open question 3.
- No Slurm job was run. Runtime behaviour that needs a job is therefore unknown: connectivity, namespaces, usable container runtimes, local disk and drivers. Scheduler-reported node shape (architecture, CPUs, memory, GRES) and documented hardware are in the profiles. The S1 test job (issue #18) is the next source for the unknowns.
- As HBEP-4 requires, the profiles hold no host names, addresses, user names, accounts, grants or job IDs. Raw command output is kept privately.

## The facts that matter most

| Fact | Helios | Athena | Sano |
|---|---|---|---|
| Slurm version | 24.11.5 | 23.11.7 | 23.11.4 |
| `squeue --json` / `sacct --json` for a user | yes / yes | yes / yes | yes / yes |
| JWT (`scontrol token`) for a plain user | no (`AuthAltTypes` unset) | no (`AuthAltTypes` unset) | no (`AuthAltTypes` unset) |
| slurmrestd on the login node | binary present; service not advertised, availability unknown | binary present; service not advertised, availability unknown | binary absent (an endpoint on another host is not excluded) |
| MinJobAge (minimum age before a completed job's record is purged) | 10 s | 10 s | 300 s |
| `JobRequeue` default | 1 (requeue on) | 1 | 1 |
| Time zone of Slurm's text output in a login session | Europe/Warsaw, no zone marker | Europe/Warsaw | UTC |
| GPU partition for general users, max time | `plgrid-gpu-gh200`, 2 days, aarch64, 4 GPUs per node | `plgrid-gpu-a100`, 2 days, x86_64, 8 A100 per node | `gpu`, 7 days (QOS caps it at 3 or 5 days), x86_64, two nodes: one with 8 A100, one with 4 A100 |
| Short partition | `plgrid-now`, 12 h, one job per user | `plgrid-now`, 1 h, one job and one GPU per user | `test`, 2 h |
| `sbatch --test-only` without `--account` | with the probing user's default account: works on CPU `plgrid`; refused on `plgrid-gpu-gh200` (Invalid account or account/partition combination) | with the probing user's default account: refused on the default partition `gpu-a100` (Invalid account or account/partition combination); `plgrid-gpu-a100` not tested | with the probing user's default account: works (`test` partition); `gpu` needs the site's required flags |
| Container runtime (login node) | Apptainer 1.5.4, setuid; no Pyxis or enroot | none of apptainer, singularity, enroot, podman or docker found; no Pyxis | Apptainer 1.5.3; no Pyxis or enroot |
| User namespaces (login node) | off (`max_user_namespaces=0`) | off (`max_user_namespaces=0`) | on: `unshare -U` works, `unshare -Ur` fails (likely the AppArmor restriction) |
| Lightweight long-running process (a poller) on a login node | unknown: docs prohibit computations but do not address a poller | unknown: docs prohibit computations but do not address a poller | unknown (no public docs) |

## What this means for the design (for the HBEP author to weigh)

- **Job state must come from accounting.** MinJobAge is 10 s on both Cyfronet systems, so a completed job can disappear from `squeue` within about 10 s, and a provider plugin that polls `squeue` can lose the final state of a job. It has to fall back to `sacct`. The evaluation's crossplane-runtime trap (an Observe that reports a purged job as missing, so the reconciler resubmits it) applies here directly.
- **SSH and CLI access works; REST is unverified.** `squeue --json` and `sacct --json` work for a plain user on all three platforms. Whether a REST job API (slurmrestd) is reachable for plain users is unverified. JWT is not enabled, but slurmrestd can also run with local (unix-socket) auth. Until that is checked, the multiplexed ssh transport is the one path known to work.
- **Requeue is on by default everywhere.** HBEP-7's requeue risk is real, so the plugin should submit with `--no-requeue`.
- **Text times depend on the login session's time zone.** The Cyfronet login sessions print Europe/Warsaw local time and Sano's prints UTC. These are the defaults of login sessions, not proven properties of the schedulers. Epoch fields are unaffected, so plugins should prefer epoch or JSON timestamps over parsing text. HBEP-4's time-zone field is still needed for any text a plugin must parse, and the plugin must convert to UTC.
- **The container runtime may not be uniform.** It should be a per-platform profile fact. Apptainer is on the Helios and Sano login nodes. On Athena's login node none of apptainer, singularity, enroot, podman or docker was found (charliecloud, udocker, sarus and shifter were not searched for), and nothing was probed on any compute node, including the Helios GH200 (aarch64) nodes. Whether the instance launchers need a container-free path is a candidate pending S1 (issue #18), not a conclusion. frpc and a static engine environment are candidates for such a path.
- **Pass an explicit account and partition.** The account probes used the probing user's default account (and, on Athena, the default partition), so they do not show what every user needs. The Cyfronet documentation says account names carry a resource suffix such as `-gpu-a100`. The provider plugin should always pass an explicit account and partition. That belongs in provider settings, not the profile.

## The typed image format (after spike S1)

The drafts' container facts come from the login nodes. Spike S1's probe 2 (issue #18) then ran Apptainer on compute nodes, and the PlatformProfile types the result per partition as `imageFormat` (`sif` or `sandbox`), the format the allocation agent builds and runs the engine image in:

| Partition | `imageFormat` | Why |
|---|---|---|
| Helios `plgrid-gpu-gh200` | `sif` | a SIF image runs |
| Athena `plgrid-gpu-a100` | `sandbox` | Apptainer cannot run a SIF image there; a sandbox directory image runs |

It is not a deployment-ID input. A partition that states no format gets `sif` (`v1alpha1.ImageFormatOf`).

## Unknown, and why

Each profile ends with an `unknown:` list. All three share these:
- compute-node outbound internet and inbound reachability: needs a job, which is spike S1;
- user namespaces and the container runtime on compute nodes: needs a job;
- the login node's effective ssh connection limits: Helios's `sshd_config` is not readable by a plain user, Sano's readable files set none and the effective values were not read, and Athena's was not probed.

Specific to each platform:
- **Helios:** whether a slurmrestd endpoint exists and is reachable for a plain user; the size of the node-local disk behind `-C localfs`; the GPU model on `plgrid-gpu-rtx`; whether a lightweight poller is allowed on the login node.
- **Athena:** whether a slurmrestd endpoint exists and is reachable for a plain user; whether Slurm's native OCI support or any other container runtime is usable on compute nodes; the sizes of `$SCRATCH_LOCAL` and `$MEMFS` on a compute node; GPU driver and CUDA versions; `sbatch --test-only` with a valid account on `plgrid-gpu-a100`; whether a lightweight poller is allowed on the login node; the login node's per-user limits.
- **Sano:** memory per A100 (40 or 80 GB); the interconnect between the two GPU nodes; the size and speed of node-local `/scratch`; login-node policy (there are no public docs).
