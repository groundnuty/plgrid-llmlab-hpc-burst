# Draft platform profiles (spike S5)

These are draft platform profiles for the three batch platforms. They come from spike S5 (issue #17) and were collected on 2026-10-30. Spike S1 (issue #18) added the first compute-node facts to the Helios and Athena files the same day, and a second probe (probe 2) added the compute-node Apptainer, user-namespace and home-readability facts.

- They are inputs to HBEP-4's open question 3 (the profile schema), not that schema. The three files share their main groups (`slurm`, `partitions`, `containers`, `storage`, `time`, `login_node_policy`, `reachability`), and each adds groups for what it found.
- Every fact records `value`, `method`, `source` and `date`. `method` is one of:
  - `measured`: a read-only command on the login node (S5), a command inside a short probe job on a compute node (S1's step 0, for the entries under `reachability` and `compute_node`), or a submit, release and cancel of a held job from the login node (S1's hold check, for `slurm.hold_release`), or short read-only jobs on a compute node (S1's probe 2, for `containers.apptainer_compute_nodes`, `containers.user_namespaces_compute_nodes` and `storage.home_readable_from_compute_nodes`);
  - `documented`: the centre's documentation, with the URL as the source;
  - `unknown`: the reason is given.
- When a command was available, the fact was measured even where documentation exists. Where the two disagree, a `note` says so.
- The partition entries group their provenance (`method`, `source`, `date`) over the whole entry instead of per field. The final envelope is HBEP-4's open question 3.
- S5 ran no Slurm job. The compute-node facts that S1's step 0 measured (below) came from one short probe job per partition: Helios `plgrid` and `plgrid-gpu-gh200`, Athena `plgrid-gpu-a100`. Sano has no S1 data. S1's probe 2 then ran one job on Helios `plgrid-gpu-gh200` (QOS `now`) and three on Athena `plgrid-gpu-a100` (two nodes), read-only, and measured Apptainer, user namespaces and home readability there. What still needs a job is unknown: inbound reachability, container runtimes other than Apptainer, local disk sizes and GPU drivers, and all of probe 2's facts on the partitions it did not run on. Scheduler-reported node shape (architecture, CPUs, memory, GRES) and documented hardware are in the profiles. S1's one-hour runs are the next source. Separately, S1's hold check ran one job per tested partition (Helios `plgrid` and `plgrid-gpu-gh200`, Athena `plgrid-gpu-a100`) that was submitted held, released and cancelled at once; none of them ran.
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
| `sbatch --hold` honoured for a plain user, and released by that user with `scontrol release` (S1 hold check) | yes on `plgrid` and `plgrid-gpu-gh200`: reason `JobHeldUser`, release exit 0, then `PENDING` with reason `Priority` | yes on `plgrid-gpu-a100`: same result | not measured |
| Time zone of Slurm's text output in a login session | Europe/Warsaw, no zone marker | Europe/Warsaw | UTC |
| GPU partition for general users, max time | `plgrid-gpu-gh200`, 2 days, aarch64, 4 GPUs per node | `plgrid-gpu-a100`, 2 days, x86_64, 8 A100 per node | `gpu`, 7 days (QOS caps it at 3 or 5 days), x86_64, two nodes: one with 8 A100, one with 4 A100 |
| Short partition | `plgrid-now`, 12 h, one job per user | `plgrid-now`, 1 h, one job and one GPU per user | `test`, 2 h |
| `sbatch --test-only` without `--account` | with the probing user's default account: works on CPU `plgrid`; refused on `plgrid-gpu-gh200` (Invalid account or account/partition combination) | with the probing user's default account: refused on the default partition `gpu-a100` (Invalid account or account/partition combination); `plgrid-gpu-a100` not tested | with the probing user's default account: works (`test` partition); `gpu` needs the site's required flags |
| Container runtime (login node) | Apptainer 1.5.4, setuid; no Pyxis or enroot | none of apptainer, singularity, enroot, podman or docker found; no Pyxis | Apptainer 1.5.3; no Pyxis or enroot |
| User namespaces (login node) | off (`max_user_namespaces=0`) | off (`max_user_namespaces=0`) | on: `unshare -U` works, `unshare -Ur` fails (likely the AppArmor restriction) |
| Lightweight long-running process (a poller) on a login node | unknown: docs prohibit computations but do not address a poller | unknown: docs prohibit computations but do not address a poller | unknown (no public docs) |
| Compute node: outbound HTTPS on 443 to github.com and pypi.org (S1 step 0) | reached on `plgrid` and on `plgrid-gpu-gh200` (HTTP 200 and 206), no proxy variable set | reached on `plgrid-gpu-a100` (HTTP 200 and 206), no proxy variable set | not measured by S1 |
| Compute node: host `python3` and architecture (S1 step 0) | 3.9.25 on both; x86_64 on `plgrid`, aarch64 on `plgrid-gpu-gh200` | 3.9.25; x86_64 on `plgrid-gpu-a100` | not measured by S1 |
| Compute node to the agents VM: its ssh port, and port 7000 (S1 step 0) | ssh port reached (banner read); port 7000 timed out (filtered or dropped on the path), on both partitions | same as Helios, on `plgrid-gpu-a100` | not measured by S1 |
| Compute node: Apptainer (S1 probe 2) | `plgrid-gpu-gh200`: 1.5.4, setuid, `/usr/bin/apptainer`; `exec docker://busybox:1.36` works (aarch64 image, runs in the container) | `plgrid-gpu-a100`: 1.4.5, setuid, `/usr/bin/apptainer`; a SIF image does not run on two nodes (the kernel squashfs mount is refused in setuid mode and the `squashfuse_ll` fallback fails), a sandbox directory image runs | not measured by S1 |
| Compute node: user namespaces (S1 probe 2) | off (`max_user_namespaces=0`; `unshare -U` and `apptainer exec --userns` fail) | off (`unshare -U` and `apptainer exec --userns` fail; the sysctl itself could not be read) | not measured by S1 |
| Compute node: home directory readable by the job (S1 probe 2) | yes: `test -r ~ && ls ~` exit 0, NFS, mode 700, same home as the login node; `~/.ssh` exists and is readable | yes, same result | not measured by S1 |

The three compute-node rows come from a probe in direct mode: a TCP connect and a banner, and `curl` to two public endpoints, with no ssh forward set up. They do not show that an ssh forward works from a compute node, and the port 7000 timeout is not a finding about the cluster's egress policy, because the agents VM itself admits only its ssh port from the Cyfronet side.

## What this means for the design (for the HBEP author to weigh)

- **Job state must come from accounting.** MinJobAge is 10 s on both Cyfronet systems, so a completed job can disappear from `squeue` within about 10 s, and a provider plugin that polls `squeue` can lose the final state of a job. It has to fall back to `sacct`. The evaluation's crossplane-runtime trap (an Observe that reports a purged job as missing, so the reconciler resubmits it) applies here directly.
- **SSH and CLI access works; REST is unverified.** `squeue --json` and `sacct --json` work for a plain user on all three platforms. Whether a REST job API (slurmrestd) is reachable for plain users is unverified. JWT is not enabled, but slurmrestd can also run with local (unix-socket) auth. Until that is checked, the multiplexed ssh transport is the one path known to work.
- **Requeue is on by default everywhere.** HBEP-7's requeue risk is real, so the plugin should submit with `--no-requeue`.
- **A held submission works on both Cyfronet systems.** HBEP-7 and S4's hold-mode safety (the Slurm provider submits with `sbatch --hold` and later runs `scontrol release`) depend on two facts: the site does not drop the hold, and a plain user can release their own user hold. Both held on every Helios and Athena partition tested (`slurm.hold_release`), with one trial each. Held-job expiry, other job shapes and Sano are not measured.
- **Text times depend on the login session's time zone.** The Cyfronet login sessions print Europe/Warsaw local time and Sano's prints UTC. These are the defaults of login sessions, not proven properties of the schedulers. Epoch fields are unaffected, so plugins should prefer epoch or JSON timestamps over parsing text. HBEP-4's time-zone field is still needed for any text a plugin must parse, and the plugin must convert to UTC.
- **The container runtime may not be uniform.** It should be a per-platform profile fact. Apptainer is on the Helios and Sano login nodes. On Athena's login node none of apptainer, singularity, enroot, podman or docker was found (charliecloud, udocker, sarus and shifter were not searched for), but Apptainer 1.4.5 (setuid install) is on the Athena `plgrid-gpu-a100` compute nodes, and Apptainer 1.5.4 (setuid install) is on the Helios `plgrid-gpu-gh200` (aarch64) compute nodes (S1 probe 2, one partition each). User namespaces are off on both, so containers depend on the setuid install. On Helios GH200 `apptainer exec docker://...` works. On Athena it does not for a SIF image, on either of two nodes; a sandbox directory image does run, so a launcher that uses Apptainer on Athena has to unpack to a directory (`apptainer build --sandbox`) or find the cause of the `squashfuse_ll` failure. The Helios CPU, RTX and bigmem partitions and Sano's compute nodes were not probed. Whether the instance launchers need a container-free path is still open: Athena is the case for one. frpc and a static engine environment are candidates for such a path.
- **Pass an explicit account and partition.** The account probes used the probing user's default account (and, on Athena, the default partition), so they do not show what every user needs. The Cyfronet documentation says account names carry a resource suffix such as `-gpu-a100`. The provider plugin should always pass an explicit account and partition. That belongs in provider settings, not the profile.

## Unknown, and why

Each profile ends with an `unknown:` list. All three share these:
- compute-node inbound reachability: needs a job with a listener. Compute-node outbound HTTPS on 443 is no longer unknown for Helios and Athena (S1 step 0, above); it stays unknown for Sano;
- container runtimes other than Apptainer on compute nodes, and Apptainer and user namespaces on the partitions probe 2 did not run on: needs a job (probe 2 covered Helios `plgrid-gpu-gh200` and Athena `plgrid-gpu-a100`);
- the login node's effective ssh connection limits: Helios's `sshd_config` is not readable by a plain user, Sano's readable files set none and the effective values were not read, and Athena's was not probed.

Specific to each platform:
- **Helios:** whether a slurmrestd endpoint exists and is reachable for a plain user; the size of the node-local disk behind `-C localfs`; the GPU model on `plgrid-gpu-rtx`; whether a lightweight poller is allowed on the login node.
- **Athena:** whether a slurmrestd endpoint exists and is reachable for a plain user; whether Slurm's native OCI support or any container runtime other than Apptainer is usable on compute nodes; why the `squashfuse_ll` image driver fails on SIF images and whether a SIF built off the cluster runs; the sizes of `$SCRATCH_LOCAL` and `$MEMFS` on a compute node; GPU driver and CUDA versions; `sbatch --test-only` with a valid account on `plgrid-gpu-a100`; whether a lightweight poller is allowed on the login node; the login node's per-user limits.
- **Sano:** memory per A100 (40 or 80 GB); the interconnect between the two GPU nodes; the size and speed of node-local `/scratch`; login-node policy (there are no public docs).
