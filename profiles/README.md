# Platform profiles

`helios.yaml` and `athena.yaml` are the alpha `PlatformProfile` objects of Helios and Athena (HBEP-4, open questions 3, 7 and 8). A Provider names one in `spec.platformProfileRef`. Sano is not shipped here; it follows in fallback mode.

| File | Object name | Partitions in the typed list |
|---|---|---|
| `helios.yaml` | `helios` | `plgrid` (CPU), `plgrid-gpu-gh200` |
| `athena.yaml` | `athena` | `plgrid-gpu-a100` |

Only a partition on which `holdRelease` was verified is in the typed list (HBEP-4, Platform profile schema). Every other partition is in the untyped block only.

## What a manifest holds

- **The typed fields** of the schema in `api/v1alpha1` (camelCase), which the Slurm provider, the lifecycle controller, the front and the deployment ID read.
- **`spec.profile`**, the untyped block, which nothing reads in alpha. It is the draft's document (`draft`, `platform`, `organization`, `kind`, `collected`, `facts`, `unknown`) with the envelope of every fact: `value`, `method`, `source`, `date`.
  - The keys stay snake_case, as in the drafts, so that a draft path leads to the fact: `spec.profile.facts.slurm.job_requeue` is `facts.slurm.job_requeue` in the draft. Typed fields are camelCase; whether the block follows is not decided.
  - Dates are quoted, so they stay text through YAML and JSON.
  - The facts of spike S1 are added from the drafts on `spike/s1-frp-dial-home`: the hold check, step 0 (outbound 443, compute-node Python and architecture) and probe 2 (compute-node Apptainer, user namespaces, home readability). Each is `measured` on 2026-10-30, and its `source` starts with the issue and comment it came from (issue #18, comment 6519285505 for step 0 and 6542078107 for probe 2; commit 35babef for the hold check, which has no comment). The drafts on `main` do not have them yet.
  - S1's `compute_to_agents_vm_ports` is left out. It describes this project's own VM, not what is true for everyone on the platform.

## Where a typed value comes from

A comment next to each typed value names its source (HBEP-4, open question 7):
- a path in `spec.profile`, written `facts.<group>.<fact>` (a partition is `partitions[name=<p>]`);
- "derived", for a division of such facts, with the numbers;
- "framework default (HBEP-n, section)", where no fact exists.

These values are framework defaults or derived, not measured facts:

| Value | Where | Source |
|---|---|---|
| `lostGrace` 2m, `unknownBound` 10m, `inflightWindow` 5m | both | HBEP-4, Platform profile schema. `inflightWindow` stays 5 min until the `sbatch` latency over ssh is measured (Graduation criteria). |
| `drainWindow` 2h10m, `drainMargin` 10m | both | HBEP-4 and HBEP-5, Draining |
| `handoverMargin` 30m | both | HBEP-4 and HBEP-7, submission plan |
| `health.*`, `restart.*` | both | HBEP-4 and HBEP-7; tuned in beta |
| `maxIdle` 30m | Athena | HBEP-4 and HBEP-7, Retention; motivated by Athena's GPU-only rule (`facts.login_node_policy.gpu_only`) |
| `cpusPerGpu` 72, `memPerGpu` 122375Mi | Helios `plgrid-gpu-gh200` | derived: 288 CPUs and 489500 MiB per node, over 4 GPUs. The centre documents a billing share of 120 GB per GPU without naming the GB, so a one-GPU request of this memory may be billed as more than one GPU. |

Slurm's "MB" is 2^20 bytes (the provider sends `--mem=<MiB>M`), so memory values use `Mi`.

## What is not set, and why

A fact that nothing states is absent, not false (HBEP-4, Platform profile schema).
- **`jobName`** (both): unset. No draft records a job-name maximum or a pattern, and the 56-character round trip is pending (HBEP-4, Risks and Graduation criteria). `maxLength` is set per partition once a conformance run has submitted a 56-character name there and read it back, and that run is its source. 56 is neither a fact nor a default (`@hpc-burst-science-agent`, 31 Oct 2026).
- **`jobCommentAccounted`** (both): unset. HBEP-4 makes it optional and only recorded; the tag is in the job name either way.
- **`maxIdle`** (Helios): unset outside Athena.
- **`imageFormat`, `cpusPerGpu`, `memPerGpu`** on Helios `plgrid`: the partition is a CPU partition for conformance runs only and hosts no Instance (`@hpc-burst-science-agent`, 31 Oct 2026). Probe 2 ran on `plgrid-gpu-gh200` only, and the partition has no GPU. Its `gpusPerNode` is 0, so the Slurm provider refuses any request for a GPU there (`internal/slurm`, `TestShippedCPUPartitionRefusesGPUAllocations`). Absent `imageFormat` means `sif` (`v1alpha1.ImageFormatOf`), which only an Instance would use.

## How the manifests are checked

- `api/v1alpha1/e2e/shipped_profiles_test.go` applies each file, read from `profiles/`, to the real CRD with strict field validation, so a misspelled typed key is refused instead of pruned. It then shows that copies with one value broken are refused (`imageFormat: oci` among them), that the untyped block comes back unchanged, and that every envelope has a known method, a source and a text date.
- `internal/slurm/shipped_profiles_test.go` runs the Slurm provider's own `ReadProfile` on each alpha partition.
- The same file shows that the provider refuses a GPU request on Helios `plgrid` and accepts it on `plgrid-gpu-gh200`.
- None compares a typed value with the fact it was copied from: that would prove only the copy. The checks that matter are admission, the provider's own checks, and the conformance runs that read these profiles.
- **Open:** the conformance harness builds its own inline profile and does not read `profiles/*.yaml` yet. It is wired with #93's Athena run, together with the 56-character name round trip on all three partitions.

## Content rules

HBEP-4, open question 8: no host name, address, user name, account, grant or job ID. Accounts and grants are Provider settings.

## Draft platform profiles (spike S5)

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

### The facts that matter most

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

### What this means for the design (for the HBEP author to weigh)

- **Job state must come from accounting.** MinJobAge is 10 s on both Cyfronet systems, so a completed job can disappear from `squeue` within about 10 s, and a provider plugin that polls `squeue` can lose the final state of a job. It has to fall back to `sacct`. The evaluation's crossplane-runtime trap (an Observe that reports a purged job as missing, so the reconciler resubmits it) applies here directly.
- **SSH and CLI access works; REST is unverified.** `squeue --json` and `sacct --json` work for a plain user on all three platforms. Whether a REST job API (slurmrestd) is reachable for plain users is unverified. JWT is not enabled, but slurmrestd can also run with local (unix-socket) auth. Until that is checked, the multiplexed ssh transport is the one path known to work.
- **Requeue is on by default everywhere.** HBEP-7's requeue risk is real, so the plugin should submit with `--no-requeue`.
- **Text times depend on the login session's time zone.** The Cyfronet login sessions print Europe/Warsaw local time and Sano's prints UTC. These are the defaults of login sessions, not proven properties of the schedulers. Epoch fields are unaffected, so plugins should prefer epoch or JSON timestamps over parsing text. HBEP-4's time-zone field is still needed for any text a plugin must parse, and the plugin must convert to UTC.
- **The container runtime may not be uniform.** It should be a per-platform profile fact. Apptainer is on the Helios and Sano login nodes. On Athena's login node none of apptainer, singularity, enroot, podman or docker was found (charliecloud, udocker, sarus and shifter were not searched for), and nothing was probed on any compute node, including the Helios GH200 (aarch64) nodes. Whether the instance launchers need a container-free path is a candidate pending S1 (issue #18), not a conclusion. frpc and a static engine environment are candidates for such a path.
- **Pass an explicit account and partition.** The account probes used the probing user's default account (and, on Athena, the default partition), so they do not show what every user needs. The Cyfronet documentation says account names carry a resource suffix such as `-gpu-a100`. The provider plugin should always pass an explicit account and partition. That belongs in provider settings, not the profile.

### The typed image format (after spike S1)

The drafts' container facts come from the login nodes. Spike S1's probe 2 (issue #18) then ran Apptainer on compute nodes, and the PlatformProfile types the result per partition as `imageFormat` (`sif` or `sandbox`), the format the allocation agent builds and runs the engine image in:

| Partition | `imageFormat` | Why |
|---|---|---|
| Helios `plgrid-gpu-gh200` | `sif` | a SIF image runs |
| Athena `plgrid-gpu-a100` | `sandbox` | Apptainer cannot run a SIF image there; a sandbox directory image runs |

It is not a deployment-ID input. A partition that states no format gets `sif` (`v1alpha1.ImageFormatOf`).

### Unknown, and why

Each profile ends with an `unknown:` list. All three share these:
- compute-node outbound internet and inbound reachability: needs a job, which is spike S1;
- user namespaces and the container runtime on compute nodes: needs a job;
- the login node's effective ssh connection limits: Helios's `sshd_config` is not readable by a plain user, Sano's readable files set none and the effective values were not read, and Athena's was not probed.

Specific to each platform:
- **Helios:** whether a slurmrestd endpoint exists and is reachable for a plain user; the size of the node-local disk behind `-C localfs`; the GPU model on `plgrid-gpu-rtx`; whether a lightweight poller is allowed on the login node.
- **Athena:** whether a slurmrestd endpoint exists and is reachable for a plain user; whether Slurm's native OCI support or any other container runtime is usable on compute nodes; the sizes of `$SCRATCH_LOCAL` and `$MEMFS` on a compute node; GPU driver and CUDA versions; `sbatch --test-only` with a valid account on `plgrid-gpu-a100`; whether a lightweight poller is allowed on the login node; the login node's per-user limits.
- **Sano:** memory per A100 (40 or 80 GB); the interconnect between the two GPU nodes; the size and speed of node-local `/scratch`; login-node policy (there are no public docs).
