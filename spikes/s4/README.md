# Spike S4: a Slurm provider on crossplane-runtime, against a fake Slurm

Issue #24. Code, tests and this report are in this directory.

## How to run

Go 1.27.x on PATH, then:

```
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@v0.24.1
export KUBEBUILDER_ASSETS=$(setup-envtest use 1.35.0 -p path)
cd spikes/s4
./run-tests.sh          # unit tests, then every e2e scenario; or:
go test ./internal/... ./fakeslurm/... && go test -v -count=1 ./e2e/
```

`run-tests.sh` also waits for a low load on a shared host (`S4_MAX_LOAD`, default 4.0), records peak memory, and lists any leftover process. Builds, logs and fake state go under `S4_WORK_ROOT` (default: the system temp directory). A full run took 3 min 24 s.

## Question

Can a provider built on crossplane-runtime's managed reconciler run in-process, outside a Kubernetes cluster, against a Slurm command line, without ever resubmitting or duplicating a job? The hazards are Slurm's MinJobAge (a finished job leaves `squeue` within 10 s on Helios and Athena), default requeue, a reconciler killed mid-submit, and two reconcilers on one Allocation.

## Answer

Yes, with four rules in the provider. crossplane-runtime's own create annotations are not enough on their own.

1. **The external name is a deterministic job tag**, set by an Initializer and persisted before any `sbatch`. The tag is `hb-<name>-<12 hex of sha256(namespace/name/uid)>`, passed as `--job-name`. The Slurm job ID is observed state only.
2. **Observe looks the tag up in `squeue --json`, then in `sacct --json -X -D` over an explicit window**, and reports an ended job as existing and terminal.
3. **"Not found" is never "missing" once a submission is known**, and never while a create may still be in flight (the in-flight guard, below).
4. **Submit with `--no-requeue`, and report a requeue as `Failed`.** The verdict stays in status after the job is purged.

With these rules, every scenario ends with exactly one `sbatch`, counted from the fake's call log over all jobs in the test. The controls run the same scenario with rules switched off (flags in the table) and duplicate the job against the same fake, so the tests can tell the two apart. Only the scenario-4 control removes exactly one rule; the one-rule control for scenario 1 shows the guards working as an independent layer.

| Scenario | Test | sbatch calls (want) | Result |
|---|---|---|---|
| 1. Job ends; gone from `squeue` past MinJobAge | `TestS1EndedJobNotResubmitted` | 1 (1) | PASS: `Succeeded`, source `sacct` |
| 1. Control: `--observe=squeue-only --guard-succeeded=false --guard-inflight=false` | `TestS1ControlSqueueOnlyResubmits` | 2 (≥ 2) | PASS: the fake produces the bug |
| 1. One-rule control: `--observe=squeue-only`, guards on | `TestS1SqueueOnlyWithGuardsReportsLost` | 1 (1) | PASS: no resubmission, but phase `Lost` instead of `Succeeded` |
| 2. Killed after `sbatch` returned | `TestS2KillAfterSbatchReturned` | 1 (1) | PASS |
| 2. Same, the job ends and is purged while the reconciler is down | `TestS2KillAfterSbatchJobEndsWhileDown` | 1 (1) | PASS: found in `sacct` |
| 2. Killed while `sbatch` blocked (lost reply) | `TestS2KillWhileSbatchBlocked` | 1 (1) | PASS |
| 2. Killed after create-pending, before `sbatch` | `TestS2KillBeforeSbatch` | 1 (1) | PASS: submitted after the in-flight window |
| 2. Annotations alone: `--observe=jobid --deterministic-external-name=false` | `TestS2AnnotationsOnlyStalls` | 1 (1) | PASS: no duplicate, but stuck |
| 2. Control: `--observe=jobid --deterministic-external-name=true`, both guards off | `TestS2ControlNoTagLookupDuplicates` | 2 (2) | PASS: duplicate |
| 3. Requeue under `--no-requeue` (data_parser v0.0.42 and v0.0.40) | `TestS3RequeueReportedFailed` | 1 (1) | PASS: `Failed`, reason `Requeued`; survives the purge |
| Delete a pending Allocation | `TestDeletePendingCancels` | 1 (1), `scancel` 1 | PASS |
| 4. Two reconcilers, in-flight guard | `TestS4TwoReconcilersInflightGuard` | 1 (1) | PASS |
| 4. Two reconcilers, crossplane default: `--deterministic-external-name=false --guard-inflight=false` | `TestS4TwoReconcilersCrossplaneDefault` | 1 (1) | PASS (at the cost of the scenario-2 stall) |
| 4. Control: `--guard-inflight=false` | `TestS4ControlNoGuardDuplicates` | 2 (2) | PASS: duplicate, reported in `status.atProvider.duplicates`; the older job is kept |
| 4. Lease (leader election), leader killed mid-submit | `TestS4LeaderElection` | 1 (1) | PASS |
| envtest runs no garbage collection | `TestEnvtestHasNoGarbageCollection` | — | PASS: owned object kept, namespace stays `Terminating` |

Measured run (30 Oct 2026, 08:42-08:46 UTC, one `run-tests.sh`, all tests PASS, 3 min 24 s in total):
- **Scenario 1:** the job was purged from `squeue` 10.0 s after it ended. In the following 8 s the provider made 9 `squeue` and 9 `sacct` calls and no `sbatch`. The control resubmitted about 2 s after the purge: its test took 12.6 s, of which 10.1 s went to waiting for the purge.
- **Scenario 2:** after each kill, the precondition held on the API object (create-pending set, no result, no job ID). The restarted reconciler found the job each time; each test then watched for 5 s more for a late duplicate. After a kill before `sbatch`, it submitted 12.2 s after the dead reconciler's create-pending, which is the 12 s in-flight window.
- **Scenario 4:** during the other reconciler's 2 s `sbatch`, the non-submitting one called `squeue` 19 times and did not submit. With the Lease, the follower took over and found the job 7.7 s after the leader was killed, with a 6 s lease duration.
- **Kill tests** SIGKILL the provider's whole process group. In the lost-reply test the blocked `sbatch` died with it, and the test checks that.

## Do the create annotations alone handle scenario 2?

**No. A tag lookup is still needed.** The annotations detect that a create was interrupted. They cannot say whether the job exists, so they can only stop the reconciler or let it guess.

What the reconciler does (crossplane-runtime v2.4.3, `pkg/reconciler/managed/reconciler.go`):
- Before `Create` it writes `crossplane.io/external-create-pending` with a plain `Update`, so a stale resource version fails with 409 (l. 1355-1376).
- After `Create` it writes `external-create-succeeded` (or `-failed`) with `UpdateCriticalAnnotations`, which retries on conflict (l. 1397, 1437).
- On the next reconcile, a pending annotation newer than both results means "create incomplete" (`meta.ExternalCreateIncomplete`). This check runs after the Initializers and before Observe (l. 1104):
  - with `WithDeterministicExternalName(false)`, the default, the reconciler refuses to continue. It sets `Synced=False` with "cannot determine creation result - remove the crossplane.io/external-create-pending annotation if it is safe to proceed" and returns without requeueing;
  - with `true` it goes on to Observe, and whatever Observe says decides whether `Create` runs again.

The three scenario-2 variants show the consequences:
- **Annotations alone** (`TestS2AnnotationsOnlyStalls`): this is crossplane's classic pattern for a non-deterministic name. `Create` sets the external name to the job ID, and `deterministic=false`. After the restart there is no duplicate (1 `sbatch`). But the Allocation is stuck: the restarted reconciler made no `squeue` or `sacct` call at all, the job runs unobserved, and only a person removing the annotation unblocks it.
- **Deterministic flag without a tag lookup** (`TestS2ControlNoTagLookupDuplicates`): Observe has no job ID to look up, reports the job missing, and `Create` submits again (2 `sbatch`).
- **Tag lookup** (the design, four variants): Observe finds the job by tag in `squeue`, or in `sacct` when it was purged while the reconciler was down. One `sbatch`. Observe then records `external-create-succeeded` itself (returned as `ResourceLateInitialized`, which the reconciler persists), so the create is no longer incomplete.

In this design the kill window loses only the job ID and the create result, never the external name: the tag is persisted before the create-pending annotation. The issue's wording ("before the external name was stored") fits the job-ID pattern of the first variant.

## Design of Observe

```
squeue --me --json --name=<tag>                       # filtered again client-side by name
  found -> exists; phase from job_state; requeued if restart_cnt > 0 or REQUEUED
sacct --json -X -D --name=<tag> -S now-<age+1h> -E now   # client-side name filter
  found -> exists; latest record's state; requeued if >1 record, a REQUEUED record or restart_cnt > 0
not found:
  being deleted                          -> missing (finalizer can go)
  create-succeeded set, or a job ID seen -> exists, phase Lost (never resubmit)
  create incomplete, pending < window    -> error (requeue with backoff): a create may be in flight
  otherwise                              -> missing -> Create
```

Further rules, each tied to a hazard:
- **The oldest job with the tag is the Allocation's.** Any others are listed in `status.atProvider.duplicates` (checked in `TestS4ControlNoGuardDuplicates`). HBEP-7 proposes cancelling a pending duplicate; the spike reports it only.
- **Explicit `-S` on sacct.** Without `-S` or `-j`, sacct starts at midnight of the current day (sacct man page, 24.11.5), so a job from yesterday would read as missing. The relative form `now-N` avoids the login session's time zone (Europe/Warsaw on the Cyfronet systems, S5).
- **The tag lives in `--job-name`, not `--comment`.** slurmdbd stores the job comment only with `AccountingStoreFlags=job_comment` (slurm.conf), so a comment tag would vanish from sacct. Sano's GPU partition requires a non-generic `--job-name` anyway (S5).
- **During deletion an ended job is reported missing.** Otherwise the reconciler calls Delete, re-observes the job in sacct, and loops (l. 1241). A requeued job that is pending again has not ended, so deleting its Allocation still cancels it. `scancel` on an ended or unknown job counts as success.
- **The requeue verdict is sticky in status.** sacct in data_parser v0.0.40 (Slurm 23.11: Athena, Sano) has no `restart_cnt`. After the purge, only the `-D` duplicate records show the requeue, and only within the window. `--no-requeue` does not exclude requeues: `PrologFlags=ForceRequeueOnFail` overrides it (sbatch man page).
- **Update is never reached.** Observe always reports up to date. The Allocation's default management policies are `[Observe, Create, Delete, LateInitialize]` (no Update, a set crossplane-runtime supports), and `Update` returns an error anyway. LateInitialize stays in, because Observe uses it to persist the create result.

### The in-flight guard (scenario 4)

Optimistic concurrency on the create-pending write protects only two reconcilers that read the same resource version:
1. Reconciler A writes create-pending (version N) and starts `sbatch`.
2. Reconciler B reads version N and observes while A's `sbatch` is still running. It finds nothing.
3. B writes its own create-pending on version N. Nothing has bumped the version, so the write succeeds, and B submits a second job.

`TestS4ControlNoGuardDuplicates` shows exactly this (2 `sbatch`, one from each reconciler).

Two guards close the gap:
- **crossplane's default.** `deterministic=false` makes B stop at "create incomplete" (`TestS4TwoReconcilersCrossplaneDefault`, 1 `sbatch`), but it also produces the scenario-2 stall.
- **The design's in-flight guard.** While create-pending is newer than any result and younger than a window (12 s in the tests; it must exceed the reconcile timeout, 5 s in the tests, because that bounds a live peer's `sbatch`), Observe returns an error instead of "not found". B retries with backoff until A's job appears or the window passes. In `TestS4TwoReconcilersInflightGuard` the non-submitting reconciler observed many times during the other's 2 s `sbatch` and did not submit. After a kill before `sbatch`, the restarted reconciler waits out the window and then submits once (`TestS2KillBeforeSbatch`).

The provider's Lease (HBEP-7 open question 11) also works: only the leader reconciles, the follower made no Slurm call, and after the leader was killed mid-submit the new leader took the Lease after the lease duration, found the job by tag and did not submit (`TestS4LeaderElection`). The new leader found the job only because of the tag lookup, so the Lease does not replace it. That a deposed but still running leader's `sbatch` can land after the handover is reasoning from how leader election works, not a test result; it is why the in-flight guard should stay with a Lease.

## Method

- **Provider** (`cmd/provider-slurm`, `internal/controller`, `internal/slurm`): the managed reconciler of crossplane-runtime v2.4.3 with an `ExternalClient` for the `Allocation` kind (`apis/v1alpha1`, CRD in `config/crd`). It runs `sbatch`, `squeue`, `sacct` and `scancel` found on PATH. A real provider would run the same commands over one multiplexed ssh connection (HBEP-4); `slurm.Runner` is the seam.
- **Flags select the controls:**
  - `--observe=tag|squeue-only|jobid`;
  - `--deterministic-external-name`;
  - `--guard-succeeded` and `--guard-inflight`;
  - `--leader-elect`.
- **Fault hooks** (environment variables of the provider): `S4_FAULT_AFTER_SBATCH` and `S4_FAULT_BEFORE_SBATCH` write a marker file and block inside `Create`. The test waits for the marker and SIGKILLs the provider's process group. Each kill test then checks its precondition on the API object (create-pending set, no result, no job ID) before restarting.
- **Fake Slurm** (`cmd/fakeslurm`, `fakeslurm/`): one Go binary under the names `sbatch`, `squeue`, `sacct`, `scancel` and `fakeslurmctl`.
  - State lives in one JSON file under flock.
  - Every call goes to a JSON-lines log with argv and the caller's identity. The tests count `sbatch` from that log, never from the reconciler's return values.
  - It forgets terminal jobs from `squeue` after `FAKE_SLURM_MIN_JOB_AGE` (10 s) and keeps them in `sacct`. `fakeslurmctl requeue` requeues a job regardless of `--no-requeue`.
  - The JSON follows Slurm's data_parser field tables (v0.0.40 for Slurm 23.11, v0.0.42 for 24.11) in `src/plugins/data_parser/*/parsers.c`. squeue times are `{set, infinite, number}` objects, `job_state` is a list, and sacct times are plain integers. `restart_cnt` appears in sacct only in v0.0.42.
  - Options: an accounting delay, a delay or a block inside `sbatch`, and a failing `sbatch`.
- **Tests** (`e2e/`): each test gets its own namespace, fake state and provider processes. The providers watch only that namespace. Expected values come from the issue and the HBEPs.

## Running outside a cluster

crossplane-runtime ran outside a cluster. The API server is envtest's: a real `kube-apiserver` and `etcd` (Kubernetes 1.35.0, from `setup-envtest`), started by the test binary on 127.0.0.1 with random ports. There are no nodes, no kubelet and no controller manager. Each provider is a plain process: controller-runtime's manager with an explicit `--kubeconfig` (it refuses to start without one, so it cannot fall back to another cluster) and an envtest user in `system:masters`.

Measured in the same run:
- envtest's `kube-apiserver` and `etcd` were up in 3.9 s.
- Peak resident memory (sampled every 2 s): `kube-apiserver` 241 MiB, `etcd` 49 MiB, one `provider-slurm` 59 MiB.
- Up to two providers ran at once.

Gotchas:
- controller-runtime v0.23.1 serves metrics on `:8080` by default (`metricsserver.DefaultBindAddress`), on all interfaces, so a second provider on the same host would collide on the port. The provider sets metrics, health and pprof to `"0"`.
- controller-runtime registers `--kubeconfig` on the global flag set, so the provider uses its own.
- envtest runs no controller manager. `TestEnvtestHasNoGarbageCollection` shows the effect: 10 s after deletion, an object owned by a deleted owner still exists and the deleted namespace is still `Terminating`. This confirms HBEP-11's note that a "control plane without nodes" must also run the Kubernetes controller manager (or a bundled binary that includes it) as soon as owner references matter. The spike needed only finalizers, which work without it.
- kcp was not tried (optional in the brief).

## What this means for the HBEPs

Checked against the HBEPs on main at c1adb88 (PR #22).

- **HBEP-4 (provider interface):**
  - It confirms three proposals now on main: final state comes from accounting (`sacct`), a job missing from `squeue` is not read as never submitted, and the plugin submits with `--no-requeue`.
  - It adds:
    - "find by tag" as a core operation with two sources (live queue, then accounting) and an explicit `sacct` window, since the default window starts at midnight;
    - the tag in `--job-name`, including the object UID. This answers "Plugins find their own jobs by scanning" (Risks).
  - Profile fields beyond the requeue default main already proposes: MinJobAge, whether accounting stores the job comment, and the data_parser version.
- **HBEP-7:**
  - Open question 11 (submission protocol):
    - neither a Lease alone nor crossplane's annotations alone is sufficient;
    - the tested protocol is a deterministic tag in `--job-name`, Observe by tag before every create, never "missing" after a known submission, and an in-flight window longer than the reconcile timeout;
    - for Slurm, the tag mechanism is the job name, not the comment.
  - The requeue risk: `--no-requeue` plus "requeued means Failed" works, but the verdict must be kept in status, because Slurm 23.11's sacct JSON has no restart count.
  - What to do with a requeued job (cancel it, or let the lifecycle controller decide) is open. The spike only reports it, and deleting the Allocation cancels it.
- **HBEP-11:**
  - The "control plane without nodes" option works for the reconciler.
  - The garbage-collection test confirms HBEP-11's note that the controller manager must run too.
  - Kill-mid-handover safety (Test plan) holds for the submit step.

## Environment

- Agents VM: 6 cores, 31 GB RAM, shared with other spikes. Tests ran with `nice -n 10`.
- Go 1.27.3; crossplane-runtime v2.4.3; crossplane apis v2.3.4; controller-runtime v0.23.1; k8s client libraries v0.35.3; envtest Kubernetes 1.35.0.
- Load during the measured run: 1-minute load 3.24 at the start and 4.52 at the end (5-minute 6.21 and 5.33), from other agents' work; 20 GB memory available. Timing-sensitive steps had margins of at least 2 s. The final test code ran once or twice per test with no timing failure; the early failures were harness bugs. That is too few runs to rule out flakiness.

## Still unknown

- **The fake's JSON is a model, not a recording.**
  - It follows the data_parser source, but no real `squeue --json` or `sacct --json` output from Helios or Athena was compared. The S5 raw output is private.
  - Unchecked on a real cluster:
    - whether `--name` and `--me` filter `squeue --json` server-side (the man page: "most filtering arguments are still used");
    - the exact state list of a requeued job;
    - whether earlier executions under `-D` show `REQUEUED`.
- **The squeue-to-sacct gap.**
  - Can a job leave slurmctld before slurmdbd has its record? The man page is silent.
  - MinJobAge purges can also run late during backfill.
  - The Lost rule makes such a gap harmless after a recorded submission. A gap inside the in-flight window after a kill is not covered.
- **Remote `sbatch` timing.** Over ssh, a local timeout does not stop a remote `sbatch`, so the in-flight window must exceed the worst real submit time. That needs measurement on the clusters.
- **Duplicates the guards miss.** These are reported, not cancelled.
- **Time.** The pending annotation has one-second resolution and is compared with the reader's clock, so reconcilers on different hosts need synchronised clocks.

## Provenance

| Component | Project and URL | Licence | Version | Use |
|---|---|---|---|---|
| crossplane-runtime | https://github.com/crossplane/crossplane-runtime | Apache-2.0 | v2.4.3 | managed reconciler (library) |
| crossplane core APIs | https://github.com/crossplane/crossplane (`apis/v2`) | Apache-2.0 | v2.3.4 | condition and policy types |
| controller-runtime | https://github.com/kubernetes-sigs/controller-runtime | Apache-2.0 | v0.23.1 | manager, client, envtest |
| setup-envtest | same repository, `tools/setup-envtest` | Apache-2.0 | v0.24.1 | downloads the envtest binaries |
| kube-apiserver, etcd | https://github.com/kubernetes/kubernetes, https://github.com/etcd-io/etcd (envtest release of controller-tools) | Apache-2.0 | Kubernetes 1.35.0 | test API server |
| Slurm data_parser field names | https://github.com/SchedMD/slurm (`src/plugins/data_parser/v0.0.40` at slurm-23-11-7-1, `v0.0.42` at slurm-24-11-5-1) | GPL-2.0 (read, not copied) | as given | shape of the fake's JSON |
| Provider, fake Slurm, tests | new (this spike) | repository licence | — | — |

No code was copied from another project, so nothing is under `third_party/forked/`.
