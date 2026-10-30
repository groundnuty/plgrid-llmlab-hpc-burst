# Spike S4: a Slurm provider on crossplane-runtime, against a fake Slurm

Issue #24. Code, tests and this report are in this directory. This version answers two independent reviews of PR #32 (Codex Astra and Opus). It replaces the first design's timing-based guard with a two-phase submission protocol.

## How to run

Go 1.26.0 or newer on PATH (the `go.mod` minimum; tested with Go 1.27.3), then:

```
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@v0.24.1
export KUBEBUILDER_ASSETS=$(setup-envtest use 1.35.0 -p path)
cd spikes/s4
./run-tests.sh          # unit tests, then every e2e scenario; or:
go test ./internal/... ./fakeslurm/... && go test -v -count=1 ./e2e/
```

`run-tests.sh` also waits for a low load on a shared host (`S4_MAX_LOAD`, default 4.0), records peak memory, and lists any leftover process. Builds, logs and fake state go under `S4_WORK_ROOT` (default: the system temp directory). A full run took 7 min 43 s to 8 min 9 s (two runs, 30 Oct 2026).

## Question

Can a provider built on crossplane-runtime's managed reconciler run in-process, outside a Kubernetes cluster, against a Slurm command line, without ever resubmitting or duplicating a job? The hazards:
- Slurm's MinJobAge: a finished job leaves `squeue` within 10 s on Helios and Athena;
- default requeue;
- a reconciler killed mid-submit;
- two reconcilers on one Allocation;
- and, from the reviews, an `sbatch` whose outcome is unknown.

## Answer

**Against a fake Slurm, yes.** It needs a two-phase submission protocol, and three facts still need a real cluster (see "Still unknown").

**Proven on the fake.** The guarantee depends on the platform profile's `hold_release`:
- **Hold mode (the default).** At most one job with an Allocation's tag is ever released, so at most one can run, whatever the timing.
- **Fallback mode (`hold_release: false`).** Nothing is resubmitted unless a job is found by tag or an operator abandons the attempt. After an abandon, a late registration of the abandoned attempt becomes a reported duplicate, which is cancelled on deletion.

Both hold in every scenario and race the reviews raised, including:
- an `sbatch` that fails ambiguously and registers its job later;
- accounting that lags behind `squeue`;
- two reconcilers racing a same-second failure and retry;
- deletion while a submission is in flight.

**Not proven.** Three facts need a real cluster:
- whether a plain user can `scontrol release` their own `--hold` job on Helios and Athena, where a Lua `job_submit` plugin runs;
- the real JSON shapes;
- the real `sbatch` latency over ssh.

The protocol ([state machine](#the-submission-protocol)):
1. **The external name is a deterministic job tag**, set by an Initializer before any `sbatch`: `hb-<name>-<12 hex of sha256(namespace/name/uid)>`, passed as `--job-name`. Observe, Create, Delete and status all use the persisted name. Submission is refused when the name is empty or not the canonical tag.
2. **Two-phase submit:** claim an attempt (compare-and-swap on the object), `sbatch --hold --no-requeue`, record the job ID once (compare-and-swap; never overwritten), then cancel every other held job with the tag and `scontrol release` the recorded job, once. In the tests the hold lasted 11-33 ms when Create released at once, and under 0.5 s when a restarted reconciler adopted a held job. While no reconciler runs, a held job waits for one to return (6.7-7.4 s across a Lease handover); it gains no queue age meanwhile.
3. **An ambiguous `sbatch`** (timeout, signal, transport error, unparseable output, transient reason) is **Unknown**, never Failed, and is resolved by looking the tag up. In hold mode a late registration lands held and is cancelled, so a retry after the in-flight window is safe. In fallback mode, for a platform whose profile has `hold_release: false`, only a job found by tag permits progress. After a bound, an event and a condition ask an operator.
4. **Observe looks the tag up in `squeue --json`, then in `sacct --json -X -D` over an explicit window.** An ended job is existing and terminal. The established job is reported Lost only after it has been absent from both for longer than the profile's `lost_grace`, and it returns to its real state if it reappears.
5. **Deletion cancels every live job with the tag** (held, pending or running), and keeps the finalizer while a submission is unresolved.
6. **Submit with `--no-requeue` and report a requeue as `Failed`.** The verdict stays in status after every trace of the requeue is gone.

**Counts come from the fake's call log,** over all jobs in the test, never from the reconciler's return values. "Runs" means released by the provider (the only way a held job can start). The controls switch rules off and reproduce the bugs against the same fake.

| Scenario or finding | Test | Want (source) | Result |
|---|---|---|---|
| 1. Job ends; gone from `squeue` past MinJobAge | `TestS1EndedJobNotResubmitted` | 1 sbatch, 1 release (issue) | PASS: 1, 1; `Succeeded` from `sacct` |
| 1. Control: squeue-only Observe, no guards, fallback | `TestS1ControlSqueueOnlyResubmits` | ≥ 2 (bug reproduced) | PASS: 2 |
| 1. One-rule control: squeue-only, guards on | `TestS1SqueueOnlyWithGuardsReportsLost` | 1; `Lost` after lost_grace | PASS |
| 2. Killed after `sbatch` returned (hold, fallback) | `TestS2KillAfterSbatchReturned` | 1 each; hold: 1 release | PASS |
| 2. Same, job ends and is purged while down (fallback) | `TestS2KillAfterSbatchJobEndsWhileDown` | 1; found in `sacct` | PASS |
| 2. Killed while `sbatch` blocked, reply lost (hold, fallback) | `TestS2KillWhileSbatchBlocked` | 1 each | PASS |
| 2. Killed after the claim, before `sbatch` | `TestS2KillBeforeSbatch` | hold: 1, after the window; fallback: 0 until an operator decides, then 1 | PASS: hold submitted at 13.3 s (window 12 s); fallback condition at 15 s, 1 after the abandon |
| 2. Annotations alone (classic, `--deterministic-external-name=false`) | `TestS2AnnotationsOnlyStalls` | 1; stuck | PASS |
| 2. Control: classic, deterministic, guards off | `TestS2ControlNoTagLookupDuplicates` | 2 | PASS |
| 3. Requeue under `--no-requeue`, count-only evidence; verdict sticky after all evidence is gone (v0.0.42, v0.0.40) | `TestS3RequeueReportedFailed` | `Failed`, reason `Requeued`; 1 sbatch, 1 release | PASS |
| 3. Requeue, rerun, end and purge while down: accounting only | `TestS3RequeueWhileDown` | `Failed` from `sacct` | PASS (v0.0.40: flag-only) |
| 3. Requeue leaves the job held (`JobHeldUser`) | `TestS3RequeueHoldNeverReleased` | 1 release in total; job stays held | PASS |
| Delete a pending Allocation | `TestDeletePendingCancels` | 1 scancel | PASS |
| 4. Two reconcilers, gate holds the submitter before registration | `TestS4TwoReconcilersInflightGuard` | 1 sbatch; the other logs the guard naming attempt 1 and the claimant; ≤ 2 calls/s | PASS: 1.0 squeue/s, 1.0 sacct/s |
| 4. crossplane default (`deterministic=false`) | `TestS4TwoReconcilersCrossplaneDefault` | 1 | PASS |
| 4. Control: guard off, same gate (hold, fallback) | `TestS4ControlNoGuard` | 2 distinct callers; hold: 1 released, other cancelled while held; fallback: 2 live, duplicate reported | PASS |
| 4. Lease, leader killed mid-submit | `TestS4LeaderElection` | 1 | PASS |
| Astra B2: same-second failure and retry, two reconcilers | `TestB2SameSecondFailureRetryRace` | 2 calls, 1 rejected, 1 accepted; tie asserted | PASS (tie at one second) |
| Astra B3: delete while `sbatch` in flight | `TestB3DeleteWhileSubmissionInFlight` | finalizer kept; late job cancelled before the object goes; 0 live | PASS |
| Same, fallback mode (the late job would run unheld) | `TestB3FallbackDeleteWhileSubmissionInFlight` | as above | PASS |
| Fallback deletion with a dead claimant | `TestFallbackDeleteWaitsForUnresolvedAttempt` | finalizer kept past the window until the operator abandons; 0 sbatch | PASS |
| A reply lost after slurmctld accepted the job ("Socket timed out") (hold, fallback) | `TestLostReplyIsUnknown` | Unknown, adopted by tag; 1 sbatch | PASS |
| Astra B4: prepopulated non-canonical external name | `TestPrepopulatedExternalName` | refused, 0 sbatch; a restored job is observed and cancelled on delete | PASS |
| Astra B5: duplicate; original completes and is purged; `squeue` in descending order | `TestDuplicateKeptAfterOriginalPurged` | established ID kept; duplicate reported | PASS |
| Adoption with descending `squeue` order | `TestAdoptLowestWithReversedOrder` | lowest ID adopted; the other cancelled while held | PASS |
| Opus 1: ambiguous `sbatch`, job registers 7 s later | `TestReviewAmbiguousSbatchFailure` | 1 job runs | PASS: 1 submitted, 1 released |
| Same, registration after the window (hold, fallback) | `TestAmbiguousLateRegistrationAfterWindow` | hold: 1 released, late job cancelled while held; fallback: 1 sbatch, operator condition, then adopted | PASS |
| Opus 2: accounting lag after a kill (hold, fallback) | `TestReviewAcctLagAfterKill` | 1 job; fallback: no resubmission, operator condition | PASS |
| Opus 3: delete with a live duplicate | `TestReviewDeleteWithDuplicate` | every job with the tag cancelled | PASS: 2 of 2 |
| Science agent: Lost grace, then reappearance | `TestLostGraceAndReappearance` | not Lost before lost_grace; Lost after; back to `Succeeded` | PASS: Lost at 8.4 s (grace 8 s) |
| Opus nit: spec edits | `TestSpecImmutable` | refused | PASS |
| envtest runs no garbage collection | `TestEnvtestHasNoGarbageCollection` | owned object kept | PASS |

Unit tests (`internal/...`, `fakeslurm/...`) cover:
- outcome classification (accepted, rejected, unknown on timeout, signal, ssh error, transient reason or bad output);
- hold, release and requeue parsing, with count-only and flag-only cases;
- sorting and record merging;
- adoption choice;
- the critical-annotation updater against crossplane's own (the control rolls the record back);
- the poll hook;
- every new fake behaviour.

Measured in two full runs of `run-tests.sh` (30 Oct 2026, 10:07-10:15 and 10:21-10:30 UTC, every test PASS both times, 463 s and 489 s; the second is at the final test set):
- **Hold time** (sbatch registering the job to its release, from the fake's log):
  - 11-33 ms when Create released at once;
  - 106-473 ms when a reconciler adopted the job on its next Observe;
  - 6.7-7.4 s across a Lease handover, and 13.6-14.1 s for a restart delayed past the window on purpose, because no reconciler was running.
- **No hot loop.** A reconciler waiting on the guard polled 0.95-1.10 `squeue` and as many `sacct` calls per second. The first design spun at about 6 per second each.
- **Scenario 1:** the job left `squeue` 10.2 s after it ended. In the next 8 s the provider made 7-8 `squeue` and as many `sacct` calls, and no `sbatch`.
- **Scenario 4 with the Lease:** the follower found and released the held job 6.6-7.2 s after the leader was killed (lease duration 6 s).
- **Lost:** 8.35-8.38 s after the job left both `squeue` and `sacct`, with `lost_grace` 8 s.

## The submission protocol

The science agent checked this state machine against HBEP-7 before it was implemented, and asked for two changes: Lost needs a grace period and must be revisable, and the hold must last seconds. Both are in.

**Persisted** as annotations on the Allocation, each written with the resourceVersion as precondition. Annotations rather than status, because they survive a restore without status, as crossplane's own create annotations do.
- `spike.hpc-burst.example/submission`: the latest attempt `{n, token, state, by, at, reason}`, where state is InFlight, Unknown, Rejected, Done or Abandoned.
- `spike.hpc-burst.example/job-id`: the established job ID, written once.
- `spike.hpc-burst.example/released`: the job ID after its one release.
- `spike.hpc-burst.example/abandon-attempt`: an operator's decision on an unresolved attempt.

**Transitions:**
- **Claim.** The claim succeeds only on an unchanged object. Two reconcilers that read the same version cannot both claim; one that read an older version fails with 409. So one attempt is in flight at a time, and no timestamp is compared. That fixes the second-resolution tie (Astra B2).
- **Outcome.** The `sbatch` outcome becomes:
  - Done, with the job ID;
  - Rejected (Create returns an error and crossplane retries with backoff);
  - Unknown (Create returns no error).

  Only "Batch job submission failed:" with a non-transient reason, or a command that never started, counts as Rejected.
- **Adoption.** A job found by tag is adopted whatever the attempt's state:
  - a live job that is not held wins (only an established job is ever released, so it proves a lost record);
  - next, a job that ran;
  - else the lowest job ID.

  Results are sorted by job ID, never taken in output order.
- **Settle.** It runs in Create right after the job ID is recorded, and on every Observe while not deleting:
  - cancel every held job with the tag except the established one;
  - then `scontrol release` the established job, if all of these hold: no release marker, restart count 0, never started, reason `JobHeldUser`.

  A plain user may release a user hold: job_mgr.c 24.11:13762-13766 and 23.11:14942-14946. A submit with priority 0 from the user is a user hold: 24.11:7431-7435, 23.11:8922-8926.

  A requeue can put a job back into `JobHeldUser` (24.11:17613-17628), so the provider never releases twice. An admin hold is reported as a condition.
- **The in-flight guard.** While an attempt is InFlight within the window, or Unknown, Observe reports the resource as existing (Creating) and logs `in-flight guard: submission attempt <n> by <id> is unresolved`. The status message is stable, and a poll-interval hook re-checks after 1 s (default 5 s). Status-only writes no longer trigger a reconcile (`resource.DesiredStateChanged()`). So the guard waits on a `RequeueAfter`, not in a hot loop. The first design spun at 6 `squeue` and 6 `sacct` calls per second.

**Safety argument.** Only the job whose ID is recorded is ever released, and that ID is written once under compare-and-swap. So at most one job with the tag runs, whatever the timing. Held jobs never leave `squeue` until cancelled, so settle sees every late or concurrent submission.

**A hazard in crossplane-runtime itself.** v2.4.3's `RetryingCriticalAnnotationUpdater` re-applies every in-memory annotation after a conflict (`pkg/reconciler/managed/api.go:283-301`), so a stale copy can roll a provider's annotation back. The spike replaces it (`managed.WithCriticalAnnotationUpdater`) with one that re-applies only crossplane's four keys and re-reads from the API server, not the cache. A unit test shows the rollback with crossplane's updater and none with the replacement.

## Do the create annotations alone handle scenario 2?

**They prevent a second submission only when the process dies, and they never discover the outcome.**
- **When the process dies.** For an interrupted first create, crossplane's default (`WithDeterministicExternalName(false)`) stops the reconciler. `TestS2AnnotationsOnlyStalls` shows no duplicate, but the Allocation is stuck until a person removes the annotation. The annotations give no automatic discovery or recovery of the outcome.
- **When Create returns an error.** A Create that returns an error, which includes an ambiguous `sbatch` killed at the reconcile timeout, is recorded as complete and failed. The annotations then allow a second create. That was the review's first reproduction (2 jobs at 9b3d066).
- **The timestamp tie.** The annotations have one-second resolution, and "incomplete" needs create-pending strictly newer than create-failed. A failure and its retry in the same second therefore look complete.

What the reconciler does (crossplane-runtime v2.4.3, `pkg/reconciler/managed/reconciler.go`):
- Before `Create` it writes `external-create-pending` with a plain `Update`, so a stale resource version fails with 409 (l. 1363-1376).
- After `Create` it writes create-failed on any error (l. 1397) or create-succeeded (l. 1437), with `UpdateCriticalAnnotations`.
- With `WithDeterministicExternalName(false)` an incomplete create stops the reconciler before Observe (l. 1104-1113). With `true` it goes on to Observe.

The spike therefore keeps its own attempt record and uses crossplane's annotations for nothing.

## Design of Observe

```
squeue --me --json --name=<tag>              # sorted by job ID; held = hold:true / priority 0 / JobHeld*
sacct --json -X -D --name=<tag> -S now-<age+1h> -E now    # when squeue lacks the job ID, or shows nothing
abandon annotation = current unresolved attempt -> Abandoned
nothing established, jobs found        -> adopt (record job-id)
established, hold mode, not deleting   -> settle: cancel held extras, release once
being deleted                          -> exists while any job with the tag is live or an attempt is unresolved; Delete cancels all live
established, found                     -> phase from the job; duplicates (sticky) reported
established, absent from both          -> keep phase until lost_grace, then Lost; back to the real phase if it reappears
nothing found, attempt unresolved      -> guard (hold mode: until the window passes; fallback: until found or abandoned)
non-canonical external name            -> Refused (no sbatch)
otherwise                              -> missing -> Create
```

Further rules, each tied to a hazard:
- **Explicit `-S` on sacct.** Without `-S` or `-j`, sacct starts at midnight of the current day (sacct man page, 24.11.5), so a job from yesterday would read as missing. The relative form `now-N` avoids the login session's time zone (Europe/Warsaw on the Cyfronet systems, S5).
- **The tag lives in `--job-name`, not `--comment`.** slurmdbd stores the job comment only with `AccountingStoreFlags=job_comment`, so a comment tag would vanish from sacct.
- **Duplicates are sticky.** A job ID seen once with the tag stays in `status.atProvider.duplicates`, with an event, across both sources. When the established job leaves `squeue`, sacct is consulted, so the established ID is kept even after it is purged (Astra B5).
- **`forProvider` is immutable** (a CEL rule), so an edit is refused rather than silently ignored. Update stays excluded by the management policies.

## Requeue (scenario 3)

- **What Slurm shows.** A requeued job is set to `PENDING`, or `PENDING|COMPLETING`, with `restart_cnt` raised and no `REQUEUED` flag in `squeue`: `_set_requeued_job_pending_completing`, job_mgr.c 23.11:4111-4121 and 24.11:2415-2425. sacct keeps each earlier execution under `-D` as `PENDING` plus the `REQUEUED` flag. sacct carries `restart_cnt` only in data_parser v0.0.42.
- **The fake now matches.** The tests cover:
  - count-only detection (`TestS3RequeueReportedFailed`, with a precondition that `squeue` shows `["PENDING"]` and `restart_cnt` 1);
  - flag-only detection (v0.0.40 accounting, `TestS3RequeueWhileDown`, plus unit tests);
  - recovery from accounting alone after a requeue, rerun, end and purge while no reconciler ran.
- **Sticky verdict.** It is checked after `fakeslurmctl forget-requeue`, which models an accounting purge of the earlier records and leaves no evidence at all.
- **Defence in depth on Helios and Athena.** Slurm refuses to requeue a `--no-requeue` job: `_job_requeue_op` returns `ESLURM_DISABLED` unless a launch failure meets `PrologFlags=ForceRequeueOnFail` (job_mgr.c 23.11:18671-18678, 24.11:17440-17447). Both profiles record `PrologFlags=Alloc,DeferBatch,NoHold`, so scenario 3 is defence in depth there. Where a forced requeue can happen, the job may come back held as `JobHeldUser`. `TestS3RequeueHoldNeverReleased` shows the provider leaves it held and reports Failed.

## Evidence against the old logic (9b3d066)

Both races Astra reported without a test were run against 9b3d066's provider, with the new fake and this branch's `e2e/races_test.go` and `e2e/barrier_test.go` overlaid on an export of that commit. Both fail there, for the reasons named:

```
races_test.go:58: tie: create-failed and the retry's create-pending are both 2026-10-30T10:06:18Z; r2's retry is held at the gate
races_test.go:65: B2: 3 sbatch calls, 1 rejected, 2 jobs accepted for one Allocation; want 2, 1, 1
races_test.go:74: B2: 2 live jobs able to run, 0 released; want at most 1
--- FAIL: TestB2SameSecondFailureRetryRace (14.51s)
races_test.go:100: B3: the finalizer was released while r2's sbatch was in flight
races_test.go:119: B3: job 1001, which landed after the delete, was not cancelled before the Allocation disappeared
races_test.go:122: B3: Allocation deleted, 1 job(s) with its tag still live: [{ID:1001 ... Transitions:[{State:PENDING}]}]
--- FAIL: TestB3DeleteWhileSubmissionInFlight (3.86s)
```

The Opus review's three reproductions failed at 9b3d066 in the review's own runs:
- `TestReviewAmbiguousSbatchFailure` and `TestReviewAcctLagAfterKill` each produced 2 `sbatch` calls and 2 registered jobs for one Allocation;
- `TestReviewDeleteWithDuplicate` left job 1002 `PENDING` after the Allocation was gone.

This branch keeps them, adapted to the protocol: the metric is "jobs able to run", the hold-mode job is no longer started by hand, and repro 3 takes its duplicate from the fallback no-guard control. All three now pass.

## Method

- **Provider** (`cmd/provider-slurm`, `internal/controller`, `internal/slurm`): the managed reconciler of crossplane-runtime v2.4.3 with an `ExternalClient` for the `Allocation` kind (`apis/v1alpha1`, CRD in `config/crd`). It runs `sbatch`, `squeue`, `sacct`, `scancel` and `scontrol` found on PATH. A real provider would run the same commands over one multiplexed ssh connection (HBEP-4); `slurm.Runner` is the seam.
- **Flags.**
  - Profile values:
    - `--hold-release` (`hold_release`, default true);
    - `--lost-grace` (`lost_grace`, default 2 min);
    - `--inflight-window`, which must exceed `--timeout`;
    - `--unknown-bound` (default 10 min);
    - `--unresolved-poll`.
  - Controls:
    - `--observe=tag|squeue-only|jobid`;
    - `--deterministic-external-name`;
    - `--guard-succeeded` and `--guard-inflight`;
    - `--leader-elect`.
- **Fault hooks** (environment variables of the provider): `S4_FAULT_AFTER_SBATCH` and `S4_FAULT_BEFORE_SBATCH` write a marker file and block inside `Create`. The test waits for the marker, SIGKILLs the provider's process group, and checks its precondition on the API object before restarting.
- **Fake Slurm** (`cmd/fakeslurm`, `fakeslurm/`): one Go binary under the names `sbatch`, `squeue`, `sacct`, `scancel`, `scontrol` and `fakeslurmctl`.
  - **State and log.** State lives in one JSON file under flock. Every call goes to a JSON-lines log with argv and the caller's identity.
  - **Purge.** It forgets terminal jobs from `squeue` after `FAKE_SLURM_MIN_JOB_AGE` (10 s) and keeps them in `sacct`.
  - **Holds.** It models `--hold`: a held job shows `hold: true`, `priority` 0 and `JobHeldUser`, and cannot start. It also models `scontrol release` with Slurm's error cases.
  - **JSON.** The JSON follows Slurm's data_parser field tables (v0.0.40 for Slurm 23.11, v0.0.42 for 24.11). As in real sacct, `-n` means `--noheader`, and sacct lists only the invoking user's jobs unless `-a` or `-u` is given.
  - **Fault knobs.** Every knob is a barrier or a counted fault, not a timing guess:
    - a gate that holds `sbatch` before registration;
    - a definite failure once, aligned to a second boundary;
    - a reply lost after registration;
    - requeue and requeue-hold, and an admin hold;
    - hiding a job from accounting;
    - forgetting requeue records;
    - descending `squeue` order.
- **Tests** (`e2e/`): each test gets its own namespace, fake state and provider processes. Expected values come from issue #24, the HBEPs and the Slurm source, never from the code under test.

## Running outside a cluster

crossplane-runtime ran outside a cluster:
- **API server.** It is envtest's: a real `kube-apiserver` and `etcd` (Kubernetes 1.35.0, from `setup-envtest`), started by the test binary on 127.0.0.1 with random ports. There are no nodes, no kubelet and no controller manager.
- **Providers.** Each provider is a plain process: controller-runtime's manager with an explicit `--kubeconfig` and an envtest user in `system:masters`. It refuses to start without the kubeconfig, so it cannot fall back to another cluster.

Measured in the same run:
- envtest's `kube-apiserver` and `etcd` were up in 3.3-4.2 s.
- Peak resident memory (sampled every 2 s): `kube-apiserver` 262 MiB, `etcd` 52 MiB, one `provider-slurm` 63 MiB.
- Up to two providers ran at once.

Gotchas:
- controller-runtime v0.23.1 serves metrics on `:8080` by default, on all interfaces; the provider sets metrics, health and pprof to `"0"`.
- controller-runtime registers `--kubeconfig` on the global flag set, so the provider uses its own.
- envtest runs no controller manager. `TestEnvtestHasNoGarbageCollection` shows that an object owned by a deleted owner still exists 10 s later, and the deleted namespace stays `Terminating`. This confirms HBEP-11's note that a "control plane without nodes" must also run the Kubernetes controller manager once owner references matter.

## What this means for the HBEPs

Checked against the HBEPs on main at cf92f48 (merged into this branch).

- **HBEP-4 (provider interface):**
  - **Submission becomes two operations:** submit-held and release. Its outcome is Accepted, Rejected or Unknown, and Unknown is resolved by the core operation "find by tag" (squeue, then sacct with an explicit window).
  - **Profile fields this needs:**
    - `hold_release` (whether a plain user may hold and release; unverified on every platform);
    - `lost_grace` (above the measured squeue-to-sacct gap);
    - MinJobAge;
    - whether accounting stores the job comment;
    - the data_parser version.
  - **The tag goes in `--job-name`,** with the object UID. This answers "Plugins find their own jobs by scanning" (Risks).
- **HBEP-7, open question 11 (submission protocol).**
  - **The tested protocol is two-phase hold and release, with an explicit Unknown state.** The record is written by compare-and-swap, and only the recorded job is ever released.
  - **Neither alternative suffices alone:**
    - a Lease does not: the new leader found the job only by tag;
    - crossplane's annotations do not (above);
    - a time window does not, because accounting lag has no bound (below).
  - **"A duplicate found later is cancelled while still pending"** is what settle does for held duplicates. A released duplicate (fallback mode, or from outside the protocol) is reported and cancelled on deletion.
  - **The requeue risk.** `--no-requeue` plus "requeued means Failed" works. Within the `sacct -D` window the earlier records show the requeue even on v0.0.40. Keeping the verdict in status is what survives an accounting purge of those records (`TestS3RequeueReportedFailed`). On Helios and Athena Slurm refuses the requeue, so this is defence in depth there. What to do with a requeued job remains open.
- **HBEP-11:**
  - **The "control plane without nodes" option works** for the reconciler, and the controller manager must run too (above).
  - **"A released Allocation's provider job is cancelled before its object disappears" holds,** including for a job that lands after the delete (`TestB3DeleteWhileSubmissionInFlight`, and its fallback variant) and for duplicates (`TestReviewDeleteWithDuplicate`).
  - **Kill-mid-handover safety holds for the submit step on the fake.**

## Known hazards

- **The squeue-to-sacct gap is real.** slurmctld purges a completed job after MinJobAge even when slurmdbd has no record of it yet. 23.11 queues the start record "when slurmdbd comes back up" (`_list_find_job_old`, job_mgr.c 23.11:11519-11528). In 24.11 the purge has no accounting condition at all (24.11:9841-9921). Slurm buffers accounting during slurmdbd outages (accounting documentation), so the gap has no bound.
  - In hold mode this cannot cause a second running job.
  - In fallback mode, an Unknown attempt in the gap ends in the operator condition, never in a resubmission (`TestReviewAcctLagAfterKill/fallback`).
  - The interval at risk was misplaced in the first report. Inside the in-flight window the old guard held; the hole was a gap that outlasts the window.
- **A hold dropped at submit.** A site `job_submit` plugin (Helios, Athena and Sano all run `JobSubmitPlugins=lua`) can rewrite a job's priority, so a `--hold` might not hold. job_mgr.c 24.11:7425-7435: "The job_submit plugin may have set the priority directly or put the job on hold." The provider releases only the recorded job, but a job whose hold was dropped could run before the record. This is the first thing to verify on a real cluster.
- **An orphan held job.** In hold mode, a registration arriving after the in-flight window, once the Allocation is gone, leaves a held orphan. It never runs, and HBEP-11's orphan report lists it.
- **Clock.** The in-flight window and lost_grace measure age with the reader's clock, so reconcilers on different hosts need roughly synchronised clocks. No ordering depends on timestamps any more.

## Still unknown (needs a real cluster)

- **Release by a plain user.** Can a plain user `sbatch --hold` and `scontrol release` their own job on Helios and Athena? The Slurm source says yes for a user hold. The Lua `job_submit` plugin on all three platforms may change that, or drop the hold. The profile's `hold_release` stays unverified until then.
- **The real JSON shapes.** The fake follows the data_parser source (`hold`, `priority`, `state_reason`, `restart_cnt`, `job_state` flags), but no real `squeue --json` or `sacct --json` output from Helios or Athena was compared. Also unchecked:
  - whether `--name` and `--me` filter `squeue --json` server-side;
  - whether squeue's `end_time` of a running job is its expected end.
- **`sbatch` latency over ssh.** How long it takes bounds the in-flight window. A local timeout does not stop a remote `sbatch`. The hold time on a real cluster is this latency plus one `squeue` and one `scontrol` round trip.

## Environment

- Agents VM: 6 cores, 31 GB RAM, shared with other spikes. Tests ran with `nice -n 10`.
- Go 1.27.3 (`go.mod` minimum 1.26.0); crossplane-runtime v2.4.3; crossplane apis v2.3.4; controller-runtime v0.23.1; k8s client libraries v0.35.3; envtest Kubernetes 1.35.0.
- Load during the measured runs: 1-minute load 3.96 and 3.38 at the start, 4.89 and 2.71 at the end (other agents' work), with 19 GB of memory available.
- Every race test uses a barrier in the fake, not a sleep, to set its interleaving. The waits that remain are upper bounds on observation.
- The full suite ran twice end to end, and each new test ran at least once more in focused runs, with no timing failure; one harness race (a SIGKILLed sbatch not yet reaped) was fixed. That is too few runs to rule out flakiness.

## Provenance

| Component | Project and URL | Licence | Version | Use |
|---|---|---|---|---|
| crossplane-runtime | https://github.com/crossplane/crossplane-runtime | Apache-2.0 | v2.4.3 | managed reconciler (library) |
| crossplane core APIs | https://github.com/crossplane/crossplane (`apis/v2`) | Apache-2.0 | v2.3.4 | condition and policy types |
| controller-runtime | https://github.com/kubernetes-sigs/controller-runtime | Apache-2.0 | v0.23.1 | manager, client, envtest |
| setup-envtest | same repository, `tools/setup-envtest` | Apache-2.0 | v0.24.1 | downloads the envtest binaries |
| kube-apiserver, etcd | https://github.com/kubernetes/kubernetes, https://github.com/etcd-io/etcd (envtest release of controller-tools) | Apache-2.0 | Kubernetes 1.35.0 | test API server |
| Slurm source (data_parser, job_mgr.c) | https://github.com/SchedMD/slurm (slurm-23-11-7-1, slurm-24-11-5-1) | GPL-2.0 (read, not copied) | as given | field names, hold, release and requeue semantics |
| Provider, fake Slurm, tests | new (this spike) | none yet: the repository has no licence (HBEP-15) | — | — |

No code was copied from another project, so nothing is under `third_party/forked/`.
