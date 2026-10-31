# HBEP-4: Provider interface and profiles

## Summary

This HBEP defines how the framework works with providers: our access to one platform each (HBEP-3). Providers share one core interface, with capability flags that say which core operations a provider supports. What is true for everyone on a platform is data in its platform profile. What one provider adds (its account, grant, credentials reference, allowed partitions and labels) is its provider settings. Upstream bugs go to a known-issues register tied to versions, with a regression test and a workaround expressed as configuration. It is written for authors of provider plugins and of the lifecycle controller (HBEP-7).

For alpha it decides:
- the Slurm provider: the Slurm command-line tools over one multiplexed ssh connection per login node and credential, under crossplane-runtime's managed reconciler (spike S4, PR #32);
- its core operations, written as an interface;
- the Slurm dialect: the tag in `--job-name`, the queue listing then accounting, and the JSON of data_parser v0.0.40 and v0.0.42;
- the typed fields of the platform profile that alpha reads;
- the LLMLab managed provider as a read-only catalog.

## Motivation

Stage 1 (hit the limit), stage 2 (get resources), stage 3 (keep it running) and stage 7 (pay and give back), as defined in HBEP-2.

Problems this addresses, in general terms:
- **Submission rules differ per cluster and are learned by failure:** required options, job-name rules, compute nodes without home directories, schedulers that print local time.
- **Scheduling facts break naive designs:**
  - start estimates swing by hours and flip to unknown;
  - a freed slot goes to another user at once;
  - cancelling a parent starts its pending follow-on at once;
  - a dependency offset counts from the parent's start, not its end;
  - users cannot extend a job's time limit, and a running job's account cannot be changed;
  - a fast-start partition allows one job per user, pending jobs included.
- **A managed gateway has its own failure modes:**
  - it cuts replies that send no byte for a fixed time, and stalls periodically;
  - models go inactive without the listing showing it;
  - unknown, inactive and blocked models return the same error;
  - it rewrites requests;
  - keys expire mid-run.
- **Upstream bugs:** a router's default total deadline cut long streams without a log line; an engine's keep-alive was shorter than the router's connection reuse; a compile on an unseen shape ran past the engine's step limit and killed it. Each workaround lived in notes or one-off scripts, and nothing turned it off once the bug was fixed.
- **Usage was hard to count:** the center's accounting tool counted only finished jobs, billing formulas differ per cluster, and behaviour at grant exhaustion was unknown until it happened.

### Goals

- One core interface that every provider implements. Differences are flags and data, not branches on a provider's name.
- Every contingency as a profile parameter with thresholds, enabled by default and cheap when idle.
- For each upstream bug: a register entry tied to versions, pinned versions per profile, one regression test, and a workaround as configuration.
- A new platform is added by writing its platform profile, plus plugin code only where data cannot express a difference. A new provider on a known platform is only provider settings.
- Expected performance in each platform profile, as the reference for qualification (HBEP-3, HBEP-7).

### Non-goals

- Lifecycle decisions: when to hedge, over-allocate, hand over, drain, restart or repurpose (HBEP-7). This HBEP exposes what those decisions need.
- What runs inside an Allocation, and how Instances start and restart (HBEP-7). This HBEP starts the job and stages the files it needs.
- How instances are reached (HBEP-6). The profile records the reachability facts HBEP-6 relies on.
- Which providers serve a model name. That is routing policy (HBEP-3, HBEP-5).
- Center priority pushes. The framework never assumes one.
- The static kind's interface, which comes later.

## Proposal

### User stories

- An operator adds Cyfronet Athena. They write its platform profile (time limits, submission options, billing formula, container runtime, expected performance), and reuse plugin code where the profile can express the difference. Then they add a provider for their grant there: provider settings with the account, a credentials reference, the allowed partitions and the labels.
- The operator gets a second grant on Helios. It is a second provider: new provider settings over the same platform profile.
- The lifecycle controller hedges `example-27b` across a provider on Helios and one on Athena. It reads each provider's start estimate as advisory, submits on both, keeps the first Allocation whose Instance serves and cancels the pending loser (HBEP-7). Hedging across providers at once comes after alpha.
- The controller qualifies a new Instance on Athena. Its benchmark falls well below the profile's expected performance for that node type, so the controller discards it (HBEP-7).
- LLMLab takes a model offline. Its listing does not show it, since the listing carries no activity flag (Managed provider in alpha), so in alpha the model shows only as LLMLab's 503 at serving. After alpha, the managed provider reports the model's state and returns a defined error instead of an ambiguous one.
- An engine upgrade fixes a bug. The profile's pin passes the register entry's fixed-in version, the workaround turns off, and the regression test keeps running.
- An agent asks what an allocation has cost so far. The profile's billing formula and the allocation's actual running time give the answer.
- The controller restarts while an `sbatch` is in flight on Helios. The Slurm provider finds the job by its tag, adopts it, and lifts its hold once. No second job runs.

### Notes, constraints and caveats

- **Alpha scope** (science-agent decision, 30 Oct 2026, within the alpha the owner set for HBEP-11):
  - one batch platform kind, Slurm, over ssh and the command line;
  - Helios and Athena as the alpha platforms, both with `holdRelease` true, measured by spike S1 (issue #18). Sano runs fallback mode until its hold is checked, and its required QoS option is outside alpha (Provider settings);
  - the LLMLab managed provider as a read-only catalog, with no start or restart;
  - a single user (HBEP-11).
- **Two layers.**
  - A platform profile holds what is true for everyone on a platform: hardware, architecture, limits, time zone, filesystems, requeue behaviour, expected performance and known issues.
  - Provider settings add one provider's account, grant, credentials reference, allowed partitions and labels (organization, platform, partition, account; HBEP-3).
  - Every provider on a platform uses that platform's profile.
- **A provider is a plugin, a platform profile and provider settings.** Providers share a common core plus capability flags.
- **Two controllers.** Only a Provider's own controller calls its platform. The lifecycle controller (HBEP-7) creates and deletes Allocation objects and reads their status; it never calls a provider (Design details).
- **Cluster API.** The provider model follows Cluster API's idea of pluggable infrastructure providers behind common resources, applied to model serving.
- **Kinds** (HBEP-3) follow the platform: managed (a fixed catalog, no start or restart), batch (leases with unknown start and hard end, continuity through follow-ons) and static (later).
- **LLMLab will improve but keeps the same kinds of characteristics.** Its contingencies stay in its platform profile even when they rarely fire; thresholds keep them cheap.
- **Hedging is the framework's lever** (HBEP-3, HBEP-7). The core must therefore let the controller submit on several providers, read start estimates marked advisory, see promptly when an allocation starts, cancel a pending allocation, and read each allocation's hard end.
- **Agent primitives** (HBEP-10) require idempotent operations and reports of partial failure, so every core operation reports per-job results, and every one except submission is idempotent. For submission, the tag on the job supports recovery but does not enforce uniqueness. In hold mode every job is submitted held and only the recorded job's hold is ever lifted, so at most one job per Allocation runs (HBEP-7, open question 11).
- **Spike S5 (PR #19).** S5 recorded login-node facts for Helios, Athena and Sano as draft platform profiles in `profiles/`. Spike S1's step 0 (issue #18) added the first compute-node facts and the hold check to the Helios and Athena drafts, on its branch (commit 35babef). The entries below that cite S5 rest on its measurements and on the centers' documentation.
- **The job transport is decided:** the Slurm command-line tools over one multiplexed ssh connection per login node and credential. `squeue --json` and `sacct --json` work for a plain user on all three platforms (spike S5, PR #19). A REST job API stays a later option (open question 13).
- **The container runtime is a per-platform fact.** Apptainer is on the Helios and Sano login nodes. None of Apptainer, Singularity, enroot, Podman or Docker was found on Athena's login node, and other runtimes were not searched for. User namespaces are off on both Cyfronet login nodes (spike S5, PR #19). On the compute nodes, spike S1's probe 2 (issue #18) found Apptainer on both alpha partitions, with user namespaces off. Helios GH200 runs a SIF image; Athena A100 runs a sandbox directory but not a SIF. Each partition's `imageFormat` records which (open question 15).
- **Time.** Slurm prints local time (Europe/Warsaw) on the Cyfronet systems and UTC on Sano (spikes S5 and S7). The plugin reads epoch fields from the JSON and converts every time to UTC. No alpha code parses a text time.

### Risks and mitigations

- **A stale profile acts on old facts with confidence.** Centers change policies without notice. Mitigation: each draft fact records its method, source and date (S5's envelope), and the Slurm provider checks the one fact that every reply shows, the JSON version (Design details). Probes for other facts are open question 7.
- **Start estimates are trusted.** They are advisory. Decisions act on actual starts.
- **The framework's own polling trips login throttles.** Too many parallel logins have tripped a cluster's connection limit. Mitigation: one multiplexed connection per login node and credential, so two providers' credentials never share one; one queue listing per Provider per poll interval, shared by its Allocations; at most four commands at once on a connection. Spike S5 (PR #19) could not establish the login nodes' ssh limits, so a per-platform budget waits for them (open question 17).
- **Local time in scheduler output** gives wrong end times, worst across a daylight-saving change. Spike S5 (PR #19) measured it: Slurm's text output in a login session is in Europe/Warsaw local time, with no zone marker, on Helios and Athena, and in UTC on Sano. Mitigation: the plugin reads epoch times from the JSON and converts every time to UTC; it parses no text time. Tests cross a daylight-saving change.
- **A finished job vanishes from the queue listing.** Helios and Athena purge a finished job from `squeue` about 10 s after it ends (MinJobAge), and Sano after 300 s (spike S5, PR #19). A plugin that polls only `squeue` loses the final state, and a controller that reads the job as missing may submit it again. Mitigation: final state comes from accounting (`sacct`). A job missing from `squeue` is never read as completed or as never submitted; with no accounting record either, its state is unknown until `lostGrace` passes (Design details).
- **"Not listed" has two shapes.** Right after a job ends, Helios answers `squeue --json -j <id>` with an empty `jobs` list and exit 0; once the ID is purged, and on Athena at once, it exits 1 with "Invalid job id specified" (spike S7's capture, `spikes/s7/slurm-json-real.md`). Both mean "ask accounting". Any other non-zero exit is a failed read, never "not listed".
- **The scheduler requeues a job.** Requeue is on by default on all three platforms (`JobRequeue=1`; spike S5, PR #19), so Slurm may run a job's script again under the same job ID. Mitigation: submit with `--no-requeue`, record each platform's default in its profile, and report a requeue that still happens as Failed. HBEP-3 decides identity across executions (open question 13, from spike S4): a requeue fails that Instance, and its replacement is a new Allocation.
- **A job without GPUs on a GPU-only platform.** Athena is meant for GPU jobs only, and its documentation warns that workloads without GPUs can lead to account suspension (spike S5, PR #19). Mitigation: the plugin refuses a submission that requests no GPU on a partition whose profile entry names a GPU model, and refuses a partition that the profile does not list. Athena's profile lists only GPU partitions. Whether an idle GPU Allocation counts is HBEP-7's open question 13.
- **Cancelling in the wrong order starts a follow-on early.** Alpha submits no provider-side dependencies (HBEP-7, open question 3), so there is no dependent to cancel. A plugin that adds them must cancel dependents before their parent.
- **The hold lift has a check-then-act window.** Between reading a job and releasing it, another lifter, or a requeue, can change it. Mitigation: immediately before `scontrol release`, the leader re-reads the marker and the job under a per-job lock, and releases only a user-held job with restart count 0 that has not started and carries no marker (Core operations, `LiftHold`). Known risk: the lock holds within one process only, so a window remains after the re-read: a leader stalls after its re-read; another leader releases the job, and it starts; an administrator requeues it with a hold (the plugin submits `--no-requeue`, so only an administrator's action can requeue it); then the stalled leader resumes and releases that later hold. `scontrol release` names the job, not the hold, so nothing stops it. It needs a stall and a manual requeue with a hold in the same window, so it is rare, and it is accepted for alpha (science-agent decision, 31 Oct 2026, from PR #96's review). This is the one exception to "the hold is lifted exactly once" (HBEP-3, HBEP-7, HBEP-11). Its test is tracked in #93.
- **A partly applied operation is reported as failed.** An interrupted multi-job cancel may already have cancelled some jobs. Results are reported per job, never as one status.
- **A submission is accepted but runs as something else,** for example with another partition, time limit or job name, or becomes unschedulable after an in-place change. Mitigation: the plugin reads every job back after it is established and compares it with what it submitted; a submitted job is never edited (Design details).
- **Plugins find their own jobs by scanning.** The newest matching job may belong to someone else. Decided in HBEP-3, from spike S4 (PR #32): every job carries its Allocation's tag in `--job-name`, and jobs are found only by tag.
- **A site rewrites the job name.** A `job_submit` plugin could shorten or change it, and a job then can never be found by tag. Mitigation: the read-back compares the name with the tag; a mismatch refuses the Allocation and cancels the job. A 56-character name is checked on each alpha partition before alpha (Graduation criteria).
- **A failed read looks like absence.** A transport error, a time-out or an unparseable reply says nothing about the job. Mitigation: only reads that succeeded count towards `lostGrace`; a failed read keeps the last known state and raises a condition.
- **A Slurm upgrade changes the JSON.** A new data_parser version renames or moves fields. Mitigation: every reply's `meta.plugin.data_parser`, without its `data_parser/` prefix, must equal the profile's; otherwise the reply is not interpreted and the Provider gets a condition.
- **A workaround outlives its bug, or masks another fault,** for example a raised time-out hiding a hang. Mitigation: each register entry names the symptom it covers and the version range it applies to (open question 6).
- **Contingencies cost when idle.** Probing a shared managed gateway adds load to it. Mitigation: each contingency's idle cost is stated in the profile, and the alpha managed provider reads the catalog at most once a minute.
- **Flags too coarse for real providers** push provider-specific code into the core. A new flag is an amendment to this HBEP.
- **Account data leaks into a public profile.** Accounts, grants and credentials references are provider settings, never platform-profile fields. Login node names and gateway addresses stay with the credentials (Provider settings).
- **Expected performance goes out of date,** so qualification discards good Instances or keeps poor ones. The field records its source and date like any other (open question 7). Alpha holds no expected performance (HBEP-7, open question 12).

### Open questions

1. **Core operations.** **Decided:** for a batch provider, the platform-profile reader, find by tag, observe one job, submit (held in hold mode), lift the hold, cancel, ensure a file, and remove an Allocation's files; for a managed provider, the profile reader and list the catalog (Design details, Core operations). Source: spike S4 (PR #32) for submit, find by tag, lift the hold, cancel and observe; science-agent decision, 30 Oct 2026, for the file operations, which the allocation agent of HBEP-7 needs. **Not needed for alpha:** describe, estimate, follow-on, usage, remaining quota, and launching or stopping an Instance as a provider operation (HBEP-7 starts Instances through the allocation agent).
2. **Capability flags.** **Decided:** for alpha, the kind fixes the operation set (batch or managed), and the one flag is `holdRelease`. Source: spike S4 (PR #32) and HBEP-11. **Not needed for alpha:** the other candidates: can restart; has a hard end; gives start estimates; supports follow-on dependencies; supports node pinning; a hard end that can move later (the controller re-reads it and never plans for it, HBEP-3); can change a running job's account; compute nodes can connect out; compute nodes are reachable in; reports model state; exposes revisions and limits; rewrites requests; requires streaming. Each is added by amendment when a plugin first needs it.
3. **Profile schema.** **Decided:** for alpha, the typed fields in Design details (Platform profile schema), plus an untyped `profile` block that keeps the drafts' `facts` with their envelope. Provider settings are decided (owner decision): account, grant, credentials reference, allowed partitions and labels. The format of both is decided by HBEP-11: the PlatformProfile and Provider kinds. Source: HBEP-11's list of typed fields; spikes S4 (PR #32), S5 (PR #19) and S1 (issue #18) for the values; science-agent decision, 30 Oct 2026, for the fields the lifecycle controller reads. **Open (blocks beta):** the typed schema of the remaining field groups (hardware, environment, reachability, version pins, billing and exhaustion, managed-gateway behaviour, references to known issues) and the form of expected performance, decided with HBEP-7's open question 12. The LLMLab draft profile is still to come.
4. **Plugin granularity.** **Decided:** one plugin per scheduler type, here Slurm, with one platform profile per platform. The three drafts express every alpha difference between Helios and Athena as data: the JSON version, the GPU model, the GRES form and the time zone. Source: spike S5 (PR #19) and S7's capture; science-agent decision, 30 Oct 2026.
5. **Plugin boundary.** **Decided:** an in-process Go library inside each Provider's controller, a controller-runtime manager of its own (HBEP-11). There is one binary in alpha, so the core's version is the binary's. Source: spike S4 (PR #32), HBEP-11 (Controllers). **Not needed for alpha:** plugins in separate binaries that declare the core version they implement.
6. **Register format.** **Not needed for alpha:** alpha reads no register. The known workarounds for alpha are plain configuration in HBEP-5's front and the ModelDeployment's engine flags. The candidate entry fields stay: identifier, symptom, component and affected versions, fixed-in version, platform scope and stage (HBEP-2 dimensions), fix owner, detection check, workaround configuration, regression test, upstream link. **Open (blocks beta):** the format, so that each entry's regression test can run in CI. Source: science-agent decision, 30 Oct 2026, within the alpha scope (Notes).
7. **Profile provenance and probes.** **Decided:** for alpha, each fact in the untyped block keeps the drafts' envelope (value, method, source, date), and each typed value is copied from such a fact or is a framework default. The Slurm provider re-checks the JSON version on every reply. Source: spike S5 (PR #19); science-agent decision, 30 Oct 2026. **Open (blocks beta):** which other fields are re-checked by probes, how often and at what cost; decided by a spike on one batch and one managed provider.
8. **Public and private profiles.** **Decided:** accounts, grants and credentials references are provider settings, never platform-profile data, so they never ship in this repository. The Helios, Athena and Sano profiles ship here, since their drafts are already public (PR #19); the LLMLab profile ships with alpha. Login node names and gateway addresses are kept with the credentials, so no shipped profile holds one. Source: owner decision, 28 Oct 2026, for the provider settings; science-agent decision, 30 Oct 2026, for the rest. **Not needed for alpha:** how a private cluster's profile stays local. Alpha uses only the shipped profiles.
9. **Engine-wide data.** **Decided:** for alpha, HBEP-3 keeps the deployment-ID input table in code, with its scheme `v1`. Source: HBEP-3. **Not needed for alpha:** where engine-wide known issues and pins live beyond alpha.
10. **A managed allocation's hard end.** **Decided:** none for alpha (HBEP-3). A key about to expire, or a rejected key, sets a condition on the Provider (Managed provider in alpha); it is not a lease end. Source: HBEP-3, open question 10.
11. **Overrides.** **Decided:** for alpha, a ModelDeployment overrides two profile values, and may only shorten each. The lease length: an Allocation's time limit is the partition's `maxTime`, or the ModelDeployment's `leaseLimit` when that is shorter (HBEP-7). A short lease is what makes a real handover testable in hours. The drain window: an Instance's window is `drainWindow`, or the ModelDeployment's `routing.maxRequestDuration` plus `drainMargin` when that is shorter (HBEP-5). A short lease needs a short window to fit, and the front and the lifecycle controller compute it from the same fields. Source: science-agent decision, 30 Oct 2026; HBEP-5, open question 12, for the drain window. **Not needed for alpha:** other overrides.
12. **The partition label.** **Decided:** for alpha, a Provider allows exactly one partition, so its `partition` label holds one value. Source: science-agent decision, 30 Oct 2026. **Open (blocks beta):** how a provider that allows several partitions is labelled: one label per partition, or one provider per partition. On the Cyfronet systems an account is tied to one resource kind, so one provider per partition may be enough. Owner discussion.
13. **A REST job API.** **Not needed for alpha:** the transport is decided as the command line over ssh. Spike S5 (PR #19) found the slurmrestd binary on the Helios and Athena login nodes, none on Sano's, and no REST service that any center advertises; JWT is not enabled on any of the three. Asking each center stays the way to answer it, for a later transport. Source: spike S5 (PR #19).
14. **A long-running process on a login node.** **Decided:** no framework process runs on a login node. The Provider's controller polls from the control-plane host over ssh, and each command exits when done. Source: science-agent decision, 30 Oct 2026, following Cyfronet's prohibition of computation on login nodes and Sano's lack of a policy (spike S5, PR #19). **Not needed for alpha:** the centers' answer on lightweight processes, which a login-node relay (HBEP-6) would need.
15. **Launch without containers.** **Decided:** for alpha, an Instance runs from an OCI image named by digest, which HBEP-3 already hashes, run with Apptainer on the compute node (HBEP-7). Apptainer on the compute nodes of each alpha partition is a precondition, verified like `holdRelease`: a partition where it is missing hosts no Instance in alpha. Source: science-agent decision, 30 Oct 2026. **Decided:** the precondition holds on both alpha partitions, so Athena is in alpha. Helios GH200 runs Apptainer 1.5.4 with a SIF image. Athena A100's Apptainer 1.4.5 cannot run a SIF on the compute nodes, but runs a sandbox directory built from the same image digest, so the deployment ID does not change. Each partition's `imageFormat`, `sif` or `sandbox` (default `sif`), says which format the agent builds and runs (Platform profile schema; HBEP-7). Source: spike S1's probe 2 (issue #18, comment 6542078107) for the measurements; science-agent decision, 31 Oct 2026, for `imageFormat`. **Open (blocks beta):** a launch path that needs no container runtime, such as a packed engine environment, and how its digest enters the deployment ID. Alpha needs none, since both alpha partitions run Apptainer.
16. **Compute-node facts.** **Decided:** spike S1's step 0 (issue #18) measured outbound 443 to public endpoints from Helios CPU, Helios GH200 and Athena A100 (reached, no proxy) and compute-node Python (3.9); its hold check measured `holdRelease` true on those three partitions (commit 35babef). Three more facts are alpha preconditions, measured per alpha partition: Apptainer on the compute nodes (question 15); a 56-character job name that comes back unchanged; and, for ssh dial-home mode, the user's home directory readable from compute nodes, since the dial-home key lives there (HBEP-7). Source: spike S1 (issue #18) for the measurements; science-agent decision, 30 Oct 2026, for the preconditions. **Decided:** spike S1's probe 2 (issue #18, comment 6542078107) measured the first and the last on both alpha partitions:
    - Apptainer runs on Helios GH200 (1.5.4, SIF) and on Athena A100 (1.4.5, sandbox only; question 15);
    - user namespaces are off on both, so Apptainer runs through its setuid starter;
    - the home directory is readable from compute nodes on both: the node sees the login node's home, and `~/.ssh` can be entered. Reading a key file there was not tested, so the first Instance's registration confirms it.

    **Decided: two dial-home conditions, with different writers** (science-agent decision, 31 Oct 2026):
    - `DialHomeKeyMissing`: the Provider's controller sets it at Create, when the key files are absent from `~/.hpcburst/dial-home/`, before any submission (Two controllers);
    - `DialHomeUnreachable`: the lifecycle controller sets it when an Instance's registration over the dial-home path fails, a home directory that compute nodes cannot read included. The controller sees only that no registration came, so the trigger is `health.registrationTimeout` passing while the job runs (`RegistrationTimeout`); the condition clears at the next successful registration of an agent on that Provider. It applies it to the Provider's status by server-side apply under its own field manager (Provider status; HBEP-7).

    **Not needed for alpha:** Sano, inbound reachability, node-local disk sizes and GPU drivers.
17. **A connection budget per platform.** **Not needed for alpha:** one multiplexed connection per login node and credential, and at most four commands at once on it, are code defaults. **Open (blocks beta):** a per-platform budget in the profile, sized from the login nodes' measured ssh limits, which S5 could not read. Source: spike S5 (PR #19); science-agent decision, 30 Oct 2026, for the defaults.

## Design details

### Two controllers

- **The Provider's controller** is the only component that calls a platform. Each Provider has its own controller-runtime manager, with its own leader `Lease`, `provider-<name>`, in the namespace `hpcburst-system`, which the launcher creates (HBEP-11). It writes under the field manager `hpcburst-provider` (Provider status).
- **For a Slurm Provider** that manager runs crossplane-runtime's managed reconciler over the Allocations whose `providerConfigRef` names the Provider. The management policies exclude Update, so a submitted job is never edited. Its external client maps onto the core operations as follows, and HBEP-7's open question 11 gives the full protocol:
  - **Connect:** the profile reader. A failure sets a condition on the Provider and makes no call.
  - **Observe:** find by tag; adopt a job when none is recorded; settle (cancel every held job with the tag except the recorded one, then lift the recorded job's hold once); read the recorded job back; report Lost after `lostGrace`; report a requeue as Failed.
  - **Create:** in ssh dial-home mode, check that the dial-home key files exist (Submission), and otherwise set the Provider condition `DialHomeKeyMissing` and submit nothing; ensure the agent's binary, frpc and the Allocation's files (Submission); claim an attempt by compare-and-swap, submit, record the job ID by compare-and-swap, then settle in the same reconcile.
  - **Delete:** cancel every live job with the tag. The finalizer stays while any job with the tag is live, or an attempt is unresolved. Then write the Allocation's accounting record, `acct-<Allocation UID>`, waiting at most `lostGrace` for the read that gives the final elapsed time, and writing what is known after that, with unknown fields marked unknown (HBEP-8, Provider accounting). Then remove the Allocation's files.
  - **Update:** never.
  - crossplane-runtime's critical-annotation updater is replaced by one that re-applies only crossplane's own keys (spike S4; HBEP-11).
  - The reconciler runs with `managed.WithDeterministicExternalName(true)`, because the tag is a deterministic external name. Without it, a create interrupted before its outcome is recorded stops the reconciler before Observe, and the Allocation stays stuck until a person removes the annotation (S4: `spikes/s4/README.md`; `spikes/s4/cmd/provider-slurm/main.go`).
- **The lifecycle controller** (HBEP-7) creates Allocations with their submission parameters, deletes them, and reads their status. It never calls a provider.
- **The managed Provider's controller** runs the catalog loop under Managed provider in alpha.

### Core operations

The interface a provider plugin implements. Every time it returns is in UTC. Every operation reports per-job results. "Unknown" means the plugin cannot tell; it is never read as "none" or "failed".

For a batch provider (Slurm):
- **`ReadProfile(provider) → (profile, settings, error)`.** The platform-profile reader. It reads the Provider, its PlatformProfile and the credentials reference, and validates them:
  - the profile's kind matches the plugin;
  - the Provider allows exactly one partition, and the profile lists it;
  - `jobName.maxLength` is at least 56, and the tag form matches `jobName.pattern` when one is set;
  - `jsonDataParser` is one the plugin knows;
  - `inflightWindow` is set and exceeds the `sbatch` time-out (60 s);
  - the credentials file, and for ssh the key file it names, are mode 600 and owned by the controller's user (HBEP-11).

  It fails with every invalid field named. Read-only.
- **`FindByTag(tag, since) → (jobs, error)`.** Every job of the calling user whose name is exactly the tag: from the queue listing, and from accounting for any job the listing does not show, starting at `since`. Jobs are sorted by job ID, never taken in output order. Read-only and idempotent. An error means unknown.
- **`Observe(jobID) → (job, found, error)`.** One job: from the queue listing, else from accounting. `found` is false only when both reads succeeded and neither shows the job. Read-only and idempotent.
- **`Submit(request) → (jobID, outcome, error)`.** One `sbatch`, held when `request.hold` is set, which hold mode always sets. The outcome is:
  - Accepted: `sbatch` printed a job ID;
  - Rejected: a definite refusal ("Batch job submission failed:" with a reason that is not transient), or a command that never started;
  - Unknown: a time-out, a signal, a transport error, unparseable output, or a transient reason. Slurm may still have registered the job.

  Not idempotent. It runs only under a claimed attempt (HBEP-7, open question 11), and never retries by itself.
- **`LiftHold(jobID) → error`.** `scontrol release`. Its errors are: not held, an admin hold (reported, never retried), not found, and unknown. Within one process the reconciler calls it at most once per Allocation, guarded by the `hold-lifted` marker and the re-read below; across a leader handover a second call can still come (What remains). The operation itself does not guard.
  - **The re-read before the release** (science-agent ruling, 31 Oct 2026, for #76). Immediately before `scontrol release`, the leader re-reads the Allocation's marker from the API server and the job from Slurm, under a per-job lock in its process. It releases only a job that is held by its user (`JobHeldUser`), has a restart count of 0, has not started, and carries no lift marker. A leader that stalls before its re-read therefore cannot free a requeued job.
  - **What remains** is a known risk (Risks): a leader stalls after its re-read; another leader releases the job, and it starts; an administrator requeues it with a hold (the plugin submits `--no-requeue`, so only an administrator's action can requeue it); then the stalled leader resumes and releases that later hold. `scontrol release` names the job, not the hold, so nothing stops it. It needs a stall and a manual requeue with a hold in the same window, so it is rare, and it is accepted for alpha (science-agent decision, 31 Oct 2026, from PR #96's review). The test is tracked in #93.
- **`Cancel(jobIDs) → results`.** `scancel` per job, with a result per job: cancelled, already ended, or error. Cancelling an ended job reports "already ended", so the operation is idempotent.
- **`EnsureFile(path, sha256, mode, content) → error`.** Places one file under the profile's `stagingDir`. It does nothing when a file there already has that SHA-256. Otherwise it writes the content through the ssh connection's standard input to a temporary name, sets the mode, and renames it into place. The content never appears on a command line. Idempotent.
- **`RemoveAllocationFiles(tag) → error`.** Removes the Allocation's directory under `stagingDir` once its jobs are gone. Idempotent.

A job record holds:
- the job ID, the name, and the state as Slurm lists it (base state and flags);
- the phase;
- whether it is held, and whether the hold is a user or an admin hold;
- the state reason, the restart count, and whether it was requeued;
- the partition, the time limit and the GPU count;
- the submit time, start time, hard end and end time, in UTC;
- its source (queue or accounting) and the data_parser version of the reply.

Phases:
- `Held`: established, with its hold not yet lifted, or an admin hold, which also sets a condition.
- `Pending`, `Running`.
- `Ended`: COMPLETED, TIMEOUT (the lease end) or CANCELLED.
- `Failed`: FAILED, NODE_FAIL, OUT_OF_MEMORY, BOOT_FAIL, PREEMPTED or DEADLINE, or a requeue in any state (reason `Requeued`, kept once seen).
- `Lost`: absent from both sources for longer than `lostGrace`, counting only reads that succeeded. It returns to the real phase if the job reappears.
- While submitting: `Submitting` and `SubmissionUnknown`.
- `Refused`: an invalid profile, a non-canonical external name, or a rewritten job name.

For a managed provider:
- **`ReadProfile`**, as above, for the managed kind.
- **`ListCatalog() → (entries, error)`.** The models the gateway lists, each with its name and any revision it reports. LLMLab reports none (Managed provider in alpha). Read-only.

### Transport

- **One OpenSSH master connection per login node and credential,** shared by every command of that Provider (`ControlMaster`, `ControlPersist`, and a `ControlPath` private to the Provider). `BatchMode` is on. The master carries no port forward and no other session, because tunnels on a shared master broke kill-by-PID (L-7.3).
- **Where the login node is, and as whom,** is in the ssh client configuration that the Provider's credentials reference names (Provider settings). It is never in the profile, the API or a log.
- **Commands are fixed argument templates.** Every value placed in one (tag, partition, account, time limit, path) is first checked against a safe pattern of letters, digits and `._:=,/-`. A value that fails is refused before any call. The remote command runs in the login node's non-interactive shell, as S5 measured it.
- **Paths are resolved once per connection.** After each new master connection, the plugin reads `HOME` on the login node with `printenv HOME`, and, when `stagingDir` starts with an environment variable, that variable with `printenv <VAR>`. Each result must be an absolute path that matches the safe pattern. An unset or empty variable, or a result that fails the check, sets the Provider condition `PathUnresolved`, and no call that needs the path is made. Commands, the bootstrap and `agent.json` hold only the resolved absolute paths, never a variable or `~`.
- **Time-outs:** 60 s for `sbatch`, 30 s for every other command. A time-out on `sbatch` is Unknown; on a read it is an error, so the state is unknown.
- **Concurrency:** at most four commands at once on one connection (a code default; open question 17).
- **Reconnects:** when the master fails, the plugin closes it and reconnects with exponential backoff from 1 s to 1 min. Calls in flight report transport errors.
- **Poll rate:** the queue listing is read once per poll interval for the whole Provider (`squeue --me --json`) and cached. Every Allocation's Observe reads that cache, after a forced refresh after any write. Accounting is read only for jobs the listing does not show, and once for each recorded job after it is first seen started, for its queue wait (Provider status). The interval is 30 s, and 5 s while any of the Provider's attempts is unresolved or a job is held.

### The Slurm dialect

- **Submission:** `sbatch --parsable --job-name=<tag> --no-requeue [--hold] --partition=<p> --account=<a> --time=<limit> --nodes=1 --gres=gpu:<n> --cpus-per-task=<c> --mem=<m> --output=<dir>/job.out --error=<dir>/job.err --wrap=<bootstrap>`.
  - `--parsable` prints `<jobid>[;<cluster>]`.
  - GPUs are requested untyped, `--gres=gpu:<n>`, because Helios's GH200 GRES carries no type and Sano refuses the typed form (spike S5, PR #19).
  - The account comes from the Provider's settings. `<dir>` is the Allocation's directory under `stagingDir`.
- **The queue listing:** `squeue --me --json`, and `squeue --me --json --jobs=<id>` for one job.
  - Results are filtered by exact name or ID on our side, since `squeue --json` does not promise to apply every filter (S4), and sorted by job ID.
  - "Not listed" is an empty `jobs` list with exit 0, or exit 1 with "Invalid job id specified" (S7's capture). Any other failure is an error.
- **Accounting:** `sacct --json -X -D --name=<tag> -S now-<seconds> -E now`, and `sacct --json -X -D -j <id>` for one job.
  - `-S` is always given, as a relative time. Without it `sacct` starts at midnight, and the relative form avoids the login session's time zone (S4).
  - `-D` keeps earlier executions, which show a requeue.
  - `<seconds>` reaches back to the Allocation's creation plus one hour.
- **JSON fields,** per data_parser version. The v0.0.42 table is checked against S7's real captures on Helios. The v0.0.40 table follows Slurm's source, as S4's fake does; Athena's real v0.0.40 documents were not kept, so it is checked there before alpha (Graduation criteria).
  - **queue listing:** `job_id`, `name`, `job_state` (a list: base state plus flags), `state_reason`, `hold`, `priority`, `restart_cnt`, `partition`, `time_limit`, `tres_req_str`, and `submit_time`, `start_time` and `end_time` as `{set, infinite, number}` triples in epoch seconds;
  - **accounting:** `job_id`, `name`, `state.current` (a list) and `state.reason`, and `time.submission`, `time.start` and `time.end` as epoch integers; for accounting (HBEP-8), `time.elapsed` in seconds and `tres.allocated`, of which the entry of type `cpu` and the entry of type `gres` named `gpu`, from every record `-D` returns. `restart_cnt` exists only in v0.0.42; on v0.0.40 an earlier execution shows as a record with the `REQUEUED` state.
  - **Allocated time** (HBEP-8, Provider accounting). The Allocation's `status.atProvider.accounting` (proposed names) holds `allocatedCPUs` and `allocatedGPUs`, from the accounting read made once after the job is first seen started, and `elapsedSeconds` and `readTime`. While the job runs, elapsed time runs from the scheduler's start time; once it has ended, it is the queue listing's `end_time` minus `start_time` when the final state came from there, else accounting's `time.elapsed`. A requeued job's hours are unknown (HBEP-8). A pending Allocation has none: unknown. The plugin makes one accounting read per execution, after it is first seen started, retried on failure, and none at delete; a read error that persists at delete is bounded by `lostGrace`, like a missing read. The numbers are allocated, not billed.
  - Each reply's `meta.plugin.data_parser`, after its `data_parser/` prefix is stripped, must equal the profile's `jsonDataParser`. Helios's real replies carry `data_parser/v0.0.42` (`spikes/s7/slurm-json-real.md`), which matches a profile value of `v0.0.42`. Otherwise the reply is not interpreted, the read is unknown, and the Provider gets the condition `DialectMismatch`.
- **Held:** `hold` true, or a set `priority` of 0, or a `state_reason` that starts with `JobHeld`. `JobHeldUser` is a user hold, which the job's user may lift. `JobHeldAdmin` is an admin hold.
- **Requeue:** `restart_cnt` above 0, or a `REQUEUED` record in accounting.
- **Times:** every epoch value is converted to UTC.
  - While a job is RUNNING, the queue listing's `end_time` is its projected end, start plus the time limit (S7's capture). That is the hard end, re-read on every observe, so a center's extension shows up without being planned for (HBEP-3).
  - For a pending job, `start_time` is an estimate, recorded as advisory and never planned on.
  - The profile's `timeZone` is used only to print diagnostics.
- **Read-back:** the job's settings (partition, time limit and GPU count) are compared with the request on the first observe after the job is established, and only then. Its name is compared with the tag on every observe (science-agent decision, 31 Oct 2026).
  - A name that differs refuses the Allocation, with phase `Refused` and reason `JobRenamed`, and cancels the job. Because the name is checked on every observe, a cancel that failed is retried.
  - A settings difference sets the condition `SubmittedAsOther`, with reason `ReadBackMismatch`, and the lifecycle controller replaces the Allocation (HBEP-7).
  - A later change of settings is not a mismatch. An administrator's extension of the time limit shows as a later hard end, re-read on every observe (Times), and is never flagged.

### Submission

- **The Allocation's submission parameters,** immutable (HBEP-11): `partition`, `timeLimit`, `gpus`, `cpusPerTask`, `memory`, `nodes` (1 in alpha) and `agentDigest`, the SHA-256 of the allocation agent's binary for the partition's `cpuArch` (HBEP-7). The lifecycle controller sets them; the plugin adds the account, the tag and the fixed options. S4's `account` and `wrap` are not parameters: the account comes from the Provider's settings, and the batch script is the fixed bootstrap.
- **GPU sizing.** In alpha the lifecycle controller sets `gpus` to the tensor-parallel size times the pipeline-parallel size, and refuses a `data-parallel-size` above 1 with `Degraded` reason `SpecUnsupported` on the ModelDeployment, because alpha runs one engine worker per Instance (HBEP-7). Beta sizes GPUs as DP × TP × PP.
- **Refused before any call:**
  - a partition that the Provider does not allow or the profile does not list;
  - no GPU on a partition whose `gpuModel` is not `none`;
  - more GPUs than `gpusPerNode`;
  - a time limit above `maxTime`;
  - a memory request of 0, because Slurm reads `--mem=0` as the whole node (science-agent decision, 31 Oct 2026).
- **Files first.** Before claiming an attempt, the plugin ensures four files under `stagingDir`:
  - the agent's binary, at `agent/<digest>/hb-agent`, copied from the control-plane host's build of that digest;
  - frpc, the tunnel's client (HBEP-6), at `frpc/<digest>/frpc`, for the partition's `cpuArch`, copied from the control-plane host's copy, whose checksum `build/dependencies.yaml` pins (HBEP-15);
  - the Allocation's credential, at `alloc/<tag>/credential`, mode 600;
  - the launcher's dial-home certificate, at `alloc/<tag>/dial-home.crt`. It is public; frpc verifies the front with it (HBEP-6, HBEP-11).

  The credential is the token the lifecycle controller generated for the Allocation (HBEP-7). It never appears in the batch script, on a command line or in a log (HBEP-6).

  A fifth file, `alloc/<tag>/agent.json`, mode 600, tells the agent where the control-plane host is, which dial-home mode it uses (direct or ssh) and the pin of the agent endpoint's certificate, from the launcher's configuration (HBEP-11), and the resolved staging and dial-home directories (Transport). It also carries frpc's digest, and in ssh mode the dial-home listener's loopback port next to the agent endpoint's (HBEP-6; field names are the code agent's). It names a private address, so it is never in the repository.
- **The dial-home key** for the ssh mode is not staged. The operator generates the restricted key and places the pinned host key on the platform once, in `~/.hpcburst/dial-home/`, mode 600 (HBEP-7). They live outside `stagingDir`, on storage that is not purged, because Helios purges scratch files older than 30 days (spike S5, PR #19). The plugin names that directory by the resolved home directory, never by `~` (Transport). Create checks that both files exist there before any submission, and sets `DialHomeKeyMissing` when they do not (Two controllers). Whether a compute node can use them shows at the first registration: a failure there sets `DialHomeUnreachable`, the lifecycle controller's condition (open question 16).
- **The bootstrap** is the `--wrap` command. It checks the binary's SHA-256 against the digest, then runs it with the Allocation's directory as its only argument. Nothing else runs in the batch script.

### Platform profile schema

The PlatformProfile's spec has its kind, the typed fields below, and an untyped block, `profile`, which holds the drafts' `facts` groups with their envelope (value, method, source, date). No alpha code reads the untyped block. Typed fields are refused when unknown; the untyped block accepts anything.

**Typed fields that the Slurm provider and the deployment ID read.** This is HBEP-11's list.

| Field | Type and default | Use | Source in the drafts |
|---|---|---|---|
| `providerKind` | `managed` or `batch` | selects the plugin; named so, not `kind`, because the object's own kind is PlatformProfile | `kind` |
| `partitions[].name`, `.gpuModel`, `.cpuArch` | strings; `gpuModel` is `none` for a CPU partition | the accelerator class, a deployment-ID input (HBEP-3); the GPU rule at submission | `facts.partitions.<p>.arch` and `.gres`; for Athena also `facts.hardware.node_documented.accelerator`; for Helios GH200, whose GRES has no type, the center's documentation |
| `holdRelease` | bool, default false | hold mode or fallback mode (HBEP-7, open question 11); false, fallback mode, until verified | `facts.slurm.hold_release`, on S1's branch: true on Helios `plgrid` and `plgrid-gpu-gh200`, and on Athena `plgrid-gpu-a100`. Sano: not measured, so false |
| `lostGrace` | duration, 2 min | time absent from both sources before Lost | none; above the measured gap between queue and accounting, about 2 s on Helios and Athena (S7's capture). The gap has no bound while accounting is down (S4), which Lost's revisability covers |
| `inflightWindow` | duration, required for batch, no default | time an unresolved attempt is waited on; must exceed the `sbatch` time-out | none. The alpha profiles set 5 min until the `sbatch` latency over ssh is measured, before alpha (Graduation criteria) |
| `unknownBound` | duration, 10 min | in fallback mode, time before an operator is asked | none; S4's default |
| `timeZone` | IANA name | diagnostics only | `facts.time.slurm_time_zone` (Helios, Athena: Europe/Warsaw); `facts.time.scheduler_time_zone` (Sano: UTC) |
| `requeueDefault` | bool | recorded; the plugin always submits `--no-requeue` | `facts.slurm.job_requeue` (Helios, Athena: 1); `facts.slurm.requeue` (Sano) |
| `minJobAge` | duration | how long the queue listing keeps a finished job | `facts.slurm.min_job_age_s` (Helios: 10 s; Sano: 300 s); `facts.slurm.min_job_age` (Athena: 10 s) |
| `jobName.maxLength`, `jobName.pattern` | integer; RE2 pattern | the 56-character tag must fit and, when a pattern is set, match it | `facts.slurm.submit_rules` on Sano refuses the generic names `wrap` and `job`, which the tag form can never be, so Sano needs no pattern. No draft records a maximum length, so it is measured before alpha |
| `jobCommentAccounted` | bool, optional | recorded; the tag is in the job name either way (S4) | not in the drafts |
| `jsonDataParser` | `v0.0.40` or `v0.0.42` | the JSON field table; checked on every reply, against the reply's value without its `data_parser/` prefix | `facts.slurm.squeue_json` (Helios: v0.0.42; Athena, Sano: v0.0.40) |

The GPU model is a deployment-ID input, so its strings are fixed here. It is the product name without the vendor and the memory size, upper case: `GH200` (Helios `plgrid-gpu-gh200`), `A100` (Athena `plgrid-gpu-a100`). The CPU architecture is the kernel's machine name: `aarch64` or `x86_64`.

A partition enters the typed list only when `holdRelease` has been verified on it, or when its platform runs fallback mode.

**Typed fields that the lifecycle controller and the file staging read.** HBEP-11 leaves the schema to this HBEP; these fields are added to HBEP-11's list by amendment.

| Field | Type and default | Use | Source in the drafts |
|---|---|---|---|
| `partitions[].maxTime` | duration | the longest time limit | `facts.partitions.<p>.max_time` (2 days on both alpha GPU partitions) |
| `partitions[].gpusPerNode`, `.cpusPerGpu`, `.memPerGpu` | integer; integer; quantity | the most GPUs per Allocation, and CPUs and memory in proportion to its GPUs (Submission, GPU sizing) | `facts.partitions.<p>.gres`, `.cpus_per_node` and `.max_mem_per_node_mb` (Helios); `facts.hardware.node_documented.proportional_cpus_per_gpu` and `.proportional_ram_per_gpu_mb` (Athena) |
| `partitions[].imageFormat` | `sif` or `sandbox`, default `sif` | the image format Apptainer runs on the partition's compute nodes. The lifecycle controller sends it to the agent, which builds and runs the engine image in it (HBEP-7). Not a deployment-ID input: both formats run the same image digest (open question 15) | `facts.containers.apptainer_compute_nodes`, on S1's branch, from probe 2 (issue #18): `sif` on Helios GH200, `sandbox` on Athena A100 |
| `stagingDir` | string: an absolute path, or one that starts with an environment variable set on login and compute nodes | where the agent binary, Allocation directories, images and checkpoints live; a variable is resolved on the login node once per connection (Transport) | `facts.storage.scratch` (Helios, Athena: `$SCRATCH`) |
| `drainWindow` | duration, 2 h 10 min | HBEP-5's drain window, read by the front and the controller; at least the front's `maxRequestDuration` (2 h) plus `drainMargin`, owner decision of 30 Oct 2026. A ModelDeployment may only shorten it (open question 11) | none (HBEP-5) |
| `drainMargin` | duration, 10 min | the margin above a request's longest duration in the drain window. Read by the front and the controller, so both compute the same drain start: an Instance's window is `drainWindow`, or its ModelDeployment's `routing.maxRequestDuration` plus this margin when that is shorter (HBEP-5) | none (HBEP-5) |
| `handoverMargin` | duration, 30 min | the margin in HBEP-7's submission plan: a replacement is submitted at drain start minus (p90 observed wait + start-to-ready time + this margin), never before its predecessor's start | none (HBEP-7) |
| `maxIdle` | duration, optional | an Allocation with no Instance is released after this long, counted from its `idle-since` annotation, even when its retention policy says keep (HBEP-7, Retention) | `facts.login_node_policy.gpu_only` (Athena) motivates it. Athena: 30 min (HBEP-7); unset elsewhere |
| `health.probeInterval`, `.failureThreshold`, `.noProgressTimeout`, `.errorOnlyWindow`, `.kvSkewTimeout`, `.agentTimeout`, `.registrationTimeout`, `.startTimeout` | 10 s, 3, 30 min, 1 min, 2 min, 2 min, 15 min, 1 h | HBEP-7's malfunction signals; the longest time from the job's start to its agent's registration; the longest time from an Instance's own start instruction to `ready`. In alpha `noProgressTimeout` equals HBEP-5's `idleTimeout` (30 min), so a healthy prefill that the front allows is never restarted (HBEP-7, open question 6) | none; defaults, tuned in beta |
| `restart.stopGrace`, `.attemptsPerRung`, `.window`, `.backoff` | 10 s, 2, 30 min, 30 s | HBEP-7's restart ladder | none; defaults, tuned in beta |

**Draft fields that alpha reads.** Only through the typed fields above, whose values are copied from the draft paths named in their last column. Alpha code reads nothing in the untyped `profile` block.

**A managed profile** has `kind: managed` and no batch fields. The gateway's address is not in it (Provider settings).

### Provider settings

- The account, the grant, a credentials reference, the allowed partitions and the labels (owner decision, 28 Oct 2026).
- In alpha a Provider allows exactly one partition (open question 12).
- The credentials reference names a mode-600 file owned by the controller's user (HBEP-11). For a Slurm provider it is an ssh client configuration naming the login node, the user and the key file, which must be mode 600 too. For a managed provider it holds the key, the gateway's base URL, and the key's expiry date when one is known, as strict JSON (science-agent decision, 31 Oct 2026; PR #71):
  - one object with exactly the members `key`, `baseURL` and an optional `expiry`; an unknown or misspelt member is an error;
  - `baseURL` is the gateway's OpenAI-style base URL, ending in `/v1`. It is https only, with http allowed on a loopback host;
  - `expiry` is an RFC 3339 time;
  - the managed Provider's controller and the front read it with one function, so they agree on every byte (HBEP-5). A file that fails these checks, or the mode and owner check, sets the Provider condition `CredentialsInvalid`, and no call is made (HBEP-11).
- The grant is recorded in alpha but not read, since billing comes after alpha.
- There is no QoS field, and Sano's partition `gpu` requires `--qos` (spike S5, PR #19). Sano is therefore outside alpha.

### Provider status

- The age of the last successful queue read (`lastSuccessfulStatusRead`), the `Ready` condition (Operational readiness 1) and the other conditions.
- **`queueWaits`: the last 20 observed queue waits,** oldest dropped first. Each is one recorded job's wait from submission to start, taken from the scheduler's own recorded submission and start times (the queue listing's while the job is listed, its accounting record's `time.submission` and `time.start` after that), never from when the controller saw the change. Where the plugin needs the accounting record, it reads it once, after the job is first seen started; a failed read is retried on the next poll. A wait is recorded once per job: the Allocation's `status.atProvider.queueWaitRecorded` is set only after the Provider status write succeeds, so a failed write is retried at the next observe. Entries are keyed by job ID, so a repeated observe adds nothing. The lifecycle controller plans replacements from them (HBEP-7, open question 2). They live in status, not only in memory (HBEP-11), so a restarted controller plans from the same waits.
- For a managed Provider, `catalog` and `lastCatalogRead` (Managed provider in alpha).
- **Writers** (science-agent decision, 31 Oct 2026). The Provider's controller writes Provider status under the field manager `hpcburst-provider`, and keeps every condition it does not own. The lifecycle controller owns only the condition `DialHomeUnreachable` on a Provider, which it applies by server-side apply under its field manager, `hpcburst-lifecycle` (open question 16; HBEP-11). Neither removes the other's.

### Managed provider in alpha

- **A read-only catalog.** The managed Provider's controller lists the catalog (`GET /v1/models` with the provider's key) at most once a minute.
- **The catalog lives in the Provider's status** in alpha, as `catalog`, with `lastCatalogRead` the time of the last successful read: one entry per model the gateway has listed, with its name, any revision it reports, its managed deployment ID (HBEP-3, Deployment ID), and whether it is listed now. An entry that leaves the listing stays, marked unlisted, until the catalog's bound needs room (science-agent decision, 31 Oct 2026; PR #85):
  - **The bound** is 1,024 entries. When a read would exceed it, unlisted entries are pruned, the oldest last-listed time (the entry's `lastListedTime`) first, then by name. A listed entry is never pruned to make room.
  - **An entry that cannot be held,** such as an invalid name or one longer than the Provider status allows (253 characters for a name, 256 for a revision), is skipped, with an event naming it, raised when the entry is first skipped and then at most once an hour while it stays skipped (HBEP-8, Repeating conditions). The rest is recorded, and the read still counts as successful.
  - **If listed entries alone exceed the bound,** which LLMLab is not expected to reach, the first 1,024 in name order are kept, and a `CatalogTruncated` event is raised when the cut starts and then at most once an hour until they fit. `Ready` stays True, because the read succeeded.
  - A ModelDeployment whose catalog name was pruned shows `CatalogEntryUnlisted`, as for any unlisted entry.
  - **Events** carry HBEP-8's type in `action`: `failing` for `FileRefused`, `NoCredentialsFile`, `KeyRejected`, `Expired` and `ReadsFailing`; `degraded` for `ExpiresWithinADay`, `CatalogEntrySkipped` and `CatalogTruncated` (HBEP-8, Reasons in alpha).
- **Readiness.** A managed Provider's `Ready` follows the batch rule (Operational readiness 1): each successful catalog read writes `lastSuccessfulStatusRead`, and `Ready` goes False after 5 min without one, about five failed reads (science-agent decision, 31 Oct 2026; PR #85). In alpha it gates nothing: the front and a managed-source ModelDeployment still go by the last successful read and `CredentialsRejected` (HBEP-5, HBEP-7). A stale catalog is shown, not acted on; if the gateway itself is down, the front's own failed requests and pause handle it.
  - HBEP-3 models each catalog entry as an Allocation. Alpha keeps the entries in the Provider's status instead, because nothing in alpha starts, releases or repurposes them. Catalog entries as Allocation objects, through Crossplane's Observe-only import, come after alpha.
- **What LLMLab's catalog reports** (the real read of 31 Oct 2026, issue #69, [comment 6547016114](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/69#issuecomment-6547016114)):
  - 23 models;
  - no revision field, so a managed deployment ID never carries a revision for LLMLab (HBEP-3, Deployment ID);
  - no activity flag: `shutdown_date` is null on every entry, and nothing else in an entry names a state. An inactive model therefore shows only as LLMLab's 503 at serving, which the front passes to the client (HBEP-5).
- **The lifecycle controller never acts on a managed provider.** The front routes to the listed models of attached providers. Only a ModelDeployment with a managed source attaches a managed Provider, and a served one ignores it even when labels match (HBEP-5).
- **Keys.** From 24 h before the `expiry` in the credentials file, the Provider has the condition `CredentialsExpiring`: False with reason `NotExpiring` while the expiry is more than 24 h away or the file names none, True with `ExpiresWithinADay` within 24 h of it, and True with `Expired` after it. The file is read again on every pass, so rotating to a new key clears an earlier `Expired` at the next read (science-agent decision, 31 Oct 2026; PR #85). A rejected key (401 or 403) sets `CredentialsRejected`. A credentials file that fails its checks sets `CredentialsInvalid` (Provider settings). These are the key-expiry warnings of open question 10.
- **What a managed-source ModelDeployment shows** (HBEP-7, Reconcile loop, step 1). `Progressing` is false with reason `ManagedSource`. `Ready` follows the catalog entry, and is false with reason `CatalogEntryUnlisted`, `CredentialsRejected` or `ProviderNotOnPlatform`.
- **Not needed for alpha:** the model state that the listing does not show (LLMLab's has no activity flag), error mapping, revisions and limits, and the gateway-behaviour fields. HBEP-5 configures the front for the no-byte cut and the stalls.

### Known-issues register

The register holds upstream bugs tied to versions. Each entry has one regression test and a workaround expressed as configuration. A platform profile pins the versions it uses, and an entry's workaround applies only within its version range. Alpha reads no register: its format waits for beta (open question 6), and alpha's pins are the ModelDeployment's image digest and `build/dependencies.yaml` (HBEP-15).

### Test plan

- **S4's suite, ported to the plugin, against the fake Slurm.** It covers every scenario S4 ran, and the fake gains:
  - the real v0.0.40 and v0.0.42 shapes from S7's captures;
  - both "not listed" shapes, and Helios' three phases after a job ends (listed, empty list, then the error);
  - a reply whose data_parser differs from the profile's, and one with the `data_parser/v0.0.42` value of S7's real capture, which matches a `v0.0.42` profile;
  - a job name the fake shortens.
- **Batch fake behaviours:**
  - swinging estimates;
  - freed slots taken at once;
  - a requeue that reruns a job under the same job ID;
  - a finished job that leaves the queue listing seconds after it ends;
  - local-time text output across a daylight-saving change, which no code reads;
  - a one-job-per-user partition (a Rejected outcome, then backoff);
  - a GPU-only platform;
  - a login throttle (master failures, then reconnect).
- **Managed fake behaviours:** a no-byte cut, periodic stalls, models going inactive, ambiguous errors, key expiry and request rewriting. The alpha catalog loop is tested against listing changes and a rejected key.
- **Two providers on one fake platform,** with separate accounts: their Allocations, files and credentials never mix, and their ssh masters are separate.
- **Final state and GPU rules.** A job purged from the queue listing is reported from accounting as ended, never as missing. A submission without GPUs to a GPU partition, or to an unlisted partition, is refused before any call, and so is one with a memory request of 0.
- **Failed reads are not absence.** With the transport failing, an Allocation is never reported Lost, however long the failure lasts.
- **Read-back.** A job established with another time limit sets `SubmittedAsOther`; one whose name came back shortened is refused and cancelled. A time limit raised by the fake after the first observe moves the hard end and sets no condition. A job renamed after the first observe is refused at the next observe, and a cancel that fails is retried at the one after.
- **Time.** Hard ends parsed from epoch fields equal the expected UTC instants across a daylight-saving change.
- **Files.** `EnsureFile` places a file once, and a second call does nothing. The credential's content never reaches the fake's argv log.
- **The staging directory.** A `stagingDir` of `$SCRATCH` is resolved once per master connection to the fake's absolute path, and only that path appears in commands, the bootstrap and `agent.json`. An unset variable sets `PathUnresolved`, and no file call is made. An unset `HOME` sets `PathUnresolved`, and no `sbatch` runs.
- **The dial-home key.** In ssh mode, with a key file missing from `~/.hpcburst/dial-home/`, Create sets `DialHomeKeyMissing` and no `sbatch` runs. A Provider status write keeps a `DialHomeUnreachable` condition that the lifecycle controller applied, unchanged.
- **Managed credentials.** A file with an unknown member, a `baseURL` over http to a host that is not loopback, or an `expiry` that is not RFC 3339 sets `CredentialsInvalid`, and no catalog read is made. `CredentialsExpiring` is set from 24 h before the expiry, and not a minute earlier; a rotation to a file with no `expiry` sets it False with `NotExpiring` at the next read. A catalog double that lists more than 1,024 names over time keeps every listed name and prunes the oldest unlisted first; one name that cannot be held is skipped with an event while the rest are recorded. A double that starts failing turns the managed Provider's `Ready` False after 5 min, while its ModelDeployments' `Ready` still follows the last successful read. A double that lists more than 1,024 names at once keeps the first 1,024 in name order, raises `CatalogTruncated` at its first read and not again within the hour, and leaves `Ready` True.
- **The hold lift's re-read** (tracked in #93). A job that the fake requeues, or starts, between the reconciler's first read and its re-read is not released: the re-read under the per-job lock sees a restart count above 0 or a start, and no `scontrol release` reaches the fake's log. A job whose marker another lifter wrote in that gap is not released either. The known risk's interleaving runs too, with a stall injected between the re-read and its release: leader A re-reads and stalls; leader B releases the job; it starts; the fake requeues it with a hold, as an administrator would; A resumes and releases that later hold. The test pins this as the accepted residual, so a change in either direction shows.
- **An interrupted create.** The controller is killed between the claim and the job-ID record: after restart the reconciler reaches Observe, because it runs with `WithDeterministicExternalName(true)`, and finds the job by tag.
- **Queue waits.** Each started job adds one wait, from the scheduler's recorded submit and start times (the queue listing's, or the accounting record's after the job has left it); a repeated observe adds none; a 21st wait drops the oldest; the waits survive a controller restart. A Provider status write that fails leaves `queueWaitRecorded` false, and the next observe adds the wait exactly once.
- **One regression test per register entry,** once the register exists. Once CI exists, an entry without a test fails it.
- **A profile is never verified against values derived from itself.** Writing a profile from a probe's output and then checking the probe against that profile proves nothing; expectations come from documentation or an independent measurement. Expected performance likewise never comes from the Instances that qualification judges. The fake's JSON is checked against S7's real captures, not against the parser it feeds.
- **Conformance probes** against real providers stay cheap, read-only where possible, and rate-limited.

### Compatibility and upgrades

- The platform-profile schema, the provider-settings schema and the core interface are versioned with the kinds, at `v1alpha1` (HBEP-11). Unknown typed fields are refused; the untyped `profile` block takes any field.
- Version pins change by PR to the platform profile. A workaround switches off when the pin passes its fixed-in version, and its regression test stays.
- A Slurm upgrade that changes the data_parser version stops interpretation and raises `DialectMismatch`. The fix is a profile change, plus a field table if the version is new.
- A plugin upgrade keeps every annotation of the Allocation's record (HBEP-11), so a newer plugin adopts what an older one submitted.
- Client compatibility with LLMLab is HBEP-5's concern.

### Security

- Provider credentials (cluster keys, gateway keys) are never in platform profiles, on command lines or in logs. Provider settings hold only a reference to them. For alpha, HBEP-11 keeps them in a mode-600 file on the control-plane host, or in a Secret in the team option.
- An Allocation's credential reaches the platform only through `EnsureFile`, over the ssh connection's standard input, into a mode-600 file.
- A plugin acts with the user's credentials and can spend their grant, so irreversible actions go through approvals (HBEP-10). In alpha the lifecycle controller takes no action outside the spec (HBEP-7, open question 8).
- Platform profiles hold no account names, grant names, host names or addresses. Accounts and grants are provider settings (open question 8).
- Values placed into commands are checked against a safe pattern first (Transport).

### Graduation criteria

- **Alpha:**
  - the Slurm plugin passes the Test plan's fake-Slurm suite, the ported S4 scenarios included;
  - on each alpha platform, Helios and Athena, one Allocation is submitted held, its hold lifted once, read back, observed Running with its hard end in UTC, cancelled by deleting it, and then found ended in accounting after it has left the queue listing;
  - the `sbatch` latency over ssh is measured on each alpha platform, and each profile's `inflightWindow` exceeds the worst value seen;
  - a 56-character job name comes back unchanged on each alpha partition;
  - the LLMLab provider lists its catalog into its status, with managed IDs that match HBEP-3's managed golden vector.
- **Beta:**
  - hedging across two Slurm platforms at once (HBEP-7);
  - the register's format (open question 6), profile probes (open question 7), the remaining profile schema (open question 3), and a connection budget from measured limits (open question 17);
  - the partition label decided (open question 12).
- **Stable:** every register entry's regression test runs in CI, and the profile schema has reached a stable API version.

## Operational readiness

1. **Working:** per provider, the age of the last successful queue read is the component signal, shown in the Provider's status, with a `Ready` condition. An Allocation submitted and seen Running is the end-to-end signal. A growing read age, `DialectMismatch`, or a submission never seen in the queue listing says it is not working.
2. **Restart:** a plugin re-reads its Allocations and their record annotations from the control plane, and finds their jobs by tag. It holds nothing in local files except the ssh master's socket, which it recreates.
3. **Silent failure:** a submission that succeeds but runs with other settings or another name; a failed read taken as absence; a reply in a changed JSON version. The read-back, the rule that only successful reads count towards `lostGrace`, and the data_parser check show the difference.
4. **Provider limits:** recorded per platform profile: time limits, MinJobAge, requeue defaults, per-user job limits, job-name rules, and the GPU-only rule. Login throttles are met with one master per login node and a cached queue listing.
5. **Capacity and exhaustion:** a per-user job limit or a used-up grant makes `sbatch` refuse. The outcome is Rejected with Slurm's reason, and the attempt waits for backoff. Running jobs keep running to their hard ends. What each center does at grant exhaustion is not measured; it stays a profile fact to record.
6. **Idle cost:** one ssh master per login node and credential, one queue listing per Provider per 30 s, and one catalog read per minute per managed Provider. No allocation is held while idle unless a retention policy keeps it (HBEP-7).
7. **Rollback:** disabling a provider stops new submissions. Its running allocations keep serving until their deployments hand over, because no instance is released before its replacement serves while its deployment keeps its desired capacity (HBEP-3). An instance whose replacement is late still drains before its lease end. Deleting an Allocation cancels every job with its tag. Profile changes revert by PR.

## Drawbacks

- Profiles need upkeep, and many facts are learned only by failure.
- The register is maintenance work that grows with every engine and router version.
- The command line over ssh is slower than a REST API, and each poll costs a login-node command.

## Alternatives

- **Per-site code with no shared core.** Rejected: every lifecycle rule would be written once per site.
- **One profile per provider, with account data inside.** Rejected: two grants on one platform would copy every platform fact, and account data would sit in files meant to be public.
- **Kubernetes-native offloading** (interLink and other virtual-kubelet offloading, Slinky / slurm-bridge). The evaluation (28 Oct 2026) found that they hide the lease's hard end and start estimate. Models run in provider jobs, not pods (HBEP-3, HBEP-11).
- **Contingencies off by default, enabled when needed.** Rejected: LLMLab keeps the same kinds of characteristics, so the contingencies stay on.
- **Workarounds as code patches.** Rejected: workarounds are configuration, so they can be switched off when the bug is fixed.
- **slurmrestd or FirecREST as the alpha transport.** Not chosen: no center advertises either, and JWT is off on all three platforms (S5). They stay optional transports (open question 13).
- **The tag in `--comment`.** Rejected (S4): accounting keeps the comment only where a site enables it, so the tag would vanish once the job leaves the queue listing.
- **Parsing text output with `SLURM_TIME_FORMAT`.** Not needed: the JSON gives epoch times on every platform, and parsing text needs the session's zone.

## Prior art and reuse

The Slurm provider plugin grows from www-2027's per-cluster ssh over one shared master connection, its job listing converted to UTC, and its watchers for state changes, moved start times and grant use, while the platform profiles and the known-issues register grow from its handbook of standing rules and gotchas.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| ssh, multiplexed | [OpenSSH](https://www.openssh.com) client with ControlMaster | BSD-style | Dependency: the alpha transport, one master per login node and credential; the only transport spike S5 (PR #19) found working |
| crossplane-runtime | [crossplane/crossplane-runtime](https://github.com/crossplane/crossplane-runtime) | Apache-2.0 | Dependency, from spike S4 (PR #32; HBEP-3, HBEP-11): the managed reconciler under each Slurm Provider, with our own dialect layer; after alpha, Observe-only import for managed catalog entries. Not Crossplane core or compositions |
| controller-runtime | [kubernetes-sigs/controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) | Apache-2.0 | Dependency (HBEP-11): one manager and `Lease` per Provider |
| FirecREST v2; slurmrestd through Slinky slurm-client | [eth-cscs/firecrest-v2](https://github.com/eth-cscs/firecrest-v2); [SlinkyProject/slurm-client](https://github.com/SlinkyProject/slurm-client) | BSD-3-Clause; Apache-2.0 | Optional transports, after alpha, where a center offers them; none is verified on Helios, Athena or Sano (open question 13) |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| A core object plus a per-platform kind; contract versions in CRD labels and release metadata | Cluster API provider contracts ([contracts](https://cluster-api.sigs.k8s.io/developer/providers/contracts/infra-machine)) | The common core with platform profiles; open question 5 |
| Typed errors, plus a "queued" state never mapped to insufficient capacity | Karpenter ([kubernetes-sigs/karpenter](https://github.com/kubernetes-sigs/karpenter)) | The outcomes and phases of the core operations |
| Management policies without Update; deterministic external names; Observe-only import | Crossplane ([crossplane/crossplane](https://github.com/crossplane/crossplane)) | A submitted job is never edited; jobs are found by tag; managed catalog entries as Allocations, after alpha |
| Queue-while-loading: a request for an unloaded model waits while a job starts | ALCF FIRST ([argonne-lcf/FIRST](https://github.com/argonne-lcf/FIRST)) | A managed-provider behaviour (cold-model queueing) for the profile, after alpha |
| One cached status channel per cluster | ALCF FIRST (qstat cache) | One cached queue listing per Provider |
| Pause a provider after repeated failures, with stepped backoff | HyperQueue autoalloc ([config.rs](https://raw.githubusercontent.com/It4innovations/hyperqueue/v0.27.0/crates/hyperqueue/src/server/autoalloc/config.rs)) | Contingency parameters per platform |
| `--no-requeue`; work as `srun --overlap` steps inside a held allocation; a staged TERM, then cancel; a table of unsupported features with reasons | SkyPilot's Slurm provisioner ([skypilot-org/skypilot](https://github.com/skypilot-org/skypilot)) | The requeue rule; capability flags |
| Epoch times from `squeue` and `sacct` | FirecREST v2 ([squeue_command.py](https://github.com/eth-cscs/firecrest-v2/blob/master/src/lib/scheduler_clients/slurm/cli_commands/squeue_command.py)) | Read epoch times, never parse local text |
| A two-phase, held submission with an explicit Unknown outcome, resolved by tag | Spike S4 (PR #32), against Slurm's job_mgr.c | Submit, LiftHold and FindByTag |

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| Submission rules differ per cluster and are learned by failure. | L-1.16 | Submission rules are platform-profile data, checked before any call. |
| Schedulers print local time, so end times came out wrong. | L-1.19 | Epoch times from the JSON, converted to UTC; no text time is parsed. |
| Start estimates swing by hours and flip to unknown. | L-1.9 | Estimates are advisory; decisions act on actual starts. |
| A freed slot goes to another user at once. | L-1.2 | The core reports starts promptly, so HBEP-7 can release only after a replacement serves. |
| Cancelling a parent starts its pending follow-on at once. | L-1.20 | Alpha submits no dependencies; a plugin that adds them cancels dependents before their parent. |
| A dependency offset counts from the parent's start, not its end. | L-1.8 | Dependency semantics are platform-profile data; alpha uses none. |
| Users cannot extend a job's time limit. | L-1.6 | The hard end is re-read on every observe, never planned for. |
| A running job's account cannot be changed. | L-1.7 | The account belongs to provider settings and is fixed at submission. |
| A fast-start partition allows one job per user, pending jobs included. | L-1.14 | Per-user job limits are platform-profile data; a refusal is Rejected, with backoff. |
| The time limit decides how long a job queues. | L-1.12 | Limits and their queueing effect are platform-profile data. |
| An in-place edit left pending jobs unschedulable. | L-1.5 | A submitted job is never edited, and every job is read back once established. |
| A managed gateway cuts replies that send no byte for a fixed time. | L-6.1 | A managed-gateway profile field, after alpha; HBEP-5 handles it in the front. |
| A managed gateway stalls periodically. | L-6.2 | A managed-gateway profile field: the stall pattern. |
| A managed gateway's shared capacity binds under load. | L-6.3 | The gateway's concurrency ceiling is profile data (readiness question 5). |
| Gateway keys expire mid-run. | L-6.4 | A key about to expire, or rejected, sets a condition on the Provider; it is not a lease end (HBEP-3). |
| Models go inactive unannounced, and errors for different causes look alike. | L-6.5 | The managed provider reports model state and returns a defined error, after alpha; alpha tracks the listing, which has no activity flag on LLMLab, so an inactive model shows as LLMLab's 503 at serving. |
| A managed gateway rewrites requests. | L-6.6 | A capability flag: rewrites requests. |
| Center-run endpoints hide their limits and revisions. | L-6.8 | A capability flag: exposes revisions and limits. |
| A router's default total deadline cut long streams without a log line. | L-4.1 | A known-issues entry with a version range, a test and a workaround as configuration. |
| An engine's keep-alive was shorter than a router's connection reuse. | L-2.5 | A known-issues entry. |
| A compile on an unseen shape ran past the engine's step limit. | L-2.8 | A known-issues entry. |
| A kernel build failed on nodes without a matching toolkit. | L-2.2 | A known-issues entry, with the environment in the profile. |
| Host software differs per cluster. | L-2.13 | Environment fields in the platform profile; the container runtime is an alpha precondition, and `imageFormat` records the image format each partition runs. |
| Filesystems behave differently per cluster. | L-12.5 | Environment fields in the platform profile; `stagingDir` per platform. |
| Parallel logins tripped a login node's connection limit, and tunnels on the shared master broke kill-by-PID. | L-7.3 | One master per login node and credential, with no forwards on it, and a cached queue listing. |
| A partly applied cancel was reported as refused. | L-11.7 | Results are reported per job, never as one status. |
| The center's accounting tool counted only finished jobs. | L-13.1 | Usage from the billing formula and each allocation's actual running time. |
| Billing formulas differ per cluster. | L-13.2 | The billing formula is platform-profile data. |
| Behaviour at grant exhaustion was unknown until it happened. | L-13.4 | A platform-profile field: behaviour at grant exhaustion. |

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #4.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).
- 2026-10-30: Prior art and reuse filled from the state-of-the-art evaluation; spike S5 facts carried in (HBEP-4, HBEP-6, HBEP-7 only).
- 2026-10-30: aligned with HBEP-3 and HBEP-11 becoming implementable: requeue, the job tag, the profile and provider kinds, a managed allocation's hard end, and where credentials live.
- 2026-10-30: the alpha typed profile fields (accelerator class per partition, `holdRelease`, `lostGrace`, the in-flight window, the Unknown bound) and held submission carried in from HBEP-11 and HBEP-7.
- 2026-10-30: implementable. Design completed for alpha: the two controllers, the core operations as an interface, the ssh transport, the Slurm dialect (S4's protocol, S7's real JSON, S1's hold check), submission and its files, the typed profile fields with their draft sources, provider settings, and the read-only LLMLab catalog. Every open question is decided, not needed for alpha, or open for beta; question 17 added.
- 2026-10-30: amendment with HBEP-5 becoming implementable: `drainWindow` defaults to 2 h 10 min (owner decision); `drainMargin` is added; open question 11 names the drain-window override; only a managed-source ModelDeployment attaches a managed Provider; `health.noProgressTimeout` defaults to 30 min.
- 2026-10-31: amendment from the merged contracts (PR #71) and spike S1's probe 2 (issue #18): `partitions[].imageFormat`, which puts Athena in alpha; the two dial-home conditions and their writers; the field managers; the Provider's `Lease`; Provider status fields; the managed credentials file; the read-back on first observe; memory 0 refused; `PathUnresolved`.
- 2026-10-31: amendment with HBEP-6 becoming implementable: the plugin stages frpc and the launcher's dial-home certificate, and `agent.json` carries frpc's digest and the dial-home listener's port.
- 2026-10-31: the managed catalog's bound, readiness and expiry rules (science-agent decisions while building PR #85).
- 2026-10-31: amendment from the science agent's rulings: the hold lift's re-read and its remaining window as a known risk (#76, #93); what LLMLab's catalog reports, from the real read (issue #69).
- 2026-10-31: amendment with HBEP-8 becoming implementable, from the review round of PR #105: accounting's `time.elapsed` and `tres.allocated`, the Allocation's `status.atProvider.accounting`, the accounting record before the finalizer goes, and the managed catalog's events typed and raised at most once an hour while a condition lasts.
- 2026-10-31: accounting reads and requeued jobs' hours, from building #113 (HBEP-8's amendment of the same day).

## Amendments

- **2026-10-30: the drain window and the no-progress time-out.** Made with HBEP-5 becoming implementable, and from the review of PR #63.
  - `drainWindow` defaults to 2 h 10 min, the front's `maxRequestDuration` of 2 h plus the margin. The 30 min default was a placeholder until HBEP-5 set it (owner decision, 30 Oct 2026).
  - `drainMargin` (10 min) is a typed field next to `drainWindow`, read by the front and the lifecycle controller, so both compute the same drain start (HBEP-5, open question 12).
  - Open question 11 names a second override: a ModelDeployment shortens its Instances' drain window through `routing.maxRequestDuration` (HBEP-5).
  - Only a ModelDeployment with a managed source attaches a managed Provider; a served one ignores it, even when labels match (HBEP-5).
  - `health.noProgressTimeout` defaults to 30 min, equal to HBEP-5's `idleTimeout`. At 2 min, HBEP-7's watchdog would have restarted a worker in a healthy prefill that the front allows, such as S2's first token after 1,100 s. A signal that tells prefill from a stall is HBEP-7's, open for beta (HBEP-7, open question 6).
- **2026-10-31: decisions from building the contracts, and spike S1's probe 2.** Science-agent decisions of 31 Oct 2026, made while the contracts were built (PR #71), and the measurements of probe 2 (issue #18, comment 6542078107). The names are those on `main`.
  - **`partitions[].imageFormat`,** `sif` or `sandbox` (default `sif`), is a typed profile field. Helios GH200 runs Apptainer 1.5.4 with a SIF. Athena A100's Apptainer 1.4.5 cannot run a SIF, but runs a sandbox directory built from the same image digest, so the deployment ID does not change. User namespaces are off on both, and the home directory is readable from compute nodes on both. Open questions 15 and 16 record the facts. Athena is in alpha; before, the Apptainer precondition was unmeasured there.
  - **Two dial-home conditions, with different writers** (open question 16). `DialHomeKeyMissing` is the Provider's controller's, at Create, before any submission. `DialHomeUnreachable` is the lifecycle controller's, when an Instance's registration through the tunnel fails. Before, a failed registration set `DialHomeKeyMissing`.
  - **Field managers.** `hpcburst-provider` writes Provider status; `hpcburst-lifecycle` owns only the `DialHomeUnreachable` entry, by server-side apply.
  - **The `Lease`.** Each Provider's controller has its own leader `Lease`, `provider-<name>`, in the namespace `hpcburst-system`, which the launcher creates.
  - **Provider status** holds `queueWaits`, `catalog` and `lastCatalogRead`. A queue wait is recorded once per job, after the status write succeeds (`queueWaitRecorded`).
  - **The managed credentials file** is strict JSON with exactly `key`, `baseURL` and an optional `expiry`. `baseURL` ends in `/v1`, over https, or http on loopback; `expiry` is RFC 3339. `CredentialsExpiring` applies from 24 h before the expiry, and a file that fails the checks sets `CredentialsInvalid`.
  - **Read-back** compares the job's settings on the first observe only, and its name on every observe. An administrator's extension of the time limit is not flagged.
  - **A memory request of 0 is refused** before any call, because Slurm reads `--mem=0` as the whole node.
  - **`PathUnresolved`** is the Provider condition for a `HOME` or `stagingDir` variable that does not resolve. Before, the text named only "a condition".
- **2026-10-31: staging the tunnel's files** (HBEP-6 becoming implementable; science-agent decision, 31 Oct 2026).
  - **Files first** (Submission) gains frpc, at `frpc/<digest>/frpc` for the partition's `cpuArch`, from the control-plane host's copy pinned in `build/dependencies.yaml`, and the launcher's public dial-home certificate, at `alloc/<tag>/dial-home.crt`. frpc is a pinned release binary, not part of the agent's build, so the agent cannot carry it. Before, the plugin ensured the agent's binary and the credential only.
  - **`agent.json`** also carries frpc's digest, which the agent checks before it runs frpc, and in ssh mode the dial-home listener's loopback port, the target of the second forward (HBEP-7). The field names are the code agent's.
  - **Wording.** `DialHomeUnreachable` is set when an Instance's registration over the dial-home path fails; the text said "through the tunnel", which HBEP-6 now uses for frp's serving tunnel only. The trigger is unchanged.
- **2026-10-31: the managed catalog's bound, readiness and expiry** (science-agent decisions while building PR #85).
  - **The catalog is bounded at 1,024 entries.** Unlisted entries are pruned, the oldest `lastListedTime` first, and a listed entry never is. An entry that cannot be held is skipped with an event, and listed entries beyond the bound are cut in name order with a `CatalogTruncated` event. Before, an unlisted entry stayed forever, and a full catalog stopped every read.
  - **A managed Provider's `Ready`** follows the batch staleness rule, and in alpha gates nothing: routing and a managed-source ModelDeployment's `Ready` still go by the last successful read. Before, nothing turned it False.
  - **A credentials file with no `expiry`** gives `CredentialsExpiring` False (`NotExpiring`). Before, an earlier `Expired` outlived a rotation.
- **2026-10-31: the hold lift's window, and what LLMLab's catalog reports.** Science-agent rulings of 31 Oct 2026. The names are those on `main`.
  - **The hold lift** (Core operations, `LiftHold`; Risks), ruled for #76. Immediately before `scontrol release`, the leader re-reads the Allocation's marker and the job under a per-job lock, and releases only a job held by its user (`JobHeldUser`) with restart count 0, never started and with no lift marker, so a leader that stalls before its re-read cannot free a requeued job. The lock is per process, so a known risk remains, accepted for alpha (from PR #96's review): a leader that stalls after its re-read can release a later hold, once another leader has released the job, it has started, and an administrator has requeued it with a hold. HBEP-3, HBEP-7 and HBEP-11 qualify "lifted exactly once" with it. The test, with that interleaving, is tracked in #93. Before, the text said the reconciler calls `LiftHold` at most once per Allocation, which only holds within one process.
  - **LLMLab's catalog** (Managed provider in alpha), from the real read of 31 Oct 2026 (issue #69): 23 models, no revision field, and no activity flag (`shutdown_date` null on all). So a managed deployment ID never carries a revision for LLMLab, and an inactive model shows only as a 503 at serving. Before, these were unmeasured.
- **2026-10-31: accounting and the managed catalog's events** (Two controllers, Delete; The Slurm dialect; Managed provider in alpha; Test plan). Made with HBEP-8 becoming implementable, from the review round of PR #105.
  - **Accounting reads.** The plugin also reads accounting's `time.elapsed` and `tres.allocated` (its `cpu` entry, and its `gres` entry named `gpu`), from every record `-D` returns, in the reads it already makes. Before, it read no allocated resources or elapsed time.
  - **`status.atProvider.accounting`** holds the allocated CPUs and GPUs, the elapsed seconds and the read time: allocated, not billed. Before, the Allocation's status held no accounting.
  - **The finalizer's record step.** Before the finalizer goes, the Provider controller writes the `acct-<Allocation UID>` record, waiting at most `lostGrace` for the final read and then writing what is known. Before, deletion wrote no record.
  - **The managed catalog's events** carry HBEP-8's type in `action`, and `CatalogEntrySkipped` and `CatalogTruncated` fire when the condition starts and then at most once an hour. Before, the code set `action` to `ReadCatalog`, and the text raised `CatalogTruncated` on every read.
- **2026-10-31: accounting from building #113** (science-agent rulings). One accounting read per execution, after it is first seen started, retried on failure, and none at delete; before, the text said no extra call was made. A requeued job's hours are unknown; before, executions were summed. A persistent read error at delete is bounded by `lostGrace`.
