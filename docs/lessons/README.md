# Field lessons

These lessons come from operating LLM serving on Slurm clusters for the www-2027 research project, in October 2026. The serving ran on Cyfronet's Helios and Athena and on Sano's cluster, next to PLGrid LLMLab, and was operated by people and software agents. The project's problem list was private; this catalogue restates it in general terms, without the evidence behind each item. Section 17 holds lessons from building the framework itself, which join the same catalogue (HBEP-15).

[HBEP-15](../../hbep/0015-dependencies-and-provenance/README.md) defines this catalogue and its rules.

## How to use it

- **IDs.** Each lesson has an ID `L-<section>.<item>`, for example L-4.1. The numbering is the private list's, so the IDs are stable from the start.
  - IDs are never renumbered, merged or reused.
  - A new lesson takes the next free item number in its section. A new section takes the next section number.
  - A lesson found to be wrong stays, tagged `[withdrawn]`, with one line saying why.
  - Section 16 had no item numbers in the private list. Its lessons are numbered L-16.1 to L-16.5 in the order they appeared there.
- **Citing.** An HBEP cites lessons by ID in the "Field lessons answered" table of its "Prior art and reuse" section.
- **Status tags** give the state when the project ended, and for section 17 the state on `main` when the lesson was added:
  - `[fixed]`: no longer seen. Most fixes were a written rule or a one-off script, not reusable code. Those rules are what the framework should encode.
  - `[worked around]`: avoided, while the cause stayed.
  - `[open]`: unsolved.
  - A short qualifier may follow, such as "by rule" or "security open".
  - `[out of scope]` (section 16) and `[non-LLM service; parked]` (section 9) mark lessons kept for their IDs and their general point, though the framework does not address them (HBEP-2).
- **Content rules.** This repository is public. Entries name platforms, organisations and public software, and never hosts, addresses, job IDs, accounts, grants, private paths or any results or data of the study (HBEP-1, Content rules).

Terms used below:
- **Stack:** one serving setup inside a Slurm job: engine workers (vLLM) plus a router (vllm-router).
- **Follow-on:** a job submitted to take over from a running job when it ends.
- **Front:** a balancer in front of several stacks.
- **Runner:** a client process that sends the experiment's requests. **Step-down:** a runner lowering its concurrency when latency rises.

## 1. Scheduling and allocation (Slurm on Sano, Helios and Athena)

- **L-1.1 Pinned same-node follow-ons may never start.** [worked around; mostly open]
  A follow-on pinned to its parent's node pended for days on partly allocated nodes. A handover that cancels the parent early, counting on such a follow-on, is unsafe. The workaround kept a second job ageing in the queue for each node.
- **L-1.2 A freed slot goes to another user at once.** [worked around]
  Cancelling a job before its replacement ran lost the node: other users' jobs took the freed resources within seconds, and the replacement started with no queue age and a multi-day estimate. Rule: the replacement runs before anything is released.
- **L-1.3 A priority push for one of our jobs took the nodes our pinned follow-on waited for.** [open]
  The pushed job started on those nodes, so the follow-on never ran. Priority requests and pinned placements must be planned together.
- **L-1.4 Fair share starves a heavy user on a small cluster.** [worked around]
  After a day of heavy use our fair-share priority fell near the minimum, so any other user's job outranked ours. On a cluster with two GPU nodes, a follow-on lost its freed cores to another user's CPU array and a model was down meanwhile.
- **L-1.5 Editing a pending job with `scontrol update` can make it unschedulable.** [fixed by revert; cause unknown]
  Raising a pending job's CPU count without its per-node minimum left jobs at zero priority with BadConstraints for hours. A specification that the `--test-only` dry run accepted still failed, and `scontrol` memory fields take MB.
- **L-1.6 Time limits cannot be extended, and centre staff do not act at weekends.** [open]
  The 48 h GPU time limit forces handovers. When a deadline moved, several serving jobs ended before it with no working cover. Plan every lease to its hard end.
- **L-1.7 A running job's account cannot be changed.** [worked around]
  Jobs kept billing a small GPU grant after their work moved elsewhere, and overdrew it. Mirrors were started under another grant. The grant is chosen once, at submission.
- **L-1.8 Slurm dependencies count from different moments.** [fixed]
  `after:<job>+N` counts from the parent's start, while `afterany` counts from its end, so follow-ons became eligible at the wrong time. Check which event each dependency type counts from.
- **L-1.9 Start estimates are unreliable.** [worked around]
  Estimates swung by hours and flipped to "Unknown" on each backfill pass, and actual starts came hours earlier. Act on observed starts, not on estimates.
- **L-1.10 Priority came only from a manual admin push.** [open]
  The only way to raise priority was a list e-mailed to the centre's admins. Later jobs were never pushed, nothing was pushed at weekends, and removing a dependency gave an estimate but no priority.
- **L-1.11 Other users' jobs are invisible.** [open]
  Users cannot see other users' jobs, so the job that blocks a pinned or stuck job cannot be identified.
- **L-1.12 The time limit decides queueing.** [worked around]
  On Helios' CPU partitions, jobs with limits from 1 h to 2.5 days started within a minute, while 3-day jobs waited hours. Request a limit under the threshold and renew.
- **L-1.13 Chained starts block each other.** [worked around]
  An `afterany` chain on a job that can sit in the queue blocks every job after it, and jobs that started in the same second clashed on a shared git lock. `--begin` only delays when a job becomes eligible, so jobs can still start together; serialise the shared operation (a lock held across start-up) or give each job its own checkout.
- **L-1.14 An express partition takes one job per user.** [worked around]
  A partition that starts jobs at once, for at most 12 h, allows one job per user, pending jobs included. Jobs for it must be sequenced.
- **L-1.15 A cluster was blocked for days.** [open]
  `ReqNodeNotAvail` covered nearly every A100 node on Athena for days. Pinned follow-ons also ranked below other jobs, because age counts only from eligibility.
- **L-1.16 Submission rules differ per cluster and are learned by failure.** [worked around]
  Sano requires explicit QOS, memory, CPU and job-name flags, rejects generic job names, has no home directories on compute nodes, and its login node resolves compute nodes only by full name or address. Helios requires a job-name prefix. These facts belong in a per-platform profile.
- **L-1.17 Our own jobs compete with each other.** [worked around]
  To let one job start, we held the others and released them only after it left PENDING; a timer-based release fired too early. Users may not reorder their own jobs with `scontrol top`, and lowering the nice value below the site's setting broke site rules.
- **L-1.18 A restart pinned to its old node can pend forever.** [worked around]
  It pended with no estimate. Clearing the node requirement started it within minutes.
- **L-1.19 Clusters print local time.** [fixed]
  Helios and Athena print local time, which moved from CEST to CET during the project, so wrong end times went into plans. Use `TZ=UTC` or convert explicitly.
- **L-1.20 Cancelling a parent starts its pending `afterany` follow-on at once.** [fixed by rule]
  Follow-ons must be cancelled before their parent.
- **L-1.21 The first handover design wasted allocation.** [fixed]
  It cancelled the predecessor as soon as the follow-on answered, handing back hours of every 48 h allocation. The predecessor now serves until its own end.
- **L-1.22 Follow-ons on new nodes change the URL.** [open]
  A follow-on on another node has a new address, so every client must re-point. A follow-on on the same node and port cannot start before its parent ends. Clients need a name that outlives the job.

## 2. Serve stacks: environment and start-up

- **L-2.1 A prefill/decode split produced gibberish.** [fixed]
  Under light load, a disaggregated prefill/decode deployment freed KV blocks before decode read them. Plain independent servers replaced it.
- **L-2.2 FlashInfer JIT fails on nodes without a CUDA toolkit.** [worked around]
  The A100 nodes have no CUDA toolkit, and pip's nvcc did not match the installed headers. On Athena, NVRTC also needs every pip `nvidia/*/lib` directory on the library path. FlashInfer's sampler was turned off.
- **L-2.3 vLLM and FlashInfer write under $HOME at import.** [fixed]
  On compute nodes without home directories this fails. HOME was moved to shared storage.
- **L-2.4 Port problems.** [worked around]
  The kernel's ephemeral port range took fixed ports, and ports derived from the job ID collided when two serve scripts ran in one job, with no override. Take ports from the OS, or allow an override.
- **L-2.5 vLLM's 5 s keep-alive dropped the router's reused connections.** [fixed]
  The router reused idle connections the engine had already closed. A 120 s keep-alive fixed it.
- **L-2.6 The untuned A100 prefill budget queued requests behind one long prompt.** [fixed]
  With the default batched-token budget, one long prompt delayed other requests' first token by seconds. A budget of 8192 tokens fixed it.
- **L-2.7 Start-up ran out of memory.** [fixed]
  A fused-MoE JIT ran one nvcc per core and exhausted a large node's memory, and the default safetensors prefetch loaded a checkpoint of several hundred GiB into every node's page cache. Bound compile parallelism (`MAX_JOBS`) and load lazily.
- **L-2.8 JIT compiles stall and kill engines.** [worked around]
  A Triton JIT on an unseen shape ran past vLLM's 300 s execute-model limit and killed a multi-node stack. First requests after a start were many times slower than later ones, and newly joined workers stalled. Raise the limit, and warm up with real request shapes before serving.
- **L-2.9 A multi-node rank 0 exited with code 0 after an engine time-out.** [fixed]
  The step and its holder kept waiting on the other node, so nothing restarted. Supervision must treat any exit of a required process as a failure.
- **L-2.10 Compile caches were lost with every job.** [fixed]
  They lived in node-local memory storage, so each start recompiled for about ten minutes, and cleanup killed only top-level processes. Keep caches on persistent storage, and clean up whole process trees.
- **L-2.11 Environment builds broke.** [fixed]
  URL-encoded wheel names (`%2Bcu129`) and a wrong import check broke a start, and one build took about half an hour. Build each environment once per architecture and check it before use.
- **L-2.12 In-job restarts need a fresh Apptainer image.** [fixed]
  A terminated `apptainer exec` keeps the job's temporary image locked, so a restart in the same job must use a new one.
- **L-2.13 Host Python on Helios is 3.6.** [fixed]
  Suites and runners need a pinned Apptainer image instead of host Python.

## 3. Serve stacks: lifecycle (retarget, follow-on, restart)

- **L-3.1 Follow-ons killed themselves.** [worked around]
  Under `set -e`, `squeue -j` on the ended predecessor returned 1 and aborted the follow-on seconds after it deployed, and two stacks were down unnoticed for over an hour. The same unguarded line was in several serve scripts, which stayed unchanged because identity records hashed them.
- **L-3.2 Restart helpers are topology-specific.** [fixed]
  A router-restart tool for two-node stacks, used on a one-node stack, stopped that stack's own workers, and the plain router-only tool could not reach the second node of a retargeted stack. Tools must check the topology they act on.
- **L-3.3 A SIGTERMed router can hang in graceful shutdown.** [fixed]
  A router hung for minutes after TERM. Send KILL after a short grace period.
- **L-3.4 NCCL fails in overlapping job steps.** [fixed]
  aws-ofi-nccl's Slingshot initialisation segfaulted in a TP=2 overlap step, and the retry landed on a GPU another step used. Set `NCCL_NET_PLUGIN=none` and pin `CUDA_VISIBLE_DEVICES`.
- **L-3.5 Hand-launched steps need a clean login environment.** [worked around]
  `--export=NONE,VAR=val` dropped the variable, so the wrong instance name was deployed. `--export=ALL` breaks `module load` on GH200, and a step dies with the login-node `srun` that launched it.
- **L-3.6 Shell pitfalls.** [fixed case by case]
  Under `set -e` a failing `$(…)` aborts and runs the EXIT trap; pipes lose variables; `srun` consumes the rest of `ssh bash -s` input; a backgrounded `cd X && cmd &` holds ssh open for days. Untested edits to a serve script killed a stack's first two starts.
- **L-3.7 Live serve scripts cannot be edited.** [fixed by convention]
  Identity checks hashed serve scripts and environments, so one edit held four live runs, and an older job's restart picked up a newer script. With no versioning, every change became a new file plus an announcement. Version scripts by content.
- **L-3.8 Steps are coupled.** [worked around]
  Two nodes' stacks in one step with `--kill-on-bad-exit=1` cannot restart separately, and the old task stayed stopped until the job ended. Give each independently restartable part its own step.
- **L-3.9 Converting idle stacks between models is manual.** [worked around]
  Each of about ten conversions took minutes plus per-job config files, and workers bound to loopback needed a restart to join pools.
- **L-3.10 Full stack restarts take minutes.** [worked around]
  A full restart took 2–4 min on Helios and longer on Sano, and each outage tripped the runners' step-down. Restart only the router where possible.
- **L-3.11 Failing servers were waited out instead of restarted.** [fixed by rule]
  Hours were lost until the rule "restart failing models".
- **L-3.12 KV cache exhaustion and preemption.** [worked around via routing]
  Servers stalled for minutes with dozens of preemptions each, and there was no per-server concurrency cap. One model needed a GPU memory fraction of 0.95 for a very long request.
- **L-3.13 A running bash script must not be edited in place.** [fixed by rule]
  Bash reads a script while it runs, so an edit changes what the running copy executes.

## 4. Routing, fronts and worker pools

- **L-4.1 The router silently cuts long streams.** [fixed]
  vllm-router's `--request-timeout-secs` defaults to 1,800 s and is a total deadline: it cut streamed replies with no log line, so every request over 30 min failed. It was raised to 7,200 s.
- **L-4.2 Router settings at first needed a full stack restart.** [fixed]
  Each settings change cost minutes of downtime. With router arguments in config files, a restart takes seconds.
- **L-4.3 No routing policy fits long multi-turn sessions.** [worked around]
  `round_robin` fed a saturated worker. `power_of_two` polls `/get_load`, which vLLM does not serve, so it routed at random and lost session affinity. `cache_aware` piled sessions up because its in-flight counter undercounts cut calls, and `consistent_hash` needs a session header the clients did not send.
- **L-4.4 Pile-ups keep coming back.** [open]
  One worker sat at a full KV cache with preemptions while others idled, under every policy tried. Repeated router restarts were needed.
- **L-4.5 A routing change was gated on the fingerprint alone.** [fixed by rule]
  The check confirmed identity but missed a throughput collapse. Gate routing changes on per-session throughput, prefix-cache hits and time to first token, before and after.
- **L-4.6 Requests stay on a saturated worker up to the time-out.** [open]
  The router does not say which worker answered, so failures cannot be attributed.
- **L-4.7 Only global caps.** [open]
  Concurrency, queue and rate caps are global; there is no per-worker cap.
- **L-4.8 `add_worker` and `remove_worker` are not persistent.** [fixed]
  A router restart dropped joined workers, and a five-minute gap re-pinned every session. One adder skipped routers with no extra workers. Workers are now re-added within milliseconds of a restart.
- **L-4.9 Cross-node pools are unsafe and awkward.** [worked around; security open]
  Workers had to listen on all interfaces without a key, because vllm-router's `--api-key` is command-line only and also checks clients. The router's admin API (add and remove worker) is unauthenticated, so anyone on the cluster network can call it.
- **L-4.10 Worker removals before a job ends were scheduled by hand.** [fixed]
  Removals were planned 30 min ahead, and one sweep failed silently on an ssh glitch. Drain from each lease's end automatically, and check the result.
- **L-4.11 Stale pool membership.** [fixed by hand]
  A router carried stale workers into a new role, and a pool decision went stale when the deadline moved.
- **L-4.12 A router restart leaves stale client claims.** [worked around]
  The runner skipped them until it was restarted with its reclaim option. Check for stale claims after each router restart.
- **L-4.13 The router has no model-name option.** [worked around]
  Servers had to serve both names.
- **L-4.14 Front (balancer) traps.** [worked around]
  A backend was ejected after three failures and never re-enabled; the upstream time-out was shorter than the runners'; caps fixed when a backend was added gave 503s and step-downs once workers outnumbered them. Per-backend drain times needed timed removals, and stale forwards stayed.
- **L-4.15 Long requests were cut by connection errors.** [open]
  Long requests to one model failed after 16–20 min, under the router's 30-min deadline. The cause was never found.

## 5. Capacity sizing and load

- **L-5.1 Estimates from development runs were far off.** [fixed]
  Measured KV cache per session was well above the estimate, which caused repeated step-downs overnight. Size capacity on the real workload.
- **L-5.2 Capacity differs by workload.** [worked around]
  A short-context ramp overstated long-context capacity, so each deployment needs its own ramp for each workload.
- **L-5.3 An unwarmed follow-on tripped a run's load guard.** [worked around]
  Its first pass was slow enough to trip the guard. Long warm-ups cannot run on busy workers without evicting live requests, so warm up before joining a pool.
- **L-5.4 Latency baselines come from the wrong calls.** [open]
  The runner's governor took its baseline from the first, short-context calls, so long runs almost always stepped down. The first window after a restart also inflated baselines.
- **L-5.5 A per-server threshold fired on skew, not harm.** [fixed]
  One server at high KV use while others idled triggered it. The rule moved to the router level.
- **L-5.6 Load measures don't compare across deployments.** [fixed by rule; per-token latency open]
  KV usage means different things per model (a sparse-attention model counts about double), and streams with few chunks give no per-token latency.
- **L-5.7 Output caps and request time-outs were set without measurement.** [fixed]
  Output caps cut some attempts, and the cap label missed cuts in the middle of JSON. A 600 s default time-out cut calls that were still progressing. Set limits per model from measurement, and disclose them.
- **L-5.8 No concurrency control across clients.** [open]
  Each runner checked only its own budget on the managed gateway, so two runners doubled the load. The total was held by hand.

## 6. The managed gateway (LLMLab)

- **L-6.1 The gateway cuts a request after 180 s without a byte.** [worked around]
  It returns 504, so every model must stream, and a non-streaming call must finish within 180 s.
- **L-6.2 Periodic all-model stalls.** [open: gateway side]
  A database sync every 30 min, and other periodic events, raised time to first byte many times over, more under load. They confounded every probe.
- **L-6.3 Shared capacity.** [open]
  A second API key shared the first key's GPUs, and the gateway saturated at a modest total number of concurrent sessions.
- **L-6.4 Keys expire mid-run.** [open]
  A key expired during a run, and requests failed with 401.
- **L-6.5 Models flap without warning.** [open]
  Models switch between active and inactive (503), and the model listing does not show the state. 400 errors look alike for unknown, inactive and grant-blocked models, and server-side `max_tokens` caps are hidden.
- **L-6.6 The gateway changes requests.** [worked around]
  It adds a fixed prompt segment of 10–15 tokens, allowlists body fields (`cache_salt` gives 422) and rejects `max_tokens` equal to the context window before tokenising. So a deployment behind the gateway differs from a direct one.
- **L-6.7 Streams get truncated.** [open]
  Some of one model's streamed tool-call arguments arrived cut.
- **L-6.8 Centre-run endpoints are opaque.** [worked around]
  Their limits and model revisions cannot be read. A centre-run stack the work depended on died, and only the project owner could ask for a restart. The work moved to a self-run stack.
- **L-6.9 Tool-call formats differ per model and parser.** [worked around]
  Nested arguments came back double-encoded, `<tool_call>` markup leaked, and calls landed inside the reasoning. Per-model settings handle it.

## 7. Networking, tunnels and access

- **L-7.1 Login nodes are the only way in.** [worked around]
  Compute-node listeners are reachable only through login nodes, Athena is unreachable from Helios and from our other hosts, and node-to-node ssh inside jobs is denied. Relays and forward watchdogs carried the traffic.
- **L-7.2 Too many hand-made tunnels.** [open]
  Hundreds of `ssh -N` forwards and watchdogs accumulated. Ports computed from indexes were misread, and two agents' watchdogs shared a name and PID files.
- **L-7.3 Login throttles.** [worked around]
  About twenty parallel logins tripped the clusters' sshd `MaxStartups`. The shared ControlMaster that fixed it glitched in bursts, and tunnels on the master break kill-by-PID.
- **L-7.4 A shared host's sshd stopped accepting new logins.** [worked around; no pooling]
  Clients that opened a new connection on every poll filled it for about 20 min. Pool connections.
- **L-7.5 Loopback ports on a shared host were live service tunnels.** [worked around]
  Test suites and our own builds sent requests to them, and polluted a load test. Never assume a loopback port is free or private.
- **L-7.6 The cluster key lives only in a Kubernetes Secret.** [worked around]
  It is used through a short-lived relay pod, so every cluster action depends on that pod.
- **L-7.7 Loopback relay ports clash between jobs on shared nodes.** [worked around]
  Excluding the busy nodes (`--exclude`) avoided it.

## 8. Identity, fingerprints and reproducibility

- **L-8.1 Engine fingerprints differ per site for the same checkpoint and flags.** [open: identity is per site]
  vLLM's `system_fingerprint` covers the engine configuration after start-up, so an offline tool cannot reproduce it.
- **L-8.2 Real differences hide behind one checkpoint.** [open: disclosed]
  One model ran dense attention on A100, through a local patch, and sparse attention on GH200. Another model's attention kernels differed per site, and one tensor-parallel configuration behaved differently for an unknown reason.
- **L-8.3 Checkpoints are served without `--revision`.** [open]
  Identity rested on the Hugging Face cache holding one snapshot, although one model's tokenizer, chat template and config changed upstream after its weights. Pin an immutable revision.
- **L-8.4 Proving that only one configuration served is hard.** [worked around]
  A list of admitted fingerprints let a second one pass silently, and replies without a fingerprint were never compared. The managed gateway and a self-run copy of one model used different chat templates. A scanner checked afterwards.
- **L-8.5 The route URL is part of the identity.** [worked around]
  Moving capacity meant re-pointing the forward behind the same URL.
- **L-8.6 Verification was not uniform.** [open]
  Some deployments were checked only by a chat reply, without an identity record.
- **L-8.7 Configuration had to be attributed after the fact.** [worked around]
  Backends were admitted without a hash of their route, so each turn's configuration was rebuilt from its `system_fingerprint`.

## 9. Store copies hosted on the clusters (QLever)

These lessons come from running copies of a SPARQL query service (QLever) inside GPU jobs. Non-LLM services are outside the framework, so the section is parked.

- **L-9.1 The image was built for amd64 only.** [worked around] [non-LLM service; parked]
  Copies on arm64 GH200 nodes died until an arm64 build of the same commit was added, and that build then answered differently from x86 in some cases. arm64 is used only where tested.
- **L-9.2 One query can kill a copy.** [worked around] [non-LLM service; parked]
  A single query exhausted memory even in a large allocation, and the client's automatic re-send killed the restarted copy again. The engine also aborted under concurrent load. A bigger copy did not help; restart in place.
- **L-9.3 Timed-out queries keep running.** [worked around] [non-LLM service; parked]
  After the client gave up, a query kept eight threads busy for hours while memory grew, and nothing was logged. Watch and restart.
- **L-9.4 Memory sizing.** [worked around] [non-LLM service; parked]
  Copies were killed for memory at several sizes, and copies sized just above the engine's own memory budget died near it. Resuming with more sessions raised memory use, and the copy ran out minutes later.
- **L-9.5 Copies inside GPU jobs need their own `--mem` and TMPDIR.** [fixed] [non-LLM service; parked]
  Slurm sets TMPDIR per step, and two copies that shared a scratch directory overwrote each other's index.
- **L-9.6 Service speed confounds measurements.** [worked around] [non-LLM service; parked]
  Speed differed by site and architecture, arm64 most under node contention, and only first runs measured speed because of the result cache. Measurements that crossed speed classes needed a decision on whether to repeat them.
- **L-9.7 Names, numbers and ports collided across agents.** [fixed in one script; process open] [non-LLM service; parked]
  Several agents that launched copies chose the same names, numbers and ports.
- **L-9.8 Handovers were armed by hand per job.** [open] [non-LLM service; parked]
  Most of them never fired.
- **L-9.9 Service logs are easy to misparse.** [fixed] [non-LLM service; parked]
  A thousands separator in durations made a parser drop every slow answer, and logs were in local time, so wrong percentiles reached the coordinator.
- **L-9.10 Serve-script traps.** [worked around] [non-LLM service; parked]
  An acceptance gate refused older builds, and an old EXIT trap deleted another job's endpoint file.
- **L-9.11 A client time-out does not end the work on the server.** [fixed] [non-LLM service; parked]
  A client-side limit on requests to a shared server had to hold across processes until the server's own time-out passed, not the client's. A shared gate enforced it.

## 10. Monitoring, verification and alerting

- **L-10.1 New stacks and waiting phases went unnoticed.** [fixed: generic start watcher; the rest by rule]
  Stacks served for up to an hour and a half before anyone was told, phases parked behind an allow file left GPUs idle for hours, and a paused run was never resumed.
- **L-10.2 Watchers die silently.** [worked around]
  They died with session restarts, expired without firing, kept samples in vanished temporary files, or ran from the wrong directory. A session summary listed a dead watcher as live, and reports went unposted for hours.
- **L-10.3 Untracked watchers wake nobody.** [fixed by rule]
  A watcher started with a plain `&` notified no one when it fired, and a second copy doubled the alerts.
- **L-10.4 Pipes that never deliver.** [worked around]
  `cut` block-buffers, a `grep -m1` wrapper (ugrep) did not exit, and `tail -F` missed a match, so alerts came late or never. A Python tailer replaced them.
- **L-10.5 False readings.** [fixed]
  Empty samples printed as 0, so busy routers looked idle. Reads came from the wrong node, `/get_load` was misread, and a summary line "0 aborted" matched an abort watch.
- **L-10.6 Checks that cannot see what they check.** [fixed one by one]
  A rate check read short claim leases as starts and always said OK, a load monitor counted only errors in completed turns, and a window count returned silent zeros.
- **L-10.7 Before/after comparisons were invalid.** [worked around]
  Preemption counters reset when a server restarted, and baselines lacked cache-hit rates.
- **L-10.8 Timing aliasing gave false alerts.** [fixed]
  A heartbeat sent in batches, read against a fixed reporter tick, produced false "runner down" alerts.
- **L-10.9 Configuration read once at start.** [open as design]
  Reporters and waiters read their maps only at start, so new partitions and stacks went uncounted for hours.
- **L-10.10 Stale endpoint files.** [worked around]
  A job ended by its time limit runs no exit trap and leaves its endpoint file behind, which gives false alarms and a wrong inventory.
- **L-10.11 Names stopped matching contents after retargets.** [open]
  Jobs named after one model ran another, and operators misread them.
- **L-10.12 Multi-homed nodes change address.** [open: use host names]
  A node's endpoint address changed between relaunches while the old address still answered.
- **L-10.13 Steps in one job share a log directory.** [worked around]
  The verifier printed a stale router line from another step.
- **L-10.14 Free-text state events are parsed by other tools.** [worked around]
  A router move was read as a store move, and an event about naming a copy blocked that copy. Events need types, not prose.
- **L-10.15 Moving "cold" folders broke live markers.** [fixed]
  Folders thought unused held markers that live tools read, which gave false alerts.
- **L-10.16 Deliberate kills looked like crashes.** [fixed by rule]
  A restart not written into the death-watch log was counted as an out-of-memory death. Record deliberate restarts.
- **L-10.17 No inventory of who started what.** [open]
  Nothing listed what had been started on which host or cluster, or by whom.

## 11. Process control on shared hosts

- **L-11.1 One shared Unix user for every agent session.** [open]
  Jobs, processes, ssh masters and cron entries cannot be attributed to a session.
- **L-11.2 Kill by pattern kills the wrong thing.** [fixed by rule: kill by recorded PID]
  `pgrep -f` and `pkill -f` matched the caller's own shell and killed two other agents' scans, a `pgrep`-based wait deadlocked for an hour, and `/proc` argv scans matched their own launcher chain.
- **L-11.3 Recorded PIDs can be wrong.** [fixed by rule]
  `setsid … & echo $!` records the setsid parent, and a PID found by basename belonged to another operator.
- **L-11.4 ssh quirks.** [fixed by rule]
  ssh inside a `while read` loop consumed the list, so one item was checked for hours; `ssh -f 'A & B &'` started only A; an EXIT trap that restarts a daemon hangs the caller's pipe.
- **L-11.5 Restarted tools lose the launch environment.** [fixed by rule]
  Tools restarted without the token environment stopped a runner and then failed midway, three times in one day.
- **L-11.6 Dry checks that ran for real.** [fixed by rule: explicit confirmation]
  A refusal test wrote a real hold, and a defaulted empty key turned a keyless dry run into a live run. A real action needs an explicit confirmation flag.
- **L-11.7 An interrupted command still did part of its work.** [open]
  A parallel release across three clusters was reported as rejected, but two clusters' parts had already cancelled jobs before the owner chose the scope.

## 12. Hosts and filesystems

- **L-12.1 A shared host's disk filled.** [fixed: shared gate, disk floor]
  It evicted another team's workloads on that host, and left hundreds of empty claim files that blocked work for half an hour.
- **L-12.2 Home filesystems filled on shared hosts.** [worked around]
  Review snapshots, worktrees, deployments and test temporary directories filled them almost to capacity.
- **L-12.3 A shared VM froze for half an hour on a large replay.** [fixed by rule: heavy jobs on Slurm]
  Heavy jobs belong on the cluster, not on a shared machine.
- **L-12.4 Batch work starved other work on a shared host.** [fixed: shared gate]
  Batch jobs held the load near the core count and starved a deliverable.
- **L-12.5 Filesystem quirks.** [worked around]
  Lustre read-ahead read gigabytes for a megabyte-sized request, which the admins noticed; Lustre has 1 s mtimes and a 4 MB block size. /dev/shm is noexec, the Apptainer session directory is capped at 64 MB, TMPDIR counts against `--mem`, and `fincore` reads 0 on CephFS, which only Sano has.
- **L-12.6 Deleting a deployment with `chmod -R` changed hard-linked live deployments.** [fixed by rule]
  uv deployments share files by hard link, so they became writable too, and a launcher refused to start.

## 13. Cost, grants and usage

- **L-13.1 The official usage tool counts only finished jobs.** [worked around: own forecast tools]
  It showed a fraction of the hours that `sacct` reported, because running jobs were missing.
- **L-13.2 Billing formulas differ per cluster.** [open]
  Helios bills max(cores, GB/2), Athena max(GPUs, cores/16, GB/128) and Sano CPUs, so memory drove the cost.
- **L-13.3 A forecast counted pending jobs as starting now.** [fixed]
  Pending jobs with start time "Unknown" were counted as starting now, which showed a false gap in the grant.
- **L-13.4 Behaviour at grant exhaustion was unknown until it happened.** [worked around]
  A CPU grant was overdrawn, and the cluster turned out to be lenient.
- **L-13.5 Releases and grants pull against each other.** [open trade-off]
  Releasing capacity saves hours but risks losing the node (see L-1.2), so the rule became "nothing released unless certain".
- **L-13.6 No usage metering.** [open]
  Tokens served and GPU hours were rebuilt afterwards from logs and `sacct`, and capacity sat idle as spares because starts were unpredictable.
- **L-13.7 Spend counted by file modification time was inflated.** [fixed by rule]
  Counting a day's API spend by each record's mtime, instead of each call's own timestamp, inflated the estimate.

## 14. Agent and operator coordination

- **L-14.1 Addresses change with every session restart.** [worked around: address by name]
  Messages to stale addresses were lost. After the lead session restarted, sub-agents' messages missed it for about two hours.
- **L-14.2 A peer was misidentified by its name.** [fixed by rule]
  A peer named like a tool belonged to another project.
- **L-14.3 Second-hand directives.** [fixed by rule]
  An accidental instruction to cancel many stacks was relayed as an owner decision, and a conditional go-ahead was acted on while its condition was being re-asked.
- **L-14.4 A GitHub issue used as a request channel went unwatched.** [worked around: poller, then direct messages]
  Requests waited for over an hour.
- **L-14.5 Rules changed mid-run.** [worked around]
  A deadline moved, and release rules were withdrawn and reissued, so earlier decisions had to be rechecked.
- **L-14.6 Misreports.** [fixed by rule]
  Agents reported wrong time zones, false zeros, estimated timestamps, invented hashes and an overstated grant gap. Take times from the clock and values from a command run at the time.
- **L-14.7 Restart announcements are manual.** [worked around]
  Each restart needed a message about ten minutes ahead and a "back" notice.
- **L-14.8 An auto-approval classifier blocked sub-agents.** [worked around]
  It denied their live cluster steps, later even read-only ssh, and each needed the owner's approval.
- **L-14.9 Agents fail in long operations.** [worked around]
  Context exhaustion, stale handbacks, a terminal pane limit for named teammates, and reviewer reports sent to the lead instead of the requester.

## 15. Cleanup and teardown

- **L-15.1 Cron respawners revived killed processes.** [fixed: cron removed]
  Relay loops came back the next day.
- **L-15.2 Keyword sweeps missed processes.** [worked around]
  Hundreds of tunnels and worktree advancers survived a keyword sweep, so a sweep by working directory was needed. Week-old coding-agent servers sat in deleted worktrees.
- **L-15.3 Shared hosts run processes that are not ours.** [worked around]
  A CI runner and Kubernetes ran on the same hosts, so sweeps must exclude them.
- **L-15.4 Finished experiments kept notifying the owner.** [fixed]
  Notifications continued until every watcher was swept.

## 16. Adjacent: outside the framework

These concern the clients and the research process rather than serving. HBEP-2 proposes them as out of scope.

- **L-16.1 Client-runner semantics.** [out of scope]
  Claims, leases and reclaim orphans across nodes; a replay check after a TERM stop; step-downs that persist across restarts; holds that drain and exit; one lock per output.
- **L-16.2 Checker replay nondeterminism.** [out of scope]
  Replaying a result checker was not deterministic, and a SPARQL parser was not thread-safe.
- **L-16.3 Protected-data hygiene.** [out of scope]
  Sparse worktrees still wrote into a protected directory, deploys had to come from archives, and push scans streamed the very files they guarded.
- **L-16.4 Public data release.** [out of scope]
  Generated keep trees froze into deny lists, and neutralised copies renamed strings that tools parse.
- **L-16.5 Review capacity.** [out of scope]
  Reviewer models' quotas limit review rounds, and only a reviewer's final verdict is acted on.

## 17. Building the framework

These come from building this framework, from 30 Oct 2026, not from www-2027's operation (HBEP-15: "Lessons from the framework's own operation join the same catalogue"). Their status tags give the state on `main` when each was added.

- **L-17.1 A /proc liveness check misreads a live process as a zombie during `execve`.** [fixed]
  While one thread of a process runs `execve`, its leader reads Z in /proc, though the process lives. k3s re-executes itself, and a check that counted Z as dead once left an orphan. The launcher decides liveness from a pidfd, which the kernel signals only after every thread has exited, not from a /proc scan, which can find no live thread at that moment.
- **L-17.2 A write refused for a full store can still take effect.** [fixed by rule]
  On SQLite, a create that returned "database or disk is full" left its object stored. Controllers re-read state after any write error, and never assume the write failed.
- **L-17.3 A user-level k3s can silently run root's binaries.** [fixed]
  On a machine whose system k3s runs the same release, a user-level k3s ran root's unpacked binaries from `/var/lib/rancher` instead of its own. The launcher reads `/proc/<pid>/exe` and refuses a host whose k3s runs from outside its own data directory.
- **L-17.4 A library default that the text called inert was not.** [fixed]
  crossplane's embedded `managementPolicies` default, `["*"]`, would have stalled every Allocation, because the provider registers fewer policies. The Allocation declares the fields itself, with the provider's set as an immutable default (PR #48).
- **L-17.5 The Hub's tree listing page size depends on the query.** [fixed by rule]
  A spike saw a listing cut at 50 entries. Later the Hub sent one page of 77 entries to a request with no `limit`, and 50 with a `Link` to one with `expand=true`. Send `limit=1000`, and always follow `Link`.
- **L-17.6 An approval given before a code-review round finished was used to merge the post-fix head.** [out of scope; fixed by rule in the agents' process]
  An objective check approved a head; the review round then added fix commits, and the PR merged at the fixed head on that approval. The objective check now comes after the reviews and their fixes, names the full head SHA, and any later commit voids it.
- **L-17.7 An ssh banner proves only that something answers.** [fixed by rule]
  On 30 Oct, spike S1's step 0 read an ssh banner on the ssh port at the agents' VM's address as proof that the clusters could reach the VM. On 31 Oct the host key there turned out to be another machine's: a gateway's (issue #18). Check the host key against the target's own, not just the banner. S1's kit, on its spike branch, now does it in a preflight, before any forward.
