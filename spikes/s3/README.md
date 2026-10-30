# Spike S3: host for the single-user control plane

Issue: groundnuty/plgrid-llmlab-hpc-burst#26. Decides the open question in HBEP-11: which process hosts the Kubernetes API for one user.

## Question

HBEP-11 makes the Kubernetes API (custom resources plus controllers) the control plane. For a single user the host is open between k3s with SQLite, kcp (no nodes, in-process garbage collection), kcp embedded as a Go library, and a plain kube-apiserver with etcd. Which fits a small VM, and which behaves correctly for what the HBEP relies on (owner-reference cascade, finalizers, validation, watches, upgrades)?

## Summary

| option | measured? | result against the budget below |
|---|---|---|
| plain kube-apiserver + etcd, no controller manager (`envtest-bare`) | yes | fails the e2e suite on the three cascade tests, as designed: it is the negative control (nothing garbage-collects) |
| kube-apiserver + etcd + kube-controller-manager limited to garbage collection and namespaces (`envtest-gc`) | yes | passes every line: 365 / 386 MiB, ready in 3.9 s cold, upgrade kept 103 of 103 objects |
| kcp release binary, embedded etcd (`kcp`) | yes | passes every line: 500 / 478 MiB, ready in 13.9 s cold and 8.8 s warm, upgrade kept 103 of 103 objects |
| **k3s with SQLite, rootless and agentless (`k3s`)** | **yes** | **passes every line as an ordinary user, with no root and no system change: 363 / 407 MiB, ready in 10.5 s cold and 6.0 s warm, all eleven tests including the cascade, upgrade v1.35.9 to v1.36.5 kept 103 of 103 objects. Packaged components were disabled (details below).** |
| kcp embedded as a Go library | no | not measured: the build did not get past a dependency conflict and needs more disk than the shared VM has free (details below) |

(RSS idle / RSS with 100 objects, summed over the host's processes.) The three measured hosts that garbage-collect fit the small VM with a wide margin: the host takes 0.35 to 0.49 GiB of the 4 GiB budget VM. From the idle to the loaded reading the RSS moved by −22 to +43 MiB. That reading also has the CRDs, a connected controller and, for kcp, a workspace, so it is not the cost of the 100 objects alone (see the notes under the results); it is small next to the server's own 280 to 500 MiB. What separates the hosts is shape: three processes that we assemble and pin (lightest and fastest to start, plain Kubernetes), one downloaded process that runs without root and is a 0.x project (kcp, about 90 to 135 MiB more than the assembled host and 6 to 10 s slower to start), and one downloaded process that runs without root and is the team option too (k3s, about the same idle RSS as the assembled host, 21 MiB more with objects, 3 to 7 s slower to start). Section "What this means for the HBEPs" gives the spike's reading and what is still open.

## Options and what ran

| id | what runs | versions (old for the upgrade, then new) |
|---|---|---|
| `envtest-bare` | kube-apiserver + etcd from controller-runtime's setup-envtest. No controller manager. | 1.35.0, 1.36.2 |
| `envtest-gc` | the same, plus kube-controller-manager started with only the garbage-collector and namespace controllers | 1.35.0, 1.36.2 |
| `kcp` | the kcp release binary with its embedded etcd, one process, the e2e suite running inside a child workspace | 0.32.5, 0.33.3 |
| `k3s` | the k3s release binary, `server --rootless --disable-agent`, SQLite (kine), one process holding the apiserver and the embedded controller manager; scheduler, cloud controller manager, helm controller, kube-proxy, network policy controller and packaged add-ons disabled | v1.35.9+k3s1, v1.36.5+k3s1 |
| kcp as a library | not measured | kcp v0.33.3 sources |

**Deviation from the issue.** The issue asks that the same tests pass "on a plain kube-apiserver (envtest)". A plain kube-apiserver with etcd fails the owner-reference cascade tests by design, so `envtest-bare` is the negative control and `envtest-gc` is the passing baseline.

Why two envtest baselines: setup-envtest ships etcd and kube-apiserver but no controller manager, so a bare envtest has no garbage collector. HBEP-11 says so as well ("Garbage collection runs in the Kubernetes controller manager, so that runs too"). `envtest-bare` is the negative control for the cascade test; `envtest-gc` is the real "control plane without nodes" baseline.

## Budget (set before any measurement)

This section and the protocol below were committed before the first measurement; the commit order is the evidence.

A "small VM" is 2 vCPU and 4 GiB RAM, with the operating system, the front, the controllers and provider plugins on the same machine. The control-plane host may take at most about a fifth of the memory and must not make starting or upgrading the framework slow.

| budget line | limit |
|---|---|
| RSS idle (sum over all host processes, after ready plus a 30 s settle) | at most 600 MiB |
| RSS with about 100 custom objects (same sum, after the objects exist and are reconciled, plus a 30 s settle) | at most 800 MiB |
| cold start to ready (empty data directory) | at most 30 s |
| warm start to ready (existing data directory) | at most 30 s |
| minor-version upgrade | every object kept with the same UID, the e2e suite passes afterwards, and stop to ready at most 60 s |
| e2e suite | every test passes |

An option passes when every line holds. The controllers' own RSS is reported separately and is not part of the host sum.

## Method

- **One e2e binary.** `go test -c ./e2e` is compiled once per invocation of the run script. The same binary runs against each host; only `S3_KUBECONFIG` differs. For kcp the harness first creates a child workspace and hands the binary that workspace's kubeconfig, so the binary does not know which host it talks to.
- **CRD set and controller** (`kit/`): three CRDs in the spike-only group `s3.spike.invalid` (Fleet ~ ModelDeployment, Worker ~ Instance, Slot ~ Allocation), with a status subresource, schema validation, a CEL immutability rule, a revision pattern and defaulting. One controller-runtime controller keeps the Workers of a Fleet (owned by it, with status) and runs a cancel finalizer on Slots. The controller has no deletion logic for a Fleet's Workers.
- **The eleven e2e tests** (`e2e/`):

  | test | what it checks |
  |---|---|
  | TestDiscovery | the CRDs and their status subresources are served |
  | TestValidation | an unpinned revision, an out-of-range count and a missing field are refused and not stored; a changed revision is refused by the CEL rule; a defaulted field is filled |
  | TestReconcileAndScale | owned Workers appear with the right ownerReference and status; scaling up and down keeps survivors (same UID); a deleted Worker is recreated |
  | TestRestartConvergence | with the controller stopped the world changes and nothing moves; a new controller converges from observed state |
  | TestSelectors | inventory by label selector, set-based selector and field selector |
  | TestConflict | an update with a stale resourceVersion is refused |
  | TestWatch | a watch from a list's resourceVersion delivers added, modified, deleted in order |
  | TestCascadeBackground | deleting an owner deletes its dependents two levels deep, by UID, and spares an unowned object and another owner's children |
  | TestCascadeForeground | the owner disappears only after its blocking dependents are gone |
  | TestFinalizerHoldsDeletion | a Slot with the cancel finalizer stays, with a deletion time, while the provider double refuses; it goes after release, with exactly one successful cancel |
  | TestFinalizerThroughCascade | the same Slot, reached through the garbage collector from a deleted Fleet |

- **The cascade tests cannot pass vacuously.** The controller is not running in the two plain cascade tests, and has no code that deletes Workers in the third. Objects are created through the API with the owner's real UID. The tests assert by UID that each dependent is gone within 90 s. `envtest-bare` has no garbage collector and must fail them; it does (below).
- **Measurements** (`cmd/s3bench`, one option at a time, never two options at once):
  - RSS is `VmRSS` from `/proc/<pid>/status`, summed over every process the harness started for the host and their descendants (etcd and kube-apiserver, plus kube-controller-manager for `envtest-gc`; one process for kcp and one for k3s). The controller's own RSS is reported separately.
  - Idle: after ready, a 30 s settle, then the median of three samples 5 s apart. This is before the CRDs exist.
  - With objects: 20 Fleets with 4 Workers each, so 100 custom objects, created with the controller running. The harness waits until a list shows all of them and every status is reconciled, settles 30 s, and takes the median of three samples while the controller is still connected. The loaded figure is read only after the objects exist and are reconciled.
  - Start time to ready: from just before exec to an authenticated `/readyz` answering 200 (for `envtest-gc` and `k3s` also the controller manager's `/healthz`). `usable` adds the child workspace (kcp) and the CRDs being Established and served. Three cold starts (empty data directory) and three warm starts (the loaded data directory), median reported.
  - Upgrade: old version, CRDs, the 100 objects, then the host stops with no controller running. The UID and creation time of every object and CRD are recorded (103 objects), the new version starts on the same data directory, and the records are compared object by object (`compareSnapshots`, with unit tests that show it can say "not kept"). Then the full e2e suite and a post-upgrade test run: scale a loaded Fleet, delete another and wait for its Workers to be garbage-collected. "Stop to ready" is the old host's stop time plus the new host's time to ready. The wait for a quiet VM between the two is not counted.
  - The 1-minute load average and available memory are recorded at each sample.
- **Ports and data:** loopback only, ports 29101 to 29130, every data directory under the scratch directory. Data directories are deleted as soon as a phase no longer needs them.
- **What the harness checks about the processes it measures** (added after review; each check has a unit test that fails when the check is removed, `cmd/s3bench/verify_test.go`):
  - before every launch (cold, warm and upgrade), every port the host will bind is free; a taken port fails the launch;
  - after readiness, every expected port is held by a LISTEN socket owned by the launched PIDs or their descendants; a port held by another PID, a missing port and an empty listener set are failures; nothing may listen beyond loopback;
  - the readiness probe is `/readyz`; for `envtest-gc` and `k3s` the kube-controller-manager launched for this run must also answer `/healthz`, be alive at the probe, and own its port;
  - every sample (idle, loaded) is rejected if a required process has exited or its `/proc` data cannot be read, at any of the three readings; the controller process counts as required while it is being sampled;
  - the 1-minute load, available memory and free disk (at least 3 GiB) are gated before every launch.
- **Expected outcomes are required.** The expected test sets are written into the harness from the table above (`cmd/s3bench/expect.go`), not read from the suite: eleven passes and the two harness-driven phase tests skipped for a plain run; for `envtest-bare` exactly the three cascade tests fail and the post-upgrade test fails. Zero passing tests, a missing or extra test, a wrong exit status, an upgrade error or a failed budget line that is not the negative control's designed failure all make the harness exit non-zero.
- **Records per phase.** The result file carries one record per phase (main, upgrade) with its own start and end time and the sha256 of the e2e binary, of the harness binary and of every host binary the phase ran, so a partial rerun that is merged into a result never inherits another phase's time or binaries. The e2e binary rebuilds bit for bit from the same source and toolchain (checked: same sha256); the harness binary embeds the git revision, so its hash differs between commits and identifies the build, not the source.
- **"Cold" means a fresh data directory.** Operating-system caches are not cleared, so the host binary may already be in the page cache. For k3s a cold start also includes unpacking about 250 MB of embedded binaries into the new data directory.
- **Changes to the protocol after the budget commit.** The budget numbers did not change.
  - Before the first results: (1) kcp refuses to restart without the admin kubeconfig it wrote, so the harness no longer deletes it; (2) the first upgrade timing for kcp included an 8-minute wait for the shared VM to quiet down, so stop to ready excludes that wait.
  - After the review of PR 36: k3s became a measured option (it had been excluded wrongly); the harness gained the process, listener and sample checks and the expected test sets described above, and per-phase records. Every option was then run again from scratch with the final harness (one full run each: k3s, envtest-gc, kcp, envtest-bare), so the numbers below replace the earlier ones, which are not kept.

## Environment

- The shared agents VM: 6 cores, 31 GB RAM, no swap, Linux 6.8 x86_64, Go 1.27.3. Other agents' sessions, other spikes and a root-owned k3s service were running during every measurement, so the 1-minute load at the readings was between 2.2 and 5.5 (recorded in each result file) and available memory about 20 GiB. The harness waits for load at most 4.0, at least 8 GiB of available memory and at least 3 GiB of free disk before every start.
- **These are observations, not bounds.** They come from one loaded 6-core machine with warm operating-system caches, not from the 2 vCPU budget VM; no start time here is an upper bound for that VM. The RSS readings vary from run to run by about plus or minus 10% (see the notes under the results).
- Library versions of the test kit: controller-runtime v0.25.2, Kubernetes client libraries v0.37.0.

## Results

One full run of each option through the final harness, in the order k3s, `envtest-gc`, `kcp`, `envtest-bare`; the harness exited 0 for each (for `envtest-bare` only because its failures are the expected ones). Raw numbers: `results/*.json` (each carries the start and end time of its two phases and the sha256 of every binary it ran); test output: `results/logs/`. Each figure is one run: the RSS columns are medians of three readings in that run, the start times medians of three starts.

| | `envtest-bare` | `envtest-gc` | `kcp` | `k3s` |
|---|---|---|---|---|
| host processes | 2 (etcd, kube-apiserver) | 3 (plus kube-controller-manager) | 1 | 1 (apiserver, controller manager and SQLite inside) |
| server version | Kubernetes 1.36.2 | Kubernetes 1.36.2 | kcp 0.33.3 (reports Kubernetes v1.36.0) | k3s v1.36.5+k3s1 (Kubernetes v1.36.5) |
| RSS idle (budget 600 MiB) | 282 MiB | 365 MiB | 500 MiB | 363 MiB |
| RSS with 100 objects (budget 800 MiB) | 320 MiB | 386 MiB | 478 MiB | 407 MiB |
| the three loaded readings | 303 to 325 | 385 to 404 | 478 to 478 | 406 to 420 |
| of which, median loaded reading | etcd 51, apiserver 269 | etcd 51, apiserver 261, controller manager 74 | kcp 478 | k3s 407 |
| controller process, with objects (not in the sum) | 39 MiB | 39 MiB | 38 MiB | 39 MiB |
| cold start to ready (budget 30 s) | 3.6 s | 3.9 s | 13.9 s | 10.5 s (`/readyz` after 9.4 s) |
| warm start to ready (budget 30 s) | 3.1 s | 3.0 s | 8.8 s | 6.0 s |
| cold start to usable (child workspace, CRDs served) | 3.7 s | 4.0 s | 15.3 s | 10.6 s |
| stop | 1.2 s | 1.2 s | 1.2 s | 2.3 s |
| data directory after load | 138 MiB | 138 MiB | 138 MiB | 259 MiB, of which the database is 5.8 MiB |
| minor upgrade, objects kept | 103 of 103 (Kubernetes 1.35.0 to 1.36.2) | 103 of 103 (1.35.0 to 1.36.2) | 103 of 103 (kcp 0.32.5 to 0.33.3, same Kubernetes base) | 103 of 103 (v1.35.9+k3s1 to v1.36.5+k3s1) |
| stop to ready, upgrade (budget 60 s) | 2.5 s | 5.0 s | 10.3 s | 13.0 s |
| e2e suite (11 tests; 2 harness-driven phase tests are skipped in a plain run) | 8 pass, 3 fail | 11 pass | 11 pass | 11 pass |
| e2e after the upgrade | 8 pass, 3 fail | 11 pass | 11 pass | 11 pass |
| post-upgrade test (scale one Fleet, cascade-delete another) | fail (no garbage collection) | pass | pass | pass |
| budget verdict | fails the e2e line and the upgrade line, both only because the cascade tests fail without garbage collection | passes | passes | passes |

Notes on the numbers.
- **What the RSS columns are.** The sum of VmRSS over the host's processes. They are not comparable to "memory the VM needs": shared pages are counted in each process, and the load on the machine moves the figure.
- **The idle to loaded difference is not the cost of 100 objects.** Between the two readings in these runs the sum moved by +38 MiB (`envtest-bare`), +20 (`envtest-gc`), −22 (`kcp`) and +43 (`k3s`). The loaded reading also differs from the idle one in other ways: the CRDs exist (so the server serves three more APIs), the controller is connected and reconciling, and for kcp there is a child workspace. Three readings in one run, 5 s apart, span up to 19 MiB (`envtest-gc`) and 15 MiB (`k3s`) by themselves. Earlier complete runs of the same protocol gave idle and loaded RSS of 304 and 310, 289 and 322, 295 and 317 MiB (`envtest-bare`); 358 and 391 (the run committed before the review), 370 and 387 MiB (`envtest-gc`); 519 and 466, 453 and 459, 476 and 463 MiB (`kcp`). They were not kept as result files. For kcp the spread between runs (66 MiB in the idle figure) is larger than its idle to loaded difference; for `envtest-gc` the loaded reading was above the idle one by 17 to 33 MiB in all three runs, which the CRDs and the connected controller may explain as much as the objects. kcp's idle figure is the least stable and is higher than its loaded figure in all but one of the runs; it is sampled soon after start, and I did not look into why (start-up allocations not yet returned is a guess). The honest reading is: the loaded figures are 320, 386, 478 and 407 MiB, the objects' own part is not resolved by this spike, and every figure is far below the 800 MiB line.
- **Start times are single observations on a loaded 6-core machine** (1-minute load 2.2 to 5.5 at the starts and readings), with warm operating-system caches. They show that nothing is near the 30 s line here; they are not upper bounds for a 2 vCPU VM. The three starts of an option differ by up to 1.4 s (`envtest-gc` cold: 3.2, 3.9 and 4.1 s; `kcp` cold: 12.8, 13.9 and 14.1 s).
- **kcp and k3s are slower to start** than the Kubernetes binaries (3 to 4 s). kcp bootstraps its root workspaces and system APIs on every start; k3s starts its datastore shim, certificates and controllers, and a cold k3s start unpacks about 250 MB of embedded binaries into a fresh data directory and sets up a new cluster's certificates and add-on state (a warm start does neither: 6.0 s against 10.5 s).
- **The data directory.** For the three etcd-based hosts it is two preallocated 64 MB write-ahead log files plus a database of about 17 MiB (138 MiB). k3s keeps its state in a 5.8 MiB SQLite database; the rest of its 259 MiB is the unpacked binaries, which are a property of how the single file is distributed, not state.
- **The three failures of `envtest-bare` are the expected ones.** `TestCascadeBackground`, `TestCascadeForeground` and `TestFinalizerThroughCascade` time out at 90 s waiting for garbage collection, and the post-upgrade test fails the same way. The other eight pass, so the suite does discriminate: owner-reference deletion needs the controller manager's garbage collector, which a bare apiserver lacks.
- **Garbage collection is quick where it exists:** `TestCascadeBackground` takes 0.20 to 0.22 s on `envtest-gc`, `kcp` and `k3s`.
- **Local access only:** the harness checked the listening sockets of every host process; all were on loopback (127.0.0.1, and ::1 for k3s's supervisor).

### k3s: measured, rootless and agentless, with no root and no system change

The first version of this section said k3s needed root. That was wrong: it tried `--rootless` and `--disable-agent` separately, and each fails alone, but the two together start as an ordinary user. What each attempt did on this VM, as an ordinary user:

| flags | result |
|---|---|
| `server --disable-agent` | the supervisor and its SQLite store start, but the API server never becomes ready: it loops on `open /etc/rancher/node/password: permission denied`. The node password path is fixed at `/etc/rancher/node/` and is root-owned here. |
| `server --rootless` (with the agent) | exits at once with `failed to start the child: fork/exec /proc/self/exe: operation not permitted`. It needs `newuidmap`, `newgidmap`, `slirp4netns` and `rootlesskit`, which are not installed, and `apparmor_restrict_unprivileged_userns` is 1. |
| `server --rootless --disable-agent` | starts. With the agent disabled, k3s skips the user-namespace setup, and `--rootless` only moves the paths that were fixed under `/etc` to the data directory (the node password lands in `<data-dir>/agent/etc/rancher/node/password`). That is the only effect it has here. |

What ran (`k3sHost` in `cmd/s3bench/hosts.go`): one process, `k3s server --rootless --disable-agent` with SQLite (the default datastore, no `--cluster-init`), the apiserver, the embedded kube-controller-manager (garbage collector included) and the SQLite store (kine) inside it. Flags: `--data-dir` and `--write-kubeconfig` in the scratch directory, `--bind-address 127.0.0.1`, `--https-listen-port 29121`, `--apiserver-port 29120`, `--lb-server-port 29125`, `--kube-controller-manager-arg=secure-port=29122`, `--private-registry` and `K3S_CONFIG_FILE` pointing at empty files in scratch, and `--disable-scheduler --disable-cloud-controller --disable-kube-proxy --disable-network-policy --disable-helm-controller` plus `--disable=` for coredns, servicelb, traefik, local-storage, metrics-server and runtimes. The process runs under `env -i` with `HOME`, `XDG_RUNTIME_DIR` and `KUBECONFIG` in scratch.

- **Isolation from the root-owned k3s on the shared VM.** The system k3s configuration is not read (`K3S_CONFIG_FILE` is an empty scratch file) and neither is its registry file (`--private-registry`; an earlier trial without it logged that it read `registries.yaml`). The harness fails a run if a k3s log mentions `/etc/rancher`, `/var/lib/rancher` or `/run/k3s`, and records the modification time of those three paths before and after the run: unchanged, and no k3s log mentioned them. The ports are all in this spike's block; the system k3s's own ports were never bound.
- **Caveats on what was measured.** `--rootless` is labelled experimental by k3s. The hidden `--apiserver-port` flag is needed only because the system k3s holds 6444; on a machine with no other k3s the default would do. The scheduler, cloud controller manager, helm controller, kube-proxy, network policy controller and the packaged add-ons were disabled, because a host without nodes has no use for them; RSS with k3s's default components is not measured and would be higher. The garbage collector is k3s's embedded kube-controller-manager running with its default controllers, so the cascade tests passing is evidence that it works without nodes.
- **A cold k3s start includes unpacking about 250 MB** of embedded binaries into the new data directory; the data directory is therefore 259 MiB after load, of which the SQLite database is 5.8 MiB.
- **A Kubernetes minor upgrade, which the kcp rows do not give:** v1.35.9+k3s1 to v1.36.5+k3s1, with the 103 objects kept (same UIDs and creation times), the whole suite passing afterwards and the post-upgrade test passing.

### kcp as a Go library: not measured

- `third_party/forked/kcp/s3-embedded/main.go` is adapted from kcp's own `cmd/kcp/kcp.go` at v0.33.3 (same option types, embedded etcd, `server.NewServer`) and adds the in-process controller-runtime manager. `scripts/build-embedded.sh` builds it. It has never compiled to the end, so treat it as an untested scaffold.
- What was learned. A consumer's `go.mod` must repeat kcp's `replace` blocks: about 35 `k8s.io/*` modules redirected to kcp's own Kubernetes fork at a 1.37 alpha commit, and five `kcp-dev` modules pointing at staging directories inside the kcp repository. The script generates that from kcp's `go.mod`, and the dependency graph resolves. The first compile then failed in controller-runtime v0.25.2 (`undefined: events.AnnotatedEventRecorder`): the controller-runtime that matches Kubernetes 1.37 final does not build against kcp's fork snapshot. Embedding therefore pins the controllers' controller-runtime and client-go to whatever matches kcp's fork. A controller-runtime v0.24.x was the next thing to try and was not tried.
- Why it stops here: the build and the downloaded fork modules drove the shared VM's root disk below 1 GB free. The build was stopped by PID and files I had created were removed. It was not retried; a retry needs about 3 GB of free disk for the Go build cache. My disk watchdog fired at 1.47 GB free and stopped the build script, but not the `go build` child (the child was orphaned and kept compiling), so the disk kept filling for a few minutes more until I stopped that PID by hand.
- My estimate of what the measurement would show, which is not itself measured: the server code is the same as the `kcp` binary measured above and the controller measures 38 MiB as a separate process, so embedding should save about that much and one process. The build stopped for lack of disk, which does not show that embedding is infeasible.

## What this means for the HBEPs

1. **Footprint is not the deciding factor for the single-user host.** Every measured host that garbage-collects used 0.35 to 0.49 GiB of RSS and was ready in 3 to 14 s cold on a loaded VM, inside a 600 MiB and 30 s budget. These are observations on a shared 6-core machine, not guarantees for a 2 vCPU VM. HBEP-11's concern "too heavy for one user" is not borne out for any of the shapes.
2. **A host needs the garbage collector, and that is a separate component in a Kubernetes-binary assembly.** A bare kube-apiserver plus etcd does not delete dependents. HBEP-11's "a control plane without nodes" must say the kube-controller-manager (garbage-collector and namespace controllers; 74 MiB here) is part of it. kcp and k3s have it in-process, and the cascade tests pass on both.
3. **All the shapes keep objects across a minor upgrade** (103 of 103, same UID and creation time) and pass the full suite afterwards. k3s and the assembled host were upgraded across a Kubernetes minor (v1.35.9 to v1.36.5 with k3s's SQLite store; 1.35.0 to 1.36.2 with etcd 3.6.6 to 3.6.8). The kcp step was a kcp minor (0.32.5 to 0.33.3) on the same Kubernetes base (v1.36.0), so it is no evidence about a Kubernetes minor inside kcp. Downgrade is not tested (HBEP-11 leaves it TBD).
4. **The spike's reading for HBEP-11's open question, for the owner and the science agent to decide.** User-level k3s is a viable single-user host, which the first version of this spike wrongly excluded:
   - It runs as an ordinary user with no root and no system change (`server --rootless --disable-agent`), one downloaded binary with an embedded SQLite store, and it passes the whole budget and all eleven tests including the cascade. It is also the team option in HBEP-11, so one software would serve both deployments.
   - Its costs against the assembled host: 3 to 7 s slower to start (10.5 s cold against 3.9 s, 6.0 s warm against 3.0 s), 21 MiB more RSS with objects, a 259 MiB data directory because of the unpacked binaries, and a configuration the k3s project calls experimental (`--rootless`), which works here only because `--disable-agent` skips the namespace setup. It was measured with the packaged components disabled; with the defaults it would be heavier by an amount not measured.
   - **The assembled apiserver + etcd + kube-controller-manager** was the lightest and fastest, with the versions pinned by us, but we would own the packaging (certificates, tokens, flags that change across minors); `cmd/s3bench/hosts.go` is a working sketch of that assembly.
   - **The kcp release binary** is one process with no root and in-process garbage collection; it costs the most memory (500 MiB idle) and the longest start (13.9 s cold), it is a 0.x project, and it has the open questions below.
5. **Embedded etcd is the dev default of `kcp start`.** kcp's own Helm chart (`charts/kcp/values.yaml` in kcp-dev/helm-charts) says "Do not set serverAddress to 'embedded', it is not supported" for chart deployments. Running kcp with its embedded etcd for one user relies on the dev default. Running kcp against an external etcd was not measured; it would add an etcd process (about 50 MiB here).
6. **Embedding kcp as a library has a version cost** (the controllers' controller-runtime must match kcp's Kubernetes fork snapshot), so the "one bundled binary" option of HBEP-11 is more work than it looks. It is not measured.

## What is still unknown

- **HBEP-11 test-plan items not exercised here:** admission webhooks (the unpinned-revision refusal here is a CRD schema rule, not a webhook; the guard on deleting CRDs needs a webhook or policy), per-identity RBAC and the audit log, leader-election `Lease` objects, Events, and a watch resuming after compaction. These decide whether kcp is acceptable and were not tested on any of the hosts. Whether kcp workspaces serve Leases and Events, and how webhooks are configured there, is the first thing to check before adopting it.
- **kcp with an external etcd**, and long-run behaviour of either host: RSS growth over days, etcd database growth and compaction, defragmentation.
- **k3s with its default components** (scheduler, cloud controller manager, helm controller and the packaged add-ons were disabled here, so RSS with the defaults is higher by an amount not measured), **k3s rootless with an agent**, k3s with an external datastore, and **kcp as a library**.
- **Upgrade variants:** a Kubernetes minor change inside kcp, a skipped minor, a downgrade, and an upgrade with the controllers running.
- **A smaller VM.** The budget VM (2 vCPU, 4 GiB) was not used; the measurements are RSS and wall time on a 6-core shared VM, so start times on 2 vCPU are not measured.
- **Controller footprint under real work** (the test controller is tiny), and the front's and provider plugins' share of the budget.

## How to run

Go 1.27.x on PATH (the spike ran on 1.27.3), a writable scratch directory with at least 6 GB free (the harness waits while the disk has less than 3 GiB free), and this directory as the working directory.

```
SCRATCH=<dir> scripts/run-all.sh            # fetch pinned binaries, vet, unit tests, e2e binary, harness, then k3s, envtest-gc, kcp and envtest-bare
SCRATCH=<dir> SKIP_FETCH=1 scripts/run-all.sh kcp    # one option, binaries already fetched
go test ./cmd/s3bench/                       # unit tests of the harness checks and of the upgrade comparison
```

About 10 to 25 minutes per option on a quiet VM (`envtest-bare` takes longest: its three cascade tests wait out a 90 s timeout in each of two suite runs, and the post-upgrade test waits once more). The harness waits while the machine is busy (load above 4.0, under 8 GiB available, or under 3 GiB of free disk). The harness exits non-zero on any unexpected outcome. A single run of the e2e binary against any control plane you may fill with test objects: `S3_KUBECONFIG=<kubeconfig> go test ./e2e -v` (it creates three CRDs and a few namespaces). `-option kcp -only upgrade` on the harness reruns just the upgrade part and merges it into the stored result, with its own phase record.

## Provenance of what is reused

Everything third-party is used unmodified, except one file that is adapted: `third_party/forked/kcp/s3-embedded/main.go` from kcp's `cmd/kcp/kcp.go` (kcp-dev/kcp, Apache-2.0, v0.33.3, commit 003eb8c7649b0f204ec1bd3844e10f633f90065b), with kcp's LICENSE and an `ORIGIN.md` (the HBEP-15 record) beside it. New code: the test kit, the e2e suite, the harness and the scripts.

| component | project and URL | licence | version |
|---|---|---|---|
| kcp | kcp-dev/kcp, https://github.com/kcp-dev/kcp | Apache-2.0 | v0.32.5, v0.33.3 (release binaries, sha256 pinned in `scripts/fetch.sh`) |
| kube-apiserver, etcd, kubectl | Kubernetes and etcd, via setup-envtest bundles | Apache-2.0 | 1.35.0 (etcd 3.6.6) and 1.36.2 (etcd 3.6.8) |
| kube-controller-manager | Kubernetes, https://dl.k8s.io | Apache-2.0 | v1.35.0, v1.36.2 (sha256 pinned) |
| setup-envtest, controller-runtime | kubernetes-sigs/controller-runtime, https://github.com/kubernetes-sigs/controller-runtime | Apache-2.0 | v0.25.2 |
| Kubernetes Go libraries (client-go, apimachinery, apiextensions-apiserver, api) | https://github.com/kubernetes | Apache-2.0 | v0.37.0 |
| logr | go-logr/logr, https://github.com/go-logr/logr | Apache-2.0 | v1.4.3 |
| k3s | k3s-io/k3s, https://github.com/k3s-io/k3s | Apache-2.0 | v1.35.9+k3s1, v1.36.5+k3s1 (release binaries, sha256 pinned in `scripts/fetch.sh`, checked against each release's `sha256sum-amd64.txt`) |

## Files

- `kit/`: CRDs (`crds.yaml`), the controller, kcp workspace helper.
- `e2e/`: the suite; `main_test.go` reads `S3_KUBECONFIG`.
- `cmd/s3bench/`: the harness.
- `embedded/go.mod.tmpl`, `scripts/build-embedded.sh`: the untested embedded-kcp scaffold; its `main.go` is adapted from kcp and lives in `third_party/forked/kcp/s3-embedded/`.
- `scripts/`: `fetch.sh`, `run-all.sh`.
- `../../third_party/forked/kcp/`: the one adapted file, with `LICENSE` and `ORIGIN.md`.
- `results/`: one JSON per option and the test-framework lines of each e2e log.
