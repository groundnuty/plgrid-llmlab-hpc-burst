# Spike S3: host for the single-user control plane

Issue: groundnuty/plgrid-llmlab-hpc-burst#26. Decides the open question in HBEP-11: which process hosts the Kubernetes API for one user.

## Question

HBEP-11 makes the Kubernetes API (custom resources plus controllers) the control plane. For a single user the host is open between k3s with SQLite, kcp (no nodes, in-process garbage collection), kcp embedded as a Go library, and a plain kube-apiserver with etcd. Which fits a small VM, and which behaves correctly for what the HBEP relies on (owner-reference cascade, finalizers, validation, watches, upgrades)?

## Summary

| option | measured? | result against the budget below |
|---|---|---|
| plain kube-apiserver + etcd, no controller manager (`envtest-bare`) | yes | fails the e2e suite on the three cascade tests, as designed: it is the negative control (nothing garbage-collects) |
| kube-apiserver + etcd + kube-controller-manager limited to garbage collection and namespaces (`envtest-gc`) | yes | passes every line: 358 / 391 MiB, ready in 3.8 s, upgrade kept 103 of 103 objects |
| kcp release binary, embedded etcd (`kcp`) | yes | passes every line: 519 / 466 MiB, ready in 13.0 s cold and 8.9 s warm, upgrade kept 103 of 103 objects |
| k3s with SQLite | no | not measured: needs root or system change (reason below) |
| kcp embedded as a Go library | no | not measured: the build did not get past a dependency conflict and needs more disk than the shared VM has free (details below) |

Both measured hosts that garbage-collect fit the small VM with wide margin: the host takes 0.35 to 0.51 GiB of the 4 GiB budget VM, and the 100 objects add nothing measurable. What separates them is not footprint but shape: three processes that we assemble and pin (lighter, faster, plain Kubernetes) against one downloaded process that runs without root (kcp, about 75 to 160 MiB more and 5 to 9 s slower to start). Section "What this means for the HBEPs" gives the spike's lean and what is still open.

## Options and what ran

| id | what runs | versions (old for the upgrade, then new) |
|---|---|---|
| `envtest-bare` | kube-apiserver + etcd from controller-runtime's setup-envtest. No controller manager. | 1.35.0, 1.36.2 |
| `envtest-gc` | the same, plus kube-controller-manager started with only the garbage-collector and namespace controllers | 1.35.0, 1.36.2 |
| `kcp` | the kcp release binary with its embedded etcd, one process, the e2e suite running inside a child workspace | 0.32.5, 0.33.3 |
| k3s | not measured | v1.36.5+k3s1 was tried (see below) |
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
  - RSS is `VmRSS` from `/proc/<pid>/status`, summed over every process the harness started for the host (etcd, kube-apiserver, and kube-controller-manager for `envtest-gc`; one process for kcp). The controller's own RSS is reported separately.
  - Idle: after ready, a 30 s settle, then the median of three samples 5 s apart. This is before the CRDs exist.
  - With objects: 20 Fleets with 4 Workers each, so 100 custom objects, created with the controller running. The harness waits until a list shows all of them and every status is reconciled, settles 30 s, and takes the median of three samples while the controller is still connected. The loaded figure is read only after the objects exist and are reconciled.
  - Start time to ready: from just before exec to an authenticated `/readyz` answering 200 (for `envtest-gc` also the controller manager's `/healthz`). `usable` adds the child workspace (kcp) and the CRDs being Established and served. Three cold starts (empty data directory) and three warm starts (the loaded data directory), median reported.
  - Upgrade: old version, CRDs, the 100 objects, then the host stops with no controller running. The UID and creation time of every object and CRD are recorded (103 objects), the new version starts on the same data directory, and the records are compared object by object (`compareSnapshots`, with unit tests that show it can say "not kept"). Then the full e2e suite and a post-upgrade test run: scale a loaded Fleet, delete another and wait for its Workers to be garbage-collected. "Stop to ready" is the old host's stop time plus the new host's time to ready. The wait for a quiet VM between the two is not counted.
  - The 1-minute load average and available memory are recorded at each sample.
- **Ports and data:** loopback only (the harness fails the run if any host process listens elsewhere), ports 29101 to 29130, every data directory under the scratch directory.
- **Changes to the protocol after the budget commit.** The budget numbers did not change. Three fixes to the harness: (1) kcp refuses to restart without the admin kubeconfig it wrote, so the harness no longer deletes it; (2) the first upgrade timing for kcp included an 8-minute wait for the shared VM to quiet down (490 s), so stop to ready now excludes that wait; (3) the kcp upgrade result was produced by a separate `s3bench -option kcp -only upgrade` run, because a cleanup of mine had removed the old kcp binary during the wrapper run; the merge into `results/kcp.json` is done by the harness itself.

## Environment

- The shared agents VM: 6 cores, 31 GB RAM, no swap, Linux 6.8 x86_64, Go 1.27.3. Other agents' sessions, other spikes and a root-owned k3s service were running during every measurement, so the 1-minute load was between 1.7 and 4.6 at the samples (recorded in each result file) and available memory about 20 GiB. The harness waits for load at most 4.0 and at least 8 GiB available before every start. Numbers are from a loaded machine: read the RSS to about plus or minus 10% and the start times as upper bounds.
- Library versions of the test kit: controller-runtime v0.25.2, Kubernetes client libraries v0.37.0.

## Results

Final run of each option through `scripts/run-all.sh`, plus the kcp upgrade rerun noted above. Raw numbers: `results/*.json`; test output: `results/logs/`.

| | `envtest-bare` | `envtest-gc` | `kcp` |
|---|---|---|---|
| host processes | 2 (etcd, kube-apiserver) | 3 (plus kube-controller-manager) | 1 |
| server version | Kubernetes 1.36.2 | Kubernetes 1.36.2 | kcp 0.33.3 (reports Kubernetes v1.36.0) |
| RSS idle (budget 600 MiB) | 304 MiB | 358 MiB | 519 MiB |
| RSS with 100 objects (budget 800 MiB) | 310 MiB | 391 MiB | 466 MiB |
| of which, with objects | etcd 51, apiserver 259 | etcd 50, apiserver 267, controller manager 73 | kcp 466 |
| controller process, with objects (not in the sum) | 39 MiB | 38 MiB | 38 MiB |
| cold start to ready (budget 30 s) | 3.3 s | 3.8 s | 13.0 s |
| warm start to ready (budget 30 s) | 2.6 s | 3.5 s | 8.9 s |
| cold start to usable (child workspace, CRDs served) | 3.5 s | 3.9 s | 14.3 s |
| stop | 1.2 s | 1.2 s | 1.2 s |
| data directory after load | 138 MiB | 138 MiB | 138 MiB |
| minor upgrade, objects kept | 103 of 103 (1.35.0 to 1.36.2) | 103 of 103 (1.35.0 to 1.36.2) | 103 of 103 (0.32.5 to 0.33.3) |
| stop to ready, upgrade (budget 60 s) | 2.6 s | 4.2 s | 10.5 s |
| e2e suite (11 tests; 2 harness-driven phase tests are skipped in a plain run) | 8 pass, 3 fail | 11 pass | 11 pass |
| e2e after the upgrade | 8 pass, 3 fail | 11 pass | 11 pass |
| post-upgrade test (scale one Fleet, cascade-delete another) | fail (no garbage collection) | pass | pass |
| budget verdict | fails the e2e line and the upgrade line, both only because the cascade tests fail without garbage collection | passes | passes |

Notes on the numbers.
- **The data directory size is the same everywhere** because every option runs etcd: two preallocated 64 MB write-ahead log files plus a database of about 17 MiB.
- **RSS varies from run to run.** Earlier complete runs of the same protocol, made while the harness was being fixed and not kept as result files, gave idle and loaded RSS of 289 and 322, then 295 and 317 MiB (`envtest-bare`); 370 and 387 MiB (`envtest-gc`); 453 and 459, then 476 and 463 MiB (`kcp`). kcp's idle figure is the least stable (453 to 519 MiB) and is higher than its loaded figure; it is sampled soon after start, and I did not look into why (start-up allocations not yet returned is a guess). The loaded figure is steady at 459 to 466 MiB. Neither host's RSS grows measurably with 100 objects; the footprint is the server itself.
- **kcp is slower to start** (13 s cold, 9 s warm) than the Kubernetes binaries (3 to 4 s). It bootstraps its root workspaces and system APIs on every start; the numbers are still well inside the 30 s budget.
- **The three failures of `envtest-bare` are the expected ones.** `TestCascadeBackground`, `TestCascadeForeground` and `TestFinalizerThroughCascade` time out at 90 s waiting for garbage collection, and the post-upgrade test fails the same way. The other eight pass, so the suite does discriminate: owner-reference deletion needs the controller manager's garbage collector, which a bare apiserver lacks.
- **kcp's garbage collection is fast:** the five dependents (two levels) were gone 120 ms after the delete.
- **Local access only:** the harness checked the listening sockets of every host process; all were on 127.0.0.1.

### k3s: not measured, needs root or system change

Checked on this VM, as an ordinary user:
- `/etc/subuid` and `/etc/subgid` have entries for the user, but `newuidmap`, `newgidmap`, `slirp4netns` and `rootlesskit` are not installed, and the kernel setting `apparmor_restrict_unprivileged_userns` is 1.
- `k3s server --rootless` (v1.36.5+k3s1) exits at once with `failed to start the child: fork/exec /proc/self/exe: operation not permitted`. Fixing it needs a package install or a sysctl change, which are system changes, so it was not forced.
- A plain user-level `k3s server --disable-agent` with its own data directory and loopback ports starts the supervisor and its SQLite store, but the API server never becomes ready. It loops on `open /etc/rancher/node/password: permission denied`. That path is fixed (I found no flag to move it) and is root-owned on this VM.
- **Pitfall for anyone repeating this on a VM that already runs k3s:** `k3s server` reads the system configuration file by default. Point `K3S_CONFIG_FILE` at an empty file and `HOME` at a scratch directory first. My first attempt did not; it ran for under a minute, appeared to load that configuration (it logged `Managed etcd cluster initializing`), stopped on the permission error above without binding any of the existing server's ports, and was stopped by PID. The existing server's listeners were verified up afterwards.

So k3s is "not measured: needs root or system change" (root, or a writable /etc/rancher/node plus the user-namespace tools for rootless mode). It remains HBEP-11's team option; its single-user footprint is unknown from this spike.

### kcp as a Go library: not measured

- `third_party/forked/kcp/s3-embedded/main.go` is adapted from kcp's own `cmd/kcp/kcp.go` at v0.33.3 (same option types, embedded etcd, `server.NewServer`) and adds the in-process controller-runtime manager. `scripts/build-embedded.sh` builds it. It has never compiled to the end, so treat it as an untested scaffold.
- What was learned. A consumer's `go.mod` must repeat kcp's `replace` blocks: about 35 `k8s.io/*` modules redirected to kcp's own Kubernetes fork at a 1.37 alpha commit, and five `kcp-dev` modules pointing at staging directories inside the kcp repository. The script generates that from kcp's `go.mod`, and the dependency graph resolves. The first compile then failed in controller-runtime v0.25.2 (`undefined: events.AnnotatedEventRecorder`): the controller-runtime that matches Kubernetes 1.37 final does not build against kcp's fork snapshot. Embedding therefore pins the controllers' controller-runtime and client-go to whatever matches kcp's fork. A controller-runtime v0.24.x was the next thing to try and was not tried.
- Why it stops here: the build and the downloaded fork modules drove the shared VM's root disk below 1 GB free. The build was stopped by PID and files I had created were removed. It was not retried; a retry needs about 3 GB of free disk for the Go build cache. My disk watchdog fired at 1.47 GB free and stopped the build script, but not the `go build` child (the child was orphaned and kept compiling), so the disk kept filling for a few minutes more until I stopped that PID by hand.
- The expected value of the measurement is low: the server code is the same as the `kcp` binary measured above, and the controller already measures 38 MiB as a separate process, so embedding could save at most that much and one process.

## What this means for the HBEPs

1. **Footprint is not the deciding factor for the single-user host.** Every measured host that garbage-collects uses 0.35 to 0.51 GiB and starts in 4 to 13 s on a loaded VM, inside a 600 MiB and 30 s budget, and 100 objects add nothing measurable. HBEP-11's concern "too heavy for one user" does not hold for either shape.
2. **A host needs the garbage collector, and that is a separate component.** A bare kube-apiserver plus etcd does not delete dependents. HBEP-11's "a control plane without nodes" must say the kube-controller-manager (garbage-collector and namespace controllers; 73 MiB) is part of it. kcp has it in-process.
3. **Both shapes keep objects across a minor upgrade** (103 of 103, same UID and creation time) and pass the full suite afterwards. The kcp step was a kcp minor (0.32.5 to 0.33.3); both versions report the same Kubernetes base (v1.36.0), so a Kubernetes minor change inside kcp is not covered. Downgrade is not tested (HBEP-11 leaves it TBD).
4. **The spike's lean for HBEP-11's open question, for the owner and the science agent to decide:**
   - If the single user has no root and wants to download one thing: **the kcp release binary**. One process, no root, in-process garbage collection, all eleven tests and the upgrade pass. The costs are about 75 to 160 MiB and 5 to 9 s more than the assembled alternative, a 0.x project (v0.33 here), and the unknowns below.
   - If footprint and start time matter more, or if HBEP-11's test plan needs full Kubernetes behaviour: **the assembled apiserver + etcd + kube-controller-manager**, with the versions pinned by us. It was the lightest and fastest, but we would own the packaging (certificates, tokens, flags that change across minors); `cmd/s3bench/hosts.go` is a working sketch of that assembly.
   - **k3s** stays the team option. This spike could not tell whether it is also the best single-user host.
5. **Embedded etcd is the dev default of `kcp start`.** kcp's own Helm chart (`charts/kcp/values.yaml` in kcp-dev/helm-charts) says "Do not set serverAddress to 'embedded', it is not supported" for chart deployments. Running kcp with its embedded etcd for one user relies on the dev default. Running kcp against an external etcd was not measured; it would add an etcd process (about 50 MiB here).
6. **Embedding kcp as a library has a version cost** (the controllers' controller-runtime must match kcp's Kubernetes fork snapshot), so the "one bundled binary" option of HBEP-11 is more work than it looks. It is not measured.

## What is still unknown

- **HBEP-11 test-plan items not exercised here:** admission webhooks (the unpinned-revision refusal here is a CRD schema rule, not a webhook; the guard on deleting CRDs needs a webhook or policy), per-identity RBAC and the audit log, leader-election `Lease` objects, Events, and a watch resuming after compaction. These decide whether kcp is acceptable and were not tested on either host. Whether kcp workspaces serve Leases and Events, and how webhooks are configured there, is the first thing to check before adopting it.
- **kcp with an external etcd**, and long-run behaviour of either host: RSS growth over days, etcd database growth and compaction, defragmentation.
- **k3s** on one user's machine, and **kcp as a library**.
- **Upgrade variants:** a Kubernetes minor change inside kcp, a skipped minor, a downgrade, and an upgrade with the controllers running.
- **A smaller VM.** The budget VM (2 vCPU, 4 GiB) was not used; the measurements are RSS and wall time on a 6-core shared VM, so start times on 2 vCPU are not measured.
- **Controller footprint under real work** (the test controller is tiny), and the front's and provider plugins' share of the budget.

## How to run

Go 1.27.x on PATH (the spike ran on 1.27.3), a writable scratch directory with at least 6 GB free, and this directory as the working directory.

```
SCRATCH=<dir> scripts/run-all.sh            # fetch pinned binaries, vet, unit tests, e2e binary, harness, then envtest-bare, envtest-gc and kcp
SCRATCH=<dir> SKIP_FETCH=1 scripts/run-all.sh kcp    # one option, binaries already fetched
go test ./cmd/s3bench/                       # unit tests of the upgrade comparison
```

About 10 to 25 minutes per option on a quiet VM (`envtest-bare` takes longest: its three cascade tests wait out a 90 s timeout in each of two suite runs, and the post-upgrade test waits once more). The harness waits while the machine is busy (load above 4.0 or under 8 GiB available). A single run of the e2e binary against any control plane you may fill with test objects: `S3_KUBECONFIG=<kubeconfig> go test ./e2e -v` (it creates three CRDs and a few namespaces). `-option kcp -only upgrade` on the harness reruns just the upgrade part and merges it into the stored result.

## Provenance of what is reused

Everything third-party is used unmodified, except one file that is adapted: `third_party/forked/kcp/s3-embedded/main.go` from kcp's `cmd/kcp/kcp.go` (kcp-dev/kcp, Apache-2.0, v0.33.3, commit 003eb8c7649b0f204ec1bd3844e10f633f90065b), with kcp's LICENSE and a NOTES file beside it. New code: the test kit, the e2e suite, the harness and the scripts.

| component | project and URL | licence | version |
|---|---|---|---|
| kcp | kcp-dev/kcp, https://github.com/kcp-dev/kcp | Apache-2.0 | v0.32.5, v0.33.3 (release binaries, sha256 pinned in `scripts/fetch.sh`) |
| kube-apiserver, etcd, kubectl | Kubernetes and etcd, via setup-envtest bundles | Apache-2.0 | 1.35.0 (etcd 3.6.6) and 1.36.2 (etcd 3.6.8) |
| kube-controller-manager | Kubernetes, https://dl.k8s.io | Apache-2.0 | v1.35.0, v1.36.2 (sha256 pinned) |
| setup-envtest, controller-runtime | kubernetes-sigs/controller-runtime, https://github.com/kubernetes-sigs/controller-runtime | Apache-2.0 | v0.25.2 |
| Kubernetes Go libraries (client-go, apimachinery, apiextensions-apiserver, api) | https://github.com/kubernetes | Apache-2.0 | v0.37.0 |
| logr | go-logr/logr, https://github.com/go-logr/logr | Apache-2.0 | v1.4.3 |
| k3s (tried, not measured) | k3s-io/k3s, https://github.com/k3s-io/k3s | Apache-2.0 | v1.36.5+k3s1 (sha256 in `scripts/fetch.sh`, behind `WITH_K3S=1`) |

## Files

- `kit/`: CRDs (`crds.yaml`), the controller, kcp workspace helper.
- `e2e/`: the suite; `main_test.go` reads `S3_KUBECONFIG`.
- `cmd/s3bench/`: the harness.
- `embedded/go.mod.tmpl`, `scripts/build-embedded.sh`: the untested embedded-kcp scaffold; its `main.go` is adapted from kcp and lives in `third_party/forked/kcp/s3-embedded/`.
- `scripts/`: `fetch.sh`, `run-all.sh`.
- `results/`: one JSON per option and the test-framework lines of each e2e log.
