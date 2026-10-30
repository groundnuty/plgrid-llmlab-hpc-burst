# Spike S3: host for the single-user control plane

Issue: groundnuty/plgrid-llmlab-hpc-burst#26. Decides the open question in HBEP-11: which process hosts the Kubernetes API for one user.

## Question

HBEP-11 makes the Kubernetes API (custom resources plus controllers) the control plane. For a single user the host is open between k3s with SQLite, kcp (no nodes, in-process garbage collection), kcp embedded as a Go library, and a plain kube-apiserver with etcd. Which fits a small VM, and which behaves correctly for the behaviour the HBEP relies on (owner-reference cascade, finalizers, validation, watches, upgrades)?

## Options

| id | what runs | version(s) |
|---|---|---|
| `envtest-bare` | kube-apiserver + etcd from controller-runtime's setup-envtest. No controller manager. | 1.35.0 then 1.36.2 |
| `envtest-gc` | the same, plus kube-controller-manager restricted to the garbage-collector and namespace controllers | 1.35.0 then 1.36.2 |
| `kcp` | the kcp release binary, embedded etcd, one process | 0.32.5 then 0.33.3 |
| `k3s-nodeless` | k3s release binary as a server without a node (SQLite via kine, in-process controller manager), as an ordinary user | 1.35.9 then 1.36.5 |
| `kcp-embedded` | kcp as a Go library inside one binary that also runs the controllers | see its section |

Why two baselines: setup-envtest ships etcd and kube-apiserver but no controller manager, so a bare envtest has no garbage collector. HBEP-11 says so as well ("Garbage collection runs in the Kubernetes controller manager, so that runs too"). `envtest-bare` is the negative control for the cascade test; `envtest-gc` is the real "control plane without nodes" baseline.

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

- **One e2e binary.** `go test -c ./e2e` is compiled once. The same binary runs against each host; only `S3_KUBECONFIG` differs. For kcp the harness first creates a child workspace and hands the binary that workspace's kubeconfig, so the binary does not know which host it talks to.
- **CRD set and controller** (`kit/`): three CRDs in the spike-only group `s3.spike.invalid` (Fleet ~ ModelDeployment, Worker ~ Instance, Slot ~ Allocation), with a status subresource, schema validation, a CEL immutability rule, a revision pattern and defaulting. One controller-runtime controller keeps the Workers of a Fleet (owned by it, with status) and runs a cancel finalizer on Slots. The controller has no deletion logic for a Fleet's Workers.
- **Cascade test cannot pass vacuously.** The controller is not running during the cascade tests. Objects are created through the API with the owner's real UID, two levels deep. The test asserts by UID that every dependent is gone within 90 s, and that an unowned object and an object owned by another owner survive. The `envtest-bare` run must fail it.
- **Finalizer test.** A Slot's provider job is "cancelled" through an injected double that fails until released; the Slot must stay, with a deletion timestamp, for 3 s, then go after release, with exactly one successful cancel. A second variant reaches the Slot through the garbage collector from a deleted Fleet.
- **Measurements** (`cmd/s3bench`, one option at a time, never two options at once):
  - RSS is `VmRSS` from `/proc/<pid>/status`, summed over every process the harness started for the host (etcd, kube-apiserver and kube-controller-manager for the baseline; one process for kcp and k3s).
  - Idle: after ready, a 30 s settle, then the median of three samples 5 s apart.
  - With objects: 20 Fleets with 4 Workers each, 100 custom objects in all, created with the controller running. The harness waits until the count is verified by listing and every status is reconciled, settles 30 s, and takes the median of three samples while the controller is still connected.
  - Start time to ready: from just before exec to an authenticated `/readyz` answering 200 (`ready`), and to the first custom resource list that works after the CRDs are Established (`usable`). Three cold starts (empty data directory) and three warm starts (data directory of the loaded run), median reported.
  - Upgrade: old version, CRDs, the 100 objects, then the host stops with no controller running. Every object's UID and creation time are recorded, the new version starts on the same data directory, and the records are compared object by object. Then the e2e suite and a post-upgrade test (scale a loaded Fleet, cascade-delete another) run.
  - The load average and available memory are recorded at each sample.
- **Ports and data:** loopback only, ports in 29101 to 29130, every data directory under the scratch directory.

## Environment, results, meaning, unknowns

Filled in after the measurements; see below.
