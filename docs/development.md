# Development

The framework is one Go module at the repository root, `github.com/groundnuty/plgrid-llmlab-hpc-burst`. The minimum Go version is 1.26.0.

Code under `spikes/` and `third_party/forked/*` is not part of this module.
- **Spikes:** each Go spike has its own `go.mod`. S7 is Python and shell, with no Go packages.
- **Forked packages:** each one has its own `go.mod`. A build tag alone is not enough, because `go mod tidy` considers every build tag. Check with `go mod tidy -diff`; it must show nothing.

## Layout

| Path | What lives there | HBEP |
|---|---|---|
| `api/v1alpha1/` | Go types for the custom resources, with kubebuilder and CEL markers | HBEP-3, HBEP-11 |
| `config/crd/` | CRD manifests generated from `api/`, committed. #40 adds `hack/verify-generated.sh` to check they match a fresh generation. | HBEP-11 |
| `pkg/deploymentid/` | deployment ID, scheme `v1`: canonical object, RFC 8785 bytes, SHA-256 | HBEP-3 |
| `pkg/provider/` | the provider core interface: operations, outcomes, error kinds, the capability flag, the Slurm phase table. `fake/`: an in-memory Slurm (`Batch`) and a stand-in for a Provider's controller (`Controller`) | HBEP-4 |
| `pkg/agentapi/` | the agent protocol, standard library only: wire types, HTTP framing, `agent.json`, the shared file layout, the certificate pin, and the `Backend` seam. `fake/`: a fake agent and a fake `Backend` | HBEP-7 |
| `internal/launcher/` | starts and supervises the single-user control-plane host (user-level k3s); the only code that starts k3s | HBEP-11 |
| `cmd/hpcburst/` | the `hpcburst` command (`up`, `down`, `status`, `upgrade`) | HBEP-11 |
| `hack/` | scripts for generation, verification and pinned tools | |
| `third_party/forked/<project>/` | copied code, each with its `LICENSE` and `ORIGIN.md` | HBEP-15 |
| `spikes/<id>/` | spike code and results; each Go spike is its own module | |

`pkg/` is for packages other components import. `internal/` is for packages only this module uses.

## Controllers and the agent

### Process layout

- **One controller binary** in alpha, `cmd/hpcburst-controller` (#61 owns its `main`). `hpcburst up` starts and supervises it, and restarts it when it exits (#61, in `internal/launcher`).
- **Inside it, one controller-runtime manager per Provider** (HBEP-4, Two controllers; HBEP-11, Controllers).
  - Each runs crossplane-runtime's managed reconciler over the Allocations naming its Provider.
  - Each has its own leader `Lease`, `v1alpha1.ProviderLeaseName(<provider>)`, which is `provider-<name>` (HBEP owner, 31 Oct 2026).
  - A supervisor starts a manager when a Provider appears and stops it when the Provider goes, so one Provider's failure does not stop the others (#59 owns it, and `main` calls it).
- **The lifecycle controller** is one more manager in the same binary (#61), with the `Lease` `v1alpha1.LeaseLifecycle`.
- **Every `Lease` lives in `v1alpha1.ControllerNamespace`, `hpcburst-system`.** The launcher creates that namespace before it starts the controller binary; #61, which owns the supervision in `internal/launcher`, adds it.
- **The agent API** runs inside the lifecycle controller's process:
  - #60's HTTP server handles TLS, the routes, authentication and the long poll;
  - it calls the lifecycle controller through `agentapi.Backend`, which #61 implements.
- **The allocation agent** is a separate static binary, `cmd/hb-agent` (#60), built for `aarch64` and `x86_64`, that runs inside the job.

### Package boundaries

| Path | Owner | What |
|---|---|---|
| `api/v1alpha1/`, `config/crd/`, `pkg/provider/`, `pkg/agentapi/` | contracts owner | shared types, interfaces and fakes |
| `internal/slurm/` | #59 | `provider.Batch` and `provider.ProfileReader` for Slurm over ssh |
| the managed Provider's controller (`provider.Managed` for LLMLab, the catalog loop) | not yet assigned | `fake.Managed` and `fake.ManagedController` stand in for it |
| `internal/providerctl/` | #59 | the managed reconciler and the per-Provider manager supervisor |
| `internal/agentserver/` | #60 | the agent API's HTTP server, calling `agentapi.Backend` |
| `internal/agent/`, `cmd/hb-agent/` | #60 | the allocation agent |
| `internal/lifecycle/` | #61 | the lifecycle manager, its verification controller and the `Backend` |
| `cmd/hpcburst-controller/`, `internal/launcher/` | #61 | the controller binary, and supervising it from `hpcburst up` |

The lifecycle controller never imports `internal/slurm` or `internal/providerctl`. It acts on providers only through Allocation objects, and reads Allocation and Provider status (HBEP-7, Controllers). Its tests drive `pkg/provider/fake.Controller` until the real provider lands, and `fake.ManagedController` for a managed Provider's catalog. #59's reconciler tests use `fake.Batch` for the protocol and S4's fake Slurm for the dialect.

### Single writer of the API

- **Only the contracts owner edits `api/` and `config/crd/`.** A builder who needs a field, a condition type or a reason asks for it and gets a small PR. Builders never regenerate or hand-edit the CRDs.
- **Each field of each object has one writer.** Every write carries a resourceVersion precondition: an Update, or a patch with an optimistic lock. A conflict means re-read, never overwrite (HBEP-7, open question 11). The one server-side apply is the lifecycle controller's `DialHomeUnreachable` (below).
- **Field managers.** Every write names its controller's field manager, so `managedFields` shows who wrote what: `v1alpha1.FieldManagerLifecycle` (`hpcburst-lifecycle`) for the lifecycle controller and its `Backend`, and `v1alpha1.FieldManagerProvider` (`hpcburst-provider`) for every Provider's controller.

| Object, part | Writer |
|---|---|
| Allocation: creation, deletion, spec, labels | lifecycle controller |
| Allocation: annotations `external-name`, `submission`, `job-id`, `hold-lifted`; finalizer `cancel-all`; all of status, including `status.retention` (a mirror of the spec) | the Provider's controller (crossplane-runtime rewrites the whole status) |
| Allocation: annotation `abandon-attempt` | an operator |
| Instance: creation, deletion, spec, status, annotations (`restart-<part>`, `predecessor`), finalizer `drain` | lifecycle controller, including its `Backend` |
| Instance: `status.leaseEnd` | lifecycle controller: on every observe it copies the Allocation's `status.atProvider.hardEnd`, so a later hard end moves it. The front reads it and never routes to an Instance without it (HBEP-5, HBEP-7) |
| ModelDeployment status, including a managed source's `Ready` and `Progressing` and the reason `ProviderNotOnPlatform` | lifecycle controller. The front only filters: it routes a managed entry to no Provider off `spec.source.managed.platform` |
| Provider status, the managed `catalog` and `lastCatalogRead` included | the Provider's controller, under `hpcburst-provider`. It keeps every condition it does not own |
| Provider status: the condition `DialHomeUnreachable` | lifecycle controller, by server-side apply to the status subresource under `hpcburst-lifecycle`, with forced ownership, owning only that list entry (`v1alpha1.DialHomeUnreachableApply`). Neither writer removes the other's condition (HBEP owner, 31 Oct 2026) |
| identity and verification records | lifecycle controller (its verification controller) |

### Runtime conventions

- **The controller's credentials directory** is mode 700 under the launcher's state directory.
  - `agentapi.AllocationCredentialPath(dir, <Allocation UID>)` holds each Allocation's credential: 64 lowercase hex characters, no newline, mode 600. The lifecycle controller writes it as soon as the create returns the UID. Until the file exists, the provider's Create waits and requeues: it neither submits nor marks the attempt Rejected. The provider reads the file to stage it (`alloc/<tag>/credential`), and the agent API reads it to authenticate.
  - `agentapi.InstanceKeyPath(dir, <Instance UID>)` holds each Instance's key, which the front reads.
- **The agent's identity** is a random agent instance ID that the agent draws at its start (`agentapi.HeaderAgentInstanceID`), and the scheduler's restart count of the job in the registration (`RegisterRequest.RestartCount`, from `SLURM_RESTART_COUNT`). The kernel's boot ID is never used: a requeued job can land on the same node with no reboot. Registration is soft state: after a controller restart every agent registers again on its next poll, with the same ID, and no Instance fails and no generation rises.
- **The launch** always passes `--max-num-seqs`: the serving spec's value, else `v1alpha1.DefaultMaxNumSeqs` (256). `launch.maxNumSeqs` records it, and the front's cap, when `routing.instanceCap` is unset, is that times the Instance's workers (HBEP-5, HBEP-7).
- **The drain window** of an Instance is `v1alpha1.InstanceDrainWindow(profile, routing)`: `drainWindow`, or `routing.maxRequestDuration` plus `drainMargin` when that is shorter. The front and the lifecycle controller both call it, so both compute the same drain start (`v1alpha1.DrainStart`).
- **The image format.** Each partition's `imageFormat` (`sif` or `sandbox`) says what Apptainer runs on its compute nodes: `sif` on Helios GH200, `sandbox` on Athena A100 (spike S1, probe 2). It is not a deployment-ID input. The lifecycle controller sends `v1alpha1.ImageFormatOf(partition)` (`sif` when unstated) in `DesiredState.Settings.ImageFormat`, and the agent keeps the image at `agentapi.StagedImagePath(digest, format)`.
- **Agent builds** are a directory of `agentapi.AgentBuildName(<cpuArch>)` files. The lifecycle controller sets `agentDigest` to the SHA-256 of the build for the partition's `cpuArch`. The provider finds the file by that digest and stages it at `agentapi.StagedAgentPath(digest)`.
- **`agent.json`** is `agentapi.AgentConfig`, written by the provider from the launcher's front settings, the agent endpoint's port and certificate pin, and the paths it resolved.
  - In ssh mode its user is the user who runs the controller.
  - The launcher creates the endpoint's certificate and key, and binds the endpoint to loopback on a port from its own block.
- **Time limits** are durations in whole minutes. The lifecycle controller rounds `min(maxTime, leaseLimit)` down to a minute, and the provider refuses any other value before a call.
- **Restart generations** live in Instance annotations, `v1alpha1.RestartGenerationAnnotation(part)`. Part is `instance` for the whole Instance, or `worker-<i>`. The controller emits the restart's event first, then raises the generation by compare-and-swap.
- **Verification records** are one per verification, `ver-<instance UID>-<generation>`, with the generation counting from 1 (`v1alpha1.NewVerificationRecord`). The Instance's status names the latest. A record that already exists is read, never rewritten.

## Pinned versions

The Kubernetes libraries match crossplane-runtime v2.4.3, which HBEP-4's provider builds on. That way the provider needs no version bump:

| Dependency | Version |
|---|---|
| `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go` | v0.35.3 |
| `k8s.io/apiextensions-apiserver` | v0.35.0 |
| `sigs.k8s.io/controller-runtime` | v0.23.1 |
| `sigs.k8s.io/controller-tools` (`controller-gen`) | v0.20.0 |
| `github.com/crossplane/crossplane-runtime/v2` | v2.4.3 (Apache-2.0) |
| `sigs.k8s.io/yaml` | v1.6.0 (MIT, with BSD-3-Clause parts; direct, for `hack/mutate-cel`) |
| `github.com/crossplane/crossplane/apis/v2` | v2.3.4 (Apache-2.0), the version crossplane-runtime v2.4.3 requires |
| k3s (single-user and team host) | v1.36.5+k3s1 |
| `kube-apiserver`, `kube-controller-manager` (the assembled test host of `internal/testenv`) | v1.36.2, sha256-pinned in `hack/test-binaries.sh` |
| `etcd` (the assembled test host) | v3.6.8, sha256-pinned in `hack/test-binaries.sh` |

Kubernetes' explicit ±1-minor skew rule is written for kubectl. client-go instead documents compatibility through the APIs a client and a cluster share ([client-go compatibility](https://github.com/kubernetes/client-go/blob/v0.35.3/README.md#compatibility-client-go---kubernetes-clusters)). A 0.35 client against a 1.36 server is a reasonable choice for shared stable APIs, and the tests against k3s 1.36 check the APIs we use. A change to any of these pins is its own PR.

## Checks

- `hack/check.sh` runs gofmt, vet, `go mod tidy -diff`, the generated-file check and the tests, then lists every skipped test. Run it under `nice` and the shared build lock.
- `hack/mutate-cel.sh` neutralises each CEL admission rule in a copy of the CRDs and runs the test that covers it, which must fail. It takes about ten minutes, so it is not part of `hack/check.sh`; run it when a rule or its test changes. A rule with no entry in its table fails the script.
- The flag-name tests read the known flag names from HBEP-3's v1 table, so the admission rules' lists of known names are checked against the specification. A flag added to the table fails `TestEngineFlagsRefuseEveryPrefixOfAKnownFlag` until the list in the `EngineFlag` markers (`api/v1alpha1/modeldeployment_types.go`) gains it.
- The test host (`internal/testenv`) binds only loopback ports 29200 to 29219 and, in k3s mode, accepts only a kubeconfig whose server is loopback on 29200 to 29239. The control-plane binaries are always sha256-checked; `HPCBURST_TESTENV_UNPINNED=1` is the explicit override for a binary you built yourself.

## Running the e2e suite on the launcher's k3s

`hack/check.sh` runs the e2e suite on the assembled host of `internal/testenv`. The same suite also runs on the user-level k3s that `hpcburst up` starts, which is the host HBEP-11 specifies; `TestRunsOnK3s` then passes instead of skipping. Run it before merging a change to the CRDs:

```
hack/fetch-k3s.sh <bin-dir>      # once: the pinned k3s binaries
go build -o <tmp>/hpcburst ./cmd/hpcburst
<tmp>/hpcburst up --state-dir <state> --port-base 29220 --bin-dir <bin-dir> --manifests config/crd
HPCBURST_TEST_HOST=k3s HPCBURST_KUBECONFIG=<state>/kubeconfig go test -count=1 ./api/v1alpha1/e2e/
<tmp>/hpcburst down --state-dir <state>
```

The launcher's block is 29220 to 29239 and the suite refuses a kubeconfig that points elsewhere. The deletion-guard policy in `config/admission/` is not applied by `--manifests`: `TestCRDDeletionGuard` installs and removes it itself, so keep it out of the manifests directory.

## API field names

Every field of the CRDs is camelCase, as the Kubernetes API conventions ask (owner decision, PR #48): `holdRelease`, `lostGrace`, `imageDigest`, `gpuModel`, `timeZone`, `jobName.maxLength`, `cpusPerGpu`, `memPerGpu`, `gpusPerNode`. HBEP-3's hash object keeps its snake_case keys (`image_digest`, `gpu_model`, `cpu_arch`); `pkg/deploymentid` maps the API's fields onto them, and nothing in the API changes the hash. `TestEveryCRDFieldIsCamelCase` scans the committed manifests. HBEP text that writes a field in snake_case (`hold_release` in HBEP-11) means the camelCase field.

## Rules for every change

- **Dependencies:** each one's licence must be on HBEP-15's allowlist (Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC and the others it lists). Copied code goes under `third_party/forked/<project>/` with its upstream `LICENSE` and an `ORIGIN.md`.
- **Tests:** they come with the change. Expected values come from specifications or an independent implementation, never from the code under test.
- **Public content:** this repository is public. No host names, IP addresses other than loopback, job IDs, account or grant names, credentials or internal paths (HBEP-1).

## The launcher

`hpcburst` (`cmd/hpcburst`, code in `internal/launcher`) starts and supervises the single-user control-plane host of HBEP-11: the pinned k3s as the invoking user, with `server --rootless --disable-agent` and SQLite. It supports Linux only, because every health and stop decision is tied to `/proc`.

```
hpcburst up      --state-dir D --port-base 29220 [--version V] [--manifests DIR] [--front-mode direct|ssh --front-addr HOST[:PORT] [--front-key FILE]]
hpcburst status  --state-dir D [--json] [--no-api]
hpcburst upgrade --state-dir D --to V
hpcburst restore --state-dir D --backup D/backups/<backup>
hpcburst down    --state-dir D
```

- **State:** everything the launcher owns is in the state directory (default `$XDG_STATE_HOME/hpcburst`, else `~/.local/state/hpcburst`): k3s's data directory, home and log, the record of the processes it started (`state.json`, with PID and start time), and `kubeconfig` (mode 600, written only once the host is ready).
- **Binaries:** `up` and `upgrade` look for `k3s-X.Y.Z/k3s` under `--bin-dir` (or `$HPCBURST_BIN_DIR`, else `~/.cache/hpcburst/k3s`), open it, and check the sha256 of what they opened against `internal/launcher/k3s-pins.txt`. k3s is then executed from that same descriptor (`/proc/self/fd/3`), so the bytes that run are the bytes that were checked. `hack/fetch-k3s.sh [DIR]` downloads and checks them. The pins file is the one place that says which versions are allowed. HBEP-11 puts these checksums in `build/dependencies.yaml` (HBEP-15); when HBEP-15's check lands, the pins move there, and this file and `hack/fetch-k3s.sh` read them from it rather than keeping a copy.
- **Versions:** `store-version.json` in the state directory records the release that last wrote the store, and `down` keeps it. `up` without `--version` starts that release; `up` with another one is refused. A newer release goes through `upgrade` (the next minor, or a newer patch or k3s revision of the same minor), which backs the store up first. An older one only through `restore`, from a backup that `upgrade` took.
- **Backups and restore:** `upgrade` stops the host, copies `data/server/db` and the server token to `backups/<time>-<release>/` (mode 0600, synced), records the new release, and only then starts it. `restore` needs the host stopped. It moves the current store and token aside to `data/server/replaced-<time>/`, copies the backup back, and records the backup's release. Changes made after the backup are lost from the store.
- **Health:** `status` judges from the recorded processes, through a pidfd for each: PID, start time and boot ID must match. k3s must run a binary unpacked in its own data directory (k3s otherwise falls back silently to a system k3s's `/var/lib/rancher/k3s/data`). The process must hold exactly its loopback ports. Only then does it ask `/readyz`, the controller manager's `/healthz`, a small write to the store, and a garbage-collector probe (an owner and a dependent ConfigMap in namespace `hpcburst-launcher`; the dependent must be gone within a minute; `--no-gc-probe` skips it). Afterwards it checks the listeners and identities again. A listener that answers on a port but is not held by the recorded process never counts.
- **Processes:** the launcher signals only through a pidfd it verified (taken at launch from clone, or with `pidfd_open`), and reads exits from the pidfd. A process whose identity cannot be checked is never signalled, and `down` then exits 1 and keeps the record.
- **Audit:** the API server writes an audit log at the Metadata level to `logs/audit.log` in the state directory (no request or response bodies, so never a Secret's content), at most four files of 100 MB.
- **Exit codes:** 0 success, 1 failure (for `status`: unhealthy), 2 usage error, 3 (`status` only) stopped.
- **Front:** `up` records the front address and mode. When none is given it still starts, and warns that no jobs will be submitted until a front is configured: the lifecycle controller refuses to submit while no front is set (a later issue).

### Testing the launcher

`go test ./...` runs the unit tests. The tests that start a real k3s need the pinned binaries and skip, loudly, without them. To run everything:

```
hack/fetch-k3s.sh "$DIR" 1.35.9 1.36.5
HPCBURST_BIN_DIR="$DIR" hack/test-launcher.sh
```

The script sets `HPCBURST_REQUIRE_INTEGRATION=1`, which turns every skip of a required test into a failure (in `-short` mode too), and it exits non-zero if any test was skipped. The tests use loopback ports 29220–29239 and state directories under `$TMPDIR` (about 0.6 GB at peak, removed after each test). Set `HPCBURST_TEST_ROOT_K3S=1` on a machine that runs a system k3s: the tests then fail if it is missing, and assert after each test that it still listens on 6443, 6444 and 10250. Before starting k3s the tests wait for a quiet machine; `HPCBURST_TEST_MIN_AVAIL_GIB` (default 8) sets how much available memory that needs. The store-full test mounts a size-limited FUSE volume as the invoking user, so it needs `/dev/fuse` and `fusermount3`.
