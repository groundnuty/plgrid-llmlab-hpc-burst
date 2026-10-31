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
| `pkg/hubfiles/` | the checkpoint's per-file list from the Hugging Face Hub (paginated listing, `lfs.oid`, hashes of the small files, optional token file); its golden test runs with `HPCBURST_TEST_HUB=1` | HBEP-3 |
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
  - Each has its own leader `Lease`, `v1alpha1.ProviderLeaseName(<provider>)`, which is `provider-<name>` (`@hpc-burst-science-agent`, 31 Oct 2026).
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
| `internal/managed/` | #69 | the managed Provider's controller: `provider.Managed` for LLMLab, the catalog loop, and the LLMLab double. It reads the credentials file with `provider.ReadManagedCredentials`. Until it lands, `fake.Managed` and `fake.ManagedController` stand in for it |
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
| Allocation: annotations `external-name`, `submission`, `job-id`, `hold-lifted`, `read-back` (`v1alpha1.AnnotationReadBack`: the job ID whose settings matched in the read-back, no default); finalizer `cancel-all`; all of status, including `status.retention` (a mirror of the spec) | the Provider's controller (crossplane-runtime rewrites the whole status) |
| Allocation: annotation `abandon-attempt` | an operator |
| Allocation: annotation `idle-since` (`v1alpha1.AnnotationIdleSince`) | lifecycle controller, with a resourceVersion precondition (Runtime conventions, idle anchor) |
| Instance: creation, deletion, spec, status, annotations (`restart-<part>`, `predecessor`), finalizer `drain` | lifecycle controller, including its `Backend` |
| Instance: the condition `TunnelConnected` (`v1alpha1.ConditionTunnelConnected`) | lifecycle controller, only from frps's proxy list (HBEP-6, Path state): True `ProxyOnline`, False `ProxyOffline`, Unknown `ProxyListUnreadable`, never True on a list it could not read. An Instance is `ready` only with it True. One that is not ready at `health.startTimeout` while it is not True ends with `status.endReason` `TunnelNotConnected` (`v1alpha1.ReasonTunnelNotConnected`), otherwise `StartTimeout` (HBEP-7, Instance start) |
| Instance: `status.leaseEnd` | lifecycle controller: on every observe it copies the Allocation's `status.atProvider.hardEnd`, so a later hard end moves it. The front reads it and never routes to an Instance without it (HBEP-5, HBEP-7) |
| ModelDeployment status, including a managed source's `Ready` and `Progressing` and the reason `ProviderNotOnPlatform` | lifecycle controller. The front only filters: it routes a managed entry to no Provider off `spec.source.managed.platform` |
| Provider status, the managed `catalog` and `lastCatalogRead` included | the Provider's controller, under `hpcburst-provider`. It keeps every condition it does not own |
| Provider status: the condition `DialHomeUnreachable` | lifecycle controller, by server-side apply to the status subresource under `hpcburst-lifecycle`, with forced ownership, owning only that list entry (`v1alpha1.DialHomeUnreachableApply`). Neither writer removes the other's condition (`@hpc-burst-science-agent`, 31 Oct 2026) |
| identity and verification records | lifecycle controller (its verification controller) |
| PlatformProfile status (`lastChecked`) | nobody in alpha: no component checks a profile's facts yet |

### Runtime conventions

- **The host layout.** `agentapi.Host{StateDir, PortBase}` names every path and port that several components share on the control-plane host. The launcher owns the state directory and its loopback port block, and passes both to the controller binary and the front (#61 and #64 fill a `Host` from them):
  - `Host.CredentialsDir()`, `<state>/credentials`, mode 700: the controller's credentials directory;
  - `Host.AgentBuildsDir()`, `<state>/agent-builds`: the agent builds;
  - `Host.AgentCertFile()` and `Host.AgentKeyFile()`, `<state>/agent-endpoint/tls.crt` and `tls.key` (key mode 600): the agent endpoint's certificate, which the launcher creates. #60's server and, in direct mode, the front's dial-home listener present it;
  - `Host.AgentEndpointPort()`, the port base plus 3 (the launcher itself uses base, base+1, base+2 and base+5), bound on loopback by #60's server; `Host.AgentEndpointAddr()` is the front's passthrough target.
  - `Host.DialHomePort()`, the port base plus 7, is the front's default dial-home listener in ssh dial-home mode (`Host.DialHomeAddr()`, on loopback; `@hpc-burst-science-agent`, 31 Oct 2026). Each platform's restricted VM key permits `-L` forwards to exactly the agent endpoint's port and this one, so **a launcher started with another port base needs new keys**. In direct mode the dial-home listener's address stays required and explicit (compute nodes must reach it), and this default does not apply.
  - The frp server's four loopback ports (HBEP-6, Loopback ports), from the base: `Host.FrpsPluginPort()` base+8 (`FrpsPluginPortOffset`), the server plugin's plain HTTP endpoint at `FrpsPluginPath`, which the lifecycle controller binds; `Host.FrpsBindPort()` base+9, frps's bind port, where the front's dial-home listener passes frpc's websocket; `Host.FrpsTCPMuxPort()` base+10, the tcpmux `CONNECT` port the front dials; `Host.FrpsWebPort()` base+11, the web server whose API lists the proxies. Each has an `...Addr()` helper, `127.0.0.1:<port>`. The launcher starts frps, which binds base+9 to base+11 (the launcher's `PortsFromBase` range check, which stops at base+5, grows with that item).
  - `Host.FrpsWebPasswordFile()`, `<state>/frps/web-password`, mode 600 (the directory 700): the password of frps's web server (`FrpsWebUser`, `hpcburst`). It has a key file's form (64 lowercase hex characters, no newline): the launcher writes it with `agentapi.NewKey` and `agentapi.WriteKeyFile` and puts the same password in frps's configuration; the lifecycle controller reads it with `agentapi.ReadKeyFile` to list the proxies.
  - The whole block at the default base 29220: base API, +1 HTTPS, +2 controller manager, +3 agent endpoint, +4 front `/healthz`, +5 load balancer (reserved, never bound; `internal/launcher/ports.go`), +6 lifecycle controller `/healthz`, +7 dial-home listener (ssh mode), +8 frps plugin, +9 frps bind, +10 frps tcpmux, +11 frps web server. Of these, +4 and +6 are assigned to #64 and #61, and +8 to +11 to the tunnel items of #90; none of them is bound on main yet.
- **Key files.** Both kinds of key file in the credentials directory have one form: 64 lowercase hex characters (256 random bits), no newline, mode 600, owned by the controller's user. They are written with `agentapi.WriteKeyFile` (temporary file, fsync, rename) and read with `agentapi.ReadKeyFile`, which refuses any other form, a file with any permission bit for its group or others or one its owner cannot read (so modes 600 and 400 pass, and 640 or 200 do not), a file owned by another user, a symlink and anything that is not a regular file.
  - `agentapi.AllocationCredentialPath(dir, <Allocation UID>)` holds each Allocation's credential. The lifecycle controller writes it as soon as the create returns the UID. Until the file exists, the provider's Create waits and requeues: it neither submits nor marks the attempt Rejected. The provider reads the file to stage it (`alloc/<tag>/credential`), and the agent API reads it to authenticate.
  - `agentapi.InstanceKeyPath(dir, <Instance UID>)` holds each Instance's key. The agent generates it with `agentapi.NewKey` and sends it in `InstanceReport.Key` until `KeyStored`; the lifecycle controller checks it (`agentapi.CheckKey`) and writes it; the front reads it and sends `Authorization: Bearer <key>` to the Instance.
- **A managed Provider's credentials file** is `provider.ManagedCredentials`: one JSON object with exactly `key`, `baseURL` and an optional `expiry` (RFC 3339), member names exact, mode 600 (HBEP-4, Provider settings). `baseURL` is the gateway's OpenAI-style base URL with its version segment, for example `https://gateway.example/v1`; a request for the front's `/v1/<rest>` goes to `baseURL/<rest>` (`URLFor`), and the catalog is `baseURL/models` (`CatalogURL`). https only, or http on loopback. #69 and #64 both read it with `provider.ReadManagedCredentials`; `Expiring` says when `CredentialsExpiring` applies (a day before the expiry).
- **Dial-home settings in `agent.json`.** `Mode` and `Endpoint` are the front the launcher recorded (`hpcburst up --front-mode --front-addr`); in ssh mode `SSH.User` is the user who runs the controller and `SSH.AgentPort` is `Host.AgentEndpointPort()`; `CertSPKISHA256` is `agentapi.PinOf` of the certificate at `Host.AgentCertFile()`.
- **The agent's identity** is a random agent instance ID that the agent draws at its start (`agentapi.HeaderAgentInstanceID`), and the scheduler's restart count of the job in the registration (`RegisterRequest.RestartCount`, from `SLURM_RESTART_COUNT`). The kernel's boot ID is never used: a requeued job can land on the same node with no reboot. Registration is soft state: after a controller restart every agent registers again on its next poll, with the same ID, and no Instance fails and no generation rises.
- **The launch** always passes `--max-num-seqs`: the serving spec's value, else `v1alpha1.DefaultMaxNumSeqs` (256). `launch.maxNumSeqs` records it, and the front's cap, when `routing.instanceCap` is unset, is that times the Instance's workers (HBEP-5, HBEP-7). The controller renders every flag as one `--name=value` argument, and a CEL rule on the launch refuses a max-num-seqs argument in any other form or with another value than `launch.maxNumSeqs`.
- **The drain window** of an Instance is `v1alpha1.InstanceDrainWindow(profile, routing)`: `drainWindow`, or `routing.maxRequestDuration` plus `drainMargin` when that is shorter. The front and the lifecycle controller both call it, so both compute the same drain start (`v1alpha1.DrainStart`).
- **The image format.** Each partition's `imageFormat` (`sif` or `sandbox`) says what Apptainer runs on its compute nodes: `sif` on Helios GH200, `sandbox` on Athena A100 (spike S1, probe 2). It is not a deployment-ID input. The lifecycle controller sends `v1alpha1.ImageFormatOf(partition)` (`sif` when unstated) in `DesiredState.Settings.ImageFormat`, and the agent keeps the image at `agentapi.StagedImagePath(digest, format)`.
- **Agent builds** are a directory, `Host.AgentBuildsDir()`, of `agentapi.AgentBuildName(<cpuArch>)` files. The lifecycle controller sets `agentDigest` to the SHA-256 of the build for the partition's `cpuArch`. The provider finds the file by that digest and stages it at `agentapi.StagedAgentPath(digest)`.
- **`agent.json`** is `agentapi.AgentConfig`, written by the provider from the launcher's front settings, the agent endpoint's port and certificate pin, and the paths it resolved.
  - In ssh mode its user is the user who runs the controller.
  - The launcher creates the endpoint's certificate and key, and binds the endpoint to loopback on a port from its own block.
- **Time limits** are durations in whole minutes. The lifecycle controller rounds `min(maxTime, leaseLimit)` down to a minute, and the provider refuses any other value before a call.
- **Restart generations** live in Instance annotations, `v1alpha1.RestartGenerationAnnotation(part)`. Part is `instance` for the whole Instance, or `worker-<i>`. The controller records the restart in `status.restarts` (`v1alpha1.InstanceRestart`: part, generation, time, signal; entries within `restart.window`, at most 16) and emits its event first, then raises the generation by compare-and-swap. The attempts per rung are counted from `status.restarts`, so they survive a controller restart.
- **Provider readiness.** A batch Provider's controller reads the platform at least every `v1alpha1.ProviderPollInterval` (1 min): on a pass that observes no job it calls `provider.Batch.Poll`. `Ready` is True with reason `Polled` after a pass whose reads all succeeded; after a failed pass whose last success is older than `v1alpha1.ProviderReadyStaleness` (5 min), or with none, it is False with `ReadsFailing`. The lifecycle controller judges with `v1alpha1.ProviderReady(provider, now)`, which also checks the age, so a stopped Provider controller stops counting as ready.
- **Queue waits** are recorded once per job: when the recorded job is first seen with a start time and `status.atProvider.queueWaitRecorded` is false, the Provider's controller adds the wait to the Provider's status (`v1alpha1.AddQueueWait`, keyed by job ID), and only after that write succeeded sets `queueWaitRecorded`. A failed write is retried at the next observe.
- **The idle anchor.** Retention's `idleLimit` and the profile's `maxIdle` count from the Allocation's `idle-since` annotation (`v1alpha1.IdleSince`), which only the lifecycle controller writes: the latest `status.endTime` of the Allocation's Instances when its last Instance ends, or the job's start when the job starts with no Instance placed; it is removed when an Instance is placed. It survives a controller restart and the deletion of the Instance.
- **Warm-up gates ready.** The lifecycle controller marks an Instance ready only after a report whose `WarmUp.Done` is true for the current verification generation: it judges settling from the rounds, sends a desired state without `WarmUp`, and the agent cancels in-flight warm-up requests and reports Done (`agentapi.Checks.WarmUp`).
- **Report size.** `workload.targetSessions` is at most 1024 (`v1alpha1.MaxTargetSessions`, `agentapi.MaxTargetSessions`), and the agent cuts a timing's error to `agentapi.MaxTimingErrorBytes` and other messages to `agentapi.MaxMessageBytes`, so the warm-up results of all five rounds take at most `agentapi.WarmUpBudgetBytes` (3 MiB) of the 4 MiB body limit. Both sides encode and decode bodies with `agentapi.EncodeBody` and `agentapi.DecodeBody`, which apply the limit. Bodies and the Authorization header are never logged; `Redacted()` gives a desired state or report without its secrets.
- **Health metrics can be unknown.** `agentapi.Health`'s gauges (`RunningRequests`, `WaitingRequests`, `GeneratedTokens`, `KVCacheUsage`) are pointers, and `ResponsesByStatus` is a map: a value the agent did not read is absent, which decodes to nil and means unknown, never zero. `Health.MetricsReadAt` is the time of the last successful `/metrics` read, by the agent's clock. The lifecycle controller takes no gauge-based action (idle, KV pressure, the only-errors and no-progress signals) while the metrics are not fresh, that is while `MetricsReadAt` is nil or older than `agentapi.MetricsStaleProbeIntervals` (2) probe intervals. `Health.MetricsFresh(reportTime, receivedAt, now, probeInterval)` judges three ages, each at most two intervals: the read when the sample was taken (`Time` minus `MetricsReadAt`), the sample when its Report was made (`reportTime` minus `Time`), both on the agent's clock so skew does not enter, and the Report since the controller received it (`now` minus `receivedAt`, on the controller's clock). The last two close the gap of a cached sample that an agent keeps reporting. The controller also compares two samples' counters only when `MetricsReadAt` advanced. `ResponsesByStatus` has no `omitempty`: nil is unknown (`null` on the wire), `{}` is a read that found no responses. `@hpc-burst-science-agent`, 31 Oct 2026. The fakes' Health doubles read their metrics at every sample; `HealthMetricsUnreadable`, `HealthMetricsStale` and `HealthCached` model the three ways they are not.
- **Check outcomes carry a reason code.** A failed check's `agentapi.CheckOutcome` has `Reason`, a CamelCase code from the `agentapi.Reason*` constants (`pkg/agentapi/checks.go`: the corruption check's four from `agentapi.JudgeCorruption`, the answer failures, `Loop` and `ReasoningBudgetExhausted`), and `Message` stays human text. The lifecycle controller matches on `Reason` and never on `Message`. The behaviour check also reports `ContextTokens`, the prompt length it actually ran at, which the controller records. Without a reasoning parser that is `contextTokens.longest`; with one the agent shortens it to max-model-len minus the template overhead minus the typical output minus 4,096 (`agentapi.ReasoningHeadroomTokens`), never above `longest` and never below `typical` (`agentapi.BehaviourContextTokens`). `@hpc-burst-science-agent`, 31 Oct 2026.
- **Engine doubles.** The Go result doubles in `pkg/agentapi/fake` drive the fake agent only. #60's real agent needs its own socket-level, OpenAI-compatible engine double (HBEP-7: an engine double behind a real agent); it lives with #60's tests.
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

### The licence check

**Disabled since 31 Oct 2026.** The owner dropped licence checking ("I don't care about those licenses, so feel free to disable this job for now"), so both jobs below carry `if: false`. The scripts and fixtures in `hack/` are kept, and deleting the two `if: false` lines switches the check back on. Provenance is still recorded: every reuse names its origin and URL.

When enabled, HBEP-15's licence check runs in two CI jobs, **not yet required checks**: they become required only after they have been green on main (HBEP-15's protocol), and a matrix job's name, `licences (<module>)`, changes when a module is added.

- **`licence-modules`** runs the tests of the licence scripts (`hack/test_licence*.py`, Python standard library only, no Go; run them with `python3 -I -m unittest discover -s hack -p 'test_licence*.py' -v`), then `hack/test-licence-fixtures.sh` with the pinned go-licenses (see below), then `hack/licence-modules.sh --json .`. That lists every `go.mod` outside the top-level `spikes/`, found by a search of the whole tree, so a module added anywhere is checked without editing the workflow. Spikes are outside the check (HBEP-15). The search refuses, and names, a directory symlink anywhere outside `spikes/` (find does not follow one, so a module behind it would be left out), and a module directory whose name has a newline, another control character or a character outside `A-Z a-z 0-9 . _ - /`.
- **`licences (<module>)`** runs `hack/licence-check.sh` once per module, with the Go version that module's `go.mod` names (`third_party/forked/kubeai` names a newer one than the root) and go-licenses v2.0.1, against `hack/allowed-licenses.txt`, HBEP-15's list (a test compares the two). There is no exemption: a module's own packages need a `LICENSE`, the root's for the root module and the copy's own under `third_party/forked/`. The root module is also checked with `--include_tests`, and with a negative control that takes BSD-3-Clause off the list and must fail. A failing module does not stop the others.
- **No empty run.** go-licenses exits 0 over a module with no buildable package having checked nothing, so a run that saw no package fails unless `hack/known-zero-modules.txt` lists the module with a reason. Today that is `third_party/forked/kcp`: its one Go file is behind the `s3embedded` build tag, so no package matches. HBEP-15's copy check (Test plan) is meant to cover the copy; it is planned and does not run in CI yet. Add a line `<module dir>: <reason>` only when an empty run is expected, never to make a run pass.
- **Fixtures.** `hack/test-licence-fixtures.sh <go-licenses>` runs `hack/licence-check.sh` with the real go-licenses on five throwaway repositories whose licences are written by hand: an MIT dependency (the passing control, which must show in the report), an off-list BSD-4-Clause one and an unknown one (each must be rejected, naming its library), a package whose own `LICENSE` is unreadable beside a readable one at its module's root (go-licenses logs the error and exits 0; the gate must fail it), and the same dependency without the unreadable file. It takes about 15 seconds, offline. It is ported from spike S12 (`spikes/s12/licence-fixtures.sh`).
- **Fails closed.** `licence-check.sh` checks every write it makes (temporary directory, report template, a module's section, the output files); a write that fails exits non-zero and never prints `licence check: PASS`.

To run it on your machine, install `github.com/google/go-licenses/v2@v2.0.1` and call `hack/licence-check.sh --negative-control <go-licenses> . <output dir> [module dir ...]`. With no module dir it finds the modules itself. Use a Go toolchain at least as new as the newest `go.mod` it checks, or `third_party/forked/kubeai` fails.

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
