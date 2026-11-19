# plgrid-llmlab-hpc-burst

A framework for bursting LLM serving from PLGrid LLMLab onto HPC clusters, behind one endpoint that behaves like LLMLab's.

## What it is

You run a small control plane on your own Linux machine, as a plain user. In it you declare where a model may run (a Provider: a Slurm partition you hold a grant on, or LLMLab itself) and which model you want (a ModelDeployment). The framework submits and supervises the Slurm jobs, starts vLLM in them, and serves every Instance behind one OpenAI-style endpoint, with LLMLab as a route beside your own Instances. A client changes only its base URL and its key.

It's for researchers who outgrow LLMLab's shared capacity and hold HPC grants, and for the agents that run deployments for them. It also records which deployment answered each request, so a result can say exactly which model produced it ([HBEP-2](hbep/0002-scope-and-user-story/README.md) tells the user story).

## Features

Each feature below is on `main` today. The labels follow the HBEPs and the guides:
- **alpha**: part of alpha, and in use as described;
- **beta, on**: a beta feature that is on by default;
- **beta, opt-in**: a beta feature that stays off until you switch it on, or that acts only when a request or an object asks for it.

### One endpoint

- **OpenAI-style API** with a key per client: `POST /v1/chat/completions`, `/v1/completions`, `/v1/responses` and `/v1/embeddings`, and `GET /v1/models`. *alpha* ([HBEP-5](hbep/0005-llmlab-compatible-front/README.md))
- **Model names at three levels:** the plain name, a Provider (`<model>@<provider>`), or one exact deployment (`<model>@<provider>/<deployment-id>`). *alpha* ([HBEP-3](hbep/0003-resource-model-and-naming/README.md))
- **Sessions stay on their home Instance,** placed by a conversation key on a consistent-hash ring with bounded loads, so the engine's prefix cache is reused. *alpha* ([HBEP-5](hbep/0005-llmlab-compatible-front/README.md))
- **LLMLab as overflow:** a managed Provider attaches a model that LLMLab already serves, under the same plain name. *alpha* ([quickstart](docs/quickstart.md#optional-attach-llmlab))
- **Attribution on every response,** in headers that name the Provider, the deployment ID and the Instance that answered. *alpha* ([HBEP-9](hbep/0009-identity-and-attribution/README.md))
- **A usage record for every request,** and a queue in front of full Instances. *alpha* ([HBEP-5](hbep/0005-llmlab-compatible-front/README.md))
- **One retry:** a request that fails before any response byte goes once more to another ready Instance of yours, never to LLMLab. *beta, on* ([release notes](docs/releases/v0.1.0-alpha.2.md))
- **Workloads:** a request names its class, by a header or a `#<workload>` suffix. Each Workload has a token budget per Instance, taken from the engine's KV cache, a route order (`OwnFirst`, or `ManagedFirst` with `managedSessions`), and a strict FIFO of its own. *beta, opt-in* ([operations](docs/operations.md#workloads-beta), [HBEP-764](hbep/0764-single-study-workloads/README.md))
- **Sticky routes:** a Workload with `sessionAffinity: Route` pins each conversation to one target, LLMLab included, and writes the pin to disk before it sends the request. *beta, opt-in; on `main`, not in v0.1.0-alpha.2* ([operations](docs/operations.md#workloads-beta))

### Lifecycle

- **From a ModelDeployment to served Instances:** the controller submits Allocations (Slurm jobs), and the agent in each job stages the engine image and the checkpoint itself. *alpha* ([HBEP-7](hbep/0007-lifecycle-controller/README.md))
- **Verified before it serves:** each Instance is checked against its deployment's identity, then qualified against the profile's expected performance and warmed up. *alpha* ([HBEP-7](hbep/0007-lifecycle-controller/README.md), [HBEP-9](hbep/0009-identity-and-attribution/README.md))
- **Handover before the time limit:** a successor takes over before the job ends, and the streams in flight finish. A partition that runs one job at a time gets a no-overlap mode. *alpha* ([HBEP-7](hbep/0007-lifecycle-controller/README.md))
- **Restarts by rung,** the worker and then the whole Instance, with a pause when a class keeps failing (`RestartLoop`), and a retention policy for Allocations left without an Instance. *alpha* ([operations](docs/operations.md#why-a-modeldeployment-is-degraded))
- **Over-allocation:** one extra Allocation per Provider, qualified, ranked and kept as warm capacity (`overAllocation: 1`). *beta, on* ([HBEP-7](hbep/0007-lifecycle-controller/README.md))
- **Hedging across Providers** when a start is predicted to be late (`hedgeWidth: 2`). *beta, on* ([operations](docs/operations.md#hedging-across-providers-beta))
- **Several workers per Instance** behind an Envoy router (`hpcburst up --instance-router envoy`, `workersPerInstance`). *beta, opt-in* ([operations](docs/operations.md#the-router-and-several-workers-beta))

### Control plane and commands

- **A single-user host:** a user-level k3s, the controllers, the front and the tunnel server, on loopback, with no root. `hpcburst up`, `status`, `down`, `upgrade` and `restore` run it, and `up` installs a guard against deleting the framework's CRDs. *alpha* ([HBEP-11](hbep/0011-kubernetes-control-plane/README.md))
- **Agent identities, the capacity guard and the API server's audit log:** `up` installs the guard and the log, and `hpcburst identity add` gives an agent its own kubeconfig. *beta, on* ([operations](docs/operations.md#giving-an-agent-its-own-identity))
- **Operations with a plan, a confirm and approvals** (`hpcburst scale`, `end`, `release`, `confirm`, `approve`), and one inventory query (`hpcburst inventory`). *beta, opt-in* ([agent commands](docs/agent-cli.md), [HBEP-10](hbep/0010-agent-primitives/README.md))
- **Accounting:** a record for every Allocation the controller deletes (*alpha*), and `hpcburst accounting`, which totals the records per grant (*beta*). ([operations](docs/operations.md#where-the-accounting-records-are))

### Reaching the jobs

- **Three dial-home modes:** `direct` (each job dials your host), `ssh` (each job reaches it through a gateway, with a restricted key) and `host-dialed` (your host dials each job through the login node, so no inbound route is needed). Each Instance's tokens run through its own frp tunnel. *alpha* ([HBEP-6](hbep/0006-instance-reachability/README.md))
- **Bulk path:** a second tunnel for large non-streamed replies (`hpcburst up --bulk-path`). *beta, opt-in* ([operations](docs/operations.md#the-bulk-path-beta))
- **Relay keeper:** a controller restart cuts no host-dialed stream (`hpcburst up --relay-keeper`). *beta, opt-in* ([HBEP-6](hbep/0006-instance-reachability/README.md))

### Providers and identity

- **Slurm Providers over ssh,** with a platform profile for each cluster. Athena is the *alpha* platform; Helios and Sano are *beta*. Sano's profile picks the QOS by the job's GPU count. ([HBEP-4](hbep/0004-provider-interface/README.md), [profiles](profiles/README.md))
- **A managed Provider for LLMLab** reads LLMLab's catalog once a minute and routes to it. It starts nothing. *alpha* ([HBEP-4](hbep/0004-provider-interface/README.md))
- **Deployment IDs:** a SHA-256 over the deployment's identity record, with the checkpoint revision and the engine image digest pinned. vLLM's boolean flags are rendered by the engine image's own parser. *alpha* ([HBEP-3](hbep/0003-resource-model-and-naming/README.md))

### Observability and releases

- **Conditions, typed events and Prometheus metrics** from the front and the lifecycle controller, and `hpcburst status` for the host. *alpha* ([HBEP-8](hbep/0008-observability-and-events/README.md), [operations](docs/operations.md#reading-hpcburst-status))
- **A durable event log** (embedded NATS JetStream) with a rolling copy of the usage records (`hpcburst up --event-log`). *beta, opt-in* ([operations](docs/operations.md#the-event-log-beta))
- **Release archives** that need no clone: the host binaries, the pinned k3s and frps, the agent builds for `x86_64` and `aarch64`, and the CRDs, profiles and examples, with `SHA256SUMS`. *alpha* ([HBEP-15](hbep/0015-dependencies-and-provenance/README.md))

## Providers, and what has run on each

| Provider | Label | Run live so far |
|---|---|---|
| Cyfronet Athena, `plgrid-gpu-a100` | alpha | On `plgrid-now`, the one-job test partition on the same A100 nodes, in host-dialed mode: staging, serving with attribution and a worker restart, the one-slot handover, and GPU accounting ([v0.1.0-alpha.1](docs/releases/v0.1.0-alpha.1.md)). Beta's [router](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/522#issuecomment-6879687542), [relay-keeper](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/522#issuecomment-6880186348) and [bulk-path](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/522#issuecomment-6882612340) rows passed there on 14 Nov 2026, the bulk path with one open finding. Still open, on `plgrid-gpu-a100`: the handover with two jobs at once, and the one-hour tunnel run. |
| Sano, `gpu` (A100 80GB) | beta | Host-dialed mode: [staging](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/735#issuecomment-6977050910), [serving](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/735#issuecomment-6978367684), and a [handover](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/735#issuecomment-6981304247) with two jobs at once (18 Nov 2026). Direct mode: [serving](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/735#issuecomment-6998371105), and the [LLMLab baseline row](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/764#issuecomment-6998612797), with overflow to LLMLab ([v0.1.0-alpha.2](docs/releases/v0.1.0-alpha.2.md)). Sano never uses `ssh` mode. |
| Cyfronet Helios, `plgrid-gpu-gh200` | beta | Its conformance (a held Allocation's round trip, a 56-character job name) was met on 8 Nov 2026 ([alpha runbook](docs/alpha-runbook.md#alpha-rows-and-beta-rows)). Its serving rows are beta's ([#373](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/pull/373)), to run when the GH200 nodes return. |
| PLGrid LLMLab | alpha (managed) | The overflow route of the Sano [baseline row](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/764#issuecomment-6998612797) ([v0.1.0-alpha.2](docs/releases/v0.1.0-alpha.2.md)). |

## The current release

**[v0.1.0-alpha.2](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/releases/tag/v0.1.0-alpha.2)** is a GitHub pre-release, for Linux on x86_64. [Its notes](docs/releases/v0.1.0-alpha.2.md) say what it adds, what was measured, and which limits still apply.

- **From the archive:** download `hpcburst-v0.1.0-alpha.2-linux-amd64.tar.gz` and `SHA256SUMS` from the release page, then follow the [quickstart](docs/quickstart.md#release-archive-v010-alpha2). No clone is needed.
- **From source:** check out the tag `v0.1.0-alpha.2` and build as the quickstart's step 2 says, with Go 1.26. This is also the way while the release page is not up.
- **`v0.1.0-alpha.3` follows.** It adds sticky routes (beta, opt-in), which are on `main` now.

## Documentation

- [Quickstart](docs/quickstart.md): from a clone or the release archive to one request through the front.
- [Operations guide](docs/operations.md): what `hpcburst status` and each condition mean, and what to do.
- [Agent commands](docs/agent-cli.md): Operations, approvals and the inventory.
- [Alpha runbook](docs/alpha-runbook.md): the live checks on the clusters.
- [HBEP index](hbep/README.md): the design decisions, as enhancement proposals.
- [Examples](examples) and [platform profiles](profiles/README.md): the manifests the quickstart applies.
- [Development](docs/development.md): the layout, the pins, the checks and the rules for a change.
- [Release notes](docs/releases).

## Status and limits

- **It's an alpha.** The API is at `v1alpha1`. Alpha's last criteria wait on Athena's `plgrid-gpu-a100` queue ([#145](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/145)), and Helios's on the GH200 nodes.
- **One study is its first user.** Workloads and sticky routes exist for that study ([HBEP-764](hbep/0764-single-study-workloads/README.md)). Several studies sharing one front is out of scope for now.
- **One user, one host.** The control plane is a single-user k3s on one machine. A team option isn't part of alpha.
- **The release archive is for Linux on x86_64.** Its agent builds cover `x86_64` and `aarch64` compute nodes.
- **The engine is vLLM.** The framework is written against v0.32.0, and other versions run per deployment, by image digest.
- **Not built yet:** HBEP-10's event subscriptions, reservations and owner-lease expiry, which come at stable, and a tool server for language-model agents. Billing is recorded, but nothing charges against a grant.

The [quickstart's limits](docs/quickstart.md#alphas-limits) and the release notes give the rest.

## Licence

Apache-2.0; see [`LICENSE`](LICENSE). Copied code under `third_party/forked/` keeps its own licence.
