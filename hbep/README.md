# HBEPs: hpc-burst enhancement proposals

Design decisions for this framework are made as HBEPs, modelled on Kubernetes Enhancement Proposals. [HBEP-1](0001-process/README.md) defines the process.

| HBEP | Title | Status | Stage |
|---|---|---|---|
| [1](0001-process/README.md) | Enhancement proposal process | implementable | none |
| [2](0002-scope-and-user-story/README.md) | Scope and user story | provisional | none |
| [3](0003-resource-model-and-naming/README.md) | Resource model and naming | implementable | none |
| [4](0004-provider-interface/README.md) | Provider interface and profiles | provisional | none |
| [5](0005-llmlab-compatible-front/README.md) | LLMLab-compatible front | provisional | none |
| [6](0006-instance-reachability/README.md) | Instance reachability | provisional | none |
| [7](0007-lifecycle-controller/README.md) | Lifecycle controller | provisional | none |
| [8](0008-observability-and-events/README.md) | Observability and events | provisional | none |
| [9](0009-identity-and-attribution/README.md) | Identity and attribution | provisional | none |
| [10](0010-agent-primitives/README.md) | Agent primitives | provisional | none |
| [11](0011-kubernetes-control-plane/README.md) | Kubernetes as the control plane | implementable | none |
| [15](0015-dependencies-and-provenance/README.md) | Dependencies and provenance | provisional | none |

## Starting an HBEP

1. Open an issue titled `HBEP: <title>` with the `hbep` label. Its number is the HBEP number.
2. Copy `template/` to `NNNN-short-title/`, with the number zero-padded to four digits.
3. Set `status: provisional` in its `metadata.yaml`, add a row above, and open a PR.

## Spikes

Spikes are short experiments allowed from provisional HBEPs (HBEP-1). They settle open questions that the HBEPs cite as "spike Sx". They come from the state-of-the-art evaluation of 28 Oct 2026. A spike gets an issue when it is scheduled.

| Spike | Question | HBEPs | Issue |
|---|---|---|---|
| S1 | Does a dial-home tunnel (frp) from Helios and Athena compute nodes carry long streams? | 6 | #18 |
| S2 | Which router inside an Instance (llm-d endpoint picker with Envoy, vllm-router, or none) keeps prefix-cache affinity and survives 2 h streams? | 5, 6 | #29 |
| S3 | Which single-user control-plane host (k3s with SQLite, or kcp) fits a small VM in memory, start-up and upgrades? | 3, 10, 11 | #26 |
| S4 | Can a Slurm provider built on crossplane-runtime run outside a cluster, and handle purged finished jobs and interrupted submits without duplicates? | 4 | #24 |
| S5 | Login-node facts per platform: Slurm interfaces, requeue, time zone, containers, login-node policy | 4, 6, 7 | #17 (done, PR #19) |
| S6 | Does an Agent Router edge pass bodies and long streams through unchanged and accept an LLMLab-format key? | 5 | not scheduled |
| S7 | How long does a per-file checkpoint hash take on parallel filesystems, and does it match the Hub's hashes? | 9 | #30 (done, PR #33) |
| S8 | Does embedded NATS JetStream lose no acknowledged record across a crash, a slow subscriber and redelivery? | 8, 10 | #25 (done, PR #28) |
| S9 | Do metrics scraped through the front and tunnel show a gone Instance as down, never as a stale zero? | 8 | not scheduled |
| S10 | Which conversation key and load bound give a good prefix-cache hit rate without overloading an Instance? | 5 | #27 |
