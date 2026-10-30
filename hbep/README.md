# HBEPs: hpc-burst enhancement proposals

Design decisions for this framework are made as HBEPs, modelled on Kubernetes Enhancement Proposals. [HBEP-1](0001-process/README.md) defines the process.

| HBEP | Title | Status | Stage |
|---|---|---|---|
| [1](0001-process/README.md) | Enhancement proposal process | implementable | none |
| [2](0002-scope-and-user-story/README.md) | Scope and user story | provisional | none |
| [3](0003-resource-model-and-naming/README.md) | Resource model and naming | provisional | none |
| [4](0004-provider-interface/README.md) | Provider interface and profiles | provisional | none |
| [5](0005-llmlab-compatible-front/README.md) | LLMLab-compatible front | provisional | none |
| [6](0006-instance-reachability/README.md) | Instance reachability | provisional | none |
| [7](0007-lifecycle-controller/README.md) | Lifecycle controller | provisional | none |
| [8](0008-observability-and-events/README.md) | Observability and events | provisional | none |
| [9](0009-identity-and-attribution/README.md) | Identity and attribution | provisional | none |
| [10](0010-agent-primitives/README.md) | Agent primitives | provisional | none |
| [11](0011-kubernetes-control-plane/README.md) | Kubernetes as the control plane | provisional | none |
| [15](0015-dependencies-and-provenance/README.md) | Dependencies and provenance | provisional | none |

## Starting an HBEP

1. Open an issue titled `HBEP: <title>` with the `hbep` label. Its number is the HBEP number.
2. Copy `template/` to `NNNN-short-title/`, with the number zero-padded to four digits.
3. Set `status: provisional` in its `metadata.yaml`, add a row above, and open a PR.
