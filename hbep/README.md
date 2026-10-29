# HBEPs: hpc-burst enhancement proposals

Design decisions for this framework are made as HBEPs, modelled on Kubernetes Enhancement Proposals. [HBEP-1](0001-process/README.md) defines the process.

| HBEP | Title | Status | Stage |
|---|---|---|---|
| [1](0001-process/README.md) | Enhancement proposal process | implementable | none |

## Starting an HBEP

1. Open an issue titled `HBEP: <title>` with the `hbep` label. Its number is the HBEP number.
2. Copy `template/` to `NNNN-short-title/`, with the number zero-padded to four digits.
3. Set `status: provisional` in its `metadata.yaml`, add a row above, and open a PR.
