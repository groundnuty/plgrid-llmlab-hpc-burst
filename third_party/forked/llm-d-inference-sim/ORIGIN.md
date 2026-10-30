# llm-d-inference-sim: loopback bind patch

- Origin: https://github.com/llm-d/llm-d-inference-sim, tag v1.0.0, commit 7bda03f9fef85139617cb4f724dccafe1cf8cbbf. Licence: Apache-2.0 (LICENSE in this directory is upstream's).
- SPDX licence: Apache-2.0.
- Date copied: 2026-10-30.
- Why copied rather than imported: the change is a bind-address patch to a binary we build and run, not a library we import; upstream has no option for it, so a patch applied at build time is the smallest change.
- Used by: spike S2 (`spikes/s2`), as an engine double.
- Change: `loopback-bind.patch` makes the HTTP listener bind `127.0.0.1:<port>` instead of all interfaces
  (`pkg/communication/http.go`, one line). Upstream has no bind-address option. Nothing else is changed.
- Apply: `git apply loopback-bind.patch` in a checkout of the commit above; `spikes/s2/scripts/setup.sh` does this.
