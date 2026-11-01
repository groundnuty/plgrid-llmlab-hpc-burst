# llm-d-inference-sim: loopback bind patch

- Upstream URL: https://github.com/llm-d/llm-d-inference-sim
- Commit or tag: tag v1.0.0, commit 7bda03f9fef85139617cb4f724dccafe1cf8cbbf.
- SPDX licence: Apache-2.0 (LICENSE in this directory is upstream's).
- Date copied: 2026-10-30.
- Why copied rather than imported: the change is a bind-address patch to a binary we build and run, not a library we import; upstream has no option for it, so a patch applied at build time is the smallest change.
- Used by: spike S2 (`spikes/s2`), as an engine double.
- Changes: `loopback-bind.patch` makes the HTTP listener bind `127.0.0.1:<port>` instead of all interfaces
  (`pkg/communication/http.go`, one line). Upstream has no bind-address option. The patch also adds a
  modification notice (a comment after the licence block) to that file, as Apache-2.0 section 4(b) and HBEP-15
  require. Nothing else is changed.
- Apply: `git apply loopback-bind.patch` in a checkout of the commit above; `spikes/s2/scripts/setup.sh` does this.
