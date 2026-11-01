# llm-d-router (EPP): loopback bind patch

- Upstream URL: https://github.com/llm-d/llm-d-router
- Commit or tag: tag v1.0.0, commit b9393d1a9f1d5a72ecd86f8d2b643ab54c2a9b92.
- SPDX licence: Apache-2.0 (LICENSE in this directory is upstream's).
- Date copied: 2026-10-30.
- Why copied rather than imported: the change is a bind-address patch to a binary we build and run, not a library we import; upstream has no option for it, so a patch applied at build time is the smallest change.
- Used by: spike S2 (`spikes/s2`), as the endpoint picker (EPP).
- Changes: `loopback-bind.patch` makes the EPP's gRPC servers (ext-proc and health, `internal/runnable/grpc.go`),
  its metrics server and one HTTP server (`cmd/epp/runner/runner.go`, two lines) bind `127.0.0.1:<port>`
  instead of all interfaces. Upstream has no listen-address option; its ports are `:<port>` and, with
  `--secure-serving=false`, unauthenticated. The patch also adds a modification notice (a comment after the
  licence block) to each of the two files, as Apache-2.0 section 4(b) and HBEP-15 require. Nothing else is changed.
- Apply: `git apply loopback-bind.patch` in a checkout of the commit above; `spikes/s2/scripts/setup.sh` does this.
