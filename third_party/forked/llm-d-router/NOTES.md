# llm-d-router (EPP): loopback bind patch

- Origin: https://github.com/llm-d/llm-d-router, tag v1.0.0, commit b9393d1a9f1d5a72ecd86f8d2b643ab54c2a9b92. Licence: Apache-2.0 (LICENSE in this directory is upstream's).
- Used by: spike S2 (`spikes/s2`), as the endpoint picker (EPP).
- Change: `loopback-bind.patch` makes the EPP's gRPC servers (ext-proc and health, `internal/runnable/grpc.go`),
  its metrics server and one HTTP server (`cmd/epp/runner/runner.go`, two lines) bind `127.0.0.1:<port>`
  instead of all interfaces. Upstream has no listen-address option; its ports are `:<port>` and, with
  `--secure-serving=false`, unauthenticated. Nothing else is changed.
- Apply: `git apply loopback-bind.patch` in a checkout of the commit above; `spikes/s2/scripts/setup.sh` does this.
