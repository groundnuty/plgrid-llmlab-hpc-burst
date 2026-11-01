# kcp (adapted code for spike S3)

Record required by HBEP-15, "Copied code". Resyncing with upstream is a PR that updates this file.

| field | value |
|---|---|
| Upstream URL | https://github.com/kcp-dev/kcp |
| File copied | `cmd/kcp/kcp.go` |
| Commit or tag | v0.33.3, commit 003eb8c7649b0f204ec1bd3844e10f633f90065b |
| SPDX licence | Apache-2.0 (`LICENSE` in this directory is upstream's file at that tag, byte for byte; upstream has no NOTICE or PATENTS file at that tag) |
| Date copied | 2026-10-30 (first committed on 2026-10-30 as `spikes/s3/embedded/main.go` in commit 9317e98, moved here in commit 005fcf7) |
| Where it lives | `s3-embedded/main.go`, built only by `spikes/s3/scripts/build-embedded.sh` (build tag `s3embedded`). It never compiled to the end; see the README of spike S3. |

## Why copied rather than imported

Upstream's start sequence is the `main` function of `cmd/kcp/kcp.go`, which is `package main`, and Go cannot import a `main` package. Spike S3 wanted to run kcp as a library, in one process together with the spike's controller-runtime manager (the "one bundled binary for a single user" option of HBEP-11), so the start sequence had to be written out in a main package of our own. The packages it calls (`pkg/server`, `pkg/server/options`) are importable and are imported, not copied.

## Changes

The modified file carries a line saying it was adapted (the comment under the licence header).

- Kept the order of kcp's start sequence: root-directory pre-scan, `options.NewOptions`, logging setup, `Complete`, `Validate`, `server.NewConfig`, `Complete`, embedded etcd on its own context, `server.NewServer`, `Run`.
- Removed the cobra command tree, the help and usage functions, the `options` sub-command and the `miniproxy-mapping-file` handling. Flags are registered from kcp's own option sets on a plain pflag set.
- Added `--embedded-controllers` (default true): a goroutine waits for the admin kubeconfig, creates the child workspace, installs the spike's CRDs and runs the spike's controller-runtime manager inside the same process (`runControllers`, `nopCanceller`).
