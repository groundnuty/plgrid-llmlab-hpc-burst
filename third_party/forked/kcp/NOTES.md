# kcp (adapted code for spike S3)

- **Project:** kcp, https://github.com/kcp-dev/kcp
- **Licence:** Apache-2.0 (`LICENSE` in this directory is the file from the tag below)
- **Origin:** `cmd/kcp/kcp.go` at tag v0.33.3, commit 003eb8c7649b0f204ec1bd3844e10f633f90065b
- **File here:** `s3-embedded/main.go`, built only by `spikes/s3/scripts/build-embedded.sh` (build tag `s3embedded`). It never compiled to the end; see the README of spike S3.

## Changes

- Kept the order of kcp's start sequence: root-directory pre-scan, `options.NewOptions`, logging setup, `Complete`, `Validate`, `server.NewConfig`, `Complete`, embedded etcd on its own context, `server.NewServer`, `Run`.
- Removed the cobra command tree, the help and usage functions, the `options` sub-command and the `miniproxy-mapping-file` handling. Flags are registered from kcp's own option sets on a plain pflag set.
- Added `--embedded-controllers` (default true): a goroutine waits for the admin kubeconfig, creates the child workspace, installs the spike's CRDs and runs the spike's controller-runtime manager inside the same process (`runControllers`, `nopCanceller`).
