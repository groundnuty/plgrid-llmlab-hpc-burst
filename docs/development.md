# Development

The framework is one Go module at the repository root, `github.com/groundnuty/plgrid-llmlab-hpc-burst`. The minimum Go version is 1.26.0.

Code under `spikes/` and `third_party/forked/*` is not part of this module.
- **Spikes:** each Go spike has its own `go.mod`. S7 is Python and shell, with no Go packages.
- **Forked packages:** each one has its own `go.mod`. A build tag alone is not enough, because `go mod tidy` considers every build tag. Check with `go mod tidy -diff`; it must show nothing.

## Layout

| Path | What lives there | HBEP |
|---|---|---|
| `api/v1alpha1/` | Go types for the custom resources, with kubebuilder and CEL markers | HBEP-3, HBEP-11 |
| `config/crd/` | CRD manifests generated from `api/`, committed. #40 adds `hack/verify-generated.sh` to check they match a fresh generation. | HBEP-11 |
| `pkg/deploymentid/` | deployment ID, scheme `v1`: canonical object, RFC 8785 bytes, SHA-256 | HBEP-3 |
| `internal/launcher/` | starts and supervises the single-user control-plane host (user-level k3s); the only code that starts k3s | HBEP-11 |
| `cmd/hpcburst/` | the `hpcburst` command (`up`, `down`, `status`, `upgrade`) | HBEP-11 |
| `hack/` | scripts for generation, verification and pinned tools | |
| `third_party/forked/<project>/` | copied code, each with its `LICENSE` and `ORIGIN.md` | HBEP-15 |
| `spikes/<id>/` | spike code and results; each Go spike is its own module | |

`pkg/` is for packages other components import. `internal/` is for packages only this module uses.

## Pinned versions

The Kubernetes libraries match crossplane-runtime v2.4.3, which HBEP-4's provider builds on. That way the provider needs no version bump:

| Dependency | Version |
|---|---|
| `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go` | v0.35.3 |
| `k8s.io/apiextensions-apiserver` | v0.35.0 |
| `sigs.k8s.io/controller-runtime` | v0.23.1 |
| `sigs.k8s.io/controller-tools` (`controller-gen`) | v0.20.0 |
| k3s (single-user and team host) | v1.36.5+k3s1 |

Kubernetes' explicit ±1-minor skew rule is written for kubectl. client-go instead documents compatibility through the APIs a client and a cluster share ([client-go compatibility](https://github.com/kubernetes/client-go/blob/v0.35.3/README.md#compatibility-client-go---kubernetes-clusters)). A 0.35 client against a 1.36 server is a reasonable choice for shared stable APIs, and the tests against k3s 1.36 check the APIs we use. A change to any of these pins is its own PR.

## Rules for every change

- **Dependencies:** each one's licence must be on HBEP-15's allowlist (Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC and the others it lists). Copied code goes under `third_party/forked/<project>/` with its upstream `LICENSE` and an `ORIGIN.md`.
- **Tests:** they come with the change. Expected values come from specifications or an independent implementation, never from the code under test.
- **Public content:** this repository is public. No host names, IP addresses other than loopback, job IDs, account or grant names, credentials or internal paths (HBEP-1).

## The launcher

`hpcburst` (`cmd/hpcburst`, code in `internal/launcher`) starts and supervises the single-user control-plane host of HBEP-11: the pinned k3s as the invoking user, with `server --rootless --disable-agent` and SQLite. It supports Linux only, because every health and stop decision is tied to `/proc`.

```
hpcburst up      --state-dir D --port-base 29220 [--version V] [--manifests DIR] [--front-mode direct|ssh --front-addr HOST[:PORT] [--front-key FILE]]
hpcburst status  --state-dir D [--json] [--no-api]
hpcburst upgrade --state-dir D --to V
hpcburst down    --state-dir D
```

- **State:** everything the launcher owns is in the state directory (default `$XDG_STATE_HOME/hpcburst`, else `~/.local/state/hpcburst`): k3s's data directory, home and log, the record of the processes it started (`state.json`, with PID and start time), and `kubeconfig` (mode 600, written only once the host is ready).
- **Binaries:** `up` and `upgrade` look for `k3s-X.Y.Z/k3s` under `--bin-dir` (or `$HPCBURST_BIN_DIR`, else `~/.cache/hpcburst/k3s`) and check its sha256 against `internal/launcher/k3s-pins.txt`. `hack/fetch-k3s.sh [DIR]` downloads and checks them. The pins file is the one place that says which versions are allowed.
- **Health:** `status` judges from the recorded processes: PID, start time and boot ID must match, the process must hold exactly its loopback ports, and only then does it ask `/readyz`, the controller manager's `/healthz` and a small write to the store. A listener that answers on a port but is not held by the recorded process never counts.
- **Exit codes:** 0 success, 1 failure (for `status`: unhealthy), 2 usage error, 3 (`status` only) stopped.
- **Front:** `up` records the front address and mode and warns when none is given. The live reachability check comes later, with the front.

### Testing the launcher

`go test ./...` runs the unit tests. The tests that start a real k3s need the pinned binaries and skip, loudly, without them. To run everything:

```
hack/fetch-k3s.sh "$DIR" 1.35.9 1.36.5
HPCBURST_BIN_DIR="$DIR" hack/test-launcher.sh
```

The script turns a missing binary into a failure and lists any skipped test. The tests use loopback ports 29220–29239 and state directories under `$TMPDIR` (about 0.6 GB at peak, removed after each test). Set `HPCBURST_TEST_ROOT_K3S=1` on a machine that runs a system k3s: the tests then fail if it is missing, and assert after each test that it still listens on 6443 and 10250. The store-full test mounts a size-limited FUSE volume as the invoking user, so it needs `/dev/fuse` and `fusermount3`; it skips with that reason when the mount fails, unless `HPCBURST_REQUIRE_INTEGRATION=1`.
