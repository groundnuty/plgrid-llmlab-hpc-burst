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
| `github.com/crossplane/crossplane-runtime/v2` | v2.4.3 (Apache-2.0) |
| `sigs.k8s.io/yaml` | v1.6.0 (MIT, with BSD-3-Clause parts; direct, for `hack/mutate-cel`) |
| `github.com/crossplane/crossplane/apis/v2` | v2.3.4 (Apache-2.0), the version crossplane-runtime v2.4.3 requires |
| k3s (single-user and team host) | v1.36.5+k3s1 |
| `kube-apiserver`, `kube-controller-manager` (the assembled test host of `internal/testenv`) | v1.36.2, sha256-pinned in `hack/test-binaries.sh` |
| `etcd` (the assembled test host) | v3.6.8, sha256-pinned in `hack/test-binaries.sh` |

Kubernetes' explicit ±1-minor skew rule is written for kubectl. client-go instead documents compatibility through the APIs a client and a cluster share ([client-go compatibility](https://github.com/kubernetes/client-go/blob/v0.35.3/README.md#compatibility-client-go---kubernetes-clusters)). A 0.35 client against a 1.36 server is a reasonable choice for shared stable APIs, and the tests against k3s 1.36 check the APIs we use. A change to any of these pins is its own PR.

## Checks

- `hack/check.sh` runs gofmt, vet, `go mod tidy -diff`, the generated-file check and the tests, then lists every skipped test. Run it under `nice` and the shared build lock.
- `hack/mutate-cel.sh` neutralises each CEL admission rule in a copy of the CRDs and runs the test that covers it, which must fail. It takes about ten minutes, so it is not part of `hack/check.sh`; run it when a rule or its test changes. A rule with no entry in its table fails the script.
- The flag-name tests read the known flag names from HBEP-3's v1 table, so the admission rules' lists of known names are checked against the specification. A flag added to the table fails `TestEngineFlagsRefuseEveryPrefixOfAKnownFlag` until the list in the `EngineFlag` markers (`api/v1alpha1/modeldeployment_types.go`) gains it.
- The test host (`internal/testenv`) binds only loopback ports 29200 to 29219 and, in k3s mode, accepts only a kubeconfig whose server is loopback on 29200 to 29239. The control-plane binaries are always sha256-checked; `HPCBURST_TESTENV_UNPINNED=1` is the explicit override for a binary you built yourself.

## API field names

Every field of the CRDs is camelCase, as the Kubernetes API conventions ask (owner decision, PR #48): `holdRelease`, `lostGrace`, `imageDigest`, `gpuModel`, `timeZone`, `jobName.maxLength`. HBEP-3's hash object keeps its snake_case keys (`image_digest`, `gpu_model`, `cpu_arch`); `pkg/deploymentid` maps the API's fields onto them, and nothing in the API changes the hash. `TestEveryCRDFieldIsCamelCase` scans the committed manifests. HBEP text that writes a field in snake_case (`hold_release` in HBEP-11) means the camelCase field.

## Rules for every change

- **Dependencies:** each one's licence must be on HBEP-15's allowlist (Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC and the others it lists). Copied code goes under `third_party/forked/<project>/` with its upstream `LICENSE` and an `ORIGIN.md`.
- **Tests:** they come with the change. Expected values come from specifications or an independent implementation, never from the code under test.
- **Public content:** this repository is public. No host names, IP addresses other than loopback, job IDs, account or grant names, credentials or internal paths (HBEP-1).
