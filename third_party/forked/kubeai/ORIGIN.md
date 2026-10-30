# Origin: KubeAI

The fields HBEP-15 ("Copied code") requires:

- **Upstream URL:** https://github.com/kubeai-project/kubeai
- **Commit or tag:** commit `89dfff462887cad30748a631e2c26c8aa2bbf492` on `main` (committed 2026-10-30T08:14:02+01:00). See "Pin" below for tag `v0.24.0`.
- **SPDX licence:** `Apache-2.0`. `LICENSE` is upstream's, copied unchanged. Upstream has no `NOTICE` or `PATENTS` file.
- **Date copied:** 2026-10-30.
- **Why copied rather than imported:** the balancer sits under Go `internal/` (`internal/loadbalancer`), so another module cannot import it. Spike S10 (issue #27) drives it from a simulator to choose HBEP-5's conversation key and load bound.
- **Changes:** the table below. The selection algorithm is unchanged.

## Pin

The three copied `loadbalancer` files and `internal/metrics/metrics.go` are byte-identical at tag `v0.24.0` (commit `ba42a28c6ed562d71a500ee76d86bd8d0217e1a9`). The copied definitions in the two type files (`LoadBalancing`, `LoadBalancingStrategy` and its constants, `PrefixHash`, and the `Request` struct) are identical there too. The whole type files are not: at the tag, `model_types.go` lacks the `TextClassification` feature and `request.go` lacks Anthropic Messages parsing, neither of which is copied. Upstream's `go.mod` also differs: OpenTelemetry v1.45.0 at the tag, v1.47.0 at the commit.

The pin stays at the commit, because the commit is what was copied and its `go.mod` set the OpenTelemetry version used here. Moving the pin to `v0.24.0` would change no copied line, only that version.

## What is copied, and every change

Each modified file says so in a header comment, as Apache-2.0 section 4(b) asks. Of the upstream files, only `api/k8s/v1/model_types.go` carries a copyright and licence header (`Copyright 2024.` and the Apache-2.0 notice); it is kept verbatim above the modification notice. The other upstream files have no header, so none is added.

| File here | Upstream file | Change |
|---|---|---|
| `loadbalancer/balance_chwbl.go` | `internal/loadbalancer/balance_chwbl.go` | Header comment; the `internal/metrics` import path points at the stub below. Code unchanged. |
| `loadbalancer/group.go` | `internal/loadbalancer/group.go` | Header comment; the `api/k8s/v1` and `internal/apiutils` import paths point at the trimmed copies below. Code unchanged. |
| `loadbalancer/balance_least_load.go` | `internal/loadbalancer/balance_least_load.go` | None (byte-identical). `group.go` calls it. |
| `api/k8s/v1/model_types.go` | `api/k8s/v1/model_types.go` | Upstream's copyright and licence header kept, then a modification notice. Trimmed to `LoadBalancing`, `LoadBalancingStrategy` and its two constants, and `PrefixHash`, which `group.go` uses. Definitions and comments unchanged. |
| `internal/apiutils/request.go` | `internal/apiutils/request.go` | Trimmed to the `Request` fields that `group.go` reads: `Model`, `Adapter`, `LoadBalancing`, `Prefix`. Request parsing is not copied. |
| `internal/metrics/metrics.go` | `internal/metrics/metrics.go` | Trimmed to the four hash-lookup instruments that `balance_chwbl.go` records and `AttrEndpoint`. The instruments are OpenTelemetry no-op instruments instead of ones registered in `Init`. |

Path change: upstream's `internal/loadbalancer` is `loadbalancer/` here, so that code outside this module (the simulator in `spikes/s10`) can import it.

## What is added (not from KubeAI)

- `go.mod`, `go.sum`: this directory is its own Go module.
- `loadbalancer/sim_api.go`: an exported handle (`Group`) that calls upstream's `reconcileEndpoints`, `removeEndpoint` and `getBestAddr`. It re-implements no selection rule. `getBestAddr` gets an already-cancelled context, so with no endpoints it returns an error instead of blocking. It also holds a test seam, `SetSelectionHook`. It swaps the no-op instrument that upstream records between CHWBL's load check and the counter increment (`InferenceRequestsHashLookupFinal`) for one that calls a hook, so a test can hold a selector at that point. No upstream line changes.
- `loadbalancer/sim_api_test.go`: hand-checked tests of the copied behaviour (same key, same endpoint; spill at the bound; release after removal).
- `loadbalancer/concurrency_test.go`: hand-checked tests of three upstream properties a front must work around:
  - KubeAI's `<=` admits one request over the strict capacity ⌈c·(T+1)/n⌉ when c·(T+1)/n is a whole number;
  - the release function decrements on every call, so a double release undercounts;
  - two selectors under the read lock can both pass the load check on the same loads and both increment.

## Dependencies

| Module | Version | Licence | Why |
|---|---|---|---|
| `github.com/cespare/xxhash` | v1.1.0 (as pinned upstream) | MIT | The ring hash. The version fixes the ring positions. |
| `go.opentelemetry.io/otel`, `go.opentelemetry.io/otel/metric` | v1.47.0 (as pinned at the commit) | Apache-2.0 | The `attribute` and `metric` types in `balance_chwbl.go`; the `noop` instruments. |

## Upstream behaviour worth knowing

- `chwblLoadOK` compares an endpoint's load before the new request with `c * (total + 1) / n`. When that threshold is a whole number, the chosen endpoint can end one request above `ceil(c * mean)`, the capacity in Mirrokni et al.'s definition.
- `getBestAddr` checks the load and increments the counter under the group's read lock, so concurrent selections are not serialized.
- Draining keeps a terminating endpoint's open requests in `totalInFlight` while `n` drops, until they finish.
  - The main path is `reconcileEndpoints`. The controller leaves pods with a `DeletionTimestamp` out of the observed set, and `reconcileEndpoints` removes them without decrementing ("No need to decrement in-flight counts"). `removeEndpoint` does the same for a pod already gone from the cache.
  - This is deliberate upstream. PR #747 ("fix: exclude terminating pods from routing", merged 2026-10-06) added it, and its test `TestReconcileStopsRoutingToTerminatingPods` asserts that "the draining request remains accounted for".
  - HBEP-5's front diverges on purpose: it releases a draining Instance's requests at drain start (`spikes/s10/front`).
- `PrefixHash` keys on the first `PrefixCharLength` (default 100) characters of the first user message, plus the adapter name.
