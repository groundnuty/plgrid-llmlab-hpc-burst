# Forked from KubeAI

- **Project:** KubeAI, https://github.com/kubeai-project/kubeai
- **Licence:** Apache-2.0 (`LICENSE`, copied unchanged). Upstream has no NOTICE file.
- **Commit:** `89dfff462887cad30748a631e2c26c8aa2bbf492` (main, committed 2026-10-30T08:14:02+01:00), copied 2026-10-30.
- **Why:** spike S10 (issue #27) simulates HBEP-5's conversation affinity with KubeAI's consistent hashing with bounded loads (CHWBL). The balancer sits under Go `internal/`, so it cannot be imported and is copied.

## What is copied, and every change

The algorithm is unchanged. Each modified file says so in a header comment, as Apache-2.0 section 4(b) asks.

| File here | Upstream file | Change |
|---|---|---|
| `loadbalancer/balance_chwbl.go` | `internal/loadbalancer/balance_chwbl.go` | Header comment; the `internal/metrics` import path points at the stub below. Code unchanged. |
| `loadbalancer/group.go` | `internal/loadbalancer/group.go` | Header comment; the `api/k8s/v1` and `internal/apiutils` import paths point at the trimmed copies below. Code unchanged. |
| `loadbalancer/balance_least_load.go` | `internal/loadbalancer/balance_least_load.go` | None (byte-identical). `group.go` calls it. |
| `api/k8s/v1/model_types.go` | `api/k8s/v1/model_types.go` | Trimmed to `LoadBalancing`, `LoadBalancingStrategy` and its two constants, and `PrefixHash`, which `group.go` uses. Definitions and comments unchanged. |
| `internal/apiutils/request.go` | `internal/apiutils/request.go` | Trimmed to the `Request` fields that `group.go` reads: `Model`, `Adapter`, `LoadBalancing`, `Prefix`. Request parsing is not copied. |
| `internal/metrics/metrics.go` | `internal/metrics/metrics.go` | Trimmed to the four hash-lookup instruments that `balance_chwbl.go` records and `AttrEndpoint`. The instruments are OpenTelemetry no-op instruments instead of ones registered in `Init`. |

Path change: upstream's `internal/loadbalancer` is `loadbalancer/` here, so that code outside this module (the simulator in `spikes/s10`) can import it.

## What is added (not from KubeAI)

- `go.mod`, `go.sum`: this directory is its own Go module.
- `loadbalancer/sim_api.go`: an exported handle (`Group`) that calls upstream's `reconcileEndpoints`, `removeEndpoint` and `getBestAddr`. It re-implements no selection rule. `getBestAddr` gets an already-cancelled context, so with no endpoints it returns an error instead of blocking.
- `loadbalancer/sim_api_test.go`: hand-checked tests of the copied behaviour (same key, same endpoint; spill at the bound; release after removal).

## Dependencies

| Module | Version | Licence | Why |
|---|---|---|---|
| `github.com/cespare/xxhash` | v1.1.0 (as pinned upstream) | MIT | The ring hash. The version fixes the ring positions. |
| `go.opentelemetry.io/otel`, `go.opentelemetry.io/otel/metric` | v1.47.0 (as pinned upstream) | Apache-2.0 | The `attribute` and `metric` types in `balance_chwbl.go`; the `noop` instruments. |

## Upstream behaviour worth knowing

- `chwblLoadOK` compares an endpoint's load before the new request with `c * (total + 1) / n`. When that threshold is a whole number, the chosen endpoint can end one request above `ceil(c * mean)`, the capacity in Mirrokni et al.'s definition.
- `removeEndpoint` keeps the removed endpoint's open requests in `totalInFlight` while `n` drops. Until they finish, the mean, and with it the bound, is higher than the members' own load gives.
- `PrefixHash` keys on the first `PrefixCharLength` (default 100) characters of the first user message, plus the adapter name.
