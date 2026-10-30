// Modified for plgrid-llmlab-hpc-burst (spike S10): trimmed copy of KubeAI's
// internal/metrics/metrics.go keeping only the instruments that
// internal/loadbalancer/balance_chwbl.go records. Upstream creates them from a
// meter in Init; here they are OpenTelemetry no-op instruments, so recording
// does nothing and needs no setup. See ../../ORIGIN.md.

package metrics

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

var (
	InferenceRequestsHashLookupIterations metric.Int64Histogram = noop.Int64Histogram{}
	InferenceRequestsHashLookupInitial    metric.Int64Counter   = noop.Int64Counter{}
	InferenceRequestsHashLookupFinal      metric.Int64Counter   = noop.Int64Counter{}
	InferenceRequestsHashLookupDefault    metric.Int64Counter   = noop.Int64Counter{}
)

var (
	AttrEndpoint = attribute.Key("endpoint")
)
