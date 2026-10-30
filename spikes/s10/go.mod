module github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10

go 1.27.3

require (
	github.com/cespare/xxhash v1.1.0
	github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
)

replace github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai => ../../third_party/forked/kubeai
