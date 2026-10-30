module github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10b

go 1.27.3

replace github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10 => ../s10

replace github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai => ../../third_party/forked/kubeai

require github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s10 v0.0.0-00010101000000-000000000000

require (
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai v0.0.0 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
)
