// Modified for plgrid-llmlab-hpc-burst (spike S10): trimmed copy of KubeAI's
// internal/apiutils/request.go keeping only the Request fields that
// internal/loadbalancer/group.go reads (Model, Adapter, LoadBalancing, Prefix).
// Request parsing, which fills Prefix from the first user message, is not
// copied. See ../../NOTES.md.

package apiutils

import (
	k8sv1 "github.com/groundnuty/plgrid-llmlab-hpc-burst/third_party/forked/kubeai/api/k8s/v1"
)

type Request struct {
	Model   string
	Adapter string

	LoadBalancing k8sv1.LoadBalancing

	Prefix string
}
