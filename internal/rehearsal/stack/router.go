package stack

import (
	"os"
	"path/filepath"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// InstanceRouter makes StartHost's up run Instances behind Envoy: hpcburst up --instance-router envoy
// (beta, opt-in; #429, #454). The controller records the router in each new Instance's launch, and the
// Provider's controller stages Envoy's build to every job.
func InstanceRouter(h *Host) { h.upExtra = append(h.upExtra, "--instance-router", "envoy") }

// PlaceEnvoyBuild puts the Envoy at bin where the controller reads it, as an operator does with
// hack/fetch-envoy.sh --agent-builds: <state>/agent-builds/envoy-<cpuArch>. The controller stages it
// on the cluster by its digest when Instances run behind a router.
func (h *Host) PlaceEnvoyBuild(arch, bin string) {
	h.T.Helper()
	b, err := os.ReadFile(bin)
	if err != nil {
		h.T.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.State, "agent-builds", agentapi.EnvoyBuildName(arch)), b, 0o700); err != nil {
		h.T.Fatal(err)
	}
}
