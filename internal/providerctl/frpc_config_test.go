package providerctl

import (
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// agent.json must name frpc's digest (agentapi.AgentConfig.Validate refuses it
// otherwise), and the digest depends on the partition's cpuArch (HBEP-4,
// Submission: Files first). A Front with a digest for another cpuArch
// submits nothing: no file is staged and no sbatch runs.
func TestNoFrpcDigestForTheCPUArchSubmitsNothing(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", func(e *realEnv) {
		e.opts.Front = &FrontSettings{Mode: agentapi.DialHomeDirect, Endpoint: "front.example.org:29443", CertPin: strings.Repeat("c", 64),
			FrpcDigests: map[string]string{"aarch64": strings.Repeat("d", 64)}} // the partition is x86_64
	})
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)
	if n := sl.Count("sbatch"); n != 0 {
		t.Errorf("%d sbatch calls with no frpc digest for the partition's cpuArch, want 0", n)
	}
	if n := sl.Count("cat"); n != 0 {
		t.Errorf("%d file writes with no frpc digest for the partition's cpuArch, want 0", n)
	}
}
