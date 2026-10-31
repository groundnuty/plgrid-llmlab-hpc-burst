package providerctl

import (
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// agent.json must name frpc's digest (agentapi.AgentConfig.Validate refuses it
// otherwise), and the digest depends on the partition's cpuArch (HBEP-4,
// Submission: Files first). The two cases differ only in the Front's digest
// map. With a digest for the partition's cpuArch (x86_64) the Allocation
// submits: files are staged, agent.json names the digest, and one sbatch runs.
// With a digest for another cpuArch only, the fake login node's call log shows
// no sbatch, no file written, and nothing under the staging directory.
func TestFrpcDigestForThePartitionsCPUArch(t *testing.T) {
	const staging = "/scratch/user-a/stage/"
	digest := strings.Repeat("d", 64)
	for _, tc := range []struct {
		name    string
		digests map[string]string
		submits bool
	}{
		{"digest for the partition's cpuArch", map[string]string{"x86_64": digest}, true},
		{"digest for another cpuArch only", map[string]string{"aarch64": digest}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", func(e *realEnv) {
				e.opts.Front = &FrontSettings{Mode: agentapi.DialHomeDirect, Endpoint: "front.example.org:29443",
					CertPin: strings.Repeat("c", 64), FrpcDigests: tc.digests}
			})
			a := e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 3)

			var staged []string
			for p := range sl.Files() {
				if strings.HasPrefix(p, staging) {
					staged = append(staged, p)
				}
			}
			if !tc.submits {
				if n := sl.Count("sbatch"); n != 0 {
					t.Errorf("%d sbatch calls with no frpc digest for the partition's cpuArch, want 0", n)
				}
				if n := sl.Count("cat"); n != 0 {
					t.Errorf("%d file writes with no frpc digest for the partition's cpuArch, want 0", n)
				}
				if len(staged) != 0 {
					t.Errorf("files staged with no frpc digest for the partition's cpuArch: %v", staged)
				}
				return
			}
			if n := sl.Count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls with a frpc digest for the partition's cpuArch, want 1", n)
			}
			js, ok := sl.Files()[staging+agentapi.StagedConfigPath(v1alpha1.JobTag(a.Namespace, a.Name, a.UID))]
			if !ok {
				t.Fatalf("agent.json was not staged; staged: %v", staged)
			}
			cfg, err := agentapi.DecodeAgentConfig(js.Content)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.FrpcDigest != "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" {
				t.Errorf("agent.json frpcDigest %q, want the x86_64 digest", cfg.FrpcDigest)
			}
		})
	}
}
