package providerctl

import (
	"strings"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// agent.json must name frpc's digest (agentapi.AgentConfig.Validate refuses it
// otherwise), and the digest depends on the partition's cpuArch (HBEP-4,
// Submission: Files first). The cases differ only in the Front's digest map
// (and the dial-home mode). With a digest for the partition's cpuArch (x86_64)
// the Allocation submits: files are staged, agent.json names the digest, and
// one sbatch runs.
//
// With a digest for another cpuArch only, the refusal is a configuration check
// in prerequisites, so it comes before any remote call or status write. The
// fake login node's call log then holds only the reconcile's own read-only job
// listing (squeue, sacct): no printenv to resolve the paths, no file write, no
// sbatch; and in ssh mode with the dial-home keys absent, the Provider does not
// get DialHomeKeyMissing, which Create would otherwise have written first.
func TestFrpcDigestForThePartitionsCPUArch(t *testing.T) {
	const staging = "/scratch/user-a/stage/"
	digest := strings.Repeat("d", 64)
	for _, tc := range []struct {
		name    string
		mode    agentapi.DialHomeMode
		digests map[string]string
		submits bool
	}{
		{"direct, digest for the partition's cpuArch", agentapi.DialHomeDirect, map[string]string{"x86_64": digest}, true},
		{"direct, digest for another cpuArch only", agentapi.DialHomeDirect, map[string]string{"aarch64": digest}, false},
		{"ssh without the dial-home keys, digest for another cpuArch only", agentapi.DialHomeSSH, map[string]string{"aarch64": digest}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", func(e *realEnv) {
				f := &FrontSettings{Mode: tc.mode, Endpoint: "front.example.org:29443", CertPin: strings.Repeat("c", 64), FrpcDigests: tc.digests}
				if tc.mode == agentapi.DialHomeSSH {
					f.Endpoint, f.SSHUser, f.AgentPort, f.DialHomePort = "front.example.org:22", "example-user", 29270, 29274
				}
				e.opts.Front = f
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
				for _, c := range sl.Calls() {
					if w := c.Words[0]; w != "squeue" && w != "sacct" {
						t.Errorf("a platform call %v with no frpc digest for the partition's cpuArch; only the job listing (squeue, sacct) may run", c.Words)
					}
				}
				if n := sl.Count("sbatch"); n != 0 {
					t.Errorf("%d sbatch calls with no frpc digest for the partition's cpuArch, want 0", n)
				}
				if len(staged) != 0 {
					t.Errorf("files staged with no frpc digest for the partition's cpuArch: %v", staged)
				}
				if c := apimeta.FindStatusCondition(e.provStatus().Conditions, v1alpha1.ConditionDialHomeKeyMissing); c != nil {
					t.Errorf("DialHomeKeyMissing is %+v, want it untouched: the digest check comes first", c)
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
