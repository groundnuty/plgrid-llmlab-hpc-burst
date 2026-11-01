package providerctl

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl/ctltest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
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
	// The digest of the example frpc build for x86_64, hashed here, not by the
	// fixture's helper.
	sum := sha256.Sum256(ctltest.FrpcContent(testArch))
	digest := hex.EncodeToString(sum[:])
	for _, tc := range []struct {
		name    string
		mode    agentapi.DialHomeMode
		digests map[string]string
		submits bool
		// cpuArch, when set, replaces the partition's cpuArch; reason is what the
		// Allocation's reason must name when nothing is submitted.
		cpuArch, reason string
	}{
		{"direct, digest for the partition's cpuArch", agentapi.DialHomeDirect, map[string]string{"x86_64": digest}, true, "", ""},
		{"direct, digest for another cpuArch only", agentapi.DialHomeDirect, map[string]string{"aarch64": digest}, false, "", "cpuArch x86_64"},
		{"ssh without the dial-home keys, digest for another cpuArch only", agentapi.DialHomeSSH, map[string]string{"aarch64": digest}, false, "", "cpuArch x86_64"},
		// frp's own name for the architecture is not a cpuArch: the digests are keyed by the
		// profile's strings, and nothing maps one to the other here (internal/launcher's
		// cpuArchOf does it once, where the pins are read), so the lookup fails closed.
		{"direct, a partition whose cpuArch is frp's amd64", agentapi.DialHomeDirect, map[string]string{"x86_64": digest, "aarch64": digest}, false, "amd64", "cpuArch amd64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", func(e *realEnv) {
				f := e.opts.Front
				f.Mode, f.FrpcDigests = tc.mode, tc.digests
				if tc.mode == agentapi.DialHomeSSH {
					f.Endpoint, f.SSHUser, f.AgentPort, f.DialHomePort = "front.example.org:22", "example-user", 29270, 29274
				}
				e.opts.Front = f
				if tc.cpuArch != "" {
					e.profiles.set(func(v *slurm.Validated) { v.Profile.Partition.AcceleratorClass.CPUArch = tc.cpuArch })
				}
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
				if got := e.get("alloc-a").Annotations[v1alpha1.AnnotationSubmission]; got != "" {
					t.Errorf("a submission record %q was written, want no claim", got)
				}
				if reason := e.get("alloc-a").Status.AtProvider.Reason; !strings.Contains(reason, tc.reason) {
					t.Errorf("reason %q does not name %q", reason, tc.reason)
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
			if cfg.FrpcDigest != digest {
				t.Errorf("agent.json frpcDigest %q, want the x86_64 digest %s", cfg.FrpcDigest, digest)
			}
		})
	}
}
