package providerctl

// Envoy staged beside frpc, by its digest, when the controller runs Instances behind a router (#454):
// Front.EnvoyDigests set, as hpcburst-controller sets it from envoy-pins.txt under
// --instance-router envoy. Without it nothing new is staged. The fixtures are written out in full,
// and their digests are the output of sha256sum(1) on exactly those bytes; an expected path is
// written by hand.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	envoyX86, envoyX86Digest         = "example envoy for x86_64\n", "6c44295c3f3cb08b242131926c544a3d15b634a541a0a782d7da6d18636af691"
	envoyAarch64, envoyAarch64Digest = "example envoy for aarch64\n", "f1c7bee40edc33eff6e6e3e4408cfed9637310669468304b60fa1c4f5c3fdd30"
	envoyOther, envoyOtherDigest     = "another envoy build, not the pinned one\n", "2331dacc6838a85a53b36e16e91160c66c2f04eef6ade4fc3fc1a367b475480c"
)

// stageEnvoyBuilds writes the two Envoy fixtures into the agent-builds directory, and, when router is
// set, pins their digests in front as the controller does under --instance-router envoy.
func stageEnvoyBuilds(t *testing.T, builds string, front *FrontSettings, router bool) {
	t.Helper()
	for arch, content := range map[string]string{"x86_64": envoyX86, "aarch64": envoyAarch64} {
		if err := os.WriteFile(filepath.Join(builds, "envoy-"+arch), []byte(content), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if router {
		front.EnvoyDigests = map[string]string{"x86_64": envoyX86Digest, "aarch64": envoyAarch64Digest}
	}
}

// Behind a router, the Envoy staged is the partition's cpuArch's, at envoy/<its digest>/envoy, mode 700,
// with exactly its bytes; the other architecture's is not staged.
func TestARouterStagesThePartitionsEnvoyByDigest(t *testing.T) {
	for _, tc := range []struct {
		name, arch, content, digest, other string
		gh200                              bool
	}{
		{name: "an x86_64 partition", arch: "x86_64", content: envoyX86, digest: envoyX86Digest, other: envoyAarch64Digest},
		{name: "a GH200 partition", arch: "aarch64", content: envoyAarch64, digest: envoyAarch64Digest, other: envoyX86Digest, gh200: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, func(e *env) {
				stageBuilds(t, e.buildDir, e.opts.Front)
				stageEnvoyBuilds(t, e.buildDir, e.opts.Front, true)
				e.profiles.set(func(v *slurm.Validated) { v.Profile.Partition.AcceleratorClass.CPUArch = tc.arch })
				if tc.gh200 {
					e.profiles.set(func(v *slurm.Validated) { v.Profile.Partition.AcceleratorClass.GPUModel = "GH200" })
					if err := os.WriteFile(filepath.Join(e.buildDir, agentapi.AgentBuildName("aarch64")), []byte(agentAarch64), 0o700); err != nil {
						t.Fatal(err)
					}
					e.digest = agentAarch64Digest
				}
			})
			e.newAlloc("a")
			e.run("a", 3)
			if n := e.count("sbatch"); n != 1 {
				t.Fatalf("%d sbatch calls, want 1 (reason %q)", n, e.get("a").Status.AtProvider.Reason)
			}
			files := e.batch.Files()
			f, ok := files["envoy/"+tc.digest+"/envoy"]
			if !ok {
				t.Fatalf("no file at envoy/%s/envoy; staged: %v", tc.digest, stagedNames(files))
			}
			if string(f.Content) != tc.content || sha256Hex(f.Content) != tc.digest || f.Mode.Perm() != 0o700 {
				t.Errorf("the staged Envoy: content %q, sha256 %s, mode %o; want %q, %s, 700", f.Content, sha256Hex(f.Content), f.Mode.Perm(), tc.content, tc.digest)
			}
			if _, ok := files["envoy/"+tc.other+"/envoy"]; ok {
				t.Error("the other architecture's Envoy was staged too")
			}
			// Envoy is staged before agent.json, which stays last (HBEP-4: agent.json is staged after the
			// files it names).
			var order []string
			for _, c := range e.batch.Calls() {
				if c.Op == "ensure-file" {
					order = append(order, c.Args[0])
				}
			}
			if n := len(order); n < 2 || !strings.HasSuffix(order[n-1], "/agent.json") || order[n-2] != "envoy/"+tc.digest+"/envoy" {
				t.Errorf("the staging order %v, want Envoy just before agent.json, which stays last", order)
			}
		})
	}
}

// Without a router, nothing new is staged, even with Envoy's builds in the directory: alpha's files are
// the agent, frpc, the credential, the certificate and agent.json. The router run above is its control.
func TestWithoutARouterNoEnvoyIsStaged(t *testing.T) {
	e := newEnv(t, func(e *env) {
		stageBuilds(t, e.buildDir, e.opts.Front)
		stageEnvoyBuilds(t, e.buildDir, e.opts.Front, false)
	})
	e.newAlloc("a")
	e.run("a", 3)
	if n := e.count("sbatch"); n != 1 {
		t.Fatalf("%d sbatch calls, want 1 (reason %q)", n, e.get("a").Status.AtProvider.Reason)
	}
	for _, p := range stagedNames(e.batch.Files()) {
		if strings.HasPrefix(p, "envoy/") {
			t.Errorf("without a router, %s was staged", p)
		}
	}
	if n := len(e.batch.Files()); n != 5 {
		t.Errorf("%d files staged without a router, want alpha's 5: %v", n, stagedNames(e.batch.Files()))
	}
}

// Behind a router, an Envoy build that is not the pinned one, or none, is refused before any call to
// the platform, as frpc's is; once the pinned build is there, the job is submitted.
func TestAWrongOrMissingEnvoyIsRefusedBeforeAnyClaim(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(builds string)
		want   []string
	}{
		{"an Envoy with another digest", func(builds string) {
			if err := os.WriteFile(filepath.Join(builds, "envoy-x86_64"), []byte(envoyOther), 0o700); err != nil {
				t.Fatal(err)
			}
		}, []string{"Envoy build", envoyOtherDigest, envoyX86Digest}},
		{"no Envoy for the partition's cpuArch", func(builds string) {
			if err := os.Remove(filepath.Join(builds, "envoy-x86_64")); err != nil {
				t.Fatal(err)
			}
		}, []string{"no Envoy build for cpuArch x86_64"}},
		{"the other architecture's Envoy under this one's name", func(builds string) {
			if err := os.WriteFile(filepath.Join(builds, "envoy-x86_64"), []byte(envoyAarch64), 0o700); err != nil {
				t.Fatal(err)
			}
		}, []string{"Envoy build", envoyAarch64Digest, envoyX86Digest}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", stageReal(t))
			stageEnvoyBuilds(t, e.builds, e.opts.Front, true)
			tc.break_(e.builds)
			e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 3)
			refusedBeforeAnyCall(t, e, sl, "alloc-a", tc.want...)

			stageEnvoyBuilds(t, e.builds, e.opts.Front, true)
			e.run("alloc-a", 4)
			if n := sl.Count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls with the Envoy as pinned, want 1", n)
			}
		})
	}
}

// Behind a router with no Envoy digest for the partition's cpuArch, the Allocation is refused before
// any call, naming the cpuArch.
func TestARouterWithNoEnvoyDigestForTheArchIsRefused(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", stageReal(t))
	stageEnvoyBuilds(t, e.builds, e.opts.Front, true)
	delete(e.opts.Front.EnvoyDigests, "x86_64")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 3)
	refusedBeforeAnyCall(t, e, sl, "alloc-a", "no Envoy digest is configured for cpuArch x86_64")
}
