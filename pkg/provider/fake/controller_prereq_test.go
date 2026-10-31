package fake

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// With Prerequisites set, the Controller submits only once a front is
// configured, the Allocation's credential is a mode-600 key file and an agent
// build with the agentDigest exists, as the real provider-slurm does (review of
// PR 76, Opus's item 5). Each missing piece keeps the Allocation Submitting
// with its reason, no sbatch and no record; once it is fixed, one sbatch.
func TestPrerequisitesHoldTheSubmission(t *testing.T) {
	const arch = "x86_64"
	build := []byte("#!/bin/sh\n# an example agent build\n")
	sum := sha256.Sum256(build)
	digest := hex.EncodeToString(sum[:])
	for _, tc := range []struct {
		name   string
		want   string
		breaks func(p *Prerequisites, credPath string)
		fix    func(p *Prerequisites, credPath string)
	}{
		{"no front", PrerequisiteNoFront,
			func(p *Prerequisites, _ string) { p.Front = false },
			func(p *Prerequisites, _ string) { p.Front = true }},
		{"no credential", PrerequisiteNoCredential,
			func(_ *Prerequisites, c string) { _ = os.Remove(c) },
			func(_ *Prerequisites, c string) { writeKey(t, c) }},
		{"a credential of mode 644", PrerequisiteNoCredential,
			func(_ *Prerequisites, c string) { mustChmod(t, c, 0o644) },
			func(_ *Prerequisites, c string) { mustChmod(t, c, 0o600) }},
		{"an agent build with another digest", PrerequisiteAgentMismatch,
			func(p *Prerequisites, _ string) { writeBuild(t, p, arch, []byte("another build")) },
			func(p *Prerequisites, _ string) { writeBuild(t, p, arch, build) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			p := &Prerequisites{Front: true, CredentialsDir: t.TempDir(), AgentBuildsDir: t.TempDir(), CPUArch: arch}
			writeBuild(t, p, arch, build)
			e.ctl.Prerequisites = p
			a := e.newAlloc("a1", testProvider)
			a.Spec.ForProvider.Batch.AgentDigest = digest
			if err := e.c.Update(t.Context(), a); err != nil {
				t.Fatal(err)
			}
			cred := agentapi.AllocationCredentialPath(p.CredentialsDir, string(a.UID))
			writeKey(t, cred)
			tc.breaks(p, cred)
			e.sync()
			e.sync()
			if n := e.batch.Count("sbatch"); n != 0 {
				t.Fatalf("%d sbatch calls with %s, want 0", n, tc.name)
			}
			if _, ok := e.alloc("a1").Annotations[v1alpha1.AnnotationSubmission]; ok {
				t.Error("a submission record was written: the wait is not an attempt")
			}
			if o := e.obs("a1"); o.Phase != v1alpha1.AllocationSubmitting || !strings.HasPrefix(o.Reason, tc.want) {
				t.Errorf("phase %s reason %q, want Submitting with %q", o.Phase, o.Reason, tc.want)
			}
			tc.fix(p, cred)
			e.sync()
			if n := e.batch.Count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls once %s is fixed, want 1", n, tc.name)
			}
		})
	}
}

// The default, no Prerequisites, submits with none of them, as before.
func TestNoPrerequisitesByDefault(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a1", testProvider)
	e.sync()
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Errorf("%d sbatch calls with no Prerequisites, want 1", n)
	}
}

func writeKey(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Repeat("ab", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mustChmod(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func writeBuild(t *testing.T, p *Prerequisites, arch string, content []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(p.AgentBuildsDir, agentapi.AgentBuildName(arch)), content, 0o700); err != nil {
		t.Fatal(err)
	}
}
