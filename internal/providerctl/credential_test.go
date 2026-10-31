package providerctl

// The Allocation's credential is read as its consumers read it, with
// agentapi.ReadKeyFile, and must be mode 600 (HBEP-4, Provider settings and
// Submission; review of PR 76: Astra B9, Opus N13). A credential in any other
// form is waited for, never staged, and the reason names no path.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestCredentialInAnotherFormIsNeverStaged(t *testing.T) {
	key := []byte(strings.Repeat("ab", 32))
	place := func(e *env, a *v1alpha1.Allocation, content []byte, mode os.FileMode) {
		e.t.Helper()
		p := agentapi.AllocationCredentialPath(e.credDir, string(a.UID))
		_ = os.Remove(p)
		if err := os.WriteFile(p, content, 0o600); err != nil {
			e.t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			e.t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name   string
		breaks func(e *env, a *v1alpha1.Allocation)
	}{
		{"a symlink to a valid credential", func(e *env, a *v1alpha1.Allocation) {
			target := filepath.Join(e.t.TempDir(), "elsewhere")
			if err := os.WriteFile(target, key, 0o600); err != nil {
				e.t.Fatal(err)
			}
			p := agentapi.AllocationCredentialPath(e.credDir, string(a.UID))
			_ = os.Remove(p)
			if err := os.Symlink(target, p); err != nil {
				e.t.Fatal(err)
			}
		}},
		{"mode 400", func(e *env, a *v1alpha1.Allocation) { place(e, a, key, 0o400) }},
		{"mode 700", func(e *env, a *v1alpha1.Allocation) { place(e, a, key, 0o700) }},
		{"mode 640", func(e *env, a *v1alpha1.Allocation) { place(e, a, key, 0o640) }},
		{"not a key", func(e *env, a *v1alpha1.Allocation) { place(e, a, []byte("not-a-key\n"), 0o600) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			a := e.newAlloc("a")
			tc.breaks(e, a)
			e.run("a", 3)
			if n := e.count("sbatch"); n != 0 {
				t.Fatalf("%d sbatch calls with the credential %s, want 0", n, tc.name)
			}
			if n := e.count("ensure-file"); n != 0 {
				t.Errorf("%d files staged with the credential %s, want 0", n, tc.name)
			}
			o := e.get("a").Status.AtProvider
			if o.Phase != v1alpha1.AllocationSubmitting || !strings.Contains(o.Reason, "credential") {
				t.Errorf("phase %s reason %q, want Submitting, waiting for the credential", o.Phase, o.Reason)
			}
			if strings.Contains(o.Reason, e.credDir) {
				t.Errorf("the reason %q names the credentials directory", o.Reason)
			}
			for _, ev := range e.rec.events {
				if strings.Contains(ev.Message, e.credDir) {
					t.Errorf("event %s %q names the credentials directory", ev.Reason, ev.Message)
				}
			}

			// The control: the same Allocation submits once the credential is a
			// mode-600 key file.
			if err := os.Remove(agentapi.AllocationCredentialPath(e.credDir, string(a.UID))); err != nil {
				t.Fatal(err)
			}
			e.writeCredential(a)
			e.run("a", 3)
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls once the credential is a mode-600 key file, want 1", n)
			}
		})
	}
}
