package rehearsal

// New. The tool check and the skip-or-fail rule, and the refusal of a session
// that asks for no command.

import (
	"bytes"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// recorder stands in for *testing.T to see what RequireTools does with it.
type recorder struct {
	testing.TB
	skipped, failed string
}

func (r *recorder) Helper()                   {}
func (r *recorder) Skipf(f string, a ...any)  { r.skipped = fmt.Sprintf(f, a...) }
func (r *recorder) Fatalf(f string, a ...any) { r.failed = fmt.Sprintf(f, a...) }

func TestMissingToolsSkipOrFail(t *testing.T) {
	t.Setenv("HPCBURST_SSHD", filepath.Join(t.TempDir(), "no-such-sshd"))
	err := CheckTools(SSHDPath())
	if err == nil || !strings.Contains(err.Error(), "sshd at ") || !strings.Contains(err.Error(), "HPCBURST_SSHD") {
		t.Errorf("CheckTools with a missing sshd: %v, want an error naming sshd and HPCBURST_SSHD", err)
	}
	t.Setenv(EnvRequire, "")
	skip := &recorder{}
	RequireTools(skip)
	if skip.skipped == "" || skip.failed != "" {
		t.Errorf("a missing sshd without %s: skipped %q, failed %q; want a skip that says why", EnvRequire, skip.skipped, skip.failed)
	}
	t.Setenv(EnvRequire, "1")
	fail := &recorder{}
	RequireTools(fail)
	if fail.failed == "" || fail.skipped != "" {
		t.Errorf("a missing sshd with %s=1: skipped %q, failed %q; want a failure", EnvRequire, fail.skipped, fail.failed)
	}
	// ssh and ssh-keygen are looked up on PATH.
	t.Setenv("PATH", t.TempDir())
	if err := CheckTools("/"); err == nil || !strings.Contains(err.Error(), "ssh on PATH") || !strings.Contains(err.Error(), "ssh-keygen on PATH") {
		t.Errorf("CheckTools with an empty PATH: %v, want ssh and ssh-keygen named", err)
	}
}

// Only commands are offered: a session that asks for a shell ends with exit
// 255 and says so, and no command runs.
func TestNoShellIsOffered(t *testing.T) {
	r, _ := newRig(t)
	var stderr bytes.Buffer
	cmd := exec.CommandContext(t.Context(), "ssh", "-F", filepath.Join(r.base, "creds"),
		"-o", "BatchMode=yes", "-o", "IdentityAgent=none", "-o", "ConnectTimeout=20", "-T", alias)
	cmd.Stderr = &stderr
	err := cmd.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 255 || !strings.Contains(stderr.String(), "no command") {
		t.Errorf("ssh with no command: %v, stderr %q; want exit 255 and a message that no command was given", err, stderr.String())
	}
	if n := len(r.plat.Calls()); n != 0 {
		t.Errorf("%d commands ran on the platform, want none", n)
	}
}
