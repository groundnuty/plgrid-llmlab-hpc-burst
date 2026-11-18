package slurm

// The bootstrap names no path on the job's standard error (#762, Astra's review; HBEP-4, Provider settings: the staging
// directory is account data, and job.err is read by people and carried by tools). The generated line runs under a real shell,
// with the staged agent binary missing, with a digest of other bytes, and with the binary present, its digest matching and its
// mode 0644. In each the job fails, and the shell's standard error holds neither the binary's path, nor the directory it is
// under, nor that directory's unique segment. The control is the agent it must still start: its own standard error reaches
// job.err untouched.

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// shellsForTheBootstrap is the login node's /bin/sh and, where there is one, bash: they word their errors differently.
func shellsForTheBootstrap(t *testing.T) []string {
	t.Helper()
	out := []string{"/bin/sh"}
	if p, err := exec.LookPath("bash"); err == nil {
		out = append(out, p)
	}
	return out
}

func TestBootstrapStderrNamesNoPath(t *testing.T) {
	needTools(t, "sh", "sha256sum", "cut")
	const segment = "zq7f3akx"
	for _, shell := range shellsForTheBootstrap(t) {
		for _, c := range []struct {
			name  string
			stage bool
			mode  os.FileMode
			wrong bool // a digest of other bytes
		}{
			{"no binary", false, 0, false},
			{"a digest of other bytes", true, 0o755, true},
			{"the digest matches and the binary is not executable", true, 0o644, false},
		} {
			t.Run(filepath.Base(shell)+" "+c.name, func(t *testing.T) {
				base := t.TempDir()
				staging := filepath.Join(base, segment, "stage")
				agent := filepath.Join(staging, "agent", "d", "hb-agent")
				marker := filepath.Join(base, "ran")
				body := []byte("#!/bin/sh\ntouch " + marker + "\n")
				if c.stage {
					if err := os.MkdirAll(filepath.Dir(agent), 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(agent, body, c.mode); err != nil {
						t.Fatal(err)
					}
					if err := os.Chmod(agent, c.mode); err != nil {
						t.Fatal(err)
					}
				}
				digest := sha256Hex(body)
				if c.wrong {
					digest = strings.Repeat("0", 64)
				}
				cmd := exec.Command(shell, "-c", bootstrap(agent, digest, filepath.Join(staging, "alloc", "hb-x")))
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				err := cmd.Run()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() == 0 {
					t.Errorf("the bootstrap ended with %v; want a non-zero exit, so the job fails", err)
				}
				if _, serr := os.Stat(marker); !os.IsNotExist(serr) {
					t.Errorf("the agent ran (marker: %v)", serr)
				}
				for _, bad := range []string{agent, staging, base, segment} {
					if strings.Contains(stderr.String(), bad) {
						t.Errorf("the job's standard error names %q: %q", bad, stderr.String())
					}
				}
				if !strings.Contains(stderr.String(), "hb: the staged agent binary is missing, changed or not executable") {
					t.Errorf("the job's standard error does not say why it failed: %q", stderr.String())
				}
			})
		}
		// The control: a runnable binary with the right digest starts, and its own standard error still reaches job.err.
		t.Run(filepath.Base(shell)+" control", func(t *testing.T) {
			base := t.TempDir()
			agent := filepath.Join(base, segment, "stage", "agent", "d", "hb-agent")
			if err := os.MkdirAll(filepath.Dir(agent), 0o755); err != nil {
				t.Fatal(err)
			}
			body := []byte("#!/bin/sh\necho \"the agent's own words\" >&2\nexit 7\n")
			if err := os.WriteFile(agent, body, 0o755); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(shell, "-c", bootstrap(agent, sha256Hex(body), filepath.Join(base, "alloc")))
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			err := cmd.Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 7 {
				t.Errorf("the bootstrap ended with %v; want the agent's exit status 7", err)
			}
			if got := stderr.String(); got != "the agent's own words\n" {
				t.Errorf("the job's standard error is %q, want only the agent's own words", got)
			}
		})
	}
}
