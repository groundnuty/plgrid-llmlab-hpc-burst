package rehearsal

// New. What the login node and the gateway need from the host, and the skip-or-fail rule of
// the tests that need it (the rule of internal/agent's sshdBinary, with its
// own switch).

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// EnvRequire makes a missing tool fail the test that needs it instead of
// skipping it, as CI sets it.
const EnvRequire = "HPCBURST_REQUIRE_SSHD"

// SSHDPath is the sshd binary: $HPCBURST_SSHD, else /usr/sbin/sshd.
func SSHDPath() string {
	if p := os.Getenv("HPCBURST_SSHD"); p != "" {
		return p
	}
	return "/usr/sbin/sshd"
}

// CheckTools reports which of sshd, ssh and ssh-keygen is missing, or nil.
func CheckTools(sshd string) error {
	var missing []string
	if fi, err := os.Stat(sshd); err != nil || fi.IsDir() {
		missing = append(missing, "sshd at "+sshd+" (set HPCBURST_SSHD)")
	}
	for _, tool := range []string{"ssh", "ssh-keygen"} {
		if _, err := exec.LookPath(tool); err != nil {
			missing = append(missing, tool+" on PATH")
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("rehearsal: the login node and the gateway need OpenSSH; missing: %s", strings.Join(missing, ", "))
	}
	return nil
}

// RequireTools skips the test, saying why, when sshd, ssh or ssh-keygen is
// missing, and fails it instead when HPCBURST_REQUIRE_SSHD=1.
func RequireTools(t testing.TB) {
	t.Helper()
	err := CheckTools(SSHDPath())
	switch {
	case err == nil:
	case os.Getenv(EnvRequire) == "1":
		t.Fatalf("%v: %s=1 forbids skipping", err, EnvRequire)
	default:
		t.Skipf("%v", err)
	}
}
