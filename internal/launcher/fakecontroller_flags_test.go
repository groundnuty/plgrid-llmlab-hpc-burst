package launcher

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"
)

// TestFakeControllerRefusesFrontFlagsTheRealOneRefuses: the fake stands in for the controller's
// command line (cmd/hpcburst-controller), so a launcher that passes --front-addr with host-dialed
// mode, or a direct or ssh mode without one, fails its start. HBEP-6, Dial-home modes: host-dialed
// has no address because the host dials the jobs; direct and ssh need one. The accepted forms are
// run by TestSuperviseCommandLine, whose fake would exit 2 on any of them if they were refused.
func TestFakeControllerRefusesFrontFlagsTheRealOneRefuses(t *testing.T) {
	bin := fakeControllerBin(t)
	base := []string{"--state-dir", t.TempDir(), "--port-base", "29220", "--health-addr", "127.0.0.1:29226"}
	for name, extra := range map[string][]string{
		"host-dialed with an address": {"--front-mode", "host-dialed", "--front-addr", "front.example.test:22"},
		"direct without an address":   {"--front-mode", "direct"},
		"ssh without an address":      {"--front-mode", "ssh"},
		"an address without a mode":   {"--front-addr", "front.example.test:22"},
		"an unknown mode and address": {"--front-mode", "tunnel", "--front-addr", "front.example.test:22"},
		"an unknown mode and no addr": {"--front-mode", "tunnel"},
	} {
		t.Run(name, func(t *testing.T) {
			// A line the fake accepts would make it serve until a TERM: the deadline turns that into a failure.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bin, append(append([]string{}, base...), extra...)...)
			cmd.Env = []string{"FAKECTRL_DIR=" + t.TempDir()}
			err := cmd.Run()
			var ee *exec.ExitError
			if !errors.As(err, &ee) || ee.ExitCode() != 2 {
				t.Fatalf("the fake controller with %q: got %v, want exit status 2", extra, err)
			}
		})
	}
}
