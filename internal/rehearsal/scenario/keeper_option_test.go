package scenario_test

// The keeper rehearsal's options of the rig (#343, PR 5) only append their own
// flags to hpcburst up: an alpha rig's up runs the arguments it ran before
// them, and an up after down (Host.UpAgain) runs the first up's arguments
// without the options'. No stack starts: hpcburst is a script that records its
// arguments.

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// TestRigKeeperOptionsAppendOnlyTheirFlags: up's recorded arguments, without
// the options, with --relay-keeper and --version, and on an up after down.
//
//livecheck:none a test of the rig's host on a double of hpcburst; nothing live runs
func TestRigKeeperOptionsAppendOnlyTheirFlags(t *testing.T) {
	// The controller's /readyz, on up's base+6: a kernel-chosen port, and the base it implies.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(srv.Close)
	_, port, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	ports := stack.Ports{Base: n - 6, ClientAddr: "127.0.0.1:1"}
	up := func(s rigSetup) (*stack.Host, string, []string) {
		dir := stack.ShortDir(t)
		bin := filepath.Join(dir, "hpcburst")
		// up records its arguments under its state directory, $3; down stops nothing.
		script := "#!/bin/sh\nif [ \"$1\" = up ]; then mkdir -p \"$3\" && printf '%s\\n' \"$@\" >> \"$3/up-args\"; fi\n"
		if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
		h := startRigHost(t, stack.Binaries{HPCBurst: bin, K3sDir: dir, Repo: dir}, dir, stack.DialHome{Mode: "host-dialed"}, os.Environ(), ports, s.hostOptions()...)
		alpha := []string{"up", "--state-dir", filepath.Join(dir, "st"), "--bin-dir", dir, "--port-base", strconv.Itoa(ports.Base), "--nice", "10",
			"--manifests", filepath.Join(dir, "config", "crd"), "--controller-bin", "", "--front-config", filepath.Join(dir, "front.json"),
			"--front-bin", "", "--frps-bin", "", "--front-mode", "host-dialed"}
		return h, filepath.Join(dir, "st", "up-args"), alpha
	}
	recorded := func(path string) []string {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	}

	_, path, alpha := up(rigSetup{})
	if got := recorded(path); !slices.Equal(got, alpha) {
		t.Errorf("an alpha rig's up ran %q, want %q", got, alpha)
	}
	_, path, alpha = up(rigSetup{relayKeeper: true})
	if got, want := recorded(path), append(alpha, "--relay-keeper"); !slices.Equal(got, want) {
		t.Errorf("a keeper rig's up ran %q, want %q", got, want)
	}
	h, path, alpha := up(rigSetup{relayKeeper: true, k3sVersion: "1.35.9"})
	if got, want := recorded(path), append(alpha, "--relay-keeper", "--version", "1.35.9"); !slices.Equal(got, want) {
		t.Errorf("a keeper rig's up on an older k3s ran %q, want %q", got, want)
	}
	if err := h.DownErr(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if out, err := h.UpAgain("--relay-keeper"); err != nil {
		t.Fatalf("up again: %v\n%s", err, out)
	}
	if got, want := recorded(path), append(alpha, "--relay-keeper"); !slices.Equal(got, want) {
		t.Errorf("the up after down ran %q, want %q: the first up's arguments without the options'", got, want)
	}
}
