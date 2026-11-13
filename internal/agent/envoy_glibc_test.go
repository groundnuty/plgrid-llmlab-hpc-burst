package agent_test

// Envoy runs bare, and its release binary needs glibc 2.30 or later through its loader (#454): the agent
// checks the node's glibc before it starts Envoy, and refuses an older one, naming the glibc it found and
// the one Envoy needs, rather than let the exec fail with a loader's error. Each case is a fake node: its
// loader, at the path Envoy's ELF header names, and its getconf, which reports the node's glibc. The
// loader refuses --version, as every glibc before 2.33 does (#477's review): the check must read the
// glibc another way. The real Envoy pinned for this machine is the binary.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// preVersionLoader is a loader as glibc's were before 2.33: it takes --version for a program to run.
const preVersionLoader = "#!/bin/sh\necho \"$1: cannot open shared object file: No such file or directory\" >&2\nexit 127\n"

// fakeNode makes the check see a node with that loader and a getconf that prints getconfOut and exits
// getconfCode.
func fakeNode(t *testing.T, loader, getconfOut string, getconfCode int) {
	t.Helper()
	dir := shortDir(t)
	ld := filepath.Join(dir, "ld.so")
	if err := os.WriteFile(ld, []byte(loader), 0o755); err != nil {
		t.Fatal(err)
	}
	gc := filepath.Join(dir, "getconf")
	script := fmt.Sprintf("#!/bin/sh\n[ \"$1\" = GNU_LIBC_VERSION ] || exit 2\nprintf '%%s\\n' '%s'\nexit %d\n", getconfOut, getconfCode)
	if err := os.WriteFile(gc, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(agent.SetEnvoyLoaderForTest(ld))
	t.Cleanup(agent.SetGlibcProbeForTest(gc, "GNU_LIBC_VERSION"))
}

// startWithLoader starts the pinned Envoy as the agent does, and stops it when the test ends.
func startWithLoader(t *testing.T) error {
	t.Helper()
	dir := shortDir(t)
	rd, err := agent.NewReadiness()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rd.Close() })
	p, err := agent.StartEnvoy(agent.EnvoyStart{Binary: testenv.EnvoyBinary(t), Ready: 30 * time.Second,
		Config: agent.EnvoyConfig{RunDir: dir, Workers: []agent.EnvoyWorker{{Name: "worker-0", Socket: filepath.Join(dir, "worker-0.sock")}}, ReadinessPort: rd.Port()}}, time.Now())
	if err == nil {
		t.Cleanup(func() { p.Group.Stop(5 * time.Second); <-p.Group.Done(); p.Close() })
	}
	return err
}

func TestEnvoyIsRefusedOnAGlibcOlderThanItNeeds(t *testing.T) {
	for _, c := range []struct {
		name, getconf string
		code          int
		refused       []string // what the refusal names; none when the start must pass
	}{
		// A 2.30 to 2.32 node, whose loader has no --version yet, runs Envoy.
		{"2.30, a loader without --version", "glibc 2.30", 0, nil},
		{"2.32, a loader without --version", "glibc 2.32", 0, nil},
		{"Athena's 2.34", "glibc 2.34", 0, nil},
		// Helios's 2.28, the ruling's own case, is refused with both versions named.
		{"Helios's 2.28", "glibc 2.28", 0, []string{"needs glibc 2.30", "this node's glibc is 2.28"}},
		{"2.29", "glibc 2.29", 0, []string{"needs glibc 2.30", "this node's glibc is 2.29"}},
		{"getconf names no version", "something else", 0, []string{"needs glibc 2.30", "names no glibc version"}},
		{"getconf fails", "", 1, []string{"needs glibc 2.30", "reading this node's glibc"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			fakeNode(t, preVersionLoader, c.getconf, c.code)
			err := startWithLoader(t)
			if c.refused == nil {
				if err != nil {
					t.Fatalf("with glibc %q: %v, want Envoy started", c.getconf, err)
				}
				return
			}
			if err == nil || !errors.Is(err, agent.ErrEnvoyRefused) {
				t.Fatalf("with glibc %q: %v, want a refusal", c.getconf, err)
			}
			for _, w := range c.refused {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("the refusal %q does not name %q", err, w)
				}
			}
		})
	}
}

// A node without the loader Envoy names is refused, naming it, before any exec could fail with ENOENT.
func TestEnvoyIsRefusedOnANodeWithoutItsLoader(t *testing.T) {
	t.Cleanup(agent.SetEnvoyLoaderForTest(filepath.Join(shortDir(t), "no-such-ld.so")))
	t.Cleanup(agent.SetGlibcProbeForTest("getconf", "GNU_LIBC_VERSION"))
	err := startWithLoader(t)
	if err == nil || !errors.Is(err, agent.ErrEnvoyRefused) || !strings.Contains(err.Error(), "which this node does not have") || !strings.Contains(err.Error(), "ld-linux") {
		t.Fatalf("with no loader: %v, want a refusal naming Envoy's loader", err)
	}
}

// The real node: this machine's getconf reads its glibc, and the pinned Envoy starts (CI's runners and
// this VM have glibc 2.39).
func TestEnvoyStartsOnThisMachinesGlibc(t *testing.T) {
	if err := startWithLoader(t); err != nil {
		t.Fatalf("on this machine: %v", err)
	}
}
