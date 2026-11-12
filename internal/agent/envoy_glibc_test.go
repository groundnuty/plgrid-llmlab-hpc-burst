package agent_test

// Envoy runs bare, and its release binary needs glibc 2.30 or later through its loader (#454): the agent
// checks the node's glibc before it starts Envoy, and refuses an older one, naming the glibc it found and
// the one Envoy needs, rather than let the exec fail with a loader's error. A fake loader stands in for
// the node's, as the science agent's ruling asks; the real Envoy pinned for this machine is the binary.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// fakeLoader writes a loader that answers --version with out, and makes the check run it.
func fakeLoader(t *testing.T, out string) {
	t.Helper()
	p := filepath.Join(shortDir(t), "ld.so")
	if err := os.WriteFile(p, []byte("#!/bin/sh\nprintf '%s\\n' '"+out+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(agent.SetEnvoyLoaderForTest(p))
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
		name, loader string
		refused      []string // what the refusal names; none when the start must pass
	}{
		{"RHEL 8's 2.28", "ld.so (GNU libc) stable release version 2.28.", []string{"needs glibc 2.30", "this node's glibc is 2.28"}},
		{"2.29", "ld.so (GNU libc) stable release version 2.29.", []string{"needs glibc 2.30", "glibc is 2.29"}},
		{"exactly 2.30", "ld.so (GNU libc) stable release version 2.30.", nil},
		{"Athena's 2.34", "ld.so (GNU libc) stable release version 2.34.", nil},
		{"a loader that names no version", "something else", []string{"needs glibc 2.30", "names no glibc version"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			fakeLoader(t, c.loader)
			err := startWithLoader(t)
			if c.refused == nil {
				if err != nil {
					t.Fatalf("with %q: %v, want Envoy started", c.loader, err)
				}
				return
			}
			if err == nil || !errors.Is(err, agent.ErrEnvoyRefused) {
				t.Fatalf("with %q: %v, want a refusal", c.loader, err)
			}
			for _, w := range c.refused {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("the refusal %q does not name %q", err, w)
				}
			}
		})
	}
}
