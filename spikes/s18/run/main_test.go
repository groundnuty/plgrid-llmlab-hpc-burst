// Package run is spike S18's runs: the controls that make its measurements valid, and the measuring
// cells. It is a test binary because the engine double (internal/agent/enginedouble) is imported only by
// tests: TestMain hands the process to the double when it was started as one.
package run

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s18/rig"
)

func TestMain(m *testing.M) {
	enginedouble.Dispatch()
	os.Exit(m.Run())
}

// engine is the doubles' command: this test binary as the engine.
func engine(t testing.TB) rig.EngineCmd {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return func(socket, log, modes, key string) *exec.Cmd {
		c := exec.Command(exe, "serve", "/ckpt", "--uds="+socket, "--served-model-name="+rig.Model, "--max-num-seqs=64")
		c.Env = []string{enginedouble.EnvRole + "=" + enginedouble.RoleEngine, enginedouble.EnvModes + "=" + modes,
			enginedouble.EnvEngineLog + "=" + log, enginedouble.EnvAPIKey + "=" + key}
		return c
	}
}

// stand starts a stand of 3 Instances with fault and policy (nil: arm a), stopped at the test's end.
func stand(t *testing.T, fault string, p *rig.Policy) *rig.Rig {
	t.Helper()
	r, err := rig.New(shortDir(t), rig.Config{N: 3, Fault: fault, Policy: p, D1: 200 * time.Millisecond, D2: time.Second,
		Engine: engine(t), TokenDelay: "5ms"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r
}

// shortDir is a temporary directory with a short path: a Unix socket's path is at most 107 bytes.
func shortDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "s18")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}

// sendMany sends n unmarked requests, each in a session of its own, half of them streamed, at most
// par at once, and returns their results.
func sendMany(ctx context.Context, c *rig.Client, n, par int) []rig.Result {
	out := make([]rig.Result, n)
	sem := make(chan struct{}, par)
	var wg sync.WaitGroup
	for k := 0; k < n; k++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(k int) {
			defer wg.Done()
			defer func() { <-sem }()
			out[k] = c.Do(ctx, c.NextBase(), 0, "", k%2 == 1, "")
		}(k)
	}
	wg.Wait()
	return out
}
