package main

// The agent's startup errors name no path (#762, Astra's review; HBEP-4, Provider settings: the staging directory is account
// data, and the Allocation's directory is under it). With no agent.json and no credential in the Allocation's directory, the
// agent ends with a fixed message about each, and its standard error holds neither the directory nor its unique segment. The
// agent.json that cannot be read (a directory in its place) says why in the operating system's own words and names no path.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestStartupWithoutItsFilesNamesNoPath(t *testing.T) {
	const segment = "zq7f3akx"
	for _, c := range []struct {
		name  string
		setup func(t *testing.T, alloc string)
		want  []string
	}{
		{"no agent.json and no credential", func(t *testing.T, alloc string) {}, []string{
			"the agent's configuration file is missing in its Allocation directory",
			"the Allocation's credential is missing in its Allocation directory",
		}},
		{"an agent.json that is a directory", func(t *testing.T, alloc string) {
			if err := os.Mkdir(filepath.Join(alloc, agentapi.ConfigFileName), 0o700); err != nil {
				t.Fatal(err)
			}
		}, []string{"the agent's configuration file cannot be read: is a directory"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			base := t.TempDir()
			alloc := filepath.Join(base, segment, "stage", "alloc", "hb-x")
			if err := os.MkdirAll(alloc, 0o700); err != nil {
				t.Fatal(err)
			}
			c.setup(t, alloc)
			var stderr bytes.Buffer
			getenv := func(k string) string { return map[string]string{"SLURM_JOB_ID": "1"}[k] }
			if code := run([]string{alloc}, getenv, &stderr); code != 1 {
				t.Fatalf("exit %d, want 1; stderr %q", code, stderr.String())
			}
			out := stderr.String()
			for _, want := range c.want {
				if !strings.Contains(out, want) {
					t.Errorf("stderr lacks %q: %q", want, out)
				}
			}
			for _, bad := range []string{alloc, base, segment} {
				if strings.Contains(out, bad) {
					t.Errorf("stderr names %q: %q", bad, out)
				}
			}
		})
	}
}
