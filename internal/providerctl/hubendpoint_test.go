package providerctl

import (
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// agent.json carries the controller's Hub origin (Options.HubEndpoint, from
// its HF_ENDPOINT), since a job does not inherit the control-plane host's
// environment (ruling of 1 Nov 2026 on #156); with none configured the field
// is absent, and the agent falls back to its own environment.
func TestAgentJSONCarriesTheHubEndpoint(t *testing.T) {
	const staging = "/scratch/user-a/stage/"
	for _, hub := range []string{"http://127.0.0.1:29537", ""} {
		t.Run("hub="+hub, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			// The environment's own Front, with its staged frpc build and
			// certificate; only the Hub origin differs between the cases.
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", func(e *realEnv) {
				e.opts.HubEndpoint = hub
			})
			a := e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 3)
			js, ok := sl.Files()[staging+agentapi.StagedConfigPath(v1alpha1.JobTag(a.Namespace, a.Name, a.UID))]
			if !ok {
				t.Fatal("agent.json was not staged")
			}
			cfg, err := agentapi.DecodeAgentConfig(js.Content)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.HubEndpoint != hub {
				t.Errorf("agent.json hubEndpoint %q, want %q", cfg.HubEndpoint, hub)
			}
			if hub == "" && strings.Contains(string(js.Content), "hubEndpoint") {
				t.Errorf("agent.json names hubEndpoint with none configured: %s", js.Content)
			}
		})
	}
}
