package launcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// frontPluginNoop is the call the launcher posts to the front's frps plugin to see that it
// answers. frps never posts a Ping there (the front's plugin is subscribed to NewProxy and
// CloseProxy only, frontPluginOps), and the front answers any op but those two with no action.
const frontPluginNoop = `{"version":"0.1.0","op":"Ping","content":{}}`

// checkFrontPlugin posts frontPluginNoop to the front's plugin on its health address and adds a
// problem unless the front accepts it (HBEP-6, Operational readiness; #371): frps refuses every new
// proxy while the front's plugin does not answer, since frps posts each registration to it, after
// the controller's, and stops at the first plugin that fails.
func checkFrontPlugin(addr string, eh *EntryHealth, add func(string, ...any)) {
	ok := false
	eh.FrpsPluginOK = &ok
	if err := postFrontPlugin(addr); err != nil {
		add("POST %s on %s: %v; frps refuses every new proxy while the front's plugin does not answer (HBEP-6, Admission)", agentapi.FrontFrpsPluginPath, addr, err)
		return
	}
	ok = true
}

// checkFrpsNamesTheFront adds a problem when the host runs a front and frps, and frps's own
// configuration does not post to the front's plugin at the front's health address (#371). frps
// would then register proxies the front never hears of, and the no-op call of checkFrontPlugin
// would still find the front answering: it proves the front listens, not that frps calls it.
func checkFrpsNamesTheFront(l layout, st *State, bad func(string, ...any)) {
	if st.FrontRun == nil || st.Frps == nil {
		return
	}
	path := FrpsConfigFile(l.host(st.Ports))
	b, err := os.ReadFile(path)
	if err != nil {
		bad("frps's configuration: %v", err)
		return
	}
	if addr := st.Ports.FrontHealthAddr(); !strings.Contains(string(b), frontPluginBlock(addr)) {
		bad("frps's configuration %s does not post to the front's plugin at %s: frps registers proxies the front never hears of (HBEP-6, Admission)", path, addr)
	}
}

// checkFrpsBulkNamesTheFront is checkFrpsNamesTheFront for the bulk path's second frps: with the
// bulk path and a front, its configuration posts to the front's plugin at the front's bulk path
// (HBEP-6, The bulk path), or the front would never take a bulk UID out of routing.
func checkFrpsBulkNamesTheFront(l layout, st *State, bad func(string, ...any)) {
	if !st.BulkPath || st.FrontRun == nil || st.Frps == nil {
		return
	}
	path := FrpsBulkConfigFile(l.host(st.Ports))
	b, err := os.ReadFile(path)
	if err != nil {
		bad("the bulk path's frps configuration: %v", err)
		return
	}
	if addr := st.Ports.FrontHealthAddr(); !strings.Contains(string(b), frontPluginBlockAt(addr, agentapi.FrontFrpsBulkPluginPath)) {
		bad("the bulk path's frps configuration %s does not post to the front's plugin at %s%s: it registers bulk proxies the front never hears of (HBEP-6, The bulk path)", path, addr, agentapi.FrontFrpsBulkPluginPath)
	}
}

// checkFrontBulkPlugin is checkFrontPlugin for the bulk path's plugin path on the front's health
// address (HBEP-6, The bulk path): the second frps posts each bulk registration there, and refuses
// every new bulk proxy while it does not answer.
func checkFrontBulkPlugin(addr string, eh *EntryHealth, add func(string, ...any)) {
	ok := false
	eh.FrpsBulkPluginOK = &ok
	if err := postFrontPluginAt(addr, agentapi.FrontFrpsBulkPluginPath); err != nil {
		add("POST %s on %s: %v; the bulk path's frps refuses every new bulk proxy while the front's plugin does not answer (HBEP-6, The bulk path)", agentapi.FrontFrpsBulkPluginPath, addr, err)
		return
	}
	ok = true
}

func postFrontPlugin(addr string) error {
	return postFrontPluginAt(addr, agentapi.FrontFrpsPluginPath)
}

// postFrontPluginAt posts frontPluginNoop to the front's plugin at path on addr.
func postFrontPluginAt(addr, path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), healthzTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, strings.NewReader(frontPluginNoop))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: healthzTimeout}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return fmt.Errorf("read the answer: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("answered %d", resp.StatusCode)
	}
	var v struct {
		Reject *bool `json:"reject"`
	}
	if json.Unmarshal(body, &v) != nil || v.Reject == nil || *v.Reject {
		return fmt.Errorf("answered %q, not an acceptance", body)
	}
	return nil
}
