package launcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

func postFrontPlugin(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), healthzTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+agentapi.FrontFrpsPluginPath, strings.NewReader(frontPluginNoop))
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
