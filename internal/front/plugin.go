package front

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The operations frps posts to the front's plugin (frp v0.71.1,
// pkg/plugin/server/types.go). The launcher subscribes it to these two only.
const (
	opNewProxy   = "NewProxy"
	opCloseProxy = "CloseProxy"
)

// pluginBodyLimit bounds a plugin call's body. frps's NewProxy and CloseProxy
// are a few hundred bytes (the Login's metadata and the proxy's settings).
const pluginBodyLimit = 64 << 10

// frpsPlugin is the front's half of frps's server plugin (HBEP-6, Admission, as
// amended for #371), served at agentapi.FrontFrpsPluginPath on the front's
// health address. frps calls its plugins in turn, the lifecycle controller's
// first, and stops at the first that refuses or does not answer (frp v0.71.1,
// pkg/plugin/server/manager.go:70-98). So the front hears each NewProxy the
// controller admitted, and frps registers the proxy only after the front has
// answered, and has stopped routing to the UID (Tunnel.Bump). A route proven
// in an earlier epoch then no longer counts, and the UID is re-probed at once
// (Prober.Reregistered).
//
// frps waits for the answer on the frpc session's control connection with no
// timeout of its own (pkg/plugin/server/http.go:43-50), so the handler only
// changes the Tunnel's and the Prober's maps and answers: no I/O, no lock held
// across anything slow. HBEP-6's rule that a plugin answers within 1 s holds
// for it as for the controller's.
//
// It answers every call with a verdict, as frps expects. It refuses a NewProxy
// it cannot read, or whose proxy name no frpc could have registered, since it
// could not stop routing to that UID (failing closed; the controller's plugin
// refuses those first). A CloseProxy takes the UID's registration away too,
// and re-probes it the same way: frps posts CloseProxy from a goroutine of its
// own after it has dropped the proxy (server/control.go:667-681), so an old
// session's CloseProxy can come after the new session's NewProxy, and the
// re-probe gives the new registration back within its backoff. Any other call
// is answered with no action: it is how the launcher checks that the plugin
// answers (Operational readiness).
type frpsPlugin struct {
	tun    *Tunnel
	prober *Prober // nil when the front probes nothing
	log    *slog.Logger
}

// pluginRequest is frps's call (pkg/plugin/server/types.go:21-25, :46-54).
type pluginRequest struct {
	Op      string `json:"op"`
	Content struct {
		ProxyName string `json:"proxy_name"`
	} `json:"content"`
}

// pluginResponse is the answer frps reads (pkg/plugin/server/types.go:27-32).
type pluginResponse struct {
	Reject       bool   `json:"reject"`
	RejectReason string `json:"reject_reason,omitempty"`
	Unchange     bool   `json:"unchange"`
}

func (h *frpsPlugin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != agentapi.FrontFrpsPluginPath {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req pluginRequest
	err := json.NewDecoder(http.MaxBytesReader(w, r.Body, pluginBodyLimit)).Decode(&req)
	resp := pluginResponse{Unchange: true}
	switch {
	case err != nil:
		// The op is unknown, so it may be a NewProxy: refuse, rather than let
		// frps register a proxy the front did not hear of.
		resp.Reject, resp.RejectReason = true, "the front's plugin could not read the call; retry"
		h.log.Warn("frps's plugin call could not be read; refused", "error", err.Error())
	case req.Op == opNewProxy:
		uid := types.UID(req.Content.ProxyName)
		if err := checkUID(uid); err != nil {
			resp.Reject, resp.RejectReason = true, "proxy_name is not an Instance UID"
			h.log.Warn("frps's NewProxy names no Instance UID; refused", "proxy", req.Content.ProxyName)
			break
		}
		h.tun.Bump(uid)
		if h.prober != nil {
			h.prober.Reregistered(uid)
		}
		h.log.Info("frps registers the Instance's tunnel again; it routes nothing to it until a probe passes", "uid", string(uid))
	case req.Op == opCloseProxy:
		if uid := types.UID(req.Content.ProxyName); checkUID(uid) == nil {
			h.tun.Bump(uid)
			if h.prober != nil {
				h.prober.Reregistered(uid)
			}
			h.log.Info("frps closed the Instance's tunnel; it routes nothing to it until a probe passes", "uid", string(uid))
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
