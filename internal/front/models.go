package front

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/naming"
)

// modelEntry is one entry of GET /v1/models: OpenAI's model object plus the
// additive routable member (HBEP-5, open question 9).
type modelEntry struct {
	ID       string `json:"id"`
	Object   string `json:"object"`
	Created  int64  `json:"created"`
	OwnedBy  string `json:"owned_by"`
	Routable bool   `json:"routable"`
}

// Models lists each plain model name that has a ModelDeployment, and whether
// a request for it could be placed now: an eligible Instance of ours, or an
// attached managed entry that is not paused.
func (r *Router) Models() []modelEntry {
	r.lockFresh()
	defer r.mu.Unlock()
	if r.view == nil {
		return nil
	}
	now := r.clk.Now()
	var out []modelEntry
	for name, m := range r.view.Models {
		routable := false
		for _, d := range m.Served {
			for _, i := range d.Members {
				routable = routable || (i.Eligible && !r.pausedLocked(i.Key, now))
			}
		}
		for _, t := range m.Managed {
			routable = routable || !r.pausedLocked(t.Key, now)
		}
		out = append(out, modelEntry{ID: name, Object: "model", OwnedBy: "hpcburst", Routable: routable})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// serveModels answers GET /v1/models and GET /v1/models/{model}.
func (p *Proxy) serveModels(w http.ResponseWriter, r *http.Request, x *exchange) {
	models := p.router.Models()
	var body any = struct {
		Object string       `json:"object"`
		Data   []modelEntry `json:"data"`
	}{Object: "list", Data: append([]modelEntry{}, models...)}
	if id, ok := strings.CutPrefix(r.URL.Path, "/v1/models/"); ok {
		// "<model>#<workload>" (HBEP-5, Workloads, Naming it; written %23 in the URL, which the path above is decoded from): the model's card
		// when the Workload exists for that model, an unlisted name as any other when it does not, or when the name breaks the grammar. The
		// card is the model's own, unchanged.
		name := id
		known := true
		if model, wl, hasSuffix := strings.Cut(id, "#"); hasSuffix {
			name = model
			known = model != "" && naming.IsWorkloadName(wl) && p.router.HasWorkload(model, wl)
		}
		var found *modelEntry
		for i := range models {
			if models[i].ID == name && known {
				found = &models[i]
			}
		}
		if found == nil {
			// An additive extension: LLMLab has no retrieve route. The name
			// is the path's rest, decoded once, so a raw "/" and %2F name the
			// same model; an unlisted name is 404 model_not_found, as in
			// OpenAI's contract (@hpc-burst-science-agent, 31 Oct 2026).
			p.fail(w, x, http.StatusNotFound, codeModelNotFound, "", "", "the model "+echoedName(id)+" is not listed")
			return
		}
		body = found
	}
	x.attr.set(w.Header())
	w.Header().Set("Content-Type", "application/json")
	x.rec.Status = http.StatusOK
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(body)
}

// HasWorkload reports whether a Workload of that name has that model as its
// spec.model, in a namespace the front watches.
func (r *Router) HasWorkload(model, name string) bool {
	r.lockFresh()
	defer r.mu.Unlock()
	return r.view != nil && len(r.view.Workloads[model][name]) > 0
}
