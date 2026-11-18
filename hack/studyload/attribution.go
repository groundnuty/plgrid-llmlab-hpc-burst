package main

import "net/http"

// The front's attribution headers (HBEP-5, "Attribution headers"). The names are copied from internal/front/errors.go lines 85-89 on
// main (8832492c): importing internal/front would pull the whole Kubernetes client into a load tool. TestAttributionNamesMatchTheFront
// reads that file and fails if a name here drifts from its constant. A route is (provider, deployment ID, instance).
const (
	headerRequestID    = "Hpcburst-Request-Id"
	headerEmitter      = "Hpcburst-Emitter"
	headerProvider     = "Hpcburst-Provider"
	headerDeploymentID = "Hpcburst-Deployment-Id"
	headerInstance     = "Hpcburst-Instance"
)

// Attribution is what a response's attribution headers said; a header the response lacked is "".
type Attribution struct {
	RequestID    string `json:"requestId,omitempty"`
	Emitter      string `json:"emitter,omitempty"`
	Provider     string `json:"provider,omitempty"`
	DeploymentID string `json:"deploymentId,omitempty"`
	Instance     string `json:"instance,omitempty"`
}

func attributionOf(h http.Header) Attribution {
	return Attribution{
		RequestID:    h.Get(headerRequestID),
		Emitter:      h.Get(headerEmitter),
		Provider:     h.Get(headerProvider),
		DeploymentID: h.Get(headerDeploymentID),
		Instance:     h.Get(headerInstance),
	}
}

// Route is the (provider, deployment ID, instance) a request was answered by.
type Route struct{ Provider, DeploymentID, Instance string }

func (a Attribution) route() Route { return Route{a.Provider, a.DeploymentID, a.Instance} }

// String is the route as a summary line names it; a response with no attribution headers is "(unattributed)".
func (r Route) String() string {
	if r == (Route{}) {
		return "(unattributed)"
	}
	return "provider=" + r.Provider + " deployment=" + r.DeploymentID + " instance=" + r.Instance
}
