package launcher

import (
	"fmt"
	"net/http"
)

// The names of the lifecycle controller's two readiness checks (internal/lifecycle, ReadyzCaches
// and ReadyzLeading; HBEP-11 names them). controller-runtime serves each on its own at
// /readyz/<name>, besides the aggregate /readyz.
const (
	ReadyzCaches  = "caches"
	ReadyzLeading = "leading"
)

// The states of EntryHealth.ReadyzState.
const (
	ReadyzReady      = "ready"
	ReadyzStarting   = "starting"    // the caches have not synced
	ReadyzNotLeading = "not leading" // the caches have synced, and the controller does not hold its Lease
	// ReadyzUnspecified is the state of a controller that does not serve the two checks apart (it
	// answers 404 on their paths: one from before the split, whose single check "serving" covers
	// both) and whose /readyz is not 200. The aggregate does not say which of the two fails.
	ReadyzUnspecified = "starting, or not leading its Lease: /readyz does not say which"
)

// checkReadyz reads the lifecycle controller's readiness (HBEP-8, Observers: /readyz fails until
// the caches have synced and the controller leads its Lease, and /healthz stays liveness) and adds
// a problem unless it is ready. It reads each check on its own path and decides by status code
// only, never by a body (`@hpc-burst-science-agent`, 1 Nov 2026, #147):
//
//   - GET /readyz/caches not 200: starting.
//   - caches 200 and GET /readyz/leading not 200: not leading.
//   - both 200: ready.
//   - 404 on either path: an older controller, which has not split its check. The aggregate
//     GET /readyz decides: 200 is ready, any other status is ReadyzUnspecified.
//
// The paths are controller-runtime v0.23.1's (sigs.k8s.io/controller-runtime): pkg/manager/
// internal.go:308-310 mounts /readyz and /readyz/ on the health server; pkg/healthz/healthz.go:
// 158-165 serves /readyz/<name> for one registered check and answers 404 for an unknown name;
// CheckHandler (healthz.go:175-180) answers 200 when the check passes and 500 when it fails; and
// the aggregate answers 500 when any check fails (healthz.go:96-97). The body of a failing check
// withholds its reason, so there is nothing in it to read.
func checkReadyz(addr string, eh *EntryHealth, add func(string, ...any)) {
	ready := false
	eh.ReadyzOK = &ready
	probe := func(path string) (int, bool) {
		code, _, err := probeEndpoint(addr, path, healthzTimeout)
		if err != nil {
			add("GET %s on %s: %v", path, addr, err)
			return 0, false
		}
		return code, true
	}
	cachesPath, leadingPath := "/readyz/"+ReadyzCaches, "/readyz/"+ReadyzLeading
	code, ok := probe(cachesPath)
	if !ok {
		return
	}
	if code != http.StatusNotFound {
		if code != http.StatusOK {
			eh.ReadyzState = ReadyzStarting
			add("not ready: %s: GET %s on %s answered %d", ReadyzStarting, cachesPath, addr, code)
			return
		}
		if code, ok = probe(leadingPath); !ok {
			return
		}
		switch code {
		case http.StatusOK:
			ready, eh.ReadyzState = true, ReadyzReady
			return
		case http.StatusNotFound: // falls back below
		default:
			eh.ReadyzState = ReadyzNotLeading
			add("not ready: %s: GET %s on %s answered %d", ReadyzNotLeading, leadingPath, addr, code)
			return
		}
	}
	// A 404 on either path: an older controller. Only its aggregate can be read.
	if code, ok = probe("/readyz"); !ok {
		return
	}
	if code == http.StatusOK {
		ready, eh.ReadyzState = true, ReadyzReady
		return
	}
	eh.ReadyzState = ReadyzUnspecified
	add("not ready: GET /readyz on %s answered %d (%s)", addr, code, ReadyzUnspecified)
}

// ReadReadiness reads the lifecycle controller's readiness at its health address the way status
// does, and returns the state (EntryHealth.ReadyzState: ReadyzReady, ReadyzStarting,
// ReadyzNotLeading, ReadyzUnspecified, or empty when the paths could not be read) and the
// problems status would report. The controller's own tests read a real controller with it.
func ReadReadiness(addr string) (state string, problems []string) {
	var eh EntryHealth
	checkReadyz(addr, &eh, func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) })
	return eh.ReadyzState, problems
}
