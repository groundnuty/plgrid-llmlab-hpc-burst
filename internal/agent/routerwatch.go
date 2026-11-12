package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The agent's view of a running router (beta; HBEP-5, Worker health; HBEP-7, Several workers): every
// probe interval it reads each worker's health from Envoy's admin, /clusters?format=json on the
// read-only admin socket, and probes the router itself with GET /health through its listener, with no
// x-session-id, which reaches a worker Envoy holds healthy. Both go into the router's report; the
// controller writes status.healthyWorkers from the first and judges the router from the second.

// routerReadTimeout bounds one admin read or one probe.
const routerReadTimeout = 2 * time.Second

// watchRouterHealth starts the polling of a running router, until it exits or the agent ends.
func (a *Agent) watchRouterHealth(r *routerPart) {
	g := r.proc.Group
	admin := filepath.Join(a.o.RunDir, EnvoyAdminSocket)
	listener := a.routerListener()
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		for {
			t := time.NewTimer(time.Duration(a.probeEvery.Load()))
			select {
			case <-t.C:
			case <-g.Exited():
				t.Stop()
				return
			case <-a.ctx.Done():
				t.Stop()
				return
			}
			now := a.o.Now()
			ws, err := readClusters(a.ctx, admin)
			ok := probeRouter(a.ctx, listener)
			a.post(func() { a.routerRead(r, now, ws, err, ok) })
		}
	}()
}

// routerRead records one round of the router's polling on the loop.
func (a *Agent) routerRead(r *routerPart, now time.Time, ws []agentapi.RouterWorker, err error, ok bool) {
	if r.phase != agentapi.PartRunning {
		return
	}
	if err != nil {
		// Unknown this round: the last read stands, and its time says how old it is.
		a.log.Warn("router: cannot read its workers' health", "err", err)
	} else {
		prev := map[string]int32{}
		for _, w := range r.workers {
			prev[w.Name] = w.ConsecutiveFailedChecks
		}
		for i := range ws {
			if ws[i].FailedActiveCheck {
				ws[i].ConsecutiveFailedChecks = prev[ws[i].Name] + 1
			}
		}
		r.workers, r.clustersReadAt = ws, &now
	}
	p := agentapi.RouterProbe{Time: now, OK: ok}
	if !ok {
		p.ConsecutiveFailures = 1
		if r.probe != nil {
			p.ConsecutiveFailures = r.probe.ConsecutiveFailures + 1
		}
	}
	r.probe = &p
}

// unixGet sends one GET to a Unix socket and returns the status and body, at most 4 MiB of it.
func unixGet(ctx context.Context, socket, url string) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, routerReadTimeout)
	defer cancel()
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}, DisableKeepAlives: true, DisableCompression: true}
	defer tr.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, err
	}
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return resp.StatusCode, b, err
}

// probeRouter is the router's probe: GET /health through its listener, passed by a 200.
func probeRouter(ctx context.Context, listener string) bool {
	status, _, err := unixGet(ctx, listener, "http://instance/health")
	return err == nil && status == http.StatusOK
}

// readClusters reads each worker's health from the admin socket's /clusters?format=json.
func readClusters(ctx context.Context, admin string) ([]agentapi.RouterWorker, error) {
	status, body, err := unixGet(ctx, admin, "http://admin"+envoyClustersPath)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("%s answered %d", envoyClustersPath, status)
	}
	return parseClusters(body)
}

// parseClusters reads the workers' cluster out of Envoy's /clusters?format=json. A worker is healthy
// when Envoy routes to it: its active check is not failing, nor yet to pass for the first time, and
// outlier detection has not ejected it.
func parseClusters(body []byte) ([]agentapi.RouterWorker, error) {
	var doc struct {
		Clusters []struct {
			Name  string `json:"name"`
			Hosts []struct {
				Hostname string `json:"hostname"`
				Health   struct {
					FailedActive  bool `json:"failed_active_health_check"`
					FailedOutlier bool `json:"failed_outlier_check"`
					PendingActive bool `json:"pending_active_hc"`
				} `json:"health_status"`
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("%s: %w", envoyClustersPath, err)
	}
	for _, c := range doc.Clusters {
		if c.Name != envoyClusterName {
			continue
		}
		var ws []agentapi.RouterWorker
		for _, h := range c.Hosts {
			ws = append(ws, agentapi.RouterWorker{
				Name:              h.Hostname,
				Healthy:           !h.Health.FailedActive && !h.Health.FailedOutlier && !h.Health.PendingActive,
				FailedActiveCheck: h.Health.FailedActive,
				Ejected:           h.Health.FailedOutlier,
			})
		}
		return ws, nil
	}
	return nil, fmt.Errorf("%s names no cluster %q", envoyClustersPath, envoyClusterName)
}
