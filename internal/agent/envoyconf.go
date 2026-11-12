package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

// Envoy's bootstrap, as the agent renders it for an Instance's router (HBEP-5, Inside an Instance;
// beta, opt-in, #429). It is spike S13's plain proxy (spikes/s13/envoy/ring-hash.yaml: ring_hash on
// x-session-id, upstream connections idle after 50 s, HTTP/1.1 to the workers) with HBEP-5's settings
// and the science agent's rulings on #429 (12 Nov 2026):
//   - the listener is a mode-0600 Unix socket in the job's private directory, never a TCP port, and
//     the admin likewise, allowing exactly /clusters?format=json (Q3), with no admin runtime layer;
//   - the route's timeout and idle_timeout and the connection manager's stream_idle_timeout are each
//     EnvoyRouterTimeout, 10,800 s, which the launch guard checks (Q7, as corrected);
//   - the circuit breakers' three thresholds are Envoy's field maximum (Q8), since the front's cap
//     governs and an overflow 503 would count toward the Instance's pause;
//   - the router filter suppresses the x-envoy-* headers Envoy would add, so the workers see only
//     what HBEP-5 lists (the observation of #429's stage 0).
//
// The JSON is written with encoding/json from the types below, which mirror Envoy's v3 API names; the
// agent links nothing outside the standard library. A guard that reads the rendered bytes back
// (envoyguard.go) decides whether Envoy may run them.

// Names and paths in the job's private run directory.
const (
	// EnvoyListenerSocket is the router's listener, the Instance's entry point behind the tunnel.
	EnvoyListenerSocket = "router.sock"
	// EnvoyAdminSocket is Envoy's admin interface, read-only: the agent reads /clusters from it.
	EnvoyAdminSocket = "envoy-admin.sock"
	// EnvoyConfigFile is the bootstrap the agent renders, mode 0600.
	EnvoyConfigFile = "envoy.json"
	// EnvoyLogFile is Envoy's log (--log-path): a trace-level log would print request headers, the
	// Instance key among them, so it stays in the private directory (HBEP-5).
	EnvoyLogFile = "envoy.log"

	// envoyClusterName is the cluster of the Instance's workers, the ring's members.
	envoyClusterName = "workers"
	// envoySessionHeader is the header ring_hash hashes: the session key the front sets on every
	// request to an Instance (HBEP-5, Conversation key).
	envoySessionHeader = "x-session-id"
	// envoyPipeMode is the mode of both of Envoy's sockets, 0600.
	envoyPipeMode = 0o600
	// envoyUpstreamIdle is how long Envoy keeps an idle connection to a worker: below the workers'
	// VLLM_HTTP_TIMEOUT_KEEP_ALIVE of 120 s, so the side that reuses connections closes them first
	// (HBEP-5; L-2.5).
	envoyUpstreamIdle = 50 * time.Second
	// envoyConnectTimeout bounds a connection to a worker's socket (S13's setting).
	envoyConnectTimeout = 5 * time.Second
	// envoyMaxThreshold is Envoy's field maximum for a circuit-breaker threshold (a uint32).
	envoyMaxThreshold = 1<<32 - 1
	// EnvoyRouterTimeout is each of the router's three timeouts: the front's maximum request duration,
	// 2 h, plus 1 h (HBEP-5; HBEP-7, Several workers). It is never taken from a deployment's
	// routing.maxRequestDuration, which is mutable and can only shorten the front's: Envoy's config is
	// rendered once, so a deployment created with a short value that later dropped it would have its long
	// replies cut, with no line in any log (L-4.1). A shorter deployment value needs nothing, since the
	// front cuts first (the science agent's ruling on #429, Q7, as corrected on 12 Nov 2026).
	EnvoyRouterTimeout = 3 * time.Hour
	// envoyClustersPath is the one admin path allowed: the agent's read of each worker's health.
	envoyClustersPath = "/clusters?format=json"
)

// EnvoyWorker is one endpoint of the workers' cluster: a worker by name, on its engine socket.
type EnvoyWorker struct {
	Name   string // WorkerName(i)
	Socket string // the engine's Unix socket in the run directory
}

// EnvoyConfig is what the bootstrap is rendered from.
type EnvoyConfig struct {
	// RunDir is the job's private run directory; both of Envoy's sockets are in it.
	RunDir string
	// Workers are the Instance's workers, in order.
	Workers []EnvoyWorker
}

// The bootstrap's types: only the fields the agent sets, in Envoy's JSON names.
type (
	envoyBootstrap struct {
		Admin           envoyAdmin           `json:"admin"`
		StaticResources envoyStaticResources `json:"static_resources"`
	}
	envoyAdmin struct {
		Address    envoyAddress        `json:"address"`
		AllowPaths []map[string]string `json:"allow_paths"`
	}
	envoyAddress struct {
		Pipe *envoyPipe `json:"pipe"`
	}
	envoyPipe struct {
		Path string `json:"path"`
		Mode uint32 `json:"mode,omitempty"`
	}
	envoyStaticResources struct {
		Listeners []envoyListener `json:"listeners"`
		Clusters  []envoyCluster  `json:"clusters"`
	}
	envoyListener struct {
		Name         string             `json:"name"`
		Address      envoyAddress       `json:"address"`
		FilterChains []envoyFilterChain `json:"filter_chains"`
	}
	envoyFilterChain struct {
		Filters []envoyNamedConfig `json:"filters"`
	}
	envoyNamedConfig struct {
		Name        string `json:"name"`
		TypedConfig any    `json:"typed_config"`
	}
	envoyHCM struct {
		Type              string             `json:"@type"`
		StatPrefix        string             `json:"stat_prefix"`
		StreamIdleTimeout string             `json:"stream_idle_timeout"`
		RouteConfig       envoyRouteConfig   `json:"route_config"`
		HTTPFilters       []envoyNamedConfig `json:"http_filters"`
	}
	envoyRouteConfig struct {
		Name         string             `json:"name"`
		VirtualHosts []envoyVirtualHost `json:"virtual_hosts"`
	}
	envoyVirtualHost struct {
		Name    string       `json:"name"`
		Domains []string     `json:"domains"`
		Routes  []envoyRoute `json:"routes"`
	}
	envoyRoute struct {
		Match map[string]string `json:"match"`
		Route envoyRouteAction  `json:"route"`
	}
	envoyRouteAction struct {
		Cluster     string            `json:"cluster"`
		Timeout     string            `json:"timeout"`
		IdleTimeout string            `json:"idle_timeout"`
		HashPolicy  []envoyHashPolicy `json:"hash_policy"`
	}
	envoyHashPolicy struct {
		Header map[string]string `json:"header"`
	}
	envoyRouterFilter struct {
		Type                 string `json:"@type"`
		SuppressEnvoyHeaders bool   `json:"suppress_envoy_headers"`
	}
	envoyCluster struct {
		Name                          string                    `json:"name"`
		Type                          string                    `json:"type"`
		LBPolicy                      string                    `json:"lb_policy"`
		ConnectTimeout                string                    `json:"connect_timeout"`
		CircuitBreakers               envoyCircuitBreakers      `json:"circuit_breakers"`
		TypedExtensionProtocolOptions map[string]envoyHTTPProto `json:"typed_extension_protocol_options"`
		LoadAssignment                envoyLoadAssignment       `json:"load_assignment"`
	}
	envoyCircuitBreakers struct {
		Thresholds []envoyThreshold `json:"thresholds"`
	}
	envoyThreshold struct {
		Priority           string `json:"priority"`
		MaxConnections     uint32 `json:"max_connections"`
		MaxPendingRequests uint32 `json:"max_pending_requests"`
		MaxRequests        uint32 `json:"max_requests"`
	}
	envoyHTTPProto struct {
		Type                      string            `json:"@type"`
		CommonHTTPProtocolOptions map[string]string `json:"common_http_protocol_options"`
		ExplicitHTTPConfig        map[string]any    `json:"explicit_http_config"`
	}
	envoyLoadAssignment struct {
		ClusterName string               `json:"cluster_name"`
		Endpoints   []envoyLocalityLBEps `json:"endpoints"`
	}
	envoyLocalityLBEps struct {
		LBEndpoints []envoyLBEndpoint `json:"lb_endpoints"`
	}
	envoyLBEndpoint struct {
		Endpoint envoyEndpoint `json:"endpoint"`
	}
	envoyEndpoint struct {
		Hostname string       `json:"hostname"`
		Address  envoyAddress `json:"address"`
	}
)

const (
	typeHCM         = "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager"
	typeRouter      = "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router"
	typeHTTPOptions = "type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions"
)

// envoySeconds is a whole-second duration in Envoy's JSON form ("10800s").
func envoySeconds(d time.Duration) string { return fmt.Sprintf("%ds", int64(d/time.Second)) }

// RenderEnvoyConfig renders the bootstrap. It refuses what it cannot render correctly; whether the
// rendered bytes may run is the guard's decision (CheckEnvoyConfig).
func RenderEnvoyConfig(c EnvoyConfig) ([]byte, error) {
	if c.RunDir == "" || !filepath.IsAbs(c.RunDir) {
		return nil, fmt.Errorf("envoy: run directory %q is not an absolute path", c.RunDir)
	}
	if len(c.Workers) == 0 {
		return nil, errors.New("envoy: no worker to route to")
	}
	timeout := envoySeconds(EnvoyRouterTimeout)
	var eps []envoyLBEndpoint
	for _, w := range c.Workers {
		eps = append(eps, envoyLBEndpoint{Endpoint: envoyEndpoint{Hostname: w.Name, Address: envoyAddress{Pipe: &envoyPipe{Path: w.Socket}}}})
	}
	b := envoyBootstrap{
		Admin: envoyAdmin{
			Address:    envoyAddress{Pipe: &envoyPipe{Path: filepath.Join(c.RunDir, EnvoyAdminSocket), Mode: envoyPipeMode}},
			AllowPaths: []map[string]string{{"exact": envoyClustersPath}},
		},
		StaticResources: envoyStaticResources{
			Listeners: []envoyListener{{
				Name:    "instance",
				Address: envoyAddress{Pipe: &envoyPipe{Path: filepath.Join(c.RunDir, EnvoyListenerSocket), Mode: envoyPipeMode}},
				FilterChains: []envoyFilterChain{{Filters: []envoyNamedConfig{{
					Name: "envoy.filters.network.http_connection_manager",
					TypedConfig: envoyHCM{
						Type: typeHCM, StatPrefix: "instance", StreamIdleTimeout: timeout,
						RouteConfig: envoyRouteConfig{Name: "instance", VirtualHosts: []envoyVirtualHost{{
							Name: "instance", Domains: []string{"*"},
							Routes: []envoyRoute{{
								Match: map[string]string{"prefix": "/"},
								Route: envoyRouteAction{
									Cluster: envoyClusterName, Timeout: timeout, IdleTimeout: timeout,
									HashPolicy: []envoyHashPolicy{{Header: map[string]string{"header_name": envoySessionHeader}}},
								},
							}},
						}}},
						HTTPFilters: []envoyNamedConfig{{Name: "envoy.filters.http.router", TypedConfig: envoyRouterFilter{Type: typeRouter, SuppressEnvoyHeaders: true}}},
					},
				}}}},
			}},
			Clusters: []envoyCluster{{
				Name: envoyClusterName, Type: "STATIC", LBPolicy: "RING_HASH", ConnectTimeout: envoySeconds(envoyConnectTimeout),
				CircuitBreakers: envoyCircuitBreakers{Thresholds: []envoyThreshold{{
					Priority: "DEFAULT", MaxConnections: envoyMaxThreshold, MaxPendingRequests: envoyMaxThreshold, MaxRequests: envoyMaxThreshold,
				}}},
				TypedExtensionProtocolOptions: map[string]envoyHTTPProto{
					"envoy.extensions.upstreams.http.v3.HttpProtocolOptions": {
						Type:                      typeHTTPOptions,
						CommonHTTPProtocolOptions: map[string]string{"idle_timeout": envoySeconds(envoyUpstreamIdle)},
						ExplicitHTTPConfig:        map[string]any{"http_protocol_options": map[string]any{}},
					},
				},
				LoadAssignment: envoyLoadAssignment{ClusterName: envoyClusterName, Endpoints: []envoyLocalityLBEps{{LBEndpoints: eps}}},
			}},
		},
	}
	return json.MarshalIndent(b, "", "  ")
}
