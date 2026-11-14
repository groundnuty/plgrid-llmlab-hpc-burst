package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
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
//     what HBEP-5 lists (the observation of #429's stage 0);
//   - worker health (HBEP-5, Worker health; HBEP-7, Several workers): an active HTTP check of each
//     worker every 5 s with a 2 s time-out, to the agent's one readiness port with the worker's name as
//     its Host, unhealthy after 2 failures or at once on a 503, healthy after 1 pass, and the same 5 s
//     before any traffic; outlier detection ejecting a worker after 3 consecutive 5xx for 30 s, longer
//     after each repeat up to 300 s (Q10), any share of the workers, and never brought back early by a
//     passing check; and no panic mode, so with no healthy worker Envoy answers 503 rather than route
//     to an unhealthy one.
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

	// Worker health (HBEP-5, Worker health).
	envoyHealthPath  = "/ready"
	envoyHCInterval  = 5 * time.Second
	envoyHCTimeout   = 2 * time.Second
	envoyHCUnhealthy = 2
	envoyHCHealthy   = 1
	envoyOutlier5xx  = 3
	// envoyOutlierLocalOrigin: a worker whose connection fails, resets before the request, or times out is
	// ejected at once (HBEP-5, Worker health; the science agent's ruling on #561): on the engine's local
	// socket that means a dead engine or a full accept queue. Envoy's local-origin errors are split from
	// the engine's own 5xx, which still need envoyOutlier5xx in a row. Without it, ring_hash's bounded
	// load walked requests, and their retries, to a dead worker not yet ejected: it holds no request, so
	// it is never overloaded (#561).
	envoyOutlierLocalOrigin   = 1
	envoyEnforcingLocalOrigin = 100
	envoyEjectionBase         = 30 * time.Second
	envoyEjectionMax          = 300 * time.Second
	envoyMaxEjectionPercent   = 100
	envoyHealthCheckAddress   = "127.0.0.1"

	// envoyHashBalanceFactor is ring_hash's bounded load, in percent of the mean (HBEP-5, Inside an
	// Instance; the science agent's decision on #518, provisional until S2c tunes it): a worker above 1.25
	// times the mean of the workers' active requests takes no new request, which moves on round the
	// ring, so sessions don't pile onto one worker while a sibling idles. It is the front's own tight
	// bound (c = 1.25).
	envoyHashBalanceFactor = 125

	// The route's retry (HBEP-5, Worker health; the science agent's ruling on #439, PR 4's finding): once,
	// on a failure to connect or a reset before the request was sent, to another worker. Such a request
	// reached no engine, so the retry cannot run anything twice; nothing written to a worker is retried.
	// It answers a request Envoy sends to a worker that has just died, before its next active check.
	envoyRetryOn = "connect-failure,reset-before-request"
	// The retry's jittered back-off (HBEP-5, Worker health; #561): a 100 ms base and a 250 ms maximum, so
	// the retry follows the ring's update after a dead worker's ejection, which reaches Envoy's worker
	// threads a moment after the failure that caused it. With Envoy's default 25 ms base, measured on
	// 1.40.2, a wave of 40 requests at a worker's death lost 8 of 1,600 to retries that beat the update;
	// with 100 ms, none.
	envoyRetryBackOffBase = 100 * time.Millisecond
	envoyRetryBackOffMax  = 250 * time.Millisecond
	envoyNumRetries       = 1
	envoyPreviousHosts    = "envoy.retry_host_predicates.previous_hosts"
	// envoyHostSelectionAttempts is how many times the retry's host selection may pick again when it
	// lands on the worker that failed (previous_hosts). Under RING_HASH a pick can land on the same worker
	// again. Measured on Envoy 1.40.2 with two workers, a request in the dead worker's session (#439): with
	// the field unset the retry went back to the dead worker in 2 runs of 3, with 0 in 1 of 6, with 1 in
	// none of 6, and with 20 in none of 15. Picks again cost nothing, so 20.
	envoyHostSelectionAttempts = 20
	envoyHealthyPanicPercent   = 0
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
	// ReadinessPort is the agent's readiness endpoint on 127.0.0.1 (Readiness), which Envoy
	// health-checks every worker through.
	ReadinessPort int
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
		RetryPolicy envoyRetryPolicy  `json:"retry_policy"`
	}
	envoyRetryPolicy struct {
		RetryOn                       string             `json:"retry_on"`
		NumRetries                    int                `json:"num_retries"`
		RetryHostPredicate            []envoyNamedConfig `json:"retry_host_predicate"`
		HostSelectionRetryMaxAttempts int                `json:"host_selection_retry_max_attempts"`
		RetryBackOff                  envoyBackOff       `json:"retry_back_off"`
	}
	envoyBackOff struct {
		BaseInterval string `json:"base_interval"`
		MaxInterval  string `json:"max_interval"`
	}
	envoyTyped struct {
		Type string `json:"@type"`
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
		CommonLBConfig                envoyCommonLB             `json:"common_lb_config"`
		HealthChecks                  []envoyHealthCheck        `json:"health_checks"`
		OutlierDetection              envoyOutlier              `json:"outlier_detection"`
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
		MaxRetries         uint32 `json:"max_retries"`
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
		Hostname          string                 `json:"hostname"`
		Address           envoyAddress           `json:"address"`
		HealthCheckConfig envoyHealthCheckConfig `json:"health_check_config"`
	}
	envoyCommonLB struct {
		HealthyPanicThreshold     envoyPercent        `json:"healthy_panic_threshold"`
		ConsistentHashingLBConfig envoyConsistentHash `json:"consistent_hashing_lb_config"`
	}
	envoyConsistentHash struct {
		HashBalanceFactor uint32 `json:"hash_balance_factor"`
	}
	envoyPercent struct {
		Value float64 `json:"value"`
	}
	envoyHealthCheck struct {
		Timeout            string            `json:"timeout"`
		Interval           string            `json:"interval"`
		NoTrafficInterval  string            `json:"no_traffic_interval"`
		UnhealthyThreshold uint32            `json:"unhealthy_threshold"`
		HealthyThreshold   uint32            `json:"healthy_threshold"`
		HTTPHealthCheck    map[string]string `json:"http_health_check"`
	}
	envoyOutlier struct {
		Consecutive5xx                         uint32 `json:"consecutive_5xx"`
		BaseEjectionTime                       string `json:"base_ejection_time"`
		MaxEjectionTime                        string `json:"max_ejection_time"`
		MaxEjectionPercent                     uint32 `json:"max_ejection_percent"`
		SuccessfulActiveHealthCheckUnejectHost bool   `json:"successful_active_health_check_uneject_host"`
		SplitExternalLocalOriginErrors         bool   `json:"split_external_local_origin_errors"`
		ConsecutiveLocalOriginFailure          uint32 `json:"consecutive_local_origin_failure"`
		EnforcingConsecutiveLocalOriginFailure uint32 `json:"enforcing_consecutive_local_origin_failure"`
	}
	envoyHealthCheckConfig struct {
		Address  envoySocketAddress `json:"address"`
		Hostname string             `json:"hostname"`
	}
	envoySocketAddress struct {
		SocketAddress envoyIPPort `json:"socket_address"`
	}
	envoyIPPort struct {
		Address   string `json:"address"`
		PortValue int    `json:"port_value"`
	}
)

const (
	typeHCM         = "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager"
	typeRouter      = "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router"
	typeHTTPOptions = "type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions"
	// typePreviousHosts is the retry host predicate that sends a retry to another worker.
	typePreviousHosts = "type.googleapis.com/envoy.extensions.retry.host.previous_hosts.v3.PreviousHostsPredicate"
)

// envoySeconds is a whole-second duration in Envoy's JSON form ("10800s").
func envoySeconds(d time.Duration) string { return fmt.Sprintf("%ds", int64(d/time.Second)) }

// envoyDuration is d as a protobuf Duration's JSON, for a value under a second: 100 ms is "0.1s".
func envoyDuration(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
}

// RenderEnvoyConfig renders the bootstrap. It refuses what it cannot render correctly; whether the
// rendered bytes may run is the guard's decision (CheckEnvoyConfig).
func RenderEnvoyConfig(c EnvoyConfig) ([]byte, error) {
	if c.RunDir == "" || !filepath.IsAbs(c.RunDir) {
		return nil, fmt.Errorf("envoy: run directory %q is not an absolute path", c.RunDir)
	}
	if len(c.Workers) == 0 {
		return nil, errors.New("envoy: no worker to route to")
	}
	if c.ReadinessPort <= 0 || c.ReadinessPort > 65535 {
		return nil, fmt.Errorf("envoy: readiness port %d is not a port", c.ReadinessPort)
	}
	timeout := envoySeconds(EnvoyRouterTimeout)
	var eps []envoyLBEndpoint
	for _, w := range c.Workers {
		eps = append(eps, envoyLBEndpoint{Endpoint: envoyEndpoint{
			Hostname: w.Name, Address: envoyAddress{Pipe: &envoyPipe{Path: w.Socket}},
			// The check goes to the agent's readiness port, naming the worker by its Host.
			HealthCheckConfig: envoyHealthCheckConfig{
				Address:  envoySocketAddress{SocketAddress: envoyIPPort{Address: envoyHealthCheckAddress, PortValue: c.ReadinessPort}},
				Hostname: w.Name,
			},
		}})
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
									RetryPolicy: envoyRetryPolicy{RetryOn: envoyRetryOn, NumRetries: envoyNumRetries,
										RetryBackOff:                  envoyBackOff{BaseInterval: envoyDuration(envoyRetryBackOffBase), MaxInterval: envoyDuration(envoyRetryBackOffMax)},
										RetryHostPredicate:            []envoyNamedConfig{{Name: envoyPreviousHosts, TypedConfig: envoyTyped{Type: typePreviousHosts}}},
										HostSelectionRetryMaxAttempts: envoyHostSelectionAttempts},
								},
							}},
						}}},
						HTTPFilters: []envoyNamedConfig{{Name: "envoy.filters.http.router", TypedConfig: envoyRouterFilter{Type: typeRouter, SuppressEnvoyHeaders: true}}},
					},
				}}}},
			}},
			Clusters: []envoyCluster{{
				Name: envoyClusterName, Type: "STATIC", LBPolicy: "RING_HASH", ConnectTimeout: envoySeconds(envoyConnectTimeout),
				CommonLBConfig: envoyCommonLB{HealthyPanicThreshold: envoyPercent{Value: envoyHealthyPanicPercent},
					ConsistentHashingLBConfig: envoyConsistentHash{HashBalanceFactor: envoyHashBalanceFactor}},
				HealthChecks: []envoyHealthCheck{{
					Timeout: envoySeconds(envoyHCTimeout), Interval: envoySeconds(envoyHCInterval), NoTrafficInterval: envoySeconds(envoyHCInterval),
					UnhealthyThreshold: envoyHCUnhealthy, HealthyThreshold: envoyHCHealthy,
					HTTPHealthCheck: map[string]string{"path": envoyHealthPath},
				}},
				OutlierDetection: envoyOutlier{
					Consecutive5xx: envoyOutlier5xx, BaseEjectionTime: envoySeconds(envoyEjectionBase), MaxEjectionTime: envoySeconds(envoyEjectionMax),
					MaxEjectionPercent: envoyMaxEjectionPercent, SuccessfulActiveHealthCheckUnejectHost: false,
					SplitExternalLocalOriginErrors: true, ConsecutiveLocalOriginFailure: envoyOutlierLocalOrigin,
					EnforcingConsecutiveLocalOriginFailure: envoyEnforcingLocalOrigin,
				},
				CircuitBreakers: envoyCircuitBreakers{Thresholds: []envoyThreshold{{
					Priority: "DEFAULT", MaxConnections: envoyMaxThreshold, MaxPendingRequests: envoyMaxThreshold, MaxRequests: envoyMaxThreshold,
					// The retry's slot is held from the retry's decision until the retried attempt answers:
					// Envoy's default of 3 would answer a fourth concurrent retry with an overflow 503 (#507's
					// review). As for the other three, the front's cap governs.
					MaxRetries: envoyMaxThreshold,
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
