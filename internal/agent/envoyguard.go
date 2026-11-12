package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// CheckEnvoyConfig decides whether Envoy may run a bootstrap: the agent's launch runs nothing it
// refuses. It reads the bytes back as plain JSON, not through the writer's types, and holds them to
// what HBEP-5 and the rulings on #429 require of an Instance's router (envoyconf.go):
//   - nothing beside the admin and the static listener and cluster: no runtime layer, which could
//     override the cluster's settings, and no dynamic resources;
//   - the admin and the one listener on mode-0600 Unix sockets at their paths in the run directory,
//     never a TCP address; the admin allowing exactly /clusters?format=json;
//   - each of the route's timeout and idle_timeout and the connection manager's stream_idle_timeout
//     at least EnvoyRouterTimeout (10,800 s), and no other deadline set, so a value that
//     would cut a long reply is refused at launch rather than found mid-stream;
//   - the router filter suppressing Envoy's own x-envoy-* headers;
//   - the circuit breakers' three thresholds at Envoy's field maximum;
//   - the workers on Unix sockets, each named, each health-checked through the agent's readiness port
//     with its own name as the check's Host;
//   - the health checks, outlier detection and panic threshold at HBEP-5's values (Worker health).
//
// Each map is held to the keys the agent writes, so a field that is not checked here cannot be
// added unnoticed. The first fault is returned, naming its place.
func CheckEnvoyConfig(raw []byte, c EnvoyConfig) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var root any
	if err := dec.Decode(&root); err != nil {
		return refuse("the bootstrap is not JSON: %v", err)
	}
	g := guard{floor: EnvoyRouterTimeout, runDir: c.RunDir, readinessPort: c.ReadinessPort}
	top, err := g.object(root, "", "admin", "static_resources")
	if err != nil {
		return err
	}
	if err := g.admin(top["admin"]); err != nil {
		return err
	}
	sr, err := g.object(top["static_resources"], "static_resources", "listeners", "clusters")
	if err != nil {
		return err
	}
	ls, err := g.list(sr["listeners"], "static_resources.listeners", 1)
	if err != nil {
		return err
	}
	if err := g.listener(ls[0], "static_resources.listeners[0]"); err != nil {
		return err
	}
	cs, err := g.list(sr["clusters"], "static_resources.clusters", 1)
	if err != nil {
		return err
	}
	return g.cluster(cs[0], "static_resources.clusters[0]")
}

// ErrEnvoyRefused marks a bootstrap, or an Envoy binary, the agent will not run.
var ErrEnvoyRefused = errors.New("envoy refused")

func refuse(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrEnvoyRefused, fmt.Sprintf(format, a...))
}

type guard struct {
	floor         time.Duration
	runDir        string
	readinessPort int
}

// num checks a JSON number equal to want.
func (g guard) num(v any, at string, want int64) error {
	if n, ok := v.(json.Number); !ok || n.String() != fmt.Sprint(want) {
		return refuse("%s is %v, want %d", at, v, want)
	}
	return nil
}

// health checks the cluster's worker health: the active check, outlier detection and no panic mode.
func (g guard) health(c map[string]any, at string) error {
	lb, err := g.object(c["common_lb_config"], at+".common_lb_config", "healthy_panic_threshold")
	if err != nil {
		return err
	}
	pt, err := g.object(lb["healthy_panic_threshold"], at+".common_lb_config.healthy_panic_threshold", "value")
	if err != nil {
		return err
	}
	if err := g.num(pt["value"], at+".common_lb_config.healthy_panic_threshold.value", envoyHealthyPanicPercent); err != nil {
		return refuse("%v: with a panic threshold Envoy routes to unhealthy workers once few are healthy", err)
	}
	hcs, err := g.list(c["health_checks"], at+".health_checks", 1)
	if err != nil {
		return err
	}
	hat := at + ".health_checks[0]"
	hc, err := g.object(hcs[0], hat, "timeout", "interval", "no_traffic_interval", "unhealthy_threshold", "healthy_threshold", "http_health_check")
	if err != nil {
		return err
	}
	for k, want := range map[string]time.Duration{"timeout": envoyHCTimeout, "interval": envoyHCInterval, "no_traffic_interval": envoyHCInterval} {
		if err := g.str(hc[k], hat+"."+k, envoySeconds(want)); err != nil {
			return err
		}
	}
	if err := g.num(hc["unhealthy_threshold"], hat+".unhealthy_threshold", envoyHCUnhealthy); err != nil {
		return err
	}
	if err := g.num(hc["healthy_threshold"], hat+".healthy_threshold", envoyHCHealthy); err != nil {
		return err
	}
	hh, err := g.object(hc["http_health_check"], hat+".http_health_check", "path")
	if err != nil {
		return err
	}
	if err := g.str(hh["path"], hat+".http_health_check.path", envoyHealthPath); err != nil {
		return err
	}
	oat := at + ".outlier_detection"
	od, err := g.object(c["outlier_detection"], oat, "consecutive_5xx", "base_ejection_time", "max_ejection_time", "max_ejection_percent", "successful_active_health_check_uneject_host")
	if err != nil {
		return err
	}
	if err := g.num(od["consecutive_5xx"], oat+".consecutive_5xx", envoyOutlier5xx); err != nil {
		return err
	}
	if err := g.str(od["base_ejection_time"], oat+".base_ejection_time", envoySeconds(envoyEjectionBase)); err != nil {
		return err
	}
	if err := g.str(od["max_ejection_time"], oat+".max_ejection_time", envoySeconds(envoyEjectionMax)); err != nil {
		return err
	}
	if err := g.num(od["max_ejection_percent"], oat+".max_ejection_percent", envoyMaxEjectionPercent); err != nil {
		return refuse("%v: Envoy's default 10%% would keep a failing worker in a small Instance", err)
	}
	if u, ok := od["successful_active_health_check_uneject_host"].(bool); !ok || u {
		return refuse("%s.successful_active_health_check_uneject_host is %v, want false: a passing check would bring back a worker that fails requests", oat, od["successful_active_health_check_uneject_host"])
	}
	return nil
}

// healthCheckConfig checks an endpoint's check target: the readiness port on 127.0.0.1, named host.
func (g guard) healthCheckConfig(v any, at, host string) error {
	h, err := g.object(v, at, "address", "hostname")
	if err != nil {
		return err
	}
	a, err := g.object(h["address"], at+".address", "socket_address")
	if err != nil {
		return err
	}
	sa, err := g.object(a["socket_address"], at+".address.socket_address", "address", "port_value")
	if err != nil {
		return err
	}
	if err := g.str(sa["address"], at+".address.socket_address.address", envoyHealthCheckAddress); err != nil {
		return err
	}
	if err := g.num(sa["port_value"], at+".address.socket_address.port_value", int64(g.readinessPort)); err != nil {
		return refuse("%v: the check must reach the agent's readiness endpoint", err)
	}
	if err := g.str(h["hostname"], at+".hostname", host); err != nil {
		return refuse("%v: the check names the worker by its Host", err)
	}
	return nil
}

// object returns v as a map whose keys are exactly allowed's, all present.
func (g guard) object(v any, at string, allowed ...string) (map[string]any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, refuse("%s is not an object", place(at))
	}
	want := map[string]bool{}
	for _, k := range allowed {
		want[k] = true
		if _, ok := m[k]; !ok {
			return nil, refuse("%s has no %s", place(at), k)
		}
	}
	var extra []string
	for k := range m {
		if !want[k] {
			extra = append(extra, k)
		}
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		return nil, refuse("%s sets %s, which the agent's router does not allow", place(at), strings.Join(extra, ", "))
	}
	return m, nil
}

func place(at string) string {
	if at == "" {
		return "the bootstrap"
	}
	return at
}

// list returns v as a list of exactly n elements.
func (g guard) list(v any, at string, n int) ([]any, error) {
	l, ok := v.([]any)
	if !ok || len(l) != n {
		return nil, refuse("%s must hold exactly %d element(s)", at, n)
	}
	return l, nil
}

func (g guard) str(v any, at, want string) error {
	if s, ok := v.(string); !ok || s != want {
		return refuse("%s is %v, want %q", at, v, want)
	}
	return nil
}

// pipe checks an address: a Unix socket at want, with mode 0600 when mode is set.
func (g guard) pipe(v any, at, want string, mode bool) error {
	addr, ok := v.(map[string]any)
	if _, isPipe := addr["pipe"]; !ok || !isPipe || len(addr) != 1 {
		return refuse("%s must be a Unix socket (pipe) and nothing else, never a TCP address: %v", at, v)
	}
	keys := []string{"path"}
	if mode {
		keys = append(keys, "mode")
	}
	p, err := g.object(addr["pipe"], at+".pipe", keys...)
	if err != nil {
		return err
	}
	path, _ := p["path"].(string)
	if want != "" && path != want {
		return refuse("%s.pipe.path is %q, want %q", at, path, want)
	}
	if !filepath.IsAbs(path) || filepath.Dir(path) != filepath.Clean(g.runDir) {
		return refuse("%s.pipe.path %q is not in the run directory %s", at, path, g.runDir)
	}
	if len(path) > maxSocketPath {
		return refuse("%s.pipe.path is %d bytes, longer than a Unix socket's %d", at, len(path), maxSocketPath)
	}
	if mode {
		if n, ok := p["mode"].(json.Number); !ok || n.String() != fmt.Sprint(envoyPipeMode) {
			return refuse("%s.pipe.mode is %v, want %d (0600)", at, p["mode"], envoyPipeMode)
		}
	}
	return nil
}

func (g guard) admin(v any) error {
	a, err := g.object(v, "admin", "address", "allow_paths")
	if err != nil {
		return err
	}
	if err := g.pipe(a["address"], "admin.address", filepath.Join(g.runDir, EnvoyAdminSocket), true); err != nil {
		return err
	}
	paths, err := g.list(a["allow_paths"], "admin.allow_paths", 1)
	if err != nil {
		return err
	}
	m, err := g.object(paths[0], "admin.allow_paths[0]", "exact")
	if err != nil {
		return err
	}
	return g.str(m["exact"], "admin.allow_paths[0].exact", envoyClustersPath)
}

// timeout checks a duration at least the floor.
func (g guard) timeout(v any, at string) error {
	s, _ := v.(string)
	d, err := time.ParseDuration(s)
	if err != nil {
		return refuse("%s is %v, not a duration", at, v)
	}
	if d < g.floor {
		return refuse("%s is %v, below the front's maximum request duration plus 1 h (%v): it would cut a long or slow reply", at, d, g.floor)
	}
	return nil
}

func (g guard) listener(v any, at string) error {
	l, err := g.object(v, at, "name", "address", "filter_chains")
	if err != nil {
		return err
	}
	if err := g.pipe(l["address"], at+".address", filepath.Join(g.runDir, EnvoyListenerSocket), true); err != nil {
		return err
	}
	fcs, err := g.list(l["filter_chains"], at+".filter_chains", 1)
	if err != nil {
		return err
	}
	fc, err := g.object(fcs[0], at+".filter_chains[0]", "filters")
	if err != nil {
		return err
	}
	fs, err := g.list(fc["filters"], at+".filter_chains[0].filters", 1)
	if err != nil {
		return err
	}
	at += ".filter_chains[0].filters[0]"
	f, err := g.object(fs[0], at, "name", "typed_config")
	if err != nil {
		return err
	}
	if err := g.str(f["name"], at+".name", "envoy.filters.network.http_connection_manager"); err != nil {
		return err
	}
	return g.hcm(f["typed_config"], at+".typed_config")
}

func (g guard) hcm(v any, at string) error {
	h, err := g.object(v, at, "@type", "stat_prefix", "stream_idle_timeout", "route_config", "http_filters")
	if err != nil {
		return err
	}
	if err := g.str(h["@type"], at+".@type", typeHCM); err != nil {
		return err
	}
	if err := g.timeout(h["stream_idle_timeout"], at+".stream_idle_timeout"); err != nil {
		return err
	}
	rc, err := g.object(h["route_config"], at+".route_config", "name", "virtual_hosts")
	if err != nil {
		return err
	}
	vhs, err := g.list(rc["virtual_hosts"], at+".route_config.virtual_hosts", 1)
	if err != nil {
		return err
	}
	vh, err := g.object(vhs[0], at+".route_config.virtual_hosts[0]", "name", "domains", "routes")
	if err != nil {
		return err
	}
	rs, err := g.list(vh["routes"], at+".route_config.virtual_hosts[0].routes", 1)
	if err != nil {
		return err
	}
	rat := at + ".route_config.virtual_hosts[0].routes[0]"
	r, err := g.object(rs[0], rat, "match", "route")
	if err != nil {
		return err
	}
	ra, err := g.object(r["route"], rat+".route", "cluster", "timeout", "idle_timeout", "hash_policy")
	if err != nil {
		return err
	}
	if err := g.str(ra["cluster"], rat+".route.cluster", envoyClusterName); err != nil {
		return err
	}
	hps, err := g.list(ra["hash_policy"], rat+".route.hash_policy", 1)
	if err != nil {
		return err
	}
	hp, err := g.object(hps[0], rat+".route.hash_policy[0]", "header")
	if err != nil {
		return err
	}
	hh, err := g.object(hp["header"], rat+".route.hash_policy[0].header", "header_name")
	if err != nil {
		return err
	}
	if err := g.str(hh["header_name"], rat+".route.hash_policy[0].header.header_name", envoySessionHeader); err != nil {
		return err
	}
	for _, k := range []string{"timeout", "idle_timeout"} {
		if err := g.timeout(ra[k], rat+".route."+k); err != nil {
			return err
		}
	}
	fs, err := g.list(h["http_filters"], at+".http_filters", 1)
	if err != nil {
		return err
	}
	f, err := g.object(fs[0], at+".http_filters[0]", "name", "typed_config")
	if err != nil {
		return err
	}
	rf, err := g.object(f["typed_config"], at+".http_filters[0].typed_config", "@type", "suppress_envoy_headers")
	if err != nil {
		return err
	}
	if s, ok := rf["suppress_envoy_headers"].(bool); !ok || !s {
		return refuse("%s.http_filters[0].typed_config.suppress_envoy_headers is %v, want true: the workers must see no x-envoy-* header Envoy adds", at, rf["suppress_envoy_headers"])
	}
	return g.str(rf["@type"], at+".http_filters[0].typed_config.@type", typeRouter)
}

func (g guard) cluster(v any, at string) error {
	c, err := g.object(v, at, "name", "type", "lb_policy", "connect_timeout", "common_lb_config", "health_checks", "outlier_detection", "circuit_breakers", "typed_extension_protocol_options", "load_assignment")
	if err != nil {
		return err
	}
	if err := g.health(c, at); err != nil {
		return err
	}
	if err := g.str(c["name"], at+".name", envoyClusterName); err != nil {
		return err
	}
	if err := g.str(c["lb_policy"], at+".lb_policy", "RING_HASH"); err != nil {
		return err
	}
	tpo, err := g.object(c["typed_extension_protocol_options"], at+".typed_extension_protocol_options", "envoy.extensions.upstreams.http.v3.HttpProtocolOptions")
	if err != nil {
		return err
	}
	hpo, err := g.object(tpo["envoy.extensions.upstreams.http.v3.HttpProtocolOptions"], at+".typed_extension_protocol_options.HttpProtocolOptions", "@type", "common_http_protocol_options", "explicit_http_config")
	if err != nil {
		return err
	}
	chpo, err := g.object(hpo["common_http_protocol_options"], at+".typed_extension_protocol_options.HttpProtocolOptions.common_http_protocol_options", "idle_timeout")
	if err != nil {
		return err
	}
	// Envoy reuses connections to the workers, so it idles them out first: below the workers' 120 s.
	if err := g.str(chpo["idle_timeout"], at+".typed_extension_protocol_options.HttpProtocolOptions.common_http_protocol_options.idle_timeout", envoySeconds(envoyUpstreamIdle)); err != nil {
		return err
	}
	cb, err := g.object(c["circuit_breakers"], at+".circuit_breakers", "thresholds")
	if err != nil {
		return err
	}
	ths, err := g.list(cb["thresholds"], at+".circuit_breakers.thresholds", 1)
	if err != nil {
		return err
	}
	th, err := g.object(ths[0], at+".circuit_breakers.thresholds[0]", "priority", "max_connections", "max_pending_requests", "max_requests")
	if err != nil {
		return err
	}
	for _, k := range []string{"max_connections", "max_pending_requests", "max_requests"} {
		if n, ok := th[k].(json.Number); !ok || n.String() != fmt.Sprint(uint64(envoyMaxThreshold)) {
			return refuse("%s.circuit_breakers.thresholds[0].%s is %v, want Envoy's maximum %d: an overflow 503 would count toward the Instance's pause", at, k, th[k], uint64(envoyMaxThreshold))
		}
	}
	la, err := g.object(c["load_assignment"], at+".load_assignment", "cluster_name", "endpoints")
	if err != nil {
		return err
	}
	locs, err := g.list(la["endpoints"], at+".load_assignment.endpoints", 1)
	if err != nil {
		return err
	}
	loc, err := g.object(locs[0], at+".load_assignment.endpoints[0]", "lb_endpoints")
	if err != nil {
		return err
	}
	eps, ok := loc["lb_endpoints"].([]any)
	if !ok || len(eps) == 0 {
		return refuse("%s.load_assignment.endpoints[0].lb_endpoints holds no worker", at)
	}
	for i, e := range eps {
		eat := fmt.Sprintf("%s.load_assignment.endpoints[0].lb_endpoints[%d]", at, i)
		lbe, err := g.object(e, eat, "endpoint")
		if err != nil {
			return err
		}
		ep, err := g.object(lbe["endpoint"], eat+".endpoint", "hostname", "address", "health_check_config")
		if err != nil {
			return err
		}
		n, _ := ep["hostname"].(string)
		if n == "" {
			return refuse("%s.endpoint.hostname is empty: each worker is named", eat)
		}
		if err := g.healthCheckConfig(ep["health_check_config"], eat+".endpoint.health_check_config", n); err != nil {
			return err
		}
		if err := g.pipe(ep["address"], eat+".endpoint.address", "", false); err != nil {
			return err
		}
	}
	return nil
}
