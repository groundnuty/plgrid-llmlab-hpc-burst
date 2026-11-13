package agent_test

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The router's bootstrap and its launch guard (envoyconf.go, envoyguard.go). Expected values are
// HBEP-5's and the rulings on #429, typed here; a config is never compared with a golden file the
// writer produced. Each fault below is made to the agent's own rendered config, so only the one field
// differs from what the agent writes.

const envoyRunDir = "/hpcburst/run-example" // only a string here: nothing binds it

func envoyCfg() agent.EnvoyConfig {
	return agent.EnvoyConfig{
		RunDir:        envoyRunDir,
		Workers:       []agent.EnvoyWorker{{Name: "worker-0", Socket: envoyRunDir + "/worker-0.sock"}},
		ReadinessPort: 30160, // only a number here: nothing binds it
	}
}

func renderMap(t *testing.T, c agent.EnvoyConfig) map[string]any {
	t.Helper()
	raw, err := agent.RenderEnvoyConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// at walks a decoded bootstrap by keys and list indexes.
func at(t *testing.T, v any, path ...any) any {
	t.Helper()
	for _, p := range path {
		switch k := p.(type) {
		case string:
			v = v.(map[string]any)[k]
		case int:
			v = v.([]any)[k]
		}
	}
	return v
}

func hcmOf(t *testing.T, m map[string]any) map[string]any {
	return at(t, m, "static_resources", "listeners", 0, "filter_chains", 0, "filters", 0, "typed_config").(map[string]any)
}

func routeOf(t *testing.T, m map[string]any) map[string]any {
	return at(t, hcmOf(t, m), "route_config", "virtual_hosts", 0, "routes", 0, "route").(map[string]any)
}

func retryOf(t *testing.T, m map[string]any) map[string]any {
	return routeOf(t, m)["retry_policy"].(map[string]any)
}

func clusterOf(t *testing.T, m map[string]any) map[string]any {
	return at(t, m, "static_resources", "clusters", 0).(map[string]any)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The values HBEP-5 and the rulings name, read from the rendered bootstrap.
func TestEnvoyConfigCarriesHBEP5sSettings(t *testing.T) {
	m := renderMap(t, envoyCfg())
	if err := agent.CheckEnvoyConfig(mustJSON(t, m), envoyCfg()); err != nil {
		t.Fatalf("the agent's own config is refused: %v", err)
	}
	r, h, c := routeOf(t, m), hcmOf(t, m), clusterOf(t, m)
	for name, got := range map[string]any{
		"route timeout":                  r["timeout"],
		"route idle_timeout":             r["idle_timeout"],
		"stream_idle_timeout":            h["stream_idle_timeout"],
		"listener":                       at(t, m, "static_resources", "listeners", 0, "address", "pipe", "path"),
		"listener mode":                  at(t, m, "static_resources", "listeners", 0, "address", "pipe", "mode"),
		"admin":                          at(t, m, "admin", "address", "pipe", "path"),
		"admin mode":                     at(t, m, "admin", "address", "pipe", "mode"),
		"admin path":                     at(t, m, "admin", "allow_paths", 0, "exact"),
		"suppress_envoy_headers":         at(t, h, "http_filters", 0, "typed_config", "suppress_envoy_headers"),
		"hash header":                    at(t, r, "hash_policy", 0, "header", "header_name"),
		"retry on":                       at(t, r, "retry_policy", "retry_on"),
		"retries":                        at(t, r, "retry_policy", "num_retries"),
		"retry predicate":                at(t, r, "retry_policy", "retry_host_predicate", 0, "name"),
		"retry picks":                    at(t, r, "retry_policy", "host_selection_retry_max_attempts"),
		"lb_policy":                      c["lb_policy"],
		"upstream idle":                  at(t, c, "typed_extension_protocol_options", "envoy.extensions.upstreams.http.v3.HttpProtocolOptions", "common_http_protocol_options", "idle_timeout"),
		"max_connections":                at(t, c, "circuit_breakers", "thresholds", 0, "max_connections"),
		"max_pending_requests":           at(t, c, "circuit_breakers", "thresholds", 0, "max_pending_requests"),
		"max_requests":                   at(t, c, "circuit_breakers", "thresholds", 0, "max_requests"),
		"worker-0's socket":              at(t, c, "load_assignment", "endpoints", 0, "lb_endpoints", 0, "endpoint", "address", "pipe", "path"),
		"worker-0's name":                at(t, c, "load_assignment", "endpoints", 0, "lb_endpoints", 0, "endpoint", "hostname"),
		"check timeout":                  at(t, c, "health_checks", 0, "timeout"),
		"check interval":                 at(t, c, "health_checks", 0, "interval"),
		"check before traffic":           at(t, c, "health_checks", 0, "no_traffic_interval"),
		"unhealthy after":                at(t, c, "health_checks", 0, "unhealthy_threshold"),
		"healthy after":                  at(t, c, "health_checks", 0, "healthy_threshold"),
		"check path":                     at(t, c, "health_checks", 0, "http_health_check", "path"),
		"check address":                  at(t, c, "load_assignment", "endpoints", 0, "lb_endpoints", 0, "endpoint", "health_check_config", "address", "socket_address", "address"),
		"check port":                     at(t, c, "load_assignment", "endpoints", 0, "lb_endpoints", 0, "endpoint", "health_check_config", "address", "socket_address", "port_value"),
		"check Host":                     at(t, c, "load_assignment", "endpoints", 0, "lb_endpoints", 0, "endpoint", "health_check_config", "hostname"),
		"consecutive 5xx":                at(t, c, "outlier_detection", "consecutive_5xx"),
		"base ejection":                  at(t, c, "outlier_detection", "base_ejection_time"),
		"max ejection":                   at(t, c, "outlier_detection", "max_ejection_time"),
		"max ejection percent":           at(t, c, "outlier_detection", "max_ejection_percent"),
		"uneject on a passing check":     at(t, c, "outlier_detection", "successful_active_health_check_uneject_host"),
		"panic threshold":                at(t, c, "common_lb_config", "healthy_panic_threshold", "value"),
		"the number of top-level fields": float64(len(m)),
	} {
		want := map[string]any{
			"route timeout": "10800s", "route idle_timeout": "10800s", "stream_idle_timeout": "10800s",
			"listener": envoyRunDir + "/router.sock", "listener mode": float64(0o600),
			"admin": envoyRunDir + "/envoy-admin.sock", "admin mode": float64(0o600), "admin path": "/clusters?format=json",
			"suppress_envoy_headers": true, "hash header": "x-session-id", "lb_policy": "RING_HASH", "upstream idle": "50s",
			"retry on": "connect-failure,reset-before-request", "retries": float64(1), "retry predicate": "envoy.retry_host_predicates.previous_hosts",
			"retry picks":     float64(20),
			"max_connections": float64(4294967295), "max_pending_requests": float64(4294967295), "max_requests": float64(4294967295),
			"worker-0's socket": envoyRunDir + "/worker-0.sock", "worker-0's name": "worker-0",
			"the number of top-level fields": float64(2),
			"check timeout":                  "2s", "check interval": "5s", "check before traffic": "5s",
			"unhealthy after": float64(2), "healthy after": float64(1), "check path": "/ready",
			"check address": "127.0.0.1", "check port": float64(30160), "check Host": "worker-0",
			"consecutive 5xx": float64(3), "base ejection": "30s", "max ejection": "300s",
			"max ejection percent": float64(100), "uneject on a passing check": false, "panic threshold": float64(0),
		}[name]
		if got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

func TestEnvoyGuardRefusesEachFault(t *testing.T) {
	cfg := envoyCfg()
	for _, c := range []struct {
		name string
		edit func(m map[string]any)
		want string
	}{
		{"Envoy's default 15 s route timeout", func(m map[string]any) { routeOf(t, m)["timeout"] = "15s" }, "route.timeout is 15s"},
		{"no route timeout, so Envoy's 15 s", func(m map[string]any) { delete(routeOf(t, m), "timeout") }, "has no timeout"},
		{"a route idle timeout one second short", func(m map[string]any) { routeOf(t, m)["idle_timeout"] = "10799s" }, "route.idle_timeout is 2h59m59s"},
		{"Envoy's default 5 min stream idle timeout", func(m map[string]any) { hcmOf(t, m)["stream_idle_timeout"] = "300s" }, "stream_idle_timeout is 5m0s"},
		{"another deadline on the connection manager", func(m map[string]any) { hcmOf(t, m)["request_timeout"] = "1s" }, "sets request_timeout"},
		// The route retries once, on connect-failure and reset-before-request only, to another worker
		// (HBEP-5, Worker health; the science agent's ruling on #439): any other retry is refused.
		{"no route retry policy", func(m map[string]any) { delete(routeOf(t, m), "retry_policy") }, "has no retry_policy"},
		{"a retry on 5xx", func(m map[string]any) { retryOf(t, m)["retry_on"] = "5xx" }, "retry_policy.retry_on is 5xx"},
		{"a retry on a reset after the request was sent", func(m map[string]any) {
			retryOf(t, m)["retry_on"] = "connect-failure,reset"
		}, "retry_policy.retry_on is connect-failure,reset"},
		{"two retries", func(m map[string]any) { retryOf(t, m)["num_retries"] = 2 }, "retry_policy.num_retries"},
		{"a retry to the same worker", func(m map[string]any) { delete(retryOf(t, m), "retry_host_predicate") }, "has no retry_host_predicate"},
		{"another host predicate", func(m map[string]any) {
			at(t, retryOf(t, m), "retry_host_predicate", 0).(map[string]any)["name"] = "envoy.retry_host_predicates.omit_canary_hosts"
		}, "retry_host_predicate[0].name"},
		{"a per-try timeout", func(m map[string]any) { retryOf(t, m)["per_try_timeout"] = "1s" }, "sets per_try_timeout"},
		{"Envoy's one pick again", func(m map[string]any) { delete(retryOf(t, m), "host_selection_retry_max_attempts") }, "has no host_selection_retry_max_attempts"},
		{"fewer picks again", func(m map[string]any) { retryOf(t, m)["host_selection_retry_max_attempts"] = 1 }, "host_selection_retry_max_attempts"},
		{"a TCP listener on loopback", func(m map[string]any) {
			at(t, m, "static_resources", "listeners", 0).(map[string]any)["address"] = map[string]any{"socket_address": map[string]any{"address": "127.0.0.1", "port_value": 0}}
		}, "must be a Unix socket"},
		{"a TCP listener on every address", func(m map[string]any) {
			at(t, m, "static_resources", "listeners", 0).(map[string]any)["address"] = map[string]any{"socket_address": map[string]any{"address": "0.0.0.0", "port_value": 30199}}
		}, "must be a Unix socket"},
		{"a listener of mode 0644", func(m map[string]any) {
			at(t, m, "static_resources", "listeners", 0, "address", "pipe").(map[string]any)["mode"] = 0o644
		}, "pipe.mode is 420"},
		{"a listener without a mode", func(m map[string]any) {
			delete(at(t, m, "static_resources", "listeners", 0, "address", "pipe").(map[string]any), "mode")
		}, "has no mode"},
		{"a listener outside the run directory", func(m map[string]any) {
			at(t, m, "static_resources", "listeners", 0, "address", "pipe").(map[string]any)["path"] = "/tmp/router.sock"
		}, "want \"" + envoyRunDir + "/router.sock\""},
		{"a second listener", func(m map[string]any) {
			ls := at(t, m, "static_resources").(map[string]any)
			ls["listeners"] = append(ls["listeners"].([]any), ls["listeners"].([]any)[0])
		}, "listeners must hold exactly 1"},
		{"an admin on TCP", func(m map[string]any) {
			m["admin"].(map[string]any)["address"] = map[string]any{"socket_address": map[string]any{"address": "127.0.0.1", "port_value": 9901}}
		}, "admin.address must be a Unix socket"},
		{"an admin of mode 0666", func(m map[string]any) { at(t, m, "admin", "address", "pipe").(map[string]any)["mode"] = 0o666 }, "admin.address.pipe.mode is 438"},
		{"an admin allowing every path", func(m map[string]any) {
			m["admin"].(map[string]any)["allow_paths"] = []any{map[string]any{"prefix": "/"}}
		}, "allow_paths[0] has no exact"},
		{"an admin allowing a second path", func(m map[string]any) {
			a := m["admin"].(map[string]any)
			a["allow_paths"] = append(a["allow_paths"].([]any), map[string]any{"exact": "/runtime_modify"})
		}, "allow_paths must hold exactly 1"},
		{"an admin allowing plain /clusters", func(m map[string]any) {
			m["admin"].(map[string]any)["allow_paths"] = []any{map[string]any{"exact": "/clusters"}}
		}, `want "/clusters?format=json"`},
		{"a runtime layer that could re-enable panic mode", func(m map[string]any) {
			m["layered_runtime"] = map[string]any{"layers": []any{map[string]any{"name": "admin", "admin_layer": map[string]any{}}}}
		}, "the bootstrap sets layered_runtime"},
		{"Envoy's own headers left on", func(m map[string]any) {
			at(t, hcmOf(t, m), "http_filters", 0, "typed_config").(map[string]any)["suppress_envoy_headers"] = false
		}, "suppress_envoy_headers is false"},
		{"Envoy's default circuit breaker", func(m map[string]any) {
			at(t, clusterOf(t, m), "circuit_breakers", "thresholds", 0).(map[string]any)["max_requests"] = 1024
		}, "max_requests is 1024"},
		{"no circuit breakers", func(m map[string]any) { delete(clusterOf(t, m), "circuit_breakers") }, "has no circuit_breakers"},
		{"a worker on TCP", func(m map[string]any) {
			at(t, clusterOf(t, m), "load_assignment", "endpoints", 0, "lb_endpoints", 0, "endpoint").(map[string]any)["address"] = map[string]any{"socket_address": map[string]any{"address": "127.0.0.1", "port_value": 8000}}
		}, "endpoint.address must be a Unix socket"},
		{"an unnamed worker", func(m map[string]any) {
			at(t, clusterOf(t, m), "load_assignment", "endpoints", 0, "lb_endpoints", 0, "endpoint").(map[string]any)["hostname"] = ""
		}, "hostname is empty"},
		{"round robin", func(m map[string]any) { clusterOf(t, m)["lb_policy"] = "ROUND_ROBIN" }, "lb_policy is ROUND_ROBIN"},
		{"hashing another header", func(m map[string]any) {
			at(t, routeOf(t, m), "hash_policy", 0, "header").(map[string]any)["header_name"] = "x-other"
		}, "header_name is x-other"},
		{"no panic threshold, so Envoy's default 50%", func(m map[string]any) { delete(clusterOf(t, m), "common_lb_config") }, "has no common_lb_config"},
		{"a panic threshold of 50%", func(m map[string]any) {
			at(t, clusterOf(t, m), "common_lb_config", "healthy_panic_threshold").(map[string]any)["value"] = 50
		}, "healthy_panic_threshold.value is 50"},
		{"no health checks", func(m map[string]any) { delete(clusterOf(t, m), "health_checks") }, "has no health_checks"},
		{"checks every 60 s", func(m map[string]any) {
			at(t, clusterOf(t, m), "health_checks", 0).(map[string]any)["interval"] = "60s"
		}, "interval is 60s"},
		{"no no_traffic_interval, so Envoy's 60 s", func(m map[string]any) {
			delete(at(t, clusterOf(t, m), "health_checks", 0).(map[string]any), "no_traffic_interval")
		}, "has no no_traffic_interval"},
		{"unhealthy after 3 failures", func(m map[string]any) {
			at(t, clusterOf(t, m), "health_checks", 0).(map[string]any)["unhealthy_threshold"] = 3
		}, "unhealthy_threshold is 3"},
		{"a check of the engine's own /health", func(m map[string]any) {
			at(t, clusterOf(t, m), "health_checks", 0, "http_health_check").(map[string]any)["path"] = "/health"
		}, "path is /health"},
		{"no outlier detection", func(m map[string]any) { delete(clusterOf(t, m), "outlier_detection") }, "has no outlier_detection"},
		{"Envoy's default 10% ejection", func(m map[string]any) {
			clusterOf(t, m)["outlier_detection"].(map[string]any)["max_ejection_percent"] = 10
		}, "max_ejection_percent is 10"},
		{"a passing check unejects", func(m map[string]any) {
			clusterOf(t, m)["outlier_detection"].(map[string]any)["successful_active_health_check_uneject_host"] = true
		}, "successful_active_health_check_uneject_host is true"},
		{"a check of another port", func(m map[string]any) {
			at(t, clusterOf(t, m), "load_assignment", "endpoints", 0, "lb_endpoints", 0, "endpoint", "health_check_config", "address", "socket_address").(map[string]any)["port_value"] = 30161
		}, "port_value is 30161"},
		{"a check off loopback", func(m map[string]any) {
			at(t, clusterOf(t, m), "load_assignment", "endpoints", 0, "lb_endpoints", 0, "endpoint", "health_check_config", "address", "socket_address").(map[string]any)["address"] = "0.0.0.0"
		}, "address is 0.0.0.0"},
		{"a check naming another worker", func(m map[string]any) {
			at(t, clusterOf(t, m), "load_assignment", "endpoints", 0, "lb_endpoints", 0, "endpoint", "health_check_config").(map[string]any)["hostname"] = "worker-1"
		}, "hostname is worker-1"},
		{"a check with no Host", func(m map[string]any) {
			delete(at(t, clusterOf(t, m), "load_assignment", "endpoints", 0, "lb_endpoints", 0, "endpoint", "health_check_config").(map[string]any), "hostname")
		}, "has no hostname"},
		{"Envoy's default 1 h upstream idle", func(m map[string]any) {
			at(t, clusterOf(t, m), "typed_extension_protocol_options", "envoy.extensions.upstreams.http.v3.HttpProtocolOptions", "common_http_protocol_options").(map[string]any)["idle_timeout"] = "3600s"
		}, "idle_timeout is 3600s"},
	} {
		t.Run(c.name, func(t *testing.T) {
			m := renderMap(t, cfg)
			c.edit(m)
			err := agent.CheckEnvoyConfig(mustJSON(t, m), cfg)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("the guard: %v; want a refusal containing %q", err, c.want)
			}
			if !errors.Is(err, agent.ErrEnvoyRefused) {
				t.Errorf("the refusal %v is not ErrEnvoyRefused", err)
			}
		})
	}
}

// The floor is the front's maximum request duration plus 1 h, 10,800 s, exactly: at it the config
// runs, one second below it does not (Q7, as corrected).
func TestEnvoyGuardFloorIsTheFrontsMaximumPlusAnHour(t *testing.T) {
	if agent.EnvoyRouterTimeout != 10800*time.Second {
		t.Fatalf("EnvoyRouterTimeout = %v, want 10,800 s: the front's 2 h plus 1 h", agent.EnvoyRouterTimeout)
	}
	cfg := envoyCfg()
	for _, c := range []struct {
		value string
		ok    bool
	}{{"10800s", true}, {"10801s", true}, {"10799s", false}, {"10799.5s", false}, {"7200s", false}} {
		m := renderMap(t, cfg)
		routeOf(t, m)["timeout"] = c.value
		err := agent.CheckEnvoyConfig(mustJSON(t, m), cfg)
		if (err == nil) != c.ok {
			t.Errorf("a route timeout of %s: %v, want accepted=%v", c.value, err, c.ok)
		}
	}
	for _, bad := range []agent.EnvoyConfig{
		{RunDir: "relative", Workers: cfg.Workers},
		{RunDir: envoyRunDir},
	} {
		if _, err := agent.RenderEnvoyConfig(bad); err == nil {
			t.Errorf("RenderEnvoyConfig(%+v) rendered", bad)
		}
	}
}

// No deployment's maxRequestDuration can reach the router's timeouts (Q7, as corrected): neither what
// the agent renders from (EnvoyConfig) nor what the controller sends it (DesiredRouter, DesiredInstance)
// has a duration a timeout could be taken from, so a deployment with a short maxRequestDuration gets
// 10,800 s like any other. A field added for that fails here.
func TestTheRouterTimeoutsTakeNothingFromADeployment(t *testing.T) {
	durationType := reflect.TypeOf(time.Duration(0))
	wireDuration := reflect.TypeOf(agentapi.Duration(0))
	for _, typ := range []reflect.Type{reflect.TypeOf(agent.EnvoyConfig{}), reflect.TypeOf(agentapi.DesiredRouter{})} {
		for i := 0; i < typ.NumField(); i++ {
			if f := typ.Field(i); f.Type == durationType || f.Type == wireDuration {
				t.Errorf("%s.%s is a duration: the router's timeouts must not come from a deployment", typ.Name(), f.Name)
			}
		}
	}
	for _, f := range []string{"MaxRequestDuration", "RouterTimeout"} {
		if _, ok := reflect.TypeOf(agentapi.DesiredInstance{}).FieldByName(f); ok {
			t.Errorf("DesiredInstance has %s: the router's timeouts must not come from a deployment", f)
		}
	}
	r := routeOf(t, renderMap(t, envoyCfg()))
	if r["timeout"] != "10800s" || r["idle_timeout"] != "10800s" {
		t.Errorf("the route's timeouts are %v and %v, want 10800s each", r["timeout"], r["idle_timeout"])
	}
}

// The real Envoy 1.40.2 accepts the agent's bootstrap as it is written (--mode validate reads every
// field against Envoy's own schema). The control shows validation can fail: one misspelt field,
// made to the same bytes, is refused.
func TestTheRealEnvoyValidatesTheAgentsConfig(t *testing.T) {
	bin := testenv.EnvoyBinary(t)
	dir := t.TempDir()
	cfg := envoyCfg()
	cfg.RunDir = dir
	cfg.Workers[0].Socket = filepath.Join(dir, "worker-0.sock")
	raw, err := agent.RenderEnvoyConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.CheckEnvoyConfig(raw, cfg); err != nil {
		t.Fatal(err)
	}
	validate := func(name string, b []byte) (string, error) {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		out, err := exec.Command(bin, "--mode", "validate", "-c", p, "--disable-hot-restart", "--log-level", "warn").CombinedOutput()
		return string(out), err
	}
	if out, err := validate("envoy.json", raw); err != nil || !strings.Contains(out, "OK") {
		t.Fatalf("Envoy refused the agent's config: %v\n%s", err, out)
	}
	bad := strings.Replace(string(raw), `"suppress_envoy_headers"`, `"suppress_envoy_headerz"`, 1)
	if out, err := validate("bad.json", []byte(bad)); err == nil || !strings.Contains(out, "suppress_envoy_headerz") {
		t.Fatalf("Envoy accepted a misspelt field: %v\n%s", err, out)
	}
}
