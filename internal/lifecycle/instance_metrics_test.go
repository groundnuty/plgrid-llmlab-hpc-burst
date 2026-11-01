package lifecycle

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// The tests in this file cover HBEP-8, Metrics: the engine gauges per Instance
// on /metrics, under HBEP-8's hpcburst_ names, and an unknown sample as an
// absent series plus hpcburst_instance_metrics_unknown{reason}, never 0 and
// never NaN (Unknown samples). The scrape goes through promhttp's handler of a
// pedantic registry, so the text is what /metrics serves and the series agree
// with the collector's descriptors. Expected values are the engine double's,
// written out here.

// engineNames are HBEP-8's eight engine metrics (Engine gauges per Instance).
var engineNames = []string{
	"hpcburst_instance_running_requests",
	"hpcburst_instance_waiting_requests",
	"hpcburst_instance_time_per_output_token_seconds",
	"hpcburst_instance_preemptions_total",
	"hpcburst_instance_prefix_cache_queried_tokens_total",
	"hpcburst_instance_prefix_cache_hit_tokens_total",
	"hpcburst_instance_kv_cache_usage_ratio",
	"hpcburst_instance_generation_tokens_total",
}

// expoSample is one sample line of the text exposition.
type expoSample struct {
	name   string
	labels map[string]string
	value  string
}

// exposition is a scrape: its samples, and the TYPE and HELP of each family.
type exposition struct {
	samples []expoSample
	types   map[string]string
	help    map[string]string
}

var (
	sampleLine = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})? (\S+)$`)
	labelPair  = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"`)
)

// scrape serves the rig's engine gauges as /metrics and parses the text.
func scrape(t *testing.T, rg *rig) exposition {
	t.Helper()
	return scrapeCollectors(t, &instanceCollector{inv: rg.r.inventory(), backend: rg.backend, clock: rg.clock})
}

// scrapeCollectors serves collectors as /metrics through a pedantic registry
// and parses the text.
func scrapeCollectors(t *testing.T, cs ...prometheus.Collector) exposition {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(cs...)
	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{ErrorHandling: promhttp.HTTPErrorOnError}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: %d", resp.StatusCode)
	}
	ex := exposition{types: map[string]string{}, help: map[string]string{}}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if f := strings.Fields(line); len(f) == 4 && f[0] == "#" && f[1] == "TYPE" {
			ex.types[f[2]] = f[3]
			continue
		}
		if f := strings.SplitN(line, " ", 4); len(f) == 4 && f[0] == "#" && f[1] == "HELP" {
			ex.help[f[2]] = f[3]
			continue
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		m := sampleLine.FindStringSubmatch(line)
		if m == nil {
			t.Fatalf("unparsed exposition line %q", line)
		}
		s := expoSample{name: m[1], labels: map[string]string{}, value: m[3]}
		for _, p := range labelPair.FindAllStringSubmatch(m[2], -1) {
			s.labels[p[1]] = p[2]
		}
		ex.samples = append(ex.samples, s)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return ex
}

// find returns the samples of a name whose labels include want.
func (ex exposition) find(name string, want map[string]string) []expoSample {
	var out []expoSample
	for _, s := range ex.samples {
		if s.name != name {
			continue
		}
		ok := true
		for k, v := range want {
			if s.labels[k] != v {
				ok = false
			}
		}
		if ok {
			out = append(out, s)
		}
	}
	return out
}

// value returns the one sample's value, failing on none or several.
func (ex exposition) value(t *testing.T, name string, want map[string]string) string {
	t.Helper()
	got := ex.find(name, want)
	if len(got) != 1 {
		t.Fatalf("%s%v: %d samples, want 1 (all: %v)", name, want, len(got), ex.samples)
	}
	return got[0].value
}

// engineSeries returns the samples of any engine metric (a summary's _sum and
// _count included) for an Instance.
func (ex exposition) engineSeries(uid string) []expoSample {
	var out []expoSample
	for _, s := range ex.samples {
		base := strings.TrimSuffix(strings.TrimSuffix(s.name, "_sum"), "_count")
		if (slices.Contains(engineNames, s.name) || slices.Contains(engineNames, base)) && s.labels["instance_uid"] == uid {
			out = append(out, s)
		}
	}
	return out
}

// unknowns returns the metric names marked unknown for a worker, by reason.
func (ex exposition) unknowns(uid, worker string) map[string][]string {
	out := map[string][]string{}
	for _, s := range ex.find("hpcburst_instance_metrics_unknown", map[string]string{"instance_uid": uid, "worker": worker}) {
		if s.value != "1" {
			panic("hpcburst_instance_metrics_unknown with value " + s.value)
		}
		out[s.labels["reason"]] = append(out[s.labels["reason"]], s.labels["metric"])
	}
	for _, v := range out {
		slices.Sort(v)
	}
	return out
}

func sorted(names []string) []string {
	out := slices.Clone(names)
	slices.Sort(out)
	return out
}

// knownHealth is an engine with a distinct known value in every field, a real
// zero of preemptions among them.
func knownHealth(now time.Time, _ afake.Process) agentapi.Health {
	read := now
	return agentapi.Health{
		Time: now, ProbeOK: true, MetricsReadAt: &read,
		RunningRequests: agentapi.Int64(3), WaitingRequests: agentapi.Int64(2), GeneratedTokens: agentapi.Int64(1234),
		KVCacheUsage: agentapi.Float64(0.25), Preemptions: agentapi.Int64(0),
		PrefixCacheQueriedTokens: agentapi.Int64(1000), PrefixCacheHitTokens: agentapi.Int64(600),
		TimePerOutputTokenSecondsSum: agentapi.Float64(1.5), TimePerOutputTokenCount: agentapi.Int64(30),
		ResponsesByStatus: map[string]int64{"2xx": 30},
	}
}

// TestMetricsFreshEngineGauges: a fresh report gives every engine series with
// the engine's values under HBEP-8's names, types and labels, a real zero as
// 0, the info series with the Instance's descriptive labels, the read time, and
// no unknown series.
func TestMetricsFreshEngineGauges(t *testing.T) {
	rg, in, alloc := healthRig(t, func() afake.Engine { return afake.Engine{Health: knownHealth} })
	uid := string(in.UID)
	ex := scrape(t, rg)
	if v := ex.value(t, "hpcburst_instance_info", map[string]string{"instance_uid": uid, "namespace": rigNS, "instance": in.Name,
		"model_deployment": "md", "provider": rigProvider, "gpu_model": rigGPU}); v != "1" {
		t.Fatalf("hpcburst_instance_info = %s, want 1", v)
	}
	ids := map[string]string{"instance_uid": uid, "worker": "worker-0", "generation": "0", "worker_generation": "0"}
	for name, want := range map[string]string{
		"hpcburst_instance_running_requests":                    "3",
		"hpcburst_instance_waiting_requests":                    "2",
		"hpcburst_instance_generation_tokens_total":             "1234",
		"hpcburst_instance_kv_cache_usage_ratio":                "0.25",
		"hpcburst_instance_preemptions_total":                   "0",
		"hpcburst_instance_prefix_cache_queried_tokens_total":   "1000",
		"hpcburst_instance_prefix_cache_hit_tokens_total":       "600",
		"hpcburst_instance_time_per_output_token_seconds_sum":   "1.5",
		"hpcburst_instance_time_per_output_token_seconds_count": "30",
	} {
		if got := ex.value(t, name, ids); got != want {
			t.Errorf("%s = %s, want %s", name, got, want)
		}
	}
	for name, typ := range map[string]string{
		"hpcburst_instance_running_requests":              "gauge",
		"hpcburst_instance_kv_cache_usage_ratio":          "gauge",
		"hpcburst_instance_preemptions_total":             "counter",
		"hpcburst_instance_generation_tokens_total":       "counter",
		"hpcburst_instance_time_per_output_token_seconds": "summary",
		"hpcburst_instance_info":                          "gauge",
	} {
		if ex.types[name] != typ {
			t.Errorf("TYPE of %s = %q, want %s", name, ex.types[name], typ)
		}
	}
	if q := ex.find("hpcburst_instance_time_per_output_token_seconds", nil); len(q) != 0 {
		t.Errorf("time per output token carries quantiles %v; HBEP-8 has _sum and _count only", q)
	}
	if u := ex.unknowns(uid, "worker-0"); len(u) != 0 {
		t.Errorf("unknown series %v for a fresh report", u)
	}
	rep, ok := rg.agentOf(alloc).agent.LastReport()
	if !ok {
		t.Fatal("the agent sent no report")
	}
	readAt := worker0(rep.Instance).Health.MetricsReadAt
	got, err := strconv.ParseFloat(ex.value(t, "hpcburst_instance_metrics_read_timestamp_seconds", map[string]string{"instance_uid": uid, "worker": "worker-0"}), 64)
	if err != nil || got != float64(readAt.Unix()) {
		t.Errorf("read timestamp %v (%v), want %d, the report's metricsReadAt", got, err, readAt.Unix())
	}
}

// TestMetricsStaleReportIsUnknownNeverZero: a sample that is not fresh shows
// every engine metric as unknown with its reason, and no engine series at all:
// no 0 and no NaN. The scrape judges freshness at its own time, so a report 21
// s old at the scrape is stale with no pass between; the control at 20 s, the
// bound, is fresh. An unreachable agent gives agent_unreachable.
func TestMetricsStaleReportIsUnknownNeverZero(t *testing.T) {
	for _, tc := range []struct {
		name    string
		health  func(time.Time, afake.Process) agentapi.Health
		prepare func(*rig, string)
		reason  string // "" is the fresh control
		readAt  bool
	}{
		{name: "read-25s-old", health: afake.HealthMetricsStale(25 * time.Second), reason: unknownStale, readAt: true},
		{name: "never-read", health: afake.HealthMetricsUnreadable, reason: unknownStale},
		{name: "no-report-for-21s", health: knownHealth, prepare: func(rg *rig, _ string) { rg.clock.Advance(21 * time.Second) }, reason: unknownStale, readAt: true},
		{name: "no-report-for-20s-control", health: knownHealth, prepare: func(rg *rig, _ string) { rg.clock.Advance(20 * time.Second) }, readAt: true},
		{name: "agent-unreachable", health: knownHealth, prepare: func(rg *rig, alloc string) {
			rg.agentOf(alloc).agent.Silence(true)
			rg.run(2*time.Minute, startTick)
		}, reason: unknownAgentUnreachable, readAt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg, in, alloc := healthRig(t, func() afake.Engine { return afake.Engine{Health: tc.health} })
			if tc.prepare != nil {
				tc.prepare(rg, alloc)
			}
			if tc.reason == unknownAgentUnreachable && !isTrue(rg.instance(in.Name).Status.Conditions, v1alpha1.ConditionAgentUnreachable) {
				t.Fatal("the agent is not judged unreachable; the case proves nothing")
			}
			uid := string(in.UID)
			ex := scrape(t, rg)
			series := ex.engineSeries(uid)
			u := ex.unknowns(uid, "worker-0")
			if tc.reason == "" {
				if len(series) != 9 || len(u) != 0 {
					t.Fatalf("fresh control: %d engine samples, unknowns %v; want all 9 samples and none unknown", len(series), u)
				}
				return
			}
			if len(series) != 0 {
				t.Fatalf("engine series %v while the sample is %s; an unknown value is absent, never 0 or NaN", series, tc.reason)
			}
			if len(u) != 1 || !slices.Equal(u[tc.reason], sorted(engineNames)) {
				t.Fatalf("unknown series %v, want every engine metric with reason %s", u, tc.reason)
			}
			if got := len(ex.find("hpcburst_instance_metrics_read_timestamp_seconds", map[string]string{"instance_uid": uid})); (got == 1) != tc.readAt {
				t.Fatalf("%d read timestamp series, want present=%v (absent only while there has been no read)", got, tc.readAt)
			}
			if len(ex.find("hpcburst_instance_info", map[string]string{"instance_uid": uid})) != 1 {
				t.Fatal("no info series for an Instance that has not ended")
			}
		})
	}
}

// TestMetricsNotReportedSeries: an engine double that omits the prefix-cache
// counter lines shows those two series absent with reason not_reported, and
// every other series present (HBEP-8, Test plan). A time-per-output-token sum
// without its count is unknown as a whole.
func TestMetricsNotReportedSeries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		health  func(time.Time, afake.Process) agentapi.Health
		missing []string
	}{
		{"prefix-cache-lines-omitted", afake.HealthWithoutPrefixCache,
			[]string{"hpcburst_instance_prefix_cache_hit_tokens_total", "hpcburst_instance_prefix_cache_queried_tokens_total"}},
		{"tpot-sum-without-count", func(now time.Time, p afake.Process) agentapi.Health {
			h := knownHealth(now, p)
			h.TimePerOutputTokenCount = nil
			return h
		}, []string{"hpcburst_instance_time_per_output_token_seconds"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg, in, _ := healthRig(t, func() afake.Engine { return afake.Engine{Health: tc.health} })
			uid := string(in.UID)
			ex := scrape(t, rg)
			if u := ex.unknowns(uid, "worker-0"); len(u) != 1 || !slices.Equal(u[unknownNotReported], tc.missing) {
				t.Fatalf("unknown series %v, want %v not_reported", u, tc.missing)
			}
			for _, name := range engineNames {
				n := len(ex.find(name, map[string]string{"instance_uid": uid})) + len(ex.find(name+"_sum", map[string]string{"instance_uid": uid})) +
					len(ex.find(name+"_count", map[string]string{"instance_uid": uid}))
				if missing := slices.Contains(tc.missing, name); missing != (n == 0) {
					t.Errorf("%s: %d samples, want present=%v", name, n, !missing)
				}
			}
		})
	}
}

// TestMetricsSeriesScope (`@hpc-burst-science-agent`, 31 Oct 2026, for #111):
// an Instance with no agent yet has its info series and nothing else; once it
// has ended, nothing of it is left.
func TestMetricsSeriesScope(t *testing.T) {
	t.Run("pending-info-only", func(t *testing.T) {
		rg := newRig(t)
		rg.noAgents = true
		rg.create(mdSpec("md"))
		rg.run(time.Minute, startTick)
		insts := rg.instances("md")
		if len(insts) != 1 || insts[0].Status.Phase != v1alpha1.InstancePending {
			t.Fatalf("Instances %v, want one pending", phases(insts))
		}
		uid := string(insts[0].UID)
		ex := scrape(t, rg)
		var of []expoSample
		for _, s := range ex.samples {
			if s.labels["instance_uid"] == uid {
				of = append(of, s)
			}
		}
		if len(of) != 1 || of[0].name != "hpcburst_instance_info" {
			t.Fatalf("series of the pending Instance %v, want its info series only", of)
		}
	})
	// HBEP-8, Test plan, Series scope: an Instance not yet ready whose report
	// lists the worker, with no fresh read yet, shows each engine metric of the
	// worker as unknown with reason stale, beside its info series.
	t.Run("starting-worker-stale", func(t *testing.T) {
		rg := newRig(t)
		rg.engine = func(string) afake.Engine { return afake.Engine{Health: afake.HealthMetricsUnreadable} }
		rg.create(mdSpec("md"))
		var in v1alpha1.Instance
		startUntil(rg, "md", 10*time.Minute, "a report that lists worker-0", func() bool {
			insts := rg.instances("md")
			if len(insts) != 1 || insts[0].Spec.AllocationRef.Name == "" {
				return false
			}
			in = insts[0]
			ra, ok := rg.agents[in.Spec.AllocationRef.Name]
			if !ok {
				return false
			}
			rep, ok := ra.agent.LastReport()
			return ok && rep.Instance != nil && worker0(rep.Instance) != nil
		})
		in = *rg.instance(in.Name)
		if ph := in.Status.Phase; ph != v1alpha1.InstanceStarting && ph != v1alpha1.InstanceVerifying {
			t.Fatalf("phase %s when worker-0 is first reported, want starting or verifying; the case proves nothing", ph)
		}
		uid := string(in.UID)
		ex := scrape(t, rg)
		if len(ex.find("hpcburst_instance_info", map[string]string{"instance_uid": uid})) != 1 {
			t.Fatal("no info series for the starting Instance")
		}
		if u := ex.unknowns(uid, "worker-0"); len(u) != 1 || !slices.Equal(u[unknownStale], sorted(engineNames)) {
			t.Fatalf("unknown series %v, want every engine metric of worker-0 with reason stale", u)
		}
		if series := ex.engineSeries(uid); len(series) != 0 {
			t.Fatalf("engine series %v before the first fresh read", series)
		}
	})
	t.Run("ended-nothing", func(t *testing.T) {
		rg, in, _ := healthRig(t, func() afake.Engine { return afake.Engine{} })
		uid := string(in.UID)
		if len(scrape(t, rg).find("hpcburst_instance_info", map[string]string{"instance_uid": uid})) != 1 {
			t.Fatal("no info series while ready; the case proves nothing")
		}
		md := rg.md("md")
		if err := rg.c.Delete(rg.ctx, md); err != nil {
			t.Fatal(err)
		}
		rg.until(10*time.Minute, startTick, "the Instance ended", func() bool {
			var got v1alpha1.Instance
			err := rg.c.Get(rg.ctx, client.ObjectKey{Namespace: rigNS, Name: in.Name}, &got)
			return err != nil || ended(&got)
		})
		for _, s := range scrape(t, rg).samples {
			if s.labels["instance_uid"] == uid {
				t.Fatalf("series %v of an ended Instance", s)
			}
		}
	})
}

// TestMetricsRestartAtEachRungStartsNewSeries covers HBEP-8, Test plan,
// "Restart an engine at each rung: a new series starts under the new
// generation labels, and no counter series decreases". With attemptsPerRung 1,
// two crashes climb the worker rung (worker_generation 1) and then the
// whole-Instance rung (generation 1). The engine double's counters start again
// at every new process, as a real engine's do. Scraped at every step, no label
// set's counter ever goes down, each rung shows a label set of its own, and a
// superseded label set is gone once the new process is reported.
func TestMetricsRestartAtEachRungStartsNewSeries(t *testing.T) {
	rg, in, alloc := healthRig(t, func() afake.Engine { return afake.Engine{Health: afake.HealthEngineCounters()} },
		withProfile(func(p *v1alpha1.PlatformProfileSpec) {
			p.Restart = &v1alpha1.RestartPolicy{AttemptsPerRung: ptrTo(int32(1))}
		}))
	uid := string(in.UID)
	counters := []string{
		"hpcburst_instance_generation_tokens_total",
		"hpcburst_instance_preemptions_total",
		"hpcburst_instance_prefix_cache_queried_tokens_total",
		"hpcburst_instance_prefix_cache_hit_tokens_total",
		"hpcburst_instance_time_per_output_token_seconds_sum",
		"hpcburst_instance_time_per_output_token_seconds_count",
	}
	last := map[string]float64{}  // counter + its full label set -> last value
	seenGens := map[string]bool{} // generation/worker_generation pairs seen
	check := func() []string {
		ex := scrape(t, rg)
		var gens []string
		for _, name := range counters {
			for _, s := range ex.find(name, map[string]string{"instance_uid": uid}) {
				g := s.labels["generation"] + "/" + s.labels["worker_generation"]
				key := name + "{" + g + "}"
				v, err := strconv.ParseFloat(s.value, 64)
				if err != nil {
					t.Fatalf("%s = %q: %v", key, s.value, err)
				}
				if old, ok := last[key]; ok && v < old {
					t.Fatalf("%s went down from %v to %v", key, old, v)
				}
				last[key] = v
				seenGens[g] = true
				if !slices.Contains(gens, g) {
					gens = append(gens, g)
				}
			}
		}
		return gens
	}
	ag := rg.agentOf(alloc).agent
	for _, want := range []string{"0/1", "1/1"} { // the worker rung, then the whole Instance
		ag.CrashWorker(healthWorker, 1)
		for end := rg.clock.Now().Add(15 * time.Minute); ; startStep(rg, "md", startTick) {
			gens := check()
			if rg.readyCount("md") == 1 && slices.Equal(gens, []string{want}) {
				break
			}
			if !rg.clock.Now().Before(end) {
				t.Fatalf("label sets %v at %s, want only generation/worker_generation %s once ready again", gens, rg.clock.Now().Format(time.TimeOnly), want)
			}
		}
		// The new process's counters rise from its own start.
		for i := 0; i < 3; i++ {
			startStep(rg, "md", startTick)
			check()
		}
	}
	for _, g := range []string{"0/0", "0/1", "1/1"} {
		if !seenGens[g] {
			t.Errorf("generation/worker_generation %s never exposed (seen %v)", g, seenGens)
		}
	}
	if got := rg.instance(in.Name).Status.Restarts; len(got) != 2 {
		t.Fatalf("%d restarts recorded, want 2 (worker, then whole Instance)", len(got))
	}
}
