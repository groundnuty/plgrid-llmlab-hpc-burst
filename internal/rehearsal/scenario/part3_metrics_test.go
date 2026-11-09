package scenario_test

// Item 2 of issue #193, HBEP-8's metrics: the front's metrics for both kinds
// of target, and the lifecycle controller's engine gauges per Instance, with
// unknowns shown as unknown. The metric names and labels are HBEP-8's
// (Metrics), written here, not read from the code.

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// sample is one line of Prometheus's text format.
type sample struct {
	name   string
	labels map[string]string
	value  float64
}

// parseExposition reads Prometheus's text format: comments are skipped, and
// each sample line is name{label="value",...} value.
func parseExposition(text string) ([]sample, error) {
	var out []sample
	sc := bufio.NewScanner(strings.NewReader(text))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		s := sample{labels: map[string]string{}}
		rest := l
		if i := strings.IndexAny(l, "{ "); i >= 0 && l[i] == '{' {
			s.name = l[:i]
			rest = l[i+1:]
			for {
				if strings.HasPrefix(rest, "}") {
					rest = rest[1:]
					break
				}
				eq := strings.Index(rest, `="`)
				if eq < 0 {
					return nil, fmt.Errorf("a label without a value: %q", l)
				}
				key := strings.TrimPrefix(rest[:eq], ",")
				rest = rest[eq+2:]
				var v strings.Builder
				for i := 0; ; i++ {
					if i >= len(rest) {
						return nil, fmt.Errorf("an unterminated label value: %q", l)
					}
					c := rest[i]
					if c == '\\' && i+1 < len(rest) {
						switch rest[i+1] {
						case 'n':
							v.WriteByte('\n')
						default:
							v.WriteByte(rest[i+1])
						}
						i++
						continue
					}
					if c == '"' {
						rest = rest[i+1:]
						break
					}
					v.WriteByte(c)
				}
				s.labels[key] = v.String()
				rest = strings.TrimPrefix(rest, ",")
			}
		} else if i >= 0 {
			s.name, rest = l[:i], l[i:]
		} else {
			return nil, fmt.Errorf("a line with no value: %q", l)
		}
		f := strings.Fields(rest)
		if len(f) == 0 {
			return nil, fmt.Errorf("a line with no value: %q", l)
		}
		v, err := strconv.ParseFloat(f[0], 64)
		if err != nil {
			return nil, fmt.Errorf("value of %q: %v", l, err)
		}
		s.value = v
		out = append(out, s)
	}
	return out, sc.Err()
}

// scrape reads and parses one /metrics; ok is false when it could not.
func scrape(url string) ([]sample, error) {
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return parseExposition(string(b))
}

// find returns the samples named name whose labels include want.
func find(ss []sample, name string, want map[string]string) []sample {
	var out []sample
	for _, s := range ss {
		if s.name != name {
			continue
		}
		ok := true
		for k, v := range want {
			if s.labels[k] != v {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, s)
		}
	}
	return out
}

func sum(ss []sample) float64 {
	var t float64
	for _, s := range ss {
		t += s.value
	}
	return t
}

// frontCuts are HBEP-5's cutBy values, the cut_by label's (HBEP-8, Front
// metrics).
var frontCuts = []string{"front_deadline", "front_idle", "front_headers", "tunnel", "upstream", "lease_end", "client", "front_shutdown"}

// frontMetricsBothKinds is HBEP-8's front metrics for both kinds of target:
// two chat requests, one streamed, to the queued primary and two to the
// managed target, between two scrapes of the front's /metrics with no other
// request in between. Each target's requests counter rises by its requests,
// with the labels of its usage records; its time-to-first-token histogram by
// the records with a first token; and the counts of its own events (cuts and
// pauses) exist at 0, never absent. The managed requests also check the
// managed target's attribution.
func (p3 *part3) frontMetricsBothKinds(d *depRun) {
	r := p3.rig
	r.t.Helper()
	url := r.ports.URL(4, "/metrics")
	before, err := scrape(url)
	if err != nil {
		r.t.Fatalf("the front's /metrics: %v", err)
	}
	type target struct {
		kind, model string
		want        wantAttr
		rows        []usageRow
	}
	targets := []*target{
		{kind: "the Instance", model: d.Model, want: wantInstance(d, d.primary)},
		{kind: "the managed target", model: managedModel, want: p3.wantManaged()},
	}
	for _, tg := range targets {
		rp := r.send("/v1/chat/completions", chatBodyFor(tg.model, 16, 0, false), "")
		if rp.err != nil || rp.status != http.StatusOK {
			r.t.Fatalf("a chat request to %s: %d %v: %.300s", tg.kind, rp.status, rp.err, rp.body)
		}
		row := r.rowOf(rp.id)
		p3.attribute("a chat request to "+tg.kind, rp, row, tg.want)
		first, done := r.startStream(chatBodyFor(tg.model, 16, 0, true), "rehearsal-part3-metrics")
		<-first
		s := <-done
		if s.err != nil || s.status != http.StatusOK {
			r.t.Fatalf("a streamed chat request to %s: %d %v: %.300s", tg.kind, s.status, s.err, s.body)
		}
		if fin, ok := sseDone(s.body); !fin || !ok {
			r.t.Errorf("the streamed chat request to %s: [DONE] %v, a finish reason %v", tg.kind, fin, ok)
		}
		srow := r.rowOf(s.id)
		p3.attribute("a streamed chat request to "+tg.kind, s.reply, srow, tg.want)
		tg.rows = []usageRow{row, srow}
	}
	after, err := scrape(url)
	if err != nil {
		r.t.Fatalf("the front's /metrics: %v", err)
	}
	for _, tg := range targets {
		dep := tg.rows[0].ModelDeployment
		prov := tg.rows[0].Provider
		if dep == "" || prov == "" {
			r.t.Errorf("%s: its usage record names no deployment (%q) or provider (%q)", tg.kind, dep, prov)
			continue
		}
		labels := map[string]string{"code": "200", "endpoint": "chat", "provider": prov, "model_deployment": dep}
		if got := sum(find(after, "hpcburst_front_requests_total", labels)) - sum(find(before, "hpcburst_front_requests_total", labels)); got != 2 {
			r.t.Errorf("%s: hpcburst_front_requests_total%v rose by %v across its 2 requests", tg.kind, labels, got)
		}
		firsts := 0
		for _, row := range tg.rows {
			if row.FirstTokenMs != nil {
				firsts++
			}
		}
		tl := map[string]string{"provider": prov, "model_deployment": dep, "endpoint": "chat"}
		got := sum(find(after, "hpcburst_front_time_to_first_token_seconds_count", tl)) - sum(find(before, "hpcburst_front_time_to_first_token_seconds_count", tl))
		if firsts == 0 || got != float64(firsts) {
			r.t.Errorf("%s: hpcburst_front_time_to_first_token_seconds_count%v rose by %v; %d of its records have a first token", tg.kind, tl, got, firsts)
		}
		for _, c := range frontCuts {
			if len(find(after, "hpcburst_front_cuts_total", map[string]string{"cut_by": c, "provider": prov, "model_deployment": dep})) != 1 {
				r.t.Errorf("%s: no hpcburst_front_cuts_total series for cut_by %s: a count of the front's own events starts at 0 (HBEP-8)", tg.kind, c)
			}
		}
		if len(find(after, "hpcburst_front_pauses_total", map[string]string{"provider": prov, "model_deployment": dep})) != 1 {
			r.t.Errorf("%s: no hpcburst_front_pauses_total series", tg.kind)
		}
		r.t.Logf("front metrics, %s: provider %s, model_deployment %s: requests +2, time to first token +%d", tg.kind, prov, dep, firsts)
	}
	recs := len(r.usageRows())
	if got := sum(find(after, "hpcburst_usage_records_total", nil)); got < float64(recs) {
		r.t.Errorf("hpcburst_usage_records_total is %v, below the %d records in the usage files", got, recs)
	}
	if got := find(after, "hpcburst_usage_write_failures_total", nil); len(got) != 1 || got[0].value != 0 {
		r.t.Errorf("hpcburst_usage_write_failures_total: %v, want one series at 0", got)
	}
	if got := find(after, "hpcburst_usage_dir_free_bytes", nil); len(got) != 1 || got[0].value <= 0 {
		r.t.Errorf("hpcburst_usage_dir_free_bytes: %v, want one positive series", got)
	}
}

// The engine gauges of HBEP-8's table (Metrics, Engine gauges per Instance):
// the metric label of an unknown sample names them (Unknown samples).
const (
	gRunning   = "hpcburst_instance_running_requests"
	gWaiting   = "hpcburst_instance_waiting_requests"
	gTPOT      = "hpcburst_instance_time_per_output_token_seconds"
	gPreempt   = "hpcburst_instance_preemptions_total"
	gPrefixQ   = "hpcburst_instance_prefix_cache_queried_tokens_total"
	gPrefixHit = "hpcburst_instance_prefix_cache_hit_tokens_total"
	gKV        = "hpcburst_instance_kv_cache_usage_ratio"
	gGenerated = "hpcburst_instance_generation_tokens_total"
)

var engineGauges = []string{gRunning, gWaiting, gTPOT, gPreempt, gPrefixQ, gPrefixHit, gKV, gGenerated}

// notReported are the gauges the served engines do not export: the engine
// double's no-prefix-cache-metrics mode (part3EngineModes).
var notReported = map[string]bool{gPrefixQ: true, gPrefixHit: true}

// seriesNames are a gauge's series in the exposition: a summary's _sum and
// _count (HBEP-8: no quantiles), else its own name.
func seriesNames(g string) []string {
	if g == gTPOT {
		return []string{g + "_sum", g + "_count"}
	}
	return []string{g}
}

// gaugeView is what the gauge check expects of one Instance.
type gaugeView struct {
	uid, namespace, name, deployment, provider, gpuModel string
	// reported: the agent has reported fresh metrics, so each gauge is
	// either present or unknown not_reported; otherwise each is unknown
	// with staleReason.
	reported    bool
	staleReason string
	// minGenerated is a lower bound on generation_tokens_total: the tokens
	// the front saw the Instance generate.
	minGenerated float64
}

// gaugeProblems holds one Instance's series to HBEP-8 (Unknown samples;
// Which Instances have series): its info series with its descriptive labels;
// for worker-0, each engine gauge either present, with the instance_uid,
// worker, generation and worker_generation labels, or absent with one
// hpcburst_instance_metrics_unknown naming it and its reason, never both and
// never neither; not_reported for the gauges the engine does not export,
// stale (or another given reason) for an Instance with no report; the read
// time while it has one; and generation_tokens_total no lower than what was
// generated.
func gaugeProblems(ss []sample, v gaugeView) []string {
	var out []string
	info := find(ss, "hpcburst_instance_info", map[string]string{"instance_uid": v.uid})
	if len(info) != 1 || info[0].value != 1 {
		out = append(out, fmt.Sprintf("%d hpcburst_instance_info series for %s, want one at 1", len(info), v.uid))
	} else {
		for k, want := range map[string]string{"namespace": v.namespace, "instance": v.name, "model_deployment": v.deployment, "provider": v.provider, "gpu_model": v.gpuModel} {
			if got := info[0].labels[k]; got != want {
				out = append(out, fmt.Sprintf("hpcburst_instance_info's %s is %q, want %q", k, got, want))
			}
		}
	}
	w := map[string]string{"instance_uid": v.uid, "worker": "worker-0"}
	for _, g := range engineGauges {
		var present []sample
		for _, n := range seriesNames(g) {
			present = append(present, find(ss, n, w)...)
		}
		unknown := find(ss, "hpcburst_instance_metrics_unknown", map[string]string{"instance_uid": v.uid, "worker": "worker-0", "metric": g})
		wantReason := ""
		switch {
		case !v.reported:
			wantReason = v.staleReason
		case notReported[g]:
			wantReason = "not_reported"
		}
		if wantReason == "" {
			if len(present) != len(seriesNames(g)) || len(unknown) != 0 {
				out = append(out, fmt.Sprintf("%s: %d series and %d unknown samples, want it known", g, len(present), len(unknown)))
				continue
			}
			for _, s := range present {
				for _, k := range []string{"generation", "worker_generation"} {
					if s.labels[k] == "" {
						out = append(out, fmt.Sprintf("%s has no %s label: %v", s.name, k, s.labels))
					}
				}
			}
			continue
		}
		if len(present) != 0 || len(unknown) != 1 || unknown[0].labels["reason"] != wantReason || unknown[0].value != 1 {
			reasons := []string{}
			for _, u := range unknown {
				reasons = append(reasons, u.labels["reason"])
			}
			out = append(out, fmt.Sprintf("%s: %d series and unknown samples with reasons %v, want it absent and unknown %s", g, len(present), reasons, wantReason))
		}
	}
	if readAt := find(ss, "hpcburst_instance_metrics_read_timestamp_seconds", w); v.reported && len(readAt) != 1 {
		out = append(out, fmt.Sprintf("%d read-time series for a worker with a fresh read", len(readAt)))
	}
	if v.reported {
		gen := find(ss, gGenerated, w)
		if len(gen) == 1 && gen[0].value < v.minGenerated {
			out = append(out, fmt.Sprintf("%s is %v, below the %v tokens the front saw the Instance generate", gGenerated, gen[0].value, v.minGenerated))
		}
	}
	return out
}

// served tokens is what the front saw an Instance generate: the completion
// tokens of its usage records.
func (p3 *part3) servedTokens(uid string) float64 {
	var n float64
	for _, row := range p3.usageRows() {
		if row.InstanceUID == uid && row.CompletionTokens != nil {
			n += float64(*row.CompletionTokens)
		}
	}
	return n
}

// gauges reads the lifecycle controller's /metrics (base + 12) once the
// primary's generated-token counter has caught up with what the front saw it
// generate, so the read is never before the scenario produced any, and holds
// the primary to a fresh report and a pending successor to stale.
func (p3 *part3) gauges(d *depRun, when string) {
	r := p3.rig
	r.t.Helper()
	url := r.ports.URL(12, "/metrics")
	in, _ := r.instance(d.primary.Name)
	min := p3.servedTokens(string(in.UID))
	if min <= 0 {
		r.t.Fatalf("gauges %s: the front saw Instance %s generate nothing yet: a read now would prove nothing", when, in.Name)
	}
	var ss []sample
	stack.WaitState(r.t, stack.WaitDeadline, "the controller's gauges to show the tokens the primary generated", func() (bool, string) {
		var err error
		ss, err = scrape(url)
		if err != nil {
			return false, err.Error()
		}
		g := find(ss, gGenerated, map[string]string{"instance_uid": string(in.UID), "worker": "worker-0"})
		return len(g) == 1 && g[0].value >= min, fmt.Sprintf("%s: %v, want at least %v", gGenerated, g, min)
	})
	view := gaugeView{uid: string(in.UID), namespace: in.Namespace, name: in.Name, deployment: d.Name, provider: d.Provider,
		gpuModel: "example-gpu", reported: true, minGenerated: min}
	for _, p := range gaugeProblems(ss, view) {
		r.t.Errorf("gauges %s, primary %s: %s", when, in.Name, p)
	}
	var pending []string
	for _, x := range p3.instancesOf(d.Name) {
		if x.Status.Phase != v1alpha1.InstancePending {
			continue
		}
		pending = append(pending, x.Name)
		pv := gaugeView{uid: string(x.UID), namespace: x.Namespace, name: x.Name, deployment: d.Name, provider: d.Provider,
			gpuModel: "example-gpu", staleReason: "stale"}
		problems, none := pendingGaugeProblems(ss, pv)
		for _, p := range problems {
			r.t.Errorf("gauges %s, pending successor %s: %s", when, x.Name, p)
		}
		if none {
			r.t.Logf("FINDING (#249): gauges %s: the pending successor %s, whose agent has not registered, has its info series but no "+
				"hpcburst_instance_metrics_unknown sample for any engine metric (internal/lifecycle/metrics.go:241, \"if !e.hasAgent { continue }\"); "+
				"HBEP-8, Unknown samples, says an Instance with no report yet, new or after a restart, has every engine metric of worker-0 "+
				"unknown with reason stale, while its ruling on which Instances have series names only the workers of the latest report", when, x.Name)
		}
	}
	sort.Strings(pending)
	if !d.oneSlot && len(pending) == 0 {
		r.t.Errorf("gauges %s: the queued path has no pending successor", when)
	}
	r.t.Logf("gauges %s: primary %s fresh (%s at least %v, prefix-cache not_reported); pending %v", when, in.Name, gGenerated, min, pending)
}

// pendingGaugeProblems holds an Instance whose agent has not registered to
// HBEP-8: its info series; no engine series, since nothing was read; and,
// when it has unknown samples, one for each engine metric, each stale. none
// reports that it has no unknown sample at all (a FINDING, not a failure: see
// gauges).
func pendingGaugeProblems(ss []sample, v gaugeView) (problems []string, none bool) {
	for _, p := range gaugeProblems(ss, v) {
		if strings.Contains(p, "hpcburst_instance_info") {
			problems = append(problems, p)
		}
	}
	unknown := 0
	for _, g := range engineGauges {
		for _, n := range seriesNames(g) {
			if s := find(ss, n, map[string]string{"instance_uid": v.uid}); len(s) > 0 {
				problems = append(problems, fmt.Sprintf("%s has %d series, with no read of its engine", n, len(s)))
			}
		}
		unknown += len(find(ss, "hpcburst_instance_metrics_unknown", map[string]string{"instance_uid": v.uid, "metric": g}))
	}
	if unknown == 0 {
		return problems, true
	}
	for _, p := range gaugeProblems(ss, v) {
		if !strings.Contains(p, "hpcburst_instance_info") {
			problems = append(problems, p)
		}
	}
	return problems, false
}

// restartScraper reads the controller's /metrics every 250 ms across step
// (f)'s restart and keeps the primary's samples of each read that answered.
type restartScraper struct {
	stop, done chan struct{}
	once       sync.Once
	reads      [][]sample
}

// startRestartScraper starts the scraper; the test's clean-up stops it, so a
// test that fails part-way reads nothing after it.
func (p3 *part3) startRestartScraper(uid string) *restartScraper {
	s := &restartScraper{stop: make(chan struct{}), done: make(chan struct{})}
	p3.t.Cleanup(s.halt)
	url := p3.ports.URL(12, "/metrics")
	go func() {
		defer close(s.done)
		for {
			if ss, err := scrape(url); err == nil {
				var mine []sample
				for _, x := range ss {
					if x.labels["instance_uid"] == uid {
						mine = append(mine, x)
					}
				}
				s.reads = append(s.reads, mine)
			}
			select {
			case <-s.stop:
				return
			case <-time.After(250 * time.Millisecond):
			}
		}
	}()
	return s
}

// halt stops the scraper; it is safe to call again.
func (s *restartScraper) halt() {
	s.once.Do(func() {
		close(s.stop)
		<-s.done
	})
}

func (s *restartScraper) end() [][]sample {
	s.halt()
	return s.reads
}

// restartReadProblems holds every read of an Instance's gauges across a
// controller restart to HBEP-8: in a read that has the Instance's info series,
// each engine gauge of worker-0 is either known or absent with exactly one
// unknown sample, never both and never neither, whatever the controller knows
// at that read; a gauge the engine does not export is never known. An unknown
// sample carries stale (no report yet since the restart), or not_reported on
// a gauge the engine does not export; any other reason is a problem, and
// agent_unreachable never fits here, since step (f) fails when the agent is
// judged unreachable (HBEP-8, Unknown samples; Test plan). It counts
// the reads that showed the gauges stale (no report yet, as after a restart)
// and fresh, and the reads with no series of the Instance at all (unserved:
// a controller that does not know the Instance yet), which hold no gauge
// sample either.
func restartReadProblems(reads [][]sample, uid string) (problems []string, stale, fresh, unserved int) {
	w := map[string]string{"instance_uid": uid, "worker": "worker-0"}
	for i, ss := range reads {
		if len(find(ss, "hpcburst_instance_info", map[string]string{"instance_uid": uid})) == 0 {
			unserved++
			if len(ss) != 0 {
				problems = append(problems, fmt.Sprintf("read %d: %d samples of the Instance with no info series", i, len(ss)))
			}
			continue
		}
		allStale, allKnown := true, true
		for _, g := range engineGauges {
			var present []sample
			for _, n := range seriesNames(g) {
				present = append(present, find(ss, n, w)...)
			}
			unknown := find(ss, "hpcburst_instance_metrics_unknown", map[string]string{"instance_uid": uid, "worker": "worker-0", "metric": g})
			switch {
			case len(unknown) == 1 && len(present) == 0:
				reason := unknown[0].labels["reason"]
				if reason != "stale" && (reason != "not_reported" || !notReported[g]) {
					want := "stale"
					if notReported[g] {
						want = "stale or not_reported"
					}
					problems = append(problems, fmt.Sprintf("read %d: %s is unknown with reason %q, want %s", i, g, reason, want))
				}
				if reason != "not_reported" || !notReported[g] {
					allKnown = false
				}
				if reason != "stale" {
					allStale = false
				}
			case len(unknown) == 0 && len(present) == len(seriesNames(g)) && !notReported[g]:
				allStale = false
			default:
				problems = append(problems, fmt.Sprintf("read %d: %s has %d series and %d unknown samples", i, g, len(present), len(unknown)))
				allStale, allKnown = false, false
			}
		}
		if allStale {
			stale++
		}
		if allKnown {
			fresh++
		}
	}
	return problems, stale, fresh, unserved
}
