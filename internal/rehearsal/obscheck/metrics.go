package obscheck

// HBEP-8's metrics: a parser of Prometheus's text format, and the engine gauges per Instance with unknowns shown as
// unknown (Metrics, Engine gauges per Instance; Unknown samples). The metric names and labels are HBEP-8's, written
// here, not read from the code.

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Sample is one line of Prometheus's text format.
type Sample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// ParseExposition reads Prometheus's text format: comments are skipped, and
// each sample line is name{label="value",...} value.
func ParseExposition(text string) ([]Sample, error) {
	var out []Sample
	sc := bufio.NewScanner(strings.NewReader(text))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		s := Sample{Labels: map[string]string{}}
		rest := l
		if i := strings.IndexAny(l, "{ "); i >= 0 && l[i] == '{' {
			s.Name = l[:i]
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
				s.Labels[key] = v.String()
				rest = strings.TrimPrefix(rest, ",")
			}
		} else if i >= 0 {
			s.Name, rest = l[:i], l[i:]
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
		s.Value = v
		out = append(out, s)
	}
	return out, sc.Err()
}

// Scrape reads and parses one /metrics; ok is false when it could not.
func Scrape(url string) ([]Sample, error) {
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
	return ParseExposition(string(b))
}

// Find returns the samples named name whose labels include want.
func Find(ss []Sample, name string, want map[string]string) []Sample {
	var out []Sample
	for _, s := range ss {
		if s.Name != name {
			continue
		}
		ok := true
		for k, v := range want {
			if s.Labels[k] != v {
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

// The engine gauges of HBEP-8's table (Metrics, Engine gauges per Instance):
// the metric label of an unknown sample names them (Unknown samples).
const (
	GaugeRunning   = "hpcburst_instance_running_requests"
	GaugeWaiting   = "hpcburst_instance_waiting_requests"
	GaugeTPOT      = "hpcburst_instance_time_per_output_token_seconds"
	GaugePreempt   = "hpcburst_instance_preemptions_total"
	GaugePrefixQ   = "hpcburst_instance_prefix_cache_queried_tokens_total"
	GaugePrefixHit = "hpcburst_instance_prefix_cache_hit_tokens_total"
	GaugeKV        = "hpcburst_instance_kv_cache_usage_ratio"
	GaugeGenerated = "hpcburst_instance_generation_tokens_total"
)

var EngineGauges = []string{GaugeRunning, GaugeWaiting, GaugeTPOT, GaugePreempt, GaugePrefixQ, GaugePrefixHit, GaugeKV, GaugeGenerated}

// notReported are the gauges the served engines do not export: the engine
// double's no-prefix-cache-metrics mode (part3EngineModes).
var notReported = map[string]bool{GaugePrefixQ: true, GaugePrefixHit: true}

// seriesNames are a gauge's series in the exposition: a summary's _sum and
// _count (HBEP-8: no quantiles), else its own name.
func seriesNames(g string) []string {
	if g == GaugeTPOT {
		return []string{g + "_sum", g + "_count"}
	}
	return []string{g}
}

// GaugeView is what the gauge check expects of one Instance.
type GaugeView struct {
	UID, Namespace, Name, Deployment, Provider, GPUModel string
	// reported: the agent has reported fresh metrics, so each gauge is
	// either present or unknown not_reported; otherwise each is unknown
	// with staleReason.
	Reported    bool
	StaleReason string
	// minGenerated is a lower bound on generation_tokens_total: the tokens
	// the front saw the Instance generate.
	MinGenerated float64
}

// GaugeProblems holds one Instance's series to HBEP-8 (Unknown samples;
// Which Instances have series): its info series with its descriptive labels;
// for worker-0, each engine gauge either present, with the instance_uid,
// worker, generation and worker_generation labels, or absent with one
// hpcburst_instance_metrics_unknown naming it and its reason, never both and
// never neither; not_reported for the gauges the engine does not export,
// stale (or another given reason) for an Instance with no report; the read
// time while it has one; and generation_tokens_total no lower than what was
// generated.
func GaugeProblems(ss []Sample, v GaugeView) []string {
	var out []string
	info := Find(ss, "hpcburst_instance_info", map[string]string{"instance_uid": v.UID})
	if len(info) != 1 || info[0].Value != 1 {
		out = append(out, fmt.Sprintf("%d hpcburst_instance_info series for %s, want one at 1", len(info), v.UID))
	} else {
		for k, want := range map[string]string{"namespace": v.Namespace, "instance": v.Name, "model_deployment": v.Deployment, "provider": v.Provider, "gpu_model": v.GPUModel} {
			if got := info[0].Labels[k]; got != want {
				out = append(out, fmt.Sprintf("hpcburst_instance_info's %s is %q, want %q", k, got, want))
			}
		}
	}
	w := map[string]string{"instance_uid": v.UID, "worker": "worker-0"}
	for _, g := range EngineGauges {
		var present []Sample
		for _, n := range seriesNames(g) {
			present = append(present, Find(ss, n, w)...)
		}
		unknown := Find(ss, "hpcburst_instance_metrics_unknown", map[string]string{"instance_uid": v.UID, "worker": "worker-0", "metric": g})
		wantReason := ""
		switch {
		case !v.Reported:
			wantReason = v.StaleReason
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
					if s.Labels[k] == "" {
						out = append(out, fmt.Sprintf("%s has no %s label: %v", s.Name, k, s.Labels))
					}
				}
			}
			continue
		}
		if len(present) != 0 || len(unknown) != 1 || unknown[0].Labels["reason"] != wantReason || unknown[0].Value != 1 {
			reasons := []string{}
			for _, u := range unknown {
				reasons = append(reasons, u.Labels["reason"])
			}
			out = append(out, fmt.Sprintf("%s: %d series and unknown samples with reasons %v, want it absent and unknown %s", g, len(present), reasons, wantReason))
		}
	}
	if readAt := Find(ss, "hpcburst_instance_metrics_read_timestamp_seconds", w); v.Reported && len(readAt) != 1 {
		out = append(out, fmt.Sprintf("%d read-time series for a worker with a fresh read", len(readAt)))
	}
	if v.Reported {
		gen := Find(ss, GaugeGenerated, w)
		if len(gen) == 1 && gen[0].Value < v.MinGenerated {
			out = append(out, fmt.Sprintf("%s is %v, below the %v tokens the front saw the Instance generate", GaugeGenerated, gen[0].Value, v.MinGenerated))
		}
	}
	return out
}

// PendingGaugeProblems holds an Instance whose agent has not registered to
// HBEP-8: its info series; no engine series, since nothing was read; and,
// when it has unknown samples, one for each engine metric, each stale. none
// reports that it has no unknown sample at all (a FINDING, not a failure: see
// gauges).
func PendingGaugeProblems(ss []Sample, v GaugeView) (problems []string, none bool) {
	for _, p := range GaugeProblems(ss, v) {
		if strings.Contains(p, "hpcburst_instance_info") {
			problems = append(problems, p)
		}
	}
	unknown := 0
	for _, g := range EngineGauges {
		for _, n := range seriesNames(g) {
			if s := Find(ss, n, map[string]string{"instance_uid": v.UID}); len(s) > 0 {
				problems = append(problems, fmt.Sprintf("%s has %d series, with no read of its engine", n, len(s)))
			}
		}
		unknown += len(Find(ss, "hpcburst_instance_metrics_unknown", map[string]string{"instance_uid": v.UID, "metric": g}))
	}
	if unknown == 0 {
		return problems, true
	}
	for _, p := range GaugeProblems(ss, v) {
		if !strings.Contains(p, "hpcburst_instance_info") {
			problems = append(problems, p)
		}
	}
	return problems, false
}

// RestartReadProblems holds every read of an Instance's gauges across a
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
func RestartReadProblems(reads [][]Sample, uid string) (problems []string, stale, fresh, unserved int) {
	w := map[string]string{"instance_uid": uid, "worker": "worker-0"}
	for i, ss := range reads {
		if len(Find(ss, "hpcburst_instance_info", map[string]string{"instance_uid": uid})) == 0 {
			unserved++
			if len(ss) != 0 {
				problems = append(problems, fmt.Sprintf("read %d: %d samples of the Instance with no info series", i, len(ss)))
			}
			continue
		}
		allStale, allKnown := true, true
		for _, g := range EngineGauges {
			var present []Sample
			for _, n := range seriesNames(g) {
				present = append(present, Find(ss, n, w)...)
			}
			unknown := Find(ss, "hpcburst_instance_metrics_unknown", map[string]string{"instance_uid": uid, "worker": "worker-0", "metric": g})
			switch {
			case len(unknown) == 1 && len(present) == 0:
				reason := unknown[0].Labels["reason"]
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
