package scenario_test

// Item 2 of issue #193, HBEP-8's metrics: the front's metrics for both kinds
// of target, and the lifecycle controller's engine gauges per Instance, with
// unknowns shown as unknown. The metric names and labels are HBEP-8's
// (Metrics), written here, not read from the code.

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/obscheck"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

func sum(ss []obscheck.Sample) float64 {
	var t float64
	for _, s := range ss {
		t += s.Value
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
	before, err := obscheck.Scrape(url)
	if err != nil {
		r.t.Fatalf("the front's /metrics: %v", err)
	}
	type target struct {
		kind, model string
		want        wantAttr
		rows        []stack.UsageRow
	}
	targets := []*target{
		{kind: "the Instance", model: d.Model, want: wantInstance(d, d.primary)},
		{kind: "the managed target", model: managedModel, want: p3.wantManaged()},
	}
	for _, tg := range targets {
		rp := r.fc.SendCtx(context.Background(), "/v1/chat/completions", stack.ChatBody(tg.model, 16, 0, false, ""), "")
		if rp.Err != nil || rp.Status != http.StatusOK {
			r.t.Fatalf("a chat request to %s: %d %v: %.300s", tg.kind, rp.Status, rp.Err, rp.Body)
		}
		row := r.fc.RowOf(rp.ID)
		p3.attribute("a chat request to "+tg.kind, rp, row, tg.want)
		first, done := r.fc.StartStream(stack.ChatBody(tg.model, 16, 0, true, ""), "rehearsal-part3-metrics")
		<-first
		s := <-done
		if s.Err != nil || s.Status != http.StatusOK {
			r.t.Fatalf("a streamed chat request to %s: %d %v: %.300s", tg.kind, s.Status, s.Err, s.Body)
		}
		if fin, ok := stack.SSEDone(s.Body); !fin || !ok {
			r.t.Errorf("the streamed chat request to %s: [DONE] %v, a finish reason %v", tg.kind, fin, ok)
		}
		srow := r.fc.RowOf(s.ID)
		p3.attribute("a streamed chat request to "+tg.kind, s.Reply, srow, tg.want)
		tg.rows = []stack.UsageRow{row, srow}
	}
	after, err := obscheck.Scrape(url)
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
		if got := sum(obscheck.Find(after, "hpcburst_front_requests_total", labels)) - sum(obscheck.Find(before, "hpcburst_front_requests_total", labels)); got != 2 {
			r.t.Errorf("%s: hpcburst_front_requests_total%v rose by %v across its 2 requests", tg.kind, labels, got)
		}
		firsts := 0
		for _, row := range tg.rows {
			if row.FirstTokenMs != nil {
				firsts++
			}
		}
		tl := map[string]string{"provider": prov, "model_deployment": dep, "endpoint": "chat"}
		got := sum(obscheck.Find(after, "hpcburst_front_time_to_first_token_seconds_count", tl)) - sum(obscheck.Find(before, "hpcburst_front_time_to_first_token_seconds_count", tl))
		if firsts == 0 || got != float64(firsts) {
			r.t.Errorf("%s: hpcburst_front_time_to_first_token_seconds_count%v rose by %v; %d of its records have a first token", tg.kind, tl, got, firsts)
		}
		for _, c := range frontCuts {
			if len(obscheck.Find(after, "hpcburst_front_cuts_total", map[string]string{"cut_by": c, "provider": prov, "model_deployment": dep})) != 1 {
				r.t.Errorf("%s: no hpcburst_front_cuts_total series for cut_by %s: a count of the front's own events starts at 0 (HBEP-8)", tg.kind, c)
			}
		}
		if len(obscheck.Find(after, "hpcburst_front_pauses_total", map[string]string{"provider": prov, "model_deployment": dep})) != 1 {
			r.t.Errorf("%s: no hpcburst_front_pauses_total series", tg.kind)
		}
		r.t.Logf("front metrics, %s: provider %s, model_deployment %s: requests +2, time to first token +%d", tg.kind, prov, dep, firsts)
	}
	recs := len(r.fc.UsageRows())
	if got := sum(obscheck.Find(after, "hpcburst_usage_records_total", nil)); got < float64(recs) {
		r.t.Errorf("hpcburst_usage_records_total is %v, below the %d records in the usage files", got, recs)
	}
	if got := obscheck.Find(after, "hpcburst_usage_write_failures_total", nil); len(got) != 1 || got[0].Value != 0 {
		r.t.Errorf("hpcburst_usage_write_failures_total: %v, want one series at 0", got)
	}
	if got := obscheck.Find(after, "hpcburst_usage_dir_free_bytes", nil); len(got) != 1 || got[0].Value <= 0 {
		r.t.Errorf("hpcburst_usage_dir_free_bytes: %v, want one positive series", got)
	}
}

// served tokens is what the front saw an Instance generate: the completion
// tokens of its usage records.
func (p3 *part3) servedTokens(uid string) float64 {
	var n float64
	for _, row := range p3.fc.UsageRows() {
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
	var ss []obscheck.Sample
	stack.WaitState(r.t, stack.WaitDeadline, "the controller's gauges to show the tokens the primary generated", func() (bool, string) {
		var err error
		ss, err = obscheck.Scrape(url)
		if err != nil {
			return false, err.Error()
		}
		g := obscheck.Find(ss, obscheck.GaugeGenerated, map[string]string{"instance_uid": string(in.UID), "worker": "worker-0"})
		return len(g) == 1 && g[0].Value >= min, fmt.Sprintf("%s: %v, want at least %v", obscheck.GaugeGenerated, g, min)
	})
	view := obscheck.GaugeView{UID: string(in.UID), Namespace: in.Namespace, Name: in.Name, Deployment: d.Name, Provider: d.Provider,
		GPUModel: "example-gpu", Reported: true, MinGenerated: min, NotReported: obscheck.DoubleNotReported}
	for _, p := range obscheck.GaugeProblems(ss, view) {
		r.t.Errorf("gauges %s, primary %s: %s", when, in.Name, p)
	}
	var pending []string
	for _, x := range p3.instancesOf(d.Name) {
		if x.Status.Phase != v1alpha1.InstancePending {
			continue
		}
		pending = append(pending, x.Name)
		pv := obscheck.GaugeView{UID: string(x.UID), Namespace: x.Namespace, Name: x.Name, Deployment: d.Name, Provider: d.Provider,
			GPUModel: "example-gpu", StaleReason: "stale", NotReported: obscheck.DoubleNotReported}
		problems, none := obscheck.PendingGaugeProblems(ss, pv)
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
	r.t.Logf("gauges %s: primary %s fresh (%s at least %v, prefix-cache not_reported); pending %v", when, in.Name, obscheck.GaugeGenerated, min, pending)
}

// restartScraper reads the controller's /metrics every 250 ms across step
// (f)'s restart and keeps the primary's samples of each read that answered.
type restartScraper struct {
	stop, done chan struct{}
	once       sync.Once
	reads      [][]obscheck.Sample
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
			if ss, err := obscheck.Scrape(url); err == nil {
				var mine []obscheck.Sample
				for _, x := range ss {
					if x.Labels["instance_uid"] == uid {
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

func (s *restartScraper) end() [][]obscheck.Sample {
	s.halt()
	return s.reads
}
