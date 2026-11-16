package agent_test

// The measurement behind TestAWaveAtAWorkersDeathGetsNoServiceUnavailable (issue #614). The wave test lost 3 of 40 on CI, and its log put
// the three at one moment: that moment is when the test printed them, after the whole wave. What matters is when each request's retry
// went out against when the dead worker left the ring, so this runs the same wave and prints, per wave, one line of JSON ("WAVE {...}"):
// every request's latency and status, each worker's connection failures, and when the ejection first showed on Envoy's /clusters, read
// every millisecond.
//
// It runs only with HPCBURST_WAVE_MEASURE=1 and asserts nothing: it is a tool, off by default, for go test -count=N beside a load
// generator (internal/agent's reproduction of #614 pinned the process to 2 CPUs and ran 2 spinners). HPCBURST_WAVE_WORKERS sets the
// Instance's workers (default 2, up to 8); worker-1 is killed, as in the wave test. Where the admin allows /stats (the shipped bootstrap
// allows /clusters alone, so only a diagnostic build does), each line also carries the cluster's retry, connect-failure and ejection
// counters for the wave.

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// waveSample is one read of Envoy's /clusters during a wave, kept when something changed.
type waveSample struct {
	AtMS      float64 `json:"t"`
	DeadFails int     `json:"w1f"`
	LiveFails int     `json:"lf"`
	Ejected   bool    `json:"ej,omitempty"`
}

// clusterStats reads the cluster's retry, connect and ejection counters from Envoy's /stats, keyed by the name after the cluster's, when the
// admin allows that path: a diagnostic build does, and the shipped bootstrap allows /clusters alone, which reads as nil.
func clusterStats(runDir string) map[string]int {
	c := unixClient(filepath.Join(runDir, agent.EnvoyAdminSocket))
	defer c.Transport.(*http.Transport).CloseIdleConnections()
	filter := `cluster\..*\.(upstream_rq_retry.*|upstream_cx_connect_fail|upstream_rq_timeout|upstream_rq_per_try_timeout|outlier_detection\..*)$`
	resp, err := c.Get("http://admin/stats?format=json&filter=" + url.QueryEscape(filter))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var doc struct {
		Stats []struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		} `json:"stats"`
	}
	if json.NewDecoder(resp.Body).Decode(&doc) != nil {
		return nil
	}
	name := regexp.MustCompile(`^cluster\.[^.]+\.`)
	out := map[string]int{}
	for _, st := range doc.Stats {
		out[name.ReplaceAllString(st.Name, "")] = st.Value
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// waveWorkers is the Instance's size for the measurement: HPCBURST_WAVE_WORKERS, 2 by default.
func waveWorkers(t *testing.T) int {
	t.Helper()
	v := os.Getenv("HPCBURST_WAVE_WORKERS")
	if v == "" {
		return 2
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 2 || n > 8 {
		t.Fatalf("HPCBURST_WAVE_WORKERS=%q, want a number from 2 to 8", v)
	}
	return n
}

func TestWaveMeasure(t *testing.T) {
	if os.Getenv("HPCBURST_WAVE_MEASURE") != "1" {
		t.Skip("a measurement, not a check: set HPCBURST_WAVE_MEASURE=1 (see this file's header)")
	}
	workers := waveWorkers(t)
	var devices, names []string
	holding := map[string]bool{}
	for i := 0; i < workers; i++ {
		devices = append(devices, strconv.Itoa(i))
		names = append(names, "worker-"+strconv.Itoa(i))
		holding["worker-"+strconv.Itoa(i)] = true
	}
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t), baseEnv: []string{"CUDA_VISIBLE_DEVICES=" + strings.Join(devices, ",")}})
	d := workersRouterState(h, workers)
	d.Instance.Launch.Env = append(d.Instance.Launch.Env, agentapi.EnvVar{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=50ms"})
	h.set(d)
	r := h.waitReport("Envoy holding every worker healthy", time.Minute, routerHolds(holding))
	key := r.Instance.Key
	if key == "" {
		t.Fatal("the report carries no Instance key")
	}
	before := hostStats(t, h.runDir)
	statsBefore := clusterStats(h.runDir)
	killAt := killInWindow(t, h, &d, 1)
	liveFails := func(st map[string]map[string]int) int {
		n := 0
		for _, w := range names {
			if w != "worker-1" {
				n += st[w]["cx_connect_fail"] - before[w]["cx_connect_fail"]
			}
		}
		return n
	}

	const n = 40
	codes, bodies, lat := make([]int, n), make([]string, n), make([]float64, n)
	var start, wg, pollWG sync.WaitGroup
	start.Add(1)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Wait()
			begun := time.Now()
			codes[i], bodies[i] = chatIn(h, key, "wave-"+strconv.Itoa(i), 20)
			lat[i] = float64(time.Since(begun).Microseconds()) / 1000
		}()
	}
	t0 := time.Now() // set before the poller starts, and not again
	done := make(chan struct{})
	var samples []waveSample
	pollWG.Add(1)
	go func() {
		defer pollWG.Done()
		var last waveSample
		for {
			select {
			case <-done:
				return
			default:
			}
			st, err := hostStatsErr(h.runDir)
			if err != nil {
				continue
			}
			s := waveSample{
				AtMS:      float64(time.Since(t0).Microseconds()) / 1000,
				DeadFails: st["worker-1"]["cx_connect_fail"] - before["worker-1"]["cx_connect_fail"],
				LiveFails: liveFails(st),
				Ejected:   st["worker-1"]["health:failed_outlier_check"] == 1,
			}
			// Only the changes: the first read, a failure counted, the ejection seen.
			if len(samples) == 0 || s.DeadFails != last.DeadFails || s.LiveFails != last.LiveFails || s.Ejected != last.Ejected {
				samples = append(samples, s)
			}
			last = s
			time.Sleep(time.Millisecond)
		}
	}()
	killToWave := t0.Sub(killAt)
	start.Done()
	wg.Wait()
	waveMS := float64(time.Since(t0).Microseconds()) / 1000
	close(done)
	pollWG.Wait()
	after := hostStats(t, h.runDir)
	statsDelta := map[string]int{}
	if statsAfter := clusterStats(h.runDir); statsAfter != nil && statsBefore != nil {
		for k, v := range statsAfter {
			if v != statsBefore[k] {
				statsDelta[k] = v - statsBefore[k]
			}
		}
	}

	var okMS, badMS []float64
	var badWhat []string
	for i := range codes {
		if codes[i] == http.StatusOK {
			okMS = append(okMS, lat[i])
		} else {
			badMS = append(badMS, lat[i])
			badWhat = append(badWhat, strconv.Itoa(codes[i])+" "+truncate(bodies[i], 140))
		}
	}
	sort.Float64s(okMS)
	sort.Float64s(badMS)
	fails, requests := map[string]int{}, map[string]int{}
	for _, w := range names {
		fails[w] = after[w]["cx_connect_fail"] - before[w]["cx_connect_fail"]
		requests[w] = after[w]["rq_total"] - before[w]["rq_total"]
	}
	line, _ := json.Marshal(map[string]any{
		"workers": workers, "lost": len(badMS), "bad_ms": badMS, "bad_what": badWhat, "ok_ms": okMS, "wave_ms": waveMS,
		"fails": fails, "requests": requests,
		"w1_fails": fails["worker-1"], "w0_fails": fails["worker-0"], "live_fails": liveFails(after),
		"ejected": after["worker-1"]["health:failed_outlier_check"] == 1, "kill_to_wave_ms": killToWave.Milliseconds(),
		"timeline": samples, "stats_delta": statsDelta,
	})
	t.Logf("WAVE %s", line)
}
