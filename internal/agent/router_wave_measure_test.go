package agent_test

// The measurement behind TestAWaveAtAWorkersDeathGetsNoServiceUnavailable's allowance (issue #614). The wave test lost 3 of 40 on CI, and
// its log put the three at the same moment: that moment is when the test printed them, after the whole wave. What matters is when each
// request's retry went out against when the dead worker left the ring, so this runs the same wave and prints, per wave, one line of
// what was seen: every request's latency and status, the connection failures of both workers, and when the ejection first showed on
// Envoy's /clusters. It runs only with HPCBURST_WAVE_MEASURE=1 (go test -count=N, beside a load generator), and asserts nothing: the
// lines are the output, read by scripts/wave-measure.py of the reviews directory or by eye.

import (
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// waveSample is one read of Envoy's /clusters during a wave.
type waveSample struct {
	AtMS      float64 `json:"t"`
	W1Fails   int     `json:"w1f"`
	W0Fails   int     `json:"w0f"`
	W1Ejected bool    `json:"ej,omitempty"`
}

func TestWaveMeasure(t *testing.T) {
	if os.Getenv("HPCBURST_WAVE_MEASURE") != "1" {
		t.Skip("a measurement, not a check: set HPCBURST_WAVE_MEASURE=1 (see this file's header)")
	}
	h := newHarness(t, harnessOptions{envoy: testenv.EnvoyBinary(t), baseEnv: []string{"CUDA_VISIBLE_DEVICES=0,1"}})
	d := workersRouterState(h, 2)
	d.Instance.Launch.Env = append(d.Instance.Launch.Env, agentapi.EnvVar{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=50ms"})
	h.set(d)
	r := h.waitReport("Envoy holding both workers healthy", time.Minute, routerHolds(map[string]bool{"worker-0": true, "worker-1": true}))
	key := r.Instance.Key
	if key == "" {
		t.Fatal("the report carries no Instance key")
	}
	before := hostStats(t, h.runDir)
	killAt := killInWindow(t, h, &d, 1)

	const n = 40
	codes, bodies, lat := make([]int, n), make([]string, n), make([]float64, n)
	var start, wg sync.WaitGroup
	start.Add(1)
	t0 := time.Now()
	done := make(chan struct{})
	var samples []waveSample
	var pollWG sync.WaitGroup
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
				W1Fails:   st["worker-1"]["cx_connect_fail"] - before["worker-1"]["cx_connect_fail"],
				W0Fails:   st["worker-0"]["cx_connect_fail"] - before["worker-0"]["cx_connect_fail"],
				W1Ejected: st["worker-1"]["health:failed_outlier_check"] == 1,
			}
			// Only the changes: the first read, a failure counted, the ejection seen.
			if len(samples) == 0 || s.W1Fails != last.W1Fails || s.W0Fails != last.W0Fails || s.W1Ejected != last.W1Ejected {
				samples = append(samples, s)
			}
			last = s
			time.Sleep(time.Millisecond)
		}
	}()
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
	t0 = time.Now()
	killToWave := t0.Sub(killAt)
	start.Done()
	wg.Wait()
	waveMS := float64(time.Since(t0).Microseconds()) / 1000
	close(done)
	pollWG.Wait()
	after := hostStats(t, h.runDir)

	var okMS, badMS []float64
	for i := range codes {
		if codes[i] == http.StatusOK {
			okMS = append(okMS, lat[i])
		} else {
			badMS = append(badMS, lat[i])
		}
	}
	sort.Float64s(okMS)
	sort.Float64s(badMS)
	line, _ := json.Marshal(map[string]any{
		"lost": len(badMS), "bad_ms": badMS, "ok_ms": okMS, "wave_ms": waveMS,
		"w1_fails": after["worker-1"]["cx_connect_fail"] - before["worker-1"]["cx_connect_fail"],
		"w0_fails": after["worker-0"]["cx_connect_fail"] - before["worker-0"]["cx_connect_fail"],
		"w0_rq":    after["worker-0"]["rq_total"] - before["worker-0"]["rq_total"],
		"w1_rq":    after["worker-1"]["rq_total"] - before["worker-1"]["rq_total"],
		"ejected":  after["worker-1"]["health:failed_outlier_check"] == 1, "kill_to_wave_ms": killToWave.Milliseconds(),
		"timeline": samples,
	})
	t.Logf("WAVE %s", line)
}
