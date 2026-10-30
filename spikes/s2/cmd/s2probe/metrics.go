package main

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Simulator metric names (llm-d-inference-sim v1.0.0, pkg/engine/vllm/metrics.go).
const (
	metricSuccess = "vllm:request_success_total"
	metricRunning = "vllm:num_requests_running"
)

// scrape returns the value of a metric summed over its label sets.
func scrape(metricsURL, name string) (float64, error) {
	c := http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get(metricsURL)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var sum float64
	found := false
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, name) {
			continue
		}
		rest := line[len(name):]
		if len(rest) == 0 || (rest[0] != '{' && rest[0] != ' ') {
			continue
		}
		f := strings.Fields(line)
		v, perr := strconv.ParseFloat(f[len(f)-1], 64)
		if perr != nil {
			continue
		}
		sum += v
		found = true
	}
	if !found {
		// A counter series does not exist until its first increment. Accept absence as zero only
		// from a page that is plainly a simulator's metrics, not from a wrong or dead endpoint.
		if !strings.Contains(string(b), "vllm:num_requests_running") {
			return 0, fmt.Errorf("metric %s not found, and %s does not look like a simulator's /metrics", name, metricsURL)
		}
		return 0, nil
	}
	return sum, nil
}

func scrapeAll(urls []string, name string) ([]float64, error) {
	out := make([]float64, len(urls))
	for i, u := range urls {
		v, err := scrape(u, name)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}
