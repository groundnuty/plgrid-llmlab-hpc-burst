package main

import (
	"fmt"
	"io"
	"sort"
	"time"
)

type timing struct {
	ReadyzMS int64   `json:"readyz_ms"` // exec to authenticated /readyz = 200
	ReadyMS  int64   `json:"ready_ms"`  // plus whatever the option needs to be complete (kube-controller-manager healthy)
	UsableMS int64   `json:"usable_ms"` // plus workspace (kcp) and CRDs Established and served
	Load1    float64 `json:"load1_at_start"`
}

type rssSample struct {
	MiB           []float64          `json:"samples_mib"`
	MedianMiB     float64            `json:"median_mib"`
	ByProc        map[string]float64 `json:"by_process_mib"`
	ControllerMiB float64            `json:"controller_mib,omitempty"`
	Load1         []float64          `json:"load1_at_samples"`
	MemAvailGiB   []float64          `json:"mem_available_gib_at_samples"`
}

type upgradeResult struct {
	From, To      string    `json:"-"`
	ServerBefore  string    `json:"server_version_before"`
	ServerAfter   string    `json:"server_version_after"`
	ObjectsBefore int       `json:"objects_before"`
	ObjectsKept   int       `json:"objects_identical_after"`
	Diffs         []string  `json:"differences,omitempty"`
	StopToReadyMS int64     `json:"stop_to_ready_ms"`
	E2EAfter      e2eResult `json:"e2e_after"`
	PostUpgrade   e2eResult `json:"post_upgrade_test"`
	Error         string    `json:"error,omitempty"`
}

type budgetLine struct {
	Line   string `json:"line"`
	Limit  string `json:"limit"`
	Actual string `json:"actual"`
	Pass   bool   `json:"pass"`
}

type result struct {
	Option     string         `json:"option"`
	Old        string         `json:"version_old"`
	New        string         `json:"version_new"`
	Started    string         `json:"started_utc"`
	Cold       []timing       `json:"cold_starts"`
	Warm       []timing       `json:"warm_starts"`
	ColdMedian timing         `json:"cold_median"`
	WarmMedian timing         `json:"warm_median"`
	Idle       *rssSample     `json:"rss_idle,omitempty"`
	Loaded     *rssSample     `json:"rss_loaded,omitempty"`
	Listeners  []string       `json:"listeners"`
	StopMS     int64          `json:"stop_ms"`
	DataDirMiB float64        `json:"data_dir_mib_after_load"`
	E2E        *e2eResult     `json:"e2e,omitempty"`
	Upgrade    *upgradeResult `json:"upgrade,omitempty"`
	Budget     []budgetLine   `json:"budget"`
	Notes      []string       `json:"notes,omitempty"`
}

func newResult(h host) *result {
	o, n := h.versions()
	return &result{Option: h.name(), Old: o, New: n, Started: time.Now().UTC().Format(time.RFC3339)}
}

func median(xs []int64) int64 {
	if len(xs) == 0 {
		return 0
	}
	c := append([]int64(nil), xs...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	return c[len(c)/2]
}

func medianTiming(ts []timing) timing {
	var a, b, c []int64
	for _, t := range ts {
		a, b, c = append(a, t.ReadyzMS), append(b, t.ReadyMS), append(c, t.UsableMS)
	}
	return timing{ReadyzMS: median(a), ReadyMS: median(b), UsableMS: median(c)}
}

func (r *result) finish() {
	r.ColdMedian, r.WarmMedian = medianTiming(r.Cold), medianTiming(r.Warm)
	r.Budget = nil
	add := func(line, limit, actual string, pass bool) {
		r.Budget = append(r.Budget, budgetLine{line, limit, actual, pass})
	}
	if r.Idle != nil {
		add("RSS idle", "<= 600 MiB", fmt.Sprintf("%.0f MiB", r.Idle.MedianMiB), r.Idle.MedianMiB <= 600)
	}
	if r.Loaded != nil {
		add("RSS with ~100 objects", "<= 800 MiB", fmt.Sprintf("%.0f MiB", r.Loaded.MedianMiB), r.Loaded.MedianMiB <= 800)
	}
	if len(r.Cold) > 0 {
		add("cold start to ready", "<= 30 s", fmt.Sprintf("%.1f s (median of %d)", float64(r.ColdMedian.ReadyMS)/1000, len(r.Cold)), r.ColdMedian.ReadyMS <= 30000)
	}
	if len(r.Warm) > 0 {
		add("warm start to ready", "<= 30 s", fmt.Sprintf("%.1f s (median of %d)", float64(r.WarmMedian.ReadyMS)/1000, len(r.Warm)), r.WarmMedian.ReadyMS <= 30000)
	}
	if u := r.Upgrade; u != nil {
		ok := u.Error == "" && u.ObjectsBefore > 0 && u.ObjectsKept == u.ObjectsBefore && len(u.E2EAfter.Failed) == 0 &&
			u.E2EAfter.Exit == 0 && u.PostUpgrade.Exit == 0 && len(u.PostUpgrade.Failed) == 0 && u.StopToReadyMS <= 60000
		add("minor upgrade", "all objects kept, e2e passes, stop to ready <= 60 s",
			fmt.Sprintf("%d/%d kept, e2e failed=%d, post-upgrade failed=%d, stop to ready %.1f s %s", u.ObjectsKept, u.ObjectsBefore,
				len(u.E2EAfter.Failed), len(u.PostUpgrade.Failed), float64(u.StopToReadyMS)/1000, u.Error), ok)
	}
	if r.E2E != nil {
		add("e2e suite", "every test passes", fmt.Sprintf("%d passed, %d failed %v", len(r.E2E.Passed), len(r.E2E.Failed), r.E2E.Failed), len(r.E2E.Failed) == 0 && r.E2E.Exit == 0)
	}
}

func (r *result) print(w io.Writer) {
	fmt.Fprintf(w, "== %s (new %s, upgrade from %s)\n", r.Option, r.New, r.Old)
	for _, b := range r.Budget {
		v := "PASS"
		if !b.Pass {
			v = "FAIL"
		}
		fmt.Fprintf(w, "  %-4s %-24s limit %-48s actual %s\n", v, b.Line, b.Limit, b.Actual)
	}
	fmt.Fprintf(w, "  start medians: cold readyz %d ms ready %d ms usable %d ms; warm readyz %d ms ready %d ms usable %d ms\n",
		r.ColdMedian.ReadyzMS, r.ColdMedian.ReadyMS, r.ColdMedian.UsableMS, r.WarmMedian.ReadyzMS, r.WarmMedian.ReadyMS, r.WarmMedian.UsableMS)
	if r.Idle != nil && r.Loaded != nil {
		fmt.Fprintf(w, "  rss idle %.0f MiB, loaded %.0f MiB (controller %.0f MiB); by process loaded: %v\n", r.Idle.MedianMiB, r.Loaded.MedianMiB, r.Loaded.ControllerMiB, r.Loaded.ByProc)
	}
	for _, n := range r.Notes {
		fmt.Fprintf(w, "  note: %s\n", n)
	}
}
