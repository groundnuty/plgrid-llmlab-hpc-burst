package rig

import (
	"sort"
	"time"
)

// Cell is one measured run's configuration, recorded with its results.
type Cell struct {
	Name          string        `json:"name"`
	Arm           string        `json:"arm"` // a, b, c, d
	Burst         float64       `json:"burst,omitempty"`
	Refill        float64       `json:"refill,omitempty"`
	Fault         string        `json:"fault"`
	Rate          float64       `json:"rate,omitempty"` // the share of requests that carry the fault
	Stream        bool          `json:"stream"`
	RPS           float64       `json:"rps"`
	Duration      time.Duration `json:"duration_ns"`
	Trials        int           `json:"trials,omitempty"` // F4: poison requests
	ClientRetries bool          `json:"client_retries"`
	N             int           `json:"n"`
	D1            time.Duration `json:"d1_ns"`
	D2            time.Duration `json:"d2_ns"`
	TokenDelay    string        `json:"token_delay"`
	Seed          int64         `json:"seed"`
}

// Windows are the controller-side crash-correlation candidate's windows the analysis scores.
var Windows = []time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute}

// Summary is a cell's measures.
type Summary struct {
	Cell Cell `json:"cell"`

	Requests, Marked, Fired int // requests (bases); those carrying the fault; those whose fault fired
	Attempts                int // the client's attempts (front requests)

	// Shares of requests whose last attempt was a whole 200: all, the marked, those whose fault fired
	// (a marked request whose target was out of routing goes to another Instance and never fires), and
	// the unmarked.
	Success, SuccessMarked, SuccessFired, SuccessUnmarked float64

	Duplicates   int // front requests an engine began more than once (a front-side resend after a write)
	ReExecutions int // requests whose client attempts were begun more than once (a client retry)

	Deaths, Restarts int
	// TTFB, of whole 200s, in ms: p50 and p95 of the requests whose fault fired, and of the unmarked.
	TTFBFiredP50, TTFBFiredP95, TTFBUnmarkedP50, TTFBUnmarkedP95 float64

	// Candidate (i), front-side: front requests with two or more closed attempts (the layer's), and of
	// them those with no engine dead under them (a flapping tunnel).
	CorrFront, CorrFrontFalse int
	// Candidate (ii), controller-side: deaths that came within the window after a death of ANOTHER
	// Instance of the deployment, per window.
	CorrController map[string]int
	// Candidate (iii), derived, front-side by request content: requests (one content, across the client's
	// retries; the base ID stands in for a body hash) with two or more written-then-closed failures, as
	// the front sees them, and of them those with no engine dead under them. All of a request's attempts
	// come within seconds (the SDK's backoff is 0.5 s and 1 s), so no window is applied.
	CorrContent, CorrContentFalse int
	// FalseDeaths: closed attempts whose engine stayed alive (to the front, a death). FalseStops: requests
	// arm (d)'s two-deaths stop gave up with no engine dead under them.
	FalseDeaths, FalseStops int

	// F4: per poison request, the engines that died under it, and whether each candidate fired.
	Poison []PoisonTrial `json:"poison,omitempty"`
}

// PoisonTrial is one poison request's cost.
type PoisonTrial struct {
	Base           string `json:"base"`
	Attempts       int    `json:"attempts"`
	Deaths         int    `json:"deaths"`
	Final          int    `json:"final_status"`
	CorrFront      bool   `json:"corr_front"`
	CorrController bool   `json:"corr_controller"` // deaths of two or more distinct Instances under it
	CorrContent    bool   `json:"corr_content"`
}

// Analyze computes a cell's summary from the client's attempts, the engines' records, the process ends
// and the layer's attempts (none under arm a).
func Analyze(cell Cell, results []Result, begins map[string][]Begin, exits []Exit, attempts []Attempt, restarts int) Summary {
	s := Summary{Cell: cell, Attempts: len(results), Restarts: restarts, CorrController: map[string]int{}}
	bases := map[string][]Result{}
	var order []string
	for _, r := range results {
		if _, ok := bases[r.Base]; !ok {
			order = append(order, r.Base)
		}
		bases[r.Base] = append(bases[r.Base], r)
	}
	var deaths []Exit
	for _, e := range exits {
		if !e.Stopped {
			deaths = append(deaths, e)
		}
	}
	s.Deaths = len(deaths)
	closedByID := map[string]int{}
	for _, a := range attempts {
		if a.Kind == "closed" {
			closedByID[a.ID]++
			if !diedUnder(begins[a.ID], a.Target) {
				s.FalseDeaths++
			}
		}
	}
	var ttfbFired, ttfbUnmarked []float64
	okAll, okMarked, okFired, okUnmarked := 0, 0, 0, 0
	for _, base := range order {
		rs := bases[base]
		s.Requests++
		marked := rs[0].Marker != ""
		fired := false
		begun := 0
		for _, r := range rs {
			bs := begins[r.ID]
			if Admissions(bs) > 1 {
				s.Duplicates++
			}
			if Admissions(bs) > 0 {
				begun++
			}
			if FaultFired(bs) {
				fired = true
			}
			if closedByID[r.ID] >= 2 {
				s.CorrFront++
				if !anyDeathUnder(bs) {
					s.CorrFrontFalse++
				}
			}
		}
		if n, died := contentClosed(rs, begins, closedByID, len(attempts) > 0); n >= 2 {
			s.CorrContent++
			if !died {
				s.CorrContentFalse++
			}
		}
		if begun > 1 {
			s.ReExecutions++
		}
		last := rs[len(rs)-1]
		whole := last.Status == 200 && last.Complete
		if marked {
			s.Marked++
			if fired {
				s.Fired++
			}
			if whole {
				okMarked++
				if fired {
					okFired++
				}
			}
		} else if whole {
			okUnmarked++
		}
		if whole {
			okAll++
			ms := float64(last.FirstByte) / float64(time.Millisecond)
			switch {
			case fired:
				ttfbFired = append(ttfbFired, ms)
			case !marked:
				ttfbUnmarked = append(ttfbUnmarked, ms)
			}
		}
		if cell.Arm == "d" && !whole && closedByID[last.ID] >= 2 && !anyDeathUnder(begins[last.ID]) {
			s.FalseStops++
		}
	}
	s.Success = share(okAll, s.Requests)
	s.SuccessMarked = share(okMarked, s.Marked)
	s.SuccessFired = share(okFired, s.Fired)
	s.SuccessUnmarked = share(okUnmarked, s.Requests-s.Marked)
	s.TTFBFiredP50, s.TTFBFiredP95 = pct(ttfbFired, 50), pct(ttfbFired, 95)
	s.TTFBUnmarkedP50, s.TTFBUnmarkedP95 = pct(ttfbUnmarked, 50), pct(ttfbUnmarked, 95)
	for _, w := range Windows {
		s.CorrController[w.String()] = correlated(deaths, w)
	}
	return s
}

// FaultFired reports whether an attempt's engine records show its fault: begun and never ended (the
// engine died under it), refused (F2), or cancelled (F3's cut).
func FaultFired(bs []Begin) bool {
	for _, b := range bs {
		if (b.Admitted && b.Outcome == "") || b.Outcome == "refused" || b.Outcome == "cancelled" {
			return true
		}
	}
	return false
}

func anyDeathUnder(bs []Begin) bool {
	for _, b := range bs {
		if b.Admitted && b.Outcome == "" {
			return true
		}
	}
	return false
}

// diedUnder reports whether Instance i's engine died under the request: its record shows it admitted
// and never ended. A closed attempt whose engine recorded an end lived through it (F3's cut): to the
// front a death, a false one. Judged from the engine's records of that request, never a time window,
// which another request's death nearby would fool.
func diedUnder(bs []Begin, i int) bool {
	for _, b := range bs {
		if b.Instance == i && b.Admitted && b.Outcome == "" {
			return true
		}
	}
	return false
}

// correlated counts the deaths that came within w after a death of ANOTHER Instance of the deployment:
// each one the controller-side candidate would raise its event on. One Instance dying again (its own
// crash loop) is the restart ladder's, not a correlation across Instances.
func correlated(deaths []Exit, w time.Duration) int {
	ds := append([]Exit(nil), deaths...)
	sort.Slice(ds, func(a, b int) bool { return ds[a].At.Before(ds[b].At) })
	n := 0
	for k := 1; k < len(ds); k++ {
		for j := k - 1; j >= 0 && ds[k].At.Sub(ds[j].At) <= w; j-- {
			if ds[j].Instance != ds[k].Instance {
				n++
				break
			}
		}
	}
	return n
}

// contentClosed is how many written-then-closed failures the front saw for one request content (its
// attempts rs, across the client's retries), and whether an engine died under any of them: the layer's
// closed attempts when the arm has a layer (layered), or else the front's own 502 upstream_closed answers.
func contentClosed(rs []Result, begins map[string][]Begin, closedByID map[string]int, layered bool) (int, bool) {
	n, died := 0, false
	for _, r := range rs {
		if layered {
			n += closedByID[r.ID]
		} else if r.Status == 502 && r.Code == "upstream_closed" {
			n++
		}
		if anyDeathUnder(begins[r.ID]) {
			died = true
		}
	}
	return n, died
}

func share(n, of int) float64 {
	if of == 0 {
		return 0
	}
	return float64(n) / float64(of)
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	k := int(float64(len(s)-1) * p / 100)
	return s[k]
}

// PoisonCost is one poison request's cost: its attempts, the engines that died under them, and whether
// each crash-correlation candidate fired.
func PoisonCost(base string, rs []Result, begins map[string][]Begin, attempts []Attempt) PoisonTrial {
	t := PoisonTrial{Base: base, Attempts: len(rs)}
	if len(rs) > 0 {
		t.Final = rs[len(rs)-1].Status
	}
	ids := map[string]bool{}
	for _, r := range rs {
		ids[r.ID] = true
		for _, b := range begins[r.ID] {
			if b.Admitted && b.Outcome == "" {
				t.Deaths++
			}
		}
	}
	closed := map[string]int{}
	for _, a := range attempts {
		if ids[a.ID] && a.Kind == "closed" {
			closed[a.ID]++
		}
	}
	for _, n := range closed {
		if n >= 2 {
			t.CorrFront = true
		}
	}
	// Its deaths come seconds apart, inside every window: the controller-side candidate fires when they
	// are of two or more distinct Instances.
	dead := map[int]bool{}
	for _, r := range rs {
		for _, b := range begins[r.ID] {
			if b.Admitted && b.Outcome == "" {
				dead[b.Instance] = true
			}
		}
	}
	t.CorrController = len(dead) >= 2
	n, _ := contentClosed(rs, begins, closed, len(attempts) > 0)
	t.CorrContent = n >= 2
	return t
}
