package llmlabtest

import (
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 24 clients make 40 requests each over every route, while three goroutines
// flip every switch and read every counter. Under -race it shows that the
// Server's shared state is guarded, that no request is lost from the log, and
// that Arrived and Waiting hold up under load. Opus 5.5's review ran this once;
// this commits it. No earlier test had two requests in flight at once, so the
// 24 first requests are held by a manual stall until all 24 are blocked.
func TestConcurrentLoad(t *testing.T) {
	const clients, perClient = 24, 40
	const total = clients * perClient

	s := Start(t, Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{keyOne, keyTwo},
		Models: []Model{{ID: modelA}, {ID: modelB}, {ID: modelC}}})

	models := []string{modelA, modelB, modelC}
	kinds := []struct {
		method, path func(model string) string
		body         func(model string) string
	}{
		{lit(http.MethodGet), lit(routeModels), lit("")},
		{lit(http.MethodGet), func(m string) string { return routeModels + "/" + m }, lit("")},
		{lit(http.MethodGet), lit(routeModels + "/example-no-such-model"), lit("")}, // 404
		{lit(http.MethodPost), lit(routeChat), chatReq},
		{lit(http.MethodPost), lit(routeChat), func(m string) string { // streamed, with usage
			return `{"model":"` + m + `","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"one two"}]}`
		}},
		{lit(http.MethodPost), lit(routeCompletions), func(m string) string { return `{"model":"` + m + `","prompt":"one two"}` }},
		{lit(http.MethodPost), lit(routeCompletions), func(m string) string { return `{"model":"` + m + `","prompt":"one two","stream":true}` }},
		{lit(http.MethodPost), lit(routeResponses), func(m string) string { return `{"model":"` + m + `","input":"one two"}` }},
		{lit(http.MethodPost), lit(routeResponses), func(m string) string { return `{"model":"` + m + `","input":"one two","stream":true}` }},
		{lit(http.MethodPost), lit(routeEmbeddings), func(m string) string { return `{"model":"` + m + `","input":["one","two three"]}` }},
		{lit(http.MethodPost), lit(routeChat), func(m string) string { // 422 while rewriting is on
			return `{"model":"` + m + `","cache_salt":"x","messages":[{"role":"user","content":"one"}]}`
		}},
	}

	// The flippers run from before the first request to after the last answer.
	stop := make(chan struct{})
	stopFlippers := sync.OnceFunc(func() { close(stop) })
	t.Cleanup(stopFlippers)
	var rounds [3]atomic.Int64
	var flippers sync.WaitGroup
	flip := func(n int, round func(i int)) {
		flippers.Add(1)
		go func() {
			defer flippers.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				round(i)
				rounds[n].Add(1)
				time.Sleep(500 * time.Microsecond)
			}
		}()
	}
	flip(0, func(i int) {
		s.SetActive(modelB, i%2 == 0)
		s.SetBlocked(modelC, i%3 == 0)
		s.SetAmbiguous(i%4 < 2)
		if i%2 == 0 {
			s.RemoveModel(modelC)
		} else {
			s.AddModel(Model{ID: modelC})
		}
		_ = s.Listed()
		_ = s.Count(http.MethodPost, routeChat)
	})
	flip(1, func(i int) {
		s.RejectKey(keyTwo, []int{0, http.StatusUnauthorized, http.StatusForbidden}[i%3])
		if i%2 == 0 {
			s.SetRewriting(&Rewriting{})
		} else {
			s.SetRewriting(nil)
		}
		s.EchoKey(i%2 == 1)
		_ = s.Requests()
		_ = s.Stalled(time.Now())
	})
	flip(2, func(i int) {
		s.SetNoByteCut([]time.Duration{0, 2 * time.Second}[i%2])
		s.AddModel(Model{ID: modelB, MaxTokensCap: 2 * (i % 2)})
		s.SetReply([]string{DefaultReply, "one two three"}[i%2])
		_ = s.Arrived("", "")
		_ = s.Arrived(http.MethodPost, routeChat)
		_ = s.Waiting()
	})

	// All 24 clients meet the stall at once.
	s.Stall(true)
	type result struct {
		kind, status int
		body         int
		err          error
	}
	results := make([]result, total)
	var clientsDone sync.WaitGroup
	for c := 0; c < clients; c++ {
		clientsDone.Add(1)
		go func() {
			defer clientsDone.Done()
			for j := 0; j < perClient; j++ {
				k := (c + j) % len(kinds)
				model := models[(c+j/len(kinds))%len(models)]
				key := keyOne
				if j%3 == 2 {
					key = keyTwo
				}
				kd := kinds[k]
				a := send(s, kd.method(model), kd.path(model), bearer(key), kd.body(model))
				results[c*perClient+j] = result{kind: k, status: a.Status, body: len(a.Body), err: a.Err}
			}
		}()
	}
	requireWaiting(t, s, clients, "all 24 clients' first requests in the stall")
	if n := s.Arrived("", ""); n < clients {
		t.Errorf("Arrived = %d with %d requests held, want at least %d", n, clients, clients)
	}
	pollUntil(t, "each flipper has finished a round", func() bool {
		return rounds[0].Load() > 0 && rounds[1].Load() > 0 && rounds[2].Load() > 0
	})
	s.Stall(false) // never left on

	clientsDone.Wait()
	stopFlippers()
	flippers.Wait()

	hist := map[int]int{}
	for i, r := range results {
		switch {
		case r.err != nil:
			t.Errorf("request %d (kind %d): transport error: %v", i, r.kind, r.err)
		case r.status == 0:
			t.Errorf("request %d (kind %d): status 0", i, r.kind)
		case r.status == http.StatusOK && r.body == 0:
			t.Errorf("request %d (kind %d): 200 with an empty body", i, r.kind)
		}
		hist[r.status]++
	}
	var seen []string
	for st, n := range hist {
		seen = append(seen, strconv.Itoa(st)+":"+strconv.Itoa(n))
	}
	sort.Strings(seen)
	t.Logf("statuses %v", seen)
	if hist[http.StatusOK] == 0 || len(hist) < 3 {
		t.Errorf("statuses %v: want 200 and at least two other statuses, or the flipped switches never took effect", seen)
	}

	logged := waitLogged(t, s, total)
	if len(logged) != total || s.Arrived("", "") != total {
		t.Errorf("Requests %d, Arrived %d, want %d of each", len(logged), s.Arrived("", ""), total)
	}
	if g, p := s.Arrived(http.MethodGet, ""), s.Arrived(http.MethodPost, ""); g+p != total {
		t.Errorf("Arrived GET %d + POST %d, want %d in all", g, p, total)
	}
	if n := s.Waiting(); n != 0 {
		t.Errorf("Waiting = %d after every client is done, want 0", n)
	}
}

func lit(s string) func(string) string { return func(string) string { return s } }
