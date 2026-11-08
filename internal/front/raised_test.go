package front

// HBEP-8's raised-events counter in the front (Metrics, Controller metrics,
// Raised events; Test plan, Raised events). Every harness raises its events
// through the production chain, KubeRecorder over eventcount.Events, into a
// recorder that keeps them for the tests (memoryEvents), and checks at its
// end that each was counted once, with its labels, before the recorder
// received it. The front's /metrics exports the counter with its series at 0
// and the process's start time. The request path has no pass marker, so its
// absence test waits on the proxy's hook after a request's finish.

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// memoryEvents stands for client-go's events.k8s.io recorder under the
// counting wrapper: it hands each event to a MemoryRecorder, and notes how
// many events of each series it received and whether the count had risen to
// include each one when it arrived.
type memoryEvents struct {
	m *MemoryRecorder
	c *eventcount.Counter

	mu     sync.Mutex
	handed map[eventcount.Key]float64
	early  []string
}

func newMemoryEvents(m *MemoryRecorder, c *eventcount.Counter) *memoryEvents {
	return &memoryEvents{m: m, c: c, handed: map[eventcount.Key]float64{}}
}

// Eventf implements events.EventRecorder.
func (e *memoryEvents) Eventf(regarding runtime.Object, _ runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	k := eventcount.Key{Controller: eventcount.ControllerFront, Reason: reason, Action: action, Type: eventtype}
	e.mu.Lock()
	e.handed[k]++
	n := e.handed[k]
	e.mu.Unlock()
	if got := counted(e.c)[k]; got < n {
		e.mu.Lock()
		e.early = append(e.early, fmt.Sprintf("%s arrived with the count at %g, below the %g received", k, got, n))
		e.mu.Unlock()
	}
	e.m.Event(regarding, eventtype == provider.EventKindWarning, action, reason, fmt.Sprintf(note, args...))
}

// check fails unless every event received was counted exactly once with its
// labels, nothing else was counted, and no event arrived before its count.
func (e *memoryEvents) check(t *testing.T) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range e.early {
		t.Errorf("raised events: %s", s)
	}
	all := counted(e.c)
	for k, n := range e.handed {
		if all[k] != n {
			t.Errorf("raised events: %s was handed to the recorder %g times and counted %g times", k, n, all[k])
		}
	}
	for k, n := range all {
		if n != 0 && e.handed[k] == 0 {
			t.Errorf("raised events: %s counted %g times, never handed to the recorder", k, n)
		}
	}
}

// counted reads every series of the counter through a registry, as a scrape
// would.
func counted(c *eventcount.Counter) map[eventcount.Key]float64 {
	reg := prometheus.NewRegistry()
	out := map[eventcount.Key]float64{}
	if err := reg.Register(c); err != nil {
		return out
	}
	fams, _ := reg.Gather()
	for _, f := range fams {
		for _, m := range f.GetMetric() {
			out[keyOf(m.GetLabel())] = m.GetCounter().GetValue()
		}
	}
	return out
}

func keyOf(ls []*dto.LabelPair) eventcount.Key {
	var k eventcount.Key
	for _, l := range ls {
		switch l.GetName() {
		case eventcount.LabelController:
			k.Controller = l.GetValue()
		case eventcount.LabelReason:
			k.Reason = l.GetValue()
		case eventcount.LabelAction:
			k.Action = l.GetValue()
		case eventcount.LabelType:
			k.Type = l.GetValue()
		}
	}
	return k
}

// frontRead reads the front's /metrics and parses it for one series.
func frontRead(t *testing.T, m *Metrics, k eventcount.Key) eventcount.Read {
	t.Helper()
	sent := time.Now()
	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	answered := time.Now()
	if w.Code != http.StatusOK {
		t.Fatalf("GET /metrics: %d", w.Code)
	}
	r, err := eventcount.ParseRead(w.Body.Bytes(), k)
	if err != nil {
		t.Fatal(err)
	}
	r.Sent, r.Answered = sent, answered
	return r
}

// TestFrontMetricsExportTheRaisedEventsAtZero: the front's own /metrics has
// every series the front can raise at 0, and the process collector's start
// time, which an absence check compares across its reads (HBEP-8, Raised
// events: Export; Test plan: Series at 0).
func TestFrontMetricsExportTheRaisedEventsAtZero(t *testing.T) {
	h := newHarness(t, nil)
	ss := scrape(t, h.metrics)
	for _, s := range RaisedEvents() {
		wantSample(t, ss, 0, eventcount.Name, "controller", eventcount.ControllerFront, "reason", s.Reason, "action", s.Action, "type", s.Type)
	}
	if _, ok := find(t, ss, eventcount.ProcessStartName); !ok {
		t.Errorf("no %s on the front's /metrics", eventcount.ProcessStartName)
	}
}

// hookedRequests records each request the proxy has finished, and when.
type hookedRequests struct {
	mu   sync.Mutex
	done map[string]time.Time
	more chan struct{}
}

func hookRequests(h *harness) *hookedRequests {
	r := &hookedRequests{done: map[string]time.Time{}, more: make(chan struct{}, 1)}
	h.proxy.afterFinish = func(id string) {
		r.mu.Lock()
		r.done[id] = time.Now()
		r.mu.Unlock()
		select {
		case r.more <- struct{}{}:
		default:
		}
	}
	return r
}

// next waits until the proxy has finished one more request than it had at
// the call's start, and returns when it did.
func (r *hookedRequests) next(t *testing.T, before int) time.Time {
	t.Helper()
	deadline := time.After(time.Minute)
	for {
		r.mu.Lock()
		if len(r.done) > before {
			var last time.Time
			for _, at := range r.done {
				if at.After(last) {
					last = at
				}
			}
			r.mu.Unlock()
			return last
		}
		r.mu.Unlock()
		select {
		case <-r.more:
		case <-deadline:
			t.Fatal("the proxy finished no request within a minute")
		}
	}
}

// TestFrontRequestPathAbsenceOfUsageWriteFailed proves, by the four
// conditions, that a request whose usage record is written raises no
// UsageWriteFailed, with the front holding its Lease, so the event could be
// raised. The control: a request whose record cannot be written raises it,
// and the same check says so. The barrier is the proxy's hook after the
// request's finish, which writes the record and raises the event (HBEP-8,
// Raised events, condition 3: the front's request path).
func TestFrontRequestPathAbsenceOfUsageWriteFailed(t *testing.T) {
	k := eventcount.Key{Controller: eventcount.ControllerFront, Reason: reasonUsageWriteFailed, Action: eventFailing, Type: provider.EventKindWarning}
	quiet := "UsageWriteFailed is raised by a request's finish and by the Lease keeper's flush; this harness runs no Lease keeper"
	for _, tc := range []struct {
		name  string
		block bool // a directory where the day's file goes makes the write fail
		want  eventcount.Verdict
	}{{"a written record", false, eventcount.NotRaised}, {"a record that cannot be written", true, eventcount.Raised}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newScFix(t, scOpts{})
			f.router.lease.set(true, "lease-uid")
			if tc.block {
				if err := os.Mkdir(filepath.Join(f.cfg.UsageDir, "usage-2026-10-31.jsonl"), 0o700); err != nil { // the fake clock's day
					t.Fatal(err)
				}
			}
			hook := hookRequests(f.harness)
			first := frontRead(t, f.metrics, k)
			change := time.Now()
			resp := f.errPost(scChat, errChatBody(scModel, false, "hi"))
			readAll(t, resp)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status %d, want 200: the front keeps serving", resp.StatusCode)
			}
			b := eventcount.Barrier{What: "the request's finish (" + tc.name + ")", PassStart: change, Observed: hook.next(t, 0)}
			second := frontRead(t, f.metrics, k)
			a := eventcount.Absence{Key: k, First: first, Second: second, Change: change, Barriers: []eventcount.Barrier{b}, Quiet: quiet}
			v, why := a.Judge()
			if v != tc.want {
				t.Fatalf("%s (%s), want %s", v, why, tc.want)
			}
			if err := a.Require(); (err == nil) != (tc.want == eventcount.NotRaised) || errors.Is(err, eventcount.ErrInconclusive) {
				t.Fatalf("Require = %v", err)
			}
		})
	}
}
