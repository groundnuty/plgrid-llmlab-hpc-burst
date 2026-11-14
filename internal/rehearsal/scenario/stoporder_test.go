package scenario_test

// HBEP-11's stop order on the whole stack (#553): on down, the launcher's supervisor stops the front
// first and waits for it to exit, then the tunnel processes and the controllers. So a stream in flight
// is cut by the front itself, as HBEP-5 says (after its 10 s shutdown grace: stream_cut, detail shutdown,
// cutBy front_shutdown), while its tunnel is still up, and frps exits only after the front.

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// exitWatch records when each pinned process was seen to exit, by a blocking poll on its pidfd, so the
// order of two exits is read from the processes, not from the launcher's report.
type exitWatch struct {
	mu   sync.Mutex
	at   map[string]time.Time
	done map[string]chan struct{}
}

func watchExits(t *testing.T, pins map[string]*stackPin) *exitWatch {
	t.Helper()
	w := &exitWatch{at: map[string]time.Time{}, done: map[string]chan struct{}{}}
	for name, p := range pins {
		ch := make(chan struct{})
		w.done[name] = ch
		go func() {
			defer close(ch)
			fds := []unix.PollFd{{Fd: int32(p.FD), Events: unix.POLLIN}}
			for {
				n, err := unix.Poll(fds, -1)
				if err == unix.EINTR {
					continue
				}
				if err != nil || n > 0 {
					break
				}
			}
			w.mu.Lock()
			w.at[name] = time.Now()
			w.mu.Unlock()
		}()
	}
	return w
}

// exitedAt waits up to d for the process to be seen exited, and returns when it was.
func (w *exitWatch) exitedAt(t *testing.T, name string, d time.Duration) time.Time {
	t.Helper()
	select {
	case <-w.done[name]:
	case <-time.After(d):
		t.Fatalf("the %s did not exit within %s of down", name, d)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.at[name]
}

// TestRehearsalKeeperDownStopsTheFrontFirst: a stream in flight through the front when down runs gets the
// front's own cut, after its shutdown grace, and only then do the tunnel processes stop: the stream's last
// event is stream_cut with detail shutdown, its usage record is cut by front_shutdown, frps (and the relay
// keeper, and the controller) exit after the front, and frps is still running when the cut reaches the
// client. Before #553 every process was stopped at once: frps closed the tunnel at once, and the stream
// ended by a tunnel cut within the front's grace.
//
//livecheck:none the keeper rehearsal; hack/rehearsal.sh runs it, with REHEARSAL_RUN naming it
func TestRehearsalKeeperDownStopsTheFrontFirst(t *testing.T) {
	requireRehearsal(t)
	// No watch: down stops the API it would follow.
	r := startKeeperRig(t, rigSetup{noWatch: true})
	in, _ := r.keeperStart()
	key := r.fc.KeyFor(in)
	sup, err := pinSupervisor(t, r.h.State)
	if err != nil {
		t.Fatalf("pin the supervisor: %v", err)
	}
	pins := map[string]*stackPin{}
	for _, name := range []string{"front", "frps", "relaykeeper", "controller"} {
		p, _, err := pinEntry(t, r.h.State, name, sup)
		if err != nil {
			t.Fatalf("pin the %s: %v", name, err)
		}
		pins[name] = p
	}
	s := r.startOpenStream("the stream through down", key)
	r.waitFirst(s)
	r.waitTokens(s, marginTokens, time.Time{}, "before down")
	w := watchExits(t, pins)
	downAt := time.Now()
	if err := r.h.DownErr(); err != nil {
		t.Fatalf("down: %v", err)
	}
	r.waitEnd(s, stack.WaitDeadline)
	status, endErr, byClient := s.state()
	s.mu.Lock()
	events, endAt := append([]string(nil), s.events...), s.endAt
	s.mu.Unlock()
	if byClient || status != http.StatusOK {
		t.Fatalf("the stream: status %d, ended %v, by its client %v; want a 200 stream the front ended", status, endErr, byClient)
	}
	if p := openEventProblems(events, true); len(p) != 0 {
		t.Errorf("the stream's events: %v", p)
	}
	var last struct {
		Error *stack.FrontError `json:"error"`
	}
	if len(events) == 0 || json.Unmarshal([]byte(events[len(events)-1]), &last) != nil || last.Error == nil ||
		last.Error.Code != "stream_cut" || last.Error.Detail == nil || *last.Error.Detail != "shutdown" {
		tail := ""
		if len(events) > 0 {
			tail = events[len(events)-1]
		}
		t.Errorf("the stream's last event %.200s; want stream_cut with detail shutdown, the front's own cut (HBEP-5)", tail)
	}
	if row := r.fc.RowOf(s.id); row.CutBy == nil || *row.CutBy != "front_shutdown" || row.Finish == nil || *row.Finish != "cut" {
		t.Errorf("the stream's usage record: finish %s, cut by %s; want cut by front_shutdown", strOrNil(row.Finish), strOrNil(row.CutBy))
	}
	frontAt := w.exitedAt(t, "front", time.Minute)
	for _, name := range []string{"frps", "relaykeeper", "controller"} {
		if at := w.exitedAt(t, name, time.Minute); at.Before(frontAt) {
			t.Errorf("the %s exited %s before the front: the front was not stopped first", name, frontAt.Sub(at))
		}
	}
	if frpsAt := w.exitedAt(t, "frps", time.Minute); !endAt.Before(frpsAt) {
		t.Errorf("frps exited %s before the stream's cut reached the client: its tunnel closed under the stream", endAt.Sub(frpsAt))
	}
	t.Logf("down at %s: the stream cut by the front %s later, the front exited at +%s, frps at +%s", downAt.Format(time.TimeOnly),
		endAt.Sub(downAt).Round(time.Millisecond), frontAt.Sub(downAt).Round(time.Millisecond), w.exitedAt(t, "frps", time.Minute).Sub(downAt).Round(time.Millisecond))
}
