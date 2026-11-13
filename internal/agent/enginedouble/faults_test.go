package enginedouble

// The fault modes of spike S18 (#528): a request's "admitted" record carries its Tail, so a request
// the engine dies under is still seen to have begun; die-on ends the engine under a marked request
// once it is recorded, with no reply byte; fail-on answers a marked request 500 then. Each fires only
// on the marked request: the unmarked ones before and after it complete.

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// faultChat is a chat request whose one user message ends with text.
func faultChat(text string) map[string]any {
	return map[string]any{
		"model":      "m",
		"messages":   []map[string]string{{"role": "user", "content": "please answer this request " + text}},
		"max_tokens": 4, "ignore_eos": true,
	}
}

// rawPost sends body to path and returns the status and the body read, or the error of a request that
// got no answer.
func rawPost(t *testing.T, hc *http.Client, path string, body any) (int, []byte, error) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := hc.Post("http://engine"+path, "application/json", bytes.NewReader(b))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	return resp.StatusCode, out, err
}

// recordedServer serves newHandler(cfg) in this process and keeps every record it makes.
func recordedServer(t *testing.T, cfg Config) (*http.Client, func() []LogEvent) {
	t.Helper()
	var mu sync.Mutex
	var events []LogEvent
	socket := sockPath(t, shortDir(t), "e.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: newHandler(cfg, func(ev LogEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return unixClient(socket), func() []LogEvent {
		mu.Lock()
		defer mu.Unlock()
		return append([]LogEvent(nil), events...)
	}
}

// eventsOf is the records of one event whose Tail carries id.
func eventsOf(evs []LogEvent, event, id string) []LogEvent {
	var out []LogEvent
	for _, ev := range evs {
		if ev.Event == event && strings.Contains(ev.Tail, id) {
			out = append(out, ev)
		}
	}
	return out
}

func TestTheAdmittedRecordCarriesTheRequestsTail(t *testing.T) {
	hc, records := recordedServer(t, cfgFor())
	if code, body, err := rawPost(t, hc, "/v1/chat/completions", faultChat("id-0007")); err != nil || code != 200 {
		t.Fatalf("the request: %d %v %s", code, err, body)
	}
	if code, body, err := rawPost(t, hc, "/v1/completions", map[string]any{"model": "m", "prompt": "a prompt id-0008", "max_tokens": 2, "ignore_eos": true}); err != nil || code != 200 {
		t.Fatalf("the completions request: %d %v %s", code, err, body)
	}
	eventually(t, 10*time.Second, "both requests' records", func() bool {
		return len(eventsOf(records(), "request", "id-000")) == 2
	})
	for _, id := range []string{"id-0007", "id-0008"} {
		adm, req := eventsOf(records(), "admitted", id), eventsOf(records(), "request", id)
		if len(adm) != 1 || len(req) != 1 || adm[0].Tail != req[0].Tail {
			t.Errorf("%s: admitted %+v, request %+v; want one of each, with the same Tail", id, adm, req)
		}
	}
}

// fail-on: the marked request is admitted, recorded, and answered 500; the unmarked ones around it
// complete. On both generation endpoints.
func TestFailOnAnswers500OnlyToTheMarkedRequest(t *testing.T) {
	cfg := cfgFor()
	cfg.FailOn = "FAULT-F2"
	hc, records := recordedServer(t, cfg)
	for _, c := range []struct {
		id   string
		body map[string]any
		want int
	}{
		{"id-0001", faultChat("id-0001"), 200},
		{"id-0002", faultChat("id-0002 FAULT-F2"), 500},
		{"id-0003", faultChat("id-0003"), 200},
		{"id-0004", map[string]any{"model": "m", "prompt": "a prompt id-0004 FAULT-F2", "max_tokens": 2}, 500},
	} {
		code, body, err := rawPost(t, hc, map[bool]string{true: "/v1/completions", false: "/v1/chat/completions"}[c.body["prompt"] != nil], c.body)
		if err != nil || code != c.want {
			t.Errorf("%s: %d %v %.120s, want %d", c.id, code, err, body, c.want)
		}
	}
	eventually(t, 10*time.Second, "the four requests' records", func() bool {
		return len(eventsOf(records(), "request", "id-000")) == 4
	})
	for _, id := range []string{"id-0002", "id-0004"} {
		if adm, req := eventsOf(records(), "admitted", id), eventsOf(records(), "request", id); len(adm) != 1 || len(req) != 1 || req[0].Outcome != "refused" {
			t.Errorf("%s, marked: admitted %+v, request %+v; want it admitted, then refused", id, adm, req)
		}
	}
	for _, id := range []string{"id-0001", "id-0003"} {
		if req := eventsOf(records(), "request", id); len(req) != 1 || req[0].Outcome != "completed" {
			t.Errorf("%s, unmarked: request %+v, want completed", id, req)
		}
	}
}

// die-on, in a handler this test serves: the marked request is admitted and recorded, then its
// connection closes with no byte of a reply, and the handler goes on serving the unmarked ones.
func TestDieOnAbortsOnlyTheMarkedRequestInAServedHandler(t *testing.T) {
	cfg := cfgFor()
	cfg.DieOn = "FAULT-F1"
	hc, records := recordedServer(t, cfg)
	if code, _, err := rawPost(t, hc, "/v1/chat/completions", faultChat("id-0001")); err != nil || code != 200 {
		t.Fatalf("the unmarked request before: %d %v", code, err)
	}
	code, body, err := rawPost(t, hc, "/v1/chat/completions", faultChat("id-0002 FAULT-F1"))
	if err == nil {
		t.Fatalf("the marked request got an answer: %d %.120s; want its connection closed with no reply", code, body)
	}
	if code, _, err := rawPost(t, hc, "/v1/chat/completions", faultChat("id-0003")); err != nil || code != 200 {
		t.Fatalf("the unmarked request after: %d %v", code, err)
	}
	eventually(t, 10*time.Second, "the marked request's records", func() bool {
		return len(eventsOf(records(), "request", "id-0002")) == 1
	})
	if adm, req := eventsOf(records(), "admitted", "id-0002"), eventsOf(records(), "request", "id-0002"); len(adm) != 1 || req[0].Outcome != "died" {
		t.Errorf("the marked request: admitted %+v, request %+v; want it admitted, then died", adm, req)
	}
}

// die-on in the engine process: an unmarked request completes (the mode armed, nothing marked); the
// marked one is admitted and recorded, and the engine process ends at once under it, killed, with no
// reply byte and no "request" line for it.
func TestDieOnEndsTheEngineProcessUnderTheMarkedRequest(t *testing.T) {
	dir := shortDir(t)
	socket, logPath := sockPath(t, dir, "e.sock"), filepath.Join(dir, "log")
	p := start(t, engineCmd(t, socket, logPath, "die-on=FAULT-F1"))
	waitSocket(t, socket)
	hc := unixClient(socket)
	if code, body, err := rawPost(t, hc, "/v1/chat/completions", faultChat("id-0001")); err != nil || code != 200 {
		t.Fatalf("the unmarked request: %d %v %.120s", code, err, body)
	}
	code, body, err := rawPost(t, hc, "/v1/chat/completions", faultChat("id-0002 FAULT-F1"))
	if err == nil {
		t.Fatalf("the marked request got an answer: %d %.120s; want the engine gone under it", code, body)
	}
	if c := p.exitCode(t, 10*time.Second); c != -1 {
		t.Fatalf("the engine exited with code %d, want it killed by a signal", c)
	}
	var ee *exec.ExitError
	if !errors.As(p.err, &ee) || ee.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Errorf("the engine ended with %v, want KILL", p.err)
	}
	log := readLog(t, logPath)
	if adm := eventsOf(log, "admitted", "id-0002"); len(adm) != 1 {
		t.Errorf("admitted records of the marked request: %+v, want one", adm)
	}
	if req := eventsOf(log, "request", "id-0002"); len(req) != 0 {
		t.Errorf("request records of the marked request: %+v, want none: the engine died under it", req)
	}
	if req := eventsOf(log, "request", "id-0001"); len(req) != 1 || req[0].Outcome != "completed" {
		t.Errorf("the unmarked request's record: %+v, want completed", req)
	}
}
