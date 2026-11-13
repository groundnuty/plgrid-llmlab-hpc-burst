package enginedouble

// The fault modes of spike S18 (#528): a request's "admitted" record carries its Tail, so a request
// the engine dies under is still seen to have begun; die-on ends the engine under a marked request
// once it is recorded, with no reply byte; fail-on answers a marked request 500 then. Each fires only
// on the marked request: the unmarked ones before and after it complete.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
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

// recordedServer serves newHandler(cfg) in this process and keeps every record it makes; the server's
// own error log goes to errLog, when given.
func recordedServer(t *testing.T, cfg Config, errLog ...*syncBuffer) (*http.Client, func() []LogEvent) {
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
	if len(errLog) > 0 {
		srv.ErrorLog = log.New(errLog[0], "", 0)
	}
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

// early-headers (Q1 of #528): a streamed reply's status and headers go out the moment it is admitted,
// before the wait for its first token, as vLLM does. With slow-first=1h the engine provably sits in that
// wait (its admitted record is written and no token can come for an hour): with the mode the client has
// the 200 and the event-stream header then; without it, it has nothing while the engine waits.
func TestEarlyHeadersSendsAStreamsHeadersBeforeItsFirstToken(t *testing.T) {
	for _, early := range []bool{true, false} {
		cfg := cfgFor()
		cfg.SlowFirst, cfg.EarlyHeaders = time.Hour, early
		hc, records := recordedServer(t, cfg)
		body, _ := json.Marshal(map[string]any{"model": "m", "messages": []map[string]string{{"role": "user", "content": "id-0001"}}, "max_tokens": 4, "stream": true})
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://engine/v1/chat/completions", bytes.NewReader(body))
		got := make(chan *http.Response, 1)
		go func() {
			if resp, err := hc.Do(req); err == nil {
				got <- resp
			}
			close(got)
		}()
		eventually(t, 10*time.Second, "the request admitted", func() bool { return len(eventsOf(records(), "admitted", "id-0001")) == 1 })
		if early {
			select {
			case resp := <-got:
				if resp == nil || resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
					t.Errorf("early-headers: the headers while the engine waits: %+v, want 200 and an event stream", resp)
				}
			case <-time.After(30 * time.Second):
				t.Error("early-headers: no headers while the engine sits in its hour's wait for the first token")
			}
		} else {
			select {
			case resp := <-got:
				t.Errorf("without early-headers the headers came while the engine waits for its first token: %+v", resp)
			default:
			}
		}
		cancel()
	}
}

// With early-headers, a fault on a marked streamed request still comes before the headers: fail-on
// answers 500, not a 200 stream. And an unmarked stream's body is as without the mode: its events, then
// [DONE], with its headers written once (net/http logs a second WriteHeader as superfluous).
func TestEarlyHeadersKeepsAFaultBeforeTheHeadersAndTheBodyUnchanged(t *testing.T) {
	var errLog syncBuffer
	body := func(early bool, text string) (int, string) {
		cfg := cfgFor()
		cfg.EarlyHeaders, cfg.FailOn = early, "FAULT-F2"
		hc, _ := recordedServer(t, cfg, &errLog)
		code, b, err := rawPost(t, hc, "/v1/chat/completions", map[string]any{"model": "m", "messages": []map[string]string{{"role": "user", "content": text}}, "max_tokens": 4, "ignore_eos": true, "stream": true})
		if err != nil {
			t.Fatalf("the stream: %v", err)
		}
		return code, string(b)
	}
	if code, b := body(true, "id-0002 FAULT-F2"); code != 500 {
		t.Errorf("a marked stream with early-headers: %d %.120s, want 500 before any header of a stream", code, b)
	}
	events := func(b string) int { return strings.Count(b, "data: ") }
	_, with := body(true, "id-0001")
	_, without := body(false, "id-0001")
	if events(with) != events(without) || !strings.HasSuffix(with, "data: [DONE]\n\n") {
		t.Errorf("an unmarked stream: %d events with early-headers, %d without; want the same, ending [DONE]:\n%s", events(with), events(without), with)
	}
	if l := errLog.String(); l != "" {
		t.Errorf("the server's error log: %q, want nothing (the headers written once)", l)
	}
}

// syncBuffer is a bytes.Buffer safe for the server's goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
