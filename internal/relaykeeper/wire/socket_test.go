package wire_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
)

// None of these tests runs in parallel: some replace wire.PeerUID, a package
// variable, and some change the umask or the working directory.

// deadline bounds every wait. It is generous: a test passes as soon as the
// state it waits on is reached, and fails only if that never happens.
const deadline = 30 * time.Second

// shortDir makes a directory with a short name, for a socket: a socket path
// is at most 107 bytes, and t.TempDir's names are longer.
func shortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ws")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if len(dir) > 60 {
		t.Fatalf("the temporary directory's path is %d bytes; a socket path of at most 107 would leave no room (set TMPDIR to a short path)", len(dir))
	}
	return dir
}

// waitFor polls cond until it holds. The condition is observable state, never
// a delay.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for !cond() {
		if time.Now().After(end) {
			t.Fatalf("gave up waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// server is a Serve over a Listen, with the handler's calls counted.
type server struct {
	path   string
	cancel context.CancelFunc
	done   chan error
	calls  atomic.Int64

	mu   sync.Mutex
	reqs []wire.Envelope

	once  sync.Once
	err   error
	ended bool
}

// serve listens at path and serves h until the test ends (or stop is called).
func serve(t *testing.T, path string, h wire.Handler) *server {
	t.Helper()
	ln, err := wire.Listen(path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	s := &server{path: path, done: make(chan error, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	counted := func(ctx context.Context, req wire.Envelope) (wire.Kind, any) {
		s.mu.Lock()
		s.reqs = append(s.reqs, req)
		s.mu.Unlock()
		s.calls.Add(1)
		return h(ctx, req)
	}
	go func() { s.done <- wire.Serve(ctx, ln, counted, nil) }()
	t.Cleanup(func() {
		s.cancel()
		if err := s.wait(t); err != nil {
			t.Errorf("Serve returned %v after its context ended", err)
		}
	})
	return s
}

// startServer serves h at a path of its own.
func startServer(t *testing.T, h wire.Handler) *server {
	t.Helper()
	return serve(t, filepath.Join(shortDir(t), "s"), h)
}

// wait waits for Serve to return, and returns its result.
func (s *server) wait(t *testing.T) error {
	t.Helper()
	s.once.Do(func() {
		select {
		case s.err = <-s.done:
			s.ended = true
		case <-time.After(deadline):
		}
	})
	if !s.ended {
		t.Error("Serve did not return")
	}
	return s.err
}

// requests are the envelopes that reached the handler.
func (s *server) requests() []wire.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]wire.Envelope(nil), s.reqs...)
}

// sampleReply is each request kind's sample reply.
func sampleReply(_ context.Context, req wire.Envelope) (wire.Kind, any) {
	switch req.Kind {
	case wire.KindPing:
		return wire.KindPong, wire.Pong{KeeperEpoch: 9}
	case wire.KindSet:
		return wire.KindReport, sampleReport()
	case wire.KindAck:
		return wire.KindAcked, wire.Acked{Provider: "provider-a", Decided: true}
	case wire.KindStatus:
		return wire.KindStatusReport, sampleStatus()
	}
	return wire.KindRefused, wire.Refused{Reason: wire.RefusedMalformed, Message: "not a request"}
}

func exchange(t *testing.T, path string, kind wire.Kind, body, reply any) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	return wire.Exchange(ctx, path, kind, body, reply)
}

func dial(t *testing.T, path string) net.Conn {
	t.Helper()
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	_ = c.SetDeadline(time.Now().Add(deadline))
	return c
}

// rawReply writes line (as a client that is not this package's would) and
// reads the one reply.
func rawReply(t *testing.T, path, line string) (wire.Envelope, error) {
	t.Helper()
	c := dial(t, path)
	if _, err := io.WriteString(c, line); err != nil {
		t.Fatalf("writing %q: %v", line, err)
	}
	return wire.Read(wire.NewReader(c))
}

// rawRefusal is rawReply of a reply that must be a refusal.
func rawRefusal(t *testing.T, path, line string) wire.Refused {
	t.Helper()
	env, err := rawReply(t, path, line)
	if err != nil {
		t.Fatalf("the reply to %q: %v", line, err)
	}
	if env.Kind != wire.KindRefused {
		t.Fatalf("the reply to %q has kind %q; want refused", line, env.Kind)
	}
	var r wire.Refused
	if err := env.Decode(&r); err != nil {
		t.Fatalf("decoding the refusal of %q: %v", line, err)
	}
	return r
}

// Exchange and Serve

func TestExchangeOfEachRequestKindReachesTheHandlerAndReturnsItsReply(t *testing.T) {
	s := startServer(t, sampleReply)
	cases := []struct {
		kind     wire.Kind
		body     any
		newBody  func() any
		newReply func() any
		want     any
	}{
		{wire.KindPing, struct{}{}, func() any { return new(struct{}) }, func() any { return new(wire.Pong) }, wire.Pong{KeeperEpoch: 9}},
		{wire.KindSet, validSet(), func() any { return new(wire.DesiredSet) }, func() any { return new(wire.Report) }, sampleReport()},
		{wire.KindAck, validAck(), func() any { return new(wire.Ack) }, func() any { return new(wire.Acked) }, wire.Acked{Provider: "provider-a", Decided: true}},
		{wire.KindStatus, struct{}{}, func() any { return new(struct{}) }, func() any { return new(wire.StatusReport) }, sampleStatus()},
	}
	for i, c := range cases {
		t.Run(string(c.kind), func(t *testing.T) {
			reply := c.newReply()
			if err := exchange(t, s.path, c.kind, c.body, reply); err != nil {
				t.Fatalf("Exchange: %v", err)
			}
			if got := reflect.ValueOf(reply).Elem().Interface(); !reflect.DeepEqual(got, c.want) {
				t.Errorf("the reply is %+v; want %+v", got, c.want)
			}
			reqs := s.requests()
			if len(reqs) != i+1 {
				t.Fatalf("the handler has been called %d times; want %d", len(reqs), i+1)
			}
			req := reqs[i]
			if req.V != wire.Version || req.Kind != c.kind {
				t.Errorf("the handler's request has v=%d kind=%q; want v=%d kind=%q", req.V, req.Kind, wire.Version, c.kind)
			}
			body := c.newBody()
			if err := req.Decode(body); err != nil {
				t.Fatalf("decoding the request the handler got: %v", err)
			}
			if got := reflect.ValueOf(body).Elem().Interface(); !reflect.DeepEqual(got, c.body) {
				t.Errorf("the handler got the body %+v; want %+v", got, c.body)
			}
		})
	}
}

func TestServeAnswersALineThatHasNoBody(t *testing.T) {
	s := startServer(t, sampleReply)
	env, err := rawReply(t, s.path, `{"v":1,"kind":"ping"}`+"\n")
	if err != nil || env.Kind != wire.KindPong {
		t.Fatalf("the reply to a ping with no body: kind %q, %v; want pong", env.Kind, err)
	}
	var p wire.Pong
	if err := env.Decode(&p); err != nil || p.KeeperEpoch != 9 {
		t.Errorf("the pong: %+v, %v", p, err)
	}
}

func TestExchangeOfAKindThatIsNotARequestFailsWithoutDialing(t *testing.T) {
	path := filepath.Join(shortDir(t), "none") // nothing listens: a dial would fail with another error
	for _, kind := range []wire.Kind{wire.KindPong, wire.KindReport, wire.KindAcked, wire.KindStatusReport, wire.KindRefused, "bogus"} {
		err := exchange(t, path, kind, struct{}{}, new(struct{}))
		if err == nil || !strings.Contains(err.Error(), "not a request") {
			t.Errorf("Exchange of %q: %v; want an error that it is not a request", kind, err)
		}
	}
}

func TestExchangeFailsWhereNobodyListens(t *testing.T) {
	path := filepath.Join(shortDir(t), "none")
	if err := exchange(t, path, wire.KindPing, struct{}{}, new(wire.Pong)); err == nil {
		t.Error("Exchange with no keeper succeeded")
	}
}

func TestAHandlersRefusalIsARefusedError(t *testing.T) {
	refusals := []wire.Refused{
		{Reason: wire.RefusedEpoch, HighestEpoch: 7, Message: "an older lead"},
		{Reason: wire.RefusedLoginHost, LoginHost: "login.example", Holder: "provider-b", Message: "the login host is held"},
		{Reason: wire.RefusedMalformed, Message: "no"},
	}
	for _, kind := range []wire.Kind{wire.KindPing, wire.KindSet, wire.KindAck, wire.KindStatus} {
		for _, want := range refusals {
			t.Run(string(kind)+"/"+want.Reason, func(t *testing.T) {
				s := startServer(t, func(context.Context, wire.Envelope) (wire.Kind, any) { return wire.KindRefused, want })
				err := exchange(t, s.path, kind, struct{}{}, new(struct{}))
				var re *wire.RefusedError
				if !errors.As(err, &re) {
					t.Fatalf("Exchange = %v; want a *RefusedError", err)
				}
				if !reflect.DeepEqual(re.Refused, want) {
					t.Errorf("the RefusedError carries %+v; want %+v", re.Refused, want)
				}
				if !strings.Contains(err.Error(), want.Reason) || !strings.Contains(err.Error(), want.Message) {
					t.Errorf("the error text %q names neither the reason %q nor the message %q", err, want.Reason, want.Message)
				}
			})
		}
	}
}

func TestAReplyOfTheWrongKindIsAnError(t *testing.T) {
	wrong := []struct {
		request wire.Kind
		reply   wire.Kind
		body    any
	}{
		{wire.KindPing, wire.KindReport, sampleReport()},
		{wire.KindSet, wire.KindPong, wire.Pong{}},
		{wire.KindAck, wire.KindReport, sampleReport()},
		{wire.KindStatus, wire.KindAcked, wire.Acked{}},
		{wire.KindPing, wire.KindPing, struct{}{}}, // a request kind as the reply
	}
	for _, c := range wrong {
		t.Run(string(c.request)+" answered "+string(c.reply), func(t *testing.T) {
			s := startServer(t, func(context.Context, wire.Envelope) (wire.Kind, any) { return c.reply, c.body })
			err := exchange(t, s.path, c.request, struct{}{}, new(struct{}))
			if err == nil {
				t.Fatal("Exchange accepted a reply of the wrong kind")
			}
			var re *wire.RefusedError
			if errors.As(err, &re) {
				t.Errorf("a reply of the wrong kind is a *RefusedError: %v", err)
			}
			if !strings.Contains(err.Error(), string(c.reply)) {
				t.Errorf("the error %q does not name the kind received, %q", err, c.reply)
			}
		})
	}
}

func TestAReplyOfAnUnknownKindOrWithAnUnknownBodyFieldIsAnError(t *testing.T) {
	cases := []struct {
		name  string
		kind  wire.Kind
		body  any
		wantE error
	}{
		{"an unknown kind", "bogus", struct{}{}, wire.ErrMalformed},
		{"an unknown body field", wire.KindPong, struct {
			KeeperEpoch int64 `json:"keeperEpoch"`
			Extra       int   `json:"extra"`
		}{1, 2}, wire.ErrMalformed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := startServer(t, func(context.Context, wire.Envelope) (wire.Kind, any) { return c.kind, c.body })
			if err := exchange(t, s.path, wire.KindPing, struct{}{}, new(wire.Pong)); !errors.Is(err, c.wantE) {
				t.Errorf("Exchange = %v; want %v", err, c.wantE)
			}
		})
	}
}

func TestARequestOfAnotherVersionIsRefusedAndNeverReachesTheHandler(t *testing.T) {
	s := startServer(t, sampleReply)
	for _, line := range []string{
		`{"v":2,"kind":"ping"}` + "\n",
		`{"v":2,"kind":"set","body":{}}` + "\n",
		`{"v":99,"kind":"status"}` + "\n",
		`{"v":0,"kind":"ping"}` + "\n",
		`{"v":-1,"kind":"ping"}` + "\n",
	} {
		r := rawRefusal(t, s.path, line)
		if r.Reason != wire.RefusedVersion {
			t.Errorf("the refusal of %q has reason %q; want %q", line, r.Reason, wire.RefusedVersion)
		}
		if !reflect.DeepEqual(r.Supported, []int{1}) {
			t.Errorf("the refusal of %q supports %v; want [1]", line, r.Supported)
		}
	}
	if n := s.calls.Load(); n != 0 {
		t.Errorf("the handler was called %d times for requests of another version", n)
	}
	// The keeper still serves its own version.
	if err := exchange(t, s.path, wire.KindPing, struct{}{}, new(wire.Pong)); err != nil {
		t.Errorf("a ping after the refusals: %v", err)
	}
	if n := s.calls.Load(); n != 1 {
		t.Errorf("the handler was called %d times; want 1, for the last ping", n)
	}
}

func TestAMalformedLineIsRefusedAndNeverReachesTheHandler(t *testing.T) {
	s := startServer(t, sampleReply)
	for _, line := range []string{
		"not json\n",
		"\n",
		"[]\n",
		`{"v":1,"kind":"frobnicate"}` + "\n",
		`{"v":1}` + "\n",
		`{"v":1,"kind":"ping","extra":1}` + "\n",
		`{"v":1,"kind":"ping"} {"v":1,"kind":"ping"}` + "\n",
		`{"v":1,"kind":"ping"} x` + "\n",
		`{"v":"1","kind":"ping"}` + "\n",
		`{"v":1,"kind":"ping"` + "\n",
	} {
		r := rawRefusal(t, s.path, line)
		if r.Reason != wire.RefusedMalformed {
			t.Errorf("the refusal of %q has reason %q; want %q", line, r.Reason, wire.RefusedMalformed)
		}
		if r.Message == "" {
			t.Errorf("the refusal of %q has no message", line)
		}
	}
	if n := s.calls.Load(); n != 0 {
		t.Errorf("the handler was called %d times for malformed lines", n)
	}
	if err := exchange(t, s.path, wire.KindPing, struct{}{}, new(wire.Pong)); err != nil {
		t.Errorf("a ping after the refusals: %v", err)
	}
}

func TestALineOverMaxMessageIsRefusedAsMalformed(t *testing.T) {
	s := startServer(t, sampleReply)
	c := dial(t, s.path)
	line := strings.Repeat("a", wire.MaxMessage+2) + "\n"
	// The keeper stops reading at the limit, so the end of the write may fail.
	go func() { _, _ = io.WriteString(c, line) }()
	env, err := wire.Read(wire.NewReader(c))
	if err != nil || env.Kind != wire.KindRefused {
		t.Fatalf("the reply to a line over MaxMessage: kind %q, %v; want a refusal", env.Kind, err)
	}
	var r wire.Refused
	if err := env.Decode(&r); err != nil || r.Reason != wire.RefusedMalformed {
		t.Errorf("the refusal: %+v, %v; want reason %q", r, err, wire.RefusedMalformed)
	}
	if n := s.calls.Load(); n != 0 {
		t.Errorf("the handler was called %d times", n)
	}
}

func TestAReplyKindSentAsARequestIsRefusedAsMalformed(t *testing.T) {
	s := startServer(t, sampleReply)
	for _, kind := range []wire.Kind{wire.KindReport, wire.KindPong, wire.KindAcked, wire.KindStatusReport, wire.KindRefused} {
		line := `{"v":1,"kind":"` + string(kind) + `","body":{}}` + "\n"
		r := rawRefusal(t, s.path, line)
		if r.Reason != wire.RefusedMalformed {
			t.Errorf("the refusal of a %s as a request has reason %q; want %q", kind, r.Reason, wire.RefusedMalformed)
		}
		if !strings.Contains(r.Message, string(kind)) {
			t.Errorf("the refusal's message %q does not name the kind %q", r.Message, kind)
		}
	}
	if n := s.calls.Load(); n != 0 {
		t.Errorf("the handler was called %d times for kinds that are not requests", n)
	}
}

// PeerUID is a package variable that the keeper's connection goroutines read.
// Each case here replaces it before its own server starts, and restores it
// after that server has stopped (t.Cleanup runs last-in first), so the
// replacement is ordered with every read of it, and the test is clean under
// -race.
func TestAPeerOfAnotherUserGetsNoReplyAndNeverReachesTheHandler(t *testing.T) {
	// The real PeerUID first: this user's own peer is served.
	base := startServer(t, sampleReply)
	if err := exchange(t, base.path, wire.KindPing, struct{}{}, new(wire.Pong)); err != nil {
		t.Fatalf("this user's peer: %v", err)
	}
	if n := base.calls.Load(); n != 1 {
		t.Fatalf("the handler was called %d times; want 1", n)
	}

	cases := []struct {
		name string
		peer func(*net.UnixConn) (int, error)
	}{
		{"another uid", func(*net.UnixConn) (int, error) { return os.Geteuid() + 1, nil }},
		{"an error reading the peer", func(*net.UnixConn) (int, error) { return os.Geteuid(), errors.New("no credentials") }},
	}
	if os.Geteuid() != 0 {
		cases = append(cases, struct {
			name string
			peer func(*net.UnixConn) (int, error)
		}{"root", func(*net.UnixConn) (int, error) { return 0, nil }})
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			old := wire.PeerUID
			wire.PeerUID = c.peer
			t.Cleanup(func() { wire.PeerUID = old })

			s := startServer(t, sampleReply)
			conn := dial(t, s.path)
			// The keeper closes at once, so this write may find the connection
			// closed already; either way, nothing may come back.
			_, _ = io.WriteString(conn, `{"v":1,"kind":"ping"}`+"\n")
			got, err := io.ReadAll(conn)
			if errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal("the keeper left the connection of another user's peer open")
			}
			if len(got) != 0 {
				t.Errorf("the keeper answered another user's peer: %q", got)
			}
			if n := s.calls.Load(); n != 0 {
				t.Errorf("the handler was called %d times; want it never for another user's peer", n)
			}
		})
	}

	// Restored: this user's peer is served again.
	after := startServer(t, sampleReply)
	if err := exchange(t, after.path, wire.KindPing, struct{}{}, new(wire.Pong)); err != nil {
		t.Errorf("this user's peer after the others: %v", err)
	}
}

func TestPeerUIDOfARealConnectionIsThisUser(t *testing.T) {
	path := filepath.Join(shortDir(t), "s")
	ln, err := wire.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan *net.UnixConn, 1)
	go func() {
		if c, err := ln.AcceptUnix(); err == nil {
			accepted <- c
		}
	}()
	dial(t, path)
	select {
	case srv := <-accepted:
		defer srv.Close()
		uid, err := wire.PeerUID(srv)
		if err != nil || uid != os.Geteuid() {
			t.Errorf("PeerUID = %d, %v; want this user's %d", uid, err, os.Geteuid())
		}
	case <-time.After(deadline):
		t.Fatal("the listener accepted nothing")
	}
}

func TestServeReturnsAndTheSocketStopsAcceptingWhenItsContextEnds(t *testing.T) {
	path := filepath.Join(shortDir(t), "s")
	s := serve(t, path, sampleReply)
	if err := exchange(t, path, wire.KindPing, struct{}{}, new(wire.Pong)); err != nil {
		t.Fatalf("before cancel: %v", err)
	}

	s.cancel()
	if err := s.wait(t); err != nil {
		t.Errorf("Serve returned %v; want nil once its context ended", err)
	}
	if c, err := net.Dial("unix", path); err == nil {
		c.Close()
		t.Error("the socket path still accepts a connection after Serve returned")
	}
	if err := exchange(t, path, wire.KindPing, struct{}{}, new(wire.Pong)); err == nil {
		t.Error("Exchange succeeded after Serve returned")
	}
}

func TestServeReturnsWhenItsContextEndedBeforeItStarted(t *testing.T) {
	path := filepath.Join(shortDir(t), "s")
	ln, err := wire.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- wire.Serve(ctx, ln, sampleReply, nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve = %v; want nil", err)
		}
	case <-time.After(deadline):
		ln.Close()
		t.Fatal("Serve did not return for a context that had ended")
	}
}

func TestServeReturnsAnErrorWhenItsListenerFailsUnderIt(t *testing.T) {
	path := filepath.Join(shortDir(t), "s")
	ln, err := wire.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- wire.Serve(context.Background(), ln, sampleReply, nil) }()
	// Serve is accepting once a connection is answered.
	if err := exchange(t, path, wire.KindPing, struct{}{}, new(wire.Pong)); err != nil {
		t.Fatal(err)
	}
	ln.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Error("Serve returned nil although its context never ended")
		}
	case <-time.After(deadline):
		t.Fatal("Serve did not return after its listener was closed")
	}
}

func TestServeWaitsForAnExchangeInFlightAfterItsContextEnds(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	s := startServer(t, func(ctx context.Context, req wire.Envelope) (wire.Kind, any) {
		close(entered)
		<-release
		return wire.KindPong, wire.Pong{KeeperEpoch: 4}
	})
	replied := make(chan error, 1)
	var pong wire.Pong
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), deadline)
		defer cancel()
		replied <- wire.Exchange(ctx, s.path, wire.KindPing, struct{}{}, &pong)
	}()
	select {
	case <-entered:
	case <-time.After(deadline):
		t.Fatal("the handler was never called")
	}

	s.cancel()
	// The listener is closed: nothing new is accepted. The exchange in flight
	// is still there, so Serve has not returned.
	waitFor(t, "the socket to stop accepting", func() bool {
		c, err := net.Dial("unix", s.path)
		if err == nil {
			c.Close()
			return false
		}
		return true
	})
	select {
	case <-s.done:
		t.Fatal("Serve returned while an exchange was still in flight")
	default:
	}

	close(release)
	select {
	case err := <-replied:
		if err != nil || pong.KeeperEpoch != 4 {
			t.Errorf("the exchange in flight: %+v, %v; want its reply", pong, err)
		}
	case <-time.After(deadline):
		t.Fatal("the exchange in flight never got its reply")
	}
	if err := s.wait(t); err != nil {
		t.Errorf("Serve returned %v", err)
	}
}
