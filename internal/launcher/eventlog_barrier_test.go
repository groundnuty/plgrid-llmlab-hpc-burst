package launcher

// The log's start barrier waits for a client, not for the streams alone (issue #669; HBEP-8, The
// durable log: the launcher starts the log "before the controllers and the front", so that they can
// connect). nats-server lists the streams it restored from the store on its monitoring port before it
// opens the client port, so after a restart on an existing store the streams are listed while a
// connection is still refused. These rows stand that state up exactly: a monitoring port that lists
// both streams from the start, and a client port that opens late, with the token.
//
// Ports: the log tests' own pair, base+16 (clients) and base+17 (monitoring) of base 28480, inside
// #625's block 28480-28519. The fake controller's eventlog role runs no log here (eventlog-idle), so
// nothing else binds them, and the tests run one at a time like the other log tests.

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

// fakeMonitor serves the log's monitoring port from the test: GET /healthz answers 200, and GET /jsz
// lists both streams, as nats-server does once it has restored them. It counts the /jsz asks, which
// are the supervisor's barrier polling for the streams.
type fakeMonitor struct{ jszAsks atomic.Int64 }

func startFakeMonitor(t *testing.T, addr string) *fakeMonitor {
	t.Helper()
	m := &fakeMonitor{}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"ok"}`)) })
	mux.HandleFunc("/jsz", func(w http.ResponseWriter, _ *http.Request) {
		m.jszAsks.Add(1)
		stream := func(name string) map[string]any {
			return map[string]any{"name": name, "config": map[string]any{"max_bytes": 64 << 20}, "state": map[string]any{"messages": 5, "bytes": 915}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"account_details": []any{
			map[string]any{"stream_detail": []any{stream(eventlog.StreamEvents), stream(eventlog.StreamUsage)}},
		}})
	})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("the fake monitoring port %s: %v", addr, err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return m
}

// startClientPort opens the log's client address with an embedded nats-server that requires token,
// and returns once it accepts connections. It stands for the late-opening client port of a log
// restarted on an existing store. Its cleanup waits for the listener to be closed.
func startClientPort(t *testing.T, port int, token string) *natsserver.Server {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: port, Authorization: token, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatalf("the embedded server did not open 127.0.0.1:%d within 10 s", port)
	}
	return srv
}

// With the streams listed from the start and the client port closed, the others are held back; once
// the port opens they start, and not before. The held state is read from what the test can observe:
// the supervisor has asked the monitoring port for the streams three more times (it polls every 25 ms,
// so the barrier has looked and gone on holding), and the controller's record is still "starting". A
// barrier that waited for the streams alone releases at its first ask, asks no more, and starts the
// controller while the port is closed: the test says so.
func TestTheBarrierHoldsTheControllersUntilTheLogAcceptsAClient(t *testing.T) {
	f := eventLogFixture(t, plentyConfig())
	host := f.l.host(f.ports)
	f.steer("eventlog-idle", "1")
	mon := startFakeMonitor(t, f.ports.EventLogMonitorAddr())
	// EventLogStartWait is far over this test, so the barrier's own bound is not what releases it.
	f.supervise(SuperviseOptions{disk: f.diskFor(100*gb, 0, 0), EventLogStartWait: 2 * time.Minute})
	f.logUp()

	// The state under test: the streams are listed, and a client is refused.
	if !eventLogStreamsExist(f.ports.EventLogMonitorAddr()) {
		t.Fatal("the fake monitoring port does not list both streams: the test does not stand up its state")
	}
	if eventLogAcceptsClient(host) {
		t.Fatal("the log is ready for a client while nothing listens on its client port")
	}
	// The asks above were this test's own: count the supervisor's from here.
	asked := mon.jszAsks.Load()
	deadline := time.Now().Add(20 * time.Second)
	for mon.jszAsks.Load() < asked+3 {
		if time.Now().After(deadline) {
			t.Fatal(f.withChildLogs("the supervisor did not ask the monitoring port for the streams three times within 20 s: the barrier is not polling, or it was released"))
		}
		for _, e := range f.record().Entries {
			if e.Name == controllerEntryName && (e.State == entryRunning || e.Proc != nil) {
				t.Fatalf("the controller started while the log's client port was closed: %+v", e)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, e := range f.record().Entries {
		if e.Name == controllerEntryName && (e.State != entryStarting || e.Proc != nil) {
			t.Fatalf("the barrier is not holding the controller after three asks of the streams: %+v", e)
		}
	}

	// The port opens: the barrier releases, and the controller starts after it.
	opening := time.Now()
	startClientPort(t, host.EventLogPort(), f.logToken())
	ctl := f.ctlUp()
	if !startTimeOf(t, ctl).After(opening) {
		t.Errorf("the controller started at %v, not after the client port began to open at %v", startTimeOf(t, ctl), opening)
	}
	if !eventLogAcceptsClient(host) {
		t.Error("the log is not ready for a client although its port is open and the streams are listed")
	}
}

// The readiness check is the token's: a log whose port is open but whose token is another is not
// ready, and a connection the check makes is closed again (an open one would stay in the server's
// count, which is read until it is zero).
func TestTheLogIsReadyOnlyForItsOwnTokenAndTheCheckClosesItsConnection(t *testing.T) {
	f := eventLogFixture(t, plentyConfig())
	host := f.l.host(f.ports)
	startFakeMonitor(t, f.ports.EventLogMonitorAddr())
	token := f.logToken()
	other := "0123456789abcdef"
	if token == other {
		t.Fatal("the fixture's token is the test's other token")
	}
	srv := startClientPort(t, host.EventLogPort(), other)
	if eventLogAcceptsClient(host) {
		t.Fatal("the log is ready for a client whose token it refuses")
	}
	// Control: the same log with the token the file holds (rewritten as the launcher's own file) is ready.
	if err := os.WriteFile(host.EventLogTokenFile(), []byte(other+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !eventLogAcceptsClient(host) {
		t.Fatal("the log is not ready for a client with the token it requires")
	}
	deadline := time.Now().Add(10 * time.Second)
	for srv.NumClients() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the readiness check left %d connection(s) open on the log", srv.NumClients())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
