package front

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The /~!frp passthrough on its own, against a raw TCP server standing in for
// frps's bind port, so the test sees the exact bytes passed. That frps takes
// what it passes, over wss in both modes, is TestRunTunnelInBothDialHomeModes.

// rawUpstream accepts connections and keeps what each sent, up to the end of
// its request's headers plus anything after them it reads before reply.
type rawUpstream struct {
	ln    net.Listener
	mu    sync.Mutex
	got   []string
	reply string
}

func startRawUpstream(t *testing.T, reply string) *rawUpstream {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	u := &rawUpstream{ln: ln, reply: reply}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go u.serve(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return u
}

func (u *rawUpstream) serve(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(c)
	var b strings.Builder
	for {
		l, err := br.ReadString('\n')
		b.WriteString(l)
		if err != nil || l == "\r\n" {
			break
		}
	}
	// What came with the headers: the test sends "EARLY" right after them.
	early := make([]byte, 5)
	if _, err := io.ReadFull(br, early); err == nil {
		b.Write(early)
	}
	u.mu.Lock()
	u.got = append(u.got, b.String())
	u.mu.Unlock()
	_, _ = io.WriteString(c, u.reply)
	// Then echo, as the tunnel's bytes flow both ways.
	_, _ = io.Copy(c, br)
}

func (u *rawUpstream) received() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.got...)
}

// serveDialHomePlain serves DialHome in ssh mode on a plain listener, with the
// passthrough to addr.
func serveDialHomePlain(t *testing.T, addr string) (string, *frpPassthrough) {
	t.Helper()
	pass := newFrpPassthrough(addr, time.Second, discardLog())
	dh, err := NewDialHome(ModeSSH, agentapi.Host{}, pass)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: dh, ReadHeaderTimeout: time.Minute}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close(); pass.Close() })
	return ln.Addr().String(), pass
}

const frpUpgrade = "GET /~!frp HTTP/1.1\r\nHost: dial-home.hpcburst.invalid\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
	"Origin: http://dial-home.hpcburst.invalid\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"

// TestFrpPassthroughPassesTheUpgradeUnchanged: frps matches the plain bytes
// "GET /~!frp" (HBEP-6, frp facts), so the request line reaches it as sent.
// Host, which Go's server takes out of the headers, is passed too, and so are
// the bytes the client sent right after its headers, which the server read
// ahead. Then the connection carries bytes both ways, until Close cuts it.
func TestFrpPassthroughPassesTheUpgradeUnchanged(t *testing.T) {
	up := startRawUpstream(t, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
	addr, pass := serveDialHomePlain(t, up.ln.Addr().String())
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.WriteString(c, frpUpgrade+"EARLY"); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("the upgrade's answer: %v, %v", resp, err)
	}
	got := up.received()
	if len(got) != 1 {
		t.Fatalf("frps saw %d connections, want 1", len(got))
	}
	if first, _, _ := strings.Cut(got[0], "\r\n"); first != "GET /~!frp HTTP/1.1" {
		t.Errorf("the request line reached frps as %q", first)
	}
	for _, want := range []string{"\r\nHost: dial-home.hpcburst.invalid\r\n", "\r\nUpgrade: websocket\r\n", "\r\nSec-Websocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n", "\r\n\r\nEARLY"} {
		if !strings.Contains(got[0], want) {
			t.Errorf("frps did not get %q in:\n%s", want, got[0])
		}
	}
	if _, err := io.WriteString(c, "ping"); err != nil {
		t.Fatal(err)
	}
	echo := make([]byte, 4)
	if _, err := io.ReadFull(br, echo); err != nil || string(echo) != "ping" {
		t.Fatalf("the spliced connection echoed %q, %v", echo, err)
	}
	pass.Close()
	if n, err := br.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Errorf("after Close the tunnel still carries bytes: %d, %v", n, err)
	}
}

// TestFrpPassthroughRefusesEverythingElse: only GET of exactly /~!frp with a
// websocket upgrade is passed; every other request gets 404 and opens nothing
// to frps (HBEP-6: every other path gets 404). In ssh mode /agent/v1/ is not
// passed either.
func TestFrpPassthroughRefusesEverythingElse(t *testing.T) {
	up := startRawUpstream(t, "HTTP/1.1 101 Switching Protocols\r\n\r\n")
	addr, _ := serveDialHomePlain(t, up.ln.Addr().String())
	upgrade := "\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
	for name, req := range map[string]string{
		"no upgrade":        "GET /~!frp HTTP/1.1\r\nHost: x\r\n\r\n",
		"no connection":     "GET /~!frp HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n\r\n",
		"POST":              "POST /~!frp HTTP/1.1" + upgrade,
		"longer path":       "GET /~!frpx HTTP/1.1" + upgrade,
		"a query":           "GET /~!frp?x=1 HTTP/1.1" + upgrade,
		"re-encoded":        "GET /%7E!frp HTTP/1.1" + upgrade,
		"root":              "GET / HTTP/1.1" + upgrade,
		"agent in ssh mode": "GET /agent/v1/poll HTTP/1.1\r\nHost: x\r\n\r\n",
	} {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		_, _ = io.WriteString(c, req)
		resp, err := http.ReadResponse(bufio.NewReader(c), nil)
		if err != nil || resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s: %v, %v; want 404", name, resp, err)
		}
		c.Close()
	}
	if n := len(up.received()); n != 0 {
		t.Errorf("frps got %d connections from refused requests", n)
	}
}

// TestFrpPassthroughAnswers502WithoutFrps: with nothing on frps's bind port
// the upgrade gets 502, before the connection is taken over.
func TestFrpPassthroughAnswers502WithoutFrps(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gone := ln.Addr().String()
	ln.Close()
	addr, _ := serveDialHomePlain(t, gone)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.WriteString(c, frpUpgrade)
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil || resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("an upgrade with frps down: %v, %v; want 502", resp, err)
	}
}
