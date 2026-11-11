package testenv

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"
)

// A proxy that frps lists online is not yet a route (#368). Read at frp v0.71.1:
//   - frps lists a proxy online once it is in the proxy manager and in the stats (server/
//     control.go:886 and :766; server/http/controller.go:234-251), and answers a CONNECT for it
//     with 200 as soon as its route exists (control.go:876; pkg/util/vhost/vhost.go:184-193). Only
//     after both does it send frpc the NewProxyResp (control.go:768).
//   - frpc takes a work connection for the proxy only once it has handled that NewProxyResp, which
//     sets the proxy's phase to running (client/proxy/proxy_wrapper.go:139-165); before, it
//     closes the connection it is handed (proxy_wrapper.go:266-278).
//   - frpc reads transport.poolCount = 0 as unset and makes it 1 (pkg/config/v1/client.go:153),
//     so frps asks for one spare work connection at the login (server/control.go:446, :690-692)
//     and gives it to the first user connection without asking frpc (control.go:606-612). frpc
//     reads the StartWorkConn on that connection (server/proxy/proxy.go:176) in another goroutine
//     (client/control.go:146-161) than the one that handles the control stream's NewProxyResp
//     (pkg/msg/handler.go:170-188).
//
// So a CONNECT sent as soon as the proxy is listed online can get its 200 and then the end of the
// connection before any byte of a reply, since frps closes the user connection when the work
// connection ends (server/proxy/proxy.go:268-292). The product never routes on the listing: the
// front routes only after its own probe, a GET through the tunnel, has passed
// (internal/front/tunnel.go:150-157, probe.go:225; HBEP-6, The front's half). The tests wait the
// same way, with ProvenRoute (moved here from internal/launcher's tests, #369, for #371 and
// #372).

// ErrClosedBeforeReply is an early close: frps answered the CONNECT with 200, and the connection
// ended before any byte of the reply. That race ends this way, but an early close does not prove
// it: frpc also closes the work connection before any reply when its dial of the engine's socket
// fails (pkg/plugin/client/unix_domain_socket.go:52-57). A reset counts as well as a close: frps
// may close the user connection with the request still unread, and the kernel then resets it.
var ErrClosedBeforeReply = errors.New("frps answered the CONNECT with 200, then the connection ended before any byte of a reply")

// TCPMuxGet sends CONNECT host:80 to frps's tcpmux port at addr, as the front does (HBEP-6, The
// front's half), and on a 200 sends GET path through the tunnel. It returns the CONNECT's status
// line and, after a 200, the reply's status code and the first 64 bytes of its body, so a body
// compared with it must be shorter. Everything happens before deadline.
func TCPMuxGet(addr, host, path string, deadline time.Time) (status string, code int, body string, err error) {
	c, err := net.DialTimeout("tcp", addr, time.Until(deadline))
	if err != nil {
		return "", 0, "", err
	}
	defer c.Close()
	if err := c.SetDeadline(deadline); err != nil {
		return "", 0, "", err
	}
	if _, err := fmt.Fprintf(c, "CONNECT %s:80 HTTP/1.1\r\nHost: %s:80\r\n\r\n", host, host); err != nil {
		return "", 0, "", err
	}
	rd := bufio.NewReader(c)
	line, err := rd.ReadString('\n')
	status = strings.TrimSpace(line)
	if err != nil {
		return status, 0, "", fmt.Errorf("the CONNECT's status line: %w", err)
	}
	if !strings.Contains(status, " 200 ") {
		return status, 0, "", nil
	}
	for {
		l, err := rd.ReadString('\n')
		if err != nil {
			return status, 0, "", fmt.Errorf("the CONNECT's headers: %w", err)
		}
		if strings.TrimSpace(l) == "" {
			break
		}
	}
	if _, err := fmt.Fprintf(c, "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", path, host); err != nil {
		if endedEarly(err) {
			return status, 0, "", ErrClosedBeforeReply
		}
		return status, 0, "", fmt.Errorf("writing the request through the tunnel: %w", err)
	}
	if _, err := rd.Peek(1); err != nil {
		if endedEarly(err) {
			return status, 0, "", ErrClosedBeforeReply
		}
		return status, 0, "", fmt.Errorf("waiting for the reply through the tunnel: %w", err)
	}
	resp, err := http.ReadResponse(rd, nil)
	if err != nil {
		return status, 0, "", fmt.Errorf("the reply through the tunnel: %w", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	if err != nil {
		return status, resp.StatusCode, string(b), fmt.Errorf("the reply's body: %w", err)
	}
	return status, resp.StatusCode, string(b), nil
}

// endedEarly reports whether err is the end of the connection: a close, or a reset by the peer.
func endedEarly(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
}

// ProvenRoute waits until a CONNECT for host through frps's tcpmux port at addr, and a GET path
// through it, bring back a 200 with the body want: only a reply that crossed the tunnel proves the
// route, since frps lists the proxy online, and answers its CONNECT with 200, before frpc can
// serve it (above). It retries ErrClosedBeforeReply alone, on a new connection each time; any
// other outcome (a CONNECT status other than 200, a timeout, another error, another reply) ends it
// at once with that error. When bound runs out it returns the last error, so an early close that
// lasts (a socket that cannot be dialled) fails the wait. It returns how many attempts it made.
func ProvenRoute(addr, host, path, want string, bound time.Duration) (attempts int, err error) {
	var last error
	for end := time.Now().Add(bound); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		attempts++
		deadline := time.Now().Add(10 * time.Second)
		if deadline.After(end) {
			deadline = end
		}
		status, code, body, err := TCPMuxGet(addr, host, path, deadline)
		switch {
		case errors.Is(err, ErrClosedBeforeReply):
			last = err
			continue
		case err != nil:
			return attempts, fmt.Errorf("CONNECT %s: %q: %w", host, status, err)
		case !strings.Contains(status, " 200 "):
			return attempts, fmt.Errorf("CONNECT %s: %q, want 200", host, status)
		case code != http.StatusOK || body != want:
			return attempts, fmt.Errorf("CONNECT %s: the reply through the tunnel is %d %q, want 200 %q", host, code, body, want)
		}
		return attempts, nil
	}
	return attempts, fmt.Errorf("CONNECT %s: no reply crossed the tunnel in %d attempts within %s; the last: %v", host, attempts, bound, last)
}
