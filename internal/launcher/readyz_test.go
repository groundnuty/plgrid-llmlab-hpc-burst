package launcher

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

// rawAnswer is one path's answer from rawServer: a status line and headers that promise a body of
// 1,000 bytes, a few bytes of it, and then either a closed connection (truncated) or a connection
// held open until the test ends (stalled).
type rawAnswer struct {
	code    int
	stalled bool
}

// rawServer serves GET paths from answers over a hand-written HTTP/1.1 response, which a Go
// handler would not write: a body that is shorter than its Content-Length, or that never ends.
// A path it has no answer for gets a 404 with a complete body. It returns its loopback address.
func rawServer(t *testing.T, answers map[string]rawAnswer) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	t.Cleanup(func() { close(done); l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				line, err := r.ReadString('\n')
				if err != nil {
					return
				}
				for { // the request's headers, up to the empty line
					h, err := r.ReadString('\n')
					if err != nil || strings.TrimSpace(h) == "" {
						break
					}
				}
				f := strings.Fields(line)
				if len(f) < 2 {
					return
				}
				a, ok := answers[f[1]]
				if !ok {
					_, _ = c.Write([]byte("HTTP/1.1 404 Not Found\r\nContent-Length: 19\r\nConnection: close\r\n\r\n404 page not found\n"))
					return
				}
				_, _ = c.Write([]byte("HTTP/1.1 " + strings.TrimSpace(statusLine(a.code)) + "\r\nContent-Length: 1000\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\npartial"))
				if a.stalled {
					<-done
				}
			}()
		}
	}()
	return l.Addr().String()
}

func statusLine(code int) string {
	switch code {
	case 200:
		return "200 OK"
	case 404:
		return "404 Not Found"
	default:
		return "500 Internal Server Error"
	}
}

// TestReadinessIsReadFromTheStatusAlone: a body that is cut short, or never ends, changes nothing:
// the status code from the headers decides, for 200, 500 and 404 on each path
// (`@hpc-burst-science-agent`, 1 Nov 2026, #147; PR 187 review, B2). A probe that read the body
// would lose the status of a truncated answer (an empty state) and would hold a stalled one until
// its timeout.
func TestReadinessIsReadFromTheStatusAlone(t *testing.T) {
	const caches, leading, aggregate = "/readyz/caches", "/readyz/leading", "/readyz"
	for _, c := range []struct {
		name    string
		answers map[string]rawAnswer
		state   string
		problem string // a part of the one problem; empty when ready
	}{
		{"caches 500, truncated", map[string]rawAnswer{caches: {500, false}}, ReadyzStarting, "starting: GET /readyz/caches"},
		{"caches 500, stalled", map[string]rawAnswer{caches: {500, true}}, ReadyzStarting, "starting: GET /readyz/caches"},
		{"caches 200 stalled, leading 500 truncated", map[string]rawAnswer{caches: {200, true}, leading: {500, false}}, ReadyzNotLeading, "not leading: GET /readyz/leading"},
		{"caches 200 truncated, leading 500 stalled", map[string]rawAnswer{caches: {200, false}, leading: {500, true}}, ReadyzNotLeading, "not leading: GET /readyz/leading"},
		{"both 200, truncated and stalled", map[string]rawAnswer{caches: {200, false}, leading: {200, true}}, ReadyzReady, ""},
		{"both 200, stalled and truncated", map[string]rawAnswer{caches: {200, true}, leading: {200, false}}, ReadyzReady, ""},
		{"caches 404 truncated, aggregate 200 stalled: an older controller, ready", map[string]rawAnswer{caches: {404, false}, aggregate: {200, true}}, ReadyzReady, ""},
		{"caches 404 stalled, aggregate 500 truncated: the fallback text", map[string]rawAnswer{caches: {404, true}, aggregate: {500, false}}, ReadyzUnspecified, "GET /readyz on"},
		{"leading 404 truncated, aggregate 500 stalled: the fallback text", map[string]rawAnswer{caches: {200, false}, leading: {404, false}, aggregate: {500, true}}, ReadyzUnspecified, "GET /readyz on"},
		{"leading 404 stalled, aggregate 200 truncated: ready", map[string]rawAnswer{caches: {200, true}, leading: {404, true}, aggregate: {200, false}}, ReadyzReady, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			state, problems := ReadReadiness(rawServer(t, c.answers))
			if state != c.state {
				t.Fatalf("state %q, problems %v; want state %q", state, problems, c.state)
			}
			switch {
			case c.problem == "" && len(problems) != 0:
				t.Fatalf("problems %v, want none", problems)
			case c.problem != "" && (len(problems) != 1 || !strings.Contains(problems[0], c.problem)):
				t.Fatalf("problems %v, want one with %q", problems, c.problem)
			}
		})
	}
}
