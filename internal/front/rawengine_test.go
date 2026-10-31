package front

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// rawEngine serves h on a loopback port of the test block, for the cases the
// engine double cannot play: a cut inside an event or a body, an echoed key,
// a split write. It reads the request body first, as an engine does.
func rawEngine(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	ln, err := enginetest.Listen(testPortLo, testPortHi)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		h(w, r)
	}), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

// abortAfter writes the status, the headers and raw, flushes, then closes
// the connection without ending the body.
func abortAfter(w http.ResponseWriter, status int, hdr map[string]string, raw string) {
	for k, v := range hdr {
		w.Header().Set(k, v)
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, raw)
	_ = http.NewResponseController(w).Flush()
	if hj, ok := w.(http.Hijacker); ok {
		if c, _, err := hj.Hijack(); err == nil {
			_ = c.Close()
			return
		}
	}
	panic(http.ErrAbortHandler)
}

// hijackConn returns the raw connection of a request, for writes the
// handler times itself.
func hijackConn(t *testing.T, w http.ResponseWriter) (net.Conn, *bufio.ReadWriter) {
	t.Helper()
	c, rw, err := w.(http.Hijacker).Hijack()
	if err != nil {
		t.Errorf("hijack: %v", err)
		panic(http.ErrAbortHandler)
	}
	return c, rw
}

// tryPost is errPost that returns the transport error instead of failing.
func (h *harness) tryPost(path, body string, hdr ...string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, h.url+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+h.clientKey)
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	return errClient.Do(req)
}

// waitRecord waits until the request's usage record is written and returns it.
func (h *harness) waitRecord(id string) {
	h.t.Helper()
	errWaitFor(h.t, "the usage record of "+id, func() bool {
		for _, r := range h.records() {
			if r.RequestID == id {
				return true
			}
		}
		return false
	})
}

// zeroCounts fails unless the deployment's group holds no request.
func (h *harness) zeroCounts(dep, inst string) {
	h.t.Helper()
	if n := h.scLoad(dep, inst); n != 0 {
		h.t.Errorf("%s still counts %d in flight after its only request ended", inst, n)
	}
	if total, members, open := h.scGroup(dep).Counts(); total != 0 || members != 0 || open != 0 {
		h.t.Errorf("counts total %d members %d open %d, want all 0", total, members, open)
	}
}
