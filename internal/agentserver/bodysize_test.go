package agentserver

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// paddedReport is a valid Report of exactly n bytes: JSON allows white space
// after the value.
func paddedReport(t *testing.T, n int) []byte {
	t.Helper()
	b := reportBody(t, agentapi.Report{Time: time.Unix(1, 0).UTC()})
	if len(b) > n {
		t.Fatalf("a report is %d bytes, above %d", len(b), n)
	}
	return append(b, bytes.Repeat([]byte(" "), n-len(b))...)
}

func TestAReportOfExactlyTheLimitIsAccepted(t *testing.T) {
	x := newRig(t).serve()
	x.register(x.good())
	resp, body := x.do(http.MethodPost, agentapi.PathReport, x.good(), paddedReport(t, agentapi.MaxBodyBytes))
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d (%s), want 204", resp.StatusCode, body)
	}
}

// The client must read the 400: a server that answers and closes with the body
// unread resets the connection, and the client sees an error instead. The
// bodies are not far above the limit (what a buggy agent sends) and well above
// it (what the server reads and discards).
func TestAnOversizeReportGetsA400TheClientCanRead(t *testing.T) {
	for name, size := range map[string]int{
		"one byte over": agentapi.MaxBodyBytes + 1,
		"1 MiB over":    agentapi.MaxBodyBytes + 1<<20,
		"2x the limit":  2 * agentapi.MaxBodyBytes,
	} {
		t.Run(name, func(t *testing.T) {
			x := newRig(t).serve()
			x.register(x.good())
			resp, body := x.do(http.MethodPost, agentapi.PathReport, x.good(), paddedReport(t, size))

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d (%s), want 400", resp.StatusCode, body)
			}
			if e := errorOf(t, resp.StatusCode, resp.Header, body); e.Code != agentapi.CodeBadRequest {
				t.Errorf("code = %q, want %q", e.Code, agentapi.CodeBadRequest)
			}
			if !resp.Close {
				t.Errorf("the answer does not close the connection (Connection: %q)", resp.Header.Get("Connection"))
			}
			if n := x.backend.Calls("Report"); n != 0 {
				t.Errorf("Backend.Report calls = %d, want 0", n)
			}
		})
	}
}

func TestAnOversizeRegistrationGetsA400TheClientCanRead(t *testing.T) {
	x := newRig(t).serve()
	resp, body := x.do(http.MethodPost, agentapi.PathRegister, x.good(), bytes.Repeat([]byte(" "), agentapi.MaxBodyBytes+1))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d (%s), want 400", resp.StatusCode, body)
	}
	x.wantBackendCalls(0)
}

func TestAMalformedBodyKeepsTheConnectionOpen(t *testing.T) {
	x := newRig(t).serve()
	resp, _ := x.do(http.MethodPost, agentapi.PathRegister, x.good(), []byte("{"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if resp.Close {
		t.Error("a body that was read to its end closed the connection")
	}
}

// zeros is an endless body that counts what is read from it.
type zeros struct{ n int64 }

func (z *zeros) Read(p []byte) (int, error) {
	clear(p)
	z.n += int64(len(p))
	return len(p), nil
}

// A body above the limit is read past it and discarded up to a bound, never to
// the end: an endless body must not hold a handler.
func TestARefusedBodyIsDiscardedUpToABoundAndTheAnswerClosesTheConnection(t *testing.T) {
	x := newRig(t)
	x.register(x.good())
	body := &zeros{}
	req := httptest.NewRequest(http.MethodPost, agentapi.PathReport, body)
	x.good().apply(req.Header)
	rec := httptest.NewRecorder()
	x.srv.Handler().ServeHTTP(rec, req)

	wantError(t, rec, agentapi.CodeBadRequest)
	if got := rec.Header().Get("Connection"); got != "close" {
		t.Errorf("Connection = %q, want close", got)
	}
	read := body.n
	if read <= agentapi.MaxBodyBytes+1 {
		t.Errorf("read %d bytes, want the rest of the body discarded beyond the limit", read)
	}
	if read > agentapi.MaxBodyBytes+1+3*agentapi.MaxBodyBytes {
		t.Errorf("read %d bytes of an endless body, want a bound", read)
	}
}
