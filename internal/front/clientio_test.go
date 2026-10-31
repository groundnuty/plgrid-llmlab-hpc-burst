package front

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// PR #95 review (Astra B9, Opus's stalled-client nit): a client that stops
// sending its body, or stops reading the response, must not hold the
// handler, its reservation and its usage record past the front's own cut.

const ciMaxReq = 10 * time.Second

func ciFix(t *testing.T) *scFix {
	return newScFix(t, scOpts{cfg: func(c *Config) {
		c.MaxRequestDuration = Duration(ciMaxReq)
		c.writeGrace = 200 * time.Millisecond
	}})
}

// ciDial opens a raw client connection to the front.
func ciDial(t *testing.T, f *scFix) *net.TCPConn {
	t.Helper()
	c, err := net.Dial("tcp", strings.TrimPrefix(f.url, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c.(*net.TCPConn)
}

// TestStalledUploadIsCutAtTheDeadline: the client announces 1,000 body bytes,
// sends 10 and stalls. At maxRequestDuration the front answers 504
// request_timeout, detail deadline, and writes the record.
func TestStalledUploadIsCutAtTheDeadline(t *testing.T) {
	f := ciFix(t)
	base := f.clk.Pending()
	c := ciDial(t, f)
	fmt.Fprintf(c, "POST %s HTTP/1.1\r\nHost: front\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{\"model\":", scChat, f.clientKey)
	errWaitFor(t, "the handler to start (its deadline timer)", func() bool { return f.clk.Pending() == base+1 })
	f.clk.Advance(ciMaxReq)
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("no response after the deadline: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 504 {
		t.Fatalf("status %d, want 504: %s", resp.StatusCode, body)
	}
	checkErrorBody(t, string(body), codeRequestTimeout, "deadline")
	id := resp.Header.Get("hpcburst-request-id")
	f.waitRecord(id)
	if rec := f.record(id); rec.CutBy == nil || *rec.CutBy != cutFrontDeadline {
		t.Errorf("cutBy %v, want front_deadline", rec.CutBy)
	}
}

// TestStalledDownloadIsCutAtTheDeadline: the client reads the headers and
// stops reading while the engine floods the stream, so the front blocks
// writing to it. At maxRequestDuration the handler must end: the record is
// written with cutBy front_deadline and the reservation is released.
func TestStalledDownloadIsCutAtTheDeadline(t *testing.T) {
	f := ciFix(t)
	var written atomic.Int64
	f.retarget(f.uids[0], rawEngine(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		rc := http.NewResponseController(w)
		for r.Context().Err() == nil {
			n, err := io.WriteString(w, chatToken)
			if err != nil {
				return
			}
			written.Add(int64(n))
			_ = rc.Flush()
		}
	}))
	c := ciDial(t, f)
	_ = c.SetReadBuffer(4096)
	body := prChatStream
	fmt.Fprintf(c, "POST %s HTTP/1.1\r\nHost: front\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", scChat, f.clientKey, len(body), body)
	br := bufio.NewReaderSize(c, 4096)
	line, err := br.ReadString('\n')
	if err != nil || !strings.Contains(line, "200") {
		t.Fatalf("status line %q: %v", line, err)
	}
	// The client reads nothing more. Wait until the flood stops: the front
	// is blocked writing to the client and no longer reads the engine.
	last := int64(-1)
	errWaitFor(t, "the front to block on the stalled client", func() bool {
		n := written.Load()
		stalled := n == last && n > 0
		last = n
		time.Sleep(200 * time.Millisecond)
		return stalled
	})
	if n := f.scLoad(scDep, f.names[0]); n != 1 {
		t.Fatalf("the Instance counts %d in flight while the stream is stalled, want 1", n)
	}
	f.clk.Advance(ciMaxReq)
	errWaitFor(t, "the usage record after the deadline", func() bool { return len(f.records()) == 1 })
	if rec := f.records()[0]; rec.CutBy == nil || *rec.CutBy != cutFrontDeadline {
		t.Errorf("cutBy %v, want front_deadline", rec.CutBy)
	}
	f.zeroCounts(scDep, f.names[0])
}
