package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net"
	"os"
	"regexp"
	"sync"
	"time"
)

// A plugin's call latency, timed at its server and the same way for both of
// frps's plugins: a listener whose connections stamp the first byte of each
// request they read and the first write of the reply that follows. HTTP/1.1
// on one connection carries one call at a time (frps's Go client does not
// pipeline), so the bytes between two replies are one request. The op is read
// from the request line's query (frps sends it there and in the body), and the
// verdict from the reply's JSON.

// callRec is one plugin call as its server saw it.
type callRec struct {
	Plugin  string `json:"plugin"`
	StartNs int64  `json:"start_ns"` // the request's first byte, Unix ns
	EndNs   int64  `json:"end_ns"`   // the reply's first write, Unix ns
	Op      string `json:"op"`
	Proxy   string `json:"proxy,omitempty"` // proxy_name, for NewProxy and CloseProxy
	Reject  bool   `json:"reject"`
	Reason  string `json:"reason,omitempty"`
	Status  string `json:"status"` // the reply's status line's code
}

// callLog appends one JSON line per call; Flush writes them out.
type callLog struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

func openCallLog(path string) (*callLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &callLog{f: f, w: bufio.NewWriterSize(f, 64<<10)}, nil
}

func (l *callLog) add(r callRec) {
	b, _ := json.Marshal(r)
	l.mu.Lock()
	_, _ = l.w.Write(append(b, '\n'))
	l.mu.Unlock()
}

func (l *callLog) Flush() {
	l.mu.Lock()
	_ = l.w.Flush()
	l.mu.Unlock()
}

func (l *callLog) Close() error {
	l.Flush()
	return l.f.Close()
}

// flushEvery flushes the log every d until stop is closed.
func (l *callLog) flushEvery(d time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			l.Flush()
		case <-stop:
			return
		}
	}
}

type timedListener struct {
	net.Listener
	plugin string
	log    *callLog
	now    func() time.Time
}

func newTimedListener(ln net.Listener, plugin string, log *callLog) *timedListener {
	return &timedListener{Listener: ln, plugin: plugin, log: log, now: time.Now}
}

func (l *timedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &timedConn{Conn: c, l: l}, nil
}

type timedConn struct {
	net.Conn
	l *timedListener

	mu    sync.Mutex
	busy  bool
	start time.Time
	head  []byte // the request's first bytes, for its op
}

// headKeep bounds the bytes kept of a request, in memory only: they hold the
// session's token, which is never logged.
const headKeep = 4096

func (c *timedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		now := c.l.now()
		c.mu.Lock()
		if !c.busy {
			c.busy, c.start, c.head = true, now, c.head[:0]
		}
		if room := headKeep - len(c.head); room > 0 {
			c.head = append(c.head, p[:min(n, room)]...)
		}
		c.mu.Unlock()
	}
	return n, err
}

func (c *timedConn) Write(p []byte) (int, error) {
	now := c.l.now()
	c.mu.Lock()
	if c.busy {
		c.busy = false
		rec := callRec{Plugin: c.l.plugin, StartNs: c.start.UnixNano(), EndNs: now.UnixNano(), Op: opOf(c.head), Proxy: proxyOf(c.head)}
		rec.Status, rec.Reject, rec.Reason = verdictOf(p)
		c.mu.Unlock()
		c.l.log.add(rec)
	} else {
		c.mu.Unlock()
	}
	return c.Conn.Write(p)
}

var (
	opQuery  = regexp.MustCompile(`[?&]op=([A-Za-z]+)`)
	opBody   = regexp.MustCompile(`"op"\s*:\s*"([A-Za-z]+)"`)
	reasonRE = regexp.MustCompile(`"reject_reason"\s*:\s*"((?:[^"\\]|\\.)*)"`)
	rejectRE = regexp.MustCompile(`"reject"\s*:\s*true`)
	proxyRE  = regexp.MustCompile(`"proxy_name"\s*:\s*"([^"\\]*)"`)
)

// proxyOf is the call's proxy_name, when its first bytes hold one.
func proxyOf(head []byte) string {
	if m := proxyRE.FindSubmatch(head); m != nil {
		return string(m[1])
	}
	return ""
}

// opOf is the call's op: from the request line's query, else from the body.
func opOf(head []byte) string {
	line, _, _ := bytes.Cut(head, []byte("\r\n"))
	if m := opQuery.FindSubmatch(line); m != nil {
		return string(m[1])
	}
	if m := opBody.FindSubmatch(head); m != nil {
		return string(m[1])
	}
	return ""
}

// verdictOf reads the reply's status code and the plugin's verdict.
func verdictOf(reply []byte) (status string, reject bool, reason string) {
	line, _, _ := bytes.Cut(reply, []byte("\r\n"))
	if f := bytes.Fields(line); len(f) >= 2 {
		status = string(f[1])
	}
	reject = rejectRE.Match(reply)
	if m := reasonRE.FindSubmatch(reply); m != nil {
		reason = string(m[1])
	}
	return status, reject, reason
}
