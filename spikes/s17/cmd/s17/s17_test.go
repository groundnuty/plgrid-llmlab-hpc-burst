package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// fakeFrps accepts connections and reports each one's first line.
func fakeFrps(t *testing.T) (string, <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	lines := make(chan string, 4)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				line, _ := bufio.NewReader(c).ReadString('\n')
				lines <- line
			}()
		}
	}()
	return ln.Addr().String(), lines
}

// The listener sends a websocket made for the bulk server name to the second
// frps, and every other one to the first; the passthrough itself is unchanged.
func TestDialHomeRoutesTheBulkNameToTheSecondFrps(t *testing.T) {
	mainAddr, mainLines := fakeFrps(t)
	bulkAddr, bulkLines := fakeFrps(t)
	certPEM, keyPEM, err := selfSigned([]string{DialHomeServerName, BulkServerName}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	discard := slog.New(slog.DiscardHandler)
	m, b := newFrpPassthrough(mainAddr, time.Second, discard), newFrpPassthrough(bulkAddr, time.Second, discard)
	srv := &http.Server{Handler: dialHome(m, b), ReadHeaderTimeout: time.Minute}
	go func() {
		_ = srv.Serve(tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}))
	}()
	defer func() { _ = srv.Close(); m.Close(); b.Close() }()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	upgrade := func(name string) {
		c, err := tls.Dial("tcp4", ln.Addr().String(), &tls.Config{RootCAs: pool, ServerName: name})
		if err != nil {
			t.Fatalf("TLS for %s: %v", name, err)
		}
		t.Cleanup(func() { c.Close() })
		if _, err := io.WriteString(c, "GET /~!frp HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
	}
	expect := func(lines <-chan string, what string) {
		select {
		case l := <-lines:
			if l != "GET /~!frp HTTP/1.1\r\n" {
				t.Errorf("%s got %q", what, l)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s saw no upgrade", what)
		}
	}
	upgrade(BulkServerName)
	expect(bulkLines, "the second frps")
	upgrade(DialHomeServerName)
	expect(mainLines, "the first frps")
	select {
	case l := <-bulkLines:
		t.Fatalf("the second frps also got %q", l)
	case l := <-mainLines:
		t.Fatalf("the first frps got a second upgrade %q", l)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestUploadTakesTheReceiversTimes(t *testing.T) {
	var got int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first := time.Now().UnixNano()
		n, _ := io.Copy(io.Discard, r.Body)
		got = n
		_ = json.NewEncoder(w).Encode(map[string]int64{"bytes": n, "first_byte_unix_ns": first, "last_byte_unix_ns": time.Now().UnixNano()})
	}))
	defer s.Close()
	t0 := time.Now()
	rel := func(x time.Time) int64 { return x.Sub(t0).Nanoseconds() }
	tr := doUpload(s.Client(), s.URL, 5<<20, rel)
	if tr.Err != "" || tr.Bytes != 5<<20 || got != 5<<20 {
		t.Fatalf("upload: %+v (server saw %d)", tr, got)
	}
	if !(tr.StartNs <= tr.RecvFirstNs && tr.RecvFirstNs <= tr.RecvLastNs && tr.RecvLastNs <= tr.EndNs && tr.SentNs > 0) {
		t.Fatalf("times out of order: %+v", tr)
	}
}

func TestDownloadCountsEveryByteAtTheClient(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("bytes"))
		w.Header().Set("Content-Length", strconv.Itoa(n))
		_, _ = w.Write(make([]byte, n))
	}))
	defer s.Close()
	t0 := time.Now()
	tr := doDownload(s.Client(), s.URL, 3<<20+5, func(x time.Time) int64 { return x.Sub(t0).Nanoseconds() })
	if tr.Err != "" || tr.Bytes != 3<<20+5 || tr.RecvFirstNs > tr.RecvLastNs || tr.Status != 200 {
		t.Fatalf("download: %+v", tr)
	}
	// A reply shorter than asked is a failed transfer, not a fast one.
	shortSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 10))
	}))
	defer shortSrv.Close()
	short := doDownload(shortSrv.Client(), shortSrv.URL, 20, func(x time.Time) int64 { return x.Sub(t0).Nanoseconds() })
	if short.Err == "" || short.Bytes != 10 {
		t.Fatalf("10 of 20 bytes: %+v; want an error naming the short count", short)
	}
}

func TestCountingBodyGivesExactlyItsSizeAndSaysWhenDone(t *testing.T) {
	calls := 0
	b := &countingBody{left: 100_001, done: func() { calls++ }}
	n, err := io.Copy(io.Discard, b)
	if err != nil || n != 100_001 || calls != 1 {
		t.Fatalf("read %d (%v), done called %d times; want 100001 and once", n, err, calls)
	}
}

func TestStepperRecordsCumulativeBytesPerStep(t *testing.T) {
	var s stepper
	t0 := time.Unix(10, 0)
	s.add(t0, 100)
	s.add(t0.Add(50*time.Millisecond), 50)
	s.add(t0.Add(350*time.Millisecond), 10) // steps 1 and 2 carry the total so far
	want := []int64{150, 150, 150, 160}
	if len(s.steps) != len(want) {
		t.Fatalf("steps %v, want %v", s.steps, want)
	}
	for i := range want {
		if s.steps[i] != want[i] {
			t.Fatalf("steps %v, want %v", s.steps, want)
		}
	}
}
