package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestLongestSilence(t *testing.T) {
	ms := time.Millisecond
	cases := []struct {
		name string
		at   []time.Duration
		want time.Duration
	}{
		{"no token is the whole window", nil, 1000 * ms},
		{"before the first", []time.Duration{700 * ms, 800 * ms, 990 * ms}, 700 * ms},
		{"between two", []time.Duration{100 * ms, 600 * ms, 990 * ms}, 500 * ms},
		{"after the last, unsorted", []time.Duration{300 * ms, 0, 100 * ms}, 700 * ms},
	}
	for _, c := range cases {
		if got := longestSilence(c.at, 1000*ms); got != c.want {
			t.Errorf("%s: longestSilence = %v, want %v", c.name, got, c.want)
		}
	}
}

// A tunnel that carries no stream in the window must show it: zero tokens,
// every bucket empty and the whole window silent. Its requests outside the
// window are not counted as its window's.
func TestSummarizeLoadShowsATunnelThatCarriedNothing(t *testing.T) {
	w0 := time.Unix(1000, 0)
	wEnd := w0.Add(3 * time.Second)
	tok := func(start time.Time, n int, every time.Duration) []time.Duration {
		var out []time.Duration
		for i := 0; i < n; i++ {
			out = append(out, time.Duration(i+1)*every)
		}
		return out
	}
	results := []*streamResult{
		// Tunnel 0: one request in the window, 3 s of tokens every 100 ms.
		{Tunnel: 0, Start: w0, Tokens: tok(w0, 29, 100*time.Millisecond), Status: 200, Finished: true, Done: true, WantToken: 29},
		// Tunnel 1: a request that started and ended before the window.
		{Tunnel: 1, Start: w0.Add(-5 * time.Second), Tokens: tok(w0, 10, 100*time.Millisecond), Status: 200, Finished: true, Done: true, WantToken: 10},
	}
	results[0].TTFT = results[0].Tokens[0]
	s := summarizeLoad(results, make([][]probeResult, 2), 2, w0, wEnd, time.Second)
	if s.Requests != 1 || s.Completed != 1 || s.AllRequests != 2 {
		t.Fatalf("requests %d, completed %d, all %d; want 1, 1, 2", s.Requests, s.Completed, s.AllRequests)
	}
	t0, t1 := s.PerTunnel[0], s.PerTunnel[1]
	if t0.Tokens != 29 || len(t0.Buckets) != 3 || t0.Buckets[0] != 9 || t0.Buckets[1] != 10 || t0.Buckets[2] != 10 {
		t.Errorf("tunnel 0: tokens %d, buckets %v; want 29, [9 10 10]", t0.Tokens, t0.Buckets)
	}
	if t0.LongestSilenceMs != 100 {
		t.Errorf("tunnel 0: longest silence %v ms, want 100", t0.LongestSilenceMs)
	}
	if t1.Tokens != 0 || t1.Requests != 0 || t1.Buckets[0]+t1.Buckets[1]+t1.Buckets[2] != 0 || t1.LongestSilenceMs != 3000 {
		t.Errorf("tunnel 1: tokens %d, requests %d, buckets %v, silence %v; want 0, 0, empty, 3000", t1.Tokens, t1.Requests, t1.Buckets, t1.LongestSilenceMs)
	}
	if s.GapMs.N != 28 || s.GapMs.P50 != 100 {
		t.Errorf("gaps: %+v, want 28 gaps of 100 ms", s.GapMs)
	}
}

// An incomplete request started in the window is a failure, named by its
// error, and its gaps are not counted.
func TestSummarizeLoadCountsACutStream(t *testing.T) {
	w0 := time.Unix(1000, 0)
	r := &streamResult{Tunnel: 0, Start: w0.Add(time.Second), Tokens: []time.Duration{50 * time.Millisecond, 100 * time.Millisecond},
		Status: 200, Err: errors.New("cut: EOF before [DONE]"), WantToken: 60}
	s := summarizeLoad([]*streamResult{r}, make([][]probeResult, 1), 1, w0, w0.Add(2*time.Second), time.Second)
	if s.Failed != 1 || s.Errors["cut: EOF before [DONE]"] != 1 || s.GapMs.N != 0 || s.PerTunnel[0].Tokens != 2 {
		t.Fatalf("failed %d, errors %v, gaps %d, tokens %d; want 1, the cut, 0, 2", s.Failed, s.Errors, s.GapMs.N, s.PerTunnel[0].Tokens)
	}
}

func TestReadEventsCountsContentTokensOnly(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"b\"},\"finish_reason\":\"length\"}]}\n\n" +
		"data: [DONE]\n\n"
	res := &streamResult{Start: time.Now(), WantToken: 2, Status: 200}
	if err := readEvents(bufio.NewReader(strings.NewReader(body)), res); err != nil {
		t.Fatal(err)
	}
	if len(res.Tokens) != 2 || !res.Finished || !res.Done || !res.Complete() {
		t.Fatalf("tokens %d, finished %v, done %v; want 2, true, true", len(res.Tokens), res.Finished, res.Done)
	}
	cut := &streamResult{Start: time.Now(), WantToken: 2, Status: 200}
	if err := readEvents(bufio.NewReader(strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n")), cut); err == nil {
		t.Fatal("a body that ends before [DONE] was not reported as cut")
	}
}

func TestSelfSignedVerifiesForTheServerName(t *testing.T) {
	certPEM, _, err := selfSigned(DialHomeServerName, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(certPEM)
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(c)
	if _, err := c.Verify(x509.VerifyOptions{Roots: pool, DNSName: DialHomeServerName}); err != nil {
		t.Errorf("the certificate does not verify for %s against itself: %v", DialHomeServerName, err)
	}
	if _, err := c.Verify(x509.VerifyOptions{Roots: pool, DNSName: "other.invalid"}); err == nil {
		t.Error("the certificate verified for another name")
	}
}

// The listener passes frpc's websocket upgrade to frps with the request line
// unchanged, over TLS, and answers 404 to /agent/v1/ and to anything else.
func TestDialHomePassesTheUpgradeAndRefusesTheRest(t *testing.T) {
	frps, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer frps.Close()
	first := make(chan string, 1)
	go func() {
		c, err := frps.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		line, _ := bufio.NewReader(c).ReadString('\n')
		first <- line
	}()
	certPEM, keyPEM, err := selfSigned(DialHomeServerName, time.Now())
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
	frp := newFrpPassthrough(frps.Addr().String(), time.Second, slog.New(slog.DiscardHandler))
	srv := &http.Server{Handler: dialHome(frp), ReadHeaderTimeout: time.Minute}
	go func() {
		_ = srv.Serve(tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}))
	}()
	defer func() { _ = srv.Close(); frp.Close() }()

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	dial := func() *tls.Conn {
		c, err := tls.Dial("tcp4", ln.Addr().String(), &tls.Config{RootCAs: pool, ServerName: DialHomeServerName})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	status := func(req string) string {
		c := dial()
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.WriteString(c, req); err != nil {
			t.Fatal(err)
		}
		line, _ := bufio.NewReader(c).ReadString('\n')
		return strings.TrimSpace(line)
	}
	if got := status("GET /agent/v1/register HTTP/1.1\r\nHost: x\r\n\r\n"); !strings.Contains(got, "404") {
		t.Errorf("/agent/v1/: %q, want 404", got)
	}
	if got := status("GET /other HTTP/1.1\r\nHost: x\r\n\r\n"); !strings.Contains(got, "404") {
		t.Errorf("another path: %q, want 404", got)
	}
	c := dial()
	defer c.Close()
	if _, err := io.WriteString(c, "GET /~!frp HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case line := <-first:
		if line != "GET /~!frp HTTP/1.1\r\n" {
			t.Errorf("frps's first line: %q, want the upgrade's request line unchanged", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("frps saw no connection for the upgrade")
	}
}
