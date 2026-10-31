package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// runProbe writes one CSV line per probe and per change of the proxy's state in frps's admin API:
//
//	probe,<start epoch>,<end epoch>,<outcome>,<detail>
//	api,<epoch>,<epoch>,<status>,<proxy name>
//
// Outcomes: ok (200 from the engine's /health through the tunnel); refused (frps answered the
// CONNECT with a non-200 status: no route); closed (the connection ended without a reply, e.g.
// frps gave up waiting for a work connection); timeout (no reply within -timeout); dial-error.
// Probes start every -interval and run concurrently, so a hung probe never delays the next one.
func runProbe(args []string) error {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	target := fs.String("target", "", "frps tcpmux CONNECT port (127.0.0.1:port)")
	mux := fs.String("mux", "inst", "tcpmux host")
	path := fs.String("path", "/health", "engine path to GET")
	interval := fs.Duration("interval", 200*time.Millisecond, "probe interval")
	timeout := fs.Duration("timeout", 15*time.Second, "probe timeout")
	duration := fs.Duration("duration", 90*time.Second, "total run time")
	api := fs.String("api", "", "frps admin API URL listing tcpmux proxies (empty = no polling)")
	apiUser := fs.String("api-user", "admin", "admin API user")
	apiPassFile := fs.String("api-pass-file", "", "file holding the admin API password")
	apiEvery := fs.Duration("api-interval", 100*time.Millisecond, "admin API poll interval")
	proxy := fs.String("proxy", "inst.engine", "proxy name to follow in the API")
	out := fs.String("out", "", "CSV output path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireLoopback(*target); err != nil {
		return err
	}
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	var mu sync.Mutex
	emit := func(kind string, a, b time.Time, outcome, detail string) {
		mu.Lock()
		defer mu.Unlock()
		detail = strings.NewReplacer(",", ";", "\n", " ", "\r", " ").Replace(detail)
		fmt.Fprintf(f, "%s,%.3f,%.3f,%s,%s\n", kind, epoch(a), epoch(b), outcome, detail)
		_ = f.Sync()
	}
	stop := time.Now().Add(*duration)
	var wg sync.WaitGroup
	if *api != "" {
		pass := ""
		if *apiPassFile != "" {
			b, err := os.ReadFile(*apiPassFile)
			if err != nil {
				return err
			}
			pass = strings.TrimSpace(string(b))
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := ""
			hc := &http.Client{Timeout: 2 * time.Second}
			for time.Now().Before(stop) {
				t := time.Now()
				st := apiStatus(hc, *api, *apiUser, pass, *proxy)
				if st != last {
					emit("api", t, time.Now(), st, *proxy)
					last = st
				}
				time.Sleep(time.Until(t.Add(*apiEvery)))
			}
		}()
	}
	for t := time.Now(); t.Before(stop); t = t.Add(*interval) {
		time.Sleep(time.Until(t))
		wg.Add(1)
		go func(start time.Time) {
			defer wg.Done()
			outcome, detail := probeOnce(*target, *mux, *path, *timeout)
			emit("probe", start, time.Now(), outcome, detail)
		}(time.Now())
	}
	wg.Wait()
	return nil
}

func epoch(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

func probeOnce(target, host, path string, timeout time.Duration) (string, string) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c, err := dialPath(ctx, target, host)
	if err != nil {
		var ref *errRefused
		switch {
		case errors.As(err, &ref):
			return "refused", ref.status
		case ctx.Err() != nil:
			return "timeout", "during CONNECT"
		case strings.Contains(err.Error(), "CONNECT"):
			return "closed", err.Error()
		default:
			return "dial-error", err.Error()
		}
	}
	defer c.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	}
	if _, err := fmt.Fprintf(c, "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", path, host); err != nil {
		return "closed", err.Error()
	}
	status, err := readHeaderBlock(c)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return "timeout", "no reply"
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return "closed", "EOF before reply"
		}
		return "closed", err.Error()
	}
	if strings.Contains(status, " 200") {
		return "ok", status
	}
	return "bad-status", status
}

// apiStatus returns the proxy's status from frps's admin API ("online", "offline", "absent"), or
// "api-error" when the API does not answer.
func apiStatus(hc *http.Client, url, user, pass, name string) string {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "api-error"
	}
	req.SetBasicAuth(user, pass)
	resp, err := hc.Do(req)
	if err != nil {
		return "api-error"
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Sprintf("api-%d", resp.StatusCode)
	}
	var body struct {
		Proxies []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"proxies"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "api-bad-json"
	}
	for _, p := range body.Proxies {
		if p.Name == name {
			return p.Status
		}
	}
	return "absent"
}
