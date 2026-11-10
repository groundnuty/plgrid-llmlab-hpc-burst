// Command s13fields sends every case of package cases through a router (or straight to the
// recorder, the control) and records what reached the workers, byte for byte.
//
//	s13fields -out DIR [-router PATH -policy NAME -label TEXT] [-base 29900]
//
// Ports: the two recorders listen on base and base+1, the router on base+2, its metrics on
// base+3, all on 127.0.0.1. Without -router the cases go to the first recorder directly, and the
// run passes only if every body arrives as the exact bytes sent.
//
// Exit codes: 0 a completed run (with -router, losses are findings, not failures); 1 the control
// saw a body that is not its bytes; 2 the harness could not run (the router did not start, a
// listener was not loopback, a port was taken).
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/cases"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/recorder"
)

func main() { os.Exit(runMain()) }

// errorf prints a message and returns the exit code, so that every deferred cleanup still runs.
func errorf(code int, format string, args ...any) int {
	fmt.Fprintf(os.Stderr, "s13fields: "+format+"\n", args...)
	return code
}

func runMain() int {
	out := flag.String("out", "", "directory for the results (created)")
	routerCmd := flag.String("router", "", "the vllm-router launcher; empty sends straight to a recorder")
	policy := flag.String("policy", "consistent_hash", "the router's --policy")
	label := flag.String("label", "", "the run's name in the report")
	base := flag.Int("base", 29900, "first of four ports, all inside 29900-29919")
	work := flag.String("work", "", "scratch directory for the router's log and HOME, never committed (created)")
	flag.Parse()
	if *out == "" {
		return errorf(2, "-out is required")
	}
	if *base < 29900 || *base+3 > 29919 {
		return errorf(2, "-base %d: the ports base..base+3 must lie inside 29900-29919", *base)
	}
	if *label == "" {
		*label = "direct to the recorder (control)"
		if *routerCmd != "" {
			*label = filepath.Base(*routerCmd) + " " + *policy
		}
	}
	if *routerCmd != "" && *work == "" {
		return errorf(2, "-work is required with -router")
	}
	for _, d := range []string{*out, *work} {
		if d == "" {
			continue
		}
		if err := os.MkdirAll(d, 0o700); err != nil {
			return errorf(2, "%v", err)
		}
	}

	rec := &recorder.Recorder{}
	var servers []*http.Server
	for i, name := range []string{"worker-a", "worker-b"} {
		ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(*base+i))
		if err != nil {
			return errorf(2, "recorder %s: %v", name, err)
		}
		srv := &http.Server{Handler: rec.Handler(name), ReadHeaderTimeout: 10 * time.Second}
		servers = append(servers, srv)
		go func() { _ = srv.Serve(ln) }()
	}
	defer func() {
		for _, s := range servers {
			_ = s.Shutdown(context.Background())
		}
	}()

	target := "http://127.0.0.1:" + strconv.Itoa(*base)
	stopRouter := func() {}
	// The router's path names this host's directories, so the record keeps only its last three parts.
	run := &Run{Label: *label, Policy: *policy, Router: lastParts(*routerCmd, 3), Started: time.Now().UTC().Format(time.RFC3339)}
	if *routerCmd != "" {
		r, err := startRouter(*routerCmd, *policy, *base, *work)
		if err != nil {
			return errorf(2, "router: %v", err)
		}
		defer r.stop(run) // a no-op once stopped below
		target = "http://127.0.0.1:" + strconv.Itoa(*base+2)
		if err := r.waitReady(target, rec); err != nil {
			return errorf(2, "router: %v (log: %s)", err, filepath.Join(*work, "router.log"))
		}
		listeners, err := r.listeners(29900, 29919)
		run.Listeners = listeners
		if err != nil {
			return errorf(2, "router listeners: %v", err)
		}
		stopRouter = func() { r.stop(run) }
	}

	client := &http.Client{Timeout: 60 * time.Second}
	for _, c := range cases.All() {
		for _, v := range c.Variants() {
			run.Results = append(run.Results, send(client, target, rec, v))
		}
	}
	stopRouter()
	run.Probes = probes(rec)
	if err := writeResults(*out, run); err != nil {
		return errorf(2, "writing results: %v", err)
	}
	fmt.Println(summaryLine(run))
	if *routerCmd == "" {
		for _, r := range run.Results {
			if !r.Identical {
				return errorf(1, "control: %s did not arrive as its bytes", r.Key)
			}
		}
	}
	return 0
}

// send posts one variant's exact bytes and compares them with what reached a worker.
func send(client *http.Client, target string, rec *recorder.Recorder, v cases.Variant) Result {
	res := Result{Key: v.Key(), Case: v.Case.Key(), Endpoint: string(v.Case.Endpoint), Stream: v.Stream, Note: v.Case.Note, Sent: string(v.Body)}
	req, err := http.NewRequest(http.MethodPost, target+string(v.Case.Endpoint), bytes.NewReader(v.Body))
	if err != nil {
		res.Error = err.Error()
		return res
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(recorder.CaseHeader, v.Key())
	req.Header.Set("x-session-id", "s13-"+v.Key()) // HBEP-5: the front sets it on every request
	resp, err := client.Do(req)
	if err != nil {
		res.Error = err.Error()
	} else {
		body, rerr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		res.Status = resp.StatusCode
		if rerr != nil {
			res.Error = "reading the response: " + rerr.Error()
		}
		if v.Stream {
			res.StreamDone = bytes.Contains(body, []byte("data: [DONE]"))
		}
		if resp.StatusCode != http.StatusOK {
			res.Response = truncate(string(body), 400)
		}
	}
	arrivals := rec.ByCase(v.Key())
	res.Arrivals = len(arrivals)
	if len(arrivals) > 0 {
		a := arrivals[0]
		res.Worker = a.Listener
		res.Received = string(a.Body)
		res.WorkerContentType = a.Header.Get("Content-Type")
		res.SessionHeader = a.Header.Get("x-session-id") != ""
	}
	res.compare(v)
	return res
}

// lastParts returns the last n elements of a path.
func lastParts(p string, n int) string {
	parts := strings.Split(filepath.Clean(p), string(filepath.Separator))
	if len(parts) > n {
		parts = parts[len(parts)-n:]
	}
	return filepath.Join(parts...)
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
