package agent

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

// Forwarder serves an Instance's serving path on the agent's own Unix socket
// and forwards it to the engine worker's socket (HBEP-7, What it runs: "The
// agent forwards the Instance's serving path to that socket"; HBEP-6). In
// alpha the target is worker-0's engine socket; once Instances have a router,
// the target becomes the router's socket and nothing else here changes.
//
// HBEP-6's tunnel carries the front's requests to this socket: frpc's
// tcpmux proxy for the Instance's UID serves it through frp's
// unix_domain_socket plugin (Frpc, ServingSocket).
//
// The proxy streams: it flushes every write at once (FlushInterval -1) and
// sets no response deadline, because the front allows long streams (spike S2
// phase A found a 1,800 s cut on a router path and kept 2 h streams).
type Forwarder struct {
	path string
	ln   net.Listener
	srv  *http.Server

	mu     sync.Mutex
	target string // the engine socket; "" when no worker serves
}

// NewForwarder listens on the Unix socket at path, mode 0600.
func NewForwarder(path string) (*Forwarder, error) {
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, err
	}
	f := &Forwarder{path: path, ln: ln}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(&url.URL{Scheme: "http", Host: "engine"})
			r.Out.Host = r.In.Host
		},
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				t := f.Target()
				if t == "" {
					return nil, errNoWorker
				}
				var d net.Dialer
				return d.DialContext(ctx, "unix", t)
			},
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
		},
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			status := http.StatusBadGateway
			if errors.Is(err, errNoWorker) {
				status = http.StatusServiceUnavailable
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"message":"the Instance's engine is not serving","type":"ServiceUnavailable","code":` + strconv.Itoa(status) + `}}`))
		},
	}
	f.srv = &http.Server{Handler: proxy, ReadHeaderTimeout: 30 * time.Second}
	go func() { _ = f.srv.Serve(ln) }()
	return f, nil
}

var errNoWorker = errors.New("no engine worker serves")

// Path is the serving socket's path.
func (f *Forwarder) Path() string { return f.path }

// SetTarget sets the engine socket to forward to; "" stops forwarding, and new
// requests then get 503.
func (f *Forwarder) SetTarget(socket string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.target = socket
}

// Target returns the current engine socket.
func (f *Forwarder) Target() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.target
}

// Close stops serving and removes the socket.
func (f *Forwarder) Close() error {
	err := f.srv.Close()
	_ = os.Remove(f.path)
	return err
}
