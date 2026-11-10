package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// tunnelSetupTimeout is the front's default tunnelSetupTimeout
// (internal/front/config.go, DefaultTunnelSetupTimeout), which the front
// passes to the passthrough as its dial timeout to frps.
const tunnelSetupTimeout = 5 * time.Second

// runListener is the front's dial-home listener as internal/front builds it
// (server.go, the dial-home server; dialhome.go, DialHome and dialHomeTLS) in
// ssh and host-dialed modes, as in spike S14: TLS with the launcher's
// certificate, at least TLS 1.2; /agent/v1/ answers 404; every other path
// goes to the /~!frp passthrough to frps (frppass.go, copied); ReadHeaderTimeout
// one minute.
//
// With -frps-bulk, a websocket whose TLS server name is BulkServerName goes to
// that second frps instead: layout (c)'s frpc with tcpMux off cannot use the
// first, which wraps every connection in a multiplexed session (frp v0.71.1,
// server/service.go:747-753). The front has no such routing today; this is a
// stand-in for the change layout (c) would need, and part of its cost.
func runListener(args []string) error {
	fs := flag.NewFlagSet("listener", flag.ContinueOnError)
	listen := fs.String("listen", "127.0.0.1:29983", "the dial-home listener's address")
	frps := fs.String("frps", "127.0.0.1:29980", "frps's bind port")
	frpsBulk := fs.String("frps-bulk", "", "the second frps's bind port, for the bulk server name (layout c)")
	cert := fs.String("cert", "", "certificate (PEM)")
	key := fs.String("key", "", "key (PEM)")
	logPath := fs.String("log", "", "log file (default: standard error)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, a := range []string{*listen, *frps} {
		if err := requireLoopback(a); err != nil {
			return err
		}
	}
	if *frpsBulk != "" {
		if err := requireLoopback(*frpsBulk); err != nil {
			return err
		}
	}
	pair, err := tls.LoadX509KeyPair(*cert, *key)
	if err != nil {
		return err
	}
	log, closeLog, err := openLog(*logPath)
	if err != nil {
		return err
	}
	defer closeLog()
	main := newFrpPassthrough(*frps, tunnelSetupTimeout, log)
	var bulk *frpPassthrough
	if *frpsBulk != "" {
		bulk = newFrpPassthrough(*frpsBulk, tunnelSetupTimeout, log)
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	tl := tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12})
	srv := &http.Server{Handler: dialHome(main, bulk), ReadHeaderTimeout: time.Minute}
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(tl) }()
	select {
	case <-term:
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	_ = srv.Close()
	main.Close()
	if bulk != nil {
		bulk.Close()
	}
	return nil
}

// dialHome routes as internal/front's DialHome does outside direct mode, and
// sends a connection made for BulkServerName to bulk when there is one.
func dialHome(main, bulk *frpPassthrough) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/agent/v1/") {
			http.NotFound(w, r)
			return
		}
		if bulk != nil && r.TLS != nil && r.TLS.ServerName == BulkServerName {
			bulk.ServeHTTP(w, r)
			return
		}
		main.ServeHTTP(w, r)
	})
}

// openLog opens a text logger at Info on path, or on standard error.
func openLog(path string) (*slog.Logger, func(), error) {
	if path == "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil)), func() {}, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, err
	}
	return slog.New(slog.NewTextHandler(f, nil)), func() { _ = f.Close() }, nil
}
