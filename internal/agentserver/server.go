// Package agentserver is the controller-side HTTP server of the agent API
// (HBEP-7, The allocation agent: the endpoint; pkg/agentapi, Transport). It
// terminates TLS 1.3 with the endpoint certificate the launcher created,
// authenticates every call from the Allocation's tag, credential file and
// recorded job ID, and passes the authenticated call to an agentapi.Backend,
// which the lifecycle controller implements. It holds no session and keeps no
// state of its own.
package agentserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Defaults and limits of the HTTP server.
const (
	// DefaultPollInterval is how often a long poll asks the Backend again.
	DefaultPollInterval = time.Second

	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 120 * time.Second
	// writeTimeoutMargin is added to the long-poll timeout to make the write
	// timeout, which also covers the handler: a shorter one would cut every poll.
	writeTimeoutMargin = 30 * time.Second
	// shutdownGrace is how long Serve waits for in-flight calls after its
	// context ends, before it closes their connections.
	shutdownGrace = 5 * time.Second
)

// Config configures a Server.
type Config struct {
	// Lookup finds the Allocation a tag names.
	Lookup AllocationLookup
	// CredentialsDir is agentapi.Host.CredentialsDir(): where the Allocations'
	// credential files are.
	CredentialsDir string
	// Backend is the lifecycle controller's side of the API.
	Backend agentapi.Backend
	// Host gives the certificate and key (Serve) and the loopback address
	// (Start).
	Host agentapi.Host
	// Logger receives the server's lines; nil is slog.Default().
	Logger *slog.Logger
	// PollInterval is how often a long poll calls Backend.Desired again; 0 is
	// DefaultPollInterval.
	PollInterval time.Duration
	// LongPollTimeout is how long a poll waits for a change; 0 is
	// agentapi.LongPollTimeout.
	LongPollTimeout time.Duration
}

// Server is the agent API's HTTP server. It is safe for concurrent use.
type Server struct {
	lookup       AllocationLookup
	credDir      string
	backend      agentapi.Backend
	host         agentapi.Host
	log          *slog.Logger
	pollInterval time.Duration
	longPoll     time.Duration

	// addr is where Start listens: the Host's agent endpoint address.
	addr string
}

// New validates cfg and returns a Server.
func New(cfg Config) (*Server, error) {
	var problems []error
	if cfg.Lookup == nil {
		problems = append(problems, errors.New("Lookup is required"))
	}
	if cfg.Backend == nil {
		problems = append(problems, errors.New("Backend is required"))
	}
	if cfg.CredentialsDir == "" {
		problems = append(problems, errors.New("CredentialsDir is required"))
	}
	if cfg.Host.StateDir == "" {
		problems = append(problems, errors.New("Host.StateDir is required"))
	}
	if cfg.PollInterval < 0 {
		problems = append(problems, fmt.Errorf("PollInterval %v is negative", cfg.PollInterval))
	}
	if cfg.LongPollTimeout < 0 {
		problems = append(problems, fmt.Errorf("LongPollTimeout %v is negative", cfg.LongPollTimeout))
	}
	if err := errors.Join(problems...); err != nil {
		return nil, fmt.Errorf("agentserver: %w", err)
	}
	s := &Server{
		lookup:       cfg.Lookup,
		credDir:      cfg.CredentialsDir,
		backend:      cfg.Backend,
		host:         cfg.Host,
		log:          cfg.Logger,
		pollInterval: cfg.PollInterval,
		longPoll:     cfg.LongPollTimeout,
		addr:         cfg.Host.AgentEndpointAddr(),
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	if s.pollInterval == 0 {
		s.pollInterval = DefaultPollInterval
	}
	if s.longPoll == 0 {
		s.longPoll = agentapi.LongPollTimeout
	}
	return s, nil
}

// Handler returns the routes, without TLS: for tests, and for a caller that
// wraps them.
func (s *Server) Handler() http.Handler { return http.HandlerFunc(s.serveHTTP) }

// NeedLeaderElection is false: a replica that cannot serve answers
// agentapi.CodeUnavailable through its Backend, and the agent retries.
func (s *Server) NeedLeaderElection() bool { return false }

// Start listens on the Host's agent endpoint address, which it refuses unless
// it is a loopback IP, and serves until ctx ends. It makes the Server a
// controller-runtime Runnable.
func (s *Server) Start(ctx context.Context) error {
	if err := checkLoopback(s.addr); err != nil {
		return fmt.Errorf("agentserver: %w", err)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("agentserver: %w", err)
	}
	return s.Serve(ctx, ln)
}

// checkLoopback returns an error unless addr is host:port with a loopback IP
// literal and a valid port: the agent endpoint is reached only through the
// forward or the front on the control-plane host, never from the network.
func checkLoopback(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("agent endpoint address %q: %w", addr, err)
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("agent endpoint address %q is not a loopback IP address", addr)
	}
	if p, err := net.LookupPort("tcp", port); err != nil || p < 1 {
		return fmt.Errorf("agent endpoint address %q has no valid port", addr)
	}
	return nil
}

// httpServer returns the http.Server of Serve, whose requests run under the
// context requests.
func (s *Server) httpServer(requests context.Context) *http.Server {
	return &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		WriteTimeout:      s.longPoll + writeTimeoutMargin,
		ErrorLog:          slog.NewLogLogger(s.log.Handler(), slog.LevelWarn),
		BaseContext:       func(net.Listener) context.Context { return requests },
	}
}

// Serve serves TLS 1.3 on ln with the certificate and key of the Host, until
// ctx ends. It takes ownership of ln and closes it. On ctx's end it cancels the
// requests in flight, so long polls answer at once, waits briefly for them,
// then closes what is left, and returns nil. Any other end is an error.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	cert, err := tls.LoadX509KeyPair(s.host.AgentCertFile(), s.host.AgentKeyFile())
	if err != nil {
		ln.Close() // Serve owns ln; the load error is the one to report
		return fmt.Errorf("agentserver: endpoint certificate: %w", err)
	}
	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		// HTTP/1.1 only: "Connection: close" after an oversize body means
		// nothing to HTTP/2, and the agent's client needs no more.
		NextProtos: []string{"http/1.1"},
	}
	requests, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()
	srv := s.httpServer(requests)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(tls.NewListener(ln, tlsConfig)) }()

	select {
	case err := <-served: // Serve returns an error whenever it ends
		return fmt.Errorf("agentserver: %w", err)
	case <-ctx.Done():
	}
	cancelRequests()
	grace, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(grace); err != nil {
		s.log.Warn("agent API: shutdown ran out of time; closing connections", "err", err)
		srv.Close() // the connections' close errors are not actionable
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("agentserver: %w", err)
	}
	return nil
}
