package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// transport is the agent's dial-home transport (HBEP-6, Dial-home modes): the
// control client, and frpc, which serves the Instance the agent is told to
// serve. In ssh mode two forwards carry them, each in its own reconnect loop:
// connection 1 the control client's traffic, connection 2 frpc's. In
// host-dialed mode the agent's two loopback listeners carry them, over the
// relays the batch Provider's controller starts (hostdial.Job).
//
// With agent.json's bulk block (beta; HBEP-6, The bulk path), and not in ssh
// mode, a second frpc, the bulk frpc, serves the same Instance from the same
// serving socket to the same target. It has its own files and its own restarts,
// apart from the token frpc's.
type transport struct {
	client *agent.Client
	frpc   *agent.Frpc
	// bulk is the bulk frpc: nil without the bulk block, and in ssh mode.
	bulk  *agent.Frpc
	stops []func()
}

// stop ends every part, the last started first (frpc before the forwards),
// and returns once each process is gone and reaped (HBEP-6, teardown). It
// runs on every path out of run, so no ssh or frpc outlives the agent.
func (t *transport) stop() {
	for i := len(t.stops) - 1; i >= 0; i-- {
		t.stops[i]()
	}
	t.stops = nil
}

// start runs fn until stop.
func (t *transport) start(log *slog.Logger, name string, fn func(context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := fn(ctx); err != nil {
			log.Error(name+" ended", "err", err)
		}
	}()
	t.stops = append(t.stops, func() { cancel(); <-done })
}

// startTransport starts the transport for the job. The caller stops it,
// whatever the error.
func startTransport(job agent.Job, runDir, aid string, getenv func(string) string, stderr io.Writer, log *slog.Logger) (*transport, error) {
	t := &transport{}
	co := agent.ClientOptions{
		Endpoint:        job.Config.Endpoint,
		Pin:             job.Config.CertSPKISHA256,
		Credential:      job.Credential,
		Tag:             job.Config.Tag,
		JobID:           job.JobID,
		AgentInstanceID: aid,
	}
	// frpc runs in every mode, staged by digest, with the launcher's
	// certificate in the Allocation's directory (HBEP-6, frpc in the job).
	t.frpc = &agent.Frpc{
		Binary:         job.Config.FrpcPath(),
		FrpcDigest:     job.Config.FrpcDigest,
		CertFile:       filepath.Join(job.AllocDir, agentapi.StagedDialHomeCertName),
		CertSPKISHA256: job.Config.CertSPKISHA256,
		Credential:     job.Credential,
		RunDir:         runDir,
		Socket:         agent.ServingSocket(runDir),
		Env:            os.Environ(),
		Logger:         log,
	}
	var control *agent.SSHForward
	switch job.Config.Mode {
	case agentapi.DialHomeHostDialed:
		if err := t.startHostDialed(job, getenv("TMPDIR"), &co, log); err != nil {
			return t, err
		}
	case agentapi.DialHomeSSH:
		sshBin, err := lookTool(getenv("HPCBURST_SSH"), "ssh")
		if err != nil {
			return t, err
		}
		forward := func(name string, remote int) *agent.SSHForward {
			return &agent.SSHForward{SSH: sshBin, Config: job.Config, RemotePort: remote, Name: name, Log: stderr, Logger: log}
		}
		control = forward("control", job.Config.SSH.AgentPort)
		tunnel := forward("tunnel", job.Config.SSH.DialHomePort)
		t.start(log, "the control forward", control.Run)
		t.start(log, "the tunnel forward", tunnel.Run)
		co.Dial = control.Dial
		t.frpc.Server = agent.ForwardServer(tunnel)
	default:
		server, err := agent.DirectServer(job.Config.Endpoint)
		if err != nil {
			return t, err
		}
		t.frpc.Server = server
	}
	t.client = agent.NewClient(co)
	// The bulk frpc has the token frpc's target, which the mode set above.
	t.bulk = newBulkFrpc(job, runDir, t.frpc, log)
	if control != nil {
		// A forward that moved to a new port leaves the client's idle
		// connections pointing at the old one.
		t.start(log, "the control client's port watch", func(ctx context.Context) error {
			agent.OnPortChange(ctx, control, func(int) { t.client.CloseIdle() })
			return nil
		})
	}
	// A refused frpc does not stop the agent: the Instance cannot be reached,
	// and the controller ends it with TunnelNotConnected (HBEP-6, Path state).
	t.start(log, "frpc (the Instance cannot be reached, and ends with TunnelNotConnected)", t.frpc.Run)
	if t.bulk != nil {
		// A refused or ended bulk frpc stops neither the agent nor the token
		// frpc: every request then goes through the Instance's one tunnel
		// (HBEP-6, The bulk path: it is absent when its frpc is down).
		t.start(log, "the bulk frpc (requests take the Instance's one tunnel)", t.bulk.Run)
	}
	return t, nil
}

// bulkWanted says whether the agent runs a bulk frpc: agent.json carries the
// bulk block and the mode is not ssh mode (HBEP-6, The bulk path: "Ssh mode gets
// no bulk frpc"). AgentConfig.Validate refuses the block in ssh mode before the
// agent gets here, so in production the mode never decides; it is checked again
// so that the transport never starts a bulk frpc on a mode the bulk path is not
// built for.
func bulkWanted(cfg agentapi.AgentConfig) bool {
	return cfg.Bulk != nil && cfg.Mode != agentapi.DialHomeSSH
}

// newBulkFrpc returns the bulk frpc for the job, or nil when there is none
// (bulkWanted). It runs the token frpc's binary (checked against the same
// digest) to the token frpc's target, from the same serving socket, with its
// own staged certificate, dial-home-bulk.crt of the Allocation's directory, and
// its own files in the run directory (agent.Frpc, Bulk). token's Server must be
// set.
func newBulkFrpc(job agent.Job, runDir string, token *agent.Frpc, log *slog.Logger) *agent.Frpc {
	if !bulkWanted(job.Config) {
		return nil
	}
	return &agent.Frpc{
		Binary:         token.Binary,
		FrpcDigest:     token.FrpcDigest,
		CertFile:       filepath.Join(job.AllocDir, agentapi.StagedBulkDialHomeCertName),
		CertSPKISHA256: job.Config.CertSPKISHA256,
		Credential:     job.Credential,
		RunDir:         runDir,
		Socket:         token.Socket,
		Server:         token.Server,
		Env:            token.Env,
		Logger:         log.With("frpc", "bulk"),
		Bulk:           true,
	}
}

// tunnels tells each of the dial-home transport's frpc which Instance to serve.
type tunnels []agent.Tunnel

// Serve passes uid to every tunnel; none blocks.
func (ts tunnels) Serve(uid string) {
	for _, t := range ts {
		t.Serve(uid)
	}
}

// tunnel is what the agent's convergence loop tells which Instance to serve
// (agent.Options.Tunnel): frpc, and with the bulk path the bulk frpc as well,
// so the Instance's end stops both. Without the bulk frpc it is the token frpc
// itself, as before.
func (t *transport) tunnel() agent.Tunnel {
	if t.bulk == nil {
		return t.frpc
	}
	return tunnels{t.frpc, t.bulk}
}

// startHostDialed binds the agent's two loopback listeners and its relay
// socket, publishes the socket in the Allocation's directory, and points the
// control client and frpc at the listeners (HBEP-6, Dial-home modes,
// host-dialed mode). Both ports are the kernel's, taken once, kept for the
// Allocation's life, and never change when a relay restarts, so frpc is
// never restarted for a relay. tmp is the job's TMPDIR as set, not the
// agent's /tmp default: an unset one is the bootstrap's misconfiguration,
// and the socket has no place (hostdial.ErrNoSocketDir), so the agent stops
// at once instead of holding the job's GPU until its registration deadline.
func (t *transport) startHostDialed(job agent.Job, tmp string, co *agent.ClientOptions, log *slog.Logger) error {
	j, err := hostdial.StartJob(hostdial.JobOptions{TmpDir: tmp, AllocDir: job.AllocDir, Allocation: job.Config.Allocation, Logger: log})
	if err != nil {
		return err
	}
	t.stops = append(t.stops, func() { _ = j.Close() })
	control := j.Addr(agentapi.RelayControl)
	co.Dial = func(ctx context.Context) (net.Conn, error) {
		d := net.Dialer{Timeout: 10 * time.Second}
		return d.DialContext(ctx, "tcp4", control)
	}
	port := j.Port(agentapi.RelayServing)
	t.frpc.Server = func() (agent.FrpcTarget, <-chan struct{}) {
		return agent.FrpcTarget{Host: "127.0.0.1", Port: port, Ready: true}, nil
	}
	// The TMPDIR the socket's directory is under, unless the path was too long for a socket (hostdial.DevShm): the
	// reach-host-dialed rows compare the two (internal/providerctl/conformance, testdata/serving-probe.sh).
	log.Info("host-dialed: listening for the relays", "control", control, "serving", port, "socket", j.Socket(), "tmpdir", tmp)
	return nil
}
