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
type transport struct {
	client *agent.Client
	frpc   *agent.Frpc
	stops  []func()
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
		if err := t.startHostDialed(job, &co, getenv, log); err != nil {
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
	return t, nil
}

// startHostDialed binds the agent's two loopback listeners and the relay
// sockets for the job, and points the control client and frpc at the
// listeners (HBEP-6, Dial-home modes, host-dialed mode). Both ports are the
// kernel's, taken once, kept for the Allocation's life, and never change when
// a relay restarts, so frpc is never restarted for a relay.
func (t *transport) startHostDialed(job agent.Job, co *agent.ClientOptions, getenv func(string) string, log *slog.Logger) error {
	dir, err := hostdial.MakeRelayDir(agentapi.RelayRoot(getenv), job.JobID)
	if err != nil {
		return err
	}
	j, err := hostdial.StartJob(hostdial.JobOptions{Dir: dir, Logger: log})
	if err != nil {
		_ = os.Remove(dir)
		return err
	}
	unpublish, err := hostdial.PublishRelayDir(job.Config.StagingDir, job.JobID, dir)
	if err != nil {
		_ = j.Close()
		_ = os.Remove(dir)
		return err
	}
	t.stops = append(t.stops, func() {
		unpublish()
		_ = j.Close()
		_ = os.Remove(dir)
	})
	control := j.Addr(agentapi.RelayControl)
	co.Dial = func(ctx context.Context) (net.Conn, error) {
		d := net.Dialer{Timeout: 10 * time.Second}
		return d.DialContext(ctx, "tcp4", control)
	}
	port := j.Port(agentapi.RelayServing)
	t.frpc.Server = func() (agent.FrpcTarget, <-chan struct{}) {
		return agent.FrpcTarget{Host: "127.0.0.1", Port: port, Ready: true}, nil
	}
	log.Info("host-dialed: listening for the relays", "control", control, "serving", port, "relayDir", dir)
	return nil
}
