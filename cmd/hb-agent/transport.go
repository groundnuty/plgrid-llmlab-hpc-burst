package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// transport is the agent's dial-home transport (HBEP-6, Dial-home modes): the
// control client, and frpc, which serves the Instance the agent is told to
// serve. In ssh mode two forwards carry them, each in its own reconnect loop:
// connection 1 the control client's traffic, connection 2 frpc's.
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
	// frpc runs in both modes, staged by digest, with the launcher's
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
	if job.Config.Mode == agentapi.DialHomeSSH {
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
	} else {
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
