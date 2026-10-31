// Command hb-agent is the allocation agent (HBEP-7, The allocation agent): the
// only process an Allocation's job runs. The batch script's bootstrap checks
// its digest and runs it with the Allocation's directory as the one argument
// (HBEP-4, Submission):
//
//	hb-agent <allocation directory>
//	hb-agent -version
//
// It reads agent.json and the credential from that directory and the job's ID
// and restart count from Slurm's environment (SLURM_JOB_ID,
// SLURM_RESTART_COUNT), and runs until it is told to stop or the job ends.
//
// The environment may name the tools it runs: HPCBURST_APPTAINER (default
// "apptainer" on PATH), HPCBURST_SSH (default "ssh"), HPCBURST_NVIDIA_SMI
// (default "nvidia-smi"), and HF_ENDPOINT, the Hugging Face Hub (default
// https://huggingface.co).
//
// It is built statically, with CGO_ENABLED=0, for amd64 and arm64.
//
// Exit codes: 0 when it stopped on instruction or on a signal, 1 when it could
// not start, 2 on a usage error.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/engine"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/stage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// version is set at build time with -ldflags "-X main.version=<version>".
var version = "dev"

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stderr))
}

func run(args []string, getenv func(string) string, stderr io.Writer) int {
	fs := flag.NewFlagSet("hb-agent", flag.ContinueOnError)
	fs.SetOutput(stderr)
	showVersion := fs.Bool("version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintf(stderr, "hb-agent %s, protocol %s\n", version, agentapi.ProtocolVersion)
		return 0
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: hb-agent <allocation directory>")
		return 2
	}
	log := slog.New(slog.NewTextHandler(stderr, nil))
	if err := agent.CloseInheritedFDs(); err != nil {
		log.Warn("cannot mark inherited file descriptors close-on-exec", "err", err)
	}
	job, err := agent.LoadJob(fs.Arg(0), getenv)
	if err != nil {
		log.Error("cannot start", "err", err)
		return 1
	}
	aid, err := agentapi.NewAgentInstanceID()
	if err != nil {
		log.Error("cannot draw an agent instance ID", "err", err)
		return 1
	}
	log = log.With("tag", job.Config.Tag, "job", job.JobID, "agentInstanceID", aid)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	runDir, err := agent.MakeRunDir(job.TmpDir)
	if err != nil {
		log.Error("cannot start", "err", err)
		return 1
	}
	defer os.RemoveAll(runDir)

	apptainer, err := lookTool(getenv("HPCBURST_APPTAINER"), "apptainer")
	if err != nil {
		log.Error("no apptainer on this node (HBEP-4, open question 15)", "err", err)
		return 1
	}
	reg := agentapi.RegisterRequest{
		RestartCount: job.RestartCount,
		AgentVersion: version,
		Node:         agent.Hostname(),
	}
	if reg.CPUArch, err = agent.CPUArch(); err != nil {
		log.Error("cannot read the CPU architecture", "err", err)
		return 1
	}
	if reg.GPUs, err = agent.ListGPUs(ctx, getenv("HPCBURST_NVIDIA_SMI")); err != nil {
		log.Warn("no GPU list: registering without one", "err", err)
	}

	co := agent.ClientOptions{
		Endpoint:        job.Config.Endpoint,
		Pin:             job.Config.CertSPKISHA256,
		Credential:      job.Credential,
		Tag:             job.Config.Tag,
		JobID:           job.JobID,
		AgentInstanceID: aid,
	}
	var control *agent.SSHForward
	if job.Config.Mode == agentapi.DialHomeSSH {
		sshBin, err := lookTool(getenv("HPCBURST_SSH"), "ssh")
		if err != nil {
			log.Error("no ssh for the ssh dial-home mode", "err", err)
			return 1
		}
		// Connection 1, the control connection to the agent endpoint (HBEP-6,
		// Dial-home modes). Connection 2 and frpc wait for agent.json's
		// dial-home port, frpc digest and certificate (the contracts PR).
		control = &agent.SSHForward{SSH: sshBin, RemotePort: job.Config.SSH.AgentPort, Name: "control", Config: job.Config, Log: stderr, Logger: log}
		// The forward outlives the agent's loop, which sends its last report
		// through it. It is stopped before this function returns, on every
		// path (a stop, a signal, an error): its ssh process gets TERM, then
		// KILL, and is reaped (HBEP-6, teardown). os.Exit would otherwise leave
		// ssh behind, an orphan holding the job's output open.
		fwdCtx, cancelFwd := context.WithCancel(context.Background())
		fwdDone := make(chan struct{})
		go func() {
			defer close(fwdDone)
			if err := control.Run(fwdCtx); err != nil {
				log.Error("ssh forward", "err", err)
			}
		}()
		defer func() {
			cancelFwd()
			<-fwdDone
		}()
		co.Dial = control.Dial
	}
	client := agent.NewClient(co)
	if control != nil {
		// A forward that moved to a new port leaves the client's idle
		// connections pointing at the old one.
		watchCtx, cancelWatch := context.WithCancel(context.Background())
		defer cancelWatch()
		go agent.OnPortChange(watchCtx, control, func(int) { client.CloseIdle() })
	}

	stager, err := stage.New(stage.Config{
		StagingDir:  job.Config.StagingDir,
		Apptainer:   apptainer,
		TmpDir:      runDir,
		HubEndpoint: getenv("HF_ENDPOINT"),
		BaseEnv:     os.Environ(),
		Log:         stderr,
	})
	if err != nil {
		log.Error("cannot start", "err", err)
		return 1
	}
	a, err := agent.New(agent.Options{
		API: client,
		Engines: func(socket, key string, l agentapi.Launch) agent.Engine {
			return engine.New(socket, key, engine.WithReasoning(agent.DeclaresReasoningParser(l.Command)))
		},
		Stager:       stager,
		Apptainer:    apptainer,
		RunDir:       runDir,
		LogDir:       job.AllocDir,
		Registration: reg,
		BaseEnv:      os.Environ(),
		Logger:       log,
	})
	if err != nil {
		log.Error("cannot start", "err", err)
		return 1
	}
	log.Info("allocation agent starting", "version", version, "mode", job.Config.Mode, "cpuArch", reg.CPUArch, "gpus", len(reg.GPUs), "restartCount", job.RestartCount)
	if err := a.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("agent failed", "err", err)
		return 1
	}
	log.Info("allocation agent exiting")
	return 0
}

// lookTool returns the tool's path: the configured one, else name on PATH.
func lookTool(configured, name string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	return exec.LookPath(name)
}
