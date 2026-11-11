package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// runOwner runs internal/hostdial's relay owner, unchanged, for every tunnel
// of a host-dialed spec: two relays per Allocation (control and serving), each
// a real OpenSSH client started by sshrun.Relay with the Provider's -F
// configuration (written by sshrun from the login node's credentials), which
// runs the relay line on the login node (internal/rehearsal's sshd, through
// run-mode fakeslurm's streamed srun, whose step is the node binary running
// hostdial.Relay). The serving relays' streams join the front's dial-home
// listener; nothing opens a stream on the control relays, which hold an idle
// session with its keepalives, as in S14.
//
// -ssh names the ssh binary sshrun starts. The spike's is a wrapper that
// execs the system ssh with "-v -E <log>" (and, when S16_CIPHER is set,
// "-o Ciphers=<it>") before sshrun's own arguments, so each relay's log holds
// the negotiated KEX, cipher and MAC; the process that runs is ssh itself.
func runOwner(args []string) error {
	fs := flag.NewFlagSet("owner", flag.ContinueOnError)
	specPath := fs.String("spec", "", "the configuration's spec (host-dialed)")
	loginPath := fs.String("login", "", "the login node's ready file (its credentials and the held job)")
	sshBin := fs.String("ssh", "", "the ssh binary for sshrun.Relay (the spike's wrapper)")
	agentPath := fs.String("agent", "", "the relay step's program: the node test binary")
	controlDir := fs.String("control-dir", "", "sshrun's private directory for the -F file (mode 700)")
	statePath := fs.String("state", "", "where to write the relays' state every 2 s")
	logPath := fs.String("log", "", "log file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sp, err := readSpec(*specPath)
	if err != nil {
		return err
	}
	if sp.Mode != ModeHostDialed {
		return fmt.Errorf("the owner runs only in host-dialed mode, not %q", sp.Mode)
	}
	if *loginPath == "" || *sshBin == "" || *agentPath == "" || *controlDir == "" || *statePath == "" {
		return errors.New("-login, -ssh, -agent, -control-dir and -state are required")
	}
	var login struct {
		SSHConfig string `json:"ssh_config"`
		JobID     string `json:"job_id"`
	}
	if err := readJSONFile(*loginPath, &login); err != nil {
		return err
	}
	creds, err := slurm.ParseSSHConfig([]byte(login.SSHConfig), "")
	if err != nil {
		return fmt.Errorf("the login node's credentials: %w", err)
	}
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "S16_SSH_LOGDIR=" + os.Getenv("S16_SSH_LOGDIR")}
	if c := os.Getenv("S16_CIPHER"); c != "" {
		env = append(env, "S16_CIPHER="+c)
	}
	relay, err := sshrun.NewRelay(sshrun.RelayOptions{Name: "s16", Config: creds, ControlDir: *controlDir, Binary: *sshBin, Env: env})
	if err != nil {
		return err
	}
	defer relay.Close()
	lf, err := os.OpenFile(*logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer lf.Close()
	log := newSlog(lf)
	host := agentapi.Host{StateDir: sp.StateDir, PortBase: sp.PortBase}
	o, err := hostdial.NewOwner(hostdial.OwnerOptions{
		Ports: host.RelayChannelPorts(),
		Epoch: func(context.Context) (int64, error) { return 1, nil },
		// One login node for all: a pair of slots per Allocation, so the cap
		// of 4 per login does not hold the third Allocation back. The cap is
		// HBEP-6's, not what is measured (as in S14).
		Slots:  hostdial.NewSlots(2 * len(sp.Tunnels)),
		Logger: log,
	})
	if err != nil {
		return err
	}
	jobs := jobList{}
	for _, t := range sp.Tunnels {
		jobs[t.Tag] = provider.Job{ID: login.JobID, Name: t.Tag, Phase: v1alpha1.AllocationRunning}
	}
	for _, t := range sp.Tunnels {
		if err := os.MkdirAll(t.Dir, 0o700); err != nil {
			return err
		}
		o.Want(hostdial.Target{
			Tag: t.Tag, JobID: login.JobID, AgentPath: *agentPath, AllocDir: t.Dir, UID: t.Allocation, Login: relay.Login(),
			Jobs: jobs, Gen: "1", Command: relay.Command, Stderr: sshrun.Stderr, Describe: sshrun.Classify,
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	ran := make(chan error, 1)
	go func() { ran <- o.Start(ctx) }()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if err := writeJSONFile(*statePath, o.Relays()); err != nil {
				log.Warn("the state file", "err", err)
			}
		case <-term:
			err := writeJSONFile(*statePath, o.Relays())
			cancel()
			if serr := <-ran; err == nil {
				err = serr
			}
			return err
		case err := <-ran:
			return fmt.Errorf("the relay owner stopped by itself: %v", err)
		}
	}
}

// jobList is the Provider's listing as the owner reads it: every Allocation's
// job RUNNING under its tag.
type jobList map[string]provider.Job

func (j jobList) ListByTag(_ context.Context, tag string) ([]provider.Job, error) {
	if job, ok := j[tag]; ok {
		return []provider.Job{job}, nil
	}
	return nil, nil
}
