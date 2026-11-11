package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// exampleAgentPath is the relay line's agent binary: a path of the staged
// agent's shape on a cluster. Nothing runs it; the node side reads only the
// relay's arguments after it.
const exampleAgentPath = "/staging/example/agent/hb-agent"

// exampleLogin names the one login node every Target shares; the owner is
// given slots for all of them on it.
const exampleLogin = "login.example.invalid"

// runOwner runs internal/hostdial's relay owner, unchanged, for every tunnel
// of the spec: two relays per Allocation (control and serving), each a
// s17bridge process started as the owner starts ssh, with pipes for its
// standard streams, and the session's host end (hostdial.Serve) over them.
// The serving relay's streams join the dial-home listener's port; nothing
// opens a stream on the control relay in this spike, so it holds an idle
// session with its keepalives.
func runOwner(args []string) error {
	fs := flag.NewFlagSet("owner", flag.ContinueOnError)
	specPath := fs.String("spec", "", "the configuration's spec (JSON)")
	bridge := fs.String("bridge", "", "the s17bridge binary, the ssh stand-in")
	link := fs.String("link", "127.0.0.1:29925", "the node's relay acceptor, through the link emulator")
	controlPort := fs.Int("control-port", 29927, "the control channel's loopback port (the agent endpoint's; nothing listens in this spike)")
	servingPort := fs.Int("serving-port", 29923, "the serving channel's loopback port: the dial-home listener")
	statePath := fs.String("state", "", "where to write the relays' state every 2 s")
	sshConfig := fs.String("ssh-config", "", "ssh mode: the ssh configuration for the login node's sshd, through the link (ssh_config form)")
	nodeReady := fs.String("node-ready", "", "ssh mode: the ssh node's ready file (each Allocation's job, directory and UID, and the agent's path)")
	sshControl := fs.String("ssh-control-dir", "", "ssh mode: a private directory for the relay clients' configuration file")
	logPath := fs.String("log", "", "log file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	spec, err := readSpec(*specPath)
	if err != nil {
		return err
	}
	if spec.Mode != ModeHostDialed && spec.Mode != ModeSSH {
		return fmt.Errorf("the owner runs only in host-dialed and ssh mode, not %q", spec.Mode)
	}
	if spec.Mode == ModeHostDialed {
		if err := requireLoopback(*link); err != nil {
			return err
		}
		if *bridge == "" {
			return fmt.Errorf("-bridge is required")
		}
	}
	if *statePath == "" {
		return fmt.Errorf("-state is required")
	}
	log, closeLog, err := openLog(*logPath)
	if err != nil {
		return err
	}
	defer closeLog()

	errs := &stderrs{by: map[*exec.Cmd]*capped{}}
	o, err := hostdial.NewOwner(hostdial.OwnerOptions{
		Ports: map[agentapi.RelayChannel]int{agentapi.RelayControl: *controlPort, agentapi.RelayServing: *servingPort},
		Epoch: func(context.Context) (int64, error) { return 1, nil },
		// One login node for all: give it a pair of slots per Allocation,
		// so the cap of 4 per login (DefaultRelaysPerLogin) does not hold
		// the 3rd Allocation back. The cap is HBEP-6's, not what is measured.
		Slots:  hostdial.NewSlots(2 * len(spec.Tunnels)),
		Logger: log,
	})
	if err != nil {
		return err
	}
	jobs := jobList{}
	for _, t := range spec.Tunnels {
		jobs[t.Tag] = provider.Job{ID: t.Job, Name: t.Tag, Phase: v1alpha1.AllocationRunning}
	}
	if spec.Mode == ModeSSH {
		// The relays' ssh is the system ssh (sshrun.Relay, as the relay owner starts it), to the login node's
		// real sshd through the link; each Allocation is a job on the ssh node's fake Slurm.
		cb, err := os.ReadFile(*sshConfig)
		if err != nil {
			return err
		}
		cfg, err := slurm.ParseSSHConfig(cb, "")
		if err != nil {
			return err
		}
		relay, err := sshrun.NewRelay(sshrun.RelayOptions{Name: "s17", Config: cfg, ControlDir: *sshControl})
		if err != nil {
			return err
		}
		defer relay.Close()
		var rd sshReady
		rb, err := os.ReadFile(*nodeReady)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(rb, &rd); err != nil {
			return err
		}
		for _, t := range rd.Tunnels {
			jobs[t.Tag] = provider.Job{ID: t.JobID, Name: t.Tag, Phase: v1alpha1.AllocationRunning}
		}
		for _, t := range rd.Tunnels {
			o.Want(hostdial.Target{
				Tag: t.Tag, JobID: t.JobID, AgentPath: rd.Agent, AllocDir: t.AllocDir, UID: t.UID, Login: relay.Login(),
				Jobs: jobs, Gen: "1", Command: relay.Command, Stderr: sshrun.Stderr, Describe: sshrun.Classify,
			})
		}
	} else {
		for _, t := range spec.Tunnels {
			o.Want(hostdial.Target{
				Tag: t.Tag, JobID: t.Job, AgentPath: exampleAgentPath, AllocDir: t.Dir, UID: t.UID, Login: exampleLogin,
				Jobs: jobs, Gen: "1",
				Command: func(line string) (*exec.Cmd, error) {
					cmd := exec.Command(*bridge, "-addr", *link, "-line", line)
					errs.attach(cmd)
					return cmd, nil
				},
				Stderr: errs.take,
			})
		}
	}
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
			// The last state, taken while the relays are still kept: it counts
			// every start and session of the run. Start then stops each relay
			// by PID and forgets them.
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

// stderrs keeps the first 4 KiB of each relay process's standard error, for
// the owner's ClassifyEnd (as sshrun.Stderr does for ssh).
type stderrs struct {
	mu sync.Mutex
	by map[*exec.Cmd]*capped
}

func (s *stderrs) attach(cmd *exec.Cmd) {
	c := &capped{max: 4096}
	cmd.Stderr = c
	s.mu.Lock()
	s.by[cmd] = c
	s.mu.Unlock()
}

func (s *stderrs) take(cmd *exec.Cmd) string {
	s.mu.Lock()
	c := s.by[cmd]
	delete(s.by, cmd)
	s.mu.Unlock()
	if c == nil {
		return ""
	}
	return c.String()
}

// capped is a buffer that keeps at most max bytes.
type capped struct {
	mu  sync.Mutex
	max int
	b   bytes.Buffer
}

func (c *capped) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if room := c.max - c.b.Len(); room > 0 {
		c.b.Write(p[:min(len(p), room)])
	}
	return len(p), nil
}

func (c *capped) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.b.String()
}

// sshReady is the part of the ssh node's ready file (sshnode.Ready) the owner reads.
type sshReady struct {
	Agent   string `json:"agent"`
	Tunnels []struct {
		Tag      string `json:"tag"`
		UID      string `json:"uid"`
		JobID    string `json:"job"`
		AllocDir string `json:"alloc_dir"`
	} `json:"tunnels"`
}
