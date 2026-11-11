package sshnode

// The relay of host-dialed mode through a real ssh client, a real sshd and srun's standard streams, in place of
// spike S15's bridge (issue #413). The node is the world of internal/hostdial's harness_test.go, whose
// blueprint this follows: a run-mode fake Slurm (internal/slurm/fakeslurm) behind a fake login node
// (internal/rehearsal: a user-level sshd whose forced command hands each line to the fake, whose srun streams
// the step's standard input and output in frames of at most 64 KiB), a job per Allocation whose batch process
// is this binary as the agent's job side (hostdial.StartJob), and srun steps that run this binary as the
// agent's "relay <channel>" (hostdial.Relay). The ssh clients that carry the relays are the relay owner's
// (sshrun.Relay, the system ssh), in the driver's owner process, which reaches the sshd through the link
// emulator.
//
// With S17_SSH=1 the binary runs the node instead of its tests: one engine double (with the bulk paths) per
// tunnel on a Unix socket for that tunnel's frpc, the fake cluster, the login node and one job per tunnel; it
// writes the ready file once every job side listens, and runs until TERM.
//
// Provenance: internal/hostdial/harness_test.go (this repository, newWorld, submit, jobMain, relayMain).

import (
	stdctx "context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s17/node"
)

const (
	// jobRole is the first argument that makes this binary a job's batch process.
	jobRole = "s17-job"
	// clusterUser is the cluster user's name on the fake: an example name, never an account's.
	clusterUser = "example-user"
)

func TestMain(m *testing.M) {
	rehearsal.Dispatch()
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case agentapi.RelaySubcommand:
			os.Exit(relayMain(os.Args[2:]))
		case jobRole:
			os.Exit(jobMain(os.Args[2:]))
		}
	}
	if os.Getenv("S17_SSH") == "1" {
		code := 0
		if err := runNode(os.Args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "s17ssh:", err)
			code = 1
		}
		os.Exit(rehearsal.ReportLeaks(code))
	}
	os.Exit(rehearsal.ReportLeaks(m.Run()))
}

// relayMain is "relay <channel> ..." as cmd/hb-agent runs it: hostdial.Relay on the step's streams.
func relayMain(args []string) int {
	a, err := agentapi.ParseRelayArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "relay:", err)
		return 2
	}
	if err := hostdial.Relay(a, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, hostdial.RelayMessage(a, err))
		return hostdial.ExitCode(err)
	}
	return 0
}

// jobPorts is what a job side writes once its listeners are bound.
type jobPorts struct {
	Control, Serving int
	PID              int
}

// jobMain is "s17-job <ports file> <wait> <Allocation directory> <UID>": the agent's job side with the job's
// TMPDIR, until TERM.
func jobMain(args []string) int {
	if len(args) != 4 {
		return 2
	}
	wait, err := time.ParseDuration(args[1])
	if err != nil {
		return 2
	}
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	j, err := hostdial.StartJob(hostdial.JobOptions{TmpDir: os.Getenv("TMPDIR"), AllocDir: args[2], Allocation: args[3], Wait: wait, Logger: log})
	if err != nil {
		fmt.Fprintln(os.Stderr, "job:", err)
		return 1
	}
	b, _ := json.Marshal(jobPorts{Control: j.Port(agentapi.RelayControl), Serving: j.Port(agentapi.RelayServing), PID: os.Getpid()})
	tmp := args[0] + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil || os.Rename(tmp, args[0]) != nil {
		fmt.Fprintln(os.Stderr, "job: the ports file:", err)
		return 1
	}
	<-term
	_ = j.Close()
	return 0
}

type spec struct {
	Mode    string `json:"mode"`
	Tunnels []struct {
		Index  int    `json:"index"`
		UID    string `json:"uid"`
		Tag    string `json:"tag"`
		Socket string `json:"socket"`
	} `json:"tunnels"`
}

// Tunnel is one Allocation as the owner needs it.
type Tunnel struct {
	Index    int    `json:"index"`
	UID      string `json:"uid"`
	Tag      string `json:"tag"`
	JobID    string `json:"job"`
	AllocDir string `json:"alloc_dir"`
	Serving  int    `json:"serving_port"`
}

// Ready is what the driver reads: where the sshd listens, the ssh configuration for it, the agent's path and
// each Allocation.
type Ready struct {
	SSHPort   int    `json:"ssh_port"`
	SSHConfig string `json:"ssh_config"`
	Known     string `json:"known_hosts"`
	Agent     string `json:"agent"`
	Base      string `json:"base"`
	// ControlDir is a short private directory for the relay clients' configuration file: sshrun refuses a control
	// socket path above 90 bytes.
	ControlDir string   `json:"control_dir"`
	Tunnels    []Tunnel `json:"tunnels"`
	PID        int      `json:"pid"`
}

func runNode(args []string) error {
	fs := flag.NewFlagSet("sshnode", flag.ContinueOnError)
	specPath := fs.String("spec", "", "the configuration's spec (JSON)")
	readyPath := fs.String("ready", "", "the ready file")
	logPath := fs.String("log", "", "log file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var sp spec
	b, err := os.ReadFile(*specPath)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, &sp); err != nil {
		return err
	}
	if *readyPath == "" || *logPath == "" {
		return errors.New("-ready and -log are required")
	}
	lf, err := os.OpenFile(*logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer lf.Close()
	log := slog.New(slog.NewTextHandler(lf, nil))

	// The engine doubles, one per tunnel, each on its Unix socket, with the bulk paths.
	var servers []*http.Server
	defer func() {
		for _, s := range servers {
			_ = s.Close()
		}
	}()
	for _, t := range sp.Tunnels {
		cfg := enginedouble.DefaultConfig()
		cfg.TokenDelay = 50 * time.Millisecond
		_ = os.Remove(t.Socket)
		ln, err := net.Listen("unix", t.Socket)
		if err != nil {
			return fmt.Errorf("tunnel %d's engine socket: %w", t.Index, err)
		}
		srv := &http.Server{Handler: node.WithBulk(enginedouble.Handler(cfg), log.With("tunnel", t.Index))}
		servers = append(servers, srv)
		go func() { _ = srv.Serve(ln) }()
	}

	// The fake cluster, in a short private directory (sshd's files, the relay socket and the jobs' sockets
	// have path limits, and sshd's configuration allows only safe characters in them).
	base, err := os.MkdirTemp("", "hd")
	if err != nil {
		return err
	}
	root := filepath.Join(base, "r")
	home := filepath.Join(root, "home", clusterUser)
	for _, d := range []string{home, filepath.Join(base, "n"), filepath.Join(base, "s"), filepath.Join(base, "c")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}
	plat := fakeslurm.New(fake.NewClock(time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)), fakeslurm.Config{
		FirstJobID: 40_000_000 + (os.Getpid()%10_000)*1_000,
		Run:        &fakeslurm.RunMode{Root: root, JobDir: filepath.Join(base, "j"), KillWait: 2 * time.Second},
	})
	defer plat.Close()
	node, err := rehearsal.StartLoginNode(rehearsal.LoginConfig{
		Dir: filepath.Join(base, "n"), Platform: plat,
		Login: fakeslurm.Login{User: clusterUser, Env: map[string]string{"HOME": home, "HB_TEST_SHM": filepath.Join(base, "s")}},
	})
	if err != nil {
		return fmt.Errorf("StartLoginNode: %w", err)
	}
	defer node.Close()
	creds := filepath.Join(base, "creds")
	if err := node.WriteCredentials(creds); err != nil {
		return err
	}
	// The relay step's program and the job's batch process are this binary, linked (or copied) into the
	// cluster's file system as a staged agent is, <staging>/agent/<digest>/hb-agent.
	staging := filepath.Join(root, "stage")
	agent := filepath.Join(staging, agentapi.StagedAgentPath(strings.Repeat("e", 64)))
	if err := os.MkdirAll(filepath.Dir(agent), 0o700); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	if err := os.Link(exe, agent); err != nil {
		return fmt.Errorf("link the binary as the staged agent: %w", err)
	}
	rd := Ready{SSHConfig: creds, Known: filepath.Join(base, "n", "known_hosts"), Agent: agent, Base: base, ControlDir: filepath.Join(base, "c"), PID: os.Getpid()}
	if _, port, err := net.SplitHostPort(node.Addr()); err == nil {
		rd.SSHPort, _ = strconv.Atoi(port)
	}
	for _, t := range sp.Tunnels {
		allocDir := filepath.Join(staging, agentapi.AllocationDir(t.Tag))
		if err := os.MkdirAll(allocDir, 0o700); err != nil {
			return err
		}
		portsFile := filepath.Join(base, "ports-"+strconv.Itoa(t.Index))
		line, err := slurm.Line("sbatch", "--parsable", "--job-name="+t.Tag, "--wrap=exec "+agent+" "+jobRole+" "+portsFile+" 24h "+allocDir+" "+t.UID)
		if err != nil {
			return err
		}
		res, err := node.Session().Run(stdctx.Background(), slurm.Command{Line: line})
		if err != nil || res.ExitCode != 0 {
			return fmt.Errorf("sbatch for tunnel %d: %v, %+v", t.Index, err, res)
		}
		tn := Tunnel{Index: t.Index, UID: t.UID, Tag: t.Tag, JobID: strings.TrimSpace(string(res.Stdout)), AllocDir: allocDir}
		var jp jobPorts
		deadline := time.Now().Add(2 * time.Minute)
		for jp.Serving == 0 {
			if b, err := os.ReadFile(portsFile); err == nil {
				_ = json.Unmarshal(b, &jp)
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("job %s's listeners are not up after 2 min", tn.JobID)
			}
			time.Sleep(50 * time.Millisecond)
		}
		tn.Serving = jp.Serving
		rd.Tunnels = append(rd.Tunnels, tn)
	}
	b, _ = json.Marshal(rd)
	tmp := *readyPath + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil || os.Rename(tmp, *readyPath) != nil {
		return fmt.Errorf("the ready file: %v", err)
	}
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	<-term
	return nil
}
