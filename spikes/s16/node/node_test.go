// Package node is spike S16's node side, built as a test binary
// (go test -c -o s16node.test ./node) because internal/agent/enginedouble may
// be linked only by _test.go files (its import guard walks the whole module
// tree, spikes included). After spike S14's node (spikes/s14/node).
//
// With S16_NODE=1 the binary runs the node instead of its tests:
//
//   - one engine double per tunnel (enginedouble.Handler with the spec's token
//     delay, the tunnel's model as its served name and the Instance's key as
//     its API key, as the front's proxy sends it), each on its own Unix
//     socket, which that tunnel's frpc reaches through its
//     unix_domain_socket plugin;
//   - in host-dialed mode, one job side per tunnel (hostdial.StartJob: the
//     agent's two loopback listeners and its relay socket, published in the
//     Allocation's directory), frpc's serverPort being the serving listener's.
//
// With "relay" as its first argument it is the relay step that srun runs on
// the login node: hostdial.Relay over its standard input and output, as the
// agent's relay subcommand does.
//
// It writes the ready file once everything listens, and the job sides'
// session counts to the state file every 2 s and at its end.
package node

import (
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
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == agentapi.RelaySubcommand {
		os.Exit(runRelay(os.Args[2:]))
	}
	if os.Getenv("S16_NODE") == "1" {
		if err := runNode(os.Args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "s16node:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// runRelay is the relay step: hostdial.Relay with the step's standard streams,
// its failure written as the agent writes it (hostdial.RelayMessage), so the
// owner's ClassifyEnd reads it.
func runRelay(args []string) int {
	a, err := agentapi.ParseRelayArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "s16node relay:", err)
		return 2
	}
	if err := hostdial.Relay(a, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, hostdial.RelayMessage(a, err))
		return 1
	}
	return 0
}

type spec struct {
	Mode         string `json:"mode"`
	TokenDelayMs int    `json:"token_delay_ms"`
	Tunnels      []struct {
		Index       int    `json:"index"`
		UID         string `json:"uid"`
		Allocation  string `json:"allocation"`
		InstanceKey string `json:"instance_key"`
		Model       string `json:"model"`
		Socket      string `json:"socket"`
		Dir         string `json:"dir"`
	} `json:"tunnels"`
}

type ready struct {
	ServingPorts []int `json:"serving_ports"` // per tunnel, host-dialed mode only
	PID          int   `json:"pid"`
}

// jobState is one job side's sessions, for the heartbeat-loss count: more
// than one session on a channel means its relay dropped and came back.
type jobState struct {
	Index           int    `json:"index"`
	ControlSessions uint64 `json:"control_sessions"`
	ServingSessions uint64 `json:"serving_sessions"`
	Refused         uint64 `json:"refused"`
	ServingUp       bool   `json:"serving_up"`
}

func readSpec(path string) (spec, error) {
	var sp spec
	b, err := os.ReadFile(path)
	if err != nil {
		return sp, err
	}
	if err := json.Unmarshal(b, &sp); err != nil {
		return sp, err
	}
	if sp.TokenDelayMs <= 0 || len(sp.Tunnels) == 0 {
		return sp, errors.New("the spec has no token delay or no tunnels")
	}
	for i, t := range sp.Tunnels {
		if t.Index != i || t.Socket == "" || t.Model == "" || t.InstanceKey == "" {
			return sp, fmt.Errorf("tunnel %d is incomplete", i)
		}
	}
	return sp, nil
}

// engineConfig is a tunnel's engine double: the spec's pace, the tunnel's
// model as the served name, and the Instance's key, which vLLM checks and
// the front's proxy sends.
func engineConfig(delay time.Duration, model, key string) enginedouble.Config {
	cfg := enginedouble.DefaultConfig()
	cfg.TokenDelay, cfg.ServedName, cfg.APIKey = delay, model, key
	return cfg
}

func runNode(args []string) error {
	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	specPath := fs.String("spec", "", "the configuration's spec (JSON)")
	tmp := fs.String("tmp", "", "the job sides' TMPDIR, for their relay sockets")
	readyPath := fs.String("ready", "", "the ready file")
	statePath := fs.String("state", "", "the state file")
	logPath := fs.String("log", "", "log file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sp, err := readSpec(*specPath)
	if err != nil {
		return err
	}
	if *readyPath == "" || *statePath == "" {
		return errors.New("-ready and -state are required")
	}
	lf, err := os.OpenFile(*logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer lf.Close()
	log := slog.New(slog.NewTextHandler(lf, nil))
	delay := time.Duration(sp.TokenDelayMs) * time.Millisecond

	var servers []*http.Server
	defer func() {
		for _, s := range servers {
			_ = s.Close()
		}
	}()
	for _, t := range sp.Tunnels {
		_ = os.Remove(t.Socket)
		ln, err := net.Listen("unix", t.Socket)
		if err != nil {
			return fmt.Errorf("tunnel %d's engine socket: %w", t.Index, err)
		}
		srv := &http.Server{Handler: enginedouble.Handler(engineConfig(delay, t.Model, t.InstanceKey)), ReadHeaderTimeout: time.Minute}
		servers = append(servers, srv)
		go func() { _ = srv.Serve(ln) }()
	}

	rd := ready{PID: os.Getpid()}
	var jobs []*hostdial.Job
	defer func() {
		for _, j := range jobs {
			_ = j.Close()
		}
	}()
	if sp.Mode == "host-dialed" {
		for _, t := range sp.Tunnels {
			if err := os.MkdirAll(t.Dir, 0o700); err != nil {
				return err
			}
			j, err := hostdial.StartJob(hostdial.JobOptions{TmpDir: *tmp, AllocDir: t.Dir, Allocation: t.Allocation, Logger: log.With("tunnel", t.Index)})
			if err != nil {
				return fmt.Errorf("tunnel %d's job side: %w", t.Index, err)
			}
			jobs = append(jobs, j)
			rd.ServingPorts = append(rd.ServingPorts, j.Port(agentapi.RelayServing))
		}
	}
	if err := writeJSON(*readyPath, rd); err != nil {
		return err
	}
	state := func() []jobState {
		var st []jobState
		for i, j := range jobs {
			st = append(st, jobState{Index: i, ControlSessions: j.Sessions(agentapi.RelayControl),
				ServingSessions: j.Sessions(agentapi.RelayServing), Refused: j.Refused(agentapi.RelayServing),
				ServingUp: j.Connected(agentapi.RelayServing)})
		}
		return st
	}
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if err := writeJSON(*statePath, state()); err != nil {
				log.Warn("the state file", "err", err)
			}
		case <-term:
			return writeJSON(*statePath, state())
		}
	}
}

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// The node's own unit tests. They run only the node's parts; the whole stack
// runs under the driver (s16.py), never under go test.

func TestEngineConfigServesTheTunnelsModelBehindItsKey(t *testing.T) {
	cfg := engineConfig(200*time.Millisecond, "s16-m0007", "a-key")
	if cfg.TokenDelay != 200*time.Millisecond || cfg.ServedName != "s16-m0007" || cfg.APIKey != "a-key" {
		t.Fatalf("engineConfig = delay %v, name %q, key %q", cfg.TokenDelay, cfg.ServedName, cfg.APIKey)
	}
	// The rest stays the double's documented defaults.
	def := enginedouble.DefaultConfig()
	if cfg.MaxModelLen != def.MaxModelLen || cfg.PrefixCaching != def.PrefixCaching {
		t.Fatalf("engineConfig changed a default: %+v", cfg)
	}
}

func TestReadSpecRefusesATunnelWithoutItsKey(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "spec.json")
	body := `{"mode":"direct","token_delay_ms":200,"tunnels":[{"index":0,"uid":"u","model":"m","socket":"/s"}]}`
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSpec(p); err == nil {
		t.Fatal("readSpec accepted a tunnel with no Instance key")
	}
}
