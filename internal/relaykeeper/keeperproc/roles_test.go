package keeperproc

// The roles this test binary plays besides running the tests, each a process
// of its own:
//   - the login node's forced command (rehearsal.Dispatch);
//   - the agent's "relay <channel>", the srun step's program;
//   - the agent's job side, a job's batch process (jobRole);
//   - the relay keeper (keeperRole): relaykeeper.Run, stopped by the end of
//     its standard input;
//   - a controller double (controllerRole): it sends what the test tells it,
//     one JSON line per command on its standard input, over the keeper's
//     socket with wire.Exchange, and writes one JSON line per answer on its
//     standard output.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The roles' first arguments.
const (
	jobRole        = "keeperproc-job"
	keeperRole     = "keeperproc-keeper"
	controllerRole = "keeperproc-controller"
)

// TestMain dispatches the roles before the tests' flags are parsed, and
// reports the login nodes' leaks after the tests.
func TestMain(m *testing.M) {
	rehearsal.Dispatch()
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case agentapi.RelaySubcommand:
			os.Exit(relayMain(os.Args[2:]))
		case jobRole:
			os.Exit(jobMain(os.Args[2:]))
		case keeperRole:
			os.Exit(keeperMain(os.Args[2:]))
		case controllerRole:
			os.Exit(controllerMain(os.Args[2:]))
		}
	}
	os.Exit(rehearsal.ReportLeaks(m.Run()))
}

// relayMain is "relay <channel> ..." as cmd/hb-agent runs it: the arguments,
// then hostdial.Relay on the step's streams, and hostdial.RelayMessage's line
// and exit code on a failure.
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

// jobPorts is what a job writes once its listeners are bound: the channels'
// loopback ports, and its status listener's.
type jobPorts struct {
	Control, Serving, Status int
	Socket                   string
	PID                      int
}

// jobChannel is what the job side shows of one channel: the sessions it
// accepted, the relays it refused as superseded, and whether a session is up
// now (hostdial.Job's Sessions, Refused and Connected).
type jobChannel struct {
	Sessions, Refused uint64
	Connected         bool
}

// jobMain is "<jobRole> <ports file> <wait> <Allocation directory> <UID>": the
// agent's job side as cmd/hb-agent starts it, with the job's TMPDIR and the
// given wait for a session, until TERM. Besides the channels' listeners it
// binds a status listener that answers each connection with one JSON line of
// both channels' jobChannel, so a test reads the session the job side kept.
// It writes its ports to the file by a rename, so a reader never sees half
// of it.
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
	j, err := hostdial.StartJob(hostdial.JobOptions{ShmDir: os.Getenv("HB_TEST_SHM"), TmpDir: os.Getenv("TMPDIR"), AllocDir: args[2], Allocation: args[3], Wait: wait, Logger: log})
	if err != nil {
		fmt.Fprintln(os.Stderr, "job:", err)
		return 1
	}
	st, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "job: the status listener:", err)
		_ = j.Close()
		return 1
	}
	go func() {
		for {
			c, err := st.Accept()
			if err != nil {
				return
			}
			out := map[agentapi.RelayChannel]jobChannel{}
			for _, ch := range agentapi.RelayChannels {
				out[ch] = jobChannel{Sessions: j.Sessions(ch), Refused: j.Refused(ch), Connected: j.Connected(ch)}
			}
			_ = c.SetDeadline(time.Now().Add(5 * time.Second))
			_ = json.NewEncoder(c).Encode(out)
			_ = c.Close()
		}
	}()
	b, _ := json.Marshal(jobPorts{Control: j.Port(agentapi.RelayControl), Serving: j.Port(agentapi.RelayServing),
		Status: st.Addr().(*net.TCPAddr).Port, Socket: j.Socket(), PID: os.Getpid()})
	tmp := args[0] + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil || os.Rename(tmp, args[0]) != nil {
		fmt.Fprintln(os.Stderr, "job: the ports file:", err)
		return 1
	}
	<-term
	_ = st.Close()
	_ = j.Close()
	return 0
}

// keeperConfig is the keeper role's configuration, in a file the test writes.
type keeperConfig struct {
	StateDir                                string
	Control, Serving                        int
	Ready                                   string
	MinBackoffMS, MaxBackoffMS, StopGraceMS int64
}

// keeperReady is what the keeper role writes, by a rename, once its socket
// serves: E, the socket's path and its own PID.
type keeperReady struct {
	Epoch  int64
	Socket string
	PID    int
}

// keeperMain is "<keeperRole> <config file>": the relay keeper's process,
// relaykeeper.Run, with its log on standard error at Info. It stops, as the
// launcher's down stops it in effect, when its standard input ends: the test
// closes it, or the test process is gone. So no test needs a signal to stop a
// keeper, and none outlives the test process.
func keeperMain(args []string) int {
	if len(args) != 1 {
		return 2
	}
	b, err := os.ReadFile(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, "keeper:", err)
		return 2
	}
	var c keeperConfig
	if err := json.Unmarshal(b, &c); err != nil {
		fmt.Fprintln(os.Stderr, "keeper:", err)
		return 2
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()
	var readyErr error
	err = relaykeeper.Run(ctx, relaykeeper.ProcessConfig{
		StateDir:   c.StateDir,
		Ports:      map[agentapi.RelayChannel]int{agentapi.RelayControl: c.Control, agentapi.RelayServing: c.Serving},
		MinBackoff: time.Duration(c.MinBackoffMS) * time.Millisecond, MaxBackoff: time.Duration(c.MaxBackoffMS) * time.Millisecond,
		StopGrace: time.Duration(c.StopGraceMS) * time.Millisecond, Logger: log,
		Ready: func(epoch int64, socket string) {
			b, _ := json.Marshal(keeperReady{Epoch: epoch, Socket: socket, PID: os.Getpid()})
			tmp := c.Ready + ".tmp"
			if err := os.WriteFile(tmp, b, 0o600); err != nil {
				readyErr = err
			} else if err := os.Rename(tmp, c.Ready); err != nil {
				readyErr = err
			}
			if readyErr != nil {
				log.Error("the ready file could not be written", "err", readyErr)
			}
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "keeper:", err)
		return 1
	}
	if readyErr != nil {
		return 1
	}
	return 0
}

// command is one of the test's commands to a controller double:
//   - "set" sends Set once;
//   - "lead" sends Set at once and then again every EveryMS, as a leading
//     controller re-sends its level-triggered set, until the next command;
//   - "ack" sends Ack once;
//   - "pause" stops a "lead" and sends nothing.
//
// Every command first stops the "lead" before it, so the double never sends
// two things at once.
type command struct {
	Op      string           `json:"op"`
	Set     *wire.DesiredSet `json:"set,omitempty"`
	Ack     *wire.Ack        `json:"ack,omitempty"`
	EveryMS int64            `json:"everyMs,omitempty"`
}

// answer is a double's answer to one command: the keeper's reply, by kind,
// or the exchange's error.
type answer struct {
	Kind    string        `json:"kind"`
	Report  *wire.Report  `json:"report,omitempty"`
	Acked   *wire.Acked   `json:"acked,omitempty"`
	Refused *wire.Refused `json:"refused,omitempty"`
	Error   string        `json:"error,omitempty"`
}

// controllerMain is "<controllerRole> <socket>": a controller double. It ends
// when its standard input ends.
func controllerMain(args []string) int {
	if len(args) != 1 {
		return 2
	}
	sock := args[0]
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 64<<10), 4*wire.MaxMessage)
	out := json.NewEncoder(os.Stdout)
	stop := func() {}
	for in.Scan() {
		stop()
		stop = func() {}
		var c command
		var a answer
		switch err := json.Unmarshal(in.Bytes(), &c); {
		case err != nil:
			a = answer{Kind: "error", Error: "the command: " + err.Error()}
		case (c.Op == "set" || c.Op == "lead") && c.Set != nil:
			a = sendSet(sock, *c.Set)
			if c.Op == "lead" && c.EveryMS > 0 {
				stop = lead(sock, *c.Set, time.Duration(c.EveryMS)*time.Millisecond)
			}
		case c.Op == "ack" && c.Ack != nil:
			var r wire.Acked
			a = answerOf(wire.KindAcked, wire.Exchange(context.Background(), sock, wire.KindAck, *c.Ack, &r))
			if a.Kind == string(wire.KindAcked) {
				a.Acked = &r
			}
		case c.Op == "pause":
			a = answer{Kind: "paused"}
		default:
			a = answer{Kind: "error", Error: "unknown command " + c.Op}
		}
		if err := out.Encode(a); err != nil {
			stop()
			return 1
		}
	}
	stop()
	return 0
}

// sendSet sends one set and returns the keeper's answer.
func sendSet(sock string, s wire.DesiredSet) answer {
	var r wire.Report
	a := answerOf(wire.KindReport, wire.Exchange(context.Background(), sock, wire.KindSet, s, &r))
	if a.Kind == string(wire.KindReport) {
		a.Report = &r
	}
	return a
}

// answerOf is an exchange's error as an answer: a refusal, or another error,
// or none, which is a reply of kind ok.
func answerOf(ok wire.Kind, err error) answer {
	var re *wire.RefusedError
	switch {
	case errors.As(err, &re):
		return answer{Kind: string(wire.KindRefused), Refused: &re.Refused}
	case err != nil:
		return answer{Kind: "error", Error: err.Error()}
	}
	return answer{Kind: string(ok)}
}

// lead re-sends s every d until the returned stop is called; stop returns
// once no send is in flight.
func lead(sock string, s wire.DesiredSet, d time.Duration) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(d)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				var r wire.Report
				_ = wire.Exchange(ctx, sock, wire.KindSet, s, &r)
			}
		}
	}()
	return func() {
		cancel()
		wg.Wait()
	}
}
