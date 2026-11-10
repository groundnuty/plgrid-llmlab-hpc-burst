package relaykeeper

// New for issue #343 (PR 3): the real path the Runner's tests drive, copied
// from internal/hostdial/harness_test.go (its TestMain, newWorld, submit,
// roundTrip and waitFor; the last two are in realclient_test.go), not
// imported, since that is another package's test code. The keeper's real Runner (OwnerRunner) runs the system ssh to a fake
// login node (internal/rehearsal: a user-level sshd whose forced command
// hands the line to a run-mode fake Slurm), whose srun -u --overlap step runs
// this test binary as the agent's "relay <channel>" in a running job, whose
// batch process is this test binary again, as the agent's job side
// (hostdial.StartJob). Every byte a test checks crosses the job's loopback
// listener, the session over the srun step's standard streams, the ssh
// connection and the Runner's end, into a target listener of the test's, and
// back. So no test here has both ends in one process with no srun standard
// input and output between them (HBEP-6: "None of these proves anything
// here").
//
// No test here signals a process: every relay is stopped by the Runner,
// through its pidfd, and every node by its own Close.

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// rkJobRole is the first argument that makes this binary a job's batch
// process.
const rkJobRole = "relaykeeper-job"

// TestMain makes this binary the login node's forced command
// (rehearsal.Dispatch), the relay step ("relay <channel>", as hb-agent's own
// subcommand does it) and a job's batch process (rkJobRole), before the
// tests' flags are parsed.
func TestMain(m *testing.M) {
	rehearsal.Dispatch()
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case agentapi.RelaySubcommand:
			os.Exit(rkRelayMain(os.Args[2:]))
		case rkJobRole:
			os.Exit(rkJobMain(os.Args[2:]))
		}
	}
	os.Exit(rehearsal.ReportLeaks(m.Run()))
}

// rkRelayMain is "relay <channel> ..." as cmd/hb-agent runs it: the
// arguments, then hostdial.Relay on the step's streams, and
// hostdial.RelayMessage's line and exit code on a failure.
func rkRelayMain(args []string) int {
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

// rkPorts is what a job writes once its listeners are bound.
type rkPorts struct {
	Control, Serving int
}

// rkJobMain is "relaykeeper-job <ports file> <wait> <Allocation directory>
// <UID>": the agent's job side as cmd/hb-agent starts it, with the job's
// TMPDIR and the given wait for a session, until TERM. It logs to its
// standard error, the job's output file, which a test reads for the job
// side's own account of a relay it refused. It writes its ports to the file
// by a rename, so a reader never sees half of it.
func rkJobMain(args []string) int {
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
	b, _ := json.Marshal(rkPorts{Control: j.Port(agentapi.RelayControl), Serving: j.Port(agentapi.RelayServing)})
	tmp := args[0] + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil || os.Rename(tmp, args[0]) != nil {
		fmt.Fprintln(os.Stderr, "job: the ports file:", err)
		return 1
	}
	<-term
	_ = j.Close()
	return 0
}

// rkTarget is one channel's loopback port on the control-plane host, a test
// listener: it greets each connection with its name and a newline, then
// echoes every byte.
type rkTarget struct {
	name     string
	ln       net.Listener
	accepted atomic.Int64
}

func startRKTarget(t *testing.T, name string) *rkTarget {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tg := &rkTarget{name: name, ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			tg.accepted.Add(1)
			go tg.serve(c)
		}
	}()
	return tg
}

func (tg *rkTarget) serve(c net.Conn) {
	defer c.Close()
	if _, err := io.WriteString(c, tg.name+"\n"); err != nil {
		return
	}
	_, _ = io.Copy(c, c)
}

func (tg *rkTarget) port() int { return tg.ln.Addr().(*net.TCPAddr).Port }

// rkWorld is one fake cluster behind a login node, with the credentials the
// keeper's sets carry and the targets of both channels.
type rkWorld struct {
	t       *testing.T
	base    string
	root    string
	plat    *fakeslurm.Platform
	node    *rehearsal.LoginNode
	creds   slurm.SSHConfig
	agent   string // the "agent binary" on the cluster: this test binary, linked into the fake's file system
	staging string // its staging directory, which holds the Allocations' directories
	targets map[agentapi.RelayChannel]*rkTarget
	n       atomic.Int64
}

// rkWorldSeq numbers this process's worlds, so their job IDs differ.
var rkWorldSeq atomic.Int64

func newRKWorld(t *testing.T) *rkWorld {
	t.Helper()
	rehearsal.RequireTools(t)
	base, err := os.MkdirTemp("", "rk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	w := &rkWorld{t: t, base: base, root: filepath.Join(base, "r")}
	home := filepath.Join(w.root, "home", "example-user")
	for _, d := range []string{home, filepath.Join(base, "n"), filepath.Join(base, "s"), filepath.Join(base, "b")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// Job IDs of this world's own: the relay steps are found by SLURM_JOB_ID.
	first := 40_000_000 + (os.Getpid()%10_000)*1_000 + int(rkWorldSeq.Add(1))*20
	w.plat = fakeslurm.New(fake.NewClock(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)), fakeslurm.Config{
		FirstJobID: first,
		Run:        &fakeslurm.RunMode{Root: w.root, JobDir: filepath.Join(base, "j"), KillWait: 2 * time.Second},
	})
	t.Cleanup(func() {
		if err := w.plat.Close(); err != nil {
			t.Errorf("Platform.Close: %v", err)
		}
	})
	w.node, err = rehearsal.StartLoginNode(rehearsal.LoginConfig{
		Dir: filepath.Join(base, "n"), Platform: w.plat,
		// The job sides' sockets are under each job's TMPDIR, in the world's
		// directory; their fallback is a directory of the world's too, never
		// the host's /dev/shm.
		Login: fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": home, "HB_TEST_SHM": filepath.Join(base, "s")}},
	})
	if err != nil {
		t.Fatalf("StartLoginNode: %v", err)
	}
	t.Cleanup(func() {
		if err := w.node.Close(); err != nil {
			t.Errorf("LoginNode.Close: %v", err)
		}
	})
	credsPath := filepath.Join(base, "creds")
	if err := w.node.WriteCredentials(credsPath); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(credsPath)
	if err != nil {
		t.Fatal(err)
	}
	if w.creds, err = slurm.ParseSSHConfig(b, ""); err != nil {
		t.Fatal(err)
	}
	// The relay step's program and the job's batch process are this binary,
	// linked into the cluster's file system as a staged agent is.
	w.staging = filepath.Join(w.root, "stage")
	w.agent = filepath.Join(w.staging, agentapi.StagedAgentPath(strings.Repeat("e", 64)))
	if err := os.MkdirAll(filepath.Dir(w.agent), 0o700); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Link(exe, w.agent); err != nil {
		t.Fatalf("link the test binary as the staged agent: %v", err)
	}
	w.targets = map[agentapi.RelayChannel]*rkTarget{
		agentapi.RelayControl: startRKTarget(t, "control"),
		agentapi.RelayServing: startRKTarget(t, "serving"),
	}
	t.Cleanup(func() {
		if t.Failed() {
			if b, err := os.ReadFile(filepath.Join(base, "n", "sshd.log")); err == nil {
				t.Logf("sshd's log:\n%s", b)
			}
			outs, _ := filepath.Glob(filepath.Join(home, "slurm-*.out"))
			for _, o := range outs {
				if b, err := os.ReadFile(o); err == nil {
					t.Logf("%s:\n%s", filepath.Base(o), b)
				}
			}
		}
	})
	return w
}

// ports are the channels' target ports, for the Runner.
func (w *rkWorld) ports() map[agentapi.RelayChannel]int {
	return map[agentapi.RelayChannel]int{agentapi.RelayControl: w.targets[agentapi.RelayControl].port(), agentapi.RelayServing: w.targets[agentapi.RelayServing].port()}
}

// ssh is the login node's credentials as a set carries them.
func (w *rkWorld) ssh() wire.SSH {
	return wire.SSH{HostName: w.creds.HostName, User: w.creds.User, Port: w.creds.Port, IdentityFile: w.creds.IdentityFile, KnownHostsFile: w.creds.UserKnownHostsFile}
}

// rkJob is one running job whose batch process is the agent's job side.
type rkJob struct {
	tag, id  string
	allocDir string
	uid      string
	ports    rkPorts
}

// submit starts a job tagged tag whose job side waits wait for a session, and
// returns once its listeners are bound.
func (w *rkWorld) submit(tag string, wait time.Duration) rkJob {
	w.t.Helper()
	n := w.n.Add(1)
	portsFile := filepath.Join(w.base, "ports-"+strconv.FormatInt(n, 10))
	j := rkJob{tag: tag, allocDir: filepath.Join(w.staging, agentapi.AllocationDir(tag)), uid: fmt.Sprintf("0a1b2c3d-0000-4000-9100-%012d", n)}
	if err := os.MkdirAll(j.allocDir, 0o700); err != nil {
		w.t.Fatal(err)
	}
	line, err := slurm.Line("sbatch", "--parsable", "--job-name="+tag, "--wrap=exec "+w.agent+" "+rkJobRole+" "+portsFile+" "+wait.String()+" "+j.allocDir+" "+j.uid)
	if err != nil {
		w.t.Fatal(err)
	}
	res, err := w.node.Session().Run(w.t.Context(), slurm.Command{Line: line})
	if err != nil || res.ExitCode != 0 {
		w.t.Fatalf("sbatch: %v, %+v", err, res)
	}
	j.id = strings.TrimSpace(string(res.Stdout))
	rkWaitFor(w.t, time.Minute, "job "+j.id+"'s listeners", func() bool {
		b, err := os.ReadFile(portsFile)
		if err != nil {
			return false
		}
		return json.Unmarshal(b, &j.ports) == nil && j.ports.Control != 0
	})
	return j
}

// entry is the job's Allocation as a set carries it, of generation gen.
func (w *rkWorld) entry(j rkJob, gen string) wire.Entry {
	return wire.Entry{UID: j.uid, Tag: j.tag, JobID: j.id, AllocDir: j.allocDir, AgentPath: w.agent, Gen: gen}
}

// sruns are the srun lines the fake ran for a job ID.
func (w *rkWorld) sruns(jobID string) []string {
	var out []string
	for _, c := range w.plat.Calls() {
		if len(c.Words) == 0 || c.Words[0] != "srun" {
			continue
		}
		for _, word := range c.Words {
			if word == "--jobid="+jobID {
				out = append(out, strings.Join(c.Words, " "))
			}
		}
	}
	return out
}

// srunsWithSeq are the srun lines the fake ran for a job ID with --seq n.
func (w *rkWorld) srunsWithSeq(jobID string, n int64) []string {
	var out []string
	for _, l := range w.sruns(jobID) {
		if strings.HasSuffix(l, " --seq "+strconv.FormatInt(n, 10)) {
			out = append(out, l)
		}
	}
	return out
}

// jobLog is the job's output file, its job side's log.
func (w *rkWorld) jobLog(jobID string) string {
	var out string
	_ = filepath.WalkDir(w.root, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == "slurm-"+jobID+".out" {
			if b, err := os.ReadFile(p); err == nil {
				out = string(b)
			}
		}
		return nil
	})
	return out
}
