package hostdial_test

// The real path of HBEP-6's host-dialed Test plan: the relay owner's ssh
// (sshrun.Relay, the system ssh) to a fake login node (internal/rehearsal: a
// user-level sshd whose forced command hands the line to a run-mode fake
// Slurm), whose srun --overlap step runs this test binary as the agent's
// "relay <channel>" in a running job, whose batch process is this test binary
// again, as the agent's job side (hostdial.Job). Every byte a test checks
// crosses the job's loopback listener, the session over the srun step's
// standard streams, the ssh connection and the control-plane end, into a
// target listener of the test's, and back. Three processes at least sit
// between the two ends, so no test here has both ends in one process with no
// srun standard input and output between them (HBEP-6: "None of these proves
// anything here").
//
// The job-is-ours read goes over the controller's own transport (sshrun.New)
// to the same login node, so it is squeue --me as the controller reads it.

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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// jobRole is the first argument that makes this binary a job's batch process.
const jobRole = "hostdial-job"

// TestMain makes this binary the login node's forced command
// (rehearsal.Dispatch), the relay step ("relay <channel>", as hb-agent's own
// subcommand does it) and a job's batch process (jobRole), before the tests'
// flags are parsed.
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
	os.Exit(m.Run())
}

// relayMain is "relay <channel> ..." as cmd/hb-agent runs it: the arguments,
// then hostdial.Relay on the step's streams, and hostdial.RelayMessage's line
// and exit code on a failure. With HB_TEST_RELAY_GARBAGE set in its
// environment it first writes a line that is not the preamble, as a login
// shell that prints to standard output would.
func relayMain(args []string) int {
	a, err := agentapi.ParseRelayArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "relay:", err)
		return 2
	}
	if os.Getenv("HB_TEST_RELAY_GARBAGE") != "" {
		fmt.Fprintln(os.Stdout, "Welcome to login.example")
	}
	if err := hostdial.Relay(a, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, hostdial.RelayMessage(a, err))
		return hostdial.ExitCode(err)
	}
	return 0
}

// jobPorts is what a job writes once its listeners are bound.
type jobPorts struct {
	Control, Serving int
	Socket           string
	PID              int
}

// jobMain is "hostdial-job <ports file> <wait> <Allocation directory> <UID>":
// the agent's job side as cmd/hb-agent starts it, with the job's TMPDIR and
// the given wait for a session, until TERM. It writes its ports to the file
// by a rename, so a reader never sees half of it.
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
	b, _ := json.Marshal(jobPorts{Control: j.Port(agentapi.RelayControl), Serving: j.Port(agentapi.RelayServing), Socket: j.Socket(), PID: os.Getpid()})
	tmp := args[0] + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil || os.Rename(tmp, args[0]) != nil {
		fmt.Fprintln(os.Stderr, "job: the ports file:", err)
		return 1
	}
	<-term
	_ = j.Close()
	return 0
}

// target is one channel's loopback port on the control-plane host, a test
// listener: it greets each connection with its name and a newline, then
// echoes every byte, and keeps what each connection sent.
type target struct {
	name     string
	ln       net.Listener
	accepted atomic.Int64

	mu   sync.Mutex
	recv [][]byte
}

func startTarget(t *testing.T, name string) *target {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tg := &target{name: name, ln: ln}
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

func (tg *target) serve(c net.Conn) {
	defer c.Close()
	if _, err := io.WriteString(c, tg.name+"\n"); err != nil {
		return
	}
	tg.mu.Lock()
	i := len(tg.recv)
	tg.recv = append(tg.recv, nil)
	tg.mu.Unlock()
	b := make([]byte, 32<<10)
	for {
		n, err := c.Read(b)
		if n > 0 {
			tg.mu.Lock()
			tg.recv[i] = append(tg.recv[i], b[:n]...)
			tg.mu.Unlock()
			if _, werr := c.Write(b[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (tg *target) port() int { return tg.ln.Addr().(*net.TCPAddr).Port }

// received is every connection's bytes, in order of acceptance.
func (tg *target) received() [][]byte {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	out := make([][]byte, len(tg.recv))
	for i, r := range tg.recv {
		out[i] = append([]byte(nil), r...)
	}
	return out
}

// world is one fake cluster behind a login node, with the controller's
// transport, the targets of both channels, and a decoy on another port
// (the plugin endpoint's place).
type world struct {
	t       *testing.T
	base    string
	plat    *fakeslurm.Platform
	node    *rehearsal.LoginNode
	creds   slurm.SSHConfig
	agent   string // the "agent binary" on the cluster: this test binary, linked into the fake's file system
	staging string // its staging directory, which holds the Allocations' directories
	firstID int    // the fake's first job ID
	jobs    *slurm.Batch
	relay   *sshrun.Relay
	targets map[agentapi.RelayChannel]*target
	decoy   *target
	n       atomic.Int64
}

const clusterUser = "example-user"

// worldSeq numbers this process's worlds, so their job IDs differ.
var worldSeq atomic.Int64

// worldConfig changes a world's fake cluster.
type worldConfig struct {
	// stepEnv is added to every srun step's environment (fakeslurm.RunMode's
	// StepEnv).
	stepEnv map[string]string
}

func newWorld(t *testing.T, mods ...func(*worldConfig)) *world {
	t.Helper()
	var cfg worldConfig
	for _, m := range mods {
		m(&cfg)
	}
	rehearsal.RequireTools(t)
	base, err := os.MkdirTemp("", "hd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	w := &world{t: t, base: base}
	root := filepath.Join(base, "r")
	home := filepath.Join(root, "home", clusterUser)
	for _, d := range []string{home, filepath.Join(base, "c"), filepath.Join(base, "n")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// Job IDs of this world's own: the relay steps are found by SLURM_JOB_ID.
	w.firstID = 30_000_000 + (os.Getpid()%10_000)*1_000 + int(worldSeq.Add(1))*20
	w.plat = fakeslurm.New(fake.NewClock(time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)), fakeslurm.Config{
		FirstJobID: w.firstID,
		Run:        &fakeslurm.RunMode{Root: root, JobDir: filepath.Join(base, "j"), KillWait: 2 * time.Second, StepEnv: cfg.stepEnv},
	})
	t.Cleanup(func() {
		if err := w.plat.Close(); err != nil {
			t.Errorf("Platform.Close: %v", err)
		}
	})
	w.node, err = rehearsal.StartLoginNode(rehearsal.LoginConfig{
		Dir: filepath.Join(base, "n"), Platform: w.plat,
		Login: fakeslurm.Login{User: clusterUser, Env: map[string]string{"HOME": home}},
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
	// linked into the cluster's file system as a staged agent is,
	// <stagingDir>/agent/<digest>/hb-agent.
	w.staging = filepath.Join(root, "stage")
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
	tr, err := sshrun.New(sshrun.Options{Name: "hostdial", Config: w.creds, ControlDir: filepath.Join(base, "c")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	w.jobs = slurm.NewBatch(tr, provider.Profile{Name: "example", Spec: v1alpha1.PlatformProfileSpec{JSONDataParser: "v0.0.42", StagingDir: root}},
		// The Provider's own listing is what a relay's start reads (HBEP-6);
		// a short poll interval keeps it fresh within a test.
		provider.Settings{Provider: "example"}, slurm.Options{PollInterval: 200 * time.Millisecond, BusyInterval: 200 * time.Millisecond})
	if w.relay, err = sshrun.NewRelay(sshrun.RelayOptions{Name: "hostdial", Config: w.creds, ControlDir: filepath.Join(base, "c")}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.relay.Close() })
	w.targets = map[agentapi.RelayChannel]*target{
		agentapi.RelayControl: startTarget(t, "control"),
		agentapi.RelayServing: startTarget(t, "serving"),
	}
	w.decoy = startTarget(t, "plugin")
	t.Cleanup(func() {
		if t.Failed() {
			if b, err := os.ReadFile(filepath.Join(base, "n", "sshd.log")); err == nil {
				t.Logf("sshd's log:\n%s", b)
			}
		}
	})
	return w
}

// ports are the channels' target ports, for an Owner.
func (w *world) ports() map[agentapi.RelayChannel]int {
	return map[agentapi.RelayChannel]int{agentapi.RelayControl: w.targets[agentapi.RelayControl].port(), agentapi.RelayServing: w.targets[agentapi.RelayServing].port()}
}

// job is one running job whose batch process is the agent's job side.
type job struct {
	tag, id  string
	allocDir string // the Allocation's directory, which holds relay-socket
	uid      string // the Allocation's UID
	ports    jobPorts
}

// submit starts a job tagged tag whose job side waits wait for a session, and
// returns once its listeners are bound.
func (w *world) submit(tag string, wait time.Duration) job {
	w.t.Helper()
	portsFile := filepath.Join(w.base, "ports-"+strconv.FormatInt(w.n.Add(1), 10))
	j := w.allocation(tag)
	j.id = w.sbatch(tag, "exec "+w.agent+" "+jobRole+" "+portsFile+" "+wait.String()+" "+j.allocDir+" "+j.uid)
	id := j.id
	waitFor(w.t, time.Minute, "job "+id+"'s listeners", func() bool {
		b, err := os.ReadFile(portsFile)
		if err != nil {
			return false
		}
		return json.Unmarshal(b, &j.ports) == nil && j.ports.Control != 0
	})
	return j
}

// allocation is an Allocation's directory, mode 700, and UID for tag.
func (w *world) allocation(tag string) job {
	w.t.Helper()
	j := job{tag: tag, allocDir: filepath.Join(w.staging, agentapi.AllocationDir(tag)), uid: "0a1b2c3d-0000-4000-8000-" + fmt.Sprintf("%012d", w.n.Add(1))}
	if err := os.MkdirAll(j.allocDir, 0o700); err != nil {
		w.t.Fatal(err)
	}
	return j
}

// sbatch submits a running job tagged tag whose batch script is wrap, and
// returns its ID.
func (w *world) sbatch(tag, wrap string) string {
	w.t.Helper()
	line, err := slurm.Line("sbatch", "--parsable", "--job-name="+tag, "--wrap="+wrap)
	if err != nil {
		w.t.Fatal(err)
	}
	res, err := w.node.Session().Run(w.t.Context(), slurm.Command{Line: line})
	if err != nil || res.ExitCode != 0 {
		w.t.Fatalf("sbatch: %v, %+v", err, res)
	}
	return strings.TrimSpace(string(res.Stdout))
}

// target is the relay target of a job for an Owner.
func (w *world) target(j job) hostdial.Target {
	return hostdial.Target{Tag: j.tag, JobID: j.id, AgentPath: w.agent, AllocDir: j.allocDir, UID: j.uid, Login: w.relay.Login(),
		Jobs: w.jobs, Command: w.relay.Command, Stderr: sshrun.Stderr, Describe: sshrun.Classify, Gen: "1"}
}

// owner starts an Owner with test timings; it is stopped at the test's end.
func (w *world) owner(o hostdial.OwnerOptions) (*hostdial.Owner, context.CancelFunc, <-chan error) {
	w.t.Helper()
	if o.Ports == nil {
		o.Ports = w.ports()
	}
	if o.Epoch == nil {
		o.Epoch = epoch(1)
	}
	if o.Slots == nil {
		o.Slots = hostdial.NewSlots(0)
	}
	if o.MinBackoff == 0 {
		o.MinBackoff = 200 * time.Millisecond
	}
	if o.MaxBackoff == 0 {
		o.MaxBackoff = 2 * time.Second
	}
	if o.StopGrace == 0 {
		o.StopGrace = 2 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(testWriter{w.t}, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	own, err := hostdial.NewOwner(o)
	if err != nil {
		w.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error) // closed once Start returns, so the test and the clean-up may both wait
	go func() {
		if err := own.Start(ctx); err != nil {
			w.t.Errorf("the relay owner: %v", err)
		}
		close(done)
	}()
	w.t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			w.t.Error("the relay owner did not stop within 30 s")
		}
	})
	return own, cancel, done
}

// epoch is a fixed fence epoch E for an Owner.
func epoch(e int64) func(context.Context) (int64, error) {
	return func(context.Context) (int64, error) { return e, nil }
}

// testWriter logs to the test.
type testWriter struct{ t *testing.T }

func (tw testWriter) Write(p []byte) (int, error) {
	tw.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// waitFor polls cond every 50 ms until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for !cond() {
		if time.Now().After(end) {
			t.Fatalf("%s: not within %s", what, d)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// relayOf is the owner's state of one relay.
func relayOf(o *hostdial.Owner, tag string, ch agentapi.RelayChannel) hostdial.RelayState {
	for _, r := range o.Relays() {
		if r.Tag == tag && r.Channel == ch {
			return r
		}
	}
	return hostdial.RelayState{}
}

// waitUp waits until both relays of tag have a process, and returns them.
func waitUp(t *testing.T, o *hostdial.Owner, tag string) map[agentapi.RelayChannel]hostdial.RelayState {
	t.Helper()
	out := map[agentapi.RelayChannel]hostdial.RelayState{}
	waitFor(t, time.Minute, "both relays of "+tag+" up", func() bool {
		for _, ch := range agentapi.RelayChannels {
			r := relayOf(o, tag, ch)
			if r.PID == 0 {
				return false
			}
			out[ch] = r
		}
		return true
	})
	return out
}

// conn is a client of a job's loopback listener, as frpc or the control
// client is.
type conn struct {
	c     net.Conn
	r     *bufio.Reader
	greet string
}

// dial connects to a job's channel listener and reads the target's greeting,
// within d; an error means no byte came back (no session, or a cut).
func dial(j job, ch agentapi.RelayChannel, d time.Duration) (*conn, error) {
	port := j.ports.Control
	if ch == agentapi.RelayServing {
		port = j.ports.Serving
	}
	c, err := net.DialTimeout("tcp4", "127.0.0.1:"+strconv.Itoa(port), d)
	if err != nil {
		return nil, err
	}
	_ = c.SetDeadline(time.Now().Add(d))
	r := bufio.NewReader(c)
	g, err := r.ReadString('\n')
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("no greeting: %w", err)
	}
	_ = c.SetDeadline(time.Time{})
	return &conn{c: c, r: r, greet: strings.TrimSuffix(g, "\n")}, nil
}

// echo writes p and reads it back, within d.
func (c *conn) echo(p []byte, d time.Duration) error {
	_ = c.c.SetDeadline(time.Now().Add(d))
	defer c.c.SetDeadline(time.Time{})
	if _, err := c.c.Write(p); err != nil {
		return err
	}
	got := make([]byte, len(p))
	if _, err := io.ReadFull(c.r, got); err != nil {
		return err
	}
	if string(got) != string(p) {
		return errors.New("the echo differs from what was sent")
	}
	return nil
}

func (c *conn) close() { _ = c.c.Close() }

// roundTrip dials a job's channel, checks that the greeting names want, and
// echoes p.
func roundTrip(t *testing.T, j job, ch agentapi.RelayChannel, want string, p []byte) {
	t.Helper()
	c, err := dial(j, ch, 30*time.Second)
	if err != nil {
		t.Fatalf("job %s's %s listener: %v", j.id, ch, err)
	}
	defer c.close()
	if c.greet != want {
		t.Fatalf("job %s's %s listener reached the %q target, want %q", j.id, ch, c.greet, want)
	}
	if err := c.echo(p, 30*time.Second); err != nil {
		t.Fatalf("job %s's %s channel: the echo of %d bytes: %v", j.id, ch, len(p), err)
	}
}

// alive reports whether pid names a live process (not a zombie).
func alive(pid int) bool {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	return i > 0 && i+2 < len(s) && s[i+2] != 'Z'
}

// stepPIDs are the live relay steps for a channel in a job: this binary run
// as "relay <channel>" with the job's SLURM_JOB_ID.
func stepPIDs(t *testing.T, jobID string, ch agentapi.RelayChannel) []int {
	t.Helper()
	ents, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	var out []int
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		args, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err != nil {
			continue
		}
		a := strings.Split(strings.TrimRight(string(args), "\x00"), "\x00")
		if len(a) < 3 || a[1] != agentapi.RelaySubcommand || a[2] != string(ch) {
			continue
		}
		env, err := os.ReadFile("/proc/" + e.Name() + "/environ")
		if err != nil {
			continue
		}
		for _, kv := range strings.Split(string(env), "\x00") {
			if kv == "SLURM_JOB_ID="+jobID && alive(pid) {
				out = append(out, pid)
			}
		}
	}
	return out
}

// sruns are the srun lines the fake ran for a job ID.
func (w *world) sruns(jobID string) []string {
	var out []string
	for _, c := range w.plat.Calls() {
		if len(c.Words) > 0 && c.Words[0] == "srun" {
			for _, word := range c.Words {
				if word == "--jobid="+jobID {
					out = append(out, strings.Join(c.Words, " "))
				}
			}
		}
	}
	return out
}
