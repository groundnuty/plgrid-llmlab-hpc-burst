package relaykeeper

// New for issue #343 (PR 3): the real path the Runner's tests drive, copied
// from internal/hostdial/harness_test.go (its TestMain, newWorld, submit,
// roundTrip and waitFor), not imported, since that is another package's test
// code. The keeper's real Runner (OwnerRunner) runs the system ssh to a fake
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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

// lingeringSSH is an ssh client for the Runner (OwnerRunnerOptions.SSHBinary)
// that stands in for a relay process that is still there after its stop
// began. Every other relay, and every other call (ssh -G), is the system
// ssh, exec'd as it is.
//   - A relay whose N is listed in the world's trap-seqs file (lingerSeq)
//     sets its TERM to be ignored, then execs the system ssh, which keeps it
//     so: the Runner's stop ends it only when its session ends, or by the
//     KILL after the stop's grace, so it can still connect after its stop
//     began.
//   - A relay started while the world's trap-all file exists (lingerAll)
//     ignores TERM and runs the system ssh as its child; once the ssh has
//     ended, it waits for the world's release file before it exits, so the
//     Runner's process of the relay outlives its ssh connection until the
//     test lets it go, or the KILL after the stop's grace ends it.
func (w *rkWorld) lingeringSSH() string {
	w.t.Helper()
	ssh, err := exec.LookPath("ssh")
	if err != nil {
		w.t.Fatal(err)
	}
	dir := filepath.Join(w.base, "b")
	script := filepath.Join(dir, "ssh")
	body := "#!/bin/sh\n" +
		// Only a relay lingers: ssh -G, the login host's read, never does.
		"case \" $* \" in *\" srun -u --overlap \"*) ;; *) exec " + ssh + " \"$@\";; esac\n" +
		"if [ -e " + dir + "/trap-all ]; then\n" +
		"  trap '' TERM\n" +
		"  " + ssh + " \"$@\"\n" +
		"  rc=$?\n" +
		"  while [ ! -e " + dir + "/release ]; do sleep 0.02; done\n" +
		"  exit $rc\n" +
		"fi\n" +
		"if [ -e " + dir + "/trap-seqs ]; then\n" +
		"  for n in $(cat " + dir + "/trap-seqs); do\n" +
		"    case \" $* \" in *\" --seq $n \"*) trap '' TERM; : > " + dir + "/trapped-$n;; esac\n" +
		"  done\n" +
		"fi\n" +
		"exec " + ssh + " \"$@\"\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		w.t.Fatal(err)
	}
	return script
}

// lingerSeq makes the relay of N n linger past its stop's TERM
// (lingeringSSH). It must be called before that relay's process starts. Once
// the relay's ssh has set its TERM to be ignored, the world's trapped-<n>
// file exists (trapped).
func (w *rkWorld) lingerSeq(n int64) {
	w.t.Helper()
	f, err := os.OpenFile(filepath.Join(w.base, "b", "trap-seqs"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		w.t.Fatal(err)
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, n); err != nil {
		w.t.Fatal(err)
	}
}

// trapped reports whether the relay of N n has set its TERM to be ignored
// (lingerSeq).
func (w *rkWorld) trapped(n int64) bool {
	_, err := os.Stat(filepath.Join(w.base, "b", "trapped-"+strconv.FormatInt(n, 10)))
	return err == nil
}

// lingerAll makes every relay that starts from now on linger once its ssh
// has ended (lingeringSSH), until release, or, with on false, none that
// starts from now on.
func (w *rkWorld) lingerAll(on bool) {
	w.t.Helper()
	if on {
		w.t.Cleanup(w.release)
	}
	p := filepath.Join(w.base, "b", "trap-all")
	if !on {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			w.t.Fatal(err)
		}
		return
	}
	if err := os.WriteFile(p, nil, 0o600); err != nil {
		w.t.Fatal(err)
	}
}

// release lets every lingering relay process end (lingerAll).
func (w *rkWorld) release() {
	if err := os.WriteFile(filepath.Join(w.base, "b", "release"), nil, 0o600); err != nil {
		w.t.Error(err)
	}
}

// rkWaitFor polls cond every 25 ms until it holds or the deadline passes.
func rkWaitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for !cond() {
		if time.Now().After(end) {
			t.Fatalf("%s: not within %s", what, d)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// rkConn is a client of a job's loopback listener, as frpc or the control
// client is.
type rkConn struct {
	c     net.Conn
	r     *bufio.Reader
	greet string
}

// rkDial connects to a job's channel listener and reads the target's
// greeting, within d; an error means no byte came back (no session, or a
// cut).
func rkDial(j rkJob, ch agentapi.RelayChannel, d time.Duration) (*rkConn, error) {
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
	return &rkConn{c: c, r: r, greet: strings.TrimSuffix(g, "\n")}, nil
}

// echo writes p and reads it back, within d.
func (c *rkConn) echo(p []byte, d time.Duration) error {
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

func (c *rkConn) close() { _ = c.c.Close() }

// rkRoundTrip dials a job's channel, checks that the greeting names the
// channel's target, and echoes n bytes of every value.
func rkRoundTrip(t *testing.T, j rkJob, ch agentapi.RelayChannel, n int) {
	t.Helper()
	c, err := rkDial(j, ch, 30*time.Second)
	if err != nil {
		t.Fatalf("job %s's %s listener: %v", j.id, ch, err)
	}
	defer c.close()
	if c.greet != string(ch) {
		t.Fatalf("job %s's %s listener reached the %q target, want %q", j.id, ch, c.greet, ch)
	}
	if err := c.echo(rkPayload(n), 30*time.Second); err != nil {
		t.Fatalf("job %s's %s channel: the echo of %d bytes: %v", j.id, ch, n, err)
	}
}

// rkPayload is n bytes of every value, so a byte that is changed, dropped or
// added in transit shows.
func rkPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 7)
	}
	return b
}

// rkAlive reports whether pid names a live process (not a zombie).
func rkAlive(pid int) bool {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	return i > 0 && i+2 < len(s) && s[i+2] != 'Z'
}

// relaySSHs are the live relay ssh clients this test process started for
// the jobs: its children whose command line carries an srun line for one of
// them. It is the Runner's process count, read from /proc, not from the
// Runner.
func relaySSHs(t *testing.T, jobIDs ...string) []int {
	t.Helper()
	ents, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	self := os.Getpid()
	var out []int
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		s := string(b)
		i := strings.LastIndexByte(s, ')')
		if i < 0 {
			continue
		}
		f := strings.Fields(s[i+1:])
		if len(f) < 2 || f[0] == "Z" || f[1] != strconv.Itoa(self) {
			continue
		}
		args, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err != nil {
			continue
		}
		cmd := string(args)
		for _, id := range jobIDs {
			if strings.Contains(cmd, "srun -u --overlap --jobid="+id+" ") {
				out = append(out, pid)
				break
			}
		}
	}
	return out
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
			if kv == "SLURM_JOB_ID="+jobID && rkAlive(pid) {
				out = append(out, pid)
			}
		}
	}
	return out
}

// logRecorder keeps the Runner's log records, for a test to read what the
// relays did (a start's PID and N, an end's exit code), and echoes them to
// the test's log.
type logRecorder struct {
	t  *testing.T
	mu sync.Mutex
	rs []map[string]string
}

func (l *logRecorder) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (l *logRecorder) Handle(_ context.Context, r slog.Record) error {
	m := map[string]string{"msg": r.Message, "level": r.Level.String()}
	r.Attrs(func(a slog.Attr) bool { m[a.Key] = a.Value.String(); return true })
	l.mu.Lock()
	l.rs = append(l.rs, m)
	l.mu.Unlock()
	return nil
}

func (l *logRecorder) WithAttrs(as []slog.Attr) slog.Handler { return &attrHandler{l: l, as: as} }
func (l *logRecorder) WithGroup(string) slog.Handler         { return l }

// records are the records with msg, each with its attributes.
func (l *logRecorder) records(msg string) []map[string]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]string
	for _, r := range l.rs {
		if r["msg"] == msg {
			out = append(out, r)
		}
	}
	return out
}

// dump logs every record to the test's log.
func (l *logRecorder) dump() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.rs {
		l.t.Logf("%v", r)
	}
}

// attrHandler is the recorder with a logger's attributes added to each
// record.
type attrHandler struct {
	l  *logRecorder
	as []slog.Attr
}

func (h *attrHandler) Enabled(ctx context.Context, lv slog.Level) bool { return true }
func (h *attrHandler) Handle(ctx context.Context, r slog.Record) error {
	r = r.Clone()
	r.AddAttrs(h.as...)
	return h.l.Handle(ctx, r)
}
func (h *attrHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return &attrHandler{l: h.l, as: append(append([]slog.Attr{}, h.as...), as...)}
}
func (h *attrHandler) WithGroup(string) slog.Handler { return h }
